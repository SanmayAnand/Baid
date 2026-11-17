//! Loading the configuration and resolving its secrets.
//!
//! Layers, lowest to highest:
//! 1. built-in defaults ([`Config::default`]);
//! 2. the TOML file (`--config`, default `baid.toml`);
//! 3. environment variables `BAID_<SECTION>__<FIELD>`, e.g. `BAID_SERVER__LISTEN`;
//! 4. command-line flags.
//!
//! Only the `server`, `cache` and `telemetry` sections can be set from the environment.
//! Tenants live in the file. This keeps variables such as `BAID_KEY_ACME`, which *hold* API
//! keys, from being mistaken for config fields.

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::Path;

use baid_core::{Config, ConfigError, ConfigIssue, TenantId};
use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;

use crate::auth::ApiKeys;

/// Settings given on the command line; they override every other layer.
#[derive(Clone, Debug, Default)]
pub struct Overrides {
    /// `--listen`
    pub listen: Option<SocketAddr>,
    /// `--admin-listen`
    pub admin_listen: Option<SocketAddr>,
}

/// Why the configuration could not be loaded.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// The config file does not exist.
    #[error("config file {0} not found")]
    NotFound(String),
    /// The file or an environment variable could not be parsed.
    #[error("cannot read config: {0}")]
    Parse(String),
    /// The values parsed but break a rule.
    #[error(transparent)]
    Invalid(#[from] ConfigError),
}

/// Builds the layered config and validates it.
pub fn load(path: &Path, overrides: &Overrides) -> Result<Config, LoadError> {
    if !path.is_file() {
        return Err(LoadError::NotFound(path.display().to_string()));
    }
    let mut fig = Figment::from(Serialized::defaults(Config::default()))
        .merge(Toml::file(path))
        .merge(Env::prefixed("BAID_").split("__").filter(|k| {
            // Keys keep the variable's original (upper) case; compare case-insensitively.
            let k = k.as_str().to_ascii_lowercase();
            ["server.", "cache.", "telemetry."]
                .iter()
                .any(|section| k.starts_with(section))
        }));
    if let Some(a) = overrides.listen {
        fig = fig.merge(Serialized::default("server.listen", a));
    }
    if let Some(a) = overrides.admin_listen {
        fig = fig.merge(Serialized::default("server.admin_listen", a));
    }
    let config: Config = fig.extract().map_err(|e| LoadError::Parse(e.to_string()))?;
    config.validate()?;
    Ok(config)
}

/// A secret value. `Debug` and `Display` never print it.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wraps a secret.
    pub fn new(s: impl Into<String>) -> Self {
        Secret(s.into())
    }
    /// The secret itself. Call only where it is actually used.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

/// Backend credentials for one tenant.
#[derive(Clone, Debug, Default)]
pub struct AdapterCredentials {
    /// From `adapter_user_env`.
    pub user: Option<Secret>,
    /// From `adapter_password_env`.
    pub password: Option<Secret>,
}

/// Everything the `*_env` references point at.
#[derive(Debug)]
pub struct Secrets {
    /// API key → tenant.
    pub api_keys: ApiKeys,
    /// Backend credentials per tenant.
    pub adapters: HashMap<TenantId, AdapterCredentials>,
}

/// Shortest API key accepted.
pub const MIN_API_KEY_LEN: usize = 16;

/// Reads every `*_env` reference through `lookup` (the process environment in production).
/// Reports every missing or unusable value at once.
pub fn resolve_secrets(
    config: &Config,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Secrets, ConfigError> {
    let mut issues = Vec::new();
    let mut issue = |field: String, reason: &str| {
        issues.push(ConfigIssue {
            field,
            reason: reason.to_owned(),
        })
    };
    let mut keys: Vec<(Secret, TenantId)> = Vec::new();
    let mut adapters = HashMap::new();

    for t in &config.tenants {
        let field = format!("tenants[{}].api_key_env", t.id);
        match lookup(&t.api_key_env) {
            None => issue(
                field,
                &format!("environment variable {} is not set", t.api_key_env),
            ),
            Some(k) if k.len() < MIN_API_KEY_LEN => {
                issue(field, "API key is shorter than 16 characters")
            }
            Some(k) if k.chars().any(char::is_whitespace) => {
                issue(field, "API key contains whitespace")
            }
            Some(k) => {
                if keys.iter().any(|(existing, _)| existing.expose() == k) {
                    issue(field, "two tenants have the same API key");
                } else {
                    keys.push((Secret::new(k), t.id.clone()));
                }
            }
        }

        let mut creds = AdapterCredentials::default();
        for (name, var, slot) in [
            ("adapter_user_env", &t.adapter_user_env, &mut creds.user),
            (
                "adapter_password_env",
                &t.adapter_password_env,
                &mut creds.password,
            ),
        ] {
            if let Some(var) = var {
                match lookup(var) {
                    Some(v) => *slot = Some(Secret::new(v)),
                    None => issue(
                        format!("tenants[{}].{name}", t.id),
                        &format!("environment variable {var} is not set"),
                    ),
                }
            }
        }
        adapters.insert(t.id.clone(), creds);
    }

    if issues.is_empty() {
        Ok(Secrets {
            api_keys: ApiKeys::new(keys),
            adapters,
        })
    } else {
        Err(ConfigError(issues))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use figment::Jail;

    const FILE: &str = r#"
[server]
listen = "127.0.0.1:7070"

[[tenants]]
id = "acme"
api_key_env = "BAID_KEY_ACME"
adapter = "bolt"
adapter_url = "bolt://neo4j:7687"
adapter_password_env = "NEO4J_PASSWORD"

[[tenants]]
id = "globex"
api_key_env = "BAID_KEY_GLOBEX"
adapter = "s3"
adapter_url = "s3://globex"
"#;

    /// Runs `f` with an isolated cwd and environment. figment's `Jail` fixes the closure's
    /// error type to `figment::Error`, which clippy considers large.
    #[allow(clippy::result_large_err)]
    fn jailed<T>(f: impl FnOnce(&mut Jail) -> T) -> T {
        let mut out = None;
        Jail::expect_with(|jail| {
            out = Some(f(jail));
            Ok(())
        });
        out.unwrap()
    }

    #[test]
    fn layers_apply_in_order() {
        jailed(|jail| {
            jail.create_file("baid.toml", FILE).unwrap();
            jail.set_env("BAID_SERVER__ADMIN_LISTEN", "127.0.0.1:9001");
            jail.set_env("BAID_SERVER__LISTEN", "127.0.0.1:9000");
            // Holds a key, not a config field: must be ignored, not rejected.
            jail.set_env("BAID_KEY_ACME", "x");
            let overrides = Overrides {
                listen: Some("127.0.0.1:9999".parse().unwrap()),
                admin_listen: None,
            };
            let c = load(Path::new("baid.toml"), &overrides).unwrap();
            assert_eq!(c.server.listen.port(), 9999, "flag beats env and file");
            assert_eq!(c.server.admin_listen.port(), 9001, "env beats default");
            assert_eq!(c.tenants.len(), 2);
            assert_eq!(
                c.cache.ram_bytes,
                baid_core::ByteSize::gib(2),
                "default kept"
            );
        });
    }

    #[test]
    fn missing_file_is_an_error() {
        jailed(|_| {
            let e = load(Path::new("nope.toml"), &Overrides::default()).unwrap_err();
            assert!(matches!(e, LoadError::NotFound(_)));
        });
    }

    #[test]
    fn invalid_values_are_reported() {
        jailed(|jail| {
            jail.create_file(
                "baid.toml",
                &FILE.replace("127.0.0.1:7070", "127.0.0.1:7071"),
            )
            .unwrap();
            let e = load(Path::new("baid.toml"), &Overrides::default()).unwrap_err();
            assert!(e.to_string().contains("server.admin_listen"), "{e}");
        });
    }

    #[test]
    fn typo_in_file_is_a_parse_error() {
        jailed(|jail| {
            jail.create_file("baid.toml", &format!("{FILE}\n[cache]\nram = \"1GiB\"\n"))
                .unwrap();
            let e = load(Path::new("baid.toml"), &Overrides::default()).unwrap_err();
            assert!(matches!(e, LoadError::Parse(_)), "{e}");
        });
    }

    fn config() -> Config {
        toml::from_str(FILE).unwrap()
    }

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| m.get(k).cloned()
    }

    #[test]
    fn secrets_resolve() {
        let s = resolve_secrets(
            &config(),
            env(&[
                ("BAID_KEY_ACME", "acme-key-0123456789"),
                ("BAID_KEY_GLOBEX", "globex-key-0123456789"),
                ("NEO4J_PASSWORD", "hunter2"),
            ]),
        )
        .unwrap();
        let acme = TenantId::new("acme").unwrap();
        assert_eq!(s.api_keys.tenant_for("acme-key-0123456789"), Some(&acme));
        assert_eq!(
            s.adapters[&acme].password.as_ref().map(Secret::expose),
            Some("hunter2")
        );
        let printed = format!("{s:?}");
        assert!(
            !printed.contains("hunter2") && !printed.contains("acme-key"),
            "{printed}"
        );
    }

    #[test]
    fn every_secret_problem_is_reported() {
        let e = resolve_secrets(&config(), env(&[("BAID_KEY_GLOBEX", "short")])).unwrap_err();
        let fields: Vec<_> = e.0.iter().map(|i| i.field.as_str()).collect();
        assert_eq!(
            fields,
            [
                "tenants[acme].api_key_env",
                "tenants[acme].adapter_password_env",
                "tenants[globex].api_key_env"
            ]
        );
    }

    #[test]
    fn shared_keys_are_refused() {
        let e = resolve_secrets(
            &config(),
            env(&[
                ("BAID_KEY_ACME", "same-key-0123456789"),
                ("BAID_KEY_GLOBEX", "same-key-0123456789"),
                ("NEO4J_PASSWORD", "x"),
            ]),
        )
        .unwrap_err();
        assert!(e.to_string().contains("same API key"), "{e}");
    }
}
