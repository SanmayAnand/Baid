//! Repository checks that cargo and clippy cannot express.
//!
//! ```text
//! cargo xtask layers   # crate dependency rules (Code architecture section 1)
//! cargo xtask spawns   # every spawned task observes a CancellationToken
//! cargo xtask all      # both
//! ```

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use serde::Deserialize;

// ---------------------------------------------------------------------------------------
// Layer rules
// ---------------------------------------------------------------------------------------

/// Layer of each workspace crate. A crate may depend only on crates in lower layers, plus
/// the same-layer edges in `SAME_LAYER_OK`.
const LAYERS: &[(&str, u8)] = &[
    ("baid-core", 0),
    ("cache", 1),
    ("session", 1),
    ("predict", 1),
    ("pack", 1),
    ("invalidate", 1),
    ("adapters", 1),
    ("telemetry", 1),
    ("exec", 2),
    ("mcp", 2),
    ("server", 3),
    ("memharness", 3),
    ("xtask", 3),
];

/// Same-layer edges that are allowed: the invalidator bumps versions owned by the cache.
const SAME_LAYER_OK: &[(&str, &str)] = &[("invalidate", "cache")];

/// Crates that perform network or disk I/O.
const IO_CRATES: &[&str] = &[
    "tokio",
    "neo4rs",
    "object_store",
    "reqwest",
    "hyper",
    "axum",
    "tower-http",
    "rustls",
    "foyer",
];

/// Extra bans per crate, beyond the layer rule. `"@io"` means every crate in `IO_CRATES`.
const BANS: &[(&str, &[&str])] = &[
    ("baid-core", &["@io"]),
    ("predict", &["@io"]),
    ("pack", &["@io"]),
    ("session", &["@io", "cache"]),
    ("cache", &["adapters", "exec"]),
    ("invalidate", &["adapters"]),
    ("adapters", &["cache", "predict"]),
    (
        "telemetry",
        &[
            "cache",
            "session",
            "predict",
            "pack",
            "invalidate",
            "adapters",
        ],
    ),
    ("mcp", &["cache", "exec"]),
    // memharness talks to BAiD over HTTP only; core types are fine.
    (
        "memharness",
        &[
            "cache",
            "session",
            "predict",
            "pack",
            "invalidate",
            "exec",
            "mcp",
            "server",
        ],
    ),
];

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    workspace_members: Vec<String>,
}

#[derive(Deserialize)]
struct Package {
    id: String,
    name: String,
    dependencies: Vec<Dependency>,
}

#[derive(Deserialize)]
struct Dependency {
    name: String,
    /// `None` for normal, `Some("dev")`, `Some("build")`.
    kind: Option<String>,
}

fn layer_of(name: &str) -> Option<u8> {
    LAYERS.iter().find(|(n, _)| *n == name).map(|(_, l)| *l)
}

/// Returns one message per violated rule.
fn check_layers(meta: &Metadata) -> Vec<String> {
    let mut errors = Vec::new();
    for pkg in meta
        .packages
        .iter()
        .filter(|p| meta.workspace_members.contains(&p.id))
    {
        let Some(own) = layer_of(&pkg.name) else {
            errors.push(format!(
                "{}: not in the layer table; add it to LAYERS in xtask/src/main.rs",
                pkg.name
            ));
            continue;
        };
        let bans: Vec<&str> = BANS
            .iter()
            .filter(|(n, _)| *n == pkg.name)
            .flat_map(|(_, b)| b.iter().copied())
            .flat_map(|b| {
                if b == "@io" {
                    IO_CRATES.to_vec()
                } else {
                    vec![b]
                }
            })
            .collect();

        for dep in &pkg.dependencies {
            let is_dev = dep.kind.as_deref() == Some("dev");
            if let Some(dep_layer) = layer_of(&dep.name) {
                let same_ok = SAME_LAYER_OK.contains(&(pkg.name.as_str(), dep.name.as_str()));
                if dep_layer > own || (dep_layer == own && !same_ok) {
                    errors.push(format!(
                        "{} (layer {own}) depends on {} (layer {dep_layer}); depend only on lower layers",
                        pkg.name, dep.name
                    ));
                }
            }
            // Tests may use an async runtime even where the library may not.
            if !is_dev && bans.contains(&dep.name.as_str()) {
                errors.push(format!("{} must not depend on {}", pkg.name, dep.name));
            }
        }
    }
    errors
}

fn cargo_metadata() -> Result<Metadata, String> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let out = Command::new(cargo)
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()
        .map_err(|e| format!("running cargo metadata: {e}"))?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).into_owned());
    }
    serde_json::from_slice(&out.stdout).map_err(|e| format!("parsing cargo metadata: {e}"))
}

// ---------------------------------------------------------------------------------------
// Spawn rule
// ---------------------------------------------------------------------------------------

/// Calls that start a detached task.
const SPAWN_PATTERNS: &[&str] = &[
    "tokio::spawn(",
    "task::spawn(",
    "spawn_local(",
    "spawn_blocking(",
    "join_set.spawn(",
    "set.spawn(",
];

/// How many lines after the spawn may hold the cancellation check.
const SPAWN_WINDOW: usize = 40;

/// Opt-out marker, on the spawn line or the line above, with a reason.
const SPAWN_OK: &str = "spawn-ok:";

/// Finds spawn sites in `src` with no `.cancelled()` in the following lines.
/// Returns `(line number, line)` pairs, 1-based.
fn unguarded_spawns(src: &str) -> Vec<(usize, String)> {
    let lines: Vec<&str> = src.lines().collect();
    let mut found = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let code = line.split("//").next().unwrap_or("");
        if !SPAWN_PATTERNS.iter().any(|p| code.contains(p)) {
            continue;
        }
        let opted_out = line.contains(SPAWN_OK) || (i > 0 && lines[i - 1].contains(SPAWN_OK));
        let end = (i + SPAWN_WINDOW).min(lines.len());
        let guarded = lines[i..end].iter().any(|l| l.contains(".cancelled()"));
        if !opted_out && !guarded {
            found.push((i + 1, line.trim().to_owned()));
        }
    }
    found
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            if p.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

fn check_spawns(root: &Path) -> Vec<String> {
    let mut files = Vec::new();
    rust_files(&root.join("crates"), &mut files);
    files.sort();
    let mut errors = Vec::new();
    for f in files {
        let Ok(src) = std::fs::read_to_string(&f) else {
            errors.push(format!("{}: unreadable", f.display()));
            continue;
        };
        for (line, text) in unguarded_spawns(&src) {
            errors.push(format!(
                "{}:{line}: spawned task never checks a CancellationToken (`{text}`); \
                 select on `token.cancelled()` or add `// {SPAWN_OK} <reason>`",
                f.display()
            ));
        }
    }
    errors
}

// ---------------------------------------------------------------------------------------

fn workspace_root() -> PathBuf {
    // xtask lives one level below the workspace root.
    let here = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    here.parent().map(Path::to_path_buf).unwrap_or(here)
}

fn report(name: &str, errors: &[String]) -> bool {
    if errors.is_empty() {
        println!("{name}: ok");
        true
    } else {
        eprintln!("{name}: {} problem(s)", errors.len());
        for e in errors {
            eprintln!("  {e}");
        }
        false
    }
}

fn main() -> ExitCode {
    let cmd = std::env::args().nth(1).unwrap_or_else(|| "all".into());
    let run_layers = || match cargo_metadata() {
        Ok(m) => report("layers", &check_layers(&m)),
        Err(e) => report("layers", &[e]),
    };
    let run_spawns = || report("spawns", &check_spawns(&workspace_root()));
    let ok = match cmd.as_str() {
        "layers" => run_layers(),
        "spawns" => run_spawns(),
        "all" => {
            let a = run_layers();
            let b = run_spawns();
            a && b
        }
        other => {
            eprintln!("unknown command {other:?}; use layers, spawns or all");
            false
        }
    };
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Deps<'a> = &'a [(&'a str, Option<&'a str>)];

    fn meta(pkgs: &[(&str, Deps<'_>)]) -> Metadata {
        Metadata {
            workspace_members: pkgs.iter().map(|(n, _)| format!("id-{n}")).collect(),
            packages: pkgs
                .iter()
                .map(|(n, deps)| Package {
                    id: format!("id-{n}"),
                    name: n.to_string(),
                    dependencies: deps
                        .iter()
                        .map(|(d, k)| Dependency {
                            name: d.to_string(),
                            kind: k.map(String::from),
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    #[test]
    fn clean_graph_passes() {
        let m = meta(&[
            ("baid-core", &[("serde", None), ("tokio", Some("dev"))]),
            ("cache", &[("baid-core", None), ("dashmap", None)]),
            ("invalidate", &[("baid-core", None), ("cache", None)]),
            ("exec", &[("cache", None), ("tokio", None)]),
            ("server", &[("exec", None), ("axum", None)]),
        ]);
        assert_eq!(check_layers(&m), Vec::<String>::new());
    }

    #[test]
    fn upward_and_same_layer_edges_fail() {
        let m = meta(&[("cache", &[("exec", None)]), ("pack", &[("predict", None)])]);
        let e = check_layers(&m);
        assert!(
            e.iter()
                .any(|x| x.starts_with("cache (layer 1) depends on exec")),
            "{e:?}"
        );
        assert!(
            e.iter()
                .any(|x| x.starts_with("pack (layer 1) depends on predict")),
            "{e:?}"
        );
    }

    #[test]
    fn io_bans_apply_to_normal_deps_only() {
        let m = meta(&[
            ("baid-core", &[("tokio", None)]),
            ("predict", &[("reqwest", None)]),
            ("pack", &[("tokio", Some("dev"))]),
        ]);
        let e = check_layers(&m);
        assert!(e.contains(&"baid-core must not depend on tokio".to_string()));
        assert!(e.contains(&"predict must not depend on reqwest".to_string()));
        assert_eq!(e.len(), 2);
    }

    #[test]
    fn unknown_crate_is_reported() {
        let e = check_layers(&meta(&[("newcrate", &[])]));
        assert!(e[0].contains("not in the layer table"));
    }

    #[test]
    fn spawn_detection() {
        let guarded = "tokio::spawn(async move {\n  tokio::select! {\n    _ = token.cancelled() => {}\n  }\n});";
        assert!(unguarded_spawns(guarded).is_empty());

        let bare = "fn f() {\n    tokio::spawn(async { work().await });\n}";
        assert_eq!(unguarded_spawns(bare).len(), 1);
        assert_eq!(unguarded_spawns(bare)[0].0, 2);

        let opted = "// spawn-ok: finite, awaited by caller\ntokio::spawn(x);";
        assert!(unguarded_spawns(opted).is_empty());

        let commented = "// tokio::spawn(x) is avoided here";
        assert!(unguarded_spawns(commented).is_empty());
    }

    #[test]
    fn real_workspace_passes() {
        assert_eq!(check_spawns(&workspace_root()), Vec::<String>::new());
    }
}
