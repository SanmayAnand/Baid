//! One backend per tenant (Architecture section 4: "Adapter registry").

use std::collections::HashMap;
use std::sync::Arc;

use baid_core::{AdapterKind, Backend, Config, MemoryBackend, TenantId};

/// A tenant's adapter could not be built.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// The adapter kind exists in config but is not implemented yet.
    #[error("tenant {tenant}: adapter {kind:?} is not available in this build yet")]
    Unsupported {
        /// The tenant.
        tenant: TenantId,
        /// The requested kind.
        kind: AdapterKind,
    },
}

/// Tenant → backend.
#[derive(Default)]
pub struct Registry {
    backends: HashMap<TenantId, Arc<dyn Backend>>,
}

impl Registry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds one backend per configured tenant.
    pub fn from_config(config: &Config) -> Result<Self, BuildError> {
        let mut r = Registry::new();
        for t in &config.tenants {
            let backend: Arc<dyn Backend> = match t.adapter {
                AdapterKind::Memory => Arc::new(MemoryBackend::new()),
                kind => {
                    return Err(BuildError::Unsupported {
                        tenant: t.id.clone(),
                        kind,
                    })
                }
            };
            r.insert(t.id.clone(), backend);
        }
        Ok(r)
    }

    /// Sets (or replaces) a tenant's backend.
    pub fn insert(&mut self, tenant: TenantId, backend: Arc<dyn Backend>) {
        self.backends.insert(tenant, backend);
    }

    /// The tenant's backend.
    pub fn get(&self, tenant: &TenantId) -> Option<Arc<dyn Backend>> {
        self.backends.get(tenant).cloned()
    }

    /// Number of tenants with a backend.
    pub fn len(&self) -> usize {
        self.backends.len()
    }

    /// True if no tenant has a backend.
    pub fn is_empty(&self) -> bool {
        self.backends.is_empty()
    }
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ids: Vec<String> = self
            .backends
            .iter()
            .map(|(t, b)| format!("{t}={}", b.id()))
            .collect();
        ids.sort();
        write!(f, "Registry({})", ids.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(adapter: &str) -> Config {
        toml::from_str(&format!(
            "[[tenants]]\nid = \"acme\"\napi_key_env = \"K\"\nadapter = \"{adapter}\"\nadapter_url = \"x://\"\n"
        ))
        .unwrap()
    }

    #[test]
    fn memory_tenants_get_a_backend() {
        let r = Registry::from_config(&config("memory")).unwrap();
        let acme = TenantId::new("acme").unwrap();
        assert_eq!(r.get(&acme).unwrap().id().as_str(), "memory");
        assert!(r.get(&TenantId::new("other").unwrap()).is_none());
        assert_eq!(format!("{r:?}"), "Registry(acme=memory)");
    }

    #[test]
    fn unbuilt_adapters_fail_loudly() {
        let e = Registry::from_config(&config("bolt")).unwrap_err();
        assert!(e.to_string().contains("Bolt"), "{e}");
    }
}
