//! State shared by every request handler, and how it is built from config.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use adapters::{BuildError, Credentials, Registry};
use baid_core::{Config, TenantConfig, TenantId};
use cache::RamStore;
use pack::TruncatingPacker;
use session::Sessions;

use crate::config::Secrets;
use crate::engine::Engine;

/// The backend credentials resolved for `tenant` (empty if it has none).
pub fn credentials(secrets: &Secrets, tenant: &TenantId) -> Credentials {
    secrets
        .adapters
        .get(tenant)
        .map(|c| Credentials {
            user: c.user.as_ref().map(|s| s.expose().to_owned()),
            password: c.password.as_ref().map(|s| s.expose().to_owned()),
        })
        .unwrap_or_default()
}

/// Handlers receive this; cloning is a reference-count bump.
pub type AppState = Arc<State>;

/// Configuration, secrets, the engine and readiness.
#[derive(Debug)]
pub struct State {
    /// The validated configuration.
    pub config: Config,
    /// Resolved secrets (API keys, backend credentials).
    pub secrets: Secrets,
    /// The recall engine.
    pub engine: Engine,
    tenants: HashMap<TenantId, Arc<TenantConfig>>,
    ready: AtomicBool,
}

impl State {
    /// Assembles the state from an already-built engine. It starts ready.
    pub fn new(config: Config, secrets: Secrets, engine: Engine) -> AppState {
        let tenants = config
            .tenants
            .iter()
            .map(|t| (t.id.clone(), Arc::new(t.clone())))
            .collect();
        Arc::new(State {
            config,
            secrets,
            engine,
            tenants,
            ready: AtomicBool::new(true),
        })
    }

    /// Builds the production engine from config: a RAM tier per tenant, one backend per
    /// tenant, the truncating packer and a session tracker.
    pub async fn build(config: Config, secrets: Secrets) -> Result<AppState, BuildError> {
        let backends = Registry::from_config(&config, |t| credentials(&secrets, t)).await?;
        let engine = Engine::new(
            Arc::new(RamStore::from_config(&config)),
            backends,
            Arc::new(TruncatingPacker),
            Arc::new(Sessions::new()),
        );
        Ok(Self::new(config, secrets, engine))
    }

    /// A tenant's configuration.
    pub fn tenant_config(&self, id: &TenantId) -> Option<Arc<TenantConfig>> {
        self.tenants.get(id).cloned()
    }

    /// False once shutdown has begun, so load balancers stop sending traffic.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    /// Sets readiness.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
    }
}
