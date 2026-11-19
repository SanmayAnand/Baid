//! Writes free the cache entries they made stale at write time, not on the next read.

#![allow(clippy::unwrap_used)] // test file

use std::sync::Arc;
use std::time::{Duration, Instant};

use adapters::Registry;
use baid_core::mock::MockBackend;
use baid_core::{
    BackendError, CacheStore, Config, EntityId, Fact, Filters, RecallRequest, SessionId, TenantId,
    TokenBudget,
};
use cache::{RamStore, SystemClock, TierConfig};
use pack::TruncatingPacker;
use server::auth::Tenant;
use server::engine::Engine;
use session::Sessions;

struct Rig {
    engine: Engine,
    store: Arc<RamStore>,
    mock: Arc<MockBackend>,
    tenant: Tenant,
}

fn rig() -> Rig {
    let config: Config = toml::from_str(
        "[[tenants]]\nid = \"acme\"\napi_key_env = \"K\"\nadapter = \"memory\"\nadapter_url = \"memory://\"\n",
    )
    .unwrap();
    let id = config.tenants[0].id.clone();
    let store = Arc::new(RamStore::new(Arc::new(SystemClock)));
    store.add_tenant(id.clone(), TierConfig::with_capacity(16 << 20));
    let mock = MockBackend::new().shared();
    let mut reg = Registry::new();
    reg.insert(id.clone(), mock.clone());
    Rig {
        engine: Engine::new(
            store.clone(),
            reg,
            Arc::new(TruncatingPacker),
            Arc::new(Sessions::new()),
        ),
        store,
        mock,
        tenant: Tenant {
            id,
            config: Arc::new(config.tenants[0].clone()),
        },
    }
}

impl Rig {
    fn acme(&self) -> TenantId {
        self.tenant.id.clone()
    }
    fn entries(&self) -> u64 {
        self.store.stats(&self.acme()).unwrap().entries
    }
    async fn recall(&self, q: &str) {
        let req = RecallRequest {
            session: SessionId::new("reader").unwrap(),
            query: q.into(),
            budget: TokenBudget::default(),
            filters: Filters::default(),
            max_staleness: None,
        };
        self.engine.recall(&self.tenant, req).await.unwrap();
    }
    async fn remember(&self, text: &str, entity: &str) -> Result<(), String> {
        let fact = Fact {
            text: text.into(),
            entities: vec![EntityId::new(entity).unwrap()],
            labels: vec![],
            as_of: None,
            idempotency_key: None,
        };
        self.engine
            .remember(&self.tenant, &SessionId::new("writer").unwrap(), fact)
            .await
            .map(|_| ())
            .map_err(|e| e.code().to_owned())
    }
}

#[tokio::test]
async fn a_write_frees_the_entries_it_made_stale() {
    let r = rig();
    // Queries share no words, so neither result depends on the other entity.
    r.mock.seed(&r.acme(), "acme renews in march", &["acme"]);
    r.mock.seed(&r.acme(), "globex pays invoices", &["globex"]);
    r.recall("acme renews").await;
    r.recall("globex invoices").await;
    assert_eq!(r.entries(), 2);
    assert_eq!(r.engine.index().entities(), 2);

    r.remember("acme renews in may", "acme").await.unwrap();
    assert_eq!(
        r.entries(),
        1,
        "the acme entry is gone before anyone reads it"
    );
    assert_eq!(r.engine.index().entities(), 1, "and so is its index entry");

    r.remember("an unrelated fact", "initech").await.unwrap();
    assert_eq!(r.entries(), 1, "the globex entry is untouched");
}

#[tokio::test]
async fn an_ambiguous_failed_write_also_evicts() {
    let r = rig();
    r.mock.seed(&r.acme(), "acme plan is gold", &["acme"]);
    r.recall("acme plan").await;
    assert_eq!(r.entries(), 1);
    r.mock
        .fail_next(BackendError::Unavailable("connection reset".into()));
    assert_eq!(
        r.remember("acme plan is platinum", "acme").await,
        Err("backend_unavailable".into())
    );
    assert_eq!(
        r.entries(),
        0,
        "it may have been applied, so the entry goes"
    );
}

#[tokio::test]
async fn a_rejected_write_evicts_nothing() {
    let r = rig();
    r.mock.seed(&r.acme(), "acme plan is gold", &["acme"]);
    r.recall("acme plan").await;
    r.mock.fail_next(BackendError::Rejected("schema".into()));
    assert!(r.remember("bad", "acme").await.is_err());
    assert_eq!(r.entries(), 1);
}

#[tokio::test]
async fn housekeeping_prunes_references_to_evicted_entries() {
    let r = rig();
    r.mock
        .seed(&r.acme(), "acme plan is gold", &["acme", "gold"]);
    r.recall("acme plan").await;
    assert_eq!(r.engine.index().references(), 2);
    // Simulate LRU eviction: the cache drops the entry behind the index's back.
    let key = {
        let q = RecallRequest {
            session: SessionId::new("x").unwrap(),
            query: "acme plan".into(),
            budget: TokenBudget::default(),
            filters: Filters::default(),
            max_staleness: None,
        }
        .backend_query();
        baid_core::Key::derive(&r.acme(), &baid_core::AdapterId::new("mock").unwrap(), &q)
    };
    assert!(r.store.remove(&key));
    let done = r
        .engine
        .housekeeping(Instant::now(), Duration::from_secs(3600));
    assert_eq!(done.index_refs_pruned, 2);
    assert_eq!(r.engine.index().references(), 0);
}
