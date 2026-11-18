//! Concurrent misses share one backend call, and sharing never weakens the guarantees: a
//! follower that joined a load started before a write still sees that write.

#![allow(clippy::unwrap_used)] // test file

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use adapters::Registry;
use async_trait::async_trait;
use baid_core::{
    AdapterId, Backend, BackendError, BackendQuery, BackendResult, Capabilities, Config, EntityId,
    Fact, Filters, Health, MemoryBackend, RecallRequest, SessionId, TenantId, TokenBudget,
    WriteAck,
};
use cache::{RamStore, SystemClock, TierConfig};
use pack::TruncatingPacker;
use server::auth::Tenant;
use server::engine::Engine;
use session::Sessions;
use tokio::sync::Notify;

/// A memory backend whose queries read their data at once, then wait at a gate (while it is
/// closed) before replying. Writes are not gated.
struct Gated {
    inner: MemoryBackend,
    closed: std::sync::atomic::AtomicBool,
    gate: Notify,
    waiting: AtomicUsize,
    query_calls: AtomicUsize,
}

impl Gated {
    fn new() -> Arc<Self> {
        Arc::new(Gated {
            inner: MemoryBackend::new(),
            closed: std::sync::atomic::AtomicBool::new(true),
            gate: Notify::new(),
            waiting: AtomicUsize::new(0),
            query_calls: AtomicUsize::new(0),
        })
    }
    fn open(&self) {
        self.closed.store(false, Ordering::SeqCst);
        self.gate.notify_waiters();
    }
    async fn wait_for_waiters(&self, n: usize) {
        for _ in 0..1_000 {
            if self.waiting.load(Ordering::SeqCst) >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("expected {n} queries waiting at the gate");
    }
}

#[async_trait]
impl Backend for Gated {
    fn id(&self) -> AdapterId {
        self.inner.id()
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn query_batch(
        &self,
        t: &TenantId,
        qs: &[BackendQuery],
    ) -> Result<Vec<BackendResult>, BackendError> {
        self.query_calls.fetch_add(1, Ordering::SeqCst);
        // Read now, answer later: like a real slow backend, the data is as of the moment
        // the query ran, not the moment the reply arrives.
        let answer = self.inner.query_batch(t, qs).await;
        if self.closed.load(Ordering::SeqCst) {
            self.waiting.fetch_add(1, Ordering::SeqCst);
            let opened = self.gate.notified();
            if self.closed.load(Ordering::SeqCst) {
                opened.await;
            }
            self.waiting.fetch_sub(1, Ordering::SeqCst);
        }
        answer
    }
    async fn write(&self, t: &TenantId, f: &Fact) -> Result<WriteAck, BackendError> {
        self.inner.write(t, f).await
    }
    async fn health(&self) -> Health {
        Health::Up
    }
}

fn tenant() -> Tenant {
    let config: Config = toml::from_str(
        "[[tenants]]\nid = \"acme\"\napi_key_env = \"K\"\nadapter = \"memory\"\nadapter_url = \"memory://\"\nbackend_timeout = \"10s\"\n",
    )
    .unwrap();
    Tenant {
        id: config.tenants[0].id.clone(),
        config: Arc::new(config.tenants[0].clone()),
    }
}

fn engine(backend: Arc<Gated>) -> Arc<Engine> {
    let t = TenantId::new("acme").unwrap();
    let store = RamStore::new(Arc::new(SystemClock));
    store.add_tenant(t.clone(), TierConfig::with_capacity(16 << 20));
    let mut reg = Registry::new();
    reg.insert(t, backend);
    Arc::new(Engine::new(
        Arc::new(store),
        reg,
        Arc::new(TruncatingPacker),
        Arc::new(Sessions::new()),
    ))
}

fn req(session: &str, query: &str) -> RecallRequest {
    RecallRequest {
        session: SessionId::new(session).unwrap(),
        query: query.into(),
        budget: TokenBudget::default(),
        filters: Filters::default(),
        max_staleness: None,
    }
}

fn fact(text: &str, entity: &str) -> Fact {
    Fact {
        text: text.into(),
        entities: vec![EntityId::new(entity).unwrap()],
        labels: vec![],
        as_of: None,
        idempotency_key: None,
    }
}

async fn texts(e: &Engine, session: &str, query: &str) -> Vec<String> {
    let r = e.recall(&tenant(), req(session, query)).await.unwrap();
    r.packed.items.iter().map(|i| i.text.to_string()).collect()
}

#[tokio::test]
async fn concurrent_misses_share_one_backend_call() {
    let b = Gated::new();
    let acme = TenantId::new("acme").unwrap();
    b.inner.seed(&acme, "acme renews in March", &["acme"]);
    let e = engine(b.clone());

    let recalls: Vec<_> = (0..20)
        .map(|i| {
            let e = e.clone();
            async move { texts(&e, &format!("s{i}"), "acme renewal").await }
        })
        .collect();
    let release = async {
        b.wait_for_waiters(1).await;
        // Let every other recall reach the flight before opening.
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert_eq!(e.loads_in_flight(), 1);
        b.open();
    };
    let (results, ()) = tokio::join!(futures::future::join_all(recalls), release);
    for r in results {
        assert_eq!(r, ["acme renews in March"]);
    }
    assert_eq!(
        b.query_calls.load(Ordering::SeqCst),
        1,
        "one backend call for 20 misses"
    );
    assert_eq!(e.loads_in_flight(), 0);
}

/// Session W writes while a load it then joins is in flight. Read-your-writes must hold.
#[tokio::test]
async fn a_follower_still_sees_its_own_write() {
    let b = Gated::new();
    let e = engine(b.clone());
    let t = tenant();
    let w = SessionId::new("writer").unwrap();

    let leader = {
        let e = e.clone();
        async move { texts(&e, "leader", "renewal dates").await }
    };
    let writer = async {
        b.wait_for_waiters(1).await; // the leader's load is in flight, pre-write
        e.remember(&t, &w, fact("renewal dates: acme in May", "acme"))
            .await
            .unwrap();
        let joined = {
            let e = e.clone();
            async move { texts(&e, "writer", "renewal dates").await }
        };
        let open_later = async {
            for _ in 0..50 {
                tokio::task::yield_now().await;
            }
            b.open();
        };
        let (got, ()) = tokio::join!(joined, open_later);
        got
    };
    let (_leader_saw, writer_saw) = tokio::join!(leader, writer);
    assert_eq!(writer_saw, ["renewal dates: acme in May"]);
    assert_eq!(
        b.query_calls.load(Ordering::SeqCst),
        2,
        "the follower had to load again"
    );
}

/// A third session joins a load that started before another session's write to an entity
/// the results mention. It must not be served the changed data.
#[tokio::test]
async fn a_follower_never_gets_data_a_write_changed() {
    let b = Gated::new();
    let acme = TenantId::new("acme").unwrap();
    b.inner.seed(&acme, "acme plan is gold", &["acme"]);
    let e = engine(b.clone());
    let t = tenant();

    let leader = {
        let e = e.clone();
        async move { texts(&e, "leader", "acme plan").await }
    };
    let rest = async {
        b.wait_for_waiters(1).await;
        e.remember(
            &t,
            &SessionId::new("writer").unwrap(),
            fact("acme plan is platinum", "acme"),
        )
        .await
        .unwrap();
        let reader = {
            let e = e.clone();
            async move { texts(&e, "reader", "acme plan").await }
        };
        let open_later = async {
            for _ in 0..50 {
                tokio::task::yield_now().await;
            }
            b.open();
        };
        let (got, ()) = tokio::join!(reader, open_later);
        got
    };
    let (_leader_saw, reader_saw) = tokio::join!(leader, rest);
    assert!(
        reader_saw.contains(&"acme plan is platinum".to_string()),
        "reader joined after the write was acknowledged: {reader_saw:?}"
    );
}
