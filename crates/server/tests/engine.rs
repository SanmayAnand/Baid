//! Engine behaviour that needs a controllable backend: failures, timeouts, call counts.

#![allow(clippy::unwrap_used)] // test file

use std::sync::Arc;
use std::time::Duration;

use adapters::Registry;
use async_trait::async_trait;
use baid_core::mock::MockBackend;
use baid_core::{
    AdapterId, Backend, BackendError, BackendQuery, BackendResult, Capabilities, Config, Fact,
    Filters, Health, RecallRequest, SessionId, TenantId, TokenBudget, WriteAck,
};
use cache::{RamStore, SystemClock, TierConfig};
use pack::TruncatingPacker;
use server::auth::Tenant;
use server::engine::Engine;
use session::Sessions;

fn tenant(timeout: &str) -> Tenant {
    let config: Config = toml::from_str(&format!(
        "[[tenants]]\nid = \"acme\"\napi_key_env = \"K\"\nadapter = \"memory\"\nadapter_url = \"memory://\"\nbackend_timeout = \"{timeout}\"\n"
    ))
    .unwrap();
    Tenant {
        id: config.tenants[0].id.clone(),
        config: Arc::new(config.tenants[0].clone()),
    }
}

fn engine(backend: Arc<dyn Backend>) -> Engine {
    let t = TenantId::new("acme").unwrap();
    let store = RamStore::new(Arc::new(SystemClock));
    store.add_tenant(t.clone(), TierConfig::with_capacity(16 << 20));
    let mut reg = Registry::new();
    reg.insert(t, backend);
    Engine::new(
        Arc::new(store),
        reg,
        Arc::new(TruncatingPacker),
        Arc::new(Sessions::new()),
    )
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

fn fact(text: &str) -> Fact {
    Fact {
        text: text.into(),
        entities: vec![baid_core::EntityId::new("acme").unwrap()],
        labels: vec![],
        as_of: None,
        idempotency_key: None,
    }
}

fn sid(s: &str) -> SessionId {
    SessionId::new(s).unwrap()
}

#[tokio::test]
async fn repeated_recalls_call_the_backend_once() {
    let mock = MockBackend::new().shared();
    mock.seed(&TenantId::new("acme").unwrap(), "acme plan", &["acme"]);
    let e = engine(mock.clone());
    let t = tenant("2s");
    for _ in 0..5 {
        e.recall(&t, req("s", "acme plan")).await.unwrap();
    }
    assert_eq!(mock.query_calls(), 1);
}

#[tokio::test]
async fn backend_failures_map_to_api_errors() {
    let mock = MockBackend::new().shared();
    let e = engine(mock.clone());
    let t = tenant("2s");
    mock.fail_next(BackendError::Unavailable("down".into()));
    let err = e.recall(&t, req("s", "q")).await.unwrap_err();
    assert_eq!((err.code(), err.retryable()), ("backend_unavailable", true));
    mock.fail_next(BackendError::Rejected("bad cypher".into()));
    let err = e.recall(&t, req("s", "q")).await.unwrap_err();
    assert_eq!((err.code(), err.retryable()), ("backend_error", false));
}

/// A backend that never answers in time.
struct Slow;

#[async_trait]
impl Backend for Slow {
    fn id(&self) -> AdapterId {
        AdapterId::new("slow").unwrap()
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    async fn query_batch(
        &self,
        _: &TenantId,
        _: &[BackendQuery],
    ) -> Result<Vec<BackendResult>, BackendError> {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(vec![])
    }
    async fn write(&self, _: &TenantId, _: &Fact) -> Result<WriteAck, BackendError> {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Err(BackendError::Unavailable("never".into()))
    }
    async fn health(&self) -> Health {
        Health::Up
    }
}

#[tokio::test(start_paused = true)]
async fn slow_backends_time_out() {
    let e = engine(Arc::new(Slow));
    let t = tenant("50ms");
    let err = e.recall(&t, req("s", "q")).await.unwrap_err();
    assert_eq!(err.code(), "backend_unavailable");
    let err = e.remember(&t, &sid("s"), fact("x")).await.unwrap_err();
    assert_eq!(err.code(), "backend_unavailable");
}

/// A write that fails ambiguously (it may have been applied) must still invalidate.
#[tokio::test]
async fn ambiguous_write_failure_still_invalidates() {
    let mock = MockBackend::new().shared();
    mock.seed(
        &TenantId::new("acme").unwrap(),
        "acme plan is gold",
        &["acme"],
    );
    let e = engine(mock.clone());
    let t = tenant("2s");
    e.recall(&t, req("reader", "acme plan")).await.unwrap(); // cached
    mock.fail_next(BackendError::Unavailable("connection reset".into()));
    assert!(e
        .remember(&t, &sid("w"), fact("acme plan is platinum"))
        .await
        .is_err());
    e.recall(&t, req("reader", "acme plan")).await.unwrap();
    assert_eq!(
        mock.query_calls(),
        2,
        "entry was refused after the failed write"
    );
}

/// A rejected write was certainly not applied, so the cache keeps serving.
#[tokio::test]
async fn rejected_write_changes_nothing() {
    let mock = MockBackend::new().shared();
    mock.seed(
        &TenantId::new("acme").unwrap(),
        "acme plan is gold",
        &["acme"],
    );
    let e = engine(mock.clone());
    let t = tenant("2s");
    e.recall(&t, req("reader", "acme plan")).await.unwrap();
    mock.fail_next(BackendError::Rejected("schema".into()));
    let err = e.remember(&t, &sid("w"), fact("bad")).await.unwrap_err();
    assert_eq!(err.code(), "backend_error");
    e.recall(&t, req("reader", "acme plan")).await.unwrap();
    assert_eq!(mock.query_calls(), 1, "still a hit");
}
