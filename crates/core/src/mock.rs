//! [`MockBackend`]: a [`MemoryBackend`] with call counters and failure injection, for tests
//! in every crate. Enabled by the `test-util` feature.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;

use crate::backend::{BackendResult, Capabilities, Fact, Health, WriteAck};
use crate::error::BackendError;
use crate::memory::MemoryBackend;
use crate::traits::Backend;
use crate::types::{AdapterId, BackendQuery, NodeId, TenantId};

#[derive(Default)]
struct Faults {
    failures: VecDeque<BackendError>,
    health: Option<Health>,
}

/// An in-memory backend (adapter id `mock`) that counts calls and fails on demand.
pub struct MockBackend {
    inner: MemoryBackend,
    faults: Mutex<Faults>,
    query_calls: AtomicUsize,
    queries: AtomicUsize,
    write_calls: AtomicUsize,
}

impl Default for MockBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MockBackend {
    /// An empty mock.
    pub fn new() -> Self {
        MockBackend {
            inner: MemoryBackend::with_id("mock"),
            faults: Mutex::new(Faults::default()),
            query_calls: AtomicUsize::new(0),
            queries: AtomicUsize::new(0),
            write_calls: AtomicUsize::new(0),
        }
    }

    /// Wraps the mock in an `Arc`, the form the engine takes.
    pub fn shared(self) -> Arc<Self> {
        Arc::new(self)
    }

    fn faults(&self) -> MutexGuard<'_, Faults> {
        self.faults.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stores an item directly, bypassing `write` and its counters. Returns its node id.
    pub fn seed(&self, tenant: &TenantId, text: &str, entities: &[&str]) -> NodeId {
        self.inner.seed(tenant, text, entities)
    }

    /// The next `query_batch` or `write` call fails with `err`. Queue several to fail
    /// several calls.
    pub fn fail_next(&self, err: BackendError) {
        self.faults().failures.push_back(err);
    }

    /// Overrides what `health` reports.
    pub fn set_health(&self, h: Health) {
        self.faults().health = Some(h);
    }

    /// Number of `query_batch` calls (round trips).
    pub fn query_calls(&self) -> usize {
        self.query_calls.load(Ordering::SeqCst)
    }

    /// Number of individual queries across all batches.
    pub fn queries(&self) -> usize {
        self.queries.load(Ordering::SeqCst)
    }

    /// Number of `write` calls.
    pub fn write_calls(&self) -> usize {
        self.write_calls.load(Ordering::SeqCst)
    }

    fn take_failure(&self) -> Option<BackendError> {
        self.faults().failures.pop_front()
    }
}

#[async_trait]
impl Backend for MockBackend {
    fn id(&self) -> AdapterId {
        self.inner.id()
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn query_batch(
        &self,
        tenant: &TenantId,
        queries: &[BackendQuery],
    ) -> Result<Vec<BackendResult>, BackendError> {
        self.query_calls.fetch_add(1, Ordering::SeqCst);
        self.queries.fetch_add(queries.len(), Ordering::SeqCst);
        if let Some(e) = self.take_failure() {
            return Err(e);
        }
        Ok(self.inner.query_now(tenant, queries))
    }

    async fn write(&self, tenant: &TenantId, fact: &Fact) -> Result<WriteAck, BackendError> {
        self.write_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(e) = self.take_failure() {
            return Err(e);
        }
        Ok(self.inner.write_now(tenant, fact))
    }

    async fn health(&self) -> Health {
        self.faults().health.clone().unwrap_or(Health::Up)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EntityId, Filters};

    /// Minimal executor so this crate stays free of an async runtime.
    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        use std::task::{Context, Poll, Waker};
        let mut f = std::pin::pin!(f);
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
        }
    }

    fn t(s: &str) -> TenantId {
        TenantId::new(s).unwrap()
    }

    fn search(text: &str) -> BackendQuery {
        BackendQuery::Search {
            text: text.into(),
            filters: Filters::default(),
            limit: 10,
        }
    }

    fn fact(text: &str, entities: &[&str]) -> Fact {
        Fact {
            text: text.into(),
            entities: entities.iter().map(|e| EntityId::new(e).unwrap()).collect(),
            labels: vec![],
            as_of: None,
            idempotency_key: None,
        }
    }

    #[test]
    fn write_then_search() {
        let m = MockBackend::new();
        block_on(m.write(&t("acme"), &fact("Acme renews in March", &["acme"]))).unwrap();
        let r = block_on(m.query_batch(&t("acme"), &[search("acme renewal")])).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(&*r[0].items[0].text, "Acme renews in March");
        assert_eq!(m.query_calls(), 1);
        assert_eq!(m.write_calls(), 1);
    }

    #[test]
    fn tenants_are_isolated() {
        let m = MockBackend::new();
        m.seed(&t("a"), "secret plan", &[]);
        let r = block_on(m.query_batch(&t("b"), &[search("secret plan")])).unwrap();
        assert!(r[0].items.is_empty());
    }

    #[test]
    fn batch_returns_one_result_per_query_in_order() {
        let m = MockBackend::new();
        m.seed(&t("a"), "alpha", &["x"]);
        m.seed(&t("a"), "beta", &["y"]);
        let qs = [
            search("beta"),
            BackendQuery::Neighbourhood {
                entity: EntityId::new("x").unwrap(),
                hops: 1,
                limit: 5,
            },
            search("nothing"),
        ];
        let r = block_on(m.query_batch(&t("a"), &qs)).unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(&*r[0].items[0].text, "beta");
        assert_eq!(&*r[1].items[0].text, "alpha");
        assert!(r[2].items.is_empty());
        assert_eq!((m.query_calls(), m.queries()), (1, 3));
    }

    #[test]
    fn injected_failures_are_consumed_in_order() {
        let m = MockBackend::new();
        m.fail_next(BackendError::CircuitOpen);
        m.fail_next(BackendError::Unavailable("down".into()));
        assert_eq!(
            block_on(m.query_batch(&t("a"), &[search("q")])),
            Err(BackendError::CircuitOpen)
        );
        assert!(block_on(m.write(&t("a"), &fact("x", &[]))).is_err());
        assert!(block_on(m.query_batch(&t("a"), &[search("q")])).is_ok());
    }

    #[test]
    fn health_override() {
        let m = MockBackend::new();
        assert_eq!(block_on(m.health()), Health::Up);
        m.set_health(Health::Down { reason: "x".into() });
        assert!(!block_on(m.health()).is_serving());
    }
}
