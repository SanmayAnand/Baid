//! An in-memory [`Backend`] for tests in every crate. Enabled by the `test-util` feature.
//!
//! Search is naive word overlap; it exists to make engine behaviour observable (call counts,
//! injected failures, tenant isolation), not to rank well.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;

use crate::backend::{BackendResult, Capabilities, Fact, Health, WriteAck};
use crate::error::BackendError;
use crate::item::MemoryItem;
use crate::key::normalise;
use crate::traits::Backend;
use crate::types::{AdapterId, BackendQuery, EntityId, NodeId, TenantId};

#[derive(Default)]
struct State {
    items: HashMap<TenantId, Vec<MemoryItem>>,
    failures: VecDeque<BackendError>,
    health: Option<Health>,
}

/// An in-memory backend with call counters and failure injection.
pub struct MockBackend {
    id: AdapterId,
    caps: Capabilities,
    state: Mutex<State>,
    next_node: AtomicU64,
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
    /// A mock with id `mock`, native batching and neighbourhood support.
    pub fn new() -> Self {
        MockBackend {
            id: AdapterId::new("mock").unwrap_or_else(|_| unreachable!("valid id")),
            caps: Capabilities {
                native_batch: true,
                max_batch: 64,
                neighbourhood: true,
                change_feed: false,
                versions: true,
            },
            state: Mutex::new(State::default()),
            next_node: AtomicU64::new(1),
            query_calls: AtomicUsize::new(0),
            queries: AtomicUsize::new(0),
            write_calls: AtomicUsize::new(0),
        }
    }

    /// Wraps the mock in an `Arc`, the form the engine takes.
    pub fn shared(self) -> Arc<Self> {
        Arc::new(self)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stores an item directly, bypassing `write` and its counters. Returns its node id.
    pub fn seed(&self, tenant: &TenantId, text: &str, entities: &[&str]) -> NodeId {
        let id = self.new_node_id();
        let item = MemoryItem {
            id: id.clone(),
            text: Arc::from(text),
            score: 0.0,
            tokens: 0,
            entities: entities
                .iter()
                .filter_map(|e| EntityId::new(e).ok())
                .collect(),
            as_of: None,
            source: self.id.clone(),
        };
        self.lock()
            .items
            .entry(tenant.clone())
            .or_default()
            .push(item);
        id
    }

    /// The next `query_batch` or `write` call fails with `err`. Queue several to fail
    /// several calls.
    pub fn fail_next(&self, err: BackendError) {
        self.lock().failures.push_back(err);
    }

    /// Overrides what `health` reports.
    pub fn set_health(&self, h: Health) {
        self.lock().health = Some(h);
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

    fn new_node_id(&self) -> NodeId {
        let n = self.next_node.fetch_add(1, Ordering::SeqCst);
        NodeId::new(format!("m{n}")).unwrap_or_else(|_| unreachable!("valid id"))
    }

    fn take_failure(&self) -> Option<BackendError> {
        self.lock().failures.pop_front()
    }

    fn run(&self, items: &[MemoryItem], q: &BackendQuery) -> BackendResult {
        let mentions = |i: &MemoryItem, e: &EntityId| i.entities.iter().any(|x| x == e);
        let mut hits: Vec<MemoryItem> = match q {
            BackendQuery::Search {
                text,
                filters,
                limit,
            } => {
                let norm = normalise(text);
                let words: Vec<&str> = norm.split(' ').filter(|w| !w.is_empty()).collect();
                let mut scored: Vec<MemoryItem> = items
                    .iter()
                    .filter(|i| {
                        filters.entity_ids.is_empty()
                            || filters.entity_ids.iter().any(|e| mentions(i, e))
                    })
                    .filter(|i| match (filters.since, i.as_of) {
                        (Some(since), Some(at)) => at >= since,
                        (Some(_), None) => false,
                        (None, _) => true,
                    })
                    .filter_map(|i| {
                        let text = normalise(&i.text);
                        let n = words.iter().filter(|w| text.contains(**w)).count();
                        (n > 0).then(|| {
                            let mut i = i.clone();
                            i.score = n as f32 / words.len().max(1) as f32;
                            i
                        })
                    })
                    .collect();
                scored.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
                scored.truncate(usize::from(*limit));
                scored
            }
            BackendQuery::Neighbourhood { entity, limit, .. } => items
                .iter()
                .filter(|i| mentions(i, entity))
                .take(usize::from(*limit))
                .cloned()
                .collect(),
            BackendQuery::Entity { id } => items
                .iter()
                .filter(|i| mentions(i, id))
                .take(1)
                .cloned()
                .collect(),
        };
        for i in &mut hits {
            if i.score == 0.0 {
                i.score = 1.0;
            }
        }
        BackendResult { items: hits }
    }
}

#[async_trait]
impl Backend for MockBackend {
    fn id(&self) -> AdapterId {
        self.id.clone()
    }

    fn capabilities(&self) -> Capabilities {
        self.caps
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
        let state = self.lock();
        let items = state.items.get(tenant).map_or(&[][..], Vec::as_slice);
        Ok(queries.iter().map(|q| self.run(items, q)).collect())
    }

    async fn write(&self, tenant: &TenantId, fact: &Fact) -> Result<WriteAck, BackendError> {
        self.write_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(e) = self.take_failure() {
            return Err(e);
        }
        let node = self.new_node_id();
        let item = MemoryItem {
            id: node.clone(),
            text: Arc::from(fact.text.as_str()),
            score: 0.0,
            tokens: 0,
            entities: fact.entities.iter().cloned().collect(),
            as_of: fact.as_of,
            source: self.id.clone(),
        };
        self.lock()
            .items
            .entry(tenant.clone())
            .or_default()
            .push(item);
        Ok(WriteAck {
            node,
            entities: fact.entities.clone(),
            version: Some(1),
        })
    }

    async fn health(&self) -> Health {
        self.lock().health.clone().unwrap_or(Health::Up)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Filters;

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
