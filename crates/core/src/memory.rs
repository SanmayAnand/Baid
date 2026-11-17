//! [`MemoryBackend`]: a non-durable, in-process memory backend.
//!
//! It backs the `memory` adapter kind (development, demos, a sidecar smoke test) and is the
//! reference backend for property tests. Data lives only as long as the process.
//!
//! Search is naive word overlap: an item matches if its normalised text contains any word
//! of the normalised query, scored by the fraction of query words it contains.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;

use crate::backend::{BackendResult, Capabilities, Fact, Health, WriteAck};
use crate::error::BackendError;
use crate::item::MemoryItem;
use crate::key::normalise;
use crate::traits::Backend;
use crate::types::{AdapterId, BackendQuery, EntityId, NodeId, TenantId};

/// An in-process backend. Every tenant's items are kept apart.
pub struct MemoryBackend {
    id: AdapterId,
    items: Mutex<HashMap<TenantId, Vec<MemoryItem>>>,
    next_node: AtomicU64,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryBackend {
    /// An empty backend with adapter id `memory`.
    pub fn new() -> Self {
        Self::with_id("memory")
    }

    /// An empty backend with the given adapter id (must be a valid [`AdapterId`]).
    pub fn with_id(id: &str) -> Self {
        MemoryBackend {
            id: AdapterId::new(id).unwrap_or_else(|_| unreachable!("caller passes a valid id")),
            items: Mutex::new(HashMap::new()),
            next_node: AtomicU64::new(1),
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<TenantId, Vec<MemoryItem>>> {
        self.items.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn new_node_id(&self) -> NodeId {
        let n = self.next_node.fetch_add(1, Ordering::SeqCst);
        NodeId::new(format!("m{n}")).unwrap_or_else(|_| unreachable!("valid id"))
    }

    /// Stores an item directly. Returns its node id.
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
        self.lock().entry(tenant.clone()).or_default().push(item);
        id
    }

    /// Runs queries synchronously.
    pub fn query_now(&self, tenant: &TenantId, queries: &[BackendQuery]) -> Vec<BackendResult> {
        let all = self.lock();
        let items = all.get(tenant).map_or(&[][..], Vec::as_slice);
        queries.iter().map(|q| run(items, q)).collect()
    }

    /// Stores a fact synchronously.
    pub fn write_now(&self, tenant: &TenantId, fact: &Fact) -> WriteAck {
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
        self.lock().entry(tenant.clone()).or_default().push(item);
        WriteAck {
            node,
            entities: fact.entities.clone(),
            version: Some(1),
        }
    }
}

fn mentions(i: &MemoryItem, e: &EntityId) -> bool {
    i.entities.iter().any(|x| x == e)
}

fn run(items: &[MemoryItem], q: &BackendQuery) -> BackendResult {
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

#[async_trait]
impl Backend for MemoryBackend {
    fn id(&self) -> AdapterId {
        self.id.clone()
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            native_batch: true,
            max_batch: 64,
            neighbourhood: true,
            change_feed: false,
            versions: true,
        }
    }

    async fn query_batch(
        &self,
        tenant: &TenantId,
        queries: &[BackendQuery],
    ) -> Result<Vec<BackendResult>, BackendError> {
        Ok(self.query_now(tenant, queries))
    }

    async fn write(&self, tenant: &TenantId, fact: &Fact) -> Result<WriteAck, BackendError> {
        Ok(self.write_now(tenant, fact))
    }

    async fn health(&self) -> Health {
        Health::Up
    }
}
