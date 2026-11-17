//! The five extension traits. The engine holds each as an `Arc<dyn Trait>`, so any of them
//! can be swapped or mocked without touching the engine.
//!
//! `async_trait` is used because native async trait methods cannot yet be called through
//! `dyn`. Revisit when that lands on stable Rust.

use std::sync::Arc;

use async_trait::async_trait;

use crate::backend::{BackendResult, Capabilities, Fact, Health, WriteAck};
use crate::error::BackendError;
use crate::item::{
    CacheEntry, FlushScope, InvalidationReport, MemoryItem, PackedContext, PutOutcome,
};
use crate::key::Key;
use crate::predict::{Candidate, QueryClass, SessionView, TenantStats};
use crate::types::{AdapterId, BackendQuery, EntityId, TenantId, TokenBudget};

/// A memory backend: Neo4j over Bolt, S3, Zep, Mem0. The only thing the core knows about
/// backends (ADR-007).
#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// This adapter's id; part of every cache key it produces.
    fn id(&self) -> AdapterId;

    /// What the backend can do.
    fn capabilities(&self) -> Capabilities;

    /// Runs several queries, in one round trip if `capabilities().native_batch`.
    /// Returns one result per query, in the same order.
    async fn query_batch(
        &self,
        tenant: &TenantId,
        queries: &[BackendQuery],
    ) -> Result<Vec<BackendResult>, BackendError>;

    /// Stores a fact. Returns only after the backend has acknowledged it durably.
    async fn write(&self, tenant: &TenantId, fact: &Fact) -> Result<WriteAck, BackendError>;

    /// Current health, for `/readyz` and the circuit breaker.
    async fn health(&self) -> Health;
}

/// Proposes the queries a session is likely to make next. Pure CPU, no I/O; must return in
/// microseconds because it runs after every recall.
pub trait Predictor: Send + Sync + 'static {
    /// Up to `k` candidates, best first.
    fn predict(&self, view: &SessionView, stats: &TenantStats, k: usize) -> Vec<Candidate>;

    /// Records that `next` followed `prev` in one of `tenant`'s sessions.
    fn observe(&self, tenant: &TenantId, prev: Option<&QueryClass>, next: &QueryClass);
}

/// The cache: tiers, TTLs, priorities, and the entity versions behind the stale-put guard.
///
/// # The version guard
///
/// Each `(tenant, entity)` has a version number. A loader snapshots the versions of the
/// entities it will read *before* calling the backend
/// ([`snapshot_versions`](Self::snapshot_versions)) and stores the snapshot in
/// [`CacheEntry::versions`]. A writer bumps the versions
/// ([`bump_versions`](Self::bump_versions)) *before* evicting keys. [`put`](Self::put)
/// refuses an entry whose snapshot no longer matches, so a slow read that started before a
/// write can never re-insert pre-write data.
#[async_trait]
pub trait CacheStore: Send + Sync + 'static {
    /// The entry under `key`, if present and not expired. Does not check `max_staleness`;
    /// see [`CacheEntry::is_fresh`].
    async fn get(&self, key: &Key) -> Option<Arc<CacheEntry>>;

    /// True if `key` is present. Cheap; used to skip prefetching what is already cached.
    fn contains(&self, key: &Key) -> bool;

    /// Inserts `entry` unless any `(entity, version)` in `entry.versions` has moved, in
    /// which case nothing is stored and [`PutOutcome::StaleVersion`] is returned.
    async fn put(&self, key: Key, entry: CacheEntry) -> PutOutcome;

    /// Marks a speculative entry as used (first hit). Returns true if it was speculative.
    fn promote(&self, key: &Key) -> bool;

    /// Removes one key. Returns true if it was present.
    fn remove(&self, key: &Key) -> bool;

    /// Removes everything in `scope`. Returns how many entries went.
    async fn flush(&self, scope: FlushScope) -> usize;

    /// Current versions of `entities` for `tenant` (entities never written are version 0).
    fn snapshot_versions(&self, tenant: &TenantId, entities: &[EntityId]) -> Vec<(EntityId, u64)>;

    /// Increments the versions of `entities`. Returns how many were bumped.
    fn bump_versions(&self, tenant: &TenantId, entities: &[EntityId]) -> usize;
}

/// Turns recalled items into the context the agent sees. Pure CPU; under 3 ms for 200 items.
pub trait Packer: Send + Sync + 'static {
    /// Dedupes, ranks and trims `items` to `budget`, in a stable order.
    fn pack(&self, items: &[MemoryItem], budget: TokenBudget) -> PackedContext;
}

/// Keeps cached results consistent with writes.
#[async_trait]
pub trait Invalidator: Send + Sync + 'static {
    /// Records that `key`'s result depends on `entities`. Called *before* the entry is put,
    /// so a write that lands in between still finds and evicts the key.
    fn register(&self, key: &Key, entities: &[EntityId]);

    /// A write to `entities` was acknowledged: bump their versions, then evict every key
    /// that depends on them.
    async fn on_write(&self, tenant: &TenantId, entities: &[EntityId]) -> InvalidationReport;
}
