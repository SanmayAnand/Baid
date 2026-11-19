//! Write-through invalidation: an entity → cache-key index that evicts dependent entries as
//! soon as a write to an entity is acknowledged.
//!
//! # Role since ADR-011
//!
//! Correctness does not depend on this index. Every cache read re-checks the write log, so a
//! stale entry is never *served* even if nothing evicts it. What the index adds is prompt
//! eviction: the stale entry's memory is freed at write time instead of lingering until LRU
//! eviction or the next read, and `contains` (used to skip redundant prefetches) stops
//! reporting dead entries.
//!
//! That is also why the index may forget keys safely: it is bounded per entity
//! ([`EntityIndex::MAX_KEYS_PER_ENTITY`]) and swept of keys the cache already evicted
//! ([`EntityIndex::prune`]). A forgotten key costs a lingering entry, never a wrong answer.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use baid_core::{CacheStore, EntityId, InvalidationReport, Invalidator, Key, TenantId};
use dashmap::DashMap;

/// Entity → the cache keys whose results depend on it.
pub struct EntityIndex {
    store: Arc<dyn CacheStore>,
    keys: DashMap<(TenantId, EntityId), HashSet<Key>>,
}

impl EntityIndex {
    /// Most keys remembered per entity. A hub entity mentioned by more results than this
    /// keeps an arbitrary subset; the rest are still refused on read after a write.
    pub const MAX_KEYS_PER_ENTITY: usize = 1_024;

    /// An empty index that evicts from `store`.
    pub fn new(store: Arc<dyn CacheStore>) -> Self {
        EntityIndex {
            store,
            keys: DashMap::new(),
        }
    }

    /// Drops index entries for keys the cache no longer holds (evicted, expired, removed).
    /// Returns how many key references were dropped. Run periodically.
    pub fn prune(&self) -> usize {
        let mut dropped = 0;
        self.keys.retain(|_, keys| {
            let before = keys.len();
            keys.retain(|k| self.store.contains(k));
            dropped += before - keys.len();
            !keys.is_empty()
        });
        dropped
    }

    /// Entities with at least one indexed key.
    pub fn entities(&self) -> usize {
        self.keys.len()
    }

    /// Key references held, summed over entities.
    pub fn references(&self) -> usize {
        self.keys.iter().map(|e| e.value().len()).sum()
    }
}

#[async_trait]
impl Invalidator for EntityIndex {
    fn register(&self, key: &Key, entities: &[EntityId]) {
        let tenant = key.tenant();
        for e in entities {
            let mut keys = self.keys.entry((tenant.clone(), e.clone())).or_default();
            if keys.len() < Self::MAX_KEYS_PER_ENTITY || keys.contains(key) {
                keys.insert(key.clone());
            }
        }
    }

    async fn on_write(&self, tenant: &TenantId, entities: &[EntityId]) -> InvalidationReport {
        let mut distinct: Vec<&EntityId> = entities.iter().collect();
        distinct.sort_unstable();
        distinct.dedup();
        // Take each entity's key set out first (short shard locks), then evict.
        let mut doomed: HashSet<Key> = HashSet::new();
        for e in &distinct {
            if let Some((_, keys)) = self.keys.remove(&(tenant.clone(), (*e).clone())) {
                doomed.extend(keys);
            }
        }
        let keys_evicted = doomed.iter().filter(|k| self.store.remove(k)).count();
        InvalidationReport {
            entities: distinct.len(),
            keys_evicted,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use baid_core::{
        AdapterId, BackendQuery, CacheEntry, LoadInfo, MemoryItem, NodeId, PutOutcome,
    };
    use cache::{RamStore, SystemClock, TierConfig};
    use futures::executor::block_on;

    use super::*;

    fn t(s: &str) -> TenantId {
        TenantId::new(s).unwrap()
    }
    fn e(s: &str) -> EntityId {
        EntityId::new(s).unwrap()
    }
    fn key(tenant: &str, q: &str) -> Key {
        Key::derive(
            &t(tenant),
            &AdapterId::new("memory").unwrap(),
            &BackendQuery::Entity { id: e(q) },
        )
    }

    struct Rig {
        store: Arc<RamStore>,
        index: EntityIndex,
    }

    fn rig() -> Rig {
        let store = Arc::new(RamStore::new(Arc::new(SystemClock)));
        for tn in ["acme", "globex"] {
            store.add_tenant(t(tn), TierConfig::with_capacity(16 << 20));
        }
        Rig {
            index: EntityIndex::new(store.clone()),
            store,
        }
    }

    impl Rig {
        /// Caches a result for `key` that mentions `entities`, and registers it.
        fn cache(&self, k: &Key, entities: &[&str]) {
            let item = MemoryItem {
                id: NodeId::new("n").unwrap(),
                text: Arc::from("x"),
                score: 1.0,
                tokens: 0,
                entities: entities.iter().map(|x| e(x)).collect(),
                as_of: None,
                source: AdapterId::new("memory").unwrap(),
            };
            let entry = CacheEntry::new(
                vec![item],
                LoadInfo {
                    query_entities: &[],
                    loaded_at: self.store.write_seq(k.tenant()),
                    source: AdapterId::new("memory").unwrap(),
                    ttl: Duration::from_secs(60),
                    speculative: false,
                },
                Instant::now(),
            );
            let deps = entry.entities.to_vec();
            assert_eq!(
                block_on(self.store.put(k.clone(), entry)),
                PutOutcome::Stored
            );
            self.index.register(k, &deps);
        }
    }

    #[test]
    fn a_write_evicts_exactly_the_dependent_keys() {
        let r = rig();
        let (k1, k2, k3) = (key("acme", "q1"), key("acme", "q2"), key("acme", "q3"));
        r.cache(&k1, &["acme"]);
        r.cache(&k2, &["acme", "globex-corp"]);
        r.cache(&k3, &["other"]);

        let rep = block_on(r.index.on_write(&t("acme"), &[e("acme"), e("acme")]));
        assert_eq!(
            rep,
            InvalidationReport {
                entities: 1,
                keys_evicted: 2
            }
        );
        assert!(!r.store.contains(&k1) && !r.store.contains(&k2));
        assert!(r.store.contains(&k3), "unrelated entry kept");
        // The written entity's index entry is gone; the co-mentioned one still points at k2,
        // which is harmless and swept by prune.
        assert_eq!(r.index.prune(), 1);
        assert_eq!(r.index.entities(), 1);
    }

    #[test]
    fn writes_never_cross_tenants() {
        let r = rig();
        let (a, g) = (key("acme", "q"), key("globex", "q"));
        r.cache(&a, &["shared-name"]);
        r.cache(&g, &["shared-name"]);
        let rep = block_on(r.index.on_write(&t("globex"), &[e("shared-name")]));
        assert_eq!(rep.keys_evicted, 1);
        assert!(r.store.contains(&a), "acme's entry survives globex's write");
        assert!(!r.store.contains(&g));
    }

    #[test]
    fn unknown_entities_and_already_evicted_keys_are_fine() {
        let r = rig();
        let k = key("acme", "q");
        r.cache(&k, &["acme"]);
        r.store.remove(&k); // e.g. LRU eviction
        let rep = block_on(r.index.on_write(&t("acme"), &[e("acme"), e("nobody")]));
        assert_eq!(
            rep,
            InvalidationReport {
                entities: 2,
                keys_evicted: 0
            }
        );
    }

    #[test]
    fn the_index_is_bounded_per_entity() {
        let r = rig();
        for i in 0..(EntityIndex::MAX_KEYS_PER_ENTITY + 100) {
            r.index
                .register(&key("acme", &format!("q{i}")), &[e("hub")]);
        }
        assert_eq!(r.index.references(), EntityIndex::MAX_KEYS_PER_ENTITY);
        // Re-registering a known key at the cap is not a new reference.
        r.index.register(&key("acme", "q0"), &[e("hub")]);
        assert_eq!(r.index.references(), EntityIndex::MAX_KEYS_PER_ENTITY);
    }

    #[test]
    fn prune_drops_keys_the_cache_no_longer_holds() {
        let r = rig();
        let (k1, k2) = (key("acme", "q1"), key("acme", "q2"));
        r.cache(&k1, &["x"]);
        r.cache(&k2, &["x", "y"]);
        r.store.remove(&k1);
        assert_eq!(r.index.prune(), 1);
        assert_eq!(r.index.references(), 2, "k2 under x and y");
        r.store.remove(&k2);
        assert_eq!(r.index.prune(), 2);
        assert_eq!(r.index.entities(), 0);
    }
}
