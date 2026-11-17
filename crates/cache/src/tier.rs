//! One tenant's RAM tier: a foyer LRU cache plus counters.
//!
//! foyer's LRU has two priority lists. Entries inserted with `Hint::Low` go to a list that
//! is always evicted first; `Hint::Normal` entries live in a protected pool (by default up
//! to 90% of capacity). We map [`Priority::Speculative`] to `Low`, and both `Normal` and
//! `Hub` to `Normal`. A third level for hubs would need a separate pool; it is not worth
//! adding until something detects hubs (see ADR-005).

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use baid_core::{CacheEntry, Key, Priority};
use foyer::{Cache, CacheBuilder, CacheProperties, Event, EventListener, Hint, LruConfig};

/// Size and shape of one tenant's RAM tier.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TierConfig {
    /// Total weight (approximate bytes) the tier may hold.
    pub capacity_bytes: usize,
    /// foyer shards; capacity is split evenly between them. More shards, less contention.
    pub shards: usize,
    /// Share of capacity the normal-priority pool may use before it spills into the
    /// speculative list (foyer's `high_priority_pool_ratio`).
    pub normal_pool_ratio: f64,
}

impl TierConfig {
    /// A tier of `capacity_bytes` with 8 shards and a 90% normal pool.
    pub fn with_capacity(capacity_bytes: usize) -> Self {
        TierConfig {
            capacity_bytes,
            shards: 8,
            normal_pool_ratio: 0.9,
        }
    }

    /// Largest single entry accepted: a quarter of one shard. foyer would otherwise admit
    /// an entry bigger than the whole cache and evict everything else for it.
    pub fn max_entry_bytes(&self) -> usize {
        self.capacity_bytes / self.shards.max(1) / 4
    }
}

/// Counters for one tier, read by telemetry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TierStats {
    /// Entries currently held.
    pub entries: u64,
    /// Weight currently held.
    pub usage_bytes: usize,
    /// Configured capacity.
    pub capacity_bytes: usize,
    /// Entries evicted to make room.
    pub evicted: u64,
    /// Speculative entries that left (evicted, removed or flushed) without ever being hit:
    /// the numerator of the prefetch waste ratio.
    pub speculative_wasted: u64,
}

#[derive(Debug, Default)]
struct Counters {
    entries: AtomicI64,
    evicted: AtomicU64,
    speculative_wasted: AtomicU64,
}

/// Keeps the counters in step with foyer's evictions, removals and clears.
struct Listener(Arc<Counters>);

impl EventListener for Listener {
    type Key = Key;
    type Value = Arc<CacheEntry>;

    fn on_leave(&self, reason: Event, _key: &Key, value: &Arc<CacheEntry>) {
        self.0.entries.fetch_sub(1, Ordering::Relaxed);
        if matches!(reason, Event::Evict) {
            self.0.evicted.fetch_add(1, Ordering::Relaxed);
        }
        // `Replace` is a re-insert of the same key (a promotion or a fresher load), so the
        // speculative entry it replaces was not wasted.
        if value.speculative && !matches!(reason, Event::Replace) {
            self.0.speculative_wasted.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Approximate weight of one cache record: the entry plus the key.
fn weigh(key: &Key, entry: &Arc<CacheEntry>) -> usize {
    const KEY_OVERHEAD: usize = std::mem::size_of::<Key>() + 16;
    entry.approx_bytes as usize + key.tenant().as_str().len() + KEY_OVERHEAD
}

pub(crate) struct Tier {
    pub(crate) cache: Cache<Key, Arc<CacheEntry>>,
    pub(crate) config: TierConfig,
    counters: Arc<Counters>,
}

impl Tier {
    pub(crate) fn new(config: TierConfig) -> Self {
        let counters = Arc::new(Counters::default());
        let cache = CacheBuilder::new(config.capacity_bytes)
            .with_shards(config.shards.max(1))
            .with_weighter(weigh)
            .with_eviction_config(LruConfig {
                high_priority_pool_ratio: config.normal_pool_ratio,
            })
            .with_event_listener(Arc::new(Listener(counters.clone())))
            .build();
        Tier {
            cache,
            config,
            counters,
        }
    }

    pub(crate) fn insert(&self, key: Key, entry: Arc<CacheEntry>) {
        let hint = match entry.priority {
            Priority::Speculative => Hint::Low,
            Priority::Normal | Priority::Hub => Hint::Normal,
        };
        self.cache
            .insert_with_properties(key, entry, CacheProperties::default().with_hint(hint));
        self.counters.entries.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn stats(&self) -> TierStats {
        TierStats {
            entries: u64::try_from(self.counters.entries.load(Ordering::Relaxed)).unwrap_or(0),
            usage_bytes: self.cache.usage(),
            capacity_bytes: self.cache.capacity(),
            evicted: self.counters.evicted.load(Ordering::Relaxed),
            speculative_wasted: self.counters.speculative_wasted.load(Ordering::Relaxed),
        }
    }
}
