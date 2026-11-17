//! Memory items, cache entries and the outcomes of cache operations.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use time::OffsetDateTime;

use crate::key::Key;
use crate::types::{AdapterId, EntityId, NodeId, TenantId};

/// One piece of remembered knowledge, as returned by a backend.
///
/// `text` is an `Arc<str>` so the same item can sit in many cache entries and responses
/// without being copied.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemoryItem {
    /// The backend's id for this item.
    pub id: NodeId,
    /// The remembered text.
    pub text: Arc<str>,
    /// Backend relevance score; higher is better.
    pub score: f32,
    /// Token count under the tenant's tokenizer; `0` until the packer has counted it.
    #[serde(default)]
    pub tokens: u32,
    /// Entities this item mentions. A write to any of them invalidates entries holding it.
    #[serde(default)]
    pub entities: SmallVec<[EntityId; 4]>,
    /// When the fact was true or recorded, if the backend knows.
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub as_of: Option<OffsetDateTime>,
    /// The adapter that produced it.
    pub source: AdapterId,
}

impl MemoryItem {
    /// Rough heap size in bytes, for the cache weigher.
    pub fn approx_bytes(&self) -> usize {
        const FIXED: usize = std::mem::size_of::<MemoryItem>();
        FIXED
            + self.text.len()
            + self.id.as_str().len()
            + self
                .entities
                .iter()
                .map(|e| e.as_str().len() + std::mem::size_of::<EntityId>())
                .sum::<usize>()
    }
}

/// Eviction priority of a cache entry; lower priorities leave first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Priority {
    /// Prefetched and not yet used. Wrong guesses leave first.
    Speculative = 0,
    /// Loaded on demand, or a speculative entry that has been hit.
    Normal = 1,
    /// A hub entity many queries touch; leaves last.
    Hub = 2,
}

/// A cached query result plus everything needed to decide whether it may still be served.
#[derive(Clone, Debug, PartialEq)]
pub struct CacheEntry {
    /// The items, in backend order.
    pub items: Vec<MemoryItem>,
    /// Entity versions seen when the load *started*; the stale-put check compares these.
    pub versions: SmallVec<[(EntityId, u64); 8]>,
    /// True for a prefetched entry until its first hit.
    pub speculative: bool,
    /// Eviction priority.
    pub priority: Priority,
    /// When the entry was built.
    pub created_at: Instant,
    /// Hard expiry: the TTL is the upper bound on staleness from writes BAiD never sees.
    pub expires_at: Instant,
    /// The adapter the items came from.
    pub source: AdapterId,
    /// Rough size for the weigher.
    pub approx_bytes: u32,
}

impl CacheEntry {
    /// Builds an entry. `now` is passed in so callers (and tests) control the clock.
    pub fn new(
        items: Vec<MemoryItem>,
        versions: &[(EntityId, u64)],
        source: AdapterId,
        ttl: Duration,
        speculative: bool,
        now: Instant,
    ) -> Self {
        let item_bytes: usize = items.iter().map(MemoryItem::approx_bytes).sum();
        let total = std::mem::size_of::<CacheEntry>() + item_bytes;
        CacheEntry {
            items,
            versions: versions.iter().cloned().collect(),
            speculative,
            priority: if speculative {
                Priority::Speculative
            } else {
                Priority::Normal
            },
            created_at: now,
            expires_at: now.checked_add(ttl).unwrap_or(now),
            source,
            approx_bytes: u32::try_from(total).unwrap_or(u32::MAX),
        }
    }

    /// May this entry be served at `now`?
    ///
    /// False once expired. With `max_staleness`, also false when the entry is older than
    /// that; `Some(Duration::ZERO)` therefore never accepts a cached entry.
    pub fn is_fresh(&self, now: Instant, max_staleness: Option<Duration>) -> bool {
        if now >= self.expires_at {
            return false;
        }
        match max_staleness {
            None => true,
            Some(max) if max.is_zero() => false,
            Some(max) => now.saturating_duration_since(self.created_at) <= max,
        }
    }

    /// The same entry marked as used: no longer speculative, normal priority (hubs stay hubs).
    pub fn promoted(&self) -> CacheEntry {
        let mut e = self.clone();
        e.speculative = false;
        e.priority = e.priority.max(Priority::Normal);
        e
    }

    /// Every entity the entry depends on: those named in item metadata plus those whose
    /// versions were snapshotted. Sorted and deduplicated. The invalidator indexes the key
    /// under all of them.
    pub fn entities(&self) -> Vec<EntityId> {
        let mut out: Vec<EntityId> = self
            .items
            .iter()
            .flat_map(|i| i.entities.iter().cloned())
            .chain(self.versions.iter().map(|(e, _)| e.clone()))
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }
}

/// Where a recall's items came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CacheOutcome {
    /// Everything came from the cache.
    Hit,
    /// Some items from the cache, the rest loaded.
    Partial,
    /// Everything loaded from the backend.
    Miss,
}

impl CacheOutcome {
    /// Lower-case name, for metric labels and API responses.
    pub fn as_str(self) -> &'static str {
        match self {
            CacheOutcome::Hit => "hit",
            CacheOutcome::Partial => "partial",
            CacheOutcome::Miss => "miss",
        }
    }
}

/// Result of [`crate::CacheStore::put`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutOutcome {
    /// The entry is in the cache.
    Stored,
    /// A version seen at load start has moved: a write happened meanwhile, so the entry
    /// was dropped instead of re-inserting stale data.
    StaleVersion,
    /// The entry is larger than the tenant's whole capacity.
    TooLarge,
}

/// What [`crate::CacheStore::flush`] removes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FlushScope {
    /// Every entry of every tenant.
    All,
    /// Every entry of one tenant.
    Tenant(TenantId),
    /// One key.
    Key(Key),
}

/// The context handed back to the agent: items in a stable order, trimmed to the budget.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PackedContext {
    /// The kept items, in the order the agent should read them.
    pub items: Vec<MemoryItem>,
    /// Tokens used by `items`.
    pub tokens: u32,
    /// Items dropped as duplicates or to fit the budget.
    pub dropped: u32,
}

/// What a write invalidated.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InvalidationReport {
    /// Entity versions bumped.
    pub versions_bumped: usize,
    /// Cache keys evicted.
    pub keys_evicted: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(id: &str, text: &str, entities: &[&str]) -> MemoryItem {
        MemoryItem {
            id: NodeId::new(id).unwrap(),
            text: Arc::from(text),
            score: 1.0,
            tokens: 0,
            entities: entities.iter().map(|e| EntityId::new(e).unwrap()).collect(),
            as_of: None,
            source: AdapterId::new("bolt").unwrap(),
        }
    }

    fn entry(ttl_s: u64, speculative: bool, now: Instant) -> CacheEntry {
        CacheEntry::new(
            vec![item("n1", "acme renews in March", &["acme"])],
            &[(EntityId::new("acme").unwrap(), 3)],
            AdapterId::new("bolt").unwrap(),
            Duration::from_secs(ttl_s),
            speculative,
            now,
        )
    }

    #[test]
    fn freshness_respects_ttl_and_max_staleness() {
        let t0 = Instant::now();
        let e = entry(60, false, t0);
        assert!(e.is_fresh(t0, None));
        assert!(e.is_fresh(t0 + Duration::from_secs(59), None));
        assert!(!e.is_fresh(t0 + Duration::from_secs(60), None), "expired");
        let ten = Some(Duration::from_secs(10));
        assert!(e.is_fresh(t0 + Duration::from_secs(10), ten));
        assert!(!e.is_fresh(t0 + Duration::from_secs(11), ten));
        assert!(
            !e.is_fresh(t0, Some(Duration::ZERO)),
            "ZERO never serves cache"
        );
    }

    #[test]
    fn speculative_entries_start_low_and_promote() {
        let t0 = Instant::now();
        let e = entry(60, true, t0);
        assert_eq!(e.priority, Priority::Speculative);
        let p = e.promoted();
        assert!(!p.speculative);
        assert_eq!(p.priority, Priority::Normal);

        let mut hub = entry(60, false, t0);
        hub.priority = Priority::Hub;
        assert_eq!(hub.promoted().priority, Priority::Hub);
    }

    #[test]
    fn entities_are_union_sorted_and_deduped() {
        let t0 = Instant::now();
        let mut e = entry(60, false, t0);
        e.items
            .push(item("n2", "globex and acme", &["globex", "acme"]));
        let names: Vec<String> = e.entities().into_iter().map(String::from).collect();
        assert_eq!(names, ["acme", "globex"]);
    }

    #[test]
    fn approx_bytes_tracks_text_size() {
        let t0 = Instant::now();
        let small = entry(60, false, t0);
        let mut big = small.clone();
        big.items[0].text = Arc::from("x".repeat(10_000));
        let big = CacheEntry::new(
            big.items,
            &[],
            AdapterId::new("bolt").unwrap(),
            Duration::from_secs(1),
            false,
            t0,
        );
        assert!(big.approx_bytes >= small.approx_bytes + 9_000);
    }

    #[test]
    fn huge_ttl_does_not_panic() {
        let t0 = Instant::now();
        let e = CacheEntry::new(
            vec![],
            &[],
            AdapterId::new("bolt").unwrap(),
            Duration::MAX,
            false,
            t0,
        );
        assert!(e.expires_at >= t0);
    }

    #[test]
    fn memory_item_json_round_trip() {
        let mut i = item("n1", "hello", &["acme"]);
        i.as_of = Some(time::macros::datetime!(2026-11-17 10:00 UTC));
        let json = serde_json::to_string(&i).unwrap();
        let back: MemoryItem = serde_json::from_str(&json).unwrap();
        assert_eq!(back, i);
    }
}
