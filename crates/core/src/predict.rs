//! What the predictor reads (a session view, tenant statistics) and what it proposes
//! (candidates).

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use smallvec::SmallVec;

use crate::item::CacheOutcome;
use crate::key::Key;
use crate::types::{AdapterId, BackendQuery, EntityId, SessionId, TenantId};

/// A query with its entities replaced by their types, e.g. `status of {customer} renewal`.
/// Transition statistics count class-to-class moves, so they generalise across entities.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueryClass(Arc<str>);

impl QueryClass {
    /// Wraps an already-classified query string.
    pub fn new(class: impl Into<Arc<str>>) -> Self {
        QueryClass(class.into())
    }

    /// The class as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for QueryClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One recall in a session's recent window.
#[derive(Clone, Debug, PartialEq)]
pub struct RecallRecord {
    /// The key the recall used.
    pub key: Key,
    /// Its query class.
    pub class: QueryClass,
    /// Entities the recall named or got back.
    pub entities: SmallVec<[EntityId; 4]>,
    /// Hit, partial or miss.
    pub outcome: CacheOutcome,
    /// When the response went back.
    pub at: Instant,
}

/// A read-only snapshot of a session, handed to the predictor.
#[derive(Clone, Debug, PartialEq)]
pub struct SessionView {
    /// The session.
    pub session: SessionId,
    /// Its tenant.
    pub tenant: TenantId,
    /// The tenant's adapter; candidates' keys are derived with it.
    pub adapter: AdapterId,
    /// Recent recalls, oldest first (at most 16).
    pub recent: Vec<RecallRecord>,
    /// Every entity touched in the session, most recently touched first.
    pub entities: Vec<EntityId>,
    /// Moving average of the gap between a response and the next recall; `None` until the
    /// session has made two recalls.
    pub think_time: Option<Duration>,
}

impl SessionView {
    /// The latest recall, if any.
    pub fn last(&self) -> Option<&RecallRecord> {
        self.recent.last()
    }
}

/// A scored next-query class.
#[derive(Clone, Debug, PartialEq)]
pub struct ScoredClass {
    /// The class.
    pub class: QueryClass,
    /// Decayed weight; only relative order matters.
    pub weight: f32,
}

/// Per-tenant statistics the predictor learns from: decayed class-to-class transition
/// counts (top entries per source only) and overall class popularity.
///
/// This is a read-only snapshot; the predict crate owns updating it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TenantStats {
    /// For each class, the classes that followed it, heaviest first.
    pub transitions: HashMap<QueryClass, Vec<ScoredClass>>,
    /// The most asked classes, heaviest first.
    pub popular: Vec<ScoredClass>,
}

impl TenantStats {
    /// Classes seen after `from`, heaviest first; empty if none.
    pub fn next_after(&self, from: &QueryClass) -> &[ScoredClass] {
        self.transitions.get(from).map_or(&[], Vec::as_slice)
    }
}

/// Which predictor proposed a candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PredictionSource {
    /// Neighbourhood of an entity the session just touched.
    Neighbour,
    /// Class-to-class transition statistics.
    Transition,
    /// Tenant-wide popular queries.
    Popularity,
}

impl PredictionSource {
    /// Lower-case name, for metric labels.
    pub fn as_str(self) -> &'static str {
        match self {
            PredictionSource::Neighbour => "neighbour",
            PredictionSource::Transition => "transition",
            PredictionSource::Popularity => "popularity",
        }
    }
}

/// A query the predictor expects the session to need next.
#[derive(Clone, Debug, PartialEq)]
pub struct Candidate {
    /// The key the result would be stored under.
    pub key: Key,
    /// The query to prefetch.
    pub query: BackendQuery,
    /// Confidence in `0.0..=1.0`.
    pub confidence: f32,
    /// Which predictor proposed it.
    pub source: PredictionSource,
}

impl Candidate {
    /// Builds a candidate, clamping `confidence` into `0.0..=1.0` (NaN becomes `0.0`).
    pub fn new(key: Key, query: BackendQuery, confidence: f32, source: PredictionSource) -> Self {
        let confidence = if confidence.is_nan() {
            0.0
        } else {
            confidence.clamp(0.0, 1.0)
        };
        Candidate {
            key,
            query,
            confidence,
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(c: f32) -> Candidate {
        let q = BackendQuery::Entity {
            id: EntityId::new("acme").unwrap(),
        };
        let k = Key::derive(
            &TenantId::new("t").unwrap(),
            &AdapterId::new("bolt").unwrap(),
            &q,
        );
        Candidate::new(k, q, c, PredictionSource::Neighbour)
    }

    #[test]
    fn confidence_is_clamped() {
        assert_eq!(cand(1.7).confidence, 1.0);
        assert_eq!(cand(-0.2).confidence, 0.0);
        assert_eq!(cand(f32::NAN).confidence, 0.0);
        assert_eq!(cand(0.4).confidence, 0.4);
    }

    #[test]
    fn next_after_unknown_class_is_empty() {
        let mut s = TenantStats::default();
        let a = QueryClass::new("status of {customer}");
        assert!(s.next_after(&a).is_empty());
        s.transitions.insert(
            a.clone(),
            vec![ScoredClass {
                class: QueryClass::new("owner of {customer}"),
                weight: 2.0,
            }],
        );
        assert_eq!(s.next_after(&a).len(), 1);
    }
}
