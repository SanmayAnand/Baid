//! Requests that reach the engine, after the HTTP or MCP layer has parsed them.

use std::time::Duration;

use crate::error::RequestError;
use crate::key::normalise;
use crate::types::{BackendQuery, Filters, SessionId, TokenBudget};

/// A `recall`: "what do I already know about this?".
#[derive(Clone, Debug, PartialEq)]
pub struct RecallRequest {
    /// The agent session asking.
    pub session: SessionId,
    /// The question, as the agent wrote it.
    pub query: String,
    /// How much packed context to return.
    pub budget: TokenBudget,
    /// Optional filters.
    pub filters: Filters,
    /// Oldest cached data the caller accepts. `Some(Duration::ZERO)` bypasses the cache.
    pub max_staleness: Option<Duration>,
}

impl RecallRequest {
    /// Longest accepted query, in bytes.
    pub const MAX_QUERY_BYTES: usize = 4096;
    /// Most entity ids accepted in the filters.
    pub const MAX_FILTER_ENTITIES: usize = 64;

    /// Checks the parts of the request that types alone cannot.
    pub fn validate(&self) -> Result<(), RequestError> {
        if self.query.len() > Self::MAX_QUERY_BYTES {
            return Err(RequestError::QueryTooLong(self.query.len()));
        }
        if normalise(&self.query).is_empty() {
            return Err(RequestError::EmptyQuery);
        }
        if self.filters.entity_ids.len() > Self::MAX_FILTER_ENTITIES {
            return Err(RequestError::TooManyEntities(self.filters.entity_ids.len()));
        }
        Ok(())
    }

    /// True when the caller asked to skip the cache.
    pub fn bypasses_cache(&self) -> bool {
        self.max_staleness == Some(Duration::ZERO)
    }

    /// The backend search this recall turns into. The item limit comes from the budget
    /// bucket, so budgets in one bucket share a cache key.
    pub fn backend_query(&self) -> BackendQuery {
        BackendQuery::Search {
            text: self.query.clone(),
            filters: self.filters.clone(),
            limit: self.budget.bucket().item_limit(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EntityId, Filters};

    fn req(query: &str, budget: u32) -> RecallRequest {
        RecallRequest {
            session: SessionId::new("s1").unwrap(),
            query: query.into(),
            budget: TokenBudget::new(budget).unwrap(),
            filters: Filters::default(),
            max_staleness: None,
        }
    }

    #[test]
    fn validation() {
        assert!(req("status of acme", 2000).validate().is_ok());
        assert_eq!(
            req("   ?! ", 2000).validate(),
            Err(RequestError::EmptyQuery)
        );
        let long = "a".repeat(RecallRequest::MAX_QUERY_BYTES + 1);
        assert!(matches!(
            req(&long, 2000).validate(),
            Err(RequestError::QueryTooLong(_))
        ));
        let mut r = req("q", 2000);
        r.filters.entity_ids = (0..65)
            .map(|i| EntityId::new(format!("e{i}")).unwrap())
            .collect();
        assert_eq!(r.validate(), Err(RequestError::TooManyEntities(65)));
    }

    #[test]
    fn budgets_in_one_bucket_share_a_backend_query() {
        assert_eq!(
            req("q", 1100).backend_query(),
            req("q", 2000).backend_query()
        );
        assert_ne!(
            req("q", 500).backend_query(),
            req("q", 2000).backend_query()
        );
    }

    #[test]
    fn zero_staleness_bypasses() {
        let mut r = req("q", 2000);
        assert!(!r.bypasses_cache());
        r.max_staleness = Some(Duration::ZERO);
        assert!(r.bypasses_cache());
    }
}
