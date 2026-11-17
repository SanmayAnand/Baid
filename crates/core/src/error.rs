//! Error types shared by every crate.

/// An identifier failed validation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid {kind}: {reason}")]
pub struct IdError {
    /// Which identifier type was being built, e.g. `TenantId`.
    pub kind: &'static str,
    /// Why it was rejected.
    pub reason: &'static str,
}

/// A token budget outside the accepted range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TokenBudgetError {
    /// Below [`crate::TokenBudget::MIN`]; maps to the API error `budget_too_small`.
    #[error("token budget {0} is below the minimum of 64")]
    TooSmall(u32),
    /// Above [`crate::TokenBudget::MAX`].
    #[error("token budget {0} is above the maximum of 32000")]
    TooLarge(u32),
}

/// A request the engine refuses before doing any work; maps to the API error
/// `invalid_argument`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RequestError {
    /// The query is empty once normalised.
    #[error("query is empty")]
    EmptyQuery,
    /// The query is longer than the limit (bytes given).
    #[error("query is {0} bytes; the limit is 4096")]
    QueryTooLong(usize),
    /// Too many entity ids in the filters (count given).
    #[error("{0} entity filters; the limit is 64")]
    TooManyEntities(usize),
}

/// A backend call failed. Adapters map every driver error into one of these.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BackendError {
    /// No answer within the per-call deadline.
    #[error("backend timed out after {0:?}")]
    Timeout(std::time::Duration),
    /// Could not reach the backend, or it reported itself unavailable.
    #[error("backend unavailable: {0}")]
    Unavailable(String),
    /// The backend answered, but refused the request (bad query, auth, quota).
    #[error("backend rejected request: {0}")]
    Rejected(String),
    /// The circuit breaker is open; the call was not attempted.
    #[error("circuit open")]
    CircuitOpen,
}

impl BackendError {
    /// True for failures that may succeed on retry. `Rejected` will fail the same way again.
    pub fn is_retryable(&self) -> bool {
        !matches!(self, BackendError::Rejected(_))
    }

    /// Short name for metric labels.
    pub fn as_str(&self) -> &'static str {
        match self {
            BackendError::Timeout(_) => "timeout",
            BackendError::Unavailable(_) => "unavailable",
            BackendError::Rejected(_) => "rejected",
            BackendError::CircuitOpen => "circuit_open",
        }
    }
}
