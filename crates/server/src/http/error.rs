//! The HTTP error contract (Code architecture section 4.1).
//!
//! Every error response has the same body:
//!
//! ```json
//! {"error": {"code": "backend_unavailable", "message": "...", "retryable": true}}
//! ```
//!
//! Clients retry only when `retryable` is true. Internal errors never reveal their cause.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use baid_core::{BackendError, RequestError, TokenBudgetError};
use serde_json::json;

/// An error returned to an API caller.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// Missing, malformed or unknown API key.
    #[error("unauthorized")]
    Unauthorized,
    /// The request is malformed or breaks a limit.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// The token budget is below the minimum.
    #[error("budget too small")]
    BudgetTooSmall,
    /// The backend failed.
    #[error(transparent)]
    Backend(#[from] BackendError),
    /// The tenant is over its request rate.
    #[error("rate limited")]
    RateLimited,
    /// No such route.
    #[error("not found")]
    NotFound,
    /// Something inside BAiD failed. Details go to the log, never to the caller.
    #[error("internal")]
    Internal,
}

impl ApiError {
    /// HTTP status.
    pub fn status(&self) -> StatusCode {
        match self {
            ApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            ApiError::InvalidArgument(_) | ApiError::BudgetTooSmall => StatusCode::BAD_REQUEST,
            ApiError::Backend(BackendError::Rejected(_)) => StatusCode::BAD_GATEWAY,
            ApiError::Backend(_) => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            ApiError::NotFound => StatusCode::NOT_FOUND,
            ApiError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Stable machine-readable code.
    pub fn code(&self) -> &'static str {
        match self {
            ApiError::Unauthorized => "unauthorized",
            ApiError::InvalidArgument(_) => "invalid_argument",
            ApiError::BudgetTooSmall => "budget_too_small",
            ApiError::Backend(BackendError::Rejected(_)) => "backend_error",
            ApiError::Backend(_) => "backend_unavailable",
            ApiError::RateLimited => "rate_limited",
            ApiError::NotFound => "not_found",
            ApiError::Internal => "internal",
        }
    }

    /// Whether the same request may succeed if retried.
    pub fn retryable(&self) -> bool {
        match self {
            ApiError::Backend(e) => e.is_retryable(),
            ApiError::RateLimited | ApiError::Internal => true,
            _ => false,
        }
    }

    /// The message shown to the caller.
    fn public_message(&self) -> String {
        match self {
            ApiError::Internal => "internal error".to_owned(),
            other => other.to_string(),
        }
    }
}

impl From<RequestError> for ApiError {
    fn from(e: RequestError) -> Self {
        ApiError::InvalidArgument(e.to_string())
    }
}

impl From<TokenBudgetError> for ApiError {
    fn from(e: TokenBudgetError) -> Self {
        match e {
            TokenBudgetError::TooSmall(_) => ApiError::BudgetTooSmall,
            TokenBudgetError::TooLarge(_) => ApiError::InvalidArgument(e.to_string()),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({
            "error": {
                "code": self.code(),
                "message": self.public_message(),
                "retryable": self.retryable(),
            }
        });
        (self.status(), Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn mapping_matches_the_spec_table() {
        let cases: Vec<(ApiError, u16, &str, bool)> = vec![
            (ApiError::Unauthorized, 401, "unauthorized", false),
            (
                ApiError::InvalidArgument("x".into()),
                400,
                "invalid_argument",
                false,
            ),
            (ApiError::BudgetTooSmall, 400, "budget_too_small", false),
            (
                BackendError::Timeout(Duration::from_secs(1)).into(),
                503,
                "backend_unavailable",
                true,
            ),
            (
                BackendError::Unavailable("x".into()).into(),
                503,
                "backend_unavailable",
                true,
            ),
            (
                BackendError::CircuitOpen.into(),
                503,
                "backend_unavailable",
                true,
            ),
            (
                BackendError::Rejected("x".into()).into(),
                502,
                "backend_error",
                false,
            ),
            (ApiError::RateLimited, 429, "rate_limited", true),
            (ApiError::Internal, 500, "internal", true),
            (ApiError::NotFound, 404, "not_found", false),
        ];
        for (err, status, code, retry) in cases {
            assert_eq!(err.status().as_u16(), status, "{err:?}");
            assert_eq!(err.code(), code, "{err:?}");
            assert_eq!(err.retryable(), retry, "{err:?}");
        }
    }

    #[test]
    fn budget_errors_map_to_codes() {
        assert_eq!(
            ApiError::from(TokenBudgetError::TooSmall(10)).code(),
            "budget_too_small"
        );
        assert_eq!(
            ApiError::from(TokenBudgetError::TooLarge(99_999)).code(),
            "invalid_argument"
        );
    }
}
