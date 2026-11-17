//! HTTP routers: the public API and the admin API, each on its own listener.

pub mod api;
pub mod error;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::json;
use tower_http::trace::TraceLayer;

use crate::auth::Tenant;
use crate::state::AppState;
use error::ApiError;

/// Public API: health probes (no auth) and `/v1/*` (API key required).
pub fn public_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/whoami", get(whoami))
        .route("/v1/recall", post(api::recall))
        .route("/v1/remember", post(api::remember))
        .route("/v1/sessions/{id}", delete(api::end_session))
        .fallback(not_found)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Admin API: health probes now; metrics, flush and reload come later. Bind it to loopback
/// or a private network.
pub fn admin_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .fallback(not_found)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Liveness: the process is up and serving HTTP.
async fn healthz() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

/// Readiness: 503 once shutdown has begun, so traffic drains away.
async fn readyz(axum::extract::State(state): axum::extract::State<AppState>) -> impl IntoResponse {
    if state.is_ready() {
        (StatusCode::OK, Json(json!({ "status": "ready" })))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "draining" })),
        )
    }
}

/// Which tenant the caller's API key belongs to. Lets clients check a key.
async fn whoami(tenant: Tenant) -> impl IntoResponse {
    Json(json!({ "tenant": tenant.id }))
}

async fn not_found() -> ApiError {
    ApiError::NotFound
}
