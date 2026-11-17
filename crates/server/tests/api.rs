//! `/v1/recall`, `/v1/remember` and `/v1/sessions` over HTTP, against the memory adapter.
//! Each test pins one guarantee from the engine docs.

#![allow(clippy::unwrap_used)] // test file

use std::collections::HashMap;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use baid_core::Config;
use serde_json::{json, Value};
use server::config::resolve_secrets;
use server::http::public_router;
use server::state::State;
use tower::ServiceExt;

const ACME: &str = "acme-key-0123456789";
const GLOBEX: &str = "globex-key-0123456789";

fn app() -> Router {
    let config: Config = toml::from_str(
        r#"
[[tenants]]
id = "acme"
api_key_env = "K_ACME"
adapter = "memory"
adapter_url = "memory://"

[[tenants]]
id = "globex"
api_key_env = "K_GLOBEX"
adapter = "memory"
adapter_url = "memory://"
"#,
    )
    .unwrap();
    let env: HashMap<&str, &str> = [("K_ACME", ACME), ("K_GLOBEX", GLOBEX)].into();
    let secrets = resolve_secrets(&config, |k| env.get(k).map(|v| v.to_string())).unwrap();
    public_router(State::build(config, secrets).unwrap())
}

async fn send(
    app: &Router,
    method: &str,
    path: &str,
    key: Option<&str>,
    body: &str,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(k) = key {
        req = req.header("authorization", format!("Bearer {k}"));
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn recall(app: &Router, key: &str, session: &str, query: &str) -> Value {
    let (s, v) = send(
        app,
        "POST",
        "/v1/recall",
        Some(key),
        &json!({ "session_id": session, "query": query }).to_string(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    v
}

async fn remember(app: &Router, key: &str, session: &str, text: &str, entities: &[&str]) -> Value {
    let (s, v) = send(
        app,
        "POST",
        "/v1/remember",
        Some(key),
        &json!({ "session_id": session, "text": text, "entities": entities }).to_string(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    v
}

fn texts(v: &Value) -> Vec<String> {
    v["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["text"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn second_recall_is_a_hit() {
    let app = app();
    remember(&app, ACME, "s1", "Acme renews in March", &["acme"]).await;
    let first = recall(&app, ACME, "s2", "acme renewal").await;
    assert_eq!(first["cache"], "miss");
    assert_eq!(texts(&first), ["Acme renews in March"]);
    let second = recall(&app, ACME, "s2", "Acme renewal?").await; // same key after normalising
    assert_eq!(second["cache"], "hit");
    assert_eq!(texts(&second), texts(&first));
    assert!(second["tokens"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn a_session_always_sees_its_own_write() {
    let app = app();
    let before = recall(&app, ACME, "s1", "renewal dates").await; // caches an empty result
    assert!(texts(&before).is_empty());
    remember(&app, ACME, "s1", "renewal dates: acme in May", &["acme"]).await;
    let after = recall(&app, ACME, "s1", "renewal dates").await;
    assert_eq!(after["cache"], "miss", "cached entry predates s1's write");
    assert_eq!(texts(&after), ["renewal dates: acme in May"]);
}

/// The documented limit: a write *adds* a result to a search another session cached, and
/// that session sees it only after the entry expires or is reloaded.
#[tokio::test]
async fn other_sessions_may_miss_added_results_until_reload() {
    let app = app();
    recall(&app, ACME, "reader", "renewal dates").await; // empty, cached
    remember(
        &app,
        ACME,
        "writer",
        "renewal dates: acme in May",
        &["acme"],
    )
    .await;
    let reader = recall(&app, ACME, "reader", "renewal dates").await;
    assert_eq!(reader["cache"], "hit");
    assert!(
        texts(&reader).is_empty(),
        "bounded by the TTL, as documented"
    );
    let writer = recall(&app, ACME, "writer", "renewal dates").await;
    assert_eq!(texts(&writer), ["renewal dates: acme in May"]);
}

#[tokio::test]
async fn a_write_to_a_cached_entity_invalidates_for_everyone() {
    let app = app();
    remember(&app, ACME, "w1", "acme plan is gold", &["acme"]).await;
    recall(&app, ACME, "reader", "acme plan").await;
    assert_eq!(
        recall(&app, ACME, "reader", "acme plan").await["cache"],
        "hit"
    );
    remember(&app, ACME, "w2", "acme plan is platinum", &["acme"]).await;
    let v = recall(&app, ACME, "reader", "acme plan").await;
    assert_eq!(
        v["cache"], "miss",
        "reader never wrote, but the entry mentions acme"
    );
    assert_eq!(texts(&v).len(), 2);
}

#[tokio::test]
async fn tenants_never_see_each_other() {
    let app = app();
    remember(&app, ACME, "s", "acme secret plan", &["acme"]).await;
    recall(&app, ACME, "s", "secret plan").await; // cached for acme
    let g = recall(&app, GLOBEX, "s", "secret plan").await;
    assert!(texts(&g).is_empty());
    assert_eq!(
        g["cache"], "miss",
        "same question, different tenant: not shared"
    );
}

#[tokio::test]
async fn zero_staleness_skips_the_cache() {
    let app = app();
    let body = json!({ "session_id": "s", "query": "q", "max_staleness_ms": 0 }).to_string();
    for _ in 0..2 {
        let (_, v) = send(&app, "POST", "/v1/recall", Some(ACME), &body).await;
        assert_eq!(v["cache"], "miss");
    }
}

#[tokio::test]
async fn budget_trims_the_context() {
    let app = app();
    for i in 0..5 {
        let text = format!("acme note {i}: {}", "detail ".repeat(20));
        remember(&app, ACME, "s", &text, &["acme"]).await;
    }
    let body = json!({ "session_id": "s", "query": "acme note", "token_budget": 64 }).to_string();
    let (_, v) = send(&app, "POST", "/v1/recall", Some(ACME), &body).await;
    assert!(v["tokens"].as_u64().unwrap() <= 64);
    assert!(v["dropped"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn idempotent_remember_is_applied_once() {
    let app = app();
    let body = json!({ "session_id": "s", "text": "acme signed", "entities": ["acme"], "idempotency_key": "req-1" }).to_string();
    let (_, a) = send(&app, "POST", "/v1/remember", Some(ACME), &body).await;
    let (_, b) = send(&app, "POST", "/v1/remember", Some(ACME), &body).await;
    assert_eq!(a["replayed"], false);
    assert_eq!(b["replayed"], true);
    assert_eq!(a["id"], b["id"]);
    assert_eq!(
        texts(&recall(&app, ACME, "s", "acme signed").await).len(),
        1
    );
}

#[tokio::test]
async fn bad_requests_get_clear_errors() {
    let app = app();
    let cases = [
        (
            "/v1/recall",
            json!({ "session_id": "s", "query": "  ?! " }).to_string(),
            "invalid_argument",
        ),
        (
            "/v1/recall",
            json!({ "session_id": "s", "query": "q", "token_budget": 10 }).to_string(),
            "budget_too_small",
        ),
        (
            "/v1/recall",
            json!({ "session_id": "s", "query": "q", "tenant": "globex" }).to_string(),
            "invalid_argument",
        ),
        ("/v1/recall", "{not json".to_owned(), "invalid_argument"),
        (
            "/v1/recall",
            json!({ "session_id": "a\u{1}b", "query": "q" }).to_string(),
            "invalid_argument",
        ),
        (
            "/v1/remember",
            json!({ "session_id": "s", "text": "   " }).to_string(),
            "invalid_argument",
        ),
    ];
    for (path, body, code) in cases {
        let (s, v) = send(&app, "POST", path, Some(ACME), &body).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{body} -> {v}");
        assert_eq!(v["error"]["code"], code, "{body}");
    }
    let (s, _) = send(&app, "POST", "/v1/recall", None, "{}").await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn ending_a_session_is_idempotent() {
    let app = app();
    recall(&app, ACME, "s9", "q").await;
    for _ in 0..2 {
        let (s, _) = send(&app, "DELETE", "/v1/sessions/s9", Some(ACME), "").await;
        assert_eq!(s, StatusCode::NO_CONTENT);
    }
}
