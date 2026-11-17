//! The HTTP surface end to end: auth, health, errors, and graceful shutdown on real sockets.

#![allow(clippy::unwrap_used)] // test file

use std::collections::HashMap;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use baid_core::Config;
use serde_json::Value;
use server::config::resolve_secrets;
use server::http::public_router;
use server::run::serve;
use server::state::{AppState, State};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const ACME_KEY: &str = "acme-key-0123456789";
const GLOBEX_KEY: &str = "globex-key-0123456789";

fn state() -> AppState {
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
    config.validate().unwrap();
    let env: HashMap<&str, &str> = [("K_ACME", ACME_KEY), ("K_GLOBEX", GLOBEX_KEY)].into();
    let secrets = resolve_secrets(&config, |k| env.get(k).map(|v| v.to_string())).unwrap();
    State::build(config, secrets).unwrap()
}

async fn call(app: Router, path: &str, auth: Option<&str>) -> (StatusCode, Value) {
    let mut req = Request::get(path);
    if let Some(a) = auth {
        req = req.header("authorization", a);
    }
    let resp = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn health_needs_no_key() {
    let (s, body) = call(public_router(state()), "/healthz", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn api_without_or_with_bad_key_is_401() {
    let app = public_router(state());
    for auth in [
        None,
        Some("Bearer wrong-key-0123456789"),
        Some(ACME_KEY),
        Some("Basic x"),
    ] {
        let (s, body) = call(app.clone(), "/v1/whoami", auth).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED, "{auth:?}");
        assert_eq!(body["error"]["code"], "unauthorized");
        assert_eq!(body["error"]["retryable"], false);
    }
}

#[tokio::test]
async fn the_key_decides_the_tenant() {
    let app = public_router(state());
    let (s, body) = call(
        app.clone(),
        "/v1/whoami",
        Some(&format!("Bearer {ACME_KEY}")),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["tenant"], "acme");
    let (_, body) = call(app, "/v1/whoami", Some(&format!("bearer {GLOBEX_KEY}"))).await;
    assert_eq!(body["tenant"], "globex");
}

#[tokio::test]
async fn unknown_route_is_json_404() {
    let (s, body) = call(public_router(state()), "/v2/nothing", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "not_found");
}

#[tokio::test]
async fn readyz_flips_when_draining() {
    let st = state();
    let (s, _) = call(public_router(st.clone()), "/readyz", None).await;
    assert_eq!(s, StatusCode::OK);
    st.set_ready(false);
    let (s, body) = call(public_router(st), "/readyz", None).await;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["status"], "draining");
}

async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
    sock.write_all(req.as_bytes()).await.unwrap();
    let mut out = String::new();
    sock.read_to_string(&mut out).await.unwrap();
    out
}

#[tokio::test]
async fn serves_both_listeners_and_stops_on_cancel() {
    let public = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (pa, aa) = (public.local_addr().unwrap(), admin.local_addr().unwrap());
    let shutdown = CancellationToken::new();

    let server = serve(state(), public, admin, shutdown.clone());
    let client = async {
        let p = http_get(pa, "/healthz").await;
        let a = http_get(aa, "/readyz").await;
        shutdown.cancel();
        (p, a)
    };
    let (served, (p, a)) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(server, client)
    })
    .await
    .expect("server did not stop after cancel");

    served.unwrap();
    assert!(p.starts_with("HTTP/1.1 200"), "{p}");
    assert!(a.starts_with("HTTP/1.1 200"), "{a}");
}
