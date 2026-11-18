//! `api/openapi.yaml` is the contract the Python SDK and memharness are built from. These
//! tests fail when the server and the spec drift apart:
//!
//! - the spec's routes are exactly [`server::http::ROUTES`];
//! - the spec's error codes are exactly [`ApiError::CODES`];
//! - every documented request example is accepted, and the response matches the documented
//!   success schema;
//! - request bodies documented as closed (`additionalProperties: false`) really reject
//!   unknown fields, with a body that matches the `Error` schema.

#![allow(clippy::unwrap_used)] // test file

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;

use axum::body::{to_bytes, Body};
use axum::http::Request;
use axum::Router;
use baid_core::Config;
use serde_json::Value;
use server::config::resolve_secrets;
use server::http::error::ApiError;
use server::http::{public_router, ROUTES};
use server::state::State;
use tower::ServiceExt;

const KEY: &str = "contract-key-0123456789";
const METHODS: [&str; 5] = ["get", "post", "put", "patch", "delete"];

fn spec() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../api/openapi.yaml");
    let text = std::fs::read_to_string(&path).unwrap();
    serde_norway::from_str(&text).unwrap()
}

async fn app() -> Router {
    let config: Config = toml::from_str(
        "[[tenants]]\nid = \"acme\"\napi_key_env = \"K\"\nadapter = \"memory\"\nadapter_url = \"memory://\"\n",
    )
    .unwrap();
    let env: HashMap<&str, &str> = [("K", KEY)].into();
    let secrets = resolve_secrets(&config, |k| env.get(k).map(|v| v.to_string())).unwrap();
    public_router(State::build(config, secrets).await.unwrap())
}

/// Follows a local `$ref` ("#/components/...").
fn resolve<'a>(root: &'a Value, v: &'a Value) -> &'a Value {
    match v.get("$ref").and_then(Value::as_str) {
        Some(r) => {
            let mut cur = root;
            for part in r.trim_start_matches("#/").split('/') {
                cur = &cur[part];
            }
            assert!(!cur.is_null(), "dangling $ref {r}");
            resolve(root, cur)
        }
        None => v,
    }
}

fn type_matches(t: &str, v: &Value) -> bool {
    match t {
        "string" => v.is_string(),
        "integer" => v.is_i64() || v.is_u64(),
        "number" => v.is_number(),
        "boolean" => v.is_boolean(),
        "array" => v.is_array(),
        "object" => v.is_object(),
        "null" => v.is_null(),
        other => panic!("unsupported type {other}"),
    }
}

/// A small subset of JSON Schema: `$ref`, `type`, `enum`, `required`, `properties`,
/// `items`, `additionalProperties: false`. Enough to catch drift in shape and names.
fn validate(root: &Value, schema: &Value, v: &Value, at: &str, errs: &mut Vec<String>) {
    let schema = resolve(root, schema);
    if let Some(t) = schema.get("type") {
        let ok = match t {
            Value::String(t) => type_matches(t, v),
            Value::Array(ts) => ts.iter().any(|t| type_matches(t.as_str().unwrap(), v)),
            _ => panic!("bad type at {at}"),
        };
        if !ok {
            errs.push(format!("{at}: expected type {t}, got {v}"));
            return;
        }
    }
    if let Some(allowed) = schema.get("enum").and_then(Value::as_array) {
        if !allowed.contains(v) {
            errs.push(format!("{at}: {v} not in {allowed:?}"));
        }
    }
    if let (Some(obj), Some(props)) = (v.as_object(), schema.get("properties")) {
        let props = props.as_object().unwrap();
        for r in schema
            .get("required")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let r = r.as_str().unwrap();
            if !obj.contains_key(r) {
                errs.push(format!("{at}: missing required `{r}`"));
            }
        }
        for (k, val) in obj {
            match props.get(k) {
                Some(s) => validate(root, s, val, &format!("{at}.{k}"), errs),
                None if schema.get("additionalProperties") == Some(&Value::Bool(false)) => {
                    errs.push(format!("{at}: undocumented field `{k}`"));
                }
                None => errs.push(format!("{at}: field `{k}` is not in the spec")),
            }
        }
    }
    if let (Some(arr), Some(items)) = (v.as_array(), schema.get("items")) {
        for (i, x) in arr.iter().enumerate() {
            validate(root, items, x, &format!("{at}[{i}]"), errs);
        }
    }
}

struct Op<'a> {
    method: String,
    path: String,
    op: &'a Value,
}

fn operations(spec: &Value) -> Vec<Op<'_>> {
    let mut ops = Vec::new();
    for (path, item) in spec["paths"].as_object().unwrap() {
        for m in METHODS {
            if let Some(op) = item.get(m) {
                ops.push(Op {
                    method: m.to_uppercase(),
                    path: path.clone(),
                    op,
                });
            }
        }
    }
    ops
}

fn needs_auth(spec: &Value, op: &Value) -> bool {
    let sec = op.get("security").unwrap_or(&spec["security"]);
    !sec.as_array().unwrap().is_empty()
}

async fn send(
    app: &Router,
    method: &str,
    path: &str,
    auth: bool,
    body: Option<&Value>,
) -> (u16, Value) {
    let mut req = Request::builder().method(method).uri(path);
    if auth {
        req = req.header("authorization", format!("Bearer {KEY}"));
    }
    let body = match body {
        Some(b) => {
            req = req.header("content-type", "application/json");
            Body::from(b.to_string())
        }
        None => Body::empty(),
    };
    let resp = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
    let status = resp.status().as_u16();
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[test]
fn routes_match_the_spec() {
    let spec = spec();
    let in_spec: BTreeSet<(String, String)> = operations(&spec)
        .into_iter()
        .map(|o| (o.method, o.path))
        .collect();
    let in_code: BTreeSet<(String, String)> = ROUTES
        .iter()
        .map(|(m, p)| (m.to_string(), p.to_string()))
        .collect();
    assert_eq!(in_spec, in_code);
}

#[test]
fn error_codes_match_the_spec() {
    let spec = spec();
    let codes: BTreeSet<&str> = spec["components"]["schemas"]["Error"]["properties"]["error"]
        ["properties"]["code"]["enum"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c.as_str().unwrap())
        .collect();
    assert_eq!(codes, ApiError::CODES.iter().copied().collect());
}

/// The structural rules SDK generators trip over most.
#[test]
fn spec_is_structurally_sound() {
    let spec = spec();
    assert!(spec["openapi"].as_str().unwrap().starts_with("3.1."));
    assert!(spec["info"]["title"].is_string() && spec["info"]["version"].is_string());
    let mut ids = BTreeSet::new();
    for o in operations(&spec) {
        let id = o.op["operationId"].as_str();
        let id = id.unwrap_or_else(|| panic!("{} {} has no operationId", o.method, o.path));
        assert!(ids.insert(id.to_owned()), "duplicate operationId {id}");
        let responses = o.op["responses"].as_object().unwrap();
        assert!(
            responses.keys().any(|k| k.starts_with('2')),
            "{} {} documents no success response",
            o.method,
            o.path
        );
        let declared: BTreeSet<&str> = o.op["parameters"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|p| p["in"] == "path")
            .map(|p| p["name"].as_str().unwrap())
            .collect();
        let in_path: BTreeSet<&str> = o
            .path
            .split('/')
            .filter_map(|s| s.strip_prefix('{').and_then(|s| s.strip_suffix('}')))
            .collect();
        assert_eq!(
            declared, in_path,
            "{} {}: path parameters",
            o.method, o.path
        );
    }
}

#[test]
fn every_ref_resolves() {
    fn walk(root: &Value, v: &Value) {
        match v {
            Value::Object(m) => {
                if m.contains_key("$ref") {
                    resolve(root, v);
                }
                m.values().for_each(|x| walk(root, x));
            }
            Value::Array(a) => a.iter().for_each(|x| walk(root, x)),
            _ => {}
        }
    }
    let spec = spec();
    walk(&spec, &spec);
}

#[tokio::test]
async fn documented_examples_work_and_responses_match() {
    let spec = spec();
    let app = app().await;
    for o in operations(&spec) {
        let path = o.path.replace("{session_id}", "contract-session");
        let example = o.op["requestBody"]["content"]["application/json"]["example"].clone();
        let body = (!example.is_null()).then_some(&example);
        if let Some(b) = body {
            let schema = &o.op["requestBody"]["content"]["application/json"]["schema"];
            let mut errs = Vec::new();
            validate(&spec, schema, b, "request", &mut errs);
            assert!(
                errs.is_empty(),
                "{} {}: example breaks its own schema: {errs:?}",
                o.method,
                o.path
            );
        }

        let (status, resp) = send(&app, &o.method, &path, needs_auth(&spec, o.op), body).await;
        let documented = &o.op["responses"][status.to_string()];
        assert!(
            !documented.is_null(),
            "{} {} returned undocumented status {status}: {resp}",
            o.method,
            o.path
        );
        assert!(
            (200..300).contains(&status),
            "{} {} example failed: {status} {resp}",
            o.method,
            o.path
        );
        let documented = resolve(&spec, documented);
        if let Some(schema) = documented
            .get("content")
            .map(|c| &c["application/json"]["schema"])
        {
            let mut errs = Vec::new();
            validate(&spec, schema, &resp, "response", &mut errs);
            assert!(errs.is_empty(), "{} {}: {errs:?}\n{resp}", o.method, o.path);
        }
    }
}

#[tokio::test]
async fn closed_bodies_reject_unknown_fields_with_documented_errors() {
    let spec = spec();
    let app = app().await;
    for o in operations(&spec) {
        let content = &o.op["requestBody"]["content"]["application/json"];
        let Some(mut example) = content.get("example").cloned() else {
            continue;
        };
        let schema = resolve(&spec, &content["schema"]);
        if schema.get("additionalProperties") != Some(&Value::Bool(false)) {
            continue;
        }
        example["tenant"] = Value::from("someone-else");
        let (status, resp) = send(&app, &o.method, &o.path, true, Some(&example)).await;
        assert_eq!(
            status, 400,
            "{} {} accepted an unknown field",
            o.method, o.path
        );
        let mut errs = Vec::new();
        validate(
            &spec,
            &serde_json::json!({ "$ref": "#/components/schemas/Error" }),
            &resp,
            "error",
            &mut errs,
        );
        assert!(errs.is_empty(), "{errs:?}");
        assert_eq!(resp["error"]["code"], "invalid_argument");
    }
}

#[tokio::test]
async fn unauthenticated_calls_get_the_documented_401() {
    let spec = spec();
    let app = app().await;
    for o in operations(&spec)
        .into_iter()
        .filter(|o| needs_auth(&spec, o.op))
    {
        let path = o.path.replace("{session_id}", "s");
        let (status, resp) = send(&app, &o.method, &path, false, None).await;
        assert_eq!(status, 401, "{} {}", o.method, o.path);
        assert!(
            !o.op["responses"]["401"].is_null(),
            "{} {} does not document 401",
            o.method,
            o.path
        );
        let mut errs = Vec::new();
        validate(
            &spec,
            &serde_json::json!({ "$ref": "#/components/schemas/Error" }),
            &resp,
            "error",
            &mut errs,
        );
        assert!(errs.is_empty(), "{errs:?}");
    }
}
