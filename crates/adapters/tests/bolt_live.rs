//! The Bolt adapter against a real Neo4j.
//!
//! Skipped unless `BAID_TEST_NEO4J_URL` is set. With the local stack running (`just up`):
//!
//! ```text
//! just test-live
//! ```
//!
//! Each test uses a fresh tenant and deletes its nodes afterwards.

#![allow(clippy::unwrap_used)] // test file

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use adapters::{BoltBackend, BoltConfig};
use baid_core::{
    Backend, BackendError, BackendQuery, BackendResult, EntityId, Fact, Filters, Health, TenantId,
};
use time::macros::datetime;

struct Live {
    backend: BoltBackend,
    raw: neo4rs::Graph,
    tenant: TenantId,
}

fn env() -> Option<(String, String)> {
    let url = std::env::var("BAID_TEST_NEO4J_URL").ok()?;
    let pw =
        std::env::var("BAID_TEST_NEO4J_PASSWORD").unwrap_or_else(|_| "baid-dev-password".into());
    Some((url, pw))
}

async fn live() -> Option<Live> {
    let Some((url, pw)) = env() else {
        eprintln!("skipped: set BAID_TEST_NEO4J_URL (see `just test-live`)");
        return None;
    };
    static N: AtomicU32 = AtomicU32::new(0);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let tenant = TenantId::new(format!("t{stamp}-{}", N.fetch_add(1, Ordering::SeqCst))).unwrap();
    let backend = BoltBackend::connect(BoltConfig {
        url: url.clone(),
        user: None,
        password: Some(pw.clone()),
        max_connections: 4,
        call_timeout: std::time::Duration::from_secs(5),
    })
    .await
    .unwrap();
    let raw = neo4rs::Graph::new(url, "neo4j", pw).await.unwrap();
    Some(Live {
        backend,
        raw,
        tenant,
    })
}

impl Live {
    async fn cleanup(self) {
        self.raw
            .run(
                neo4rs::query("MATCH (n) WHERE (n:BaidMemory OR n:BaidEntity) AND n.tenant = $t DETACH DELETE n")
                    .param("t", self.tenant.as_str()),
            )
            .await
            .unwrap();
    }

    async fn write(&self, text: &str, entities: &[&str]) -> String {
        self.write_full(text, entities, &[], None, None).await
    }

    async fn write_full(
        &self,
        text: &str,
        entities: &[&str],
        labels: &[&str],
        as_of: Option<time::OffsetDateTime>,
        key: Option<&str>,
    ) -> String {
        let ack = self
            .backend
            .write(
                &self.tenant,
                &Fact {
                    text: text.into(),
                    entities: entities.iter().map(|e| EntityId::new(e).unwrap()).collect(),
                    labels: labels.iter().map(|l| l.to_string()).collect(),
                    as_of,
                    idempotency_key: key.map(String::from),
                },
            )
            .await
            .unwrap();
        ack.node.to_string()
    }

    async fn one(&self, tenant: &TenantId, q: BackendQuery) -> BackendResult {
        let mut r = self.backend.query_batch(tenant, &[q]).await.unwrap();
        assert_eq!(r.len(), 1);
        r.pop().unwrap()
    }
}

fn search(text: &str, filters: Filters) -> BackendQuery {
    BackendQuery::Search {
        text: text.into(),
        filters,
        limit: 20,
    }
}

fn texts(r: &BackendResult) -> Vec<String> {
    r.items.iter().map(|i| i.text.to_string()).collect()
}

fn ent(s: &str) -> EntityId {
    EntityId::new(s).unwrap()
}

#[tokio::test]
async fn write_then_search() {
    let Some(l) = live().await else { return };
    let when = datetime!(2026-11-01 09:30 UTC);
    l.write_full(
        "Acme renews on 1 March",
        &["acme", "contract-7"],
        &["contract"],
        Some(when),
        None,
    )
    .await;
    let r = l
        .one(
            &l.tenant,
            search("When does Acme renew?", Filters::default()),
        )
        .await;
    assert_eq!(texts(&r), ["Acme renews on 1 March"]);
    let item = &r.items[0];
    assert!(item.score > 0.0);
    let mut ents: Vec<&str> = item.entities.iter().map(EntityId::as_str).collect();
    ents.sort();
    assert_eq!(ents, ["acme", "contract-7"]);
    assert_eq!(item.as_of, Some(when));
    assert_eq!(item.source.as_str(), "bolt");
    l.cleanup().await;
}

#[tokio::test]
async fn tenants_never_see_each_other() {
    let Some(a) = live().await else { return };
    let Some(b) = live().await else { return };
    a.write("acme secret pricing", &["acme"]).await;
    let s = b
        .one(&b.tenant, search("acme secret pricing", Filters::default()))
        .await;
    assert!(s.items.is_empty(), "search leaked across tenants");
    let n = b
        .one(
            &b.tenant,
            BackendQuery::Neighbourhood {
                entity: ent("acme"),
                hops: 2,
                limit: 20,
            },
        )
        .await;
    assert!(n.items.is_empty(), "neighbourhood leaked across tenants");
    a.cleanup().await;
    b.cleanup().await;
}

#[tokio::test]
async fn filters_narrow_the_search() {
    let Some(l) = live().await else { return };
    l.write_full(
        "renewal for acme",
        &["acme"],
        &["contract"],
        Some(datetime!(2026-01-01 0:00 UTC)),
        None,
    )
    .await;
    l.write_full(
        "renewal for globex",
        &["globex"],
        &["note"],
        Some(datetime!(2026-10-01 0:00 UTC)),
        None,
    )
    .await;
    let by_entity = Filters {
        entity_ids: vec![ent("globex")],
        ..Filters::default()
    };
    assert_eq!(
        texts(&l.one(&l.tenant, search("renewal", by_entity)).await),
        ["renewal for globex"]
    );
    let by_label = Filters {
        labels: vec!["contract".into()],
        ..Filters::default()
    };
    assert_eq!(
        texts(&l.one(&l.tenant, search("renewal", by_label)).await),
        ["renewal for acme"]
    );
    let by_time = Filters {
        since: Some(datetime!(2026-06-01 0:00 UTC)),
        ..Filters::default()
    };
    assert_eq!(
        texts(&l.one(&l.tenant, search("renewal", by_time)).await),
        ["renewal for globex"]
    );
    l.cleanup().await;
}

#[tokio::test]
async fn neighbourhoods_and_entities() {
    let Some(l) = live().await else { return };
    l.write("acme bought the gold plan", &["acme", "gold-plan"])
        .await;
    l.write("the gold plan includes support", &["gold-plan", "support"])
        .await;
    l.write("unrelated note", &["other"]).await;

    let hop1 = l
        .one(
            &l.tenant,
            BackendQuery::Neighbourhood {
                entity: ent("acme"),
                hops: 1,
                limit: 20,
            },
        )
        .await;
    assert_eq!(texts(&hop1), ["acme bought the gold plan"]);

    let hop2 = l
        .one(
            &l.tenant,
            BackendQuery::Neighbourhood {
                entity: ent("acme"),
                hops: 2,
                limit: 20,
            },
        )
        .await;
    assert_eq!(
        texts(&hop2),
        [
            "acme bought the gold plan",
            "the gold plan includes support"
        ]
    );
    assert!(
        hop2.items[0].score > hop2.items[1].score,
        "direct mentions rank first"
    );

    l.write("acme renewed", &["acme"]).await;
    let e = l
        .one(&l.tenant, BackendQuery::Entity { id: ent("acme") })
        .await;
    assert_eq!(texts(&e), ["acme renewed"], "most recent mention");
    l.cleanup().await;
}

#[tokio::test]
async fn idempotent_writes_store_once() {
    let Some(l) = live().await else { return };
    let a = l
        .write_full("acme signed", &["acme"], &[], None, Some("req-1"))
        .await;
    let b = l
        .write_full("acme signed", &["acme"], &[], None, Some("req-1"))
        .await;
    assert_eq!(a, b);
    let r = l
        .one(&l.tenant, search("acme signed", Filters::default()))
        .await;
    assert_eq!(r.items.len(), 1);
    l.cleanup().await;
}

#[tokio::test]
async fn hostile_query_text_is_just_words() {
    let Some(l) = live().await else { return };
    l.write("plain fact about acme", &["acme"]).await;
    for q in [
        r#"a AND b* name:x "q" (x)~2 \ OR"#,
        "*",
        "?!",
        "acme) OR (*",
        "acme\" OR \"x",
    ] {
        let r = l
            .backend
            .query_batch(&l.tenant, &[search(q, Filters::default())])
            .await;
        assert!(r.is_ok(), "{q:?} -> {r:?}");
    }
    l.cleanup().await;
}

#[tokio::test]
async fn health_and_unreachable_servers() {
    let Some(l) = live().await else { return };
    assert_eq!(l.backend.health().await, Health::Up);
    let dead = BoltBackend::connect(BoltConfig {
        url: "bolt://127.0.0.1:1".into(),
        user: None,
        password: Some("x".into()),
        max_connections: 1,
        call_timeout: std::time::Duration::from_secs(5),
    })
    .await
    .unwrap();
    // Each call must fail on its own, well inside the 5 s call timeout: the driver's 60 s
    // retry loop must not be in the way (Resilient does the retrying). A refused localhost
    // connection takes about 2 s on Windows (SYN retries), so the bound is per call.
    let per_call = std::time::Duration::from_secs(3);
    let started = std::time::Instant::now();
    assert!(matches!(dead.health().await, Health::Down { .. }));
    assert!(
        started.elapsed() < per_call,
        "health took {:?}",
        started.elapsed()
    );
    let started = std::time::Instant::now();
    let err = dead
        .query_batch(&l.tenant, &[search("x", Filters::default())])
        .await
        .unwrap_err();
    assert!(err.is_retryable(), "{err:?}");
    assert!(matches!(err, BackendError::Unavailable(_)), "{err:?}");
    assert!(
        started.elapsed() < per_call,
        "query took {:?}",
        started.elapsed()
    );
    l.cleanup().await;
}

#[tokio::test]
async fn wrong_password_is_rejected_not_retried() {
    let Some((url, _)) = env() else { return };
    let b = BoltBackend::connect(BoltConfig {
        url,
        user: None,
        password: Some("definitely-wrong".into()),
        max_connections: 1,
        call_timeout: std::time::Duration::from_secs(5),
    })
    .await
    .unwrap();
    let started = std::time::Instant::now();
    let err = b
        .query_batch(
            &TenantId::new("x").unwrap(),
            &[search("x", Filters::default())],
        )
        .await
        .unwrap_err();
    assert!(
        !err.is_retryable(),
        "auth failures must not be retried: {err:?}"
    );
    assert!(err.to_string().contains("authentication"), "{err}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
}
