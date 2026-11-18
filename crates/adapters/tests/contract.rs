//! The `Backend` contract: behaviour every adapter must have, run against each of them.
//!
//! Always run (no external services): the in-process memory backend, and the S3 adapter on
//! `object_store`'s in-memory store. With the local stack up, `just test-live` adds live Neo4j
//! (`BAID_TEST_NEO4J_URL`) and live S3 (`BAID_TEST_S3_URL`, SeaweedFS).
//!
//! A new adapter is done when it passes this file (CONTRIBUTING.md).

#![allow(clippy::unwrap_used)] // test file

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use adapters::{BoltBackend, BoltConfig, S3Backend, S3Config};
use baid_core::{Backend, BackendQuery, EntityId, Fact, Filters, MemoryBackend, TenantId};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use time::macros::datetime;
use time::OffsetDateTime;

enum Cleanup {
    None,
    Bolt(neo4rs::Graph),
    S3(Arc<dyn ObjectStore>, String),
}

struct Subject {
    name: &'static str,
    backend: Arc<dyn Backend>,
    cleanup: Cleanup,
}

impl Subject {
    async fn cleanup(&self, t: &TenantId) {
        match &self.cleanup {
            Cleanup::None => {}
            Cleanup::Bolt(g) => g
                .run(
                    neo4rs::query("MATCH (n) WHERE (n:BaidMemory OR n:BaidEntity) AND n.tenant = $t DETACH DELETE n")
                        .param("t", t.as_str()),
                )
                .await
                .unwrap(),
            Cleanup::S3(store, prefix) => {
                let root = Path::from(format!("{prefix}{t}/"));
                let objects: Vec<_> = store.list(Some(&root)).try_collect().await.unwrap();
                for o in objects {
                    store.delete(&o.location).await.unwrap();
                }
            }
        }
    }
}

const S3_LIVE_PREFIX: &str = "contract/";

async fn subjects() -> Vec<Subject> {
    let mut out = vec![
        Subject {
            name: "memory",
            backend: Arc::new(MemoryBackend::new()),
            cleanup: Cleanup::None,
        },
        Subject {
            name: "s3 (in-memory store)",
            backend: Arc::new(S3Backend::with_store(
                Arc::new(object_store::memory::InMemory::new()),
                "baid",
                Duration::from_secs(5),
                1_000,
            )),
            cleanup: Cleanup::None,
        },
    ];
    if let Ok(url) = std::env::var("BAID_TEST_NEO4J_URL") {
        let pw = std::env::var("BAID_TEST_NEO4J_PASSWORD")
            .unwrap_or_else(|_| "baid-dev-password".into());
        let backend = BoltBackend::connect(BoltConfig {
            url: url.clone(),
            user: None,
            password: Some(pw.clone()),
            max_connections: 4,
            call_timeout: Duration::from_secs(10),
        })
        .await
        .unwrap();
        out.push(Subject {
            name: "bolt (live)",
            backend: Arc::new(backend),
            cleanup: Cleanup::Bolt(neo4rs::Graph::new(url, "neo4j", pw).await.unwrap()),
        });
    }
    if let Ok(url) = std::env::var("BAID_TEST_S3_URL") {
        let key = std::env::var("BAID_TEST_S3_KEY").unwrap_or_else(|_| "baid".into());
        let secret =
            std::env::var("BAID_TEST_S3_SECRET").unwrap_or_else(|_| "baid-dev-password".into());
        let backend = S3Backend::connect(S3Config {
            url: url.clone(),
            access_key: Some(key.clone()),
            secret_key: Some(secret.clone()),
            call_timeout: Duration::from_secs(10),
            scan_limit: 1_000,
        })
        .unwrap();
        // A raw client on the same bucket, for cleanup.
        let (path, query) = url
            .trim_start_matches("s3://")
            .split_once('?')
            .unwrap_or((url.trim_start_matches("s3://"), ""));
        let bucket = path.split('/').next().unwrap();
        let endpoint = query
            .split('&')
            .find_map(|p| p.strip_prefix("endpoint="))
            .unwrap_or_default();
        let raw = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region("us-east-1")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_access_key_id(key)
            .with_secret_access_key(secret)
            .build()
            .unwrap();
        out.push(Subject {
            name: "s3 (live)",
            backend: Arc::new(backend),
            cleanup: Cleanup::S3(Arc::new(raw), S3_LIVE_PREFIX.into()),
        });
    }
    out
}

fn fresh_tenant() -> TenantId {
    static N: AtomicU32 = AtomicU32::new(0);
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    TenantId::new(format!("c{ms}-{}", N.fetch_add(1, Ordering::SeqCst))).unwrap()
}

fn ent(s: &str) -> EntityId {
    EntityId::new(s).unwrap()
}

fn fact(text: &str, entities: &[&str]) -> Fact {
    Fact {
        text: text.into(),
        entities: entities.iter().map(|e| ent(e)).collect(),
        labels: vec![],
        as_of: None,
        idempotency_key: None,
    }
}

fn search(text: &str, filters: Filters) -> BackendQuery {
    BackendQuery::Search {
        text: text.into(),
        filters,
        limit: 20,
    }
}

async fn texts(s: &Subject, t: &TenantId, q: BackendQuery) -> Vec<String> {
    let mut r = s.backend.query_batch(t, &[q]).await.unwrap();
    assert_eq!(r.len(), 1, "{}: one result per query", s.name);
    r.pop()
        .unwrap()
        .items
        .iter()
        .map(|i| i.text.to_string())
        .collect()
}

fn set(v: &[String]) -> BTreeSet<&str> {
    v.iter().map(String::as_str).collect()
}

#[tokio::test]
async fn written_facts_are_found_with_their_fields() {
    for s in subjects().await {
        let t = fresh_tenant();
        let when: OffsetDateTime = datetime!(2026-11-01 09:30 UTC);
        let mut f = fact("Acme renews on 1 March", &["acme", "contract-7"]);
        f.labels = vec!["contract".into()];
        f.as_of = Some(when);
        let ack = s.backend.write(&t, &f).await.unwrap();

        let mut r = s
            .backend
            .query_batch(&t, &[search("When does Acme renew?", Filters::default())])
            .await
            .unwrap();
        let items = r.pop().unwrap().items;
        assert_eq!(items.len(), 1, "{}", s.name);
        let i = &items[0];
        assert_eq!(i.id, ack.node, "{}: ack id is the item id", s.name);
        assert_eq!(&*i.text, "Acme renews on 1 March", "{}", s.name);
        assert!(i.score > 0.0, "{}", s.name);
        let ents: BTreeSet<&str> = i.entities.iter().map(EntityId::as_str).collect();
        assert_eq!(ents, ["acme", "contract-7"].into(), "{}", s.name);
        assert_eq!(i.as_of, Some(when), "{}", s.name);
        assert_eq!(i.source, s.backend.id(), "{}", s.name);
        s.cleanup(&t).await;
    }
}

#[tokio::test]
async fn tenants_never_see_each_other() {
    for s in subjects().await {
        let (a, b) = (fresh_tenant(), fresh_tenant());
        s.backend
            .write(&a, &fact("acme secret pricing", &["acme"]))
            .await
            .unwrap();
        assert!(
            texts(&s, &b, search("acme secret pricing", Filters::default()))
                .await
                .is_empty(),
            "{}",
            s.name
        );
        let by_entity = Filters {
            entity_ids: vec![ent("acme")],
            ..Filters::default()
        };
        assert!(
            texts(&s, &b, search("secret", by_entity)).await.is_empty(),
            "{}",
            s.name
        );
        let n = BackendQuery::Neighbourhood {
            entity: ent("acme"),
            hops: 2,
            limit: 20,
        };
        assert!(texts(&s, &b, n).await.is_empty(), "{}", s.name);
        assert!(
            texts(&s, &b, BackendQuery::Entity { id: ent("acme") })
                .await
                .is_empty(),
            "{}",
            s.name
        );
        s.cleanup(&a).await;
        s.cleanup(&b).await;
    }
}

#[tokio::test]
async fn filters_narrow_a_search() {
    for s in subjects().await {
        let t = fresh_tenant();
        let mut a = fact("renewal for acme", &["acme"]);
        a.labels = vec!["contract".into()];
        a.as_of = Some(datetime!(2026-01-01 0:00 UTC));
        let mut g = fact("renewal for globex", &["globex"]);
        g.labels = vec!["note".into()];
        g.as_of = Some(datetime!(2026-10-01 0:00 UTC));
        s.backend.write(&t, &a).await.unwrap();
        s.backend.write(&t, &g).await.unwrap();

        let by_entity = Filters {
            entity_ids: vec![ent("globex")],
            ..Filters::default()
        };
        assert_eq!(
            texts(&s, &t, search("renewal", by_entity)).await,
            ["renewal for globex"],
            "{}",
            s.name
        );
        let by_label = Filters {
            labels: vec!["contract".into()],
            ..Filters::default()
        };
        assert_eq!(
            texts(&s, &t, search("renewal", by_label)).await,
            ["renewal for acme"],
            "{}",
            s.name
        );
        let by_time = Filters {
            since: Some(datetime!(2026-06-01 0:00 UTC)),
            ..Filters::default()
        };
        assert_eq!(
            texts(&s, &t, search("renewal", by_time)).await,
            ["renewal for globex"],
            "{}",
            s.name
        );
        s.cleanup(&t).await;
    }
}

#[tokio::test]
async fn neighbourhoods_rank_direct_mentions_first() {
    for s in subjects().await {
        let t = fresh_tenant();
        s.backend
            .write(
                &t,
                &fact("acme bought the gold plan", &["acme", "gold-plan"]),
            )
            .await
            .unwrap();
        s.backend
            .write(
                &t,
                &fact("the gold plan includes support", &["gold-plan", "support"]),
            )
            .await
            .unwrap();
        s.backend
            .write(&t, &fact("unrelated note", &["other"]))
            .await
            .unwrap();

        let hop1 = texts(
            &s,
            &t,
            BackendQuery::Neighbourhood {
                entity: ent("acme"),
                hops: 1,
                limit: 20,
            },
        )
        .await;
        assert_eq!(hop1, ["acme bought the gold plan"], "{}", s.name);

        let mut r = s
            .backend
            .query_batch(
                &t,
                &[BackendQuery::Neighbourhood {
                    entity: ent("acme"),
                    hops: 2,
                    limit: 20,
                }],
            )
            .await
            .unwrap();
        let items = r.pop().unwrap().items;
        let got: Vec<&str> = items.iter().map(|i| &*i.text).collect();
        assert_eq!(
            got,
            [
                "acme bought the gold plan",
                "the gold plan includes support"
            ],
            "{}",
            s.name
        );
        assert!(items[0].score > items[1].score, "{}", s.name);
        s.cleanup(&t).await;
    }
}

#[tokio::test]
async fn entity_query_returns_the_newest_mention() {
    for s in subjects().await {
        let t = fresh_tenant();
        s.backend
            .write(&t, &fact("acme signed", &["acme"]))
            .await
            .unwrap();
        s.backend
            .write(&t, &fact("acme renewed", &["acme"]))
            .await
            .unwrap();
        assert_eq!(
            texts(&s, &t, BackendQuery::Entity { id: ent("acme") }).await,
            ["acme renewed"],
            "{}",
            s.name
        );
        s.cleanup(&t).await;
    }
}

#[tokio::test]
async fn idempotent_writes_store_once() {
    for s in subjects().await {
        let t = fresh_tenant();
        let mut f = fact("acme signed", &["acme"]);
        f.idempotency_key = Some("req-1".into());
        let a = s.backend.write(&t, &f).await.unwrap();
        let b = s.backend.write(&t, &f).await.unwrap();
        assert_eq!(a.node, b.node, "{}", s.name);
        assert_eq!(
            texts(&s, &t, search("acme signed", Filters::default()))
                .await
                .len(),
            1,
            "{}",
            s.name
        );
        s.cleanup(&t).await;
    }
}

#[tokio::test]
async fn batches_answer_in_order() {
    for s in subjects().await {
        let t = fresh_tenant();
        s.backend
            .write(&t, &fact("alpha fact", &["x"]))
            .await
            .unwrap();
        s.backend
            .write(&t, &fact("beta fact", &["y"]))
            .await
            .unwrap();
        let qs = [
            search("beta", Filters::default()),
            BackendQuery::Neighbourhood {
                entity: ent("x"),
                hops: 1,
                limit: 5,
            },
            search("nothing matches this", Filters::default()),
        ];
        let r = s.backend.query_batch(&t, &qs).await.unwrap();
        assert_eq!(r.len(), 3, "{}", s.name);
        assert_eq!(&*r[0].items[0].text, "beta fact", "{}", s.name);
        assert_eq!(&*r[1].items[0].text, "alpha fact", "{}", s.name);
        assert!(r[2].items.is_empty(), "{}", s.name);
        s.cleanup(&t).await;
    }
}

#[tokio::test]
async fn odd_text_and_entity_names_are_just_data() {
    for s in subjects().await {
        let t = fresh_tenant();
        let weird = "../../other-tenant/items";
        s.backend
            .write(&t, &fact("path-like entity", &[weird, "Ünïcode / spaces"]))
            .await
            .unwrap();
        let n = texts(
            &s,
            &t,
            BackendQuery::Neighbourhood {
                entity: ent(weird),
                hops: 1,
                limit: 5,
            },
        )
        .await;
        assert_eq!(n, ["path-like entity"], "{}", s.name);
        let u = texts(
            &s,
            &t,
            BackendQuery::Entity {
                id: ent("Ünïcode / spaces"),
            },
        )
        .await;
        assert_eq!(u, ["path-like entity"], "{}", s.name);
        for q in [
            r#"a AND b* name:x "q" (x)~2 \ OR"#,
            "*",
            "?!",
            "acme) OR (*",
        ] {
            let r = s
                .backend
                .query_batch(&t, &[search(q, Filters::default())])
                .await;
            assert!(r.is_ok(), "{}: {q:?} -> {r:?}", s.name);
        }
        let other = fresh_tenant();
        assert!(
            texts(&s, &other, search("path-like entity", Filters::default()))
                .await
                .is_empty(),
            "{}",
            s.name
        );
        s.cleanup(&t).await;
    }
}

#[tokio::test]
async fn every_subject_ran() {
    let names: Vec<&str> = subjects().await.iter().map(|s| s.name).collect();
    eprintln!("contract subjects: {names:?}");
    assert!(names.len() >= 2);
    let _ = set(&[]);
}
