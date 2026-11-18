//! Regression test for a CI failure: on a fresh database, backends that use Neo4j for the
//! first time at the same moment all create the schema at once, and Neo4j aborts some of
//! those transactions with `Neo.TransientError.Transaction.DeadlockDetected`. The adapter
//! must retry them (Neo4j rolled them back) instead of failing the caller.
//!
//! This test drops BAiD's schema (constraints and indexes on `BaidMemory`/`BaidEntity`
//! only) to recreate that state; the next use recreates it. It is its own test binary so it
//! never runs alongside the other live tests. Skipped unless `BAID_TEST_NEO4J_URL` is set.

#![allow(clippy::unwrap_used)] // test file

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use adapters::{BoltBackend, BoltConfig};
use baid_core::{Backend, EntityId, Fact, TenantId};

const SCHEMA_OBJECTS: &[(&str, &str)] = &[
    ("CONSTRAINT", "baid_memory_id"),
    ("CONSTRAINT", "baid_memory_idempotency"),
    ("CONSTRAINT", "baid_entity_key"),
    ("INDEX", "baid_memory_tenant"),
    ("INDEX", "baid_memory_text"),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_first_use_on_a_fresh_database() {
    let Ok(url) = std::env::var("BAID_TEST_NEO4J_URL") else {
        eprintln!("skipped: set BAID_TEST_NEO4J_URL (see `just test-live`)");
        return;
    };
    let pw =
        std::env::var("BAID_TEST_NEO4J_PASSWORD").unwrap_or_else(|_| "baid-dev-password".into());
    let raw = neo4rs::Graph::new(url.clone(), "neo4j", pw.clone())
        .await
        .unwrap();
    for (kind, name) in SCHEMA_OBJECTS {
        raw.run(neo4rs::query(&format!("DROP {kind} {name} IF EXISTS")))
            .await
            .unwrap();
    }

    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let tenant = TenantId::new(format!("fresh{ms}")).unwrap();
    let writers: Vec<_> = (0..8)
        .map(|i| {
            let (url, pw, tenant) = (url.clone(), pw.clone(), tenant.clone());
            async move {
                // A separate backend each, as separate BAiD replicas would have.
                let b = BoltBackend::connect(BoltConfig {
                    url,
                    user: None,
                    password: Some(pw),
                    max_connections: 2,
                    call_timeout: Duration::from_secs(20),
                })
                .await
                .unwrap();
                b.write(
                    &tenant,
                    &Fact {
                        text: format!("fact {i} about acme"),
                        entities: vec![EntityId::new("acme").unwrap()],
                        labels: vec![],
                        as_of: None,
                        idempotency_key: None,
                    },
                )
                .await
            }
        })
        .collect();
    let results = futures::future::join_all(writers).await;
    let failures: Vec<_> = results.iter().filter(|r| r.is_err()).collect();
    assert!(failures.is_empty(), "first writes failed: {failures:?}");

    let mut stream = raw
        .execute(
            neo4rs::query("MATCH (m:BaidMemory {tenant: $t})-[:MENTIONS]->(e:BaidEntity {tenant: $t}) RETURN count(DISTINCT m) AS m, count(DISTINCT e) AS e")
                .param("t", tenant.as_str()),
        )
        .await
        .unwrap();
    let row = stream.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>("m").unwrap(), 8, "every fact stored once");
    assert_eq!(
        row.get::<i64>("e").unwrap(),
        1,
        "the shared entity merged into one node"
    );

    raw.run(
        neo4rs::query(
            "MATCH (n) WHERE (n:BaidMemory OR n:BaidEntity) AND n.tenant = $t DETACH DELETE n",
        )
        .param("t", tenant.as_str()),
    )
    .await
    .unwrap();
}
