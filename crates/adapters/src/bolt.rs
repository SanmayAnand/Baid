//! Neo4j (or any Bolt server) as a memory backend.
//!
//! # Graph model
//!
//! BAiD keeps its memory in its own namespaced labels, so it can share a database with
//! other data:
//!
//! ```text
//! (:BaidMemory {id, tenant, text, labels, as_of, created_at, version, idempotency_key})
//!     -[:MENTIONS]->
//! (:BaidEntity {tenant, id})
//! ```
//!
//! Every query filters on `tenant`; entity nodes are keyed by `(tenant, id)`, so a
//! `MENTIONS` path never crosses tenants.
//!
//! # Queries
//!
//! - **Search**: the `baid_memory_text` full-text index, with the user's words escaped
//!   (each becomes a quoted term), then tenant, label, entity and `since` filters.
//! - **Neighbourhood**: memories mentioning the entity (hop 1, score 1.0), plus at `hops >= 2`
//!   memories mentioning an entity co-mentioned with it (score 0.5).
//! - **Entity**: the most recent memory mentioning the entity.
//! - **Write**: `CREATE` (or `MERGE` on `(tenant, idempotency_key)` when a key is given, so a
//!   retried write is applied once), then `MERGE` the entities and `MENTIONS` edges.
//!
//! The schema (constraints and indexes) is created on first use with `IF NOT EXISTS`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use baid_core::{
    AdapterId, Backend, BackendError, BackendQuery, BackendResult, Capabilities, EntityId, Fact,
    Health, MemoryItem, NodeId, TenantId, WriteAck,
};
use neo4rs::{query, ConfigBuilder, Graph, Neo4jClientErrorKind, Neo4jErrorKind, Query};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::OnceCell;
use tracing::debug;

/// Statements that create the schema. Idempotent.
const SCHEMA: &[&str] = &[
    "CREATE CONSTRAINT baid_memory_id IF NOT EXISTS FOR (m:BaidMemory) REQUIRE m.id IS UNIQUE",
    "CREATE CONSTRAINT baid_memory_idempotency IF NOT EXISTS FOR (m:BaidMemory) REQUIRE (m.tenant, m.idempotency_key) IS UNIQUE",
    "CREATE CONSTRAINT baid_entity_key IF NOT EXISTS FOR (e:BaidEntity) REQUIRE (e.tenant, e.id) IS UNIQUE",
    "CREATE INDEX baid_memory_tenant IF NOT EXISTS FOR (m:BaidMemory) ON (m.tenant)",
    "CREATE FULLTEXT INDEX baid_memory_text IF NOT EXISTS FOR (m:BaidMemory) ON EACH [m.text]",
];

/// Columns every read query returns, after `m` (a memory) and `score` are bound.
const RETURN_ITEM: &str = "
    OPTIONAL MATCH (m)-[:MENTIONS]->(x:BaidEntity)
    WITH m, score, collect(x.id) AS entities
    RETURN m.id AS id, m.text AS text, score, entities,
           CASE WHEN m.as_of IS NULL THEN null ELSE m.as_of.epochMillis END AS as_of_ms
    ORDER BY score DESC, m.created_at DESC, id";

const SEARCH: &str = "
    CALL db.index.fulltext.queryNodes('baid_memory_text', $q) YIELD node AS m, score
    WHERE m.tenant = $tenant
      AND ($since IS NULL OR (m.as_of IS NOT NULL AND m.as_of >= datetime($since)))
      AND (size($labels) = 0 OR any(l IN coalesce(m.labels, []) WHERE l IN $labels))
      AND (size($entities) = 0 OR EXISTS {
            MATCH (m)-[:MENTIONS]->(f:BaidEntity)
            WHERE f.tenant = $tenant AND f.id IN $entities })
    WITH m, score ORDER BY score DESC, m.created_at DESC LIMIT $limit";

const NEIGHBOURS_1: &str = "
    MATCH (:BaidEntity {tenant: $tenant, id: $entity})<-[:MENTIONS]-(m:BaidMemory)
    WHERE m.tenant = $tenant
    WITH DISTINCT m, 1.0 AS score ORDER BY m.created_at DESC LIMIT $limit";

const NEIGHBOURS_2: &str = "
    MATCH (e:BaidEntity {tenant: $tenant, id: $entity})
    CALL (e) {
        MATCH (e)<-[:MENTIONS]-(m:BaidMemory)
        RETURN m, 1.0 AS score
      UNION
        MATCH (e)<-[:MENTIONS]-(:BaidMemory)-[:MENTIONS]->(n:BaidEntity)<-[:MENTIONS]-(m:BaidMemory)
        WHERE n.tenant = $tenant AND n <> e
        RETURN m, 0.5 AS score
    }
    WITH m, max(score) AS score WHERE m.tenant = $tenant
    WITH m, score ORDER BY score DESC, m.created_at DESC LIMIT $limit";

const WRITE_TAIL: &str = "
    FOREACH (eid IN $entities |
        MERGE (e:BaidEntity {tenant: $tenant, id: eid})
        MERGE (m)-[:MENTIONS]->(e))
    RETURN m.id AS id, m.version AS version";

const WRITE_NEW: &str = "
    CREATE (m:BaidMemory {
        id: randomUUID(), tenant: $tenant, text: $text, labels: $labels,
        as_of: CASE WHEN $as_of IS NULL THEN null ELSE datetime($as_of) END,
        created_at: datetime(), version: 1 })";

const WRITE_IDEMPOTENT: &str = "
    MERGE (m:BaidMemory {tenant: $tenant, idempotency_key: $idempotency_key})
    ON CREATE SET m.id = randomUUID(), m.text = $text, m.labels = $labels,
        m.as_of = CASE WHEN $as_of IS NULL THEN null ELSE datetime($as_of) END,
        m.created_at = datetime(), m.version = 1";

/// A Bolt-connected memory backend.
pub struct BoltBackend {
    graph: Arc<Graph>,
    schema: OnceCell<()>,
    call_timeout: Duration,
}

/// Connection settings for [`BoltBackend`].
#[derive(Clone, Debug)]
pub struct BoltConfig {
    /// `bolt://host:7687` or `neo4j://host:7687`.
    pub url: String,
    /// User name; `neo4j` when not given.
    pub user: Option<String>,
    /// Password; empty when the server has auth disabled.
    pub password: Option<String>,
    /// Pool size.
    pub max_connections: usize,
    /// Upper bound on one driver call, including the driver's own retries. neo4rs 0.8
    /// retries retryable errors internally for up to 60 s and cannot be configured, so
    /// without this a dead server would hold every caller for a minute.
    pub call_timeout: Duration,
}

impl BoltBackend {
    /// Creates the backend. The connection pool is lazy: nothing is contacted until the
    /// first call, so BAiD can start while Neo4j is down.
    pub async fn connect(cfg: BoltConfig) -> Result<Self, BackendError> {
        if !(cfg.url.starts_with("bolt://") || cfg.url.starts_with("neo4j://")) {
            return Err(BackendError::Rejected(format!(
                "adapter_url must start with bolt:// or neo4j://, got {}",
                cfg.url
            )));
        }
        let config = ConfigBuilder::default()
            .uri(cfg.url.as_str())
            .user(cfg.user.as_deref().unwrap_or("neo4j"))
            .password(cfg.password.as_deref().unwrap_or(""))
            .max_connections(cfg.max_connections.max(1))
            .fetch_size(500)
            .build()
            .map_err(map_err)?;
        let graph = Graph::connect(config).await.map_err(map_err)?;
        Ok(BoltBackend {
            graph: Arc::new(graph),
            schema: OnceCell::new(),
            call_timeout: cfg.call_timeout,
        })
    }

    /// Runs `fut` under [`BoltConfig::call_timeout`].
    async fn bounded<T>(
        &self,
        fut: impl Future<Output = Result<T, BackendError>>,
    ) -> Result<T, BackendError> {
        tokio::time::timeout(self.call_timeout, fut)
            .await
            .map_err(|_| BackendError::Timeout(self.call_timeout))?
    }

    /// Runs `q` in an explicit transaction, bounded by the call timeout, and returns every
    /// row. Explicit transactions are not retried by the driver (unlike `Graph::execute`,
    /// which retries even authentication failures for up to 60 s), so errors come back at
    /// once and [`crate::Resilient`] is the only retry layer, with correct classification.
    async fn tx_rows(&self, q: Query) -> Result<Vec<neo4rs::Row>, BackendError> {
        self.bounded(async {
            let mut txn = self.graph.start_txn().await.map_err(map_err)?;
            let mut stream = txn.execute(q).await.map_err(map_err)?;
            let mut rows = Vec::new();
            while let Some(row) = stream.next(txn.handle()).await.map_err(map_err)? {
                rows.push(row);
            }
            txn.commit().await.map_err(map_err)?;
            Ok(rows)
        })
        .await
    }

    /// Like [`Self::tx_rows`] for statements whose rows are not needed.
    async fn tx_run(&self, q: Query) -> Result<(), BackendError> {
        self.bounded(async {
            let mut txn = self.graph.start_txn().await.map_err(map_err)?;
            txn.run(q).await.map_err(map_err)?;
            txn.commit().await.map_err(map_err)
        })
        .await
    }

    async fn ensure_schema(&self) -> Result<(), BackendError> {
        self.schema
            .get_or_try_init(|| async {
                for stmt in SCHEMA {
                    match self.tx_run(query(stmt)).await {
                        Ok(()) => {}
                        // Another process created it between our IF NOT EXISTS check and
                        // the create: the schema is in place, which is all we need.
                        Err(BackendError::Rejected(m)) if m.contains("AlreadyExists") => {}
                        Err(e) => return Err(e),
                    }
                }
                debug!("bolt schema ready");
                Ok::<(), BackendError>(())
            })
            .await
            .map(|_| ())
    }

    async fn rows(&self, q: Query) -> Result<Vec<MemoryItem>, BackendError> {
        let source = self.id();
        self.tx_rows(q)
            .await?
            .iter()
            .map(|row| item_from_row(row, &source))
            .collect()
    }

    async fn run_one(
        &self,
        tenant: &TenantId,
        q: &BackendQuery,
    ) -> Result<BackendResult, BackendError> {
        let items = match q {
            BackendQuery::Search {
                text,
                filters,
                limit,
            } => {
                let Some(lucene) = lucene_query(text) else {
                    return Ok(BackendResult::default());
                };
                let since = filters.since.map(rfc3339).transpose()?;
                let entities: Vec<String> =
                    filters.entity_ids.iter().map(|e| e.to_string()).collect();
                self.rows(
                    query(&format!("{SEARCH}{RETURN_ITEM}"))
                        .param("q", lucene)
                        .param("tenant", tenant.as_str())
                        .param("since", since)
                        .param("labels", filters.labels.clone())
                        .param("entities", entities)
                        .param("limit", i64::from(*limit)),
                )
                .await?
            }
            BackendQuery::Neighbourhood {
                entity,
                hops,
                limit,
            } => {
                let cypher = if *hops >= 2 {
                    NEIGHBOURS_2
                } else {
                    NEIGHBOURS_1
                };
                self.rows(
                    query(&format!("{cypher}{RETURN_ITEM}"))
                        .param("tenant", tenant.as_str())
                        .param("entity", entity.as_str())
                        .param("limit", i64::from(*limit)),
                )
                .await?
            }
            BackendQuery::Entity { id } => {
                self.rows(
                    query(&format!("{NEIGHBOURS_1}{RETURN_ITEM}"))
                        .param("tenant", tenant.as_str())
                        .param("entity", id.as_str())
                        .param("limit", 1_i64),
                )
                .await?
            }
        };
        Ok(BackendResult { items })
    }
}

#[async_trait]
impl Backend for BoltBackend {
    fn id(&self) -> AdapterId {
        AdapterId::new("bolt").unwrap_or_else(|_| unreachable!("valid id"))
    }

    fn capabilities(&self) -> Capabilities {
        // One query per round trip for now; UNWIND batching comes with plan T27.
        Capabilities {
            native_batch: false,
            max_batch: 16,
            neighbourhood: true,
            change_feed: false,
            versions: true,
        }
    }

    async fn query_batch(
        &self,
        tenant: &TenantId,
        queries: &[BackendQuery],
    ) -> Result<Vec<BackendResult>, BackendError> {
        self.ensure_schema().await?;
        let mut out = Vec::with_capacity(queries.len());
        for q in queries {
            out.push(self.run_one(tenant, q).await?);
        }
        Ok(out)
    }

    async fn write(&self, tenant: &TenantId, fact: &Fact) -> Result<WriteAck, BackendError> {
        self.ensure_schema().await?;
        let head = if fact.idempotency_key.is_some() {
            WRITE_IDEMPOTENT
        } else {
            WRITE_NEW
        };
        let entities: Vec<String> = fact.entities.iter().map(|e| e.to_string()).collect();
        let q = query(&format!("{head}{WRITE_TAIL}"))
            .param("tenant", tenant.as_str())
            .param("text", fact.text.as_str())
            .param("labels", fact.labels.clone())
            .param("as_of", fact.as_of.map(rfc3339).transpose()?)
            .param("idempotency_key", fact.idempotency_key.clone())
            .param("entities", entities);
        let row = self
            .tx_rows(q)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| BackendError::Unavailable("write returned no row".into()))?;
        let id: String = row.get("id").map_err(de_err)?;
        let version: Option<i64> = row.get("version").map_err(de_err)?;
        Ok(WriteAck {
            node: NodeId::new(&id).map_err(|e| BackendError::Rejected(e.to_string()))?,
            entities: fact.entities.clone(),
            version: version.and_then(|v| u64::try_from(v).ok()),
        })
    }

    async fn health(&self) -> Health {
        match self.tx_run(query("RETURN 1")).await {
            Ok(()) => Health::Up,
            Err(e) => Health::Down {
                reason: e.to_string(),
            },
        }
    }
}

fn rfc3339(t: OffsetDateTime) -> Result<String, BackendError> {
    t.format(&Rfc3339)
        .map_err(|e| BackendError::Rejected(format!("bad timestamp: {e}")))
}

fn de_err(e: neo4rs::DeError) -> BackendError {
    BackendError::Rejected(format!("unexpected row shape: {e}"))
}

fn item_from_row(row: &neo4rs::Row, source: &AdapterId) -> Result<MemoryItem, BackendError> {
    let id: String = row.get("id").map_err(de_err)?;
    let text: String = row.get("text").map_err(de_err)?;
    let score: f64 = row.get("score").map_err(de_err)?;
    let entities: Vec<String> = row.get("entities").map_err(de_err)?;
    let as_of_ms: Option<i64> = row.get("as_of_ms").map_err(de_err)?;
    Ok(MemoryItem {
        id: NodeId::new(&id).map_err(|e| BackendError::Rejected(e.to_string()))?,
        text: Arc::from(text),
        score: score as f32,
        tokens: 0,
        entities: entities
            .iter()
            .filter_map(|e| EntityId::new(e).ok())
            .collect(),
        as_of: as_of_ms.and_then(|ms| {
            OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000).ok()
        }),
        source: source.clone(),
    })
}

/// Turns free text into a safe Lucene query: each normalised word becomes a quoted term
/// (with `\` and `"` escaped), joined by OR. Returns `None` if no words remain.
///
/// Quoting every term means no user input is ever parsed as Lucene syntax (`AND`, `*`, `~`,
/// `field:`), so a question cannot break the query or widen it.
pub fn lucene_query(text: &str) -> Option<String> {
    let norm = baid_core::normalise(text);
    let terms: Vec<String> = norm
        .split(' ')
        .map(|w| w.trim_matches(|c: char| !c.is_alphanumeric()))
        .filter(|w| !w.is_empty())
        .map(|w| format!("\"{}\"", w.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect();
    (!terms.is_empty()).then(|| terms.join(" OR "))
}

/// Maps driver errors onto the core error kinds. Connection trouble and transient server
/// errors are retryable; client errors (bad query, auth) are not.
pub fn map_err(e: neo4rs::Error) -> BackendError {
    use neo4rs::Error as E;
    match e {
        E::IOError { .. } | E::ConnectionError => BackendError::Unavailable(e.to_string()),
        E::AuthenticationError(m) => BackendError::Rejected(format!("authentication failed: {m}")),
        E::Neo4j(ref n) => match n.kind() {
            Neo4jErrorKind::Transient | Neo4jErrorKind::Database => {
                BackendError::Unavailable(e.to_string())
            }
            Neo4jErrorKind::Client(
                Neo4jClientErrorKind::SessionExpired | Neo4jClientErrorKind::TransactionTerminated,
            ) => BackendError::Unavailable(e.to_string()),
            _ => BackendError::Rejected(e.to_string()),
        },
        E::UrlParseError(_) | E::UnsupportedScheme(_) | E::InvalidDnsName(_) | E::InvalidConfig => {
            BackendError::Rejected(e.to_string())
        }
        other => BackendError::Unavailable(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lucene_terms_are_quoted_and_escaped() {
        assert_eq!(
            lucene_query("When does Acme renew?").as_deref(),
            Some("\"when\" OR \"does\" OR \"acme\" OR \"renew\"")
        );
        // Operators and syntax are just words now.
        assert_eq!(
            lucene_query("a AND b* name:x \"quoted\" (x)~2").as_deref(),
            Some("\"a\" OR \"and\" OR \"b\" OR \"name:x\" OR \"quoted\" OR \"x)~2\"")
        );
        assert_eq!(lucene_query("  ?! * "), None);
        assert_eq!(
            lucene_query(r#"back\slash"#).as_deref(),
            Some(r#""back\\slash""#)
        );
    }

    #[test]
    fn rejects_non_bolt_urls() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let r = rt.block_on(BoltBackend::connect(BoltConfig {
            url: "http://x:7474".into(),
            user: None,
            password: None,
            max_connections: 4,
            call_timeout: std::time::Duration::from_secs(5),
        }));
        assert!(matches!(r, Err(BackendError::Rejected(_))));
    }
}
