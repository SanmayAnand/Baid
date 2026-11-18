//! S3 (or any S3-compatible store: AWS, SeaweedFS, MinIO, Ceph) as a memory backend.
//!
//! # Layout
//!
//! Everything for a tenant lives under `{prefix}{tenant}/`. Tenant ids are restricted to
//! `[A-Za-z0-9_-]`, so a tenant can never address another tenant's prefix.
//!
//! ```text
//! {prefix}{tenant}/items/{id}.json            one memory (JSON)
//! {prefix}{tenant}/entities/{h(entity)}/{id}  empty marker: memory `id` mentions `entity`
//! {prefix}{tenant}/idem/{h(key)}              idempotency marker; body = memory id
//! ```
//!
//! `h` is blake3, hex: entity names and idempotency keys are arbitrary client text, so they
//! never appear in object keys (no `../`, no odd characters, fixed length).
//!
//! Memory ids start with an *inverted* millisecond timestamp, so S3's ascending key listing
//! returns the newest memory first.
//!
//! # Queries
//!
//! S3 has no query engine, so:
//! - **Entity-filtered search, neighbourhood and entity** queries read the entity markers,
//!   then fetch the listed memories. Cost grows with the entity, not the tenant.
//! - **Unfiltered search** scans only the newest [`S3Config::scan_limit`] memories and scores
//!   them by word overlap. Older memories are reachable through entity queries. A real text
//!   index belongs in front of S3, not inside this adapter.
//!
//! # Writes
//!
//! The item is written first, then its entity markers, then the idempotency marker. So a
//! marker always points at an existing item. A write that fails halfway can leave an item
//! without some markers; the engine treats that write's entities as written (G15), and an
//! idempotent retry stores the item again.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use baid_core::{
    text_score, AdapterId, Backend, BackendError, BackendQuery, BackendResult, Capabilities,
    EntityId, Fact, Filters, Health, MemoryItem, NodeId, TenantId, WriteAck,
};
use futures::{stream, StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ClientOptions, ObjectStore, ObjectStoreExt, PutPayload, RetryConfig};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Parallel object reads per query.
const FETCH_CONCURRENCY: usize = 16;
/// Entities followed from each memory for a 2-hop neighbourhood.
const MAX_SECOND_HOP_ENTITIES: usize = 16;

/// Connection settings for [`S3Backend`].
#[derive(Clone)]
pub struct S3Config {
    /// `s3://bucket` or `s3://bucket/prefix`, optionally with `?endpoint=http://host:port`
    /// (for S3-compatible stores) and `&region=...`.
    pub url: String,
    /// Access key id; falls back to the standard `AWS_*` environment variables.
    pub access_key: Option<String>,
    /// Secret access key; falls back to `AWS_*` as above.
    pub secret_key: Option<String>,
    /// Upper bound on one call, including the client's own request.
    pub call_timeout: Duration,
    /// Memories an unfiltered search scans (newest first).
    pub scan_limit: usize,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("url", &self.url)
            .field("access_key", &self.access_key.as_ref().map(|_| "***"))
            .field("secret_key", &self.secret_key.as_ref().map(|_| "***"))
            .field("call_timeout", &self.call_timeout)
            .field("scan_limit", &self.scan_limit)
            .finish()
    }
}

/// The parsed parts of an `s3://` URL.
#[derive(Debug, PartialEq, Eq)]
struct S3Url {
    bucket: String,
    prefix: String,
    endpoint: Option<String>,
    region: Option<String>,
}

fn parse_url(url: &str) -> Result<S3Url, BackendError> {
    let bad = |why: &str| BackendError::Rejected(format!("invalid S3 url {url:?}: {why}"));
    let rest = url
        .strip_prefix("s3://")
        .ok_or_else(|| bad("must start with s3://"))?;
    let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
    if bucket.is_empty() {
        return Err(bad("missing bucket"));
    }
    let prefix = prefix.trim_matches('/');
    let prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    };
    let mut endpoint = None;
    let mut region = None;
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        match pair.split_once('=') {
            Some(("endpoint", v)) if !v.is_empty() => endpoint = Some(v.to_owned()),
            Some(("region", v)) if !v.is_empty() => region = Some(v.to_owned()),
            _ => return Err(bad(&format!("unknown or empty parameter {pair:?}"))),
        }
    }
    Ok(S3Url {
        bucket: bucket.to_owned(),
        prefix,
        endpoint,
        region,
    })
}

/// One memory as stored in an object.
#[derive(Debug, Serialize, Deserialize)]
struct Stored {
    v: u8,
    id: String,
    text: String,
    #[serde(default)]
    entities: Vec<String>,
    #[serde(default)]
    labels: Vec<String>,
    #[serde(default, with = "time::serde::rfc3339::option")]
    as_of: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
}

/// An S3-backed memory backend.
pub struct S3Backend {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    call_timeout: Duration,
    scan_limit: usize,
    counter: AtomicU64,
}

fn hash(s: &str) -> String {
    blake3::hash(s.as_bytes()).to_hex()[..32].to_owned()
}

impl S3Backend {
    /// Builds the backend. Nothing is contacted until the first call.
    pub fn connect(cfg: S3Config) -> Result<Self, BackendError> {
        let url = parse_url(&cfg.url)?;
        let mut b = AmazonS3Builder::from_env()
            .with_bucket_name(&url.bucket)
            .with_region(url.region.as_deref().unwrap_or("us-east-1"))
            // Resilient is the only retry layer; the client's own retries (10 by default,
            // up to 3 minutes) would hide failures from the circuit breaker.
            .with_retry(RetryConfig {
                max_retries: 0,
                ..RetryConfig::default()
            })
            .with_client_options(
                ClientOptions::new()
                    .with_timeout(cfg.call_timeout)
                    .with_connect_timeout(cfg.call_timeout),
            );
        if let Some(ep) = &url.endpoint {
            b = b
                .with_endpoint(ep)
                .with_allow_http(ep.starts_with("http://"))
                .with_virtual_hosted_style_request(false);
        }
        if let (Some(k), Some(s)) = (&cfg.access_key, &cfg.secret_key) {
            b = b.with_access_key_id(k).with_secret_access_key(s);
        }
        let store = b.build().map_err(map_err)?;
        Ok(Self::with_store(
            Arc::new(store),
            &url.prefix,
            cfg.call_timeout,
            cfg.scan_limit,
        ))
    }

    /// Wraps any [`ObjectStore`]; tests use `object_store::memory::InMemory`.
    pub fn with_store(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        call_timeout: Duration,
        scan_limit: usize,
    ) -> Self {
        let prefix = prefix.trim_matches('/');
        S3Backend {
            store,
            prefix: if prefix.is_empty() {
                String::new()
            } else {
                format!("{prefix}/")
            },
            call_timeout,
            scan_limit: scan_limit.max(1),
            counter: AtomicU64::new(0),
        }
    }

    fn root(&self, t: &TenantId) -> String {
        format!("{}{}/", self.prefix, t)
    }
    fn item_path(&self, t: &TenantId, id: &str) -> Path {
        Path::from(format!("{}items/{id}.json", self.root(t)))
    }
    fn items_prefix(&self, t: &TenantId) -> Path {
        Path::from(format!("{}items/", self.root(t)))
    }
    fn entity_prefix(&self, t: &TenantId, e: &str) -> Path {
        Path::from(format!("{}entities/{}/", self.root(t), hash(e)))
    }
    fn marker_path(&self, t: &TenantId, e: &str, id: &str) -> Path {
        Path::from(format!("{}entities/{}/{id}", self.root(t), hash(e)))
    }
    fn idem_path(&self, t: &TenantId, key: &str) -> Path {
        Path::from(format!("{}idem/{}", self.root(t), hash(key)))
    }

    /// `{inverted millis:016x}{inverted counter:08x}-{random:08x}`: lexicographic order is
    /// newest first, also for writes in the same millisecond.
    fn new_id(&self) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let millis = u64::try_from(now.as_millis()).unwrap_or(u64::MAX);
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let salt = blake3::hash(format!("{}-{n}", now.as_nanos()).as_bytes());
        let mut r = [0u8; 4];
        r.copy_from_slice(&salt.as_bytes()[..4]);
        // The inverted counter orders writes within the same millisecond (newest first).
        let seq = u32::MAX - (n as u32);
        format!(
            "{:016x}{seq:08x}-{:08x}",
            u64::MAX - millis,
            u32::from_le_bytes(r)
        )
    }

    async fn bounded<T>(
        &self,
        fut: impl std::future::Future<Output = Result<T, BackendError>>,
    ) -> Result<T, BackendError> {
        tokio::time::timeout(self.call_timeout, fut)
            .await
            .map_err(|_| BackendError::Timeout(self.call_timeout))?
    }

    /// The last path segment of every object under `prefix`, in key order, at most `n`.
    async fn list_names(&self, prefix: &Path, n: usize) -> Result<Vec<String>, BackendError> {
        self.store
            .list(Some(prefix))
            .take(n)
            .map_err(map_err)
            .map_ok(|meta| meta.location.filename().unwrap_or_default().to_owned())
            .try_collect()
            .await
    }

    /// Fetches memories by id, in the given order; ids without an object are skipped.
    async fn load(&self, t: &TenantId, ids: &[String]) -> Result<Vec<Stored>, BackendError> {
        let fetched: Vec<Option<Stored>> = stream::iter(ids.to_vec())
            .map(|id| async move {
                match self.store.get(&self.item_path(t, &id)).await {
                    Ok(r) => {
                        let bytes = r.bytes().await.map_err(map_err)?;
                        serde_json::from_slice::<Stored>(&bytes)
                            .map(Some)
                            .map_err(|e| BackendError::Rejected(format!("corrupt item {id}: {e}")))
                    }
                    Err(object_store::Error::NotFound { .. }) => Ok(None),
                    Err(e) => Err(map_err(e)),
                }
            })
            .buffered(FETCH_CONCURRENCY)
            .try_collect()
            .await?;
        Ok(fetched.into_iter().flatten().collect())
    }

    /// Ids of memories mentioning any of `entities`, newest first, deduplicated.
    async fn ids_for(
        &self,
        t: &TenantId,
        entities: &[&str],
        n: usize,
    ) -> Result<Vec<String>, BackendError> {
        let mut ids = Vec::new();
        for e in entities {
            ids.extend(self.list_names(&self.entity_prefix(t, e), n).await?);
        }
        ids.sort_unstable();
        ids.dedup();
        ids.truncate(n);
        Ok(ids)
    }

    async fn search(
        &self,
        t: &TenantId,
        text: &str,
        f: &Filters,
        limit: usize,
    ) -> Result<Vec<MemoryItem>, BackendError> {
        let ids = if f.entity_ids.is_empty() {
            self.list_names(&self.items_prefix(t), self.scan_limit)
                .await?
                .into_iter()
                .filter_map(|name| name.strip_suffix(".json").map(str::to_owned))
                .collect()
        } else {
            let es: Vec<&str> = f.entity_ids.iter().map(EntityId::as_str).collect();
            self.ids_for(t, &es, self.scan_limit).await?
        };
        let mut hits: Vec<MemoryItem> = self
            .load(t, &ids)
            .await?
            .into_iter()
            .filter(|s| f.labels.is_empty() || s.labels.iter().any(|l| f.labels.contains(l)))
            .filter(|s| match f.since {
                Some(since) => s.as_of.is_some_and(|a| a >= since),
                None => true,
            })
            .filter(|s| {
                f.entity_ids.is_empty()
                    || s.entities
                        .iter()
                        .any(|e| f.entity_ids.iter().any(|x| x.as_str() == e))
            })
            .filter_map(|s| text_score(text, &s.text).map(|score| self.item(s, score)))
            .collect::<Result<_, _>>()?;
        sort_and_trim(&mut hits, limit);
        Ok(hits)
    }

    async fn neighbourhood(
        &self,
        t: &TenantId,
        entity: &EntityId,
        hops: u8,
        limit: usize,
    ) -> Result<Vec<MemoryItem>, BackendError> {
        let first = self.ids_for(t, &[entity.as_str()], limit).await?;
        let direct = self.load(t, &first).await?;
        let mut seen: std::collections::HashSet<String> = first.iter().cloned().collect();
        let mut second = Vec::new();
        if hops >= 2 {
            let mut next: Vec<&str> = direct
                .iter()
                .flat_map(|s| s.entities.iter().map(String::as_str))
                .filter(|e| *e != entity.as_str())
                .collect();
            next.sort_unstable();
            next.dedup();
            next.truncate(MAX_SECOND_HOP_ENTITIES);
            let ids: Vec<String> = self
                .ids_for(t, &next, limit)
                .await?
                .into_iter()
                .filter(|id| seen.insert(id.clone()))
                .collect();
            second = self.load(t, &ids).await?;
        }
        let mut out: Vec<MemoryItem> = direct
            .into_iter()
            .map(|s| self.item(s, 1.0))
            .chain(second.into_iter().map(|s| self.item(s, 0.5)))
            .collect::<Result<_, _>>()?;
        sort_and_trim(&mut out, limit);
        Ok(out)
    }

    fn item(&self, s: Stored, score: f32) -> Result<MemoryItem, BackendError> {
        Ok(MemoryItem {
            id: NodeId::new(&s.id).map_err(|e| BackendError::Rejected(e.to_string()))?,
            text: Arc::from(s.text),
            score,
            tokens: 0,
            entities: s
                .entities
                .iter()
                .filter_map(|e| EntityId::new(e).ok())
                .collect(),
            as_of: s.as_of,
            source: self.id(),
        })
    }

    async fn run_one(&self, t: &TenantId, q: &BackendQuery) -> Result<BackendResult, BackendError> {
        let items = match q {
            BackendQuery::Search {
                text,
                filters,
                limit,
            } => self.search(t, text, filters, usize::from(*limit)).await?,
            BackendQuery::Neighbourhood {
                entity,
                hops,
                limit,
            } => {
                self.neighbourhood(t, entity, *hops, usize::from(*limit))
                    .await?
            }
            BackendQuery::Entity { id } => self.neighbourhood(t, id, 1, 1).await?,
        };
        Ok(BackendResult { items })
    }

    async fn write_inner(&self, t: &TenantId, fact: &Fact) -> Result<WriteAck, BackendError> {
        if let Some(key) = &fact.idempotency_key {
            match self.store.get(&self.idem_path(t, key)).await {
                Ok(r) => {
                    let id =
                        String::from_utf8_lossy(&r.bytes().await.map_err(map_err)?).into_owned();
                    return Ok(WriteAck {
                        node: NodeId::new(id.trim())
                            .map_err(|e| BackendError::Rejected(e.to_string()))?,
                        entities: fact.entities.clone(),
                        version: None,
                    });
                }
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(map_err(e)),
            }
        }
        let id = self.new_id();
        let mut entities: Vec<String> = fact.entities.iter().map(|e| e.to_string()).collect();
        entities.sort_unstable();
        entities.dedup();
        let stored = Stored {
            v: 1,
            id: id.clone(),
            text: fact.text.clone(),
            entities: entities.clone(),
            labels: fact.labels.clone(),
            as_of: fact.as_of,
            created_at: OffsetDateTime::now_utc(),
        };
        let body =
            serde_json::to_vec(&stored).map_err(|e| BackendError::Rejected(e.to_string()))?;
        self.store
            .put(&self.item_path(t, &id), PutPayload::from(body))
            .await
            .map_err(map_err)?;
        stream::iter(entities.clone())
            .map(|e| {
                let path = self.marker_path(t, &e, &id);
                async move {
                    self.store
                        .put(&path, PutPayload::new())
                        .await
                        .map_err(map_err)
                }
            })
            .buffer_unordered(FETCH_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        if let Some(key) = &fact.idempotency_key {
            self.store
                .put(
                    &self.idem_path(t, key),
                    PutPayload::from(id.clone().into_bytes()),
                )
                .await
                .map_err(map_err)?;
        }
        Ok(WriteAck {
            node: NodeId::new(&id).map_err(|e| BackendError::Rejected(e.to_string()))?,
            entities: fact.entities.clone(),
            version: None,
        })
    }
}

/// Score descending; ties newest first (ids sort newest first).
fn sort_and_trim(items: &mut Vec<MemoryItem>, limit: usize) {
    items.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
    items.truncate(limit);
}

#[async_trait]
impl Backend for S3Backend {
    fn id(&self) -> AdapterId {
        AdapterId::new("s3").unwrap_or_else(|_| unreachable!("valid id"))
    }

    fn capabilities(&self) -> Capabilities {
        // Range-coalesced batch reads come with plan T27.
        Capabilities {
            native_batch: false,
            max_batch: 16,
            neighbourhood: true,
            change_feed: false,
            versions: false,
        }
    }

    async fn query_batch(
        &self,
        tenant: &TenantId,
        queries: &[BackendQuery],
    ) -> Result<Vec<BackendResult>, BackendError> {
        self.bounded(async {
            let mut out = Vec::with_capacity(queries.len());
            for q in queries {
                out.push(self.run_one(tenant, q).await?);
            }
            Ok(out)
        })
        .await
    }

    async fn write(&self, tenant: &TenantId, fact: &Fact) -> Result<WriteAck, BackendError> {
        self.bounded(self.write_inner(tenant, fact)).await
    }

    async fn health(&self) -> Health {
        let probe = async {
            self.store
                .list_with_delimiter(Some(&Path::from(self.prefix.as_str())))
                .await
                .map(|_| ())
                .map_err(map_err)
        };
        match self.bounded(probe).await {
            Ok(()) => Health::Up,
            Err(e) => Health::Down {
                reason: e.to_string(),
            },
        }
    }
}

/// Maps store errors onto the core kinds: auth and bad requests are not retryable; anything
/// network-shaped is.
pub fn map_err(e: object_store::Error) -> BackendError {
    use object_store::Error as E;
    match e {
        E::Unauthenticated { .. } | E::PermissionDenied { .. } => {
            BackendError::Rejected(format!("access denied: {e}"))
        }
        E::InvalidPath { .. }
        | E::NotSupported { .. }
        | E::AlreadyExists { .. }
        | E::Precondition { .. }
        | E::NotModified { .. }
        | E::UnknownConfigurationKey { .. }
        | E::NotImplemented { .. } => BackendError::Rejected(e.to_string()),
        other => BackendError::Unavailable(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_parsing() {
        assert_eq!(
            parse_url("s3://memories").unwrap(),
            S3Url {
                bucket: "memories".into(),
                prefix: String::new(),
                endpoint: None,
                region: None
            }
        );
        assert_eq!(
            parse_url("s3://b/team/a/?endpoint=http://127.0.0.1:8333&region=eu-west-1").unwrap(),
            S3Url {
                bucket: "b".into(),
                prefix: "team/a/".into(),
                endpoint: Some("http://127.0.0.1:8333".into()),
                region: Some("eu-west-1".into()),
            }
        );
        for bad in [
            "http://b",
            "s3://",
            "s3:///x",
            "s3://b?endpont=x",
            "s3://b?endpoint=",
        ] {
            assert!(parse_url(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn ids_sort_newest_first() {
        let b = S3Backend::with_store(
            Arc::new(object_store::memory::InMemory::new()),
            "",
            Duration::from_secs(1),
            10,
        );
        let a = b.new_id();
        let same_ms = b.new_id();
        assert!(same_ms < a, "{same_ms} should sort before {a}");
        std::thread::sleep(Duration::from_millis(3));
        let later = b.new_id();
        assert!(later < same_ms, "{later} should sort before {same_ms}");
        assert_eq!(a.len(), 33);
    }

    #[test]
    fn hostile_names_never_reach_keys() {
        let b = S3Backend::with_store(
            Arc::new(object_store::memory::InMemory::new()),
            "p",
            Duration::from_secs(1),
            10,
        );
        let t = TenantId::new("acme").unwrap();
        let p = b.marker_path(&t, "../../globex/items", "x").to_string();
        assert!(
            p.starts_with("p/acme/entities/") && !p.contains(".."),
            "{p}"
        );
        let k = b.idem_path(&t, "a/../../b").to_string();
        assert!(k.starts_with("p/acme/idem/") && !k.contains(".."), "{k}");
    }

    #[test]
    fn credentials_never_print() {
        let c = S3Config {
            url: "s3://b".into(),
            access_key: Some("AKIA-secret".into()),
            secret_key: Some("hunter2".into()),
            call_timeout: Duration::from_secs(1),
            scan_limit: 10,
        };
        let s = format!("{c:?}");
        assert!(!s.contains("hunter2") && !s.contains("AKIA"), "{s}");
    }
}
