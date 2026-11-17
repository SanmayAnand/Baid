//! The recall engine: recall, remember and session end, over the cache, the tenant's
//! backend and the packer.
//!
//! # Guarantees
//!
//! - **Tenant isolation.** The tenant comes from the API key; every key, cache tier,
//!   backend call and session is scoped by it.
//! - **A session always sees its own writes.** Each session remembers the write sequence
//!   number of its last `remember`; a cached entry loaded before that is a miss for that
//!   session.
//! - **No session sees data a write changed.** A write stamps the entities it touched;
//!   the cache refuses any entry that depends on them and was loaded earlier.
//! - **Results a write would *add* to someone else's cached search** appear within the
//!   tenant's TTL, the same bound as writes made outside BAiD.

use std::sync::Arc;
use std::time::{Duration, Instant};

use baid_core::{
    Backend, BackendError, BackendQuery, CacheEntry, CacheOutcome, CacheStore, EntityId, Fact, Key,
    LoadInfo, MemoryItem, NodeId, PackedContext, Packer, PutOutcome, RecallRequest, SessionId,
    TenantId,
};
use dashmap::DashMap;
use session::Sessions;
use tracing::{debug, error, warn};

use adapters::Registry;

use crate::auth::Tenant;
use crate::http::error::ApiError;

/// What a recall returns.
#[derive(Clone, Debug)]
pub struct Recall {
    /// The packed items.
    pub packed: PackedContext,
    /// Hit or miss.
    pub outcome: CacheOutcome,
}

/// What a remember returns.
#[derive(Clone, Debug, PartialEq)]
pub struct Remembered {
    /// The backend's id for the stored fact.
    pub node: NodeId,
    /// Every entity the write touched (client-named plus backend-linked).
    pub entities: Vec<EntityId>,
    /// The item's version, if the backend tracks versions.
    pub version: Option<u64>,
    /// True when this answer was replayed for a repeated idempotency key.
    pub replayed: bool,
}

/// Longest accepted idempotency key.
pub const MAX_IDEMPOTENCY_KEY: usize = 128;
/// How long an idempotency key is remembered.
pub const IDEMPOTENCY_TTL: Duration = Duration::from_secs(24 * 3600);
/// Above this many remembered keys, expired ones are pruned on insert.
const IDEMPOTENCY_PRUNE_AT: usize = 100_000;

/// Recent `remember` results by idempotency key, so a retried write is applied once.
///
/// Two *concurrent* requests with the same new key may both reach the backend; this guards
/// against retries, which are sequential.
#[derive(Debug, Default)]
struct Idempotency {
    map: DashMap<(TenantId, String), (Remembered, Instant)>,
}

impl Idempotency {
    fn get(&self, tenant: &TenantId, key: &str, now: Instant) -> Option<Remembered> {
        let hit = self.map.get(&(tenant.clone(), key.to_owned()))?;
        let (r, at) = hit.value();
        (now.saturating_duration_since(*at) < IDEMPOTENCY_TTL).then(|| r.clone())
    }

    fn insert(&self, tenant: TenantId, key: String, r: Remembered, now: Instant) {
        if self.map.len() >= IDEMPOTENCY_PRUNE_AT {
            self.prune(now);
        }
        self.map.insert((tenant, key), (r, now));
    }

    fn prune(&self, now: Instant) -> usize {
        let before = self.map.len();
        self.map
            .retain(|_, (_, at)| now.saturating_duration_since(*at) < IDEMPOTENCY_TTL);
        before.saturating_sub(self.map.len())
    }
}

/// The engine. Cheap to share behind an `Arc`.
pub struct Engine {
    cache: Arc<dyn CacheStore>,
    backends: Registry,
    packer: Arc<dyn Packer>,
    sessions: Arc<Sessions>,
    idempotency: Idempotency,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("backends", &self.backends)
            .field("sessions", &self.sessions.len())
            .finish_non_exhaustive()
    }
}

impl Engine {
    /// Builds the engine from its parts.
    pub fn new(
        cache: Arc<dyn CacheStore>,
        backends: Registry,
        packer: Arc<dyn Packer>,
        sessions: Arc<Sessions>,
    ) -> Self {
        Engine {
            cache,
            backends,
            packer,
            sessions,
            idempotency: Idempotency::default(),
        }
    }

    /// The session tracker.
    pub fn sessions(&self) -> &Sessions {
        &self.sessions
    }

    fn backend(&self, t: &Tenant) -> Result<Arc<dyn Backend>, ApiError> {
        self.backends.get(&t.id).ok_or_else(|| {
            error!(tenant = %t.id, "no backend configured for an authenticated tenant");
            ApiError::Internal
        })
    }

    /// Answers a recall from the cache if it can, otherwise from the backend (and caches
    /// the result).
    pub async fn recall(&self, t: &Tenant, req: RecallRequest) -> Result<Recall, ApiError> {
        req.validate()?;
        let now = Instant::now();
        let backend = self.backend(t)?;
        let query = req.backend_query();
        let key = Key::derive(&t.id, &backend.id(), &query);
        self.sessions.touch(&t.id, &req.session, now);
        let own_last_write = self.sessions.last_write(&t.id, &req.session);

        if !req.bypasses_cache() {
            if let Some(e) = self.cache.get(&key).await {
                if e.is_fresh(now, req.max_staleness) && e.loaded_at >= own_last_write {
                    if e.speculative {
                        self.cache.promote(&key);
                    }
                    return Ok(Recall {
                        packed: self.packer.pack(&e.items, req.budget),
                        outcome: CacheOutcome::Hit,
                    });
                }
            }
        }

        let items = self.load(t, backend.as_ref(), key, &query, now).await?;
        Ok(Recall {
            packed: self.packer.pack(&items, req.budget),
            outcome: CacheOutcome::Miss,
        })
    }

    /// One backend call, then cache the result.
    async fn load(
        &self,
        t: &Tenant,
        backend: &dyn Backend,
        key: Key,
        query: &BackendQuery,
        now: Instant,
    ) -> Result<Vec<MemoryItem>, ApiError> {
        // Read before the call: a write acknowledged after this point is newer than the
        // entry, so the cache will refuse it if the write touched any of its entities.
        let loaded_at = self.cache.write_seq(&t.id);
        let deadline = t.config.backend_timeout.get();
        let mut results = tokio::time::timeout(
            deadline,
            backend.query_batch(&t.id, std::slice::from_ref(query)),
        )
        .await
        .map_err(|_| BackendError::Timeout(deadline))??;
        if results.len() != 1 {
            error!(tenant = %t.id, got = results.len(), "adapter returned the wrong number of results");
            return Err(ApiError::Internal);
        }
        let items = results.pop().map(|r| r.items).unwrap_or_default();

        let entry = CacheEntry::new(
            items.clone(),
            LoadInfo {
                query_entities: query.entities(),
                loaded_at,
                source: backend.id(),
                ttl: t.config.ttl.get(),
                speculative: false,
            },
            now,
        );
        match self.cache.put(key, entry).await {
            PutOutcome::Stored => {}
            other => debug!(tenant = %t.id, outcome = ?other, "result not cached"),
        }
        Ok(items)
    }

    /// Forwards a fact to the backend, then marks every entity it touched as written.
    pub async fn remember(
        &self,
        t: &Tenant,
        session: &SessionId,
        fact: Fact,
    ) -> Result<Remembered, ApiError> {
        fact.validate()?;
        let now = Instant::now();
        if let Some(k) = &fact.idempotency_key {
            if k.is_empty() || k.len() > MAX_IDEMPOTENCY_KEY {
                return Err(ApiError::InvalidArgument(format!(
                    "idempotency_key must be 1 to {MAX_IDEMPOTENCY_KEY} bytes"
                )));
            }
            if let Some(mut prev) = self.idempotency.get(&t.id, k, now) {
                prev.replayed = true;
                return Ok(prev);
            }
        }
        let backend = self.backend(t)?;
        let deadline = t.config.backend_timeout.get();
        let result = tokio::time::timeout(deadline, backend.write(&t.id, &fact))
            .await
            .map_err(|_| BackendError::Timeout(deadline))
            .and_then(|r| r);

        let ack = match result {
            Ok(ack) => ack,
            Err(e) => {
                // A timeout or dropped connection may still have applied the write, so
                // treat its entities as written: that costs misses, never a wrong answer.
                // A rejection was definitely not applied.
                if !matches!(e, BackendError::Rejected(_)) {
                    let seq = self.cache.mark_written(&t.id, &fact.entities);
                    self.sessions.note_write(&t.id, session, seq, now);
                    warn!(tenant = %t.id, error = %e, "write failed; its entities are treated as written");
                }
                return Err(e.into());
            }
        };

        let mut entities = fact.entities.clone();
        entities.extend(ack.entities.iter().cloned());
        entities.sort_unstable();
        entities.dedup();
        let seq = self.cache.mark_written(&t.id, &entities);
        self.sessions.note_write(&t.id, session, seq, now);

        let r = Remembered {
            node: ack.node,
            entities,
            version: ack.version,
            replayed: false,
        };
        if let Some(k) = fact.idempotency_key {
            self.idempotency.insert(t.id.clone(), k, r.clone(), now);
        }
        Ok(r)
    }

    /// Forgets a session. Returns true if it existed.
    pub fn end_session(&self, t: &Tenant, session: &SessionId) -> bool {
        self.sessions.end(&t.id, session)
    }

    /// Periodic cleanup: idle sessions and old idempotency keys.
    pub fn housekeeping(&self, now: Instant, session_idle: Duration) -> (usize, usize) {
        (
            self.sessions.expire_idle(now, session_idle),
            self.idempotency.prune(now),
        )
    }
}
