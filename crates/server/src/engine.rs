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
//! - **One backend call per key at a time.** Concurrent misses on the same key share one
//!   load. Followers then re-read through the cache, so sharing never weakens the
//!   guarantees above.

use std::sync::Arc;
use std::time::{Duration, Instant};

use baid_core::{
    Backend, BackendError, BackendQuery, CacheEntry, CacheOutcome, CacheStore, EntityId, Fact, Key,
    LoadInfo, MemoryItem, NodeId, PackedContext, Packer, PutOutcome, RecallRequest, SessionId,
    TenantId, WriteSeq,
};
use dashmap::DashMap;
use exec::{Role, SingleFlight};
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

/// Extra time the engine allows beyond the tenant's backend timeout. The adapter layer
/// (`adapters::Resilient`) enforces the real deadline and records the failure for its
/// circuit breaker; this outer bound only catches a backend that ignores it.
pub const BACKSTOP_MARGIN: Duration = Duration::from_millis(250);

fn backstop(t: &Tenant) -> Duration {
    t.config.backend_timeout.get() + BACKSTOP_MARGIN
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

/// What a shared load yields: the items, or the backend failure every waiter sees.
type LoadResult = Result<Arc<Vec<MemoryItem>>, BackendError>;

/// One backend call plus the cache put, owning everything it needs so concurrent misses can
/// share it (see [`SingleFlight`]).
struct Load {
    cache: Arc<dyn CacheStore>,
    backend: Arc<dyn Backend>,
    tenant: TenantId,
    key: Key,
    query: BackendQuery,
    ttl: Duration,
    deadline: Duration,
    now: Instant,
}

impl Load {
    async fn run(self) -> LoadResult {
        // Read before the call: a write acknowledged after this point is newer than the
        // entry, so the cache refuses it if the write touched any of its entities.
        let loaded_at = self.cache.write_seq(&self.tenant);
        let mut results = tokio::time::timeout(
            self.deadline,
            self.backend
                .query_batch(&self.tenant, std::slice::from_ref(&self.query)),
        )
        .await
        .map_err(|_| BackendError::Timeout(self.deadline))??;
        if results.len() != 1 {
            error!(tenant = %self.tenant, got = results.len(), "adapter returned the wrong number of results");
            return Err(BackendError::Rejected(format!(
                "adapter returned {} results for 1 query",
                results.len()
            )));
        }
        let items = results.pop().map(|r| r.items).unwrap_or_default();
        let entry = CacheEntry::new(
            items.clone(),
            LoadInfo {
                query_entities: self.query.entities(),
                loaded_at,
                source: self.backend.id(),
                ttl: self.ttl,
                speculative: false,
            },
            self.now,
        );
        match self.cache.put(self.key, entry).await {
            PutOutcome::Stored => {}
            other => debug!(tenant = %self.tenant, outcome = ?other, "result not cached"),
        }
        Ok(Arc::new(items))
    }
}

/// The engine. Cheap to share behind an `Arc`.
pub struct Engine {
    cache: Arc<dyn CacheStore>,
    backends: Registry,
    packer: Arc<dyn Packer>,
    sessions: Arc<Sessions>,
    idempotency: Idempotency,
    flights: SingleFlight<LoadResult>,
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
            flights: SingleFlight::new(),
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
    /// the result). Concurrent misses on one key share a single backend call.
    pub async fn recall(&self, t: &Tenant, req: RecallRequest) -> Result<Recall, ApiError> {
        req.validate()?;
        let now = Instant::now();
        let backend = self.backend(t)?;
        let query = req.backend_query();
        let key = Key::derive(&t.id, &backend.id(), &query);
        self.sessions.touch(&t.id, &req.session, now);
        let own_last_write = self.sessions.last_write(&t.id, &req.session);

        if !req.bypasses_cache() {
            if let Some(e) = self.servable(&key, now, &req, own_last_write).await {
                return Ok(self.packed(&e.items, &req, CacheOutcome::Hit));
            }
        }

        let load = self.load(t, &backend, &key, &query, now);
        let (result, role) = self.flights.run(&key, move || load.run()).await;
        let items = result?;
        if role == Role::Follower {
            // We joined a load that may have started before something this request must
            // see: this session's own last write, or another session's acknowledged write.
            // Re-read through the cache, which applies every staleness check, and load
            // directly if it refuses.
            if let Some(e) = self.servable(&key, now, &req, own_last_write).await {
                return Ok(self.packed(&e.items, &req, CacheOutcome::Miss));
            }
            let items = self.load(t, &backend, &key, &query, now).run().await?;
            return Ok(self.packed(&items, &req, CacheOutcome::Miss));
        }
        Ok(self.packed(&items, &req, CacheOutcome::Miss))
    }

    /// The cached entry for `key`, if this request may be served from it: present, valid
    /// under the write log (checked by the store), within `max_staleness`, and loaded after
    /// this session's own last write. Promotes a speculative entry on use.
    async fn servable(
        &self,
        key: &Key,
        now: Instant,
        req: &RecallRequest,
        own_last_write: WriteSeq,
    ) -> Option<Arc<CacheEntry>> {
        let e = self.cache.get(key).await?;
        if !(e.is_fresh(now, req.max_staleness) && e.loaded_at >= own_last_write) {
            return None;
        }
        if e.speculative {
            self.cache.promote(key);
        }
        Some(e)
    }

    fn packed(&self, items: &[MemoryItem], req: &RecallRequest, outcome: CacheOutcome) -> Recall {
        Recall {
            packed: self.packer.pack(items, req.budget),
            outcome,
        }
    }

    fn load(
        &self,
        t: &Tenant,
        backend: &Arc<dyn Backend>,
        key: &Key,
        query: &BackendQuery,
        now: Instant,
    ) -> Load {
        Load {
            cache: Arc::clone(&self.cache),
            backend: Arc::clone(backend),
            tenant: t.id.clone(),
            key: key.clone(),
            query: query.clone(),
            ttl: t.config.ttl.get(),
            deadline: backstop(t),
            now,
        }
    }

    /// Loads currently in flight (shared by concurrent misses).
    pub fn loads_in_flight(&self) -> usize {
        self.flights.in_flight()
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
        let deadline = backstop(t);
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
