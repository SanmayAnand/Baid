# BAiD build plan

Sources: *Architecture and design* and *Code architecture* PDFs, plus the repo scaffolding as of
2026-11-17. One track: tasks run in order, and each ends
with `just check` green.

Status key: `[ ]` todo, `[~]` in progress, `[x]` done.

## Current status (2026-11-19)

- **Done:** T01–T16 (foundation, vertical slice, adapters, correctness). Live adapter tests:
  `just test-live` and `.github/workflows/contract.yml`.
- **Next:** **T17**, fail open and the stale-if-error window. Not started.
- **Open follow-ups** (no action until the trigger happens):
  - neo4rs 0.9 stable: switch, then drop the `deny.toml` ignores it no longer needs (G18).
  - foyer: on every upgrade run `cargo tree -i paste`; drop RUSTSEC-2024-0436 once gone (G12).
  - object_store: drop the `humantime` ignore RUSTSEC-2026-0542 once it stops depending on
    it (G20).

## 1. Spec gaps and the decisions taken

| # | Issue | Decision |
|---|---|---|
| G1 | Race in the stale-put guard: put checks versions → writer bumps → writer evicts (key not there yet) → put inserts stale data. | Done in T06: `RamStore` inserts, then re-checks versions and removes its own entry if any moved. Covered by a deterministic test and a 2,000-round threaded race test; both fail if the re-check is removed. |
| G2 | The engine calls `cache.promote()` and `cache.contains()`; the trait lacks both. | Done: `CacheStore` has `contains`, `promote` and `remove`. `evict_entity` is dropped: the invalidator owns the entity→keys index and calls `remove`. The staleness API is now `write_seq` / `mark_written` (G13, ADR-011), replacing the first `snapshot_versions` / `bump_versions` design. |
| G3 | The `Predictor` trait uses `SessionView`, `TenantStats` and `QueryClass`. | Define them in baid-core. |
| G4 | `Key::derive` takes both `kind` and a query, but the query already implies the kind. | Derive the kind from the query. |
| G5 | baid-core needs `async-trait`, `time` and `unicode-normalization`. | Add them; none does I/O. |
| G6 | `entity_ver` lives in cache (`versions.rs`), and invalidate bumps it. | Superseded by G13: the cache owns the write log; invalidate only evicts through the `CacheStore` trait and needs no cache-crate dependency. |
| G7 | mcp must call the engine without depending on cache or exec. | Add a `MemoryService` trait in baid-core at T32 (with an ADR). |
| G8 | The project was renamed from GraphCache to BAiD. | Done: crate folders drop the `gc-` prefix, the core package is `baid-core` (`core` clashes with Rust's built-in crate), keys are `baid:v1:`, env vars will be `BAID_`, metrics `baid_`, the binary is `baid`. |
| G9 | Can foyer honour custom priority (ADR-005)? | Spike done: two levels only (`Hint::Low` is evicted first). One foyer LRU per tenant; speculative → Low, normal and hub → Normal. foyer admits oversized entries, so BAiD refuses entries over 1/4 of a shard. ADR-005 is Accepted. |
| G10 | The dependency table in the PDF is garbled. | The layer-check xtask (T05) becomes the source of truth. |
| G11 | The SRS is not used. | Targets come from the two spec tabs and this plan. |
| G12 | foyer pulls in `paste` (RUSTSEC-2024-0436: archived, unmaintained, no known vulnerability), which failed `cargo deny check`. | **Resolved** with one scoped ignore in `deny.toml` (owner-approved, 2026-11-17). Follow-up: recheck on every foyer upgrade (`cargo tree -i paste`) and remove the ignore once foyer drops `paste`. |
| G13 | **Hole in the spec's version guard:** versions are snapshotted only for entities the *query* names. A write to an entity that appears only in the *results* (search "renewal dates" returns acme items) passes the put check, and stale data is served until the TTL. | Done in T08: per-tenant **write sequence numbers**. A write takes the next number after the ack and stamps each entity it touched; an entry stores the number read before its load; it is refused on put **and on every get** if any of its entities (query or results) was stamped later. Correctness no longer depends on the invalidator, which becomes a memory optimisation (T14). |
| G14 | A write can *add* a result to a search another session cached (a new fact that now matches); no entity check can see that. | Policy, pinned by tests: the writing session never sees a cached entry older than its own last write (read-your-writes); other sessions see additions within the TTL. Documented in the README. |
| G15 | Ambiguous write failures (timeout, reset) may still have been applied. | The engine stamps the fact's entities as written on any failure except `Rejected`: costs misses, never a wrong answer. |
| G16 | MinIO no longer publishes container images (`minio/minio` is gone from Docker Hub and quay.io), so the local stack cannot pin it. | The local S3 store is **SeaweedFS** (`chrislusf/seaweedfs:3.97`, Apache-2.0, one container, S3 gateway on :8333). The S3 adapter speaks plain S3, so any compatible store works. |
| G17 | neo4rs 0.8 retries internally for up to 60 s, including on authentication failures, and cannot be configured; and an outer timeout that drops a call hides the failure from the circuit breaker. | The Bolt adapter uses explicit transactions (never retried by the driver), so errors come back at once and `Resilient` is the only retry layer. Every call has one deadline shared by attempts and backoff, recorded as a failure when it runs out. The engine's own timeout is a backstop at `backend_timeout + 250ms`. |
| G18 | neo4rs 0.8 pulls in six crates flagged *unmaintained* (backoff, instant, chrono, chrono-tz, chrono-tz-build, rustls-pemfile) and webpki-roots under CDLA-Permissive-2.0, which is not on the licence allow-list. No vulnerabilities. neo4rs 0.9.0-rc.11 drops backoff, instant, rustls-pemfile and webpki-roots but keeps chrono and chrono-tz. | **Resolved for 0.8** (owner-approved, 2026-11-17): one scoped ignore per advisory in `deny.toml` (RUSTSEC-2025-0012 backoff, RUSTSEC-2024-0384 instant, RUSTSEC-2026-0543 chrono, RUSTSEC-2026-0555 chrono-tz, RUSTSEC-2026-0559 chrono-tz-build, RUSTSEC-2025-0134 rustls-pemfile) and a `[[licenses.exceptions]]` entry allowing CDLA-Permissive-2.0 for webpki-roots only. Checked first: every one is transitive via neo4rs only, none is a BAiD dependency. Follow-up: switch to neo4rs 0.9 once it is a stable release, then drop the ignores it no longer needs (0.9.0-rc.11 already drops backoff, instant, rustls-pemfile and webpki-roots). |
| G19 | S3 has no query engine; object_store retries internally (10 retries, up to 3 min) by default. | Entity-scoped queries read hashed per-entity marker objects (cost grows with the entity, not the tenant). Unfiltered search scans only the newest 1,000 memories (`S3_SCAN_LIMIT`), documented in `s3.rs`. Client retries set to 0 and requests bounded by the tenant's backend timeout, so `Resilient` is the only retry layer (as G17). |
| G20 | object_store adds `humantime` (RUSTSEC-2026-0542, unmaintained, a non-optional object_store dependency) and `webpki-root-certs` (CDLA-Permissive-2.0, via reqwest → rustls-platform-verifier). No vulnerabilities. | **Resolved** (owner-approved, 2026-11-17): RUSTSEC-2026-0542 ignored with a scoped reason, and a `[[licenses.exceptions]]` entry allowing CDLA-Permissive-2.0 for webpki-root-certs only. Both proven transitive via object_store only. Follow-up: drop the humantime ignore when object_store stops depending on it. |
| G21 | Sharing a load between concurrent misses (spec 3.2) can hand a follower data loaded before a write it must see: its own last write, or another session's acknowledged write to an entity in the results. And a shared future abandoned by all waiters would be resumed later with an expired deadline. | Followers never use the shared result directly: they re-read through the cache (full write-log check plus the session floor) and load directly if refused. Flights count waiters; the last one to leave removes the entry and drops the load. Both are pinned by tests that fail under mutation. |
| G22 | CI failure (2026-11-18): on a fresh Neo4j, backends creating the schema at the same moment deadlock (`Neo.TransientError.Transaction.DeadlockDetected` on a label lock). Concurrent `MERGE`s of one entity can do the same. Hidden locally because the schema already existed. | The Bolt adapter retries a transaction (5 attempts, 10-160 ms jittered backoff, within the call timeout) when Neo4j reports `Neo.TransientError.*`, which means rolled back and safe to retry, even for writes without an idempotency key. Connection and auth errors still go to `Resilient` only. Regression test `tests/bolt_fresh_schema.rs` drops the schema and has 8 new backends write at once; it fails 3/3 without the retry. |

## 2. Tasks

### Phase 0: foundation (baid-core)
- [x] **T01** IDs and cache keys: `TenantId`, `SessionId`, `EntityId`, `NodeId`, `AdapterId`; `BackendQuery`, `Filters`, `TokenBudget`, budget buckets; `normalise()`; `Key::derive` (blake3) and the `baid:v1:` string form.
- [x] **T02** Domain types: `RecallRequest`, `MemoryItem`, `CacheEntry`, `CacheOutcome`, `Candidate`, `Fact`, `WriteAck`, `BackendResult`, `Health`, `Capabilities`, `PutOutcome`, `FlushScope`, `PackedContext`, `InvalidationReport`, `SessionView`, `TenantStats`, `QueryClass`.
- [x] **T03** Errors and config structs (serde), with `validate()`.
- [x] **T04** The five traits (plus G2) and a `MockBackend` behind the `test-util` feature.
- [x] **T05** Layer-check xtask; a test that every `tokio::spawn` site takes a `CancellationToken`.

### Phase 1: vertical slice (cache-only, against a mock backend)
- [x] **T06** foyer spike, then the RAM tier: TTL, per-tenant capacity, priority, version-guarded put (G1), promote. Isolation proptest.
- [x] **T07** Server skeleton: CLI, figment config, tenant from API key, `/healthz` `/readyz`, ApiError→HTTP, graceful shutdown.
- [x] **T08** Engine: recall (hit/miss) and remember pass-through; read-your-writes and `max_staleness=0` bypass.
- [x] **T09** `api/openapi.yaml` v1, with a contract test against the live router (`crates/server/tests/contract.rs`).
- [x] **T10** docker-compose: Neo4j 5.26 LTS, SeaweedFS (S3, replaces MinIO: G16), Prometheus, Grafana; `just up` / `down` / `ps` / `logs`. Verified: all healthy, Bolt query, signed S3 put/get/range read, wrong key refused, Grafana datasource provisioned.
- [x] **T11** Bolt adapter (`adapters::BoltBackend`, namespaced `BaidMemory`/`BaidEntity` graph, escaped full-text search, neighbourhoods, idempotent writes), adapter registry with credentials, and `Resilient` (retries, circuit breaker, shared deadline). Live tests: `just test-live`. Dependency advisories handled in G18.
- [x] **T12** S3 adapter (`adapters::S3Backend` on object_store: per-tenant prefixes, hashed entity markers, newest-first ids, bounded scan for unfiltered search) and the `Backend` contract suite (`crates/adapters/tests/contract.rs`: memory and in-memory S3 always; live Neo4j and SeaweedFS via `just test-live` and `.github/workflows/contract.yml`).

### Phase 2: correctness
- [x] **T13** Single-flight loads (`exec::SingleFlight`): concurrent misses on a key share one backend call; waiter counting cancels and forgets abandoned loads; followers re-read through the cache so sharing keeps read-your-writes and write invalidation (G21).
- [x] **T14** Invalidator (`invalidate::EntityIndex`): entity → dependent keys, filled by each stored load; a write (and an ambiguous failed write) evicts the dependents at write time. Bounded at 1,024 keys per entity and pruned every minute; safe to forget keys because reads re-check the write log (ADR-011).
- [x] **T15** Write-through remember (ack, then stamp entities); done in T08, including ambiguous failures (G15).
- [x] **T16** Staleness property (`crates/server/tests/staleness.rs`): random multi-session traces against the real engine, backend replies delayed after reading, checked for (1) no stale filtered read after an acknowledged write and (2) read-your-writes. Paused-clock variant (256 cases, deterministic, shrinks) plus a real-thread variant (32 cases); `BAID_PROPTEST_CASES` scales both for nightly. Catches four distinct injected bugs.
- [ ] **T17** **Next.** Fail open (internal errors in cache or packer fall through to the backend; a supervisor restarts background tasks; `baid_internal_errors_total`) and the stale-if-error window (serve an *expired* but never *invalidated* entry while the backend fails). Design: Architecture §5 and §12, Code architecture §4.1, ADR-009.

### Phase 3: measuring stick (memharness)
- [ ] **T18** Datagen and the Neo4j/S3 loaders (`just load`).
- [ ] **T19** Tracegen with think time; replay mode `off`; report (memory wait, tokens per task).
- [ ] **T20** Replay mode `cache-only`.

### Phase 4: prefetch
- [ ] **T21** Telemetry: metrics, Prometheus `/metrics`, tracing, OTLP feature.
- [ ] **T22** Session tracker: window, think-time EWMA, cancel tokens. (Already there from T08: per-session last write for read-your-writes, idle expiry via housekeeping.)
- [ ] **T23** Query classes and the neighbour predictor.
- [ ] **T24** Executor: deadlines, cancellation, task state metrics.
- [ ] **T25** Budget governor: token buckets, global cap, auto-off.
- [ ] **T26** Speculative entries; precision and waste metrics.
- [ ] **T27** Batched UNWIND (Bolt) and coalesced reads (S3).
- [ ] **T28** Replay mode `prefetch`, targeting a memory-wait cut of at least 50%.

### Phase 5: packing and prediction
- [ ] **T29** Packer: dedupe, rank, trim, stable order, tiktoken counts.
- [ ] **T30** Transition and popularity predictors, and the ensemble.
- [ ] **T31** Partial hits through neighbourhood entries.

### Phase 6: surfaces
- [ ] **T32** MCP server (rmcp): Streamable HTTP at `/mcp` and the stdio bridge.
- [ ] **T33** Python SDK.

### Phase 7: operations and launch
- [ ] **T34** Admin API, SIGHUP reload, freshness probe.
- [ ] **T35** Grafana dashboards and the README quick start.
- [ ] **T36** NVMe tier (foyer hybrid, postcard with a format byte).
- [ ] **T37** nightly and release CI.
- [ ] **T38** Full benchmark and write-up.
