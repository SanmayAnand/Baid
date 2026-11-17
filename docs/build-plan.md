# BAiD / BAiD build plan

Sources: *Architecture and design* and *Code architecture* PDFs, plus the repo scaffolding as of
2026-11-17. One track: tasks run in order, and each ends
with `just check` green.

Status key: `[ ]` todo, `[~]` in progress, `[x]` done.

## 1. Spec gaps and the decisions taken

| # | Issue | Decision |
|---|---|---|
| G1 | Race in the stale-put guard: put checks versions → writer bumps → writer evicts (key not there yet) → put inserts stale data. | `put` inserts, then re-checks versions and removes its own entry if any moved. `register` runs before insert. A property test injects a delay. |
| G2 | The engine calls `cache.promote()` and `cache.contains()`; the trait lacks both. | Done: `CacheStore` has `contains`, `promote`, `remove`, `snapshot_versions` and `bump_versions`. The cache owns the version table. `put(key, entry)` reads the snapshot from `entry.versions` (no separate `seen` argument). `evict_entity` is dropped: the invalidator owns the entity→keys index and calls `remove`. |
| G3 | The `Predictor` trait uses `SessionView`, `TenantStats` and `QueryClass`. | Define them in baid-core. |
| G4 | `Key::derive` takes both `kind` and a query, but the query already implies the kind. | Derive the kind from the query. |
| G5 | baid-core needs `async-trait`, `time` and `unicode-normalization`. | Add them; none does I/O. |
| G6 | `entity_ver` lives in cache (`versions.rs`), and invalidate bumps it. | invalidate depends on cache. |
| G7 | mcp must call the engine without depending on cache or exec. | Add a `MemoryService` trait in baid-core at T32 (with an ADR). |
| G8 | The project was renamed from GraphCache to BAiD. | Done: crate folders drop the `gc-` prefix, the core package is `baid-core` (`core` clashes with Rust's built-in crate), keys are `baid:v1:`, env vars will be `BAID_`, metrics `baid_`, the binary is `baid`. |
| G9 | Can foyer honour custom priority (ADR-005)? | Spike at T06. |
| G10 | The dependency table in the PDF is garbled. | The layer-check xtask (T05) becomes the source of truth. |
| G11 | The SRS is not used. | Targets come from the two spec tabs and this plan. |

## 2. Tasks

### Phase 0: foundation (baid-core)
- [x] **T01** IDs and cache keys: `TenantId`, `SessionId`, `EntityId`, `NodeId`, `AdapterId`; `BackendQuery`, `Filters`, `TokenBudget`, budget buckets; `normalise()`; `Key::derive` (blake3) and the `baid:v1:` string form.
- [x] **T02** Domain types: `RecallRequest`, `MemoryItem`, `CacheEntry`, `CacheOutcome`, `Candidate`, `Fact`, `WriteAck`, `BackendResult`, `Health`, `Capabilities`, `PutOutcome`, `FlushScope`, `PackedContext`, `InvalidationReport`, `SessionView`, `TenantStats`, `QueryClass`.
- [x] **T03** Errors and config structs (serde), with `validate()`.
- [x] **T04** The five traits (plus G2) and a `MockBackend` behind the `test-util` feature.
- [x] **T05** Layer-check xtask; a test that every `tokio::spawn` site takes a `CancellationToken`.

### Phase 1: vertical slice (cache-only, against a mock backend)
- [~] **T06** foyer spike, then the RAM tier: TTL, per-tenant capacity, priority, version-guarded put (G1), promote. Isolation proptest.
- [ ] **T07** Server skeleton: CLI, figment config, tenant from API key, `/healthz` `/readyz`, ApiError→HTTP, graceful shutdown.
- [ ] **T08** Engine: recall (hit/miss) and remember pass-through; read-your-writes and `max_staleness=0` bypass.
- [ ] **T09** `api/openapi.yaml` v1.
- [ ] **T10** docker-compose: Neo4j, MinIO, Prometheus, Grafana (`just up`).
- [ ] **T11** Bolt adapter, registry, timeouts, retries, circuit breaker.
- [ ] **T12** S3 adapter; adapter contract tests (testcontainers).

### Phase 2: correctness
- [ ] **T13** Single-flight loads with a version snapshot.
- [ ] **T14** Invalidator: entity→keys index, bump-then-evict.
- [ ] **T15** Write-through remember (ack, then invalidate).
- [ ] **T16** Staleness property test with an injected delay.
- [ ] **T17** Fail open (supervisor, `gc_internal_errors_total`); stale-if-error window.

### Phase 3: measuring stick (memharness)
- [ ] **T18** Datagen and the Neo4j/MinIO loaders (`just load`).
- [ ] **T19** Tracegen with think time; replay mode `off`; report (memory wait, tokens per task).
- [ ] **T20** Replay mode `cache-only`.

### Phase 4: prefetch
- [ ] **T21** Telemetry: metrics, Prometheus `/metrics`, tracing, OTLP feature.
- [ ] **T22** Session tracker: window, think-time EWMA, expiry, cancel tokens.
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
