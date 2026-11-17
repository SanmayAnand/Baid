# BAiD

**A memory accelerator layer for AI agents.** BAiD sits between agents and the memory backend they
already use (a Neo4j graph, an S3 bucket, Zep, Mem0). It serves repeated recalls from a cache,
prefetches the next recall while the LLM is still thinking, and trims what comes back to the
agent's token budget. Agent tasks get faster and cheaper with no change to the database.

BAiD is a layer, not a database. It never holds source-of-truth data: everything it caches can
be dropped and rebuilt from the backend.

> **Status: pre-alpha.** The foundation crate (`baid-core`) is built and tested. The server
> and the rest of the engine are in progress; nothing runs end to end yet. See the
> [build plan](docs/build-plan.md) for what is done and what is next.

## Why

An agent loop looks like *think → recall → think → recall*. Every recall waits on a backend
round trip, often tens to hundreds of milliseconds, and often for a question this or another
session asked a moment ago. Meanwhile the LLM spends seconds thinking, and the backend sits
idle. BAiD uses that idle time:

- **Hits are memory lookups.** Repeated and shared questions are answered from RAM.
- **Prefetch hides latency.** While the agent thinks, BAiD predicts its next recalls and loads
  them, so the next recall is a hit.
- **Packing saves tokens.** Results are deduplicated, ranked and trimmed to a budget in a
  stable order, so the agent pays for less context.

## How it works

```text
              recall / remember (HTTP, MCP, Python SDK)
 Agent  ───────────────────────────────────────────────▶  BAiD
                                                           │
   read path      tenant from API key → cache key → RAM tier ──hit──▶ packer ──▶ agent
                                                    │ miss
                                                    ▼
                                    single-flight load ──▶ backend (one call per key)

   prefetch path  after the response: session tracker → predictor → budget governor
                  → batched backend call → cached as low-priority "speculative" entries

   write path     remember → backend write → ack → bump entity versions → evict keys
```

- **One contract per side.** Agents see two calls, `recall` and `remember`. Backends implement
  one adapter trait with capability flags.
- **Tenant isolation by construction.** The tenant comes only from the API key, and a cache
  key cannot be built without one, so a cross-tenant hit is a type error, not a bug to catch.
- **Never stale after your own write.** Writes go through BAiD to the backend. After the
  backend acknowledges, BAiD bumps a version number for every entity the write touched, then
  evicts the cached results that mention them. A slow read that started before the write
  carries the old versions and is refused when it tries to fill the cache. Writes that bypass
  BAiD are bounded by the per-tenant TTL.
- **Speculation is bounded.** Prefetch spends a fixed per-tenant budget, gets a deadline equal
  to the session's think time, is cancelled the moment it stops being useful, and is the first
  thing cut under load. Wrong guesses are evicted first.
- **Fail open.** Any internal error falls back to calling the backend directly. BAiD can make
  an agent faster, never less available.

The principles, in priority order: never wrong, then fast; fail open; bounded speculation; the
backend stays the source of truth; one contract per side; measure before claiming.

## Repository layout

```text
crates/
  core/         baid-core: ids, cache keys, domain types, the five traits, config, errors
  cache/        RAM and NVMe tiers, TTL, priorities, the version-guarded put
  session/      per-session window, think-time estimate, expiry
  predict/      next-query predictors: neighbour, transition, popularity, ensemble
  pack/         dedupe, rank and trim to a token budget
  invalidate/   entity → key index; bump-then-evict on writes
  adapters/     Bolt (Neo4j), S3, later Zep and Mem0; retries, timeouts, circuit breaker
  telemetry/    metrics, traces, structured logs
  exec/         single-flight loads, prefetch executor, budget governor
  mcp/          MCP server: recall and remember tools, stdio bridge
  server/       the `baid` binary: HTTP API, admin API, wiring
  memharness/   benchmark harness: datagen, trace replay, reports
xtask/          repository checks (crate layering, cancellation of spawned tasks)
api/            OpenAPI contract for the HTTP API (planned)
python/         Python SDK (planned)
deploy/         docker-compose, Helm charts, Grafana dashboards (planned)
bench/          benchmark traces and published reports (planned)
docs/adr/       architecture decision records
docs/build-plan.md  task list, status and design decisions
```

Crates depend only on lower layers: `baid-core` → building blocks → `exec` / `mcp` →
binaries. `cargo xtask layers` enforces this in CI.

## Getting started (contributors)

Requirements: Rust stable 1.91 or newer (picked up from `rust-toolchain.toml`) and
[just](https://github.com/casey/just). Docker and
[cargo-deny](https://github.com/EmbarkStudios/cargo-deny) are needed for later steps.

```bash
just check        # fmt, clippy -D warnings, layer and spawn rules, all tests
cargo xtask all   # just the repository rules
just deny         # licences, advisories, bans
```

`just up` (local Neo4j, MinIO, Prometheus and Grafana) and `just bench-small` come with the
benchmark harness.

## Configuration

One TOML file, layered under `BAID_*` environment variables and command-line flags, validated
before the server binds a port. Secrets are never written in the file; `*_env` fields name the
variable that holds them.

```toml
[server]
listen = "0.0.0.0:7070"
admin_listen = "127.0.0.1:7071"

[cache]
ram_bytes = "2GiB"
# nvme_dir = "/var/lib/baid"   # set to turn on the disk tier
# nvme_bytes = "20GiB"

[[tenants]]
id = "acme"
api_key_env = "BAID_KEY_ACME"
adapter = "bolt"
adapter_url = "bolt://neo4j:7687"
adapter_user_env = "NEO4J_USER"
adapter_password_env = "NEO4J_PASSWORD"
ttl = "5m"              # hard upper bound on staleness
stale_if_error = "30s"  # serve expired entries this long while the backend is failing

[tenants.prefetch]
enabled = true
k = 4                   # candidates per recall
min_confidence = 0.35
calls_per_second = 20   # speculative call budget
max_concurrent = 4
safety_margin = "150ms" # subtracted from the think-time estimate
```

## Deployment shapes

One binary, three topologies; only the config changes.

| Topology | When | Notes |
| --- | --- | --- |
| Sidecar | One agent app, strict latency | RAM tier only, one tenant |
| Shared service | Many apps or users on one memory | NVMe tier on, many tenants, router hashes tenant then session |
| Local dev | Demos, benchmarks, contributors | docker-compose seeds Neo4j and MinIO |

## Documentation

- [Build plan](docs/build-plan.md): tasks, status, and departures from the spec
- [Architecture decisions](docs/adr): why Rust, why fail open, why tenant-from-key, and more
- [Contributing](CONTRIBUTING.md): branches, pull requests, code rules
- [Security](SECURITY.md): report issues privately; tenant isolation bugs are security bugs

## Licence

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
