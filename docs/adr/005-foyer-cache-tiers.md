# ADR-005: foyer for the RAM and NVMe tiers

## Context

BAiD needs a weighted in-memory cache with eviction that prefers to drop speculative
(prefetched, never used) entries, and later a disk tier behind it. foyer offers both in one
crate. The open question was whether foyer can honour our priorities: speculative, then
normal, then hub.

A spike against foyer 0.22.6 (2026-11-17) found:

- foyer's LRU has **two** priority lists. `Hint::Low` entries go to a list that is always
  evicted first; `Hint::Normal` entries live in a protected pool of up to
  `high_priority_pool_ratio` of capacity (default 0.9). Verified: under pressure only `Low`
  entries were evicted.
- Re-inserting a key with `Hint::Normal` moves it to the protected pool (our "promote").
- There is no third level, no TTL, no prefix or range delete.
- **An entry larger than the cache is admitted anyway** (usage went to 25x capacity).
- An `EventListener` reports every entry that leaves, with the reason (evict, replace,
  remove, clear). That is exactly what the prefetch waste metric needs.
- foyer's storage layer always builds lz4 and zstd (C). On Windows with a global `CC=gcc`
  this fails to link for the MSVC target; `.cargo/config.toml` sets an empty
  `CC_x86_64_pc_windows_msvc` so cc-rs uses MSVC.

## Decision

Use foyer's memory cache for the RAM tier, **one cache per tenant**:

- `Priority::Speculative` → `Hint::Low`; `Normal` and `Hub` → `Hint::Normal`.
- TTL is checked on read by BAiD (`CacheEntry::expires_at`); expired entries stay until LRU
  evicts them, so stale-if-error can still reach them.
- BAiD refuses entries over a quarter of one shard's capacity (`PutOutcome::TooLarge`).
- Per-tenant caches give per-tenant capacity, flush-by-tenant via `clear()`, and physical
  tenant separation.
- The version guard lives outside foyer (`VersionTable` + insert-then-recheck in `RamStore`).

The NVMe tier (plan T36) uses foyer's hybrid cache.

## Consequences

- Hubs get no extra protection over normal entries. If measurements show hubs evicted too
  often, add a separate hub pool (a second foyer cache per tenant with its own capacity).
- Tenant capacities are fixed slices; idle tenants' memory is not lent to busy ones. Config
  validation requires the slices to sum to at most `cache.ram_bytes`.
- Building BAiD needs a C compiler (MSVC on Windows) because of foyer's compression crates.

## Status

Accepted (2026-11-17), based on the spike above.
