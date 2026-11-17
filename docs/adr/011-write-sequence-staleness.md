# ADR-011: Write sequence numbers instead of per-entity version snapshots

## Context

ADR-004 keeps write-through invalidation with a TTL backstop, guarded by entity versions:
a loader snapshots the versions of the entities its query names, and a put is refused if
any moved. That leaves a hole. A search such as "renewal dates" names no entity, but its
results mention `acme`. If `remember("acme renews in May")` is acknowledged while that load
is in flight, the snapshot (empty) still matches, the put succeeds, and pre-write data about
acme is served until the TTL. The entities a result depends on are only known after the
load returns, too late to snapshot their versions.

## Decision

Each tenant has a **write sequence number**. After the backend acknowledges a write, BAiD
takes the next number and stamps every entity the write touched with it. A load reads the
current number *before* calling the backend and stores it in the entry (`loaded_at`). An
entry is valid only while none of its entities, whether named by the query or mentioned
by the results, carries a stamp greater than `loaded_at`. The cache checks this on put
(before and after the insert) and on **every get**.

ADR-004's choices stand: writes are write-through and the TTL is the backstop. Only the
mechanism changes.

## Consequences

- The hole is closed: a stamp on a result-only entity invalidates the entry.
- Correctness no longer depends on the entity→key index; the invalidator (plan T14)
  becomes a memory optimisation that frees invalidated entries early.
- Every hit costs one map lookup per distinct entity in the entry (typically a few dozen).
- A write can still *add* a result to another session's cached search (a new fact that now
  matches). That is bounded by the TTL, as for writes made outside BAiD; the writing
  session itself always reloads (read-your-writes).
- Write failures that may have been applied (timeouts, resets) also stamp their entities.

## Status

Accepted (2026-11-17). Amends ADR-004.
