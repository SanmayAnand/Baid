# ADR-004: Write-through invalidation with entity versions, TTL as backstop

## Context

Read-your-writes and no stale puts without a distributed protocol.

## Decision

Write-through invalidation with entity versions, TTL as backstop.

## Consequences

Trade-off accepted: Writes must flow through BAiD to be instant.

## Status

Accepted. The version mechanism is amended by [ADR-011](011-write-sequence-staleness.md).

Source: architecture.md section 13. Change this decision only with a new ADR.
