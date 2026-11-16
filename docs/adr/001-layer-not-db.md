# ADR-001: BAiD is a layer, not a database

## Context

Works with every backend; no data-of-record risk; small enough to build alone.

## Decision

BAiD is a layer, not a database.

## Consequences

Trade-off accepted: Cannot speed up a backend's internal query plan.

## Status

Accepted

Source: architecture.md section 13. Change this decision only with a new ADR.
