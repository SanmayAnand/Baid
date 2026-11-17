# BAiD task runner. Run `just` to list recipes.

set windows-shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-Command"]

# List recipes
default:
    @just --list

# fmt check, clippy with warnings as errors, layer and spawn rules, all tests
check:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo xtask all
    cargo test --workspace

# Format the whole workspace
fmt:
    cargo fmt --all

# Run all tests
test:
    cargo test --workspace

# Licences, advisories, bans and sources (needs cargo-deny)
deny:
    cargo deny check

# Start Neo4j, MinIO, Prometheus and Grafana
up:
    @echo "not set up yet (see docs/build-plan.md)"

# Load a synthetic dataset into the local stack
load:
    @echo "not set up yet (see docs/build-plan.md)"

# Run the memharness benchmark
bench:
    @echo "not set up yet (see docs/build-plan.md)"

# memharness small run (referenced by CLAUDE.md)
bench-small:
    @echo "not set up yet (see docs/build-plan.md)"
