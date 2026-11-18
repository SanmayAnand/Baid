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

compose := "docker compose -f deploy/docker-compose.yml"

# Start Neo4j, SeaweedFS (S3), Prometheus and Grafana, and wait until all are healthy
up:
    {{compose}} up -d --wait
    {{compose}} exec -T seaweedfs sh -c "echo s3.bucket.create -name baid | weed shell -master=localhost:9333"

# Stop the local stack (data volumes are kept)
down:
    {{compose}} down

# Status of the local stack
ps:
    {{compose}} ps

# Follow the local stack's logs (optionally one service: `just logs neo4j`)
logs *service:
    {{compose}} logs -f {{service}}

# Integration tests against the local stack (needs `just up`)
test-live $BAID_TEST_NEO4J_URL="bolt://127.0.0.1:7687" $BAID_TEST_NEO4J_PASSWORD="baid-dev-password" $BAID_TEST_S3_URL="s3://baid/contract?endpoint=http://127.0.0.1:8333":
    cargo test -p adapters --test bolt_fresh_schema --test bolt_live --test contract -- --test-threads=4

# Load a synthetic dataset into the local stack
load:
    @echo "not set up yet (see docs/build-plan.md)"

# Run the memharness benchmark
bench:
    @echo "not set up yet (see docs/build-plan.md)"

# memharness small run
bench-small:
    @echo "not set up yet (see docs/build-plan.md)"
