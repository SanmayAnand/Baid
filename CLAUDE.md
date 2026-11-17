# BAiD: rules for Claude Code

BAiD is a Rust memory accelerator for AI agents: a cache and prefetcher between agents and
their memory backend. Read [README.md](README.md) for the overview.

- Specs live in the team doc (tabs: Architecture and design, Code architecture).
- The work plan is [docs/build-plan.md](docs/build-plan.md): tasks T01–T38 in order, plus a
  table of spec gaps and the decisions taken (G1, G2, ...). Mark a task `[~]` when you start
  it and `[x]` when `just check` is green. Record any new decision that departs from the
  spec in the gaps table.

## Git: hard rules
1. NEVER push. Do not run `git push` in any form, `gh pr create`, `gh pr merge`,
   `gh repo sync`, or anything else that sends commits to a remote. The human pushes.
2. NEVER force anything: no `--force`, no `reset --hard`, no `clean -fd`, no branch deletion,
   unless the human asks in this session and names the branch.
3. At the start of every session, run the sync check ("Session start" below) before editing,
   unless the human says to skip it.
4. Work only on a branch named `<github-handle>/<topic>`. Never commit on `main`.
5. Rebase onto `upstream/main`; never merge `main` into a branch.
6. If a rebase conflicts, stop, run `git rebase --abort`, and report the conflicting files.
   Do not resolve conflicts in files this branch did not change.
7. End every session with the report in "Session end". The human reads it, then pushes.

## Session start
- `git status` must be clean. If not, stop and report what is uncommitted.
- `git fetch upstream --prune` and `git fetch origin --prune`.
- Report how far `main` and the current branch are behind `upstream/main`, with the new
  commits' one-line summaries and which crates they touch.
- Fast-forward local `main` to `upstream/main` (`git merge --ff-only`). If that fails, stop and report.
- Rebase the working branch onto `upstream/main`, then run `just check`.

## Session end
Report: tasks done (plan ids), files changed, decisions taken, new metrics, anything blocked
or left for the human.

## Crates and layers
A crate depends only on crates in lower layers; `cargo xtask layers` enforces it.

| Layer | Crates |
| --- | --- |
| 0 foundation | `baid-core` (folder `crates/core`; no I/O, no async runtime) |
| 1 building blocks | `cache`, `session`, `predict`, `pack`, `invalidate` (may use `cache`), `adapters`, `telemetry` |
| 2 orchestration | `exec`, `mcp` |
| 3 binaries | `server` (binary `baid`), `memharness`, `xtask` |

- CODEOWNERS decides who reviews which folder. Claude may work in any crate the human asks for.
- A change to `baid-core` goes in a PR that does nothing else (plus compile fixes in callers).

## Code rules
- Follow the Code architecture tab: layer rules, traits, error types, config.
- `just check` must pass before a commit: fmt, clippy `-D warnings`, `cargo xtask all`, tests.
- No `unwrap()` or `expect()` outside tests and `main` (`clippy.toml` allows them in tests).
  No lock held across `.await`.
- Every spawned task selects on a `CancellationToken`'s `.cancelled()`; `cargo xtask spawns`
  checks this. The only escape is a `// spawn-ok: <reason>` comment on the line above.
- Every public item in `baid-core` has a doc comment (`#![deny(missing_docs)]`).
- Every new behaviour gets a test; every new metric (prefix `baid_`) is listed in the report.
- Config env vars use the prefix `BAID_`; secrets are referenced by `*_env` names, never inlined.
- Commits: Conventional Commits, small and focused, e.g. `feat(cache): add RAM tier`.

## Commands
- `just check`        fmt, clippy, layer and spawn rules, tests
- `cargo xtask all`   layer and spawn rules only (`layers` or `spawns` for one)
- `just deny`         licences, advisories, bans (needs cargo-deny)
- `just up`           start Neo4j, MinIO, Prometheus, Grafana (not set up yet)
- `just bench-small`  memharness small run (not set up yet)
