# Contributing to BAiD

One shared repository is the source of truth. Each person works in a fork, on short branches,
and changes land only through reviewed pull requests. [CODEOWNERS](.github/CODEOWNERS) decides
who reviews which folder. The work plan, with task ids and status, is
[docs/build-plan.md](docs/build-plan.md).

## Setup

You need Rust (stable, 1.91 or newer; `rust-toolchain.toml` picks it), [just](https://github.com/casey/just),
Docker (for the local stack, later) and, optionally, [cargo-deny](https://github.com/EmbarkStudios/cargo-deny).

```bash
just check   # fmt, clippy -D warnings, layer and spawn rules, tests
```

## Remotes

| Remote | Points to | Who writes to it |
| --- | --- | --- |
| `upstream` | The shared repo | Nobody directly: changes land only by merged pull requests |
| `origin` | Your own fork | You, by `git push` |

One-time setup in each clone:

```bash
git clone git@github.com:YOUR_USER/BAiD.git
cd BAiD
git remote add upstream git@github.com:froth-labs/BAiD.git
git fetch upstream
git branch --set-upstream-to=upstream/main main
```

If you skip forks, `origin` is the shared repo; read `upstream` as `origin` everywhere below.

## Branches

- Name branches `<github-handle>/<topic>`, for example `sanmayanand/cache-ram-tier`.
- Never commit on `main`. It is protected: pull request required, one approval, CI green,
  linear history.
- A branch lives at most 3 days.

## Rebase, never merge

Bring your branch up to date with `git fetch upstream` and `git rebase upstream/main`. Never
merge `main` into a branch. After a rebase, push with `git push --force-with-lease`, never
`--force`.

## Pull requests

- Keep them small: under about 400 changed lines. Split bigger work.
- Run `just check` before opening one. CI runs the same checks.
- Someone other than the author reviews every pull request; CODEOWNERS requests them.
- We squash-merge. The PR title becomes the commit message, in Conventional Commits form:
  `feat(cache): add RAM tier`, `fix(exec): ...`, `docs: ...`.
- Fill in the pull request template, including the plan task ids (T01, T02, ...).

## Code rules

- **Layers.** Crates depend only on lower layers (`baid-core` → building blocks →
  `exec`/`mcp` → binaries). `cargo xtask layers` fails on a forbidden edge. To add a crate,
  add it to the table in `xtask/src/main.rs`.
- **`baid-core` changes** go in their own small pull request: `baid-core` plus the compile
  fixes in its callers, nothing else. Every public item there needs a doc comment.
- **No `unwrap()` / `expect()`** outside tests and `main`. No lock held across `.await`.
- **Every spawned task** selects on a `CancellationToken`. `cargo xtask spawns` checks it; a
  justified exception carries `// spawn-ok: <reason>` on the line above.
- **Every behaviour has a test.** Tenant isolation and staleness are guarded by property tests.
- **Every feature ships with its metric** (prefix `baid_`), and is done only when memharness
  shows it does not make accuracy, cost or p95 latency worse.

## Claude Code

Claude Code prepares changes locally; a human pushes. It never pushes, never opens or merges
pull requests, and never forces anything. [CLAUDE.md](CLAUDE.md) states the rules and
[.claude/settings.json](.claude/settings.json) blocks those commands at the tool level. Read
Claude's session report and the diff before you push. Personal overrides go in
`CLAUDE.local.md` or `.claude/settings.local.json`, which are not committed.

## Architecture decisions

A change to a recorded decision needs a new ADR in [docs/adr](docs/adr), copied from
`000-template.md`. Smaller departures from the spec are recorded in the gaps table at the top
of `docs/build-plan.md`.

## Security

Tenant isolation bugs are security bugs. Report them privately; see [SECURITY.md](SECURITY.md).
