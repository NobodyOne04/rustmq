# rustmq

A small in-memory TCP message broker in Rust (Tokio, serde_json, tracing, clap): an
educational project for learning Rust and testing the Claude Code workflow.

## Commands
None yet (planned: `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test`).

## Layout
- `docs/SPEC.md`: the project specification (scope, non-goals, Definition of Done)
- `docs/adr/`: architecture decision records (`NNNN-title.adr`)

## Conventions
- Staged workflow from SPEC §13: requirements → architecture (ADRs, my approval) → plan →
  implementation → review → security audit → verification. Never skip a stage or implement
  an unapproved design.
- No scope beyond SPEC §11 non-goals without an explicit scope revision.
- No new dependency without a written reason; no `unsafe`.
- Explain non-obvious Rust choices: this is a learning project.
