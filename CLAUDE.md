# rustmq

A small in-memory TCP message broker in Rust (Tokio, serde_json, tracing, clap): an
educational project for learning Rust and testing the Claude Code workflow.

## Commands
```sh
cargo fmt --check                                   # CI
cargo clippy --all-targets --locked -- -D warnings  # CI
cargo test --locked                                 # CI
cargo run -- --help
```

## Layout
- `docs/SPEC.md`: the project specification (scope, non-goals, Definition of Done)
- `docs/adr/`: architecture decision records (`NNNN-title.adr`), index in its README
- `src/lib.rs`: everything; `src/main.rs`: only the clap CLI (ADR 0001)
- `tests/`: integration tests, through the library's public API

## Conventions
- Staged workflow from SPEC §13: requirements → architecture (ADRs, my approval) → plan →
  implementation → review → security audit → verification. Never skip a stage or implement
  an unapproved design.
- No scope beyond SPEC §11 non-goals without an explicit scope revision.
- No new dependency without a written reason. `unsafe` is forbidden in `Cargo.toml`.
- One ADR, one PR: the ADR and its implementation; lower numbers first.
- Explain non-obvious Rust choices: this is a learning project.
