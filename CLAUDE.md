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
- `docs/PROTOCOL.md`: the wire protocol, language-neutral; keep it in sync with `src/protocol.rs`
- `docs/adr/`: architecture decision records (`NNNN-title.adr`), index in its README
- `src/lib.rs`: everything; `src/main.rs`: only the clap CLI (ADR 0001)
- `src/broker.rs`: queue registry; `src/broker/queue.rs`: the per-queue actor (ADR 0003)
- `src/client.rs`: the typed client, one request in flight (ADR 0011)
- `src/metrics.rs`: shared atomic counters, read by STATS (ADR 0008)
- `src/server.rs`: `run(listener, broker, config, shutdown)`, the accept loop and shutdown
  (ADR 0007); per connection a reader and a writer task (ADR 0004)
- `tests/`: integration tests, through the library's public API; `tests/server.rs` over real TCP
  with raw JSON (wire format), `tests/client.rs` through `client`
  (`tests/logs.rs` is its own binary: `tracing` caches who listens per process)

## Conventions
- Staged workflow from SPEC §13: requirements → architecture (ADRs, my approval) → plan →
  implementation → review → security audit → verification. Never skip a stage or implement
  an unapproved design.
- No scope beyond SPEC §11 non-goals without an explicit scope revision.
- No new dependency without a written reason. `unsafe` is forbidden in `Cargo.toml`.
- One ADR, one PR: the ADR and its implementation; lower numbers first.
- Explain non-obvious Rust choices: this is a learning project.
- Never log a message body, at any level; log its size (`bytes`). `tests/logs.rs` checks.
- Deadlines use `tokio::time::Instant`, never `std::time::Instant`: tests pause tokio's
  clock (`start_paused`, `advance`) and std's would ignore it (ADR 0005).
