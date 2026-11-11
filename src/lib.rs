//! rustmq: a small in-memory TCP message broker.
//!
//! The library holds everything; `src/main.rs` is only the CLI. Integration tests in
//! `tests/` use this crate's public API (ADR 0001).

pub mod protocol;
