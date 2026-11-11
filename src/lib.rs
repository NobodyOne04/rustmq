//! rustmq: a small in-memory TCP message broker.
//!
//! The library holds everything; `src/main.rs` is only the CLI. Integration tests in
//! `tests/` use this crate's public API (ADR 0001).

pub mod broker;
pub mod client;
pub mod config;
pub mod metrics;
pub mod protocol;
pub mod server;
