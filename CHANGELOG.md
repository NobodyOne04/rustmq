# Changelog

## Unreleased

### Added
- Graceful shutdown: stop accepting, close every connection, wait at most `shutdown_timeout`, then abort
- Resource limits: connections, queues, queue length and total bytes held, each with its own error
- Ack timeout (unacked deliveries go back to the front) and message TTL, both checked by the queue's own task
- TCP server: a reader and a writer task per connection, so a client that stops reading slows only itself
- Queues: one task per queue, FIFO, competing consumers with prefetch, requeue when a consumer leaves
- Wire protocol: JSON frames, HELLO handshake, error codes, documented in `docs/PROTOCOL.md`
- Rust crate skeleton (library + `rustmq` binary) and CI: fmt, clippy, tests
- Project specification in `docs/SPEC.md`, with the agreed clarifications

### Changed
- ADRs 0001–0003 rewritten in the new template (purpose, technical details, technologies), with the details of what was built

### Fixed
- A huge `ttl_ms` (or ack timeout) no longer panics the queue's task and leaves the queue answering INTERNAL
