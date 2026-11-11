# Changelog

## Unreleased

### Added
- Queues: one task per queue, FIFO, competing consumers with prefetch, requeue when a consumer leaves
- Wire protocol: JSON frames, HELLO handshake, error codes, documented in `docs/PROTOCOL.md`
- Rust crate skeleton (library + `rustmq` binary) and CI: fmt, clippy, tests
- Project specification in `docs/SPEC.md`, with the agreed clarifications
