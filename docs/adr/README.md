# Architecture Decision Records

Files are `NNNN-title.adr` (Markdown). Lower numbers are implemented first, one PR each.

| # | Decision | Status |
|---|---|---|
| [0001](0001-single-crate-lib-and-bin.adr) | One crate: a library and the `rustmq` binary | Accepted |
| [0002](0002-wire-protocol-ndjson.adr) | Wire protocol: newline-delimited JSON with a HELLO handshake | Accepted |
| [0003](0003-queue-actor-per-queue.adr) | Queue concurrency: one task (actor) owns each queue | Accepted |
| [0004](0004-connection-reader-writer.adr) | Connections: separate reader and writer; queues never wait on a socket | Accepted |
| [0005](0005-delivery-state-requeue-ttl.adr) | Delivery state: ack timeout and TTL in the queue actor | Accepted |
| [0006](0006-resource-limits.adr) | Resource limits: where each one is enforced | Accepted |
| [0007](0007-graceful-shutdown.adr) | Graceful shutdown: a watch channel, a JoinSet and a timeout | Accepted |
| [0008](0008-observability.adr) | Observability: tracing logs and atomic counters read via STATS | Accepted |
| [0009](0009-typed-errors.adr) | Errors: one enum per module, mapped to protocol codes at the edge | Accepted |
| [0010](0010-test-strategy.adr) | Tests: in-process server, reply-driven sync, paused clock | Accepted |
| [0011](0011-client-library.adr) | Client: a typed, single-owner client in the library | Accepted |
