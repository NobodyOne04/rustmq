# Architecture Decision Records

Files are `NNNN-title.adr` (Markdown). Lower numbers are implemented first, one PR each.

| # | Decision | Status |
|---|---|---|
| [0001](0001-single-crate-lib-and-bin.adr) | One crate: a library and the `rustmq` binary | Accepted |
| [0002](0002-wire-protocol-ndjson.adr) | Wire protocol: newline-delimited JSON with a HELLO handshake | Accepted |
| [0003](0003-queue-actor-per-queue.adr) | Queue concurrency: one task (actor) owns each queue | Accepted |
