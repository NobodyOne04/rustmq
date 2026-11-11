# Project Specification: Mini Message Broker

**Working name:** RustMQ  
**Language:** Rust  
**Project type:** Educational systems programming project  
**Estimated scope:** 1–2 weeks  
**Primary objective:** Learn Rust through practical implementation of a concurrent message broker and validate the engineering workflow configured in Claude Code.

---

## 1. Project Overview

Build a lightweight TCP-based message broker supporting asynchronous message producers and consumers.

The broker must allow multiple independent clients to publish messages to named queues and consume messages from those queues.

The initial implementation will use in-memory storage. No persistence, clustering, distributed consensus, or external database is required.

The project should prioritize:
- Correctness over feature count.
- Explicit concurrency guarantees.
- Clear separation of responsibilities.
- Idiomatic Rust.
- Testability.
- Minimal dependencies.

This is an educational implementation, not an attempt to replace RabbitMQ or Kafka.

## 2. Functional Requirements

### 2.1 Broker Server

The application must:
- Start a TCP server on a configurable address and port.
- Accept multiple simultaneous client connections.
- Support graceful shutdown.
- Handle client disconnections without crashing.
- Reject malformed requests safely.
- Support configurable message size and connection limits.

### 2.2 Queues

The broker must:
- Support multiple named queues.
- Create queues automatically when a producer or consumer first references them.
- Maintain FIFO message ordering within each queue.
- Support multiple producers and consumers per queue.
- Keep queues isolated from one another.

Queue names must be validated and have configurable length limits.

### 2.3 Producers

A producer must be able to:
- Connect to the broker.
- Publish a message to a named queue.
- Receive an explicit response indicating whether the broker accepted the message.
- Disconnect cleanly.

The broker must not acknowledge a message before successfully accepting it into the in-memory queue.

### 2.4 Consumers

A consumer must be able to:
- Subscribe to a named queue.
- Receive messages asynchronously.
- Acknowledge successfully processed messages.
- Continue consuming until disconnection or cancellation.

Messages must not be delivered to multiple consumers simultaneously.

For the initial implementation, use at-most-one active delivery per message.

Unacknowledged messages must be requeued when a consumer disconnects or loses its connection.

The broker must support multiple concurrent consumers competing for messages from the same queue.

### 2.5 Message Lifecycle

Implement the following lifecycle:

1. Producer publishes a message.
2. Broker validates and enqueues it.
3. Consumer receives the message.
4. Consumer processes the message.
5. Consumer sends an acknowledgment.
6. Broker removes the message from the unacknowledged delivery state.

If a consumer disconnects before acknowledgment, the message must return to the queue.

No guarantee of exactly-once processing is required. Redelivery can result in duplicate processing.

### 2.6 Queue Isolation

Messages published to one queue must never be delivered to another queue.

A slow consumer on one queue must not block producers or consumers operating on unrelated queues.

## 3. Network Protocol

Implement a small application-level protocol over TCP.

Requirements:
- Explicit message framing.
- Versioned protocol.
- JSON payload encoding.
- Maximum frame size.
- Request-response correlation through request IDs.
- Explicit error responses.
- Support for persistent TCP connections.

Define protocol messages for:
- PUBLISH
- SUBSCRIBE
- ACK
- MESSAGE
- RESPONSE
- ERROR

The protocol specification must be documented independently of implementation details.

Do not use HTTP or WebSockets. The objective is to practice network programming.

## 4. Architecture

Use a modular architecture with clear boundaries.

Suggested logical components:

- **Server:** TCP listener and connection lifecycle.
- **Protocol:** Framing, serialization, request validation.
- **Broker:** Core message routing and coordination.
- **Queue:** FIFO storage and delivery state.
- **Consumer:** Subscription and acknowledgment handling.
- **Producer:** Client-side publishing interface.
- **Configuration:** Runtime configuration.
- **Observability:** Structured logs and basic metrics.

These are logical responsibilities, not mandatory one-to-one Rust crates.

The architecture must be proposed and reviewed before implementation.

Avoid unnecessary abstraction and premature microservice-style decomposition.

## 5. Technology Constraints

Required:
- Rust stable.
- Tokio for asynchronous runtime.
- Serde and serde_json for serialization.
- Tracing for structured logging.
- Clap for CLI argument parsing.
- Thiserror or an equivalent typed error approach.

Use standard Rust synchronization primitives or Tokio synchronization primitives where appropriate.

Avoid adding dependencies without a clear technical justification.

Do not introduce unsafe Rust unless there is a documented and reviewed necessity. The initial implementation should ideally contain no unsafe code.

## 6. CLI

The application must support:

**Server:**
- Start broker.
- Configure bind address.
- Configure maximum connections.
- Configure maximum message size.
- Configure logging verbosity.

**Client:**
- Publish a message to a queue.
- Consume messages from a queue.
- Acknowledge received messages.

Example interface:

```bash
rustmq server --bind 127.0.0.1:7000

rustmq publish \
  --address 127.0.0.1:7000 \
  --queue events \
  --message '{"event":"created"}'

rustmq consume \
  --address 127.0.0.1:7000 \
  --queue events
```

The CLI should be usable for manual integration testing.

## 7. Reliability and Concurrency

The implementation must guarantee:
- No simultaneous delivery of the same message to multiple consumers.
- FIFO ordering of queued messages.
- Safe concurrent publishing and consumption.
- Correct handling of disconnects.
- No indefinite blocking of unrelated queues.
- Graceful cancellation of background tasks.
- Bounded memory consumption through configurable limits.

Document the exact concurrency guarantees and their scope.

## 8. Testing Requirements

### Unit tests
- Protocol serialization and deserialization.
- Frame parsing.
- Queue operations.
- Message state transitions.
- Queue name validation.
- Error handling.

### Integration tests
- Publish and consume a message.
- Multiple producers.
- Multiple consumers.
- FIFO ordering.
- Multiple independent queues.
- Consumer disconnect before acknowledgment.
- Message redelivery.
- Malformed requests.
- Oversized messages.
- Server shutdown with active connections.

### Concurrency tests
- Concurrent publishing to the same queue.
- Multiple consumers competing for messages.
- No duplicate simultaneous delivery.
- Queue isolation under concurrent load.

Tests must be deterministic wherever practical. Avoid relying on arbitrary sleeps for synchronization.

## 9. Observability

Implement structured logging for:
- Client connections and disconnections.
- Queue creation.
- Message publishing.
- Message delivery.
- Acknowledgments.
- Redelivery.
- Protocol errors.
- Server shutdown.

Expose basic runtime metrics:
- Active connections.
- Number of queues.
- Total published messages.
- Total delivered messages.
- Total acknowledgments.
- Total redeliveries.
- Current queued messages.

Metrics can initially be exposed through a local CLI status command or an internal metrics interface. A Prometheus endpoint is optional and should not be implemented unless justified.

Never log message contents by default.

## 10. Security Requirements

- Validate all incoming requests.
- Enforce frame and message size limits.
- Limit concurrent connections.
- Validate queue names.
- Avoid unbounded in-memory growth.
- Handle malformed JSON without crashing.
- Avoid exposing internal errors directly to remote clients.
- Prevent resource exhaustion through uncontrolled subscriptions or pending deliveries.

TLS, authentication, authorization, and encryption are explicitly out of scope for version 1.

Document these limitations.

## 11. Non-Goals

Do not implement:
- Persistent storage.
- Disk-based message recovery.
- Replication.
- Clustering.
- Distributed consensus.
- Dead-letter queues.
- Message priorities.
- Delayed delivery.
- Topic exchanges.
- TLS.
- Authentication.
- Management web UI.
- Kafka compatibility.
- Exactly-once processing.

These features must not be added without an explicit scope revision.

## 12. Definition of Done

The project is considered complete when:

1. The broker runs as a standalone executable.
2. Multiple producers and consumers can connect concurrently.
3. Messages can be published, delivered, and acknowledged.
4. Unacknowledged messages are redelivered after consumer disconnection.
5. FIFO and queue isolation guarantees are verified by tests.
6. Malformed and oversized requests are handled safely.
7. Graceful shutdown works.
8. Unit and integration tests pass.
9. Clippy reports no unresolved warnings.
10. Formatting checks pass.
11. No known critical or high-severity security issues remain.
12. Architecture, protocol, limitations, and usage are documented.

---

## 13. Engineering Workflow

This project must be developed using the configured Claude Code engineering workflow and available Skills.

Follow these stages:

**Stage 1 — Requirements Review**
- Review the specification.
- Identify ambiguity, missing requirements, and contradictions.
- Propose necessary clarifications.
- Do not write implementation code.

**Stage 2 — Architecture**
- Design the system architecture.
- Define module responsibilities and interfaces.
- Document concurrency and message lifecycle semantics.
- Identify technical risks.
- Record significant architectural decisions as ADRs.
- Obtain approval before implementation.

**Stage 3 — Implementation Planning**
- Break architecture into independently testable milestones.
- Define acceptance criteria for each milestone.
- Identify dependencies between tasks.
- Keep implementation increments small.

**Stage 4 — Implementation**
- Implement incrementally.
- Add tests alongside functionality.
- Run formatting, tests, and static analysis.
- Avoid unrelated changes.
- Do not silently expand scope.

**Stage 5 — Code Review**
- Review correctness, ownership, concurrency, error handling, and maintainability.
- Fix confirmed issues.
- Document unresolved concerns.

**Stage 6 — Security Audit**
- Perform an explicit security audit.
- Check input validation, memory limits, network handling, resource exhaustion, and concurrency safety.
- Produce a security report in the repository.

**Stage 7 — Final Verification**
- Run the complete test suite.
- Run Clippy and formatting checks.
- Verify all Definition of Done requirements.
- Update documentation.

### Important workflow constraints

- Do not skip engineering stages.
- Do not implement architecture before its review and approval.
- Do not assume approval of proposed architectural decisions.
- Do not create unnecessary documentation or abstractions.
- Do not add features outside the agreed scope.
- Explain non-obvious Rust-specific design decisions so the developer can learn from them.
- Treat this as a collaborative learning project, not an autonomous code-generation exercise.

---

## 14. Clarifications (Stage 1, agreed 2026-11-11)

Answers to the requirements review. Where they differ from sections 1–13, this section wins.

### 14.1 Delivery semantics
- **At-least-once.** A message has at most one consumer holding it unacked at a time (§2.4
  "at-most-one active delivery" means this, not at-most-once delivery). Redelivery can duplicate.
- **FIFO** is the order messages are handed out. A requeued message goes back to the **front**
  of its queue. With competing consumers the order they finish processing is not guaranteed.
- **Requeue** happens when the consumer disconnects or its `ack_timeout` runs out; both count
  as a redelivery.
- **Cancelling** a subscription means disconnecting. No UNSUBSCRIBE in v1.
- A connection can publish and subscribe at once, up to `max_subscriptions` queues.

### 14.2 Protocol
- **Framing:** newline-delimited JSON, one compact JSON object per line. Frame limit is
  `max_message_size + 4 KiB`.
- **Versioning:** the first frame on a connection is `HELLO {versions:[…]}`. The server answers
  with the chosen version and its limits, or `ERROR UNSUPPORTED_VERSION` and closes.
- **Message types:** client sends HELLO, PUBLISH, SUBSCRIBE, ACK, STATS; server sends
  RESPONSE, ERROR, MESSAGE.
- **Body:** a UTF-8 string the broker never parses; its size in bytes is checked against
  `max_message_size`.
- **IDs** (all u64):
  - `request_id`: picked by the client, echoed in RESPONSE/ERROR, not checked for uniqueness.
  - `message_id`: picked by the server from one global counter, returned on PUBLISH, carried
    by every MESSAGE, unchanged on redelivery so consumers can drop duplicates.
  - `delivery_id`: per connection, new for every delivery. ACK names a `delivery_id`; an
    unknown or timed-out one gets `ERROR UNKNOWN_DELIVERY`.
- **Over the connection limit:** an ERROR frame, then close.

### 14.3 Scope changes (approved)
- **TTL:** PUBLISH may carry `ttl_ms`; otherwise `default_ttl_ms` applies (none by default),
  capped by `max_ttl_ms`. Expiry is checked only when a message is about to be handed out; a
  message a consumer holds unacked never expires. Expired messages are dropped and counted
  in an `expired` metric (no dead-letter queue, §11 still applies).
- **Ack timeout:** an unacked delivery older than `ack_timeout` is requeued. `0` turns it off.
- **HELLO** and **STATS** message types; STATS backs a `rustmq status` CLI command for metrics.

### 14.4 Configuration
Every limit is a setting (CLI flag on `rustmq server`), with these defaults:

| Setting | Default | Notes |
|---|---|---|
| `bind` | `127.0.0.1:7000` | |
| `max_connections` | 1024 | |
| `max_message_size` | 64 KiB | at most 16 MiB |
| `max_queues` | 1024 | queues are never deleted in v1 |
| `max_queue_len` | 10 000 | ready messages per queue; full → `QUEUE_FULL` |
| `max_total_bytes` | 256 MiB | all held messages, ready and unacked: body + 128 bytes each (ADR 0014); full → `QUEUE_FULL` (see 14.6) |
| `max_queue_name_len` | 255 | names match `[A-Za-z0-9._-]+` |
| `max_subscriptions` | 16 | per connection |
| `prefetch` | 1 | unacked deliveries per subscription |
| `ack_timeout` | 30 s | `0` = off |
| `default_ttl_ms` / `max_ttl_ms` | none / none | |
| `shutdown_timeout` | 5 s | |
| `log` | `info` | |

### 14.5 CLI
- `rustmq consume` prints each message, then acks it; `--count N` exits after N messages.
  There is no separate `ack` command: an ack only counts on the connection that got the message.
- `rustmq status` prints the metrics (via STATS).

### 14.6 Layout
One crate: a library (`config`, `protocol`, `broker`, `server`, `client`, `metrics`) and a
`rustmq` binary that is only the clap CLI. Integration tests in `tests/` use the library and
the `client` module.

`max_total_bytes` is new in this section: without it, the other limits allow
1024 queues × 10 000 × 64 KiB ≈ 640 GiB, which does not meet "bounded memory" (§7). Accepted in ADR 0006.

### 14.7 Known limitations and tech debt
- **Oversized frame closes the connection.** With newline framing the server cannot reliably
  skip the rest of the frame, so it sends `ERROR FRAME_TOO_LARGE` and closes. A length prefix
  would allow skipping it; revisit if clients need to survive this.
- **Shutdown drops messages.** The server stops accepting, closes connections and waits at most
  `shutdown_timeout`; queued and unacked messages are lost (in-memory only). No draining.
- **No TLS, authentication or authorization** (§10): anyone who can reach the port can publish
  and consume. Bind to localhost or a trusted network only.
