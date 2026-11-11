# rustmq protocol, version 1

How a client talks to a rustmq broker over TCP. Any language with a socket and a JSON
library can implement it. Decisions behind it: [ADR 0002](adr/0002-wire-protocol-ndjson.adr).

## Framing
- A connection carries **frames**: one compact JSON object, then `\n` (byte `0x0A`).
- JSON escapes newlines inside strings, so a raw `\n` only ever ends a frame.
- A frame (without its `\n`) is at most `max_frame_size` bytes, which is
  `max_message_size + 4096`. The server announces both in the HELLO response.
- A frame over the limit: the server sends `ERROR FRAME_TOO_LARGE` and closes the
  connection, because it cannot reliably find where that frame ends.
- Note: `max_message_size` counts the body's UTF-8 bytes; escaping can make the frame bigger
  (a control character becomes `\u0000`, 6 bytes). A body full of them can hit the frame
  limit first.
- Every frame has a `"type"`. Field names and types are exact; unknown fields are an error.
- The connection stays open for any number of requests.

## Session
1. Client sends `HELLO` as its first frame, within the server's `hello_timeout` (default 10 s)
   of connecting; otherwise it gets `ERROR BAD_REQUEST` (no `request_id`) and is closed.
2. Server answers `RESPONSE` with the chosen version and its limits, or `ERROR
   UNSUPPORTED_VERSION` and closes. Any other first frame: `ERROR BAD_REQUEST` and close.
3. Client sends any requests, in any order, without waiting for answers. Each answer
   (`RESPONSE` or `ERROR`) carries the request's `request_id`.
4. After a `SUBSCRIBE`, the server also pushes `MESSAGE` frames at any time.
5. Either side closes the connection to end it. Unacked messages go back to their queue.

## IDs
All IDs are unsigned 64-bit integers.

| ID | Set by | Meaning |
|---|---|---|
| `request_id` | client | Echoed in the answer. The server doesn't check that it is unique. |
| `message_id` | server | One per published message, unique on the server. Unchanged on redelivery, so consumers can drop duplicates. |
| `delivery_id` | server | One per delivery, unique within the connection. A redelivery gets a new one. `ACK` names it. |

## Client → server

### HELLO
```json
{"type":"HELLO","request_id":1,"versions":[1]}
```
`versions`: the protocol versions the client speaks.

Response:
```json
{"type":"RESPONSE","request_id":1,"version":1,"limits":{"max_message_size":65536,"max_frame_size":69632,"max_queue_name_len":255,"max_subscriptions":16,"prefetch":1}}
```

### PUBLISH
```json
{"type":"PUBLISH","request_id":2,"queue":"events","body":"{\"event\":\"created\"}","ttl_ms":60000}
```
- `queue`: 1 to `max_queue_name_len` bytes of `A-Z a-z 0-9 . _ -`. Created on first use.
- `body`: any UTF-8 string, at most `max_message_size` bytes. The broker never looks inside.
- `ttl_ms` (optional): drop the message if it hasn't been handed out within this time.
  Without it the server's default applies (none unless configured).

Response, sent only once the message is in the queue:
```json
{"type":"RESPONSE","request_id":2,"message_id":17}
```

### SUBSCRIBE
```json
{"type":"SUBSCRIBE","request_id":3,"queue":"events"}
```
Response `{"type":"RESPONSE","request_id":3}`, then `MESSAGE` frames. A connection may
subscribe to at most `max_subscriptions` queues. Consumers of one queue compete: each message
goes to one of them.

### ACK
```json
{"type":"ACK","request_id":4,"delivery_id":1}
```
Confirms a delivery was processed; the message is then gone. Answer `RESPONSE`, or
`ERROR UNKNOWN_DELIVERY` if this connection has no such unacked delivery (already acked,
timed out and requeued, or never sent).

### STATS
```json
{"type":"STATS","request_id":5}
```
Response:
```json
{"type":"RESPONSE","request_id":5,"stats":{"active_connections":3,"queues":2,"published":120,"delivered":118,"acked":110,"redelivered":4,"expired":0,"queued":6}}
```

## Server → client

### RESPONSE
Success. `request_id`, plus the fields listed above for the request it answers.

### ERROR
```json
{"type":"ERROR","request_id":2,"code":"QUEUE_FULL","message":"queue or broker is full"}
```
`request_id` is absent when the request couldn't be parsed. `message` is a fixed text for the
code; it never contains internal details.

| Code | When | Connection |
|---|---|---|
| `BAD_REQUEST` | not JSON, unknown type or field, wrong field type, HELLO missing or repeated | open (closed if HELLO is missing) |
| `UNSUPPORTED_VERSION` | no common version in HELLO | closed |
| `FRAME_TOO_LARGE` | frame over `max_frame_size` | closed |
| `MESSAGE_TOO_LARGE` | body over `max_message_size` | open |
| `INVALID_QUEUE_NAME` | queue name empty, too long or bad characters | open |
| `QUEUE_FULL` | queue at its length limit, or broker at its memory limit | open |
| `TOO_MANY_QUEUES` | publishing/subscribing would create a queue past the limit | open |
| `TOO_MANY_SUBSCRIPTIONS` | connection at its subscription limit | open |
| `TOO_MANY_CONNECTIONS` | server at its connection limit (sent instead of a HELLO answer) | closed |
| `UNKNOWN_DELIVERY` | ACK for a delivery this connection doesn't hold | open |
| `INTERNAL` | unexpected server error | open |

### MESSAGE
```json
{"type":"MESSAGE","queue":"events","message_id":17,"delivery_id":1,"body":"{\"event\":\"created\"}","redelivered":false}
```
- At most `prefetch` unacked messages per subscription are in flight; the next comes after an ACK.
- `redelivered: true`: an earlier delivery of this message wasn't acked.

## Delivery guarantees
- **At least once.** A message can arrive more than once (after a disconnect or ack timeout);
  use `message_id` to drop duplicates.
- **One holder at a time.** A message is never unacked at two consumers at once.
- **FIFO hand-out per queue.** Messages are handed out in publish order; a message that comes
  back unacked goes to the front. With several consumers, the order they *finish* in is not
  guaranteed.
- **Queues are independent.** Nothing on one queue delays another.

## Not in version 1
TLS, authentication, unsubscribe (disconnect instead), persistence. See SPEC §11 and §14.7.

## Try it by hand
```sh
nc localhost 7000
{"type":"HELLO","request_id":1,"versions":[1]}
{"type":"SUBSCRIBE","request_id":2,"queue":"events"}
```
