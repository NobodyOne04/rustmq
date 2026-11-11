# rustmq

A small in-memory TCP message broker in Rust: an educational systems programming project.

## Usage

```sh
cargo build --release
target/release/rustmq server --bind 127.0.0.1:7000          # Ctrl-C stops it gracefully

# in another terminal
target/release/rustmq publish --queue events --message '{"event":"created"}'   # prints the message id
target/release/rustmq consume --queue events --count 1     # prints the body, then acks it
target/release/rustmq status                               # metrics, one `name: value` per line
```

Client commands default to `--address 127.0.0.1:7000`. `consume` without `--count` runs until
Ctrl-C; messages it hasn't acked go back to the queue. Every limit is a `server` flag (see
`rustmq server --help`; defaults in [SPEC §14.4](docs/SPEC.md)); a bad value stops the server at
start with a message naming the setting. `--log` takes `error`, `warn`, `info`, `debug` or
`trace`; logs go to stderr and never contain message bodies.

The wire protocol is in [docs/PROTOCOL.md](docs/PROTOCOL.md); you can also speak it with `nc`.
