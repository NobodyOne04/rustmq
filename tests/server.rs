//! The server over real TCP (ADR 0004): handshake, publish/subscribe/ack, disconnects, limits
//! (ADR 0006), shutdown (ADR 0007), and
//! a client that stops reading. Tests wait on replies, never on sleeps; the timeout in
//! `recv` only turns a hang into a failure.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use rustmq::broker::{Broker, BrokerConfig};
use rustmq::protocol::{Limits, read_frame};
use rustmq::server::{ServerConfig, run};
use serde_json::{Value, json};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

fn limits(prefetch: usize, max_subscriptions: usize) -> Limits {
    Limits {
        max_message_size: 64 * 1024,
        max_frame_size: 64 * 1024 + 4096,
        max_queue_name_len: 255,
        max_subscriptions,
        prefetch,
    }
}

/// Starts a server on a free port; returns its address.
async fn start(limits: Limits) -> std::net::SocketAddr {
    start_with(limits, BrokerConfig::default(), 1024).await
}

/// Like `start`, with these queue settings (`prefetch` comes from `limits`).
async fn start_with(
    limits: Limits,
    config: BrokerConfig,
    max_connections: usize,
) -> std::net::SocketAddr {
    let server = Server::start(limits, config, max_connections, Duration::from_secs(5)).await;
    // Never stopped: the test's runtime ends it. Forgetting the sender keeps `stop` pending.
    std::mem::forget(server.stop);
    server.addr
}

/// A running server that a test can shut down.
struct Server {
    addr: std::net::SocketAddr,
    stop: oneshot::Sender<()>,
    task: JoinHandle<std::io::Result<()>>,
}

impl Server {
    async fn start(
        limits: Limits,
        config: BrokerConfig,
        max_connections: usize,
        shutdown_timeout: Duration,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let broker = Arc::new(Broker::with_config(BrokerConfig {
            prefetch: limits.prefetch,
            ..config
        }));
        let config = ServerConfig {
            limits,
            max_connections,
            shutdown_timeout,
        };
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(run(listener, broker, config, async {
            let _ = stopped.await;
        }));
        Server { addr, stop, task }
    }

    /// Signals shutdown and waits for `run` to return.
    async fn shut_down(self) -> std::io::Result<()> {
        self.stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .expect("run didn't return")
            .unwrap()
    }
}

struct Client {
    read: BufReader<OwnedReadHalf>,
    write: OwnedWriteHalf,
    next_id: u64,
    /// MESSAGEs that arrived while waiting for an answer, for `message` to return.
    pending: VecDeque<Value>,
}

impl Client {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        let (read, write) = TcpStream::connect(addr).await.unwrap().into_split();
        Client {
            read: BufReader::new(read),
            write,
            next_id: 0,
            pending: VecDeque::new(),
        }
    }

    /// Connects and completes HELLO.
    async fn hello(addr: std::net::SocketAddr) -> Self {
        let mut c = Self::connect(addr).await;
        let r = c.request(json!({"type": "HELLO", "versions": [1]})).await;
        assert_eq!(r["version"], 1, "{r}");
        c
    }

    async fn send_raw(&mut self, line: &[u8]) {
        self.write.write_all(line).await.unwrap();
        self.write.write_all(b"\n").await.unwrap();
    }

    /// Sends `frame` with a fresh `request_id`; returns that id.
    async fn send(&mut self, mut frame: Value) -> u64 {
        self.next_id += 1;
        frame["request_id"] = self.next_id.into();
        self.send_raw(frame.to_string().as_bytes()).await;
        self.next_id
    }

    /// The next frame, or `None` once the server closed the connection.
    async fn recv(&mut self) -> Option<Value> {
        let frame = tokio::time::timeout(
            Duration::from_secs(10),
            read_frame(&mut self.read, &mut Vec::new(), 1 << 20),
        )
        .await
        .expect("server didn't answer in time")
        .ok()??;
        Some(serde_json::from_slice(&frame).unwrap())
    }

    /// Sends a request and returns its answer; MESSAGEs in between are kept for `message`.
    async fn request(&mut self, frame: Value) -> Value {
        let id = self.send(frame).await;
        loop {
            let f = self.recv().await.expect("connection closed");
            if f["type"] == "MESSAGE" {
                self.pending.push_back(f);
            } else {
                assert_eq!(f["request_id"], id, "{f}");
                return f;
            }
        }
    }

    async fn publish(&mut self, queue: &str, body: &str) -> u64 {
        let r = self
            .request(json!({"type": "PUBLISH", "queue": queue, "body": body}))
            .await;
        r["message_id"].as_u64().unwrap_or_else(|| panic!("{r}"))
    }

    async fn subscribe(&mut self, queue: &str) {
        let r = self
            .request(json!({"type": "SUBSCRIBE", "queue": queue}))
            .await;
        assert_eq!(r["type"], "RESPONSE", "{r}");
    }

    async fn ack(&mut self, delivery_id: &Value) -> Value {
        self.request(json!({"type": "ACK", "delivery_id": delivery_id}))
            .await
    }

    async fn message(&mut self) -> Value {
        if let Some(f) = self.pending.pop_front() {
            return f;
        }
        let f = self.recv().await.expect("connection closed");
        assert_eq!(f["type"], "MESSAGE", "{f}");
        f
    }
}

fn code(frame: &Value) -> &str {
    assert_eq!(frame["type"], "ERROR", "{frame}");
    frame["code"].as_str().unwrap()
}

#[tokio::test]
async fn hello_must_come_first() {
    let addr = start(limits(1, 16)).await;

    let mut c = Client::connect(addr).await;
    c.send(json!({"type": "STATS"})).await;
    assert_eq!(code(&c.recv().await.unwrap()), "BAD_REQUEST");
    assert!(
        c.recv().await.is_none(),
        "closed after a non-HELLO first frame"
    );

    let mut c = Client::connect(addr).await;
    c.send_raw(b"not json").await;
    let e = c.recv().await.unwrap();
    assert_eq!((code(&e), &e["request_id"]), ("BAD_REQUEST", &Value::Null));
    assert!(c.recv().await.is_none());

    let mut c = Client::connect(addr).await;
    c.send(json!({"type": "HELLO", "versions": [9]})).await;
    assert_eq!(code(&c.recv().await.unwrap()), "UNSUPPORTED_VERSION");
    assert!(c.recv().await.is_none());
}

#[tokio::test]
async fn hello_answers_version_and_limits() {
    let addr = start(limits(3, 7)).await;
    let mut c = Client::connect(addr).await;
    let r = c
        .request(json!({"type": "HELLO", "versions": [2, 1]}))
        .await;
    assert_eq!(r["version"], 1);
    assert_eq!(r["limits"]["prefetch"], 3);
    assert_eq!(r["limits"]["max_subscriptions"], 7);
    // a second HELLO is an error, but the connection stays open
    let again = c.request(json!({"type": "HELLO", "versions": [1]})).await;
    assert_eq!(code(&again), "BAD_REQUEST");
    assert_eq!(
        c.request(json!({"type": "STATS"})).await["type"],
        "RESPONSE"
    );
}

#[tokio::test]
async fn bad_requests_keep_the_connection_open() {
    let addr = start(limits(1, 1)).await;
    let mut c = Client::hello(addr).await;

    c.send_raw(b"{\"type\":\"NOPE\"}").await;
    assert_eq!(code(&c.recv().await.unwrap()), "BAD_REQUEST");
    let r = c
        .request(json!({"type": "PUBLISH", "queue": "bad name", "body": "x"}))
        .await;
    assert_eq!(code(&r), "INVALID_QUEUE_NAME");
    let r = c
        .request(json!({"type": "PUBLISH", "queue": "q", "body": "x".repeat(64 * 1024 + 1)}))
        .await;
    assert_eq!(code(&r), "MESSAGE_TOO_LARGE");
    let r = c.request(json!({"type": "ACK", "delivery_id": 1})).await;
    assert_eq!(code(&r), "UNKNOWN_DELIVERY");

    c.subscribe("a").await;
    c.subscribe("a").await; // again: same subscription, no new slot
    let r = c.request(json!({"type": "SUBSCRIBE", "queue": "b"})).await;
    assert_eq!(code(&r), "TOO_MANY_SUBSCRIPTIONS");

    assert_eq!(
        c.request(json!({"type": "STATS"})).await["stats"]["queues"],
        1
    );
}

#[tokio::test]
async fn oversized_frame_closes_the_connection() {
    let addr = start(limits(1, 1)).await;
    let mut c = Client::hello(addr).await;
    // Just over the limit: the server reads it whole, so closing doesn't reset the socket
    // and lose the ERROR (see SPEC §14.7 on why it must close).
    c.send_raw(&vec![b'x'; 64 * 1024 + 4096 + 1]).await;
    assert_eq!(code(&c.recv().await.unwrap()), "FRAME_TOO_LARGE");
    assert!(c.recv().await.is_none());
}

#[tokio::test]
async fn publish_consume_ack() {
    let addr = start(limits(1, 16)).await;
    let mut producer = Client::hello(addr).await;
    let mut consumer = Client::hello(addr).await;
    consumer.subscribe("events").await;

    let id = producer.publish("events", "{\"event\":\"created\"}").await;
    let m = consumer.message().await;
    assert_eq!(
        (&m["queue"], &m["message_id"], &m["body"], &m["redelivered"]),
        (
            &json!("events"),
            &json!(id),
            &json!("{\"event\":\"created\"}"),
            &json!(false)
        )
    );
    assert_eq!(m["delivery_id"], 1);

    // prefetch 1: the next message waits for the ACK
    let id2 = producer.publish("events", "second").await;
    assert_eq!(consumer.ack(&m["delivery_id"]).await["type"], "RESPONSE");
    let m2 = consumer.message().await;
    assert_eq!(
        (&m2["message_id"], &m2["delivery_id"]),
        (&json!(id2), &json!(2))
    );

    assert_eq!(
        code(&consumer.ack(&m["delivery_id"]).await),
        "UNKNOWN_DELIVERY"
    );
    assert_eq!(code(&consumer.ack(&json!(99)).await), "UNKNOWN_DELIVERY");
}

#[tokio::test]
async fn a_connection_publishes_and_consumes_at_once() {
    let addr = start(limits(1, 16)).await;
    let mut c = Client::hello(addr).await;
    c.subscribe("loop").await;
    // The MESSAGE can arrive before or after the PUBLISH answer, so collect both by hand.
    let id = c
        .send(json!({"type": "PUBLISH", "queue": "loop", "body": "me"}))
        .await;
    let (a, b) = (c.recv().await.unwrap(), c.recv().await.unwrap());
    let (answer, message) = if a["type"] == "MESSAGE" {
        (b, a)
    } else {
        (a, b)
    };
    assert_eq!(answer["request_id"], id);
    assert_eq!(message["message_id"], answer["message_id"]);
}

#[tokio::test]
async fn disconnect_mid_delivery_requeues() {
    let addr = start(limits(1, 16)).await;
    let mut producer = Client::hello(addr).await;
    let mut first = Client::hello(addr).await;
    first.subscribe("q").await;
    let id = producer.publish("q", "work").await;
    assert_eq!(first.message().await["message_id"], id);

    let mut second = Client::hello(addr).await;
    second.subscribe("q").await;
    drop(first); // never acked
    let m = second.message().await;
    assert_eq!(
        (&m["message_id"], &m["redelivered"]),
        (&json!(id), &json!(true))
    );
    assert_eq!(second.ack(&m["delivery_id"]).await["type"], "RESPONSE");
}

/// A consumer that never reads its socket fills its own buffers and stalls only itself:
/// the other consumer of the same queue, and other queues, keep flowing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_reader_slows_only_itself() {
    const PREFETCH: usize = 300;
    const MESSAGES: usize = 800;
    let addr = start(limits(PREFETCH, 1)).await;

    let mut stalled = Client::hello(addr).await;
    stalled.subscribe("big").await;
    // From here on `stalled` never reads. Its share is at most PREFETCH messages, far more
    // bytes than the socket buffers hold, so its writer blocks.

    let mut producer = Client::hello(addr).await;
    let body = "x".repeat(32 * 1024);
    for _ in 0..MESSAGES {
        producer.publish("big", &body).await;
    }

    let mut other = Client::hello(addr).await;
    other.subscribe("big").await;
    for _ in 0..MESSAGES - PREFETCH {
        let m = other.message().await;
        assert_eq!(other.ack(&m["delivery_id"]).await["type"], "RESPONSE");
    }

    producer.publish("small", "still here").await;
    let mut third = Client::hello(addr).await;
    third.subscribe("small").await;
    assert_eq!(third.message().await["body"], "still here");
    drop(stalled);
}

#[tokio::test]
async fn ttl_ms_reaches_the_queue() {
    let addr = start(limits(1, 16)).await;
    let mut producer = Client::hello(addr).await;
    // TTL 0: expired by the time anyone could take it
    let r = producer
        .request(json!({"type": "PUBLISH", "queue": "q", "body": "gone", "ttl_ms": 0}))
        .await;
    assert!(r["message_id"].is_u64(), "still accepted: {r}");
    producer.publish("q", "kept").await;
    let mut consumer = Client::hello(addr).await;
    consumer.subscribe("q").await;
    assert_eq!(consumer.message().await["body"], "kept");
}

/// The ack timeout takes a delivery back from a live connection: its old `delivery_id` is
/// then unknown, the redelivery has a new one. Real clock, short timeout: the test waits on
/// the redelivery, not on a sleep.
#[tokio::test]
async fn ack_after_timeout_is_unknown() {
    let config = BrokerConfig {
        ack_timeout: Duration::from_millis(50),
        ..BrokerConfig::default()
    };
    let addr = start_with(limits(1, 16), config, 1024).await;
    let mut producer = Client::hello(addr).await;
    let mut consumer = Client::hello(addr).await;
    consumer.subscribe("q").await;
    let id = producer.publish("q", "slow work").await;

    let first = consumer.message().await;
    let mut last = first.clone();
    // Not acking: it comes back, to the same consumer, several times over.
    for _ in 0..3 {
        let again = consumer.message().await;
        assert_eq!(
            (&again["message_id"], &again["redelivered"]),
            (&json!(id), &json!(true))
        );
        assert_ne!(again["delivery_id"], last["delivery_id"]);
        last = again;
    }
    // The connection forgot the old ids: unknown without asking the queue.
    assert_eq!(
        code(&consumer.ack(&first["delivery_id"]).await),
        "UNKNOWN_DELIVERY"
    );
    // The latest may itself have timed out by now; either answer is correct.
    let r = consumer.ack(&last["delivery_id"]).await;
    assert!(
        r["type"] == "RESPONSE" || code(&r) == "UNKNOWN_DELIVERY",
        "{r}"
    );
}

#[tokio::test]
async fn connection_limit() {
    let addr = start_with(limits(1, 16), BrokerConfig::default(), 2).await;
    let mut a = Client::hello(addr).await;
    let _b = Client::hello(addr).await;

    // The third already sent HELLO when it is rejected; it must still read the ERROR.
    let mut c = Client::connect(addr).await;
    c.send(json!({"type": "HELLO", "versions": [1]})).await;
    assert_eq!(code(&c.recv().await.unwrap()), "TOO_MANY_CONNECTIONS");
    assert!(c.recv().await.is_none(), "closed");

    // Accepted connections are unaffected.
    assert_eq!(
        a.request(json!({"type": "STATS"})).await["type"],
        "RESPONSE"
    );

    // A slot frees when a connection ends. The server notices the close on its own time,
    // so retry; each attempt is a full round trip, not a sleep.
    drop(a);
    for attempt in 0.. {
        let mut d = Client::connect(addr).await;
        // Not `request`: a rejection has no `request_id` to match.
        d.send(json!({"type": "HELLO", "versions": [1]})).await;
        let r = d.recv().await.expect("an answer before the close");
        if r["type"] == "RESPONSE" {
            break;
        }
        assert_eq!(code(&r), "TOO_MANY_CONNECTIONS");
        assert!(attempt < 1000, "slot never freed");
    }
}

#[tokio::test]
async fn broker_limits_reach_the_client() {
    let config = BrokerConfig {
        max_queues: 1,
        max_queue_len: 1,
        ..BrokerConfig::default()
    };
    let addr = start_with(limits(1, 16), config, 1024).await;
    let mut c = Client::hello(addr).await;
    c.publish("q", "a").await;
    let r = c
        .request(json!({"type": "PUBLISH", "queue": "q", "body": "b"}))
        .await;
    assert_eq!(code(&r), "QUEUE_FULL");
    let r = c
        .request(json!({"type": "SUBSCRIBE", "queue": "other"}))
        .await;
    assert_eq!(code(&r), "TOO_MANY_QUEUES");
    // still open, and the failed SUBSCRIBE used no subscription slot
    c.subscribe("q").await;
    assert_eq!(c.message().await["body"], "a");
}

#[tokio::test]
async fn shutdown_closes_active_connections() {
    let server = Server::start(
        limits(1, 16),
        BrokerConfig::default(),
        1024,
        Duration::from_secs(5),
    )
    .await;
    let addr = server.addr;
    let mut producer = Client::hello(addr).await;
    let mut consumer = Client::hello(addr).await;
    consumer.subscribe("q").await;
    let id = producer.publish("q", "unacked").await;
    assert_eq!(consumer.message().await["message_id"], id);
    let mut before_hello = Client::connect(addr).await;
    let mut mid_frame = Client::hello(addr).await;
    mid_frame.write.write_all(b"{\"type\":\"STA").await.unwrap();

    server.shut_down().await.unwrap();

    for c in [
        &mut producer,
        &mut consumer,
        &mut before_hello,
        &mut mid_frame,
    ] {
        assert!(c.recv().await.is_none(), "closed by the server");
    }
    assert!(
        TcpStream::connect(addr).await.is_err(),
        "no longer accepting"
    );
}

/// A connection stuck writing to a client that doesn't read can't hold shutdown past
/// `shutdown_timeout`. (That its writer task is aborted too, not left blocked on the
/// socket, isn't visible here: draining the socket would unblock it either way.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_timeout_aborts_stuck_connections() {
    const PREFETCH: usize = 300;
    let server = Server::start(
        limits(PREFETCH, 1),
        BrokerConfig::default(),
        1024,
        Duration::from_millis(200),
    )
    .await;
    let mut stalled = Client::hello(server.addr).await;
    stalled.subscribe("big").await;
    let mut producer = Client::hello(server.addr).await;
    let body = "x".repeat(32 * 1024);
    for _ in 0..PREFETCH {
        producer.publish("big", &body).await;
    }
    // `stalled` has read nothing: its server-side writer is blocked on a full socket.
    // Queue one more answer so the reader is stuck too, behind the full outbound channel.
    for _ in 0..100 {
        stalled.send(json!({"type": "STATS"})).await;
    }

    let started = std::time::Instant::now();
    server.shut_down().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );

    // The connection ends: reading drains what was sent, then the socket closes.
    let mut frames = 0;
    while stalled.recv().await.is_some() {
        frames += 1;
    }
    assert!(frames <= PREFETCH + 100 + 1, "{frames}");
}
