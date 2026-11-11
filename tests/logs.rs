//! Message bodies never reach the logs, at any level (SPEC §9, ADR 0008).
//!
//! Its own test binary (process) on purpose: `tracing` caches, per log statement, whether
//! anyone listens. Tests running alongside without a subscriber would make it cache "no"
//! and this test would capture nothing.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rustmq::broker::Broker;
use rustmq::protocol::{Limits, read_frame};
use rustmq::server::{ServerConfig, run};
use serde_json::{Value, json};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn bodies_are_never_logged() {
    let logs = Capture::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    // `#[tokio::test]` is single-threaded: the server's tasks run on this thread and log
    // through this subscriber.
    let _guard = tracing::subscriber::set_default(subscriber);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = ServerConfig {
        limits: Limits {
            max_message_size: 1024,
            max_frame_size: 1024 + 4096,
            max_queue_name_len: 255,
            max_subscriptions: 16,
            prefetch: 1,
        },
        max_connections: 16,
        shutdown_timeout: Duration::from_secs(1),
    };
    tokio::spawn(run(
        listener,
        Arc::new(Broker::new(1)),
        config,
        std::future::pending(),
    ));

    let (read, mut write) = TcpStream::connect(addr).await.unwrap().into_split();
    let mut read = BufReader::new(read);
    let mut next = async |frame: Value| -> Value {
        write
            .write_all(format!("{frame}\n").as_bytes())
            .await
            .unwrap();
        let line = read_frame(&mut read, &mut Vec::new(), 1 << 20)
            .await
            .unwrap()
            .unwrap();
        serde_json::from_slice(&line).unwrap()
    };
    next(json!({"type": "HELLO", "request_id": 1, "versions": [1]})).await;
    next(json!({"type": "SUBSCRIBE", "request_id": 2, "queue": "q"})).await;
    // The MESSAGE and the PUBLISH answer arrive in either order; both carry the delivery.
    let a =
        next(json!({"type": "PUBLISH", "request_id": 3, "queue": "q", "body": "SECRET-BODY-42"}))
            .await;
    let b = next(
        json!({"type": "PUBLISH", "request_id": 4, "queue": "bad name", "body": "SECRET-BODY-42"}),
    )
    .await;
    let message = [a, b]
        .into_iter()
        .find(|f| f["type"] == "MESSAGE")
        .expect("the delivery");
    // That read the MESSAGE and PUBLISH 3's answer; PUBLISH 4's answer is still unread, so
    // read until the ACK's own answer: then every event below has been logged.
    let mut frame =
        next(json!({"type": "ACK", "request_id": 5, "delivery_id": message["delivery_id"]})).await;
    while frame["request_id"] != 5 {
        frame = next(json!({"type": "STATS", "request_id": 6})).await;
    }
    assert_eq!(frame["type"], "RESPONSE", "{frame}");

    let logs = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    for event in [
        "connect",
        "queue created",
        "publish",
        "deliver",
        "ack",
        "request refused",
    ] {
        assert!(logs.contains(event), "no {event:?} in:\n{logs}");
    }
    assert!(!logs.contains("SECRET-BODY"), "a body was logged:\n{logs}");
}
