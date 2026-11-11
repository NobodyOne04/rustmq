//! The typed client (ADR 0011) against an in-process server (ADR 0010), plus a scripted fake
//! server for the one case a real server can't produce on demand: a MESSAGE arriving while
//! the client waits for an answer.

use std::sync::Arc;
use std::time::Duration;

use rustmq::broker::{Broker, BrokerConfig};
use rustmq::client::{Client, ClientError};
use rustmq::protocol::{ErrorCode, Limits, Request, ServerFrame, decode, read_frame, write_frame};
use rustmq::server::{ServerConfig, run};
use tokio::io::BufReader;
use tokio::net::TcpListener;

fn limits() -> Limits {
    Limits {
        max_message_size: 64 * 1024,
        max_frame_size: 64 * 1024 + 4096,
        max_queue_name_len: 255,
        max_subscriptions: 16,
        prefetch: 10,
    }
}

/// Starts a server on a free port; the test's runtime ends it.
async fn start() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let broker = Arc::new(Broker::with_config(BrokerConfig {
        prefetch: limits().prefetch,
        ..BrokerConfig::default()
    }));
    let config = ServerConfig {
        limits: limits(),
        max_connections: 1024,
        shutdown_timeout: Duration::from_secs(5),
    };
    tokio::spawn(run(listener, broker, config, std::future::pending()));
    addr
}

/// Turns a hang into a failure.
async fn within<T>(f: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .expect("timed out")
}

#[tokio::test]
async fn connect_keeps_the_servers_limits() {
    let addr = start().await;
    let client = within(Client::connect(addr)).await.unwrap();
    assert_eq!(client.limits(), &limits());
}

#[tokio::test]
async fn publish_subscribe_ack_and_stats() {
    let addr = start().await;
    let mut producer = within(Client::connect(addr)).await.unwrap();
    let mut consumer = within(Client::connect(addr)).await.unwrap();

    let first = within(producer.publish("jobs", "a\nb", None))
        .await
        .unwrap();
    let second = within(producer.publish("jobs", "", Some(Duration::from_secs(60))))
        .await
        .unwrap();
    assert!(second > first);

    within(consumer.subscribe("jobs")).await.unwrap();
    let m1 = within(consumer.next_message()).await.unwrap();
    let m2 = within(consumer.next_message()).await.unwrap();
    assert_eq!(
        (
            m1.queue.as_str(),
            m1.message_id,
            m1.body.as_str(),
            m1.redelivered
        ),
        ("jobs", first, "a\nb", false)
    );
    assert_eq!((m2.message_id, m2.body.as_str()), (second, ""));

    within(consumer.ack(m1.delivery_id)).await.unwrap();
    within(consumer.ack(m2.delivery_id)).await.unwrap();
    let stats = within(consumer.stats()).await.unwrap();
    assert_eq!((stats.published, stats.acked, stats.queued), (2, 2, 0));
}

#[tokio::test]
async fn server_errors_are_typed_and_the_client_stays_usable() {
    let addr = start().await;
    let mut c = within(Client::connect(addr)).await.unwrap();

    let err = within(c.publish("bad name", "x", None)).await.unwrap_err();
    assert!(
        matches!(
            err,
            ClientError::Server {
                code: ErrorCode::InvalidQueueName,
                ..
            }
        ),
        "{err:?}"
    );
    let err = within(c.ack(42)).await.unwrap_err();
    assert!(
        matches!(
            err,
            ClientError::Server {
                code: ErrorCode::UnknownDelivery,
                ..
            }
        ),
        "{err:?}"
    );
    within(c.publish("ok", "x", None)).await.unwrap();
}

#[tokio::test]
async fn messages_that_arrive_before_an_answer_are_kept_in_order() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // A fake server: answers HELLO, then sends two MESSAGEs before answering STATS.
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let (read, mut write) = socket.into_split();
        let mut read = BufReader::new(read);
        let mut buf = Vec::new();
        let mut next = async || -> Request {
            let frame = read_frame(&mut read, &mut buf, 1 << 16).await.unwrap();
            decode(&frame.unwrap()).unwrap()
        };
        let hello = next().await;
        let answer = ServerFrame::Response {
            request_id: hello.request_id(),
            version: Some(1),
            limits: Some(limits()),
            message_id: None,
            stats: None,
        };
        write_frame(&mut write, &answer).await.unwrap();

        let stats = next().await;
        for id in [1, 2] {
            let message = ServerFrame::Message {
                queue: "q".into(),
                message_id: id,
                delivery_id: id,
                body: format!("m{id}"),
                redelivered: false,
            };
            write_frame(&mut write, &message).await.unwrap();
        }
        let answer = ServerFrame::Response {
            request_id: stats.request_id(),
            version: None,
            limits: None,
            message_id: None,
            stats: Some(Default::default()),
        };
        write_frame(&mut write, &answer).await.unwrap();
        // Then a frame that answers nothing, and the end of the connection.
        write_frame(&mut write, &ServerFrame::ok(999))
            .await
            .unwrap();
        std::future::pending::<()>().await;
    });

    let mut c = within(Client::connect(addr)).await.unwrap();
    within(c.stats()).await.unwrap();
    assert_eq!(within(c.next_message()).await.unwrap().body, "m1");
    assert_eq!(within(c.next_message()).await.unwrap().body, "m2");
    let err = within(c.next_message()).await.unwrap_err();
    assert!(matches!(err, ClientError::Unexpected), "{err:?}");
}

#[tokio::test]
async fn a_closed_connection_is_reported() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // Reads HELLO, then hangs up without answering. (Hanging up with HELLO unread could
    // reset the connection instead of closing it.)
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = BufReader::new(socket);
        read_frame(&mut socket, &mut Vec::new(), 1 << 16)
            .await
            .unwrap();
    });
    let err = within(Client::connect(addr)).await.err().unwrap();
    assert!(matches!(err, ClientError::Closed), "{err:?}");
}
