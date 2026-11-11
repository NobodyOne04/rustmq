//! The TCP server: accepts connections and runs each one as a reader task and a writer
//! task joined by a bounded channel, so a slow client only slows itself (ADR 0004).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::BufReader;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::broker::{Broker, BrokerError, ConsumerId, Delivery};
use crate::protocol::{
    ErrorCode, Limits, ProtocolError, Request, ServerFrame, Stats, decode, negotiate, read_frame,
    write_frame,
};

/// Frames waiting for the writer. When full, the reader waits: back-pressure on the client
/// that isn't reading its answers, and on no one else.
const OUTBOUND_BUFFER: usize = 64;

/// Accepts connections until accepting fails for good.
///
/// `limits.prefetch` must be the `prefetch` the broker was built with: the delivery
/// channel is sized from it.
pub async fn serve(
    listener: TcpListener,
    broker: Arc<Broker>,
    limits: Limits,
) -> std::io::Result<()> {
    loop {
        // shortcut: an accept error (e.g. out of file descriptors) stops the server;
        // ADR 0006 bounds connections and ADR 0008 adds the log to tell why.
        let (stream, _peer) = listener.accept().await?;
        tokio::spawn(connection(stream, broker.clone(), limits.clone()));
    }
}

async fn connection(stream: TcpStream, broker: Arc<Broker>, limits: Limits) {
    // Rust note: `into_split` gives two owned halves, so the reader and the writer can live
    // in different tasks without sharing (or locking) the stream.
    let (read, write) = stream.into_split();
    let (outbound, outbound_rx) = mpsc::channel(OUTBOUND_BUFFER);
    let writer = tokio::spawn(write_loop(write, outbound_rx));

    let consumer = broker.new_consumer_id();
    // Every queue's credit (`prefetch`) bounds what it can have in this channel, so it never
    // fills unless a queue misbehaves; then that queue drops us as too slow (ADR 0003).
    let (deliveries_tx, deliveries) =
        mpsc::channel((limits.max_subscriptions * limits.prefetch).max(1));
    let mut conn = Connection {
        broker,
        limits,
        consumer,
        outbound,
        deliveries_tx,
        subscriptions: Vec::new(),
        in_flight: HashMap::new(),
        next_delivery_id: 0,
    };
    conn.read_loop(BufReader::new(read), deliveries).await;

    for queue in &conn.subscriptions {
        conn.broker.consumer_gone(queue, consumer).await;
    }
    // Dropping the last sender lets the writer send what is queued, then finish and close.
    drop(conn);
    let _ = writer.await;
}

/// Writes frames until the channel closes or the client is gone.
async fn write_loop(mut write: OwnedWriteHalf, mut frames: mpsc::Receiver<ServerFrame>) {
    while let Some(frame) = frames.recv().await {
        if write_frame(&mut write, &frame).await.is_err() {
            // The reader notices when its next send fails (the receiver is dropped here).
            return;
        }
    }
}

struct Connection {
    broker: Arc<Broker>,
    limits: Limits,
    consumer: ConsumerId,
    outbound: mpsc::Sender<ServerFrame>,
    deliveries_tx: mpsc::Sender<Delivery>,
    subscriptions: Vec<String>,
    /// `delivery_id` → the queue and its tag, to route an ACK. A queue that takes a delivery
    /// back (ack timeout) doesn't tell us; `on_delivery` drops such entries.
    in_flight: HashMap<u64, (Arc<str>, u64)>,
    next_delivery_id: u64,
}

/// What the reader does after a frame.
enum Next {
    Continue,
    Close,
}

impl Connection {
    async fn read_loop(
        &mut self,
        mut reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
        mut deliveries: mpsc::Receiver<Delivery>,
    ) {
        let mut buf = Vec::new();
        let mut hello_done = false;
        loop {
            // Rust note: `select!` polls both futures and runs the branch of whichever is
            // ready first, dropping the other. `read_frame` keeps a half-read frame in `buf`,
            // so losing the race to a delivery loses no bytes.
            let next = tokio::select! {
                frame = read_frame(&mut reader, &mut buf, self.limits.max_frame_size) => {
                    match frame {
                        Ok(Some(frame)) => self.on_frame(&frame, &mut hello_done).await,
                        Ok(None) => Next::Close,
                        Err(ProtocolError::Io(_)) => Next::Close,
                        // The stream is mid-frame: say why, then close (PROTOCOL.md).
                        Err(e) => {
                            let _ = self.outbound.send(ServerFrame::error(None, e.code())).await;
                            Next::Close
                        }
                    }
                }
                Some(d) = deliveries.recv() => self.on_delivery(d).await,
            };
            if let Next::Close = next {
                return;
            }
        }
    }

    async fn on_frame(&mut self, frame: &[u8], hello_done: &mut bool) -> Next {
        let request = match decode::<Request>(frame) {
            Ok(r) => r,
            Err(e) => {
                let next = if *hello_done {
                    Next::Continue
                } else {
                    Next::Close // a first frame that isn't HELLO
                };
                return self.reply(ServerFrame::error(None, e.code()), next).await;
            }
        };
        let id = request.request_id();
        if !*hello_done {
            let Request::Hello { versions, .. } = &request else {
                return self
                    .reply(
                        ServerFrame::error(Some(id), ErrorCode::BadRequest),
                        Next::Close,
                    )
                    .await;
            };
            let Some(version) = negotiate(versions) else {
                let e = ServerFrame::error(Some(id), ErrorCode::UnsupportedVersion);
                return self.reply(e, Next::Close).await;
            };
            *hello_done = true;
            let answer = ServerFrame::Response {
                request_id: id,
                version: Some(version),
                limits: Some(self.limits.clone()),
                message_id: None,
                stats: None,
            };
            return self.reply(answer, Next::Continue).await;
        }
        let answer = match self.handle(request).await {
            Ok(frame) => frame,
            Err(code) => ServerFrame::error(Some(id), code),
        };
        self.reply(answer, Next::Continue).await
    }

    /// One request after HELLO. Errors are protocol codes, never internal text (ADR 0009).
    async fn handle(&mut self, request: Request) -> Result<ServerFrame, ErrorCode> {
        request.validate(self.limits.max_queue_name_len, self.limits.max_message_size)?;
        match request {
            Request::Hello { .. } => Err(ErrorCode::BadRequest), // repeated HELLO
            Request::Publish {
                request_id,
                queue,
                body,
                ttl_ms,
            } => {
                let message_id = self
                    .broker
                    .publish(&queue, body.into(), ttl_ms.map(Duration::from_millis))
                    .await
                    .map_err(broker_code)?;
                Ok(ServerFrame::Response {
                    request_id,
                    version: None,
                    limits: None,
                    message_id: Some(message_id),
                    stats: None,
                })
            }
            Request::Subscribe { request_id, queue } => {
                if !self.subscriptions.contains(&queue) {
                    // The delivery channel is sized for this many subscriptions.
                    if self.subscriptions.len() >= self.limits.max_subscriptions {
                        return Err(ErrorCode::TooManySubscriptions);
                    }
                    self.broker
                        .subscribe(&queue, self.consumer, self.deliveries_tx.clone())
                        .await
                        .map_err(broker_code)?;
                    self.subscriptions.push(queue);
                }
                Ok(ServerFrame::ok(request_id))
            }
            Request::Ack {
                request_id,
                delivery_id,
            } => {
                // Unknown here means never sent or already acked: no need to ask a queue.
                let Some((queue, tag)) = self.in_flight.remove(&delivery_id) else {
                    return Err(ErrorCode::UnknownDelivery);
                };
                self.broker
                    .ack(&queue, self.consumer, tag)
                    .await
                    .map_err(broker_code)?;
                Ok(ServerFrame::ok(request_id))
            }
            Request::Stats { request_id } => Ok(ServerFrame::Response {
                request_id,
                version: None,
                limits: None,
                message_id: None,
                // shortcut: only `queues` is filled; ADR 0008 adds the counters.
                stats: Some(Stats {
                    queues: self.broker.queue_count() as u64,
                    ..Stats::default()
                }),
            }),
        }
    }

    async fn on_delivery(&mut self, d: Delivery) -> Next {
        self.next_delivery_id += 1;
        let delivery_id = self.next_delivery_id;
        self.in_flight.insert(delivery_id, (d.queue.clone(), d.tag));
        self.forget_timed_out(&d.queue);
        let frame = ServerFrame::Message {
            queue: d.queue.to_string(),
            message_id: d.message_id,
            delivery_id,
            body: d.body.to_string(),
            redelivered: d.redelivered,
        };
        self.reply(frame, Next::Continue).await
    }

    /// A queue holds at most `prefetch` live deliveries per connection, so more entries for
    /// `queue` mean some timed out. With one ack timeout per queue they time out in the order
    /// they were sent, so the oldest are the dead ones; their ACK now gets UNKNOWN_DELIVERY
    /// here, as the queue would answer. Keeps `in_flight` bounded for a client that never ACKs.
    // shortcut: a scan of all entries per delivery, at most max_subscriptions × prefetch + 1;
    // index by queue if those limits grow large.
    fn forget_timed_out(&mut self, queue: &str) {
        let mut ids: Vec<u64> = self
            .in_flight
            .iter()
            .filter(|(_, (q, _))| &**q == queue)
            .map(|(&id, _)| id)
            .collect();
        if ids.len() <= self.limits.prefetch {
            return;
        }
        ids.sort_unstable();
        for id in &ids[..ids.len() - self.limits.prefetch] {
            self.in_flight.remove(id);
        }
    }

    /// Queues a frame for the writer. Waits if the client isn't reading (its own
    /// back-pressure); closes if the writer is gone.
    async fn reply(&self, frame: ServerFrame, next: Next) -> Next {
        match self.outbound.send(frame).await {
            Ok(()) => next,
            Err(_) => Next::Close,
        }
    }
}

fn broker_code(e: BrokerError) -> ErrorCode {
    match e {
        BrokerError::UnknownDelivery => ErrorCode::UnknownDelivery,
        BrokerError::QueueStopped => ErrorCode::Internal,
    }
}
