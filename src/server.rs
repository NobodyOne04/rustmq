//! The TCP server: accepts connections and runs each one as a reader task and a writer
//! task joined by a bounded channel, so a slow client only slows itself (ADR 0004).
//! Shutdown is one `watch` signal plus a bounded wait for connections (ADR 0007).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::broker::{Broker, BrokerError, ConsumerId, Delivery};
use crate::protocol::{
    ErrorCode, Limits, ProtocolError, Request, ServerFrame, Stats, decode, negotiate, read_frame,
    write_frame,
};

/// Frames waiting for the writer. When full, the reader waits: back-pressure on the client
/// that isn't reading its answers, and on no one else.
const OUTBOUND_BUFFER: usize = 64;

/// How long a rejected connection gets to read its ERROR before it is dropped.
const REJECT_LINGER: Duration = Duration::from_secs(1);

/// The server's own settings; queue settings live in the broker's `BrokerConfig`.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Sent in the HELLO answer and enforced per connection. `limits.prefetch` must be the
    /// broker's `prefetch`: the delivery channel is sized from it.
    pub limits: Limits,
    /// Connections served at once; past that a client gets `TOO_MANY_CONNECTIONS`.
    pub max_connections: usize,
    /// After the shutdown signal, how long connections get to finish before they are
    /// aborted.
    pub shutdown_timeout: Duration,
}

/// Serves connections until `shutdown` completes (or accepting fails), then stops accepting,
/// tells every connection to close and waits at most `shutdown_timeout` for them.
///
/// `main` passes Ctrl-C as `shutdown`; tests pass their own future. Queued and unacked
/// messages are lost (SPEC §14.7): the queues end when the last `Arc<Broker>` is dropped.
pub async fn run(
    listener: TcpListener,
    broker: Arc<Broker>,
    config: ServerConfig,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    let slots = Arc::new(Semaphore::new(config.max_connections));
    // Rust note: a `watch` channel holds one value every receiver can read, and wakes them
    // when it changes: a broadcast "stop" that late joiners still see.
    let (stop_tx, stop) = watch::channel(false);
    let mut connections = JoinSet::new();
    // Rust note: `select!` polls `shutdown` by reference on every turn of the loop, so it
    // must stay at one place in memory: `pin!` fixes it on the stack.
    tokio::pin!(shutdown);
    let result = loop {
        tokio::select! {
            () = &mut shutdown => break Ok(()),
            accepted = listener.accept() => {
                // shortcut: an accept error (e.g. out of file descriptors) stops the
                // server; ADR 0008 adds the log to tell why.
                let (stream, _peer) = match accepted {
                    Ok(a) => a,
                    Err(e) => break Err(e),
                };
                // Rust note: an owned permit can move into the task; dropping it when the
                // task ends frees the slot, whichever way the connection ends.
                match slots.clone().try_acquire_owned() {
                    Ok(permit) => connections.spawn(connection(
                        stream,
                        broker.clone(),
                        config.limits.clone(),
                        permit,
                        stop.clone(),
                    )),
                    Err(_) => connections.spawn(reject(stream)),
                };
            }
            // Finished tasks stay in a JoinSet until joined: reap them as they end.
            Some(_) = connections.join_next() => {}
        }
    };

    // shortcut: ADR 0007 also logs shutdown start, connections left and a hit timeout;
    // that needs `tracing` (ADR 0008).
    drop(listener); // refuse new connections right away
    let _ = stop_tx.send(true);
    let drained = tokio::time::timeout(config.shutdown_timeout, async {
        while connections.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        // A connection stuck on a client (e.g. a write to one that doesn't read).
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }
    result
}

/// Sends `TOO_MANY_CONNECTIONS` and closes. The client has likely sent HELLO already;
/// closing with it unread would reset the connection and could lose the ERROR, so this
/// shuts down writing first and reads until the client closes (or `REJECT_LINGER`).
async fn reject(mut stream: TcpStream) {
    let error = ServerFrame::error(None, ErrorCode::TooManyConnections);
    let _ = tokio::time::timeout(REJECT_LINGER, async {
        write_frame(&mut stream, &error).await.ok()?;
        stream.shutdown().await.ok()?;
        let mut sink = [0u8; 1024];
        while stream.read(&mut sink).await.ok()? > 0 {}
        Some(())
    })
    .await;
}

async fn connection(
    stream: TcpStream,
    broker: Arc<Broker>,
    limits: Limits,
    _permit: OwnedSemaphorePermit,
    stop: watch::Receiver<bool>,
) {
    // Rust note: `into_split` gives two owned halves, so the reader and the writer can live
    // in different tasks without sharing (or locking) the stream.
    let (read, write) = stream.into_split();
    let (outbound, outbound_rx) = mpsc::channel(OUTBOUND_BUFFER);
    // Aborting this task (shutdown timeout) must not leave the writer behind, stuck on a
    // client that doesn't read: the guard aborts it when this function ends, however it ends.
    let mut writer = AbortOnDrop(tokio::spawn(write_loop(write, outbound_rx)));

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
    conn.read_loop(BufReader::new(read), deliveries, stop).await;

    for queue in &conn.subscriptions {
        conn.broker.consumer_gone(queue, consumer).await;
    }
    // Dropping the last sender lets the writer send what is queued, then finish and close.
    drop(conn);
    let _ = (&mut writer.0).await;
}

/// Aborts the task when dropped (a plain `JoinHandle` lets it run on).
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
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
        mut stop: watch::Receiver<bool>,
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
                // Shutdown: leave through the normal end-of-connection path. `wait_for`
                // also sees a stop sent before this connection started waiting.
                // Rust note: it returns a guard holding a lock; the `async` block drops it
                // at once, or it would sit in `select!`'s output and make this future !Send.
                () = async { let _ = stop.wait_for(|&stopped| stopped).await; } => Next::Close,
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
        BrokerError::QueueFull => ErrorCode::QueueFull,
        BrokerError::TooManyQueues => ErrorCode::TooManyQueues,
    }
}
