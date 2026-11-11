//! Client (ADR 0011): speaks the protocol with the crate's own types, one request at a time.
//!
//! Every method takes `&mut self`, so at most one request is in flight and the next
//! RESPONSE/ERROR answers it. MESSAGEs that arrive while waiting for that answer are kept
//! for [`Client::next_message`]. There are no timeouts inside: wrap a call in
//! `tokio::time::timeout` if needed, and drop the client if one fires, since the answer it
//! was waiting for may still arrive and would be taken for the next request's.

use std::collections::VecDeque;
use std::time::Duration;

use tokio::io::BufReader;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::protocol::{
    ENVELOPE_OVERHEAD, ErrorCode, Limits, ProtocolError, Request, ServerFrame, Stats, VERSIONS,
    decode, read_frame, write_frame,
};

/// What can go wrong talking to the server.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol: {0}")]
    Protocol(#[from] ProtocolError),
    /// The server answered with an ERROR frame.
    #[error("server error {code:?}: {message}")]
    Server { code: ErrorCode, message: String },
    /// A frame that doesn't answer the request just sent. Never carries the frame: it may
    /// hold a message body, which must not end up in a log.
    #[error("unexpected frame from the server")]
    Unexpected,
    #[error("connection closed by the server")]
    Closed,
}

/// A delivered message; ACK it with its `delivery_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub queue: String,
    pub message_id: u64,
    pub delivery_id: u64,
    pub body: String,
    pub redelivered: bool,
}

pub struct Client {
    read: BufReader<OwnedReadHalf>,
    write: OwnedWriteHalf,
    /// Bytes of a frame not read whole yet. Kept here, not per call, so a call cancelled by
    /// a timeout leaves `read_frame` (cancel-safe) able to resume where it stopped.
    buf: Vec<u8>,
    next_id: u64,
    limits: Limits,
    /// MESSAGEs read while waiting for an answer. Bounded by the server: it sends at most
    /// `prefetch` unacked messages per subscription.
    pending: VecDeque<Message>,
}

impl Client {
    /// Connects and completes HELLO.
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Self, ClientError> {
        let (read, write) = TcpStream::connect(addr).await?.into_split();
        let mut client = Client {
            read: BufReader::new(read),
            write,
            buf: Vec::new(),
            next_id: 0,
            // Placeholder until HELLO answers: a zero frame size makes `read` accept
            // `ENVELOPE_OVERHEAD` bytes, plenty for the HELLO response.
            limits: Limits {
                max_message_size: 0,
                max_frame_size: 0,
                max_queue_name_len: 0,
                max_subscriptions: 0,
                prefetch: 0,
            },
            pending: VecDeque::new(),
        };
        let hello = Request::Hello {
            request_id: client.next_id(),
            versions: VERSIONS.to_vec(),
        };
        match client.call(hello).await? {
            ServerFrame::Response {
                limits: Some(limits),
                ..
            } => client.limits = limits,
            _ => return Err(ClientError::Unexpected),
        }
        Ok(client)
    }

    /// The limits the server announced in its HELLO response.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Publishes `body` to `queue`; returns the message id.
    pub async fn publish(
        &mut self,
        queue: &str,
        body: &str,
        ttl: Option<Duration>,
    ) -> Result<u64, ClientError> {
        let request = Request::Publish {
            request_id: self.next_id(),
            queue: queue.to_owned(),
            body: body.to_owned(),
            // Rust note: `as_millis` is a u128; a TTL past u64::MAX ms is "forever" anyway.
            ttl_ms: ttl.map(|t| u64::try_from(t.as_millis()).unwrap_or(u64::MAX)),
        };
        match self.call(request).await? {
            ServerFrame::Response {
                message_id: Some(id),
                ..
            } => Ok(id),
            _ => Err(ClientError::Unexpected),
        }
    }

    /// Subscribes to `queue`; its messages then come from [`Client::next_message`].
    pub async fn subscribe(&mut self, queue: &str) -> Result<(), ClientError> {
        let request = Request::Subscribe {
            request_id: self.next_id(),
            queue: queue.to_owned(),
        };
        self.call(request).await.map(drop)
    }

    /// Waits for the next MESSAGE, oldest first.
    pub async fn next_message(&mut self) -> Result<Message, ClientError> {
        if let Some(message) = self.pending.pop_front() {
            return Ok(message);
        }
        match self.read().await? {
            ServerFrame::Message {
                queue,
                message_id,
                delivery_id,
                body,
                redelivered,
            } => Ok(Message {
                queue,
                message_id,
                delivery_id,
                body,
                redelivered,
            }),
            // An ERROR without a request_id answers nothing in particular: pass it on.
            ServerFrame::Error {
                request_id: None,
                code,
                message,
            } => Err(ClientError::Server { code, message }),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn ack(&mut self, delivery_id: u64) -> Result<(), ClientError> {
        let request = Request::Ack {
            request_id: self.next_id(),
            delivery_id,
        };
        self.call(request).await.map(drop)
    }

    pub async fn stats(&mut self) -> Result<Stats, ClientError> {
        let request = Request::Stats {
            request_id: self.next_id(),
        };
        match self.call(request).await? {
            ServerFrame::Response {
                stats: Some(stats), ..
            } => Ok(stats),
            _ => Err(ClientError::Unexpected),
        }
    }

    fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    /// Sends `request` and returns the RESPONSE that answers it; MESSAGEs read meanwhile go
    /// to `pending`.
    async fn call(&mut self, request: Request) -> Result<ServerFrame, ClientError> {
        let id = request.request_id();
        write_frame(&mut self.write, &request).await?;
        loop {
            match self.read().await? {
                ServerFrame::Message {
                    queue,
                    message_id,
                    delivery_id,
                    body,
                    redelivered,
                } => self.pending.push_back(Message {
                    queue,
                    message_id,
                    delivery_id,
                    body,
                    redelivered,
                }),
                frame @ ServerFrame::Response { request_id, .. } if request_id == id => {
                    return Ok(frame);
                }
                // No request_id: the server couldn't tell which request failed, but this
                // one is the only one in flight.
                ServerFrame::Error {
                    request_id,
                    code,
                    message,
                } if request_id.is_none_or(|r| r == id) => {
                    return Err(ClientError::Server { code, message });
                }
                _ => return Err(ClientError::Unexpected),
            }
        }
    }

    async fn read(&mut self) -> Result<ServerFrame, ClientError> {
        // Headroom over `max_frame_size`: a MESSAGE carries more fields than the PUBLISH
        // that brought its body, so it can be slightly longer than the largest PUBLISH frame.
        // `saturating_add`: the size comes from the server, and `+` would panic on overflow.
        let max = self.limits.max_frame_size.saturating_add(ENVELOPE_OVERHEAD);
        let frame = read_frame(&mut self.read, &mut self.buf, max)
            .await?
            .ok_or(ClientError::Closed)?;
        Ok(decode(&frame)?)
    }
}
