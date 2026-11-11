//! Wire protocol (ADR 0002): newline-delimited JSON frames, a HELLO handshake, and the
//! messages both sides exchange. `docs/PROTOCOL.md` is the language-neutral description.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Protocol versions this build speaks, highest last.
pub const VERSIONS: &[u32] = &[1];

/// Room for everything in a frame except the body: the frame limit is
/// `max_message_size + ENVELOPE_OVERHEAD`.
pub const ENVELOPE_OVERHEAD: usize = 4 * 1024;

/// Frames a client sends.
// Rust note: `tag = "type"` makes serde read the `type` field to pick the variant, so
// `{"type":"ACK",...}` becomes `Request::Ack`; an unknown type is a parse error, not a panic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum Request {
    Hello {
        request_id: u64,
        versions: Vec<u32>,
    },
    Publish {
        request_id: u64,
        queue: String,
        body: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
    },
    Subscribe {
        request_id: u64,
        queue: String,
    },
    Ack {
        request_id: u64,
        delivery_id: u64,
    },
    Stats {
        request_id: u64,
    },
}

impl Request {
    pub fn request_id(&self) -> u64 {
        match self {
            Request::Hello { request_id, .. }
            | Request::Publish { request_id, .. }
            | Request::Subscribe { request_id, .. }
            | Request::Ack { request_id, .. }
            | Request::Stats { request_id } => *request_id,
        }
    }

    /// Checks what the frame alone can tell: queue names and body size.
    pub fn validate(
        &self,
        max_queue_name_len: usize,
        max_message_size: usize,
    ) -> Result<(), ErrorCode> {
        match self {
            Request::Publish { queue, body, .. } => {
                validate_queue_name(queue, max_queue_name_len)?;
                if body.len() > max_message_size {
                    return Err(ErrorCode::MessageTooLarge);
                }
                Ok(())
            }
            Request::Subscribe { queue, .. } => validate_queue_name(queue, max_queue_name_len),
            Request::Hello { .. } | Request::Ack { .. } | Request::Stats { .. } => Ok(()),
        }
    }
}

/// Frames the server sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum ServerFrame {
    /// Success. Which optional fields are set depends on the request it answers.
    Response {
        request_id: u64,
        /// HELLO: the chosen version.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<u32>,
        /// HELLO: the server's limits.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limits: Option<Limits>,
        /// PUBLISH: the id given to the message.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<u64>,
        /// STATS: the server's metrics.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stats: Option<Stats>,
    },
    /// Failure. `request_id` is absent when the frame couldn't be parsed.
    Error {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
        code: ErrorCode,
        message: String,
    },
    /// A delivery to a subscriber.
    Message {
        queue: String,
        message_id: u64,
        delivery_id: u64,
        body: String,
        /// True if this message was handed out before and came back unacked.
        redelivered: bool,
    },
}

impl ServerFrame {
    /// A RESPONSE with no extra fields (SUBSCRIBE, ACK).
    pub fn ok(request_id: u64) -> Self {
        ServerFrame::Response {
            request_id,
            version: None,
            limits: None,
            message_id: None,
            stats: None,
        }
    }

    /// An ERROR with the code's fixed text: internal details never reach the client.
    pub fn error(request_id: Option<u64>, code: ErrorCode) -> Self {
        ServerFrame::Error {
            request_id,
            code,
            message: code.message().to_owned(),
        }
    }
}

/// The server's limits, sent in the HELLO response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub max_message_size: usize,
    pub max_frame_size: usize,
    pub max_queue_name_len: usize,
    pub max_subscriptions: usize,
    pub prefetch: usize,
}

/// Runtime metrics (SPEC §9), sent in the STATS response.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stats {
    pub active_connections: u64,
    pub queues: u64,
    pub published: u64,
    pub delivered: u64,
    pub acked: u64,
    pub redelivered: u64,
    pub expired: u64,
    pub queued: u64,
}

/// Every error a client can see.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    BadRequest,
    UnsupportedVersion,
    FrameTooLarge,
    MessageTooLarge,
    InvalidQueueName,
    QueueFull,
    TooManyQueues,
    TooManySubscriptions,
    TooManyConnections,
    UnknownDelivery,
    Internal,
}

impl ErrorCode {
    pub fn message(self) -> &'static str {
        match self {
            ErrorCode::BadRequest => "malformed or unexpected request",
            ErrorCode::UnsupportedVersion => "no supported protocol version in common",
            ErrorCode::FrameTooLarge => "frame exceeds the maximum frame size",
            ErrorCode::MessageTooLarge => "message body exceeds the maximum message size",
            ErrorCode::InvalidQueueName => {
                "queue name is empty, too long or has invalid characters"
            }
            ErrorCode::QueueFull => "queue or broker is full",
            ErrorCode::TooManyQueues => "maximum number of queues reached",
            ErrorCode::TooManySubscriptions => "maximum subscriptions per connection reached",
            ErrorCode::TooManyConnections => "maximum number of connections reached",
            ErrorCode::UnknownDelivery => "no such delivery on this connection",
            ErrorCode::Internal => "internal error",
        }
    }
}

/// What can go wrong reading a frame.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("frame exceeds {0} bytes")]
    FrameTooLarge(usize),
    #[error("malformed frame: {0}")]
    Malformed(#[from] serde_json::Error),
    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),
}

impl ProtocolError {
    /// The code a client is told; the error's own text stays in the server log.
    pub fn code(&self) -> ErrorCode {
        match self {
            ProtocolError::FrameTooLarge(_) => ErrorCode::FrameTooLarge,
            ProtocolError::Malformed(_) => ErrorCode::BadRequest,
            ProtocolError::Io(_) => ErrorCode::Internal,
        }
    }
}

/// Picks the highest version both sides speak.
pub fn negotiate(client_versions: &[u32]) -> Option<u32> {
    VERSIONS
        .iter()
        .rev()
        .copied()
        .find(|v| client_versions.contains(v))
}

/// Queue names: 1..=`max_len` bytes of `[A-Za-z0-9._-]`.
pub fn validate_queue_name(name: &str, max_len: usize) -> Result<(), ErrorCode> {
    let ok = !name.is_empty()
        && name.len() <= max_len
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(ErrorCode::InvalidQueueName)
    }
}

/// Reads one frame (without its `\n`). `Ok(None)` is a clean end of stream.
///
/// At most `max_frame + 1` bytes are buffered, so an oversized frame is caught without
/// reading it whole. After `FrameTooLarge` the stream is mid-frame: close the connection.
///
/// Cancel-safe: bytes of an unfinished frame stay in `buf`, so the call can lose a
/// `select!` race and be repeated with the same `buf` without losing data (ADR 0004).
pub async fn read_frame<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    max_frame: usize,
) -> Result<Option<Vec<u8>>, ProtocolError> {
    // Rust note: `take` consumes its receiver, so it gets `&mut *reader` (a fresh borrow)
    // instead of `reader` itself; the reader is usable again once this call returns.
    // The limit counts what an earlier, cancelled call already put in `buf`.
    let limit = (max_frame as u64 + 1).saturating_sub(buf.len() as u64);
    (&mut *reader).take(limit).read_until(b'\n', buf).await?;
    if buf.last() == Some(&b'\n') {
        buf.pop();
        // Rust note: `mem::take` moves the frame out and leaves `buf` empty for the next one.
        return Ok(Some(std::mem::take(buf)));
    }
    if buf.len() > max_frame {
        return Err(ProtocolError::FrameTooLarge(max_frame));
    }
    if buf.is_empty() {
        return Ok(None);
    }
    // Stream ended in the middle of a frame.
    Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into())
}

/// Parses a frame read by [`read_frame`].
pub fn decode<T: DeserializeOwned>(frame: &[u8]) -> Result<T, ProtocolError> {
    Ok(serde_json::from_slice(frame)?)
}

/// Writes one frame: compact JSON (never contains a raw newline) plus `\n`.
pub async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    frame: &T,
) -> Result<(), ProtocolError> {
    let mut bytes = serde_json::to_vec(frame)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    fn roundtrip<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(value: T) {
        let json = serde_json::to_vec(&value).unwrap();
        assert!(!json.contains(&b'\n'));
        assert_eq!(decode::<T>(&json).unwrap(), value);
    }

    #[test]
    fn every_frame_roundtrips() {
        roundtrip(Request::Hello {
            request_id: 1,
            versions: vec![1],
        });
        roundtrip(Request::Publish {
            request_id: 2,
            queue: "q".into(),
            body: "a\nb".into(),
            ttl_ms: Some(5),
        });
        roundtrip(Request::Publish {
            request_id: 2,
            queue: "q".into(),
            body: "x".into(),
            ttl_ms: None,
        });
        roundtrip(Request::Subscribe {
            request_id: 3,
            queue: "q".into(),
        });
        roundtrip(Request::Ack {
            request_id: 4,
            delivery_id: 9,
        });
        roundtrip(Request::Stats { request_id: 5 });
        roundtrip(ServerFrame::ok(3));
        roundtrip(ServerFrame::error(None, ErrorCode::BadRequest));
        roundtrip(ServerFrame::Response {
            request_id: 1,
            version: Some(1),
            limits: Some(Limits {
                max_message_size: 1,
                max_frame_size: 2,
                max_queue_name_len: 3,
                max_subscriptions: 4,
                prefetch: 5,
            }),
            message_id: None,
            stats: Some(Stats::default()),
        });
        roundtrip(ServerFrame::Message {
            queue: "q".into(),
            message_id: 7,
            delivery_id: 1,
            body: "{\"event\":\"created\"}".into(),
            redelivered: false,
        });
    }

    #[test]
    fn wire_format_is_the_documented_one() {
        let json = r#"{"type":"PUBLISH","request_id":1,"queue":"events","body":"hi","ttl_ms":500}"#;
        assert_eq!(
            decode::<Request>(json.as_bytes()).unwrap(),
            Request::Publish {
                request_id: 1,
                queue: "events".into(),
                body: "hi".into(),
                ttl_ms: Some(500)
            }
        );
        let err =
            serde_json::to_string(&ServerFrame::error(Some(7), ErrorCode::QueueFull)).unwrap();
        assert_eq!(
            err,
            r#"{"type":"ERROR","request_id":7,"code":"QUEUE_FULL","message":"queue or broker is full"}"#
        );
        assert_eq!(
            serde_json::to_string(&ServerFrame::ok(2)).unwrap(),
            r#"{"type":"RESPONSE","request_id":2}"#
        );
    }

    #[test]
    fn bad_frames_are_errors_not_panics() {
        for bad in [
            "",
            "not json",
            "[]",
            r#"{"request_id":1}"#,                               // no type
            r#"{"type":"NOPE","request_id":1}"#,                 // unknown type
            r#"{"type":"ACK","request_id":1}"#,                  // missing field
            r#"{"type":"ACK","request_id":-1,"delivery_id":1}"#, // wrong type
            r#"{"type":"STATS","request_id":1,"extra":true}"#,   // unknown field
            r#"{"type":"ack","request_id":1,"delivery_id":1}"#,  // wrong case
        ] {
            let err = decode::<Request>(bad.as_bytes()).unwrap_err();
            assert_eq!(err.code(), ErrorCode::BadRequest, "{bad}");
        }
    }

    #[test]
    fn queue_names() {
        assert!(validate_queue_name("events", 255).is_ok());
        assert!(validate_queue_name("a.b_c-D9", 255).is_ok());
        assert!(validate_queue_name(&"x".repeat(255), 255).is_ok());
        for bad in ["", "a b", "a/b", "é", "a\n", &"x".repeat(256)] {
            assert_eq!(
                validate_queue_name(bad, 255),
                Err(ErrorCode::InvalidQueueName),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn validate_checks_name_and_body_size() {
        let publish = |queue: &str, body: &str| Request::Publish {
            request_id: 1,
            queue: queue.into(),
            body: body.into(),
            ttl_ms: None,
        };
        assert_eq!(publish("q", "1234").validate(255, 4), Ok(()));
        assert_eq!(
            publish("q", "12345").validate(255, 4),
            Err(ErrorCode::MessageTooLarge)
        );
        assert_eq!(
            publish("bad name", "1").validate(255, 4),
            Err(ErrorCode::InvalidQueueName)
        );
        // body size counts bytes, not characters
        assert_eq!(
            publish("q", "éé€").validate(255, 6),
            Err(ErrorCode::MessageTooLarge)
        );
        assert_eq!(
            Request::Subscribe {
                request_id: 1,
                queue: "".into()
            }
            .validate(255, 4),
            Err(ErrorCode::InvalidQueueName)
        );
        assert_eq!(Request::Stats { request_id: 1 }.validate(255, 4), Ok(()));
    }

    #[test]
    fn negotiation() {
        assert_eq!(negotiate(&[1]), Some(1));
        assert_eq!(negotiate(&[3, 1, 2]), Some(1));
        assert_eq!(negotiate(&[2]), None);
        assert_eq!(negotiate(&[]), None);
    }

    #[tokio::test]
    async fn reads_frames_until_eof() {
        let mut r = BufReader::new(&b"{\"a\":1}\n\n{\"b\":2}\n"[..]);
        assert_eq!(
            read_frame(&mut r, &mut Vec::new(), 100)
                .await
                .unwrap()
                .unwrap(),
            b"{\"a\":1}"
        );
        assert_eq!(
            read_frame(&mut r, &mut Vec::new(), 100)
                .await
                .unwrap()
                .unwrap(),
            b""
        );
        assert_eq!(
            read_frame(&mut r, &mut Vec::new(), 100)
                .await
                .unwrap()
                .unwrap(),
            b"{\"b\":2}"
        );
        assert!(
            read_frame(&mut r, &mut Vec::new(), 100)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn frame_limit_is_exact() {
        let mut r = BufReader::new(&b"12345\n"[..]);
        assert_eq!(
            read_frame(&mut r, &mut Vec::new(), 5)
                .await
                .unwrap()
                .unwrap(),
            b"12345"
        );
        let mut r = BufReader::new(&b"123456\n"[..]);
        assert!(matches!(
            read_frame(&mut r, &mut Vec::new(), 5).await,
            Err(ProtocolError::FrameTooLarge(5))
        ));
    }

    #[tokio::test]
    async fn oversized_frame_is_not_buffered_whole() {
        let big = vec![b'x'; 1_000_000];
        // A tiny buffer shows the limit applies to what is read, not to what the
        // BufReader happens to hold.
        let mut r = BufReader::with_capacity(16, &big[..]);
        assert!(matches!(
            read_frame(&mut r, &mut Vec::new(), 64).await,
            Err(ProtocolError::FrameTooLarge(64))
        ));
    }

    #[tokio::test]
    async fn eof_mid_frame_is_an_error() {
        let mut r = BufReader::new(&b"{\"a\""[..]);
        let err = read_frame(&mut r, &mut Vec::new(), 100).await.unwrap_err();
        assert!(
            matches!(err, ProtocolError::Io(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof)
        );
    }

    /// A read cancelled mid-frame (as when it loses a `select!`) keeps the bytes it read.
    #[tokio::test]
    async fn cancelled_read_keeps_partial_frame() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut r = BufReader::new(server);
        let mut buf = Vec::new();
        client.write_all(b"{\"a\":").await.unwrap();
        let first = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            read_frame(&mut r, &mut buf, 10),
        )
        .await;
        assert!(first.is_err(), "no full frame yet");
        assert_eq!(buf, b"{\"a\":");
        client.write_all(b"1}\n").await.unwrap();
        assert_eq!(
            read_frame(&mut r, &mut buf, 10).await.unwrap().unwrap(),
            b"{\"a\":1}"
        );
        assert!(buf.is_empty());
        // the limit counts the bytes kept from the cancelled call
        client.write_all(b"123456").await.unwrap();
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            read_frame(&mut r, &mut buf, 10),
        )
        .await;
        client.write_all(b"78901\n").await.unwrap();
        assert!(matches!(
            read_frame(&mut r, &mut buf, 10).await,
            Err(ProtocolError::FrameTooLarge(10))
        ));
    }

    #[tokio::test]
    async fn write_then_read() {
        let frame = Request::Publish {
            request_id: 1,
            queue: "q".into(),
            body: "line1\nline2".into(),
            ttl_ms: None,
        };
        let mut out = Vec::new();
        write_frame(&mut out, &frame).await.unwrap();
        assert_eq!(
            out.iter().filter(|&&b| b == b'\n').count(),
            1,
            "body newline must be escaped"
        );
        let mut r = BufReader::new(&out[..]);
        let line = read_frame(&mut r, &mut Vec::new(), 1000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decode::<Request>(&line).unwrap(), frame);
    }
}
