//! One queue = one task that alone owns the queue's state (ADR 0003). Everything else
//! sends it [`Command`]s; FIFO and "one holder per message" follow from this task handling
//! one command at a time. Ack timeouts and TTL are deadlines this same task checks, so no
//! timer can race an ACK (ADR 0005).

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

// Rust note: tokio's `Instant`, not std's: tests pause and advance tokio's clock, which
// std's ignores (ADR 0005).
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, sleep_until};

use super::{BrokerConfig, BrokerError, ConsumerId};

/// A message handed to a consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub queue: Arc<str>,
    pub message_id: u64,
    /// Identifies this delivery within its queue; ACKs name it.
    pub tag: u64,
    pub body: Arc<str>,
    pub redelivered: bool,
}

/// Size of a queue's per-queue numbers, for STATS.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueStats {
    pub ready: usize,
    pub unacked: usize,
    /// Messages dropped because their TTL ran out before they were handed out.
    pub expired: u64,
}

pub(super) enum Command {
    Publish {
        body: Arc<str>,
        /// The client's TTL, if it gave one.
        ttl: Option<Duration>,
        bytes: Reservation,
        /// The message id, or `QueueFull`.
        reply: oneshot::Sender<Result<u64, BrokerError>>,
    },
    Subscribe {
        consumer: ConsumerId,
        tx: mpsc::Sender<Delivery>,
        reply: oneshot::Sender<()>,
    },
    Ack {
        consumer: ConsumerId,
        tag: u64,
        reply: oneshot::Sender<Result<(), BrokerError>>,
    },
    ConsumerGone {
        consumer: ConsumerId,
    },
    Stats {
        reply: oneshot::Sender<QueueStats>,
    },
}

/// A message's share of the broker-wide byte limit (ADR 0006), given back when dropped.
// Rust note: releasing in `Drop` means every way a message can go (ACK, expiry, a rejected
// or cancelled publish, the queue task ending) gives the bytes back; none can forget to.
pub(super) struct Reservation {
    size: usize,
    held: Arc<AtomicUsize>,
}

impl Reservation {
    /// Takes `size` bytes if the total stays within `max`.
    pub(super) fn take(held: &Arc<AtomicUsize>, size: usize, max: usize) -> Option<Self> {
        // Rust note: `fetch_update` is a compare-and-swap loop: it retries if another
        // publish changed the total in between, so two can't both take the last bytes.
        // Relaxed: the counter guards no other memory.
        held.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |h| {
            h.checked_add(size).filter(|&total| total <= max)
        })
        .ok()?;
        Some(Self {
            size,
            held: held.clone(),
        })
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.held.fetch_sub(self.size, Ordering::Relaxed);
    }
}

// Rust note: `Arc<str>` is a shared, immutable string. Cloning it copies a pointer, not the
// body, so keeping a message in `unacked` and sending it to a consumer costs no copy.
struct Message {
    id: u64,
    body: Arc<str>,
    redelivered: bool,
    /// Dropped instead of handed out from this moment on.
    expires_at: Option<Instant>,
    /// Held as long as the message is; never read.
    _bytes: Reservation,
}

struct Consumer {
    id: ConsumerId,
    tx: mpsc::Sender<Delivery>,
    unacked: usize,
}

struct Unacked {
    consumer: ConsumerId,
    message: Message,
    /// Requeued if not acked by then. `None`: the ack timeout is off.
    deadline: Option<Instant>,
}

pub(super) struct QueueActor {
    name: Arc<str>,
    config: BrokerConfig,
    message_ids: Arc<AtomicU64>,
    ready: VecDeque<Message>,
    unacked: HashMap<u64, Unacked>,
    /// The `unacked` deadlines in time order, so the earliest is `first()`. The tag makes
    /// equal instants distinct (ADR 0005 names a `BTreeMap<_, ()>`; a set is the same).
    deadlines: BTreeSet<(Instant, u64)>,
    expired: u64,
    consumers: Vec<Consumer>,
    /// Where the next round-robin search for a consumer starts.
    next_consumer: usize,
    next_tag: u64,
}

impl QueueActor {
    pub(super) fn new(name: Arc<str>, config: BrokerConfig, message_ids: Arc<AtomicU64>) -> Self {
        Self {
            name,
            config,
            message_ids,
            ready: VecDeque::new(),
            unacked: HashMap::new(),
            deadlines: BTreeSet::new(),
            expired: 0,
            consumers: Vec::new(),
            next_consumer: 0,
            next_tag: 0,
        }
    }

    /// Runs until every handle to the queue is dropped.
    pub(super) async fn run(mut self, mut commands: mpsc::Receiver<Command>) {
        loop {
            let next_deadline = self.deadlines.first().map(|&(at, _)| at);
            // Rust note: a disabled branch's future is still built, just never polled, so
            // `sleep_until` needs some instant even when there is no deadline.
            // `biased`: a passed deadline is handled before any waiting command, so an ACK
            // that arrives after its deadline is always too late, never a coin toss.
            tokio::select! {
                biased;
                _ = sleep_until(next_deadline.unwrap_or_else(Instant::now)),
                    if next_deadline.is_some() => self.requeue_timed_out(Instant::now()),
                command = commands.recv() => match command {
                    Some(command) => self.handle(command),
                    None => return,
                },
            }
            self.dispatch();
        }
    }

    fn handle(&mut self, command: Command) {
        match command {
            Command::Publish {
                body,
                ttl,
                bytes,
                reply,
            } => {
                if self.ready.len() >= self.config.max_queue_len {
                    // Dropping `bytes` here gives them back.
                    let _ = reply.send(Err(BrokerError::QueueFull));
                    return;
                }
                // Relaxed is enough: the counter only has to hand out distinct, increasing
                // numbers; nothing else is synchronized through it.
                let id = self.message_ids.fetch_add(1, Ordering::Relaxed) + 1;
                let ttl = match (ttl.or(self.config.default_ttl), self.config.max_ttl) {
                    (Some(ttl), Some(max)) => Some(ttl.min(max)),
                    (ttl, _) => ttl,
                };
                self.ready.push_back(Message {
                    id,
                    body,
                    redelivered: false,
                    // `checked_add`: `+` panics past the clock's range, and `ttl_ms` comes
                    // from the client (u64::MAX ms would kill this task). Too far = never.
                    expires_at: ttl.and_then(|ttl| Instant::now().checked_add(ttl)),
                    _bytes: bytes,
                });
                // The producer may have gone; the message stays queued either way.
                let _ = reply.send(Ok(id));
            }
            Command::Subscribe {
                consumer,
                tx,
                reply,
            } => {
                if !self.consumers.iter().any(|c| c.id == consumer) {
                    self.consumers.push(Consumer {
                        id: consumer,
                        tx,
                        unacked: 0,
                    });
                }
                let _ = reply.send(());
            }
            Command::Ack {
                consumer,
                tag,
                reply,
            } => {
                let _ = reply.send(self.ack(consumer, tag));
            }
            Command::ConsumerGone { consumer } => self.remove_consumer(consumer),
            Command::Stats { reply } => {
                let _ = reply.send(QueueStats {
                    ready: self.ready.len(),
                    unacked: self.unacked.len(),
                    expired: self.expired,
                });
            }
        }
    }

    fn ack(&mut self, consumer: ConsumerId, tag: u64) -> Result<(), BrokerError> {
        // Only the holder may ack: another consumer naming this tag is an unknown delivery.
        match self.unacked.get(&tag) {
            Some(u) if u.consumer == consumer => {}
            _ => return Err(BrokerError::UnknownDelivery),
        }
        self.take_unacked(tag);
        Ok(())
    }

    /// Removes an unacked delivery and gives its consumer the credit back.
    fn take_unacked(&mut self, tag: u64) -> Option<Message> {
        let u = self.unacked.remove(&tag)?;
        if let Some(at) = u.deadline {
            self.deadlines.remove(&(at, tag));
        }
        if let Some(c) = self.consumers.iter_mut().find(|c| c.id == u.consumer) {
            c.unacked -= 1;
        }
        Some(u.message)
    }

    /// Puts unacked deliveries whose deadline is `now` or earlier back at the front; their
    /// consumers stay and get the credit back. A later ACK of these tags is unknown.
    fn requeue_timed_out(&mut self, now: Instant) {
        let due: Vec<u64> = self
            .deadlines
            .iter()
            .take_while(|&&(at, _)| at <= now)
            .map(|&(_, tag)| tag)
            .collect();
        let back = due
            .into_iter()
            .filter_map(|tag| self.take_unacked(tag))
            .collect();
        self.requeue(back);
    }

    /// Drops a consumer and puts its unacked messages back at the front, oldest first.
    fn remove_consumer(&mut self, consumer: ConsumerId) {
        let Some(pos) = self.consumers.iter().position(|c| c.id == consumer) else {
            return;
        };
        self.consumers.remove(pos);
        if pos < self.next_consumer {
            self.next_consumer -= 1;
        }
        let tags: Vec<u64> = self
            .unacked
            .iter()
            .filter(|(_, u)| u.consumer == consumer)
            .map(|(t, _)| *t)
            .collect();
        let back = tags
            .into_iter()
            .filter_map(|t| self.take_unacked(t))
            .collect();
        self.requeue(back);
    }

    /// Puts messages back at the front, oldest first, marked as redelivered.
    fn requeue(&mut self, mut back: Vec<Message>) {
        // Message ids grow in publish order within a queue, so sorting restores that order.
        back.sort_by_key(|m| m.id);
        for mut m in back.into_iter().rev() {
            m.redelivered = true;
            self.ready.push_front(m);
        }
    }

    /// Hands out ready messages, round-robin, to consumers with prefetch credit left.
    fn dispatch(&mut self) {
        while !self.ready.is_empty() {
            let Some(idx) = self.next_with_credit() else {
                return;
            };
            let message = self.ready.pop_front().expect("checked non-empty");
            // TTL is checked only here, at hand-out (ADR 0005).
            if message.expires_at.is_some_and(|at| at <= Instant::now()) {
                self.expired += 1;
                continue;
            }
            self.next_tag += 1;
            let tag = self.next_tag;
            let delivery = Delivery {
                queue: self.name.clone(),
                message_id: message.id,
                tag,
                body: message.body.clone(),
                redelivered: message.redelivered,
            };
            let consumer = &mut self.consumers[idx];
            // Never `.await` here: a stalled client must not stall the queue (ADR 0003).
            // A full channel means the consumer can't keep up with what its credit allows,
            // a closed one that it is gone; either way it's dropped and its messages requeued.
            if consumer.tx.try_send(delivery).is_ok() {
                consumer.unacked += 1;
                // A zero ack timeout turns it off; one too large to add is as good as off.
                let deadline = Instant::now()
                    .checked_add(self.config.ack_timeout)
                    .filter(|_| !self.config.ack_timeout.is_zero());
                if let Some(at) = deadline {
                    self.deadlines.insert((at, tag));
                }
                self.unacked.insert(
                    tag,
                    Unacked {
                        consumer: consumer.id,
                        message,
                        deadline,
                    },
                );
                self.next_consumer = idx + 1;
            } else {
                let id = consumer.id;
                self.ready.push_front(message);
                self.remove_consumer(id);
            }
        }
    }

    fn next_with_credit(&self) -> Option<usize> {
        let n = self.consumers.len();
        (0..n)
            .map(|i| (self.next_consumer + i) % n)
            .find(|&i| self.consumers[i].unacked < self.config.prefetch)
    }
}
