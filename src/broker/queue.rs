//! One queue = one task that alone owns the queue's state (ADR 0003). Everything else
//! sends it [`Command`]s; FIFO and "one holder per message" follow from this task handling
//! one command at a time.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{mpsc, oneshot};

use super::{BrokerError, ConsumerId};

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
}

pub(super) enum Command {
    Publish {
        body: Arc<str>,
        reply: oneshot::Sender<u64>,
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

// Rust note: `Arc<str>` is a shared, immutable string. Cloning it copies a pointer, not the
// body, so keeping a message in `unacked` and sending it to a consumer costs no copy.
struct Message {
    id: u64,
    body: Arc<str>,
    redelivered: bool,
}

struct Consumer {
    id: ConsumerId,
    tx: mpsc::Sender<Delivery>,
    unacked: usize,
}

struct Unacked {
    consumer: ConsumerId,
    message: Message,
}

pub(super) struct QueueActor {
    name: Arc<str>,
    prefetch: usize,
    message_ids: Arc<AtomicU64>,
    ready: VecDeque<Message>,
    unacked: HashMap<u64, Unacked>,
    consumers: Vec<Consumer>,
    /// Where the next round-robin search for a consumer starts.
    next_consumer: usize,
    next_tag: u64,
}

impl QueueActor {
    pub(super) fn new(name: Arc<str>, prefetch: usize, message_ids: Arc<AtomicU64>) -> Self {
        Self {
            name,
            prefetch,
            message_ids,
            ready: VecDeque::new(),
            unacked: HashMap::new(),
            consumers: Vec::new(),
            next_consumer: 0,
            next_tag: 0,
        }
    }

    /// Runs until every handle to the queue is dropped.
    pub(super) async fn run(mut self, mut commands: mpsc::Receiver<Command>) {
        while let Some(command) = commands.recv().await {
            self.handle(command);
            self.dispatch();
        }
    }

    fn handle(&mut self, command: Command) {
        match command {
            Command::Publish { body, reply } => {
                // Relaxed is enough: the counter only has to hand out distinct, increasing
                // numbers; nothing else is synchronized through it.
                let id = self.message_ids.fetch_add(1, Ordering::Relaxed) + 1;
                self.ready.push_back(Message {
                    id,
                    body,
                    redelivered: false,
                });
                // The producer may have gone; the message stays queued either way.
                let _ = reply.send(id);
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
        self.unacked.remove(&tag);
        if let Some(c) = self.consumers.iter_mut().find(|c| c.id == consumer) {
            c.unacked -= 1;
        }
        Ok(())
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
        let mut back: Vec<Message> = tags
            .into_iter()
            .filter_map(|t| self.unacked.remove(&t))
            .map(|u| u.message)
            .collect();
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
                self.unacked.insert(
                    tag,
                    Unacked {
                        consumer: consumer.id,
                        message,
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
            .find(|&i| self.consumers[i].unacked < self.prefetch)
    }
}
