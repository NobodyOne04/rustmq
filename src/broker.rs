//! The broker: a registry of queues, each run by its own task (ADR 0003).

mod queue;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot};

use queue::{Command, QueueActor};
pub use queue::{Delivery, QueueStats};

/// Commands waiting per queue before senders have to wait (back-pressure on producers).
const COMMAND_BUFFER: usize = 1024;

/// Identifies one subscriber (a connection) across all queues.
pub type ConsumerId = u64;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BrokerError {
    #[error("no such unacked delivery for this consumer")]
    UnknownDelivery,
    #[error("queue task has stopped")]
    QueueStopped,
}

pub struct Broker {
    prefetch: usize,
    // Rust note: a std (not tokio) Mutex is right here: it's held only for a map lookup and
    // never across an `.await`, so it can't block the async runtime.
    queues: Mutex<HashMap<Arc<str>, mpsc::Sender<Command>>>,
    message_ids: Arc<AtomicU64>,
    consumer_ids: AtomicU64,
}

impl Broker {
    /// `prefetch`: unacked deliveries allowed per consumer per queue (at least 1).
    pub fn new(prefetch: usize) -> Self {
        Self {
            prefetch: prefetch.max(1),
            queues: Mutex::new(HashMap::new()),
            message_ids: Arc::new(AtomicU64::new(0)),
            consumer_ids: AtomicU64::new(0),
        }
    }

    pub fn new_consumer_id(&self) -> ConsumerId {
        self.consumer_ids.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Adds a message to the end of `queue` (created if new). Returns its message id once
    /// it is in the queue.
    pub async fn publish(&self, queue: &str, body: Arc<str>) -> Result<u64, BrokerError> {
        let (reply, rx) = oneshot::channel();
        self.send(queue, Command::Publish { body, reply }).await?;
        rx.await.map_err(|_| BrokerError::QueueStopped)
    }

    /// Starts delivering `queue`'s messages to `tx`. `tx` must hold at least `prefetch`
    /// deliveries, or the consumer is dropped as too slow.
    pub async fn subscribe(
        &self,
        queue: &str,
        consumer: ConsumerId,
        tx: mpsc::Sender<Delivery>,
    ) -> Result<(), BrokerError> {
        let (reply, rx) = oneshot::channel();
        self.send(
            queue,
            Command::Subscribe {
                consumer,
                tx,
                reply,
            },
        )
        .await?;
        rx.await.map_err(|_| BrokerError::QueueStopped)
    }

    pub async fn ack(
        &self,
        queue: &str,
        consumer: ConsumerId,
        tag: u64,
    ) -> Result<(), BrokerError> {
        let (reply, rx) = oneshot::channel();
        self.send(
            queue,
            Command::Ack {
                consumer,
                tag,
                reply,
            },
        )
        .await?;
        rx.await.map_err(|_| BrokerError::QueueStopped)?
    }

    /// The consumer left `queue`: its unacked messages go back to the front.
    pub async fn consumer_gone(&self, queue: &str, consumer: ConsumerId) {
        let _ = self.send(queue, Command::ConsumerGone { consumer }).await;
    }

    /// `None` if the queue doesn't exist.
    pub async fn queue_stats(&self, queue: &str) -> Option<QueueStats> {
        let tx = self
            .queues
            .lock()
            .expect("registry lock poisoned")
            .get(queue)
            .cloned()?;
        let (reply, rx) = oneshot::channel();
        tx.send(Command::Stats { reply }).await.ok()?;
        rx.await.ok()
    }

    pub fn queue_count(&self) -> usize {
        self.queues.lock().expect("registry lock poisoned").len()
    }

    async fn send(&self, queue: &str, command: Command) -> Result<(), BrokerError> {
        // The lock guard is dropped at the end of this statement, before the `.await`.
        let tx = self.queue(queue);
        tx.send(command)
            .await
            .map_err(|_| BrokerError::QueueStopped)
    }

    /// The queue's command sender, starting its task on first use.
    fn queue(&self, name: &str) -> mpsc::Sender<Command> {
        let mut queues = self.queues.lock().expect("registry lock poisoned");
        if let Some(tx) = queues.get(name) {
            return tx.clone();
        }
        let name: Arc<str> = name.into();
        let (tx, rx) = mpsc::channel(COMMAND_BUFFER);
        let actor = QueueActor::new(name.clone(), self.prefetch, self.message_ids.clone());
        tokio::spawn(actor.run(rx));
        queues.insert(name, tx.clone());
        tx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tokio::sync::mpsc::error::TryRecvError;

    fn body(s: &str) -> Arc<str> {
        s.into()
    }

    async fn subscriber(
        b: &Broker,
        queue: &str,
        cap: usize,
    ) -> (ConsumerId, mpsc::Receiver<Delivery>) {
        let id = b.new_consumer_id();
        let (tx, rx) = mpsc::channel(cap);
        b.subscribe(queue, id, tx).await.unwrap();
        (id, rx)
    }

    /// The queue has handled every earlier command once this returns (commands are FIFO).
    async fn settle(b: &Broker, queue: &str) -> QueueStats {
        b.queue_stats(queue).await.unwrap()
    }

    #[tokio::test]
    async fn fifo_with_one_consumer() {
        let b = Broker::new(10);
        let mut ids = Vec::new();
        for i in 0..5 {
            ids.push(b.publish("q", body(&i.to_string())).await.unwrap());
        }
        assert!(ids.windows(2).all(|w| w[0] < w[1]));
        let (_, mut rx) = subscriber(&b, "q", 10).await;
        for (i, id) in ids.iter().enumerate() {
            let d = rx.recv().await.unwrap();
            assert_eq!(
                (d.message_id, &*d.body, &*d.queue, d.redelivered),
                (*id, &*i.to_string(), "q", false)
            );
        }
    }

    #[tokio::test]
    async fn prefetch_limits_unacked() {
        let b = Broker::new(1);
        let (c, mut rx) = subscriber(&b, "q", 1).await;
        b.publish("q", body("a")).await.unwrap();
        b.publish("q", body("b")).await.unwrap();
        let first = rx.recv().await.unwrap();
        assert_eq!(
            settle(&b, "q").await,
            QueueStats {
                ready: 1,
                unacked: 1
            }
        );
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        b.ack("q", c, first.tag).await.unwrap();
        assert_eq!(&*rx.recv().await.unwrap().body, "b");
    }

    #[tokio::test]
    async fn ack_must_come_from_the_holder() {
        let b = Broker::new(1);
        let (c1, mut rx1) = subscriber(&b, "q", 1).await;
        let (c2, _rx2) = subscriber(&b, "q", 1).await;
        b.publish("q", body("a")).await.unwrap();
        let d = rx1.recv().await.unwrap();
        assert_eq!(
            b.ack("q", c2, d.tag).await,
            Err(BrokerError::UnknownDelivery)
        );
        assert_eq!(b.ack("q", c1, 999).await, Err(BrokerError::UnknownDelivery));
        b.ack("q", c1, d.tag).await.unwrap();
        assert_eq!(
            b.ack("q", c1, d.tag).await,
            Err(BrokerError::UnknownDelivery),
            "double ack"
        );
        assert_eq!(settle(&b, "q").await, QueueStats::default());
    }

    #[tokio::test]
    async fn consumers_compete_round_robin() {
        let b = Broker::new(1);
        let (_, mut rx1) = subscriber(&b, "q", 1).await;
        let (_, mut rx2) = subscriber(&b, "q", 1).await;
        b.publish("q", body("a")).await.unwrap();
        b.publish("q", body("b")).await.unwrap();
        b.publish("q", body("c")).await.unwrap();
        assert_eq!(&*rx1.recv().await.unwrap().body, "a");
        assert_eq!(&*rx2.recv().await.unwrap().body, "b");
        // both hold one unacked message: "c" waits
        assert_eq!(
            settle(&b, "q").await,
            QueueStats {
                ready: 1,
                unacked: 2
            }
        );
    }

    #[tokio::test]
    async fn gone_consumer_requeues_to_front_in_order() {
        let b = Broker::new(2);
        let (c1, mut rx1) = subscriber(&b, "q", 2).await;
        for s in ["a", "b", "c"] {
            b.publish("q", body(s)).await.unwrap();
        }
        let a = rx1.recv().await.unwrap();
        let _b = rx1.recv().await.unwrap();
        b.consumer_gone("q", c1).await;
        assert_eq!(
            settle(&b, "q").await,
            QueueStats {
                ready: 3,
                unacked: 0
            }
        );

        let (c2, mut rx2) = subscriber(&b, "q", 3).await;
        let got: Vec<_> = [rx2.recv().await.unwrap(), rx2.recv().await.unwrap()].into();
        assert_eq!(
            (&*got[0].body, got[0].redelivered, got[0].message_id),
            ("a", true, a.message_id)
        );
        assert_eq!((&*got[1].body, got[1].redelivered), ("b", true));
        assert_ne!(got[0].tag, a.tag, "a redelivery is a new delivery");
        // prefetch is 2 and both slots are taken: "c" comes only after an ack
        assert_eq!(rx2.try_recv(), Err(TryRecvError::Empty));
        b.ack("q", c2, got[0].tag).await.unwrap();
        let c = rx2.recv().await.unwrap();
        assert_eq!((&*c.body, c.redelivered), ("c", false));
    }

    #[tokio::test]
    async fn dropped_receiver_counts_as_gone() {
        let b = Broker::new(1);
        let (_, rx1) = subscriber(&b, "q", 1).await;
        drop(rx1);
        let (_, mut rx2) = subscriber(&b, "q", 1).await;
        b.publish("q", body("a")).await.unwrap();
        let d = rx2.recv().await.unwrap();
        assert_eq!(&*d.body, "a");
    }

    #[tokio::test]
    async fn too_small_channel_drops_consumer_not_message() {
        let b = Broker::new(2);
        let (c1, mut rx1) = subscriber(&b, "q", 1).await; // smaller than prefetch: a misuse
        b.publish("q", body("a")).await.unwrap();
        b.publish("q", body("b")).await.unwrap();
        // "b" didn't fit: c1 was dropped and "a" requeued with it
        assert_eq!(
            settle(&b, "q").await,
            QueueStats {
                ready: 2,
                unacked: 0
            }
        );
        assert_eq!(&*rx1.recv().await.unwrap().body, "a");
        assert_eq!(b.ack("q", c1, 1).await, Err(BrokerError::UnknownDelivery));
    }

    #[tokio::test]
    async fn queues_are_isolated() {
        let b = Broker::new(1);
        let (_, mut stuck) = subscriber(&b, "a", 1).await;
        let (_, mut rx_b) = subscriber(&b, "b", 1).await;
        for i in 0..100 {
            b.publish("a", body(&i.to_string())).await.unwrap();
        }
        // "a" has a consumer that never acks; "b" still flows
        b.publish("b", body("x")).await.unwrap();
        let d = rx_b.recv().await.unwrap();
        assert_eq!((&*d.queue, &*d.body), ("b", "x"));
        assert_eq!(&*stuck.recv().await.unwrap().queue, "a");
        assert_eq!(rx_b.try_recv(), Err(TryRecvError::Empty));
        assert_eq!(b.queue_count(), 2);
    }

    /// Many producers and competing consumers on one queue: every message is delivered
    /// exactly once (nobody disconnects) and no message is held by two consumers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_publish_and_consume() {
        const PRODUCERS: u64 = 8;
        const PER_PRODUCER: u64 = 500;
        const CONSUMERS: usize = 4;
        let b = Arc::new(Broker::new(3));
        let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
        for _ in 0..CONSUMERS {
            let (c, mut rx) = subscriber(&b, "q", 3).await;
            let (b, seen_tx) = (b.clone(), seen_tx.clone());
            tokio::spawn(async move {
                while let Some(d) = rx.recv().await {
                    assert!(!d.redelivered);
                    seen_tx.send(d.body.to_string()).unwrap();
                    b.ack("q", c, d.tag).await.unwrap();
                }
            });
        }
        let producers: Vec<_> = (0..PRODUCERS)
            .map(|p| {
                let b = b.clone();
                tokio::spawn(async move {
                    for i in 0..PER_PRODUCER {
                        b.publish("q", format!("{p}:{i}").into()).await.unwrap();
                    }
                })
            })
            .collect();
        for p in producers {
            p.await.unwrap();
        }
        let mut seen = HashSet::new();
        // The timeout only turns a hang (e.g. a panicked consumer task) into a failure.
        let all = async {
            for _ in 0..PRODUCERS * PER_PRODUCER {
                assert!(
                    seen.insert(seen_rx.recv().await.unwrap()),
                    "delivered twice"
                );
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(30), all)
            .await
            .expect("not all delivered");
        assert_eq!(seen.len() as u64, PRODUCERS * PER_PRODUCER);
    }
}
