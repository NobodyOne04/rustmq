//! The broker: a registry of queues, each run by its own task (ADR 0003).

mod queue;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use queue::{Command, QueueActor};
pub use queue::{Delivery, QueueStats};

/// Commands waiting per queue before senders have to wait (back-pressure on producers).
const COMMAND_BUFFER: usize = 1024;

/// Identifies one subscriber (a connection) across all queues.
pub type ConsumerId = u64;

/// What every queue of a broker follows (SPEC §14.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerConfig {
    /// Unacked deliveries allowed per consumer per queue (at least 1).
    pub prefetch: usize,
    /// An unacked delivery older than this goes back to its queue. Zero turns it off.
    pub ack_timeout: Duration,
    /// TTL for a message published without one. `None`: no TTL.
    pub default_ttl: Option<Duration>,
    /// Upper bound for any message's TTL.
    pub max_ttl: Option<Duration>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            prefetch: 1,
            ack_timeout: Duration::from_secs(30),
            default_ttl: None,
            max_ttl: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BrokerError {
    #[error("no such unacked delivery for this consumer")]
    UnknownDelivery,
    #[error("queue task has stopped")]
    QueueStopped,
}

pub struct Broker {
    config: BrokerConfig,
    // Rust note: a std (not tokio) Mutex is right here: it's held only for a map lookup and
    // never across an `.await`, so it can't block the async runtime.
    queues: Mutex<HashMap<Arc<str>, mpsc::Sender<Command>>>,
    message_ids: Arc<AtomicU64>,
    consumer_ids: AtomicU64,
}

impl Broker {
    /// The default config with this `prefetch`.
    pub fn new(prefetch: usize) -> Self {
        Self::with_config(BrokerConfig {
            prefetch,
            ..BrokerConfig::default()
        })
    }

    pub fn with_config(config: BrokerConfig) -> Self {
        Self {
            config: BrokerConfig {
                prefetch: config.prefetch.max(1),
                ..config
            },
            queues: Mutex::new(HashMap::new()),
            message_ids: Arc::new(AtomicU64::new(0)),
            consumer_ids: AtomicU64::new(0),
        }
    }

    pub fn new_consumer_id(&self) -> ConsumerId {
        self.consumer_ids.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Adds a message to the end of `queue` (created if new). Returns its message id once
    /// it is in the queue. `ttl`: the client's, else the config's default; capped by
    /// `max_ttl`.
    pub async fn publish(
        &self,
        queue: &str,
        body: Arc<str>,
        ttl: Option<Duration>,
    ) -> Result<u64, BrokerError> {
        let (reply, rx) = oneshot::channel();
        self.send(queue, Command::Publish { body, ttl, reply })
            .await?;
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
        let actor = QueueActor::new(name.clone(), self.config, self.message_ids.clone());
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
            ids.push(b.publish("q", body(&i.to_string()), None).await.unwrap());
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
        b.publish("q", body("a"), None).await.unwrap();
        b.publish("q", body("b"), None).await.unwrap();
        let first = rx.recv().await.unwrap();
        assert_eq!(
            settle(&b, "q").await,
            QueueStats {
                ready: 1,
                unacked: 1,
                expired: 0,
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
        b.publish("q", body("a"), None).await.unwrap();
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
        b.publish("q", body("a"), None).await.unwrap();
        b.publish("q", body("b"), None).await.unwrap();
        b.publish("q", body("c"), None).await.unwrap();
        assert_eq!(&*rx1.recv().await.unwrap().body, "a");
        assert_eq!(&*rx2.recv().await.unwrap().body, "b");
        // both hold one unacked message: "c" waits
        assert_eq!(
            settle(&b, "q").await,
            QueueStats {
                ready: 1,
                unacked: 2,
                expired: 0,
            }
        );
    }

    #[tokio::test]
    async fn gone_consumer_requeues_to_front_in_order() {
        let b = Broker::new(2);
        let (c1, mut rx1) = subscriber(&b, "q", 2).await;
        for s in ["a", "b", "c"] {
            b.publish("q", body(s), None).await.unwrap();
        }
        let a = rx1.recv().await.unwrap();
        let _b = rx1.recv().await.unwrap();
        b.consumer_gone("q", c1).await;
        assert_eq!(
            settle(&b, "q").await,
            QueueStats {
                ready: 3,
                unacked: 0,
                expired: 0,
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
        b.publish("q", body("a"), None).await.unwrap();
        let d = rx2.recv().await.unwrap();
        assert_eq!(&*d.body, "a");
    }

    #[tokio::test]
    async fn too_small_channel_drops_consumer_not_message() {
        let b = Broker::new(2);
        let (c1, mut rx1) = subscriber(&b, "q", 1).await; // smaller than prefetch: a misuse
        b.publish("q", body("a"), None).await.unwrap();
        b.publish("q", body("b"), None).await.unwrap();
        // "b" didn't fit: c1 was dropped and "a" requeued with it
        assert_eq!(
            settle(&b, "q").await,
            QueueStats {
                ready: 2,
                unacked: 0,
                expired: 0,
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
            b.publish("a", body(&i.to_string()), None).await.unwrap();
        }
        // "a" has a consumer that never acks; "b" still flows
        b.publish("b", body("x"), None).await.unwrap();
        let d = rx_b.recv().await.unwrap();
        assert_eq!((&*d.queue, &*d.body), ("b", "x"));
        assert_eq!(&*stuck.recv().await.unwrap().queue, "a");
        assert_eq!(rx_b.try_recv(), Err(TryRecvError::Empty));
        assert_eq!(b.queue_count(), 2);
    }

    fn timed(ack_timeout_s: u64, default_ttl_s: Option<u64>, max_ttl_s: Option<u64>) -> Broker {
        Broker::with_config(BrokerConfig {
            prefetch: 2,
            ack_timeout: Duration::from_secs(ack_timeout_s),
            default_ttl: default_ttl_s.map(Duration::from_secs),
            max_ttl: max_ttl_s.map(Duration::from_secs),
        })
    }

    fn stats(ready: usize, unacked: usize, expired: u64) -> QueueStats {
        QueueStats {
            ready,
            unacked,
            expired,
        }
    }

    // Rust note: `start_paused` freezes tokio's clock; `advance` moves it, and when every
    // task is idle tokio jumps it to the next timer. No test here really sleeps.
    #[tokio::test(start_paused = true)]
    async fn ack_timeout_requeues_to_the_front() {
        let b = timed(10, None, None);
        let (c, mut rx) = subscriber(&b, "q", 2).await;
        for s in ["a", "b", "c"] {
            b.publish("q", body(s), None).await.unwrap();
        }
        let a = rx.recv().await.unwrap();
        let _b = rx.recv().await.unwrap();
        tokio::time::advance(Duration::from_secs(9)).await;
        assert_eq!(settle(&b, "q").await, stats(1, 2, 0), "not yet");
        tokio::time::advance(Duration::from_secs(1)).await;
        // Both timed out, went back in front of "c", and come again (the credit came back).
        let again: Vec<_> = [rx.recv().await.unwrap(), rx.recv().await.unwrap()].into();
        assert_eq!(
            (&*again[0].body, again[0].redelivered, again[0].message_id),
            ("a", true, a.message_id)
        );
        assert_eq!((&*again[1].body, again[1].redelivered), ("b", true));
        assert_eq!(settle(&b, "q").await, stats(1, 2, 0));
        assert_eq!(
            b.ack("q", c, a.tag).await,
            Err(BrokerError::UnknownDelivery),
            "the timed-out delivery"
        );
        b.ack("q", c, again[0].tag).await.unwrap();
        assert_eq!(&*rx.recv().await.unwrap().body, "c");
    }

    #[tokio::test(start_paused = true)]
    async fn ack_before_the_deadline_wins() {
        let b = timed(10, None, None);
        let (c, mut rx) = subscriber(&b, "q", 2).await;
        b.publish("q", body("a"), None).await.unwrap();
        let d = rx.recv().await.unwrap();
        tokio::time::advance(Duration::from_secs(9)).await;
        b.ack("q", c, d.tag).await.unwrap();
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(settle(&b, "q").await, stats(0, 0, 0));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test(start_paused = true)]
    async fn zero_ack_timeout_is_off() {
        let b = timed(0, None, None);
        let (_, mut rx) = subscriber(&b, "q", 2).await;
        b.publish("q", body("a"), None).await.unwrap();
        rx.recv().await.unwrap();
        tokio::time::advance(Duration::from_secs(24 * 3600)).await;
        assert_eq!(settle(&b, "q").await, stats(0, 1, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn ttl_drops_messages_not_handed_out_in_time() {
        let b = timed(0, None, None);
        b.publish("q", body("short"), Some(Duration::from_secs(5)))
            .await
            .unwrap();
        b.publish("q", body("long"), Some(Duration::from_secs(60)))
            .await
            .unwrap();
        b.publish("q", body("none"), None).await.unwrap();
        tokio::time::advance(Duration::from_secs(5)).await;
        // Expiry is checked at hand-out only: still counted as ready.
        assert_eq!(settle(&b, "q").await, stats(3, 0, 0));
        let (_, mut rx) = subscriber(&b, "q", 2).await;
        assert_eq!(&*rx.recv().await.unwrap().body, "long");
        assert_eq!(&*rx.recv().await.unwrap().body, "none");
        assert_eq!(settle(&b, "q").await, stats(0, 2, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn default_ttl_applies_and_max_ttl_caps() {
        let b = timed(0, Some(5), Some(10));
        b.publish("q", body("default"), None).await.unwrap();
        b.publish("q", body("capped"), Some(Duration::from_secs(3600)))
            .await
            .unwrap();
        b.publish("q", body("own"), Some(Duration::from_secs(7)))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_secs(6)).await;
        let (_, mut rx) = subscriber(&b, "q", 2).await;
        assert_eq!(&*rx.recv().await.unwrap().body, "capped");
        assert_eq!(&*rx.recv().await.unwrap().body, "own");
        assert_eq!(settle(&b, "q").await, stats(0, 2, 1));

        let b = timed(0, Some(5), Some(10));
        b.publish("q", body("capped"), Some(Duration::from_secs(3600)))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_secs(10)).await;
        let (_, _rx) = subscriber(&b, "q", 2).await;
        assert_eq!(
            settle(&b, "q").await,
            stats(0, 0, 1),
            "max_ttl caps at 10 s"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unacked_never_expires_but_is_checked_after_requeue() {
        let b = timed(0, None, None);
        let (c, mut rx) = subscriber(&b, "q", 2).await;
        b.publish("q", body("a"), Some(Duration::from_secs(1)))
            .await
            .unwrap();
        rx.recv().await.unwrap();
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(settle(&b, "q").await, stats(0, 1, 0), "held: not expired");
        b.consumer_gone("q", c).await;
        let (_, mut rx2) = subscriber(&b, "q", 2).await;
        assert_eq!(
            settle(&b, "q").await,
            stats(0, 0, 1),
            "requeued, then expired"
        );
        assert_eq!(rx2.try_recv(), Err(TryRecvError::Empty));
    }

    /// Durations past the clock's range come from clients (`ttl_ms`) and flags; they must
    /// mean "never", not panic the queue's task.
    #[tokio::test(start_paused = true)]
    async fn huge_durations_mean_never() {
        let b = Broker::with_config(BrokerConfig {
            prefetch: 1,
            ack_timeout: Duration::MAX,
            default_ttl: None,
            max_ttl: None,
        });
        b.publish("q", body("a"), Some(Duration::from_millis(u64::MAX)))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_secs(365 * 24 * 3600)).await;
        let (_, mut rx) = subscriber(&b, "q", 1).await;
        assert_eq!(&*rx.recv().await.unwrap().body, "a");
        tokio::time::advance(Duration::from_secs(365 * 24 * 3600)).await;
        assert_eq!(
            settle(&b, "q").await,
            stats(0, 1, 0),
            "the queue task is alive"
        );
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
                        b.publish("q", format!("{p}:{i}").into(), None)
                            .await
                            .unwrap();
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
