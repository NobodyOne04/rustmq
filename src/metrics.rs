//! Runtime counters (ADR 0008), shared by the broker, every queue and every connection, and
//! read by STATS.

use std::sync::atomic::{AtomicU64, Ordering};

/// Totals since the server started, plus the connection gauge. Each field is updated on its
/// own with `Relaxed`: no counter guards other memory, and STATS may read them at slightly
/// different moments (an accepted trade-off, ADR 0008).
#[derive(Debug, Default)]
pub struct Metrics {
    pub active_connections: AtomicU64,
    pub published: AtomicU64,
    pub delivered: AtomicU64,
    pub acked: AtomicU64,
    /// Deliveries of a message that had been handed out before and came back unacked.
    pub redelivered: AtomicU64,
    pub expired: AtomicU64,
}

impl Metrics {
    pub fn add(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}
