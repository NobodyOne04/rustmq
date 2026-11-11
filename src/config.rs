//! Server settings (SPEC §14.4, ADR 0012): defaults, and the checks run before anything
//! starts, so a bad setting fails with a clear message instead of a panic later. No clap
//! here: the CLI fills a `Config`, tests build one directly.

use std::net::SocketAddr;
use std::time::Duration;

use crate::broker::BrokerConfig;
use crate::protocol::{ENVELOPE_OVERHEAD, Limits};
use crate::server::ServerConfig;

/// SPEC §14.4's ceiling for `max_message_size`.
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
/// Ceiling for `max_subscriptions × prefetch`: the per-connection delivery channel's size.
pub const MAX_DELIVERIES_PER_CONNECTION: usize = 1 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub bind: SocketAddr,
    pub max_connections: usize,
    pub max_message_size: usize,
    pub max_queues: usize,
    pub max_queue_len: usize,
    pub max_total_bytes: usize,
    pub max_queue_name_len: usize,
    pub max_subscriptions: usize,
    pub prefetch: usize,
    /// Zero turns the ack timeout off.
    pub ack_timeout: Duration,
    pub default_ttl: Option<Duration>,
    pub max_ttl: Option<Duration>,
    pub shutdown_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        let broker = BrokerConfig::default();
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 7000)),
            max_connections: 1024,
            max_message_size: 64 * 1024,
            max_queues: broker.max_queues,
            max_queue_len: broker.max_queue_len,
            max_total_bytes: broker.max_total_bytes,
            max_queue_name_len: 255,
            max_subscriptions: 16,
            prefetch: broker.prefetch,
            ack_timeout: broker.ack_timeout,
            default_ttl: broker.default_ttl,
            max_ttl: broker.max_ttl,
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

/// A setting that breaks a rule; the message names both.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("{0} must be at least 1")]
    Zero(&'static str),
    #[error("max_message_size must be at most {MAX_MESSAGE_SIZE} bytes")]
    MessageSizeTooLarge,
    #[error(
        "max_subscriptions × prefetch must be at most {MAX_DELIVERIES_PER_CONNECTION} \
         (unacked deliveries one connection can hold)"
    )]
    TooManyDeliveries,
    #[error("default_ttl must not be above max_ttl")]
    DefaultTtlAboveMax,
}

impl Config {
    /// Checks every rule; returns the settings split the way the broker and server take them.
    pub fn validate(self) -> Result<(BrokerConfig, ServerConfig), ConfigError> {
        let at_least_one = [
            ("max_connections", self.max_connections),
            ("max_message_size", self.max_message_size),
            ("max_queues", self.max_queues),
            ("max_queue_len", self.max_queue_len),
            ("max_total_bytes", self.max_total_bytes),
            ("max_queue_name_len", self.max_queue_name_len),
            ("max_subscriptions", self.max_subscriptions),
            ("prefetch", self.prefetch),
        ];
        if let Some((name, _)) = at_least_one.iter().find(|(_, value)| *value == 0) {
            return Err(ConfigError::Zero(name));
        }
        if self.max_message_size > MAX_MESSAGE_SIZE {
            return Err(ConfigError::MessageSizeTooLarge);
        }
        // `checked_mul`: a plain `*` panics on overflow in debug builds and wraps in release.
        match self.max_subscriptions.checked_mul(self.prefetch) {
            Some(n) if n <= MAX_DELIVERIES_PER_CONNECTION => {}
            _ => return Err(ConfigError::TooManyDeliveries),
        }
        if let (Some(default), Some(max)) = (self.default_ttl, self.max_ttl)
            && default > max
        {
            return Err(ConfigError::DefaultTtlAboveMax);
        }

        let broker = BrokerConfig {
            prefetch: self.prefetch,
            ack_timeout: self.ack_timeout,
            default_ttl: self.default_ttl,
            max_ttl: self.max_ttl,
            max_queues: self.max_queues,
            max_queue_len: self.max_queue_len,
            max_total_bytes: self.max_total_bytes,
        };
        let server = ServerConfig {
            limits: Limits {
                max_message_size: self.max_message_size,
                max_frame_size: self.max_message_size + ENVELOPE_OVERHEAD,
                max_queue_name_len: self.max_queue_name_len,
                max_subscriptions: self.max_subscriptions,
                prefetch: self.prefetch,
            },
            max_connections: self.max_connections,
            shutdown_timeout: self.shutdown_timeout,
        };
        Ok((broker, server))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_and_match_the_spec() {
        let (broker, server) = Config::default().validate().unwrap();
        assert_eq!(broker, BrokerConfig::default());
        assert_eq!(server.limits.max_message_size, 64 * 1024);
        assert_eq!(server.limits.max_frame_size, 64 * 1024 + ENVELOPE_OVERHEAD);
        assert_eq!(server.limits.prefetch, broker.prefetch);
        assert_eq!(server.max_connections, 1024);
    }

    /// A setting's name and a function setting it to zero.
    type Zeroing = (&'static str, fn(&mut Config));

    #[test]
    fn every_count_and_size_must_be_at_least_one() {
        let zeroed: [Zeroing; 8] = [
            ("max_connections", |c| c.max_connections = 0),
            ("max_message_size", |c| c.max_message_size = 0),
            ("max_queues", |c| c.max_queues = 0),
            ("max_queue_len", |c| c.max_queue_len = 0),
            ("max_total_bytes", |c| c.max_total_bytes = 0),
            ("max_queue_name_len", |c| c.max_queue_name_len = 0),
            ("max_subscriptions", |c| c.max_subscriptions = 0),
            ("prefetch", |c| c.prefetch = 0),
        ];
        for (name, zero) in zeroed {
            let mut config = Config::default();
            zero(&mut config);
            assert_eq!(config.validate().unwrap_err(), ConfigError::Zero(name));
        }
    }

    #[test]
    fn message_size_has_a_ceiling() {
        let at = Config {
            max_message_size: MAX_MESSAGE_SIZE,
            ..Config::default()
        };
        assert!(at.validate().is_ok());
        let over = Config {
            max_message_size: MAX_MESSAGE_SIZE + 1,
            ..Config::default()
        };
        assert_eq!(
            over.validate().unwrap_err(),
            ConfigError::MessageSizeTooLarge
        );
    }

    #[test]
    fn deliveries_per_connection_are_bounded_without_overflow() {
        let at = Config {
            max_subscriptions: 1024,
            prefetch: 1024,
            ..Config::default()
        };
        assert!(at.validate().is_ok());
        let over = Config {
            max_subscriptions: 1024,
            prefetch: 1025,
            ..Config::default()
        };
        assert_eq!(over.validate().unwrap_err(), ConfigError::TooManyDeliveries);
        let overflow = Config {
            max_subscriptions: usize::MAX,
            prefetch: 2,
            ..Config::default()
        };
        assert_eq!(
            overflow.validate().unwrap_err(),
            ConfigError::TooManyDeliveries
        );
    }

    #[test]
    fn default_ttl_must_not_exceed_max_ttl() {
        let secs = |s| Some(Duration::from_secs(s));
        let ok = Config {
            default_ttl: secs(5),
            max_ttl: secs(5),
            ..Config::default()
        };
        assert!(ok.validate().is_ok());
        let bad = Config {
            default_ttl: secs(6),
            max_ttl: secs(5),
            ..Config::default()
        };
        assert_eq!(bad.validate().unwrap_err(), ConfigError::DefaultTtlAboveMax);
    }

    #[test]
    fn errors_name_the_setting() {
        assert_eq!(
            ConfigError::Zero("prefetch").to_string(),
            "prefetch must be at least 1"
        );
    }
}
