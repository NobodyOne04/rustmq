//! The `rustmq` binary: only CLI wiring (ADR 0001, 0012). Settings are checked in
//! `rustmq::config`, clients go through `rustmq::client`.

use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use rustmq::broker::Broker;
use rustmq::client::Client;
use rustmq::config::Config;
use tokio::net::TcpListener;

/// A small in-memory TCP message broker.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

const ADDRESS: &str = "127.0.0.1:7000";

#[derive(Subcommand)]
enum Command {
    /// Runs the broker until Ctrl-C.
    Server(ServerArgs),
    /// Publishes one message and prints its id.
    Publish {
        #[arg(long, default_value = ADDRESS)]
        address: String,
        #[arg(long)]
        queue: String,
        #[arg(long)]
        message: String,
        #[arg(long)]
        ttl_ms: Option<u64>,
    },
    /// Prints each message body on its own line, then acks it.
    Consume {
        #[arg(long, default_value = ADDRESS)]
        address: String,
        #[arg(long)]
        queue: String,
        /// Exit after this many messages; without it, runs until Ctrl-C.
        #[arg(long)]
        count: Option<u64>,
    },
    /// Prints the server's metrics.
    Status {
        #[arg(long, default_value = ADDRESS)]
        address: String,
    },
}

// Rust note: the defaults are read from `Config::default()`, so they are written only once;
// clap evaluates these expressions when it builds the parser.
#[derive(Args)]
struct ServerArgs {
    #[arg(long, default_value_t = Config::default().bind)]
    bind: SocketAddr,
    #[arg(long, default_value_t = Config::default().max_connections)]
    max_connections: usize,
    #[arg(long, default_value_t = Config::default().max_message_size)]
    max_message_size: usize,
    #[arg(long, default_value_t = Config::default().max_queues)]
    max_queues: usize,
    #[arg(long, default_value_t = Config::default().max_queue_len)]
    max_queue_len: usize,
    #[arg(long, default_value_t = Config::default().max_total_bytes)]
    max_total_bytes: usize,
    #[arg(long, default_value_t = Config::default().max_queue_name_len)]
    max_queue_name_len: usize,
    #[arg(long, default_value_t = Config::default().max_subscriptions)]
    max_subscriptions: usize,
    #[arg(long, default_value_t = Config::default().prefetch)]
    prefetch: usize,
    /// 0 turns the ack timeout off.
    #[arg(long, default_value_t = millis(Config::default().ack_timeout))]
    ack_timeout_ms: u64,
    #[arg(long)]
    default_ttl_ms: Option<u64>,
    #[arg(long)]
    max_ttl_ms: Option<u64>,
    #[arg(long, default_value_t = millis(Config::default().shutdown_timeout))]
    shutdown_timeout_ms: u64,
    /// error, warn, info, debug or trace.
    #[arg(long, default_value_t = tracing::Level::INFO)]
    log: tracing::Level,
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

// Rust note: `ExitCode` instead of `Result` from `main`: a returned `Err` would be printed
// with `Debug` (`Zero("prefetch")`); this prints the `Display` text and still exits non-zero.
#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse().command).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(command: Command) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::Server(args) => serve(args).await,
        Command::Publish {
            address,
            queue,
            message,
            ttl_ms,
        } => {
            let mut client = Client::connect(address).await?;
            let ttl = ttl_ms.map(Duration::from_millis);
            println!("{}", client.publish(&queue, &message, ttl).await?);
            Ok(())
        }
        Command::Consume {
            address,
            queue,
            count,
        } => {
            let mut client = Client::connect(address).await?;
            client.subscribe(&queue).await?;
            // Ctrl-C needs no handling: the process ends, the connection closes, and the
            // server requeues whatever wasn't acked yet.
            let mut consumed = 0;
            while count.is_none_or(|n| consumed < n) {
                let message = client.next_message().await?;
                println!("{}", message.body);
                client.ack(message.delivery_id).await?;
                consumed += 1;
            }
            Ok(())
        }
        Command::Status { address } => {
            let s = Client::connect(address).await?.stats().await?;
            let metrics = [
                ("active_connections", s.active_connections),
                ("queues", s.queues),
                ("published", s.published),
                ("delivered", s.delivered),
                ("acked", s.acked),
                ("redelivered", s.redelivered),
                ("expired", s.expired),
                ("queued", s.queued),
            ];
            for (name, value) in metrics {
                println!("{name}: {value}");
            }
            Ok(())
        }
    }
}

async fn serve(args: ServerArgs) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(args.log)
        .with_writer(std::io::stderr)
        .init();
    let config = Config {
        bind: args.bind,
        max_connections: args.max_connections,
        max_message_size: args.max_message_size,
        max_queues: args.max_queues,
        max_queue_len: args.max_queue_len,
        max_total_bytes: args.max_total_bytes,
        max_queue_name_len: args.max_queue_name_len,
        max_subscriptions: args.max_subscriptions,
        prefetch: args.prefetch,
        ack_timeout: Duration::from_millis(args.ack_timeout_ms),
        default_ttl: args.default_ttl_ms.map(Duration::from_millis),
        max_ttl: args.max_ttl_ms.map(Duration::from_millis),
        shutdown_timeout: Duration::from_millis(args.shutdown_timeout_ms),
    };
    let (broker_config, server_config) = config.validate()?;
    let listener = TcpListener::bind(args.bind).await?;
    tracing::info!(address = %listener.local_addr()?, "listening");
    let broker = Arc::new(Broker::with_config(broker_config));
    rustmq::server::run(listener, broker, server_config, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    Ok(())
}
