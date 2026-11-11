use clap::Parser;

/// A small in-memory TCP message broker.
#[derive(Parser)]
#[command(version, about)]
struct Cli {}

fn main() {
    Cli::parse();
}
