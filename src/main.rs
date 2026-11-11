use clap::Parser;

/// A small in-memory TCP message broker.
#[derive(Parser)]
#[command(version, about)]
struct Cli {}

// Rust note: returning `Result` from `main` prints the error and exits with a failure code,
// so any module's typed error can surface here with `?` (ADR 0009).
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Cli::parse();
    Ok(())
}
