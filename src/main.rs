use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde_json::Value;
use taskforge::{Client, Master, SubmitJob, TaskRegistry, Worker};
use tokio::net::TcpListener;
use uuid::Uuid;

const DEFAULT_MASTER: &str = "http://127.0.0.1:7878";

#[derive(Parser)]
#[command(version, about = "Run Rust tasks across worker processes")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the scheduler and HTTP API.
    Master {
        #[arg(long, default_value = "127.0.0.1:7878")]
        bind: SocketAddr,
        #[arg(long, default_value = "15", value_parser = clap::value_parser!(u64).range(2..=86_400))]
        lease_seconds: u64,
    },
    /// Run a worker with the built-in square task.
    Worker {
        #[arg(long, default_value = DEFAULT_MASTER)]
        master: String,
        #[arg(long, default_value = "1")]
        capacity: NonZeroUsize,
    },
    /// Queue a named task with a JSON input.
    Submit {
        #[arg(long, default_value = DEFAULT_MASTER)]
        master: String,
        #[arg(long, default_value = "3")]
        max_attempts: NonZeroU32,
        task: String,
        #[arg(allow_negative_numbers = true, value_parser = parse_json)]
        input: Value,
    },
    /// Retrieve a job's state and, when available, its result.
    Status {
        #[arg(long, default_value = DEFAULT_MASTER)]
        master: String,
        id: Uuid,
    },
}

fn parse_json(input: &str) -> Result<Value, serde_json::Error> {
    serde_json::from_str(input)
}

#[tokio::main]
async fn main() -> Result<()> {
    match Args::parse().command {
        Command::Master {
            bind,
            lease_seconds,
        } => {
            let listener = TcpListener::bind(bind)
                .await
                .with_context(|| format!("could not bind to {bind}"))?;
            eprintln!("TaskForge master listening on {}", listener.local_addr()?);
            Master::new(Duration::from_secs(lease_seconds))
                .serve(listener)
                .await?;
        }
        Command::Worker { master, capacity } => {
            let client = Client::new(&master)?;
            let mut registry = TaskRegistry::default();
            registry.register("square", |input: i64| {
                input.checked_mul(input).ok_or("square overflowed i64")
            })?;
            eprintln!("TaskForge worker polling {master} with capacity {capacity}");
            Worker::new(client, registry, capacity).run().await?;
        }
        Command::Submit {
            master,
            max_attempts,
            task,
            input,
        } => {
            let job = Client::new(&master)?
                .submit(&SubmitJob {
                    task,
                    input,
                    max_attempts: max_attempts.get(),
                })
                .await?;
            println!("{}", serde_json::to_string_pretty(&job)?);
        }
        Command::Status { master, id } => {
            let job = Client::new(&master)?.status(id).await?;
            println!("{}", serde_json::to_string_pretty(&job)?);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_parses_json_instead_of_treating_input_as_a_string() {
        let args = Args::try_parse_from(["taskforge", "submit", "square", "-12"]).unwrap();
        assert!(matches!(args.command, Command::Submit { input, .. } if input == -12));
        assert!(Args::try_parse_from(["taskforge", "submit", "square", "invalid-json"]).is_err());
    }
}
