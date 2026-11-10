use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde_json::Value;
use taskforge::{Client, Master, SubmitJob, SubmitWorkflow, TaskRegistry, Worker, WorkflowStep};
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
        /// Path to the SQLite job database.
        #[arg(long, default_value = "taskforge.sqlite3")]
        database: PathBuf,
    },
    /// Run a worker with the built-in tasks.
    Worker {
        #[arg(long, default_value = DEFAULT_MASTER)]
        master: String,
        #[arg(long, default_value = "1")]
        capacity: NonZeroUsize,
        /// Label this worker in the dashboard.
        #[arg(long)]
        name: Option<String>,
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
    /// Submit two tasks, passing the first task's output to the second.
    SubmitWorkflow {
        #[arg(long, default_value = DEFAULT_MASTER)]
        master: String,
        #[arg(long, default_value = "3")]
        max_attempts: NonZeroU32,
        first: String,
        second: String,
        #[arg(allow_negative_numbers = true, value_parser = parse_json)]
        input: Value,
    },
    /// Retrieve both steps of a workflow.
    WorkflowStatus {
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
            database,
        } => {
            let listener = TcpListener::bind(bind)
                .await
                .with_context(|| format!("could not bind to {bind}"))?;
            let master = Master::open(Duration::from_secs(lease_seconds), &database)?;
            eprintln!(
                "TaskForge master listening on {} (database {})",
                listener.local_addr()?,
                database.display()
            );
            master.serve(listener).await?;
        }
        Command::Worker {
            master,
            capacity,
            name,
        } => {
            let client = Client::new(&master)?;
            let mut registry = TaskRegistry::default();
            registry.register("square", |input: i64| {
                input.checked_mul(input).ok_or("square overflowed i64")
            })?;
            registry.register("count_words", |input: String| {
                Ok::<_, std::convert::Infallible>(input.split_whitespace().count())
            })?;
            registry.register("format_report", |count: u64| {
                Ok::<_, std::convert::Infallible>(format!("Word count: {count}"))
            })?;
            eprintln!("TaskForge worker polling {master} with capacity {capacity}");
            let worker = Worker::new(client, registry, capacity);
            match name {
                Some(name) => worker.with_name(name),
                None => worker,
            }
            .run()
            .await?;
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
        Command::SubmitWorkflow {
            master,
            max_attempts,
            first,
            second,
            input,
        } => {
            let max_attempts = max_attempts.get();
            let workflow = Client::new(&master)?
                .submit_workflow(&SubmitWorkflow {
                    first: SubmitJob {
                        task: first,
                        input,
                        max_attempts,
                    },
                    second: WorkflowStep {
                        task: second,
                        max_attempts,
                    },
                })
                .await?;
            println!("{}", serde_json::to_string_pretty(&workflow)?);
        }
        Command::WorkflowStatus { master, id } => {
            let workflow = Client::new(&master)?.workflow_status(id).await?;
            println!("{}", serde_json::to_string_pretty(&workflow)?);
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
