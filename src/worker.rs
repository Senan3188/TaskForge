use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::Client;
use crate::protocol::{AttemptResult, Completion, Poll};

type Handler = Box<dyn Fn(Value) -> Result<Value> + Send + Sync>;

/// Named handlers with JSON inputs and outputs.
#[derive(Default)]
pub struct TaskRegistry {
    handlers: HashMap<String, Handler>,
}

impl TaskRegistry {
    /// Registers a synchronous Rust function to run on the worker's blocking thread pool.
    pub fn register<I, O, E, F>(&mut self, name: impl Into<String>, handler: F) -> Result<()>
    where
        I: DeserializeOwned,
        O: Serialize,
        E: Display,
        F: Fn(I) -> std::result::Result<O, E> + Send + Sync + 'static,
    {
        let name = name.into();
        ensure!(!name.trim().is_empty(), "task name must be nonempty");
        ensure!(
            !self.handlers.contains_key(&name),
            "task already registered: {name}"
        );
        self.handlers.insert(
            name,
            Box::new(move |input| {
                let input = serde_json::from_value(input).context("invalid task input")?;
                let output = handler(input).map_err(|error| anyhow::anyhow!("{error}"))?;
                serde_json::to_value(output).context("could not serialize task output")
            }),
        );
        Ok(())
    }
}

/// A process that polls a master and executes tasks up to its configured capacity.
pub struct Worker {
    client: Client,
    registry: Arc<TaskRegistry>,
    capacity: NonZeroUsize,
    name: Option<String>,
}

impl Worker {
    pub fn new(client: Client, registry: TaskRegistry, capacity: NonZeroUsize) -> Self {
        Self {
            client,
            registry: Arc::new(registry),
            capacity,
            name: None,
        }
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Polls once per second, reporting running and completed attempts on each poll.
    pub async fn run(self) -> Result<()> {
        ensure!(
            !self.registry.handlers.is_empty(),
            "register at least one task before starting"
        );
        let worker_id = Uuid::new_v4();
        eprintln!("TaskForge worker ID: {worker_id}");
        let mut poll = Poll {
            name: self.name,
            capacity: self.capacity.get(),
            tasks: self.registry.handlers.keys().cloned().collect(),
            active: HashSet::new(),
            completed: Vec::new(),
        };
        let mut running = JoinSet::<Completion>::new();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            interval.tick().await;
            while let Some(completion) = running.try_join_next() {
                let completion = completion.context("worker could not collect task completion")?;
                poll.active.remove(&completion.attempt_id);
                poll.completed.push(completion);
            }

            let assignments = match self.client.poll(worker_id, &poll).await {
                Ok(assignments) => assignments,
                Err(error) => {
                    eprintln!("worker {worker_id} could not poll master: {error:#}");
                    continue;
                }
            };
            poll.completed.clear();

            for assignment in assignments {
                if poll.active.len() >= poll.capacity {
                    break;
                }
                if !poll.active.insert(assignment.attempt_id) {
                    continue;
                }
                let registry = Arc::clone(&self.registry);
                running.spawn(async move {
                    let attempt_id = assignment.attempt_id;
                    let result = tokio::task::spawn_blocking(move || {
                        let handler = registry
                            .handlers
                            .get(&assignment.task)
                            .with_context(|| format!("unregistered task {}", assignment.task))?;
                        handler(assignment.input)
                    })
                    .await;

                    let result = match result {
                        Ok(Ok(output)) => AttemptResult::Success { output },
                        Ok(Err(error)) => AttemptResult::Failure {
                            error: format!("{error:#}"),
                        },
                        Err(error) => AttemptResult::Failure {
                            error: format!("task stopped: {error}"),
                        },
                    };
                    Completion { attempt_id, result }
                });
            }
        }
    }
}
