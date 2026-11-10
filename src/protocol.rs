use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
pub struct SubmitJob {
    pub task: String,
    pub input: Value,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
}

pub(crate) const fn default_max_attempts() -> u32 {
    3
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SubmitWorkflow {
    pub first: SubmitJob,
    pub second: WorkflowStep,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WorkflowStep {
    pub task: String,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WorkflowStatus {
    pub id: Uuid,
    pub first: JobStatus,
    pub second: JobStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JobStatus {
    pub id: Uuid,
    pub task: String,
    pub attempts: u32,
    pub max_attempts: u32,
    #[serde(flatten)]
    pub state: JobState,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Running,
    Waiting,
    Blocked { error: String },
    Succeeded { output: Value },
    Failed { error: String },
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Assignment {
    pub attempt_id: Uuid,
    pub task: String,
    pub input: Value,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Poll {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub capacity: usize,
    pub tasks: HashSet<String>,
    pub active: HashSet<Uuid>,
    pub completed: Vec<Completion>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Completion {
    pub attempt_id: Uuid,
    pub result: AttemptResult,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub(crate) enum AttemptResult {
    Success { output: Value },
    Failure { error: String },
}
