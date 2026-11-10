use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, TransactionBehavior, params};
use serde_json::Value;
use uuid::Uuid;

use crate::protocol::{JobState, JobStatus, SubmitJob};

pub(crate) struct Job {
    pub status: JobStatus,
    pub input: Value,
    pub submitted_at_ms: i64,
    pub workflow_id: Option<Uuid>,
}

pub(crate) struct Workflow {
    pub id: Uuid,
    pub first: Uuid,
    pub second: Uuid,
}

impl Job {
    pub fn new(request: SubmitJob, state: JobState, workflow_id: Option<Uuid>) -> Result<Self> {
        Ok(Self {
            status: JobStatus {
                id: Uuid::new_v4(),
                task: request.task,
                attempts: 0,
                max_attempts: request.max_attempts,
                state,
            },
            input: request.input,
            submitted_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()?,
            workflow_id,
        })
    }

    pub fn failure_state(&self, error: String) -> JobState {
        if self.status.attempts < self.status.max_attempts {
            JobState::Queued
        } else {
            JobState::Failed { error }
        }
    }
}

pub(crate) struct Store {
    db: Connection,
}

impl Store {
    pub fn open(path: &Path) -> Result<(Self, Vec<Job>, Vec<Workflow>)> {
        let mut db = Connection::open(path)?;
        db.busy_timeout(Duration::from_secs(1))?;
        // Only one master may hold leases for a database.
        db.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
        db.pragma_update(None, "synchronous", "FULL")?;
        db.pragma_update(None, "foreign_keys", "ON")?;

        let tx = db
            .transaction_with_behavior(TransactionBehavior::Exclusive)
            .context("could not lock the job database")?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY NOT NULL,
                task TEXT NOT NULL,
                input TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                max_attempts INTEGER NOT NULL,
                state TEXT NOT NULL,
                submitted_at_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS workflows (
                id TEXT PRIMARY KEY NOT NULL,
                first_job_id TEXT UNIQUE NOT NULL REFERENCES jobs(id),
                second_job_id TEXT UNIQUE NOT NULL REFERENCES jobs(id)
            )",
        )?;

        let jobs = {
            let mut stmt = tx.prepare(
                "SELECT id, task, input, attempts, max_attempts, state, submitted_at_ms
                 FROM jobs ORDER BY submitted_at_ms, rowid",
            )?;
            let mut rows = stmt.query([])?;
            let mut jobs = Vec::new();
            while let Some(row) = rows.next()? {
                let id: String = row.get(0)?;
                let input: String = row.get(2)?;
                let state: String = row.get(5)?;
                jobs.push(Job {
                    status: JobStatus {
                        id: id.parse()?,
                        task: row.get(1)?,
                        attempts: row.get(3)?,
                        max_attempts: row.get(4)?,
                        state: serde_json::from_str(&state)?,
                    },
                    input: serde_json::from_str(&input)?,
                    submitted_at_ms: row.get(6)?,
                    workflow_id: None,
                });
            }
            jobs
        };

        let workflows = {
            let mut stmt = tx.prepare("SELECT id, first_job_id, second_job_id FROM workflows")?;
            let mut rows = stmt.query([])?;
            let mut workflows = Vec::new();
            while let Some(row) = rows.next()? {
                let id: String = row.get(0)?;
                let first: String = row.get(1)?;
                let second: String = row.get(2)?;
                workflows.push(Workflow {
                    id: id.parse()?,
                    first: first.parse()?,
                    second: second.parse()?,
                });
            }
            workflows
        };

        tx.commit()?;
        Ok((Self { db }, jobs, workflows))
    }

    pub fn insert(&self, job: &Job) -> Result<()> {
        Self::insert_job(&self.db, job)
    }

    pub fn insert_workflow(
        &mut self,
        workflow: &Workflow,
        first: &Job,
        second: &Job,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        Self::insert_job(&tx, first)?;
        Self::insert_job(&tx, second)?;
        tx.execute(
            "INSERT INTO workflows (id, first_job_id, second_job_id) VALUES (?1, ?2, ?3)",
            params![
                workflow.id.to_string(),
                workflow.first.to_string(),
                workflow.second.to_string()
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn insert_job(db: &Connection, job: &Job) -> Result<()> {
        db.execute(
            "INSERT INTO jobs (id, task, input, attempts, max_attempts, state, submitted_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                job.status.id.to_string(),
                job.status.task,
                serde_json::to_string(&job.input)?,
                job.status.attempts,
                job.status.max_attempts,
                serde_json::to_string(&job.status.state)?,
                job.submitted_at_ms,
            ],
        )?;
        Ok(())
    }

    pub fn save_state(&self, job: &Job, attempts: u32, state: &JobState) -> Result<()> {
        Self::update_state(&self.db, job, attempts, state)
    }

    pub fn advance_workflow(
        &mut self,
        first: &Job,
        state: &JobState,
        second: &Job,
        next_state: &JobState,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        Self::update_state(&tx, first, first.status.attempts, state)?;
        Self::update_state(&tx, second, second.status.attempts, next_state)?;
        if let JobState::Succeeded { output } = state {
            tx.execute(
                "UPDATE jobs SET input = ?1 WHERE id = ?2",
                params![serde_json::to_string(output)?, second.status.id.to_string()],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn update_state(db: &Connection, job: &Job, attempts: u32, state: &JobState) -> Result<()> {
        let changed = db.execute(
            "UPDATE jobs SET attempts = ?1, state = ?2 WHERE id = ?3",
            params![
                attempts,
                serde_json::to_string(state)?,
                job.status.id.to_string(),
            ],
        )?;
        ensure!(
            changed == 1,
            "job {} is missing from storage",
            job.status.id
        );
        Ok(())
    }
}
