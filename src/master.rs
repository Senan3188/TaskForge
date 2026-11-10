use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::dashboard;
use crate::protocol::{
    Assignment, AttemptResult, JobState, JobStatus, Poll, SubmitJob, SubmitWorkflow, WorkflowStatus,
};
use crate::store::{Job, Store, Workflow};

/// Schedules work and retries failed or expired attempts.
pub struct Master {
    scheduler: Scheduler,
}

impl Master {
    /// Creates an in-memory master with the given heartbeat lease duration.
    pub fn new(lease_duration: Duration) -> Self {
        assert!(!lease_duration.is_zero(), "lease duration must be positive");
        Self {
            scheduler: Scheduler::new(lease_duration),
        }
    }

    /// Opens the SQLite job database and recovers interrupted jobs before serving.
    pub fn open(lease_duration: Duration, database: impl AsRef<std::path::Path>) -> Result<Self> {
        let mut master = Self::new(lease_duration);
        let path = database.as_ref();
        let (store, jobs, workflows) = Store::open(path)
            .with_context(|| format!("could not recover job database {}", path.display()))?;
        let scheduler = &mut master.scheduler;
        scheduler.store = Some(store);
        let mut ordered = Vec::with_capacity(jobs.len());
        for job in jobs {
            let id = job.status.id;
            ordered.push(id);
            scheduler.jobs.insert(id, job);
        }
        for workflow in workflows {
            for job_id in [workflow.first, workflow.second] {
                scheduler
                    .jobs
                    .get_mut(&job_id)
                    .context("workflow job is missing")?
                    .workflow_id = Some(workflow.id);
            }
            scheduler.workflows.insert(workflow.id, workflow);
        }
        for id in ordered {
            let job = &scheduler.jobs[&id];
            match job.status.state {
                JobState::Queued => scheduler.queue.push_back(id),
                JobState::Running => {
                    let state = job.failure_state("master restarted during the attempt".into());
                    scheduler
                        .transition(id, state)
                        .context("could not recover interrupted job")?;
                }
                _ => {}
            }
        }
        Ok(master)
    }

    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
        let sweep_period = self.scheduler.lease_duration.min(Duration::from_secs(1));
        let scheduler = Arc::new(Mutex::new(self.scheduler));
        let app = Router::new()
            .route("/", get(dashboard::page))
            .route("/dashboard.css", get(dashboard::styles))
            .route("/dashboard.js", get(dashboard::script))
            .route("/v1/jobs", get(list).post(submit))
            .route("/v1/jobs/{id}", get(status))
            .route("/v1/jobs/{id}/result", get(result_preview))
            .route("/v1/workflows", post(submit_workflow))
            .route("/v1/workflows/{id}", get(workflow_status))
            .route("/v1/workers", get(list_workers))
            .route(
                "/v1/workers/{id}/poll",
                post(poll).layer(DefaultBodyLimit::disable()),
            )
            .with_state(Arc::clone(&scheduler));

        tokio::select! {
            result = axum::serve(listener, app) => result,
            () = async {
                let mut interval = tokio::time::interval(sweep_period);
                loop {
                    interval.tick().await;
                    let _ = write_scheduler(Arc::clone(&scheduler), |scheduler| {
                        scheduler.expire(Instant::now())
                    }).await;
                }
            } => unreachable!("lease sweeper runs until the server stops"),
        }
    }
}

type SharedScheduler = Arc<Mutex<Scheduler>>;

async fn write_scheduler<T: Send + 'static>(
    scheduler: SharedScheduler,
    operation: impl FnOnce(&mut Scheduler) -> Result<T> + Send + 'static,
) -> Result<T, (StatusCode, &'static str)> {
    tokio::task::spawn_blocking(move || operation(&mut scheduler.blocking_lock()))
        .await
        .context("scheduler operation stopped")
        .and_then(|result| result)
        .map_err(|error| {
            eprintln!("TaskForge scheduler error: {error:#}");
            (StatusCode::SERVICE_UNAVAILABLE, "scheduler write failed")
        })
}

#[derive(Serialize)]
struct JobSummary {
    id: Uuid,
    task: String,
    state: &'static str,
    attempts: u32,
    max_attempts: u32,
    submitted_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_step: Option<u8>,
}

async fn list(State(scheduler): State<SharedScheduler>) -> Json<Vec<JobSummary>> {
    let scheduler = scheduler.lock().await;
    let mut jobs: Vec<_> = scheduler
        .jobs
        .values()
        .map(|job| JobSummary {
            id: job.status.id,
            task: job.status.task.clone(),
            state: match job.status.state {
                JobState::Queued => "queued",
                JobState::Running => "running",
                JobState::Waiting => "waiting",
                JobState::Blocked { .. } => "blocked",
                JobState::Succeeded { .. } => "succeeded",
                JobState::Failed { .. } => "failed",
            },
            attempts: job.status.attempts,
            max_attempts: job.status.max_attempts,
            submitted_at_ms: job.submitted_at_ms,
            workflow_id: job.workflow_id,
            workflow_step: job.workflow_id.map(|id| {
                if scheduler.workflows[&id].first == job.status.id {
                    1
                } else {
                    2
                }
            }),
        })
        .collect();
    drop(scheduler);
    jobs.sort_unstable_by_key(|job| (std::cmp::Reverse(job.submitted_at_ms), job.id));
    Json(jobs)
}

async fn submit(
    State(scheduler): State<SharedScheduler>,
    Json(request): Json<SubmitJob>,
) -> Result<(StatusCode, Json<JobStatus>), (StatusCode, &'static str)> {
    if !valid_task(&request.task, request.max_attempts) {
        return Err((
            StatusCode::BAD_REQUEST,
            "task and positive max_attempts are required",
        ));
    }
    let job = write_scheduler(scheduler, move |scheduler| scheduler.submit(request)).await?;
    Ok((StatusCode::CREATED, Json(job)))
}

fn valid_task(name: &str, max_attempts: u32) -> bool {
    !name.trim().is_empty() && max_attempts > 0
}

async fn submit_workflow(
    State(scheduler): State<SharedScheduler>,
    Json(request): Json<SubmitWorkflow>,
) -> Result<(StatusCode, Json<WorkflowStatus>), (StatusCode, &'static str)> {
    if !valid_task(&request.first.task, request.first.max_attempts)
        || !valid_task(&request.second.task, request.second.max_attempts)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "each step needs a task and positive max_attempts",
        ));
    }
    let workflow = write_scheduler(scheduler, move |scheduler| {
        scheduler.submit_workflow(request)
    })
    .await?;
    Ok((StatusCode::CREATED, Json(workflow)))
}

async fn workflow_status(
    State(scheduler): State<SharedScheduler>,
    Path(id): Path<Uuid>,
) -> Result<Json<WorkflowStatus>, (StatusCode, &'static str)> {
    scheduler
        .lock()
        .await
        .workflow_status(id)
        .map(Json)
        .ok_or((StatusCode::NOT_FOUND, "workflow not found"))
}

async fn status(
    State(scheduler): State<SharedScheduler>,
    Path(id): Path<Uuid>,
) -> Result<Json<JobStatus>, (StatusCode, &'static str)> {
    let scheduler = scheduler.lock().await;
    scheduler
        .jobs
        .get(&id)
        .map(|job| Json(job.status.clone()))
        .ok_or((StatusCode::NOT_FOUND, "job not found"))
}

async fn result_preview(
    State(scheduler): State<SharedScheduler>,
    Path(id): Path<Uuid>,
) -> Result<String, (StatusCode, &'static str)> {
    let state = scheduler
        .lock()
        .await
        .jobs
        .get(&id)
        .map(|job| job.status.state.clone())
        .ok_or((StatusCode::NOT_FOUND, "job not found"))?;

    match state {
        JobState::Succeeded { output } => serde_json::to_string_pretty(&output)
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "could not format result")),
        JobState::Failed { error } | JobState::Blocked { error } => Ok(error),
        JobState::Queued | JobState::Running | JobState::Waiting => {
            Err((StatusCode::CONFLICT, "job is unfinished"))
        }
    }
}

async fn poll(
    State(scheduler): State<SharedScheduler>,
    Path(worker_id): Path<Uuid>,
    Json(request): Json<Poll>,
) -> Result<Json<Vec<Assignment>>, (StatusCode, &'static str)> {
    let assignments = write_scheduler(scheduler, move |scheduler| {
        scheduler.poll(worker_id, request, Instant::now())
    })
    .await?;
    Ok(Json(assignments))
}

struct Lease {
    worker_id: Uuid,
    job_id: Uuid,
    expires_at: Instant,
}

struct WorkerPresence {
    name: Option<String>,
    capacity: usize,
    active: HashSet<Uuid>,
    expires_at: Instant,
}

#[derive(Serialize)]
struct WorkerSummary {
    id: Uuid,
    name: Option<String>,
    capacity: usize,
    occupied: usize,
    jobs: Vec<WorkerJob>,
}

#[derive(Serialize)]
struct WorkerJob {
    id: Uuid,
    task: String,
}

async fn list_workers(State(scheduler): State<SharedScheduler>) -> Json<Vec<WorkerSummary>> {
    let scheduler = scheduler.lock().await;
    Json(scheduler.worker_summaries(Instant::now()))
}

struct Scheduler {
    store: Option<Store>,
    lease_duration: Duration,
    jobs: HashMap<Uuid, Job>,
    queue: VecDeque<Uuid>,
    leases: HashMap<Uuid, Lease>,
    workflows: HashMap<Uuid, Workflow>,
    workers: HashMap<Uuid, WorkerPresence>,
}

impl Scheduler {
    fn new(lease_duration: Duration) -> Self {
        Self {
            store: None,
            lease_duration,
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            leases: HashMap::new(),
            workflows: HashMap::new(),
            workers: HashMap::new(),
        }
    }

    fn submit(&mut self, request: SubmitJob) -> Result<JobStatus> {
        let job = Job::new(request, JobState::Queued, None)?;
        if let Some(store) = &self.store {
            store.insert(&job)?;
        }
        let status = job.status.clone();
        self.jobs.insert(status.id, job);
        self.queue.push_back(status.id);
        Ok(status)
    }

    fn submit_workflow(&mut self, request: SubmitWorkflow) -> Result<WorkflowStatus> {
        let id = Uuid::new_v4();
        let first = Job::new(request.first, JobState::Queued, Some(id))?;
        let second = Job::new(
            SubmitJob {
                task: request.second.task,
                input: serde_json::Value::Null,
                max_attempts: request.second.max_attempts,
            },
            JobState::Waiting,
            Some(id),
        )?;
        let workflow = Workflow {
            id,
            first: first.status.id,
            second: second.status.id,
        };
        if let Some(store) = &mut self.store {
            store.insert_workflow(&workflow, &first, &second)?;
        }
        let status = WorkflowStatus {
            id,
            first: first.status.clone(),
            second: second.status.clone(),
        };
        self.jobs.insert(workflow.first, first);
        self.jobs.insert(workflow.second, second);
        self.queue.push_back(workflow.first);
        self.workflows.insert(id, workflow);
        Ok(status)
    }

    fn workflow_status(&self, id: Uuid) -> Option<WorkflowStatus> {
        let workflow = self.workflows.get(&id)?;
        Some(WorkflowStatus {
            id,
            first: self.jobs[&workflow.first].status.clone(),
            second: self.jobs[&workflow.second].status.clone(),
        })
    }

    fn poll(&mut self, worker_id: Uuid, request: Poll, now: Instant) -> Result<Vec<Assignment>> {
        self.expire(now)?;
        for completion in request.completed {
            if self
                .leases
                .get(&completion.attempt_id)
                .is_some_and(|lease| lease.worker_id == worker_id)
            {
                self.finish(completion.attempt_id, completion.result)?;
            }
        }

        let mut free = request.capacity.saturating_sub(request.active.len());
        let mut assignments = Vec::new();
        let expires_at = now + self.lease_duration;
        for (&attempt_id, lease) in &mut self.leases {
            if lease.worker_id != worker_id {
                continue;
            }
            if request.active.contains(&attempt_id) {
                lease.expires_at = expires_at;
            } else if free > 0 {
                // The previous response may have been lost. Offer the same attempt again.
                let job = &self.jobs[&lease.job_id];
                assignments.push(Assignment {
                    attempt_id,
                    task: job.status.task.clone(),
                    input: job.input.clone(),
                });
                free -= 1;
            }
        }

        // Visit each queued job at most once. A worker may support only some task names.
        for _ in 0..self.queue.len() {
            if free == 0 {
                break;
            }
            let job_id = *self.queue.front().unwrap();
            let job = self.jobs.get_mut(&job_id).unwrap();
            if !request.tasks.contains(&job.status.task) {
                self.queue.pop_front();
                self.queue.push_back(job_id);
                continue;
            }

            let attempts = job.status.attempts + 1;
            if let Some(store) = &self.store {
                store.save_state(job, attempts, &JobState::Running)?;
            }
            self.queue.pop_front();
            let attempt_id = Uuid::new_v4();
            self.leases.insert(
                attempt_id,
                Lease {
                    worker_id,
                    job_id,
                    expires_at,
                },
            );
            job.status.attempts = attempts;
            job.status.state = JobState::Running;
            assignments.push(Assignment {
                attempt_id,
                task: job.status.task.clone(),
                input: job.input.clone(),
            });
            free -= 1;
        }

        self.workers.insert(
            worker_id,
            WorkerPresence {
                name: request.name,
                capacity: request.capacity,
                active: request.active,
                expires_at,
            },
        );
        Ok(assignments)
    }

    fn expire(&mut self, now: Instant) -> Result<()> {
        self.workers.retain(|_, worker| now < worker.expires_at);
        let expired: Vec<_> = self
            .leases
            .iter()
            .filter_map(|(&id, lease)| (now >= lease.expires_at).then_some(id))
            .collect();
        for id in expired {
            self.finish(
                id,
                AttemptResult::Failure {
                    error: "worker lease expired".into(),
                },
            )?;
        }
        Ok(())
    }

    fn finish(&mut self, attempt_id: Uuid, result: AttemptResult) -> Result<()> {
        let job_id = self.leases[&attempt_id].job_id;
        let state = match result {
            AttemptResult::Success { output } => JobState::Succeeded { output },
            AttemptResult::Failure { error } => self.jobs[&job_id].failure_state(error),
        };
        self.transition(job_id, state)?;
        self.leases.remove(&attempt_id);
        Ok(())
    }

    fn transition(&mut self, job_id: Uuid, state: JobState) -> Result<()> {
        let job = &self.jobs[&job_id];
        let dependent = job
            .workflow_id
            .and_then(|id| self.workflows.get(&id))
            .filter(|workflow| workflow.first == job_id)
            .and_then(|workflow| match &state {
                JobState::Succeeded { .. } => Some((workflow.second, JobState::Queued)),
                JobState::Failed { error } => Some((
                    workflow.second,
                    JobState::Blocked {
                        error: format!("Step 1 ({}) failed: {error}", job.status.task),
                    },
                )),
                _ => None,
            });
        if let Some((id, next_state)) = dependent {
            if let Some(store) = &mut self.store {
                store.advance_workflow(job, &state, &self.jobs[&id], &next_state)?;
            }
            let next = self.jobs.get_mut(&id).unwrap();
            if let JobState::Succeeded { output } = &state {
                next.input = output.clone();
                self.queue.push_back(id);
            }
            next.status.state = next_state;
        } else if let Some(store) = &self.store {
            store.save_state(job, job.status.attempts, &state)?;
        }
        if matches!(state, JobState::Queued) {
            self.queue.push_back(job_id);
        }
        self.jobs.get_mut(&job_id).unwrap().status.state = state;
        Ok(())
    }

    fn worker_summaries(&self, now: Instant) -> Vec<WorkerSummary> {
        let mut workers: HashMap<_, _> = self
            .workers
            .iter()
            .filter(|(_, worker)| now < worker.expires_at)
            .map(|(&id, worker)| {
                (
                    id,
                    WorkerSummary {
                        id,
                        name: worker.name.clone(),
                        capacity: worker.capacity,
                        occupied: worker.active.len(),
                        jobs: Vec::new(),
                    },
                )
            })
            .collect();
        for (&attempt_id, lease) in &self.leases {
            if now < lease.expires_at
                && let Some(worker) = workers.get_mut(&lease.worker_id)
            {
                let job = &self.jobs[&lease.job_id].status;
                worker.jobs.push(WorkerJob {
                    id: job.id,
                    task: job.task.clone(),
                });
                if !self.workers[&lease.worker_id].active.contains(&attempt_id) {
                    worker.occupied += 1;
                }
            }
        }
        let mut workers: Vec<_> = workers.into_values().collect();
        for worker in &mut workers {
            worker.jobs.sort_unstable_by_key(|job| job.id);
        }
        workers.sort_unstable_by(|a, b| (&a.name, a.id).cmp(&(&b.name, b.id)));
        workers
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::json;

    use super::*;
    use crate::protocol::{Completion, WorkflowStep};

    fn square_poll(active: HashSet<Uuid>, completed: Vec<Completion>) -> Poll {
        Poll {
            name: None,
            capacity: 1,
            tasks: HashSet::from(["square".into()]),
            active,
            completed,
        }
    }

    fn submit_square(scheduler: &mut Scheduler, max_attempts: u32) -> Uuid {
        scheduler
            .submit(SubmitJob {
                task: "square".into(),
                input: json!(3),
                max_attempts,
            })
            .unwrap()
            .id
    }

    #[test]
    fn workers_report_assignments_once_and_disappear_after_timeout() {
        let duration = Duration::from_secs(5);
        let mut scheduler = Scheduler::new(duration);
        let worker = Uuid::new_v4();
        let now = Instant::now();
        let job = submit_square(&mut scheduler, 1);
        let mut poll = square_poll(HashSet::new(), vec![]);
        poll.name = Some("worker-a".into());
        poll.capacity = 2;
        let attempt = scheduler.poll(worker, poll, now).unwrap()[0].attempt_id;
        let summaries = scheduler.worker_summaries(now);
        assert_eq!(summaries[0].name.as_deref(), Some("worker-a"));
        assert_eq!(summaries[0].capacity, 2);
        assert_eq!(summaries[0].occupied, 1);
        assert_eq!(summaries[0].jobs[0].id, job);
        scheduler
            .poll(worker, square_poll(HashSet::from([attempt]), vec![]), now)
            .unwrap();
        assert_eq!(scheduler.worker_summaries(now)[0].occupied, 1);
        scheduler
            .poll(
                worker,
                square_poll(
                    HashSet::new(),
                    vec![Completion {
                        attempt_id: attempt,
                        result: AttemptResult::Success { output: json!(9) },
                    }],
                ),
                now,
            )
            .unwrap();
        assert_eq!(scheduler.worker_summaries(now)[0].occupied, 0);
        assert!(scheduler.worker_summaries(now + duration).is_empty());
    }

    #[test]
    fn workflows_restore_handoffs_and_block_dependents_after_a_restart() {
        let directory = std::env::temp_dir().join(format!("taskforge-{}", Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("workflows.sqlite3");
        let lease = Duration::from_secs(5);
        let mut master = Master::open(lease, &path).unwrap();
        let submit = |scheduler: &mut Scheduler| {
            scheduler
                .submit_workflow(SubmitWorkflow {
                    first: SubmitJob {
                        task: "square".into(),
                        input: json!(3),
                        max_attempts: 1,
                    },
                    second: WorkflowStep {
                        task: "format_report".into(),
                        max_attempts: 2,
                    },
                })
                .unwrap()
        };
        let worker = Uuid::new_v4();
        let now = Instant::now();
        let first = submit(&mut master.scheduler);
        assert!(matches!(first.second.state, JobState::Waiting));
        let attempt = master
            .scheduler
            .poll(worker, square_poll(HashSet::new(), vec![]), now)
            .unwrap()[0]
            .attempt_id;
        master
            .scheduler
            .finish(attempt, AttemptResult::Success { output: json!(9) })
            .unwrap();
        let interrupted = submit(&mut master.scheduler);
        master
            .scheduler
            .poll(worker, square_poll(HashSet::new(), vec![]), now)
            .unwrap();
        drop(master);

        let mut master = Master::open(lease, &path).unwrap();
        let scheduler = &mut master.scheduler;
        let status = scheduler.workflow_status(first.id).unwrap();
        assert_eq!(status.second.id, first.second.id);
        assert!(matches!(status.second.state, JobState::Queued));
        let blocked = scheduler.workflow_status(interrupted.id).unwrap().second;
        assert_eq!(blocked.attempts, 0);
        assert!(
            matches!(blocked.state, JobState::Blocked { error } if error.contains("master restarted"))
        );
        let mut poll = square_poll(HashSet::new(), vec![]);
        poll.tasks = HashSet::from(["format_report".into()]);
        let assignments = scheduler.poll(worker, poll, now).unwrap();
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].input, json!(9));
        scheduler
            .finish(
                assignments[0].attempt_id,
                AttemptResult::Success {
                    output: json!("Word count: 9"),
                },
            )
            .unwrap();
        drop(master);

        let master = Master::open(lease, &path).unwrap();
        let status = master.scheduler.workflow_status(first.id).unwrap();
        assert!(
            matches!(status.second.state, JobState::Succeeded { output } if output == json!("Word count: 9"))
        );
        drop(master);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn missing_poll_response_reoffers_the_same_attempt() {
        let mut scheduler = Scheduler::new(Duration::from_secs(5));
        let job = submit_square(&mut scheduler, 2);
        let next_job = submit_square(&mut scheduler, 2);
        let worker = Uuid::new_v4();
        let now = Instant::now();
        let first = scheduler
            .poll(worker, square_poll(HashSet::new(), vec![]), now)
            .unwrap();
        let retry = scheduler
            .poll(worker, square_poll(HashSet::new(), vec![]), now)
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(retry.len(), 1);
        assert_eq!(first[0].attempt_id, retry[0].attempt_id);
        assert_eq!(scheduler.jobs[&job].status.attempts, 1);
        assert!(matches!(
            scheduler.jobs[&next_job].status.state,
            JobState::Queued
        ));

        let attempt_id = first[0].attempt_id;
        let acknowledge = |scheduler: &mut Scheduler| {
            scheduler
                .poll(
                    worker,
                    square_poll(
                        HashSet::new(),
                        vec![Completion {
                            attempt_id,
                            result: AttemptResult::Success { output: json!(9) },
                        }],
                    ),
                    now,
                )
                .unwrap()
        };
        let next = acknowledge(&mut scheduler);
        let replay = acknowledge(&mut scheduler);
        assert_eq!(next.len(), 1);
        assert_eq!(replay.len(), 1);
        assert_eq!(next[0].attempt_id, replay[0].attempt_id);
        assert!(
            matches!(&scheduler.jobs[&job].status.state, JobState::Succeeded { output } if output == &json!(9))
        );
        assert_eq!(scheduler.jobs[&next_job].status.attempts, 1);
    }

    #[test]
    fn heartbeat_renews_the_lease_and_late_result_cannot_finish_a_retry() {
        let mut scheduler = Scheduler::new(Duration::from_secs(5));
        let job = submit_square(&mut scheduler, 2);
        let original_worker = Uuid::new_v4();
        let replacement_worker = Uuid::new_v4();
        let start = Instant::now();
        let first = scheduler
            .poll(original_worker, square_poll(HashSet::new(), vec![]), start)
            .unwrap();
        let first_id = first[0].attempt_id;

        let heartbeat = start + Duration::from_secs(4);
        assert!(
            scheduler
                .poll(
                    original_worker,
                    square_poll(HashSet::from([first_id]), vec![]),
                    heartbeat
                )
                .unwrap()
                .is_empty()
        );
        scheduler.expire(start + Duration::from_secs(5)).unwrap();
        assert!(matches!(
            scheduler.jobs[&job].status.state,
            JobState::Running
        ));

        let deadline = heartbeat + Duration::from_secs(5);
        let replacement = scheduler
            .poll(
                replacement_worker,
                square_poll(HashSet::new(), vec![]),
                deadline,
            )
            .unwrap();
        let replacement_id = replacement[0].attempt_id;
        assert_ne!(replacement_id, first_id);
        assert_eq!(scheduler.jobs[&job].status.attempts, 2);

        scheduler
            .poll(
                original_worker,
                square_poll(
                    HashSet::new(),
                    vec![Completion {
                        attempt_id: first_id,
                        result: AttemptResult::Success { output: json!(9) },
                    }],
                ),
                deadline,
            )
            .unwrap();
        assert!(matches!(
            scheduler.jobs[&job].status.state,
            JobState::Running
        ));

        scheduler
            .poll(
                replacement_worker,
                square_poll(
                    HashSet::new(),
                    vec![Completion {
                        attempt_id: replacement_id,
                        result: AttemptResult::Failure {
                            error: "handler failed".into(),
                        },
                    }],
                ),
                deadline,
            )
            .unwrap();
        assert!(
            matches!(&scheduler.jobs[&job].status.state, JobState::Failed { error } if error == "handler failed")
        );
    }

    #[test]
    fn sqlite_recovery_preserves_results_and_invalidates_interrupted_attempts() {
        let directory = std::env::temp_dir().join(format!("taskforge-{}", Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("jobs.sqlite3");
        let lease = Duration::from_secs(5);
        let mut master = Master::open(lease, &path).unwrap();
        assert!(Master::open(lease, &path).is_err());
        let worker = Uuid::new_v4();
        let now = Instant::now();

        let completed = submit_square(&mut master.scheduler, 1);
        let first = master
            .scheduler
            .poll(worker, square_poll(HashSet::new(), vec![]), now)
            .unwrap();
        master
            .scheduler
            .finish(
                first[0].attempt_id,
                AttemptResult::Success { output: json!(9) },
            )
            .unwrap();
        let interrupted = submit_square(&mut master.scheduler, 2);
        let exhausted = submit_square(&mut master.scheduler, 1);
        let mut poll = square_poll(HashSet::new(), vec![]);
        poll.capacity = 2;
        let attempts = master.scheduler.poll(worker, poll, now).unwrap();
        let queued = submit_square(&mut master.scheduler, 1);
        let submitted_at_ms = master.scheduler.jobs[&interrupted].submitted_at_ms;
        drop(master);

        let mut master = Master::open(lease, &path).unwrap();
        let scheduler = &mut master.scheduler;
        assert!(
            matches!(&scheduler.jobs[&completed].status.state, JobState::Succeeded { output } if output == &json!(9))
        );
        assert!(
            matches!(&scheduler.jobs[&exhausted].status.state, JobState::Failed { error } if error.contains("master restarted"))
        );
        assert!(matches!(
            scheduler.jobs[&queued].status.state,
            JobState::Queued
        ));
        let job = &scheduler.jobs[&interrupted];
        assert!(matches!(job.status.state, JobState::Queued));
        assert_eq!(job.status.attempts, 1);
        assert_eq!(job.submitted_at_ms, submitted_at_ms);
        assert_eq!(job.input, json!(3));

        let stale = Completion {
            attempt_id: attempts[0].attempt_id,
            result: AttemptResult::Success { output: json!(999) },
        };
        let retry = scheduler
            .poll(worker, square_poll(HashSet::new(), vec![stale]), now)
            .unwrap();
        assert_ne!(retry[0].attempt_id, attempts[0].attempt_id);
        assert_eq!(scheduler.jobs[&interrupted].status.attempts, 2);
        assert!(matches!(
            scheduler.jobs[&interrupted].status.state,
            JobState::Running
        ));
        scheduler
            .finish(
                retry[0].attempt_id,
                AttemptResult::Success { output: json!(9) },
            )
            .unwrap();
        drop(master);

        let master = Master::open(lease, &path).unwrap();
        assert!(Master::open(lease, &path).is_err());
        assert!(
            matches!(&master.scheduler.jobs[&interrupted].status.state, JobState::Succeeded { output } if output == &json!(9))
        );
        drop(master);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
