use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use serde_json::Value;
use tokio::net::TcpListener;
use uuid::Uuid;

use crate::dashboard;
use crate::protocol::{Assignment, AttemptResult, JobState, JobStatus, Poll, SubmitJob};

/// An in-memory scheduler that retries work when an attempt fails or its lease expires.
pub struct Master {
    lease_duration: Duration,
}

impl Master {
    /// Sets how long an assignment can go without a heartbeat before it expires.
    pub fn new(lease_duration: Duration) -> Self {
        assert!(!lease_duration.is_zero(), "lease duration must be positive");
        Self { lease_duration }
    }

    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
        let scheduler = Arc::new(Mutex::new(Scheduler::new(self.lease_duration)));
        let app = Router::new()
            .route("/", get(dashboard::page))
            .route("/dashboard.css", get(dashboard::styles))
            .route("/dashboard.js", get(dashboard::script))
            .route("/v1/jobs", get(list).post(submit))
            .route("/v1/jobs/{id}", get(status))
            .route("/v1/jobs/{id}/result", get(result_preview))
            .route(
                "/v1/workers/{id}/poll",
                post(poll).layer(DefaultBodyLimit::disable()),
            )
            .with_state(Arc::clone(&scheduler));

        let sweep_period = self.lease_duration.min(Duration::from_secs(1));
        tokio::select! {
            result = axum::serve(listener, app) => result,
            () = async {
                let mut interval = tokio::time::interval(sweep_period);
                loop {
                    interval.tick().await;
                    scheduler.lock().unwrap().expire(Instant::now());
                }
            } => unreachable!("lease sweeper runs until the server stops"),
        }
    }
}

type SharedScheduler = Arc<Mutex<Scheduler>>;

#[derive(Serialize)]
struct JobSummary {
    id: Uuid,
    task: String,
    state: &'static str,
    attempts: u32,
    max_attempts: u32,
    submitted_at_ms: u128,
}

async fn list(State(scheduler): State<SharedScheduler>) -> Json<Vec<JobSummary>> {
    let mut jobs: Vec<_> = scheduler
        .lock()
        .unwrap()
        .jobs
        .values()
        .map(|job| JobSummary {
            id: job.status.id,
            task: job.status.task.clone(),
            state: match job.status.state {
                JobState::Queued => "queued",
                JobState::Running => "running",
                JobState::Succeeded { .. } => "succeeded",
                JobState::Failed { .. } => "failed",
            },
            attempts: job.status.attempts,
            max_attempts: job.status.max_attempts,
            submitted_at_ms: job.submitted_at_ms,
        })
        .collect();
    jobs.sort_unstable_by_key(|job| (std::cmp::Reverse(job.submitted_at_ms), job.id));
    Json(jobs)
}

async fn submit(
    State(scheduler): State<SharedScheduler>,
    Json(request): Json<SubmitJob>,
) -> Result<(StatusCode, Json<JobStatus>), (StatusCode, &'static str)> {
    if request.task.trim().is_empty() || request.max_attempts == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "task and positive max_attempts are required",
        ));
    }
    let job = scheduler.lock().unwrap().submit(request);
    Ok((StatusCode::CREATED, Json(job)))
}

async fn status(
    State(scheduler): State<SharedScheduler>,
    Path(id): Path<Uuid>,
) -> Result<Json<JobStatus>, (StatusCode, &'static str)> {
    let scheduler = scheduler.lock().unwrap();
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
        .unwrap()
        .jobs
        .get(&id)
        .map(|job| job.status.state.clone())
        .ok_or((StatusCode::NOT_FOUND, "job not found"))?;

    match state {
        JobState::Succeeded { output } => serde_json::to_string_pretty(&output)
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "could not format result")),
        JobState::Failed { error } => Ok(error),
        JobState::Queued | JobState::Running => Err((StatusCode::CONFLICT, "job is unfinished")),
    }
}

async fn poll(
    State(scheduler): State<SharedScheduler>,
    Path(worker_id): Path<Uuid>,
    Json(request): Json<Poll>,
) -> Json<Vec<Assignment>> {
    let mut scheduler = scheduler.lock().unwrap();
    Json(scheduler.poll(worker_id, request, Instant::now()))
}

struct Job {
    status: JobStatus,
    input: Value,
    submitted_at_ms: u128,
}

struct Lease {
    worker_id: Uuid,
    job_id: Uuid,
    expires_at: Instant,
}

struct Scheduler {
    lease_duration: Duration,
    jobs: HashMap<Uuid, Job>,
    queue: VecDeque<Uuid>,
    leases: HashMap<Uuid, Lease>,
}

impl Scheduler {
    fn new(lease_duration: Duration) -> Self {
        Self {
            lease_duration,
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            leases: HashMap::new(),
        }
    }

    fn submit(&mut self, request: SubmitJob) -> JobStatus {
        let status = JobStatus {
            id: Uuid::new_v4(),
            task: request.task,
            attempts: 0,
            max_attempts: request.max_attempts,
            state: JobState::Queued,
        };
        self.jobs.insert(
            status.id,
            Job {
                status: status.clone(),
                input: request.input,
                submitted_at_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            },
        );
        self.queue.push_back(status.id);
        status
    }

    fn poll(&mut self, worker_id: Uuid, request: Poll, now: Instant) -> Vec<Assignment> {
        self.expire(now);
        for completion in request.completed {
            if self
                .leases
                .get(&completion.attempt_id)
                .is_some_and(|lease| lease.worker_id == worker_id)
            {
                self.finish(completion.attempt_id, completion.result);
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
            let job_id = self.queue.pop_front().unwrap();
            let job = self.jobs.get_mut(&job_id).unwrap();
            if !request.tasks.contains(&job.status.task) {
                self.queue.push_back(job_id);
                continue;
            }

            let attempt_id = Uuid::new_v4();
            self.leases.insert(
                attempt_id,
                Lease {
                    worker_id,
                    job_id,
                    expires_at,
                },
            );
            job.status.attempts += 1;
            job.status.state = JobState::Running;
            assignments.push(Assignment {
                attempt_id,
                task: job.status.task.clone(),
                input: job.input.clone(),
            });
            free -= 1;
        }

        assignments
    }

    fn expire(&mut self, now: Instant) {
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
            );
        }
    }

    fn finish(&mut self, attempt_id: Uuid, result: AttemptResult) {
        let lease = self.leases.remove(&attempt_id).unwrap();
        let job = self.jobs.get_mut(&lease.job_id).unwrap();
        job.status.state = match result {
            AttemptResult::Success { output } => JobState::Succeeded { output },
            AttemptResult::Failure { .. } if job.status.attempts < job.status.max_attempts => {
                self.queue.push_back(lease.job_id);
                JobState::Queued
            }
            AttemptResult::Failure { error } => JobState::Failed { error },
        };
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::json;

    use super::*;
    use crate::protocol::Completion;

    fn square_poll(active: HashSet<Uuid>, completed: Vec<Completion>) -> Poll {
        Poll {
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
            .id
    }

    #[test]
    fn missing_poll_response_reoffers_the_same_attempt() {
        let mut scheduler = Scheduler::new(Duration::from_secs(5));
        let job = submit_square(&mut scheduler, 2);
        let next_job = submit_square(&mut scheduler, 2);
        let worker = Uuid::new_v4();
        let now = Instant::now();
        let first = scheduler.poll(worker, square_poll(HashSet::new(), vec![]), now);
        let retry = scheduler.poll(worker, square_poll(HashSet::new(), vec![]), now);
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
            scheduler.poll(
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
        let first = scheduler.poll(original_worker, square_poll(HashSet::new(), vec![]), start);
        let first_id = first[0].attempt_id;

        let heartbeat = start + Duration::from_secs(4);
        assert!(
            scheduler
                .poll(
                    original_worker,
                    square_poll(HashSet::from([first_id]), vec![]),
                    heartbeat
                )
                .is_empty()
        );
        scheduler.expire(start + Duration::from_secs(5));
        assert!(matches!(
            scheduler.jobs[&job].status.state,
            JobState::Running
        ));

        let deadline = heartbeat + Duration::from_secs(5);
        let replacement = scheduler.poll(
            replacement_worker,
            square_poll(HashSet::new(), vec![]),
            deadline,
        );
        let replacement_id = replacement[0].attempt_id;
        assert_ne!(replacement_id, first_id);
        assert_eq!(scheduler.jobs[&job].status.attempts, 2);

        scheduler.poll(
            original_worker,
            square_poll(
                HashSet::new(),
                vec![Completion {
                    attempt_id: first_id,
                    result: AttemptResult::Success { output: json!(9) },
                }],
            ),
            deadline,
        );
        assert!(matches!(
            scheduler.jobs[&job].status.state,
            JobState::Running
        ));

        scheduler.poll(
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
        );
        assert!(
            matches!(&scheduler.jobs[&job].status.state, JobState::Failed { error } if error == "handler failed")
        );
    }
}
