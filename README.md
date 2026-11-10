# TaskForge

TaskForge runs independent Rust tasks across separate worker processes. A client submits a task name and JSON input to one master. Workers advertise their registered task names and capacity. The master assigns jobs only to workers that can run them.

## Run locally

Build the binary with `cargo build`. Run the master in one terminal and start a worker in another. Start additional worker processes with the same command to add capacity.

```sh
./target/debug/taskforge master
```

```sh
./target/debug/taskforge worker --capacity 2
```

In a third terminal, submit the built-in `square` task.

```sh
./target/debug/taskforge submit square 12
```

Copy the `id` from the submission response and use it in `./target/debug/taskforge status <JOB_ID>`. Once the job succeeds, the response includes `"state": "succeeded"` and `"output": 144`.

The master defaults to `127.0.0.1:7878`. To run workers on other machines, bind the master to a reachable address with `master --bind 0.0.0.0:7878` and pass `--master http://MASTER_HOST:7878` to workers and clients. Use a trusted network because the API uses plain HTTP without authentication.

## Run with Docker

Build the master and worker images from the project directory.

```sh
docker build -f Dockerfile.master -t taskforge-master .
docker build -f Dockerfile.worker -t taskforge-worker .
```

Create a shared network and start the containers. The named volume keeps the master's SQLite database when the container is replaced.

```sh
docker network create taskforge
docker volume create taskforge-data
docker run -d --rm --name taskforge-master --network taskforge \
  -p 127.0.0.1:7878:7878 -v taskforge-data:/data taskforge-master
docker run -d --rm --name taskforge-worker-1 --network taskforge taskforge-worker \
  --master http://taskforge-master:7878 --capacity 2
```

Run the worker command again with a different container name to add workers. Submit a sample task with `docker exec taskforge-master taskforge submit square 12` and check its result in [the dashboard](http://127.0.0.1:7878/).

## Run a workflow

With the master running, start a worker and give it a recognizable dashboard name.

```sh
cargo run -- worker --name reports-1 --capacity 2
```

Submit a two-step workflow and use the returned top-level `id` to check both steps.

```sh
cargo run -- submit-workflow count_words format_report '"hello TaskForge"'
cargo run -- workflow-status <WORKFLOW_ID>
```

`count_words` takes a JSON string. `format_report` waits for the word count and returns `"Word count: 2"`. The second step stays **Waiting** while the first runs or retries. If the first step exhausts its attempts, the second shows **Blocked** with the reason. Restart the master with the same database to keep both steps, their results, and the workflow ID.

The HTTP API accepts workflows at `POST /v1/workflows` with a `first` object containing `task` and `input`, and a `second` object containing `task`. Each step accepts an optional `max_attempts`. Retrieve both steps with `GET /v1/workflows/{id}`.

## Register a task

Add TaskForge as a dependency in your Rust worker binary. Register each task with a name and a synchronous function. The function's input type must implement Serde's `DeserializeOwned`, its output type must implement `Serialize`, and its error type must implement `Display`.

```no_run
use std::num::NonZeroUsize;

use anyhow::Result;
use taskforge::{Client, TaskRegistry, Worker};

#[tokio::main]
async fn main() -> Result<()> {
    let mut tasks = TaskRegistry::default();
    tasks.register("word_count", |text: String| {
        Ok::<_, std::convert::Infallible>(text.split_whitespace().count())
    })?;

    let master = Client::new("http://127.0.0.1:7878")?;
    Worker::new(master, tasks, NonZeroUsize::new(4).unwrap()).run().await
}
```

With that worker running, use `taskforge submit word_count '"hello TaskForge"'`. A Rust client can also use `Client::submit` and `Client::status`.

## Retries and storage

Workers poll once per second to report active attempts, send results, and receive new work. The master retries a failed handler or an expired lease until the job reaches its attempt limit. The default is three attempts, including the first. Set a per-job limit with `submit --max-attempts`. The master defaults to a 15-second lease, configurable with `master --lease-seconds`.

Tasks can run more than once if a worker finishes after its lease expires or loses its connection before the master receives the result. Make handlers idempotent when they have side effects. The lease tracks worker contact, so a handler can keep its slot as long as the worker continues to report it as active.

The master stores jobs and results in `taskforge.sqlite3` in the current directory. Use `master --database /path/to/jobs.sqlite3` to select a different file. Restart with the same database to recover job IDs, inputs, attempt counts, and finished results. Each database supports one master at a time.

After a restart, running jobs with attempts left return to the queue. Running jobs that had used their last attempt are marked failed. Results from attempts started before the restart are discarded. Jobs for task names that no connected worker advertises remain queued until a matching worker polls.

## Task dashboard

Start the master and open [the local dashboard](http://127.0.0.1:7878/) in your browser. The Workers panel shows each connected worker's Busy or Idle status, occupied slots, and assigned tasks. Workers disappear from the panel when their heartbeat expires. A worker started without `--name` is identified by its generated ID.

Search by task name, job ID, or workflow ID. Click a workflow label to see both steps in order, or open a task to see its result or error. The page refreshes every two seconds and shows recovered jobs when you restart the master with the same database.
