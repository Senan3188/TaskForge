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

The master keeps jobs and results in memory until it exits. Jobs for task names that no connected worker advertises remain queued until a matching worker polls.

## Task dashboard

Start the master and open [the local dashboard](http://127.0.0.1:7878/) in your browser. It shows counts by status and the task list, with new submissions at the top. Search by task name or job ID, filter by status, or open a task to see its result or error. The page refreshes automatically every two seconds. The dashboard uses the same master address as the CLI and shows that master's in-memory jobs.
