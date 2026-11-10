# TaskForge contributor guide

## Project layout

- `src/main.rs` defines the CLI and built-in task handlers.
- `src/master.rs` owns the HTTP API, job queue, workflow transitions, attempt leases, and worker presence.
- `src/store.rs` persists jobs and workflows in SQLite. The master restores them and recovers interrupted attempts at startup.
- `src/worker.rs` manages worker capacity, heartbeats, and registered task handlers.
- `src/client.rs` and `src/protocol.rs` define the client and its JSON contract with the master.
- `dashboard/` contains the HTML, CSS, and JavaScript served by the master through `src/dashboard.rs`.
- `Dockerfile.master` and `Dockerfile.worker` build the master and worker images. The master image stores its SQLite database under `/data`.

## Working on the project

- Preserve the CLI and `/v1` API behavior unless the change calls for a contract update.
- Keep worker concurrency bounded by configured capacity. Repeated polls should preserve existing assignments, and results from expired attempts must never overwrite a newer attempt.
- Commit job changes to SQLite before making them visible in the scheduler or acknowledging them to clients and workers.
- Persist a workflow's first-step result and the second step's state and input in one transaction. Keep the second step waiting during first-step retries and mark it blocked with a reason if the first step fails permanently.
- Derive live worker status from recent polls and active leases. Count an acknowledged assignment once, and remove workers from the live list when their heartbeat expires.
- Document changes to commands, defaults, or task behavior in `README.md`.
- Keep the dashboard build-free and render worker names, task names, results, and errors as text.

## Verify changes

- Run `cargo fmt --check`, `cargo test --locked`, and `cargo clippy --locked --all-targets -- -D warnings`.
- For dashboard changes, run `node --check dashboard/dashboard.js` and open the dashboard at `http://127.0.0.1:7878/` with the master running. Check worker status, occupied slots, and task links with a named worker.
- For end-to-end changes, run `cargo run -- master` and `cargo run -- worker` in separate terminals, submit a job with `cargo run -- submit square 12`, then check it with `cargo run -- status <JOB_ID>`. A successful job returns `144`.
- For workflow changes, use the sample in `README.md`. Verify the second step receives the first result, a permanently failed first step blocks its dependent, and restarting between steps preserves the workflow ID.
- For Docker changes, build both images and verify a worker can connect to the master on the shared network. Check that replacing the master with the same volume preserves its results.
