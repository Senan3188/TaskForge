# TaskForge contributor guide

## Project layout

- `src/main.rs` defines the master, worker, submit, and status CLI commands.
- `src/master.rs` owns the HTTP API, in-memory job queue, and attempt leases.
- `src/worker.rs` manages worker capacity, heartbeats, and registered task handlers.
- `src/client.rs` and `src/protocol.rs` define the client and its JSON contract with the master.
- `dashboard/` contains the HTML, CSS, and JavaScript served by the master through `src/dashboard.rs`.

## Working on the project

- Preserve the CLI and `/v1` API behavior unless the change calls for a contract update.
- Keep worker concurrency bounded by configured capacity. Repeated polls should preserve existing assignments, and results from expired attempts must never overwrite a newer attempt.
- Document changes to commands, defaults, or task behavior in `README.md`.
- Keep the dashboard build-free and render task names, results, and errors as text.

## Verify changes

- Run `cargo fmt --check`, `cargo test --locked`, and `cargo clippy --locked --all-targets -- -D warnings`.
- For dashboard changes, run `node --check dashboard/dashboard.js` and open the dashboard at `http://127.0.0.1:7878/` with the master running.
- For end-to-end changes, run `cargo run -- master` and `cargo run -- worker` in separate terminals, submit a job with `cargo run -- submit square 12`, then check it with `cargo run -- status <JOB_ID>`. A successful job returns `144`.
