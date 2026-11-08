"use strict";

const labels = {
  queued: "Queued",
  running: "Running",
  succeeded: "Succeeded",
  failed: "Failed",
};

const byId = (id) => document.getElementById(id);
const search = byId("search");
const filter = byId("filter");
const rows = byId("jobs");
const dialog = byId("details");
const refreshButton = byId("refresh");
const connection = byId("connection");
const syncError = byId("sync-error");
const submittedFormat = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "short",
});
const timeFormat = new Intl.DateTimeFormat(undefined, { timeStyle: "medium" });

let jobs = null;
let snapshot = "";
let refreshTimer;
let refreshing = false;
let selectedId = null;
let resultId = null;
let resultRequest = null;

function icon(name) {
  const ns = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(ns, "svg");
  const use = document.createElementNS(ns, "use");
  svg.setAttribute("aria-hidden", "true");
  use.setAttribute("href", `#i-${name}`);
  svg.append(use);
  return svg;
}

function badge(state) {
  const element = document.createElement("span");
  element.className = `badge ${state}`;
  element.textContent = labels[state];
  return element;
}

function makeRow(job) {
  const row = document.createElement("tr");
  const name = document.createElement("button");
  name.className = "job-button";
  name.type = "button";
  name.dataset.jobId = job.id;
  name.append(job.task, icon("arrow"));
  name.addEventListener("click", () => {
    selectedId = job.id;
    resultId = null;
    dialog.showModal();
    updateDetails();
  });

  const jobId = document.createElement("span");
  jobId.className = "job-id";
  jobId.textContent = job.id;
  row.insertCell().append(name, jobId);
  row.insertCell().append(badge(job.state));
  row.insertCell().textContent = `${job.attempts} / ${job.max_attempts}`;

  const submitted = document.createElement("time");
  const date = new Date(job.submitted_at_ms);
  submitted.dateTime = date.toISOString();
  submitted.textContent = submittedFormat.format(date);
  row.insertCell().append(submitted);
  return row;
}

function renderJobs() {
  if (jobs === null) return;

  const counts = { all: jobs.length, queued: 0, running: 0, succeeded: 0, failed: 0 };
  for (const job of jobs) counts[job.state]++;
  for (const count of document.querySelectorAll("[data-count]")) {
    count.textContent = counts[count.dataset.count].toLocaleString();
  }

  const query = search.value.trim().toLowerCase();
  const visible = jobs.filter((job) =>
    (filter.value === "all" || filter.value === job.state) &&
    (job.task.toLowerCase().includes(query) || job.id.includes(query))
  );
  const focusedId = document.activeElement?.dataset.jobId;
  const nextRows = document.createDocumentFragment();
  for (const job of visible) nextRows.append(makeRow(job));
  rows.replaceChildren(nextRows);
  if (focusedId) {
    rows.querySelector(`[data-job-id="${focusedId}"]`)?.focus({ preventScroll: true });
  }

  byId("table").hidden = visible.length === 0;
  byId("empty").hidden = visible.length > 0;
  byId("empty-title").textContent = jobs.length ? "No matching tasks" : "No tasks yet";
  byId("empty-copy").textContent = jobs.length
    ? "Try a different search or status."
    : "Submitted tasks will appear here automatically.";
  byId("empty-example").hidden = jobs.length !== 0;
  byId("visible-count").textContent =
    `Showing ${visible.length.toLocaleString()} of ${jobs.length.toLocaleString()} tasks`;
}

function updateDetails() {
  if (!dialog.open) return;
  const job = jobs.find(({ id }) => id === selectedId);
  const result = byId("detail-result");
  if (!job) {
    byId("detail-title").textContent = "Task unavailable";
    byId("detail-status").textContent = "—";
    byId("detail-attempts").textContent = "—";
    byId("detail-submitted").textContent = "—";
    byId("detail-result-title").textContent = "Result";
    result.textContent = "This task is no longer on the master.";
    return;
  }

  byId("detail-title").textContent = job.task;
  byId("detail-id").textContent = job.id;
  byId("detail-status").replaceChildren(badge(job.state));
  byId("detail-attempts").textContent = `${job.attempts} / ${job.max_attempts}`;
  byId("detail-submitted").textContent = submittedFormat.format(job.submitted_at_ms);
  byId("detail-result-title").textContent = job.state === "failed" ? "Error" : "Result";

  if (job.state === "queued" || job.state === "running") {
    result.textContent = "The result will appear when the task finishes.";
    return;
  }
  if (resultId === job.id || resultRequest === job.id) return;

  resultRequest = job.id;
  result.textContent = "Loading…";
  loadResult(job.id);
}

async function request(path) {
  const response = await fetch(path, { cache: "no-store", signal: AbortSignal.timeout(5000) });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response;
}

async function loadResult(id) {
  try {
    const response = await request(`/v1/jobs/${id}/result`);
    const result = await response.text();
    if (dialog.open && selectedId === id) {
      byId("detail-result").textContent = result;
      resultId = id;
    }
  } catch {
    if (dialog.open && selectedId === id) {
      byId("detail-result").textContent = "Could not load the result. Retrying shortly.";
    }
  } finally {
    if (resultRequest === id) resultRequest = null;
  }
}

function showConnection(mode, text) {
  if (connection.dataset.mode === mode) return;
  connection.dataset.mode = mode;
  connection.querySelector("b").textContent = text;
}

async function refresh(manual = false) {
  if (refreshing) return;
  refreshing = true;
  clearTimeout(refreshTimer);
  if (manual) refreshButton.setAttribute("aria-busy", "true");

  try {
    const response = await request("/v1/jobs");
    const nextSnapshot = await response.text();
    if (nextSnapshot !== snapshot) {
      jobs = JSON.parse(nextSnapshot);
      snapshot = nextSnapshot;
      renderJobs();
    }
    updateDetails();
    showConnection("live", "Live");
    syncError.hidden = true;
    byId("last-update").textContent = `Last updated ${timeFormat.format(new Date())}`;
  } catch {
    showConnection("offline", "Disconnected");
    if (jobs === null) {
      byId("empty-title").textContent = "Unable to load tasks";
      byId("empty-copy").textContent = "The dashboard will retry automatically.";
    } else {
      syncError.textContent = "Connection lost. Showing the last successful update.";
      syncError.hidden = false;
    }
  } finally {
    refreshing = false;
    refreshButton.removeAttribute("aria-busy");
    refreshTimer = setTimeout(refresh, 2000);
  }
}

search.addEventListener("input", renderJobs);
filter.addEventListener("change", renderJobs);
refreshButton.addEventListener("click", () => refresh(true));
dialog.addEventListener("close", () => {
  const opener = rows.querySelector(`[data-job-id="${selectedId}"]`);
  (opener ?? search).focus({ preventScroll: true });
  selectedId = null;
});
refresh();
