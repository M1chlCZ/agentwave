# Changelog

All notable changes to this project are documented in this file. The format
follows Keep a Changelog, and the project uses semantic versioning.

## [Unreleased]

### Added

- Bounded job history: `GET /jobs/history` returns the calling actor's
  completed jobs, most recent first, with a `finishedAt` timestamp and the
  same fields as a job response. The `limit` query parameter accepts 1 to 100
  entries and defaults to 20. `Store.History`,
  `NewStoreWithOptions` and `Options.HistoryCapacity` expose the history to
  embedders; the default capacity is 64 entries.
- Server-sent events: `GET /jobs/{id}/events` replays the current state and
  streams transitions until the job finishes or the client disconnects, with
  `Cache-Control: no-store` and keep-alive comments. `?once=1` returns the
  current state and closes. Each subscriber uses a bounded buffered channel;
  a slow subscriber is dropped instead of blocking the worker. `WatchJob`
  and `JobWatcher` expose the subscription to embedders.

## [0.2.0] - 2026-11-02

### Added

- `SIGHUP` reloads the policy file for the store and the Codex runner without
  a restart; a failed reload keeps the previous policy.
- `GET /metrics` serves Prometheus text exposition format: job counters,
  in-flight generations, live entries, response counts by status class, build
  info and uptime.
- Prompt templates: the policy can define `templates` with `{{name}}`
  placeholders, and `POST /jobs` accepts
  `{"template":"...","variables":{...},"route":"..."}`.
- `Store.Stats`, `Store.ReloadPolicy`, `Store.SubmitInput`,
  `Policy.ParseSubmit`, `Policy.ResolveInput` and `Runner.ReloadPolicy`.

## [0.1.0] - 2026-10-05

### Added

- Policy-validated suggestion service on a private Unix socket.
- Sandboxed Codex CLI runner with a fixed tool-free configuration.
- Per-actor job store with bounded entries and generations.
