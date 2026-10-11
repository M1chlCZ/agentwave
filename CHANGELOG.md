# Changelog

All notable changes to this project are documented in this file. The format
follows Keep a Changelog, and the project uses semantic versioning.

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
