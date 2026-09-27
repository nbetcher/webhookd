# Proposal: webhookd queue and read-only API

**Status:** draft for review. Nothing is implemented yet.

**History:** this replaces two earlier drafts, the queue-only proposal and the queue + portal platform proposal.

**Scope:**
- A durable job queue in webhookd, configurable per webhook.
- A versioned, read-only JSON/SSE API. It exposes enough for a separate portal project to show hooks, queues, jobs, logs, metrics and events.

**Out of scope:**
- The portal itself. It is a separate project that consumes this API.
- Every write operation from the API: script editing, config editing and queue management.

**Constraint:** webhookd stays a single static Go binary with a small dependency set.

---

## 1. Scope

| Area | In scope (this proposal) | Tabled (future, separate proposals) |
|---|---|---|
| Queue | Per-hook config, admission and limits, retries and backoff, conditions, TTL and deadline, dedup, concurrency, rate limit, durability, crash recovery, drain on shutdown | Priority, `drop_oldest`, `wait_for: final`, `on_dead` chaining, NATS backend |
| Configuration | Sidecar `<script>.hook.json`, global defaults file, shared `conditions.d/` sets. All file-based and edited by hand or through git. | Any API or UI editing of config or scripts |
| Queue operations | **Viewing** queues, jobs, attempts, logs and the dead-letter queue | Cancel, requeue, snooze, pause, drain, purge and replay through the API |
| Notifications | The existing per-job notifier, unchanged. It fires once per job, when the job reaches a final state. **Alerting** (queue full, retries exhausted, script not executable, …; channels, rules, throttling). See `PORTAL_PROPOSAL.md` §4. |
| Observability | Metrics rollups, event stream, optional Prometheus text endpoint | SLAs, reports, maintenance windows, circuit breaker |
| Security | A separate optional API listener, bcrypt basic auth, static read-only bearer tokens, CORS allowlist for the portal origin | Sessions and cookies, CSRF, RBAC roles, OIDC, audit log (not needed while the API has no writes) |
| Portal | — | A separate repository built on the Neon Grid `web/nuxt` components with Vue 3 + Vite. See §9 and `PORTAL_PROPOSAL.md`. |

---

## 2. Current state

- **Unbounded wait queue.** `WorkQueue` is a buffered channel of 100 jobs (`pkg/worker/dispatcher.go:11`). The dispatcher starts one goroutine per job to wait for a worker, so the real queue is an unbounded pile of goroutines, each holding an HTTP request open.
  - It has no 429, no FIFO order, no retries, no TTL, no persistence and no drain on shutdown.
- **Every call is synchronous.** The handler blocks on an unbuffered `MessageChan`, so a slow client slows the script down.
- **Inputs to the script.**
  - The request body is passed as argv[1].
  - Params and headers become `SNAKE_CASE` environment variables.
  - The timeout sends SIGKILL to the process group.
- **Logs.**
  - Each run writes one log file, buffered and flushed only at the end.
  - Logs are read back with `GET /<hook>/<id>`.
  - Job IDs are in-memory and reset on restart.
- **No per-hook config.** The only per-request knobs are the `X-Hook-Mode`, `X-Hook-Timeout` and `X-Hook-MaxBufferedLines` headers.
- **Size today:** 2 direct dependencies, 8.2 MB stripped binary.

---

## 3. Library decision

No existing Go queue library meets "embedded, lightweight, per-hook limits, TTL, backoff with jitter, conditions".

| Candidate | Why it was rejected |
|---|---|
| asynq, gocraft/work, taskq, machinery | Need Redis or AMQP. |
| Embedded NATS JetStream | Richest native feature set, but adds about 15 MB of binary and about 50 MB of RAM, and has weak priority support. |
| River or goqite on SQLite | About +7 MB, polling-based, and still leaves most features to us. |
| dque / goque / go-diskqueue | FIFO only, and jobs can't be updated by ID. |

No library covers the hard parts anyway:
- streaming output to a waiting caller
- conditions
- 202/429 semantics
- per-hook config

**Decision:** a small push scheduler of our own, using existing libraries where they pay off.

| Dependency | Use | Binary Δ |
|---|---|---|
| `go.etcd.io/bbolt` v1.4.3 | Durable queue store and API data (metrics rollups, event log). Pinned to 1.4.x because 1.5 needs Go 1.25. Schema: [`STORAGE_SCHEMA.md`](STORAGE_SCHEMA.md). | +0.9 MB |
| `cenkalti/backoff/v5` | Backoff with jitter and max elapsed time | ~0 |
| `golang.org/x/time/rate` | Per-hook rate limit | ~0 |

`bolt` is the only durable backend. A zero-dependency spool directory, with one JSON file per job, was considered and dropped. It would duplicate bbolt, which the API needs anyway, and it lacks atomic multi-record updates and indexed lookups by hook, state and time.

- Direct dependencies go from 2 to 5.
- The binary goes from 8.2 MB to about 9.2 MB.
- fsnotify is not needed; a polling watcher is enough (§7).

---

## 4. Queue architecture

```
webhook ─► middleware (auth/sig/xff/cors, unchanged) ─► admission ─► Scheduler ─► Runner: Job.Run(ctx, Sink)
                                   429 / 202 / sync       per-hook FIFOs + eligible-      │
                                                          hook heap, rate gate            ▼
                                        Store: memory (default) | bolt               Broker (per-job seq ring)
                                                                                          │
          pkg/events bus ◄── scheduler, runner, watcher                                   ├─► sync HTTP caller
                                                                                          └─► API log follow (SSE)
             ├─► metrics aggregator ─► 1m/1h/1d rollups
             └─► API SSE hub (/_api/v1/events)
```

**Packages**

| Package | Responsibility | Est. LOC |
|---|---|---|
| `pkg/queue` + `store/{memory,bolt}` | Scheduler, admission, conditions, backoff, broker, recovery, drain | 1.9k |
| `pkg/hookcfg` | Layered config: built-in → global → `conditions.d` → sidecar → allowed headers. Validation, mtime cache, snapshots. | 350 |
| `pkg/events` | Non-blocking event bus, 2k-event ring for SSE resume | 200 |
| `pkg/watch` | Polling reconciler for scripts and config files | 200 |
| `pkg/metrics` | Counters and histograms, rollups, Prometheus text output | 400 |
| `pkg/api/v1` | Read-only API, SSE, API auth, CORS, OpenAPI document | 1.1k |
| **Total** | | **~4.15k Go** (+ ~3.0k of tests) |

**Dispatch**
- Enqueue signals a `chan struct{}`, and one timer handles the next due retry. Nothing polls.
- Latency is under 1 ms with the memory store, and about 2–4 ms with bolt (group commit through a single writer loop; see `STORAGE_SCHEMA.md` §6).
- Each hook has its own FIFO. A heap of eligible hooks, with aging, picks the next one, so a hook at its concurrency cap never blocks others.

**Job record**
- Fields: id, hook, script, method, body, env, timeout, mode, log file, config snapshot and hash, attempts[] (start, end, exit, outcome, matched rule), state, pgid, labels, dedup key.
- **States:** `pending`, `running`, `retry_wait`, `succeeded`, `failed`, `dead`, `expired`, `canceled`. `canceled` comes only from overflow or shutdown in this scope.
- Retries always use the config snapshot taken at enqueue time.

**Sync, async and streaming**
- The first attempt streams live to a sync caller, exactly as today: chunked, SSE with the `error:` convention, or buffered with the ring and exit-code → status mapping.
- If a retry is scheduled, the response includes `X-Hook-Retry-At` (buffered) or `event: retry` (SSE).
- `mode: async` or `Prefer: respond-async` returns `202` with `Location: /_api/v1/jobs/{id}`.
- A job whose success condition failed despite exit 0 returns `500` in buffered mode, with `X-Hook-Status: failed_condition`.
- The Sink never blocks when nobody is listening. This fixes the latent hang at `worker.go:48`.

**Crash and shutdown**
- **Recovery at startup:**
  - Orphaned process groups are killed, checked against their start time.
  - `on_crash` defaults to `fail`, and `conditions.default` defaults to `fail`.
  - Scripts receive `hook_attempt` and `hook_max_attempts` for idempotency.
- **Shutdown:**
  1. `/healthz` reports down, and new requests get 503.
  2. Running jobs are drained up to `-queue-drain-timeout`, then sent SIGTERM, then SIGKILL.
  3. Pending jobs stay in a durable store.

**Logs**
- One file per job, opened with `O_APPEND`, with its path stored in the job record.
- Lines are flushed one at a time, or tracked by offset, so `follow` never drops lines.
- IDs are persistent and monotonic, so `GET /<hook>/<id>` never points at the wrong run after a restart. Where the counter comes from:
  - With `-db-path` set, the counter is reserved in blocks through `sys/meta.id_hwm` (`STORAGE_SCHEMA.md` §6).
  - With no database, it is seeded at startup from the highest ID found in the log directory's file names.

**Compatibility**
- With no sidecar and default flags, behaviour is the same as today, except that jobs now run FIFO.
- The `X-Hook-*` headers, status mapping, `GET /<hook>/<id>`, log file names and the legacy notifier are all preserved.

### Sidecar schema (`scripts/<script>.hook.json`, all keys optional)

```jsonc
{
  "labels": {"team": "platform", "env": "prod"},
  "queue": {
    "mode": "sync", "allow_client_override": ["mode", "timeout", "max_buffered_lines"],
    "concurrency": 1, "max_pending": 50, "overflow": "reject",       // reject (429) | block (bounded)
    "expire_after": "10m", "deadline": "1h",
    "rate": {"limit": 30, "per": "1m", "burst": 5},
    "dedup": {"header": "X-GitHub-Delivery", "window": "10m", "on_duplicate": "return_existing"},
    "sync": {"wait_timeout": "0s", "on_disconnect": "continue", "backpressure": "block"}
  },
  "execution": {"timeout": "10s", "max_buffered_lines": 100, "max_body": "128KiB", "payload": "argv"},  // argv | stdin | file
  "retry": {"max_attempts": 5, "backoff": {"type": "exponential", "initial": "2s", "max": "5m", "multiplier": 2, "jitter": 0.2}},
  "conditions": {
    "use": ["gh-deploy"],                              // shared set(s) from conditions.d/
    "success": {"exit_codes": [0], "output_match": "^DEPLOYED"},
    "fail":    {"exit_codes": [2, 64, 65]},
    "retry":   {"exit_codes": [75], "on_timeout": true, "on_crash": false},
    "default": "fail"                                  // order: timeout → fail → success → retry → default
  },
  "retention": {"completed": "24h", "dead": "168h"},
  "payload_policy": {"persist": true, "redact_headers": ["Authorization", "Cookie", "X-Hub-Signature-256"]}
}
```

- **Global defaults:** `-queue-defaults=<file>` uses the same schema.
- **Shared conditions:** `conditions.d/<name>.json` holds the `conditions` block. Inline keys override the shared set.
- **Reserved names:** `ResolveScript` refuses `*.hook.json` and the reserved `_api` name, so neither can be run as a hook.
- **Invalid sidecar:** the last valid snapshot stays in effect, and a `config.invalid` event is emitted.

### Global flags (`WHD_*` env equivalents)

- `-queue-backend=memory|bolt`
- `-queue-max-size`
- `-queue-defaults`
- `-queue-drain-timeout=30s`
- `-db-path`, `-db-fsync=always|group|off`, `-db-mmap`, `-db-cache-jobs`, `-db-compress-min` (see `STORAGE_SCHEMA.md` §10)
- `-hook-workers` stays as the global concurrency limit.

---

## 5. Events

The event bus exists for the API's event stream (`/_api/v1/events`) and for metrics. It does not send anything anywhere. Outbound alerting on these events is tabled; see `PORTAL_PROPOSAL.md` §4 (Alerting). The event bus is the hook it will subscribe to.

**Event catalogue**

| Event | Severity |
|---|---|
| `queue.full`, `queue.near_full` (≥80%) | warning |
| `job.dead` (retries exhausted), `job.failed`, `job.expired` | error / warning |
| `script.error` (non-success, no retry) | error |
| `webhook.not_executable` (a request hit a script without +x; the caller gets a clear 500) | error |
| `script.not_executable` (found by the watcher scan) | warning |
| `script.created`, `script.modified`, `script.deleted` | info |
| `config.changed`, `config.invalid` | info / error |
| `dedup.storm`, `rate.limited` | warning |
| `disk.low`, `store.error`, `eventbus.dropped` | critical / error |
| `api.auth_failure` | warning |

---

## 6. Read-only API (`/_api/v1`)

### 6.1 Principles

- **Read-only.** Only `GET` requests are accepted. Every other method returns `405`.
- **Versioned.** Everything lives under `/_api/v1`. Breaking changes go to `/v2`. The document at `GET /_api/v1/openapi.json` is written by hand and checked in CI against the handlers' route table.
- **Collections.**
  - Keyset pagination: `?limit=` (default 50, max 500) and `?cursor=`, returning `{items, next_cursor}`.
  - Filters are plain query params.
  - Times are RFC 3339 UTC. Durations are in milliseconds, for example `wait_ms` and `duration_ms`.
- **Errors** use `application/problem+json` (RFC 9457).
- **Sensitive data is never returned**, including when the caller is authenticated:
  - request bodies and env values
  - redacted headers
  - script source
  - channel URIs and secrets

  Job detail lists only the env **keys**. Two opt-in flags widen this:
  - `-api-expose-payloads` returns the body and non-redacted env values.
  - `-api-expose-scripts` returns script source for read-only viewing.
- **Streaming.**
  - SSE endpoints send `id:` per event, honour `Last-Event-ID`, send a 15 s heartbeat comment, and set `X-Accel-Buffering: no`.
  - A portal needs one multiplexed events stream per tab.

### 6.2 Endpoints

**System**

| Endpoint | Returns |
|---|---|
| `GET /info` | Version, uptime, enabled features, queue backend, worker count, API flags |
| `GET /health` | Store status, free disk, scheduler lag, watcher last scan, event-bus drops, TLS certificate expiry |
| `GET /openapi.json` | API description |

**Hooks**

| Endpoint | Returns |
|---|---|
| `GET /hooks` | Every discovered script: name, path, exec bit, size, mtime, sha256, sidecar status (none, valid or invalid), labels, and a queue summary (running, pending, retry_wait, dead) |
| `GET /hooks/{hook}` | The same plus the **effective config**, with the source layer of each value (built-in, global, conditions.d/…, sidecar), and the last validation error |
| `GET /hooks/{hook}/source` | Script text. Only with `-api-expose-scripts`. |

**Queues**

| Endpoint | Returns |
|---|---|
| `GET /queues` | Global totals and limits (`max_size`, workers busy/total), plus per-hook depth by state, `max_pending`, concurrency in use / limit, rate-limit tokens, oldest pending age, p95 wait |
| `GET /queues/{hook}` | Detail for one hook |

**Jobs**

| Endpoint | Returns |
|---|---|
| `GET /jobs` | Filters: `hook`, `state` (multi), `label.k=v`, `since`, `until`, `dedup_key`, `replay_of`. Items: id, hook, state, attempt n/max, enqueued/started/finished times, wait_ms, duration_ms, last exit, outcome. |
| `GET /jobs/{id}` | Full record: attempts[] (times, exit, outcome, **matched rule**, e.g. `retry.exit_codes[75]`), next retry time, expiry and deadline, config snapshot hash, env keys, body size and sha256, log URL |
| `GET /jobs/{id}/config` | The config snapshot the job runs under |
| `GET /jobs/{id}/logs?attempt=N` | `text/plain` log for the whole job or one attempt. Supports `Range`. |
| `GET /jobs/{id}/logs?follow=1` | SSE: replays the log from the broker's sequenced ring, then tails it live until the job reaches a final state |
| `GET /dlq` | Shortcut for `/jobs?state=dead` |

**Conditions**

| Endpoint | Returns |
|---|---|
| `GET /conditions` | Shared sets, with a `used_by` list |
| `GET /conditions/{name}` | One shared set |

**Metrics**

| Endpoint | Returns |
|---|---|
| `GET /metrics/summary?window=1h` | Totals and per hook: enqueued, succeeded, failed, dead, expired, rejected, retries, dedup hits, p50/p95 wait and duration |
| `GET /metrics/series?hook=&metric=&res=1m\|1h\|1d&from=&to=` | Time series. Retention: 1m for 48 h, 1h for 90 d, 1d for 2 y. |

**Events**

| Endpoint | Returns |
|---|---|
| `GET /events?types=&hook=` | SSE stream of the event bus (the §5 catalogue plus `job.state` transitions). Resumable. |
| `GET /events/recent?limit=` | The last N events from the ring, as JSON, for initial paint |

**Separate from `/_api/v1`**
- `GET /metrics` returns Prometheus text format. Controlled by the `-api-prometheus` flag, served on the API listener.
- `GET /<hook>/<id>` stays, served from the queue's job records.
- `/varz` stays.
- The earlier drafts' `/_jobs` mutation routes are **dropped**.

### 6.3 Listener, auth and CORS

- **Listener (`-api-addr`).**
  - Empty (the default) disables the API.
  - Recommended: a separate `http.Server` on `127.0.0.1:8081`.
  - `same` mounts the API at `/_api` on the main listener, and `_api` is reserved in `ResolveScript`.
- **Auth.** Either method works, and an API with no auth refuses to start unless it is bound to loopback.
  - `-api-passwd`: an htpasswd file, bcrypt entries only.
  - `-api-tokens`: a file of `name:sha256(token)` lines for bearer tokens (`Authorization: Bearer whd_…`). Rotate by editing the file; the watcher reloads it.
  - Failed attempts are rate-limited per IP and emit `api.auth_failure`.
- **CORS (`-api-cors-origins`).** An explicit allowlist, for example the portal's origin. Only `GET`, `Authorization` and `Last-Event-ID` are allowed. Credentials use bearer tokens only, with no cookies, so CSRF does not apply.
- **Hook auth is independent.** The existing hook-side auth, htpasswd and signature verification are unchanged and separate from API auth.

### 6.4 Storage used by the API

The full layout (buckets, keys, indexes, in-memory structures, write path and migrations) is in [`STORAGE_SCHEMA.md`](STORAGE_SCHEMA.md). Summary:

| Domain | Contents | Retention |
|---|---|---|
| `q/` | Jobs, payloads, attempts, config snapshots, dedup, timers, and indexes by state, hook+state, hook, and time | Per-hook `retention` |
| `m/` | Rollups at 1m, 1h and 1d (per-hook counters + 16-bucket log histograms) | 48 h / 90 d / 2 y (~4 MB per 50 hooks) |
| `ev/` | Event log beyond the 2k in-memory ring | 7 d |

- One file, `-db-path`, holds all three domains.
- With the `memory` queue backend, `q/` is not created, and the job API shows only jobs since the last start.
- `/queues`, `/health`, `/events/recent` and the SSE streams are served from memory and never open a transaction.

---

## 7. Integration details

1. **Event bus.**
   - `Publish` never blocks. Each subscriber has a bounded channel and a drop counter.
   - Subscribers: the metrics aggregator, the SSE hub, and the event log.
2. **Watcher.**
   - A polling reconciler (`-watch-interval=5s`) keeps a snapshot of mode, mtime, size and hash for `scripts/`, sidecars, `conditions.d/` and the token file.
   - The diff produces `script.*` and `config.*` events and invalidates the config cache.
   - No fsnotify dependency. Polling works on NFS and bind mounts.
3. **Exec-bit check.**
   - `ResolveScript` checks the exec bit.
   - A missing bit returns `500 script not executable` plus `webhook.not_executable`, instead of the opaque exec error returned today.
4. **Existing notifier.**
   - The `HookResult` notifier is unchanged apart from being called once per job, on its final state, instead of once per run. With `max_attempts: 1` that is identical to today.
   - The notifier's prefix filter reads the final attempt's section of the log.
5. **Metrics.**
   - An in-memory aggregator flushes once a minute in one bolt transaction. Coarser buckets are rolled up from finer ones.
   - The existing expvar counters stay.

---

## 8. Rollout

| Phase | Content | Behaviour change |
|---|---|---|
| 1. Foundations | `Job.Run(ctx, Sink)`, broker, memory store, FIFO scheduler, append-mode logs, persistent IDs, `pkg/events` | None (FIFO only) |
| 2. Per-hook config | `hookcfg` layers, `conditions.d`, limits/429, TTL/deadline, dedup, concurrency, rate, exec-bit check, watcher | Only for hooks with a sidecar |
| 3. Retries | Conditions, backoff, dead state, `hook_attempt` env, final-state notifier call | Only when configured |
| 4. Durability | bolt store, crash recovery with orphan kill, drain on shutdown | Opt-in via `-queue-backend` |
| 5. Read-only API | `/_api/v1` (all of §6), API listener, auth, CORS, SSE, metrics rollups, OpenAPI, Prometheus | Off unless `-api-addr` is set |

Phases 1–4 are useful on their own. Phase 5 is what the separate portal project depends on.

---

## 9. Portal and alerting (tabled)

These are specified in [`PORTAL_PROPOSAL.md`](PORTAL_PROPOSAL.md). It covers:
- the separate portal project and its web stack
- the alerting engine, which runs inside webhookd on this proposal's event bus
- the condition tester (`evaluate`), which moved from this API to `/_api/v2`
- the `/_api/v2` write surface

Nothing in this proposal depends on that one.

---

## 10. Risks

1. **At-least-once duplicates.** A crash can cause the same job to run twice.
   - Mitigation: `on_crash: fail` by default, killing orphaned process groups, and `hook_attempt` so scripts can be idempotent.
2. **Secret exposure through the API.**
   - Mitigation: payloads, env values, script source and channel URIs are hidden by default, the API is off by default, and it is bound to loopback.
3. **Semantics change once retries are enabled.** A streamed response cannot reflect later attempts.
   - Mitigation: document this. Final state is available through `X-Hook-Status`, `/_api/v1/jobs/{id}` and log follow.
4. **Memory backend and history.** With the memory backend, job and metric history is lost on restart.
   - Mitigation: document this. Use `bolt` wherever the API matters.
5. **SSE behind proxies.** Proxies can buffer SSE, and browsers limit connections per origin.
   - Mitigation: one multiplexed stream, heartbeats, and `X-Accel-Buffering: no`.
6. **bbolt constraints.** Only one process can open the file at a time, and the file never shrinks.
   - Mitigation: fail fast if the file is locked (1 s timeout). Freed pages are reused, so the file stays flat once retention reaches steady state. Offline `webhookd db compact` is available after large purges.
7. **Upstream divergence from ncarlier/webhookd.**
   - Mitigation: keep the queue and API in isolated packages. The API can be compiled out with `-tags noapi`.
