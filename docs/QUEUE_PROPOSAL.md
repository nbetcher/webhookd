# Proposal: per-webhook queuing for webhookd

Status: draft for review, not yet implemented.

## 1. What exists today

- **Queue.** `WorkQueue` is a buffered channel of size 100 (`pkg/worker/dispatcher.go:11`). The dispatcher starts one goroutine per job, and each goroutine waits for a free worker. The pending backlog is really an unbounded set of goroutines, each holding an open HTTP request.
  - Nothing limits intake, so there is no 429.
  - Ordering is not FIFO.
  - There are no retries, TTL or persistence.
  - Nothing is drained on shutdown.
- **Response.** Every call is synchronous. The handler blocks on an unbuffered `MessageChan`, so a slow client slows the script down.
- **Script input.** The request body is passed as argv[1]. Parameters and headers are passed as `SNAKE_CASE` environment variables.
- **Timeouts.** The timeout kills the whole process group with SIGKILL.
- **Logs.**
  - Each run writes one log file, which is buffered and flushed only when the run ends.
  - Logs are read back with `GET /<hook>/<id>`.
  - IDs come from an in-memory counter, so they start over after a restart.
- **Per-hook configuration.** There is none. The only per-request settings are the `X-Hook-Mode`, `X-Hook-Timeout` and `X-Hook-MaxBufferedLines` headers.
- **Dependencies.** There are 2 direct dependencies, and the stripped binary is about 8.2 MB.

## 2. Library assessment

| Option | Extra service | Binary Δ | Native fit | Verdict |
|---|---|---|---|---|
| asynq, gocraft/work, taskq, machinery | Redis/AMQP | moderate | good | ✗ breaks the single-binary model |
| Embedded NATS JetStream | none (in-proc) | **+15 MB, ~14 modules, ~50 MB RAM** | best: per-subject caps, DiscardNew, MaxAge, dedup window, MaxDeliver + BackOff[], MaxAckPending | ✗ as default. Possible later as `-tags queue_nats` for HA |
| River (+ SQLite driver) | none / Postgres | heavy; SQLite driver still early | rich | ✗ too heavy |
| goqite + modernc SQLite | none | +7 MB | partial (lease, max-receive, delay, priority) | ✗ ~8× the weight of the bbolt option for about the same amount of our own code |
| dque / goque / go-diskqueue | none | light | FIFO only, cannot update jobs by ID | ✗ |
| **bbolt** (`go.etcd.io/bbolt`) | none | **~+0.9 MB** | storage only | ✓ durable store |
| **cenkalti/backoff/v5** | none | tiny, zero deps | exponential backoff, jitter, max-elapsed | ✓ backoff |

**Finding: no existing library meets all the requirements.** Whichever backend is chosen, we would still have to build the hardest parts ourselves:
- streaming output to a caller that is waiting
- success/failure conditions
- 202/429 semantics
- a per-hook config source
- keeping every existing feature working

The practical approach is:
- use libraries where they save real work: bbolt for durability and cenkalti/backoff for backoff;
- write a small push-based scheduler of our own, about 1–1.5k lines of code.

## 3. Recommended architecture (Proposal A, amended)

```
HTTP ─► middleware (auth/sig/…, unchanged) ─► admission ─► Scheduler ─► Runner(Job.Run ctx,Sink)
                                               │  429/202     │ heap of eligible hooks        │
                                               ▼              ▼ per-hook FIFOs               ▼
                                             Store  ◄──── write-through ─────────────► Broker (per-job
                                      memory (default)                                  sequenced ring)
                                      | spool dir | bbolt                                    │
                                                                         SSE/chunked/buffered/follow
```

- **Package layout:**
  - `pkg/hookcfg`: loads the sidecar file, merges defaults, validates, and caches by mtime.
  - `pkg/queue`: scheduler, admission, conditions and broker.
  - `pkg/queue/store/{memory,spool,bolt}`.
  - `pkg/api/jobs.go`.
- **Dispatch.** Dispatch is push-based with no polling. An enqueue signals a `chan struct{}`, and a single `time.Timer` fires for the next retry that is due. Expected latency:
  - memory: under 1 ms
  - bbolt with `Batch`: about 10 ms plus fsync
- **Scheduling.** Each hook has its own FIFO, and a heap holds the hooks that are ready to run, with aging. A job whose hook is at its concurrency cap therefore cannot block other hooks.
- **Store interface.** The interface is `Put / Update / Get / Scan / Delete / NextID`.
  - `memory` is the default and is compatible with today's behavior.
  - `spool` stores one JSON file per job using fsync plus rename, which needs no extra dependency.
  - `bolt` is optional.
  - NATS could be added later behind a build tag.
- **Runner.** `Job.Run(ctx, Sink)` replaces the unbuffered `MessageChan`.
  - The request context is passed through.
  - The Sink never blocks when no subscriber is attached, which fixes a latent hang at `worker.go:48`.
- **Broker.** The broker keeps a sequenced ring buffer of output lines per job. Only the original caller may apply blocking backpressure; followers get a lossy or buffered stream. Log writes are flushed per line, or tracked by offset, so `?follow=1` can replay without gaps.

### Per-webhook config (sidecar `deploy.sh.hook.json`, YAML optional later)

Global defaults use the same schema and are set with `-queue-defaults=<file>`. Settings are merged in this order:
1. built-in defaults
2. the global file
3. the sidecar
4. `X-Hook-*` headers, where the sidecar allows them

Retries always use the config snapshot taken at enqueue time.

```yaml
queue:
  mode: sync                # sync | async | auto (Prefer: respond-async)
  allow_client_override: [mode, timeout, max_buffered_lines]   # default = all (compat)
  concurrency: 1            # per-hook parallelism (global cap stays -hook-workers)
  max_pending: 50           # per-hook queue cap; global: -queue-max-size
  overflow: reject          # reject (429+Retry-After) | block (bounded by sync.wait_timeout) | drop_oldest*
  expire_after: 10m         # TTL while never started
  deadline: 1h              # wall-clock budget across all attempts (governs retry_wait)
  priority: 0               # *phase 2
  dedup: { header: X-GitHub-Delivery, window: 10m, on_duplicate: return_existing }
  sync:
    wait_timeout: 0         # 0 = wait forever (today); else fall back to 202 or 503
    wait_for: first_attempt # first_attempt | final*
    on_disconnect: continue # continue | cancel
    backpressure: block     # block (today) | buffer
execution: { timeout: 10s, max_buffered_lines: 100, max_body: 128KiB, payload: argv }  # argv | stdin | file
retry:
  max_attempts: 1           # 1 = today
  backoff: { type: exponential, initial: 2s, max: 5m, multiplier: 2, jitter: 0.2 }
conditions:                 # order: timeout → fail → success → retry → default
  success: { exit_codes: [0], output_match: '^DEPLOYED', output_not_match: '(?i)error' }
  fail:    { exit_codes: [2, 64, 65] }
  retry:   { exit_codes: [75], on_timeout: true, on_crash: false, output_match: 'temporarily unavailable' }
  default: fail             # safe default: never re-run on an ambiguous result
notify:    { on: [final] }  # attempt | retry | success | failure | dead | expired | canceled
on_dead:   { hook: alerts/dead-letter }   # *phase 2, chain depth capped at 1
retention: { completed: 24h, dead: 168h }
payload:   { persist: true, redact_headers: [Authorization, Cookie, X-Hub-Signature-256] }
```

Items marked `*` are deferred to phase 2 to keep the first version small.

Regex matching:
- Patterns use Go's RE2 engine, so matching time stays linear.
- Matching happens line by line as output streams, so there is no extra buffering.

### Preserving existing behavior

- **Nothing changes by default.** When a hook has no sidecar and default flags are used:
  - the memory store and sync mode are used, with 1 attempt
  - there is no TTL and there are no queue caps
  - headers, status mapping, log files, notifications and routes behave as today

  The only visible difference is that jobs are processed in FIFO order.
- **Unchanged features.**
  - Authentication and signature checks still run before a job is enqueued.
  - The chunked, SSE (including the `error:` convention) and buffered modes, and the truncation marker, work as today.
  - The `X-Hook-ID` header and the `GET /<hook>/<id>` route work as today.
  - The per-attempt timeout still kills the process group.
  - Scripts receive the same environment variables, plus two new ones: `hook_attempt` and `hook_max_attempts`.
- **Retries with a sync caller.** The first attempt streams live to the caller. The response also carries `X-Hook-Retry-At` (buffered mode) or `event: retry` (SSE). Later attempts run asynchronously and can be followed with:
  - `GET /<hook>/<id>?follow=1`, which gives a replay followed by a live tail
  - `GET /_jobs/<id>`

  Once streaming has started the status code cannot change, so the final state is reported in `X-Hook-Status` and in `/_jobs`.
- **Buffered responses when a condition fails.** A success condition can fail even when the exit code is 0. In that case the response returns 500 plus `X-Hook-Status: failed_condition`.
- **Async mode.** The response is `202` with `Location: /_jobs/<id>` and a body of `{id, status_url, log_url}`.
- **Logs.**
  - Attempts are appended to the job's single log file (`O_APPEND`), with the path stored in the job record.
  - `?attempt=N` returns one attempt's section.
  - IDs are persistent and monotonic, using a bolt sequence or a spool counter, or for the memory store a counter seeded from the log directory.
- **Notifications.**
  - `HookResult` gets an optional extension with `Attempt()` and `State()`.
  - Existing notifiers keep working unchanged.
  - The HTTP notifier gets a client timeout.
- **Metrics.** These are added to `/varz` under `hookstats.queue.*`:
  - pending (total and per hook) and running
  - enqueued, retries, succeeded, failed, dead, expired, canceled
  - rejected_full, dedup_hits
  - wait_ms

### Robustness and security

- **Job record.** Each job record stores:
  - hook, script, method, body, env
  - timeout, mode, log file, config snapshot
  - attempt, state, and the pgid of its running process
- **Crash recovery.** At startup:
  - Pending and retry-waiting jobs are put back on the heap.
  - Jobs past their TTL are marked `expired`.
  - Orphaned process groups are killed; the start time is checked to guard against PID reuse.
  - Jobs that were running follow `on_crash`, which defaults to `fail`.

  Delivery is at-least-once, so the documentation must tell users to write idempotent scripts.
- **Graceful shutdown.**
  1. `/healthz` goes down, and new requests get 503.
  2. Running jobs are drained, up to `-queue-drain-timeout`.
  3. If they are still running, they get SIGTERM, then SIGKILL.
  4. Pending jobs stay in the durable store.
- **Secrets.** Redacted headers are kept in memory but never persisted. If `payload.persist` is false, the hook runs memory-only and `on_crash` is forced to fail.
- **Admin API.** `/_jobs`, `/_jobs/{id}` (GET, DELETE) and `/_jobs/{id}/requeue`.
  - It is registered as its own route ahead of `/`, because otherwise the handler for numeric path segments would take it over.
  - `_jobs` and `*.hook.*` files are reserved in `ResolveScript`.
  - It uses a separate `-admin-passwd` file, and never returns env or body unless the caller is admin.
- **New global flags.**
  - `-queue-backend=memory|spool|bolt`
  - `-queue-path`
  - `-queue-max-size`
  - `-queue-defaults`
  - `-queue-drain-timeout`
  - `-queue-fsync=always|batch|off`

## 4. Cost

- **Dependencies:** +2 direct dependencies, `bbolt` and `cenkalti/backoff/v5`. Config files are JSON, handled by the standard library; YAML (`go.yaml.in/yaml/v3`) is added only if you want it.
- **Binary size:** about +0.9 MB, taking it from 8.2 MB to about 9.1 MB.
- **Code:** about 1.5k lines for phase 1 and about 2.1k in total, plus tests. That roughly doubles the size of `pkg/`, which is the main cost of this design.

## 5. Rollout

1. **Neutral refactor.** `Job.Run(ctx, Sink)`, broker, memory store, FIFO scheduler, and append-mode logs. No change in behavior.
2. **Per-hook config.** Sidecar config and intake control: limits, 429, TTL, deadline, dedup and concurrency.
3. **Retries.** Retries and backoff, conditions, notification policy and the `hook_attempt` environment variables.
4. **Durability.** Spool/bbolt store, crash recovery with orphan kill, drain on shutdown, `/_jobs` with admin auth, and metrics.
5. **Phase 2.** Priority with aging, `drop_oldest`, `wait_for: final`, the `on_dead` hook, YAML, and an optional NATS backend.

## 6. Alternatives rejected

- **B, embedded NATS JetStream.** It covers the most features natively, but roughly triples the binary size, adds about 50 MB of baseline RAM, has weak priority support, and splits state between a stream and a key-value store. It is only worth it if you need multiple nodes or high availability.
- **C, goqite on SQLite.** It adds about 7 MB and relies on polling, and we would still write about as much code ourselves as in A.
