# Proposal: webhookd queue and operations portal

**Status:** draft for review. Nothing here is implemented yet.

**Replaces:** the earlier queue-only proposal (`QUEUE_PROPOSAL.md`).

**What this proposal adds to webhookd:**
- A durable job queue that can be configured per webhook.
- A built-in operations portal, styled with the Neon Grid theme. From it you manage:
  - scripts, their configuration, and shared conditions
  - queues, SLAs, and notifications
  - logs, metrics, and audit history

**Constraint:** webhookd stays a single static Go binary. It gains 4 direct Go dependencies in the MVP, and the portal is optional.

---

## 1. Decisions at a glance

| Area | Decision |
|---|---|
| Queue engine | Own push scheduler, about 1.5k lines of code. Per-hook FIFOs plus a heap of eligible hooks. |
| Queue durability | `memory` by default, which matches today's behaviour. `spool` needs no dependencies. `bbolt` is optional. |
| Existing libraries used | `go.etcd.io/bbolt` (storage), `cenkalti/backoff/v5` (backoff), `golang.org/x/time/rate` (rate limits), `fsnotify` (optional file-watch trigger). |
| Rejected queue engines | NATS JetStream (+15 MB, ~50 MB RAM), River or goqite on SQLite (+7 MB), and Redis-based queues (need an external service). |
| Configuration | Files stay the source of truth: `<script>.hook.json` sidecars, `conditions.d/`, `notify.d/`, and a global defaults file. The portal edits these files. |
| Operational state and history | A separate `admin.db` (bbolt). It holds pauses, snoozes, audit, versions, metric rollups, sessions, and tokens. |
| Portal UI stack | The Neon Grid components from `web/nuxt`, ported to **Vue 3 + Vite**, built as a static SPA and served with `go:embed`. |
| Portal extras | CodeMirror 6 (editor), uPlot (metric charts), and the theme's own Chart.js setup (small report charts). |
| Binary size | Today: 8.2 MB. With queue and portal: about 9.6–10 MB. `-tags noportal` gives about 9.1 MB. |
| Default security posture | The portal is off unless `-admin-addr` is set. It listens on a separate port, bound to localhost. Script editing is read-only by default. |

---

## 2. Web stack selection (`nbetcher/neon_grid_theme/web`)

| Stack | What it is | Widgets for this portal | Weight / go:embed | Verdict |
|---|---|---|---|---|
| **`web/nuxt`** (Nuxt 4 layer) | 51 `Ng*` Vue SFCs. Tokens in `shared/tokens.ts`. 14 CSS layers of about 4.1k lines. Uses Reka UI for accessibility. Chart.js is pre-themed through `useNeonChart`. | **Most complete:** `NgAppShell`, `NgTable`, `NgTabs`, `NgModal`, `NgToastHost`, `NgActionMenu`, `NgInput/Select/Switch/Slider/Textarea`, `NgKpiTile`, `NgStatCard`, `NgChart(Panel)`, `NgTimeline`, `NgEventFeed`, `NgLivePill`, `NgStatusPill`, `NgSevChips`, `NgDetailGrid` | Builds a static bundle with `ssr:false` + `nuxi generate`. The toolchain is heavy: Nuxt, plus Node 23.6 or later. | **Best fit** |
| `web/petite-vue` | A single-page static demo. `index.html` alone is 108 kB, plus CDN dependencies. | Only CSS classes. No components or router. Its accessibility is weak. | Trivially embeddable. | Design reference only |
| `neon-grid-kit/packages/core` + Svelte / Vue / React | The narrower "Verde Watch" hybrid theme. Core is CSS plus a 6 kB controller, with thin wrappers per framework. | Only Theme, Button, NavItem, Panel, Signal, Identity. **No tables, forms, toasts, or charts.** | Lightest option: Svelte 69 kB JS, Vue 84 kB, React 210 kB. Vite output with `base:'./'`. | Lightest, but about 80% of the widgets would have to be hand-built |
| kit `examples/next`, `examples/nuxt` | Examples that assume server-side rendering. | They only wrap the six kit components. | Need a Node server. | Rejected |

**Recommendation: the `web/nuxt` components and CSS, built with plain Vue 3 + Vite.** This ranks first on fit and then as light as that fit allows.

- **Why port.** Only three components touch Nuxt APIs directly: `NgButton`, `NgIcon`, `NgNavButton`. They use `NuxtLink` and `@nuxt/icon`. The other 48 only rely on Nuxt auto-importing `ref` and `computed`. The port is:
  - add explicit imports (`unplugin-auto-import` can do this)
  - replace `NuxtLink` with `RouterLink`
  - replace `@nuxt/icon` with `unplugin-icons` (Phosphor set, tree-shaken)
- **Result.**
  - No Nuxt runtime or Nitro.
  - The same visual system: tokens are still generated from `tokens.ts`.
  - About 30–40% less JS than the Nuxt build.
  - Vue's runtime is already small (about 100 kB raw). Svelte would save about 30 kB, but would throw away the component library.
- **Fallback.** Keep the layer as it is and run `nuxi generate` with `ssr:false`. Choose this if tracking upstream theme changes matters more than bundle size.

### Gaps to fill

| Gap | Choice | Size (gzipped) |
|---|---|---|
| Code editor | CodeMirror 6: `basicSetup`, shell/JS/Python/JSON modes, `@codemirror/merge` for diffs. Themed from the tokens. | ~80 kB (loaded only on the Scripts pages) |
| Time-series charts | uPlot, fed live over SSE. | ~20 kB |
| Bulk-select tables | Add a checkbox column and a `v-model:selection` to `NgTable`. | 0 |
| Very large tables | Server-side keyset pagination (the theme README recommends this). Virtual scrolling with `@tanstack/virtual-core` only for log tails. | ~5 kB |
| Condition / rule builder, log tail, diff view | Custom `Wh*` components built from `Ng*` primitives. | — |
| Fonts | Orbitron and Share Tech Mono, **vendored**. No Google Fonts at runtime, for air-gapped installs. | ~25 kB |

- **Estimated bundle:** about 550–650 kB raw, about 200 kB with gzip or brotli.
- **Serving:** files are precompressed at build time and served from `embed.FS` with `Content-Encoding`.
- **Build:** `make portal` runs `npm ci && vite build` into `pkg/portal/dist`. The built `dist/` is committed so `go build` works without Node.

---

## 3. Architecture

```
                ┌──────────── main listener :8080 (unchanged routes) ────────────┐
 webhook ─────► │ middleware (auth/sig/xff/cors) ─► admission ─► Scheduler ─►    │
                │   429 / 202 / sync-stream         │ per-hook FIFOs +           │
                │                                   │ eligible-hook heap, rate,  │
                │                                   │ pause/snooze/breaker gates │
                │                                   ▼                            │
                │                Runner: Job.Run(ctx, Sink) ─► Broker (seq ring) │
                └──────────────────────┬──────────────────────┬──────────────────┘
                                       │ Store (memory|spool|bolt)
          ┌────────────────────────────┼────────────────────────────┐
          ▼                            ▼                            ▼
   pkg/events bus  ◄── watcher (poll 5s + optional fsnotify) ── configstore (files, ETag, history)
   │ non-blocking, 2k ring, Last-Event-ID
   ├─► notification router ─► channels (http/slack/ntfy/mailto…)
   ├─► metrics aggregator ─► admin.db 1m/1h/1d rollups ─► /metrics (optional)
   ├─► SLA evaluator (1/min) ─► sla.breach / sla.recovered
   └─► SSE hub ─► portal
                ┌──────── admin listener 127.0.0.1:8081 (off by default) ───────┐
                │ session/CSRF/RBAC/audit ─► /_admin/api/*  +  embedded SPA      │
                └────────────────────────────────────────────────────────────────┘
```

### Package layout

| Package | Responsibility | Est. LOC |
|---|---|---|
| `pkg/queue` (+ `store/{memory,spool,bolt}`) | Scheduler, admission, conditions, backoff, broker, recovery | 2.1k |
| `pkg/hookcfg` | Layered config merge: built-in → global → `conditions.d` → sidecar → headers. Validation, snapshots. | 350 |
| `pkg/events` | Event bus, ring buffer, SSE hub | 250 |
| `pkg/configstore` | Atomic file writes, ETags, version history, file watcher | 750 |
| `pkg/admin` | REST handlers, bulk actions, search, import/export | 2.2k |
| `pkg/admin/auth` | Sessions, bcrypt htpasswd, RBAC, CSRF, API tokens, step-up auth | 650 |
| `pkg/metrics` | Counters and histograms, rollups, `/metrics` text endpoint | 450 |
| `pkg/notification` (extended) | Router, throttling, digests, quiet hours, templates, Slack/ntfy formats | 600 |
| `pkg/reliability` | SLAs, rate limits, maintenance windows, circuit breaker | 550 |
| `pkg/portal` | `go:embed` of the SPA, precompressed serving, CSP | 80 |
| **Go total** | | **~8k** (+ ~5k tests) |
| `web/portal` (Vue 3 + Vite) | SPA | ~10–12k TS/Vue |

---

## 4. Queue (summary of the accepted design, with portal hooks)

The full detail from the previous proposal still applies. Key points:

- **Dispatch.**
  - Enqueue signals a `chan struct{}`, and one timer fires for the next due retry.
  - Latency is under 1 ms from memory, or about 10 ms plus fsync with bbolt.
  - Per-hook FIFOs plus a heap of eligible hooks with aging. This avoids head-of-line blocking.
- **Job record.** Stores hook, script, method, body, env, timeout, mode, log file, config snapshot, attempt, state, and pgid. Retries always use the config snapshot taken at enqueue time.
- **States:** `pending`, `running`, `retry_wait`, `snoozed`, `paused`\*, `succeeded`, `failed`, `dead`, `expired`, `canceled`. \*`paused` is a hook-level gate, not a job state.
- **Crash recovery.**
  - Orphaned process groups are killed.
  - `on_crash` defaults to `fail`, and `conditions.default` defaults to `fail`.
  - Delivery is at-least-once, so scripts receive `hook_attempt` and `hook_max_attempts` in their environment.
- **Compatibility.** With no sidecar and default flags, behaviour is identical to today, except that jobs run in FIFO order. All `X-Hook-*` headers, status mapping, SSE/chunked/buffered modes, `GET /<hook>/<id>`, log files, and notifications are preserved.
- **Additions for the portal:**
  - `snoozed` state and per-job `not_before`
  - hook gates: `paused_until`, `draining`, `breaker`
  - `replay_of` lineage
  - `labels` (free-form, for routing and search)

### Sidecar schema (final)

```jsonc
// scripts/deploy.sh.hook.json — all keys optional
{
  "labels": {"team": "platform", "env": "prod"},
  "queue": {
    "mode": "sync", "allow_client_override": ["mode", "timeout", "max_buffered_lines"],
    "concurrency": 1, "max_pending": 50, "overflow": "reject",
    "expire_after": "10m", "deadline": "1h", "priority": 0,
    "rate": {"limit": 30, "per": "1m", "burst": 5},
    "dedup": {"header": "X-GitHub-Delivery", "window": "10m", "on_duplicate": "return_existing"},
    "sync": {"wait_timeout": "0s", "wait_for": "first_attempt", "on_disconnect": "continue", "backpressure": "block"}
  },
  "execution": {"timeout": "10s", "max_buffered_lines": 100, "max_body": "128KiB", "payload": "argv"},
  "retry": {"max_attempts": 5, "backoff": {"type": "exponential", "initial": "2s", "max": "5m", "multiplier": 2, "jitter": 0.2}},
  "conditions": {"use": ["gh-deploy"], "retry": {"exit_codes": [75]}},   // shared set + inline override
  "sla": {"p95_wait": "30s", "p95_duration": "2m", "success_rate": 0.98, "window": "1h", "max_oldest_pending": "5m"},
  "maintenance": [{"days": ["sun"], "start": "02:00", "end": "04:00", "tz": "America/Chicago", "behavior": "hold"}],
  "breaker": {"consecutive_failures": 5, "cooldown": "5m"},
  "notify": {"on": ["final"]},
  "retention": {"completed": "24h", "dead": "168h"},
  "payload_policy": {"persist": true, "redact_headers": ["Authorization", "Cookie", "X-Hub-Signature-256"]}
}
```

Shared condition sets live in `conditions.d/<name>.json` and use the same schema as the `conditions` block. Inline keys override the referenced set. The portal shows every value together with the layer it came from.

---

## 5. Portal requirements

**MVP** is required for the first portal release. **P2** follows later.

### 5.1 Dashboard and health
- **MVP:**
  - KPI tiles: pending, running, dead, failed in the last hour, SLA status, notifier errors.
  - A live event feed (SSE).
  - A per-hook status grid: running/pending counts, gate state, last result, and SLA traffic light.
  - Health: store, free disk, scheduler lag, watcher, event-bus drops, TLS certificate expiry.
- **P2:** saved views and a wallboard mode.

### 5.2 Scripts
- **MVP:**
  - A tree of the scripts directory. Each entry shows the exec-bit badge, last run, sidecar present/invalid, and labels.
  - A CodeMirror editor with create, edit, rename, and delete, plus a `chmod +x` toggle.
  - Validation on save: shebang present, `sh -n`/`bash -n`, size limit, no symlinks, no path escape.
  - Version history with diffs and rollback. A rollback is saved as a new version.
  - External edits (git, ssh) appear in the history with `source=external`.
- **P2:** shellcheck (used when it is on `PATH`), script templates, and renaming a script together with its sidecar.

### 5.3 Hook configuration and shared conditions
- **MVP:**
  - A form editor and a raw JSON editor, with JSON-schema validation.
  - An effective-config view that shows which layer set each value.
  - Global defaults editor.
  - Shared condition library: create, edit, see where each set is used, and block deletion of a set that is still referenced.
  - Condition tester: paste output, an exit code, and a duration to see whether the job would be classed as success, fail, or retry, and which rule matched.
- **P2:** YAML sidecars and a visual rule builder.

### 5.4 Queues and jobs
- **MVP:**
  - Job list, filterable by hook, state, labels, time, dedup key, ID, and full text in the payload (admin only).
  - Job detail: attempts timeline, config snapshot, environment (redacted by role), and a log with live follow.
  - Per-item and **bulk** actions: delete, cancel, requeue, snooze (duration or until a time), and replay.
    - Bulk actions take a selection or a saved filter.
    - Before running, they preview how many jobs match.
    - They are capped at 10k jobs and are audited.
  - Per-hook controls:
    - **pause/resume**
    - **snooze hook** (pause until a time)
    - **drain** (stop accepting new jobs, finish pending ones)
    - **purge** (requires typing the hook name to confirm)
  - Dead-letter queue (DLQ) browser: replay as-is or with an edited payload and headers. The replay is a new job linked by `replay_of`.
  - Test-fire console: run a hook with a custom payload. A dry-run mode checks admission, dedup, and conditions without executing anything.
- **P2:**
  - Priority edits and moving a job to another hook.
  - Maintenance windows (hold or reject with 503).
  - Circuit breaker with a manual override.

### 5.5 SLAs and reliability
- **MVP:**
  - Per-hook SLA targets: p95 wait, p95 duration, success rate over a window, and age of the oldest pending job.
  - Evaluated every minute. Emits breach and recovery events.
  - An SLA board with burn-down.
  - Rate limit per hook.
- **P2:**
  - SLA compliance report (weekly or monthly).
  - Error-budget view.

### 5.6 Logs
- **MVP:**
  - Job logs with an attempt selector and live tail. ANSI colours are stripped or rendered.
  - Application logs: slog is copied into a 10k-line in-memory buffer, filterable by level and module.
- **P2:** bounded grep across the log directory, and log download.

### 5.7 Notifications (application level)
- **Channels (MVP):**
  - The existing `http(s)://` and `mailto:` notifiers.
  - A `format=` option: `generic`, `slack`, `ntfy`, or `template` (a Go `text/template`).
  - Secrets are written as `${ENV}` placeholders and never returned by the API.
  - A test-send button.
- **Rules (MVP):**
  - Match on event types, minimum severity, hook glob, and labels, then send to one or more channels.
  - Throttle, e.g. at most 1 per 10 minutes per hook and event.
  - Group events into a digest window.
  - Quiet hours with a timezone. `critical` events always get through.
- **Event catalogue:**

| Event | Default severity |
|---|---|
| `queue.full` (global or per hook), `queue.near_full` (≥80%) | warning |
| `job.dead` (retries exhausted), `job.failed`, `job.expired`, `job.canceled` | error / warning |
| `script.error` (non-zero exit, no retry) | error |
| `webhook.not_executable` (a webhook called a script without +x; the caller gets a clear 500) | error |
| `script.not_executable` (found by the file scan) | warning |
| `script.created`, `script.modified`, `script.deleted`, `script.chmod` | info; **`script.modified` is critical and cannot be muted** |
| `config.changed`, `config.invalid` (a sidecar fails validation; the last good snapshot is kept) | info / error |
| `sla.breach`, `sla.recovered` | critical / info |
| `dedup.storm`, `rate.limited`, `circuit.open`\*, `maintenance.start/end`\* | warning / info |
| `disk.low`, `store.error`, `notifier.failed`, `eventbus.dropped` | critical / error |
| `auth.failure`, `auth.lockout`, `admin.action` | warning / info |

\* P2.

- **Legacy compatibility:** `-notification-uri` keeps working. It becomes an implicit channel named `default` with one rule on `job.final`.
- **Notification log (MVP):** records every delivery with its status, with a retry button.

### 5.8 Metrics and reports
- **MVP:**
  - Per-hook and global charts over 1h, 24h, 7d, and 30d: throughput, success rate, p50/p95 wait and duration, retries, rejections, queue depth.
  - Live updates over SSE.
  - CSV export.
  - Optional Prometheus text endpoint (`-admin-metrics`), alongside the existing `/varz`.
- **P2:**
  - Scheduled email reports.
  - SLA compliance PDF, rendered server-side as HTML so users can print to PDF.
  - Top-N reports: slowest hooks, flakiest hooks, busiest callers.

### 5.9 Administration
- **MVP:**
  - Audit log recording actor, IP, action, target, and a before/after diff. Filterable, and kept for 365 days.
  - Three roles: `viewer`, `operator`, and `admin`.
  - API tokens scoped to a role, with an expiry, and revocable. Only a hash is stored.
  - Config export (tar.gz, secrets redacted) and import with a dry-run plan.
  - Global search across jobs, hooks, scripts, and the audit log.
- **P2:**
  - OIDC login, mapping a groups claim to a role.
  - Generating GitOps pull requests.
  - Multiple instances.

---

## 6. Backend integration details

### 6.1 Config ownership
- **Files remain the source of truth.** The portal writes through `configstore`:
  1. Validate.
  2. Write to a temp file, fsync, and rename.
  3. Record the content hash so the watcher ignores the portal's own write.
  4. Save a gzipped copy to history, content-addressed, in `admin.db`.
- **Concurrent edits:**
  - Reads return `ETag` (sha256). Writes require `If-Match`.
  - A mismatch returns `409` with the current content. The SPA then shows a 3-way merge (`@codemirror/merge`).
- **GitOps mode.** `-admin-config=readonly` turns off all file writes. Operational state stays writable: pause, snooze, drain, maintenance overrides, breaker overrides. The portal then shows drift and history, and exports a tree you can commit.

### 6.2 File watching
- **Primary:** a polling reconciler, controlled by `-watch-interval=5s`.
  - It diffs mode, mtime, size, and hash.
  - It has no dependencies and works on NFS and bind mounts.
- **Optional:** fsnotify (`-watch=auto|poll|fsnotify`). It only triggers an early reconcile, debounced by 250 ms, so a missed inotify event can never cause drift.
- **Exec-bit check.** `ResolveScript` now checks the exec bit. A missing bit returns a clear `500 script not executable` and emits `webhook.not_executable`, replacing the opaque exec error returned today.

### 6.3 Event bus
- `Publish` never blocks.
- Each subscriber has a bounded channel and a counter for dropped events.
- A ring of 2k events lets SSE clients resume with `Last-Event-ID`.
- **The audit log is not a bus subscriber.** Handlers write audit records synchronously, then publish.
- **Notifier interface.** A new optional `EventNotifier` interface is added, and the existing `HookResult` notifiers are wrapped in an adapter. The HTTP notifier gains a timeout and body templates.

### 6.4 Storage (`-admin-db`, bbolt, separate from the queue store)

| Bucket | Contents | Retention |
|---|---|---|
| `audit` | ts+seq → record | 365 days |
| `history`, `blobs` | path+seq → hash; gzipped content | 200 versions per file |
| `metrics_1m/1h/1d` | per-hook counters plus a 16-bucket log histogram | 48 hours / 90 days / 2 years (~1–5 MB per 50 hooks) |
| `state` | pauses, snoozes, drains, breaker and maintenance overrides | — |
| `sessions`, `tokens` | server-side sessions; sha256 of each token | — |
| `notify_log`, `notify_throttle` | delivery log; throttle keys | 30 days |

A janitor enforces retention. The file is never shrunk automatically; compaction is documented as `webhookd admin compact`.

### 6.5 API (`/_admin/api/`, JSON; `/_jobs` kept as a thin alias)

- **Session and access**
  - `session` (POST/DELETE)
  - `me`
  - `tokens`
- **Health, events, search**
  - `health`
  - `events` (SSE)
  - `search`
- **Scripts**
  - `scripts`
  - `scripts/{path}` (GET/PUT/DELETE)
  - `…/chmod`
  - `…/validate`
  - `…/history[/{v}]`
  - `…/rollback/{v}`
- **Hooks**
  - `hooks`
  - `hooks/{hook}` (effective config and its layers)
  - `hooks/{hook}/config`
  - `hooks/{hook}/{pause|resume|drain|purge|snooze|test-fire}`
  - `hooks/{hook}/breaker` (P2)
- **Conditions and defaults**
  - `conditions`
  - `conditions/{name}`
  - `conditions/evaluate`
  - `defaults`
- **Jobs**
  - `jobs` (filters, cursor)
  - `jobs/{id}`
  - `jobs/{id}/logs?attempt=&follow=`
  - `jobs/{id}/{requeue|cancel|snooze|replay}`
  - `jobs/bulk` (`{selector, action, dry_run}`)
  - `dlq`
- **SLA and maintenance**
  - `sla`
  - `sla/report`
  - `maintenance` (P2)
- **Metrics**
  - `metrics/series`
  - `metrics/summary`
  - `metrics/export.csv`
- **Notifications**
  - `notify/channels`
  - `notify/rules`
  - `notify/channels/{id}/test`
  - `notify/log`
- **Audit, logs, config**
  - `audit`
  - `logs/app`
  - `config/export`
  - `config/import?dry_run=1`

### 6.6 Security

- **Listener**
  - `-admin-addr` is empty by default, so the portal is off.
  - The recommended setting is `127.0.0.1:8081`, a separate `http.Server`.
  - `same` mounts the portal at `/_admin` on the main listener.
  - The names `_admin` and `_jobs`, and any file matching `*.hook.json`, are reserved in `ResolveScript`.
- **Authentication**
  - `-admin-passwd` must be an htpasswd file with bcrypt entries only.
  - `-admin-roles` maps users to roles.
  - Sessions use a `__Host-` cookie that is HttpOnly, Secure, and SameSite=Strict, with a 12-hour idle timeout.
  - Failed logins are rate-limited per IP and per user.
  - Bearer tokens take a separate path from cookie sessions.
- **Browser protections**
  - A CSRF synchronizer token is required on every mutation, and the `Origin` and `Sec-Fetch-Site` headers are checked.
  - CSP is `default-src 'self'` with no inline script, plus `frame-ancestors 'none'`.
- **RBAC**

  | Role | Permissions |
  |---|---|
  | viewer | Read-only access. Never sees payloads, env, or redacted headers. |
  | operator | Job and queue actions, DLQ replay, test-fire of existing hooks. |
  | admin | Config, scripts, notifications, users, tokens, and import. |

- **Script editing is remote code execution by design.**
  - `-admin-scripts=off|readonly|write` defaults to `readonly`.
  - `write` additionally requires:
    - the admin role
    - step-up re-authentication within the last 5 minutes
    - no symlinks, and no writes outside the scripts directory
    - chmod limited to `u+x`/`g+x`, never setuid
    - a `script.modified` event that cannot be muted

---

## 7. Dependencies and size

| Dependency | Why | Binary Δ |
|---|---|---|
| `go.etcd.io/bbolt` | Queue store and admin.db | +0.9 MB |
| `cenkalti/backoff/v5` | Retry backoff | ~0 |
| `golang.org/x/time/rate` | Per-hook rate limits | ~0 |
| `fsnotify/fsnotify` | Optional early trigger for the file watcher | +0.1 MB |
| Embedded SPA (precompressed) | Portal | +0.4–0.8 MB |
| P2: `coreos/go-oidc/v3` + `x/oauth2` | OIDC login | +1.5 MB |

- **Go:** 2 direct dependencies today, 6 with the MVP.
- **Binary:** 8.2 MB today, about 10 MB with the MVP.
- **Frontend:** Vue 3, vue-router, reka-ui, chart.js, uPlot, CodeMirror 6, and the Phosphor icon subset. Build-time only.

---

## 8. Rollout

| Phase | Queue | Portal |
|---|---|---|
| 1. Foundations (no behaviour change) | `Job.Run(ctx, Sink)`, broker, memory store, FIFO scheduler | `pkg/events`, slog buffer, admin.db skeleton, Vue + Vite port of the theme |
| 2. Per-hook config | `hookcfg` layers, `conditions.d`, limits/429/TTL/dedup/concurrency | `webhook.not_executable` |
| 3. Retries and alerts | Conditions, backoff, dead state | Notification router, channels and rules, throttle, quiet hours, legacy URI |
| 4. Durability and read-only portal | spool and bolt stores, recovery, drain | Admin listener, auth, viewer role. Dashboard, jobs, logs, config view, metrics, health. |
| 5. Operator and admin writes | Snooze, pause, bulk, replay | Audit, tokens, config editing with history and 409 handling, polling watcher, SLA evaluator, test-fire, import/export, script writes (behind a flag) |
| 6. P2 | Priority, `on_dead` hook, optional NATS | Maintenance windows, circuit breaker, OIDC, reports, rule builder, YAML, fsnotify |

---

## 9. Risks

1. **Scope.** The portal is more than three times the size of the queue work, at about 8k lines of Go and 11k lines of frontend code.
   - *Mitigation:* ship the read-only portal first (phase 4), and ship operator actions before admin editing.
2. **Remote code execution surface.** Script editing combined with test-fire amounts to a shell.
   - *Mitigation:* read-only by default, localhost-only access, step-up authentication, and an audit record plus a notification that cannot be muted.
3. **Config drift between two sources.** The portal and git or manual edits can change the same files.
   - *Mitigation:* ETag checks, an authoritative poller, read-only GitOps mode, and a history that records both sources.
4. **Secret leakage.** Payloads, environment variables, logs, and exports can contain secrets.
   - *Mitigation:* role-based redaction, `${ENV}` placeholders, and redacted exports.
5. **Notification storms.** Many events can fire at once and flood channels.
   - *Mitigation:* throttling and digests are in the MVP, and dropped counters are exposed.
6. **At-least-once duplicates.** A job can run twice after a crash.
   - *Mitigation:* `on_crash: fail` by default, killing orphaned processes, and the `hook_attempt` environment variable.
7. **bbolt and SSE limitations.** bbolt allows a single writer per instance and its file never shrinks. Proxies can buffer SSE.
   - *Mitigation:* document compaction, use one multiplexed SSE stream, send `X-Accel-Buffering: no`, and send heartbeats.
8. **Theme drift.** The Vue port can fall behind `web/nuxt` as the theme changes.
   - *Mitigation:* generate tokens from `tokens.ts` in CI, and keep the port diff small (three components).
9. **Upstream divergence from ncarlier/webhookd.**
   - *Mitigation:* keep the portal in isolated packages behind a `noportal` build tag.

A clickable mockup of the portal in the Neon Grid theme accompanies this proposal. It is linked from the review thread.
