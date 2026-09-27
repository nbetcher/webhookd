# Proposal: webhookd portal and alerting

**Status:** tabled. This records decisions made so far so the work can resume later.

**Depends on:** [`QUEUE_AND_API_PROPOSAL.md`](QUEUE_AND_API_PROPOSAL.md), which covers the queue and the read-only `/_api/v1`.

**Deployment:**
- The portal is a **separate project** in its own repository.
- It is a static SPA that talks to webhookd only over HTTP.
- webhookd does not embed it and has no build dependency on it.

---

## 1. Phasing

| Phase | Needs from webhookd | Portal gets |
|---|---|---|
| **A. Read-only portal** | `/_api/v1` as specified (nothing new) | Dashboard and health, hooks and effective config, queues, jobs, attempts, logs (live follow), dead-letter queue (DLQ) view, shared conditions (view), metrics, and the live event feed |
| **B. Alerting** | The alerting engine inside webhookd (§4), plus read endpoints for it | View channels, rules and delivery log. Configuration stays file-based. |
| **C. Operations (writes)** | `/_api/v2` write surface, with RBAC, audit and CSRF-safe auth (§5) | Queue actions, condition tester, config editing, alerting editing, SLAs |
| **D. Script management** | Config store with history, and a script-write mode | Script editor, history and rollback, chmod |

Phase A can start as soon as the queue/API proposal ships phase 5. Each later phase needs its own webhookd work, listed here so it doesn't get lost.

---

## 2. Web stack (from `nbetcher/neon_grid_theme/web`)

| Stack | Widgets available | Weight | Verdict |
|---|---|---|---|
| **`web/nuxt`** (Nuxt 4 layer, 51 `Ng*` components, Reka UI, Chart.js already themed, tokens in `shared/tokens.ts`) | Shell, table, tabs, modal, toasts, action menu, form controls, KPI/stat tiles, charts, timeline, event feed, status pills | Heavy toolchain (Node 23.6 or later) | **Best fit** |
| `web/petite-vue` | CSS classes only, no components | Trivial, but needs CDN dependencies | Design reference only |
| `neon-grid-kit/packages/core` + Svelte/Vue/React | Six components. No tables, forms, toasts or charts. | Lightest (Svelte build: 69 kB JS) | About 80% of widgets would be built by hand |
| kit `examples/next`, `examples/nuxt` | Wrappers around the six kit components | Need a server-side rendering server | Rejected |

**Decision: use the `web/nuxt` components on plain Vue 3 + Vite.**
- Only `NgButton`, `NgIcon` and `NgNavButton` touch Nuxt APIs, so the port is small:
  - `NuxtLink` becomes `RouterLink`.
  - `@nuxt/icon` becomes `unplugin-icons`.
  - Imports become explicit.
- The build output is plain static files. Host them anywhere: nginx, Caddy, or an object store.

**Additions the portal needs:**
- CodeMirror 6 as the code viewer and editor (~80 kB gz), lazy-loaded.
- uPlot for live time series (~20 kB gz).
- A bulk-selection column for `NgTable`.
- Fonts bundled locally.
- Estimated bundle: about 200 kB gz.

**Theme-drift risk:** keep the port diff to those three components, and generate tokens from `tokens.ts` in CI.

---

## 3. Requirements

Tags: **A**, **B**, **C** and **D** refer to the phases in §1. **P2** marks later polish.

- **Dashboard and health (A)**
  - KPI tiles: pending, running, dead, failed in the last hour.
  - Per-hook status grid.
  - Live event feed.
  - Health panel: store, disk, scheduler lag, watcher, event-bus drops, TLS expiry.
  - *P2:* wallboard mode and saved views.
- **Hooks and conditions**
  - Hook list with exec-bit badge, sidecar state and labels (A).
  - Effective-config view showing which layer each value came from (A).
  - Shared condition sets with "used by" (A).
  - **Condition tester, deferred here from the API proposal (C).**
    - Endpoint: `POST /_api/v2/conditions/evaluate`.
    - Input: a set name or an inline conditions block, `exit`, `output`, `duration_ms` and `timed_out`.
    - Output: the outcome plus the matched rule, for example `retry.exit_codes[75]`.
    - It has no side effects, but it lives with the operator tools rather than in the read-only API.
  - Form editor and raw JSON editor for sidecars, global defaults and condition sets, with ETag/409 conflict handling (C).
- **Queues and jobs**
  - Job list with filters (A).
  - Job detail: attempts timeline, matched rules, config snapshot, redacted env keys, and log with live follow (A).
  - DLQ view (A).
  - Per-job and bulk actions (C): cancel, delete, requeue, snooze, replay. Replay can use an edited payload.
    - Bulk actions preview the count first, are capped at 10k, and are audited.
  - Per-hook actions (C): pause/resume, snooze hook, drain, purge (typed confirmation).
  - Test-fire console with dry run (C).
  - *P2:* priority edits, maintenance windows, circuit breaker.
- **Scripts**
  - Read-only source view with `-api-expose-scripts` (A).
  - Editor, create/rename/delete, `chmod +x`, validation (shebang, `sh -n`, no symlinks), version history with diff and rollback (D).
- **SLAs (C)**
  - Per-hook targets: p95 wait, p95 duration, success rate over a window, oldest pending.
  - Evaluated every minute. Emits `sla.breach` and `sla.recovered`.
  - *P2:* compliance reports and error budgets.
- **Logs**
  - Job logs with attempt selector and follow (A).
  - Application log ring with level and module filters. Needs a small `/_api/v1/logs/app` addition (A, *P2*).
- **Metrics and reports**
  - Per-hook and global charts for 1h, 24h, 7d and 30d, with live updates and CSV export (A).
  - *P2:* scheduled reports and top-N reports.
- **Alerting**
  - See §4 (B for viewing, C for editing).
- **Administration (C)**
  - Audit log with before/after diffs.
  - Roles: viewer, operator, admin.
  - Scoped, expiring API tokens.
  - Config export/import with dry run.
  - Global search.
  - *P2:* OIDC.

---

## 4. Alerting

### 4.1 Where it runs

Alerting is managed from the portal, but the engine **must run inside webhookd**:
- A static SPA only sees events while someone has it open, so it cannot alert on its own.
- The engine subscribes to webhookd's existing event bus, which the queue/API proposal already specifies.
- It adds about 450 lines of Go in `pkg/alerting`.
- Configuration is file-based (`alerting.d/channels.json` and `alerting.d/rules.json`) and reloaded by the existing watcher.
- The portal reads this configuration in phase B and edits it in phase C.

### 4.2 Channels

- The existing `http(s)://` and `mailto:` notifier transports are reused.
- The message format is set with `format=generic|slack|ntfy|template`, where `template` is a Go `text/template`.
- Secrets are written as `${ENV}` placeholders. They are expanded when the config is loaded and never returned by the API.
- The HTTP transport gets a client timeout.
- A **Test send** action is available (phase C).

### 4.3 Rules

- A rule matches on event types, minimum severity, hook glob and labels, and sends to one or more channels.
- Each rule has a throttle, for example at most 1 per 10 minutes for each hook and event pair.
- Events can be grouped into a digest over a time window.
- Quiet hours can be set with a timezone. `critical` events bypass them.
- The delivery log keeps event, channel, status and error for 30 days, and offers a retry action (phase C).

### 4.4 Compatibility

`-notification-uri` keeps working unchanged. It is the per-job notifier from the queue proposal. Alerting is a separate, additive feature. It can optionally import that URI as a channel named `default`.

### 4.5 Events that can trigger alerts

The core events come from the queue/API proposal's catalogue. Rows marked with † are added by later phases here.

| Event | Default severity |
|---|---|
| `queue.full`, `queue.near_full` (≥80%) | warning |
| `job.dead` (retries exhausted), `job.failed`, `job.expired`, `job.canceled`† | error / warning |
| `script.error` | error |
| `webhook.not_executable` (a webhook called a script that is not `+x`) | error |
| `script.not_executable` (found by the watcher scan) | warning |
| `script.created`, `script.modified`, `script.deleted`, `script.chmod`† | info. `script.modified` is critical and cannot be muted once script writes (D) exist. |
| `config.changed`, `config.invalid` | info / error |
| `sla.breach`†, `sla.recovered`† | critical / info |
| `dedup.storm`, `rate.limited`, `circuit.open`† | warning |
| `disk.low`, `store.error`, `eventbus.dropped`, `alert.delivery_failed`† | critical / error |
| `api.auth_failure`, `admin.action`† | warning / info |

### 4.6 API additions

- **Phase B (read):**
  - `GET /_api/v1/alerting/channels`: URIs are redacted to scheme and host.
  - `GET /_api/v1/alerting/rules`
  - `GET /_api/v1/alerting/log`
- **Phase C (write):**
  - `PUT /_api/v2/alerting/{channels|rules}`
  - `POST /_api/v2/alerting/channels/{id}/test`
  - `POST /_api/v2/alerting/log/{id}/retry`

---

## 5. What `/_api/v2` (writes) will require from webhookd

- **Auth:**
  - Sessions using a `__Host-` cookie, or bearer tokens only.
  - A CSRF synchronizer token for cookie sessions.
  - Roles: viewer, operator and admin.
  - Step-up re-authentication for admin writes.
- **Audit:** every mutation is recorded with actor, IP, target and a before/after diff. Records are kept for 365 days in a new `audit/` bbolt domain (planned placement: `STORAGE_SCHEMA.md` §8).
- **Config store:**
  - Portal edits are written to the files atomically: temp file, fsync, rename.
  - An ETag/If-Match check makes a stale write return 409.
  - Every change goes into content-addressed version history, whether it came from the portal or an external edit.
  - `-api-config=readonly` locks this down for GitOps setups.
- **Script writes:** controlled by `-api-scripts=off|readonly|write`, default `readonly`.
  - Writing requires the admin role and step-up auth.
  - Symlinks are refused, as is any path outside the scripts directory.
  - chmod is limited to `u+x`/`g+x`.
  - Every write raises a `script.modified` alert that cannot be muted.
- **Queue actions:** requeue, cancel, snooze (`not_before`), replay (a new job linked by `replay_of`), and per-hook gates (`paused_until`, `draining`). Hook gates live in a new `ops/gates` bbolt bucket, not in the config files (`STORAGE_SCHEMA.md` §8).
- **SLA evaluator:** runs every minute over the metric rollups and emits events.
- **Estimated size in webhookd:** about 3k lines of Go on top of the queue/API work, plus alerting at about 450.

---

## 6. Risks

1. **Scope.** The full portal is roughly 10–12k lines of TS/Vue, plus about 3.5k lines of Go in webhookd. The phases are ordered so each one ships something useful.
2. **Remote code execution.** Script writes combined with test-fire amount to shell access. The mitigations are: off or read-only by default, admin role plus step-up auth, audit, and an alert that cannot be muted.
3. **Alert storms.** Throttling and digests ship with the alerting engine from the start, and the event-bus drop counters are visible.
4. **Portal and webhookd versions drifting apart.** The portal targets `/_api/v1` through its OpenAPI document. It checks `GET /_api/v1/info` features at startup and hides pages the server doesn't support.
