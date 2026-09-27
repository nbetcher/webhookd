# Storage schema: bbolt

**Status:** draft for review. This is a companion to [`QUEUE_AND_API_PROPOSAL.md`](QUEUE_AND_API_PROPOSAL.md).

**Scope:** this is the on-disk layout for:
- the `bolt` queue backend
- the data the read-only API needs: metrics rollups and the event log

Portal features must fit into this layout without redesign. They add *new* buckets and *new* record fields through the rules in §7. **None of those buckets or fields exist in v1.** §8 lists where they will go.

**Library:** `go.etcd.io/bbolt` **v1.4.3**.
- v1.5.0 requires Go 1.25, and webhookd's `go.mod` says `go 1.24.0`.
- Move to 1.5.x when the Go directive is raised.

---

## 1. Design principles

1. **Memory is the working set; bolt is the durable copy.**
   - The scheduler, counters, dedup table and timers live in memory.
   - bolt receives write-through updates. It is read only at startup, for API paging, and for cold job detail.
   - The hot path never waits on a read transaction.
2. **Hot and cold data are split.**
   - A job's small, frequently rewritten metadata (~120 B) is stored apart from its large, write-once payload and its per-attempt records.
   - A state change therefore rewrites only one small leaf entry. List scans never touch payload pages.
3. **Keys are fixed-width and big-endian, so byte order equals logical order.**
   - Every range query is a cursor `Seek` plus `Next`/`Prev`.
   - No index needs a full scan.
4. **Indexes store the key only.** The value is empty, so an index lookup never needs to decode a record.
5. **One writer goroutine with group commit.**
   - bbolt allows one writer anyway. A dedicated writer loop merges concurrent mutations into a single transaction and a single fsync.
   - It needs no lock and adds no fixed 10 ms delay, unlike `DB.Batch`.
6. **Log output never goes into bolt.** It stays in the per-job log files.
7. **Everything is versioned:**
   - the whole database: `schema_version` plus ordered migrations
   - every record: a leading version byte plus a field-numbered TLV body
   - adding a bucket, a field or an index never rewrites existing data unless a migration explicitly backfills it

---

## 2. File and options

- One file, `-db-path` (default `<log-dir>/webhookd.db`). Permissions `0600`, directory `0700`.
  - With `-queue-backend=memory`, the file still holds metrics and events, but the `q` bucket is never created.
  - With no `-db-path` and the memory backend, webhookd runs without a database, as it does today. The API then serves metrics and events only since the last start.
- `bolt.Options`:

| Option | Value | Why |
|---|---|---|
| `Timeout` | `1s` | Fail fast if another process holds the lock, instead of hanging. |
| `FreelistType` | `FreelistMapType` | Page allocation is O(1) (hashmap freelist) instead of an O(n) array scan. It stays fast as retention churn fragments the file. |
| `NoFreelistSync` | `true` | Commits don't write the freelist, which removes about one page write per commit. The cost is a freelist rebuild on open, which is fast at webhookd's scale. |
| `InitialMmapSize` | `-db-mmap=256MiB` | Avoids early remaps. A remap must wait for every open read transaction, so a large initial map keeps API reads from stalling writes. This is address space, not RAM. |
| `NoGrowSync` | `false` | Keeps the safe default. |
| `NoSync` | from `-db-fsync` | `always` or `group` → false. `off` → true: survives a process crash but not a power loss. |
| `PageSize` | OS default (4 KiB) | Records are small, and larger pages would waste space in leaves. |
| `Mlock` | `false` | The whole file isn't needed in RAM. |

- **FillPercent** is not persisted in bbolt; it is set on each bucket handle in every write transaction.
  - **`1.0`** for buckets whose keys only ever increase: `q.jobs`, `q.payload`, `q.attempts`, `q.minute`, `ev.log`. Pages stay full, giving about half the pages of the 0.5 default and denser scans.
  - **`0.5`** (default) for indexes with inserts in the middle: `q.ix.*`, `q.timer`, `q.dedup`, `m.*`.

---

## 3. Encoding

### 3.1 Keys

| Type | Encoding |
|---|---|
| Job ID | `uint64` big-endian, 8 B |
| Hook ID | `uint32` big-endian, 4 B, interned (see `sys.hooks`) |
| Time | `int64` Unix **milliseconds** big-endian, 8 B (all times are positive, so byte order is time order) |
| Minute / hour / day bucket | `uint32` big-endian: Unix minutes, hours or days |
| State | `uint8` code (§3.3) |
| Attempt number | `uint16` big-endian |
| Hashed strings | first 16 B of SHA-256 (dedup keys) or the full 32 B (config snapshots) |

### 3.2 Values

- **Layout:** `version:uint8`, then a sequence of fields. Each field is `(field_no:uvarint, wire_type:uint8, payload)`.
  - Wire types: `0` = uvarint, `1` = zigzag varint, `2` = length-prefixed bytes, `3` = fixed 8 bytes.
  - This is protobuf's wire format with no protobuf dependency: about 200 lines of code on `encoding/binary`.
- **Forward compatibility.** The decoder skips unknown field numbers, and a record that is rewritten **preserves** them. An older binary therefore never loses fields added by a newer one.
- **Field numbers** are never reused. A removed field's number is retired in the code comment of its struct.
- **Where it's used:** hot records (job meta, attempts, events, metrics) use this TLV encoding.
- **Config snapshots** are stored as **compact JSON**, because they are cold, written once, deduplicated by hash (§4.1), and useful to read raw.
- **Payloads** at or above 1 KiB are compressed with `compress/flate` at level 1, which gives about a 3–5× reduction on JSON bodies. A flag byte records whether it applied.

### 3.3 Job state codes

| Code | State | | Code | State |
|---|---|---|---|---|
| `0x01` | pending | | `0x10` | succeeded |
| `0x02` | running | | `0x11` | failed |
| `0x03` | retry_wait | | `0x12` | dead |
| | | | `0x13` | expired |
| | | | `0x14` | canceled |

- **Active** states are `0x01`–`0x0F` and **final** states are `0x10`–`0x1F`. "Is it active?" is a single comparison, and one prefix range covers every active state during recovery.
- Codes `0x20` and above are not assigned. A new state is only a new code plus the handling for it; no migration is needed.

---

## 4. Buckets (v1)

Top-level buckets are **domains**. Each has its own nested buckets, so a later feature adds a domain without touching the ones that exist.

```
sys/                      system
  meta                    "schema_version"→u32, "created_ms"→i64, "instance"→16B random, "id_hwm"→u64
  hooks                   name → hook_id(u32)            (intern table; sequence = next hook_id)
  hooks_by_id             hook_id → name
q/                        queue (only with -queue-backend=bolt)
  jobs                    job_id → JobMeta (TLV)                       FillPercent 1.0
  payload                 job_id → Payload (TLV, flate)                FillPercent 1.0
  attempts                job_id|attempt(u16) → Attempt (TLV)          FillPercent 1.0
  cfgsnap                 sha256(32) → config snapshot JSON (refcounted)
  dedup                   hook_id|hash16 → job_id(u64)|expires_ms(i64)
  minute                  unix_minute(u32) → first job_id(u64) of that minute   FillPercent 1.0
  timer                   at_ms(i64)|kind(u8)|job_id(u64) → ∅
  ix/
    state                 state(u8)|job_id → ∅
    hook_state            hook_id|state(u8)|job_id → ∅
    hook                  hook_id|job_id → ∅
m/                        metrics rollups
  1m                      hook_id|unix_minute(u32) → Rollup (TLV)
  1h                      hook_id|unix_hour(u32)   → Rollup (TLV)
  1d                      hook_id|unix_day(u32)    → Rollup (TLV)
ev/                       events
  log                     seq(u64) → Event (TLV)                       FillPercent 1.0
```

`hook_id 0` is reserved for the global aggregate in `m/*`. It is never assigned to a real hook.

### 4.1 Records

**`JobMeta`**
- This is the only record rewritten on state changes. It is about 80–160 B.

| # | Field | Type |
|---|---|---|
| 1 | hook_id | uvarint |
| 2 | state | uvarint |
| 3 | attempt | uvarint |
| 4 | max_attempts | uvarint |
| 5 | enqueued_ms | varint |
| 6 | started_ms | varint |
| 7 | finished_ms | varint |
| 8 | next_run_ms | varint |
| 9 | expires_ms | varint |
| 10 | deadline_ms | varint |
| 11 | cfg_hash | bytes 32 |
| 12 | mode | uvarint |
| 13 | timeout_ms | uvarint |
| 14 | last_exit | varint |
| 15 | last_outcome | uvarint |
| 16 | pgid | varint |
| 17 | pgid_start_ticks | uvarint |
| 18 | log_file | bytes |
| 19 | dedup_hash | bytes 16 |
| 20 | method | bytes |
| 21 | flags | uvarint bitfield: `payload_persisted`, `payload_compressed`, `sync_waiter`, … |

**`Payload`**
- Written once and deleted by retention. It is only read when a job runs, or when the API is asked for it with `-api-expose-payloads`.

| # | Field | Type |
|---|---|---|
| 1 | body | bytes |
| 2 | env | repeated bytes `K=V` |
| 3 | redacted_keys | repeated bytes |

**`Attempt`**
- One per attempt. It is written at start and rewritten once at the end.

| # | Field | Type |
|---|---|---|
| 1 | started_ms | varint |
| 2 | finished_ms | varint |
| 3 | exit | varint |
| 4 | outcome | uvarint |
| 5 | matched_rule | bytes, e.g. `retry.exit_codes[75]` |
| 6 | timed_out | uvarint |
| 7 | log_offset_start | uvarint |
| 8 | log_offset_end | uvarint |
| 9 | wait_ms | uvarint |

`log_offset_*` makes `GET …/logs?attempt=N` a byte-range read of the log file, with no searching.

**`cfgsnap`**
- A job keeps its config snapshot by content hash.
- Thousands of jobs on an unchanged sidecar share **one** stored snapshot instead of one each.
- A 4-byte reference count prefixes the JSON. Retention decrements it, and the snapshot is deleted when it reaches 0.

**`Rollup`**

| # | Field | Type |
|---|---|---|
| 1 | enqueued | uvarint |
| 2 | succeeded | uvarint |
| 3 | failed | uvarint |
| 4 | dead | uvarint |
| 5 | expired | uvarint |
| 6 | rejected | uvarint |
| 7 | retries | uvarint |
| 8 | dedup_hits | uvarint |
| 9 | wait_hist | packed 16 × uvarint |
| 10 | run_hist | packed 16 × uvarint |
| 11 | max_depth | uvarint |

- The histograms use log2 buckets from 1 ms to about 9 h. p50 and p95 are interpolated at query time.
- Rolling up means adding the fields of the finer buckets together, which is exact.

**`Event`**

| # | Field | Type |
|---|---|---|
| 1 | ts_ms | varint |
| 2 | type | uvarint, interned in code |
| 3 | severity | uvarint |
| 4 | hook_id | uvarint |
| 5 | job_id | uvarint |
| 6 | attrs | repeated `k=v` bytes |

### 4.2 Why these indexes

| Query (API or internal) | Access path | Cost |
|---|---|---|
| Recovery at startup: every active job | `ix/state` prefix `0x01..0x0F` | O(active jobs), not O(all jobs) |
| `GET /jobs?state=dead` newest first | `ix/state` seek to `0x12\|MAX`, then `Prev` | O(page) |
| `GET /jobs?hook=X&state=pending` | `ix/hook_state` prefix `X\|0x01` | O(page) |
| `GET /jobs?hook=X` | `ix/hook` prefix `X` | O(page) |
| `GET /jobs` with no filter | `q/jobs` reverse cursor | O(page) |
| `since` / `until` on any of the above | `q/minute` turns time into a job-ID range, then the ID is the last key component of every index | Two small seeks |
| TTL expiry, retry due, retention purge, dedup expiry | `q/timer` from the start while `at ≤ now`; the `kind` byte selects the handler | O(due items) |
| Dedup check at admission | In-memory map; `q/dedup` is only loaded at startup | O(1) |
| `GET /queues` depth and limits | In-memory counters `[hook][state]`, no transaction | O(hooks) |
| `GET /jobs/{id}` | `q/jobs` + `q/attempts` prefix `id` (+ `cfgsnap`) | 2–3 point reads |
| Metrics series for one hook | `m/1h` prefix `hook_id`, seek to `from` | O(points) |
| `/events/recent` | In-memory ring (2k). `ev/log` reverse cursor only beyond the ring. | O(limit) |

**Job IDs increase monotonically with enqueue time.** As a result:
- every index ends with `…|job_id`, so each one is already time-ordered, and one time-range translation covers all of them;
- `q/minute` costs one small write per minute that has at least one enqueue.

**Write cost per state change:**
- 1 × `q/jobs` put
- 2 index deletes and 2 index puts (`ix/state` and `ix/hook_state`)
- at most 1 timer put or delete

`ix/hook` is written once, when the job is created.

---

## 5. In-memory structures

| Structure | Contents | Rebuilt at startup from |
|---|---|---|
| Scheduler | Per-hook FIFO of pending IDs, a heap of eligible hooks (with aging), per-hook running set, rate-limit token buckets | `ix/state` active prefix + `q/jobs` |
| Timer heap | Next run, expiry, deadline and retention times, with one `time.Timer` for the earliest | `q/timer` |
| Counters | `[hook_id][state]` depth, oldest pending age per hook, workers busy | `ix/hook_state` active prefixes (key-only scan) |
| Dedup map | `hook_id\|hash16 → (job_id, expires)` | `q/dedup` |
| Hook intern table | `name ↔ hook_id` | `sys/hooks` |
| Hot job cache | LRU of `JobMeta`, default 10k entries (~2 MB). Always holds active jobs; final ones age out. | lazy |
| Config snapshot cache | `cfg_hash → parsed config`, LRU of 256 | lazy |
| Metrics aggregator | Current-minute `Rollup` per hook | — (flushed every 60 s) |
| Event ring | Last 2k events, the source for SSE `Last-Event-ID` | `ev/log` tail |
| Log broker | Per-running-job sequenced line ring | — (runtime only) |

**Rule: the API reads memory first.**
- `/queues`, `/health`, `/events/recent` and the SSE streams never open a bolt transaction.
- Only paged job lists, cold job detail, series outside the current minute, and old events do.

---

## 6. Write path and transaction rules

1. **Writer loop.**
   - All mutations are sent as `op` values to one goroutine over a channel.
   - The loop drains whatever is waiting, up to 512 ops or 2 ms after the first one, applies them in **one** `Update`, and commits.
   - Each caller that needs durability waits on its op's `done` channel.
2. **`-db-fsync`:**

   | Mode | Behaviour | Guarantee | Throughput |
   |---|---|---|---|
   | `always` | One op per commit | Every acknowledgement is on disk | Lowest |
   | `group` (default) | The group commit above | Every acknowledgement is on disk, with fsync cost shared across the group | Typically 2–4 ms of added latency and thousands of jobs/s |
   | `off` | `NoSync` | Survives a process crash, but not a power loss | Highest |

3. **Admission acknowledgement.**
   - A job is acknowledged (202, or the sync stream starts) only after its create op commits.
   - Running jobs, attempt starts and progress are not waited on. A crash between those writes is handled by recovery via `pgid`.
4. **ID allocation never waits on a transaction.**
   - IDs come from an in-memory counter.
   - The counter reserves blocks of 1,024 by persisting `sys/meta.id_hwm`.
   - After a crash, the counter resumes at `id_hwm`. IDs stay unique and increasing, with a small gap.
5. **Read transactions stay short.**
   - An API handler copies the rows for one page out of a `View`, then closes it before writing the response.
   - SSE and log-follow never hold a transaction.
   - This keeps remaps and page reuse unblocked, because pages freed while a read transaction is open can't be reused until it ends.
6. **Janitors delete in chunks.**
   - The retention, TTL and metrics janitors delete at most 1,000 keys per transaction and yield between chunks.
   - Deleting a job removes all of its records: `jobs`, `payload`, `attempts/*`, the index keys, the timer keys, the `dedup` entry if it is its own, and one `cfgsnap` reference.
7. **Large values.** A payload bigger than a page spans several contiguous pages in bbolt. Because payloads are in a separate bucket, that never affects `q/jobs` scans.

---

## 7. Evolution and extensibility rules

- **Schema version.**
  - `sys/meta.schema_version` starts at `1`.
  - At startup, migrations `N → N+1` run in order, each in its own transaction and each idempotent.
  - Before the first migration, the file is copied to `webhookd.db.bak-v<N>` using `Tx.WriteTo`, which takes a consistent snapshot while the database stays open.
- **Newer files are refused.** A binary refuses to open a file with a *higher* `schema_version` than it knows. The error names both versions.
- **Adding a field:** use a new TLV field number. There is no migration, and old records simply lack the field.
- **Adding a bucket or domain:** created lazily by the first feature that uses it, or by a migration. There is no change to existing buckets.
- **Adding an index:** a migration builds the new bucket from the base records in chunks. After that, the index is maintained by the writer loop.
- **Adding a state:** assign the next free code in the active (`0x01–0x0F`) or final (`0x10–0x1F`) range. Index layout doesn't change.
- **Adding an event type or metric counter:** new code or field number. Old rollups decode missing counters as 0.
- **Naming:** domain buckets use short lowercase names. Nested index buckets live under `ix/` in their domain.

---

## 8. Planned placement of portal data (not created in v1)

These buckets and fields are **not** created, and no code writes them, until the matching phase in [`PORTAL_PROPOSAL.md`](PORTAL_PROPOSAL.md) is built. They are listed here only to show that the design above absorbs them without migration or re-keying.

| Future feature (phase) | Placement | Mechanism |
|---|---|---|
| Per-hook gates: pause, snooze, drain (C) | New domain `ops/gates`: `hook_id → Gate` | New bucket |
| Job snooze (C) | Reuses `next_run_ms` + `q/timer`; adds a new active state code | New state code |
| Replay lineage (C) | New `JobMeta` field `replay_of`, and new index `q/ix/replay` (`parent\|child`) | New field + backfill migration |
| Labels on jobs (C) | New `JobMeta` field; new index `q/ix/label` (`hash(k=v)\|job_id`) | New field + index migration |
| Audit log (C) | New domain `audit/log`: `ts_ms\|seq → Record`, FillPercent 1.0 | New bucket |
| Config and script history (C/D) | New domain `cfg`: `history` (`path_hash\|seq → {hash, source, actor}`) and `blobs` (`sha256 → flate content`) | New buckets |
| Sessions and API tokens (C) | New domain `auth`: `sessions` and `tokens` (`sha256(token)`) | New buckets |
| Alerting (B) | New domain `alert`: `log` (`ts_ms\|seq`) and `throttle` (`rule\|key → until_ms`) | New buckets |
| SLA state (C) | New domain `sla`: `hook_id → state`. Evaluation reads `m/1m`. | New bucket |

---

## 9. Size and performance estimates

**Per job, excluding the body:**

| Component | Size |
|---|---|
| Meta | ~130 B |
| Attempt | ~60 B |
| Four index and minute keys | ~60 B |
| Timer | ~17 B |
| **Total** | **~0.3 KB**, or about 0.4 KB with page overhead at the chosen fill factors |

- **Bodies:** compressed bodies are typically 20–40% of their raw size.
- **Example:** at 100k jobs/day with 24 h retention and 2 KB bodies, the database is about 100 MB in steady state. Freed pages are reused, so the file stops growing once retention reaches steady state.
- **Metrics:** 50 hooks with 1m for 48 h, 1h for 90 d and 1d for 2 y is about 4 MB.
- **Throughput:** write throughput is limited by fsync. Group commit makes one sync serve a whole batch, so a single SSD sustains thousands of enqueues per second. That is far above what webhook traffic needs.
- **Read latency:** API page reads are memory-mapped B+tree seeks, microseconds per page with a warm page cache.
- **Compaction:** run offline with `webhookd db compact` (uses `bolt.Compact`). It is only needed after a large one-off purge.
- **Backup:** `webhookd db backup <file>` (uses `Tx.WriteTo`) runs online.
- **Health:** `/_api/v1/health` exposes `bolt.Stats`: free pages, pending pages, transaction count, and remaps. With the writer loop's commit latency p95, that shows fragmentation and fsync pressure.

---

## 10. New flags

| Flag | Default | Purpose |
|---|---|---|
| `-db-path` | *(empty)* | Database file. Required for `-queue-backend=bolt`. Optional with memory, for persistent metrics and events. |
| `-db-fsync` | `group` | `always` \| `group` \| `off` |
| `-db-mmap` | `256MiB` | Initial mmap size |
| `-db-cache-jobs` | `10000` | Hot job cache size |
| `-db-compress-min` | `1KiB` | Payload compression threshold; `0` turns compression off |

These replace `-queue-path` and `-queue-fsync` from the earlier draft.
