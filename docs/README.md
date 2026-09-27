# Enhancement proposals

| Document | Status | Summary |
|---|---|---|
| [QUEUE_AND_API_PROPOSAL.md](QUEUE_AND_API_PROPOSAL.md) | Proposed | Per-webhook configurable queue, events, metrics, and the read-only `/_api/v1` |
| [STORAGE_SCHEMA.md](STORAGE_SCHEMA.md) | Proposed | bbolt layout, indexes, in-memory working set, write path and migrations for the above |
| [PORTAL_PROPOSAL.md](PORTAL_PROPOSAL.md) | Tabled | Separate portal project (Neon Grid, Vue 3 + Vite), Alerting, and the `/_api/v2` write surface |

Read them in that order. The portal proposal depends on the other two. Nothing in the queue/API proposal depends on the portal.
