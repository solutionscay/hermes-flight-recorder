# Canonical event envelope v1

_Status: frozen._

The envelope is the single append-only event contract. Every part of the
collector writes and reads this one shape: the hook, the outbox, the state
adapter, and the reconciler. The validator lives in
[`hermes_flight_recorder/envelope.py`](../../hermes_flight_recorder/envelope.py).

## Field classes

- **plaintext-metadata** — operational data the service can read.
- **encrypted-content** — content the client encrypts on the host.
- **integrity** — fields that detect loss and support dedup.

## Field contract

| Field | Type | Required | Class | Meaning |
|---|---|---|---|---|
| `schema_version` | string | yes | plaintext | The envelope version. v1 is `"1"`. Each consumer reads this before the payload. |
| `event_id` | string (uuid v4) | yes | integrity | A unique id. The collector makes it at capture. It is the dedup key. `causation_id` points to it. |
| `producer_sequence` | int64 | yes | integrity | A strictly increasing integer per `installation_id`, in emit order. A missing integer between two events for one installation means the collector lost a capture. Never reuse it. Never reset it. |
| `occurred_at` | number (epoch s) | yes | plaintext | The source event time. Do not use it to order events, because clocks can skew. Keep it for people and analytics only. |
| `recorded_at` | number (epoch s) | yes | plaintext | The collector wall-clock time it appended the event. Use it with `producer_sequence` to measure capture latency. |
| `installation_id` | string (uuid v4) | yes | plaintext | A stable id for one Hermes data root. The collector generates it at `hermes-flight-recorder init` and stores it in the outbox. One outbox is one installation. It is the scope for `producer_sequence`. Profiles share it. |
| `tenant_id` | string | yes | plaintext | The workspace or tenant. If the install has no tenant, use `"default"`. |
| `profile` | string | yes | plaintext | The Hermes profile name. Normalize NULL or absent to `"default"`. Do not use `"unknown"`. |
| `runtime` | object | yes | plaintext | An inventory stamp at emit time: `kind`, `gateway_id`, `channels`, `engine`, `home_mode`, `hermes_version`, `release_date`, `install_method`, `state_schema_version`. `home_mode` is the Hermes `terminal.home_mode` policy (`auto` \| `real` \| `profile`, default `auto`) that decides where tools run and which git identity they use; it is an enum, never the resolved home path (that is encrypted content). Present on Hermes-runtime poll events (`state.db`, cron); absent on reconciler-derived findings. On gateway lifecycle events (`runtime.gateway_started`), `channels` is a plaintext list of connected Hermes platform names (for example `telegram` or `discord` — never a bot token) and `gateway_id` is a stable per-boot id; Hermes has no gateway-level transport, so the channel list is the transport surface. |
| `session_id` | string | no | plaintext | The `state.db` `sessions.id`. It joins a live hook payload back to the durable row. |
| `session_key` | string | no | plaintext | The deterministic conversation-lane key. It groups session incarnations into one lane. |
| `parent_session_id` | string | no | plaintext | The lineage edge for the execution tree. Root sessions have NULL. |
| `invocation_id` | string | no | plaintext | The `turn_id` for one cycle. Hermes hooks do not expose an authoritative turn id, so the collector pairs hook bookends with a stable synthetic id. Poll events inside the exact same session and hook-derived time window reuse that id and set `payload.invocation_attribution` to `"inferred_from_session_window"`. Events outside a window and child-session events remain unattributed. |
| `correlation_id` | string | yes | plaintext | The id that ties every event of one operation together. It gives one tree per operation. |
| `causation_id` | string | no | plaintext | The `event_id` of the direct cause. It gives causal tree edges. Best-effort. |
| `source` | string | yes | plaintext | The producing store or subsystem, for example `state.db:messages`. |
| `capture_method` | string | yes | plaintext | The exact capture path, for example `hook:agent:start` or `poll:state.db:sessions`. |
| `payload` | object | yes | plaintext | Event-specific plaintext metadata only. It must never hold message text, tool arguments or results, prompts, or file contents. It must hold `event_type`. |
| `content_ciphertext` | string (base64) | no | encrypted | The client-encrypted content blob. Absent when the event has no content. |
| `content_nonce` | string (base64) | no | encrypted | The per-record AEAD nonce for `content_ciphertext`. |
| `content_hash` | string (hex) | no | integrity | The hash of the plaintext content, computed before encryption. |
| `key_version` | string | no | encrypted | The id of the encryption key and algorithm for `content_ciphertext`. |
| `partial` | boolean | yes | integrity | True when the event is reconstructed, not yet terminal, or content was capped. Consumers treat it as provisional or consult event-specific truncation metadata. |

**Content-field invariant:** `content_nonce`, `content_hash`, and
`key_version` are present if and only if `content_ciphertext` is present.

**payload invariant:** `payload.event_type` is present and is one of the
event types below.

**`payload.surface` (on `session.created` / `subagent.child_spawned`):** the
originating surface a session entered Hermes through — plaintext operational
metadata. The `state.db` producer records the verbatim `sessions.source`
(`cli`, `desktop`, `cron`, `subagent`, or a gateway platform name such as
`telegram` or `discord`); the live hook records the gateway `platform` value
and omits it for a local session. The value set is **open-ended** — plugin
platforms extend it — so `surface` is a free-form string and is never
enum-validated. Both producers report the same semantic concept (the ingress
surface) from the best signal available to them. Consumers must treat the
values as open labels rather than assume a closed enum or a one-to-one mapping.

**`model.usage_recorded` projection semantics:** `session_model_usage` is a
cumulative row, but the event stream records **monotonic deltas**. Each changed
snapshot emits one event with `payload.usage_semantics` set to
`"monotonic_delta"`; token, call-count, and cost fields contain the increase
since the preceding snapshot, while matching `cumulative_*` fields retain the
absolute source values. An unchanged re-poll emits nothing. If Hermes resets a
counter, its current absolute value becomes the first delta of the new counter
epoch and the affected names appear in `counter_reset_fields`. Consumers can
therefore sum event values without double-counting cumulative snapshots.

**Invocation content projection:** the live `hook:agent:start` and
`hook:agent:end` records are immediate, partial, metadata-only bookends.
Hermes truncates their message/response context before the hook runs, so the
spooler removes those previews instead of persisting a silent cutoff. A later
`poll:state.db:messages` pass projects each non-empty `role='user'` row as
`invocation.started` and each non-empty `role='assistant'` row as
`invocation.completed`. These durable content carriers reuse the hook-derived
`invocation_id`, set `payload.message_role` and `payload.message_row_id`, and
store the body only in encrypted content fields. User-row attribution permits
a bounded pre-start skew because Hermes persists the row shortly before firing
`agent:start`. Empty assistant rows used for tool-call structure are not
responses; the corresponding `role='tool'` rows remain
`tool.call_completed`.

Every state-message content carrier includes `payload.content_original_bytes`,
`payload.content_captured_bytes`, and `payload.content_truncated`. The configured
byte cap applies uniformly to user, assistant, and tool bodies. Truncation
stops before a partial UTF-8 code point and sets `partial=true`; `content_hash`
is the hash of the exact captured plaintext bytes.

**`runtime.gateway_start_failed`** is emitted by the reconciler (`source`
`reconciler`, `capture_method` `derive:reconciler`, `partial` true) because
Hermes only fires the `gateway:startup` hook on success — a failed start is
invisible to live capture, so the reconciler reads the durable
`gateway_state.json` and `gateway-starts.log` read-only. Plaintext payload:
`reason_class` (`token_conflict` \| `policy_open` \| `config_invalid` \|
`absent` \| `unknown`), and, by case, `gateway_state`, `platform`,
`error_code`, `conflicting_pid`, `last_start_at`. The raw `exit_reason` /
`error_message` is sensitive and lives only in encrypted content. Liveness is
never keyed off `updated_at` — a healthy idle gateway never advances it.

**Phase 2 · Kanban task coordination (`task.*`):** Hermes ships a first-class
Kanban kernel. Its board databases —
`<HERMES_HOME>/kanban/boards/<slug>/kanban.db`, plus a legacy top-level
`kanban.db` — hold `tasks`, `task_runs` (one row per claim attempt),
`task_links` (a dependency DAG), and an append-only `task_events` audit log. The
collector reads these read-only and maps them onto the five reserved `task.*`
types. All coordination fields are plaintext metadata; task and result text are
encrypted content (and, until the encrypted-content phase, simply omitted — that
is Phase 3).

State is **not** read from the `tasks.status` column alone, because Hermes
overloads it: both a recoverable human/dependency block and a terminal
circuit-breaker give-up land in `status='blocked'`. The authoritative signal is
the `task_events.kind` together with the closing `task_runs.outcome`:

| Event | Trigger (event kind / run outcome) | Meaning |
|---|---|---|
| `task.created` | kind `created` (parked in `triage`/`todo`/`ready`/`blocked` at creation) | A card exists and is queued. |
| `task.claimed` | kind `claimed` (`ready`→`running`; a `task_runs` row opens). `claim_extended` renews the same claim — a lease update, not a new claim. | A worker took the task under a TTL lease. |
| `task.completed` | kind `completed`, run outcome `completed`, status→`done` | The one success terminal. |
| `task.blocked` | kinds `blocked`/`dependency_wait`/`scheduled`; run outcomes `reclaimed`/`stale`/`rate_limited` | Recoverable, non-terminal: awaiting human input, a parent dependency, a schedule, or released to the queue after a lease lapse. Not a failure. The inverse `unblocked` transition is not itself a `task.*` event — the task returns to the queue, and its next `claimed`/terminal event carries the progress. |
| `task.failed_terminal` | kind `gave_up`/`block_loop_detected`, run outcome `gave_up` | Hermes's circuit breaker gave up after repeated crash/timeout/spawn failures (`consecutive_failures >= failure_limit`). Terminal. |

`review` and `archived` are lifecycle states with no reserved event and are not
captured as `task.*`; a later revision may add them additively.

The five events above are **task-level**. An individual attempt — a `task_runs`
row — has its own terminal that often does *not* end the task: a `crashed`,
`timed_out`, or `spawn_failed` attempt feeds the circuit breaker and the task
retries; a `reclaimed`, `stale`, or `rate_limited` attempt releases the task
back to the queue. These attempt terminals are captured as a sixth event,
**`task.attempt_ended`**, emitted once per ended run and keyed on `run_id`. Only
the breaker's final `gave_up` fails the *task* (`task.failed_terminal`); every
attempt on the way there is a `task.attempt_ended`. Its plaintext `payload`
adds `run_id`, `run_outcome` (the raw `task_runs.outcome`), and
`attempt_disposition` — `success` (`completed`), `failure` (`crashed` /
`timed_out` / `spawn_failed` / `gave_up`), or `released` (`reclaimed` / `stale` /
`rate_limited` / `blocked` / `scheduled`) — plus the attempt's `holder`,
`claim_expires`, `worker_pid`, and `last_heartbeat_at`. An attempt's full
history is therefore the `task.claimed` that opened it paired by `run_id` with
the `task.attempt_ended` that closed it.

Plaintext `payload` for every `task.*` event: `event_type`, `board` (slug;
`"default"` for the legacy top-level DB), `task_id` (`t_<8hex>`), `status`,
`run_id` (the claiming attempt, when applicable), `holder` (the `claim_lock`
`host:pid` string), `claim_expires` (lease deadline, epoch s), `worker_pid`,
`last_heartbeat_at`, `block_kind`
(`dependency`/`needs_input`/`capability`/`transient`), `consecutive_failures`,
`priority`, `assignee`, `project_id`, `idempotency_key`, `run_outcome` (on
terminal run events), and `attempt_disposition`
(`success`/`failure`/`released`, derived from the run outcome). Encrypted
content only: `title`, `body`, `result`, `summary`, `error`,
`last_failure_error`, run `metadata`, comment bodies, and any `task_events`
payload excerpt.

**Attempt history and fencing.** Each `task_runs` row is one claim episode;
`task_runs.id` is a per-board AUTOINCREMENT integer, strictly increasing and
never reused, and Hermes itself uses it as a compare-and-swap guard
(`expected_run_id`) on every terminal write. It is therefore the recorder's
**fencing token**: capture it as `run_id` at claim time and carry it on every
later event for that attempt, so a resurrected stale worker's late write is
distinguishable by its lower `run_id`. The `tasks.current_run_id` pointer is
valid only *during* an attempt — it resets to NULL when the run closes — so the
token must be snapshotted from the claim event, never read live. Hermes has no
cross-host fencing authority and no monotonic token beyond this per-board run
id; a lease or fencing authority spanning installations is a hosted-service
(`hermes-dbass`) concern, out of scope for the recorder, which captures only the
tokens Hermes writes.

**Lease semantics (for reconciliation).** A claim carries a TTL lease:
`claim_expires = claim_time + TTL` (Hermes default 900 s, override
`HERMES_KANBAN_CLAIM_TTL_SECONDS`). A live worker renews it by heartbeat; a
one-hour heartbeat backstop reclaims even a live-but-stuck worker. When a lease
lapses without a terminal, Hermes resets the task to `ready` and closes the run
`reclaimed`. The reconciler judges this from the authoritative durable
`task_runs` row — a run still open (`outcome` NULL) whose `claim_expires` has
lapsed past a grace, with a `last_heartbeat_at` stale beyond the claim window,
is a worker that died mid-attempt (a live worker renews `claim_expires` by
heartbeat, so a lapsed lease is the death signal). It emits a
`reconcile.terminal_missing` with `subject_type='task_run'`, dedup-keyed on
`board` + `run_id` — the strictly-increasing per-board attempt id, stable across
lease renewals, never the reconcile-run clock.

## Event-type surface

**P0-poc** — captured and observed in the Phase 0 POC:
`runtime.gateway_started`, `runtime.gateway_start_failed`,
`session.created`, `session.ended`,
`invocation.started`, `invocation.completed`, `model.usage_recorded`,
`tool.call_completed`, `subagent.child_spawned`, `subagent.completed`,
`delegation.dispatched`, `cron.ticker_heartbeat`, `cron.run_claimed`,
`cron.run_finished`, `cron.run_missed`, `reconcile.gap_detected`,
`reconcile.terminal_missing`.

**Reserved** — defined in v1, not captured in the POC:
`runtime.gateway_stopped`, `session.finalized`, `session.compressed`,
`step.iterated`, `model.call_requested`, `model.call_succeeded`,
`model.call_failed`, `tool.call_requested`, `tool.approval_requested`,
`tool.approval_responded`, `delegation.delivered`, `delegation.progress`,
`cron.definition_changed`, `command.invoked`, `handoff.state_changed`,
`task.created`, `task.claimed`, `task.completed`, `task.blocked`,
`task.failed_terminal`, `task.attempt_ended`, `knowledge.record_written`,
`knowledge.record_compacted`.

## Ordering model

Keep three separate order concepts:

- **Producer order** — `producer_sequence`, strictly increasing per
  `installation_id`. This detects gaps.
- **Stream order** — one ordered stream per `installation_id`, keyed by
  `producer_sequence`, tie-broken by `event_id`.
- **Ingestion order** — `recorded_at` plus the append-only local rowid.

Do not add one global sequence. It is not needed and it makes a bottleneck.

## Identity rules

- **installation_id.** Generate a UUID at `hermes-flight-recorder init`. Store it in
  the outbox. One outbox is one installation. Multiple Hermes profiles
  under one home share one `installation_id`; `profile` is a separate
  field. For two runtimes that share one `HERMES_HOME`, give each its own
  Flight Recorder data directory, so each `init` makes its own id.
- **Defaulting.** Normalize a NULL or absent `profile` and `tenant_id` to
  `"default"`, never `"unknown"`.

## Supersession

Keep both the partial event and the later authoritative event. Consumers
match them on their stable subject identity (`session_id`, `invocation_id`,
or another event-family key) and prefer the non-partial representation. Do
not compact in the POC.

Invocation hook bookends and durable message rows are complementary rather
than duplicate content records: the hook supplies immediate timing/metadata,
the state row supplies encrypted content, and `invocation_id` joins them. A
truncated state row remains partial and advertises the configured cutoff in
its payload.

## Privacy boundary

- **Plaintext:** all identity fields, timestamps, `source` and
  `capture_method`, `payload` operational fields, model and billing
  metadata, token classes and counts, cost figures, durations, `tool_name`,
  cron schedule metadata, task coordination metadata (`board`, `task_id`,
  `run_id`, `status`, `holder`, `claim_expires`, `worker_pid`,
  `last_heartbeat_at`, `block_kind`, `run_outcome`, `attempt_disposition`),
  `content_hash`, and `key_version`.
- **Encrypted on the host:** user and assistant text, tool arguments and
  results, the system prompt, reasoning, agent and subagent goals and
  summaries, cron output, error messages, task title/body/result and run
  summaries, and sensitive path or identity context (`cwd`, `git_branch`,
  `git_repo_root`, chat names).

## Golden example

The validator round-trips this exact record. See
[`tests/fixtures/golden_event.json`](../../tests/fixtures/golden_event.json).

```json
{
  "schema_version": "1",
  "event_id": "8f2c1e90-3b7a-4c02-9d14-2a6f0b8e51aa",
  "producer_sequence": 148372,
  "occurred_at": 1752861993.417,
  "recorded_at": 1752861994.902,
  "installation_id": "b3f1c2a4-9e77-4d2b-8a1c-2f6e0d9b4a55",
  "tenant_id": "default",
  "profile": "default",
  "runtime": {
    "kind": "desktop",
    "gateway_id": "gw-nas01",
    "engine": "standard",
    "home_mode": "auto",
    "hermes_version": "0.18.2",
    "release_date": "2026.7.7.2",
    "install_method": "git",
    "state_schema_version": 22
  },
  "session_id": "20260718_175551_6397ee",
  "session_key": null,
  "parent_session_id": null,
  "invocation_id": "20260718_175551_6397ee:turn:3",
  "correlation_id": "20260718_175551_6397ee",
  "causation_id": "b1d4e7c2-90aa-4f31-8c55-7e2100af9931",
  "source": "state.db:messages",
  "capture_method": "poll:state.db:messages",
  "payload": {
    "event_type": "tool.call_completed",
    "tool_call_id": "call_a1b2c3",
    "tool_name": "write_file",
    "status": "ok",
    "effect_disposition": "effect",
    "token_count": 214,
    "message_row_id": 5127
  },
  "content_ciphertext": "kR3m9v0pYb1tQarZ4x8n2w==",
  "content_nonce": "9f4c2ade7b16c0a1d3e5f708",
  "content_hash": "sha256:2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae",
  "key_version": "aesgcm256:2026-07",
  "partial": false
}
```
