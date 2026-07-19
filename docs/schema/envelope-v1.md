# Canonical event envelope v1

_Status: frozen. Written in ASD-STE100 Simplified Technical English._

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
| `runtime` | object | yes | plaintext | An inventory stamp at emit time: `kind`, `gateway_id`, `channels`, `engine`, `home_mode`, `hermes_version`, `release_date`, `install_method`, `state_schema_version`. `home_mode` is the Hermes `terminal.home_mode` policy (`auto` \| `real` \| `profile`, default `auto`) that decides where tools run and which git identity they use; it is an enum, never the resolved home path (that is encrypted content). Present on Hermes-runtime poll events (`state.db`, cron); absent on reconciler-derived findings. On gateway lifecycle events (`runtime.gateway_started`), `channels` is a plaintext list of connected Hermes platform names (e.g. `telegram`, `discord` — never a bot token) and `gateway_id` is a stable per-boot id; Hermes has no gateway-level transport, so the channel list is the transport surface. |
| `session_id` | string | no | plaintext | The `state.db` `sessions.id`. It joins a live hook payload back to the durable row. |
| `session_key` | string | no | plaintext | The deterministic conversation-lane key. It groups session incarnations into one lane. |
| `parent_session_id` | string | no | plaintext | The lineage edge for the execution tree. Root sessions have NULL. |
| `invocation_id` | string | no | plaintext | The `turn_id` for one cycle. It is in memory only in Hermes. It is authoritative on hook events. On poll events the collector synthesizes it and sets `partial`. |
| `correlation_id` | string | yes | plaintext | The id that ties every event of one operation together. It gives one tree per operation. |
| `causation_id` | string | no | plaintext | The `event_id` of the direct cause. It gives causal tree edges. Best-effort. |
| `source` | string | yes | plaintext | The producing store or subsystem, for example `state.db:messages`. |
| `capture_method` | string | yes | plaintext | The exact capture path, for example `hook:agent:start` or `poll:state.db:sessions`. |
| `payload` | object | yes | plaintext | Event-specific plaintext metadata only. It must never hold message text, tool arguments or results, prompts, or file contents. It must hold `event_type`. |
| `content_ciphertext` | string (base64) | no | encrypted | The client-encrypted content blob. Absent when the event has no content. |
| `content_nonce` | string (base64) | no | encrypted | The per-record AEAD nonce for `content_ciphertext`. |
| `content_hash` | string (hex) | no | integrity | The hash of the plaintext content, computed before encryption. |
| `key_version` | string | no | encrypted | The id of the encryption key and algorithm for `content_ciphertext`. |
| `partial` | boolean | yes | integrity | True when the event is reconstructed or not yet terminal. Consumers treat it as provisional and expect a later, better event. |

**Content-field invariant:** `content_nonce`, `content_hash`, and
`key_version` are present if and only if `content_ciphertext` is present.

**payload invariant:** `payload.event_type` is present and is one of the
event types below.

**`payload.surface` (on `session.created` / `subagent.child_spawned`):** the
originating surface a session entered Hermes through — plaintext operational
metadata. The `state.db` producer records the verbatim `sessions.source`
(`cli`, `desktop`, `cron`, `subagent`, or a gateway platform name such as
`telegram` / `discord`); the live hook records the gateway `platform` value
and omits it for a local session. The value set is **open-ended** — plugin
platforms extend it — so `surface` is a free-form string and is never
enum-validated. The two producers use related-but-different vocabularies and
are not reconciled.

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
`task.failed_terminal`, `knowledge.record_written`,
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
  Bridge data directory, so each `init` makes its own id.
- **Defaulting.** Normalize a NULL or absent `profile` and `tenant_id` to
  `"default"`, never `"unknown"`.

## Supersession

Keep both the partial event and the later authoritative event. The
authoritative event carries a `supersedes` pointer, matched on
`correlation_id`, subject, and `content_hash`. Consumers pick the
non-partial event. Do not compact in the POC.

## Privacy boundary

- **Plaintext:** all identity fields, timestamps, `source` and
  `capture_method`, `payload` operational fields, model and billing
  metadata, token classes and counts, cost figures, durations, `tool_name`,
  cron schedule metadata, `content_hash`, and `key_version`.
- **Encrypted on the host:** user and assistant text, tool arguments and
  results, the system prompt, reasoning, agent and subagent goals and
  summaries, cron output, error messages, and sensitive path or identity
  context (`cwd`, `git_branch`, `git_repo_root`, chat names).

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
