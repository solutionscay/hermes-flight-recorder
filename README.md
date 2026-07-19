# Hermes Flight Recorder

**The local-first control plane for [Hermes](https://hermes-agent.nousresearch.com) agents.**

Your agents stay fast and local. Their state becomes durable and synchronized. You can see it and control it. The cloud does not go into the agent's critical path.

> **Status: Phase 0 (early).** We build this repository in the open, one step at a time. Nothing here is ready for production. Read the Roadmap section.

---

## What it is

Hermes Flight Recorder captures semantic execution events from a Hermes installation that runs on your host. It encrypts and synchronizes agent state. It coordinates distributed task workers. It gives you a durable execution ledger for all your runtimes. Local reads and writes stay local. The cloud gives durability, coordination, and visibility.

Hermes Flight Recorder is not "remote SQLite." It is not a vector store or a tracing dashboard. It is a control plane. It answers the questions that operators have about autonomous agents:

- What ran, in what order, and with what cost and result?
- Which subagent did the work, and where did the lineage branch or fail?
- Who claimed a task, which attempt was a success, and could a stale worker complete reassigned work?
- What failed silently? For example, a cron that never started, an invocation that never ended, or a tool loop that makes no progress.
- Can I check fleet health, but not send my agents' private prompts and outputs to a third party?

## What works today

This is the vision. The build is at **Phase 0**, and it is local-only. There is no cloud, no console, and no account yet. What runs now:

- `hermes-flight-recorder init` — create the local event log (the outbox), mint a stable installation id, and install the in-gateway capture hook.
- `hermes-flight-recorder run` — drain the live hook's spool, then poll Hermes's `state.db` and cron store read-only, writing each event to the log.
- `hermes-flight-recorder reconcile` — diff the durable stores against the log to find gaps, missing terminals, and missed cron runs, and record each finding.
- `hermes-flight-recorder observe` — render the log locally as a `--stream`, an execution `--tree` with token and cost rollups, or a `--report` of findings that exits non-zero when any exist. No network.

The log reconstructs sessions, tool calls, subagent trees, model and cost usage, and cron runs. It captures live gateway events through an in-gateway hook, which spools them for Bridge to encrypt and sequence — so the encryption key never enters Hermes. It encrypts sensitive content on the host before it writes, and it keeps a per-installation sequence so lost events are detectable. Bridge's only write into the Hermes home is the capture hook it installs under `hooks/`; it never writes Hermes's own data.

Not built yet: any cloud sync. See the [Roadmap](#roadmap).

## The problem

Autonomous agents move from short conversation sessions to persistent background workers. These workers run on laptops, servers, gateways, cron schedules, and ephemeral containers. Their operational state splits across local databases, task boards, memory files, and trajectory logs on every host.

When an agent spans more than one machine, you can no longer answer what ran, what changed, and what broke. The state dies with the machine that hosted it.

## How it works

A small local companion, Bridge, runs with Hermes:

1. **Capture.** A Hermes hook records semantic lifecycle events as they occur. These events include sessions, invocations, tool-loop steps, tool calls, delegations, task runs, and cron outcomes.
2. **Reconcile.** Hooks are non-transactional, so they can lose events. Thus Bridge also reads the durable local state of Hermes. This state includes `state.db` and the cron execution database. Bridge compares this state against the events it captured. In this way, Bridge can detect gaps in the event stream.
3. **Encrypt.** Bridge encrypts sensitive content on the host, before the content leaves it. This content includes prompts, messages, task bodies, tool inputs and outputs, memory, and artifacts.
4. **Buffer and sync.** Events go into a durable local outbox and sync asynchronously. If the network stops, the agent continues to operate. The events catch up later.
5. **Serve.** The cloud stores an immutable event ledger and queryable projections. It coordinates distributed task leases. It supplies a fleet console.

```
   YOUR MACHINE                                 HOSTED CLOUD  (you install nothing here)
   ┌──────────────────────────┐                 ┌───────────────────────────────────┐
   │  Hermes  (~/.hermes)      │  encrypted      │  Ingestion → durable event ledger  │
   │  state.db · cron · sessions│  event batches  │  and queryable projections         │
   │                           │  ──────────────► │  and distributed task coordination │
   │  ┌─────────────────────┐  │   HTTPS         │                                    │
   │  │ Bridge (companion)  │  │                 │  ┌──────────────────────────────┐  │
   │  │  capture · encrypt  │  │  ◄────────────  │  │ Console — fleet health,        │  │
   │  │  buffer · reconcile │  │  pull, signals  │  │ execution trees, tasks, audit  │  │
   │  └─────────────────────┘  │                 │  └──────────────────────────────┘  │
   └──────────────────────────┘                 └───────────────────────────────────┘
        local-first plane                                  durable coordination plane
```

## What you install

- **Bridge** — the local companion and Hermes hook. It is small and open source. It runs on your host. This is the only software that you install.
- **Console** — a hosted web dashboard. You do not install it. You log in to it.

The cloud is hosted infrastructure. It includes ingestion, the ledger, and coordination. You do not run it.

## Privacy boundary

We are precise about this. We do not make absolute claims.

| Encrypted on the host (we cannot read it) | Visible metadata (we must see it) |
|---|---|
| Prompts, responses, messages, reasoning, task descriptions, memory text, skill files, tool inputs and outputs, trajectories, artifacts | Event types, tenant and agent IDs, timestamps, status, token counts, model identifiers, durations, sequence and cursor fields |

Bridge encrypts content end-to-end with client-held keys. The service keeps some metadata in plaintext, because it needs this metadata to route, coordinate, and bill. Thus we say the service has zero knowledge of your content, but not of all metadata.

## Design principles

- **Local authority** — the agent continues to work when the cloud is not available.
- **Legibility** — you can explain every important action, task transition, and failure.
- **Privacy by architecture** — Bridge encrypts sensitive content before it leaves the host.
- **Operational honesty** — we do not claim exactly-once delivery, zero knowledge, or freedom from conflicts without precise guarantees. We synchronize semantic agent events, not arbitrary SQLite pages.

## Roadmap

| Phase | Focus |
|---|---|
| **0 — Instrumentation contract** _(current)_ | Prove that a single host can produce loss-detectable, replayable events across restarts and offline periods |
| 1 — Observability MVP | Runtimes, sessions, invocations, tool and model calls, delegation, and cron, into a cloud ledger and a fleet and execution console |
| 2 — Distributed Kanban | Task claims, leases, fencing tokens, and attempt history across hosts |
| 3 — Encrypted content sync | Workspace and device keys, encrypted messages, tasks, and memory, and key rotation |
| 4 — Knowledge and MLOps | Memory and skill provenance, trajectory export, and governed datasets |
| 5 — Cross-framework | Adapters beyond Hermes, after we prove the concept |

## Relationship to Hermes

Hermes Flight Recorder is independent infrastructure for the Hermes ecosystem. Nous Research and the Hermes project do not own, endorse, or supply it.

## License

We license the Bridge companion, the Hermes hook, and the event schema and SDK in this repository under **Apache-2.0**. The hosted cloud service is a separate, proprietary component.

---

<sub>The name means the flight recorder for your agents — the black box that survives the crash. Think of capture, sequence, sync, and replay. But the infrastructure is the point.</sub>
