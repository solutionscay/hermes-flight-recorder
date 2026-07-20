# Hermes Flight Recorder

**The local-first execution recorder and control-plane project for [Hermes](https://hermes-agent.nousresearch.com) agents.**

Your agents stay fast and local. Bridge records what happened, preserves a durable sequence, and tells you when the record is incomplete. The cloud is never part of the agent's critical path.

> **Status: Phase 0 complete; Phase 1 is next.** The local recorder works. Cloud sync, the hosted ledger, and the fleet console do not exist yet. This is an early prototype, not production software.

## What it is

Hermes Flight Recorder is a local-first observability and control-plane project for people running Hermes across laptops, servers, gateways, and other runtimes.

The repository currently contains **Bridge**, a local companion that captures Hermes execution events, encrypts sensitive captured content, assigns a durable per-installation sequence, reconciles the event stream against Hermes's durable databases, and renders the result locally.

The longer-term project will sync encrypted events to a durable cloud ledger and coordinate work across multiple Hermes installations. That part is still on the roadmap.

The project is not remote SQLite, a vector store, or another tracing wrapper. It is designed to answer operational questions such as:

- What ran, in what order, and at what token cost?
- Which sessions and subagents were involved?
- Which durable rows were never captured as live events?
- Which invocation never ended, which cron run never started, or which gateway failed before its startup hook fired?
- Can I inspect fleet health without sending private prompts and outputs to a third party in plaintext?

## What works today

Everything in the current implementation runs locally and makes no network requests.

- `hermes-flight-recorder init` creates the Bridge outbox, mints a stable installation ID, and installs a package event hook under the Hermes home.
- `hermes-flight-recorder run` drains the gateway hook spool and polls Hermes's `state.db` and cron execution store read-only.
- `hermes-flight-recorder reconcile` records sequence gaps, durable rows missing from capture, stale starts without terminals, missed cron runs, stale cron tickers, and gateway startup failures.
- `hermes-flight-recorder observe` renders the local outbox as an ordered `--stream`, an execution `--tree` with token and cost rollups, or a `--report` that exits non-zero when findings exist.

The append-only SQLite outbox owns the installation ID and monotonic `producer_sequence`. Producers use stable deduplication keys, so polling or draining the same source again does not create duplicate events or consume sequence numbers. Envelope v1 and the ingestion protocol v1 are documented under [`docs/schema/`](docs/schema/).

Sensitive content that the current collectors capture, including messages, responses, tool results, delegation details, and raw gateway failure text, is encrypted before it enters the outbox. Operational metadata remains plaintext so Bridge can reconcile and render it.

### Current Hermes limitation

Hermes package event hooks (`HOOK.yaml` plus `handler.py`) currently run only in the gateway. Bridge can still recover durable sessions, tool results, model usage, delegation, and cron activity from Hermes databases across other surfaces, but it cannot reconstruct exact in-memory invocation boundaries or authoritative turn IDs after the fact.

Upstream issue [NousResearch/hermes-agent#67798](https://github.com/NousResearch/hermes-agent/issues/67798) tracks the shared lifecycle-hook contract needed for equivalent live capture from CLI, one-shot, TUI, desktop, cron, and subagent execution paths.

## Try it locally

Use a disposable Hermes home. Bridge reads Hermes state and installs a hook, so do not point an early development checkout at an installation you cannot replace.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

export HERMES_HOME="$HOME/.hermes-dev"
hermes-flight-recorder init
```

Restart the Hermes gateway so it loads the installed hook, drive a test session, then run:

```bash
hermes-flight-recorder run
hermes-flight-recorder reconcile
hermes-flight-recorder observe --tree
hermes-flight-recorder observe --report
```

`BRIDGE_HOME` defaults to `~/.hermes-flight-recorder`. It must remain outside `HERMES_HOME`. See [docs/dev-setup.md](docs/dev-setup.md) for the development setup and safety notes.

Two executable checks exercise the complete local pipeline:

```bash
python scripts/poc_exit_gate.py -v
python scripts/live_capture_check.py -v
```

The deterministic POC injects a dropped event and a missed cron run, then proves both are detected across a Bridge restart. The live check polls a real Hermes home into a throwaway outbox and verifies that the durable Hermes files are unchanged. See [docs/poc-demo.md](docs/poc-demo.md).

## The problem

Autonomous agents are moving from short conversations to persistent background workers. They run across laptops, servers, gateways, cron schedules, and ephemeral containers. Their operational state is split across local databases, task boards, memory files, and trajectory logs on every host.

Once agents span more than one machine, it becomes difficult to answer what ran, what changed, and what broke. Local state can disappear with the machine that hosted it, while non-transactional hooks can silently omit the event that explains a failure.

## Architecture

Bridge implements the local half today. The hosted half is planned.

1. **Capture (implemented).** The Hermes gateway hook records live lifecycle events. Read-only adapters reconstruct durable session, tool, model-usage, delegation, and cron events.
2. **Reconcile (implemented).** Bridge compares captured events with the outbox sequence and Hermes's durable stores. Findings become first-class events in the same log.
3. **Encrypt (prototype).** Bridge encrypts captured sensitive content locally with AES-256-GCM before writing it to the outbox.
4. **Buffer (implemented) and sync (planned).** The outbox survives restarts. Phase 1 adds batching, a durable delivery cursor, HTTPS transport, acknowledgements, and offline retry.
5. **Serve (planned).** The hosted service will maintain the durable event ledger, queryable projections, distributed task coordination, and the fleet console.

```text
   YOUR MACHINE (IMPLEMENTED)                  HOSTED CLOUD (PLANNED)
   +---------------------------+               +-----------------------------------+
   | Hermes                    |  encrypted    | ingestion -> durable event ledger |
   | state.db / cron / hooks   |  batches      | and queryable projections         |
   |                           |  -----------> |                                   |
   | +-----------------------+ |    HTTPS      | +-------------------------------+ |
   | | Bridge                | |               | | fleet console                 | |
   | | capture / encrypt     | |               | | health / trees / tasks / audit| |
   | | buffer / reconcile    | |               | +-------------------------------+ |
   | +-----------------------+ |               +-----------------------------------+
   +---------------------------+
        local authority                           durable coordination
```

## Data and privacy boundary

The current prototype does not send anything over the network. It stores selected sensitive fields as ciphertext in the local outbox and leaves operational metadata plaintext.

| Encrypted in the local outbox today | Plaintext operational metadata |
|---|---|
| Captured messages and responses, tool results, delegation details, raw gateway failure text | Event types, installation and session IDs, timestamps, surfaces, status, model identifiers, token counts, cost, sequence fields, runtime inventory |

The prototype key is generated locally and stored as `content-dev.key` in the Bridge home with mode `0600`. This proves the envelope and local encryption boundary; it is not the final end-to-end key-custody design. Workspace/device keys, rotation, recovery, and production service access guarantees belong to the encrypted-content phase of the roadmap.

The planned service must see enough metadata to route, reconcile, coordinate, and bill. The intended guarantee is zero knowledge of encrypted content, not zero knowledge of all metadata.

## Design principles

- **Local authority** — the agent continues to work when the cloud is unavailable.
- **Legibility** — operators can explain important actions, task transitions, and failures.
- **Privacy by architecture** — sensitive content is encrypted before any future network boundary.
- **Operational honesty** — guarantees such as at-least-once delivery, detectable loss, and metadata visibility are stated precisely.
- **Semantic events** — Bridge records agent activity rather than synchronizing arbitrary SQLite pages.

## Roadmap

| Phase | Status | Focus |
|---|---|---|
| **0 — Instrumentation contract** | Complete | Loss-detectable local capture, durable sequencing, reconciliation, encrypted content fields, and local observation |
| **1 — Buffer and sync** | Next | Batching, durable delivery cursor, enrollment/auth, HTTPS transport, retry, sync CLI, and an end-to-end network loss gate |
| 2 — Distributed Kanban | Planned | Task claims, leases, fencing tokens, and attempt history across hosts |
| 3 — Encrypted content sync | Planned | Production workspace/device keys, encrypted messages, tasks and memory, recovery, and key rotation |
| 4 — Knowledge and MLOps | Planned | Memory and skill provenance, trajectory export, and governed datasets |
| 5 — Cross-framework | Planned | Adapters beyond Hermes after the Hermes path is proven |

Phase 1 starts with [#32, the sync client skeleton and durable delivery cursor](https://github.com/solutionscay/hermes-flight-recorder/issues/32). The wire contract it consumes is frozen in [docs/schema/ingestion-protocol-v1.md](docs/schema/ingestion-protocol-v1.md).

## Relationship to Hermes

Hermes Flight Recorder is independent infrastructure for the Hermes ecosystem. Nous Research and the Hermes project do not own, endorse, or supply it.

Bridge treats Hermes's durable databases as read-only. Its only write into the Hermes home is the package event hook installed under `hooks/`; all outbox data, cursors, spool files, and keys live in the separate Bridge home.

## License

The Bridge companion, Hermes hook, event schema, and ingestion protocol in this repository are licensed under **Apache-2.0**. Any future hosted cloud service will be a separate proprietary component.

---

<sub>The name means the flight recorder for your agents: the black box that survives the crash. Capture, sequence, reconcile, and eventually sync.</sub>
