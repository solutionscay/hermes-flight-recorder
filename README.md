# Hermes Flight Recorder

**The black box for Hermes agents.**

Hermes Flight Recorder records what your agents did, what failed, and where the record is incomplete. It runs beside Hermes, keeps working when the network is down, and encrypts sensitive captured content before it leaves the machine.

Hermes Flight Recorder captures Hermes activity into a durable, append-ordered event log. It can reconcile that log against Hermes state, so missing events and failed work are visible instead of silently disappearing. Optional retention can bound the local copy after the server has durably acknowledged it; retention is off by default.

## Local first. Cloud optional.

It is useful on its own: capture, inspect, reconcile, and keep the data local.

For a shared fleet view, it can sync encrypted event envelopes to **Hermes DBaaS**, the hosted control plane at `hermesdbaas.com`. The ingestion protocol is open, so you can also run your own compatible backend.

```text
Hermes → local encrypted event log → Hermes DBaaS or your backend
```

The cloud is never in an agent's critical path.

## What it records

- Sessions, invocations, tools, delegation, model usage, cron runs, and gateway lifecycle
- Terminal model-provider failures, including retry count and safe error classification
- Gaps, missed cron runs, stale work, and failed gateway starts

Messages, responses, tool output, and raw provider errors are encrypted. Operational metadata remains available for debugging and reconciliation.
Invocation hooks record timing and attribution immediately without Hermes's
truncated previews. Complete user and assistant text is collected from
`state.db` on the next poll and linked to the same invocation.

## Install

One Hermes home is one Flight Recorder installation. Runtime data lives under
`$HERMES_HOME/flight-recorder`; the only change Flight Recorder makes to the
Hermes home is the hook at `$HERMES_HOME/hooks/hermes-flight-recorder`.

```bash
pipx install hermes-flight-recorder            # or: pip install hermes-flight-recorder
hermes-flight-recorder install --hermes-home "$HOME/.hermes-dev"

# Restart the Hermes gateway to load the hook, then run the companion:
hermes-flight-recorder serve --hermes-home "$HOME/.hermes-dev"
```

`install` is idempotent: it creates the recorder home, mints the installation
identity and encryption key, writes configuration with restrictive permissions,
and installs (or repoints) the hook. It never registers an OS service.

`serve` is one portable foreground process that captures, reconciles, and —
when a sync config is present — syncs, each on its own interval, guarded by a
single-instance lock. Native service registration (systemd, launchd, Windows
Service) simply wraps this command.

The recorder home resolves by precedence: `--flight-recorder-home`, then
`$SC_HERMES_FLIGHT_RECORDER_HOME`, then `$HERMES_HOME/flight-recorder`.

### Configure sync (optional)

Syncing to Hermes DBaaS needs an ingest endpoint and a Cloudflare Access service
token. `configure-sync` writes them to a private (`0600`) `sync-config.json` in
the recorder home, keeping the secret out of shell history:

```bash
hermes-flight-recorder configure-sync --hermes-home "$HOME/.hermes-dev" \
  --client-id "<token-id>.access"
# prompts for the client secret (or reads $HFR_CF_ACCESS_CLIENT_SECRET / --client-secret-stdin)
```

`--ingest-url` defaults to the hosted endpoint. Re-running with a single flag
does a partial update, so you can change the endpoint without re-entering the
credential. `serve` then picks the config up automatically; without it, the
recorder keeps capturing and reconciling locally and the outbox buffers.

### One-shot commands

`run`, `reconcile`, and `sync` remain available as single passes an external
scheduler can drive, and `observe --tree` renders the captured log locally with
no network. `sync` is the only command that uses the network.

When retention is enabled in `recorder-config.json`, `hermes-flight-recorder prune` applies the configured age and byte limits immediately. `serve`, `run`, and `sync` also apply it automatically on a six-hour cadence. Events beyond the durable delivery cursor are never deleted.

## Status

Work in progress; not production-ready. Flight Recorder, the Hermes hook, and the protocol documents are Apache-2.0. Hermes DBaaS is a separate hosted product.

Read the [development setup](docs/dev-setup.md), [event envelope](docs/schema/envelope-v1.md), and [ingestion protocol](docs/schema/ingestion-protocol-v1.md) for the technical detail.
