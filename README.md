# Hermes Flight Recorder

**The black box for Hermes agents.**

Hermes Flight Recorder records what your agents did, what failed, and where the record is incomplete. It runs beside Hermes, keeps working when the network is down, and encrypts sensitive captured content before it leaves the machine.

Bridge—the local companion in this repository—captures Hermes activity into a durable, append-only event log. It can reconcile that log against Hermes state, so missing events and failed work are visible instead of silently disappearing.

## Local first. Cloud optional.

Bridge is useful on its own: capture, inspect, reconcile, and keep the data local.

For a shared fleet view, it can sync encrypted event envelopes to **Hermes DBaaS**, the hosted control plane at `hermesdbass.com`. The ingestion protocol is open, so you can also run your own compatible backend.

```text
Hermes → Bridge → local encrypted event log → Hermes DBaaS or your backend
```

The cloud is never in an agent's critical path.

## What it records

- Sessions, invocations, tools, delegation, model usage, cron runs, and gateway lifecycle
- Terminal model-provider failures, including retry count and safe error classification
- Gaps, missed cron runs, stale work, and failed gateway starts

Messages, responses, tool output, and raw provider errors are encrypted. Operational metadata remains available for debugging and reconciliation.

## Try it

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

export HERMES_HOME="$HOME/.hermes-dev"
hermes-flight-recorder init

# Restart the Hermes gateway, run a session, then:
hermes-flight-recorder run
hermes-flight-recorder reconcile
hermes-flight-recorder observe --tree
```

`sync` is optional and is the only command that uses the network. Configure it with an HTTPS endpoint and credentials, then run `hermes-flight-recorder sync`.

## Status

Work in progress; not production-ready. Bridge, the Hermes hook, and the protocol documents are Apache-2.0. Hermes DBaaS is a separate hosted product.

Read the [development setup](docs/dev-setup.md), [event envelope](docs/schema/envelope-v1.md), and [ingestion protocol](docs/schema/ingestion-protocol-v1.md) for the technical detail.
