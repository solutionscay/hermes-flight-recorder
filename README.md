# Hermes Flight Recorder

**The black box for Hermes agents.**

> This README uses short, simple sentences (Simplified Technical English style).
> An installer agent can follow the steps in order.

## 1. What this is

Hermes Flight Recorder is a companion program for Hermes. It records what your
agents do, what fails, and where the record is incomplete.

Flight Recorder does these things:

- It records sessions, tools, model usage, cron runs, and gateway events.
- It records failures, gaps, missed cron runs, and stale work.
- It encrypts message and tool content before the content leaves the machine.
- It keeps a local, ordered event log (the **outbox**).
- It continues to work when the network is down.

Flight Recorder is **local-first**. It is useful with no network. The cloud is
optional. The cloud is never in the critical path of an agent.

### Terms

| Term | Meaning |
|------|---------|
| **Hermes** | The agent system that Flight Recorder observes. |
| **the Hermes home** | The Hermes data directory. Its path is `$HERMES_HOME` (default `~/.hermes`). |
| **the recorder home** | The Flight Recorder data directory. Default: `$HERMES_HOME/flight-recorder`. |
| **the hook** | The capture component. Flight Recorder installs it in the Hermes gateway. |
| **Hermes DBaaS** | The optional cloud service at `hermesdbaas.com`. |

One Hermes home is one Flight Recorder installation. The only change that Flight
Recorder makes to the Hermes home is the hook at
`$HERMES_HOME/hooks/hermes-flight-recorder`. All other data stays in the
recorder home.

## 2. How it works with hermesdbaas.com

Flight Recorder can send its encrypted event log to **Hermes DBaaS**, the hosted
control plane at `hermesdbaas.com`. This gives you one view of many agents.

```text
Hermes  ->  local encrypted log (outbox)  ->  Hermes DBaaS  (or your backend)
```

Facts about the cloud connection:

- Sync is **optional**. Flight Recorder captures and reconciles with no cloud.
- The default endpoint is `https://app.hermesdbaas.com/ingest`.
- The endpoint authenticates with a **Cloudflare Access service token**. The
  token has a client id and a client secret.
- The ingestion protocol is open. You can also run your own compatible backend.
- Flight Recorder sends the token in two headers, not in the request body.
- If the network is down, the outbox keeps the events. The next sync sends them.

## 3. How to install and configure

### Prerequisites

Before you start, make sure of these three things:

1. Python 3.11 or higher is installed.
2. Hermes is installed and the Hermes home exists.
3. You know the Hermes home path. This document calls it `<HERMES_HOME>`.

### Step 1 — Install the package

Install the program from the public repository with `pipx`:

```bash
pipx install "git+https://github.com/solutionscay/hermes-flight-recorder"
```

You can also use `pip` in a virtual environment:

```bash
pip install "git+https://github.com/solutionscay/hermes-flight-recorder"
```

> The package is not on PyPI yet. Install from the repository. After the first
> release, `pipx install hermes-flight-recorder` will also work.

### Step 2 — Install into the Hermes home

Run the `install` command. Give it the Hermes home path.

```bash
hermes-flight-recorder install --hermes-home "<HERMES_HOME>"
```

The `install` command does these things:

- It creates the recorder home at `<HERMES_HOME>/flight-recorder`.
- It creates the installation identity and the encryption key.
- It writes the configuration files with private permissions (`0600`).
- It installs (or updates) the hook.
- It verifies the result.

The `install` command is idempotent. You can run it again with no harm. It does
not change the installation identity or the encryption key. It does not register
an operating-system service.

### Step 3 — Restart the Hermes gateway

Restart the Hermes gateway. The gateway loads the hook only at start.

> The hook does not capture events until you restart the gateway.

### Step 4 — Configure sync (only for cloud)

Do this step only if you send data to Hermes DBaaS. If you keep the data local,
go to Step 5.

Run the `configure-sync` command. Send the client secret through standard input,
so the secret does not go into the shell history.

```bash
printf '%s' "<CLIENT_SECRET>" | hermes-flight-recorder configure-sync \
  --hermes-home "<HERMES_HOME>" \
  --client-id "<CLIENT_ID>" \
  --client-secret-stdin
```

The command writes a private (`0600`) `sync-config.json` in the recorder home.
The `--ingest-url` value defaults to the hosted endpoint. To use a different
endpoint, add `--ingest-url "<URL>"`.

To change one field later, run the command again with only that field. The
command keeps the other fields. For example, change the endpoint but keep the
token.

### Step 5 — Start the recorder

Run the `serve` command. This is one continuous foreground process.

```bash
hermes-flight-recorder serve --hermes-home "<HERMES_HOME>"
```

The `serve` process does these things:

- It captures events on a short interval (default 15 seconds).
- It reconciles the log against Hermes state on a longer interval (default 60
  seconds), so that missing events become visible.
- It syncs to the cloud when a sync configuration exists.
- It allows only one instance for each recorder home.
- It stops cleanly on `SIGINT` or `SIGTERM`.

> `serve` runs in the foreground and does not return. To run it continuously,
> keep the process alive with a service manager (systemd, launchd, or a Windows
> service). The service manager wraps this same command.

### Step 6 — Verify

Show the status of the installation:

```bash
hermes-flight-recorder status --hermes-home "<HERMES_HOME>"
```

The command prints the installation id, the outbox state, and the capture
freshness. The exit code is `0` when capture is healthy.

To read the captured log with no network, use `observe`:

```bash
hermes-flight-recorder observe --hermes-home "<HERMES_HOME>" --tree
```

## Command summary

| Command | Purpose |
|---------|---------|
| `install` | Set up the recorder home, identity, key, config, and hook. |
| `serve` | Run capture, reconcile, and optional sync in one process. |
| `configure-sync` | Write the cloud endpoint and the Cloudflare Access token. |
| `status` | Show capture freshness and delivery lag. |
| `observe` | Show the captured log locally (stream, tree, report). |
| `run` | Run one capture pass (for an external scheduler). |
| `reconcile` | Run one reconcile pass (for an external scheduler). |
| `sync` | Run one sync pass (the only command that uses the network). |
| `prune` | Remove delivered events per the retention configuration. |

The recorder home resolves in this order: the `--flight-recorder-home` value,
then the `SC_HERMES_FLIGHT_RECORDER_HOME` variable, then
`$HERMES_HOME/flight-recorder`.

## Status

This is work in progress. It is not production-ready. Flight Recorder, the
Hermes hook, and the protocol documents are Apache-2.0. Hermes DBaaS is a
separate hosted product.

For technical detail, read the [development setup](docs/dev-setup.md), the
[event envelope](docs/schema/envelope-v1.md), and the
[ingestion protocol](docs/schema/ingestion-protocol-v1.md).
