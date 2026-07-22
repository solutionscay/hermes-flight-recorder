# Dev setup

How to develop Hermes Flight Recorder against a **dev** Hermes install, so you do not
touch your production agents.

## The golden rule

Hermes Flight Recorder attaches to a Hermes install and reads its durable state. **Never
point Hermes Flight Recorder at a production Hermes home.** Use a throwaway dev instance
you can wipe and reset freely.

Hermes locates its home directory from the `HERMES_HOME` environment
variable, and defaults to `~/.hermes`. That variable is what isolates a
dev instance from production.

## Prerequisites

- Python 3.11+
- A Hermes install to develop against (see below)
- `git`

## 1. Get an isolated dev Hermes

You have two options:

- **This machine's `~/.hermes`.** If this box has no production agents,
  its default `~/.hermes` *is* your dev instance. This is the simplest choice.
- **A dedicated home.** For full isolation, run a separate Hermes with
  its own home:

  ```bash
  export HERMES_HOME="$HOME/hermes-dev"   # isolated data dir
  hermes setup                            # configure this instance
  ```

Whichever you pick, this must not be a home that any production agent
uses.

## 2. Install Hermes Flight Recorder (editable)

From the repo root:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e .
hermes-flight-recorder --version        # verify the console script works
```

Or with uv:

```bash
uv venv --python 3.11
uv pip install -e .
```

## 3. Point Hermes Flight Recorder at the dev Hermes

Hermes Flight Recorder reads the Hermes home the same way Hermes does — from
`HERMES_HOME`, and defaults to `~/.hermes`. Set it to the dev instance
from step 1 when you run Hermes Flight Recorder commands.

`hermes-flight-recorder init` creates the outbox and installs the live
capture hook under `$HERMES_HOME/hooks/`; restart the Hermes gateway to
load it. Then `run` drains the hook spool and polls the durable stores,
`reconcile` diffs them for gaps, and `observe` renders the log.

## Recorder configuration

Optional non-secret operational settings live in
`$SC_HERMES_FLIGHT_RECORDER_HOME/recorder-config.json` (or
`~/.hermes-flight-recorder/recorder-config.json` when `SC_HERMES_FLIGHT_RECORDER_HOME` is not
set). Hermes Flight Recorder treats a missing file or missing key as its built-in default.
Environment variables take precedence over file values. The file is written
with mode `0600` by `recorder_config.save`; create it with the same mode when
managing it yourself.

```json
{
  "capture": {
    "max_content_bytes": 65536,
    "message_roles": ["user", "assistant", "tool"],
    "sources": {"hook": true}
  },
  "retention": {
    "enabled": false,
    "max_age_days": 30,
    "max_bytes": null,
    "require_delivered": true,
    "vacuum": "auto"
  },
  "sync": {
    "interval_seconds": null,
    "max_records": 500,
    "max_bytes": 1048576
  }
}
```

`capture` is the configuration surface for capture limits. Retention is off
by default, preserving the unbounded local history. When enabled,
`hermes-flight-recorder prune` removes events older than `max_age_days` or,
oldest-first, until retained event-envelope JSON fits `max_bytes`. Only events
at or below the server-acknowledged delivery cursor are eligible;
`require_delivered` must remain `true`. `vacuum: "auto"` reclaims SQLite pages
after a deletion. `run` and `sync` also check the policy at most once every six
hours. The `seq` and `meta` tables, including all producer and delivery
cursors, are preserved.

`sync.max_records` and `sync.max_bytes` are active now.
`sync.interval_seconds` is `null` by default,
preserving the current one-pass `sync` behavior; set a positive number to run
continuously. An explicit `sync --interval` takes precedence over that value.

The environment equivalents are `HFR_CAPTURE_MAX_CONTENT_BYTES`,
`HFR_CAPTURE_MESSAGE_ROLES` (a JSON array), `HFR_CAPTURE_SOURCES` (a JSON
object), `HFR_RETENTION_ENABLED`, `HFR_RETENTION_MAX_AGE_DAYS`,
`HFR_RETENTION_MAX_BYTES`, `HFR_RETENTION_REQUIRE_DELIVERED`,
`HFR_RETENTION_VACUUM`,
`HFR_SYNC_INTERVAL_SECONDS`, `HFR_SYNC_MAX_RECORDS`, and
`HFR_SYNC_MAX_BYTES`. The ingest URL and Cloudflare Access credentials remain
in the separate private `sync-config.json` or their existing environment
variables, so credentials do not mix with operational configuration.

## Safety notes

- **Hermes Flight Recorder is read-only against Hermes state.** It reads `state.db` and
  the cron execution database; it does not write to them. Its only write
  into the Hermes home is the event hook it installs under
  `$HERMES_HOME/hooks/`.
- **Agent state never enters git.** The repo `.gitignore` excludes
  `*.db`, `*.sqlite*`, the local `outbox.sqlite`, `.env`, keys, and any
  `.hermes/` or `.hermes-dev/` directory. Run a Hermes session, then
  make sure that `git status` stays clean.
