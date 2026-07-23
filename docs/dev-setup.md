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

`hermes-flight-recorder install --hermes-home <path>` is idempotent: it creates
the recorder home at `$HERMES_HOME/flight-recorder`, mints the installation
identity and encryption key, writes configuration with mode `0600`, and installs
(or repoints) the live capture hook under `$HERMES_HOME/hooks/`. Restart the
Hermes gateway to load the hook. It never registers an OS service.

`hermes-flight-recorder serve --hermes-home <path>` then runs one portable
foreground process: it drains the hook spool and polls the durable stores on
`capture.interval_seconds` (default 15s), reconciles the stores against the
outbox on `reconcile.interval_seconds` (default 60s) — independently, so it
flags capture staleness even when capture is broken — and syncs when a sync
config is present. A `runtime.lock` in the recorder home enforces a single
instance; SIGINT/SIGTERM shut it down cleanly. Native service managers (systemd,
launchd, Windows Service) wrap this same command.

`run`, `reconcile`, and `sync` remain available as one-shot passes for an
external scheduler, and `observe` renders the log locally.

The recorder home resolves by precedence: `--flight-recorder-home`, then
`$SC_HERMES_FLIGHT_RECORDER_HOME`, then `$HERMES_HOME/flight-recorder`.

## Recorder configuration

Optional non-secret operational settings live in `recorder-config.json` inside
the recorder home — by default `$HERMES_HOME/flight-recorder/recorder-config.json`
(or `$SC_HERMES_FLIGHT_RECORDER_HOME/recorder-config.json` when that override is
set). Hermes Flight Recorder treats a missing file or missing key as its built-in default.
Environment variables take precedence over file values. The file is written
with mode `0600` by `recorder_config.save`; create it with the same mode when
managing it yourself.

```json
{
  "capture": {
    "max_content_bytes": 65536,
    "message_roles": ["user", "assistant", "tool"],
    "sources": {"hook": true},
    "interval_seconds": 15
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
  },
  "reconcile": {
    "interval_seconds": 60
  }
}
```

`capture.message_roles` selects which supported `state.db` message roles
(`user`, `assistant`, and `tool`) become encrypted events.
`capture.max_content_bytes` limits each encrypted body by UTF-8 byte length
(64 KiB by default). A capped event records `content_truncated`,
`content_original_bytes`, and `content_captured_bytes` in plaintext metadata,
so a shortened body is never silent. The limit applies uniformly to user,
assistant, and tool content.

Invocation hooks remain the immediate metadata source. Hermes truncates the
message and response values it supplies to hooks, so the installed spooler
removes those previews before writing the spool. The next `state.db` poll
captures complete content-bearing user/assistant rows, encrypts them, and
attributes them to the hook invocation window. A final assistant row becomes
`invocation.completed`; non-empty assistant text attached to a tool-call step
becomes `model.call_succeeded`, so the text is retained without ending the
invocation early. Empty assistant rows that only carry tool-call structure are
skipped; their tool results are captured through the `tool` role. On the first
poll after upgrading to this capture model, a versioned cursor performs a
one-time message-table backfill. Existing tool events deduplicate by their
stable keys; user/assistant rows gain their encrypted durable record.

Retention is off by default, preserving the unbounded local history. When enabled,
`hermes-flight-recorder prune` removes events older than `max_age_days` or,
oldest-first, until retained event-envelope JSON fits `max_bytes`. Only events
at or below the server-acknowledged delivery cursor are eligible;
`require_delivered` must remain `true`. `vacuum: "auto"` reclaims SQLite pages
after a deletion. `run` and `sync` also check the policy at most once every six
hours. The `seq` and `meta` tables, including all producer and delivery
cursors, are preserved. Before an envelope is deleted, the recorder keeps a
compact tombstone with its sequence, deduplication identity, and non-content
reconciliation fields. Tombstones contain no encrypted body or full envelope;
they stop durable-store polls from recreating delivered events and stop
reconciliation from reporting intentional retention as capture loss.

`capture.interval_seconds` (default 15) and `reconcile.interval_seconds`
(default 60) set the `serve` cadences; the one-shot `run` and `reconcile`
commands ignore them. `sync.max_records` and `sync.max_bytes` are active now.
`sync.interval_seconds` is `null` by default, preserving the one-pass `sync`
behavior; under `serve`, a `null` sync interval falls back to 60s when a sync
config is present. An explicit `sync --interval` or `serve --sync-interval`
takes precedence over the file value.

The environment equivalents are `HFR_CAPTURE_MAX_CONTENT_BYTES`,
`HFR_CAPTURE_MESSAGE_ROLES` (a JSON array), `HFR_CAPTURE_SOURCES` (a JSON
object), `HFR_CAPTURE_INTERVAL_SECONDS`, `HFR_RETENTION_ENABLED`,
`HFR_RETENTION_MAX_AGE_DAYS`, `HFR_RETENTION_MAX_BYTES`,
`HFR_RETENTION_REQUIRE_DELIVERED`, `HFR_RETENTION_VACUUM`,
`HFR_RECONCILE_INTERVAL_SECONDS`, `HFR_SYNC_INTERVAL_SECONDS`,
`HFR_SYNC_MAX_RECORDS`, and `HFR_SYNC_MAX_BYTES`. The ingest URL and Cloudflare
Access credentials remain
in the separate private `sync-config.json` or their existing environment
variables, so credentials do not mix with operational configuration.

`hermes-flight-recorder configure-sync` writes that `sync-config.json` (mode
`0600`) from `--ingest-url` (default: the hosted endpoint), `--client-id`, and a
client secret read from `--client-secret-stdin`, `$HFR_CF_ACCESS_CLIENT_SECRET`,
or an interactive prompt — never a plain flag by default, so the secret stays
out of shell history. It merges over any existing file, so a single flag does a
partial update. The environment variables above still override the file at load
time for injecting a secret without writing it to disk.

## Safety notes

- **Hermes Flight Recorder is read-only against Hermes state.** It reads `state.db` and
  the cron execution database; it does not write to them. Its only write
  into the Hermes home is the event hook it installs under
  `$HERMES_HOME/hooks/`.
- **Agent state never enters git.** The repo `.gitignore` excludes
  `*.db`, `*.sqlite*`, the local `outbox.sqlite`, `.env`, keys, and any
  `.hermes/` or `.hermes-dev/` directory. Run a Hermes session, then
  make sure that `git status` stays clean.
