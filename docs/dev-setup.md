# Dev setup

How to develop Bridge against a **dev** Hermes install, without touching
your production agents.

## The golden rule

Bridge attaches to a Hermes install and reads its durable state. **Never
point Bridge at a production Hermes home.** Use a throwaway dev instance
you can wipe and reset freely.

Hermes locates its home directory from the `HERMES_HOME` environment
variable, defaulting to `~/.hermes`. That variable is what isolates a
dev instance from prod.

## Prerequisites

- Python 3.11+
- A Hermes install to develop against (see below)
- `git`

## 1. Get an isolated dev Hermes

You have two options:

- **This machine's `~/.hermes`.** If this box has no production agents,
  its default `~/.hermes` *is* your dev instance. Simplest choice.
- **A dedicated home.** For full isolation, run a separate Hermes with
  its own home:

  ```bash
  export HERMES_HOME="$HOME/hermes-dev"   # isolated data dir
  hermes setup                            # configure this instance
  ```

Whichever you pick, this must not be a home that any production agent
uses.

## 2. Install Bridge (editable)

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

## 3. Point Bridge at the dev Hermes

Bridge reads the Hermes home the same way Hermes does — from
`HERMES_HOME`, defaulting to `~/.hermes`. Set it to the dev instance
from step 1 when you run Bridge commands.

`hermes-flight-recorder init` creates the outbox and installs the live
capture hook under `$HERMES_HOME/hooks/`; restart the Hermes gateway to
load it. Then `run` drains the hook spool and polls the durable stores,
`reconcile` diffs them for gaps, and `observe` renders the log.

## Safety notes

- **Bridge is read-only against Hermes state.** It reads `state.db` and
  the cron execution database; it does not write to them. Its only write
  into the Hermes home is the event hook it installs under
  `$HERMES_HOME/hooks/`.
- **Agent state never enters git.** The repo `.gitignore` excludes
  `*.db`, `*.sqlite*`, the local `outbox.sqlite`, `.env`, keys, and any
  `.hermes/` or `.hermes-dev/` directory. Run a Hermes session, then
  confirm `git status` stays clean.
