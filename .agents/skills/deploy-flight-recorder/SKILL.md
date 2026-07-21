---
name: deploy-flight-recorder
description: Deploy or redeploy the Hermes Flight Recorder (Bridge) into its systemd runtime venv so the running capture/sync services test the latest repo code. Use when deploying, redeploying, restarting the recorder, or after changing recorder code, dependencies, or the console entry point.
---

# Deploy the Hermes Flight Recorder

The runtime that the systemd services execute is the venv at
`~/.local/share/hermes-flight-recorder-runtime/venv`. It runs an **editable**
install of this repo, so ordinary `.py` edits are already live on the next
service tick (~15s) — no deploy needed for a normal code change.

**To deploy, run from the repo root:**

```bash
./deploy.sh
```

`deploy.sh` is the source of truth for the steps. It pauses the timers, clears
stale bytecode, **removes any non-editable install that could shadow the repo**,
`pip install -e .`, verifies the deployed code, runs a smoke capture, and
restarts the timers. It is idempotent.

## When to run it

- After changing `pyproject.toml` (dependencies) or the console entry point.
- After pulling / restructuring code, or moving the repo.
- Any time you want a guaranteed-clean restart + verification.
- You do **not** need it for a plain `.py` edit — that is already live.

## The failure mode this guards against

A **non-editable (copied) install** of the package in the runtime venv's
`site-packages` silently **shadows** the repo: the console script the services
run imports the stale copy instead of your code. This once shipped a snapshot
whose `collector/run_pass` had no `gateway_log` collector, so the recorder
**silently dropped every `model.call_failed` event** (terminal model-provider
failures like 404 "model not found") while looking healthy. `deploy.sh` purges
any such copy and fails loudly if the deployed code does not import from the
repo or is missing `gateway_log`.

## Verify a deploy by hand

```bash
VENV=~/.local/share/hermes-flight-recorder-runtime/venv
# 1) The console script imports from the REPO, not site-packages, and has gateway_log:
"$VENV/bin/python" -c "import inspect, hermes_flight_recorder as p, hermes_flight_recorder.collector as c; \
print(p.__file__); print('gateway_log' in inspect.getsource(c.run_pass))"
# 2) Timers are active:
systemctl --user is-active hermes-flight-recorder-capture.timer hermes-flight-recorder-sync.timer
# 3) Watch capture live (model failures should appear within ~15s of happening):
journalctl --user -u hermes-flight-recorder-capture.service -f
```

A healthy capture pass advances the `gateway-log:agent.log` byte-cursor in the
outbox to the current size of `~/.hermes/logs/agent.log`.

## Not this skill

The **DBaaS** backend (`hermes-dbass`) is a separate Cloudflare Worker with its
own deploy (`npm run deploy` / `wrangler deploy`) — not covered here.
