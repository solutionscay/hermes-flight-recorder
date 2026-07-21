#!/usr/bin/env bash
#
# Deploy Hermes Flight Recorder (Bridge) into its systemd runtime venv.
#
# The runtime venv uses an EDITABLE install pointing at this repo, so ordinary
# .py edits are already live on the next ~15s service tick. Run this script:
#   - after changing dependencies (pyproject.toml) or the console entry point,
#   - after pulling / restructuring code,
#   - or any time you want a guaranteed-clean restart + verification.
#
# It is idempotent and safe to run repeatedly. It fails loudly (non-zero) if the
# deployed code does not import from this repo or is missing the gateway_log
# collector -- the exact failure mode that silently dropped model.call_failed
# events (a stale duplicate install shadowing the repo).
#
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="${HFR_RUNTIME_VENV:-$HOME/.local/share/hermes-flight-recorder-runtime/venv}"
PY="$VENV/bin/python"
PIP="$VENV/bin/pip"
RT="$VENV/bin/hermes-flight-recorder"
BRIDGE_HOME="${BRIDGE_HOME:-$HOME/.hermes-flight-recorder}"
HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
SERVICES=(hermes-flight-recorder-capture hermes-flight-recorder-sync)

[ -x "$PY" ] || { echo "ERROR: runtime venv not found at $VENV" >&2; exit 1; }

echo "==> Pausing timers"
for s in "${SERVICES[@]}"; do systemctl --user stop "$s.timer" 2>/dev/null || true; done

echo "==> Clearing stale bytecode in the repo"
find "$REPO/hermes_flight_recorder" -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true

echo "==> Removing any non-editable install that could shadow the repo"
# A regular (copied) install in site-packages wins over the editable finder for
# the console script. Purge it so there is a single source of truth: this repo.
for sp in "$VENV"/lib/python*/site-packages; do
  find "$sp" -maxdepth 1 -name "hermes_flight_recorder" -type d -exec rm -rf {} + 2>/dev/null || true
done

echo "==> Installing latest (editable) from $REPO"
"$PIP" install -e "$REPO" --quiet

echo "==> Verifying deployed code"
"$PY" - <<'PYEOF'
import sys, inspect
import hermes_flight_recorder as pkg
import hermes_flight_recorder.collector as col
ok = True
loc = pkg.__file__
if "/site-packages/" in loc:
    print(f"  FAIL: package resolves to site-packages, not the repo: {loc}"); ok = False
else:
    print(f"  ok: imports from repo: {loc}")
if "gateway_log" not in inspect.getsource(col.run_pass):
    print("  FAIL: run_pass is missing the gateway_log collector"); ok = False
else:
    print("  ok: run_pass includes gateway_log (model-failure capture)")
sys.exit(0 if ok else 1)
PYEOF

echo "==> Smoke capture (one pass)"
"$RT" run --bridge-home "$BRIDGE_HOME" --hermes-home "$HERMES_HOME"

echo "==> Restarting timers"
for s in "${SERVICES[@]}"; do
  systemctl --user start "$s.timer"
  systemctl --user is-active "$s.timer" >/dev/null 2>&1 && echo "  $s.timer active"
done

echo "==> Deployed: $("$RT" --version) from $REPO"
