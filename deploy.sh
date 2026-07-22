#!/usr/bin/env bash
#
# Deploy Hermes Flight Recorder into its systemd runtime venv.
#
# The runtime venv uses an EDITABLE install pointing at this repo, so ordinary
# .py edits are already live on the next service tick. Run this script:
#   - after changing dependencies (pyproject.toml) or the console entry point,
#   - after changing a CLI flag or a systemd unit template,
#   - after pulling / restructuring code,
#   - or any time you want a guaranteed-clean restart + verification.
#
# It is idempotent and safe to run repeatedly. It owns the systemd units: the
# .service/.timer files are rendered from the committed templates in systemd/*.in
# on every deploy, so a CLI-flag rename can never leave a stale flag baked into a
# unit's ExecStart (the failure that crash-looped capture on --bridge-home). It
# fails loudly (non-zero) if the deployed code does not import from this repo, is
# missing the gateway_log collector, or if a unit's real ExecStart does not run.
#
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="${HFR_RUNTIME_VENV:-$HOME/.local/share/hermes-flight-recorder-runtime/venv}"
PY="$VENV/bin/python"
PIP="$VENV/bin/pip"
RT="$VENV/bin/hermes-flight-recorder"
SC_HERMES_FLIGHT_RECORDER_HOME="${SC_HERMES_FLIGHT_RECORDER_HOME:-$HOME/.hermes-flight-recorder}"
HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
UNIT_DIR="$HOME/.config/systemd/user"

# All three unit pairs are managed (rendered) here and all three run continuously.
# reconcile runs once a minute, independent of capture, so it detects a stalled
# capture loop (capture:last_success_at frozen -> reconcile.capture_stale) and the
# other coverage/terminal/missed-cron gaps. It is network-free and read-only.
ALL_UNITS=(capture sync reconcile)
ACTIVE_TIMERS=(capture sync reconcile)

[ -x "$PY" ] || { echo "ERROR: runtime venv not found at $VENV" >&2; exit 1; }

echo "==> Pausing timers"
for s in "${ACTIVE_TIMERS[@]}"; do systemctl --user stop "hermes-flight-recorder-$s.timer" 2>/dev/null || true; done

# Guarantee the timers come back on ANY exit path (set -e abort, a failed verify,
# a broken pip install) so a partial deploy can never leave capture/sync stopped.
restart_active_timers() {
  for s in "${ACTIVE_TIMERS[@]}"; do
    systemctl --user start "hermes-flight-recorder-$s.timer" 2>/dev/null || true
  done
}
trap restart_active_timers EXIT

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

echo "==> Rendering systemd units from templates"
# Render every unit from its committed template, substituting the same paths the
# CLI is invoked with. This is what keeps ExecStart in lockstep with the CLI.
mkdir -p "$UNIT_DIR"
for tpl in "$REPO"/systemd/*.in; do
  dest="$UNIT_DIR/$(basename "${tpl%.in}")"
  sed -e "s|@RT@|$RT|g" \
      -e "s|@FR_HOME@|$SC_HERMES_FLIGHT_RECORDER_HOME|g" \
      -e "s|@HERMES_HOME@|$HERMES_HOME|g" \
      "$tpl" > "$dest"
  echo "  rendered $(basename "$dest")"
done
systemctl --user daemon-reload

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

echo "==> Verifying each unit's real ExecStart"
# oneshot `start` blocks and propagates ExecStart's exit code, so a stale/renamed
# flag (the --bridge-home failure) aborts the deploy here instead of crash-looping
# in production. capture and reconcile are network-free and must pass. sync can
# legitimately exit non-zero when the ingest endpoint is unreachable (an
# operational/config issue, not code drift), so it is verified leniently — a flag
# rename would still be caught by capture, which shares the same flag surface.
for s in capture reconcile; do
  systemctl --user start "hermes-flight-recorder-$s.service"
  echo "  ok: $s.service ran cleanly"
done
if systemctl --user start "hermes-flight-recorder-sync.service"; then
  echo "  ok: sync.service ran cleanly"
else
  echo "  WARN: sync.service exited non-zero — likely an unreachable ingest endpoint;" >&2
  echo "        check ingest_url in $SC_HERMES_FLIGHT_RECORDER_HOME/sync-config.json" >&2
fi

echo "==> Starting active timers"
for s in "${ACTIVE_TIMERS[@]}"; do
  systemctl --user enable "hermes-flight-recorder-$s.timer" >/dev/null 2>&1 || true
  systemctl --user start "hermes-flight-recorder-$s.timer"
done

echo "==> Asserting timers armed (finite next elapse)"
# OnCalendar (realtime) timers report their next fire in NextElapseUSecRealtime;
# the monotonic field is always 0 for them. A dead/un-armed timer leaves this
# empty — the exact regression (NextElapse never resolving) that caused the
# capture blackout, now caught at deploy time.
#
# One benign transient: a timer being enabled for the first time with
# Persistent=true fires its catch-up pass the instant it starts, and while that
# oneshot service runs, the timer's NextElapse reads empty. That is armed, not
# dead — it repopulates the moment the pass finishes. So retry briefly, and only
# fail if the next elapse is still unresolved after the in-flight pass settles.
for s in "${ACTIVE_TIMERS[@]}"; do
  ne=""
  for _ in 1 2 3 4 5; do
    ne=$(systemctl --user show "hermes-flight-recorder-$s.timer" -p NextElapseUSecRealtime --value)
    if [ -n "$ne" ] && [ "$ne" != "0" ]; then break; fi
    sleep 2  # let a first-enable catch-up pass finish, then re-read
  done
  if [ -z "$ne" ] || [ "$ne" = "0" ]; then
    echo "  FAIL: $s.timer has no scheduled next elapse (NextElapseUSecRealtime='$ne')" >&2
    exit 1
  fi
  echo "  ok: $s.timer armed (next: $ne)"
done

echo "==> Deployed: $("$RT" --version) from $REPO"
