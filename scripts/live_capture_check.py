#!/usr/bin/env python3
"""Live capture check — the Phase 0 enrichments against a real Hermes home.

Unlike ``poc_exit_gate.py`` (a deterministic gate over a synthetic home), this
runs the whole Flight Recorder pipeline against the **real** Hermes home on this host
and proves the Phase 0 envelope enrichments hold against real data:

- #16 ``runtime.home_mode`` — every Hermes-runtime poll event carries the
  install's ``terminal.home_mode`` policy, matching ``config.yaml``.
- #14 ``payload.surface`` — every ``session.created`` / ``subagent.child_spawned``
  records the originating surface (the row's ``sessions.source``).
- #15 gateway ``channels`` + ``gateway_id`` — a ``gateway:startup`` (seeded with
  the host's *real* connected platforms) enriches the runtime stamp.
- #13 ``runtime.gateway_start_failed`` — the reconciler raises **no** finding
  against the healthy running gateway (no false positive), and **does** fire
  against a synthetic ``startup_failed`` home (the detector works).

It is strictly **read-only** against the Hermes home (asserted byte-for-byte)
and writes only to a throwaway outbox in a temp dir. Safe to run any time.

Usage:  python scripts/live_capture_check.py [--hermes-home PATH] [-v]
Exit:   0 if every check passes, 1 otherwise.
"""

from __future__ import annotations

import json
import sys
from functools import partial
from pathlib import Path

# Runnable standalone and spec-loadable: put the sibling _gate module on the path.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import time

from _gate import run_gate
from hermes_flight_recorder import observe
from hermes_flight_recorder.collector import CAPTURE_HEARTBEAT_KEY, run_pass, state_db
from hermes_flight_recorder.collector.reconcile import ReconcileConfig
from hermes_flight_recorder.collector._common import (
    executions_db_path,
    gateway_state_path,
    load_json_dict,
    open_sqlite_read_only,
    read_home_mode,
    resolve_hermes_home,
    state_db_path,
)
from hermes_flight_recorder.collector.hook import SPOOL_FILENAME, drain as drain_hook
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import reconcile
from hermes_flight_recorder.envelope import serialize, validate

VERBOSE = "-v" in sys.argv[1:]


def _hermes_home() -> Path:
    for i, a in enumerate(sys.argv):
        if a == "--hermes-home" and i + 1 < len(sys.argv):
            return Path(sys.argv[i + 1]).expanduser()
    return resolve_hermes_home(None)


def _new_outbox(tmp: Path) -> Outbox:
    ob = Outbox.open(tmp / "flight-recorder")
    ob.initialize()
    return ob


def _log(msg: str) -> None:
    if VERBOSE:
        print(f"      {msg}")


def _collect_events(
    home: Path,
    tmp: Path,
    *,
    include_reconcile: bool = True,
) -> tuple[Outbox, list[dict]]:
    """Run the poll pipeline once and return its outbox and events."""
    ob = _new_outbox(tmp)
    try:
        # The real `run` pipeline; a missing durable store is fine here.
        run_pass(ob, home, on_source_error=lambda label, exc: None)
        if include_reconcile:
            reconcile(ob, home)
        return ob, list(ob.iter_events())
    except Exception:
        ob.close()
        raise


# --- checks ---------------------------------------------------------------
def check_read_only(home: Path, tmp: Path) -> list[str]:
    """The poll never mutates the Hermes home."""
    fails: list[str] = []
    targets = [state_db_path(home), executions_db_path(home), home / "config.yaml"]
    before = {p: p.read_bytes() for p in targets if p.exists()}
    ob, _ = _collect_events(home, tmp)
    ob.close()
    for p, data in before.items():
        if p.read_bytes() != data:
            fails.append(f"read-only: {p} changed during poll/reconcile")
    _log(f"read-only: {len(before)} durable file(s) unchanged")
    return fails


def check_home_mode(home: Path, tmp: Path) -> list[str]:
    """#16 — every poll event carries the live home_mode."""
    fails: list[str] = []
    expected = read_home_mode(home)
    ob, events = _collect_events(home, tmp, include_reconcile=False)
    ob.close()
    if not events:
        fails.append("home_mode: no events polled from the live home")
        return fails
    bad = [e for e in events if e["runtime"].get("home_mode") != expected]
    if bad:
        fails.append(
            f"home_mode: {len(bad)}/{len(events)} events missing/!= {expected!r} "
            f"(e.g. {bad[0]['payload']['event_type']} -> {bad[0]['runtime'].get('home_mode')!r})"
        )
    _log(f"home_mode: {len(events)} event(s) all carry home_mode={expected!r}")
    return fails


def check_surface(home: Path, tmp: Path) -> list[str]:
    """#14 — session events carry a surface matching sessions.source."""
    fails: list[str] = []
    if not state_db_path(home).exists():
        _log("surface: no state.db, skipped")
        return fails
    conn = open_sqlite_read_only(state_db_path(home))
    try:
        source_of = {
            r["id"]: (r["source"] or "unknown")
            for r in conn.execute("SELECT id, source FROM sessions")
        }
    finally:
        conn.close()

    ob = _new_outbox(tmp)
    state_db.poll(ob, home)
    session_events = [
        e for e in ob.iter_events()
        if e["payload"]["event_type"] in ("session.created", "subagent.child_spawned")
    ]
    ob.close()
    if not session_events:
        _log("surface: no session rows in the live home, skipped")
        return fails
    dist: dict[str, int] = {}
    for e in session_events:
        surf = e["payload"].get("surface")
        dist[surf] = dist.get(surf, 0) + 1
        expected = source_of.get(e.get("session_id"))
        if surf != expected:
            fails.append(
                f"surface: session {e.get('session_id')} surface={surf!r} != source {expected!r}"
            )
    if any(s is None for s in dist):
        fails.append("surface: at least one session.created has no surface")
    _log(f"surface: {len(session_events)} session event(s), distribution {dist}")
    return fails


def check_gateway_channels(home: Path, tmp: Path) -> list[str]:
    """#15 — a gateway:startup with the host's real platforms enriches the stamp."""
    fails: list[str] = []
    platforms = list((load_json_dict(gateway_state_path(home)).get("platforms") or {}).keys())
    if not platforms:
        platforms = ["discord"]  # a representative channel, if the host has none live

    flight_recorder_home = tmp / "gw"
    flight_recorder_home.mkdir(parents=True, exist_ok=True)
    line = {"event_type": "gateway:startup", "context": {"platforms": platforms}, "captured_at": 1_700_000_000.0}
    (flight_recorder_home / SPOOL_FILENAME).write_text(json.dumps(line) + "\n")
    ob = Outbox.open(flight_recorder_home)
    ob.initialize()
    drain_hook(ob)
    rec = next(
        (e for e in ob.iter_events() if e["payload"]["event_type"] == "runtime.gateway_started"), None
    )
    ob.close()
    if rec is None:
        fails.append("channels: gateway:startup did not map to runtime.gateway_started")
        return fails
    if rec["runtime"].get("channels") != platforms:
        fails.append(f"channels: runtime.channels {rec['runtime'].get('channels')} != {platforms}")
    gid = rec["runtime"].get("gateway_id", "")
    if not (gid.startswith("gw-") and len(gid) == 19):
        fails.append(f"channels: gateway_id {gid!r} is not a gw-<16hex> id")
    blob = serialize(rec)
    if "token" in blob.lower():
        fails.append("channels: the word 'token' leaked into a gateway record")
    _log(f"channels: gateway_started carries channels={platforms}, gateway_id={gid}")
    return fails


def check_gateway_start_failed(home: Path, tmp: Path) -> list[str]:
    """#13 — no false positive live; the detector fires on a synthetic failure."""
    fails: list[str] = []

    # (a) live: a healthy running gateway must raise nothing.
    ob = _new_outbox(tmp / "live")
    reconcile(ob, home)
    live = [e for e in ob.iter_events() if e["payload"]["event_type"] == "runtime.gateway_start_failed"]
    ob.close()
    if live:
        fails.append(
            f"gateway_start_failed: {len(live)} false positive(s) against the live home "
            f"(reasons {[e['payload'].get('reason_class') for e in live]})"
        )
    _log(f"gateway_start_failed: live findings={len(live)} (expected 0)")

    # (b) synthetic: a startup_failed home must raise exactly one finding.
    synth = tmp / "synth"
    synth.mkdir(parents=True, exist_ok=True)
    (synth / "gateway_state.json").write_text(json.dumps({
        "gateway_state": "startup_failed",
        "exit_reason": "telegram: dm_policy open is not allowed",
        "updated_at": "2026-07-19T21:29:01.661893+00:00",
    }))
    ob = Outbox.open(synth / "flight-recorder")
    ob.initialize()
    reconcile(ob, synth)
    found = [e for e in ob.iter_events() if e["payload"]["event_type"] == "runtime.gateway_start_failed"]
    leaked = any("dm_policy" in serialize(e) for e in found)
    ob.close()
    if len(found) != 1:
        fails.append(f"gateway_start_failed: synthetic startup_failed -> {len(found)} findings, want 1")
    elif found[0]["payload"].get("reason_class") != "policy_open":
        fails.append(f"gateway_start_failed: reason_class {found[0]['payload'].get('reason_class')!r} != 'policy_open'")
    if leaked:
        fails.append("gateway_start_failed: raw exit_reason leaked into plaintext")
    _log(f"gateway_start_failed: synthetic findings={len(found)} (expected 1), reason ok, no leak")
    return fails


def check_envelope_and_observe(home: Path, tmp: Path) -> list[str]:
    """Every emitted event validates; observe --report runs against the stream."""
    fails: list[str] = []
    ob, events = _collect_events(home, tmp)
    n = 0
    for e in events:
        try:
            validate(e)
        except Exception as exc:  # noqa: BLE001 — surface any invalid record
            fails.append(f"envelope: invalid record {e['payload'].get('event_type')}: {exc}")
        n += 1
    try:
        lines, code = observe.render_report(observe.load(ob))
        _log(f"observe: report rendered {len(lines)} line(s), exit {code}")
    except Exception as exc:  # noqa: BLE001
        fails.append(f"observe: render_report raised {exc}")
    ob.close()
    _log(f"envelope: {n} live event(s) all validate")
    return fails


def check_capture_heartbeat(home: Path, tmp: Path) -> list[str]:
    """The capture heartbeat advances on a real pass; the stale detector fires
    only when it freezes.

    ``run_pass`` stamps ``capture:last_success_at`` every completed pass, and
    the reconciler's ``reconcile.capture_stale`` is the instant-alert for a
    stalled capture loop — the class of failure behind the 3h20m silent
    blackout. Proven here against the live pipeline: a brand-new outbox has no
    heartbeat and raises no alert; a real pass stamps a recent one; a fresh
    heartbeat raises nothing; a frozen heartbeat raises exactly one finding.
    """
    fails: list[str] = []
    cfg = ReconcileConfig()
    ob = _new_outbox(tmp / "heartbeat")
    try:
        def stale_findings() -> list[dict]:
            return [
                e for e in ob.iter_events()
                if e["payload"]["event_type"] == "reconcile.capture_stale"
            ]

        # (a) No baseline yet: absent heartbeat raises no false alert.
        if ob.get_meta(CAPTURE_HEARTBEAT_KEY) is not None:
            fails.append("heartbeat: a fresh outbox already carries a heartbeat")
        reconcile(ob, home, config=cfg)
        if stale_findings():
            fails.append("heartbeat: stale finding raised with no heartbeat baseline")

        # (b) A real capture pass against the live home stamps a recent heartbeat.
        run_pass(ob, home, on_source_error=lambda label, exc: None)
        raw = ob.get_meta(CAPTURE_HEARTBEAT_KEY)
        if raw is None:
            fails.append("heartbeat: run_pass did not stamp capture:last_success_at")
            return fails
        stamped = float(raw)
        age = time.time() - stamped
        if not (-5.0 <= age <= 120.0):  # tolerate small clock slop, generous ceiling
            fails.append(f"heartbeat: stamp is not recent (age {age:.1f}s)")

        # (c) A fresh heartbeat raises nothing.
        reconcile(ob, home, config=cfg)
        if stale_findings():
            fails.append(f"heartbeat: {len(stale_findings())} stale finding(s) against a fresh heartbeat")

        # (d) A frozen heartbeat, well past the window, raises exactly one.
        ob.set_meta(CAPTURE_HEARTBEAT_KEY, repr(stamped - cfg.capture_stale_after - 3600.0))
        reconcile(ob, home, config=cfg)
        found = stale_findings()
        if len(found) != 1:
            fails.append(f"heartbeat: frozen heartbeat -> {len(found)} stale finding(s), want 1")
        else:
            validate(found[0])
        _log(f"heartbeat: stamped {age:.1f}s ago; fresh raised 0, frozen raised 1")
    finally:
        ob.close()
    return fails


CHECKS = [
    ("read-only against the Hermes home", check_read_only),
    ("#16 home_mode on every poll event", check_home_mode),
    ("#14 surface on session events", check_surface),
    ("#15 gateway channels + gateway_id", check_gateway_channels),
    ("#13 gateway start-failure detection", check_gateway_start_failed),
    ("capture heartbeat + stale-capture alert", check_capture_heartbeat),
    ("envelope validity + observe report", check_envelope_and_observe),
]


def main() -> int:
    home = _hermes_home()
    if not home.exists():
        print(f"FAIL — Hermes home not found at {home}")
        return 1
    return run_gate(
        [
            "Live capture check — Phase 0 enrichments vs a real Hermes home",
            f"Hermes home: {home}",
        ],
        [(name, partial(fn, home)) for name, fn in CHECKS],
        passed="CHECK PASSED — all Phase 0 enrichments hold against the live Hermes home",
        failed="CHECK FAILED",
        width=62,
        catch=True,
    )


if __name__ == "__main__":
    raise SystemExit(main())
