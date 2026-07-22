"""Tests for capture-liveness detection in the reconciler
(``_detect_capture_stale``).

The Flight Recorder watching its OWN capture loop. ``run_pass`` stamps
``capture:last_success_at`` every completed pass; the reconciler runs on its
own realtime timer, so a frozen heartbeat while reconcile keeps running is the
silent-outage signal (a dead capture timer reported active/success for ~3h20m).

Self-contained: builds its own outbox and sets the heartbeat meta directly.
Every ``reconcile`` call passes an explicit ``now`` and a ``ReconcileConfig``
with a small explicit window, so nothing here depends on wall-clock. No Hermes
home is needed — capture liveness is a pure outbox-meta signal.
"""

from __future__ import annotations

from collections import Counter

from hermes_flight_recorder.collector import CAPTURE_HEARTBEAT_KEY
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile
from hermes_flight_recorder.envelope import validate

B = 1784415000.0


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def stale_findings(outbox):
    return [
        e
        for e in outbox.iter_events()
        if e["payload"]["event_type"] == "reconcile.capture_stale"
    ]


def dedup_keys(outbox):
    return [r[0] for r in outbox._conn.execute("SELECT dedup_key FROM events").fetchall()]


def test_stale_heartbeat_emits_capture_stale(tmp_path):
    ob = new_outbox(tmp_path)
    ob.set_meta(CAPTURE_HEARTBEAT_KEY, repr(B))  # frozen at B
    cfg = ReconcileConfig(capture_stale_after=300.0)

    counts = reconcile(ob, tmp_path / "no-hermes", now=B + 600, config=cfg)

    assert counts.get("reconcile.capture_stale") == 1
    found = stale_findings(ob)
    assert len(found) == 1
    pl = found[0]["payload"]
    assert pl["last_success_at"] == B
    assert pl["staleness_seconds"] == 600.0
    assert pl["threshold_seconds"] == 300.0
    assert found[0]["correlation_id"] == ob.installation_id
    for e in ob.iter_events():
        validate(e)


def test_fresh_heartbeat_emits_nothing(tmp_path):
    ob = new_outbox(tmp_path)
    ob.set_meta(CAPTURE_HEARTBEAT_KEY, repr(B + 250))  # only 50s old at now
    cfg = ReconcileConfig(capture_stale_after=300.0)

    counts = reconcile(ob, tmp_path / "no-hermes", now=B + 300, config=cfg)

    assert counts.get("reconcile.capture_stale", 0) == 0
    assert stale_findings(ob) == []


def test_boundary_is_not_stale(tmp_path):
    """Exactly at the threshold is still alive (``<=`` window), mirroring the
    ticker rule — only strictly older fires."""
    ob = new_outbox(tmp_path)
    ob.set_meta(CAPTURE_HEARTBEAT_KEY, repr(B))
    cfg = ReconcileConfig(capture_stale_after=300.0)

    counts = reconcile(ob, tmp_path / "no-hermes", now=B + 300, config=cfg)

    assert counts.get("reconcile.capture_stale", 0) == 0


def test_absent_heartbeat_emits_nothing(tmp_path):
    """No baseline yet (fresh install, capture never ran) raises no alert."""
    ob = new_outbox(tmp_path)  # no heartbeat meta set
    cfg = ReconcileConfig(capture_stale_after=300.0)

    counts = reconcile(ob, tmp_path / "no-hermes", now=B + 10_000, config=cfg)

    assert counts.get("reconcile.capture_stale", 0) == 0
    assert stale_findings(ob) == []


def test_malformed_heartbeat_tolerated(tmp_path):
    ob = new_outbox(tmp_path)
    ob.set_meta(CAPTURE_HEARTBEAT_KEY, "not-a-number")
    cfg = ReconcileConfig(capture_stale_after=300.0)

    counts = reconcile(ob, tmp_path / "no-hermes", now=B + 10_000, config=cfg)  # no crash

    assert counts.get("reconcile.capture_stale", 0) == 0
    assert stale_findings(ob) == []


def test_dedup_key_is_deterministic_and_idempotent(tmp_path):
    """A dead capture keeps the heartbeat frozen, so repeated reconcile passes
    over the same value alert once, not once per minute."""
    ob = new_outbox(tmp_path)
    ob.set_meta(CAPTURE_HEARTBEAT_KEY, repr(B))
    cfg = ReconcileConfig(capture_stale_after=300.0)

    first = reconcile(ob, tmp_path / "no-hermes", now=B + 600, config=cfg)
    assert first.get("reconcile.capture_stale") == 1

    expected_key = f"reconcile:capture_stale:{int(B)}"
    assert dedup_keys(ob).count(expected_key) == 1

    n = ob.count()
    # A later pass, heartbeat still frozen: no new row, even at a greater now.
    second = reconcile(ob, tmp_path / "no-hermes", now=B + 900, config=cfg)
    assert ob.count() == n
    assert second.get("reconcile.capture_stale", 0) == 0
    assert len(stale_findings(ob)) == 1


def test_recovered_then_restalled_alerts_again(tmp_path):
    """After capture recovers (heartbeat advances) and stalls again at a NEW
    value, a fresh alert fires — the dedup keys off the frozen value."""
    ob = new_outbox(tmp_path)
    cfg = ReconcileConfig(capture_stale_after=300.0)

    ob.set_meta(CAPTURE_HEARTBEAT_KEY, repr(B))
    reconcile(ob, tmp_path / "no-hermes", now=B + 600, config=cfg)

    # Capture recovered and advanced the heartbeat, then died again at B+5000.
    ob.set_meta(CAPTURE_HEARTBEAT_KEY, repr(B + 5000))
    reconcile(ob, tmp_path / "no-hermes", now=B + 6000, config=cfg)

    assert len(stale_findings(ob)) == 2
    keys = set(dedup_keys(ob))
    assert f"reconcile:capture_stale:{int(B)}" in keys
    assert f"reconcile:capture_stale:{int(B + 5000)}" in keys
