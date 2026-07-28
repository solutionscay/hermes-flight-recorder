"""Reconcile install-horizon (issue #109).

A fresh install over a long-lived Hermes home must not emit
``reconcile.terminal_missing`` (or ``cron.run_missed``) findings for work that
started before the recorder existed. The horizon is the ``installed_at`` marker
stamped at install; absent the marker it is ``0.0`` (judge the whole store).

Exercised against the invocation terminal detector — the surviving
missing-terminal check. Session/subagent/cron-run terminal detection was
removed (#94/#95), so the original #109 pre-install burst it guarded against
can no longer come from those subjects; the horizon still gates invocations.
"""

from __future__ import annotations

from hermes_flight_recorder.collector._common import INSTALLED_AT_META_KEY, build_record
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile

HORIZON = 1_700_000_000.0  # a fixed "install" epoch
DAY = 86_400.0


def _outbox(tmp_path, *, installed_at: float | None = HORIZON) -> Outbox:
    ob = Outbox.open(tmp_path / "fr")
    ob.initialize()
    if installed_at is not None:
        ob.set_meta(INSTALLED_AT_META_KEY, repr(installed_at))
    return ob


def _started(ob, inv_id: str, occurred: float) -> None:
    """An invocation.started with no matching completed, occurring at ``occurred``."""
    rec = build_record(
        event_type="invocation.started",
        occurred_at=occurred,
        source="hook:test",
        capture_method="hook:test",
        runtime={"kind": "cli", "engine": "standard"},
        correlation_id=inv_id,
        invocation_id=inv_id,
        session_id=inv_id,
        payload={},
    )
    ob.append(rec)


def _flagged(ob) -> set[str]:
    return {
        e["payload"].get("subject_id")
        for e in ob.iter_events()
        if e["payload"]["event_type"] == "reconcile.terminal_missing"
    }


def test_pre_install_invocation_not_flagged_post_install_is(tmp_path):
    ob = _outbox(tmp_path)
    _started(ob, "OLD", HORIZON - 10 * DAY)  # started before install
    _started(ob, "NEW", HORIZON + 10)  # started after install

    reconcile(ob, tmp_path / "no-hermes", now=HORIZON + 100 * DAY, config=ReconcileConfig())

    flagged = _flagged(ob)
    assert "OLD" not in flagged  # started before install — suppressed
    assert "NEW" in flagged  # started after install, never completed — flagged
    ob.close()


def test_all_flagged_when_no_horizon(tmp_path):
    # No installed_at marker → horizon 0.0 → pre-#109 behavior (judge the whole
    # store). Both unpaired invocations are flagged.
    ob = _outbox(tmp_path, installed_at=None)
    _started(ob, "OLD", HORIZON - 10 * DAY)
    _started(ob, "NEW", HORIZON + 10)

    reconcile(ob, tmp_path / "no-hermes", now=HORIZON + 100 * DAY, config=ReconcileConfig())

    assert _flagged(ob) == {"OLD", "NEW"}
    ob.close()


def test_install_stamps_the_horizon(tmp_path):
    # The real `install` path stamps installed_at, so a reconcile afterward has
    # a live horizon with no manual meta write.
    from hermes_flight_recorder.collector import lifecycle

    hermes = tmp_path / "hermes"
    hermes.mkdir()
    (hermes / "config.yaml").write_text("terminal:\n  home_mode: auto\n")
    fr = lifecycle.install(None, str(hermes), log=lambda *a: None)
    ob = Outbox.open(fr)
    assert ob.get_meta(INSTALLED_AT_META_KEY) is not None
    ob.close()
