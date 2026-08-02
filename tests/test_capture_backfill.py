"""The ``install --no-backfill`` capture horizon (issue #111).

By default capture backfills the whole Hermes history. ``--no-backfill`` records
only activity at or after the install moment, so a fresh install over a
long-lived Hermes home does not ingest the entire past.
"""

from __future__ import annotations

from collections import defaultdict

import pytest

from helpers import MESSAGES_FULL, SESSIONS_FULL, _executions_db, iso, make_state_db

from hermes_flight_recorder.collector import lifecycle, run_pass
from hermes_flight_recorder.collector._common import (
    CAPTURE_BACKFILL_META_KEY,
    INSTALLED_AT_META_KEY,
    append_and_count,
    build_record,
)
from hermes_flight_recorder.collector.outbox import Outbox


@pytest.fixture(autouse=True)
def _clean_env(clean_env):
    """Every test here runs with the recorder env vars cleared."""


def _make_state(hermes, sessions, messages) -> None:
    """sessions: (sid, started) tuples; messages: (sid, role, ts, content)."""
    make_state_db(
        hermes,
        sessions=[
            (sid, "cli", None, "m", 0, 0, 0, 0, 0.0, started, None, None, "default", 1)
            for (sid, started) in sessions
        ],
        messages=[
            (i, sid, role, None, None, None, content, ts, None)
            for i, (sid, role, ts, content) in enumerate(messages, 1)
        ],
        sessions_columns=SESSIONS_FULL,
        messages_columns=MESSAGES_FULL,
    )


def _install(tmp_path, *, backfill: bool):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    (hermes / "config.yaml").write_text("terminal:\n  home_mode: auto\n")
    fr = lifecycle.install(None, str(hermes), backfill=backfill, log=lambda *a: None)
    return hermes, fr


def _counts(ob):
    sessions = sum(
        1 for e in ob.iter_events() if e["payload"]["event_type"] == "session.created"
    )
    messages = sum(1 for e in ob.iter_events() if "message_row_id" in e["payload"])
    return sessions, messages


def test_default_install_backfills_history(tmp_path):
    hermes, fr = _install(tmp_path, backfill=True)
    ob = Outbox.open(fr)
    horizon = float(ob.get_meta(INSTALLED_AT_META_KEY))
    _make_state(
        hermes,
        [("OLD", horizon - 1000), ("NEW", horizon + 1000)],
        [("OLD", "user", horizon - 1000, "old"), ("NEW", "user", horizon + 1000, "new")],
    )
    run_pass(ob, str(hermes))
    assert _counts(ob) == (2, 2)  # both historical and new captured
    assert ob.get_meta(CAPTURE_BACKFILL_META_KEY) is None  # flag not set
    ob.close()


def test_no_backfill_skips_history_keeps_new(tmp_path):
    hermes, fr = _install(tmp_path, backfill=False)
    ob = Outbox.open(fr)
    assert ob.get_meta(CAPTURE_BACKFILL_META_KEY) == "false"
    horizon = float(ob.get_meta(INSTALLED_AT_META_KEY))
    _make_state(
        hermes,
        [("OLD", horizon - 1000), ("NEW", horizon + 1000)],
        [("OLD", "user", horizon - 1000, "old"), ("NEW", "user", horizon + 1000, "new")],
    )
    run_pass(ob, str(hermes))
    assert _counts(ob) == (1, 1)  # only the post-install session and message
    types = {e["payload"]["event_type"] for e in ob.iter_events()}
    assert "session.created" in types
    ob.close()


def test_choice_persists_across_reopen(tmp_path):
    # The flag lives in the outbox, so a later run_pass (new process) honors it
    # without any CLI argument.
    hermes, fr = _install(tmp_path, backfill=False)
    ob = Outbox.open(fr)
    horizon = float(ob.get_meta(INSTALLED_AT_META_KEY))
    ob.close()

    _make_state(
        hermes,
        [("OLD", horizon - 1000)],
        [("OLD", "user", horizon - 1000, "old")],
    )
    reopened = Outbox.open(fr)  # fresh handle, as serve/run would open
    run_pass(reopened, str(hermes))
    assert _counts(reopened) == (0, 0)  # nothing historical, even after reopen
    reopened.close()


# --- append-path enforcement (issue #170) ----------------------------------
# The horizon is enforced once, in ``append_and_count``, keyed on the
# record's own ``capture_method`` — so a brand-new poll adapter that never
# heard of ``--no-backfill`` is covered without a per-row check.


def _poll_record(occurred_at: float, *, capture_method: str = "poll:new-source"):
    """A record shaped like one from a hypothetical future poll adapter."""
    return build_record(
        event_type="tool.call_completed",
        occurred_at=occurred_at,
        source="new-source",
        capture_method=capture_method,
        runtime={"kind": "cli", "engine": "standard"},
        correlation_id="corr",
        payload={},
    )


def _append(ob, record, dedup_key):
    counts: dict[str, int] = defaultdict(int)
    kept = append_and_count(ob, counts, record, dedup_key=dedup_key)
    return kept, dict(counts)


def test_new_poll_source_is_horizon_enforced_without_its_own_check(tmp_path):
    _hermes, fr = _install(tmp_path, backfill=False)
    ob = Outbox.open(fr)
    horizon = ob.capture_horizon
    assert horizon is not None

    kept, counts = _append(ob, _poll_record(horizon - 1000), "new-source:old")
    assert kept is False  # dropped: occurred before the horizon
    assert counts == {}  # a dropped record never inflates captured counts
    assert ob.count() == 0
    assert not ob.has_dedup_key("new-source:old")  # not even a dedup row

    kept, counts = _append(ob, _poll_record(horizon + 1000), "new-source:new")
    assert kept is True
    assert counts == {"tool.call_completed": 1}
    assert ob.count() == 1
    ob.close()


def test_boundary_equality_is_kept_exactly_as_occurred_before_kept_it(tmp_path):
    # occurred_before used a strict "<", so occurred_at == horizon was
    # captured; the append path preserves that boundary.
    _hermes, fr = _install(tmp_path, backfill=False)
    ob = Outbox.open(fr)
    horizon = ob.capture_horizon

    kept, counts = _append(ob, _poll_record(horizon), "new-source:boundary")
    assert kept is True
    assert counts == {"tool.call_completed": 1}
    ob.close()


def test_no_horizon_means_no_drops(tmp_path):
    _hermes, fr = _install(tmp_path, backfill=True)
    ob = Outbox.open(fr)
    assert ob.capture_horizon is None

    old = float(ob.get_meta(INSTALLED_AT_META_KEY)) - 10_000
    kept, counts = _append(ob, _poll_record(old), "new-source:backfilled")
    assert kept is True
    assert counts == {"tool.call_completed": 1}
    ob.close()


def test_missing_timestamp_sentinel_zero_is_kept(tmp_path):
    # Adapters stamp occurred_at 0.0 when the source time column is NULL;
    # occurred_before treated a missing timestamp as not-before and kept the
    # row, so the append path keeps the sentinel too.
    _hermes, fr = _install(tmp_path, backfill=False)
    ob = Outbox.open(fr)

    kept, counts = _append(ob, _poll_record(0.0), "new-source:no-timestamp")
    assert kept is True
    assert counts == {"tool.call_completed": 1}
    ob.close()


def test_non_poll_producers_are_never_horizon_dropped(tmp_path):
    # Hook, reconciler, and knowledge records describe the present even when
    # occurred_at is old (a standing gateway failure, a pre-install skill
    # mtime); --no-backfill has never suppressed them.
    _hermes, fr = _install(tmp_path, backfill=False)
    ob = Outbox.open(fr)
    horizon = ob.capture_horizon

    for method, key in (
        ("hook:agent:start", "hook:old"),
        ("derive:reconciler", "reconcile:old"),
        ("scan:knowledge_store", "knowledge:old"),
    ):
        kept, _counts = _append(
            ob, _poll_record(horizon - 1000, capture_method=method), key
        )
        assert kept is True, method
    assert ob.count() == 3
    ob.close()


def test_stale_ticker_heartbeat_survives_no_backfill(tmp_path):
    # The heartbeat snapshots the scheduler's *current* liveness; a heartbeat
    # already stale at install time carries a pre-install epoch and must
    # still be captured (it always was).
    hermes, fr = _install(tmp_path, backfill=False)
    ob = Outbox.open(fr)
    horizon = ob.capture_horizon
    cron = hermes / "cron"
    cron.mkdir()
    (cron / "ticker_heartbeat").write_text(str(horizon - 5000))

    run_pass(ob, str(hermes), on_source_error=lambda *_: None)  # no state.db
    types = [e["payload"]["event_type"] for e in ob.iter_events()]
    assert types.count("cron.ticker_heartbeat") == 1
    ob.close()


def test_cron_run_straddling_the_horizon_keeps_only_the_finished_event(tmp_path):
    # Deliberate semantics: each event is judged by its own occurred_at. A
    # run claimed before install but finished after it drops cron.run_claimed
    # (pre-horizon) and keeps cron.run_finished (post-horizon).
    hermes, fr = _install(tmp_path, backfill=False)
    ob = Outbox.open(fr)
    horizon = ob.capture_horizon
    cron = hermes / "cron"
    cron.mkdir()
    _executions_db(
        cron,
        [
            ("E-OLD", "job-1", "completed",
             iso(horizon - 900), iso(horizon - 900), iso(horizon - 800)),
            ("E-STRADDLE", "job-1", "completed",
             iso(horizon - 100), iso(horizon - 100), iso(horizon + 100)),
        ],
    )

    run_pass(ob, str(hermes), on_source_error=lambda *_: None)  # no state.db
    by_type = defaultdict(list)
    for e in ob.iter_events():
        by_type[e["payload"]["event_type"]].append(e["payload"].get("execution_id"))
    assert by_type["cron.run_claimed"] == []  # both claimed pre-horizon
    assert by_type["cron.run_finished"] == ["E-STRADDLE"]
    ob.close()
