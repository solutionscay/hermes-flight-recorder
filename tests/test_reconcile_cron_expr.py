"""Tests for cron-*expression* scheduling in the reconciler (schedule.kind='cron').

Covers the extracted 5-field parser (``_parse_cron`` / ``_parse_cron_field``)
and instant expansion (``_cron_instants``), plus the reconciler's
``_cron_missed`` diff: wildcards vs
restricted fields, ranges, steps, lists, and the standard cron day rule
(day-of-month OR day-of-week when both are restricted; AND -- which reduces
to whichever one is restricted -- otherwise). Also verifies the Sunday=0
day-of-week convention and exercises the full missed-cron pipeline through
``reconcile()`` for a job whose expression should have fired at specific
minutes with no matching execution.

A dedicated test (see ``test_cron_expression_matching_is_host_tz_independent``)
guards a correctness property: ``_cron_instants`` resolves each candidate
epoch in a FIXED zone -- the job's own UTC offset (carried by its ISO
timestamps), or UTC as a deterministic fallback -- never the host process's
ambient timezone. This keeps the cron-expression diff identical across
otherwise-identical environments, matching the reconciler's "deterministic
diff" premise ("a second run over the same durable state appends nothing
new").
"""

from __future__ import annotations

import contextlib
import datetime
import json
import os
import time

from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.cron_schedule import (
    _cron_instants,
    _dow,
    _parse_cron,
    _parse_cron_field,
)
from hermes_flight_recorder.collector.reconcile import (
    ReconcileConfig,
    _cron_missed,
    reconcile,
)

# A fixed epoch anchor and a US-Central-like offset, matching tests/test_reconcile.py's
# style (values here are independent of that module -- nothing is imported from it).
B = 1784415000.0
TZ = datetime.timezone(datetime.timedelta(hours=-5))


def iso(epoch: float) -> str:
    return datetime.datetime.fromtimestamp(epoch, TZ).isoformat()


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def findings(outbox, event_type):
    return [
        e for e in outbox.iter_events()
        if e["payload"]["event_type"] == event_type and e["source"] == "reconciler"
    ]


def utc_epoch(y: int, mo: int, d: int, h: int = 0, mi: int = 0, s: int = 0) -> float:
    """Build an epoch from a UTC calendar date/time, independent of host tz."""
    return datetime.datetime(y, mo, d, h, mi, s, tzinfo=datetime.timezone.utc).timestamp()


def utc_hour_minute(epoch: float) -> tuple[int, int]:
    d = datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc)
    return d.hour, d.minute


@contextlib.contextmanager
def host_tz(name: str):
    """Force the process's local timezone for the duration of the block.

    Pinning the host timezone keeps the hour/day-sensitive test environment
    explicit and lets the independence test verify that expansion ignores the
    process's ambient timezone.
    """
    old = os.environ.get("TZ")
    os.environ["TZ"] = name
    time.tzset()
    try:
        yield
    finally:
        if old is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old
        time.tzset()


def _jobs_json(cron_dir, jobs) -> None:
    (cron_dir / "jobs.json").write_text(json.dumps({"jobs": jobs}))


def _cron_job(job_id: str, *, expression: str, created: float) -> dict:
    return {
        "id": job_id,
        "enabled": True,
        "state": "scheduled",
        "created_at": iso(created),
        "schedule": {"kind": "cron", "expression": expression},
        "repeat": {"times": None, "completed": 0},
    }


# --- the 5-field parser --------------------------------------------------
def test_parse_cron_field_wildcard_range_step_list():
    # '*' spans the field's full bound -- an unrestricted field.
    assert _parse_cron_field("*", 0, 59) == set(range(0, 60))
    # a plain inclusive range
    assert _parse_cron_field("9-17", 0, 23) == set(range(9, 18))
    # a step off the implicit wildcard
    assert _parse_cron_field("*/2", 0, 59) == set(range(0, 60, 2))
    # a step off an explicit range
    assert _parse_cron_field("0-30/10", 0, 59) == {0, 10, 20, 30}
    # a list of discrete values
    assert _parse_cron_field("1,15,30", 0, 59) == {1, 15, 30}

    fields = _parse_cron("0,15,30,45 9-17 * * *")
    assert fields is not None
    minute, hour, dom, month, dow = fields
    assert minute == {0, 15, 30, 45}
    assert hour == set(range(9, 18))
    assert dom == set(range(1, 32))  # '*' -> full bound, i.e. unrestricted
    assert month == set(range(1, 13))
    assert dow == set(range(0, 7))


def test_parse_cron_rejects_malformed_expressions():
    assert _parse_cron("* * * *") is None       # only 4 fields
    assert _parse_cron(None) is None
    assert _parse_cron(123) is None             # not a string at all
    assert _parse_cron("60 * * * *") is None    # minute out of bounds (0-59)
    assert _parse_cron("* 24 * * *") is None    # hour out of bounds (0-23)
    assert _parse_cron("* * 0 * *") is None     # day-of-month below its lower bound (1)
    assert _parse_cron("* * * * 7") is None     # day-of-week above its upper bound (0-6)
    assert _parse_cron("10-5 * * * *") is None  # inverted range (start > end)
    assert _parse_cron("*/0 * * * *") is None   # a zero step is rejected


# --- day-of-week convention ----------------------------------------------
def test_cron_dow_sunday_is_zero():
    # Python's weekday() is Monday=0; cron convention is Sunday=0. 2026-01-04
    # is a Sunday, 2026-01-05 a Monday, 2026-01-10 a Saturday.
    assert _dow(datetime.datetime(2026, 1, 4)) == 0
    assert _dow(datetime.datetime(2026, 1, 5)) == 1
    assert _dow(datetime.datetime(2026, 1, 10)) == 6

    with host_tz("UTC"):
        fields = _parse_cron("0 0 * * 0")  # midnight, Sundays only (dom stays '*')
        sunday = utc_epoch(2026, 1, 4)
        monday = utc_epoch(2026, 1, 5)
        out = _cron_instants(fields, sunday - 60.0, monday + 60.0)
        assert sunday in out
        assert monday not in out


# --- standard cron day rule (dom/dow interaction) -------------------------
def test_cron_day_rule_both_restricted_matches_either(tmp_path):
    """When BOTH day-of-month and day-of-week are restricted, either match
    satisfies the day (2026-01-15 is a Thursday; 2026-01-11 is a Sunday)."""
    with host_tz("UTC"):
        fields = _parse_cron("0 0 15 * 0")  # day-of-month=15 OR Sunday
        out = set(_cron_instants(fields, utc_epoch(2026, 1, 10), utc_epoch(2026, 1, 16)))
        assert utc_epoch(2026, 1, 11) in out  # Sunday, not day 15 -> fires on dow
        assert utc_epoch(2026, 1, 15) in out  # day 15, not a Sunday -> fires on dom
        assert utc_epoch(2026, 1, 12) not in out  # Monday, day 12 -> matches neither
        assert utc_epoch(2026, 1, 10) not in out  # Saturday, day 10 -> matches neither


def test_cron_day_rule_single_restricted_field_governs_alone(tmp_path):
    """When only ONE of day-of-month/day-of-week is restricted, the other
    ('*') is not a restriction at all -- only the restricted field governs.
    Same calendar window as the "either" test above, contrast the result."""
    with host_tz("UTC"):
        fields = _parse_cron("0 0 15 * *")  # day-of-month=15 only; dow unrestricted
        out = set(_cron_instants(fields, utc_epoch(2026, 1, 10), utc_epoch(2026, 1, 16)))
        assert utc_epoch(2026, 1, 15) in out
        # Unlike the "both restricted" case, a Sunday that isn't day 15 does NOT fire.
        assert utc_epoch(2026, 1, 11) not in out
        assert utc_epoch(2026, 1, 12) not in out


# --- ranges and steps ------------------------------------------------------
def test_cron_instants_hour_range_9_to_17():
    with host_tz("UTC"):
        fields = _parse_cron("0 9-17 * * *")  # on the hour, business hours
        out = _cron_instants(fields, utc_epoch(2026, 1, 8), utc_epoch(2026, 1, 8, 23, 59))
        hours = sorted(utc_hour_minute(t)[0] for t in out)
        assert hours == list(range(9, 18))
        assert utc_epoch(2026, 1, 8, 8) not in out   # 08:00 excluded
        assert utc_epoch(2026, 1, 8, 18) not in out  # 18:00 excluded


def test_cron_instants_step_every_2_minutes():
    with host_tz("UTC"):
        fields = _parse_cron("*/2 * * * *")
        out = _cron_instants(fields, utc_epoch(2026, 1, 8, 10, 0), utc_epoch(2026, 1, 8, 10, 9))
        minutes = sorted(utc_hour_minute(t)[1] for t in out)
        assert minutes == [0, 2, 4, 6, 8]


def test_cron_missed_step_range_field_collapses(tmp_path):
    """'0-30/10 * * * *' -> minutes {0,10,20,30} each hour; a clean run of
    misses (no executions at all) collapses to one row."""
    with host_tz("UTC"):
        cfg = ReconcileConfig(cron_lookback=2 * 3600.0, cron_match_slack=30.0)
        created = utc_epoch(2026, 1, 8, 9, 5, 0)
        now = utc_epoch(2026, 1, 8, 10, 0, 0)
        runs = _cron_missed("0-30/10 * * * *", [], created, now, cfg)

    assert len(runs) == 1
    first_at, count, is_tail = runs[0]
    assert first_at == utc_epoch(2026, 1, 8, 9, 10, 0)  # first slot after creation
    assert count == 3  # 9:10, 9:20, 9:30; 10:00 is inside its grace window
    assert is_tail is True


def test_cron_fire_is_not_missed_until_its_slack_expires():
    created = utc_epoch(2026, 1, 8, 10, 0, 0)
    cfg = ReconcileConfig(cron_match_slack=30.0)

    within_slack = _cron_missed(
        "1 * * * *", [], created,
        utc_epoch(2026, 1, 8, 10, 1, 20), cfg,
        tz=datetime.timezone.utc,
    )
    after_slack = _cron_missed(
        "1 * * * *", [], created,
        utc_epoch(2026, 1, 8, 10, 1, 31), cfg,
        tz=datetime.timezone.utc,
    )

    assert within_slack == []
    assert after_slack == [(utc_epoch(2026, 1, 8, 10, 1, 0), 1, True)]


# --- end-to-end: list of specific minutes, no execution at all -----------
def test_cron_missed_list_minutes_no_execution_collapses_via_reconcile(tmp_path):
    """A job on '0,15,30,45 * * * *' with NO executions in its window
    surfaces as one collapsed cron.run_missed with the correct
    expected_fire_at (the first missed slot after creation)."""
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    created = utc_epoch(2026, 1, 8, 10, 5, 0)
    now = utc_epoch(2026, 1, 8, 13, 0, 0)
    expected_first = utc_epoch(2026, 1, 8, 10, 15, 0)
    _jobs_json(cron, [_cron_job("cE1", expression="0,15,30,45 * * * *", created=created)])
    (cron / "ticker_heartbeat").write_text(str(now))  # fresh -- scheduler is alive
    ob = new_outbox(tmp_path)

    cfg = ReconcileConfig(cron_lookback=24 * 3600.0, cron_match_slack=30.0)
    with host_tz("UTC"):
        reconcile(ob, hh, now=now, config=cfg)

    missed = findings(ob, "cron.run_missed")
    assert len(missed) == 1
    m = missed[0]
    assert m["payload"]["expected_fire_at"] == expected_first
    # 13:00 is still inside its grace window.
    assert m["payload"]["missed_count"] == 11  # 10:15 through 12:45
    assert m["payload"]["catch_up"] is True
    assert m["payload"]["schedule_kind"] == "cron"
    assert m["correlation_id"] == "cE1"

    # A second pass over the same durable state must add nothing new.
    with host_tz("UTC"):
        second = reconcile(ob, hh, now=now, config=cfg)
    assert second == {}


# --- the tz-mismatch concern ----------------------------------------------
def test_cron_expression_matching_is_host_tz_independent(tmp_path):
    """Regression guard: reconciling the exact same durable snapshot (same
    jobs.json, same -- empty -- executions, same `now`) must give the same
    missed-fire diagnosis no matter what timezone the reconciler process
    happens to run in. Every timestamp in this codebase is resolved to an
    absolute epoch once (``to_epoch``, which reads a timestamp's own embedded
    offset); an hour-restricted cron expression is no different.

    ``_cron_instants`` resolves each candidate instant in a FIXED zone (the
    job's own offset, or UTC when a bare epoch is passed with no job), never
    the host process's ambient local timezone. Flipping the host tz by 5
    hours (holding the job, the epoch window, and `now` fixed) must not shift
    a single matched fire instant.
    """
    expr = "0 9 * * *"  # meant to fire once a day at 09:00
    created = utc_epoch(2026, 1, 1)
    now = utc_epoch(2026, 1, 8, 12, 0, 0)
    cfg = ReconcileConfig(cron_lookback=10 * 24 * 3600.0, cron_match_slack=30.0)

    with host_tz("UTC"):
        runs_utc = _cron_missed(expr, [], created, now, cfg)
    with host_tz("Etc/GMT+5"):  # a fixed UTC-5, no DST -- a different, equally valid host tz
        runs_fixed_minus5 = _cron_missed(expr, [], created, now, cfg)

    assert runs_utc == runs_fixed_minus5
