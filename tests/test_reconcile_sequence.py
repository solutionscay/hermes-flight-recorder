"""Focused tests for sequence-gap detection (_detect_sequence_gaps), the
reconciler's proof that the append path lost a capture.

Self-contained: no imports from tests/test_reconcile.py. Mirrors its style
(a fixed epoch anchor, iso() with a fixed tz offset, new_outbox(tmp_path))
but defines everything locally.

A gap is simulated the same way the append path could actually lose a row:
by deleting from the outbox's own ``events`` table via ``ob._conn`` after
appending a contiguous run of events. No wall-clock, no network.
"""

from __future__ import annotations

import datetime

from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile
from hermes_flight_recorder.envelope import validate

# A fixed epoch anchor and a fixed tz offset, like the real cron store.
B = 1784415000.0
TZ = datetime.timezone(datetime.timedelta(hours=-5))

# A hermes_home that is never created: sequence-gap detection needs no
# durable store at all, and a nonexistent path keeps the other three
# detectors (coverage-gap, missing-terminal, missed-cron) silent since each
# guards on `path.exists()` before doing anything.
NO_HERMES = "no-hermes-home-does-not-exist"


def iso(epoch: float) -> str:
    return datetime.datetime.fromtimestamp(epoch, TZ).isoformat()


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def append_event(ob, event_type, **over):
    """Append a minimal valid producer event straight to the outbox.

    Each call consumes the next producer_sequence, giving a contiguous run
    of sequence numbers to later punch holes in.
    """
    rec = build_record(
        event_type=event_type,
        occurred_at=over.pop("occurred_at", B),
        source=over.pop("source", "hook:test"),
        capture_method=over.pop("capture_method", "hook:test"),
        runtime={"kind": "cli", "engine": "standard"},
        correlation_id=over.pop("correlation_id", "corr"),
        payload=over.pop("payload", {}),
        **over,
    )
    return ob.append(rec)


def append_n(ob, n: int) -> None:
    for _ in range(n):
        append_event(ob, "session.created")


def delete_sequence(ob, seq: int) -> None:
    """Simulate a dropped capture: remove one row by producer_sequence."""
    ob._conn.execute("DELETE FROM events WHERE producer_sequence=?", (seq,))


def gap_findings(ob) -> list[dict]:
    """All reconcile.gap_detected rows with gap_kind='sequence'."""
    return [
        e
        for e in ob.iter_events()
        if e["payload"]["event_type"] == "reconcile.gap_detected"
        and e["payload"]["gap_kind"] == "sequence"
    ]


def run_reconcile(ob, tmp_path, *, now=B, config=None):
    return reconcile(ob, tmp_path / NO_HERMES, now=now, config=config or ReconcileConfig())


# --- single hole ----------------------------------------------------------
def test_single_dropped_sequence_produces_one_finding(tmp_path):
    ob = new_outbox(tmp_path)
    append_n(ob, 5)  # producer_sequence 1..5
    delete_sequence(ob, 3)

    run_reconcile(ob, tmp_path)

    gaps = gap_findings(ob)
    assert len(gaps) == 1
    g = gaps[0]
    assert g["payload"]["missing_sequence"] == 3
    assert g["payload"]["prev_sequence"] == 2
    assert g["payload"]["next_sequence"] == 4
    assert g["partial"] is False  # a lost sequence is a fact, not an inference
    assert g["correlation_id"] == ob.installation_id
    for e in ob.iter_events():
        validate(e)


# --- two separate holes -----------------------------------------------------
def test_two_separate_holes_each_get_their_own_finding(tmp_path):
    ob = new_outbox(tmp_path)
    append_n(ob, 10)  # producer_sequence 1..10
    delete_sequence(ob, 3)
    delete_sequence(ob, 7)

    run_reconcile(ob, tmp_path)

    gaps = gap_findings(ob)
    assert len(gaps) == 2
    by_seq = {g["payload"]["missing_sequence"]: g for g in gaps}
    assert set(by_seq) == {3, 7}
    assert by_seq[3]["payload"]["prev_sequence"] == 2
    assert by_seq[3]["payload"]["next_sequence"] == 4
    assert by_seq[7]["payload"]["prev_sequence"] == 6
    assert by_seq[7]["payload"]["next_sequence"] == 8


# --- contiguous run of missing integers -------------------------------------
def test_contiguous_run_of_missing_integers_emits_one_finding_each(tmp_path):
    """Delete three consecutive sequences (4, 5, 6) out of 1..10.

    Unlike the missed-cron detector -- which explicitly collapses a
    contiguous run of misses into one row with a `missed_count` -- the
    sequence-gap detector has no such collapsing logic: it walks the integers
    between each pair of adjacent surviving sequences and emits one
    reconcile.gap_detected per missing integer. This asserts current behavior.
    A case could be made that
    collapsing a contiguous run into a single finding (with something like
    a `missing_count`, mirroring cron's `missed_count`) would be friendlier
    to a downstream consumer than three near-identical rows all bracketed
    by the same (prev=3, next=7) -- but each missing producer_sequence is
    still an independently provable, individually dedup-keyed loss, so this
    is a design nuance rather than a confirmed defect.
    """
    ob = new_outbox(tmp_path)
    append_n(ob, 10)  # producer_sequence 1..10
    delete_sequence(ob, 4)
    delete_sequence(ob, 5)
    delete_sequence(ob, 6)

    run_reconcile(ob, tmp_path)

    gaps = gap_findings(ob)
    assert len(gaps) == 3  # one finding per missing integer, not collapsed
    missing = sorted(g["payload"]["missing_sequence"] for g in gaps)
    assert missing == [4, 5, 6]
    for g in gaps:
        # Every hole in the run brackets to the same surviving neighbours.
        assert g["payload"]["prev_sequence"] == 3
        assert g["payload"]["next_sequence"] == 7
        assert g["partial"] is False


# --- range boundaries -------------------------------------------------------
def test_dropped_first_sequence_produces_a_leading_gap(tmp_path):
    ob = new_outbox(tmp_path)
    append_n(ob, 5)
    delete_sequence(ob, 1)

    run_reconcile(ob, tmp_path)

    gaps = gap_findings(ob)
    assert len(gaps) == 1
    assert gaps[0]["payload"] == {
        "event_type": "reconcile.gap_detected",
        "gap_kind": "sequence",
        "missing_sequence": 1,
        "prev_sequence": 0,
        "next_sequence": 2,
    }


def test_dropped_tail_sequence_produces_a_trailing_gap(tmp_path):
    ob = new_outbox(tmp_path)
    append_n(ob, 5)
    delete_sequence(ob, 5)

    run_reconcile(ob, tmp_path)

    gaps = gap_findings(ob)
    assert len(gaps) == 1
    assert gaps[0]["payload"] == {
        "event_type": "reconcile.gap_detected",
        "gap_kind": "sequence",
        "missing_sequence": 5,
        "prev_sequence": 4,
        "next_sequence": 6,
    }


# --- empty / single-event outboxes ------------------------------------------
def test_empty_outbox_produces_no_gap_findings(tmp_path):
    ob = new_outbox(tmp_path)  # no events appended at all

    counts = run_reconcile(ob, tmp_path)

    assert counts == {}
    assert gap_findings(ob) == []


def test_single_event_outbox_produces_no_gap_findings(tmp_path):
    ob = new_outbox(tmp_path)
    append_event(ob, "session.created")  # only producer_sequence 1 exists

    counts = run_reconcile(ob, tmp_path)

    assert counts == {}
    assert gap_findings(ob) == []


def test_damaged_high_water_has_a_bounded_finding_count(tmp_path):
    ob = new_outbox(tmp_path)
    append_event(ob, "session.created")
    ob._conn.execute(
        "UPDATE seq SET high_water=? WHERE installation_id=?",
        (10**12, ob.installation_id),
    )

    counts = run_reconcile(
        ob,
        tmp_path,
        config=ReconcileConfig(sequence_gap_limit=3),
    )

    assert counts == {"reconcile.gap_detected": 3}
    assert [
        gap["payload"]["missing_sequence"] for gap in gap_findings(ob)
    ] == [2, 3, 4]


def test_concurrent_append_after_high_water_snapshot_is_not_a_tail_gap(
    tmp_path, monkeypatch
):
    ob = new_outbox(tmp_path)
    append_event(ob, "session.created")
    read_high_water = ob.high_water

    def append_then_read_high_water(installation_id=None):
        append_event(ob, "session.created")
        return read_high_water(installation_id)

    monkeypatch.setattr(ob, "high_water", append_then_read_high_water)

    counts = run_reconcile(ob, tmp_path)

    assert counts == {}
    assert gap_findings(ob) == []


# --- brackets and partial flag ----------------------------------------------
def test_prev_and_next_sequence_are_never_none_for_a_detected_gap(tmp_path):
    """A detected hole has a stored sequence or a range boundary on each side.

    Thus, ``prev_sequence`` and ``next_sequence`` are always integers.
    """
    ob = new_outbox(tmp_path)
    append_n(ob, 6)  # producer_sequence 1..6
    delete_sequence(ob, 2)
    delete_sequence(ob, 5)

    run_reconcile(ob, tmp_path)

    gaps = gap_findings(ob)
    assert len(gaps) == 2
    for g in gaps:
        assert g["payload"]["prev_sequence"] is not None
        assert g["payload"]["next_sequence"] is not None


def test_gap_finding_partial_is_always_false(tmp_path):
    """A lost sequence is a fact, not an inference -- partial=False."""
    ob = new_outbox(tmp_path)
    append_n(ob, 6)
    delete_sequence(ob, 2)
    delete_sequence(ob, 5)

    run_reconcile(ob, tmp_path)

    gaps = gap_findings(ob)
    assert len(gaps) == 2
    assert all(g["partial"] is False for g in gaps)


# --- dedup key and idempotency ----------------------------------------------
def test_dedup_key_is_deterministic_per_installation_and_missing_sequence(tmp_path):
    ob = new_outbox(tmp_path)
    append_n(ob, 5)
    delete_sequence(ob, 3)

    run_reconcile(ob, tmp_path)

    row = ob._conn.execute(
        "SELECT dedup_key FROM events WHERE dedup_key LIKE 'reconcile:seq:%'"
    ).fetchone()
    assert row is not None
    assert row[0] == f"reconcile:seq:{ob.installation_id}:3"


def test_sequence_gap_reconcile_is_idempotent(tmp_path):
    ob = new_outbox(tmp_path)
    append_n(ob, 5)
    delete_sequence(ob, 3)

    first = run_reconcile(ob, tmp_path)
    assert first.get("reconcile.gap_detected") == 1
    n_after_first = ob.count()

    second = run_reconcile(ob, tmp_path)

    assert second == {}  # dedup_key already present -- no new row, no new sequence
    assert ob.count() == n_after_first
    assert len(gap_findings(ob)) == 1
