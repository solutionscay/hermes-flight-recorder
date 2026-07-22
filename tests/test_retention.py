"""Tests for config-controlled outbox retention (issue #69)."""

from __future__ import annotations

import json

import pytest

from hermes_flight_recorder import cli
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.recorder_config import RetentionConfig
from hermes_flight_recorder.collector.retention import (
    RetentionError,
    maybe_prune,
    prune,
)

from test_outbox import base_record


NOW = 2_000_000_000.0
OLD = NOW - 40 * 24 * 60 * 60


def open_outbox(tmp_path) -> Outbox:
    outbox = Outbox.open(tmp_path)
    outbox.initialize()
    return outbox


def append_at(
    outbox: Outbox,
    recorded_at: float,
    *,
    dedup_key: str | None = None,
    content: str | None = None,
) -> int:
    record = outbox.append(base_record(), dedup_key=dedup_key, content=content)
    sequence = record["producer_sequence"]
    outbox._conn.execute(
        "UPDATE events SET recorded_at=? WHERE producer_sequence=?",
        (recorded_at, sequence),
    )
    return sequence


def event_sequences(outbox: Outbox) -> list[int]:
    return [event["producer_sequence"] for event in outbox.iter_events()]


def event_sizes(outbox: Outbox) -> list[int]:
    return [
        row[0]
        for row in outbox._conn.execute(
            "SELECT length(CAST(envelope_json AS BLOB)) FROM events "
            "ORDER BY producer_sequence"
        )
    ]


def test_disabled_retention_preserves_append_only_behavior(tmp_path):
    outbox = open_outbox(tmp_path)
    append_at(outbox, OLD)
    outbox.set_cursor("delivery", 1)

    assert prune(outbox, RetentionConfig(), now=NOW) is None
    assert event_sequences(outbox) == [1]
    assert outbox.high_water() == 1
    outbox.close()


def test_age_policy_prunes_only_delivered_and_preserves_authority_and_meta(tmp_path):
    outbox = open_outbox(tmp_path)
    append_at(outbox, OLD, dedup_key="old-one")
    append_at(outbox, NOW, dedup_key="recent")
    append_at(outbox, OLD, dedup_key="old-three")
    append_at(outbox, OLD, dedup_key="old-undelivered")
    outbox.set_cursor("delivery", 3)
    outbox.set_cursor("state.db", 99)
    outbox.set_meta("collector:pairing", "kept")

    result = prune(
        outbox,
        RetentionConfig(enabled=True, max_age_days=30),
        now=NOW,
    )

    assert result is not None
    assert result.pruned_count == 2
    assert (result.oldest_sequence, result.newest_sequence) == (1, 3)
    assert event_sequences(outbox) == [2, 4]
    assert outbox.high_water() == 4
    assert outbox.get_cursor("delivery") == "3"
    assert outbox.get_cursor("state.db") == "99"
    assert outbox.get_meta("collector:pairing") == "kept"

    # Pruning releases an old dedup key, but the advanced producer cursor is
    # preserved and sequence authority never reuses the removed sequence.
    replacement = outbox.append(base_record(), dedup_key="old-one")
    assert replacement["producer_sequence"] == 5
    assert outbox.get_cursor("state.db") == "99"
    outbox.close()


def test_size_policy_prunes_oldest_delivered_until_under_budget(tmp_path):
    outbox = open_outbox(tmp_path)
    for marker in ("a" * 100, "b" * 200, "c" * 300, "d" * 400):
        append_at(outbox, NOW, content=marker)
    outbox.set_cursor("delivery", 3)
    sizes = event_sizes(outbox)
    total = sum(sizes)

    result = prune(
        outbox,
        RetentionConfig(
            enabled=True,
            max_age_days=None,
            max_bytes=total - sizes[0],
        ),
        now=NOW,
    )

    assert result is not None
    assert result.pruned_count == 1
    assert result.oldest_sequence == result.newest_sequence == 1
    assert result.event_bytes_after <= total - sizes[0]
    assert event_sequences(outbox) == [2, 3, 4]
    outbox.close()


def test_size_policy_stops_above_budget_when_only_undelivered_rows_remain(tmp_path):
    outbox = open_outbox(tmp_path)
    for marker in ("delivered", "also delivered", "never delivered"):
        append_at(outbox, OLD, content=marker)
    outbox.set_cursor("delivery", 2)

    result = prune(
        outbox,
        RetentionConfig(enabled=True, max_age_days=None, max_bytes=1),
        now=NOW,
    )

    assert result is not None
    assert result.pruned_count == 2
    assert event_sequences(outbox) == [3]
    assert result.event_bytes_after > 1
    outbox.close()


def test_vacuum_reclaims_database_pages_after_prune(tmp_path):
    outbox = open_outbox(tmp_path)
    for index in range(80):
        append_at(outbox, OLD, content=f"{index}:" + "x" * 4096)
    outbox.set_cursor("delivery", outbox.high_water())
    wal_path = outbox.path.with_name(outbox.path.name + "-wal")
    assert wal_path.stat().st_size > 0

    result = prune(
        outbox,
        RetentionConfig(enabled=True, max_age_days=30, vacuum="auto"),
        now=NOW,
    )

    assert result is not None
    assert result.pruned_count == 80
    assert result.database_bytes_reclaimed > 0
    assert outbox._conn.execute("PRAGMA freelist_count").fetchone()[0] == 0
    assert wal_path.stat().st_size == 0
    outbox.close()


def test_unsafe_require_delivered_false_is_refused(tmp_path):
    outbox = open_outbox(tmp_path)
    append_at(outbox, OLD)

    with pytest.raises(RetentionError, match="must be true"):
        prune(
            outbox,
            RetentionConfig(enabled=True, require_delivered=False),
            now=NOW,
        )

    assert event_sequences(outbox) == [1]
    outbox.close()


def test_automatic_prune_is_persistently_throttled(tmp_path):
    outbox = open_outbox(tmp_path)
    append_at(outbox, OLD)
    outbox.set_cursor("delivery", 1)
    config = RetentionConfig(enabled=True, max_age_days=30)

    first = maybe_prune(outbox, config, now=NOW, interval_seconds=100)
    append_at(outbox, OLD)
    outbox.set_cursor("delivery", 2)
    second = maybe_prune(outbox, config, now=NOW + 50, interval_seconds=100)
    third = maybe_prune(outbox, config, now=NOW + 100, interval_seconds=100)

    assert first is not None and first.pruned_count == 1
    assert second is None
    assert third is not None and third.pruned_count == 1
    outbox.close()


def test_prune_cli_obeys_disabled_default_and_reports_enabled_deletion(
    tmp_path, capsys
):
    outbox = open_outbox(tmp_path)
    append_at(outbox, 1.0)
    outbox.set_cursor("delivery", 1)
    outbox.close()

    args = ["prune", "--flight-recorder-home", str(tmp_path)]
    assert cli.main(args) == 0
    assert "retention disabled" in capsys.readouterr().out

    (tmp_path / "recorder-config.json").write_text(
        json.dumps({"retention": {"enabled": True, "max_age_days": 30}})
    )
    assert cli.main(args) == 0
    output = capsys.readouterr().out
    assert "pruned 1 delivered event" in output
    assert "sequences 1-1" in output
    assert "reclaimed" in output

    outbox = Outbox.open(tmp_path)
    assert outbox.count() == 0
    outbox.close()


def test_run_applies_enabled_retention_on_automatic_cadence(tmp_path, capsys):
    flight_recorder_home = tmp_path / "recorder"
    hermes_home = tmp_path / "hermes"
    outbox = open_outbox(flight_recorder_home)
    append_at(outbox, 1.0)
    outbox.set_cursor("delivery", 1)
    outbox.close()
    (flight_recorder_home / "recorder-config.json").write_text(
        json.dumps({"retention": {"enabled": True, "max_age_days": 30}})
    )

    code = cli.main(
        [
            "run",
            "--flight-recorder-home",
            str(flight_recorder_home),
            "--hermes-home",
            str(hermes_home),
        ]
    )

    assert code == 0
    outbox = Outbox.open(flight_recorder_home)
    assert outbox.count() == 0
    outbox.close()
    assert "automatic retention: pruned 1 delivered event" in capsys.readouterr().out
