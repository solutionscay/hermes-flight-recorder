from __future__ import annotations

import sqlite3

from hermes_flight_recorder.collector import cron_db
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.watermark import (
    Watermark,
    load_meta_json,
    meta_float,
    save_meta_json,
)


class MemoryStore:
    def __init__(self, value: str | None = None) -> None:
        self.value = value

    def get_cursor(self, name: str) -> str | None:
        assert name == "source"
        return self.value

    def set_cursor(self, name: str, value: str | int) -> None:
        assert name == "source"
        self.value = str(value)


def test_watermark_replays_a_bounded_overlap() -> None:
    store = MemoryStore("100")
    watermark = Watermark(store, "source", overlap=12)

    assert watermark.read() == 100
    assert watermark.lower_bound() == 88


def test_watermark_only_moves_forward() -> None:
    store = MemoryStore("100")
    watermark = Watermark(store, "source", overlap=12)

    watermark.advance(90)

    assert watermark.read() == 100


def test_watermark_recovers_from_missing_or_invalid_state() -> None:
    store = MemoryStore()
    watermark = Watermark(store, "source", overlap=12)

    assert watermark.read() == 0
    store.value = "broken"
    assert watermark.read() == 0
    watermark.advance(5)
    assert watermark.read() == 5


def _source(rows: int) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany(
        "INSERT INTO items VALUES (?, ?)",
        ((row, f"item-{row}") for row in range(1, rows + 1)),
    )
    return conn


def test_high_water_follows_the_source_maximum() -> None:
    watermark = Watermark(MemoryStore(), "source")

    assert watermark.high_water(_source(7), "items", "id") == 7


def test_high_water_never_falls_below_the_durable_mark() -> None:
    """A truncated or reset source cannot pull the scan range backwards."""
    watermark = Watermark(MemoryStore("100"), "source", overlap=12)

    assert watermark.high_water(_source(2), "items", "id") == 100


def test_bounded_rows_reads_the_overlap_range_in_order() -> None:
    watermark = Watermark(MemoryStore("5"), "source", overlap=2)

    rows, high_water = watermark.bounded_rows(_source(9), "items", "id, name", "id")

    assert high_water == 9
    assert [row[0] for row in rows] == [4, 5, 6, 7, 8, 9]


class MemoryMeta:
    def __init__(self, value: str | None = None) -> None:
        self.value = value

    def get_meta(self, key: str) -> str | None:
        return self.value

    def set_meta(self, key: str, value: str) -> None:
        self.value = value


def test_meta_json_round_trips_in_the_compact_encoding() -> None:
    store = MemoryMeta()

    save_meta_json(store, "key", ["a", "b"])

    assert store.value == '["a","b"]'
    assert load_meta_json(store, "key", []) == ["a", "b"]


def test_load_meta_json_degrades_to_the_default() -> None:
    assert load_meta_json(MemoryMeta(), "key", []) == []
    assert load_meta_json(MemoryMeta("broken"), "key", []) == []
    assert load_meta_json(MemoryMeta('{"a":1}'), "key", []) == []


def test_meta_float_parses_or_returns_the_default() -> None:
    assert meta_float(MemoryMeta("12.5"), "key") == 12.5
    assert meta_float(MemoryMeta(), "key") is None
    assert meta_float(MemoryMeta(), "key", 0.0) == 0.0
    assert meta_float(MemoryMeta("broken"), "key", 0.0) == 0.0


def _write_executions(db_path, rows: range) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS executions (id TEXT, job_id TEXT, source TEXT, "
        "pid INT, status TEXT, claimed_at REAL, started_at REAL, finished_at REAL, "
        "error TEXT)"
    )
    conn.executemany(
        "INSERT INTO executions VALUES "
        "(?, 'job', 'test', 1, 'running', ?, NULL, NULL, NULL)",
        ((f"run-{row}", float(row)) for row in rows),
    )
    conn.commit()
    conn.close()


def test_cron_watermark_survives_a_reset_executions_db(tmp_path) -> None:
    """A truncated or reset executions.db cannot regress the cron watermark."""
    hermes = tmp_path / "hermes"
    db_path = hermes / "cron" / "executions.db"
    db_path.parent.mkdir(parents=True)
    _write_executions(db_path, range(1, 101))
    outbox = Outbox.open(tmp_path / "recorder")
    outbox.initialize()

    cron_db.poll(outbox, hermes)
    assert outbox.get_cursor("cron.db:executions:v1") == "100"

    db_path.unlink()
    _write_executions(db_path, range(1, 3))
    cron_db.poll(outbox, hermes)

    assert outbox.get_cursor("cron.db:executions:v1") == "100"
    outbox.close()
