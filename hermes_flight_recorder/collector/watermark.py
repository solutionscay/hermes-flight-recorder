"""Durable integer watermarks for incremental source scans."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol


class WatermarkStore(Protocol):
    """The persistence needed by an incremental collector."""

    def get_cursor(self, name: str) -> str | None: ...

    def set_cursor(self, name: str, value: str | int) -> None: ...


class MetaStore(Protocol):
    """The durable key/value store the outbox exposes beside cursors."""

    def get_meta(self, key: str) -> str | None: ...

    def set_meta(self, key: str, value: str) -> None: ...


@dataclass(frozen=True)
class Watermark:
    """A durable high-water mark with a bounded replay overlap."""

    store: WatermarkStore
    name: str
    overlap: int = 0

    def read(self) -> int:
        raw = self.store.get_cursor(self.name)
        if raw is None:
            return 0
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return 0
        return max(0, value)

    def lower_bound(self) -> int:
        return max(0, self.read() - max(0, self.overlap))

    def advance(self, value: int) -> None:
        """Move the watermark forward after the complete scan succeeds."""
        current = self.read()
        self.store.set_cursor(self.name, max(current, int(value), 0))

    def high_water(self, conn: Any, table: str, column: str) -> int:
        """The scan upper bound for one source snapshot.

        The source ``MAX`` clamped to never fall below the durable mark, so a
        truncated or reset source cannot pull the scan range (or, through
        :meth:`advance`, the watermark) backwards.
        """
        return max(
            self.read(),
            int(
                conn.execute(
                    f"SELECT COALESCE(MAX({column}), 0) FROM {table}"
                ).fetchone()[0]
            ),
        )

    def bounded_rows(
        self, conn: Any, table: str, select: str, column: str
    ) -> tuple[list[Any], int]:
        """One bounded incremental scan: rows in ``(lower_bound, high_water]``.

        Returns the rows in ``column`` order plus the high-water mark the
        caller passes to :meth:`advance` once the complete pass succeeds.
        """
        high_water = self.high_water(conn, table, column)
        rows = conn.execute(
            f"SELECT {select} FROM {table} "
            f"WHERE {column} > ? AND {column} <= ? ORDER BY {column}",
            (self.lower_bound(), high_water),
        ).fetchall()
        return rows, high_water


def load_meta_json(store: MetaStore, key: str, default: Any) -> Any:
    """A JSON value from durable meta.

    Returns ``default`` when the key is absent, the text is not JSON, or the
    parsed value is not the same type as ``default``, so a damaged meta row
    degrades to the default instead of raising.
    """
    raw = store.get_meta(key)
    if raw is None:
        return default
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return default
    return value if isinstance(value, type(default)) else default


def save_meta_json(store: MetaStore, key: str, value: Any) -> None:
    """Write a JSON value to durable meta in the compact encoding."""
    store.set_meta(key, json.dumps(value, separators=(",", ":")))


def meta_float(
    store: MetaStore, key: str, default: float | None = None
) -> float | None:
    """A float from durable meta; ``default`` when absent or unparseable."""
    raw = store.get_meta(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


__all__ = [
    "MetaStore",
    "Watermark",
    "WatermarkStore",
    "load_meta_json",
    "meta_float",
    "save_meta_json",
]
