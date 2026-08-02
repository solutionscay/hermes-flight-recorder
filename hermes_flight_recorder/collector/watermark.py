"""Durable integer watermarks for incremental source scans."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class WatermarkStore(Protocol):
    """The persistence needed by an incremental collector."""

    def get_cursor(self, name: str) -> str | None: ...

    def set_cursor(self, name: str, value: str | int) -> None: ...


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


__all__ = ["Watermark", "WatermarkStore"]
