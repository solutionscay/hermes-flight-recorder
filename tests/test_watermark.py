from __future__ import annotations

from hermes_flight_recorder.collector.watermark import Watermark


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
