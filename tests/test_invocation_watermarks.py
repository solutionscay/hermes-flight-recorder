from __future__ import annotations

from contextlib import contextmanager

from hermes_flight_recorder.collector.invocation_watermarks import (
    read_invocation_windows,
)


class MemoryOutbox:
    def __init__(self) -> None:
        self.cursors = {"state.db:invocation-events:v1": "10000"}
        self.meta = {}
        self.after_sequences = []
        self.events = [
            {
                "producer_sequence": 9990,
                "capture_method": "hook:agent:test",
                "session_id": "session",
                "invocation_id": "invocation",
                "occurred_at": 100.0,
                "payload": {"event_type": "invocation.started"},
            }
        ]

    def get_cursor(self, name):
        return self.cursors.get(name)

    def set_cursor(self, name, value):
        self.cursors[name] = str(value)

    def get_meta(self, key):
        return self.meta.get(key)

    def set_meta(self, key, value):
        self.meta[key] = value

    def high_water(self):
        return 10000

    @contextmanager
    def batch(self):
        yield self

    def iter_events(self, *, after_sequence=0):
        self.after_sequences.append(after_sequence)
        return iter(
            event
            for event in self.events
            if event["producer_sequence"] > after_sequence
        )


def test_invocation_index_reads_only_the_watermark_overlap() -> None:
    outbox = MemoryOutbox()

    windows = read_invocation_windows(outbox, {"session"})

    assert outbox.after_sequences == [9936]
    assert windows["session"][0].invocation_id == "invocation"


def test_invocation_index_pairs_a_later_terminal() -> None:
    outbox = MemoryOutbox()
    read_invocation_windows(outbox, {"session"})
    outbox.events.append(
        {
            "producer_sequence": 10001,
            "capture_method": "hook:agent:test",
            "session_id": "session",
            "invocation_id": "invocation",
            "occurred_at": 110.0,
            "payload": {"event_type": "invocation.completed"},
        }
    )
    outbox.high_water = lambda: 10001

    windows = read_invocation_windows(outbox, {"session"})

    assert windows["session"][0].ended_at == 110.0
