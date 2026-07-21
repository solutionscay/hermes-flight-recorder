from __future__ import annotations

from hermes_flight_recorder.collector.gateway_log import poll
from hermes_flight_recorder.collector.outbox import Outbox


def _outbox(tmp_path):
    outbox = Outbox.open(tmp_path / "bridge")
    outbox.initialize()
    return outbox


def test_captures_terminal_provider_failure_with_encrypted_summary(tmp_path):
    hermes = tmp_path / "hermes"
    log = hermes / "logs" / "agent.log"
    log.parent.mkdir(parents=True)
    log.write_text(
        "2026-07-20 20:23:31,420 ERROR [session-1] agent.conversation_loop: "
        "API call failed after 3 retries. HTTP 404: Couldn't find that, sorry. "
        "| provider=nous model=tencent/hy3:free msgs=2 tokens=~4,743\n"
    )
    outbox = _outbox(tmp_path)

    assert poll(outbox, hermes) == {"model.call_failed": 1}
    event = list(outbox.iter_events())[0]
    assert event["payload"] == {
        "event_type": "model.call_failed",
        "provider": "nous",
        "model": "tencent/hy3:free",
        "attempts": 3,
        "error_class": "not_found",
        "http_status": 404,
    }
    assert event["session_id"] == "session-1"
    assert "Couldn't find" not in str(event)
    assert outbox.decrypt_content(event).decode() == "HTTP 404: Couldn't find that, sorry."


def test_cursor_deduplicates_and_waits_for_complete_line(tmp_path):
    hermes = tmp_path / "hermes"
    log = hermes / "logs" / "agent.log"
    log.parent.mkdir(parents=True)
    log.write_text(
        "2026-07-20 20:23:31,420 ERROR [session-1] agent.conversation_loop: "
        "API call failed after 3 retries. HTTP 429: busy | provider=nous model=m\n"
        "2026-07-20 20:24:31,420 ERROR [session-2] agent.conversation_loop: "
        "API call failed after 3 retries. HTTP 500: partial | provider=nous model=m"
    )
    outbox = _outbox(tmp_path)

    assert poll(outbox, hermes) == {"model.call_failed": 1}
    assert poll(outbox, hermes) == {}
    with log.open("a") as fh:
        fh.write("\n")
    assert poll(outbox, hermes) == {"model.call_failed": 1}
    events = list(outbox.iter_events())
    assert [event["payload"]["error_class"] for event in events] == [
        "rate_limited",
        "provider_server",
    ]
