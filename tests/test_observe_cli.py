"""Self-contained tests for the observe CLI wiring (hermes_flight_recorder.cli.main /
_cmd_observe), exercised through main([...]) with capsys.
"""

from __future__ import annotations

import pytest

from hermes_flight_recorder import observe
from hermes_flight_recorder.cli import main
from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.outbox import Outbox

B = 1784415000.0


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def add(ob, event_type, *, occurred_at=B, session_id=None, parent_session_id=None,
        correlation_id="corr", invocation_id=None, payload=None, partial=False,
        content=None):
    rec = build_record(
        event_type=event_type,
        occurred_at=occurred_at,
        source="test",
        capture_method="test",
        runtime={"kind": "cli", "engine": "standard"},
        correlation_id=correlation_id,
        session_id=session_id,
        parent_session_id=parent_session_id,
        invocation_id=invocation_id,
        payload=payload or {},
        partial=partial,
    )
    return ob.append(rec, content=content)


# --- not initialized ------------------------------------------------------
def test_observe_not_initialized_exits_2_with_stderr_hint(tmp_path, capsys):
    bridge = str(tmp_path / "bridge")  # never opened/initialized
    code = main(["observe", "--flight-recorder-home", bridge])
    captured = capsys.readouterr()
    assert code == 2
    assert "hermes-flight-recorder install" in captured.err
    assert captured.out == ""


# --- default view -----------------------------------------------------
def test_observe_default_view_is_stream_with_header(tmp_path, capsys):
    bridge = str(tmp_path / "bridge")
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="P", payload={"kind": "cli"})
    add(ob, "session.created", session_id="Q", payload={"kind": "cli"})
    ob.close()

    code = main(["observe", "--flight-recorder-home", bridge])  # no view flag
    out = capsys.readouterr().out

    assert code == 0
    assert "── stream (2 events) ──" in out
    # Only the stream header should appear; tree/report headers must not.
    assert "── tree ──" not in out
    assert "── report ──" not in out


# --- multiple views in one invocation --------------------------------------
def test_observe_multiple_views_each_print_header_exit_from_report(tmp_path, capsys):
    bridge = str(tmp_path / "bridge")
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="P", payload={"kind": "cli"})
    add(ob, "cron.run_missed", correlation_id="j1",
        payload={"job_id": "j1", "expected_fire_at": B, "missed_count": 1}, partial=True)
    ob.close()

    # Flags given out of order; the printed view order must still be the
    # fixed stream -> tree -> report order (per _cmd_observe).
    code = main(["observe", "--report", "--stream", "--tree", "--flight-recorder-home", bridge])
    out = capsys.readouterr().out

    assert "── stream (2 events) ──" in out
    assert "── tree ──" in out
    assert "── report ──" in out
    assert out.index("── stream") < out.index("── tree") < out.index("── report")
    # A finding exists -> the overall exit code is governed by --report.
    assert code == 1


def test_observe_multiple_views_without_report_flag_exit_zero_even_with_findings(tmp_path, capsys):
    """Spec: exit code is governed by --report. When --report is absent,
    the code stays 0 regardless of any reconcile finding in the records —
    even though --stream/--tree render the same underlying records.
    """
    bridge = str(tmp_path / "bridge")
    ob = new_outbox(tmp_path)
    add(ob, "cron.run_missed", correlation_id="j1",
        payload={"job_id": "j1", "expected_fire_at": B, "missed_count": 1}, partial=True)
    ob.close()

    code = main(["observe", "--stream", "--tree", "--flight-recorder-home", bridge])
    out = capsys.readouterr().out

    assert "── stream" in out and "── tree ──" in out
    assert "── report ──" not in out
    assert code == 0


def test_observe_report_only_clean_exits_zero(tmp_path, capsys):
    bridge = str(tmp_path / "bridge")
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="P", payload={"kind": "cli"})  # no findings
    ob.close()

    code = main(["observe", "--report", "--flight-recorder-home", bridge])
    out = capsys.readouterr().out

    assert code == 0
    assert "── report ──" in out
    assert "clean" in out


# --- bad --since ------------------------------------------------------
def test_observe_bad_since_exits_2_with_stderr_message_no_traceback(tmp_path, capsys):
    bridge = str(tmp_path / "bridge")
    ob = new_outbox(tmp_path)
    ob.close()

    code = main(["observe", "--since", "not-a-timestamp", "--flight-recorder-home", bridge])
    captured = capsys.readouterr()

    assert code == 2
    assert "--since" in captured.err
    assert "Traceback" not in captured.err
    assert captured.out == ""
    # The stderr message is exactly what observe.parse_since raises.
    with pytest.raises(ValueError) as exc:
        observe.parse_since("not-a-timestamp")
    assert str(exc.value) in captured.err


# --- --session / --since reach load() -----------------------------------
def test_observe_since_flag_reaches_load_and_filters_stream(tmp_path, capsys):
    bridge = str(tmp_path / "bridge")
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="old", occurred_at=B, payload={"kind": "cli"})
    add(ob, "session.created", session_id="new", occurred_at=B + 100, payload={"kind": "cli"})
    ob.close()

    code = main(["observe", "--since", str(B + 50), "--flight-recorder-home", bridge])
    out = capsys.readouterr().out

    assert code == 0
    assert "── stream (1 events) ──" in out
    assert "new" in out
    assert "old" not in out


def test_observe_session_flag_reaches_load_and_filters_tree(tmp_path, capsys):
    bridge = str(tmp_path / "bridge")
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="A", correlation_id="A", payload={"kind": "cli"})
    add(ob, "session.created", session_id="B", correlation_id="B", payload={"kind": "cli"})
    ob.close()

    code = main(["observe", "--tree", "--session", "A", "--flight-recorder-home", bridge])
    out = capsys.readouterr().out

    assert code == 0
    assert "cli A" in out
    assert "cli B" not in out


def test_observe_since_accepts_iso_timestamp_via_cli(tmp_path, capsys):
    """--since also accepts an ISO 8601 timestamp (observe.parse_since),
    reached identically through the CLI path.
    """
    bridge = str(tmp_path / "bridge")
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="old", occurred_at=B, payload={"kind": "cli"})
    add(ob, "session.created", session_id="new", occurred_at=B + 100, payload={"kind": "cli"})
    ob.close()

    import datetime
    since_iso = datetime.datetime.fromtimestamp(
        B + 50, datetime.timezone.utc
    ).isoformat()

    code = main(["observe", "--since", since_iso, "--flight-recorder-home", bridge])
    out = capsys.readouterr().out

    assert code == 0
    assert "new" in out
    assert "old" not in out
