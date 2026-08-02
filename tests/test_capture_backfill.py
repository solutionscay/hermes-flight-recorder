"""The ``install --no-backfill`` capture horizon (issue #111).

By default capture backfills the whole Hermes history. ``--no-backfill`` records
only activity at or after the install moment, so a fresh install over a
long-lived Hermes home does not ingest the entire past.
"""

from __future__ import annotations

import pytest

from helpers import MESSAGES_FULL, SESSIONS_FULL, make_state_db

from hermes_flight_recorder.collector import lifecycle, run_pass
from hermes_flight_recorder.collector._common import (
    CAPTURE_BACKFILL_META_KEY,
    INSTALLED_AT_META_KEY,
)
from hermes_flight_recorder.collector.outbox import Outbox


@pytest.fixture(autouse=True)
def _clean_env(clean_env):
    """Every test here runs with the recorder env vars cleared."""


def _make_state(hermes, sessions, messages) -> None:
    """sessions: (sid, started) tuples; messages: (sid, role, ts, content)."""
    make_state_db(
        hermes,
        sessions=[
            (sid, "cli", None, "m", 0, 0, 0, 0, 0.0, started, None, None, "default", 1)
            for (sid, started) in sessions
        ],
        messages=[
            (i, sid, role, None, None, None, content, ts, None)
            for i, (sid, role, ts, content) in enumerate(messages, 1)
        ],
        sessions_columns=SESSIONS_FULL,
        messages_columns=MESSAGES_FULL,
    )


def _install(tmp_path, *, backfill: bool):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    (hermes / "config.yaml").write_text("terminal:\n  home_mode: auto\n")
    fr = lifecycle.install(None, str(hermes), backfill=backfill, log=lambda *a: None)
    return hermes, fr


def _counts(ob):
    sessions = sum(
        1 for e in ob.iter_events() if e["payload"]["event_type"] == "session.created"
    )
    messages = sum(1 for e in ob.iter_events() if "message_row_id" in e["payload"])
    return sessions, messages


def test_default_install_backfills_history(tmp_path):
    hermes, fr = _install(tmp_path, backfill=True)
    ob = Outbox.open(fr)
    horizon = float(ob.get_meta(INSTALLED_AT_META_KEY))
    _make_state(
        hermes,
        [("OLD", horizon - 1000), ("NEW", horizon + 1000)],
        [("OLD", "user", horizon - 1000, "old"), ("NEW", "user", horizon + 1000, "new")],
    )
    run_pass(ob, str(hermes))
    assert _counts(ob) == (2, 2)  # both historical and new captured
    assert ob.get_meta(CAPTURE_BACKFILL_META_KEY) is None  # flag not set
    ob.close()


def test_no_backfill_skips_history_keeps_new(tmp_path):
    hermes, fr = _install(tmp_path, backfill=False)
    ob = Outbox.open(fr)
    assert ob.get_meta(CAPTURE_BACKFILL_META_KEY) == "false"
    horizon = float(ob.get_meta(INSTALLED_AT_META_KEY))
    _make_state(
        hermes,
        [("OLD", horizon - 1000), ("NEW", horizon + 1000)],
        [("OLD", "user", horizon - 1000, "old"), ("NEW", "user", horizon + 1000, "new")],
    )
    run_pass(ob, str(hermes))
    assert _counts(ob) == (1, 1)  # only the post-install session and message
    types = {e["payload"]["event_type"] for e in ob.iter_events()}
    assert "session.created" in types
    ob.close()


def test_choice_persists_across_reopen(tmp_path):
    # The flag lives in the outbox, so a later run_pass (new process) honors it
    # without any CLI argument.
    hermes, fr = _install(tmp_path, backfill=False)
    ob = Outbox.open(fr)
    horizon = float(ob.get_meta(INSTALLED_AT_META_KEY))
    ob.close()

    _make_state(
        hermes,
        [("OLD", horizon - 1000)],
        [("OLD", "user", horizon - 1000, "old")],
    )
    reopened = Outbox.open(fr)  # fresh handle, as serve/run would open
    run_pass(reopened, str(hermes))
    assert _counts(reopened) == (0, 0)  # nothing historical, even after reopen
    reopened.close()
