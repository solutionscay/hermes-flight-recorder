"""Tests for the terminal.home_mode runtime stamp (issue #16).

``terminal.home_mode`` (``auto`` | ``real`` | ``profile``, default ``auto``)
is the Hermes policy that decides where tools run and which git identity they
use. Hermes Flight Recorder captures it as plaintext operational metadata on the runtime
stamp of Hermes-runtime poll events — never the resolved home path, which is
sensitive content. The reader is a standard-library scanner (the project
declares no YAML dependency).
"""

from __future__ import annotations

import sqlite3

from hermes_flight_recorder.collector import cron_db, state_db
from hermes_flight_recorder.collector._common import read_home_mode, runtime_stamp
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.envelope import validate

from test_state_adapter import make_cron, make_state_db, new_outbox


# --- runtime_stamp ------------------------------------------------------
def test_runtime_stamp_omits_home_mode_by_default():
    assert runtime_stamp("tool") == {"kind": "tool", "engine": "standard"}


def test_runtime_stamp_includes_home_mode_when_passed():
    assert runtime_stamp("tool", home_mode="profile") == {
        "kind": "tool",
        "engine": "standard",
        "home_mode": "profile",
    }


# --- read_home_mode: parsing --------------------------------------------
def _write_config(hh, body: str) -> None:
    (hh / "config.yaml").write_text(body)


def test_read_home_mode_missing_config_defaults_auto(tmp_path):
    assert read_home_mode(tmp_path) == "auto"  # no config.yaml at all


def test_read_home_mode_missing_terminal_block_defaults_auto(tmp_path):
    _write_config(tmp_path, "gateway:\n  use_gateway: true\n")
    assert read_home_mode(tmp_path) == "auto"


def test_read_home_mode_missing_key_defaults_auto(tmp_path):
    _write_config(tmp_path, "terminal:\n  backend: local\n  timeout: 180\n")
    assert read_home_mode(tmp_path) == "auto"


def test_read_home_mode_explicit_values(tmp_path):
    for value in ("auto", "real", "profile"):
        _write_config(tmp_path, f"terminal:\n  home_mode: {value}\n")
        assert read_home_mode(tmp_path) == value


def test_read_home_mode_normalizes_aliases(tmp_path):
    for alias, canonical in (
        ("isolated", "profile"),
        ("profile_home", "profile"),
        ("profile-home", "profile"),
        ("host", "real"),
        ("user", "real"),
        ("real_home", "real"),
    ):
        _write_config(tmp_path, f"terminal:\n  home_mode: {alias}\n")
        assert read_home_mode(tmp_path) == canonical


def test_read_home_mode_strips_quotes_comments_and_case(tmp_path):
    _write_config(tmp_path, 'terminal:\n  home_mode: "PROFILE"  # trailing comment\n')
    assert read_home_mode(tmp_path) == "profile"


def test_read_home_mode_blank_value_defaults_auto(tmp_path):
    _write_config(tmp_path, "terminal:\n  home_mode:   \n")
    assert read_home_mode(tmp_path) == "auto"


def test_read_home_mode_unknown_value_defaults_auto(tmp_path):
    _write_config(tmp_path, "terminal:\n  home_mode: banana\n")
    assert read_home_mode(tmp_path) == "auto"


def test_read_home_mode_ignores_home_mode_outside_terminal(tmp_path):
    # A home_mode key under a different top-level block must not match.
    _write_config(tmp_path, "other:\n  home_mode: profile\nterminal:\n  backend: local\n")
    assert read_home_mode(tmp_path) == "auto"


def test_read_home_mode_never_leaks_adjacent_secrets(tmp_path):
    # A token in an adjacent block must never influence or leak into the result.
    _write_config(
        tmp_path,
        "discord:\n  token: super-secret-bot-token\n"
        "terminal:\n  home_mode: profile\n",
    )
    assert read_home_mode(tmp_path) == "profile"


# --- integration: poll events carry home_mode ---------------------------
def test_state_poll_stamps_home_mode(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_state_db(hh)
    _write_config(hh, "terminal:\n  home_mode: profile\n")
    ob = new_outbox(tmp_path)

    state_db.poll(ob, hh)
    events = list(ob.iter_events())
    assert events  # sanity
    assert all(e["runtime"].get("home_mode") == "profile" for e in events)
    for e in events:
        validate(e)  # additive: runtime is free-form, contract still holds


def test_cron_poll_stamps_home_mode(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_cron(hh)
    _write_config(hh, "terminal:\n  home_mode: real\n")
    ob = new_outbox(tmp_path)

    cron_db.poll(ob, hh)
    events = list(ob.iter_events())
    assert events
    assert all(e["runtime"].get("home_mode") == "real" for e in events)


def test_default_home_mode_is_auto_when_config_absent(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_state_db(hh)  # no config.yaml written
    ob = new_outbox(tmp_path)

    state_db.poll(ob, hh)
    created = next(
        e for e in ob.iter_events() if e["payload"]["event_type"] == "session.created"
    )
    assert created["runtime"]["home_mode"] == "auto"
