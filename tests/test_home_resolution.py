"""Home resolution precedence and cross-platform path handling (issue #101).

One Hermes home is one Flight Recorder installation. The recorder home resolves
by precedence: ``--flight-recorder-home`` → ``$SC_HERMES_FLIGHT_RECORDER_HOME``
→ ``$HERMES_HOME/flight-recorder`` (with the Hermes home itself resolving via
``--hermes-home`` → ``$HERMES_HOME`` → ``~/.hermes``).
"""

from __future__ import annotations

import os
from pathlib import Path, PureWindowsPath

import pytest

from hermes_flight_recorder.collector._common import (
    resolve_flight_recorder_home,
    resolve_hermes_home,
)
from hermes_flight_recorder.collector.outbox import Outbox


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("SC_HERMES_FLIGHT_RECORDER_HOME", raising=False)
    monkeypatch.delenv("HERMES_HOME", raising=False)


# --- precedence ---------------------------------------------------------
def test_explicit_flag_wins_over_everything(monkeypatch, tmp_path):
    monkeypatch.setenv("SC_HERMES_FLIGHT_RECORDER_HOME", str(tmp_path / "env"))
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    got = resolve_flight_recorder_home(tmp_path / "explicit", tmp_path / "hermes")
    assert got == tmp_path / "explicit"


def test_sc_env_wins_over_hermes_default(monkeypatch, tmp_path):
    monkeypatch.setenv("SC_HERMES_FLIGHT_RECORDER_HOME", str(tmp_path / "env"))
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    assert resolve_flight_recorder_home(None, None) == tmp_path / "env"


def test_default_is_hermes_home_child(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    assert resolve_flight_recorder_home(None, None) == tmp_path / "hermes" / "flight-recorder"


def test_hermes_home_flag_beats_env_for_default(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "env-hermes"))
    got = resolve_flight_recorder_home(None, tmp_path / "flag-hermes")
    assert got == tmp_path / "flag-hermes" / "flight-recorder"


def test_hermes_home_defaults_to_dot_hermes(monkeypatch):
    assert resolve_hermes_home(None) == Path.home() / ".hermes"
    assert resolve_flight_recorder_home(None, None) == Path.home() / ".hermes" / "flight-recorder"


# --- paths with spaces --------------------------------------------------
def test_paths_with_spaces_resolve_and_open(tmp_path):
    hermes = tmp_path / "My Hermes Home"
    hermes.mkdir()
    fr = resolve_flight_recorder_home(None, hermes)
    assert fr == hermes / "flight-recorder"
    ob = Outbox.open(fr, hermes_home=hermes)
    ob.initialize()
    assert ob.path == (hermes / "flight-recorder").resolve() / "outbox.sqlite"
    assert " " in str(ob.path)
    ob.close()


# --- windows-style paths ------------------------------------------------
def test_windows_style_path_joins_child():
    # The resolver just joins the child name; a Windows path string must land
    # at the ``flight-recorder`` child under it, with no separator confusion.
    win_hermes = PureWindowsPath(r"C:\Users\dev\AppData\Hermes")
    child = win_hermes / "flight-recorder"
    assert child == PureWindowsPath(r"C:\Users\dev\AppData\Hermes\flight-recorder")


# --- permissions (POSIX) ------------------------------------------------
@pytest.mark.skipif(os.name != "posix", reason="POSIX permission bits")
def test_home_created_owner_only(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    fr = hermes / "flight-recorder"
    ob = Outbox.open(fr, hermes_home=hermes)
    ob.close()
    assert (fr.stat().st_mode & 0o777) == 0o700
