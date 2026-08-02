"""Shared fixtures (issue #158). Plain helpers live in ``tests/helpers.py``."""

from __future__ import annotations

import pytest


@pytest.fixture
def clean_env(monkeypatch):
    """Clear recorder-related environment variables for env-sensitive tests.

    Modules opt in with a module-level autouse fixture that depends on this
    one, so the rest of the suite is untouched.
    """
    for var in (
        "SC_HERMES_FLIGHT_RECORDER_HOME",
        "HERMES_HOME",
        "HFR_INGEST_URL",
        "HFR_CF_ACCESS_CLIENT_ID",
        "HFR_CF_ACCESS_CLIENT_SECRET",
    ):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def isolated_user_home(tmp_path, monkeypatch):
    """Point ``HOME`` at a fresh directory so ``~`` never hits the real home."""
    user_home = tmp_path / "user-home"
    user_home.mkdir()
    monkeypatch.setenv("HOME", str(user_home))
    return user_home
