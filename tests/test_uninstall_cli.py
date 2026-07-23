"""The ``uninstall`` command (issue #101 follow-up).

Removes the Hermes hook and, only with ``--purge-data``, the recorder home.
Refuses while a ``serve`` process holds the runtime lock. Idempotent and never
touches other Hermes state.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hermes_flight_recorder import cli
from hermes_flight_recorder.collector.runtime_lock import RuntimeLock


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("SC_HERMES_FLIGHT_RECORDER_HOME", raising=False)
    monkeypatch.delenv("HERMES_HOME", raising=False)


def _install(tmp_path) -> tuple[Path, Path]:
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    (hermes / "config.yaml").write_text("terminal:\n  home_mode: auto\n")
    assert cli.main(["install", "--hermes-home", str(hermes)]) == 0
    return hermes, hermes / "flight-recorder"


def test_default_removes_hook_preserves_data(tmp_path, capsys):
    hermes, fr = _install(tmp_path)
    hook = hermes / "hooks" / "hermes-flight-recorder"

    rc = cli.main(["uninstall", "--hermes-home", str(hermes)])
    assert rc == 0
    assert not hook.exists()  # hook gone
    assert (fr / "outbox.sqlite").exists()  # data preserved
    assert (fr / "content-dev.key").exists()
    assert "preserved" in capsys.readouterr().out


def test_purge_data_removes_recorder_home(tmp_path):
    hermes, fr = _install(tmp_path)
    rc = cli.main(["uninstall", "--hermes-home", str(hermes), "--purge-data"])
    assert rc == 0
    assert not fr.exists()
    assert not (hermes / "hooks" / "hermes-flight-recorder").exists()


def test_refuses_while_serving(tmp_path, capsys):
    hermes, fr = _install(tmp_path)
    lock = RuntimeLock(fr / "runtime.lock")
    lock.acquire()
    try:
        rc = cli.main(["uninstall", "--hermes-home", str(hermes), "--purge-data"])
        assert rc == 2
        assert "running" in capsys.readouterr().err
        assert fr.exists()  # nothing removed
    finally:
        lock.release()


def test_idempotent_when_nothing_installed(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    # Never installed; uninstall must not error.
    assert cli.main(["uninstall", "--hermes-home", str(hermes), "--purge-data"]) == 0


def test_purge_leaves_other_hermes_state_untouched(tmp_path):
    hermes, fr = _install(tmp_path)
    (hermes / "state.db").write_text("hermes owns this")
    other_hook = hermes / "hooks" / "some-other-hook"
    other_hook.mkdir(parents=True)
    (other_hook / "HOOK.yaml").write_text("keep me")

    cli.main(["uninstall", "--hermes-home", str(hermes), "--purge-data"])

    assert (hermes / "state.db").read_text() == "hermes owns this"
    assert (other_hook / "HOOK.yaml").exists()  # only our hook is removed
