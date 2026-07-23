"""The idempotent ``install`` command (issue #101).

``install`` makes one Hermes home into one Flight Recorder installation:
recorder home, identity, encryption key, config, and hook — verified and
idempotent, never registering an OS service. It refuses the Hermes root itself
and stops (rather than silently stranding) legacy ``~/.hermes-flight-recorder``
data.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from hermes_flight_recorder import cli
from hermes_flight_recorder.collector import lifecycle
from hermes_flight_recorder.collector.hook import baked_flight_recorder_home
from hermes_flight_recorder.collector.outbox import Outbox


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("SC_HERMES_FLIGHT_RECORDER_HOME", raising=False)
    monkeypatch.delenv("HERMES_HOME", raising=False)


def _hermes(tmp_path) -> Path:
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    (hermes / "config.yaml").write_text("terminal:\n  home_mode: auto\n")
    return hermes


def test_fresh_install_default_location(tmp_path, capsys):
    hermes = _hermes(tmp_path)
    rc = cli.main(["install", "--hermes-home", str(hermes)])
    assert rc == 0

    fr = hermes / "flight-recorder"
    assert (fr / "outbox.sqlite").exists()
    assert (fr / "recorder-config.json").exists()
    key = fr / "content-dev.key"
    assert key.exists()

    hook_dir = hermes / "hooks" / "hermes-flight-recorder"
    assert (hook_dir / "handler.py").exists() and (hook_dir / "HOOK.yaml").exists()
    assert Path(baked_flight_recorder_home(hook_dir)).resolve() == fr.resolve()

    if os.name == "posix":
        assert (key.stat().st_mode & 0o077) == 0
        assert ((fr / "recorder-config.json").stat().st_mode & 0o077) == 0

    # No OS service artifacts created anywhere under the Hermes home.
    names = {p.name for p in hermes.rglob("*")}
    assert not {n for n in names if n.endswith((".service", ".timer", ".plist"))}


def test_install_is_idempotent_and_preserves_identity(tmp_path):
    hermes = _hermes(tmp_path)
    cli.main(["install", "--hermes-home", str(hermes)])
    fr = hermes / "flight-recorder"

    ob = Outbox.open(fr)
    first_id = ob.installation_id
    ob.close()

    # Operator edits the config; a re-install must not clobber it.
    cfg = fr / "recorder-config.json"
    cfg.write_text('{"capture": {"interval_seconds": 42}}')

    assert cli.main(["install", "--hermes-home", str(hermes)]) == 0
    ob = Outbox.open(fr)
    assert ob.installation_id == first_id
    ob.close()
    assert "42" in cfg.read_text()  # preserved


def test_install_refuses_hermes_root(tmp_path, capsys):
    hermes = _hermes(tmp_path)
    rc = cli.main(
        [
            "install",
            "--hermes-home",
            str(hermes),
            "--flight-recorder-home",
            str(hermes),
        ]
    )
    assert rc == 2
    assert "Hermes home root" in capsys.readouterr().err


def test_install_missing_hermes_home_fails(tmp_path, capsys):
    rc = cli.main(["install", "--hermes-home", str(tmp_path / "nope")])
    assert rc == 2
    assert "does not exist" in capsys.readouterr().err


def test_install_stops_on_legacy_data(tmp_path, capsys, monkeypatch):
    hermes = _hermes(tmp_path)
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    (legacy / "outbox.sqlite").write_text("legacy db")
    monkeypatch.setattr(lifecycle, "_legacy_home", lambda: legacy)

    rc = cli.main(["install", "--hermes-home", str(hermes)])
    assert rc == 2
    err = capsys.readouterr().err
    assert "legacy Flight Recorder data" in err
    # Nothing was created at the target.
    assert not (hermes / "flight-recorder" / "outbox.sqlite").exists()
