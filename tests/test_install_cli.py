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
from hermes_flight_recorder.collector.hook import baked_flight_recorder_home
from hermes_flight_recorder.collector.outbox import Outbox


@pytest.fixture(autouse=True)
def _clean_env(clean_env, isolated_user_home):
    """Recorder env vars cleared and HOME pointed at a fresh directory."""


def _hermes(tmp_path) -> Path:
    hermes = tmp_path / "hermes"
    hermes.mkdir(parents=True)
    (hermes / "config.yaml").write_text("terminal:\n  home_mode: auto\n")
    return hermes


def test_fresh_install_default_location(tmp_path, capsys):
    hermes = _hermes(tmp_path)
    rc = cli.main(["install", "--hermes-home", str(hermes)])
    assert rc == 0

    fr = hermes / "flight-recorder"
    assert (fr / "outbox.sqlite").exists()
    assert (fr / "recorder-config.json").exists()
    # Solo install mints both operator key halves locally.
    public = fr / "operator.pub"
    secret = fr / "operator.secret"
    assert public.exists()
    assert secret.exists()

    hook_dir = hermes / "hooks" / "hermes-flight-recorder"
    assert (hook_dir / "handler.py").exists() and (hook_dir / "HOOK.yaml").exists()
    assert Path(baked_flight_recorder_home(hook_dir)).resolve() == fr.resolve()

    if os.name == "posix":
        assert (secret.stat().st_mode & 0o077) == 0  # private key owner-only
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


def test_default_install_stops_on_legacy_data(tmp_path, capsys):
    hermes = Path.home() / ".hermes"
    hermes.mkdir()
    (hermes / "config.yaml").write_text("terminal:\n  home_mode: auto\n")
    legacy = Path.home() / ".hermes-flight-recorder"
    legacy.mkdir()
    (legacy / "outbox.sqlite").write_text("legacy db")

    rc = cli.main(["install"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "legacy Flight Recorder data" in err
    # Nothing was created at the target.
    assert not (hermes / "flight-recorder" / "outbox.sqlite").exists()


def test_explicit_hermes_homes_ignore_default_legacy_data(tmp_path):
    legacy = Path.home() / ".hermes-flight-recorder"
    legacy.mkdir()
    (legacy / "outbox.sqlite").write_text("legacy db")

    first = _hermes(tmp_path / "first")
    second = _hermes(tmp_path / "second")

    assert cli.main(["install", "--hermes-home", str(first)]) == 0
    assert cli.main(["install", "--hermes-home", str(second)]) == 0

    first_recorder = first / "flight-recorder"
    second_recorder = second / "flight-recorder"
    first_outbox = Outbox.open(first_recorder)
    second_outbox = Outbox.open(second_recorder)
    try:
        assert first_outbox.installation_id != second_outbox.installation_id
    finally:
        first_outbox.close()
        second_outbox.close()
    assert Path(
        baked_flight_recorder_home(first / "hooks" / "hermes-flight-recorder")
    ).resolve() == first_recorder.resolve()
    assert Path(
        baked_flight_recorder_home(second / "hooks" / "hermes-flight-recorder")
    ).resolve() == second_recorder.resolve()
