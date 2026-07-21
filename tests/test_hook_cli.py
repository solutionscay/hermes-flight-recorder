"""Tests for the hook's CLI wiring: ``init`` install and ``run`` drain (issue #4).

Drive ``hermes_flight_recorder.cli.main`` end to end. ``init`` must create the
outbox AND install the hook under the given ``--hermes-home``, print the
install line, honor ``--force`` on reinstall, and never touch anything else
under the Hermes home. ``run`` must drain the hook spool into the outbox
(alongside the durable-store polls) and report the counts.

Every case passes an explicit ``--hermes-home`` so the real ``~/.hermes`` is
never touched.
"""

from __future__ import annotations

import json
from pathlib import Path

from hermes_flight_recorder import cli
from hermes_flight_recorder.collector.hook import SPOOL_FILENAME
from hermes_flight_recorder.collector.outbox import Outbox


def test_init_installs_the_hook(tmp_path: Path, capsys) -> None:
    bridge, hermes = tmp_path / "bridge", tmp_path / "hermes"
    rc = cli.main(["init", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes)])
    assert rc == 0
    hook_dir = hermes / "hooks" / "hermes-flight-recorder"
    assert (hook_dir / "HOOK.yaml").exists() and (hook_dir / "handler.py").exists()
    out = capsys.readouterr().out
    assert "hook installed:" in out and "restart the Hermes gateway" in out


def test_init_twice_reports_already_installed(tmp_path: Path, capsys) -> None:
    bridge, hermes = tmp_path / "bridge", tmp_path / "hermes"
    args = ["init", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes)]
    cli.main(args)
    capsys.readouterr()
    assert cli.main(args) == 0
    assert "already installed" in capsys.readouterr().out


def test_init_force_reinstalls(tmp_path: Path, capsys) -> None:
    bridge, hermes = tmp_path / "bridge", tmp_path / "hermes"
    args = ["init", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes)]
    cli.main(args)
    capsys.readouterr()
    assert cli.main(args + ["--force"]) == 0
    assert "hook installed:" in capsys.readouterr().out


def test_run_drains_the_hook_spool(tmp_path: Path, capsys) -> None:
    bridge, hermes = tmp_path / "bridge", tmp_path / "hermes"
    cli.main(["init", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes)])
    capsys.readouterr()

    # Simulate the gateway having spooled two events.
    spool_lines = [
        {"event_type": "gateway:startup", "context": {"platforms": ["cli"]}, "captured_at": 1.0},
        {
            "event_type": "session:start",
            "context": {"session_id": "s1", "session_key": "k1"},
            "captured_at": 2.0,
        },
    ]
    (bridge / SPOOL_FILENAME).write_text("\n".join(json.dumps(x) for x in spool_lines) + "\n")

    rc = cli.main(["run", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "runtime.gateway_started: 1" in out
    assert "session.created: 1" in out

    ob = Outbox.open(bridge)
    assert {e["payload"]["event_type"] for e in ob.iter_events()} == {
        "runtime.gateway_started",
        "session.created",
    }
    ob.close()


def test_run_without_spool_is_clean(tmp_path: Path, capsys) -> None:
    bridge, hermes = tmp_path / "bridge", tmp_path / "hermes"
    cli.main(["init", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes)])
    capsys.readouterr()
    rc = cli.main(["run", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes)])
    assert rc == 0  # no spool, missing state.db/cron: still a clean pass
