"""Native ``serve`` service registration (systemd user unit).

These drive :mod:`hermes_flight_recorder.collector.service` directly with a fake
command runner and a temporary unit directory, so nothing touches the real user
manager. The suite-wide ``_disable_native_service`` autouse fixture keeps every
*other* test from registering anything; here we force availability on to test
the enabled path.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from hermes_flight_recorder.collector import service


def _make_runner(respond=None):
    calls: list[list[str]] = []

    def runner(cmd):
        cmd = list(cmd)
        calls.append(cmd)
        rc, out, err = 0, "", ""
        if respond is not None:
            reply = respond(cmd)
            if reply is not None:
                rc, out, err = reply
        return subprocess.CompletedProcess(cmd, rc, out, err)

    runner.calls = calls  # type: ignore[attr-defined]
    return runner


@pytest.fixture
def systemd_home(tmp_path, monkeypatch):
    """Point unit files at a temp dir and force systemd availability on."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setattr(service, "systemd_user_available", lambda: True)
    return tmp_path


def test_unit_content_pins_homes_and_serve(tmp_path: Path) -> None:
    text = service.unit_content(tmp_path / "fr", tmp_path / "hermes")
    assert "ExecStart=" in text
    assert "serve" in text
    assert str(tmp_path / "fr") in text
    assert str(tmp_path / "hermes") in text
    assert "Restart=always" in text
    assert "WantedBy=default.target" in text
    # A user unit must not order against the system network target.
    assert "network-online.target" not in text


def test_register_writes_unit_and_enables(systemd_home: Path) -> None:
    linger_yes = lambda cmd: (0, "yes", "") if cmd[:2] == ["loginctl", "show-user"] else None
    runner = _make_runner(linger_yes)

    result = service.register_service(
        systemd_home / "fr", systemd_home / "hermes", runner=runner, log=lambda *_: None
    )

    assert result.changed and result.supported
    assert service.unit_path().exists()
    assert ["systemctl", "--user", "daemon-reload"] in runner.calls
    assert ["systemctl", "--user", "enable", "--now", service.SERVICE_NAME] in runner.calls
    # Linger already on → do not touch it.
    assert not any(c[:2] == ["loginctl", "enable-linger"] for c in runner.calls)


def test_register_enables_linger_when_absent(systemd_home: Path) -> None:
    linger_no = lambda cmd: (0, "no", "") if cmd[:2] == ["loginctl", "show-user"] else None
    runner = _make_runner(linger_no)

    service.register_service(
        systemd_home / "fr", systemd_home / "hermes", runner=runner, log=lambda *_: None
    )

    assert any(c[:3] == ["loginctl", "enable-linger"] or c[:2] == ["loginctl", "enable-linger"]
               for c in runner.calls)


def test_register_survives_linger_failure(systemd_home: Path) -> None:
    def respond(cmd):
        if cmd[:2] == ["loginctl", "show-user"]:
            return (0, "no", "")
        if cmd[:2] == ["loginctl", "enable-linger"]:
            return (1, "", "denied")
        return None

    logs: list[str] = []
    result = service.register_service(
        systemd_home / "fr", systemd_home / "hermes",
        runner=_make_runner(respond), log=logs.append,
    )

    assert result.detail == "enabled"  # linger failure does not fail registration
    assert any("linger" in line for line in logs)


def test_register_reports_enable_failure(systemd_home: Path) -> None:
    def respond(cmd):
        if cmd[:4] == ["systemctl", "--user", "enable", "--now"]:
            return (1, "", "unit failed to start")
        return None

    result = service.register_service(
        systemd_home / "fr", systemd_home / "hermes",
        runner=_make_runner(respond), log=lambda *_: None,
    )

    assert result.supported and "enable failed" in result.detail


def test_register_skips_when_unsupported(tmp_path, monkeypatch) -> None:
    # The autouse fixture sets HFR_SERVICE_DISABLED=1, so availability is off.
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    runner = _make_runner()

    result = service.register_service(
        tmp_path / "fr", tmp_path / "hermes", runner=runner, log=lambda *_: None
    )

    assert not result.supported and not result.changed
    assert runner.calls == []
    assert not (tmp_path / "config" / "systemd").exists()


def test_unregister_removes_unit_and_disables(systemd_home: Path) -> None:
    service.register_service(
        systemd_home / "fr", systemd_home / "hermes",
        runner=_make_runner(lambda c: (0, "yes", "") if c[:2] == ["loginctl", "show-user"] else None),
        log=lambda *_: None,
    )
    assert service.unit_path().exists()

    runner = _make_runner()
    result = service.unregister_service(runner=runner, log=lambda *_: None)

    assert result.changed
    assert not service.unit_path().exists()
    assert ["systemctl", "--user", "disable", "--now", service.SERVICE_NAME] in runner.calls
    assert ["systemctl", "--user", "daemon-reload"] in runner.calls


def test_unregister_is_noop_when_absent(systemd_home: Path) -> None:
    runner = _make_runner()
    result = service.unregister_service(runner=runner, log=lambda *_: None)

    assert not result.changed
    assert runner.calls == []
