"""Register the recorder's ``serve`` runtime as a native OS service.

``serve`` is one long-lived process (capture + reconcile + sync); it only helps
if something keeps it running. This module registers it as a **systemd user
service** on Linux so a normal ``install``/``update`` yields continuous capture
and transmit that survives logout and reboot (via ``loginctl enable-linger``).

Scope today is systemd-on-Linux. Other platforms (launchd, Windows Service) are
not registered automatically: :func:`register_service` returns a skip result and
prints how to run ``serve`` yourself, so ``install`` never fails on them.

Every operation is idempotent. Set ``HFR_SERVICE_DISABLED=1`` to make the
manager a no-op (the test suite uses this so it never touches the real user
manager; it is also a supported opt-out for operators who manage ``serve``
themselves).
"""

from __future__ import annotations

import getpass
import os
import shlex
import shutil
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from . import atomic_file

SERVICE_NAME = "hermes-flight-recorder.service"
DISABLE_ENV = "HFR_SERVICE_DISABLED"
_UNIT_MODE = 0o644

# A user unit runs against the user manager, so it must NOT order against the
# system's network-online.target (unreachable from the user bus). serve retries
# sync on its own cadence and Restart=always covers a cold start before the
# network is up, so no ordering is needed.
_UNIT_TEMPLATE = """\
[Unit]
Description=Hermes Flight Recorder capture and transmit daemon
Documentation=https://github.com/solutionscay/hermes-flight-recorder

[Service]
Type=simple
Environment=PATH={path}
ExecStart={exec_start}
Restart=always
RestartSec=10
KillSignal=SIGINT
TimeoutStopSec=20

[Install]
WantedBy=default.target
"""

Runner = Callable[[Sequence[str]], "subprocess.CompletedProcess[str]"]


@dataclass(frozen=True)
class ServiceResult:
    """Outcome of a registration/unregistration attempt."""

    changed: bool  # did we write/enable or remove/disable anything
    supported: bool  # is a native service manager available on this host
    detail: str


def _default_runner(cmd: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(list(cmd), check=False, capture_output=True, text=True)


def _disabled() -> bool:
    return os.environ.get(DISABLE_ENV) == "1"


def systemd_user_available() -> bool:
    """Whether a systemd ``--user`` manager can be driven on this host."""
    if _disabled():
        return False
    if sys.platform != "linux":
        return False
    if not shutil.which("systemctl"):
        return False
    # systemctl --user needs a user bus; XDG_RUNTIME_DIR is its address.
    return bool(os.environ.get("XDG_RUNTIME_DIR"))


def unit_dir() -> Path:
    """Directory that holds this user's systemd unit files."""
    base = os.environ.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    return Path(base) / "systemd" / "user"


def unit_path() -> Path:
    return unit_dir() / SERVICE_NAME


def _serve_command(fr_home: Path, hermes_home: Path) -> list[str]:
    """The argv the unit runs: the installed console script when present, else
    the module entrypoint, always pinned to the resolved homes so the unit is
    unambiguous regardless of the caller's environment."""
    exe = shutil.which("hermes-flight-recorder")
    argv = [exe, "serve"] if exe else [sys.executable, "-m", "hermes_flight_recorder.cli", "serve"]
    argv += ["--flight-recorder-home", str(fr_home), "--hermes-home", str(hermes_home)]
    return argv


def unit_content(fr_home: Path, hermes_home: Path) -> str:
    """Render the unit file text for the given installation."""
    exec_start = " ".join(shlex.quote(part) for part in _serve_command(fr_home, hermes_home))
    path_env = os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin")
    return _UNIT_TEMPLATE.format(path=path_env, exec_start=exec_start)


def _ensure_linger(runner: Runner, *, log) -> None:
    """Best-effort: keep the service running when the user is not logged in.

    A failure here does not fail registration — the service still runs while the
    user has a session; only boot-without-login survival needs linger.
    """
    user = getpass.getuser()
    probe = runner(["loginctl", "show-user", user, "--property", "Linger", "--value"])
    if probe.returncode == 0 and probe.stdout.strip() == "yes":
        return
    result = runner(["loginctl", "enable-linger", user])
    if result.returncode != 0:
        log(
            "warning: could not enable linger; the recorder will run while you "
            f"are logged in but not across a reboot. Enable it with: "
            f"loginctl enable-linger {user}"
        )


def register_service(
    fr_home: str | os.PathLike[str],
    hermes_home: str | os.PathLike[str],
    *,
    runner: Runner | None = None,
    log=print,
) -> ServiceResult:
    """Install and enable the ``serve`` user service. Idempotent.

    Returns a :class:`ServiceResult`. On a host without a systemd user manager
    (or with ``HFR_SERVICE_DISABLED=1``) it registers nothing and prints how to
    run ``serve`` yourself, so callers never need to special-case the platform.
    """
    run = runner or _default_runner
    fr_home = Path(fr_home)
    hermes_home = Path(hermes_home)
    if not systemd_user_available():
        log(
            "no systemd user manager detected; the recorder will not transmit "
            "until a scheduler runs `hermes-flight-recorder serve`. Start it in "
            "the foreground, or wrap it in your platform's service manager."
        )
        return ServiceResult(changed=False, supported=False, detail="unsupported")

    path = unit_path()
    desired = unit_content(fr_home, hermes_home)
    existing = path.read_text(encoding="utf-8") if path.exists() else None
    if existing != desired:
        atomic_file.atomic_write(path, desired.encode("utf-8"), mode=_UNIT_MODE)

    reload_result = run(["systemctl", "--user", "daemon-reload"])
    if reload_result.returncode != 0:
        return ServiceResult(
            changed=existing != desired,
            supported=True,
            detail=f"daemon-reload failed: {reload_result.stderr.strip()}",
        )
    enable_result = run(["systemctl", "--user", "enable", "--now", SERVICE_NAME])
    if enable_result.returncode != 0:
        return ServiceResult(
            changed=True,
            supported=True,
            detail=f"enable failed: {enable_result.stderr.strip()}",
        )
    _ensure_linger(run, log=log)
    log(f"service enabled:      {path} (systemctl --user status {SERVICE_NAME})")
    return ServiceResult(changed=True, supported=True, detail="enabled")


def unregister_service(*, runner: Runner | None = None, log=print) -> ServiceResult:
    """Stop, disable, and remove the ``serve`` user service. Idempotent.

    A no-op when the unit file is absent, so it is safe to call from every
    uninstall regardless of whether a service was ever registered.
    """
    run = runner or _default_runner
    path = unit_path()
    if not path.exists():
        return ServiceResult(changed=False, supported=systemd_user_available(), detail="absent")
    if systemd_user_available():
        run(["systemctl", "--user", "disable", "--now", SERVICE_NAME])
    try:
        path.unlink()
    except OSError as exc:
        return ServiceResult(changed=False, supported=True, detail=f"remove failed: {exc}")
    if systemd_user_available():
        run(["systemctl", "--user", "daemon-reload"])
    log(f"service removed:      {path}")
    return ServiceResult(changed=True, supported=True, detail="removed")


__all__ = [
    "DISABLE_ENV",
    "SERVICE_NAME",
    "ServiceResult",
    "register_service",
    "systemd_user_available",
    "unit_content",
    "unit_path",
    "unregister_service",
]
