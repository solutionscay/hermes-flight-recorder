"""Package and source revision identification.

The distribution version identifies the release line. ``direct_url.json``
identifies the exact VCS commit installed by pip/pipx; editable/local installs
fall back to the checkout's Git revision. No Git command runs at import time.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import asdict, dataclass
from importlib import metadata
from pathlib import Path
from urllib.parse import unquote, urlparse

from . import __version__ as PACKAGE_VERSION

PACKAGE_NAME = "hermes-flight-recorder"


@dataclass(frozen=True)
class VersionInfo:
    version: str
    revision: str | None
    requested_revision: str | None
    source: str | None
    dirty: bool = False

    @property
    def build(self) -> str:
        revision = self.revision[:12] if self.revision else "unknown"
        suffix = "-dirty" if self.dirty else ""
        return f"{self.version} ({revision}{suffix})"

    def to_dict(self) -> dict[str, object]:
        return asdict(self) | {"build": self.build}


def _distribution() -> metadata.Distribution | None:
    try:
        return metadata.distribution(PACKAGE_NAME)
    except metadata.PackageNotFoundError:
        return None


def _direct_url(dist: metadata.Distribution | None) -> dict[str, object]:
    if dist is None:
        return {}
    raw = dist.read_text("direct_url.json")
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _local_checkout(url: object) -> Path | None:
    if not isinstance(url, str):
        return None
    parsed = urlparse(url)
    if parsed.scheme != "file":
        return None
    return Path(unquote(parsed.path))


def _git_revision(checkout: Path) -> tuple[str | None, bool]:
    try:
        revision = subprocess.run(
            ["git", "-C", str(checkout), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "-C", str(checkout), "status", "--porcelain"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        )
    except (OSError, subprocess.CalledProcessError):
        return None, False
    return revision or None, dirty


def current_version() -> VersionInfo:
    dist = _distribution()
    direct = _direct_url(dist)
    vcs = direct.get("vcs_info")
    revision: str | None = None
    requested: str | None = None
    dirty = False
    if isinstance(vcs, dict):
        commit = vcs.get("commit_id")
        requested_value = vcs.get("requested_revision")
        revision = commit if isinstance(commit, str) and commit else None
        requested = (
            requested_value
            if isinstance(requested_value, str) and requested_value
            else None
        )
    if revision is None:
        checkout = _local_checkout(direct.get("url"))
        if checkout is None:
            checkout = Path(__file__).resolve().parents[1]
        revision, dirty = _git_revision(checkout)
    source = direct.get("url")
    return VersionInfo(
        version=PACKAGE_VERSION,
        revision=revision,
        requested_revision=requested,
        source=source if isinstance(source, str) else None,
        dirty=dirty,
    )


def build_identity() -> str:
    """Return the package/hook identity shown by status."""
    return current_version().build


__all__ = [
    "PACKAGE_NAME",
    "PACKAGE_VERSION",
    "VersionInfo",
    "build_identity",
    "current_version",
]
