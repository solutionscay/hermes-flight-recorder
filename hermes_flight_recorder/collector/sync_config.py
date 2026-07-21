"""Sync configuration: the ingestion endpoint and the edge credential.

The sync client needs the ingestion URL and a Cloudflare Access service
token (a client id and a client secret). Ingestion protocol v1 authenticates
at the edge with these two headers, not with a field in the request body.

The credential lives in the **Flight Recorder** home, next to the outbox and the
content key, and never under the Hermes home. A process may also supply any
field from the environment, which takes priority over the file so an operator
can inject a secret without writing it to disk:

- ``HFR_INGEST_URL``
- ``HFR_CF_ACCESS_CLIENT_ID``
- ``HFR_CF_ACCESS_CLIENT_SECRET``

The config file is ``sync-config.json`` and is written with mode ``0600``.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._common import default_flight_recorder_home

CONFIG_FILENAME = "sync-config.json"

_ENV_INGEST_URL = "HFR_INGEST_URL"
_ENV_CLIENT_ID = "HFR_CF_ACCESS_CLIENT_ID"
_ENV_CLIENT_SECRET = "HFR_CF_ACCESS_CLIENT_SECRET"

# Cloudflare Access service-token header names (protocol v1 §Authentication).
CF_ACCESS_CLIENT_ID_HEADER = "CF-Access-Client-Id"
CF_ACCESS_CLIENT_SECRET_HEADER = "CF-Access-Client-Secret"


class SyncConfigError(RuntimeError):
    """The sync configuration is absent or incomplete."""


@dataclass(frozen=True)
class SyncConfig:
    """The endpoint and edge credential for one installation's sync."""

    ingest_url: str
    cf_access_client_id: str
    cf_access_client_secret: str

    def auth_headers(self) -> dict[str, str]:
        """The Cloudflare Access headers this credential sends."""
        return {
            CF_ACCESS_CLIENT_ID_HEADER: self.cf_access_client_id,
            CF_ACCESS_CLIENT_SECRET_HEADER: self.cf_access_client_secret,
        }


def config_path(flight_recorder_home: str | os.PathLike[str] | None = None) -> Path:
    """The path of the sync config file inside the Flight Recorder home."""
    home = Path(flight_recorder_home).expanduser() if flight_recorder_home else default_flight_recorder_home()
    return home / CONFIG_FILENAME


def _read_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError) as exc:
        raise SyncConfigError(f"cannot read sync config at {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise SyncConfigError(f"sync config at {path} is not a JSON object")
    return data


def load(flight_recorder_home: str | os.PathLike[str] | None = None) -> SyncConfig:
    """Load the sync config, with the environment overriding the file.

    Raise :class:`SyncConfigError` when a required field is missing from both
    the environment and the file.
    """
    data = _read_file(config_path(flight_recorder_home))

    ingest_url = os.environ.get(_ENV_INGEST_URL) or data.get("ingest_url")
    client_id = os.environ.get(_ENV_CLIENT_ID) or data.get("cf_access_client_id")
    client_secret = os.environ.get(_ENV_CLIENT_SECRET) or data.get(
        "cf_access_client_secret"
    )

    missing = [
        name
        for name, value in (
            ("ingest_url", ingest_url),
            ("cf_access_client_id", client_id),
            ("cf_access_client_secret", client_secret),
        )
        if not value
    ]
    if missing:
        raise SyncConfigError(
            "sync config is incomplete; missing " + ", ".join(missing)
        )

    return SyncConfig(
        ingest_url=ingest_url,
        cf_access_client_id=client_id,
        cf_access_client_secret=client_secret,
    )


def save(
    config: SyncConfig, flight_recorder_home: str | os.PathLike[str] | None = None
) -> Path:
    """Write the sync config to the Flight Recorder home with mode ``0600``.

    The secret never leaves the Flight Recorder home. The file is created private and
    kept private on rewrite.
    """
    path = config_path(flight_recorder_home)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ingest_url": config.ingest_url,
        "cf_access_client_id": config.cf_access_client_id,
        "cf_access_client_secret": config.cf_access_client_secret,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    # Create the file private before any bytes land in it.
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, text.encode("utf-8"))
    finally:
        os.close(fd)
    os.chmod(path, 0o600)
    return path


__all__ = [
    "CF_ACCESS_CLIENT_ID_HEADER",
    "CF_ACCESS_CLIENT_SECRET_HEADER",
    "CONFIG_FILENAME",
    "SyncConfig",
    "SyncConfigError",
    "config_path",
    "load",
    "save",
]
