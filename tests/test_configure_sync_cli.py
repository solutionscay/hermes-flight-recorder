"""The ``configure-sync`` command (issue #102).

Writes/updates the private ``sync-config.json`` holding the DBaaS ingest
endpoint and the Cloudflare Access service token, keeping the secret out of
shell history and out of stdout. Partial updates preserve untouched fields.
"""

from __future__ import annotations

import json
import os
import stat

import pytest

from hermes_flight_recorder import cli
from hermes_flight_recorder.collector import sync_config


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in (
        "SC_HERMES_FLIGHT_RECORDER_HOME",
        "HERMES_HOME",
        "HFR_INGEST_URL",
        "HFR_CF_ACCESS_CLIENT_ID",
        "HFR_CF_ACCESS_CLIENT_SECRET",
    ):
        monkeypatch.delenv(var, raising=False)


def _read(fr):
    return json.loads((fr / sync_config.CONFIG_FILENAME).read_text())


def test_env_secret_and_default_hosted_url(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("HFR_CF_ACCESS_CLIENT_SECRET", "s3cret")
    rc = cli.main(
        ["configure-sync", "--flight-recorder-home", str(tmp_path), "--client-id", "id-1.access"]
    )
    assert rc == 0
    data = _read(tmp_path)
    assert data["ingest_url"] == sync_config.HOSTED_INGEST_URL
    assert data["cf_access_client_id"] == "id-1.access"
    assert data["cf_access_client_secret"] == "s3cret"

    if os.name == "posix":
        mode = stat.S_IMODE((tmp_path / sync_config.CONFIG_FILENAME).stat().st_mode)
        assert mode == 0o600

    out = capsys.readouterr().out
    assert "s3cret" not in out  # secret never printed
    assert "id-1.ac" in out  # client id shown (redacted)


def test_explicit_flags(tmp_path):
    rc = cli.main(
        [
            "configure-sync",
            "--flight-recorder-home",
            str(tmp_path),
            "--ingest-url",
            "https://ingest.example.com/ingest",
            "--client-id",
            "cid.access",
            "--client-secret",
            "flagsecret",
        ]
    )
    assert rc == 0
    data = _read(tmp_path)
    assert data["ingest_url"] == "https://ingest.example.com/ingest"
    assert data["cf_access_client_secret"] == "flagsecret"


def test_partial_update_preserves_fields(tmp_path):
    sync_config.save(
        sync_config.SyncConfig("https://old/ingest", "keepid.access", "keepsecret"),
        tmp_path,
    )
    rc = cli.main(
        [
            "configure-sync",
            "--flight-recorder-home",
            str(tmp_path),
            "--ingest-url",
            "https://new.example.com/ingest",
        ]
    )
    assert rc == 0
    data = _read(tmp_path)
    assert data["ingest_url"] == "https://new.example.com/ingest"
    assert data["cf_access_client_id"] == "keepid.access"
    assert data["cf_access_client_secret"] == "keepsecret"


def test_missing_fields_refused_and_nothing_written(tmp_path, capsys):
    rc = cli.main(
        [
            "configure-sync",
            "--flight-recorder-home",
            str(tmp_path),
            "--ingest-url",
            "https://x/ingest",
        ]
    )
    assert rc == 2
    err = capsys.readouterr().err
    assert "cf_access_client_id" in err and "cf_access_client_secret" in err
    assert not (tmp_path / sync_config.CONFIG_FILENAME).exists()


def test_secret_from_stdin(tmp_path, monkeypatch, capsys):
    import io

    monkeypatch.setattr("sys.stdin", io.StringIO("stdinsecret\n"))
    rc = cli.main(
        [
            "configure-sync",
            "--flight-recorder-home",
            str(tmp_path),
            "--client-id",
            "cid.access",
            "--client-secret-stdin",
        ]
    )
    assert rc == 0
    assert _read(tmp_path)["cf_access_client_secret"] == "stdinsecret"


def test_legacy_hostname_normalized(tmp_path):
    rc = cli.main(
        [
            "configure-sync",
            "--flight-recorder-home",
            str(tmp_path),
            "--ingest-url",
            "https://app.hermesdbass.com/ingest",
            "--client-id",
            "cid.access",
            "--client-secret",
            "x",
        ]
    )
    assert rc == 0
    assert _read(tmp_path)["ingest_url"] == "https://app.hermesdbaas.com/ingest"


def test_plaintext_url_warns_but_writes(tmp_path, capsys):
    rc = cli.main(
        [
            "configure-sync",
            "--flight-recorder-home",
            str(tmp_path),
            "--ingest-url",
            "http://localhost:8080/ingest",
            "--client-id",
            "cid.access",
            "--client-secret",
            "x",
        ]
    )
    assert rc == 0
    assert "plaintext http" in capsys.readouterr().err
    assert _read(tmp_path)["ingest_url"] == "http://localhost:8080/ingest"
