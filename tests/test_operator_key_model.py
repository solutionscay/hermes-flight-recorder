"""The operator-keyed content encryption model (issue #121).

Covers the seam between the outbox, the keystore, and the CLI: content is
sealed to a fleet operator public key, only the operator private key reads it,
and rotation is forward-only while old keys stay available to read history.
The cryptographic core itself is tested in ``test_content_crypto.py``.
"""

from __future__ import annotations

import base64
from pathlib import Path

import pytest

from hermes_flight_recorder import cli
from hermes_flight_recorder.collector import content_crypto as cc
from hermes_flight_recorder.collector import keystore
from hermes_flight_recorder.collector.outbox import Outbox, OutboxError


def base_record(event_type: str = "tool.call_completed") -> dict:
    return {
        "occurred_at": 1752861993.417,
        "tenant_id": "default",
        "profile": "default",
        "runtime": {"kind": "cli"},
        "correlation_id": "corr-1",
        "source": "state.db:messages",
        "capture_method": "poll:state.db:messages",
        "payload": {"event_type": event_type},
        "partial": False,
    }


def _open(home: Path) -> Outbox:
    ob = Outbox.open(home)
    ob.initialize()
    return ob


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("SC_HERMES_FLIGHT_RECORDER_HOME", raising=False)
    monkeypatch.delenv("HERMES_HOME", raising=False)


# --- solo bootstrap -----------------------------------------------------
def test_solo_write_mints_keypair_and_seals_dek(tmp_path):
    ob = _open(tmp_path)
    rec = ob.append(base_record(), content="secret tool args")

    # Both key halves are minted locally on first content write.
    assert keystore.has_public(tmp_path) and keystore.has_secret(tmp_path)

    # key_version ties the record to the operator key epoch, and the wrapped
    # DEK for it is stored, sealed to that operator key.
    public = keystore.load_public_key(tmp_path)
    assert rec["key_version"].startswith(public.key_id)
    row = ob._conn.execute(
        "SELECT operator_key_id, wrapped_dek FROM content_keys WHERE key_version=?",
        (rec["key_version"],),
    ).fetchone()
    assert row is not None and row[0] == public.key_id
    assert ob.decrypt_content(rec) == b"secret tool args"
    ob.close()


def test_dek_stays_in_memory_never_on_disk(tmp_path):
    ob = _open(tmp_path)
    ob.append(base_record(), content="in memory only")
    # Only the public/secret operator key files exist; no bare DEK file, and no
    # revival of the retired symmetric dev key.
    names = {p.name for p in tmp_path.iterdir()}
    assert "content-dev.key" not in names
    assert not any(n.endswith(".key") for n in names)
    ob.close()


# --- fleet posture ------------------------------------------------------
def test_fleet_agent_writes_but_cannot_read(tmp_path):
    operator = tmp_path / "operator"
    operator.mkdir()
    keypair = keystore.mint_operator_keypair(operator)

    agent = tmp_path / "agent"
    agent.mkdir()
    keystore.write_public_key(agent, keypair.public)

    ob = _open(agent)
    rec = ob.append(base_record(), content="fleet cannot read me")
    assert not keystore.has_secret(agent)  # no private key on the host

    # The host has no private key, so the sanctioned read path refuses.
    with pytest.raises(keystore.KeystoreError):
        ob.decrypt_content(rec)

    # The operator, holding the private key off-host, can read it.
    assert ob.decrypt_content(rec, keypair=keypair) == b"fleet cannot read me"
    ob.close()


def test_missing_wrapped_dek_is_a_clear_error(tmp_path):
    ob = _open(tmp_path)
    rec = ob.append(base_record(), content="x")
    ob._conn.execute("DELETE FROM content_keys")
    with pytest.raises(OutboxError, match="no wrapped data key"):
        ob.decrypt_content(rec)
    ob.close()


# --- epochs -------------------------------------------------------------
def test_each_process_seals_under_its_own_dek_all_readable(tmp_path):
    ob1 = _open(tmp_path)
    r1 = ob1.append(base_record(), content="epoch one")
    ob1.close()

    ob2 = _open(tmp_path)  # a new process/epoch reuses the operator key
    r2 = ob2.append(base_record(), content="epoch two")

    assert r1["key_version"] != r2["key_version"]  # distinct DEK epochs
    assert (
        ob2._conn.execute("SELECT COUNT(*) FROM content_keys").fetchone()[0] == 2
    )
    # One outbox reads content from both epochs.
    assert ob2.decrypt_content(r1) == b"epoch one"
    assert ob2.decrypt_content(r2) == b"epoch two"
    ob2.close()


# --- rotation -----------------------------------------------------------
def test_rotation_is_forward_only_and_retains_old_key(tmp_path):
    ob = _open(tmp_path)
    old_record = ob.append(base_record(), content="pre-rotation history")
    old_keypair = keystore.load_keypair(tmp_path)
    ob.close()

    new_keypair = keystore.mint_operator_keypair(tmp_path, rotate=True)
    assert new_keypair.key_id != old_keypair.key_id

    # The retired key is kept (never deleted) so old content stays readable.
    retired = keystore.retired_keypairs(tmp_path)
    assert [k.key_id for k in retired] == [old_keypair.key_id]

    ob2 = _open(tmp_path)
    new_record = ob2.append(base_record(), content="post-rotation")

    # New content seals to the new key; the current keypair reads it.
    assert new_record["key_version"].startswith(new_keypair.key_id)
    assert ob2.decrypt_content(new_record) == b"post-rotation"

    # Old content needs the key that sealed it (forward-only). The current key
    # cannot read it; the retained old key can.
    with pytest.raises(cc.CryptoError):
        ob2.decrypt_content(old_record)
    assert ob2.decrypt_content(old_record, keypair=old_keypair) == b"pre-rotation history"
    ob2.close()


# --- keystore custody guards -------------------------------------------
def test_mint_refuses_over_existing_without_rotate(tmp_path):
    keystore.mint_operator_keypair(tmp_path)
    with pytest.raises(keystore.KeystoreError, match="already exists"):
        keystore.mint_operator_keypair(tmp_path)


def test_write_public_refuses_when_private_present(tmp_path):
    keystore.mint_operator_keypair(tmp_path)
    other = cc.generate_operator_keypair().public
    with pytest.raises(keystore.KeystoreError, match="private key"):
        keystore.write_public_key(tmp_path, other)


def test_fleet_agent_cannot_mint_its_own_identity(tmp_path):
    keypair = cc.generate_operator_keypair()
    keystore.write_public_key(tmp_path, keypair.public)
    with pytest.raises(keystore.KeystoreError, match="fleet agent"):
        keystore.ensure_solo_keypair(tmp_path)


# --- CLI ----------------------------------------------------------------
def _hermes(tmp_path: Path) -> Path:
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    (hermes / "config.yaml").write_text("terminal:\n  home_mode: auto\n")
    return hermes


def test_keygen_mints_and_prints_public_key(tmp_path, capsys):
    fr = tmp_path / "fr"
    rc = cli.main(["keygen", "--flight-recorder-home", str(fr)])
    assert rc == 0
    out = capsys.readouterr().out
    assert keystore.has_secret(fr) and keystore.has_public(fr)
    # The printed public key parses and matches what was written.
    printed = [ln for ln in out.splitlines() if ln.startswith("hfr-operator-public-v1:")]
    assert printed
    assert cc.load_public_key(printed[0]).key_id == keystore.load_public_key(fr).key_id


def test_keygen_without_rotate_shows_existing_key(tmp_path, capsys):
    fr = tmp_path / "fr"
    assert cli.main(["keygen", "--flight-recorder-home", str(fr)]) == 0
    before = keystore.load_keypair(fr).key_id
    capsys.readouterr()

    assert cli.main(["keygen", "--flight-recorder-home", str(fr)]) == 0
    out = capsys.readouterr().out
    assert "already present" in out
    assert keystore.load_keypair(fr).key_id == before  # unchanged


def test_keygen_rotate_replaces_and_retires(tmp_path, capsys):
    fr = tmp_path / "fr"
    assert cli.main(["keygen", "--flight-recorder-home", str(fr)]) == 0
    old = keystore.load_keypair(fr).key_id
    capsys.readouterr()

    assert cli.main(["keygen", "--flight-recorder-home", str(fr), "--rotate"]) == 0
    new = keystore.load_keypair(fr).key_id
    assert new != old
    assert [k.key_id for k in keystore.retired_keypairs(fr)] == [old]


def test_install_operator_pubkey_makes_a_fleet_agent(tmp_path):
    operator = tmp_path / "operator"
    operator.mkdir()
    keypair = keystore.mint_operator_keypair(operator)
    pub_file = keystore.public_path(operator)

    hermes = _hermes(tmp_path)
    rc = cli.main(
        ["install", "--hermes-home", str(hermes), "--operator-pubkey", str(pub_file)]
    )
    assert rc == 0
    fr = hermes / "flight-recorder"
    assert keystore.has_public(fr)
    assert not keystore.has_secret(fr)  # fleet agent holds no private key
    assert keystore.load_public_key(fr).key_id == keypair.key_id
