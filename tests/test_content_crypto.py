"""Tests for the operator-keyed content crypto core.

These pin the security-critical properties the fleet key model rests on:
a public key cannot decrypt, a wrong operator key cannot open a wrap,
tampering is detected, and the serialization round-trips without leaking a
private key where a public one is expected.
"""

from __future__ import annotations

import pytest

from hermes_flight_recorder.collector import content_crypto as cc


def test_seal_round_trip():
    kp = cc.generate_operator_keypair()
    plaintext = b"the quick brown fox"
    sealed = cc.seal(kp.public, plaintext)
    assert cc.unseal(kp, sealed) == plaintext
    # The plaintext must not appear in the sealed bytes.
    assert plaintext not in sealed


def test_dek_wrap_round_trip():
    kp = cc.generate_operator_keypair()
    dek = cc.generate_dek()
    wrapped = cc.unwrap_dek(kp, cc.wrap_dek(kp.public, dek))
    assert wrapped == dek


def test_content_round_trip_via_dek():
    dek = cc.generate_dek()
    ciphertext, nonce, content_hash = cc.encrypt_content(dek, "prompt: hello")
    assert cc.decrypt_content(dek, ciphertext, nonce) == b"prompt: hello"
    assert content_hash.startswith("sha256:")


def test_full_envelope_flow():
    """One operator key, a per-epoch DEK, content sealed for the fleet."""
    operator = cc.generate_operator_keypair()
    dek = cc.generate_dek()
    wrapped_dek = cc.wrap_dek(operator.public, dek)

    ciphertext, nonce, _ = cc.encrypt_content(dek, "tool output with a secret")

    # The reader only has the operator private key + stored blobs.
    recovered_dek = cc.unwrap_dek(operator, wrapped_dek)
    assert cc.decrypt_content(recovered_dek, ciphertext, nonce) == b"tool output with a secret"


def test_wrong_operator_key_cannot_unwrap():
    kp = cc.generate_operator_keypair()
    other = cc.generate_operator_keypair()
    wrapped = cc.wrap_dek(kp.public, cc.generate_dek())
    with pytest.raises(cc.CryptoError):
        cc.unwrap_dek(other, wrapped)


def test_tampered_seal_is_rejected():
    kp = cc.generate_operator_keypair()
    sealed = bytearray(cc.seal(kp.public, b"payload"))
    sealed[-1] ^= 0x01  # flip a ciphertext bit
    with pytest.raises(cc.CryptoError):
        cc.unseal(kp, bytes(sealed))


def test_tampered_content_is_rejected():
    dek = cc.generate_dek()
    ciphertext, nonce, _ = cc.encrypt_content(dek, "hello")
    bad = bytearray(ciphertext)
    bad[0] ^= 0x01
    with pytest.raises(cc.CryptoError):
        cc.decrypt_content(dek, bytes(bad), nonce)


def test_key_id_is_stable_and_public_derived():
    kp = cc.generate_operator_keypair()
    assert kp.key_id == kp.public.key_id
    # Reloading the public key yields the same fingerprint.
    reloaded = cc.load_public_key(kp.public.to_text())
    assert reloaded.key_id == kp.key_id
    assert kp.key_id.startswith("opk1:")


def test_public_key_text_round_trip():
    kp = cc.generate_operator_keypair()
    text = kp.public.to_text()
    assert text.startswith("hfr-operator-public-v1:")
    reloaded = cc.load_public_key(text)
    # A DEK wrapped to the reloaded public key opens with the original private.
    dek = cc.generate_dek()
    assert cc.unwrap_dek(kp, cc.wrap_dek(reloaded, dek)) == dek


def test_keypair_secret_text_round_trip():
    kp = cc.generate_operator_keypair()
    text = kp.secret_to_text()
    assert text.startswith("hfr-operator-secret-v1:")
    reloaded = cc.load_keypair(text)
    assert reloaded.key_id == kp.key_id


def test_loading_private_key_as_public_is_refused():
    kp = cc.generate_operator_keypair()
    with pytest.raises(cc.CryptoError, match="private key"):
        cc.load_public_key(kp.secret_to_text())


def test_loading_public_key_as_keypair_is_refused():
    kp = cc.generate_operator_keypair()
    with pytest.raises(cc.CryptoError):
        cc.load_keypair(kp.public.to_text())


def test_malformed_inputs_raise_crypto_error():
    with pytest.raises(cc.CryptoError):
        cc.load_public_key("garbage")
    with pytest.raises(cc.CryptoError):
        cc.load_public_key("hfr-operator-public-v1:!!!not-base64!!!")
    kp = cc.generate_operator_keypair()
    with pytest.raises(cc.CryptoError):
        cc.unseal(kp, b"\x01short")


def test_rotation_rewrap_keeps_dek():
    """Operator-key rotation re-wraps the DEK without touching content."""
    old = cc.generate_operator_keypair()
    new = cc.generate_operator_keypair()
    dek = cc.generate_dek()
    ciphertext, nonce, _ = cc.encrypt_content(dek, "history stays readable")

    wrapped_old = cc.wrap_dek(old.public, dek)
    # Re-wrap: open with the old private key, seal to the new public key.
    rewrapped = cc.wrap_dek(new.public, cc.unwrap_dek(old, wrapped_old))

    # The new operator key now reads old content; the content was never re-encrypted.
    dek_via_new = cc.unwrap_dek(new, rewrapped)
    assert cc.decrypt_content(dek_via_new, ciphertext, nonce) == b"history stays readable"
