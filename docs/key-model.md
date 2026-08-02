# Operator-keyed content encryption

The Flight Recorder encrypts sensitive content on the host before it is ever
written to disk or shipped. This document describes the key model: who holds
which key, how content is sealed and read back, how installs choose a posture,
how rotation works, and the one limitation the model does not hide.

The cryptographic primitives live in
[`content_crypto.py`](../hermes_flight_recorder/collector/content_crypto.py);
on-disk custody lives in
[`keystore.py`](../hermes_flight_recorder/collector/keystore.py).

## Why not one symmetric key per host

The earlier model minted one self-generated **symmetric** content key per
installation (`content-dev.key`). It could never be rotated, every host held a
key that could read its own history, and a fleet of 100 agents meant 100
independent keys with no fleet-wide read. The operator key model replaces it and
keeps the zero-knowledge property: the backend (DBaaS) never decrypts anything.
Server-side decryption was explicitly rejected. It would make the backend a
plaintext store for the secrets that this model protects. The recorder detects
sensitive data on the host at capture time.

## The three keys

- **Operator keypair (KEK).** One asymmetric **X25519** keypair per fleet. The
  operator mints it locally with `keygen`. The **public** half is distributed to
  every agent and is safe to hand out (even by an untrusted backend). The
  **private** half stays with the operator and never touches an agent host or
  the server. It is the one key that decrypts the fleet.
- **Data key (DEK).** A random **AES-256-GCM** key that actually encrypts
  content, minted per writing process / epoch and held **in memory only** — it
  is never written to disk.
- **Wrapped DEK.** The DEK sealed to the operator public key (an HPKE-style
  sealed box: ephemeral X25519 + HKDF-SHA256 + AES-256-GCM). It is stored
  out-of-band as an opaque blob keyed by `key_version`, in the outbox's
  `content_keys` table. Only the operator private key can open it.

The recorder also creates `secret-scan.key`. This file contains 32 random
bytes and has mode `0600`. It does not decrypt content. The recorder uses it to
calculate local HMAC fingerprints for detected secrets. The same secret gets
the same fingerprint on one installation. It gets a different fingerprint on
another installation.

The secret scan key never syncs. An update backup keeps it with the operator
key files and the local suppression baseline. An uninstall keeps it unless the
operator uses `--purge-data`.

Writing needs only the **public** key. Reading needs the **private** key. An
agent host holds only the public key and its in-memory DEK, so a compromised
host can write new content but cannot read the fleet's history.

## How a record is sealed and read

1. On the first content write of a process, the outbox mints a DEK, seals it to
   the operator public key, and records the wrapped DEK in `content_keys` under
   a fresh `key_version` (`<operator_key_id>#<dek_epoch>`).
2. Content is encrypted with the DEK. The record carries the frozen envelope v1
   content fields — `content_ciphertext`, `content_nonce`, `content_hash`,
   `key_version` — unchanged in meaning. The wrapped DEK travels separately (the
   `content_keys` stream), so the backend only ever stores and serves an opaque
   blob.
3. To read, a holder of the operator private key looks up the wrapped DEK for
   the record's `key_version`, unwraps it, and decrypts the content.
   `decrypt_content(record, keypair=None)` and `get_blob(hash, keypair=None)`
   default to the solo keystore private key; pass an explicit `keypair` to
   decrypt with a key held off-host (the operator console).

## Solo vs fleet (install behavior)

The posture is chosen by what `install` is given, not by a mode flag:

- **Solo** — `install` with no key. A keypair is minted locally; both halves are
  written (`operator.pub` at `0644`, `operator.secret` at `0600`). The box can
  decrypt its own outbox. Zero-config and local-first, matching the old
  experience. If a sync config is already present, `install` warns: a host-held
  private key defeats the compromised-host property.
- **Fleet agent** — `install --operator-pubkey <file>`. Only `operator.pub` is
  written; no private key ever lands on the host. The operator keeps the private
  key on their console.

```bash
# Operator: mint the fleet keypair once, then distribute the public key.
hermes-flight-recorder keygen --hermes-home "<OPERATOR_HOME>"

# Each agent: install sealing to that public key, no private key on the host.
hermes-flight-recorder install --hermes-home "<HERMES_HOME>" \
  --operator-pubkey /path/to/operator.pub
```

### Solo → fleet promotion

A solo box holds a private key. To convert it into a fleet agent, remove its
`operator.secret` deliberately and re-run `install --operator-pubkey`
(`install`/`write_public_key` refuse to overwrite a live private key silently,
so stranding a host's own data cannot happen by accident). Keep a copy of the
removed private key if that box wrote content you still need to read.

## Rotation

Rotation is **forward-only**. `keygen --rotate` retires the current keypair —
both halves are copied under `retired-keys/<timestamp>/` and **never deleted**.
The private file is the commit record for each key pair. If a process stops
before it replaces the public file, the recorder rebuilds that file from the
private key on the next load. The current pair stays in place until the retired
copy is durable. The command then mints a fresh keypair as current. New content
seals to the new key. Old content stays sealed to the old key.

Retaining old private keys to read history is correct, not a weakness: anyone
who already had the old key and the old ciphertext could read it, so
re-encrypting buys nothing. Reading pre-rotation history uses the key that
sealed it — pass the retained keypair explicitly (`decrypt_content(rec,
keypair=old)`); `content_keys.operator_key_id` tells tooling which retired key a
record needs. A later operator-side **re-wrap** tool can re-seal the tiny DEKs
to the new key so one current private key reads all history without
re-encrypting content. That tool is out of scope here.

## Honest limitation

At-rest theft of a host yields nothing: no decrypting key material is on disk (a
fleet agent has no private key at all; a solo box's private key opens only its
own outbox). The real limitation is a **live** compromise of a running writer:
because the current DEK lives in that process's memory, an attacker who
compromises the live process can read that run's in-memory DEK and therefore the
content of the current run — not the fleet's history. This is documented, not
hidden. The mitigation is the fleet posture (no private key on the host) plus
ordinary process isolation.

## Shipping to the backend

The wrapped DEKs are a small **keyless side-channel**, shipped out of band from
events. Each sync pass, after delivering events, the agent ships any
`content_keys` rows it has not yet had acknowledged to the ingestion service's
wrapped-DEK endpoint (`POST <ingest_url>/keys`, under the same Cloudflare Access
service token), then records a durable `shipped_at` so a later pass skips them.
Delivery is idempotent server-side by `(installation_id, key_version)`, so a
resend after a lost ack is harmless, and it is best-effort: an offline or auth
failure just leaves the rows for the next pass without affecting event delivery.
The backend stores and serves each `wrapped_dek` as an opaque blob and never
unwraps it (hermes-dbaas #62); the operator console fetches the set and unwraps
client-side with the operator private key (hermes-dbaas #63).

## Greenfield

There are no users and no migration shims. Agents are reinstalled under this
model and old installations (including any `content-dev.key`) are deleted.
