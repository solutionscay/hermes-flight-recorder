# Ingestion protocol v1

_Status: frozen._

The ingestion protocol is the wire contract between the Bridge companion (the
client) and the hosted service (the server). The Bridge ships the events it
captured; the service ingests them into a durable, per-installation ledger.
This repository owns the contract, next to the envelope it carries
([`envelope-v1.md`](envelope-v1.md)). The companion is the client of this
contract. [`hermes-dbass`](https://github.com/solutionscay/hermes-dbass)
implements the server side of it.

The service reads **plaintext metadata only**. The encrypted content fields
(`content_ciphertext`, `content_nonce`, `content_hash`, `key_version`) pass
through opaque. The service never reads them.

## Endpoint

`POST /ingest`

- **Content-Type:** `application/json`
- **Transport:** HTTPS only.

## Request

The body is one JSON object with a `records` array:

```json
{
  "protocol_version": "1",
  "records": [ <envelope v1 record>, ... ]
}
```

| Field | Type | Required | Meaning |
|---|---|---|---|
| `protocol_version` | string | no | The protocol version. v1 is `"1"`. Advisory in v1; the server ignores it. Send it so a later server can route by version. |
| `records` | array | yes | A non-empty list of full envelope v1 records, in one batch. |

**Batch rules the client must keep:**

1. `records` is not empty.
2. Every record shares one `installation_id`. One batch is one installation.
3. Every record has a string `event_id` and a number `producer_sequence`.
4. The records are ordered by `producer_sequence`, ascending.
5. Each record is a full, valid envelope v1 record. The client validates it
   before it sends it. The encrypted content fields pass through unchanged.

Rules 1–3 are enforced by the server today. Rules 4–5 are the client's duty;
the server does not depend on them, but the ledger and the reconciler do.

**Batch size.** The client bounds a batch by record count and by byte size.
The recommended limits are 500 records or 1 MiB, whichever comes first. A
larger set becomes more than one batch, each a contiguous `producer_sequence`
range.

## Response

**`202 Accepted`** — the batch was ingested (fully or in part; a duplicate is
not an error):

```json
{ "accepted": 2, "duplicates": 1, "high_water": 148373 }
```

| Field | Type | Meaning |
|---|---|---|
| `accepted` | int | Records newly stored in this call. |
| `duplicates` | int | Records already present (matched an existing `event_id`). |
| `high_water` | int | The highest `producer_sequence` the server has durably stored for this installation, across all calls. |

Always `accepted + duplicates == records.length` on a 202.

**`400 Bad Request`** — the batch is malformed:

```json
{ "error": "bad_request", "message": "..." }
```

| `error` | Cause |
|---|---|
| `bad_request` | `records` is absent, is not an array, is empty, or mixes `installation_id` values. |
| `bad_record` | A record has no `event_id` or a non-numeric `producer_sequence`. |

**`401` / `403`** — the request did not pass authentication at the edge (see
Authentication). The Worker does not produce these; the edge does.

## Delivery model

- **At-least-once.** The client may send a batch more than once. This is safe.
- **Idempotent by `event_id`.** The server stores each `event_id` once
  (`INSERT OR IGNORE` on the `event_id` primary key). A re-sent record is a
  `duplicate`, never a second row and never an error.
- **Per-installation ledger.** The server keeps one ledger per
  `installation_id`. No query crosses installations, because there is no
  shared event table.

## Ordering and the delivery cursor

- **One ordered stream per `installation_id`,** keyed by `producer_sequence`
  (the envelope v1 rule). The client sends ascending, contiguous ranges.
- **The delivery cursor** is a client-side high-water mark: the highest
  `producer_sequence` the client knows the server has stored. It is separate
  from the outbox's own `producer_sequence` high-water and from the hook
  drain's byte-offset cursor.
- **Advance rule.** After a `202`, and only then, the client advances its
  delivery cursor to the batch's maximum `producer_sequence`. On any non-2xx,
  the client does not advance the cursor.
- **Resume.** After a Bridge restart, the client resumes from its persisted
  delivery cursor. It may also read the server's `high_water` and re-ship any
  range above the cursor; re-shipping is idempotent, so this never harms.

## Loss detection end to end

The server's `high_water` and the client's outbox high-water are the two ends
of one stream. When they agree, the network lost nothing. When the server is
behind, unshipped or lost batches remain, and the client re-ships from the
delivery cursor. A `producer_sequence` gap in the server ledger has the same
meaning as on the host: a lost capture. This is the network form of the
Phase 0 exit-gate guarantee.

## Authentication

v1 authenticates at the edge with **Cloudflare Access**, not with an
application field in the body.

- **Ingestion** (`/ingest`) uses an Access **service-token** policy. The sync
  client sends the `CF-Access-Client-Id` and `CF-Access-Client-Secret`
  headers. The client reads them from its own configuration; they never live
  in the Hermes home.
- **Console** uses an Access identity policy (SSO).

A per-installation API-key table is deferred until there is a real
multi-tenant need. The `installation_id` in the envelope is the tenant key
inside one authenticated account.

## Enrollment

v1 has no separate enrollment endpoint. The server registers an installation
the first time it ingests a batch, keyed on the envelope `installation_id`
(a UUID the client mints at `hermes-flight-recorder init`). Enrollment is a
side effect of the first successful `/ingest`.

## Failure and retry

| Class | Signals | Client action |
|---|---|---|
| Retryable | a network error, a timeout, `429`, or any `5xx` | Retry the same batch with exponential backoff and jitter. It is idempotent. Do not advance the cursor. |
| Auth | `401`, `403` | Stop. Fix the service-token configuration. Do not spin. |
| Terminal | `400` (`bad_request` / `bad_record`) | Stop and surface it. This is a client defect. Retrying the same body cannot help. |

The agent keeps working while sync fails. The outbox buffers; the events
catch up when the service returns.

## Golden batch

The example that a client builds, serializes, and a server parses:
[`tests/fixtures/golden_batch.json`](../../tests/fixtures/golden_batch.json).
It carries two contiguous envelope v1 records for one installation. Every
record validates under `envelope.validate`, the two share one
`installation_id`, and their `producer_sequence` values ascend — the exact
shape the server's `/ingest` accepts.

## Versioning

This document is frozen for v1. An additive change (a new optional field the
server may ignore) stays in v1. A change that alters the request or response
shape, the status codes, or the delivery guarantees is a new protocol
version, routed by `protocol_version`.
