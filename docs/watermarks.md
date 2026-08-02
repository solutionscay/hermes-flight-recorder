# Collector watermarks

Frequent collectors use the `WatermarkStore` interface. The interface has two
operations:

- `get_cursor(name)` reads a durable high-water mark.
- `set_cursor(name, value)` writes a durable high-water mark.

The outbox implements this interface. Collectors do not depend on its database
schema for watermark access.

Each incremental scan uses this sequence:

1. Read the durable watermark.
2. Subtract the configured overlap to get the lower bound.
3. Read the source high-water mark in one source database snapshot. The mark
   is clamped to the durable watermark, so a truncated or reset source cannot
   pull the scan range or the watermark backwards.
4. Query the bounded range with an indexed source column.
5. Append each result with a stable deduplication key.
6. Advance the watermark only after all results succeed.

`Watermark.high_water` and `Watermark.bounded_rows` in
`collector/watermark.py` own this mechanism. Collectors keep only their
column lists and row interpretation.

The overlap reads a small set of old rows again. Outbox deduplication makes
these reads safe. A failed pass keeps the old watermark, so the next pass reads
the same range again.

The current collectors use these watermarks:

| Collector | Source order | Overlap |
| --- | --- | ---: |
| State sessions | SQLite `rowid` plus tracked open sessions | 64 rows |
| State messages | `messages.id` | 32 rows |
| State model usage | Indexed `session_id` for watermarked sessions | Session overlap |
| State delegations | SQLite `rowid` | 32 rows |
| Hook invocation index | Outbox `producer_sequence` | 64 events |
| Cron executions | SQLite `rowid` | 32 rows |
| Kanban task events | `task_events.id` per board | 64 rows |
| Kanban task runs | `task_runs.id` per board | 64 rows |

The frequent reconcile pass checks current runtime health. It does not load the
complete outbox or complete source histories. The audit pass does those checks
on `reconcile.audit_interval_seconds`. Its default interval is 3600 seconds.

The scale tests count SQLite virtual-machine work. They compare short source
histories with histories of 50,000 rows. The polls read the same bounded ranges
after their watermarks.
