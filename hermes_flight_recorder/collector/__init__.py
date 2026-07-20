"""Collector — capture Hermes events, buffer them, and reconcile.

Components:

- ``outbox``:    durable local SQLite queue with a monotonic
                 producer_sequence
- ``hook``:      in-gateway spooler plus a Bridge-side drain for live
                 lifecycle capture
- ``state_db``:  adapter that reads Hermes ``state.db`` into
                 canonical events
- ``cron_db``:   adapter that reads the cron execution store
- ``reconcile``: diff the durable stores against the outbox to detect
                 gaps, missing terminals, and missed cron runs
- ``sync``:      batch pending outbox events for an acknowledged transport
"""
