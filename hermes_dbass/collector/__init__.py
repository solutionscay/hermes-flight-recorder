"""Collector — capture Hermes events, buffer them, and reconcile.

Components land across Phase 0:

- ``hook``:      Hermes event hook that feeds the outbox            (Step 3)
- ``outbox``:    durable local SQLite queue with a monotonic
                 producer_sequence                                  (Step 4)
- ``state_db``:  adapter that reads Hermes ``state.db`` into
                 canonical events                                   (Step 5)
- ``reconcile``: diff ``state.db`` against the outbox to detect
                 dropped events (gaps)                              (Step 6)

Nothing is implemented yet; this package marks the layout so each
step lands in a predictable place.
"""
