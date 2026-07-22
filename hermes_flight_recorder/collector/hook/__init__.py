"""Live hook capture: an in-gateway spooler plus a Flight Recorder-side drain.

Two parts, with a durable file journal between them:

- The **spooler** (``handler.py``, installed under ``$HERMES_HOME/hooks/``)
  runs inside the Hermes gateway process. It is standard-library only: it
  appends one JSON line per lifecycle event to ``hook-spool.jsonl`` in the
  Flight Recorder home and never raises into the gateway. Agent message and
  response previews are removed because Hermes truncates them before hook
  delivery; complete content comes from ``state.db``. It imports nothing from
  this package, so it runs in whatever Python environment Hermes uses.
- The **drain** (:func:`drain`) runs in the Flight Recorder environment inside
  ``hermes-flight-recorder run``. It reads new spool lines after a stored
  byte-offset cursor, maps each event to an envelope v1 record, assigns the
  ``producer_sequence`` via the outbox, and
  appends with a dedup key.

This keeps the encryption key and the sequence authority in Hermes Flight Recorder, never
in Hermes. The hook is the fast, lossy live path; the state adapter and the
reconciler make the stream complete. See issue #4 for the rationale and the
transport-architecture research behind the spool-and-drain design.
"""

from __future__ import annotations

# The single installed hook lives under this directory name.
HOOK_DIR_NAME = "hermes-flight-recorder"
# All paths below are relative to the Flight Recorder home (never HERMES_HOME).
SPOOL_FILENAME = "hook-spool.jsonl"
ERRLOG_FILENAME = "hook-errors.log"
# The outbox meta cursor that records how far the drain has read.
CURSOR_NAME = "hook-spool"
# The Hermes gateway events the hook subscribes to.
HOOK_EVENTS = (
    "gateway:startup",
    "session:start",
    "session:end",
    "session:reset",
    "agent:start",
    "agent:end",
)

from .drain import drain  # noqa: E402  (constants above must exist first)
from .install import install_hook  # noqa: E402

__all__ = [
    "drain",
    "install_hook",
    "HOOK_DIR_NAME",
    "SPOOL_FILENAME",
    "ERRLOG_FILENAME",
    "CURSOR_NAME",
    "HOOK_EVENTS",
]
