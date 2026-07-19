"""Bridge — the local-first companion for Hermes Flight Recorder.

Bridge runs alongside a Hermes agent. It captures semantic execution
events, encrypts sensitive content on the host, buffers events in a
durable local outbox, and reconciles against Hermes's durable state so
the event stream is gap-detectable.

See the collector subpackage for the capture/reconcile components.
"""

__version__ = "0.0.0"
