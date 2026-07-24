"""Hermes Flight Recorder — local-first event capture for Hermes agents.

Hermes Flight Recorder runs alongside a Hermes agent. It captures semantic execution
events, encrypts sensitive content on the host, buffers events in a
durable local outbox, and reconciles against Hermes's durable state so
the event stream is gap-detectable.

See the collector subpackage for the capture/reconcile components.
"""

# Keep this literal dependency-free: importing ``hermes_flight_recorder.envelope``
# must not pull version/source inspection or collector modules into SDK consumers.
__version__ = "0.1.0.dev0"
