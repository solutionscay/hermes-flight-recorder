"""Shared outbox exceptions."""


class OutboxError(RuntimeError):
    pass


__all__ = ["OutboxError"]
