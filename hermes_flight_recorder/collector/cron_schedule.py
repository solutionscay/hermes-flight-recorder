"""Parse five-field cron schedules and expand their expected fire instants."""

from __future__ import annotations

import datetime
import math

CronFields = tuple[set[int], set[int], set[int], set[int], set[int]]


def expected_instants(
    expression: str | None,
    lower: float,
    upper: float,
    timezone: datetime.tzinfo | None = None,
) -> list[float]:
    """Return matching minute epochs, or an empty list for an invalid expression."""
    fields = _parse_cron(expression)
    return _cron_instants(fields, lower, upper, timezone) if fields is not None else []


def _parse_cron(expression: str | None) -> CronFields | None:
    """Parse a standard five-field cron expression into per-field integer sets."""
    if not isinstance(expression, str):
        return None
    parts = expression.split()
    if len(parts) != 5:
        return None
    bounds = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 6))
    try:
        return tuple(
            _parse_cron_field(part, low, high)
            for part, (low, high) in zip(parts, bounds)
        )
    except ValueError:
        return None


def _parse_cron_field(field: str, low: int, high: int) -> set[int]:
    values: set[int] = set()
    for token in field.split(","):
        step = 1
        if "/" in token:
            token, step_text = token.split("/", 1)
            step = int(step_text)
            if step <= 0:
                raise ValueError("step must be positive")
        if token in ("*", ""):
            start, end = low, high
        elif "-" in token:
            start_text, end_text = token.split("-", 1)
            start, end = int(start_text), int(end_text)
        else:
            start = end = int(token)
        if start < low or end > high or start > end:
            raise ValueError("field out of range")
        values.update(range(start, end + 1, step))
    return values


def _cron_instants(
    fields: CronFields,
    lower: float,
    upper: float,
    timezone: datetime.tzinfo | None = None,
) -> list[float]:
    """Expand parsed cron fields over an inclusive epoch range."""
    zone = timezone or datetime.timezone.utc
    minute, hour, day_of_month, month, day_of_week = fields
    dom_restricted = len(day_of_month) < 31
    dow_restricted = len(day_of_week) < 7
    instants: list[float] = []
    candidate = math.ceil(lower / 60.0) * 60.0
    while candidate <= upper:
        wall_time = datetime.datetime.fromtimestamp(candidate, zone)
        dom_hit = wall_time.day in day_of_month
        dow_hit = _dow(wall_time) in day_of_week
        if dom_restricted and dow_restricted:
            day_matches = dom_hit or dow_hit
        else:
            day_matches = (dom_hit or not dom_restricted) and (
                dow_hit or not dow_restricted
            )
        if (
            wall_time.minute in minute
            and wall_time.hour in hour
            and wall_time.month in month
            and day_matches
        ):
            instants.append(float(int(candidate)))
        candidate += 60.0
    return instants


def _dow(value: datetime.datetime) -> int:
    """Return cron day-of-week, where Sunday is zero."""
    return (value.weekday() + 1) % 7
