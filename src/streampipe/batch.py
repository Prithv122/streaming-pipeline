"""Offline ground truth: group the whole event log by (zone, window) with no streaming state."""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass

from streampipe.events import OrderEvent, to_cents


@dataclass(frozen=True)
class BatchWindow:
    orders: int
    revenue_cents: int


def window_start_of(event_time: float, window_s: float = 60.0) -> float:
    """Start of the tumbling window `[start, start + window_s)` containing `event_time`."""
    return math.floor(event_time / window_s) * window_s


def batch_windows(
    events: Iterable[OrderEvent], window_s: float = 60.0
) -> dict[tuple[str, float], BatchWindow]:
    """Count and revenue (integer cents) per (zone, window_start) over every event, any order."""
    orders: dict[tuple[str, float], int] = {}
    cents: dict[tuple[str, float], int] = {}
    for e in events:
        key = (e.zone, window_start_of(e.event_time, window_s))
        orders[key] = orders.get(key, 0) + 1
        cents[key] = cents.get(key, 0) + to_cents(e.amount)
    return {k: BatchWindow(orders[k], cents[k]) for k in orders}
