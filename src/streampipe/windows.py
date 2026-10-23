"""Tumbling-window core: one lateness budget, two emission policies (wait, revise).

Semantics are specified in GUIDELINES.md -> Windowing semantics.
"""

from __future__ import annotations

import heapq
import math
from dataclasses import asdict, dataclass
from typing import Any, Literal

from streampipe.batch import window_start_of
from streampipe.events import OrderEvent, to_cents

Mode = Literal["wait", "revise"]


@dataclass(frozen=True, slots=True)
class WindowResult:
    window_start: float
    window_end: float
    zone: str
    orders: int
    revenue_cents: int
    revenue: float
    late_dropped: int
    revision: int
    final: bool
    emitted_at: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class LateDrop:
    zone: str
    window_start: float
    order_id: str
    event_time: float
    dropped_at: float


class _State:
    """Running aggregate of one open window; `records` counts the records emitted so far."""

    __slots__ = ("cents", "orders", "records")

    def __init__(self) -> None:
        self.orders = 0
        self.cents = 0
        self.records = 0


class WindowCore:
    """Tumbling windows per zone with a lateness budget and a wait or revise emission policy.

    Open windows live in a dict keyed by (zone, window_start). Two heaps ordered by
    (window_end, zone) find what to fire: one for windows that will close, one (revise mode
    only) for windows that have not become due yet. Firing costs O(fired * log open), not a scan.
    """

    def __init__(self, mode: Mode, lateness_s: float, window_s: float = 60.0) -> None:
        if mode not in ("wait", "revise"):
            raise ValueError(f"mode must be 'wait' or 'revise', got {mode!r}")
        if lateness_s < 0:
            raise ValueError(f"lateness_s must be >= 0, got {lateness_s}")
        if window_s <= 0:
            raise ValueError(f"window_s must be > 0, got {window_s}")
        self.mode: Mode = mode
        self.lateness_s = float(lateness_s)
        self.window_s = float(window_s)
        self.dropped: list[LateDrop] = []
        self.peak_open_windows = 0
        self._revise = mode == "revise"
        self._max_et = -math.inf
        self._open: dict[tuple[str, float], _State] = {}
        self._closing: list[tuple[float, str, float]] = []
        self._not_due: list[tuple[float, str, float]] = []
        self._drop_counts: dict[str, int] = {}

    @property
    def open_windows(self) -> int:
        return len(self._open)

    @property
    def max_event_time(self) -> float | None:
        """M, the largest event time accepted so far; None before the first event."""
        return None if self._max_et == -math.inf else self._max_et

    def process(self, event: OrderEvent, now: float) -> list[WindowResult]:
        """Apply one event arriving at stream time `now`; return the records it triggers."""
        zone = event.zone
        start = window_start_of(event.event_time, self.window_s)
        end = start + self.window_s
        lateness = self.lateness_s
        m = self._max_et
        out: list[WindowResult] = []

        if m >= end + lateness:
            self.dropped.append(LateDrop(zone, start, event.order_id, event.event_time, now))
            self._drop_counts[zone] = self._drop_counts.get(zone, 0) + 1
            return out

        key = (zone, start)
        state = self._open.get(key)
        if state is None:
            state = self._open[key] = _State()
            heapq.heappush(self._closing, (end, zone, start))
            if self._revise and m < end and lateness > 0:
                heapq.heappush(self._not_due, (end, zone, start))
            if len(self._open) > self.peak_open_windows:
                self.peak_open_windows = len(self._open)
        state.orders += 1
        state.cents += to_cents(event.amount)
        if self._revise and m >= end:
            out.append(self._record(zone, start, end, state, now, final=False))

        if event.event_time > m:
            m = self._max_et = event.event_time

        closing = self._closing
        while closing and closing[0][0] + lateness <= m:
            c_end, c_zone, c_start = heapq.heappop(closing)
            c_state = self._open.pop((c_zone, c_start))
            out.append(self._record(c_zone, c_start, c_end, c_state, now, final=True))
        if self._revise:
            not_due = self._not_due
            while not_due and not_due[0][0] <= m:
                d_end, d_zone, d_start = heapq.heappop(not_due)
                d_state = self._open.get((d_zone, d_start))
                if d_state is not None:
                    out.append(self._record(d_zone, d_start, d_end, d_state, now, final=False))
        return out

    def flush(self, now: float) -> list[WindowResult]:
        """End of stream: a final record for every open window, in (window_end, zone) order."""
        w = self.window_s
        keys = sorted(self._open, key=lambda k: (k[1] + w, k[0]))
        out = [
            self._record(zone, start, start + w, self._open[(zone, start)], now, final=True)
            for zone, start in keys
        ]
        self._open.clear()
        self._closing.clear()
        self._not_due.clear()
        return out

    def _record(
        self, zone: str, start: float, end: float, state: _State, now: float, *, final: bool
    ) -> WindowResult:
        rec = WindowResult(
            window_start=start,
            window_end=end,
            zone=zone,
            orders=state.orders,
            revenue_cents=state.cents,
            revenue=state.cents / 100,
            late_dropped=self._drop_counts.get(zone, 0),
            revision=state.records,
            final=final,
            emitted_at=now,
        )
        state.records += 1
        return rec
