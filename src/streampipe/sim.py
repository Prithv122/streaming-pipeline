"""Replay an arrival-ordered log through a window core and collect everything it emitted."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from streampipe.events import Arrival
from streampipe.windows import LateDrop, WindowCore, WindowResult


@dataclass(frozen=True, slots=True)
class SimResult:
    """Everything one run produced. `flushed` is the subset of `records` emitted by the flush."""

    records: list[WindowResult]
    flushed: list[WindowResult]
    dropped: list[LateDrop]
    peak_open_windows: int
    events_in: int
    mode: str
    lateness_s: float


def simulate(
    arrivals: Iterable[Arrival],
    mode: str,
    lateness_s: float,
    window_s: float = 60.0,
    core_factory: Callable[..., Any] = WindowCore,
) -> SimResult:
    """Feed arrivals (already in arrival order) to a fresh core, then flush at the last arrival."""
    core = core_factory(mode, lateness_s, window_s)
    records: list[WindowResult] = []
    events_in = 0
    last_arrival = 0.0
    for a in arrivals:
        records.extend(core.process(a.event, a.arrival_time))
        events_in += 1
        last_arrival = a.arrival_time
    flushed = list(core.flush(last_arrival))
    records.extend(flushed)
    return SimResult(
        records=records,
        flushed=flushed,
        dropped=list(core.dropped),
        peak_open_windows=core.peak_open_windows,
        events_in=events_in,
        mode=mode,
        lateness_s=lateness_s,
    )
