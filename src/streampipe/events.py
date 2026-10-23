"""Order events, the arrival-ordered event log, and the seeded generator."""

from __future__ import annotations

import json
import random
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

ZONES: tuple[str, ...] = ("north", "south", "east", "west", "central", "airport")
STATUSES: tuple[str, ...] = ("placed", "picked", "delivered", "cancelled")


def to_cents(amount: float) -> int:
    """Money is aggregated in integer cents so revenue equality is exact."""
    return round(amount * 100)


@dataclass(frozen=True, slots=True)
class OrderEvent:
    order_id: str
    event_time: float
    zone: str
    amount: float
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> OrderEvent:
        return cls(
            order_id=str(d["order_id"]),
            event_time=float(d["event_time"]),
            zone=str(d["zone"]),
            amount=float(d["amount"]),
            status=str(d["status"]),
        )


@dataclass(frozen=True, slots=True)
class Arrival:
    """An event and the stream time at which it reaches the processor."""

    event: OrderEvent
    arrival_time: float

    def to_dict(self) -> dict[str, Any]:
        return {**self.event.to_dict(), "arrival_time": self.arrival_time}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Arrival:
        return cls(event=OrderEvent.from_dict(d), arrival_time=float(d["arrival_time"]))


def arrival_order(arrivals: Iterable[Arrival]) -> list[Arrival]:
    """The canonical log order: by arrival time, ties broken by order id."""
    return sorted(arrivals, key=lambda a: (a.arrival_time, a.event.order_id))


def write_log(path: Path | str, arrivals: Iterable[Arrival]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for a in arrivals:
            f.write(json.dumps(a.to_dict(), separators=(",", ":")) + "\n")


def read_log(path: Path | str) -> list[Arrival]:
    with Path(path).open(encoding="utf-8") as f:
        return [Arrival.from_dict(json.loads(line)) for line in f if line.strip()]


@dataclass(frozen=True)
class GenConfig:
    """Parameters of the seeded generator. Defaults are the frozen evaluation plan."""

    seed: int = 0
    duration_s: float = 3600.0
    rate_per_s: float = 5.0
    burst_every_s: float = 600.0
    burst_len_s: float = 30.0
    burst_mult: float = 4.0
    late_share: float = 0.05
    max_delay_s: float = 30.0
    ontime_jitter_s: float = 2.0
    zones: tuple[str, ...] = ZONES

    def __post_init__(self) -> None:
        if not self.duration_s > 0:
            raise ValueError("duration_s must be > 0")
        if not self.rate_per_s > 0:
            raise ValueError("rate_per_s must be > 0")
        if not self.burst_every_s > 0:
            raise ValueError("burst_every_s must be > 0")
        if not 0 <= self.burst_len_s <= self.burst_every_s:
            raise ValueError("burst_len_s must be in [0, burst_every_s]")
        if not self.burst_mult > 0:
            raise ValueError("burst_mult must be > 0")
        if not 0.0 <= self.late_share <= 1.0:
            raise ValueError("late_share must be in [0, 1]")
        if not self.ontime_jitter_s >= 0:
            raise ValueError("ontime_jitter_s must be >= 0")
        if not self.max_delay_s >= 0:
            raise ValueError("max_delay_s must be >= 0")
        if self.late_share > 0 and self.max_delay_s < self.ontime_jitter_s:
            raise ValueError("max_delay_s must be >= ontime_jitter_s when late_share > 0")
        if not self.zones or len(set(self.zones)) != len(self.zones):
            raise ValueError("zones must be non-empty and unique")


def _rate_segments(cfg: GenConfig) -> list[tuple[float, float, float]]:
    """Split [0, duration) into (start, end, rate) pieces of constant rate."""
    segments: list[tuple[float, float, float]] = []
    burst_rate = cfg.rate_per_s * cfg.burst_mult
    k = 0
    while (start := k * cfg.burst_every_s) < cfg.duration_s:
        burst_end = min(start + cfg.burst_len_s, cfg.duration_s)
        cycle_end = min(start + cfg.burst_every_s, cfg.duration_s)
        if burst_end > start:
            segments.append((start, burst_end, burst_rate))
        if cycle_end > burst_end:
            segments.append((burst_end, cycle_end, cfg.rate_per_s))
        k += 1
    return segments


def generate(cfg: GenConfig) -> list[Arrival]:
    """Seeded order stream in log (arrival) order.

    Event times come from an inhomogeneous Poisson process whose rate is piecewise
    constant (`rate_per_s`, times `burst_mult` while `t mod burst_every_s < burst_len_s`).
    The process is stepped segment by segment: within a segment, gaps are exponential at
    that segment's rate, and a gap that overshoots the segment end is discarded and the
    next segment starts fresh, which is exact because exponential gaps are memoryless.

    Draw order from the single `random.Random(cfg.seed)`: first every event time (in time
    order), then per event, in order: zone, status, amount, late flag, delay. Zone and status
    are picked by indexing with one uniform draw each, which is faster than `Random.choice`.
    """
    rng = random.Random(cfg.seed)
    times: list[float] = []
    for start, end, rate in _rate_segments(cfg):
        t = start
        while True:
            t += rng.expovariate(rate)
            if t >= end:
                break
            times.append(round(t, 3))
    while times and times[-1] >= cfg.duration_s:
        times.pop()

    rand = rng.random
    lognorm = rng.lognormvariate
    zones, n_zones = cfg.zones, len(cfg.zones)
    n_statuses = len(STATUSES)
    jitter, max_delay = cfg.ontime_jitter_s, cfg.max_delay_s
    arrivals: list[Arrival] = []
    for n, et in enumerate(times, start=1):
        zone = zones[int(rand() * n_zones)]
        status = STATUSES[int(rand() * n_statuses)]
        amount = round(max(50.0, lognorm(6.0, 0.6)), 2)
        if rand() < cfg.late_share:
            delay = jitter + (max_delay - jitter) * rand()
        else:
            delay = jitter * rand()
        event = OrderEvent(f"ord-{n:06d}", et, zone, amount, status)
        arrivals.append(Arrival(event, round(et + delay, 3)))
    return arrival_order(arrivals)
