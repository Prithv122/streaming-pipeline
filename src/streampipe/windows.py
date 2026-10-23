"""Tumbling-window core: one lateness budget, two emission policies (wait, revise).

Semantics are specified in GUIDELINES.md -> Windowing semantics.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal

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


class WindowCore:
    """Placeholder until the core lands."""

    def __init__(self, mode: Mode, lateness_s: float, window_s: float = 60.0) -> None:
        raise NotImplementedError
