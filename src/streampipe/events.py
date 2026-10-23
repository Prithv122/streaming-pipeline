"""Order events, the arrival-ordered event log, and the seeded generator."""

from __future__ import annotations

import json
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
