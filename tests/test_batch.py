from __future__ import annotations

import random

import pytest

from streampipe.batch import BatchWindow, batch_windows, window_start_of
from streampipe.events import GenConfig, OrderEvent, generate, to_cents


def ev(n: int, t: float, zone: str = "north", amount: float = 100.0) -> OrderEvent:
    return OrderEvent(f"ord-{n:06d}", t, zone, amount, "placed")


@pytest.mark.parametrize(
    ("t", "start"),
    [
        (0.0, 0.0),
        (0.001, 0.0),
        (59.999, 0.0),
        (60.0, 60.0),
        (119.999, 60.0),
        (120.0, 120.0),
        (3599.999, 3540.0),
    ],
)
def test_window_start_of_boundaries(t: float, start: float) -> None:
    assert window_start_of(t) == start


def test_window_start_of_custom_window() -> None:
    assert window_start_of(29.9, 30.0) == 0.0
    assert window_start_of(30.0, 30.0) == 30.0


def test_empty_input() -> None:
    assert batch_windows([]) == {}


def test_boundaries_and_zones() -> None:
    events = [
        ev(1, 0.0, "north", 10.0),
        ev(2, 59.999, "north", 20.0),
        ev(3, 60.0, "north", 30.0),
        ev(4, 60.0, "south", 40.0),
        ev(5, 119.999, "south", 50.5),
    ]
    assert batch_windows(events) == {
        ("north", 0.0): BatchWindow(2, 3000),
        ("north", 60.0): BatchWindow(1, 3000),
        ("south", 60.0): BatchWindow(2, 9050),
    }


def test_revenue_is_exact_in_cents() -> None:
    events = [ev(1, 1.0, amount=0.1), ev(2, 2.0, amount=0.2)]
    assert 0.1 + 0.2 != 0.3
    assert batch_windows(events)[("north", 0.0)].revenue_cents == 30


def test_order_independent() -> None:
    plain = [a.event for a in generate(GenConfig(seed=1, duration_s=300.0))]
    shuffled = plain[:]
    random.Random(0).shuffle(shuffled)
    assert batch_windows(plain) == batch_windows(shuffled)


def test_totals_match_input() -> None:
    plain = [a.event for a in generate(GenConfig(seed=2, duration_s=600.0))]
    windows = batch_windows(plain)
    assert sum(w.orders for w in windows.values()) == len(plain)
    assert sum(w.revenue_cents for w in windows.values()) == sum(to_cents(e.amount) for e in plain)


def test_custom_window_size() -> None:
    events = [ev(1, 10.0), ev(2, 29.0), ev(3, 31.0)]
    assert batch_windows(events, window_s=30.0) == {
        ("north", 0.0): BatchWindow(2, 20000),
        ("north", 30.0): BatchWindow(1, 10000),
    }
