"""Windowing core: hand-computed cases, hand-derived fixture, and invariants over random logs.

Expected values in the explicit cases and in `fixtures/windows_expected.json` were worked out
from the rules in GUIDELINES.md (see `fixtures/README.md`), not copied from the implementation.
"""

from __future__ import annotations

import json
import math
import random
import time
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from streampipe.events import Arrival, OrderEvent, read_log, to_cents
from streampipe.windows import LateDrop, WindowCore, WindowResult

FIXTURES = Path(__file__).parent / "fixtures"
SMALL_LOG = FIXTURES / "orders_late_small.jsonl"
EXPECTED = FIXTURES / "windows_expected.json"
FLUSH_NOW = 310.0

Row = tuple[str, float, int, bool, int, int, int, float]


def ev(order_id: str, t: float, zone: str, amount: float = 1.0) -> OrderEvent:
    return OrderEvent(order_id, t, zone, amount, "placed")


def row(r: WindowResult) -> Row:
    """(zone, window_start, revision, final, orders, revenue_cents, late_dropped, emitted_at)"""
    return (
        r.zone,
        r.window_start,
        r.revision,
        r.final,
        r.orders,
        r.revenue_cents,
        r.late_dropped,
        r.emitted_at,
    )


def feed(core: WindowCore, items: Iterable[tuple[float, OrderEvent]]) -> list[list[Row]]:
    """Process (arrival, event) pairs; one list of rows per event."""
    return [[row(r) for r in core.process(e, a)] for a, e in items]


def run_log(
    arrivals: Iterable[Arrival], mode: Any, lateness_s: float, flush_now: float
) -> tuple[list[WindowResult], WindowCore]:
    core = WindowCore(mode, lateness_s)
    records: list[WindowResult] = []
    for a in arrivals:
        records.extend(core.process(a.event, a.arrival_time))
    records.extend(core.flush(flush_now))
    return records, core


# --- small explicit cases, expected records written by hand ---------------------------------


def test_validation() -> None:
    with pytest.raises(ValueError):
        WindowCore("eager", 0)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        WindowCore("wait", -1)
    with pytest.raises(ValueError):
        WindowCore("wait", 0, window_s=0)


@pytest.mark.parametrize("mode", ["wait", "revise"])
def test_window_boundary(mode: Any) -> None:
    # 59.999 is in [0,60), exactly 60.0 starts [60,120). L=0, so 60.0 closes [0,60).
    core = WindowCore(mode, 0)
    items = [
        (30.0, ev("a", 30.0, "n", 1.00)),
        (59.999, ev("b", 59.999, "n", 2.00)),
        (60.0, ev("c", 60.0, "n", 4.00)),
    ]
    assert feed(core, items) == [[], [], [("n", 0.0, 0, True, 2, 300, 0, 60.0)]]
    assert core.open_windows == 1
    assert [row(r) for r in core.flush(100.0)] == [("n", 60.0, 0, True, 1, 400, 0, 100.0)]
    assert core.open_windows == 0


def test_late_within_lateness() -> None:
    items = [
        (10.0, ev("a", 10.0, "n", 1.00)),
        (70.0, ev("b", 70.0, "n", 2.00)),
        (75.0, ev("late", 20.0, "n", 4.00)),  # window [0,60) is due (M=70) but not closed
        (90.0, ev("c", 90.0, "n", 8.00)),  # M=90 >= 60 + 30 closes it
    ]
    wait = WindowCore("wait", 30)
    assert feed(wait, items) == [[], [], [], [("n", 0.0, 0, True, 2, 500, 0, 90.0)]]
    assert wait.dropped == []

    revise = WindowCore("revise", 30)
    assert feed(revise, items) == [
        [],
        [("n", 0.0, 0, False, 1, 100, 0, 70.0)],
        [("n", 0.0, 1, False, 2, 500, 0, 75.0)],
        [("n", 0.0, 2, True, 2, 500, 0, 90.0)],
    ]
    assert revise.dropped == []


def test_late_beyond_lateness_is_dropped_and_counted() -> None:
    items = [
        (10.0, ev("a", 10.0, "n", 1.00)),
        (95.0, ev("b", 95.0, "n", 2.00)),  # M=95 >= 90 closes [0,60)
        (100.0, ev("late", 20.0, "n", 4.00)),  # dropped
        (101.0, ev("c", 100.0, "s", 8.00)),
        (151.0, ev("d", 151.0, "n", 16.00)),  # M=151 closes [60,120) for both zones
    ]
    wait = WindowCore("wait", 30)
    out = feed(wait, items)
    assert out == [
        [],
        [("n", 0.0, 0, True, 1, 100, 0, 95.0)],
        [],
        [],
        # the zone counter shows in n's record, not in s's
        [("n", 60.0, 0, True, 1, 200, 1, 151.0), ("s", 60.0, 0, True, 1, 800, 0, 151.0)],
    ]
    assert wait.dropped == [LateDrop("n", 0.0, "late", 20.0, 100.0)]

    # M jumps from 100 to 151, so [60,120) is due and closed in one step: one final, revision 0
    revise = WindowCore("revise", 30)
    assert feed(revise, items) == out
    assert revise.dropped == [LateDrop("n", 0.0, "late", 20.0, 100.0)]


def test_late_dropped_is_per_zone_cumulative() -> None:
    core = WindowCore("wait", 0)
    feed(core, [(5.0, ev("a", 5.0, "n")), (70.0, ev("b", 70.0, "n"))])
    feed(core, [(71.0, ev("l1", 1.0, "n")), (72.0, ev("l2", 2.0, "s"))])
    assert [(d.zone, d.order_id, d.dropped_at) for d in core.dropped] == [
        ("n", "l1", 71.0),
        ("s", "l2", 72.0),
    ]
    out = feed(core, [(80.0, ev("c", 80.0, "s", 3.0)), (130.0, ev("d", 130.0, "n"))])
    assert out[1] == [
        ("n", 60.0, 0, True, 1, 100, 1, 130.0),
        ("s", 60.0, 0, True, 1, 300, 1, 130.0),
    ]


def test_out_of_order_across_boundary_creates_due_window_in_revise() -> None:
    # The event for [0,60) arrives after one for [60,120). L=30 accepts it.
    items = [
        (65.0, ev("a", 65.0, "n", 1.00)),
        (66.0, ev("b", 55.0, "n", 2.00)),
        (90.0, ev("c", 90.0, "n", 4.00)),
    ]
    wait = WindowCore("wait", 30)
    assert feed(wait, items) == [[], [], [("n", 0.0, 0, True, 1, 200, 0, 90.0)]]
    revise = WindowCore("revise", 30)
    assert feed(revise, items) == [
        [],
        [("n", 0.0, 0, False, 1, 200, 0, 66.0)],  # new window, already due: revision 0 at once
        [("n", 0.0, 1, True, 1, 200, 0, 90.0)],
    ]


def test_late_event_creates_window_with_no_prior_event() -> None:
    items = [
        (130.0, ev("a", 130.0, "n", 1.00)),
        (131.0, ev("b", 70.0, "n", 2.00)),  # [60,120) never had an event; it is due, not closed
        (150.0, ev("c", 150.0, "n", 4.00)),  # M=150 closes it
    ]
    wait = WindowCore("wait", 30)
    assert feed(wait, items) == [[], [], [("n", 60.0, 0, True, 1, 200, 0, 150.0)]]
    revise = WindowCore("revise", 30)
    assert feed(revise, items) == [
        [],
        [("n", 60.0, 0, False, 1, 200, 0, 131.0)],
        [("n", 60.0, 1, True, 1, 200, 0, 150.0)],
    ]
    # with L=0 the same event is simply dropped
    zero = WindowCore("revise", 0)
    assert feed(zero, items[:2]) == [[], []]
    assert zero.dropped == [LateDrop("n", 60.0, "b", 70.0, 131.0)]


@pytest.mark.parametrize("mode", ["wait", "revise"])
def test_due_and_closed_in_same_step_emits_one_final(mode: Any) -> None:
    core = WindowCore(mode, 30)
    items = [(10.0, ev("a", 10.0, "n", 1.00)), (200.0, ev("b", 200.0, "n", 2.00))]
    assert feed(core, items) == [[], [("n", 0.0, 0, True, 1, 100, 0, 200.0)]]
    assert [row(r) for r in core.flush(250.0)] == [("n", 180.0, 0, True, 1, 200, 0, 250.0)]


def test_flush_wait_orders_by_window_end_then_zone() -> None:
    core = WindowCore("wait", 1000)
    items = [
        (10.0, ev("a", 10.0, "south", 1.00)),
        (20.0, ev("b", 20.0, "north", 2.00)),
        (100.0, ev("c", 100.0, "north", 4.00)),
        (130.0, ev("d", 130.0, "east", 8.00)),
    ]
    assert feed(core, items) == [[], [], [], []]
    assert core.open_windows == 4
    assert [row(r) for r in core.flush(140.0)] == [
        ("north", 0.0, 0, True, 1, 200, 0, 140.0),
        ("south", 0.0, 0, True, 1, 100, 0, 140.0),
        ("north", 60.0, 0, True, 1, 400, 0, 140.0),
        ("east", 120.0, 0, True, 1, 800, 0, 140.0),
    ]
    assert core.open_windows == 0
    assert core.flush(150.0) == []


def test_flush_revise_continues_revision_numbers() -> None:
    core = WindowCore("revise", 1000)
    items = [
        (10.0, ev("a", 10.0, "south", 1.00)),
        (20.0, ev("b", 20.0, "north", 2.00)),
        (100.0, ev("c", 100.0, "north", 4.00)),
        (130.0, ev("d", 130.0, "east", 8.00)),
    ]
    assert feed(core, items) == [
        [],
        [],
        [  # M=100: [0,60) is due for both zones, provisional in (end, zone) order
            ("north", 0.0, 0, False, 1, 200, 0, 100.0),
            ("south", 0.0, 0, False, 1, 100, 0, 100.0),
        ],
        [("north", 60.0, 0, False, 1, 400, 0, 130.0)],  # M=130: [60,120) is due
    ]
    assert [row(r) for r in core.flush(140.0)] == [
        ("north", 0.0, 1, True, 1, 200, 0, 140.0),
        ("south", 0.0, 1, True, 1, 100, 0, 140.0),
        ("north", 60.0, 1, True, 1, 400, 0, 140.0),
        ("east", 120.0, 0, True, 1, 800, 0, 140.0),  # never due, so revision 0
    ]


def test_zero_lateness_wait_equals_revise() -> None:
    items = [
        (10.0, ev("a", 10.0, "n", 1.00)),
        (70.0, ev("b", 70.0, "n", 2.00)),  # closes [0,60)
        (75.0, ev("late", 20.0, "n", 4.00)),  # dropped
        (130.0, ev("c", 130.0, "s", 8.00)),  # closes [60,120)
    ]
    expected = [
        [],
        [("n", 0.0, 0, True, 1, 100, 0, 70.0)],
        [],
        [("n", 60.0, 0, True, 1, 200, 1, 130.0)],
    ]
    for mode in ("wait", "revise"):
        core = WindowCore(mode, 0)  # type: ignore[arg-type]
        assert feed(core, items) == expected
        assert [d.order_id for d in core.dropped] == ["late"]


def test_state_accessors() -> None:
    core = WindowCore("wait", 30)
    assert core.max_event_time is None
    assert core.open_windows == 0
    core.process(ev("a", 10.0, "n"), 10.0)
    core.process(ev("b", 70.0, "n"), 70.0)
    core.process(ev("c", 50.0, "n"), 71.0)
    assert core.max_event_time == 70.0
    assert core.open_windows == 2
    core.process(ev("d", 130.0, "n"), 130.0)  # closes [0,60) (M >= 90) but not [60,120)
    assert core.open_windows == 2
    assert core.peak_open_windows == 3  # [0,60), [60,120), [120,180) before the close fired


def test_does_not_scan_open_windows_per_event() -> None:
    rng = random.Random(7)
    arrivals = random_log(rng, n=20_000, horizon=3600.0, zones=6, max_delay=30.0)
    start = time.perf_counter()
    run_log(arrivals, "revise", 30.0, 4000.0)
    assert time.perf_counter() - start < 3.0  # ~0.06 s unloaded; loose bound


# --- the hand-built fixture ------------------------------------------------------------------

REGULAR_CENTS = {"north": 1000, "south": 2050, "east": 525}
ZONE_OFFSET = {"north": 0, "south": 1, "east": 2}
# (order_id, zone, event_time, amount, arrival_time), in the order listed in fixtures/README.md
SPECIALS: list[tuple[str, str, float, float, float]] = [
    ("ord-900001", "north", 59.999, 1.00, 60.0),
    ("ord-900002", "north", 50.0, 3.00, 70.25),
    ("ord-900003", "south", 59.5, 1.25, 75.25),
    ("ord-900004", "east", 10.0, 7.77, 95.25),
    ("ord-900005", "east", 120.0, 2.00, 120.75),
    ("ord-900006", "south", 120.0, 6.00, 120.25),
    ("ord-900007", "north", 118.0, 2.50, 125.25),
    ("ord-900008", "east", 100.0, 4.00, 130.25),
    ("ord-900009", "north", 119.999, 9.99, 155.25),
    ("ord-900010", "south", 170.0, 5.00, 190.25),
    ("ord-900011", "east", 175.0, 8.00, 200.25),
    ("ord-900012", "north", 130.0, 12.00, 215.25),
    ("ord-900013", "north", 200.0, 11.00, 245.25),
    ("ord-900014", "north", 305.0, 1.00, 305.25),
    ("ord-900015", "south", 290.0, 2.00, 306.25),
]


def build_small_log() -> list[Arrival]:
    """5 windows x 3 zones of regular events, plus the hand-placed special events.

    Regular: window k (start 60k), zone offset j, i = 0..11: event_time = 60k + 5i + j, fixed
    amount per zone, arrival = event_time + 0.5. The east zone has no regular events in window 1.
    """
    arrivals: list[Arrival] = []
    n = 0
    for k in range(5):
        for i in range(12):
            for zone in ("north", "south", "east"):
                if zone == "east" and k == 1:
                    continue
                n += 1
                t = 60.0 * k + 5 * i + ZONE_OFFSET[zone]
                amount = REGULAR_CENTS[zone] / 100
                event = OrderEvent(f"ord-{n:06d}", t, zone, amount, "placed")
                arrivals.append(Arrival(event, t + 0.5))
    for oid, zone, t, amount, arrival in SPECIALS:
        arrivals.append(Arrival(OrderEvent(oid, t, zone, amount, "placed"), arrival))
    return sorted(arrivals, key=lambda a: (a.arrival_time, a.event.order_id))


def _expected_cases() -> dict[str, Any]:
    return json.loads(EXPECTED.read_text(encoding="utf-8"))["cases"]


def test_fixture_file_matches_builder() -> None:
    assert read_log(SMALL_LOG) == build_small_log()


def test_fixture_shape() -> None:
    log = read_log(SMALL_LOG)
    assert 150 <= len(log) <= 250
    assert len({a.event.zone for a in log}) == 3
    keys = [(a.arrival_time, a.event.order_id) for a in log]
    assert keys == sorted(keys)
    assert len({a.event.order_id for a in log}) == len(log)
    assert all(a.arrival_time >= a.event.event_time for a in log)
    # events that arrive after one from a later window
    m = 0.0
    late = 0
    for a in log:
        if math.floor(a.event.event_time / 60) < math.floor(m / 60):
            late += 1
        m = max(m, a.event.event_time)
    assert late >= 10


CASES = [("L0", "wait"), ("L0", "revise"), ("L30", "wait"), ("L30", "revise")]


@pytest.mark.parametrize(("case", "mode"), CASES)
def test_fixture_expected_records(case: str, mode: Any) -> None:
    exp = _expected_cases()[case if case == "L0" else f"{case}_{mode}"]
    log = read_log(SMALL_LOG)
    core = WindowCore(mode, exp["lateness_s"])
    records: list[WindowResult] = []
    for a in log:
        records.extend(core.process(a.event, a.arrival_time))
    n_process = len(records)
    records.extend(core.flush(FLUSH_NOW))
    got = [
        [
            r.emitted_at,
            r.zone,
            r.window_start,
            r.revision,
            r.final,
            r.orders,
            r.revenue_cents,
            r.late_dropped,
        ]
        for r in records
    ]
    assert got == exp["records"]
    assert len(records) - n_process == exp["flushed_count"]
    assert all(r.window_end == r.window_start + 60 for r in records)
    assert all(r.revenue == r.revenue_cents / 100 for r in records)
    got_drops = [
        [d.order_id, d.zone, d.window_start, d.event_time, d.dropped_at] for d in core.dropped
    ]
    assert got_drops == exp["dropped"]
    assert core.peak_open_windows == exp["peak_open_windows"]
    assert core.open_windows == 0


# --- invariants over seeded random logs --------------------------------------------------------


def random_log(
    rng: random.Random, n: int, horizon: float, zones: int, max_delay: float
) -> list[Arrival]:
    names = ["north", "south", "east", "west", "central", "airport"][:zones]
    arrivals = []
    for i in range(n):
        t = round(rng.uniform(0, horizon), 3)
        delay = rng.uniform(0, 2) if rng.random() > 0.2 else rng.uniform(2, max_delay)
        amount = round(rng.uniform(1, 500), 2)
        e = OrderEvent(f"ord-{i:06d}", t, rng.choice(names), amount, "placed")
        arrivals.append(Arrival(e, round(t + delay, 3)))
    return sorted(arrivals, key=lambda a: (a.arrival_time, a.event.order_id))


SEEDS = range(12)
LATENESS = [0, 5, 30, 90]


def _logs() -> Iterable[tuple[int, float, list[Arrival]]]:
    for seed in SEEDS:
        rng = random.Random(seed)
        log = random_log(rng, n=400, horizon=600.0, zones=3, max_delay=[10, 40, 150][seed % 3])
        for lateness in LATENESS:
            yield seed, lateness, log


def _final_key(r: WindowResult) -> tuple[str, float, int, int, float]:
    return (r.zone, r.window_start, r.orders, r.revenue_cents, r.emitted_at)


def _group_by(arrivals: Iterable[Arrival], skip: set[str]) -> dict[tuple[str, float], list[int]]:
    truth: dict[tuple[str, float], list[int]] = defaultdict(lambda: [0, 0])
    for a in arrivals:
        if a.event.order_id in skip:
            continue
        key = (a.event.zone, math.floor(a.event.event_time / 60) * 60.0)
        truth[key][0] += 1
        truth[key][1] += to_cents(a.event.amount)
    return dict(truth)


def test_wait_equals_revise_finals_drops_and_state() -> None:
    for _seed, lateness, log in _logs():
        flush_now = log[-1].arrival_time
        wait, wcore = run_log(log, "wait", lateness, flush_now)
        revise, rcore = run_log(log, "revise", lateness, flush_now)
        assert [_final_key(r) for r in wait] == [_final_key(r) for r in revise if r.final]
        assert all(r.final and r.revision == 0 for r in wait)
        assert wcore.dropped == rcore.dropped
        assert wcore.peak_open_windows == rcore.peak_open_windows
        if lateness == 0:
            assert wait == revise


def test_revision_chains_are_consecutive_with_one_final() -> None:
    for _seed, lateness, log in _logs():
        records, _ = run_log(log, "revise", lateness, log[-1].arrival_time)
        by_window: dict[tuple[str, float], list[WindowResult]] = defaultdict(list)
        for r in records:
            by_window[(r.zone, r.window_start)].append(r)
        for chain in by_window.values():
            assert [r.revision for r in chain] == list(range(len(chain)))
            assert [r.final for r in chain] == [False] * (len(chain) - 1) + [True]
            orders = [r.orders for r in chain]
            assert orders == sorted(orders)
            times = [r.emitted_at for r in chain]
            assert times == sorted(times)


def test_infinite_lateness_matches_group_by() -> None:
    for seed in SEEDS:
        rng = random.Random(100 + seed)
        log = random_log(rng, n=400, horizon=600.0, zones=3, max_delay=150.0)
        truth = _group_by(log, set())
        for mode in ("wait", "revise"):
            records, core = run_log(log, mode, 10**9, log[-1].arrival_time)
            assert core.dropped == []
            finals = {
                (r.zone, r.window_start): [r.orders, r.revenue_cents] for r in records if r.final
            }
            assert finals == truth
            if mode == "wait":
                assert len(records) == len(truth)


def test_finals_equal_group_by_over_accepted_events() -> None:
    for _seed, lateness, log in _logs():
        records, core = run_log(log, "revise", lateness, log[-1].arrival_time)
        truth = _group_by(log, {d.order_id for d in core.dropped})
        finals = {(r.zone, r.window_start): [r.orders, r.revenue_cents] for r in records if r.final}
        assert finals == truth


class NaiveCore:
    """The spec read literally, scanning every window at every step. Slow; used as an oracle."""

    def __init__(self, mode: str, lateness_s: float, window_s: float = 60.0) -> None:
        self.mode, self.lateness_s, self.w = mode, lateness_s, window_s
        self.m = -math.inf
        self.windows: dict[tuple[str, float], list[int]] = {}  # [orders, cents, records]
        self.drops: dict[str, int] = defaultdict(int)
        self.dropped: list[LateDrop] = []
        self.peak = 0

    def _rec(self, key: tuple[str, float], now: float, final: bool) -> WindowResult:
        zone, start = key
        w = self.windows[key]
        rec = WindowResult(
            start, start + self.w, zone, w[0], w[1], w[1] / 100, self.drops[zone], w[2], final, now
        )
        w[2] += 1
        return rec

    def process(self, event: OrderEvent, now: float) -> list[WindowResult]:
        start = math.floor(event.event_time / self.w) * self.w
        end = start + self.w
        if self.m >= end + self.lateness_s:
            self.dropped.append(LateDrop(event.zone, start, event.order_id, event.event_time, now))
            self.drops[event.zone] += 1
            return []
        out = []
        key = (event.zone, start)
        w = self.windows.setdefault(key, [0, 0, 0])
        self.peak = max(self.peak, len(self.windows))
        w[0] += 1
        w[1] += to_cents(event.amount)
        if self.mode == "revise" and self.m >= end:
            out.append(self._rec(key, now, False))
        self.m = max(self.m, event.event_time)
        for k in sorted(self.windows, key=lambda k: (k[1] + self.w, k[0])):
            e = k[1] + self.w
            if self.m >= e + self.lateness_s:
                out.append(self._rec(k, now, True))
                del self.windows[k]
            elif self.mode == "revise" and self.m >= e and self.windows[k][2] == 0:
                out.append(self._rec(k, now, False))
        return out

    def flush(self, now: float) -> list[WindowResult]:
        keys = sorted(self.windows, key=lambda k: (k[1] + self.w, k[0]))
        out = [self._rec(k, now, True) for k in keys]
        self.windows.clear()
        return out


def test_matches_naive_oracle() -> None:
    for _seed, lateness, log in _logs():
        for mode in ("wait", "revise"):
            fast, fcore = run_log(log, mode, lateness, log[-1].arrival_time)
            naive = NaiveCore(mode, lateness)
            slow: list[WindowResult] = []
            for a in log:
                slow.extend(naive.process(a.event, a.arrival_time))
            slow.extend(naive.flush(log[-1].arrival_time))
            assert fast == slow
            assert fcore.dropped == naive.dropped
            assert fcore.peak_open_windows == naive.peak
