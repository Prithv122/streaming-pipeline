from typing import ClassVar

from streampipe.events import Arrival, OrderEvent
from streampipe.sim import simulate
from streampipe.windows import LateDrop, WindowResult


def _result(zone: str, start: float, final: bool, at: float, revision: int = 0) -> WindowResult:
    return WindowResult(start, start + 60, zone, 1, 100, 1.0, 0, revision, final, at)


class FakeCore:
    """Emits one record per even-numbered event, drops odd ones, and emits one record on flush."""

    instances: ClassVar[list["FakeCore"]] = []

    def __init__(self, mode: str, lateness_s: float, window_s: float = 60.0) -> None:
        self.args = (mode, lateness_s, window_s)
        self.dropped: list[LateDrop] = []
        self.peak_open_windows = 0
        self.processed: list[tuple[str, float]] = []
        self.flushed_at: float | None = None
        FakeCore.instances.append(self)

    def process(self, event: OrderEvent, now: float) -> list[WindowResult]:
        self.processed.append((event.order_id, now))
        self.peak_open_windows = max(self.peak_open_windows, len(self.processed))
        if int(event.order_id[-1]) % 2:
            self.dropped.append(LateDrop(event.zone, 0.0, event.order_id, event.event_time, now))
            return []
        return [_result(event.zone, 0.0, False, now)]

    def flush(self, now: float) -> list[WindowResult]:
        self.flushed_at = now
        return [_result("north", 60.0, True, now)]


def _arrivals() -> list[Arrival]:
    return [
        Arrival(OrderEvent(f"ord-{i:06d}", float(i), "north", 10.0, "placed"), float(i) + 1.5)
        for i in range(4)
    ]


def test_feeds_in_order_and_flushes_at_last_arrival() -> None:
    FakeCore.instances.clear()
    res = simulate(_arrivals(), "revise", 15.0, core_factory=FakeCore)
    core = FakeCore.instances[0]
    assert core.args == ("revise", 15.0, 60.0)
    assert core.processed == [(f"ord-{i:06d}", i + 1.5) for i in range(4)]
    assert core.flushed_at == 4.5
    assert res.events_in == 4
    assert (res.mode, res.lateness_s) == ("revise", 15.0)


def test_records_include_flushed_and_flushed_is_the_tail() -> None:
    res = simulate(_arrivals(), "wait", 0.0, core_factory=FakeCore)
    assert len(res.records) == 3  # events 0 and 2 emit, plus the flush record
    assert res.flushed == res.records[-1:]
    assert res.flushed[0].final
    assert [r.emitted_at for r in res.records[:2]] == [1.5, 3.5]


def test_dropped_and_peak_come_from_the_core() -> None:
    FakeCore.instances.clear()
    res = simulate(_arrivals(), "wait", 0.0, window_s=30.0, core_factory=FakeCore)
    assert [d.order_id for d in res.dropped] == ["ord-000001", "ord-000003"]
    assert res.peak_open_windows == 4
    assert FakeCore.instances[0].args[2] == 30.0


def test_empty_log() -> None:
    res = simulate([], "wait", 5.0, core_factory=FakeCore)
    assert res.events_in == 0
    assert res.records == res.flushed
    assert res.dropped == []
