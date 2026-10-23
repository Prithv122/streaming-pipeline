"""Unit tests for the broker edge, run against fakes: no broker and no confluent_kafka needed."""

from __future__ import annotations

import json

import pytest

from streampipe.events import Arrival, OrderEvent
from streampipe.kafka_io import (
    KafkaIOError,
    WindowStage,
    consume_loop,
    poll_messages,
    produce_arrivals,
)
from streampipe.windows import WindowCore, WindowResult


def arrival(i: int, event_time: float, arrival_time: float, zone: str = "north") -> Arrival:
    ev = OrderEvent(f"ord-{i:06d}", event_time, zone, 10.0 + i, "placed")
    return Arrival(ev, arrival_time)


def result(zone: str = "north", start: float = 0.0, **kw) -> WindowResult:
    base = {
        "window_start": start,
        "window_end": start + 60.0,
        "zone": zone,
        "orders": 1,
        "revenue_cents": 1000,
        "revenue": 10.0,
        "late_dropped": 0,
        "revision": 0,
        "final": True,
        "emitted_at": 70.0,
    }
    return WindowResult(**{**base, **kw})


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


class FakeMessage:
    def __init__(self, value, error=None) -> None:
        self._value, self._error = value, error

    def value(self):
        return self._value

    def error(self):
        return self._error


class FakeConsumer:
    """Serves queued messages, then None; each empty poll advances the fake clock."""

    def __init__(self, messages, clock: FakeClock, poll_s: float = 0.5) -> None:
        self.queue = list(messages)
        self.clock = clock
        self.poll_s = poll_s
        self.commits = 0
        self.polls = 0

    def poll(self, timeout):
        self.polls += 1
        if self.queue:
            return self.queue.pop(0)
        self.clock.t += timeout
        return None

    def commit(self, asynchronous=True):
        assert asynchronous is False
        self.commits += 1


class FakeProducer:
    def __init__(self, fail_with: str | None = None, buffer_full_times: int = 0) -> None:
        self.sent: list[tuple[str, str, str]] = []
        self.fail_with = fail_with
        self.buffer_full_times = buffer_full_times
        self.callbacks = []
        self.flushed = False

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self.buffer_full_times:
            self.buffer_full_times -= 1
            raise BufferError("queue full")
        self.sent.append((topic, key, value))
        self.callbacks.append(on_delivery)

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        self.flushed = True
        for cb in self.callbacks:
            if self.fail_with:
                cb(self.fail_with, None)
            else:
                cb(None, None)
        return 0


class FakeCore:
    """Emits one record per event; flush emits one final record per zone seen."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, float]] = []
        self.flush_now: float | None = None
        self.dropped = [object()]
        self.zones: list[str] = []

    def process(self, event, now):
        self.calls.append((event.order_id, now))
        self.zones.append(event.zone)
        return [result(zone=event.zone, final=False, emitted_at=now)]

    def flush(self, now):
        self.flush_now = now
        return [result(zone=z, emitted_at=now) for z in sorted(set(self.zones))]


def msg(a: Arrival) -> FakeMessage:
    return FakeMessage(json.dumps(a.event.to_dict()).encode())


# --- produce -----------------------------------------------------------------------------------


def test_produce_sends_zone_key_and_event_json_without_arrival_time():
    arrivals = [arrival(1, 5.0, 6.0, "south"), arrival(2, 7.0, 7.5, "east")]
    p = FakeProducer()
    assert produce_arrivals(p, arrivals, "t.test") == 2
    assert [(t, k) for t, k, _ in p.sent] == [("t.test", "south"), ("t.test", "east")]
    body = json.loads(p.sent[0][2])
    assert "arrival_time" not in body
    assert OrderEvent.from_dict(body) == arrivals[0].event
    assert p.flushed


def test_produce_pace_scales_arrival_gaps():
    arrivals = [arrival(1, 0.0, 10.0), arrival(2, 1.0, 12.0), arrival(3, 2.0, 12.0)]
    slept: list[float] = []
    produce_arrivals(FakeProducer(), arrivals, pace=4.0, sleep=slept.append)
    assert slept == [0.5]  # 2 s gap at 4x; the zero gap is not slept


def test_produce_without_pace_never_sleeps():
    slept: list[float] = []
    produce_arrivals(FakeProducer(), [arrival(1, 0, 1), arrival(2, 1, 9)], sleep=slept.append)
    assert slept == []


def test_produce_rejects_non_positive_pace():
    with pytest.raises(ValueError):
        produce_arrivals(FakeProducer(), [arrival(1, 0, 1)], pace=0)


def test_produce_surfaces_delivery_errors():
    with pytest.raises(KafkaIOError, match="failed delivery"):
        produce_arrivals(FakeProducer(fail_with="UNKNOWN_TOPIC"), [arrival(1, 0, 1)])


def test_produce_retries_when_local_queue_is_full():
    p = FakeProducer(buffer_full_times=2)
    assert produce_arrivals(p, [arrival(1, 0, 1)]) == 1
    assert len(p.sent) == 1


# --- poll_messages -----------------------------------------------------------------------------


def test_poll_stops_after_idle_timeout():
    clock = FakeClock()
    c = FakeConsumer([], clock)
    assert list(poll_messages(c, idle_timeout_s=2.0, clock=clock)) == []
    assert clock.t == pytest.approx(2.0)


def test_poll_idle_timer_restarts_on_each_message():
    clock = FakeClock()
    c = FakeConsumer([msg(arrival(1, 0, 0)), msg(arrival(2, 1, 1))], clock)
    assert len(list(poll_messages(c, idle_timeout_s=3.0, clock=clock))) == 2
    assert clock.t == pytest.approx(3.0)


def test_poll_honours_max_messages():
    clock = FakeClock()
    c = FakeConsumer([msg(arrival(i, i, i)) for i in range(5)], clock)
    assert len(list(poll_messages(c, idle_timeout_s=60, max_messages=3, clock=clock))) == 3
    assert len(c.queue) == 2


def test_poll_raises_on_consumer_error():
    clock = FakeClock()
    c = FakeConsumer([FakeMessage(None, error="BROKER DOWN")], clock)
    with pytest.raises(KafkaIOError, match="BROKER DOWN"):
        list(poll_messages(c, idle_timeout_s=1, clock=clock))


# --- WindowStage -------------------------------------------------------------------------------


def test_stage_clock_is_max_event_time_seen_including_current():
    core = FakeCore()
    stage = WindowStage("wait", 0.0, core=core)
    for a in (arrival(1, 10.0, 0), arrival(2, 4.0, 0), arrival(3, 25.0, 0), arrival(4, 20.0, 0)):
        stage.handle(json.dumps(a.event.to_dict()))
    assert [now for _, now in core.calls] == [10.0, 10.0, 25.0, 25.0]
    stage.finish()
    assert core.flush_now == 25.0


def test_stage_counts_events_records_and_dropped():
    core = FakeCore()
    stage = WindowStage("wait", 0.0, core=core)
    for i, z in enumerate(("north", "south", "north"), start=1):
        stage.handle(json.dumps(arrival(i, float(i), 0, z).event.to_dict()).encode())
    stage.finish()
    assert stage.summary() == {
        "events": 3,
        "records": 3 + 2,
        "dropped": 1,
        "flushed": 2,
        "bad_messages": 0,
    }


@pytest.mark.parametrize("bad", [None, b"", b"not json", b"[1, 2]", b'{"order_id": "x"}'])
def test_stage_skips_malformed_messages(bad):
    core = FakeCore()
    stage = WindowStage("wait", 0.0, core=core)
    assert stage.handle(bad) == []
    assert stage.bad_messages == 1
    assert stage.events == 0
    assert core.calls == []


def test_stage_finish_on_empty_stream_flushes_at_zero():
    core = FakeCore()
    stage = WindowStage("wait", 0.0, core=core)
    assert stage.finish() == []
    assert core.flush_now == 0.0


def test_stage_dropped_accepts_a_per_zone_count_mapping():
    core = FakeCore()
    core.dropped = {"north": 2, "south": 1}
    assert WindowStage("wait", 0.0, core=core).summary()["dropped"] == 3


# --- consume_loop ------------------------------------------------------------------------------


def test_consume_loop_produces_keyed_records_flushes_and_commits():
    clock = FakeClock()
    arrivals = [arrival(1, 1.0, 1.0, "north"), arrival(2, 2.0, 2.0, "south")]
    consumer = FakeConsumer([msg(a) for a in arrivals], clock)
    producer = FakeProducer()
    stage = WindowStage("wait", 0.0, core=FakeCore())
    summary = consume_loop(consumer, producer, stage, "w.test", idle_timeout_s=1.0, clock=clock)
    assert summary["events"] == 2
    assert summary["records"] == 4
    assert summary["flushed"] == 2
    assert [(t, k) for t, k, _ in producer.sent] == [
        ("w.test", "north"),
        ("w.test", "south"),
        ("w.test", "north"),
        ("w.test", "south"),
    ]
    assert WindowResult(**json.loads(producer.sent[0][2])).zone == "north"
    assert producer.flushed
    assert consumer.commits == 1


def test_consume_loop_stops_at_max_events_and_leaves_the_rest():
    clock = FakeClock()
    consumer = FakeConsumer([msg(arrival(i, i, i)) for i in range(1, 6)], clock)
    stage = WindowStage("wait", 0.0, core=FakeCore())
    summary = consume_loop(
        consumer, FakeProducer(), stage, idle_timeout_s=60, max_events=2, clock=clock
    )
    assert summary["events"] == 2
    assert len(consumer.queue) == 3


def test_consume_loop_empty_topic_does_not_commit():
    clock = FakeClock()
    consumer = FakeConsumer([], clock)
    summary = consume_loop(
        consumer, FakeProducer(), WindowStage("wait", 0.0, core=FakeCore()), clock=clock
    )
    assert summary["events"] == 0
    assert consumer.commits == 0


def test_consume_loop_raises_when_output_delivery_fails():
    clock = FakeClock()
    consumer = FakeConsumer([msg(arrival(1, 1.0, 1.0))], clock)
    with pytest.raises(KafkaIOError):
        consume_loop(
            consumer,
            FakeProducer(fail_with="TOPIC_AUTHORIZATION_FAILED"),
            WindowStage("wait", 0.0, core=FakeCore()),
            clock=clock,
        )
    assert consumer.commits == 0  # not committed past records that were never delivered


def test_stage_matches_direct_core_feed_with_the_same_clock():
    try:
        direct = WindowCore("revise", 10.0)
    except NotImplementedError:
        pytest.skip("WindowCore not implemented yet")
    arrivals = [
        arrival(1, 5.0, 5.0),
        arrival(2, 62.0, 62.0),
        arrival(3, 50.0, 63.0),  # out of order, within L
        arrival(4, 130.0, 130.0),
        arrival(5, 20.0, 131.0),  # beyond L: dropped
    ]
    expected: list[dict] = []
    clock = float("-inf")
    for a in arrivals:
        clock = max(clock, a.event.event_time)
        expected += [r.to_dict() for r in direct.process(a.event, clock)]
    expected += [r.to_dict() for r in direct.flush(clock)]

    stage = WindowStage("revise", 10.0)
    got: list[dict] = []
    for a in arrivals:
        got += [r.to_dict() for r in stage.handle(json.dumps(a.event.to_dict()))]
    got += [r.to_dict() for r in stage.finish()]
    assert got == expected
    assert stage.summary()["dropped"] == 1
