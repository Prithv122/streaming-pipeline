"""Broker edge: produce an event log to Redpanda/Kafka and run the window core off a topic.

`confluent_kafka` is imported lazily, only inside the functions that talk to a broker, so this
module imports cleanly in CI (which installs no Kafka client) and the pure parts are unit-tested
with fakes. The windowing core itself never sees a client.

Stream clock on this path: a consumed message has no `arrival_time`, so the clock is the largest
`event_time` seen so far (see docs/kafka.md). It is a pure function of the message order, so a
replay gives identical output. Latency metrics come from the simulator, not from this path.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import Any

from streampipe.events import Arrival, OrderEvent
from streampipe.windows import WindowCore, WindowResult

ORDERS_TOPIC = "orders.v1"
WINDOWS_TOPIC = "windows.v1"


class KafkaIOError(RuntimeError):
    """A broker-side failure: a failed delivery, a consumer error, or a missing client."""


def _require_kafka() -> Any:
    try:
        import confluent_kafka
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise KafkaIOError(
            "confluent-kafka is not installed; run `uv sync --extra kafka` to use the broker edge"
        ) from exc
    return confluent_kafka


def _dumps(obj: dict[str, Any]) -> str:
    return json.dumps(obj, separators=(",", ":"))


class _Delivery:
    """Delivery-report callback that remembers failures so they can be raised after a flush."""

    def __init__(self) -> None:
        self.errors: list[str] = []
        self.delivered = 0

    def __call__(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.errors.append(str(err))
        else:
            self.delivered += 1

    def raise_if_failed(self) -> None:
        if self.errors:
            first = self.errors[0]
            raise KafkaIOError(f"{len(self.errors)} message(s) failed delivery; first: {first}")


def _send(producer: Any, topic: str, key: str, value: str, report: _Delivery) -> None:
    """Queue one message; if the local queue is full, serve callbacks and retry."""
    while True:
        try:
            producer.produce(topic, key=key, value=value, on_delivery=report)
            break
        except BufferError:
            producer.poll(0.5)
    producer.poll(0)


def _finish_producing(producer: Any, report: _Delivery, timeout_s: float = 30.0) -> None:
    remaining = producer.flush(timeout_s)
    report.raise_if_failed()
    if remaining:
        raise KafkaIOError(f"{remaining} message(s) still queued after {timeout_s:.0f} s flush")


# --- producing an event log -------------------------------------------------------------------


def produce_arrivals(
    producer: Any,
    arrivals: Iterable[Arrival],
    topic: str = ORDERS_TOPIC,
    pace: float | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Send arrivals in log order (key = zone, value = OrderEvent JSON). Returns the count."""
    if pace is not None and pace <= 0:
        raise ValueError("pace must be positive (or None for as fast as possible)")
    report = _Delivery()
    count = 0
    prev_arrival: float | None = None
    for a in arrivals:
        if pace is not None and prev_arrival is not None:
            gap = (a.arrival_time - prev_arrival) / pace
            if gap > 0:
                sleep(gap)
        prev_arrival = a.arrival_time
        _send(producer, topic, a.event.zone, _dumps(a.event.to_dict()), report)
        count += 1
    _finish_producing(producer, report)
    return count


def produce(
    arrivals: Iterable[Arrival],
    bootstrap: str,
    topic: str = ORDERS_TOPIC,
    pace: float | None = None,
) -> int:
    """Replay an event log into `topic`.

    `pace=None` sends as fast as possible; `pace=k` sleeps so the arrival-time gaps are replayed
    at k times real time (k=1 is real time). Idempotence keeps per-partition order intact.
    """
    ck = _require_kafka()
    producer = ck.Producer(
        {"bootstrap.servers": bootstrap, "enable.idempotence": True, "linger.ms": 5}
    )
    return produce_arrivals(producer, arrivals, topic, pace)


def ensure_topics(bootstrap: str, topics: Iterable[str], partitions: int = 1) -> None:
    """Create topics that do not exist yet (one partition: order across zones matters for M)."""
    ck = _require_kafka()
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics(
        [NewTopic(t, num_partitions=partitions, replication_factor=1) for t in topics]
    )
    for topic, fut in futures.items():
        try:
            fut.result(timeout=30)
        except ck.KafkaException as exc:
            if exc.args[0].code() != ck.KafkaError.TOPIC_ALREADY_EXISTS:
                raise KafkaIOError(f"could not create topic {topic}: {exc}") from exc


# --- consuming ---------------------------------------------------------------------------------


def make_consumer(bootstrap: str, group_id: str, topic: str) -> Any:
    """A consumer that always starts at the earliest offset, whatever the group committed.

    A replay must see the whole log or its windows are not comparable to the simulator's, so the
    assignment callback rewinds every partition instead of trusting `auto.offset.reset`.
    """
    ck = _require_kafka()
    consumer = ck.Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )

    def rewind(c: Any, partitions: list[Any]) -> None:
        for p in partitions:
            p.offset = ck.OFFSET_BEGINNING
        c.assign(partitions)

    consumer.subscribe([topic], on_assign=rewind)
    return consumer


def poll_messages(
    consumer: Any,
    idle_timeout_s: float,
    max_messages: int | None = None,
    poll_s: float = 0.5,
    clock: Callable[[], float] = time.monotonic,
) -> Iterator[Any]:
    """Yield messages until `idle_timeout_s` passes with none, or `max_messages` were yielded.

    The idle timer is wall-clock and only decides when to stop waiting; it never touches window
    time. It starts at the first call, so it also covers the wait for the group to be assigned.
    """
    last = clock()
    yielded = 0
    while max_messages is None or yielded < max_messages:
        msg = consumer.poll(poll_s)
        if msg is None:
            if clock() - last >= idle_timeout_s:
                return
            continue
        err = msg.error()
        if err is not None:
            raise KafkaIOError(f"consumer error: {err}")
        last = clock()
        yielded += 1
        yield msg


def _count(dropped: Any) -> int:
    """`WindowCore.dropped` is a list of LateDrop; tolerate a per-zone count mapping too."""
    if isinstance(dropped, Mapping):
        return sum(int(v) for v in dropped.values())
    return len(dropped)


class WindowStage:
    """The pure middle of the consumer: message bytes -> OrderEvent -> core -> window records.

    No broker, no wall clock. The stream clock `now` is the largest `event_time` seen so far
    (including the event being processed); it is passed to the core as the event's arrival time
    and used as the flush time at the end.
    """

    def __init__(
        self,
        mode: str,
        lateness_s: float,
        window_s: float = 60.0,
        core: Any | None = None,
    ) -> None:
        self.core = core if core is not None else WindowCore(mode, lateness_s, window_s)
        self.clock = float("-inf")
        self.events = 0
        self.bad_messages = 0
        self.records = 0
        self.flushed = 0

    def handle(self, value: bytes | str | None) -> list[WindowResult]:
        """Process one message value. A malformed message is counted and skipped."""
        try:
            if value is None:
                raise ValueError("empty value")
            event = OrderEvent.from_dict(json.loads(value))
        except (ValueError, KeyError, TypeError):
            self.bad_messages += 1
            return []
        self.events += 1
        self.clock = max(self.clock, event.event_time)
        out = self.core.process(event, self.clock)
        self.records += len(out)
        return out

    def finish(self) -> list[WindowResult]:
        """End of stream: final records for every open window, stamped with the stream clock."""
        now = self.clock if self.events else 0.0
        out = self.core.flush(now)
        self.records += len(out)
        self.flushed = len(out)
        return out

    def summary(self) -> dict[str, int]:
        return {
            "events": self.events,
            "records": self.records,
            "dropped": _count(self.core.dropped),
            "flushed": self.flushed,
            "bad_messages": self.bad_messages,
        }


def consume_loop(
    consumer: Any,
    producer: Any,
    stage: WindowStage,
    topic_out: str = WINDOWS_TOPIC,
    idle_timeout_s: float = 10.0,
    max_events: int | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, int]:
    """Drive `stage` from `consumer`, produce every record to `topic_out`, flush, commit."""
    report = _Delivery()

    def emit(results: Iterable[WindowResult]) -> None:
        for r in results:
            _send(producer, topic_out, r.zone, _dumps(r.to_dict()), report)

    consumed = 0
    for msg in poll_messages(consumer, idle_timeout_s, max_events, clock=clock):
        consumed += 1
        emit(stage.handle(msg.value()))
    emit(stage.finish())
    _finish_producing(producer, report)
    if consumed:
        consumer.commit(asynchronous=False)
    return stage.summary()


def run_consumer(
    bootstrap: str,
    mode: str,
    lateness_s: float,
    topic_in: str = ORDERS_TOPIC,
    topic_out: str = WINDOWS_TOPIC,
    group_id: str = "streampipe",
    idle_timeout_s: float = 10.0,
    max_events: int | None = None,
) -> dict:
    """Consume `topic_in` from the earliest offset through a WindowCore into `topic_out`.

    Stops after `idle_timeout_s` with no message or after `max_events` messages, then flushes the
    open windows, produces those final records, commits and returns
    `{events, records, dropped, flushed, bad_messages}`.
    """
    ck = _require_kafka()
    stage = WindowStage(mode, lateness_s)
    producer = ck.Producer(
        {"bootstrap.servers": bootstrap, "enable.idempotence": True, "linger.ms": 5}
    )
    consumer = make_consumer(bootstrap, group_id, topic_in)
    try:
        return consume_loop(consumer, producer, stage, topic_out, idle_timeout_s, max_events)
    finally:
        consumer.close()
