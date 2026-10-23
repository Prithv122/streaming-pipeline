"""End-to-end through a real broker: needs `docker compose up -d` and the `kafka` extra.

Deselected in CI (`-m "not integration"`); skips with a reason when no broker answers.
"""

from __future__ import annotations

import json
import uuid

import pytest

from streampipe.events import GenConfig, arrival_order, generate
from streampipe.kafka_io import (
    ensure_topics,
    make_consumer,
    poll_messages,
    produce,
    run_consumer,
)
from streampipe.windows import WindowCore

BOOTSTRAP = "localhost:19092"

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def broker() -> str:
    ck = pytest.importorskip("confluent_kafka", reason="needs `uv sync --extra kafka`")
    from confluent_kafka.admin import AdminClient

    try:
        AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=3)
    except ck.KafkaException as exc:
        pytest.skip(f"no broker at {BOOTSTRAP} (docker compose up -d): {exc}")
    return BOOTSTRAP


def read_all(bootstrap: str, topic: str, expected: int) -> list[dict]:
    consumer = make_consumer(bootstrap, f"verify-{uuid.uuid4().hex[:8]}", topic)
    try:
        msgs = poll_messages(consumer, idle_timeout_s=10.0, max_messages=expected)
        return [json.loads(m.value()) for m in msgs]
    finally:
        consumer.close()


@pytest.mark.parametrize("mode", ["wait", "revise"])
def test_broker_run_reproduces_direct_core_feed(broker: str, mode: str):
    arrivals = arrival_order(generate(GenConfig(seed=7, duration_s=300.0, rate_per_s=2.0)))
    lateness = 10.0
    suffix = uuid.uuid4().hex[:8]
    topic_in, topic_out = f"orders.test-{suffix}", f"windows.test-{suffix}"
    ensure_topics(broker, [topic_in, topic_out], partitions=1)

    # Expected: the same events, same order, same stream clock (max event_time so far).
    core = WindowCore(mode, lateness)
    expected: list[dict] = []
    clock = float("-inf")
    for a in arrivals:
        clock = max(clock, a.event.event_time)
        expected += [r.to_dict() for r in core.process(a.event, clock)]
    expected += [r.to_dict() for r in core.flush(clock)]
    assert expected, "the fixture log should produce window records"

    assert produce(arrivals, broker, topic_in) == len(arrivals)
    summary = run_consumer(
        broker,
        mode,
        lateness,
        topic_in=topic_in,
        topic_out=topic_out,
        group_id=f"it-{suffix}",
        idle_timeout_s=20.0,
        max_events=len(arrivals),
    )
    assert summary["events"] == len(arrivals)
    assert summary["records"] == len(expected)
    assert summary["dropped"] == len(core.dropped)

    got = read_all(broker, topic_out, len(expected))
    assert got == expected

    # Same records as the simulator's arrival-time clock, apart from the emit timestamp.
    sim_core = WindowCore(mode, lateness)
    sim: list[dict] = []
    for a in arrivals:
        sim += [r.to_dict() for r in sim_core.process(a.event, a.arrival_time)]
    sim += [r.to_dict() for r in sim_core.flush(arrivals[-1].arrival_time)]
    strip = lambda rs: [{k: v for k, v in r.items() if k != "emitted_at"} for r in rs]  # noqa: E731
    assert strip(got) == strip(sim)
