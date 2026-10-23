"""Broker gate: does the Redpanda path reproduce the simulator's window records?

Needs `docker compose up -d` and `uv sync --extra kafka`. Two registered-scale cases (seed 0,
3,600 s): the C0 cell (5 % late, max delay 30 s, L = 30), where nothing can be dropped, and a
stress cell (20 % late, max delay 120 s, L = 15), where about 12 % of events are dropped, so the
drop path and per-zone drop counters cross the broker too. For each case and mode it:
  1. generates the log,
  2. produces it to a fresh single-partition topic and runs the windowing consumer,
  3. reads the whole output topic with no message cap, and checks the counts against the
     broker's own partition offsets (catches lost and duplicated messages),
  4. compares the records, in order, with `sim.simulate` on the same log.

The comparison rule is the one in docs/kafka.md: every field must match except `emitted_at`,
which is arrival time in the simulator and max event time on the broker path. The script also
checks `emitted_at` exactly against a direct core feed that uses the broker's clock.

Writes results/broker_check.json. It never touches the registered sweep files.
"""

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

from streampipe.events import GenConfig, arrival_order, generate
from streampipe.kafka_io import ensure_topics, make_consumer, poll_messages, produce, run_consumer
from streampipe.sim import simulate
from streampipe.windows import WindowCore

BOOTSTRAP = "127.0.0.1:19092"
CASES: tuple[tuple[GenConfig, float], ...] = (
    (GenConfig(seed=0, late_share=0.05, max_delay_s=30.0), 30.0),
    (GenConfig(seed=0, late_share=0.2, max_delay_s=120.0), 15.0),
)


def high_watermark(topic: str) -> int:
    """Messages in partition 0, by the broker's own offsets (admin API, no consumer group)."""
    from confluent_kafka import TopicPartition
    from confluent_kafka.admin import AdminClient, OffsetSpec

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    tp = TopicPartition(topic, 0)
    earliest = admin.list_offsets({tp: OffsetSpec.earliest()}, request_timeout=30)[tp].result()
    latest = admin.list_offsets({tp: OffsetSpec.latest()}, request_timeout=30)[tp].result()
    return latest.offset - earliest.offset


def read_topic(bootstrap: str, topic: str) -> tuple[list[dict], list[int]]:
    consumer = make_consumer(bootstrap, f"read-{uuid.uuid4().hex[:8]}", topic)
    try:
        msgs = list(poll_messages(consumer, idle_timeout_s=15.0))
        return [json.loads(m.value()) for m in msgs], [m.offset() for m in msgs]
    finally:
        consumer.close()


def strip(records: list[dict]) -> list[dict]:
    return [{k: v for k, v in r.items() if k != "emitted_at"} for r in records]


def first_mismatch(a: list[dict], b: list[dict]) -> dict | None:
    for i, (x, y) in enumerate(zip(a, b, strict=False)):
        if x != y:
            return {"index": i, "broker": x, "expected": y}
    if len(a) != len(b):
        return {"index": min(len(a), len(b)), "broker_len": len(a), "expected_len": len(b)}
    return None


def check_mode(arrivals: list, mode: str, lateness_s: float) -> dict:
    suffix = uuid.uuid4().hex[:8]
    topic_in, topic_out = f"orders.check-{suffix}", f"windows.check-{suffix}"
    ensure_topics(BOOTSTRAP, [topic_in, topic_out], partitions=1)

    print(f"[{mode}] producing to {topic_in}", flush=True)
    produced = produce(arrivals, BOOTSTRAP, topic_in)
    print(f"[{mode}] consuming and windowing", flush=True)
    summary = run_consumer(
        BOOTSTRAP,
        mode,
        lateness_s,
        topic_in=topic_in,
        topic_out=topic_out,
        group_id=f"check-{suffix}",
        idle_timeout_s=20.0,
        max_events=len(arrivals),
    )
    print(f"[{mode}] reading {topic_out} to the end", flush=True)
    got, offsets = read_topic(BOOTSTRAP, topic_out)
    print(f"[{mode}] comparing with the simulator", flush=True)

    sim = simulate(arrivals, mode, lateness_s)
    sim_records = [r.to_dict() for r in sim.records]

    core = WindowCore(mode, lateness_s)
    direct: list[dict] = []
    clock = float("-inf")
    for a in arrivals:
        clock = max(clock, a.event.event_time)
        direct += [r.to_dict() for r in core.process(a.event, clock)]
    direct += [r.to_dict() for r in core.flush(clock)]

    keys = [(r["zone"], r["window_start"], r["revision"]) for r in got]
    return {
        "mode": mode,
        "lateness_s": lateness_s,
        "events_in_log": len(arrivals),
        "events_produced": produced,
        "input_topic_offsets": high_watermark(topic_in),
        "events_consumed": summary["events"],
        "bad_messages": summary.get("bad_messages", 0),
        "records_simulator": len(sim_records),
        "records_consumer_reported": summary["records"],
        "output_topic_offsets": high_watermark(topic_out),
        "records_read_back": len(got),
        "read_back_offsets_contiguous_from_0": offsets == list(range(len(offsets))),
        "duplicate_record_keys": len(keys) - len(set(keys)),
        "dropped_simulator": len(sim.dropped),
        "dropped_broker": summary["dropped"],
        "flushed_simulator": len(sim.flushed),
        "flushed_broker": summary["flushed"],
        "records_equal_simulator_except_emitted_at": strip(got) == strip(sim_records),
        "records_equal_direct_feed_exactly": got == direct,
        "first_mismatch_vs_simulator": first_mismatch(strip(got), strip(sim_records)),
    }


def main() -> int:
    cases = []
    results = []
    for config, lateness_s in CASES:
        arrivals = arrival_order(generate(config))
        runs = [check_mode(arrivals, mode, lateness_s) for mode in ("wait", "revise")]
        results += runs
        cases.append(
            {
                "config": {
                    "seed": config.seed,
                    "late_share": config.late_share,
                    "max_delay_s": config.max_delay_s,
                    "duration_s": config.duration_s,
                    "lateness_s": lateness_s,
                    "partitions": 1,
                },
                "modes": runs,
            }
        )
    passed = all(
        r["events_produced"]
        == r["events_in_log"]
        == r["input_topic_offsets"]
        == r["events_consumed"]
        and r["bad_messages"] == 0
        and r["records_simulator"]
        == r["records_consumer_reported"]
        == r["output_topic_offsets"]
        == r["records_read_back"]
        and r["read_back_offsets_contiguous_from_0"]
        and r["duplicate_record_keys"] == 0
        and r["dropped_simulator"] == r["dropped_broker"]
        and r["flushed_simulator"] == r["flushed_broker"]
        and r["records_equal_simulator_except_emitted_at"]
        and r["records_equal_direct_feed_exactly"]
        for r in results
    )
    out = {"cases": cases, "passed": passed}
    path = Path("results/broker_check.json")
    text = json.dumps(out, indent=2, sort_keys=True) + "\n"
    path.write_text(text, encoding="utf-8", newline="\n")
    for r in results:
        print(
            f"{r['mode']} L={r['lateness_s']:g}: {r['events_consumed']} events, "
            f"{r['records_read_back']} records "
            f"(topic offsets {r['output_topic_offsets']}), dropped {r['dropped_broker']}, "
            f"equal to simulator: {r['records_equal_simulator_except_emitted_at']}"
        )
    print(f"broker check passed: {passed}  ->  {path}")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
