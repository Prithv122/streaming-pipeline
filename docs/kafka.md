# Running on a broker

The simulator (`sim.py`) is the measured path. The broker path exists to show the same core runs
unchanged off a real log: `orders.v1` in, `windows.v1` out, and optionally on to the dashboard hub.

## Start the broker

Docker Desktop must be running.

```bash
cd "D:\Coding projects\resume_max\34-streaming-pipeline"; docker compose up -d
```

This starts single-node Redpanda (Kafka API on `127.0.0.1:19092`) and a one-shot `init-topics`
service that creates `orders.v1` and `windows.v1` with **1 partition each**. The same thing by hand:

```bash
cd "D:\Coding projects\resume_max\34-streaming-pipeline"; docker compose exec redpanda rpk topic create orders.v1 windows.v1 -p 1 -r 1
```

Optional web console on `http://localhost:18080`:

```bash
cd "D:\Coding projects\resume_max\34-streaming-pipeline"; docker compose --profile console up -d
```

Install the Kafka client (only the broker path needs it; CI and the simulator do not):

```bash
cd "D:\Coding projects\resume_max\34-streaming-pipeline"; uv sync --extra kafka
```

Run the broker test (skips with a reason if no broker answers):

```bash
cd "D:\Coding projects\resume_max\34-streaming-pipeline"; uv run pytest -m integration
```

## Pinned images

| Image | Tag | Pushed (Docker Hub) |
|---|---|---|
| `redpandadata/redpanda` | `v26.2.4` | 2026-10-06 |
| `redpandadata/console` | `v3.12.0` | 2026-09-16 |

Chosen on 2026-10-23 as the newest tags at least two weeks old. Newer ones existed and were left
alone: Redpanda `v26.2.5` (2026-10-16), `v26.2.6` (2026-10-23); Console `v3.13.0`/`v3.13.1`
(2026-10-22). The dates are Docker Hub's `tag_last_pushed`, so a tag that was re-pushed shows its
latest push, not its first. Nothing uses `latest`.

## Partitioning: one partition

`orders.v1` has one partition. The windowing core tracks `M`, the maximum event time seen so far,
and lateness decisions depend on the order events reach it. Across several partitions Kafka only
orders within a partition, so the consumer would see a different interleaving on each run and
`M` (and the drop decisions) would change with it. One partition gives one total order, the
log order, so a replay is repeatable. The cost is no consumer parallelism, which is irrelevant at
this volume. At 100x scale the answer is a per-zone partition key with a per-partition watermark
and a merge, which is a different (and harder) design than the one measured here.

Messages are keyed by zone anyway, so the topic stays correct if it is ever repartitioned.

## The stream clock on this path

The core takes a `now` with every event: in the simulator it is the event's `arrival_time`. A
message read from Kafka has no `arrival_time` (the topic carries `OrderEvent` only), so the
consumer needs another clock. The choice is **`now` = the largest `event_time` seen so far,
including the event being processed**.

Why this and not the alternatives:

- **Message timestamp** (broker or producer time) is wall-clock time of the replay, not stream
  time. A replay at 10x speed or as fast as possible would stamp it differently every run, and
  `emitted_at` would stop being comparable across runs.
- **Wall clock in the consumer** breaks the rule that nothing in the pipeline reads the wall clock
  and makes output depend on how fast the machine is.
- **Max event time** is a pure function of the message order, so the same log in the same order
  gives byte-identical output, on any machine.

What it changes, and what it does not:

- Window membership, drops, revisions and final values do not depend on `now` at all. They depend
  on event times and arrival order only. So the broker run reproduces the simulator's records
  exactly, apart from `emitted_at`. The integration test checks both: equality with a direct
  feed using the same clock, and equality with the simulator's arrival-clock run once `emitted_at`
  is removed.
- `emitted_at` on this path is the stream position (max event time) when a record was emitted. It
  is not the time the event reached the broker, so it says nothing about latency.
- **Latency metrics (time from window end to answer, time to correction) come from the
  simulator**, where arrival time is part of the log. The broker path is a correctness check, not
  a latency measurement.

## What the consumer does

- Reads `orders.v1` from the earliest offset every run, whatever the group has committed (the
  assignment callback rewinds), so a re-run always sees the whole log.
- Stops after `idle_timeout_s` with no message, or after `max_events` messages.
- Then flushes the open windows, produces those final records to `windows.v1`, waits for every
  delivery to be acknowledged (a failed delivery raises), and only then commits offsets.
- Returns `{events, records, dropped, flushed, bad_messages}`. A malformed message is counted in
  `bad_messages` and skipped.

`windows.v1` is append-only and a re-run appends again, so a second run on the same topic shows
duplicates. Use a fresh topic (or `docker compose down -v`) for a clean comparison.

## Broker check (2026-10-23)

`scripts/verify_broker.py` is the broker gate. It runs two registered-scale logs (seed 0, 3,600 s,
20,705 events) through Redpanda in both modes and writes `results/broker_check.json`:

- the C0 cell (5 % late, max delay 30 s, `L = 30`), where nothing can be dropped;
- a stress cell (20 % late, max delay 120 s, `L = 15`), where the drop path and the per-zone drop
  counters are exercised.

For each run it reads the whole output topic with no message cap. It requires all of the following:
- events produced = events in the log = input partition offsets = events consumed;
- records in the simulator = records the consumer reported = output partition offsets = records
  read back;
- read-back offsets contiguous from 0, and no duplicate `(zone, window_start, revision)` key;
- equal drop and flush counts;
- the records, in order, equal `sim.simulate`'s on every field except `emitted_at`, and equal a
  direct core feed with the max-event-time clock exactly.

| Case | Mode | Events | Records (= partition offsets) | Dropped | Equal to simulator |
|---|---|---|---|---|---|
| 5 %, 30 s, L = 30 | wait | 20,705 | 360 | 0 | yes |
| 5 %, 30 s, L = 30 | revise | 20,705 | 1,042 | 0 | yes |
| 20 %, 120 s, L = 15 | wait | 20,705 | 360 | 2,486 | yes |
| 20 %, 120 s, L = 15 | revise | 20,705 | 1,375 | 2,486 | yes |

No message was lost or duplicated, and there were no malformed messages. `uv run pytest -m
integration` (2 tests, smaller logs) also passes. Redpanda `v26.2.4`, confluent-kafka 2.16.0
(librdkafka 2.16.0), Docker Engine 29.7.2 on Windows 11. This is a correctness gate. It measured no
throughput or latency.

Two fixes were needed on the first broker run:
- `--set=redpanda.auto_create_topics_enabled=false` is not a `redpanda start` option, and the broker
  exited at start-up. Auto-creation is now switched off by `rpk cluster config set` in
  `init-topics`.
- The broker advertised `localhost:19092`. On Windows, `localhost` resolves to `::1` first, but the
  port is bound to IPv4 loopback only. Every connection then waited about 2 s for the IPv6 attempt
  to fail, and one admin call ran out its 30 s budget. The broker now advertises `127.0.0.1:19092`,
  and the clients default to it. The integration tests went from 50 s to 4 s.

## Bridge to the dashboard hub

`bridge.run_bridge` reads `windows.v1` and POSTs each record to the hub as
`{"type": "event", "data": {...WindowResult...}}` on the `windows` channel, with `Authorization: Bearer <token>` when a
token is given (`PUBLISH_TOKEN`, the hub's setting). It stops after the idle timeout and raises on
the first 4xx/5xx, so a wrong token fails loudly instead of dropping records.
