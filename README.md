# streaming-pipeline

> An order stream aggregated into 60-second windows per zone, with a measured answer to the
> question every streaming job has to settle: how long to wait for late events.

**Live demo:** not deployed. It runs locally (simulator, or Redpanda in Docker).
**Stack:** Python 3.13 · a hand-written windowing core (stdlib only) · Redpanda (Kafka API) ·
confluent-kafka at the edge only · pytest · ruff · uv

---

## 1. The problem

A batch mart answers yesterday's question. A stream answers "how many orders per zone in the last
minute", but events arrive late and out of order, so every window result is a bet on how long to
wait. Wait longer and the number is complete but stale. Answer early and it is fresh but may be
wrong. This project measures that bet instead of hiding it behind a framework default.

Two emission policies share one windowing core and one lateness budget `L`:

- **wait**: answer once, `L` seconds after the window ends (in event time), and drop anything later.
- **revise**: answer as soon as the window ends, then send a corrected result for each late event,
  until the same `L` runs out.

Both accept exactly the same events, so their final answers are identical by construction (a tested
invariant). What differs is when the first answer goes out, how often it is wrong, and how much
correction traffic it costs downstream.

## 2. The data

| | |
|---|---|
| Source | Synthetic order events from a seeded generator (`src/streampipe/events.py`) |
| Size | about 20,700 events per run (3,600 s of event time, 6 zones); 45 logs in the sweep |
| Licence | Generated; MIT with the repo |
| Lateness model | 0 / 5 / 20 % of events are "late", delayed uniformly up to 5 / 30 / 120 s; the rest arrive within 2 s |

The lateness model is an assumption of the generator, not a property of real order systems. Real
delays are usually heavy-tailed and correlated (a phone loses signal and replays an hour of events),
which this generator does not model. Every number in §5 is a property of this generator.

## 3. Architecture

```mermaid
flowchart LR
    G[Seeded generator] --> L[(Event log<br/>JSONL, arrival order)]
    L --> S[Simulator<br/>stream clock = arrival time]
    L --> P[Producer] --> O[[orders.v1]] --> C[Consumer]
    S --> W[Window core<br/>wait / revise, budget L]
    C --> W
    W --> R[Window results]
    R --> M[Metrics vs batch recompute]
    R --> T[[windows.v1]] --> B[Bridge] --> H[Dashboard hub<br/>29-websocket-live]
    L --> BT[Batch recompute<br/>ground truth] --> M
```

- **`windows.py`**: tumbling windows keyed by `(zone, window_start)`. With `M` the latest event
  time seen, a window is *due* at `M >= end` and *closed* at `M >= end + L`. Events for a closed
  window are dropped and counted. Wait emits once at close; revise emits at due, on each late event,
  and a final record at close. Firing uses heaps ordered by window end, so it costs nothing per event.
- **`sim.py` + `metrics.py` + `evalrun.py`**: replay a log with the arrival time as the clock, score
  every window against a batch group-by of the same log, and run the pre-registered grid.
- **`kafka_io.py` + `bridge.py`**: the same core behind a Redpanda topic, and a bridge that posts
  results to the WebSocket hub from `29-websocket-live` (`POST /publish/windows`). The broker path
  is a correctness check; latency comes from the simulator ([docs/kafka.md](docs/kafka.md)).

## 4. Key decisions & tradeoffs

| Decision | Chose | Over | Why |
|---|---|---|---|
| Windowing engine | Hand-written core, Kafka client only at the edge | Flink, Bytewax, Quix Streams | The result depends on exact watermark and lateness semantics. Owning them makes them testable without a broker and explainable line by line |
| Meaning of `L` | One acceptance horizon (`M < end + L`) shared by both policies | `L` as watermark delay in one mode and grace period in the other | If `L` means two things, "revise is faster" partly measures a different budget. With one meaning, the policies differ only in when they speak |
| Ground truth | Batch recompute of the same log | Comparing the two policies to each other | Each policy is scored against an independent oracle, so "exact" means equal to what a batch job would say |
| Money | Integer cents | Float sums | Stream and batch add in different orders, and float sums would make "exact" depend on order |
| Clock | Stream time (arrival time) in the simulator; max event time on the broker | Wall clock | Results are deterministic and machine-independent; a sweep rerun is byte-identical |
| Broker | Redpanda single node, 1 partition | Kafka KRaft; several partitions | One binary for local work. One partition gives one total order, so the broker run reproduces the simulator |
| Evaluation | Claim frozen in [EVAL_PLAN.md](EVAL_PLAN.md) before any sweep code | Picking `L` after seeing the curve | The plan fixes the grid, metrics, exclusions and pass rule, and `evalrun` checks the claim mechanically |

## 5. Results

**Not yet measured.** The pre-registered sweep (450 runs) has not been run. This section will be
written from `results/` only.

## 6. How to run

Prerequisites: [uv](https://docs.astral.sh/uv/). Docker Desktop only for the broker path.

```bash
git clone https://github.com/Prithv122/streaming-pipeline.git
cd streaming-pipeline
uv sync
uv run pytest -m "not integration"
```

Simulator path (no broker):

```bash
uv run streampipe gen --seed 0 --late-share 0.05 --max-delay 30 --out data/events/s0.jsonl
uv run streampipe replay --log data/events/s0.jsonl --mode revise --lateness 30
uv run streampipe sweep --out results/sweep.csv        # the pre-registered grid
```

Broker path (Redpanda in Docker, see [docs/kafka.md](docs/kafka.md)):

```bash
docker compose up -d
uv sync --extra kafka
uv run streampipe produce --log data/events/s0.jsonl
uv run streampipe consume --mode revise --lateness 30
uv run pytest -m integration
```

`PUBLISH_TOKEN` is the only environment variable, and it is optional. Set it when the bridge posts
to a dashboard hub that requires a bearer token.

## 7. What I'd change at 100× scale

- **One partition is the first thing to break.** Ordering across zones comes from a single
  partition. At scale the topic is partitioned by zone, each partition keeps its own watermark, and
  the job's watermark is the minimum across partitions. An idle partition then stalls every window,
  so it needs idle-source detection.
- **State has to survive a restart.** Window state lives in a dict. A crash loses open windows,
  and a replay from the committed offset double-counts. The fix is checkpointed state plus
  transactional output (or idempotent downstream upserts keyed by `(zone, window_start, revision)`).
- **Revision traffic multiplies with fan-out.** Revise mode sends extra records per window to every
  consumer. With many subscribers, the hub should coalesce revisions per window, or only push
  revisions that change the displayed number.
- **The lateness model must come from data.** At scale, `L` should come from the observed delay
  distribution per source (for example the 99.9th percentile of `arrival − event time`), monitored
  for drift, not from a constant.

---

## References

- Akidau et al., *The Dataflow Model* (VLDB 2015): watermarks, allowed lateness and accumulating
  panes. Revise mode here is a minimal accumulating-pane policy.
- Apache Flink documentation, *Allowed Lateness*: the semantics this core's revise mode mirrors.
