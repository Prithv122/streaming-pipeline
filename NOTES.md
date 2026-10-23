# Build Notes — streaming-pipeline

Working notes: what broke, what was tried, why X over Y.

---

## Log

### 2026-10-23 — contract, core, review, amendment

- **Decided before code:** one lateness budget `L` with one meaning in both policies: a window
  accepts events while `M < end + L`. The first draft read "wait = watermark delay L, revise =
  emit at the watermark and keep the window open L more". That makes `L` mean two different
  things, and "revise is faster" would partly measure a different budget. With one acceptance
  horizon, wait is revise with the provisional records suppressed, and the tests pin that
  equivalence (equal finals, equal drops, equal retained state, identical output at `L = 0`).
- **Money in integer cents.** Stream and batch sum the same amounts in different orders. Float
  sums would make "exact" depend on order.
- **Built in parallel against the contract** (`GUIDELINES.md`): generator + batch truth, the
  core with hand-derived fixtures, simulator/metrics/sweep/CLI, and the Kafka edge. Shared types
  (`OrderEvent`, `WindowResult`) were committed first so no part waited on another.
- **Fixture derivation caught a bad fixture, not a bad core.** Two hand-placed events had
  `arrival_time < event_time`. One expected record was also wrong by hand: a window that becomes
  due and closed in the same step emits one final record, not a provisional followed by a final.
- **The core is checked against a literal-spec oracle.** The oracle scans every window on every
  event. It is slow, but obviously faithful to the spec, and property tests compare the two on
  seeded random logs.

### 2026-10-23 — the pre-registered claim could not fail

- An independent review before the sweep found the frozen claim was true by construction. With
  delay ≤ 30 s and `L = 30`, an event always arrives before `M` reaches `end + 30`. So nothing is
  dropped and exactness is exactly 1. Wait-mode latency is `L` + jitter + one inter-arrival gap,
  about 31.5 s, against a 90 s bar.
- Lesson: check a pre-registered threshold against the generator's own bounds before freezing it.
  A claim derived from a proposal can pass by construction without anyone noticing.
- **Fix (Amendment 1):** the original claim is kept and relabelled C0, a construction check. The
  new primary claim is falsifiable: an analytic model of the generator (`ANALYTIC.md`) predicts the
  drop rate, revise's first-answer exactness and first-emit latency. Tolerances come from the
  model's own variance at 4 SD and are frozen with the predictions. The reviewer's unregistered
  seeds 100–104 are not used to build or tune the model.
- **Other review findings fixed before any run:**
  - Accuracy is also reported over windows not closed by the end-of-stream flush. Flushed windows
    are always exact and made the stress regime look better than it is.
  - Correction traffic counts only value-changing revisions. A closing record that repeats the
    previous values is not a correction.
  - `sweep --small` no longer writes into `results/`.
  - Three mutation-test gaps are closed.

### 2026-10-23 — broker gate

- **The first `docker compose up` failed.** `--set=redpanda.auto_create_topics_enabled=false` is
  not a `redpanda start` flag, and the broker exited. Auto-creation is now off through
  `rpk cluster config set` in `init-topics`.
- **`localhost` on Windows means `::1` first.** The broker advertised `localhost:19092`, but the
  port is bound to IPv4 loopback only. Every client connection waited about 2 s for the IPv6
  attempt to fail, and one admin call timed out after 30 s. Advertising and using `127.0.0.1` fixed
  it: the integration tests went from 50 s to 4 s.
- **A passing integration test was not enough.** It read back at most the expected number of
  messages, so a duplicate after that point would not show. Its log was also small. The gate script
  reads each topic to the end, checks the counts against the broker's own partition offsets, and
  uses two registered-scale logs.
- **The first gate case could not drop anything.** It was the C0 cell (`L ≥ max delay`), so a
  stress cell (12 % dropped) was added. Otherwise the drop path would never have crossed the broker.
- **Two hangs were in my verification code, not the pipeline.** One was an offset query on a
  subscribed-but-unassigned consumer. The admin API's `list_offsets` needs no consumer group.
- Result: identical records to the simulator, in order, in all four runs. No loss, no duplicates.

---

### 2026-10-23 — bridge envelope fixed against the real hub

- The bridge posted `{"type": "window"}`. The real hub accepts only `type: "event"` and answered
  422, so the first record would have stopped the bridge. The bridge tests used a stub that
  accepted any body, so they could not catch it. The envelope is now `{"type": "event"}`, and the
  `windows` channel marks the payload as a window record, as `orders` marks an order.
- Checked end to end: a 600 s seeded log went produce → consume (revise, `L = 30`) → bridge →
  hub. The bridge forwarded 176 records, the same count the simulator emits for that log, and the
  dashboard's rows matched the simulator's records.
- Lesson: a stub that accepts everything tests the HTTP call, not the contract. The contract
  check needs the real receiver, at least once.

---

## Rejected approaches

| Approach | Why rejected |
|---|---|
| Flink / Bytewax / Quix Streams for windowing | The result depends on exact watermark and lateness semantics. A framework hides them, and needs a broker to test |
| `L` as watermark delay in wait mode, grace period in revise mode | Two meanings for one knob; the comparison would not be like for like |
| Wall clock or broker timestamps as the stream clock | Not deterministic; output would depend on machine speed and replay pace |
| Several partitions on `orders.v1` | Kafka orders only within a partition, so `M` and the drop decisions would change run to run |
| Keeping the original claim as primary and disclosing it | A claim that cannot fail tests nothing |

## Open questions

- [x] Broker run done 2026-10-23: the gate passed in all four runs (see the log entry above and
      `docs/kafka.md`).
- [x] Dashboard integration with `29-websocket-live`, done 2026-10-23 (see the log entry above).
