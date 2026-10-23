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

- [ ] Broker run not done: Docker Desktop was not running. The compose file and the integration
      test have never started a broker.
- [ ] Dashboard integration with `29-websocket-live` (the bridge posts `type: "window"`). The hub
      needs a renderer for that type.
