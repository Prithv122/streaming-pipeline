# Evaluation plan — frozen before any sweep

**Frozen:** 2026-10-23, in the commit that adds this file, before any sweep code exists.
Changes after this commit are dated amendments at the bottom, written before the runs they govern.

## Question

For a fixed lateness budget `L`, is it better to delay the first answer until the window is
complete (**wait**), or to answer as soon as the window is due and revise as late events
arrive (**revise**)? And how does `L` trade window accuracy against emit latency and dropped
late events?

Semantics of `L` and of both modes: `GUIDELINES.md` → *Windowing semantics*. In short, both
modes accept an event while `M < window_end + L` (`M` = max event time seen); wait emits once at
`M >= window_end + L`, revise emits at `M >= window_end` and revises until then.

## Ground truth

Batch recompute over the full event log: count and revenue (integer cents) per
`(zone, window_start)`, 60 s tumbling windows by event time. A window is **exact** iff the
streaming result's `orders` **and** `revenue_cents` equal the batch values. A batch window with
no streaming record is not exact (count error = batch count).

## Generator (fixed for the whole sweep)

| Parameter | Value |
|---|---|
| Duration | 3,600 s of event time (60 windows per zone) |
| Zones | 6 (`north south east west central airport`), chosen uniformly |
| Base rate | Poisson, 5 events/s |
| Bursts | every 600 s, 30 s at 4× the base rate |
| Amount | lognormal(6.0, 0.6), floor 50.00, 2 decimals (as `29-websocket-live`) |
| On-time delay | uniform(0, 2) s |
| Late delay | uniform(2, max delay) s, for the late share of events |
| Seeds | 0, 1, 2, 3, 4 |

The lateness model is an assumption. Real order systems differ, and the README says so.

## Grid

| Axis | Values |
|---|---|
| Late share | 0 %, 5 %, 20 % |
| Max delay | 5, 30, 120 s |
| L | 0, 5, 15, 30, 60 s |
| Mode | wait, revise |
| Seed | 0–4 |

3 × 3 × 5 × 2 × 5 = 450 runs. Window size is fixed at 60 s. Baseline: `L = 0` (both modes are
identical there: emit at the watermark, drop everything later).

## Metrics (per run; pooled over seeds per cell)

| Metric | Definition |
|---|---|
| `exact_rate_final` | exact windows / batch windows, using each window's final record |
| `exact_rate_first` | same, using each window's first record (provisional accuracy; equals final in wait mode) |
| `mean_abs_count_error` | mean over batch windows of abs(final `orders` − batch `orders`) |
| `late_dropped_rate` | dropped events / all events |
| `first_emit_latency_p50/p95/max` | `emitted_at − window_end` of each window's first record, in stream seconds |
| `final_latency_p50/p95/max` | same for each window's final record |
| `revisions_per_window` | mean number of records per window − 1 |
| `records_emitted` | total records sent downstream |
| `peak_open_windows` | maximum retained window states |

Latency metrics exclude windows whose final record came from the end-of-stream flush (and, for
first-emit latency, windows whose first record came from the flush). Accuracy metrics include
every window.

## Pre-registered claim (primary)

**Wait mode, `L = 30 s`, 5 % late, max delay 30 s: pooled `exact_rate_final >= 0.99` and pooled
`first_emit_latency_p95 <= 90 s`.** Both must hold. Evaluated mechanically by `evalrun` and
reported whatever the outcome.

## Reported, not claimed

- **Wait vs revise at equal `L`:** first-emit latency, `exact_rate_first`, revisions per window,
  records emitted. Final accuracy and retained state are equal by construction (a tested
  invariant), so they are shown as a check, not as a finding.
- **Stress regime:** 20 % late, max delay 120 s, every `L`. Reported regardless of outcome. No
  prediction is registered for it.
- **Null result:** if accuracy is already about 100 % at `L = 0` because delays are short next
  to the window, the report says so and points at the regime where that stops being true.

## What would not count

- Any sweep run on a config other than the one above, unless an amendment precedes it.
- Fixture or smoke runs (`--small`) as findings. They test code, not the question.
- Throughput or consumer-lag numbers from a broker run are a separate local measurement, not
  part of this claim.

## Amendments

### Amendment 1 — 2026-10-23, before any registered sweep

**What prompted it.** An independent pre-run review found that the pre-registered claim above
cannot fail under this plan's own generator. The registered sweep (seeds 0–4) has not run. The
reviewer ran unregistered seeds 100–104 on the claim cell and the stress cell only. Those numbers
are not used anywhere below: the model in `ANALYTIC.md` is derived from the generator table and
the windowing semantics alone, with no fitted parameter.

**A1.1 The original claim stays, relabelled as construction check C0.** It is still evaluated and
reported exactly as written. Why it is guaranteed:
- *Exactness.* A late delay is at most `max delay` = 30 s, and every event's time is below its
  window end `e`. So it arrives before `e + 30`. `M` never exceeds the current arrival time, so
  `M < e + L` with `L = 30` for every event. Nothing is dropped, and every final record holds the
  whole window: `exact_rate_final = 1`. The same holds in every cell with `L ≥ max delay`.
- *Latency.* Wait mode emits at the first arrival of an event with time `≥ e + L`, which is
  `L` plus on-time jitter (≤ 2 s) plus the gap to the next event. That is about `L + 1.5 s`, far
  below 90 s unless the stream pauses for about a minute, which the generator never does.

**A1.2 New primary claim: the analytic model predicts the core.** `ANALYTIC.md` derives, from the
frozen generator and semantics:
- **P1:** the late-drop rate in all 45 (late share, max delay, L) cells;
- **P2:** revise mode's first-answer exactness (`exact_rate_first_live`) in all 9 (late share, max
  delay) cells;
- **P3:** first-emit latency p95: wait mode in all 45 cells, revise mode in all 9.

The predicted values and their tolerances (`ANALYTIC.md` §6) are computed by
`src/streampipe/analytic.py` and committed in `results/predictions.json` **before** the registered
sweep runs. **The claim passes iff every P1, P2 and P3 check passes.** `evalrun` evaluates it
mechanically. One failure falsifies it, and it is reported either way. Tolerances are never widened
after results are seen.

**A1.3 Added reported metrics (no claim attached).**
- `exact_rate_final_live`, `exact_rate_first_live` and `mean_abs_count_error_live`: accuracy over
  windows whose record did not come from the end-of-stream flush. Flushed windows are always exact,
  because every event has arrived by then, so they inflate the plain rates.
- `value_revisions_per_window`: records after the first whose `(orders, revenue_cents)` changed.
  Correction traffic is reported with this metric, not with `revisions_per_window`, which also
  counts a closing record that repeats the previous values.
- The wait-vs-revise comparison keeps first-emit latency, final latency, final exactness (live and
  plain), drop rate and value-changing revisions, side by side at equal `L`.

**A1.4 Reporting notes.**
- Percentiles are nearest-rank.
- Rows with 0 % late share are identical across max delay. They are one result shown three times,
  not replications.
- For a given seed, every cell shares the same event times, zones and amounts; only arrival times
  differ. Cells are paired, not independent.
- In the stress regime (20 %, 120 s), exactness is near 0 at every `L`, so the report leads with
  count error and drop rate there.
- Bursts start at `t = 0` and fill the first half of every tenth window.

**A1.5 Process.** A fresh-context reviewer checks `ANALYTIC.md`, `analytic.py`, the predictions and
the evaluation code before the registered sweep runs. Amendment 1, the predictions and the reviewed
code are committed first.

**A1.6 Changes after the second pre-run review (same day, before any registered run).** The review
found no correctness error. It reproduced 20 predictions with independent quadrature (largest P1
gap: 0.5 events against tolerances of 94 or more). It also confirmed that plausible bugs would fail
the claim. Changes, all made before any registered run, and none informed by data:
- **P2 uses exact count regions** from the model's distribution, at the same per-tail level as a
  ±4σ normal check (`ANALYTIC.md` §6). The normal rule had a 0.46 % false-failure rate at
  (20 %, 120 s), where the expected count is 0.66.
- **P0 added:** the pooled event count must match Poisson(103,500).
- **P4 added:** wait-mode `exact_rate_final_live` per cell, predicted by `ANALYTIC.md` §4b and checked
  with the same exact regions. This covers the headline final-accuracy metric.
- **Invariant checks are now explicit:** revise must equal wait on drop counts and live final-exact
  counts, with no tolerance.
- **Correction to A1.2's wording.** The evaluator checks P3-revise at every `L` (45 checks, all
  against the same 9 predictions), not 9. The claim passes iff every P0, P1, P2, P3, P4 and
  invariant check passes.
- **Correction to A1.4.** The stress-regime sentence "exactness is near 0 at every `L`" is withdrawn.
  The model's own values (P4) replace it.
- **Disclosure.** The full grid makes 369 mechanical checks carrying about 50–55 independent tests. The false-failure
  probability if the model is right is about 0.5 % overall (`ANALYTIC.md` §6).
- **Integrity.** `results/predictions.sha256` pins the predictions file. The registered sweep refuses
  to run if the file is missing or its hash differs.
