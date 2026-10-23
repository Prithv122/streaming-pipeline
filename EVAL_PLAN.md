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

_None._
