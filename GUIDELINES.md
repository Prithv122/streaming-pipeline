# streaming-pipeline — E4

**Tier:** 3 · **Category:** E — Data engineering · **Wave:** 4

The project's own engineering guide and the contract every module is built against.

## What this is

An order-event stream (Redpanda, Kafka API) aggregated into 60 s tumbling windows per zone by a
hand-written windowing core, with a measured answer to the question every streaming job has to
settle: how long to wait for late events. Two emission policies share one core and one lateness
budget `L`: **wait** (answer once, late) and **revise** (answer early, then correct). Both are
scored against an offline batch recompute of the same event log.

## Stack

Python 3.13 · uv · stdlib-only core (no streaming framework: the windowing semantics are the
point) · `confluent-kafka` at the edge only, in the optional `kafka` extra · Redpanda single node
in Docker Compose for the local end-to-end run · pytest · ruff.

## Non-negotiables

- The core (`events`, `windows`, `batch`, `sim`, `metrics`, `evalrun`) never imports a Kafka
  client and never reads the wall clock. Time is **stream time**: the arrival time of the event
  being processed.
- Every result is deterministic for a given seed and config.
- `EVAL_PLAN.md` is frozen before the first sweep. Changes after that are dated amendments made
  before the runs they govern.
- No number goes in the README that a committed script did not produce.

## Data contract

`OrderEvent` (same fields as `29-websocket-live`, plus `event_time`):

| Field | Type | Notes |
|---|---|---|
| `order_id` | str | `ord-000001`; unique per log |
| `event_time` | float | seconds from stream start, 3 decimals |
| `zone` | str | one of `north south east west central airport` |
| `amount` | float | > 0, 2 decimals |
| `status` | str | one of `placed picked delivered cancelled` (carried, not filtered) |

Money is aggregated in integer cents: `to_cents(amount) = round(amount * 100)`. Revenue
equality is integer equality, so summation order cannot break exactness.

**Event log** (`*.jsonl`): one line per event in arrival order, the `OrderEvent` fields plus
`arrival_time` (float seconds, `>= event_time`). Order is `(arrival_time, order_id)`.

**Kafka topics.** `orders.v1`: key = zone, value = `OrderEvent` JSON (no `arrival_time`).
`windows.v1`: key = zone, value = `WindowResult` JSON.

`WindowResult`:

| Field | Type | Notes |
|---|---|---|
| `window_start`, `window_end` | float | `[start, end)`, `end = start + 60` |
| `zone` | str | |
| `orders` | int | events accepted into the window so far |
| `revenue_cents` | int | sum of `to_cents(amount)` |
| `revenue` | float | `revenue_cents / 100`, for display |
| `late_dropped` | int | cumulative late events dropped **for this zone** when the record was emitted |
| `revision` | int | 0 for the first record of a window, +1 per later record |
| `final` | bool | true on the last record of a window; nothing follows it |
| `emitted_at` | float | stream time of the emit (arrival time of the triggering event, or the flush time) |

## Windowing semantics (the core)

Tumbling windows of `window_s = 60`: `window_start = floor(event_time / 60) * 60`, keyed by
`(zone, window_start)`. Only windows with at least one accepted event exist.

`M` = the maximum `event_time` processed so far. `L` = the lateness budget, the same in both
modes. A window `[s, e)` is:

- **due** when `M >= e`,
- **closed** when `M >= e + L`. A closed window's state is discarded.

For each event, at stream time `now` (its arrival time), in this order:

1. If its window is closed (`M >= e + L`, using `M` before this event), **drop** it: append a
   `LateDrop(zone, window_start, order_id, event_time, now)` and increment the zone's drop count.
   Stop.
2. Otherwise add it to the window (create the window if needed). In **revise** mode, if the
   window is already due, emit a provisional record now (`final=False`; revision 0 if the
   window had no record yet, else last + 1).
3. Set `M = max(M, event_time)`.
4. Fire windows in `(window_end, zone)` order:
   - any window now closed → emit its final record (`final=True`) and discard its state. In
     revise mode this record is `last revision + 1`, or revision 0 if it never emitted;
     in wait mode it is always revision 0.
   - **revise mode only:** any window now due, not closed, with no record yet → emit a
     provisional revision 0.
   A window that becomes due and closed in the same step emits **one** final record.

`flush(now)` emits a final record for every open window (end of stream) and reports which
records it produced, so latency metrics can exclude them.

Consequences the tests pin down:

- **Wait mode = revise mode with provisional records suppressed.** The same events are accepted
  and dropped, final values are equal, and retained state is equal.
- With `L = 0` the two modes produce identical output.
- Retained state (`open_windows`, `peak_open_windows`) is reported, but is the same for both
  modes by construction.

## Module map

| Module | Public API |
|---|---|
| `events.py` | `ZONES`, `STATUSES`, `OrderEvent`, `Arrival(event, arrival_time)`, `GenConfig`, `generate(cfg) -> list[Arrival]`, `write_log`, `read_log`, `to_cents` |
| `batch.py` | `batch_windows(events, window_s=60) -> dict[(zone, window_start), BatchWindow(orders, revenue_cents)]` |
| `windows.py` | `WindowResult`, `LateDrop`, `WindowCore(mode, lateness_s, window_s=60)` with `.process(event, now)`, `.flush(now)`, `.dropped`, `.open_windows`, `.peak_open_windows` |
| `sim.py` | `simulate(arrivals, mode, lateness_s, window_s=60) -> SimResult(records, flushed, dropped, peak_open_windows, events_in)` |
| `metrics.py` | `score(sim_result, truth) -> dict[str, float]` (fields in `EVAL_PLAN.md`) |
| `evalrun.py` | `run_grid(grid, out_csv)`, `SMALL_GRID`, `FULL_GRID` |
| `kafka_io.py` | `produce(arrivals, bootstrap, topic, pace)`, `run_consumer(bootstrap, mode, lateness_s, ...)` |
| `bridge.py` | `to_envelope(result)`, `publish_http(url, envelope, token)`, `run_bridge(bootstrap, url, ...)` |
| `cli.py` | `streampipe gen | replay | sweep | produce | consume | bridge` |

## Acceptance criteria

- [ ] Hand-computed fixture tests pass for both modes (window boundary, late within L, late
      beyond L, out of order across a boundary, revision emit, L = 0 equivalence)
- [ ] `streampipe sweep --small` finishes in under 30 s; the full sweep writes `results/sweep.csv`
- [ ] Pre-registered claim in `EVAL_PLAN.md` evaluated mechanically and reported whatever the outcome
- [ ] Local end-to-end run through Redpanda reproduces the simulator's windows for the same log
- [ ] Runs from a clean clone using only the documented steps; tests, lint and CI green

## Conventions

- `uv` for dependencies; `ruff check` + `ruff format`; `pytest`.
- Tests that need a broker are marked `integration` and deselected in CI.
- `PUBLISH_TOKEN` (bridge to the dashboard hub) is the only env var; it is optional.

## Project-specific notes

- Docker Desktop must be running for `docker compose up` (Redpanda). Nothing else needs it.
- The lateness model is an assumption of the generator, not a property of real order systems.
