"""Score a simulated run against the batch recompute (definitions: EVAL_PLAN.md -> Metrics)."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from itertools import pairwise

from streampipe.batch import BatchWindow
from streampipe.sim import SimResult
from streampipe.windows import WindowResult

Key = tuple[str, float]

METRIC_KEYS: tuple[str, ...] = (
    "exact_rate_final",
    "exact_rate_first",
    "mean_abs_count_error",
    "late_dropped_rate",
    "first_emit_latency_p50",
    "first_emit_latency_p95",
    "first_emit_latency_max",
    "final_latency_p50",
    "final_latency_p95",
    "final_latency_max",
    "revisions_per_window",
    "records_emitted",
    "peak_open_windows",
    "n_windows",
    "n_exact_final",
    "n_exact_first",
    "n_events",
    "n_dropped",
    "n_emitted_windows",
    "abs_count_error_sum",
    "exact_rate_final_live",
    "exact_rate_first_live",
    "mean_abs_count_error_live",
    "value_revisions_per_window",
    "n_flushed_windows",
    "n_windows_live",
    "n_windows_first_live",
    "n_exact_final_live",
    "n_exact_first_live",
    "abs_count_error_sum_live",
    "n_value_revisions",
)


def percentile(values: Sequence[float], p: float) -> float:
    """Nearest-rank percentile: the value at rank ceil(p/100 * n) of the sorted list (1-based).

    Always returns an observed value, never interpolates. p50 of [1, 2, 3, 4] is 2; p95 of
    twenty values is the 19th smallest. An empty list gives nan.
    """
    if not values:
        return math.nan
    ordered = sorted(values)
    rank = max(1, math.ceil(p * len(ordered) / 100))
    return float(ordered[rank - 1])


@dataclass(frozen=True)
class RunScore:
    """Metrics of one run plus the per-window latencies needed to pool percentiles."""

    metrics: dict[str, float]
    first_latencies: list[float] = field(default_factory=list)
    final_latencies: list[float] = field(default_factory=list)


def _latency_stats(latencies: Sequence[float]) -> tuple[float, float, float]:
    if not latencies:
        return math.nan, math.nan, math.nan
    return percentile(latencies, 50), percentile(latencies, 95), float(max(latencies))


def _is_exact(record: WindowResult | None, truth: BatchWindow) -> bool:
    return (
        record is not None
        and record.orders == truth.orders
        and record.revenue_cents == truth.revenue_cents
    )


def _ratio(num: float, den: float) -> float:
    return num / den if den else math.nan


def score_run(result: SimResult, truth: dict[Key, BatchWindow]) -> RunScore:
    """Score one run. Accuracy covers every batch window; latency skips flush-produced records.

    The `*_live` accuracy metrics also skip flush-produced records: `exact_rate_final_live` and
    `mean_abs_count_error_live` cover the batch windows whose final record did not come from the
    end-of-stream flush, `exact_rate_first_live` those whose first record did not. A batch window
    with no streaming record counts as live and not exact. `value_revisions_per_window` counts
    records that changed `(orders, revenue_cents)` against the same window's previous record.
    """
    flushed_ids = {(r.zone, r.window_start, r.revision) for r in result.flushed}
    by_window: dict[Key, list[WindowResult]] = defaultdict(list)
    for r in result.records:
        by_window[(r.zone, r.window_start)].append(r)

    first: dict[Key, WindowResult] = {}
    final: dict[Key, WindowResult] = {}
    first_lat: list[float] = []
    final_lat: list[float] = []
    first_flushed: set[Key] = set()
    final_flushed: set[Key] = set()
    n_value_revisions = 0
    for key, recs in by_window.items():
        recs.sort(key=lambda r: r.revision)
        first[key] = recs[0]
        final[key] = next((r for r in recs if r.final), recs[-1])
        if (key[0], key[1], recs[0].revision) not in flushed_ids:
            first_lat.append(recs[0].emitted_at - recs[0].window_end)
        else:
            first_flushed.add(key)
        fin = final[key]
        if (key[0], key[1], fin.revision) not in flushed_ids:
            final_lat.append(fin.emitted_at - fin.window_end)
        else:
            final_flushed.add(key)
        n_value_revisions += sum(
            (a.orders, a.revenue_cents) != (b.orders, b.revenue_cents) for a, b in pairwise(recs)
        )

    n_windows = len(truth)
    n_exact_final = n_exact_first = 0
    abs_err = 0
    n_windows_live = n_windows_first_live = 0
    n_exact_final_live = n_exact_first_live = 0
    abs_err_live = 0
    for key, tw in truth.items():
        exact_final = _is_exact(final.get(key), tw)
        exact_first = _is_exact(first.get(key), tw)
        err = abs(final[key].orders - tw.orders) if key in final else tw.orders
        n_exact_final += exact_final
        n_exact_first += exact_first
        abs_err += err
        if key not in final_flushed:
            n_windows_live += 1
            n_exact_final_live += exact_final
            abs_err_live += err
        if key not in first_flushed:
            n_windows_first_live += 1
            n_exact_first_live += exact_first

    n_emitted_windows = len(by_window)
    records_emitted = len(result.records)
    n_dropped = len(result.dropped)
    f50, f95, fmax = _latency_stats(first_lat)
    l50, l95, lmax = _latency_stats(final_lat)
    metrics = {
        "exact_rate_final": n_exact_final / n_windows if n_windows else math.nan,
        "exact_rate_first": n_exact_first / n_windows if n_windows else math.nan,
        "mean_abs_count_error": abs_err / n_windows if n_windows else math.nan,
        "late_dropped_rate": n_dropped / result.events_in if result.events_in else 0.0,
        "first_emit_latency_p50": f50,
        "first_emit_latency_p95": f95,
        "first_emit_latency_max": fmax,
        "final_latency_p50": l50,
        "final_latency_p95": l95,
        "final_latency_max": lmax,
        "revisions_per_window": (
            records_emitted / n_emitted_windows - 1 if n_emitted_windows else 0.0
        ),
        "records_emitted": float(records_emitted),
        "peak_open_windows": float(result.peak_open_windows),
        "n_windows": float(n_windows),
        "n_exact_final": float(n_exact_final),
        "n_exact_first": float(n_exact_first),
        "n_events": float(result.events_in),
        "n_dropped": float(n_dropped),
        "n_emitted_windows": float(n_emitted_windows),
        "abs_count_error_sum": float(abs_err),
        "exact_rate_final_live": _ratio(n_exact_final_live, n_windows_live),
        "exact_rate_first_live": _ratio(n_exact_first_live, n_windows_first_live),
        "mean_abs_count_error_live": _ratio(abs_err_live, n_windows_live),
        "value_revisions_per_window": (
            n_value_revisions / n_emitted_windows if n_emitted_windows else 0.0
        ),
        "n_flushed_windows": float(len({(r.zone, r.window_start) for r in result.flushed})),
        "n_windows_live": float(n_windows_live),
        "n_windows_first_live": float(n_windows_first_live),
        "n_exact_final_live": float(n_exact_final_live),
        "n_exact_first_live": float(n_exact_first_live),
        "abs_count_error_sum_live": float(abs_err_live),
        "n_value_revisions": float(n_value_revisions),
    }
    return RunScore(metrics, first_lat, final_lat)


def score(result: SimResult, truth: dict[Key, BatchWindow]) -> dict[str, float]:
    """The EVAL_PLAN metrics for one run, plus the counts needed to pool runs."""
    return score_run(result, truth).metrics


def pool(runs: Sequence[RunScore]) -> dict[str, float]:
    """Pool runs (seeds) of one cell into one metrics dict.

    Rates are ratios of summed counts. Percentiles and maxima are computed over the
    concatenated per-window latencies, not averaged across runs. `records_emitted` is the
    mean per run and `peak_open_windows` the maximum over runs.
    """
    if not runs:
        raise ValueError("pool needs at least one run")

    def total(key: str) -> float:
        return sum(r.metrics[key] for r in runs)

    n_windows = total("n_windows")
    n_events = total("n_events")
    n_emitted = total("n_emitted_windows")
    records = total("records_emitted")
    first_lat = [x for r in runs for x in r.first_latencies]
    final_lat = [x for r in runs for x in r.final_latencies]
    f50, f95, fmax = _latency_stats(first_lat)
    l50, l95, lmax = _latency_stats(final_lat)
    return {
        "exact_rate_final": total("n_exact_final") / n_windows if n_windows else math.nan,
        "exact_rate_first": total("n_exact_first") / n_windows if n_windows else math.nan,
        "mean_abs_count_error": total("abs_count_error_sum") / n_windows if n_windows else math.nan,
        "late_dropped_rate": total("n_dropped") / n_events if n_events else 0.0,
        "first_emit_latency_p50": f50,
        "first_emit_latency_p95": f95,
        "first_emit_latency_max": fmax,
        "final_latency_p50": l50,
        "final_latency_p95": l95,
        "final_latency_max": lmax,
        "revisions_per_window": records / n_emitted - 1 if n_emitted else 0.0,
        "records_emitted": records / len(runs),
        "peak_open_windows": max(r.metrics["peak_open_windows"] for r in runs),
        "n_windows": n_windows,
        "n_exact_final": total("n_exact_final"),
        "n_exact_first": total("n_exact_first"),
        "n_events": n_events,
        "n_dropped": total("n_dropped"),
        "n_emitted_windows": n_emitted,
        "abs_count_error_sum": total("abs_count_error_sum"),
        "exact_rate_final_live": _ratio(total("n_exact_final_live"), total("n_windows_live")),
        "exact_rate_first_live": _ratio(total("n_exact_first_live"), total("n_windows_first_live")),
        "mean_abs_count_error_live": _ratio(
            total("abs_count_error_sum_live"), total("n_windows_live")
        ),
        "value_revisions_per_window": total("n_value_revisions") / n_emitted if n_emitted else 0.0,
        "n_flushed_windows": total("n_flushed_windows"),
        "n_windows_live": total("n_windows_live"),
        "n_windows_first_live": total("n_windows_first_live"),
        "n_exact_final_live": total("n_exact_final_live"),
        "n_exact_first_live": total("n_exact_first_live"),
        "abs_count_error_sum_live": total("abs_count_error_sum_live"),
        "n_value_revisions": total("n_value_revisions"),
    }
