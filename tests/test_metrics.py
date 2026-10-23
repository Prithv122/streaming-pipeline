import math

import pytest

from streampipe.batch import BatchWindow
from streampipe.metrics import METRIC_KEYS, RunScore, percentile, pool, score, score_run
from streampipe.sim import SimResult
from streampipe.windows import LateDrop, WindowResult


def rec(
    zone: str, start: float, orders: int, cents: int, revision: int, final: bool, at: float
) -> WindowResult:
    return WindowResult(start, start + 60, zone, orders, cents, cents / 100, 0, revision, final, at)


def sim(
    records: list[WindowResult],
    flushed: list[WindowResult] | None = None,
    dropped: int = 0,
    events: int = 10,
    peak: int = 3,
) -> SimResult:
    drops = [LateDrop("north", 0.0, f"o{i}", 1.0, 2.0) for i in range(dropped)]
    return SimResult(records, flushed or [], drops, peak, events, "revise", 30.0)


TRUTH = {
    ("north", 0.0): BatchWindow(3, 300),
    ("south", 0.0): BatchWindow(2, 200),
    ("north", 60.0): BatchWindow(4, 400),
}


def test_percentile_nearest_rank() -> None:
    assert percentile([4, 1, 3, 2], 50) == 2
    assert percentile([4, 1, 3, 2], 75) == 3
    assert percentile([4, 1, 3, 2], 100) == 4
    assert percentile(list(range(1, 21)), 95) == 19
    assert percentile(list(range(1, 101)), 95) == 95
    assert percentile([7], 95) == 7
    assert percentile([7, 9], 0) == 7
    assert math.isnan(percentile([], 50))


def test_metric_keys_cover_the_plan() -> None:
    m = score(sim([]), TRUTH)
    assert set(m) == set(METRIC_KEYS)
    plan = {
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
    }
    assert plan <= set(m)


def test_wait_style_exact_and_latency() -> None:
    records = [
        rec("north", 0.0, 3, 300, 0, True, 90.0),  # latency 30
        rec("south", 0.0, 2, 200, 0, True, 100.0),  # latency 40
        rec("north", 60.0, 4, 400, 0, True, 150.0),  # latency 30
    ]
    m = score(sim(records), TRUTH)
    assert m["exact_rate_final"] == 1.0
    assert m["exact_rate_first"] == 1.0
    assert m["mean_abs_count_error"] == 0.0
    assert m["revisions_per_window"] == 0.0
    assert m["records_emitted"] == 3
    assert m["first_emit_latency_p50"] == 30.0
    assert m["first_emit_latency_p95"] == 40.0
    assert m["first_emit_latency_max"] == 40.0
    assert m["final_latency_p50"] == 30.0
    assert m["peak_open_windows"] == 3


def test_revise_first_vs_final_and_revisions() -> None:
    records = [
        rec("north", 0.0, 2, 200, 0, False, 62.0),  # first: wrong, latency 2
        rec("north", 0.0, 3, 300, 1, True, 120.0),  # final: exact, latency 60
        rec("south", 0.0, 2, 200, 0, True, 125.0),  # first = final, latency 65
        rec("north", 60.0, 4, 400, 0, False, 125.0),  # first: exact, latency 5
        rec("north", 60.0, 4, 400, 1, False, 130.0),
        rec("north", 60.0, 4, 400, 2, True, 190.0),  # final latency 70
    ]
    m = score(sim(records), TRUTH)
    assert m["exact_rate_final"] == 1.0
    assert m["exact_rate_first"] == pytest.approx(2 / 3)
    assert m["records_emitted"] == 6
    assert m["revisions_per_window"] == pytest.approx(6 / 3 - 1)
    assert m["first_emit_latency_p50"] == 5.0
    assert m["first_emit_latency_max"] == 65.0
    assert m["final_latency_p50"] == 65.0
    assert m["final_latency_max"] == 70.0


def test_wrong_value_and_revenue_mismatch_not_exact() -> None:
    records = [
        rec("north", 0.0, 3, 301, 0, True, 90.0),  # count right, revenue wrong
        rec("south", 0.0, 1, 200, 0, True, 90.0),  # count wrong by 1
        rec("north", 60.0, 4, 400, 0, True, 150.0),
    ]
    m = score(sim(records), TRUTH)
    assert m["n_exact_final"] == 1
    assert m["exact_rate_final"] == pytest.approx(1 / 3)
    assert m["mean_abs_count_error"] == pytest.approx(1 / 3)


def test_missing_window_is_not_exact_and_error_is_batch_count() -> None:
    records = [rec("north", 0.0, 3, 300, 0, True, 90.0)]
    m = score(sim(records), TRUTH)
    assert m["n_windows"] == 3
    assert m["n_exact_final"] == 1
    assert m["exact_rate_first"] == pytest.approx(1 / 3)
    assert m["mean_abs_count_error"] == pytest.approx((0 + 2 + 4) / 3)
    assert m["n_emitted_windows"] == 1
    assert m["revisions_per_window"] == 0.0


def test_flush_records_excluded_from_latency_but_not_accuracy() -> None:
    f_south = rec("south", 0.0, 2, 200, 0, True, 500.0)  # flushed; would be latency 440
    # flushed final of a revised window; its values differ from the provisional before it
    f_north2 = rec("north", 60.0, 4, 400, 1, True, 500.0)
    records = [
        rec("north", 0.0, 3, 300, 0, True, 90.0),
        f_south,
        rec("north", 60.0, 3, 300, 0, False, 125.0),
        f_north2,
    ]
    m = score(sim(records, flushed=[f_south, f_north2]), TRUTH)
    # accuracy scores the flushed final (exact), not the provisional before it (wrong)
    assert m["exact_rate_final"] == 1.0
    assert m["exact_rate_first"] == pytest.approx(2 / 3)
    # south's only record is flushed: out of both. north@60: live first (5), flushed final.
    assert m["first_emit_latency_max"] == 30.0
    assert m["first_emit_latency_p50"] == 5.0
    assert m["final_latency_max"] == 30.0


def test_all_flushed_gives_nan_latency() -> None:
    r = rec("north", 0.0, 3, 300, 0, True, 500.0)
    m = score(sim([r], flushed=[r]), {("north", 0.0): BatchWindow(3, 300)})
    assert math.isnan(m["first_emit_latency_p95"])
    assert math.isnan(m["final_latency_max"])
    assert m["exact_rate_final"] == 1.0


def test_live_metrics_skip_windows_whose_final_came_from_the_flush() -> None:
    f_south = rec("south", 0.0, 2, 200, 0, True, 500.0)
    f_north2 = rec("north", 60.0, 4, 400, 1, True, 500.0)  # differs from its provisional
    records = [
        rec("north", 0.0, 2, 200, 0, False, 62.0),  # wrong provisional
        rec("north", 0.0, 3, 300, 1, True, 120.0),  # live, exact final
        f_south,
        rec("north", 60.0, 3, 300, 0, False, 125.0),  # wrong provisional
        f_north2,
    ]
    m = score(sim(records, flushed=[f_south, f_north2]), TRUTH)
    assert m["exact_rate_final"] == 1.0
    assert m["n_flushed_windows"] == 2
    # final-live set: north@0 only
    assert m["n_windows_live"] == 1
    assert m["n_exact_final_live"] == 1
    assert m["exact_rate_final_live"] == 1.0
    assert m["mean_abs_count_error_live"] == 0.0
    # first-live set: north@0 and north@60 (south's first record is the flush); both first wrong
    assert m["n_windows_first_live"] == 2
    assert m["exact_rate_first_live"] == 0.0
    # north@0: (2,200) -> (3,300); north@60: (3,300) -> (4,400); south has one record
    assert m["n_value_revisions"] == 2
    assert m["value_revisions_per_window"] == pytest.approx(2 / 3)
    assert m["revisions_per_window"] == pytest.approx(5 / 3 - 1)


def test_live_error_counts_a_wrong_live_final() -> None:
    f_north2 = rec("north", 60.0, 4, 400, 0, True, 500.0)
    records = [
        rec("north", 0.0, 3, 300, 0, True, 90.0),
        rec("south", 0.0, 5, 500, 0, True, 100.0),  # live, wrong by 3
        f_north2,
    ]
    m = score(sim(records, flushed=[f_north2]), TRUTH)
    assert m["n_windows_live"] == 2
    assert m["exact_rate_final_live"] == 0.5
    assert m["mean_abs_count_error_live"] == pytest.approx(3 / 2)
    assert m["mean_abs_count_error"] == pytest.approx(3 / 3)


def test_missing_window_is_live_and_not_exact() -> None:
    m = score(sim([rec("north", 0.0, 3, 300, 0, True, 90.0)]), TRUTH)
    assert m["n_windows_live"] == 3
    assert m["n_windows_first_live"] == 3
    assert m["exact_rate_final_live"] == pytest.approx(1 / 3)
    assert m["exact_rate_first_live"] == pytest.approx(1 / 3)
    assert m["mean_abs_count_error_live"] == pytest.approx((0 + 2 + 4) / 3)
    assert m["n_flushed_windows"] == 0
    assert m["n_value_revisions"] == 0


def test_no_live_windows_gives_nan_live_rates() -> None:
    r = rec("north", 0.0, 3, 300, 0, True, 500.0)
    m = score(sim([r], flushed=[r]), {("north", 0.0): BatchWindow(3, 300)})
    assert m["n_windows_live"] == 0
    assert math.isnan(m["exact_rate_final_live"])
    assert math.isnan(m["exact_rate_first_live"])
    assert math.isnan(m["mean_abs_count_error_live"])
    assert m["exact_rate_final"] == 1.0
    assert m["n_flushed_windows"] == 1


def test_unchanged_final_is_not_a_value_revision() -> None:
    flushed = rec("north", 60.0, 4, 400, 1, True, 500.0)
    records = [
        rec("north", 0.0, 3, 300, 0, False, 62.0),
        rec("north", 0.0, 3, 300, 1, True, 120.0),  # final repeats the provisional values
        rec("north", 60.0, 4, 400, 0, False, 125.0),
        flushed,  # so does the flushed final
    ]
    m = score(sim(records, flushed=[flushed]), {k: TRUTH[k] for k in TRUTH if k[0] == "north"})
    assert m["n_value_revisions"] == 0
    assert m["value_revisions_per_window"] == 0.0
    assert m["revisions_per_window"] == 1.0  # records still count as revisions


def test_revenue_only_change_is_a_value_revision() -> None:
    records = [
        rec("north", 0.0, 3, 299, 0, False, 62.0),
        rec("north", 0.0, 3, 300, 1, True, 120.0),
    ]
    m = score(sim(records), {("north", 0.0): BatchWindow(3, 300)})
    assert m["n_value_revisions"] == 1
    assert m["value_revisions_per_window"] == 1.0


def test_late_dropped_rate() -> None:
    m = score(sim([], dropped=2, events=8), TRUTH)
    assert m["late_dropped_rate"] == 0.25
    assert m["n_dropped"] == 2


def _bare(**over: float) -> dict[str, float]:
    return {k: 0.0 for k in METRIC_KEYS} | over


def test_pool_percentile_is_over_concatenated_latencies() -> None:
    a = RunScore(
        _bare(n_windows=2.0, n_exact_final=2.0),
        first_latencies=[1.0] * 19 + [100.0],
        final_latencies=[1.0],
    )
    b = RunScore(
        _bare(n_windows=2.0, n_exact_final=0.0),
        first_latencies=[2.0] * 20,
        final_latencies=[5.0],
    )
    assert percentile(a.first_latencies, 95) == 1.0
    assert percentile(b.first_latencies, 95) == 2.0
    pooled = pool([a, b])
    # 40 values: 19 ones, one 100, twenty twos; rank ceil(0.95 * 40) = 38 is a 2.0
    assert pooled["first_emit_latency_p95"] == 2.0
    assert pooled["first_emit_latency_max"] == 100.0
    assert pooled["exact_rate_final"] == 0.5
    assert pooled["final_latency_max"] == 5.0


def test_pool_p95_differs_from_mean_of_p95s() -> None:
    a = RunScore(_bare(n_windows=1.0), first_latencies=[10.0] * 10)
    b = RunScore(_bare(n_windows=1.0), first_latencies=[10.0] * 9 + [1000.0])
    # per-run p95s are 10 and 1000 (mean 505); over all 20 values rank 19 is 10
    assert percentile(a.first_latencies, 95) == 10.0
    assert percentile(b.first_latencies, 95) == 1000.0
    assert pool([a, b])["first_emit_latency_p95"] == 10.0


def test_pool_sums_counts_and_averages_records() -> None:
    r1 = score_run(sim([rec("north", 0.0, 3, 300, 0, True, 90.0)], peak=2), TRUTH)
    r2 = score_run(
        sim(
            [
                rec("north", 0.0, 3, 300, 0, False, 61.0),
                rec("north", 0.0, 3, 300, 1, True, 90.0),
                rec("south", 0.0, 2, 200, 0, True, 90.0),
            ],
            dropped=1,
            peak=5,
        ),
        TRUTH,
    )
    p = pool([r1, r2])
    assert p["n_windows"] == 6
    assert p["n_exact_final"] == 3
    assert p["exact_rate_final"] == 0.5
    assert p["records_emitted"] == 2.0
    assert p["peak_open_windows"] == 5
    assert p["revisions_per_window"] == pytest.approx(4 / 3 - 1)
    assert p["n_dropped"] == 1
    with pytest.raises(ValueError, match="at least one"):
        pool([])


def test_pool_live_metrics_are_ratios_of_sums() -> None:
    f_north2 = rec("north", 60.0, 4, 400, 1, True, 500.0)
    f_south = rec("south", 0.0, 2, 200, 0, True, 500.0)
    a = score_run(
        sim(
            [
                rec("north", 0.0, 2, 200, 0, False, 62.0),
                rec("north", 0.0, 3, 300, 1, True, 120.0),
                f_south,
                rec("north", 60.0, 3, 300, 0, False, 125.0),
                f_north2,
            ],
            flushed=[f_south, f_north2],
        ),
        TRUTH,
    )
    b = score_run(sim([rec("north", 0.0, 3, 300, 0, True, 90.0)]), TRUTH)
    assert a.metrics["exact_rate_final_live"] == 1.0
    assert b.metrics["exact_rate_final_live"] == pytest.approx(1 / 3)
    p = pool([a, b])
    # live windows: 1 + 3; exact: 1 + 1; abs error: 0 + 6
    assert p["n_windows_live"] == 4
    assert p["exact_rate_final_live"] == 0.5
    assert p["mean_abs_count_error_live"] == pytest.approx(6 / 4)
    # first-live: 2 + 3 windows, exact 0 + 1
    assert p["exact_rate_first_live"] == pytest.approx(1 / 5)
    # value revisions: 2 + 0 over 3 + 1 emitted windows
    assert p["n_value_revisions"] == 2
    assert p["value_revisions_per_window"] == 0.5
    assert p["n_flushed_windows"] == 2
