import csv
import json
import math
from pathlib import Path

from streampipe.evalrun import (
    FULL_GRID,
    SMALL_GRID,
    Grid,
    RunRow,
    evaluate_claim,
    execute_grid,
    grid_label,
    grid_metadata,
    pooled_table,
    run_grid,
)
from streampipe.metrics import METRIC_KEYS, RunScore


def test_full_grid_matches_the_plan() -> None:
    g = FULL_GRID
    assert g.late_shares == (0.0, 0.05, 0.20)
    assert g.max_delays == (5.0, 30.0, 120.0)
    assert g.lateness == (0.0, 5.0, 15.0, 30.0, 60.0)
    assert g.modes == ("wait", "revise")
    assert g.seeds == (0, 1, 2, 3, 4)
    assert g.duration_s == 3600.0
    assert g.n_runs == 450


def test_small_grid_is_short_and_contains_the_claim_cell() -> None:
    assert SMALL_GRID.duration_s <= 300.0
    assert 0.05 in SMALL_GRID.late_shares
    assert 30.0 in SMALL_GRID.max_delays
    assert 30.0 in SMALL_GRID.lateness
    assert "wait" in SMALL_GRID.modes


def _row(seed: int, exact: int, windows: int, first: list[float], **cell: object) -> RunRow:
    metrics = {k: 0.0 for k in METRIC_KEYS}
    metrics.update(n_windows=float(windows), n_exact_final=float(exact))
    base = {"late_share": 0.05, "max_delay_s": 30.0, "lateness_s": 30.0, "mode": "wait"}
    base.update(cell)
    return RunRow(
        seed=seed,
        duration_s=3600.0,
        # final latencies differ from first-emit ones, so a claim read off the wrong one fails
        score=RunScore(metrics, first_latencies=first, final_latencies=[x + 500.0 for x in first]),
        **base,  # type: ignore[arg-type]
    )


def test_pooled_table_groups_seeds_per_cell() -> None:
    rows = [
        _row(0, 99, 100, [35.0] * 10),
        _row(1, 100, 100, [35.0] * 10),
        _row(0, 50, 100, [1.0], mode="revise"),
    ]
    table = pooled_table(rows)
    assert [(t["mode"], t["n_seeds"]) for t in table] == [("wait", 2), ("revise", 1)]
    assert table[0]["exact_rate_final"] == 199 / 200
    assert table[0]["first_emit_latency_p95"] == 35.0
    assert table[1]["exact_rate_final"] == 0.5


def test_claim_passes_when_both_parts_hold() -> None:
    table = pooled_table([_row(0, 100, 100, [35.0] * 20)])
    assert table[0]["final_latency_p95"] == 535.0
    claim = evaluate_claim(table)
    assert claim["first_emit_latency_p95"]["value"] == 35.0
    assert claim["cell_present"]
    assert claim["passed"] is True
    assert claim["exact_rate_final"]["threshold"] == 0.99
    assert claim["first_emit_latency_p95"]["threshold"] == 90.0


def test_claim_fails_on_accuracy_alone() -> None:
    claim = evaluate_claim(pooled_table([_row(0, 98, 100, [35.0] * 20)]))
    assert claim["exact_rate_final"]["passed"] is False
    assert claim["first_emit_latency_p95"]["passed"] is True
    assert claim["passed"] is False


def test_claim_fails_on_latency_alone() -> None:
    claim = evaluate_claim(pooled_table([_row(0, 100, 100, [95.0] * 20)]))
    assert claim["exact_rate_final"]["passed"] is True
    assert claim["first_emit_latency_p95"]["passed"] is False
    assert claim["passed"] is False


def test_claim_boundaries_are_inclusive() -> None:
    claim = evaluate_claim(pooled_table([_row(0, 99, 100, [90.0] * 20)]))
    assert claim["passed"] is True


def test_claim_missing_cell_fails_without_error() -> None:
    table = pooled_table([_row(0, 100, 100, [35.0], lateness_s=5.0)])
    claim = evaluate_claim(table)
    assert claim["cell_present"] is False
    assert claim["passed"] is False
    assert claim["exact_rate_final"]["value"] is None
    assert not math.isnan(json.loads(json.dumps(claim))["exact_rate_final"]["threshold"])


def test_run_grid_small_writes_deterministic_outputs(tmp_path: Path) -> None:
    a = run_grid(SMALL_GRID, tmp_path / "a" / "sweep.csv")
    b = run_grid(SMALL_GRID, tmp_path / "b" / "sweep.csv")
    with a.open(newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == SMALL_GRID.n_runs
    assert {"seed", "late_share", "max_delay_s", "lateness_s", "mode", "exact_rate_final"} <= set(
        rows[0]
    )
    for name in ("sweep.csv", "sweep_pooled.csv", "sweep_claim.json"):
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes()
    assert b.exists()
    claim = json.loads((tmp_path / "a" / "sweep_claim.json").read_text())
    assert claim["cell_present"] is True
    with (tmp_path / "a" / "sweep_pooled.csv").open(newline="") as f:
        pooled = list(csv.DictReader(f))
    assert len(pooled) == SMALL_GRID.n_runs // len(SMALL_GRID.seeds)


def test_wait_and_revise_agree_on_final_accuracy_and_zero_lateness() -> None:
    runs = execute_grid(SMALL_GRID)
    by = {(r.seed, r.late_share, r.lateness_s, r.mode): r.score.metrics for r in runs}
    for (seed, share, lateness, mode), m in by.items():
        if mode != "wait":
            continue
        other = by[(seed, share, lateness, "revise")]
        for key in ("exact_rate_final", "late_dropped_rate", "peak_open_windows"):
            assert m[key] == other[key]
        if lateness == 0.0:
            assert m == other


def test_workers_do_not_change_rows() -> None:
    assert [r.score.metrics for r in execute_grid(SMALL_GRID, workers=2)] == [
        r.score.metrics for r in execute_grid(SMALL_GRID)
    ]


def test_claim_registered_grid_only_for_the_full_grid() -> None:
    table = pooled_table([_row(0, 100, 100, [35.0] * 20)])
    assert evaluate_claim(table)["registered_grid"] is False
    assert evaluate_claim(table, SMALL_GRID)["registered_grid"] is False
    assert evaluate_claim(table, FULL_GRID)["registered_grid"] is True
    one_more_seed = Grid(
        FULL_GRID.late_shares,
        FULL_GRID.max_delays,
        FULL_GRID.lateness,
        FULL_GRID.modes,
        (*FULL_GRID.seeds, 5),
    )
    assert evaluate_claim(table, one_more_seed)["registered_grid"] is False


def test_grid_label_and_metadata() -> None:
    assert grid_label(FULL_GRID) == "full"
    assert grid_label(SMALL_GRID) == "small"
    custom = Grid((0.0,), (5.0,), (0.0,), ("wait",), (0,), duration_s=120.0)
    assert grid_label(custom) == "custom"
    assert grid_metadata(custom) == {
        "n_runs": 1,
        "n_seeds": 1,
        "duration_s": 120.0,
        "late_shares": [0.0],
        "max_delays": [5.0],
        "lateness": [0.0],
        "modes": ["wait"],
    }
    full = grid_metadata(FULL_GRID)
    assert (full["n_runs"], full["n_seeds"], full["duration_s"]) == (450, 5, 3600.0)


def test_run_grid_records_grid_in_the_claim_json(tmp_path: Path) -> None:
    run_grid(SMALL_GRID, tmp_path / "s.csv")
    claim = json.loads((tmp_path / "s_claim.json").read_text())
    assert claim["grid"] == "small"
    assert claim["registered_grid"] is False
    assert claim["grid_config"] == grid_metadata(SMALL_GRID)
    custom = Grid((0.05,), (30.0,), (30.0,), ("wait",), (0,), duration_s=120.0)
    run_grid(custom, tmp_path / "c.csv")
    claim = json.loads((tmp_path / "c_claim.json").read_text())
    assert (claim["grid"], claim["registered_grid"]) == ("custom", False)
    assert claim["grid_config"]["n_runs"] == 1


def test_sweep_outputs_carry_the_live_metric_columns(tmp_path: Path) -> None:
    run_grid(SMALL_GRID, tmp_path / "s.csv")
    for name in ("s.csv", "s_pooled.csv"):
        with (tmp_path / name).open(newline="") as f:
            header = next(csv.reader(f))
        for key in ("exact_rate_final_live", "value_revisions_per_window", "n_flushed_windows"):
            assert key in header
