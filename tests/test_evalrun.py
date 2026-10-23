import copy
import csv
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import pytest

from streampipe import evalrun
from streampipe.evalrun import (
    FULL_GRID,
    SMALL_GRID,
    Grid,
    RunRow,
    evaluate_claim,
    evaluate_predictions,
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
    assert not (tmp_path / "a" / "sweep_checks.csv").exists()
    assert b.exists()
    claim = json.loads((tmp_path / "a" / "sweep_claim.json").read_text())
    assert claim["c0_construction_check"]["cell_present"] is True
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
    assert (claim["registered_grid"], claim["smoke"]) == (False, True)
    assert claim["grid_config"] == grid_metadata(SMALL_GRID)
    custom = Grid((0.05,), (30.0,), (30.0,), ("wait",), (0,), duration_s=120.0)
    run_grid(custom, tmp_path / "c.csv")
    claim = json.loads((tmp_path / "c_claim.json").read_text())
    assert (claim["grid"], claim["registered_grid"]) == ("custom", False)
    assert claim["grid_config"]["n_runs"] == 1
    assert claim["primary"] is None and claim["predictions_sha256"] is None


def test_sweep_outputs_carry_the_live_metric_columns(tmp_path: Path) -> None:
    run_grid(SMALL_GRID, tmp_path / "s.csv")
    for name in ("s.csv", "s_pooled.csv"):
        with (tmp_path / name).open(newline="") as f:
            header = next(csv.reader(f))
        for key in ("exact_rate_final_live", "value_revisions_per_window", "n_flushed_windows"):
            assert key in header


# --- analytic predictions (the primary claim) ---------------------------------------------------

CELL = (0.05, 30.0)


def _pooled(mode: str, lateness: float, **over: Any) -> dict[str, Any]:
    """A hand-built pooled row for the (0.05, 30 s) cell."""
    row: dict[str, Any] = {
        "mode": mode,
        "late_share": CELL[0],
        "max_delay_s": CELL[1],
        "lateness_s": lateness,
        "n_events": 1000.0,
        "n_dropped": 100.0 if lateness == 0.0 else 0.0,
        "n_exact_first_live": 50.0,
        "n_windows_first_live": 100.0,
        "first_emit_latency_p95": 1.0 if mode == "revise" or lateness == 0.0 else 31.0,
    }
    row.update(over)
    return row


def _table() -> list[dict[str, Any]]:
    return [_pooled(m, x) for m in ("wait", "revise") for x in (0.0, 30.0)]


def _predictions() -> dict[str, Any]:
    """Predictions that `_table()` meets exactly: 2 L values give 10 checks, 2 per family."""
    base = {"late_share": CELL[0], "max_delay_s": CELL[1], "step": 0.1}
    p3 = {"se95": 0.01, "tolerance": 0.25}
    return {
        "model": "ANALYTIC.md",
        "p1_drop_rate": [
            {**base, "lateness_s": 0.0, "rho": 0.1, "rho_coarse": 0.1, "exact_zero": False},
            {**base, "lateness_s": 30.0, "rho": 0.0, "rho_coarse": 0.0, "exact_zero": True},
        ],
        "p2_first_exact": [{**base, "pi": 0.5, "sd": 0.01, "tolerance": 0.25, "pi_coarse": 0.5}],
        "p3_first_latency": [
            {**base, **p3, "mode": "wait", "lateness_s": 0.0, "p50": 0.5, "p95": 1.0},
            {**base, **p3, "mode": "wait", "lateness_s": 30.0, "p50": 30.5, "p95": 31.0},
            {**base, **p3, "mode": "revise", "lateness_s": None, "p50": 0.5, "p95": 1.0},
        ],
    }


def _by_family(result: dict[str, Any]) -> dict[str, tuple[int, int]]:
    return {f: (v["n_checks"], v["n_failed"]) for f, v in result["families"].items()}


def test_predictions_all_pass() -> None:
    result = evaluate_predictions(_table(), _predictions())
    assert result["primary_passed"] is True
    assert (result["n_checks"], result["n_passed"], result["n_failed"]) == (10, 10, 0)
    assert _by_family(result) == {
        "p1": (2, 0),
        "p1_mode_invariant": (2, 0),
        "p2": (2, 0),
        "p3_wait": (2, 0),
        "p3_revise": (2, 0),
    }
    first = result["checks"][0]
    assert first["family"] == "p1" and first["mode"] == "wait"
    assert (first["observed"], first["predicted"], first["diff"]) == (100.0, 100.0, 0.0)
    assert first["tolerance"] == 4 * math.sqrt(200.0) + 2
    json.dumps(result, allow_nan=False)


def _mutate(table: list[dict[str, Any]], mode: str, lateness: float, **over: Any) -> None:
    for row in table:
        if row["mode"] == mode and row["lateness_s"] == lateness:
            row.update(over)


def _wrong_drops(table: list[dict[str, Any]]) -> None:
    # in both modes, so only the P1 estimate is wrong, not the invariant
    for mode in ("wait", "revise"):
        _mutate(table, mode, 0.0, n_dropped=300.0)


@pytest.mark.parametrize(
    ("mutation", "failed"),
    [
        (_wrong_drops, {"p1": 1}),
        (lambda t: _mutate(t, "revise", 30.0, n_exact_first_live=10.0), {"p2": 1}),
        (lambda t: _mutate(t, "wait", 30.0, first_emit_latency_p95=40.0), {"p3_wait": 1}),
        (lambda t: _mutate(t, "revise", 0.0, first_emit_latency_p95=9.0), {"p3_revise": 1}),
        (lambda t: _mutate(t, "revise", 30.0, n_dropped=1.0), {"p1_mode_invariant": 1}),
    ],
)
def test_predictions_fail_per_family(mutation: Any, failed: dict[str, int]) -> None:
    table = _table()
    mutation(table)
    result = evaluate_predictions(table, _predictions())
    assert result["primary_passed"] is False
    assert {f: n for f, (_, n) in _by_family(result).items() if n} == failed
    assert result["n_failed"] == sum(failed.values())


def test_p1_boundary_is_inclusive_and_uses_wait_rows() -> None:
    # rho = 0 at L = 30: tolerance is exactly 2 drops
    for dropped, ok in ((2.0, True), (3.0, False)):
        table = _table()
        for mode in ("wait", "revise"):
            _mutate(table, mode, 30.0, n_dropped=dropped)
        result = evaluate_predictions(table, _predictions())
        assert result["families"]["p1"]["n_failed"] == (0 if ok else 1)
        assert result["families"]["p1_mode_invariant"]["n_failed"] == 0
    # a drop count only in the revise row is the invariant's business, not P1's
    table = _table()
    _mutate(table, "revise", 30.0, n_dropped=50.0)
    assert evaluate_predictions(table, _predictions())["families"]["p1"]["n_failed"] == 0


def test_p2_and_p3_boundaries_are_inclusive() -> None:
    # pi = 0.5, tolerance 0.25: exactly representable, so 0.75 sits on the boundary
    for exact, ok in ((75.0, True), (76.0, False)):
        table = _table()
        _mutate(table, "revise", 30.0, n_exact_first_live=exact)
        result = evaluate_predictions(table, _predictions())
        assert result["families"]["p2"]["n_failed"] == (0 if ok else 1)
    # p95 = 31, tolerance 0.25
    for p95, ok in ((31.25, True), (31.5, False)):
        table = _table()
        _mutate(table, "wait", 30.0, first_emit_latency_p95=p95)
        result = evaluate_predictions(table, _predictions())
        assert result["families"]["p3_wait"]["n_failed"] == (0 if ok else 1)


def test_p2_and_p3_revise_are_checked_at_every_lateness() -> None:
    table = _table()
    _mutate(table, "revise", 30.0, n_exact_first_live=10.0, first_emit_latency_p95=9.0)
    result = evaluate_predictions(table, _predictions())
    bad = {(c["family"], c["lateness_s"]) for c in result["checks"] if not c["passed"]}
    assert bad == {("p2", 30.0), ("p3_revise", 30.0)}


def test_missing_cells_and_predictions_fail_with_a_note() -> None:
    table = [r for r in _table() if not (r["mode"] == "revise" and r["lateness_s"] == 30.0)]
    result = evaluate_predictions(table, _predictions())
    failed = {(c["family"], c["lateness_s"]): c for c in result["checks"] if not c["passed"]}
    assert set(failed) == {("p1_mode_invariant", 30.0), ("p2", 30.0), ("p3_revise", 30.0)}
    assert failed[("p2", 30.0)]["note"] == "missing observed cell"
    assert failed[("p2", 30.0)]["observed"] is None
    assert result["primary_passed"] is False
    # a pooled wait cell that has a prediction but no observation also fails
    table = [r for r in _table() if not (r["mode"] == "wait" and r["lateness_s"] == 30.0)]
    result = evaluate_predictions(table, _predictions())
    assert {c["family"] for c in result["checks"] if not c["passed"]} == {
        "p1",
        "p1_mode_invariant",
        "p3_wait",
    }
    # an observed cell without a prediction fails too
    preds = _predictions()
    preds["p2_first_exact"] = []
    result = evaluate_predictions(_table(), preds)
    assert result["families"]["p2"] == {"n_checks": 2, "n_passed": 0, "n_failed": 2}
    assert {c["note"] for c in result["checks"] if c["family"] == "p2"} == {"missing prediction"}
    # an absent family is a missing prediction, not a skipped check
    assert evaluate_predictions(_table(), {})["primary_passed"] is False


def test_nan_and_zero_denominators_fail() -> None:
    nan = math.nan
    cases: list[tuple[str, Any]] = [
        ("p3_wait", lambda t: _mutate(t, "wait", 30.0, first_emit_latency_p95=nan)),
        ("p3_revise", lambda t: _mutate(t, "revise", 0.0, first_emit_latency_p95=nan)),
        ("p2", lambda t: _mutate(t, "revise", 0.0, n_windows_first_live=0.0)),
        ("p1", lambda t: _mutate(t, "wait", 0.0, n_events=nan)),
    ]
    for family, mutation in cases:
        table = _table()
        mutation(table)
        result = evaluate_predictions(table, _predictions())
        assert result["families"][family]["n_failed"] == 1, family
        assert result["primary_passed"] is False
        json.dumps(result, allow_nan=False)
    # a NaN tolerance or prediction cannot be met either
    preds = _predictions()
    preds["p2_first_exact"][0]["tolerance"] = nan
    assert evaluate_predictions(_table(), preds)["families"]["p2"]["n_failed"] == 2
    preds = _predictions()
    preds["p1_drop_rate"][0]["rho"] = nan
    assert evaluate_predictions(_table(), preds)["families"]["p1"]["n_failed"] == 1


def test_grid_axes_define_the_expected_cells() -> None:
    grid = Grid((CELL[0],), (CELL[1],), (0.0, 30.0, 60.0), ("wait", "revise"), (0,))
    result = evaluate_predictions(_table(), _predictions(), grid)
    # L = 60 is in the grid but in neither the pooled table nor the predictions
    assert _by_family(result) == {
        "p1": (3, 1),
        "p1_mode_invariant": (3, 1),
        "p2": (3, 1),
        "p3_wait": (3, 1),
        "p3_revise": (3, 1),
    }
    assert result["primary_passed"] is False
    # a grid cell with no prediction at all (0 % share) fails as well
    grid = Grid((0.0, CELL[0]), (CELL[1],), (0.0, 30.0), ("wait", "revise"), (0,))
    assert evaluate_predictions(_table(), _predictions(), grid)["n_failed"] == 10


def test_duplicate_prediction_rows_raise_and_empty_input_does_not_pass() -> None:
    preds = _predictions()
    preds["p1_drop_rate"].append(copy.deepcopy(preds["p1_drop_rate"][0]))
    with pytest.raises(ValueError, match="duplicate"):
        evaluate_predictions(_table(), preds)
    result = evaluate_predictions([], {})
    assert (result["n_checks"], result["primary_passed"]) == (0, False)


def _write_preds(path: Path, preds: dict[str, Any] | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(preds or _predictions(), sort_keys=True), encoding="utf-8")
    return path


def test_run_grid_records_primary_hash_and_checks_csv(tmp_path: Path) -> None:
    preds = _write_preds(tmp_path / "predictions.json")
    run_grid(SMALL_GRID, tmp_path / "a" / "s.csv", predictions_path=preds)
    claim = json.loads((tmp_path / "a" / "s_claim.json").read_text())
    assert claim["predictions_sha256"] == hashlib.sha256(preds.read_bytes()).hexdigest()
    assert (claim["grid"], claim["registered_grid"], claim["smoke"]) == ("small", False, True)
    assert claim["c0_construction_check"]["cell_present"] is True
    primary = claim["primary"]
    # SMALL_GRID: 2 shares x 1 delay x 2 L = 4 cells per family; the 0 % share has no predictions
    assert primary["n_checks"] == 20
    assert primary["primary_passed"] is False
    assert primary["n_failed"] >= 5
    with (tmp_path / "a" / "s_checks.csv").open(newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == primary["n_checks"]
    assert list(rows[0]) == list(evalrun.CHECK_COLUMNS)
    assert {r["family"] for r in rows} == set(evalrun.FAMILIES)
    assert {r["passed"] for r in rows} <= {"True", "False"}


def test_run_grid_with_predictions_is_deterministic(tmp_path: Path) -> None:
    preds = _write_preds(tmp_path / "predictions.json")
    for name in ("a", "b"):
        run_grid(SMALL_GRID, tmp_path / name / "s.csv", predictions_path=preds)
    for name in ("s.csv", "s_pooled.csv", "s_claim.json", "s_checks.csv"):
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes()


def test_run_grid_hash_follows_the_predictions_bytes(tmp_path: Path) -> None:
    other = _predictions()
    other["model"] = "other"
    grid = Grid((0.05,), (30.0,), (30.0,), ("wait",), (0,), duration_s=60.0)
    shas = []
    for name, preds in (("p1", _predictions()), ("p2", other)):
        path = _write_preds(tmp_path / f"{name}.json", preds)
        out = tmp_path / name / "s.csv"
        run_grid(grid, out, predictions_path=path)
        shas.append(json.loads(out.with_name("s_claim.json").read_text())["predictions_sha256"])
    assert shas[0] != shas[1]


def test_registered_grid_requires_predictions_before_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*_a: Any, **_k: Any) -> None:
        raise AssertionError("the grid must not run without predictions")

    monkeypatch.setattr(evalrun, "execute_grid", boom)
    out = tmp_path / "res" / "sweep.csv"
    with pytest.raises(FileNotFoundError, match=r"commit results/predictions[.]json first"):
        run_grid(FULL_GRID, out)
    with pytest.raises(FileNotFoundError, match=r"commit results/predictions[.]json first"):
        run_grid(FULL_GRID, out, predictions_path=tmp_path / "missing.json")
    # a smoke grid that was given a path to nothing fails the same way
    with pytest.raises(FileNotFoundError, match="not found"):
        run_grid(SMALL_GRID, out, predictions_path=tmp_path / "missing.json")
    assert not (tmp_path / "res").exists()


def test_registered_grid_is_labelled_and_evaluated_against_its_axes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tiny = Grid((CELL[0],), (CELL[1],), (30.0,), ("wait", "revise"), (0,), duration_s=120.0)
    monkeypatch.setattr(evalrun, "FULL_GRID", tiny)
    preds = _write_preds(tmp_path / "predictions.json")
    run_grid(tiny, tmp_path / "s.csv", predictions_path=preds)
    claim = json.loads((tmp_path / "s_claim.json").read_text())
    assert (claim["grid"], claim["registered_grid"], claim["smoke"]) == ("full", True, False)
    assert claim["primary"]["n_checks"] == 5
    assert claim["primary"]["families"]["p1_mode_invariant"]["n_checks"] == 1
