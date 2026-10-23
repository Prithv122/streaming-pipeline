"""The parameter sweep: run the grid, write the per-run CSV, pool over seeds, check the claim."""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from streampipe.batch import batch_windows
from streampipe.events import GenConfig, generate
from streampipe.metrics import METRIC_KEYS, RunScore, pool, score_run
from streampipe.sim import simulate


@dataclass(frozen=True)
class Grid:
    """The sweep axes. Every other generator parameter stays at its `GenConfig` default."""

    late_shares: tuple[float, ...]
    max_delays: tuple[float, ...]
    lateness: tuple[float, ...]
    modes: tuple[str, ...]
    seeds: tuple[int, ...]
    duration_s: float = 3600.0

    @property
    def n_runs(self) -> int:
        return (
            len(self.late_shares)
            * len(self.max_delays)
            * len(self.lateness)
            * len(self.modes)
            * len(self.seeds)
        )


FULL_GRID = Grid(
    late_shares=(0.0, 0.05, 0.20),
    max_delays=(5.0, 30.0, 120.0),
    lateness=(0.0, 5.0, 15.0, 30.0, 60.0),
    modes=("wait", "revise"),
    seeds=(0, 1, 2, 3, 4),
)

SMALL_GRID = Grid(
    late_shares=(0.0, 0.05),
    max_delays=(30.0,),
    lateness=(0.0, 30.0),
    modes=("wait", "revise"),
    seeds=(0, 1),
    duration_s=300.0,
)


def grid_label(grid: Grid) -> str:
    """ "full" for the registered grid, "small" for the smoke grid, "custom" for anything else."""
    if grid == FULL_GRID:
        return "full"
    if grid == SMALL_GRID:
        return "small"
    return "custom"


def grid_metadata(grid: Grid) -> dict[str, Any]:
    """The axes a result was produced from, for the claim JSON."""
    return {
        "n_runs": grid.n_runs,
        "n_seeds": len(grid.seeds),
        "duration_s": grid.duration_s,
        "late_shares": list(grid.late_shares),
        "max_delays": list(grid.max_delays),
        "lateness": list(grid.lateness),
        "modes": list(grid.modes),
    }


CLAIM = {
    "mode": "wait",
    "lateness_s": 30.0,
    "late_share": 0.05,
    "max_delay_s": 30.0,
    "exact_rate_final_min": 0.99,
    "first_emit_latency_p95_max": 90.0,
}

CONFIG_COLUMNS = ("seed", "late_share", "max_delay_s", "duration_s", "lateness_s", "mode")
CELL_COLUMNS = ("late_share", "max_delay_s", "lateness_s", "mode")


@dataclass(frozen=True)
class RunRow:
    """One run: its configuration and its score (including per-window latencies)."""

    seed: int
    late_share: float
    max_delay_s: float
    duration_s: float
    lateness_s: float
    mode: str
    score: RunScore


def _run_group(args: tuple[int, float, float, Grid]) -> list[RunRow]:
    """One generated log and its batch truth, replayed under every lateness x mode."""
    seed, share, delay, grid = args
    cfg = GenConfig(seed=seed, duration_s=grid.duration_s, late_share=share, max_delay_s=delay)
    arrivals = generate(cfg)
    truth = batch_windows(a.event for a in arrivals)
    rows = []
    for lateness in grid.lateness:
        for mode in grid.modes:
            result = simulate(arrivals, mode, lateness)
            rows.append(
                RunRow(
                    seed, share, delay, grid.duration_s, lateness, mode, score_run(result, truth)
                )
            )
    return rows


def execute_grid(grid: Grid, workers: int = 1) -> list[RunRow]:
    """Run every cell. Row order is fixed by the axes, whatever the number of workers."""
    groups = [
        (seed, share, delay, grid)
        for seed in grid.seeds
        for share in grid.late_shares
        for delay in grid.max_delays
    ]
    if workers > 1:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            chunks = list(ex.map(_run_group, groups))
    else:
        chunks = [_run_group(g) for g in groups]
    return [row for chunk in chunks for row in chunk]


def pooled_table(rows: Sequence[RunRow]) -> list[dict[str, Any]]:
    """Pool the seeds of each (late share, max delay, L, mode) cell, in first-seen order."""
    cells: dict[tuple[Any, ...], list[RunRow]] = {}
    for r in rows:
        cells.setdefault((r.late_share, r.max_delay_s, r.lateness_s, r.mode), []).append(r)
    table = []
    for (share, delay, lateness, mode), group in cells.items():
        entry: dict[str, Any] = {
            "late_share": share,
            "max_delay_s": delay,
            "lateness_s": lateness,
            "mode": mode,
            "n_seeds": len(group),
        }
        entry.update(pool([g.score for g in group]))
        table.append(entry)
    return table


def evaluate_claim(pooled: Sequence[dict[str, Any]], grid: Grid | None = None) -> dict[str, Any]:
    """Mechanically check the pre-registered claim against the pooled table.

    `registered_grid` is true only when `grid` is `FULL_GRID`, the grid the claim was registered
    on. For any other grid the pass/fail values are a smoke check, not the registered result.
    """
    cell = next(
        (
            p
            for p in pooled
            if p["mode"] == CLAIM["mode"]
            and p["lateness_s"] == CLAIM["lateness_s"]
            and p["late_share"] == CLAIM["late_share"]
            and p["max_delay_s"] == CLAIM["max_delay_s"]
        ),
        None,
    )
    out: dict[str, Any] = {
        "cell": {k: CLAIM[k] for k in ("mode", "lateness_s", "late_share", "max_delay_s")},
        "cell_present": cell is not None,
        "registered_grid": grid is not None and grid == FULL_GRID,
    }
    exact = cell["exact_rate_final"] if cell else math.nan
    p95 = cell["first_emit_latency_p95"] if cell else math.nan
    out["exact_rate_final"] = {
        "value": _jsonable(exact),
        "threshold": CLAIM["exact_rate_final_min"],
        "comparison": ">=",
        "passed": bool(exact >= CLAIM["exact_rate_final_min"]),
    }
    out["first_emit_latency_p95"] = {
        "value": _jsonable(p95),
        "threshold": CLAIM["first_emit_latency_p95_max"],
        "comparison": "<=",
        "passed": bool(p95 <= CLAIM["first_emit_latency_p95_max"]),
    }
    out["passed"] = out["exact_rate_final"]["passed"] and out["first_emit_latency_p95"]["passed"]
    return out


def _jsonable(x: float) -> float | None:
    return None if isinstance(x, float) and math.isnan(x) else x


def _num(x: Any) -> float:
    """`x` as a float, or nan when it is missing or not a number."""
    try:
        return float(x)
    except (TypeError, ValueError):
        return math.nan


def _finite_or_none(x: float) -> float | None:
    return x if math.isfinite(x) else None


CHECK_COLUMNS = (
    "family",
    "late_share",
    "max_delay_s",
    "lateness_s",
    "mode",
    "observed",
    "predicted",
    "tolerance",
    "diff",
    "passed",
    "note",
)
FAMILIES = ("p1", "p1_mode_invariant", "p2", "p3_wait", "p3_revise")

Cell = tuple[float, float, float]


def _check(
    family: str,
    cell: Cell,
    mode: str,
    observed: float,
    predicted: float,
    tolerance: float,
    note: str = "",
) -> dict[str, Any]:
    """One check row. A missing or non-finite value fails; `diff <= tolerance` is inclusive."""
    finite = all(math.isfinite(v) for v in (observed, predicted, tolerance))
    diff = abs(observed - predicted) if finite else math.nan
    return {
        "family": family,
        "late_share": cell[0],
        "max_delay_s": cell[1],
        "lateness_s": cell[2],
        "mode": mode,
        "observed": _finite_or_none(observed),
        "predicted": _finite_or_none(predicted),
        "tolerance": _finite_or_none(tolerance),
        "diff": _finite_or_none(diff),
        "passed": bool(finite and diff <= tolerance),
        "note": note or ("" if finite else "non-finite value"),
    }


def _failed(family: str, cell: Cell, mode: str, note: str, tolerance: float = math.nan) -> dict:
    """A check that could not be evaluated: it fails."""
    return _check(family, cell, mode, math.nan, math.nan, tolerance, note)


def _missing(row: dict[str, Any] | None, pred: dict[str, Any] | None) -> str:
    if row is None and pred is None:
        return "missing observed cell and prediction"
    return "missing observed cell" if row is None else "missing prediction"


def _index_predictions(
    rows: Sequence[dict[str, Any]], name: str, with_lateness: bool
) -> dict[tuple[float, ...], dict[str, Any]]:
    """Predictions rows keyed by (late share, max delay[, L]). A repeated key raises."""
    out: dict[tuple[float, ...], dict[str, Any]] = {}
    for r in rows:
        key = (float(r["late_share"]), float(r["max_delay_s"]))
        if with_lateness:
            key = (*key, float(r["lateness_s"]))
        if key in out:
            raise ValueError(f"predictions: duplicate {name} row for {key}")
        out[key] = r
    return out


def evaluate_predictions(
    pooled: Sequence[dict[str, Any]],
    predictions: dict[str, Any],
    grid: Grid | None = None,
) -> dict[str, Any]:
    """Check the pooled sweep against the analytic predictions (ANALYTIC.md, section 6).

    Families: `p1` (late-drop counts, wait rows), `p1_mode_invariant` (revise drops equal wait
    drops), `p2` (revise first-answer exactness at every L), `p3_wait` and `p3_revise` (first-emit
    latency p95, revise at every L). A missing observed cell, a missing prediction or a non-finite
    value is a failed check, never skipped. With `grid` the expected cells are its axes; without
    it, the cells present in the predictions or the pooled table. `primary_passed` is true iff
    there is at least one check and every check passes.
    """
    obs = {
        (p["mode"], float(p["late_share"]), float(p["max_delay_s"]), float(p["lateness_s"])): p
        for p in pooled
    }
    p1 = _index_predictions(predictions.get("p1_drop_rate", []), "p1_drop_rate", True)
    p2 = _index_predictions(predictions.get("p2_first_exact", []), "p2_first_exact", False)
    p3 = predictions.get("p3_first_latency", [])
    p3_wait = _index_predictions([r for r in p3 if r["mode"] == "wait"], "p3 wait", True)
    p3_revise = _index_predictions([r for r in p3 if r["mode"] == "revise"], "p3 revise", False)

    p1_cells: list[Cell]
    wait_cells: list[Cell]
    if grid is not None:
        pairs = [(float(s), float(d)) for s in grid.late_shares for d in grid.max_delays]
        lateness_axis = [float(x) for x in grid.lateness]
        p1_cells = wait_cells = [(s, d, x) for s, d in pairs for x in lateness_axis]
        pairs_p2 = pairs_p3 = pairs
    else:
        lateness_axis = sorted({k[3] for k in obs})
        wait_obs = {k[1:] for k in obs if k[0] == "wait"}
        revise_pairs = {k[1:3] for k in obs if k[0] == "revise"}
        p1_cells = sorted(set(p1) | wait_obs)  # type: ignore[arg-type]
        wait_cells = sorted(set(p3_wait) | wait_obs)  # type: ignore[arg-type]
        pairs_p2 = sorted(set(p2) | revise_pairs)  # type: ignore[arg-type]
        pairs_p3 = sorted(set(p3_revise) | revise_pairs)  # type: ignore[arg-type]

    checks: list[dict[str, Any]] = []

    for cell in p1_cells:
        row, pred = obs.get(("wait", *cell)), p1.get(cell)
        if row is None or pred is None:
            checks.append(_failed("p1", cell, "wait", _missing(row, pred)))
            continue
        expected = _num(pred.get("rho")) * _num(row.get("n_events"))
        tol = 4.0 * math.sqrt(2.0 * expected) + 2.0 if expected >= 0 else math.nan
        checks.append(_check("p1", cell, "wait", _num(row.get("n_dropped")), expected, tol))

    for cell in p1_cells:
        wait, revise = obs.get(("wait", *cell)), obs.get(("revise", *cell))
        if wait is None or revise is None:
            note = "missing wait cell" if wait is None else "missing revise cell"
            checks.append(_failed("p1_mode_invariant", cell, "revise", note, 0.0))
            continue
        d_wait, d_revise = _num(wait.get("n_dropped")), _num(revise.get("n_dropped"))
        checks.append(_check("p1_mode_invariant", cell, "revise", d_revise, d_wait, 0.0))

    for share, delay in pairs_p2:
        for lateness in lateness_axis:
            cell = (share, delay, lateness)
            row, pred = obs.get(("revise", *cell)), p2.get((share, delay))
            if row is None or pred is None:
                checks.append(_failed("p2", cell, "revise", _missing(row, pred)))
                continue
            den = _num(row.get("n_windows_first_live"))
            rate = _num(row.get("n_exact_first_live")) / den if den > 0 else math.nan
            tol = _num(pred.get("tolerance"))
            checks.append(_check("p2", cell, "revise", rate, _num(pred.get("pi")), tol))

    for cell in wait_cells:
        row, pred = obs.get(("wait", *cell)), p3_wait.get(cell)
        if row is None or pred is None:
            checks.append(_failed("p3_wait", cell, "wait", _missing(row, pred)))
            continue
        observed = _num(row.get("first_emit_latency_p95"))
        tol = _num(pred.get("tolerance"))
        checks.append(_check("p3_wait", cell, "wait", observed, _num(pred.get("p95")), tol))

    for share, delay in pairs_p3:
        for lateness in lateness_axis:
            cell = (share, delay, lateness)
            row, pred = obs.get(("revise", *cell)), p3_revise.get((share, delay))
            if row is None or pred is None:
                checks.append(_failed("p3_revise", cell, "revise", _missing(row, pred)))
                continue
            observed = _num(row.get("first_emit_latency_p95"))
            tol = _num(pred.get("tolerance"))
            checks.append(_check("p3_revise", cell, "revise", observed, _num(pred.get("p95")), tol))

    families = {}
    for fam in FAMILIES:
        n = sum(c["family"] == fam for c in checks)
        ok = sum(c["family"] == fam and c["passed"] for c in checks)
        families[fam] = {"n_checks": n, "n_passed": ok, "n_failed": n - ok}
    n_passed = sum(c["passed"] for c in checks)
    return {
        "checks": checks,
        "families": families,
        "n_checks": len(checks),
        "n_passed": n_passed,
        "n_failed": len(checks) - n_passed,
        "primary_passed": bool(checks) and n_passed == len(checks),
    }


def load_predictions(path: Path | str) -> tuple[dict[str, Any], str]:
    """Read the predictions file once; return its parsed content and the sha256 of its bytes."""
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(
            f"predictions file not found: {p}; commit results/predictions.json first"
        )
    data = p.read_bytes()
    return json.loads(data), hashlib.sha256(data).hexdigest()


def _write_csv(path: Path, columns: Sequence[str], records: Sequence[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(columns), lineterminator="\n")
        w.writeheader()
        w.writerows(records)


def run_grid(
    grid: Grid,
    out_csv: Path | str,
    workers: int = 1,
    predictions_path: Path | str | None = None,
) -> Path:
    """Run the grid; write `<out>.csv` (one row per run), `<out>_pooled.csv`, `<out>_claim.json`.

    The registered grid requires a predictions file and fails before running anything without
    one. Any other grid is a smoke run: predictions are evaluated only when a path is given. When
    they are, `<out>_checks.csv` holds one row per check.
    """
    out = Path(out_csv)
    registered = grid == FULL_GRID
    if predictions_path is None and registered:
        raise FileNotFoundError(
            "the registered grid needs a predictions file: commit results/predictions.json first"
        )
    predictions, sha = (
        load_predictions(predictions_path) if predictions_path is not None else (None, None)
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    rows = execute_grid(grid, workers)

    run_records = [
        {
            "seed": r.seed,
            "late_share": r.late_share,
            "max_delay_s": r.max_delay_s,
            "duration_s": r.duration_s,
            "lateness_s": r.lateness_s,
            "mode": r.mode,
            **r.score.metrics,
        }
        for r in rows
    ]
    _write_csv(out, (*CONFIG_COLUMNS, *METRIC_KEYS), run_records)

    pooled = pooled_table(rows)
    _write_csv(
        out.with_name(f"{out.stem}_pooled.csv"),
        (*CELL_COLUMNS, "n_seeds", *METRIC_KEYS),
        pooled,
    )
    primary = evaluate_predictions(pooled, predictions, grid) if predictions is not None else None
    if primary is not None:
        _write_csv(out.with_name(f"{out.stem}_checks.csv"), CHECK_COLUMNS, primary["checks"])
    claim: dict[str, Any] = {
        "c0_construction_check": evaluate_claim(pooled, grid),
        "grid": grid_label(grid),
        "grid_config": grid_metadata(grid),
        "predictions_sha256": sha,
        "primary": primary,
        "registered_grid": registered,
        "smoke": not registered,
    }
    claim_path = out.with_name(f"{out.stem}_claim.json")
    claim_path.write_text(json.dumps(claim, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out
