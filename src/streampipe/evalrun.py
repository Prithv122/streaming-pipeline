"""The parameter sweep: run the grid, write the per-run CSV, pool over seeds, check the claim."""

from __future__ import annotations

import csv
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


def evaluate_claim(pooled: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Mechanically check the pre-registered claim against the pooled table."""
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


def _write_csv(path: Path, columns: Sequence[str], records: Sequence[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(columns), lineterminator="\n")
        w.writeheader()
        w.writerows(records)


def run_grid(grid: Grid, out_csv: Path | str, workers: int = 1) -> Path:
    """Run the grid; write `<out>.csv` (one row per run), `<out>_pooled.csv`, `<out>_claim.json`."""
    out = Path(out_csv)
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
    claim = evaluate_claim(pooled)
    claim_path = out.with_name(f"{out.stem}_claim.json")
    claim_path.write_text(json.dumps(claim, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out
