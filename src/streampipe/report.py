"""The sweep report: a mechanical rendering of the committed result files.

Every number is read from `results/` (`sweep.csv`, `sweep_pooled.csv`, `sweep_claim.json`,
`sweep_checks.csv`, `predictions.json`, `predictions.sha256`). Nothing is computed beyond ratios
of two printed values, there is no interpretation text, and the output has no timestamp, so
rerunning on the same files gives the same bytes.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

from streampipe.evalrun import FAMILIES, pin_path

REPORT_OUT = Path("results/REPORT.md")
STRESS_CELL = (0.2, 120.0)

SWEEP_NAME = "sweep.csv"
POOLED_NAME = "sweep_pooled.csv"
CLAIM_NAME = "sweep_claim.json"
CHECKS_NAME = "sweep_checks.csv"
PREDICTIONS_NAME = "predictions.json"

CHECK_NUMBERS = ("late_share", "max_delay_s", "lateness_s")
CHECK_VALUES = ("observed", "predicted", "tolerance", "accept_lo", "accept_hi", "diff")

Row = dict[str, float]
Check = dict[str, Any]


def _f(x: Any) -> float:
    """`x` as a float; an empty or non-numeric value is nan."""
    try:
        return float(x)
    except (TypeError, ValueError):
        return math.nan


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


class _Row(dict):
    """A pooled row; a column or a whole cell that is absent reads as nan."""

    def __missing__(self, key: str) -> float:
        return math.nan


def _load_pooled(path: Path) -> dict[tuple[str, float, float, float], _Row]:
    out: dict[tuple[str, float, float, float], _Row] = {}
    for rec in _read_csv(path):
        row = _Row({k: _f(v) for k, v in rec.items() if k != "mode"})
        out[(rec["mode"], row["late_share"], row["max_delay_s"], row["lateness_s"])] = row
    return out


def _load_checks(path: Path) -> list[Check]:
    checks: list[Check] = []
    for rec in _read_csv(path):
        c: Check = {k: _f(rec[k]) for k in (*CHECK_NUMBERS, *CHECK_VALUES)}
        c.update(
            family=rec["family"],
            quantity=rec["quantity"],
            mode=rec["mode"],
            passed=rec["passed"] == "True",
            note=rec["note"],
        )
        checks.append(c)
    return checks


# ---------------------------------------------------------------- formatting


def _pct(x: float) -> str:
    return "n/a" if math.isnan(x) else f"{x * 100:.1f}%"


def _sec(x: float) -> str:
    return "n/a" if math.isnan(x) else f"{x:.1f}"


def _cnt(x: float) -> str:
    return "n/a" if math.isnan(x) else f"{x:.2f}"


def _g(x: float) -> str:
    return "n/a" if math.isnan(x) else f"{x:.6g}"


def _frac(x: float) -> str:
    return "n/a" if math.isnan(x) else f"{x:.5f}"


def _share(x: float) -> str:
    return f"{x * 100:g}%"


def _span(values: Iterable[float], fmt: Callable[[float], str]) -> str:
    """One formatted value, or `lo to hi` when the values differ."""
    vals = list(values)
    shown = {fmt(v) for v in vals}
    if len(shown) == 1:
        return fmt(vals[0])
    finite = [v for v in vals if not math.isnan(v)]
    return f"{fmt(min(finite))} to {fmt(max(finite))}" if finite else "n/a"


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines.extend("| " + " | ".join(r) + " |" for r in rows)
    return lines


def _yes(flag: bool) -> str:
    return "yes" if flag else "no"


# ---------------------------------------------------------------- 1. provenance


def _provenance(results: Path, claim: dict[str, Any]) -> list[str]:
    cfg = claim["grid_config"]
    seeds = sorted({int(r["seed"]) for r in _read_csv(results / SWEEP_NAME)})
    claim_sha = claim.get("predictions_sha256")
    predictions = results / PREDICTIONS_NAME
    file_sha = (
        hashlib.sha256(predictions.read_bytes()).hexdigest() if predictions.is_file() else None
    )
    pin = pin_path(predictions)
    pinned: str | None = None
    if pin.is_file():
        words = pin.read_text(encoding="utf-8").split()
        pinned = words[0].lower() if words else ""

    def named(x: str | None, missing: str) -> str:
        return missing if x is None else (x or "(empty)")

    same_claim = _yes(claim_sha is not None and claim_sha == pinned)
    same_file = _yes(file_sha is not None and file_sha == pinned)
    lines = [
        "## 1. Provenance",
        "",
        f"- grid: {claim['grid']}",
        f"- registered grid: {_yes(claim['registered_grid'])}",
        f"- n_runs: {cfg['n_runs']}",
        f"- seeds ({cfg['n_seeds']}): {', '.join(str(s) for s in seeds)}",
        f"- duration_s per run: {cfg['duration_s']:g}",
        f"- late shares: {', '.join(_share(x) for x in cfg['late_shares'])}",
        f"- max delays (s): {', '.join(f'{x:g}' for x in cfg['max_delays'])}",
        f"- lateness L (s): {', '.join(f'{x:g}' for x in cfg['lateness'])}",
        f"- modes: {', '.join(cfg['modes'])}",
        f"- predictions sha256 (in {CLAIM_NAME}): {named(claim_sha, 'none recorded')}",
        f"- predictions sha256 (computed from {PREDICTIONS_NAME}): "
        f"{named(file_sha, 'file not found')}",
        f"- predictions sha256 (pinned in {pin.name}): {named(pinned, 'pin file not found')}",
        f"- claim sha256 equals pinned sha256: {same_claim}",
        f"- computed sha256 equals pinned sha256: {same_file}",
    ]
    return lines


# ---------------------------------------------------------------- 2. claims


def _failed_line(c: Check) -> str:
    where = f"share={_share(c['late_share'])} delay={c['max_delay_s']:g}s L={c['lateness_s']:g}s"
    parts = [
        f"{c['family']} {c['quantity']} mode={c['mode']} {where}",
        f"observed={_g(c['observed'])}",
        f"predicted={_g(c['predicted'])}",
    ]
    if not (math.isnan(c["accept_lo"]) and math.isnan(c["accept_hi"])):
        parts.append(f"region=[{_g(c['accept_lo'])}, {_g(c['accept_hi'])}]")
    else:
        parts.append(f"tolerance={_g(c['tolerance'])}")
    parts.append(f"diff={_g(c['diff'])}")
    if c["note"]:
        parts.append(f"note={c['note']}")
    return "- " + " ".join(parts)


def _claims(claim: dict[str, Any], checks: list[Check] | None) -> list[str]:
    c0 = claim["c0_construction_check"]
    cell = c0["cell"]
    lines = [
        "## 2. Claims",
        "",
        "### C0 construction check",
        "",
        f"Cell: mode={cell['mode']}, L={cell['lateness_s']:g}s, "
        f"late share={_share(cell['late_share'])}, max delay={cell['max_delay_s']:g}s "
        f"(present in the pooled table: {_yes(c0['cell_present'])}).",
        "",
    ]
    rows = [
        [
            name,
            _g(_f(c0[name]["value"])),
            c0[name]["comparison"],
            _g(_f(c0[name]["threshold"])),
            _yes(c0[name]["passed"]),
        ]
        for name in ("exact_rate_final", "first_emit_latency_p95")
    ]
    lines += _table(["metric", "value", "comparison", "threshold", "passed"], rows)
    lines += ["", f"C0 passed: {_yes(c0['passed'])}", "", "### Primary claim", ""]

    primary = claim.get("primary")
    if primary is None:
        lines.append("Not evaluated: the sweep ran without a predictions file.")
        return lines

    families = primary["families"]
    order = [f for f in FAMILIES if f in families] + sorted(set(families) - set(FAMILIES))
    rows = [
        [
            f,
            str(families[f]["n_checks"]),
            str(families[f]["n_passed"]),
            str(families[f]["n_failed"]),
        ]
        for f in order
    ]
    rows.append(
        ["all", str(primary["n_checks"]), str(primary["n_passed"]), str(primary["n_failed"])]
    )
    lines += _table(["family", "n_checks", "n_passed", "n_failed"], rows)
    lines += ["", f"primary_passed: {_yes(primary['primary_passed'])}"]
    if checks is not None:
        failed = [c for c in checks if not c["passed"]]
        lines.append(
            f"Checks in {CHECKS_NAME}: {len(checks)}; failed in {CHECKS_NAME}: {len(failed)}; "
            f"failed in {CLAIM_NAME}: {primary['n_failed']}."
        )
        lines += ["", "Failed checks:", ""]
        lines += [_failed_line(c) for c in failed] if failed else ["none"]
    return lines


# ---------------------------------------------------------------- 3. predicted vs observed


def _by_pair(checks: list[Check], family: str) -> dict[tuple[float, float], list[Check]]:
    out: dict[tuple[float, float], list[Check]] = {}
    for c in checks:
        if c["family"] == family:
            out.setdefault((c["late_share"], c["max_delay_s"]), []).append(c)
    return dict(sorted(out.items()))


def _cell_key(c: Check) -> tuple[float, float, float]:
    return (c["late_share"], c["max_delay_s"], c["lateness_s"])


def _index(rows: Iterable[dict[str, Any]], with_lateness: bool) -> dict[tuple[float, ...], Any]:
    out: dict[tuple[float, ...], Any] = {}
    for r in rows:
        key = (_f(r["late_share"]), _f(r["max_delay_s"]))
        out[(*key, _f(r["lateness_s"])) if with_lateness else key] = r
    return out


def _passed_of(group: Sequence[Check]) -> str:
    return f"{sum(c['passed'] for c in group)}/{len(group)}"


def _lateness_note(group: dict[Any, list[Check]]) -> str:
    sizes = sorted({len(g) for g in group.values()})
    return ", ".join(str(n) for n in sizes)


def _predicted_vs_observed(checks: list[Check], predictions: dict[str, Any]) -> list[str]:
    lines = ["## 3. Predicted vs observed", ""]

    # P1 ---------------------------------------------------------------
    rho = {k: _f(v["rho"]) for k, v in _index(predictions["p1_drop_rate"], True).items()}
    p1 = sorted((c for c in checks if c["family"] == "p1"), key=_cell_key)
    positive = [c for c in p1 if c["predicted"] != 0.0]
    zero = [c for c in p1 if c["predicted"] == 0.0]
    lines += ["### P1: late-drop counts (cells with rho > 0)", ""]
    rows = [
        [
            _share(c["late_share"]),
            f"{c['max_delay_s']:g}",
            f"{c['lateness_s']:g}",
            _g(rho.get(_cell_key(c), math.nan)),
            _g(c["observed"]),
            _g(c["predicted"]),
            _g(c["tolerance"]),
            _g(c["diff"]),
            _yes(c["passed"]),
        ]
        for c in positive
    ]
    lines += _table(
        [
            "late share",
            "max delay s",
            "L s",
            "rho",
            "observed D",
            "expected rho*N",
            "tolerance",
            "diff",
            "passed",
        ],
        rows,
    )
    zero_hit = sum(c["observed"] == 0.0 for c in zero)
    lines += [
        "",
        f"Cells with rho = 0: {len(zero)}; observed D = 0 in {zero_hit} of them; "
        f"checks passed: {sum(c['passed'] for c in zero)} of {len(zero)}.",
        "",
    ]

    # P2 ---------------------------------------------------------------
    p2_pred = _index(predictions["p2_first_exact"], False)
    p2 = _by_pair(checks, "p2")
    lines += [
        "### P2: revise first-answer exact count, per (late share, max delay)",
        "",
        "The check repeats at every L; each row stands for the L values counted in the last "
        "column. Where the observed counts differ across L the row shows `lo to hi`.",
        "",
    ]
    rows = []
    for (share, delay), group in p2.items():
        pred = p2_pred.get((share, delay), {})
        n_exp = _f(pred.get("n_expected"))
        obs = [c["observed"] for c in group]
        rows.append(
            [
                _share(share),
                f"{delay:g}",
                _g(n_exp),
                _span(obs, _g),
                f"[{_span([c['accept_lo'] for c in group], _g)}, "
                f"{_span([c['accept_hi'] for c in group], _g)}]",
                _frac(_f(pred.get("pi"))),
                _span([o / n_exp for o in obs], _frac),
                _passed_of(group),
            ]
        )
    lines += _table(
        [
            "late share",
            "max delay s",
            "n windows",
            "observed count",
            "region [lo, hi]",
            "pi",
            "observed rate",
            "passed (L values)",
        ],
        rows,
    )
    lines.append("")

    # P4 ---------------------------------------------------------------
    p4_pred = _index(predictions["p4_final_exact"], True)
    p4 = sorted((c for c in checks if c["family"] == "p4"), key=_cell_key)
    below = [c for c in p4 if _f(p4_pred.get(_cell_key(c), {}).get("pi")) != 1.0]
    exact_one = [c for c in p4 if c not in below]
    lines += ["### P4: wait final-exact count (cells with pi4 < 1)", ""]
    rows = []
    for c in below:
        pred = p4_pred.get(_cell_key(c), {})
        n_exp = _f(pred.get("n_expected"))
        rows.append(
            [
                _share(c["late_share"]),
                f"{c['max_delay_s']:g}",
                f"{c['lateness_s']:g}",
                _g(n_exp),
                _g(c["observed"]),
                f"[{_g(c['accept_lo'])}, {_g(c['accept_hi'])}]",
                _frac(_f(pred.get("pi"))),
                _frac(c["observed"] / n_exp),
                _yes(c["passed"]),
            ]
        )
    lines += _table(
        [
            "late share",
            "max delay s",
            "L s",
            "n windows",
            "observed count",
            "region [lo, hi]",
            "pi4",
            "observed rate",
            "passed",
        ],
        rows,
    )
    lines += [
        "",
        f"Cells with pi4 = 1: {len(exact_one)}; checks passed: "
        f"{sum(c['passed'] for c in exact_one)} of {len(exact_one)}.",
        "",
    ]

    # P3 wait ----------------------------------------------------------
    lines += ["### P3: first-emit latency p95, wait mode (seconds)", ""]
    p3w = sorted((c for c in checks if c["family"] == "p3_wait"), key=_cell_key)
    rows = [
        [
            _share(c["late_share"]),
            f"{c['max_delay_s']:g}",
            f"{c['lateness_s']:g}",
            _g(c["observed"]),
            _g(c["predicted"]),
            _g(c["tolerance"]),
            _g(c["diff"]),
            _yes(c["passed"]),
        ]
        for c in p3w
    ]
    lines += _table(
        [
            "late share",
            "max delay s",
            "L s",
            "observed",
            "predicted",
            "tolerance",
            "diff",
            "passed",
        ],
        rows,
    )
    lines.append("")

    # P3 revise --------------------------------------------------------
    p3r = _by_pair(checks, "p3_revise")
    lines += [
        "### P3: first-emit latency p95, revise mode, per (late share, max delay) (seconds)",
        "",
        f"The check repeats at every L ({_lateness_note(p3r)} L values per row). Where values "
        "differ across L the row shows `lo to hi`.",
        "",
    ]
    rows = [
        [
            _share(share),
            f"{delay:g}",
            _span([c["observed"] for c in group], _g),
            _span([c["predicted"] for c in group], _g),
            _span([c["tolerance"] for c in group], _g),
            _span([c["diff"] for c in group], _g),
            _passed_of(group),
        ]
        for (share, delay), group in p3r.items()
    ]
    lines += _table(
        [
            "late share",
            "max delay s",
            "observed",
            "predicted",
            "tolerance",
            "diff",
            "passed (L values)",
        ],
        rows,
    )
    return lines


# ---------------------------------------------------------------- 4 and 5. wait vs revise

Getter = Callable[[Row, Row], str]
Column = tuple[str, Getter]


def _per_mode(key: str, label: str, fmt: Callable[[float], str]) -> list[Column]:
    return [
        (f"{label} wait", lambda w, r: fmt(w[key])),
        (f"{label} revise", lambda w, r: fmt(r[key])),
    ]


def _shared(key: str, label: str, fmt: Callable[[float], str]) -> list[Column]:
    def get(w: Row, r: Row) -> str:
        a, b = fmt(w[key]), fmt(r[key])
        return a if a == b else f"{a} wait / {b} revise"

    return [(label, get)]


COLUMN_GROUPS: dict[str, list[Column]] = {
    "first": _per_mode("first_emit_latency_p95", "first-emit p95 s", _sec),
    "final": _per_mode("final_latency_p95", "final p95 s", _sec),
    "exact_first": _per_mode("exact_rate_first_live", "exact_rate_first_live", _pct),
    "exact_final": _shared("exact_rate_final_live", "exact_rate_final_live", _pct),
    "dropped": _shared("late_dropped_rate", "late_dropped_rate", _pct),
    "mae": _shared("mean_abs_count_error_live", "mean_abs_count_error_live", _cnt),
    "valrev": [
        (
            "value_revisions_per_window (revise)",
            lambda w, r: _cnt(r["value_revisions_per_window"]),
        )
    ],
    "records": _per_mode("records_emitted", "records_emitted per run", _cnt),
}
MAIN_ORDER = ("first", "final", "exact_first", "exact_final", "dropped", "mae", "valrev", "records")
STRESS_ORDER = (
    "mae",
    "dropped",
    "first",
    "final",
    "exact_first",
    "exact_final",
    "valrev",
    "records",
)

Pooled = dict[tuple[str, float, float, float], _Row]


def _cell_table(pooled: Pooled, share: float, delay: float, order: Sequence[str]) -> list[str]:
    columns = [c for name in order for c in COLUMN_GROUPS[name]]
    levels = sorted({k[3] for k in pooled if k[1] == share and k[2] == delay})
    empty = _Row()
    rows = []
    for level in levels:
        wait = pooled.get(("wait", share, delay, level), empty)
        revise = pooled.get(("revise", share, delay, level), empty)
        rows.append([f"{level:g}", *(get(wait, revise) for _, get in columns)])
    return _table(["L s", *(h for h, _ in columns)], rows)


def _same(a: dict[str, float], b: dict[str, float], skip: str) -> bool:
    """Equal in every column but `skip`, with nan equal to nan."""
    keys = (set(a) | set(b)) - {skip}
    return all(
        a.get(k, math.nan) == b.get(k, math.nan)
        or (math.isnan(a.get(k, math.nan)) and math.isnan(b.get(k, math.nan)))
        for k in keys
    )


def _n_seeds(pooled: Pooled, share: float, delay: float) -> str:
    seeds = {int(r["n_seeds"]) for k, r in pooled.items() if k[1] == share and k[2] == delay}
    return ", ".join(str(n) for n in sorted(seeds))


def _wait_vs_revise(pooled: Pooled) -> list[str]:
    shares = sorted({k[1] for k in pooled})
    delays = sorted({k[2] for k in pooled})
    lines = [
        "## 4. Wait vs revise at equal L",
        "",
        "Pooled over seeds. Latencies are stream seconds. Live metrics exclude windows closed by "
        "the end-of-stream flush. `records_emitted per run` is the mean per run.",
        "",
    ]
    if 0.0 in shares:
        base = delays[0]
        lines += [
            "### late share 0%",
            "",
            f"Shown for max delay {base:g}s "
            f"(seeds pooled per cell: {_n_seeds(pooled, 0.0, base)}).",
            "",
        ]
        rest = {k: r for k, r in pooled.items() if k[1] == 0.0}
        identical = all(
            _same(r, rest.get((k[0], 0.0, base, k[3]), _Row()), skip="max_delay_s")
            for k, r in rest.items()
        )
        lines += [
            f"Rows identical across max delay ({', '.join(f'{d:g}' for d in delays)}s): "
            f"{_yes(identical)}.",
            "",
            *_cell_table(pooled, 0.0, base, MAIN_ORDER),
            "",
        ]
    for share in shares:
        if share <= 0.0:
            continue
        for delay in delays:
            lines += [
                f"### late share {_share(share)}, max delay {delay:g}s",
                "",
                f"Seeds pooled per cell: {_n_seeds(pooled, share, delay)}.",
                "",
                *_cell_table(pooled, share, delay, MAIN_ORDER),
                "",
            ]
    return lines[:-1] if lines[-1] == "" else lines


def _stress(pooled: Pooled) -> list[str]:
    share, delay = STRESS_CELL
    lines = [
        f"## 5. Stress regime (late share {_share(share)}, max delay {delay:g}s)",
        "",
        f"Seeds pooled per cell: {_n_seeds(pooled, share, delay)}.",
        "",
    ]
    if not any(k[1] == share and k[2] == delay for k in pooled):
        return [lines[0], "", "Not present in the pooled table."]
    return [*lines, *_cell_table(pooled, share, delay, STRESS_ORDER)]


# ---------------------------------------------------------------- 6. notes

NOTES = (
    "Rows with a 0% late share are identical across max delay. They are one result shown three "
    "times, not replications.",
    "For a given seed every cell shares the same event times, zones and amounts; only arrival "
    "times differ. Cells are paired within a seed, not independent.",
    "Percentiles are nearest-rank.",
    "Latency is stream time: the arrival time of the triggering event minus the window end, "
    "not wall-clock time.",
    "The generator's lateness model is an assumption, not a property of real order systems. "
    "The predictions and the simulator share the same assumed generator.",
    "Bursts start at t = 0 and fill the first half of every tenth window.",
    "Within the checks, P2 and P3-revise repeat across L, P4 at L = 0 equals P2, and the "
    "invariant checks are deterministic, so the independent statistical content is smaller "
    "than the check count (EVAL_PLAN.md, A1.6; ANALYTIC.md section 6).",
)


def _notes() -> list[str]:
    return ["## 6. Notes", "", *(f"- {n}" for n in NOTES)]


# ---------------------------------------------------------------- entry points


def render_report(results_dir: Path | str) -> str:
    """The report for the result files in `results_dir`, as markdown text."""
    results = Path(results_dir)
    claim = json.loads((results / CLAIM_NAME).read_text(encoding="utf-8"))
    pooled = _load_pooled(results / POOLED_NAME)
    primary = claim.get("primary")
    checks = _load_checks(results / CHECKS_NAME) if primary is not None else None

    blocks = [
        ["# Sweep report", "", "Every value below is read from the committed result files."],
        _provenance(results, claim),
        _claims(claim, checks),
    ]
    if checks is not None:
        predictions = json.loads((results / PREDICTIONS_NAME).read_text(encoding="utf-8"))
        blocks.append(_predicted_vs_observed(checks, predictions))
    else:
        blocks.append(["## 3. Predicted vs observed", "", "Not evaluated: no predictions file."])
    blocks += [_wait_vs_revise(pooled), _stress(pooled), _notes()]
    return "\n\n".join("\n".join(b) for b in blocks) + "\n"


def write_report(results_dir: Path | str, out: Path | str = REPORT_OUT) -> Path:
    """Render the report and write it as UTF-8 with LF line endings."""
    target = Path(out)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_report(results_dir), encoding="utf-8", newline="\n")
    return target
