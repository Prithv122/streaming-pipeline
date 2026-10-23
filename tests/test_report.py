import csv
import json
import re
from pathlib import Path

import pytest

from streampipe import cli
from streampipe.evalrun import Grid, run_grid, write_pin
from streampipe.report import render_report, write_report

# Small enough to run in a few seconds; it contains the C0 cell (wait, 30 s, 5 %, 30 s) and the
# stress cell (20 %, 120 s), and two max delays so the 0 % rows can be compared across them.
GRID = Grid(
    late_shares=(0.0, 0.05, 0.20),
    max_delays=(30.0, 120.0),
    lateness=(0.0, 30.0),
    modes=("wait", "revise"),
    seeds=(0, 1),
    duration_s=120.0,
)


def _read(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _predictions(pooled: list[dict[str, str]], perfect: bool) -> dict:
    """Predictions copied from the observed cells (perfect) or deliberately off (not perfect)."""

    def cell(r: dict[str, str]) -> dict:
        return {
            "late_share": float(r["late_share"]),
            "max_delay_s": float(r["max_delay_s"]),
            "lateness_s": float(r["lateness_s"]),
        }

    def region(count: float, n: float) -> dict:
        off = 0.0 if perfect else n + 5.0
        return {"n_expected": n, "accept_lo": count + off, "accept_hi": count + off}

    wait = [r for r in pooled if r["mode"] == "wait"]
    revise = [r for r in pooled if r["mode"] == "revise"]
    first_l = min(float(r["lateness_s"]) for r in revise)
    revise_pairs = [r for r in revise if float(r["lateness_s"]) == first_l]
    n_events = float(wait[0]["n_events"])
    shift = 0.0 if perfect else 1000.0
    rho_off = 0.0 if perfect else 0.3
    return {
        "p0_events": {
            "expected": n_events,
            "accept_lo": n_events - 10 + shift,
            "accept_hi": n_events + 10 + shift,
        },
        "p1_drop_rate": [
            {**cell(r), "rho": float(r["n_dropped"]) / float(r["n_events"]) + rho_off} for r in wait
        ],
        "p2_first_exact": [
            {
                **cell(r),
                "pi": float(r["n_exact_first_live"]) / float(r["n_windows_first_live"]),
                **region(float(r["n_exact_first_live"]), float(r["n_windows_first_live"])),
            }
            for r in revise_pairs
        ],
        "p3_first_latency": [
            {
                **cell(r),
                "mode": r["mode"],
                "p95": float(r["first_emit_latency_p95"]) + shift / 100,
                "tolerance": 0.1,
            }
            for r in wait + revise_pairs
        ],
        "p4_final_exact": [
            {
                **cell(r),
                "pi": float(r["n_exact_final_live"]) / float(r["n_windows_live"]),
                **region(float(r["n_exact_final_live"]), float(r["n_windows_live"])),
            }
            for r in wait
        ],
    }


def _make_results(root: Path, perfect: bool) -> Path:
    out = root / ("good" if perfect else "bad")
    out.mkdir()
    run_grid(GRID, out / "sweep.csv")  # first pass: only the observed cells are needed
    predictions = out / "predictions.json"
    predictions.write_text(
        json.dumps(_predictions(_read(out / "sweep_pooled.csv"), perfect)), encoding="utf-8"
    )
    write_pin(predictions)
    run_grid(GRID, out / "sweep.csv", predictions_path=predictions)
    return out


@pytest.fixture(scope="module")
def good(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return _make_results(tmp_path_factory.mktemp("report_good"), perfect=True)


@pytest.fixture(scope="module")
def bad(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return _make_results(tmp_path_factory.mktemp("report_bad"), perfect=False)


def _section(text: str, heading: str) -> str:
    start = text.index(heading)
    nxt = re.search(r"^## ", text[start + len(heading) :], re.MULTILINE)
    return text[start : start + len(heading) + nxt.start()] if nxt else text[start:]


def test_sections_come_in_order_and_provenance_is_read_from_the_files(good: Path) -> None:
    text = render_report(good)
    heads = [
        "## 1. Provenance",
        "## 2. Claims",
        "## 3. Predicted vs observed",
        "## 4. Wait vs revise at equal L",
        "## 5. Stress regime",
        "## 6. Notes",
    ]
    positions = [text.index(h) for h in heads]
    assert positions == sorted(positions)
    claim = json.loads((good / "sweep_claim.json").read_text(encoding="utf-8"))
    prov = _section(text, "## 1. Provenance")
    assert "- grid: custom" in prov
    assert "- n_runs: 48" in prov
    assert "- seeds (2): 0, 1" in prov
    assert "- duration_s per run: 120" in prov
    assert f"(in sweep_claim.json): {claim['predictions_sha256']}" in prov
    assert "- claim sha256 equals pinned sha256: yes" in prov
    assert "- computed sha256 equals pinned sha256: yes" in prov


def test_claims_section_reports_c0_and_family_counts(good: Path) -> None:
    text = render_report(good)
    claim = json.loads((good / "sweep_claim.json").read_text(encoding="utf-8"))
    sec = _section(text, "## 2. Claims")
    c0 = claim["c0_construction_check"]
    assert c0["cell_present"]
    assert f"| exact_rate_final | {c0['exact_rate_final']['value']:.6g} | >= | 0.99 |" in sec
    primary = claim["primary"]
    assert f"| all | {primary['n_checks']} | {primary['n_passed']} | {primary['n_failed']} |" in sec
    for fam, n in primary["families"].items():
        assert f"| {fam} | {n['n_checks']} | {n['n_passed']} | {n['n_failed']} |" in sec
    assert primary["primary_passed"] is True
    assert "primary_passed: yes" in sec
    assert sec.rstrip().endswith("Failed checks:\n\nnone")


def test_failed_checks_are_listed_in_full(bad: Path) -> None:
    text = render_report(bad)
    claim = json.loads((bad / "sweep_claim.json").read_text(encoding="utf-8"))
    n_failed = claim["primary"]["n_failed"]
    assert n_failed > 0
    sec = _section(text, "## 2. Claims")
    assert "primary_passed: no" in sec
    listed = [ln for ln in sec.splitlines() if ln.startswith("- p")]
    assert len(listed) == n_failed
    assert any(ln.startswith("- p1 n_dropped mode=wait") for ln in listed)
    assert any(ln.startswith("- p0 n_events") and "region=[" in ln for ln in listed)
    assert not sec.rstrip().endswith("none")
    assert f"failed in sweep_claim.json: {n_failed}." in sec


def test_predicted_vs_observed_tables_carry_the_checked_values(good: Path) -> None:
    sec = _section(render_report(good), "## 3. Predicted vs observed")
    checks = _read(good / "sweep_checks.csv")
    p1 = next(c for c in checks if c["family"] == "p1" and float(c["predicted"]) > 0)
    cells = (
        f"| {float(p1['late_share']) * 100:g}% | {float(p1['max_delay_s']):g} | "
        f"{float(p1['lateness_s']):g} |"
    )
    row = next(ln for ln in sec.splitlines() if ln.startswith(cells))
    assert f"| {float(p1['observed']):.6g} | {float(p1['predicted']):.6g} |" in row
    assert re.search(r"Cells with rho = 0: \d+; observed D = 0 in \d+ of them", sec)
    p2 = sec[sec.index("### P2") : sec.index("### P4")]
    assert "| 2/2 |" in p2  # each (share, delay) row stands for the two L values
    assert len([ln for ln in p2.splitlines() if ln.endswith("| 2/2 |")]) == 6
    assert "### P3: first-emit latency p95, revise mode" in sec


def test_wait_vs_revise_tables_come_from_the_pooled_file(good: Path) -> None:
    text = render_report(good)
    pooled = {
        (r["mode"], float(r["late_share"]), float(r["max_delay_s"]), float(r["lateness_s"])): r
        for r in _read(good / "sweep_pooled.csv")
    }
    sec = _section(text, "## 4. Wait vs revise at equal L")
    assert "### late share 5%, max delay 30s" in sec
    assert "### late share 20%, max delay 120s" in sec
    assert sec.count("### late share 0%") == 1
    assert "Rows identical across max delay (30, 120s): yes." in sec
    assert "| L s | first-emit p95 s wait | first-emit p95 s revise |" in sec
    w = pooled[("wait", 0.2, 120.0, 30.0)]
    r = pooled[("revise", 0.2, 120.0, 30.0)]
    stress = _section(text, "## 5. Stress regime (late share 20%, max delay 120s)")
    row = next(ln for ln in stress.splitlines() if ln.startswith("| 30 |"))
    cells = [c.strip() for c in row.strip("|").split("|")]
    assert cells[1] == f"{float(w['mean_abs_count_error_live']):.2f}"
    assert cells[2] == f"{float(w['late_dropped_rate']) * 100:.1f}%"
    assert cells[3] == f"{float(w['first_emit_latency_p95']):.1f}"
    assert cells[4] == f"{float(r['first_emit_latency_p95']):.1f}"
    assert cells[-1] == f"{float(r['records_emitted']):.2f}"
    assert stress.index("mean_abs_count_error_live") < stress.index("first-emit p95 s wait")


def test_notes_carry_the_reporting_caveats(good: Path) -> None:
    notes = _section(render_report(good), "## 6. Notes")
    for phrase in (
        "identical across max delay",
        "paired within a seed",
        "nearest-rank",
        "stream time",
        "assumption",
    ):
        assert phrase in notes


def test_report_is_deterministic_and_has_no_timestamp(good: Path, tmp_path: Path) -> None:
    text = render_report(good)
    assert render_report(good) == text
    assert not re.search(r"\d{4}-\d{2}-\d{2}", text)
    a = write_report(good, tmp_path / "a" / "REPORT.md")
    b = write_report(good, tmp_path / "b" / "REPORT.md")
    assert a.read_bytes() == b.read_bytes() == text.encode("utf-8")
    assert b"\r" not in a.read_bytes()


def test_sha_mismatch_and_missing_pin_are_reported(good: Path, tmp_path: Path) -> None:
    copy = tmp_path / "mismatch"
    copy.mkdir()
    for f in good.iterdir():
        (copy / f.name).write_bytes(f.read_bytes())
    (copy / "predictions.sha256").write_text(f"{'0' * 64}  predictions.json\n", encoding="utf-8")
    prov = _section(render_report(copy), "## 1. Provenance")
    assert f"(pinned in predictions.sha256): {'0' * 64}" in prov
    assert "- claim sha256 equals pinned sha256: no" in prov
    assert "- computed sha256 equals pinned sha256: no" in prov

    (copy / "predictions.sha256").unlink()
    prov = _section(render_report(copy), "## 1. Provenance")
    assert "(pinned in predictions.sha256): pin file not found" in prov
    assert "- claim sha256 equals pinned sha256: no" in prov


def test_report_without_predictions_says_so(tmp_path: Path) -> None:
    run_grid(GRID, tmp_path / "sweep.csv")
    text = render_report(tmp_path)
    assert "Not evaluated: the sweep ran without a predictions file." in text
    assert "Not evaluated: no predictions file." in text
    assert "(in sweep_claim.json): none recorded" in text
    assert "## 4. Wait vs revise at equal L" in text


def test_stress_section_when_the_cell_is_absent(tmp_path: Path) -> None:
    small = Grid((0.05,), (30.0,), (0.0, 30.0), ("wait", "revise"), (0,), 120.0)
    run_grid(small, tmp_path / "sweep.csv")
    stress = _section(render_report(tmp_path), "## 5. Stress regime")
    assert "Not present in the pooled table." in stress


def test_cli_writes_the_report_and_reports_missing_files(
    good: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "out" / "REPORT.md"
    assert cli.main(["report", "--results", str(good), "--out", str(out)]) == 0
    assert out.read_text(encoding="utf-8") == render_report(good)
    assert "wrote" in capsys.readouterr().out
    args = cli._build_parser().parse_args(["report"])
    assert (args.results, args.out) == (Path("results"), Path("results/REPORT.md"))
    assert cli.main(["report", "--results", str(tmp_path / "nope"), "--out", str(out)]) == 2
    assert "cannot render the report" in capsys.readouterr().err
