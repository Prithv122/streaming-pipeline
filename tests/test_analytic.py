"""Tests of the analytic model's mathematics.

Nothing here runs the generator or the windowing core: the model is checked against its own
definitions, independent quadrature, finite differences and closed forms.
"""

from __future__ import annotations

import ast
import inspect
import itertools
import json
import math
from pathlib import Path

import numpy as np
import pytest

from streampipe import analytic as an
from streampipe.analytic import Model, ModelParams

ROOT = Path(__file__).resolve().parents[1]
SMALL = ModelParams(duration_s=1200.0)  # two burst periods: fast, same structure
FORBIDDEN = {"windows", "sim", "metrics", "evalrun", "events", "batch", "cli"}

# -- independent reference pieces -------------------------------------------------------------


def cdf(x: np.ndarray, p: float, j: float, y: float) -> np.ndarray:
    """Delay CDF built from its definition: a mixture of two uniform CDFs."""
    x = np.asarray(x, float)
    out = (1 - p) * np.clip(x / j, 0, 1)
    if p > 0:
        out = out + p * np.clip((x - j) / (y - j), 0, 1)
    return out


def gauss(fun, lo, hi, kinks, n=6):
    """Gauss-Legendre integral of fun over [lo, hi] (arrays), split at the given kink arrays."""
    lo, hi = np.asarray(lo, float), np.asarray(hi, float)
    pts = np.stack([lo, *[np.clip(k, lo, hi) for k in kinks], hi], axis=-1)
    pts = np.sort(pts, axis=-1)
    nodes, weights = np.polynomial.legendre.leggauss(n)
    total = np.zeros(lo.shape)
    for i in range(pts.shape[-1] - 1):
        u, v = pts[..., i], pts[..., i + 1]
        s = 0.5 * (v - u)[..., None] * nodes + 0.5 * (u + v)[..., None]
        total += 0.5 * (v - u) * (fun(s) * weights).sum(axis=-1)
    return total


def a_ref(m, tau, p, j, y, params):
    """A(m, tau) by quadrature of lambda(s) F(tau - s) over the rate segments."""
    m, tau = np.broadcast_arrays(np.asarray(m, float), np.asarray(tau, float))
    total = np.zeros(m.shape)
    for sa, sb, lam in an.rate_segments(params):
        lo = np.minimum(np.maximum(sa, m), tau)
        hi = np.minimum(np.maximum(sb, m), tau)
        total += lam * gauss(
            lambda s, t=tau: cdf(t[..., None] - s, p, j, y), lo, hi, [tau - j, tau - y]
        )
    return total


def mu_ref(e, x, p, j, y, params):
    """Mean number of one zone's window events arriving after x, by quadrature."""
    w = params.window_s
    e, x = np.broadcast_arrays(np.asarray(e, float), np.asarray(x, float))
    total = np.zeros(e.shape)
    for sa, sb, lam in an.rate_segments(params):
        lo = np.clip(sa, e - w, e)
        hi = np.clip(sb, e - w, e)
        total += lam * gauss(
            lambda t, xx=x: 1 - cdf(xx[..., None] - t, p, j, y), lo, hi, [x - j, x - y]
        )
    return total / params.zones


# -- plan constants --------------------------------------------------------------------------


def test_params_match_generator_defaults_and_the_plan() -> None:
    from streampipe.evalrun import FULL_GRID
    from streampipe.events import GenConfig

    g, m = GenConfig(), ModelParams()
    assert m.rate_per_s == g.rate_per_s == 5.0
    assert m.burst_mult == g.burst_mult == 4.0
    assert m.burst_every_s == g.burst_every_s == 600.0
    assert m.burst_len_s == g.burst_len_s == 30.0
    assert m.duration_s == g.duration_s == 3600.0
    assert m.ontime_jitter_s == g.ontime_jitter_s == 2.0
    assert m.zones == len(g.zones) == 6
    assert m.window_s == 60.0
    assert m.seeds == len(FULL_GRID.seeds) == 5
    assert an.LATE_SHARES == FULL_GRID.late_shares == (0.0, 0.05, 0.20)
    assert an.MAX_DELAYS == FULL_GRID.max_delays == (5.0, 30.0, 120.0)
    assert an.LATENESS == FULL_GRID.lateness == (0.0, 5.0, 15.0, 30.0, 60.0)


def test_expected_events_and_segments() -> None:
    assert an.expected_events() == pytest.approx(20_700.0)
    segs = an.rate_segments()
    assert segs[0] == (0.0, 30.0, 20.0)
    assert segs[1] == (30.0, 600.0, 5.0)
    assert segs[-1][1] == 3600.0
    assert len(segs) == 12


# -- F and G ---------------------------------------------------------------------------------


@pytest.mark.parametrize(("p", "y"), [(0.0, 30.0), (0.05, 5.0), (0.2, 120.0)])
def test_f_matches_definition_monotone_and_limits(p: float, y: float) -> None:
    m = Model(p, y)
    x = np.linspace(-3, 150, 6001)
    f = m.F(x)
    assert np.allclose(f, cdf(x, p, 2.0, m.y))
    assert np.all(np.diff(f) >= 0)
    assert f[0] == 0.0 and f[-1] == 1.0
    assert float(m.F(0.0)) == 0.0
    assert float(m.F(m.y)) == pytest.approx(1.0)
    if p > 0:
        assert float(m.F(2.0)) == pytest.approx(1 - p)


@pytest.mark.parametrize(("p", "y"), [(0.0, 30.0), (0.05, 5.0), (0.2, 120.0)])
def test_g_is_integral_of_f(p: float, y: float) -> None:
    m = Model(p, y)
    assert float(m.G(0.0)) == 0.0 and float(m.G(-5.0)) == 0.0
    grid = np.unique(np.concatenate([np.linspace(0, 200, 20001), [2.0, m.y]]))
    f = m.F(grid)
    trapezoid = np.concatenate([[0.0], np.cumsum(0.5 * (f[1:] + f[:-1]) * np.diff(grid))])
    assert np.allclose(m.G(grid), trapezoid, atol=1e-9)  # trapezoid is exact: F is piecewise linear
    g = np.diff(m.G(grid)) / np.diff(grid)
    assert np.all(np.diff(g) >= -1e-12)  # G is convex because F is non-decreasing
    xs = np.array([0.3, 1.9, 2.5, 4.0, 20.0, 150.0])
    h = 1e-6
    fd = (m.G(xs + h) - m.G(xs - h)) / (2 * h)
    assert np.allclose(fd, m.F(xs), atol=1e-6)


def test_p_zero_ignores_max_delay() -> None:
    a, b = Model(0.0, 5.0), Model(0.0, 120.0)
    x = np.linspace(0, 50, 101)
    assert np.array_equal(a.F(x), b.F(x))
    assert np.array_equal(a.G(x), b.G(x))
    assert np.allclose(a.F(x), np.minimum(x / 2.0, 1.0))
    assert float(a.G(1.0)) == pytest.approx(0.25)
    assert float(a.G(10.0)) == pytest.approx(1.0 + 8.0)


def test_model_rejects_bad_delay_settings() -> None:
    with pytest.raises(ValueError, match="late_share"):
        Model(1.5, 30.0)
    with pytest.raises(ValueError, match="exceed"):
        Model(0.1, 2.0)


# -- A, a, mu --------------------------------------------------------------------------------


@pytest.mark.parametrize(("p", "y"), [(0.0, 30.0), (0.2, 30.0), (0.05, 120.0)])
def test_a_matches_quadrature_across_bursts_and_near_t(p: float, y: float) -> None:
    mod = Model(p, y)
    params = mod.params
    cases = [
        (590.0, 640.0),  # crosses the burst start at 600
        (0.0, 100.0),
        (20.0, 35.0),  # crosses the burst end at 30
        (3570.0, 3599.9),
        (3590.0, 3700.0),  # beyond T the rate is zero
        (3700.0, 3800.0),
        (100.0, 100.0),
        (200.0, 100.0),  # m > tau: nothing
    ]
    m = np.array([c[0] for c in cases])
    tau = np.array([c[1] for c in cases])
    got = mod.A(m, tau)
    want = a_ref(m, tau, p, 2.0, mod.y, params)
    assert np.allclose(got, want, rtol=1e-9, atol=1e-9)
    assert got[-1] == 0.0
    assert got[-2] == 0.0


def test_a_is_zero_when_no_event_can_have_arrived() -> None:
    assert float(Model(0.2, 30.0).A(100.0, 100.0)) == 0.0


@pytest.mark.parametrize(("p", "y"), [(0.0, 30.0), (0.2, 30.0)])
def test_small_a_is_the_derivative_of_big_a(p: float, y: float) -> None:
    mod = Model(p, y)
    for e in (60.0, 600.0, 3540.0):
        x = e + np.array([0.7, 2.3, 5.0, 11.0, 27.0])
        h = 1e-5
        fd = (mod.A(e, x + h) - mod.A(e, x - h)) / (2 * h)
        assert np.allclose(mod.a(e, x), fd, rtol=1e-6, atol=1e-8)
    assert float(mod.a(600.0, 600.0)) == 0.0


@pytest.mark.parametrize(("p", "y"), [(0.0, 30.0), (0.2, 30.0), (0.05, 120.0)])
def test_mu_matches_quadrature(p: float, y: float) -> None:
    mod = Model(p, y)
    for e in (60.0, 600.0, 3540.0):  # burst inside the window, burst right after, plain
        x = e + np.array([0.0, 0.5, 5.0, 40.0, 130.0])
        got = mod.mu(e, x)
        want = mu_ref(e, x, p, 2.0, mod.y, mod.params)
        assert np.allclose(got, want, rtol=1e-9, atol=1e-9)
        assert np.all(np.diff(got) <= 1e-12)  # fewer events remain to arrive as x grows
    # Hand check, p = 0, plain window [540, 600): (r / Z) * (W - G(W)) with G(60) = 1 + 58.
    assert float(Model(0.0, 30.0).mu(600.0, 600.0)) == pytest.approx(5 / 6 * (60 - 59))


# -- P1 --------------------------------------------------------------------------------------


def test_exact_zeros_for_drop_rate() -> None:
    assert an.drop_rate(0.05, 5.0, 5.0, 2.0) == 0.0
    assert an.drop_rate(0.2, 30.0, 30.0, 2.0) == 0.0
    assert an.drop_rate(0.2, 30.0, 60.0, 2.0) == 0.0
    assert an.drop_rate(0.0, 120.0, 2.0, 2.0) == 0.0  # p = 0: nothing exceeds J
    assert an.drop_rate(0.0, 5.0, 15.0, 2.0) == 0.0
    assert an.drop_rate(0.0, 30.0, 0.0, 1.0) > 0.0
    assert an.drop_rate(0.2, 30.0, 15.0, 1.0) > 0.0
    assert an.drops_impossible(0.05, 5.0, 5.0, 2.0)
    assert not an.drops_impossible(0.05, 30.0, 5.0, 2.0)
    assert an.drops_impossible(0.0, 30.0, 2.0, 2.0)


def test_drop_rate_is_non_increasing_in_lateness() -> None:
    for p, y in [(0.05, 120.0), (0.2, 30.0), (0.2, 120.0)]:
        rho = [an.drop_rate(p, y, lat, 2.0) for lat in an.LATENESS]
        assert all(b <= a + 1e-12 for a, b in itertools.pairwise(rho))
        assert rho[0] > 0.0


def test_drop_rate_grows_with_late_share_and_max_delay() -> None:
    assert an.drop_rate(0.05, 30.0, 5.0, 1.0) < an.drop_rate(0.2, 30.0, 5.0, 1.0)
    assert an.drop_rate(0.2, 5.0, 0.0, 1.0) < an.drop_rate(0.2, 120.0, 0.0, 1.0)


@pytest.mark.parametrize(("p", "y", "lat"), [(0.2, 30.0, 0.0), (0.2, 30.0, 5.0), (0.05, 5.0, 0.0)])
def test_drop_rate_matches_independent_quadrature(p: float, y: float, lat: float) -> None:
    params = SMALL
    mod = Model(p, y, params)
    w = params.window_s
    expected = 0.0
    for e in an.window_ends(params):
        m = e + lat
        for sa, sb, lam in an.rate_segments(params):
            lo, hi = max(sa, e - w), min(sb, e)
            if hi <= lo:
                continue
            for prob, dlo, dhi in mod.delay_components():

                def inner(t: np.ndarray, m=m, dlo=dlo, dhi=dhi, prob=prob) -> np.ndarray:
                    d_lo = np.maximum(dlo, m - t)
                    d_hi = np.full_like(t, dhi)
                    d_lo = np.minimum(d_lo, d_hi)

                    def pd(d: np.ndarray) -> np.ndarray:
                        tau = t[:, None] + d
                        return -np.expm1(-a_ref(m, tau, p, 2.0, mod.y, params))

                    return prob / (dhi - dlo) * gauss(pd, d_lo, d_hi, [], n=24)

                expected += lam * float(gauss(inner, np.array(lo), np.array(hi), [], n=24))
    want = expected / an.expected_events(params)
    got = an.drop_rate(p, y, lat, 0.25, params)
    assert got == pytest.approx(want, rel=4e-3)


def test_window_types_dedupe_by_period_position() -> None:
    types = an._window_types(an.window_ends(), 30.0 + 60.0, an.DEFAULT_PARAMS)
    assert sum(c for _, c in types) == 60
    assert len(types) == 10 + 2  # ten burst positions plus the two windows too close to T
    # Position-equivalent windows really do give the same integral.
    mod = Model(0.2, 30.0)
    a = an._window_drops(mod, 120.0, 5.0, 1.0)
    b = an._window_drops(mod, 120.0 + 1200.0, 5.0, 1.0)
    assert a == pytest.approx(b, rel=1e-12)


# -- P2 --------------------------------------------------------------------------------------


def test_first_exact_has_no_lateness_input() -> None:
    assert "lateness_s" not in inspect.signature(an.first_exact).parameters


def test_first_exact_density_is_proper_and_pi_in_unit_interval() -> None:
    mod = Model(0.2, 30.0)
    for e in (60.0, 600.0, 3540.0):
        x_up = an._x_horizon(mod, e)
        x, w = an._midpoints(e, x_up, 0.02)
        mass = w * float(np.sum(mod.a(e, x) * np.exp(-mod.A(e, x))))
        assert mass == pytest.approx(1.0, abs=3e-4)
    pi, var = an.first_exact(0.05, 30.0, 0.05)
    assert 0.0 < pi < 1.0 and var > 0.0
    # More late events make an early first answer less likely to be exact.
    assert an.first_exact(0.2, 30.0, 0.05)[0] < pi
    assert an.first_exact(0.0, 30.0, 0.05)[0] > pi


def test_first_exact_matches_independent_quadrature() -> None:
    params = SMALL
    p, y = 0.2, 30.0
    ends = [e for e in an.window_ends(params) if e <= params.duration_s - params.window_s]
    z, s = params.zones, params.seeds
    sum_c = sum_var = 0.0
    for e in ends:
        x = e + np.linspace(0.0, 16.0, 4001)
        surv = np.exp(-a_ref(e, x, p, 2.0, y, params))
        dens = surv[:-1] - surv[1:]  # P(tau* in each cell)
        mid = 0.5 * (x[1:] + x[:-1])
        c = np.exp(-mu_ref(e, mid, p, 2.0, y, params))
        ec, ec2 = float((c * dens).sum()), float((c * c * dens).sum())
        sum_c += ec
        sum_var += z * (ec - ec2) + z * z * (ec2 - ec * ec)
    n_e = len(ends)
    pi_want, var_want = sum_c / n_e, sum_var * s / (s * z * n_e) ** 2
    pi, var = an.first_exact(p, y, 0.01, params)
    assert pi == pytest.approx(pi_want, rel=2e-3)
    assert var == pytest.approx(var_want, rel=2e-3)


# -- P3 --------------------------------------------------------------------------------------


def test_live_windows() -> None:
    assert len(an.live_ends(0.0)) == 59
    assert len(an.live_ends(30.0)) == 59
    assert len(an.live_ends(60.0)) == 58  # the window ending at 3540 closes at 3600 = T
    assert an.live_ends(60.0)[-1] == 3480.0


@pytest.mark.parametrize("lat", [0.0, 5.0, 30.0, 60.0])
def test_latency_closed_form_for_constant_rate_without_bursts(lat: float) -> None:
    """No bursts, p = 0: P(tau_L - e - L > x) = exp(-r G(x)), so the quantile has a closed form."""
    params = ModelParams(burst_mult=1.0)
    r, j = params.rate_per_s, params.ontime_jitter_s
    p50, p95, se95 = an.first_latency(0.0, 30.0, lat, 0.0125, params)
    # G(x) = x^2 / (2 J) for x <= J, so x = sqrt(2 J ln(1/(1-q)) / r)
    want95 = lat + math.sqrt(2 * j * math.log(20.0) / r)
    want50 = lat + math.sqrt(2 * j * math.log(2.0) / r)
    assert p95 == pytest.approx(want95, abs=2e-3)
    assert p50 == pytest.approx(want50, abs=2e-3)
    x95 = want95 - lat
    density = 0.05 * r * min(x95 / j, 1.0)  # h = r F(x) exp(-r G(x))
    n_live = len(an.live_ends(lat))
    want_se = math.sqrt(0.95 * 0.05 / (params.seeds * n_live)) / density
    assert se95 == pytest.approx(want_se, rel=2e-3)


def test_latency_closed_form_with_late_share_below_the_jitter_scale() -> None:
    """With p = 0.2 the quantile still lies in the on-time range, where G(x) = (1-p) x^2 / 2J."""
    params = ModelParams(burst_mult=1.0)
    p = 0.2
    _, p95, _ = an.first_latency(p, 30.0, 5.0, 0.0125, params)
    c = math.log(20.0) / params.rate_per_s
    assert p95 == pytest.approx(5.0 + math.sqrt(2 * 2.0 * c / (1 - p)), abs=2e-3)


def test_latency_grows_with_lateness_and_bursts_help() -> None:
    p95 = [an.first_latency(0.05, 30.0, lat, 0.05)[1] for lat in (0.0, 15.0, 30.0)]
    assert p95[0] < p95[1] < p95[2]
    assert all(v >= lat for v, lat in zip(p95, (0.0, 15.0, 30.0), strict=True))
    flat = an.first_latency(0.0, 30.0, 0.0, 0.05, ModelParams(burst_mult=1.0))[1]
    assert an.first_latency(0.0, 30.0, 0.0, 0.05)[1] < flat  # burst windows fill faster


# -- isolation -------------------------------------------------------------------------------


def test_imports_are_isolated_from_the_code_it_predicts() -> None:
    tree = ast.parse((ROOT / "src" / "streampipe" / "analytic.py").read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {a.name for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            imported.add(base)
            imported |= {f"{base}.{a.name}" for a in node.names}
    assert not any(name.split(".")[0] == "streampipe" for name in imported), imported
    assert not any(
        name.rsplit(".", 1)[-1] in FORBIDDEN for name in imported if "streampipe" in name
    )
    assert "generate" not in {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}


# -- predictions file -----------------------------------------------------------------------


TINY_PARAMS = ModelParams(duration_s=600.0)
TINY = {
    "params": TINY_PARAMS,
    "late_shares": (0.0, 0.2),
    "max_delays": (5.0, 30.0),
    "lateness": (0.0, 30.0),
    "steps": an.Steps(p1=2.0, p2=0.1, p3=0.1),
}


def test_predictions_schema_on_a_tiny_grid(tmp_path: Path) -> None:
    out = tmp_path / "nested" / "predictions.json"
    preds = an.write_predictions(out, **TINY)
    first = out.read_bytes()
    assert first.decode() == json.dumps(preds, indent=2, sort_keys=True) + "\n"
    d = json.loads(first)
    assert list(d) == sorted(d)
    assert d["model"] == "ANALYTIC.md"
    assert d["params"] == {
        "rate_per_s": 5.0,
        "burst_mult": 4.0,
        "burst_every_s": 600.0,
        "burst_len_s": 30.0,
        "duration_s": 600.0,
        "ontime_jitter_s": 2.0,
        "zones": 6,
        "window_s": 60.0,
        "seeds": 5,
    }
    assert d["tolerance_rules"] == {
        "p1": "abs(D - rho*N) <= 4*sqrt(2*rho*N) + 2",
        "p2": "abs(pi_hat - pi) <= 4*sd",
        "p3": "abs(p95_hat - p95) <= max(4*se95, 0.05)",
    }
    p1, p2, p3 = d["p1_drop_rate"], d["p2_first_exact"], d["p3_first_latency"]
    assert (len(p1), len(p2), len(p3)) == (8, 4, 12)
    for r in p1:
        assert set(r) == {
            "late_share", "max_delay_s", "lateness_s", "rho", "rho_coarse", "step", "exact_zero"
        }  # fmt: skip
    for r in p2:
        assert set(r) == {
            "late_share", "max_delay_s", "pi", "sd", "tolerance", "pi_coarse", "step"
        }  # fmt: skip
        assert r["tolerance"] == pytest.approx(4 * r["sd"])
    for r in p3:
        assert set(r) == {
            "mode", "late_share", "max_delay_s", "lateness_s", "p50", "p95", "se95",
            "tolerance", "p95_coarse", "step",
        }  # fmt: skip
        assert r["tolerance"] == max(4 * r["se95"], 0.05)
    assert [r["mode"] for r in p3] == ["wait"] * 8 + ["revise"] * 4
    assert all(r["lateness_s"] is None for r in p3 if r["mode"] == "revise")
    zero = [r for r in p1 if r["exact_zero"]]
    assert {(r["late_share"], r["max_delay_s"], r["lateness_s"]) for r in zero} == {
        (0.0, 5.0, 30.0),
        (0.0, 30.0, 30.0),
        (0.2, 5.0, 30.0),
        (0.2, 30.0, 30.0),
    }
    assert all(r["rho"] == 0.0 for r in zero)
    # p = 0 rows are one computation shown for every max delay.
    a = [r for r in p1 if r["late_share"] == 0.0 and r["max_delay_s"] == 5.0]
    b = [r for r in p1 if r["late_share"] == 0.0 and r["max_delay_s"] == 30.0]
    assert [r["rho"] for r in a] == [r["rho"] for r in b]
    # Every value agrees with its coarse companion to the stated convergence rule.
    for r in p1:
        assert abs(r["rho"] - r["rho_coarse"]) <= max(1e-6, 1e-3 * abs(r["rho"]))
    for r in p2:
        assert abs(r["pi"] - r["pi_coarse"]) <= max(1e-6, 1e-3 * abs(r["pi"]))
    for r in p3:
        assert abs(r["p95"] - r["p95_coarse"]) <= max(1e-6, 1e-3 * abs(r["p95"]))


def test_committed_predictions_have_the_full_grid() -> None:
    path = ROOT / "results" / "predictions.json"
    if not path.exists():
        pytest.skip("results/predictions.json has not been generated")
    d = json.loads(path.read_text(encoding="utf-8"))
    assert (len(d["p1_drop_rate"]), len(d["p2_first_exact"]), len(d["p3_first_latency"])) == (
        45,
        9,
        54,
    )
    assert d["params"] == {
        "rate_per_s": 5.0,
        "burst_mult": 4.0,
        "burst_every_s": 600.0,
        "burst_len_s": 30.0,
        "duration_s": 3600.0,
        "ontime_jitter_s": 2.0,
        "zones": 6,
        "window_s": 60.0,
        "seeds": 5,
    }
    assert sum(r["mode"] == "wait" for r in d["p3_first_latency"]) == 45
