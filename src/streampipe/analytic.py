"""Closed-form reference model of the window core (see ANALYTIC.md).

The generator is a marked Poisson process, so the event count, the late-drop rate, the exactness
of the first (revise) and final (wait) answers and the first-emit latency have formulas that need
only a few one-dimensional integrals. This module evaluates them. It is deliberately independent
of the code it predicts: it imports neither the windowing core, the simulator, the metrics, the
sweep nor the generator, and takes every parameter from `ModelParams` and the per-cell (late
share, max delay, L).
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np

# Evaluation grid (EVAL_PLAN.md).
LATE_SHARES: tuple[float, ...] = (0.0, 0.05, 0.20)
MAX_DELAYS: tuple[float, ...] = (5.0, 30.0, 120.0)
LATENESS: tuple[float, ...] = (0.0, 5.0, 15.0, 30.0, 60.0)

# Per-tail probability of the exact count regions: the two-sided level of a normal +-4 sigma
# (2 * 3.167e-5 = 6.334e-5), so P2 and P4 are as strict as the old 4-sigma rule was meant to be.
ALPHA_PER_TAIL = 3.167e-5

TOLERANCE_RULES: dict[str, str] = {
    "p0": "accept_lo <= N_total <= accept_hi, Poisson(S*E[N]) +- 4 sd",
    "p1": "abs(D - rho*N) <= 4*sqrt(2*rho*N) + 2",
    "p2": "n_windows == n_expected and accept_lo <= X <= accept_hi (exact tails, alpha_per_tail)",
    "p3": "abs(p95_hat - p95) <= max(4*se95, 0.05)",
    "p4": "n_windows == n_expected and accept_lo <= X <= accept_hi (exact tails, alpha_per_tail)",
}

# exp(-A) below this is treated as zero when choosing integration limits.
_A_TAIL = 30.0
_QUANTILE_P95 = 0.95
_QUANTILE_P50 = 0.50


@dataclass(frozen=True)
class ModelParams:
    """The frozen generator and sweep constants (r, b, P, B, T, J, Z, W, S in ANALYTIC.md)."""

    rate_per_s: float = 5.0
    burst_mult: float = 4.0
    burst_every_s: float = 600.0
    burst_len_s: float = 30.0
    duration_s: float = 3600.0
    ontime_jitter_s: float = 2.0
    zones: int = 6
    window_s: float = 60.0
    seeds: int = 5


DEFAULT_PARAMS = ModelParams()


@dataclass(frozen=True)
class Steps:
    """Starting (coarse) grid steps in seconds; each computation also runs at half the step."""

    p1: float = 2.0
    p2: float = 0.05
    p3: float = 0.05
    p4: float = 0.05
    max_halvings: int = 6


Segment = tuple[float, float, float]


def rate_segments(params: ModelParams = DEFAULT_PARAMS) -> list[Segment]:
    """Constant-rate pieces `(start, end, lambda)` of the event-time intensity on [0, T)."""
    segs: list[Segment] = []
    k = 0
    while (start := k * params.burst_every_s) < params.duration_s:
        burst_end = min(start + params.burst_len_s, params.duration_s)
        cycle_end = min(start + params.burst_every_s, params.duration_s)
        if burst_end > start:
            segs.append((start, burst_end, params.rate_per_s * params.burst_mult))
        if cycle_end > burst_end:
            segs.append((burst_end, cycle_end, params.rate_per_s))
        k += 1
    return segs


def expected_events(params: ModelParams = DEFAULT_PARAMS) -> float:
    """E[N]: expected events in one run."""
    return sum(lam * (b - a) for a, b, lam in rate_segments(params))


def window_ends(params: ModelParams = DEFAULT_PARAMS) -> list[float]:
    """Window end times W, 2W, ..., up to and including the one that holds the last event."""
    n = math.ceil(params.duration_s / params.window_s - 1e-9)
    return [params.window_s * k for k in range(1, n + 1)]


class Model:
    """The delay distribution plus the Poisson intensity, with the closed forms of sections 1-4.

    `p` is the late share and `y` the max delay. For `p == 0` the max delay plays no part and the
    delay is U(0, J).
    """

    def __init__(
        self, late_share: float, max_delay_s: float, params: ModelParams = DEFAULT_PARAMS
    ) -> None:
        j = params.ontime_jitter_s
        if not 0.0 <= late_share <= 1.0:
            raise ValueError("late_share must be in [0, 1]")
        if late_share > 0 and not max_delay_s > j:
            raise ValueError("max_delay_s must exceed the on-time jitter when late_share > 0")
        self.params = params
        self.p = float(late_share)
        self.j = float(j)
        self.y = float(max_delay_s) if late_share > 0 else self.j
        self._dy = (self.y - self.j) if late_share > 0 else 1.0  # guards 0 * (1/0)
        self.segments = rate_segments(params)
        self._g_y = self._g_raw(np.float64(self.y))

    # -- delay CDF F and its integral G (section 1) --------------------------------------------

    def F(self, x: np.ndarray | float) -> np.ndarray:
        x = np.asarray(x, dtype=float)
        p, j, y = self.p, self.j, self.y
        low = (1.0 - p) * x / j
        mid = (1.0 - p) + p * (x - j) / self._dy
        return np.where(x <= 0, 0.0, np.where(x <= j, low, np.where(x <= y, mid, 1.0)))

    def _g_raw(self, x: np.ndarray | float) -> np.ndarray:
        x = np.asarray(x, dtype=float)
        p, j, y = self.p, self.j, self.y
        low = (1.0 - p) * x * x / (2.0 * j)
        d = x - j
        mid = (1.0 - p) * j / 2.0 + (1.0 - p) * d + p * d * d / (2.0 * self._dy)
        return np.where(x <= 0, 0.0, np.where(x <= j, low, np.where(x <= y, mid, np.nan)))

    def G(self, x: np.ndarray | float) -> np.ndarray:
        x = np.asarray(x, dtype=float)
        # Evaluate the middle branch on a clipped argument, then add the unit slope beyond Y.
        inner = self._g_raw(np.minimum(x, self.y))
        return np.where(x > self.y, self._g_y + (x - self.y), inner)

    # -- the three Poisson means (sections 2 and 4) --------------------------------------------

    def _pieces(self, lo: np.ndarray | float, hi: np.ndarray | float, upper: np.ndarray | float):
        """Yield (lam, a, b) for each rate segment clipped to [lo, hi], skipping disjoint ones."""
        lo_min = float(np.min(lo))
        up_max = float(np.max(upper))
        for sa, sb, lam in self.segments:
            if sb <= lo_min or sa >= up_max:
                continue
            a = np.minimum(np.maximum(sa, lo), hi)
            b = np.minimum(np.maximum(sb, lo), hi)
            yield lam, a, b

    def A(self, m: np.ndarray | float, tau: np.ndarray | float) -> np.ndarray:
        """Mean number of events with event time in [m, tau] that have arrived by tau."""
        m, tau = np.broadcast_arrays(np.asarray(m, float), np.asarray(tau, float))
        total = np.zeros(m.shape)
        for lam, a, b in self._pieces(m, tau, tau):
            total += lam * (self.G(tau - a) - self.G(tau - b))
        return total

    def a(self, e: np.ndarray | float, x: np.ndarray | float) -> np.ndarray:
        """dA/dx for A(e, x): the arrival-rate density of events with event time >= e."""
        e, x = np.broadcast_arrays(np.asarray(e, float), np.asarray(x, float))
        total = np.zeros(e.shape)
        for lam, a, b in self._pieces(e, x, x):
            total += lam * (self.F(x - a) - self.F(x - b))
        return total

    def mu(self, e: np.ndarray | float, x: np.ndarray | float) -> np.ndarray:
        """Mean number of one zone's events of window [e-W, e) that arrive after stream time x."""
        w = self.params.window_s
        e, x = np.broadcast_arrays(np.asarray(e, float), np.asarray(x, float))
        total = np.zeros(e.shape)
        for lam, a, b in self._pieces(e - w, e, e):
            total += lam * ((b - a) - (self.G(x - a) - self.G(x - b)))
        return total / self.params.zones

    def delay_components(self) -> list[tuple[float, float, float]]:
        """(probability, low, high) of the uniform components of the delay."""
        comps = [(1.0 - self.p, 0.0, self.j)]
        if self.p > 0:
            comps.append((self.p, self.j, self.y))
        return comps


# -- numerics --------------------------------------------------------------------------------


def _midpoints(lo: float, hi: float, step: float) -> tuple[np.ndarray, float]:
    """Midpoint-rule nodes of [lo, hi] with spacing at most `step`, and the common weight."""
    n = max(1, math.ceil((hi - lo) / step - 1e-9))
    w = (hi - lo) / n
    return lo + (np.arange(n) + 0.5) * w, w


def _close(a: float, b: float) -> bool:
    return abs(a - b) < 1e-6 or abs(a - b) <= 1e-3 * abs(b)


def _refine(
    fn: Callable[[float], tuple[float, ...]],
    coarse_step: float,
    max_halvings: int,
    exact: Sequence[int] = (),
) -> tuple[tuple[float, ...], tuple[float, ...], float]:
    """Run `fn` at h and h/2, halving until every output moves < 0.1 % (or < 1e-6).

    Outputs whose index is in `exact` (integer count bounds) must be identical at both steps.
    """

    def agree(c: tuple[float, ...], f: tuple[float, ...]) -> bool:
        return all(
            (a == b) if i in exact else _close(a, b)
            for i, (a, b) in enumerate(zip(c, f, strict=True))
        )

    coarse = fn(coarse_step)
    step = coarse_step / 2.0
    fine = fn(step)
    for _ in range(max_halvings):
        if agree(coarse, fine):
            return coarse, fine, step
        coarse, step = fine, step / 2.0
        fine = fn(step)
    if agree(coarse, fine):
        return coarse, fine, step
    raise RuntimeError(f"no convergence by step {step}: {coarse} vs {fine}")


# -- P1: late-drop rate (section 3) ----------------------------------------------------------


def drops_impossible(late_share: float, max_delay_s: float, lateness_s: float, j: float) -> bool:
    """True when rho is exactly zero: no delay can exceed L."""
    top = max_delay_s if late_share > 0 else j
    return lateness_s >= top


def _window_drops(model: Model, e: float, lateness_s: float, step: float) -> float:
    """Expected number of dropped events among those whose window ends at `e`."""
    w = model.params.window_s
    m = e + lateness_s
    total = 0.0
    for sa, sb, lam in model.segments:
        lo, hi = max(sa, e - w), min(sb, e)
        if hi <= lo:
            continue
        t, wt = _midpoints(lo, hi, step)
        for prob, dlo, dhi in model.delay_components():
            d0 = np.maximum(dlo, m - t)  # a drop needs t + d > e + L
            span = dhi - d0
            keep = span > 0
            if not keep.any():
                continue
            nd = max(1, math.ceil((dhi - dlo) / step - 1e-9))
            u = (np.arange(nd) + 0.5) / nd
            tk, d0k, spk = t[keep], d0[keep], span[keep]
            tau = tk[:, None] + d0k[:, None] + spk[:, None] * u[None, :]
            p_drop = -np.expm1(-model.A(m, tau))
            inner = (prob / (dhi - dlo)) * (spk / nd) * p_drop.sum(axis=1)
            total += lam * wt * float(inner.sum())
    return total


def _window_types(
    ends: Sequence[float], reach: float, params: ModelParams
) -> list[tuple[float, int]]:
    """Group windows: same position in the burst period and range clear of T share one integral."""
    groups: dict[object, list[float]] = {}
    for e in ends:
        if e + reach <= params.duration_s:
            key: object = ("pos", round(e % params.burst_every_s, 6))
        else:
            key = ("end", e)
        groups.setdefault(key, [e, 0.0])[1] += 1
    return [(g[0], int(g[1])) for g in groups.values()]


def drop_rate(
    late_share: float,
    max_delay_s: float,
    lateness_s: float,
    step: float,
    params: ModelParams = DEFAULT_PARAMS,
) -> float:
    """rho: predicted fraction of events dropped as late (identical for both modes)."""
    if drops_impossible(late_share, max_delay_s, lateness_s, params.ontime_jitter_s):
        return 0.0
    model = Model(late_share, max_delay_s, params)
    reach = model.y + params.window_s  # relevant s-range is [e + L, e + Y + W]
    expected = 0.0
    for e, count in _window_types(window_ends(params), reach, params):
        expected += count * _window_drops(model, e, lateness_s, step)
    return expected / expected_events(params)


# -- P0: event count (section 6) ---------------------------------------------------------------


def event_count_region(params: ModelParams = DEFAULT_PARAMS) -> dict[str, float]:
    """Total events over S seeds is Poisson(S*E[N]); the region is mean +- 4 sd, widened to ints."""
    expected = params.seeds * expected_events(params)
    sd = math.sqrt(expected)
    return {
        "expected": expected,
        "sd": sd,
        "accept_lo": float(math.floor(expected - 4.0 * sd)),
        "accept_hi": float(math.ceil(expected + 4.0 * sd)),
    }


# -- P2 and P4: exactness of the first (revise) and final (wait) answers (sections 4, 4b) ------


def live_ends(lateness_s: float, params: ModelParams = DEFAULT_PARAMS) -> list[float]:
    """Windows whose first record is not produced by the end-of-stream flush."""
    limit = params.duration_s - params.window_s
    return [e for e in window_ends(params) if e <= limit and e + lateness_s < params.duration_s]


def _x_horizon(model: Model, m: float) -> float:
    """Smallest x > m with A(m, x) >= _A_TAIL, found by doubling and then bisection."""
    horizon = model.params.duration_s
    hi_gap = 1.0
    while float(model.A(m, m + hi_gap)) < _A_TAIL and m + hi_gap < horizon:
        hi_gap *= 2.0
    lo_gap = hi_gap / 2.0 if hi_gap > 1.0 else 0.0
    for _ in range(20):
        mid = 0.5 * (lo_gap + hi_gap)
        if float(model.A(m, m + mid)) < _A_TAIL:
            lo_gap = mid
        else:
            hi_gap = mid
    return m + hi_gap


def _window_exact_stats(
    model: Model, e: float, lateness_s: float, x_up: float, step: float
) -> tuple[np.ndarray, float, float]:
    """(pmf of the number of exact zones, E[c], E[c^2]) of the window ending at e, for one seed.

    The trigger time is the first arrival of an event with event time >= m = e + L, with density
    g(x) = a(m, x)*exp(-A(m, x)); given it equals x, each of the Z zones is exact with probability
    c = exp(-mu(e, x)), independently. The pmf over k = 0..Z is the integral of the binomial pmf
    against g. All three outputs are normalised by the quadrature mass of g (1 to ~1e-4), so the
    pmf sums to 1 and its mean is Z*E[c].
    """
    z = model.params.zones
    m = e + lateness_s
    x, w = _midpoints(m, x_up, step)
    g = model.a(m, x) * np.exp(-model.A(m, x))
    mu = model.mu(e, x)
    mass = float(w * np.sum(g))
    ks = np.arange(z + 1)
    binom = np.array([math.comb(z, int(k)) for k in ks], dtype=float)
    # C(Z,k) c^k (1-c)^(Z-k), with 1-c = -expm1(-mu) so that small mu keeps its precision.
    pmf_x = (
        binom[None, :]
        * np.exp(-mu[:, None] * ks[None, :])
        * (-np.expm1(-mu))[:, None] ** (z - ks)[None, :]
    )
    pmf = w * (g[:, None] * pmf_x).sum(axis=0) / mass
    c = np.exp(-mu)
    return pmf, float(w * np.sum(g * c) / mass), float(w * np.sum(g * c * c) / mass)


def _convolve_power(pmf: np.ndarray, n: int) -> np.ndarray:
    """The n-fold convolution of `pmf` with itself, by repeated squaring (positive terms only)."""
    result = np.ones(1)
    base = pmf
    while n:
        if n & 1:
            result = np.convolve(result, base)
        n >>= 1
        if n:
            base = np.convolve(base, base)
    return result


def acceptance_region(pmf: np.ndarray, alpha: float) -> tuple[int, int, float]:
    """(lo, hi, P(X<lo) + P(X>hi)): the largest lo and smallest hi with each tail <= alpha."""
    lower = np.concatenate(([0.0], np.cumsum(pmf)))  # lower[k] = P(X < k), k = 0..N+1
    upper = np.concatenate((np.cumsum(pmf[::-1])[::-1][1:], [0.0]))  # upper[k] = P(X > k)
    lo = int(np.max(np.nonzero(lower <= alpha)[0]))
    hi = int(np.min(np.nonzero(upper <= alpha)[0]))
    return lo, hi, float(lower[lo] + upper[hi])


@dataclass(frozen=True)
class ExactStats:
    """Exactness prediction of one cell: the rate, its sd, and the exact count region."""

    pi: float
    var: float
    n_expected: int
    lo: int
    hi: int
    p_fail: float


def _exact_pmf(
    model: Model, lateness_s: float, step: float
) -> tuple[np.ndarray, float, float, int]:
    """(pmf of the pooled exact count X, pi, Var(pi_hat), live windows) at quadrature step `step`.

    X sums the per-window counts of every live window and seed, which are independent, so its pmf
    is the convolution of the per-window pmfs. Windows at the same burst position share one pmf.
    """
    params = model.params
    ends = live_ends(lateness_s, params)
    horizons = {e: _x_horizon(model, e + lateness_s) for e in ends}
    groups: dict[object, list[float]] = {}
    for e in ends:
        key: object = (
            ("pos", round(e % params.burst_every_s, 6)) if horizons[e] <= params.duration_s else e
        )
        groups.setdefault(key, [e, 0.0])[1] += 1
    z, s = params.zones, params.seeds
    sum_c = sum_var = 0.0
    total = np.ones(1)
    for e, count in ((g[0], int(g[1])) for g in groups.values()):
        pmf, ec, ec2 = _window_exact_stats(model, e, lateness_s, horizons[e], step)
        sum_c += count * ec
        sum_var += count * (z * (ec - ec2) + z * z * (ec2 - ec * ec))
        total = np.convolve(total, _convolve_power(pmf, count * s))
    n_e = len(ends)
    return total, sum_c / n_e, sum_var * s / (s * z * n_e) ** 2, n_e


def _exact_at(model: Model, lateness_s: float, step: float, alpha: float) -> ExactStats:
    total, pi, var, n_e = _exact_pmf(model, lateness_s, step)
    lo, hi, p_fail = acceptance_region(total, alpha)
    return ExactStats(pi, var, model.params.seeds * model.params.zones * n_e, lo, hi, p_fail)


def exact_stats(
    late_share: float,
    max_delay_s: float,
    lateness_s: float,
    step: float,
    params: ModelParams = DEFAULT_PARAMS,
    alpha: float = ALPHA_PER_TAIL,
) -> ExactStats:
    """P4 for wait mode at budget L; at `lateness_s=0` it is P2 (revise's first answer)."""
    return _exact_at(Model(late_share, max_delay_s, params), lateness_s, step, alpha)


def first_exact(
    late_share: float,
    max_delay_s: float,
    step: float,
    params: ModelParams = DEFAULT_PARAMS,
) -> tuple[float, float]:
    """(pi, Var(pi_hat)): revise mode's first-answer exactness over live windows, pooled over S."""
    st = exact_stats(late_share, max_delay_s, 0.0, step, params)
    return st.pi, st.var


# -- P3: first-emit latency (section 5) -------------------------------------------------------


def _latency_at(model: Model, lateness_s: float, step: float) -> tuple[float, float, float]:
    """(p50, p95, SE of the p95) of tau_L - e from the equal-weight mixture over live windows."""
    params = model.params
    ends = np.asarray(live_ends(lateness_s, params))
    m = (ends + lateness_s)[:, None]
    gap = 1.0
    while float(np.min(model.A(m, m + gap))) < _A_TAIL and float(np.max(m)) + gap < (
        params.duration_s
    ):
        gap *= 2.0
    n = math.ceil(gap / step)
    y = lateness_s + step * np.arange(n + 1)
    h_cdf = np.mean(-np.expm1(-model.A(m, ends[:, None] + y[None, :])), axis=0)

    def quantile(q: float) -> float:
        i = int(np.searchsorted(h_cdf, q))
        if i == 0 or i > n:
            raise RuntimeError("quantile outside the latency grid")
        frac = (q - h_cdf[i - 1]) / (h_cdf[i] - h_cdf[i - 1])
        return float(y[i - 1] + frac * step)

    p50, p95 = quantile(_QUANTILE_P50), quantile(_QUANTILE_P95)
    tau = ends + p95
    dens = float(
        np.mean(model.a(ends + lateness_s, tau) * np.exp(-model.A(ends + lateness_s, tau)))
    )
    se95 = math.sqrt(_QUANTILE_P95 * (1.0 - _QUANTILE_P95) / (params.seeds * len(ends))) / dens
    return p50, p95, se95


def first_latency(
    late_share: float,
    max_delay_s: float,
    lateness_s: float,
    step: float,
    params: ModelParams = DEFAULT_PARAMS,
) -> tuple[float, float, float]:
    """(p50, p95, SE95) of first-emit latency. For revise mode pass `lateness_s=0`."""
    return _latency_at(Model(late_share, max_delay_s, params), lateness_s, step)


# -- predictions file ------------------------------------------------------------------------


def _converged_drop_rate(
    p: float, y: float, lateness: float, params: ModelParams, steps: Steps
) -> dict[str, object]:
    if drops_impossible(p, y, lateness, params.ontime_jitter_s):
        return {"rho": 0.0, "rho_coarse": 0.0, "step": 0.0, "exact_zero": True}
    coarse, fine, step = _refine(
        lambda h: (drop_rate(p, y, lateness, h, params),), steps.p1, steps.max_halvings
    )
    return {"rho": fine[0], "rho_coarse": coarse[0], "step": step, "exact_zero": False}


def _exact_row(st: ExactStats, coarse_pi: float, step: float, alpha: float) -> dict[str, object]:
    sd = math.sqrt(st.var)
    return {
        "pi": st.pi,
        "sd": sd,
        "n_expected": st.n_expected,
        "accept_lo": st.lo,
        "accept_hi": st.hi,
        "alpha_per_tail": alpha,
        "p_fail_model": st.p_fail,
        "pi_coarse": coarse_pi,
        "step": step,
    }


def _converged_exact(
    p: float, y: float, lateness: float, params: ModelParams, step0: float, steps: Steps
) -> dict[str, object]:
    """Exactness prediction at the converged step (rate and sd to 0.1 %, count bounds exactly)."""

    def run(h: float) -> tuple[float, ...]:
        st = exact_stats(p, y, lateness, h, params)
        return (st.pi, st.var, float(st.lo), float(st.hi), st.p_fail, float(st.n_expected))

    coarse, fine, step = _refine(run, step0, steps.max_halvings, exact=(2, 3, 5))
    # p_fail is reported, not converged: it is a tail mass and moves with every quadrature digit.
    st = ExactStats(fine[0], fine[1], int(fine[5]), int(fine[2]), int(fine[3]), fine[4])
    return _exact_row(st, coarse[0], step, ALPHA_PER_TAIL)


def _converged_first_exact(
    p: float, y: float, params: ModelParams, steps: Steps
) -> dict[str, object]:
    row = _converged_exact(p, y, 0.0, params, steps.p2, steps)
    row["tolerance"] = 4.0 * float(row["sd"])  # type: ignore[arg-type]
    return row


def _converged_final_exact(
    p: float, y: float, lateness: float, params: ModelParams, steps: Steps
) -> dict[str, object]:
    if drops_impossible(p, y, lateness, params.ontime_jitter_s):
        n = params.seeds * params.zones * len(live_ends(lateness, params))
        return {
            "pi": 1.0,
            "sd": 0.0,
            "n_expected": n,
            "accept_lo": n,
            "accept_hi": n,
            "alpha_per_tail": ALPHA_PER_TAIL,
            "p_fail_model": 0.0,
            "pi_coarse": 1.0,
            "step": 0.0,
            "exact_one": True,
        }
    row = _converged_exact(p, y, lateness, params, steps.p4, steps)
    row["exact_one"] = False
    return row


def _converged_latency(
    p: float, y: float, lateness: float, params: ModelParams, steps: Steps
) -> dict[str, object]:
    coarse, fine, step = _refine(
        lambda h: first_latency(p, y, lateness, h, params), steps.p3, steps.max_halvings
    )
    return {
        "p50": fine[0],
        "p95": fine[1],
        "se95": fine[2],
        "tolerance": max(4.0 * fine[2], 0.05),
        "p95_coarse": coarse[1],
        "step": step,
    }


def build_predictions(
    params: ModelParams = DEFAULT_PARAMS,
    late_shares: Sequence[float] = LATE_SHARES,
    max_delays: Sequence[float] = MAX_DELAYS,
    lateness: Sequence[float] = LATENESS,
    steps: Steps | None = None,
) -> dict[str, object]:
    """All predictions and tolerances for the grid, as the JSON-ready dict of the schema."""
    steps = steps or Steps()
    p1: list[dict[str, object]] = []
    p2: list[dict[str, object]] = []
    p4: list[dict[str, object]] = []
    wait: list[dict[str, object]] = []
    revise: list[dict[str, object]] = []
    # With no late events the max delay plays no part: compute once, copy to every Y row.
    cache: dict[tuple[object, ...], dict[str, object]] = {}

    def cached(kind: str, p: float, y: float, lat: float | None) -> dict[str, object]:
        key = (kind, p, y if p > 0 else None, lat)
        if key not in cache:
            if kind == "p1":
                assert lat is not None
                cache[key] = _converged_drop_rate(p, y, lat, params, steps)
            elif kind == "p2":
                cache[key] = _converged_first_exact(p, y, params, steps)
            elif kind == "p4":
                assert lat is not None
                cache[key] = _converged_final_exact(p, y, lat, params, steps)
            else:
                cache[key] = _converged_latency(p, y, lat or 0.0, params, steps)
        return cache[key]

    for p in late_shares:
        for y in max_delays:
            cell: dict[str, object] = {"late_share": p, "max_delay_s": y}
            for lat in lateness:
                p1.append({**cell, "lateness_s": lat, **cached("p1", p, y, lat)})
            p2.append({**cell, **cached("p2", p, y, None)})
            for lat in lateness:
                p4.append({**cell, "lateness_s": lat, **cached("p4", p, y, lat)})
            for lat in lateness:
                wait.append(
                    {"mode": "wait", **cell, "lateness_s": lat, **cached("wait", p, y, lat)}
                )
            revise.append(
                {"mode": "revise", **cell, "lateness_s": None, **cached("revise", p, y, 0.0)}
            )
    return {
        "model": "ANALYTIC.md",
        "params": asdict(params),
        "tolerance_rules": dict(TOLERANCE_RULES),
        "p0_events": event_count_region(params),
        "p1_drop_rate": p1,
        "p2_first_exact": p2,
        "p3_first_latency": wait + revise,
        "p4_final_exact": p4,
    }


def write_predictions(path: Path | str, **kwargs: object) -> dict[str, object]:
    """Write the predictions as deterministic JSON (sorted keys, no timestamps)."""
    preds = build_predictions(**kwargs)  # type: ignore[arg-type]
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(preds, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
    )
    return preds
