# Analytic model of the window core

Predictions for the sweep, derived from the frozen generator (`EVAL_PLAN.md` → Generator) and the
windowing semantics (`GUIDELINES.md`) only. No simulation output was used to build or tune this
model. `src/streampipe/analytic.py` implements these formulas independently of the core: it does not
import `windows`, `sim` or `events.generate`.

## 1. The generator as a marked Poisson process

- **Event times.** Poisson process on `[0, T)`, `T = 3600`, with piecewise-constant rate
  `λ(s) = r·b` if `(s mod P) < B`, else `r`, and `λ(s) = 0` outside `[0, T)`.
  Here `r = 5/s`, `b = 4`, `P = 600 s`, `B = 30 s`. Expected events per run:
  `E[N] = r·(T + (b − 1)·B·T/P) = 20,700`.
- **Zones.** Each event gets one of `Z = 6` zones, uniformly and independently.
- **Delays.** Each event gets an independent delay `d`: with probability `1 − p`, `d ~ U(0, J)`; with
  probability `p`, `d ~ U(J, Y)`. Here `J = 2 s`, `p` is the late share and `Y` the max delay. CDF:

  | `x` | `F(x)` |
  |---|---|
  | `x ≤ 0` | `0` |
  | `0 ≤ x ≤ J` | `(1 − p)·x / J` |
  | `J ≤ x ≤ Y` | `(1 − p) + p·(x − J) / (Y − J)` |
  | `x ≥ Y` | `1` |

  For `p = 0`, `F(x) = min(x/J, 1)`, and `Y` plays no part.
- **Arrival** `τ = t + d`. Marks (zone, delay, amount) are independent of each other and of the
  event times. So any subset of events picked by a rule on `(t, d, zone)` is again a Poisson
  process (thinning), and disjoint subsets are independent.

Integrated CDF `G(x) = ∫₀ˣ F(y) dy`, closed form:

| `x` | `G(x)` |
|---|---|
| `x ≤ 0` | `0` |
| `0 ≤ x ≤ J` | `(1 − p)·x² / (2J)` |
| `J ≤ x ≤ Y` | `(1 − p)·J/2 + (1 − p)·(x − J) + p·(x − J)² / (2(Y − J))` |
| `x ≥ Y` | `G(Y) + (x − Y)` |

## 2. The watermark `M` at an arrival

`M` before the event that arrives at `τ` is the largest event time among events that arrived
earlier. `M ≥ m` iff at least one event with event time in `[m, τ]` has arrived by `τ`. The number
of such events is Poisson with mean

```
A(m, τ) = ∫_m^τ λ(s)·F(τ − s) ds  =  Σ_segments λ_k·[ G(τ − a_k) − G(τ − b_k) ]
```

where the sum runs over the constant-rate segments `[a_k, b_k)` of `λ` clipped to `[m, τ]`. Hence

```
P(M(τ) ≥ m) = 1 − exp(−A(m, τ))        (m ≤ τ; 0 if m > τ)
```

The event arriving at `τ` is not in the "before" set. By the Palm property of the Poisson process,
the other events are still Poisson given that one, so this holds conditionally on the event itself.

## 3. P1: late-drop rate (both modes; mode does not matter)

An event with event time `t` lies in the window ending at `e(t) = W·(⌊t/W⌋ + 1)`, `W = 60`. It is
dropped iff `M(τ) ≥ e + L` at its arrival `τ = t + d`. That needs `τ > e + L`, so

```
P_drop(t, d) = 1 − exp(−A(e(t) + L, t + d))   if t + d > e(t) + L,   else 0
E[D] = ∫_0^T λ(t) ∫ f(d)·P_drop(t, d) dd dt
predicted late_dropped_rate  ρ = E[D] / E[N]
```

Exact zeros: since `t < e`, a drop needs `d > L`. So `ρ = 0` exactly when `L ≥ Y` (late share > 0),
or when `L ≥ J` with `p = 0`.

## 4. P2: revise mode, first answer exact

Take window `(z, e)` with `e ≤ T − W`; the last window per zone (`e = T`) is never due and is
excluded, as in `exact_rate_first_live`. Its first record is emitted while processing the first
arrival of an event with event time `≥ e` (any zone), at stream time `τ*`:

```
P(τ* > x) = exp(−A(e, x)),     density  g(x) = a(e, x)·exp(−A(e, x)),
a(e, x) = ∂A/∂x = Σ_segments λ_k·[ F(x − a_k) − F(x − b_k) ]      (segments clipped to [e, x])
```

The first record holds exactly the window's events that arrived by `τ*`. It equals the batch value
iff none of the window's events arrives after `τ*`. The window's events (zone `z`, event time in
`[e − W, e)`) are independent of `τ*`, because their event times are disjoint from `[e, ∞)`. The
number of them arriving after `x` is Poisson with mean

```
μ(e, x) = (1/Z)·∫_{e−W}^{e} λ(t)·(1 − F(x − t)) dt
        = (1/Z)·Σ_segments λ_k·[ (b_k − a_k) − (G(x − a_k) − G(x − b_k)) ]      (segments clipped to [e − W, e))
```

So, conditional on `τ* = x`, the window is exact with probability `c(e, x) = exp(−μ(e, x))`,
independently across zones. The prediction is

```
q(e) = ∫_e^∞ c(e, x)·g(x) dx,       predicted exact_rate_first_live  π = mean of q(e) over e = W, 2W, …, T − W
```

This does not depend on `L`. Revise's first record goes out when the window is due, whatever the
budget.

Ignored, each with probability below `10⁻⁶`: a window with no events (`exp(−50)`); a window
none of whose events has arrived by `τ*`; exact ties in arrival time.

## 5. P3: first-emit latency

Wait mode emits a window's first (and only) record while processing the first arrival of an event
with event time `≥ e + L`. Revise mode does the same with `L = 0`. With `τ_L` that arrival:

```
P(τ_L − e ≤ y) = 1 − exp(−A(e + L, e + y))        for y ≥ L
```

All `Z` zones of a window share `τ_L`. Live windows are those closed before the end of the stream:
`e + L < T`, with `e ≤ T − W`. For the grid's `L` values that means `e + L ≤ T − 30`, and
`P(M_final < T − 30)` is below `10⁻⁶⁰`. The pooled latency distribution is the equal-weight
mixture `H(y) = mean over live e of P(τ_L − e ≤ y)`. The prediction is the 95th percentile
`H⁻¹(0.95)`, and likewise the 50th.

## 6. Tolerances (fixed with the predictions, before the sweep)

The observed values pool 5 seeds. Every tolerance is a model-derived spread at 4 standard
deviations. None was chosen by looking at data.

- **P1, drops, per (late share, max delay, L) cell.** `D` = observed dropped events over 5 seeds,
  `N` = observed events, expected drops `Ê = ρ·N`. Pass iff `|D − Ê| ≤ 4·√(2·Ê) + 2`. The factor 2
  allows up to twice the Poisson variance for drops that share the same `M` path. The `+2` covers
  cells whose prediction is exactly zero.
- **P2, first-answer exactness, per (late share, max delay) cell**, in revise mode at every `L`
  (identical across `L` by construction). Over `S = 5` seeds and the live windows, given `τ*`, the
  `Z` zones are independent Bernoulli(`c`), so

  ```
  Var(π̂) = [ Σ_e ( Z·E[c(1 − c)] + Z²·Var(c) ) ] · S / (S·Z·n_e)²
  ```

  with `E[c]`, `E[c²]` integrated against `g` as in §4. Windows are treated as independent of each
  other. Pass iff `|π̂ − π| ≤ 4·√Var(π̂)`.
- **P3, first-emit latency p95, per cell**: wait at every `L`, and revise at each (late share, max
  delay). Nearest-rank p95 over `n = S·Z·n_live` values, but only `S·n_live` of them are
  independent, because zones share `τ_L`. Asymptotic standard error
  `√(0.95·0.05 / (S·n_live)) / h(H⁻¹(0.95))`, with `h = H′` the mixture density. Pass iff
  `|p95̂ − p95| ≤ max(4·SE, 0.05 s)`. The 0.05 s floor covers rounding to the millisecond and the
  nearest-rank step.

## 7. Numerical method

All `A`, `a`, `μ` and `G` terms are closed form. Two integrals are numerical:

- the double integral for `E[D]`, on a midpoint grid in `t` within each window and in `d` within
  each delay component;
- the one-dimensional integrals over `x` (P2) and `y` (P3), on a midpoint grid with an upper limit
  where `exp(−A) < 10⁻¹²`.

Windows with the same position in the 600 s burst period and the same distance from `T` give
identical integrals, so each distinct window type is computed once. Every grid is refined until
halving the step changes each prediction by less than 0.1 % (relative) or `10⁻⁶` (absolute). The
two step sizes and both values are written to `results/predictions.json`.

## 8. What the prediction does and does not test

It tests whether the implemented core (drop rule, due/close thresholds, first-emit timing), the
generator, and the metrics behave as the stated semantics imply. A mismatch beyond tolerance means
one of them, or this derivation, is wrong. It does not test anything about real order streams: the
model and the simulator share the same assumed generator.
