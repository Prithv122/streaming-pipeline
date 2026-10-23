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

Ignored, each with probability below `10⁻⁶`: a window with no events (`exp(−50)`), and a
window none of whose events has arrived by `τ*`.

**Ties are not negligible in frequency, only in effect.** Arrival times are rounded to the
millisecond, so about 0.5–2 % of events share a millisecond with another event. The log then
orders them by `order_id`, which processes the earlier event time first. A tie matters only when it
pairs the event that moves `M` past a threshold with a window event or a drop candidate, about
`10⁻³` per window. It always favours "included" and "not dropped". Bound (second pre-run review):
at most about 0.1 % relative on P1 (about 0.1 drops per run), `5·10⁻⁴` absolute on P2 and P4, and
0.5 ms on P3. That is far inside every tolerance. Rounding event times to the millisecond is
likewise negligible.

## 4b. P4: wait mode, final answer exact

In wait mode, a window's events that arrive after `σ` (the first arrival of an event with event
time `≥ e + L`) are dropped, and every earlier one is accepted. So the final record is exact iff no
window event arrives after `σ`. This is §4 with `A(e + L, ·)` in place of `A(e, ·)`:

```
q4(e) = ∫_{e+L}^∞ exp(−μ(e, x))·a(e + L, x)·exp(−A(e + L, x)) dx,
predicted exact_rate_final_live  π4 = mean of q4(e) over live windows (e ≤ T − W, e + L < T)
```

At `L = 0` this is the same quantity as P2. When `L ≥ Y` (or `p = 0` and `L ≥ J`), no window
event can arrive after `σ`, so `π4 = 1` exactly. Revise mode accepts the same events, so its
final exactness is the same, and that is checked as an invariant.

## 5. P3: first-emit latency

Wait mode emits a window's first (and only) record while processing the first arrival of an event
with event time `≥ e + L`. Revise mode does the same with `L = 0`. With `τ_L` that arrival:

```
P(τ_L − e ≤ y) = 1 − exp(−A(e + L, e + y))        for y ≥ L
```

All `Z` zones of a window share `τ_L`. Live windows are those closed before the end of the stream:
`e + L < T`, with `e ≤ T − W`. That is 59 windows for `L ≤ 30` and 58 for `L = 60` (the window
ending at 3540 would close at `M ≥ 3600 = T`, which never happens, so it is flushed). For every
live window `e + L ≤ T − 30`, and `P(M_final < T − 30)` is below `10⁻⁶⁰`. The pooled latency distribution is the equal-weight
mixture `H(y) = mean over live e of P(τ_L − e ≤ y)`. The prediction is the 95th percentile
`H⁻¹(0.95)`, and likewise the 50th.

## 6. Tolerances (fixed with the predictions, before the sweep)

The observed values pool 5 seeds. Every acceptance rule comes from the model at the level of a
two-sided normal ±4σ check (`α = 3.167·10⁻⁵` per tail). None was chosen by looking at data. The
second pre-run review replaced P2's normal rule with exact count regions, and added P0 and P4
(see `EVAL_PLAN.md` A1.6).

- **P0, event count.** Total events over the 5 seeds is Poisson(`S·E[N]` = 103,500). Pass iff it
  lies within `103,500 ± 4·√103,500` (rounded outward). Event times do not depend on the cell, so
  this is one test repeated in every (late share, max delay) cell. It catches rate and burst bugs.
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
  other. This `sd` is reported, but **the pass rule uses the exact count distribution instead**.
  The count of exact window-zones `X` is the convolution, over all live windows and seeds, of the
  mixture pmf `P_e(k) = ∫ C(Z,k)·c^k·(1 − c)^(Z−k)·g(x) dx`. Pass iff the observed denominator
  equals `n_expected = S·Z·n_live` and `X` lies in `[lo, hi]`, the tightest region with at most `α`
  of the model's mass below and above. A normal rule was wrong at (20 %, 120 s): with `π = 0.00037`
  the expected count is 0.66, and "±4σ" would have failed 0.46 % of the time when the model is right.
- **P4, wait-mode final exactness, per (late share, max delay, L) cell.** The same exact-region
  rule, using §4b's mixture. Cells with `π4 = 1` exactly must have every live window exact.
- **P3, first-emit latency p95, per cell**: wait at every `L`, and revise at each (late share, max
  delay). Nearest-rank p95 over `n = S·Z·n_live` values, but only `S·n_live` of them are
  independent, because zones share `τ_L`. Asymptotic standard error
  `√(0.95·0.05 / (S·n_live)) / h(H⁻¹(0.95))`, with `h = H′` the mixture density. Pass iff
  `|p95̂ − p95| ≤ max(4·SE, 0.05 s)`. The 0.05 s floor covers rounding to the millisecond and the
  nearest-rank step. Latency is right-skewed, so the exact order-statistic tail is heavier than
  normal: about `1.5·10⁻⁴` false-failure per cell, about 2.4× nominal. This was accepted.
- **Invariants (no tolerance).** Revise and wait must have identical drop counts, and identical
  live final-exact counts and denominators, in every cell.

**How much independent evidence the checks carry.** For a seed, the event times, zones and amounts
are the same in every cell. The late flags are the same for a given late share, and the on-time
delays are the same in every cell. As a result:
- rows with `p = 0` are identical across max delay;
- P3 barely changes across max delay when `p > 0`;
- P2 and P3-revise repeat across `L`, and P3-revise equals P3-wait at `L = 0`;
- P4 at `L = 0` equals P2;
- exact-zero P1 cells and `π4 = 1` cells are deterministic;
- invariants are deterministic.

Of the roughly 300 mechanical checks, about 45 carry independent statistical content.

**False-failure probability if the model is exactly right**, from the second pre-run review's
exact calculations:

| Family | Probability |
|---|---|
| P1 | about `10⁻⁵` |
| P2 and P4 with exact regions | `1.25·10⁻³` summed over the 42 distinct cells (`p_fail_model` in the predictions file) |
| P3 | about 0.2–0.5 % in total |
| Overall | about 0.35–0.6 % |

The claim's falsifiability rests on the other side: plausible core, generator or metric bugs shift a
family well past its region. Examples are a per-zone `M`, a revise first record sent at close, latency
measured from the window start, a burst rate inverted, or late delays drawn from 0.

## 7. Numerical method

All `A`, `a`, `μ` and `G` terms are closed form. Two integrals are numerical:

- the double integral for `E[D]`, on a midpoint grid in `t` within each window and in `d` within
  each delay component;
- the one-dimensional integrals over `x` (P2, P4) and `y` (P3), on a midpoint grid with an upper limit
  where `exp(−A) < 10⁻¹²`. The count regions convolve the per-window pmfs exactly.

Windows with the same position in the 600 s burst period and the same distance from `T` give
identical integrals, so each distinct window type is computed once. Every grid is refined until
halving the step changes each prediction by less than 0.1 % (relative) or `10⁻⁶` (absolute). The
two step sizes and both values are written to `results/predictions.json`.

## 8. What the prediction does and does not test

It tests whether the implemented core (drop rule, due/close thresholds, first-emit timing), the
generator, and the metrics behave as the stated semantics imply. A mismatch beyond tolerance means
one of them, or this derivation, is wrong. It does not test anything about real order streams: the
model and the simulator share the same assumed generator.
