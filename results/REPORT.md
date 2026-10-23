# Sweep report

Every value below is read from the committed result files.

## 1. Provenance

- grid: full
- registered grid: yes
- n_runs: 450
- seeds (5): 0, 1, 2, 3, 4
- duration_s per run: 3600
- late shares: 0%, 5%, 20%
- max delays (s): 5, 30, 120
- lateness L (s): 0, 5, 15, 30, 60
- modes: wait, revise
- predictions sha256 (in sweep_claim.json): 8c134c4942a7c085a7b9be6a615f4b06b052f85c27ae3e346e246dd56bceb2f4
- predictions sha256 (computed from predictions.json): 8c134c4942a7c085a7b9be6a615f4b06b052f85c27ae3e346e246dd56bceb2f4
- predictions sha256 (pinned in predictions.sha256): 8c134c4942a7c085a7b9be6a615f4b06b052f85c27ae3e346e246dd56bceb2f4
- claim sha256 equals pinned sha256: yes
- computed sha256 equals pinned sha256: yes

## 2. Claims

### C0 construction check

Cell: mode=wait, L=30s, late share=5%, max delay=30s (present in the pooled table: yes).

| metric | value | comparison | threshold | passed |
|---|---|---|---|---|
| exact_rate_final | 1 | >= | 0.99 | yes |
| first_emit_latency_p95 | 31.622 | <= | 90 | yes |

C0 passed: yes

### Primary claim

| family | n_checks | n_passed | n_failed |
|---|---|---|---|
| p0 | 9 | 9 | 0 |
| p1 | 45 | 45 | 0 |
| mode_invariant | 135 | 135 | 0 |
| p2 | 45 | 45 | 0 |
| p3_wait | 45 | 45 | 0 |
| p3_revise | 45 | 45 | 0 |
| p4 | 45 | 45 | 0 |
| all | 369 | 369 | 0 |

primary_passed: yes
Checks in sweep_checks.csv: 369; failed in sweep_checks.csv: 0; failed in sweep_claim.json: 0.

Failed checks:

none

## 3. Predicted vs observed

### P1: late-drop counts (cells with rho > 0)

| late share | max delay s | L s | rho | observed D | expected rho*N | tolerance | diff | passed |
|---|---|---|---|---|---|---|---|---|
| 0% | 5 | 0 | 0.00609976 | 614 | 630.794 | 144.075 | 16.7943 | yes |
| 0% | 30 | 0 | 0.00609976 | 614 | 630.794 | 144.075 | 16.7943 | yes |
| 0% | 120 | 0 | 0.00609976 | 614 | 630.794 | 144.075 | 16.7943 | yes |
| 5% | 5 | 0 | 0.00759975 | 767 | 785.912 | 160.585 | 18.9124 | yes |
| 5% | 30 | 0 | 0.0165027 | 1648 | 1706.6 | 235.69 | 58.5988 | yes |
| 5% | 30 | 5 | 0.00746688 | 732 | 772.173 | 159.193 | 40.1729 | yes |
| 5% | 30 | 15 | 0.00257449 | 237 | 266.236 | 94.3014 | 29.2357 | yes |
| 5% | 120 | 0 | 0.0420784 | 4332 | 4351.46 | 375.158 | 19.458 | yes |
| 5% | 120 | 5 | 0.0343396 | 3539 | 3551.16 | 339.101 | 12.1559 | yes |
| 5% | 120 | 15 | 0.0301633 | 3082 | 3119.28 | 317.938 | 37.2761 | yes |
| 5% | 120 | 30 | 0.0238866 | 2419 | 2470.18 | 283.151 | 51.1813 | yes |
| 5% | 120 | 60 | 0.0111949 | 1105 | 1157.7 | 194.475 | 52.7028 | yes |
| 20% | 5 | 0 | 0.0119429 | 1245 | 1235.05 | 200.8 | 9.95275 | yes |
| 20% | 30 | 0 | 0.0475694 | 4885 | 4919.29 | 398.759 | 34.2912 | yes |
| 20% | 30 | 5 | 0.0296967 | 3006 | 3071.02 | 315.485 | 65.0218 | yes |
| 20% | 30 | 15 | 0.0101985 | 1011 | 1054.66 | 185.709 | 43.6616 | yes |
| 20% | 120 | 0 | 0.149942 | 15471 | 15506 | 706.409 | 34.9957 | yes |
| 20% | 120 | 5 | 0.137239 | 14151 | 14192.3 | 675.91 | 41.3168 | yes |
| 20% | 120 | 15 | 0.120535 | 12362 | 12464.8 | 633.566 | 102.849 | yes |
| 20% | 120 | 30 | 0.0954239 | 9767 | 9868.07 | 563.942 | 101.072 | yes |
| 20% | 120 | 60 | 0.0446661 | 4571 | 4619.06 | 386.46 | 48.0572 | yes |

Cells with rho = 0: 24; observed D = 0 in 24 of them; checks passed: 24 of 24.

### P2: revise first-answer exact count, per (late share, max delay)

The check repeats at every L; each row stands for the L values counted in the last column. Where the observed counts differ across L the row shows `lo to hi`.

| late share | max delay s | n windows | observed count | region [lo, hi] | pi | observed rate | passed (L values) |
|---|---|---|---|---|---|---|---|
| 0% | 5 | 1770 | 1290 | [1170, 1352] | 0.71340 | 0.72881 | 5/5 |
| 0% | 30 | 1770 | 1290 | [1170, 1352] | 0.71340 | 0.72881 | 5/5 |
| 0% | 120 | 1770 | 1290 | [1170, 1352] | 0.71340 | 0.72881 | 5/5 |
| 5% | 5 | 1770 | 1181 | [1065, 1251] | 0.65467 | 0.66723 | 5/5 |
| 5% | 30 | 1770 | 706 | [602, 776] | 0.38889 | 0.39887 | 5/5 |
| 5% | 120 | 1770 | 175 | [129, 230] | 0.10024 | 0.09887 | 5/5 |
| 20% | 5 | 1770 | 917 | [810, 1000] | 0.51135 | 0.51808 | 5/5 |
| 20% | 30 | 1770 | 116 | [74, 156] | 0.06368 | 0.06554 | 5/5 |
| 20% | 120 | 1770 | 0 | [0, 6] | 0.00037 | 0.00000 | 5/5 |

### P4: wait final-exact count (cells with pi4 < 1)

| late share | max delay s | L s | n windows | observed count | region [lo, hi] | pi4 | observed rate | passed |
|---|---|---|---|---|---|---|---|---|
| 0% | 5 | 0 | 1770 | 1290 | [1170, 1352] | 0.71340 | 0.72881 | yes |
| 0% | 30 | 0 | 1770 | 1290 | [1170, 1352] | 0.71340 | 0.72881 | yes |
| 0% | 120 | 0 | 1770 | 1290 | [1170, 1352] | 0.71340 | 0.72881 | yes |
| 5% | 5 | 0 | 1770 | 1181 | [1065, 1251] | 0.65467 | 0.66723 | yes |
| 5% | 30 | 0 | 1770 | 706 | [602, 776] | 0.38889 | 0.39887 | yes |
| 5% | 30 | 5 | 1770 | 1179 | [1063, 1224] | 0.64628 | 0.66610 | yes |
| 5% | 30 | 15 | 1770 | 1550 | [1462, 1579] | 0.86022 | 0.87571 | yes |
| 5% | 120 | 0 | 1770 | 175 | [129, 230] | 0.10024 | 0.09887 | yes |
| 5% | 120 | 5 | 1770 | 258 | [212, 332] | 0.15272 | 0.14576 | yes |
| 5% | 120 | 15 | 1770 | 338 | [272, 402] | 0.18964 | 0.19096 | yes |
| 5% | 120 | 30 | 1770 | 479 | [394, 541] | 0.26349 | 0.27062 | yes |
| 5% | 120 | 60 | 1740 | 944 | [819, 984] | 0.51814 | 0.54253 | yes |
| 20% | 5 | 0 | 1770 | 917 | [810, 1000] | 0.51135 | 0.51808 | yes |
| 20% | 30 | 0 | 1770 | 116 | [74, 156] | 0.06368 | 0.06554 | yes |
| 20% | 30 | 5 | 1770 | 309 | [250, 378] | 0.17652 | 0.17458 | yes |
| 20% | 30 | 15 | 1770 | 1000 | [891, 1059] | 0.55108 | 0.56497 | yes |
| 20% | 120 | 0 | 1770 | 0 | [0, 6] | 0.00037 | 0.00000 | yes |
| 20% | 120 | 5 | 1770 | 0 | [0, 8] | 0.00072 | 0.00000 | yes |
| 20% | 120 | 15 | 1770 | 3 | [0, 12] | 0.00168 | 0.00169 | yes |
| 20% | 120 | 30 | 1770 | 7 | [1, 26] | 0.00600 | 0.00395 | yes |
| 20% | 120 | 60 | 1740 | 145 | [92, 181] | 0.07724 | 0.08333 | yes |

Cells with pi4 = 1: 24; checks passed: 24 of 24.

### P3: first-emit latency p95, wait mode (seconds)

| late share | max delay s | L s | observed | predicted | tolerance | diff | passed |
|---|---|---|---|---|---|---|---|
| 0% | 5 | 0 | 1.501 | 1.52504 | 0.266247 | 0.0240435 | yes |
| 0% | 5 | 5 | 6.476 | 6.52504 | 0.266247 | 0.0490435 | yes |
| 0% | 5 | 15 | 16.522 | 16.525 | 0.266247 | 0.00304353 | yes |
| 0% | 5 | 30 | 31.614 | 31.5482 | 0.262354 | 0.0658392 | yes |
| 0% | 5 | 60 | 61.501 | 61.5246 | 0.268612 | 0.0236308 | yes |
| 0% | 30 | 0 | 1.501 | 1.52504 | 0.266247 | 0.0240435 | yes |
| 0% | 30 | 5 | 6.476 | 6.52504 | 0.266247 | 0.0490435 | yes |
| 0% | 30 | 15 | 16.522 | 16.525 | 0.266247 | 0.00304353 | yes |
| 0% | 30 | 30 | 31.614 | 31.5482 | 0.262354 | 0.0658392 | yes |
| 0% | 30 | 60 | 61.501 | 61.5246 | 0.268612 | 0.0236308 | yes |
| 0% | 120 | 0 | 1.501 | 1.52504 | 0.266247 | 0.0240435 | yes |
| 0% | 120 | 5 | 6.476 | 6.52504 | 0.266247 | 0.0490435 | yes |
| 0% | 120 | 15 | 16.522 | 16.525 | 0.266247 | 0.00304353 | yes |
| 0% | 120 | 30 | 31.614 | 31.5482 | 0.262354 | 0.0658392 | yes |
| 0% | 120 | 60 | 61.501 | 61.5246 | 0.268612 | 0.0236308 | yes |
| 5% | 5 | 0 | 1.522 | 1.5647 | 0.273192 | 0.0426954 | yes |
| 5% | 5 | 5 | 6.507 | 6.5647 | 0.273192 | 0.0576954 | yes |
| 5% | 5 | 15 | 16.578 | 16.5647 | 0.273192 | 0.0133046 | yes |
| 5% | 5 | 30 | 31.622 | 31.5883 | 0.269121 | 0.0336771 | yes |
| 5% | 5 | 60 | 61.522 | 61.5643 | 0.275608 | 0.0422598 | yes |
| 5% | 30 | 0 | 1.522 | 1.5647 | 0.273192 | 0.0426954 | yes |
| 5% | 30 | 5 | 6.507 | 6.5647 | 0.273192 | 0.0576954 | yes |
| 5% | 30 | 15 | 16.578 | 16.5647 | 0.273192 | 0.0133046 | yes |
| 5% | 30 | 30 | 31.622 | 31.5883 | 0.269121 | 0.0336771 | yes |
| 5% | 30 | 60 | 61.522 | 61.5643 | 0.275608 | 0.0422598 | yes |
| 5% | 120 | 0 | 1.522 | 1.5647 | 0.273192 | 0.0426954 | yes |
| 5% | 120 | 5 | 6.507 | 6.5647 | 0.273192 | 0.0576954 | yes |
| 5% | 120 | 15 | 16.578 | 16.5647 | 0.273192 | 0.0133046 | yes |
| 5% | 120 | 30 | 31.622 | 31.5883 | 0.269121 | 0.0336771 | yes |
| 5% | 120 | 60 | 61.522 | 61.5643 | 0.275608 | 0.0422598 | yes |
| 20% | 5 | 0 | 1.756 | 1.70519 | 0.297794 | 0.0508066 | yes |
| 20% | 5 | 5 | 6.659 | 6.70519 | 0.297794 | 0.0461934 | yes |
| 20% | 5 | 15 | 16.661 | 16.7052 | 0.297794 | 0.0441934 | yes |
| 20% | 5 | 30 | 31.782 | 31.7309 | 0.293302 | 0.0511255 | yes |
| 20% | 5 | 60 | 61.756 | 61.7047 | 0.300423 | 0.0512853 | yes |
| 20% | 30 | 0 | 1.756 | 1.70519 | 0.297794 | 0.0508066 | yes |
| 20% | 30 | 5 | 6.659 | 6.70519 | 0.297794 | 0.0461934 | yes |
| 20% | 30 | 15 | 16.661 | 16.7052 | 0.297794 | 0.0441934 | yes |
| 20% | 30 | 30 | 31.782 | 31.7309 | 0.293302 | 0.0511255 | yes |
| 20% | 30 | 60 | 61.756 | 61.7047 | 0.300423 | 0.0512853 | yes |
| 20% | 120 | 0 | 1.756 | 1.70519 | 0.297794 | 0.0508066 | yes |
| 20% | 120 | 5 | 6.659 | 6.70519 | 0.297794 | 0.0461934 | yes |
| 20% | 120 | 15 | 16.661 | 16.7052 | 0.297794 | 0.0441934 | yes |
| 20% | 120 | 30 | 31.782 | 31.7309 | 0.293302 | 0.0511255 | yes |
| 20% | 120 | 60 | 61.756 | 61.7047 | 0.300423 | 0.0512853 | yes |

### P3: first-emit latency p95, revise mode, per (late share, max delay) (seconds)

The check repeats at every L (5 L values per row). Where values differ across L the row shows `lo to hi`.

| late share | max delay s | observed | predicted | tolerance | diff | passed (L values) |
|---|---|---|---|---|---|---|
| 0% | 5 | 1.501 | 1.52504 | 0.266247 | 0.0240435 | 5/5 |
| 0% | 30 | 1.501 | 1.52504 | 0.266247 | 0.0240435 | 5/5 |
| 0% | 120 | 1.501 | 1.52504 | 0.266247 | 0.0240435 | 5/5 |
| 5% | 5 | 1.522 | 1.5647 | 0.273192 | 0.0426954 | 5/5 |
| 5% | 30 | 1.522 | 1.5647 | 0.273192 | 0.0426954 | 5/5 |
| 5% | 120 | 1.522 | 1.5647 | 0.273192 | 0.0426954 | 5/5 |
| 20% | 5 | 1.756 | 1.70519 | 0.297794 | 0.0508066 | 5/5 |
| 20% | 30 | 1.756 | 1.70519 | 0.297794 | 0.0508066 | 5/5 |
| 20% | 120 | 1.756 | 1.70519 | 0.297794 | 0.0508066 | 5/5 |

## 4. Wait vs revise at equal L

Pooled over seeds. Latencies are stream seconds. Live metrics exclude windows closed by the end-of-stream flush. `records_emitted per run` is the mean per run.

### late share 0%

Shown for max delay 5s (seeds pooled per cell: 5).

Rows identical across max delay (5, 30, 120s): yes.

| L s | first-emit p95 s wait | first-emit p95 s revise | final p95 s wait | final p95 s revise | exact_rate_first_live wait | exact_rate_first_live revise | exact_rate_final_live | late_dropped_rate | mean_abs_count_error_live | value_revisions_per_window (revise) | records_emitted per run wait | records_emitted per run revise |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.5 | 1.5 | 1.5 | 1.5 | 72.9% | 72.9% | 72.9% | 0.6% | 0.35 | 0.00 | 360.00 | 360.00 |
| 5 | 6.5 | 1.5 | 6.5 | 6.5 | 100.0% | 72.9% | 100.0% | 0.0% | 0.00 | 0.34 | 360.00 | 836.80 |
| 15 | 16.5 | 1.5 | 16.5 | 16.5 | 100.0% | 72.9% | 100.0% | 0.0% | 0.00 | 0.34 | 360.00 | 836.80 |
| 30 | 31.6 | 1.5 | 31.6 | 31.6 | 100.0% | 72.9% | 100.0% | 0.0% | 0.00 | 0.34 | 360.00 | 836.80 |
| 60 | 61.5 | 1.5 | 61.5 | 61.5 | 100.0% | 72.9% | 100.0% | 0.0% | 0.00 | 0.34 | 360.00 | 836.80 |

### late share 5%, max delay 5s

Seeds pooled per cell: 5.

| L s | first-emit p95 s wait | first-emit p95 s revise | final p95 s wait | final p95 s revise | exact_rate_first_live wait | exact_rate_first_live revise | exact_rate_final_live | late_dropped_rate | mean_abs_count_error_live | value_revisions_per_window (revise) | records_emitted per run wait | records_emitted per run revise |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.5 | 1.5 | 1.5 | 1.5 | 66.7% | 66.7% | 66.7% | 0.7% | 0.43 | 0.00 | 360.00 | 360.00 |
| 5 | 6.5 | 1.5 | 6.5 | 6.5 | 100.0% | 66.7% | 100.0% | 0.0% | 0.00 | 0.43 | 360.00 | 867.40 |
| 15 | 16.6 | 1.5 | 16.6 | 16.6 | 100.0% | 66.7% | 100.0% | 0.0% | 0.00 | 0.43 | 360.00 | 867.40 |
| 30 | 31.6 | 1.5 | 31.6 | 31.6 | 100.0% | 66.7% | 100.0% | 0.0% | 0.00 | 0.43 | 360.00 | 867.40 |
| 60 | 61.5 | 1.5 | 61.5 | 61.5 | 100.0% | 66.7% | 100.0% | 0.0% | 0.00 | 0.43 | 360.00 | 867.40 |

### late share 5%, max delay 30s

Seeds pooled per cell: 5.

| L s | first-emit p95 s wait | first-emit p95 s revise | final p95 s wait | final p95 s revise | exact_rate_first_live wait | exact_rate_first_live revise | exact_rate_final_live | late_dropped_rate | mean_abs_count_error_live | value_revisions_per_window (revise) | records_emitted per run wait | records_emitted per run revise |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.5 | 1.5 | 1.5 | 1.5 | 39.9% | 39.9% | 39.9% | 1.6% | 0.93 | 0.00 | 360.00 | 360.00 |
| 5 | 6.5 | 1.5 | 6.5 | 6.5 | 66.6% | 39.9% | 66.6% | 0.7% | 0.41 | 0.51 | 360.00 | 897.20 |
| 15 | 16.6 | 1.5 | 16.6 | 16.6 | 87.6% | 39.9% | 87.6% | 0.2% | 0.13 | 0.78 | 360.00 | 996.20 |
| 30 | 31.6 | 1.5 | 31.6 | 31.6 | 100.0% | 39.9% | 100.0% | 0.0% | 0.00 | 0.92 | 360.00 | 1043.60 |
| 60 | 61.5 | 1.5 | 61.5 | 61.5 | 100.0% | 39.9% | 100.0% | 0.0% | 0.00 | 0.92 | 360.00 | 1043.60 |

### late share 5%, max delay 120s

Seeds pooled per cell: 5.

| L s | first-emit p95 s wait | first-emit p95 s revise | final p95 s wait | final p95 s revise | exact_rate_first_live wait | exact_rate_first_live revise | exact_rate_final_live | late_dropped_rate | mean_abs_count_error_live | value_revisions_per_window (revise) | records_emitted per run wait | records_emitted per run revise |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.5 | 1.5 | 1.5 | 1.5 | 9.9% | 9.9% | 9.9% | 4.2% | 2.45 | 0.00 | 360.00 | 360.00 |
| 5 | 6.5 | 1.5 | 6.5 | 6.5 | 14.6% | 9.9% | 14.6% | 3.4% | 2.00 | 0.44 | 360.00 | 872.60 |
| 15 | 16.6 | 1.5 | 16.6 | 16.6 | 19.1% | 9.9% | 19.1% | 3.0% | 1.74 | 0.69 | 360.00 | 964.00 |
| 30 | 31.6 | 1.5 | 31.6 | 31.6 | 27.1% | 9.9% | 27.1% | 2.3% | 1.37 | 1.06 | 360.00 | 1096.60 |
| 60 | 61.5 | 1.5 | 61.5 | 61.5 | 54.3% | 9.9% | 54.3% | 1.1% | 0.64 | 1.79 | 360.00 | 1359.40 |

### late share 20%, max delay 5s

Seeds pooled per cell: 5.

| L s | first-emit p95 s wait | first-emit p95 s revise | final p95 s wait | final p95 s revise | exact_rate_first_live wait | exact_rate_first_live revise | exact_rate_final_live | late_dropped_rate | mean_abs_count_error_live | value_revisions_per_window (revise) | records_emitted per run wait | records_emitted per run revise |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.8 | 1.8 | 1.8 | 1.8 | 51.8% | 51.8% | 51.8% | 1.2% | 0.70 | 0.00 | 360.00 | 360.00 |
| 5 | 6.7 | 1.8 | 6.7 | 6.7 | 100.0% | 51.8% | 100.0% | 0.0% | 0.00 | 0.69 | 360.00 | 963.00 |
| 15 | 16.7 | 1.8 | 16.7 | 16.7 | 100.0% | 51.8% | 100.0% | 0.0% | 0.00 | 0.69 | 360.00 | 963.00 |
| 30 | 31.8 | 1.8 | 31.8 | 31.8 | 100.0% | 51.8% | 100.0% | 0.0% | 0.00 | 0.69 | 360.00 | 963.00 |
| 60 | 61.8 | 1.8 | 61.8 | 61.8 | 100.0% | 51.8% | 100.0% | 0.0% | 0.00 | 0.69 | 360.00 | 963.00 |

### late share 20%, max delay 30s

Seeds pooled per cell: 5.

| L s | first-emit p95 s wait | first-emit p95 s revise | final p95 s wait | final p95 s revise | exact_rate_first_live wait | exact_rate_first_live revise | exact_rate_final_live | late_dropped_rate | mean_abs_count_error_live | value_revisions_per_window (revise) | records_emitted per run wait | records_emitted per run revise |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.8 | 1.8 | 1.8 | 1.8 | 6.6% | 6.6% | 6.6% | 4.7% | 2.76 | 0.00 | 360.00 | 360.00 |
| 5 | 6.7 | 1.8 | 6.7 | 6.7 | 17.5% | 6.6% | 17.5% | 2.9% | 1.70 | 1.04 | 360.00 | 1089.80 |
| 15 | 16.7 | 1.8 | 16.7 | 16.7 | 56.5% | 6.6% | 56.5% | 1.0% | 0.57 | 2.15 | 360.00 | 1488.80 |
| 30 | 31.8 | 1.8 | 31.8 | 31.8 | 100.0% | 6.6% | 100.0% | 0.0% | 0.00 | 2.71 | 360.00 | 1691.00 |
| 60 | 61.8 | 1.8 | 61.8 | 61.8 | 100.0% | 6.6% | 100.0% | 0.0% | 0.00 | 2.71 | 360.00 | 1691.00 |

### late share 20%, max delay 120s

Seeds pooled per cell: 5.

| L s | first-emit p95 s wait | first-emit p95 s revise | final p95 s wait | final p95 s revise | exact_rate_first_live wait | exact_rate_first_live revise | exact_rate_final_live | late_dropped_rate | mean_abs_count_error_live | value_revisions_per_window (revise) | records_emitted per run wait | records_emitted per run revise |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.8 | 1.8 | 1.8 | 1.8 | 0.0% | 0.0% | 0.0% | 15.0% | 8.74 | 0.00 | 360.00 | 360.00 |
| 5 | 6.7 | 1.8 | 6.7 | 6.7 | 0.0% | 0.0% | 0.0% | 13.7% | 7.99 | 0.73 | 360.00 | 978.00 |
| 15 | 16.7 | 1.8 | 16.7 | 16.7 | 0.2% | 0.0% | 0.2% | 12.0% | 6.98 | 1.73 | 360.00 | 1335.80 |
| 30 | 31.8 | 1.8 | 31.8 | 31.8 | 0.4% | 0.0% | 0.4% | 9.4% | 5.52 | 3.17 | 360.00 | 1854.80 |
| 60 | 61.8 | 1.8 | 61.8 | 61.8 | 8.3% | 0.0% | 8.3% | 4.4% | 2.63 | 6.06 | 360.00 | 2894.00 |

## 5. Stress regime (late share 20%, max delay 120s)

Seeds pooled per cell: 5.

| L s | mean_abs_count_error_live | late_dropped_rate | first-emit p95 s wait | first-emit p95 s revise | final p95 s wait | final p95 s revise | exact_rate_first_live wait | exact_rate_first_live revise | exact_rate_final_live | value_revisions_per_window (revise) | records_emitted per run wait | records_emitted per run revise |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 8.74 | 15.0% | 1.8 | 1.8 | 1.8 | 1.8 | 0.0% | 0.0% | 0.0% | 0.00 | 360.00 | 360.00 |
| 5 | 7.99 | 13.7% | 6.7 | 1.8 | 6.7 | 6.7 | 0.0% | 0.0% | 0.0% | 0.73 | 360.00 | 978.00 |
| 15 | 6.98 | 12.0% | 16.7 | 1.8 | 16.7 | 16.7 | 0.2% | 0.0% | 0.2% | 1.73 | 360.00 | 1335.80 |
| 30 | 5.52 | 9.4% | 31.8 | 1.8 | 31.8 | 31.8 | 0.4% | 0.0% | 0.4% | 3.17 | 360.00 | 1854.80 |
| 60 | 2.63 | 4.4% | 61.8 | 1.8 | 61.8 | 61.8 | 8.3% | 0.0% | 8.3% | 6.06 | 360.00 | 2894.00 |

## 6. Notes

- Rows with a 0% late share are identical across max delay. They are one result shown three times, not replications.
- For a given seed every cell shares the same event times, zones and amounts; only arrival times differ. Cells are paired within a seed, not independent.
- Percentiles are nearest-rank.
- Latency is stream time: the arrival time of the triggering event minus the window end, not wall-clock time.
- The generator's lateness model is an assumption, not a property of real order systems. The predictions and the simulator share the same assumed generator.
- Bursts start at t = 0 and fill the first half of every tenth window.
- Within the checks, P2 and P3-revise repeat across L, P4 at L = 0 equals P2, and the invariant checks are deterministic, so the independent statistical content is smaller than the check count (EVAL_PLAN.md, A1.6; ANALYTIC.md section 6).
