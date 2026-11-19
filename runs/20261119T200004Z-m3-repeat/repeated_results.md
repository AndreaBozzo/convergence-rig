# Repeated Matched-Pair Results (Baseline vs Concurrent REPACK)

- Matched pairs: `3` (orders: `['baseline-first', 'repack-first', 'baseline-first']`, counterbalanced: `True`)
- All parameters matched within each pair: `True`
- Outcomes: Baseline `3/3` converged, Concurrent REPACK `3/3` converged (`all_converged=True`)
- Concurrent write overlap confirmed in all pairs: `True`
- Final source read opened strictly after REPACK finish in all pairs: `True`
- Project-only cleanup verified in all runs: `True`
- Container digests identical across all runs: `True`

## 1. Per-Run CDC Convergence Evidence

| Pair / Role | Order | Seed | Outcome | Snapshot ID | Rows (Src = Lake) | Diffs (Src-only / Lake-only / Dup / Val) | Final Vis | Commit-to-Match | Diff Time | Raw Run |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| P1 Baseline | `baseline-first` | `20261119` | `converged` | `6200656978523028915` | `249,994` = `249,994` | `0/0/0/0` | `9074.73 ms` | `26181.15 ms` | `16720.78 ms` | `runs/20261119T200004Z-m3-p1-baseline` |
| P1 REPACK (CONC) | `baseline-first` | `20261119` | `converged` | `3056416639867533545` | `249,994` = `249,994` | `0/0/0/0` | `10405.10 ms` | `28174.79 ms` | `17429.53 ms` | `runs/20261119T200420Z-m3-p1-repack` |
| P2 Baseline | `repack-first` | `20261120` | `converged` | `5017017340226159991` | `250,019` = `250,019` | `0/0/0/0` | `9910.69 ms` | `35722.71 ms` | `25141.59 ms` | `runs/20261119T201259Z-m3-p2-baseline` |
| P2 REPACK (CONC) | `repack-first` | `20261120` | `converged` | `2826934594941022224` | `250,019` = `250,019` | `0/0/0/0` | `10161.07 ms` | `34584.03 ms` | `23844.99 ms` | `runs/20261119T200836Z-m3-p2-repack` |
| P3 Baseline | `baseline-first` | `20261121` | `converged` | `915937396461439926` | `249,974` = `249,974` | `0/0/0/0` | `10990.72 ms` | `35405.49 ms` | `23840.48 ms` | `runs/20261119T201731Z-m3-p3-baseline` |
| P3 REPACK (CONC) | `baseline-first` | `20261121` | `converged` | `2656832467375503428` | `249,974` = `249,974` | `0/0/0/0` | `7815.05 ms` | `30163.76 ms` | `21852.13 ms` | `runs/20261119T202201Z-m3-p3-repack` |
| Control Plain REPACK | `control` | `20261119` | `converged` | `7392093235743349883` | `249,994` = `249,994` | `0/0/0/0` | `10131.23 ms` | `34040.15 ms` | `23348.61 ms` | `runs/20261119T202629Z-m3-control-plain-repack` |

## 2. Concurrent REPACK Execution Across Pairs

| Pair | Order | Seed | Duration | Committed Ops During REPACK | Heap/TOAST Swap | Total Size Delta | Peak CDC Confirmed-Flush WAL Gap | Phases Observed | Blocked Waiters / Final Lock |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Pair 1 | `baseline-first` | `20261119` | `4374.26 ms` | `43` | `True/True` | `-516,096 B` | `401,762,512 B` | `['initializing', 'seq scanning heap', 'rebuilding index', 'performing final cleanup']` | `0` / `True` |
| Pair 2 | `repack-first` | `20261120` | `4240.52 ms` | `43` | `True/True` | `-114,688 B` | `401,453,080 B` | `['initializing', 'seq scanning heap', 'rebuilding index', 'performing final cleanup']` | `0` / `True` |
| Pair 3 | `baseline-first` | `20261121` | `4822.56 ms` | `48` | `True/True` | `-573,440 B` | `401,829,272 B` | `['initializing', 'seq scanning heap', 'rebuilding index', 'performing final cleanup']` | `0` / `True` |
| **Median [min, max]** | `n=3` | `matched` | `4374.26 ms [4240.52, 4822.56]` | `43.0 ops [43.0, 48.0]` | `3/3 swapped` | `-516096 B [-573440, -114688]` | `401762512 B [401453080, 401829272]` | `n/a` | `0` total / `3/3` |

## 3. Host Performance Summary: Median and Range Across Pairs (This Host Only)

| Metric | Baseline Median [min, max] | Concurrent REPACK Median [min, max] | Paired Delta (REPACK - Baseline) Median [min, max] |
| --- | --- | --- | --- |
| Matched interval: Exec p50 | `19.38 ms [17.61, 29.36]` | `31.65 ms [31.14, 36.90]` | `+11.76 ms [+7.55, +14.04]` |
| Matched interval: Exec p99 | `140.59 ms [131.34, 214.53]` | `181.23 ms [151.21, 203.46]` | `+19.86 ms [-11.07, +40.64]` |
| Matched interval: S2C p50 | `20.55 ms [18.57, 33.26]` | `35.82 ms [32.80, 42.97]` | `+12.25 ms [+9.71, +17.25]` |
| Matched interval: S2C p99 | `143.02 ms [140.94, 261.31]` | `219.42 ms [152.39, 248.33]` | `+11.44 ms [-12.98, +76.40]` |
| Matched interval: S2C max | `155.85 ms [153.85, 298.32]` | `241.52 ms [175.24, 248.68]` | `+19.38 ms [-49.64, +87.67]` |
| Full window (180 s): Exec p50 | `14.34 ms [13.06, 14.43]` | `14.54 ms [12.71, 14.70]` | `+0.21 ms [-0.35, +0.26]` |
| Full window (180 s): Exec p99 | `160.62 ms [147.15, 168.01]` | `177.73 ms [144.92, 182.94]` | `+17.11 ms [-23.08, +35.79]` |
| Full window (180 s): S2C p50 | `15.02 ms [13.99, 15.18]` | `15.20 ms [13.29, 15.39]` | `+0.19 ms [-0.71, +0.21]` |
| Full window (180 s): S2C p99 | `227.37 ms [186.78, 228.07]` | `247.97 ms [165.72, 296.61]` | `+20.60 ms [-62.35, +109.83]` |
| Full window (180 s): S2C max | `398.73 ms [337.18, 536.19]` | `384.82 ms [254.46, 542.12]` | `-13.91 ms [-281.73, +204.95]` |
| Steady-state probe lag: p50 | `10296.32 ms [10113.73, 11023.89]` | `8528.70 ms [8207.98, 10837.19]` | `-1585.03 ms [-2815.91, +540.88]` |
| Steady-state probe lag: max | `15404.96 ms [14886.48, 15501.74]` | `14215.64 ms [13992.43, 14670.12]` | `-734.84 ms [-1509.31, -670.84]` |
| Final marker commit-to-visibility | `9910.69 ms [9074.73, 10990.72]` | `10161.07 ms [7815.05, 10405.10]` | `+250.38 ms [-3175.67, +1330.37]` |
| Final marker commit-to-match | `35405.49 ms [26181.15, 35722.71]` | `30163.76 ms [28174.79, 34584.03]` | `-1138.68 ms [-5241.73, +1993.64]` |

## 4. Per-Run Host Performance Breakdown

| Run | Attempted/Completed | Err/TO | Missed Sched / Harness | Matched Int Exec p50 / p99 | Matched Int S2C p50 / p99 / max | Full Win S2C p50 / p99 / max | Probe Lag p50 / max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| P1 Baseline | `1800/1800` | `0/0` | `10/0` | `19.38 / 140.594 ms` | `20.55 / 143.018 / 153.854 ms` | `13.992 / 228.069 / 536.194 ms` | `11023.885 / 14886.48 ms` |
| P1 REPACK (CONC) | `1800/1800` | `0/0` | `0/0` | `31.14 / 181.233 ms` | `32.804 / 219.418 / 241.523 ms` | `13.285 / 165.72 / 254.46 ms` | `8207.98 / 14215.64 ms` |
| P2 Baseline | `1800/1800` | `0/0` | `0/0` | `17.612 / 131.344 ms` | `18.574 / 140.944 / 155.855 ms` | `15.015 / 186.779 / 337.176 ms` | `10113.725 / 15501.74 ms` |
| P2 REPACK (CONC) | `1800/1800` | `0/0` | `13/0` | `31.651 / 151.209 ms` | `35.824 / 152.388 / 175.236 ms` | `15.203 / 296.608 / 542.122 ms` | `8528.7 / 13992.43 ms` |
| P3 Baseline | `1800/1800` | `0/0` | `3/0` | `29.355 / 214.527 ms` | `33.255 / 261.308 / 298.318 ms` | `15.184 / 227.369 / 398.729 ms` | `10296.32 / 15404.96 ms` |
| P3 REPACK (CONC) | `1800/1800` | `0/0` | `2/0` | `36.903 / 203.461 ms` | `42.969 / 248.332 / 248.676 ms` | `15.395 / 247.966 / 384.823 ms` | `10837.195 / 14670.12 ms` |
| Control Plain REPACK | `1800/1800` | `0/0` | `44/0` | `4.091 / 2448.637 ms` | `2212.585 / 4176.186 / 4215.836 ms` | `15.526 / 2502.744 / 4215.836 ms` | `9780.265 / 14563.05 ms` |

## 5. Plain REPACK Control (`REPACK public.orders`)

- Raw evidence: `runs/20261119T202629Z-m3-control-plain-repack` (same 250k fixture and seed `20261119` as Pair 1)
- Outcome: `converged`, duration `4269.47 ms`, heap/TOAST swap `True/True`
- `AccessExclusiveLock` observed by sampler: `True` (evidence: `{"triggers": ["blocked_waiter_on_orders", "repack_lock:orders:AccessExclusiveLock:granted=True"], "sample_hits": 93, "first_offset_ms": 30080.806, "last_offset_ms": 34300.523, "observed_span_ms": 4219.717}`)
- Blocked waiter samples on `public.orders`: `46`
- Backpressure-delayed ops (full window): `124` (`missed_schedule_count=44`, `harness_missed_arrival_count=0`)
- Peak CDC slot confirmed-flush WAL gap: `390,225,792 B`
- Phases observed: `['initializing', 'seq scanning heap', 'rebuilding index']`, temporary repack slots: `[]`

## Reproducible Artifacts

- `repeated_summary.json`: aggregated per-run, per-pair, and median/range JSON
- `repeated_results.md`: this results table
- `latency_comparison.svg`: matched-interval and full-window writer latency comparison
- `repack_window_timeseries.svg`: per-operation latency traces around `t = 20 s..45 s`
- `reader_lag.svg`: steady-state probe lag and final-marker convergence timing
