# C1 Results: Concurrent REPACK Pairs with pg2iceberg Compaction Enabled

- Matched pairs: `3` (orders: `['baseline-first', 'repack-first', 'baseline-first']`, counterbalanced: `True`)
- Compaction config: `compose/pg2iceberg.compaction.yaml`
- Outcomes: Baseline `3/3` converged, Concurrent REPACK `3/3` converged (`all_valid_runs=True`)
- Compaction ran inside measurement window in all runs: `True`
- `pg2iceberg.log` free of conflicts, retries, errors and unexpected warnings in all runs: `True`
- Concurrent write overlap confirmed in all pairs: `True`
- Final source read opened strictly after REPACK finish in all pairs: `True`
- Project-only cleanup verified in all runs: `True`
- Container digests identical across all runs: `True`

## 1. Per-Run Convergence and Compaction Proof

| Run | Order | Seed | Outcome | Verified Snapshot ID | Rows (Src = Lake) | Diffs | Compactions in Window (Merge / Del-Rewrite) | Verified Live Data / Eq-Del Files (Eq-Del Recs) | Final Vis | Commit-to-Match | Diff Time |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| P1 Baseline | `baseline-first` | `20261119` | `converged` | `7011613701619538094` | `249,994` = `249,994` | `0/0/0/0` | `14 (12 / 2)` | `4 / 5 (461)` | `3954.34 ms` | `14301.41 ms` | `10060.93 ms` |
| P1 REPACK (CONC) | `baseline-first` | `20261119` | `converged` | `6711568726880290289` | `249,994` = `249,994` | `0/0/0/0` | `14 (12 / 2)` | `4 / 5 (461)` | `4883.16 ms` | `14944.94 ms` | `9777.77 ms` |
| P2 Baseline | `repack-first` | `20261120` | `converged` | `4408359321324628831` | `250,019` = `250,019` | `0/0/0/0` | `13 (11 / 2)` | `5 / 5 (448)` | `3863.67 ms` | `15979.33 ms` | `11844.98 ms` |
| P2 REPACK (CONC) | `repack-first` | `20261120` | `converged` | `6145901343645389365` | `250,019` = `250,019` | `0/0/0/0` | `13 (11 / 2)` | `5 / 5 (447)` | `3957.76 ms` | `13868.56 ms` | `9625.44 ms` |
| P3 Baseline | `baseline-first` | `20261121` | `converged` | `232788033961330573` | `249,974` = `249,974` | `0/0/0/0` | `14 (12 / 2)` | `4 / 5 (452)` | `4900.59 ms` | `15414.92 ms` | `10217.15 ms` |
| P3 REPACK (CONC) | `baseline-first` | `20261121` | `converged` | `5820889228101709122` | `249,974` = `249,974` | `0/0/0/0` | `14 (12 / 2)` | `4 / 5 (452)` | `3887.27 ms` | `14205.53 ms` | `10003.26 ms` |

## 2. Compaction Overlap and pg2iceberg Log Audit

| Run | Total Compactions (<= Verified) | Data Files Replaced / Del Files Removed | Overlapped REPACK (Commit Inside / Span Overlap) | Final-Marker Overlap (Verified=Replace / Parent=Replace / Between Commit & Verified) | Log Audit (Err / Conflict / Retry / Unexp Warn) |
| --- | --- | --- | --- | --- | --- |
| P1 Baseline | `15` | `58 / 12` | `n/a` | `False / True / True` | `0 / 0 / 0 / 0` |
| P1 REPACK (CONC) | `15` | `58 / 12` | `True (1) / True (2)` | `False / True / True` | `0 / 0 / 0 / 0` |
| P2 Baseline | `14` | `56 / 12` | `n/a` | `False / True / True` | `0 / 0 / 0 / 0` |
| P2 REPACK (CONC) | `14` | `56 / 12` | `True (1) / True (2)` | `False / True / True` | `0 / 0 / 0 / 0` |
| P3 Baseline | `15` | `58 / 12` | `n/a` | `False / True / True` | `0 / 0 / 0 / 0` |
| P3 REPACK (CONC) | `15` | `58 / 12` | `True (1) / True (2)` | `False / True / True` | `0 / 0 / 0 / 0` |

## 3. Concurrent REPACK Execution Across C1 Pairs

| Pair | Order | Seed | Duration | Committed Ops During REPACK | Heap/TOAST Swap | Total Size Delta | Peak CDC Confirmed-Flush WAL Gap | Blocked Waiters / Final Lock |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Pair 1 | `baseline-first` | `20261119` | `2178.22 ms` | `22` | `True/True` | `-696,320 B` | `401,402,968 B` | `0` / `False` |
| Pair 2 | `repack-first` | `20261120` | `2177.41 ms` | `22` | `True/True` | `-458,752 B` | `401,056,192 B` | `0` / `False` |
| Pair 3 | `baseline-first` | `20261121` | `2061.39 ms` | `21` | `True/True` | `-704,512 B` | `401,218,200 B` | `0` / `False` |
| **Median [min, max]** | `n=3` | `matched` | `2177.41 ms [2061.39, 2178.22]` | `22.0 ops [21.0, 22.0]` | `3/3 swapped` | `-696320 B [-704512, -458752]` | `401218200 B [401056192, 401402968]` | `0` total / `0/3` |

## 4. C1 Host Performance Summary (Median and Range Across Pairs)

| Metric | Baseline Median [min, max] | Concurrent REPACK Median [min, max] | Paired Delta (REPACK - Baseline) Median [min, max] |
| --- | --- | --- | --- |
| Matched interval: Exec p50 | `5.13 ms [4.96, 5.43]` | `15.77 ms [13.93, 16.74]` | `+10.81 ms [+8.80, +11.31]` |
| Matched interval: Exec p99 | `7.61 ms [6.43, 79.04]` | `33.01 ms [32.41, 35.10]` | `+25.40 ms [-43.94, +25.98]` |
| Matched interval: S2C p50 | `5.73 ms [5.38, 6.30]` | `16.36 ms [14.54, 17.21]` | `+10.91 ms [+8.82, +10.98]` |
| Matched interval: S2C p99 | `8.01 ms [7.05, 79.69]` | `34.03 ms [33.47, 35.45]` | `+26.02 ms [-44.24, +26.42]` |
| Matched interval: S2C max | `8.07 ms [7.16, 81.30]` | `34.17 ms [33.89, 35.49]` | `+26.10 ms [-45.81, +26.73]` |
| Full window (180 s): Exec p50 | `5.10 ms [5.07, 5.13]` | `5.16 ms [5.14, 5.17]` | `+0.04 ms [+0.03, +0.10]` |
| Full window (180 s): Exec p99 | `11.14 ms [9.17, 19.45]` | `23.20 ms [21.42, 23.21]` | `+12.08 ms [+1.97, +14.04]` |
| Full window (180 s): S2C p50 | `5.73 ms [5.67, 5.73]` | `5.75 ms [5.73, 5.76]` | `+0.04 ms [+0.02, +0.06]` |
| Full window (180 s): S2C p99 | `12.96 ms [9.60, 20.38]` | `23.38 ms [23.25, 23.81]` | `+10.29 ms [+3.43, +13.78]` |
| Full window (180 s): S2C max | `108.04 ms [81.30, 138.04]` | `117.93 ms [115.08, 154.71]` | `+9.88 ms [-22.97, +73.42]` |
| Steady-state probe lag: p50 | `10804.93 ms [10598.93, 11041.75]` | `10579.33 ms [9804.74, 10795.92]` | `-245.83 ms [-794.20, -225.60]` |
| Steady-state probe lag: max | `13558.35 ms [13414.12, 13630.25]` | `13678.67 ms [13340.20, 13997.45]` | `+264.55 ms [-290.05, +439.10]` |
| Final marker commit-to-visibility | `3954.34 ms [3863.67, 4900.59]` | `3957.76 ms [3887.27, 4883.16]` | `+94.09 ms [-1013.32, +928.82]` |
| Final marker commit-to-match | `15414.92 ms [14301.41, 15979.33]` | `14205.53 ms [13868.56, 14944.94]` | `-1209.39 ms [-2110.77, +643.53]` |
| Full diff duration | `10217.15 ms [10060.93, 11844.98]` | `9777.77 ms [9625.44, 10003.26]` | `-283.16 ms [-2219.54, -213.89]` |

## 5. Per-Run C1 Host Performance and Pipeline Positions

| Run | Attempted/Comp | Err/TO | Missed Sched/Harness | Matched Int Exec p50/p99 | Matched Int S2C p50/p99/max | Full Win S2C p50/p99/max | Probe Lag p50/max | Final WAL Gap (Flush / Restart) | Final `mat_cursor` / `log_seq` |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| P1 Baseline | `1800/1800` | `0/0` | `0/0` | `5.429 / 79.038 ms` | `6.3 / 79.694 / 81.298 ms` | `5.726 / 20.384 / 81.298 ms` | `10598.935 / 13630.25 ms` | `1,656 B / 656,024 B` | `1811 / 1811` |
| P1 REPACK (CONC) | `1800/1800` | `0/0` | `0/0` | `16.737 / 35.099 ms` | `17.206 / 35.45 / 35.492 ms` | `5.764 / 23.812 / 154.714 ms` | `9804.735 / 13340.2 ms` | `1,656 B / 785,400 B` | `1811 / 1811` |
| P2 Baseline | `1800/1800` | `0/0` | `0/0` | `5.133 / 6.429 ms` | `5.728 / 7.052 / 7.158 ms` | `5.726 / 12.959 / 138.041 ms` | `11041.745 / 13414.12 ms` | `1,856 B / 122,832 B` | `1811 / 1811` |
| P2 REPACK (CONC) | `1800/1800` | `0/0` | `0/0` | `13.93 / 32.41 ms` | `14.545 / 33.468 / 33.888 ms` | `5.75 / 23.25 / 115.075 ms` | `10795.915 / 13678.67 ms` | `1,544 B / 801,256 B` | `1811 / 1811` |
| P3 Baseline | `1800/1800` | `0/0` | `0/0` | `4.959 / 7.613 ms` | `5.375 / 8.008 / 8.075 ms` | `5.668 / 9.598 / 108.044 ms` | `10804.935 / 13558.35 ms` | `1,656 B / 151,392 B` | `1811 / 1811` |
| P3 REPACK (CONC) | `1800/1800` | `0/0` | `0/0` | `15.774 / 33.01 ms` | `16.357 / 34.033 / 34.173 ms` | `5.728 / 23.377 / 117.927 ms` | `10579.335 / 13997.45 ms` | `1,456 B / 727,664 B` | `1811 / 1811` |

## 6. Side-by-Side Comparison: C1 (Compaction On) vs M3 (Compaction Off, Same Seeds)

- All seeds matched (`20261119`, `20261120`, `20261121`): `True`

### Aggregate across all 6 matched runs (n=6) and by role (n=3)

| Metric | M3 (Compaction Off) Median [min, max] | C1 (Compaction On) Median [min, max] | Paired Delta (C1 - M3) Median [min, max] |
| --- | --- | --- | --- |
| Verified live data files (all 6) | `43.0 files [43.0, 43.0]` | `4.0 files [4.0, 5.0]` | `-39.0 files [-39.0, -38.0]` |
| Verified live eq-delete files (all 6) | `17.0 files [17.0, 17.0]` | `5.0 files [5.0, 5.0]` | `-12.0 files [-12.0, -12.0]` |
| Full diff duration (all 6) | `22846.31 ms [16720.78, 25141.59]` | `10032.10 ms [9625.44, 11844.98]` | `-12572.74 ms [-14219.55, -6659.85]` |
| Final marker visibility (all 6) | `10035.88 ms [7815.05, 10990.72]` | `3956.05 ms [3863.67, 4900.59]` | `-5784.48 ms [-6203.31, -3927.78]` |
| Final marker commit-to-match (all 6) | `32373.89 ms [26181.15, 35722.71]` | `14623.17 ms [13868.56, 15979.33]` | `-17850.80 ms [-20715.47, -11879.74]` |
| Steady-state probe lag p50 (all 6) | `10205.02 ms [8207.98, 11023.89]` | `10697.42 ms [9804.74, 11041.75]` | `+718.32 ms [-424.95, +2267.22]` |
| Steady-state probe lag max (all 6) | `14778.30 ms [13992.43, 15501.74]` | `13594.30 ms [13340.20, 13997.45]` | `-1065.83 ms [-2087.62, -313.76]` |
| Full window Exec p50 (all 6) | `14.38 ms [12.71, 14.70]` | `5.14 ms [5.07, 5.17]` | `-9.27 ms [-9.55, -7.54]` |
| Matched interval Exec p50 (Baseline, n=3) | `19.38 ms [17.61, 29.36]` | `5.13 ms [4.96, 5.43]` | `-13.95 ms [-24.40, -12.48]` |
| Matched interval Exec p50 (REPACK, n=3) | `31.65 ms [31.14, 36.90]` | `15.77 ms [13.93, 16.74]` | `-17.72 ms [-21.13, -14.40]` |

### Per-run side-by-side (matched on seed and role)

| Run (Seed) | Outcome (M3 / C1) | Live Data/Del Files (M3 -> C1) | Final Vis (M3 -> C1) | Commit-to-Match (M3 -> C1) | Diff Time (M3 -> C1) | Probe p50 (M3 -> C1) | Matched Int Exec p50 (M3 -> C1) | Full Win Exec p50 (M3 -> C1) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| P1 Baseline (`20261119`) | `converged` / `converged` | `43/17` -> `4/5` | `9074.73 ms` -> `3954.34 ms` | `26181.15 ms` -> `14301.41 ms` | `16720.78 ms` -> `10060.93 ms` | `11023.89 ms` -> `10598.93 ms` | `19.38 ms` -> `5.43 ms` | `13.06 ms` -> `5.07 ms` |
| P1 REPACK (`20261119`) | `converged` / `converged` | `43/17` -> `4/5` | `10405.10 ms` -> `4883.16 ms` | `28174.79 ms` -> `14944.94 ms` | `17429.53 ms` -> `9777.77 ms` | `8207.98 ms` -> `9804.74 ms` | `31.14 ms` -> `16.74 ms` | `12.71 ms` -> `5.17 ms` |
| P2 Baseline (`20261120`) | `converged` / `converged` | `43/17` -> `5/5` | `9910.69 ms` -> `3863.67 ms` | `35722.71 ms` -> `15979.33 ms` | `25141.59 ms` -> `11844.98 ms` | `10113.73 ms` -> `11041.75 ms` | `17.61 ms` -> `5.13 ms` | `14.34 ms` -> `5.13 ms` |
| P2 REPACK (`20261120`) | `converged` / `converged` | `43/17` -> `5/5` | `10161.07 ms` -> `3957.76 ms` | `34584.03 ms` -> `13868.56 ms` | `23844.99 ms` -> `9625.44 ms` | `8528.70 ms` -> `10795.92 ms` | `31.65 ms` -> `13.93 ms` | `14.54 ms` -> `5.16 ms` |
| P3 Baseline (`20261121`) | `converged` / `converged` | `43/17` -> `4/5` | `10990.72 ms` -> `4900.59 ms` | `35405.49 ms` -> `15414.92 ms` | `23840.48 ms` -> `10217.15 ms` | `10296.32 ms` -> `10804.93 ms` | `29.36 ms` -> `4.96 ms` | `14.43 ms` -> `5.10 ms` |
| P3 REPACK (`20261121`) | `converged` / `converged` | `43/17` -> `4/5` | `7815.05 ms` -> `3887.27 ms` | `30163.76 ms` -> `14205.53 ms` | `21852.13 ms` -> `10003.26 ms` | `10837.19 ms` -> `10579.33 ms` | `36.90 ms` -> `15.77 ms` | `14.70 ms` -> `5.14 ms` |
