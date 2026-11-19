# Matched Pair Results (Baseline vs Concurrent REPACK)

- Baseline evidence: `runs/20261119T201259Z-m3-p2-baseline`
- REPACK evidence: `runs/20261119T200836Z-m3-p2-repack`
- Matched parameters: `{"seed_rows": 250000, "toast_rows": 25000, "duration_s": 180.0, "rate_ops_s": 10.0, "rng_seed": 20261120, "batch_size": 2000, "probe_interval_s": 20.0, "probe_poll_interval_s": 0.5}` (`parameters_matched=True`)
- Overlap confirmed: `True` (`43` writes committed during REPACK)
- Final source read opened strictly after REPACK finish: `True` (`repack_end_utc=2026-11-19T20:09:52.953082Z`, `source_read_opened_utc=2026-11-19T20:12:29.644639Z`)

## 1. CDC Convergence Evidence

| Check | Baseline | Concurrent REPACK |
| --- | --- | --- |
| Run outcome | `converged` | `converged` |
| Verified snapshot ID | `5017017340226159991` | `2826934594941022224` |
| Schema check (`schema_id`) | `passed=True` (`schema_id=0`) | `passed=True` (`schema_id=0`) |
| Source / Lake rows | `250019` / `250019` | `250019` / `250019` |
| Source-only / Lake-only / Duplicate lake IDs | `0` / `0` / `0` | `0` / `0` / `0` |
| Value mismatch rows | `0` | `0` |
| Final marker commit-to-visibility | `9910.69 ms` | `10161.07 ms` |
| Final marker commit to verified match | `35722.71 ms` | `34584.03 ms` |
| Full diff duration | `25141.59 ms` | `23844.99 ms` |

## 2. Concurrent REPACK Execution, Sizes and Sampling

- Command: `REPACK (CONCURRENTLY) public.orders` (PID `411`), outcome: `ok`
- Window interval: `30084.812 ms` to `34325.327 ms` (duration `4240.515 ms`)
- Writes overlapping REPACK: `43` committed during REPACK (`43` strictly contained, `43` any overlap)
- Relation file swap confirmed: `heap_relfilenode_changed=True`, `toast_relfilenode_changed=True`
- Table / index sizes before -> after: heap `46661632` -> `46989312` bytes; TOAST `414826496` -> `414375936` bytes; indexes `5636096` -> `5644288` bytes; total `467165184` -> `467050496` bytes
- Progress samples: `53` total (`53` with `pg_stat_progress_repack` row) at `0.05 s` cadence
- Phases observed: `['initializing', 'seq scanning heap', 'rebuilding index', 'performing final cleanup']`
- Temporary repack slots observed: `['pg_repack_413']`
- Final lock interval observed: `True`
  - Observed final-lock triggers: `['repack_lock:orders:AccessExclusiveLock:granted=True']`, span `34288.522 ms` to `34288.522 ms` (`0.0 ms`, `1` sample hits)
  - Overlapping ops during observed final lock: REPACK `n/a (n=0)` vs Baseline `n/a (n=0)`

## 3. Host Performance Observations (This Host Only)

### Full Measurement Window

| Metric | Baseline | Concurrent REPACK |
| --- | --- | --- |
| Attempted / Completed ops | `1800` / `1800` | `1800` / `1800` |
| Errors / Timeouts | `0` / `0` | `0` / `0` |
| Missed schedule (`queue > thresh`) / Harness missed | `0` / `0` | `13` / `0` |
| Completed throughput | `10.004 ops/s` | `10.004 ops/s` |
| Scheduled-to-completed latency | `p50=15.015 ms, p99=186.779 ms, max=337.176 ms (n=1800)` | `p50=15.203 ms, p99=296.608 ms, max=542.122 ms (n=1800)` |
| Execution latency | `p50=14.337 ms, p99=147.146 ms, max=266.555 ms (n=1800)` | `p50=14.544 ms, p99=182.937 ms, max=388.133 ms (n=1800)` |
| Queue delay | `p50=0.554 ms, p99=86.914 ms, max=237.274 ms (n=1800)` | `p50=0.649 ms, p99=196.739 ms, max=442.211 ms (n=1800)` |
| Steady-state reader lag probes | `8/8` visible, `0` errors, `p50=10113.725 ms, p99=15387.24 ms, max=15501.74 ms (n=8)` | `8/8` visible, `0` errors, `p50=8528.7 ms, p99=13942.912 ms, max=13992.43 ms (n=8)` |

### Matched REPACK Interval (`30084.812 ms` to `34325.327 ms`, duration `4240.515 ms`)

| Metric (ops scheduled in interval) | Baseline (matching interval) | Concurrent REPACK |
| --- | --- | --- |
| Attempted / Completed ops | `43` / `43` | `43` / `43` |
| Errors / Timeouts | `0` / `0` | `0` / `0` |
| Completed throughput | `10.14 ops/s` | `10.14 ops/s` |
| Scheduled-to-completed latency | `p50=18.574 ms, p99=140.944 ms, max=155.855 ms (n=43)` | `p50=35.824 ms, p99=152.388 ms, max=175.236 ms (n=43)` |
| Execution latency | `p50=17.612 ms, p99=131.344 ms, max=154.673 ms (n=43)` | `p50=31.651 ms, p99=151.209 ms, max=174.189 ms (n=43)` |
| Queue delay | `p50=0.478 ms, p99=41.09 ms, max=56.003 ms (n=43)` | `p50=0.422 ms, p99=52.466 ms, max=75.317 ms (n=43)` |
