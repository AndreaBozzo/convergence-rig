# Matched Pair Results (Baseline vs Concurrent REPACK)

- Baseline evidence: `runs/20261119T200004Z-m3-p1-baseline`
- REPACK evidence: `runs/20261119T200420Z-m3-p1-repack`
- Matched parameters: `{"seed_rows": 250000, "toast_rows": 25000, "duration_s": 180.0, "rate_ops_s": 10.0, "rng_seed": 20261119, "batch_size": 2000, "probe_interval_s": 20.0, "probe_poll_interval_s": 0.5}` (`parameters_matched=True`)
- Overlap confirmed: `True` (`43` writes committed during REPACK)
- Final source read opened strictly after REPACK finish: `True` (`repack_end_utc=2026-11-19T20:05:37.856277Z`, `source_read_opened_utc=2026-11-19T20:08:14.285314Z`)

## 1. CDC Convergence Evidence

| Check | Baseline | Concurrent REPACK |
| --- | --- | --- |
| Run outcome | `converged` | `converged` |
| Verified snapshot ID | `6200656978523028915` | `3056416639867533545` |
| Schema check (`schema_id`) | `passed=True` (`schema_id=0`) | `passed=True` (`schema_id=0`) |
| Source / Lake rows | `249994` / `249994` | `249994` / `249994` |
| Source-only / Lake-only / Duplicate lake IDs | `0` / `0` / `0` | `0` / `0` / `0` |
| Value mismatch rows | `0` | `0` |
| Final marker commit-to-visibility | `9074.73 ms` | `10405.1 ms` |
| Final marker commit to verified match | `26181.15 ms` | `28174.79 ms` |
| Full diff duration | `16720.78 ms` | `17429.53 ms` |

## 2. Concurrent REPACK Execution, Sizes and Sampling

- Command: `REPACK (CONCURRENTLY) public.orders` (PID `413`), outcome: `ok`
- Window interval: `30128.982 ms` to `34503.244 ms` (duration `4374.261 ms`)
- Writes overlapping REPACK: `43` committed during REPACK (`43` strictly contained, `44` any overlap)
- Relation file swap confirmed: `heap_relfilenode_changed=True`, `toast_relfilenode_changed=True`
- Table / index sizes before -> after: heap `46661632` -> `46686208` bytes; TOAST `415006720` -> `414457856` bytes; indexes `5636096` -> `5644288` bytes; total `467345408` -> `466829312` bytes
- Progress samples: `54` total (`54` with `pg_stat_progress_repack` row) at `0.05 s` cadence
- Phases observed: `['initializing', 'seq scanning heap', 'rebuilding index', 'performing final cleanup']`
- Temporary repack slots observed: `['pg_repack_416']`
- Final lock interval observed: `True`
  - Observed final-lock triggers: `['repack_lock:orders:AccessExclusiveLock:granted=True']`, span `34451.938 ms` to `34451.938 ms` (`0.0 ms`, `1` sample hits)
  - Overlapping ops during observed final lock: REPACK `n/a (n=0)` vs Baseline `p50=89.316 ms, p99=89.316 ms, max=89.316 ms (n=1)`

## 3. Host Performance Observations (This Host Only)

### Full Measurement Window

| Metric | Baseline | Concurrent REPACK |
| --- | --- | --- |
| Attempted / Completed ops | `1800` / `1800` | `1800` / `1800` |
| Errors / Timeouts | `0` / `0` | `0` / `0` |
| Missed schedule (`queue > thresh`) / Harness missed | `10` / `0` | `0` / `0` |
| Completed throughput | `10.005 ops/s` | `10.005 ops/s` |
| Scheduled-to-completed latency | `p50=13.992 ms, p99=228.069 ms, max=536.194 ms (n=1800)` | `p50=13.285 ms, p99=165.72 ms, max=254.46 ms (n=1800)` |
| Execution latency | `p50=13.064 ms, p99=168.006 ms, max=469.46 ms (n=1800)` | `p50=12.709 ms, p99=144.922 ms, max=240.717 ms (n=1800)` |
| Queue delay | `p50=0.719 ms, p99=128.15 ms, max=436.277 ms (n=1800)` | `p50=0.589 ms, p99=65.859 ms, max=154.633 ms (n=1800)` |
| Steady-state reader lag probes | `8/8` visible, `0` errors, `p50=11023.885 ms, p99=14872.182 ms, max=14886.48 ms (n=8)` | `8/8` visible, `0` errors, `p50=8207.98 ms, p99=14119.48 ms, max=14215.64 ms (n=8)` |

### Matched REPACK Interval (`30128.982 ms` to `34503.244 ms`, duration `4374.261 ms`)

| Metric (ops scheduled in interval) | Baseline (matching interval) | Concurrent REPACK |
| --- | --- | --- |
| Attempted / Completed ops | `44` / `44` | `44` / `44` |
| Errors / Timeouts | `0` / `0` | `0` / `0` |
| Completed throughput | `10.059 ops/s` | `10.059 ops/s` |
| Scheduled-to-completed latency | `p50=20.55 ms, p99=143.018 ms, max=153.854 ms (n=44)` | `p50=32.804 ms, p99=219.418 ms, max=241.523 ms (n=44)` |
| Execution latency | `p50=19.38 ms, p99=140.594 ms, max=152.812 ms (n=44)` | `p50=31.14 ms, p99=181.233 ms, max=240.717 ms (n=44)` |
| Queue delay | `p50=0.288 ms, p99=43.131 ms, max=53.939 ms (n=44)` | `p50=0.77 ms, p99=119.566 ms, max=141.671 ms (n=44)` |
