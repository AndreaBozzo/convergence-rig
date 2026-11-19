# Matched Pair Results (Baseline vs Concurrent REPACK)

- Baseline evidence: `runs/20261119T201731Z-m3-p3-baseline`
- REPACK evidence: `runs/20261119T202201Z-m3-p3-repack`
- Matched parameters: `{"seed_rows": 250000, "toast_rows": 25000, "duration_s": 180.0, "rate_ops_s": 10.0, "rng_seed": 20261121, "batch_size": 2000, "probe_interval_s": 20.0, "probe_poll_interval_s": 0.5}` (`parameters_matched=True`)
- Overlap confirmed: `True` (`48` writes committed during REPACK)
- Final source read opened strictly after REPACK finish: `True` (`repack_end_utc=2026-11-19T20:23:28.255837Z`, `source_read_opened_utc=2026-11-19T20:26:02.073292Z`)

## 1. CDC Convergence Evidence

| Check | Baseline | Concurrent REPACK |
| --- | --- | --- |
| Run outcome | `converged` | `converged` |
| Verified snapshot ID | `915937396461439926` | `2656832467375503428` |
| Schema check (`schema_id`) | `passed=True` (`schema_id=0`) | `passed=True` (`schema_id=0`) |
| Source / Lake rows | `249974` / `249974` | `249974` / `249974` |
| Source-only / Lake-only / Duplicate lake IDs | `0` / `0` / `0` | `0` / `0` / `0` |
| Value mismatch rows | `0` | `0` |
| Final marker commit-to-visibility | `10990.72 ms` | `7815.05 ms` |
| Final marker commit to verified match | `35405.49 ms` | `30163.76 ms` |
| Full diff duration | `23840.48 ms` | `21852.13 ms` |

## 2. Concurrent REPACK Execution, Sizes and Sampling

- Command: `REPACK (CONCURRENTLY) public.orders` (PID `484`), outcome: `ok`
- Window interval: `30079.241 ms` to `34901.804 ms` (duration `4822.564 ms`)
- Writes overlapping REPACK: `48` committed during REPACK (`48` strictly contained, `49` any overlap)
- Relation file swap confirmed: `heap_relfilenode_changed=True`, `toast_relfilenode_changed=True`
- Table / index sizes before -> after: heap `46661632` -> `46710784` bytes; TOAST `415072256` -> `414441472` bytes; indexes `5636096` -> `5644288` bytes; total `467410944` -> `466837504` bytes
- Progress samples: `53` total (`53` with `pg_stat_progress_repack` row) at `0.05 s` cadence
- Phases observed: `['initializing', 'seq scanning heap', 'rebuilding index', 'performing final cleanup']`
- Temporary repack slots observed: `['pg_repack_486']`
- Final lock interval observed: `True`
  - Observed final-lock triggers: `['repack_lock:orders:AccessExclusiveLock:granted=True']`, span `34858.195 ms` to `34858.195 ms` (`0.0 ms`, `1` sample hits)
  - Overlapping ops during observed final lock: REPACK `n/a (n=0)` vs Baseline `p50=104.235 ms, p99=104.235 ms, max=104.235 ms (n=1)`

## 3. Host Performance Observations (This Host Only)

### Full Measurement Window

| Metric | Baseline | Concurrent REPACK |
| --- | --- | --- |
| Attempted / Completed ops | `1800` / `1800` | `1800` / `1800` |
| Errors / Timeouts | `0` / `0` | `0` / `0` |
| Missed schedule (`queue > thresh`) / Harness missed | `3` / `0` | `2` / `0` |
| Completed throughput | `10.001 ops/s` | `10.001 ops/s` |
| Scheduled-to-completed latency | `p50=15.184 ms, p99=227.369 ms, max=398.729 ms (n=1800)` | `p50=15.395 ms, p99=247.966 ms, max=384.823 ms (n=1800)` |
| Execution latency | `p50=14.433 ms, p99=160.622 ms, max=356.104 ms (n=1800)` | `p50=14.696 ms, p99=177.729 ms, max=364.572 ms (n=1800)` |
| Queue delay | `p50=0.639 ms, p99=127.498 ms, max=298.869 ms (n=1800)` | `p50=0.707 ms, p99=148.183 ms, max=284.967 ms (n=1800)` |
| Steady-state reader lag probes | `8/8` visible, `0` errors, `p50=10296.32 ms, p99=15291.506 ms, max=15404.96 ms (n=8)` | `8/8` visible, `0` errors, `p50=10837.195 ms, p99=14610.92 ms, max=14670.12 ms (n=8)` |

### Matched REPACK Interval (`30079.241 ms` to `34901.804 ms`, duration `4822.564 ms`)

| Metric (ops scheduled in interval) | Baseline (matching interval) | Concurrent REPACK |
| --- | --- | --- |
| Attempted / Completed ops | `49` / `49` | `49` / `49` |
| Errors / Timeouts | `0` / `0` | `0` / `0` |
| Completed throughput | `10.161 ops/s` | `10.161 ops/s` |
| Scheduled-to-completed latency | `p50=33.255 ms, p99=261.308 ms, max=298.318 ms (n=49)` | `p50=42.969 ms, p99=248.332 ms, max=248.676 ms (n=49)` |
| Execution latency | `p50=29.355 ms, p99=214.527 ms, max=297.351 ms (n=49)` | `p50=36.903 ms, p99=203.461 ms, max=211.797 ms (n=49)` |
| Queue delay | `p50=0.843 ms, p99=161.531 ms, max=198.445 ms (n=49)` | `p50=0.487 ms, p99=148.502 ms, max=148.802 ms (n=49)` |
