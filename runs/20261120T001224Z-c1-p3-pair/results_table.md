# Matched Pair Results (Baseline vs Concurrent REPACK)

- Baseline evidence: `runs/20261120T000438Z-c1-p3-baseline`
- REPACK evidence: `runs/20261120T000831Z-c1-p3-repack`
- Matched parameters: `{"seed_rows": 250000, "toast_rows": 25000, "duration_s": 180.0, "rate_ops_s": 10.0, "rng_seed": 20261121, "batch_size": 2000, "probe_interval_s": 20.0, "probe_poll_interval_s": 0.5}` (`parameters_matched=True`)
- Overlap confirmed: `True` (`21` writes committed during REPACK)
- Final source read opened strictly after REPACK finish: `True` (`repack_end_utc=2026-11-20T00:09:39.533681Z`, `source_read_opened_utc=2026-11-20T00:12:11.670671Z`)

## 1. CDC Convergence Evidence

| Check | Baseline | Concurrent REPACK |
| --- | --- | --- |
| Run outcome | `converged` | `converged` |
| Verified snapshot ID | `232788033961330573` | `5820889228101709122` |
| Schema check (`schema_id`) | `passed=True` (`schema_id=0`) | `passed=True` (`schema_id=0`) |
| Source / Lake rows | `249974` / `249974` | `249974` / `249974` |
| Source-only / Lake-only / Duplicate lake IDs | `0` / `0` / `0` | `0` / `0` / `0` |
| Value mismatch rows | `0` | `0` |
| Final marker commit-to-visibility | `4900.59 ms` | `3887.27 ms` |
| Final marker commit to verified match | `15414.92 ms` | `14205.53 ms` |
| Full diff duration | `10217.15 ms` | `10003.26 ms` |

## 2. Concurrent REPACK Execution, Sizes and Sampling

- Command: `REPACK (CONCURRENTLY) public.orders` (PID `396`), outcome: `ok`
- Window interval: `30051.226 ms` to `32112.615 ms` (duration `2061.389 ms`)
- Writes overlapping REPACK: `21` committed during REPACK (`21` strictly contained, `21` any overlap)
- Relation file swap confirmed: `heap_relfilenode_changed=True`, `toast_relfilenode_changed=True`
- Table / index sizes before -> after: heap `46661632` -> `46678016` bytes; TOAST `415072256` -> `414343168` bytes; indexes `5636096` -> `5644288` bytes; total `467410944` -> `466706432` bytes
- Progress samples: `34` total (`33` with `pg_stat_progress_repack` row) at `0.05 s` cadence
- Phases observed: `['initializing', 'seq scanning heap', 'rebuilding index']`
- Temporary repack slots observed: `['pg_repack_398']`
- Final lock interval observed: `False`
  - Final-lock-specific figures are omitted because the brief `AccessExclusiveLock` / `swapping relation files` interval was not observed in sampler polls.

## 3. Host Performance Observations (This Host Only)

### Full Measurement Window

| Metric | Baseline | Concurrent REPACK |
| --- | --- | --- |
| Attempted / Completed ops | `1800` / `1800` | `1800` / `1800` |
| Errors / Timeouts | `0` / `0` | `0` / `0` |
| Missed schedule (`queue > thresh`) / Harness missed | `0` / `0` | `0` / `0` |
| Completed throughput | `10.005 ops/s` | `10.005 ops/s` |
| Scheduled-to-completed latency | `p50=5.668 ms, p99=9.598 ms, max=108.044 ms (n=1800)` | `p50=5.728 ms, p99=23.377 ms, max=117.927 ms (n=1800)` |
| Execution latency | `p50=5.1 ms, p99=9.169 ms, max=107.387 ms (n=1800)` | `p50=5.143 ms, p99=23.205 ms, max=117.031 ms (n=1800)` |
| Queue delay | `p50=0.682 ms, p99=1.241 ms, max=23.526 ms (n=1800)` | `p50=0.695 ms, p99=1.315 ms, max=17.996 ms (n=1800)` |
| Steady-state reader lag probes | `8/8` visible, `0` errors, `p50=10804.935 ms, p99=13523.512 ms, max=13558.35 ms (n=8)` | `8/8` visible, `0` errors, `p50=10579.335 ms, p99=13940.176 ms, max=13997.45 ms (n=8)` |

### Matched REPACK Interval (`30051.226 ms` to `32112.615 ms`, duration `2061.389 ms`)

| Metric (ops scheduled in interval) | Baseline (matching interval) | Concurrent REPACK |
| --- | --- | --- |
| Attempted / Completed ops | `21` / `21` | `21` / `21` |
| Errors / Timeouts | `0` / `0` | `0` / `0` |
| Completed throughput | `10.187 ops/s` | `10.187 ops/s` |
| Scheduled-to-completed latency | `p50=5.375 ms, p99=8.008 ms, max=8.075 ms (n=21)` | `p50=16.357 ms, p99=34.033 ms, max=34.173 ms (n=21)` |
| Execution latency | `p50=4.959 ms, p99=7.613 ms, max=7.655 ms (n=21)` | `p50=15.774 ms, p99=33.01 ms, max=33.227 ms (n=21)` |
| Queue delay | `p50=0.85 ms, p99=1.12 ms, max=1.132 ms (n=21)` | `p50=0.684 ms, p99=1.277 ms, max=1.33 ms (n=21)` |
