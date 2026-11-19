# Matched Pair Results (Baseline vs Concurrent REPACK)

- Baseline evidence: `runs/20261119T193919Z-m2-baseline`
- REPACK evidence: `runs/20261119T194343Z-m2-repack`
- Matched parameters: `{"seed_rows": 250000, "toast_rows": 25000, "duration_s": 180.0, "rate_ops_s": 10.0, "rng_seed": 20261119, "batch_size": 2000, "probe_interval_s": 20.0, "probe_poll_interval_s": 0.5}` (`parameters_matched=True`)
- Overlap confirmed: `True` (`45` writes committed during REPACK)
- Final source read opened strictly after REPACK finish: `True` (`repack_end_utc=2026-11-19T19:45:01.022169Z`, `source_read_opened_utc=2026-11-19T19:47:36.168440Z`)

## 1. CDC Convergence Evidence

| Check | Baseline | Concurrent REPACK |
| --- | --- | --- |
| Run outcome | `converged` | `converged` |
| Verified snapshot ID | `4016379093415438975` | `8007425982733359985` |
| Schema check (`schema_id`) | `passed=True` (`schema_id=0`) | `passed=True` (`schema_id=0`) |
| Source / Lake rows | `249994` / `249994` | `249994` / `249994` |
| Source-only / Lake-only / Duplicate lake IDs | `0` / `0` / `0` | `0` / `0` / `0` |
| Value mismatch rows | `0` | `0` |
| Final marker commit-to-visibility | `11854.66 ms` | `8985.81 ms` |
| Final marker commit to verified match | `34466.17 ms` | `26109.87 ms` |
| Full diff duration | `22268.32 ms` | `16750.21 ms` |

## 2. Concurrent REPACK Execution, Sizes and Sampling

- Command: `REPACK (CONCURRENTLY) public.orders` (PID `413`), outcome: `ok`
- Window interval: `30057.056 ms` to `34556.483 ms` (duration `4499.427 ms`)
- Writes overlapping REPACK: `45` committed during REPACK (`45` strictly contained, `45` any overlap)
- Relation file swap confirmed: `heap_relfilenode_changed=True`, `toast_relfilenode_changed=True`
- Table / index sizes before -> after: heap `46661632` -> `47005696` bytes; TOAST `415006720` -> `414457856` bytes; indexes `5636096` -> `5644288` bytes; total `467345408` -> `467148800` bytes
- Progress samples: `59` total (`58` with `pg_stat_progress_repack` row) at `0.05 s` cadence
- Phases observed: `['initializing', 'seq scanning heap', 'rebuilding index']`
- Temporary repack slots observed: `['pg_repack_415']`
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
| Scheduled-to-completed latency | `p50=13.764 ms, p99=175.378 ms, max=316.206 ms (n=1800)` | `p50=14.463 ms, p99=217.793 ms, max=310.579 ms (n=1800)` |
| Execution latency | `p50=13.048 ms, p99=157.11 ms, max=210.187 ms (n=1800)` | `p50=13.571 ms, p99=169.397 ms, max=272.664 ms (n=1800)` |
| Queue delay | `p50=0.706 ms, p99=75.499 ms, max=216.331 ms (n=1800)` | `p50=0.663 ms, p99=117.947 ms, max=210.735 ms (n=1800)` |
| Steady-state reader lag probes | `8/8` visible, `0` errors, `p50=10544.675 ms, p99=14787.139 ms, max=14806.29 ms (n=8)` | `8/8` visible, `0` errors, `p50=10856.26 ms, p99=14651.748 ms, max=14656.28 ms (n=8)` |

### Matched REPACK Interval (`30057.056 ms` to `34556.483 ms`, duration `4499.427 ms`)

| Metric (ops scheduled in interval) | Baseline (matching interval) | Concurrent REPACK |
| --- | --- | --- |
| Attempted / Completed ops | `45` / `45` | `45` / `45` |
| Errors / Timeouts | `0` / `0` | `0` / `0` |
| Completed throughput | `10.001 ops/s` | `10.001 ops/s` |
| Scheduled-to-completed latency | `p50=19.562 ms, p99=157.477 ms, max=157.701 ms (n=45)` | `p50=29.422 ms, p99=118.582 ms, max=120.49 ms (n=45)` |
| Execution latency | `p50=18.986 ms, p99=136.99 ms, max=156.543 ms (n=45)` | `p50=28.192 ms, p99=118.074 ms, max=120.189 ms (n=45)` |
| Queue delay | `p50=0.858 ms, p99=57.644 ms, max=57.867 ms (n=45)` | `p50=0.373 ms, p99=18.723 ms, max=20.59 ms (n=45)` |
