# Matched Pair Results (Baseline vs Concurrent REPACK)

- Baseline evidence: `runs/20261120T000044Z-c1-p2-baseline`
- REPACK evidence: `runs/20261119T235651Z-c1-p2-repack`
- Matched parameters: `{"seed_rows": 250000, "toast_rows": 25000, "duration_s": 180.0, "rate_ops_s": 10.0, "rng_seed": 20261120, "batch_size": 2000, "probe_interval_s": 20.0, "probe_poll_interval_s": 0.5}` (`parameters_matched=True`)
- Overlap confirmed: `True` (`22` writes committed during REPACK)
- Final source read opened strictly after REPACK finish: `True` (`repack_end_utc=2026-11-19T23:57:59.391142Z`, `source_read_opened_utc=2026-11-20T00:00:31.485901Z`)

## 1. CDC Convergence Evidence

| Check | Baseline | Concurrent REPACK |
| --- | --- | --- |
| Run outcome | `converged` | `converged` |
| Verified snapshot ID | `4408359321324628831` | `6145901343645389365` |
| Schema check (`schema_id`) | `passed=True` (`schema_id=0`) | `passed=True` (`schema_id=0`) |
| Source / Lake rows | `250019` / `250019` | `250019` / `250019` |
| Source-only / Lake-only / Duplicate lake IDs | `0` / `0` / `0` | `0` / `0` / `0` |
| Value mismatch rows | `0` | `0` |
| Final marker commit-to-visibility | `3863.67 ms` | `3957.76 ms` |
| Final marker commit to verified match | `15979.33 ms` | `13868.56 ms` |
| Full diff duration | `11844.98 ms` | `9625.44 ms` |

## 2. Concurrent REPACK Execution, Sizes and Sampling

- Command: `REPACK (CONCURRENTLY) public.orders` (PID `410`), outcome: `ok`
- Window interval: `30030.984 ms` to `32208.399 ms` (duration `2177.415 ms`)
- Writes overlapping REPACK: `22` committed during REPACK (`22` strictly contained, `22` any overlap)
- Relation file swap confirmed: `heap_relfilenode_changed=True`, `toast_relfilenode_changed=True`
- Table / index sizes before -> after: heap `46661632` -> `46710784` bytes; TOAST `414826496` -> `414310400` bytes; indexes `5636096` -> `5644288` bytes; total `467165184` -> `466706432` bytes
- Progress samples: `36` total (`35` with `pg_stat_progress_repack` row) at `0.05 s` cadence
- Phases observed: `['initializing', 'seq scanning heap', 'rebuilding index']`
- Temporary repack slots observed: `['pg_repack_412']`
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
| Scheduled-to-completed latency | `p50=5.726 ms, p99=12.959 ms, max=138.041 ms (n=1800)` | `p50=5.75 ms, p99=23.25 ms, max=115.075 ms (n=1800)` |
| Execution latency | `p50=5.13 ms, p99=11.139 ms, max=136.918 ms (n=1800)` | `p50=5.162 ms, p99=23.215 ms, max=113.949 ms (n=1800)` |
| Queue delay | `p50=0.683 ms, p99=1.458 ms, max=38.116 ms (n=1800)` | `p50=0.701 ms, p99=1.294 ms, max=15.203 ms (n=1800)` |
| Steady-state reader lag probes | `8/8` visible, `0` errors, `p50=11041.745 ms, p99=13391.985 ms, max=13414.12 ms (n=8)` | `8/8` visible, `0` errors, `p50=10795.915 ms, p99=13667.18 ms, max=13678.67 ms (n=8)` |

### Matched REPACK Interval (`30030.984 ms` to `32208.399 ms`, duration `2177.415 ms`)

| Metric (ops scheduled in interval) | Baseline (matching interval) | Concurrent REPACK |
| --- | --- | --- |
| Attempted / Completed ops | `22` / `22` | `22` / `22` |
| Errors / Timeouts | `0` / `0` | `0` / `0` |
| Completed throughput | `10.104 ops/s` | `10.104 ops/s` |
| Scheduled-to-completed latency | `p50=5.728 ms, p99=7.052 ms, max=7.158 ms (n=22)` | `p50=14.545 ms, p99=33.468 ms, max=33.888 ms (n=22)` |
| Execution latency | `p50=5.133 ms, p99=6.429 ms, max=6.449 ms (n=22)` | `p50=13.93 ms, p99=32.41 ms, max=32.925 ms (n=22)` |
| Queue delay | `p50=0.547 ms, p99=0.894 ms, max=0.917 ms (n=22)` | `p50=0.636 ms, p99=1.33 ms, max=1.417 ms (n=22)` |
