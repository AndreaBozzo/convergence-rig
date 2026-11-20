# Matched Pair Results (Baseline vs Concurrent REPACK)

- Baseline evidence: `runs/20261119T234901Z-c1-p1-baseline`
- REPACK evidence: `runs/20261119T235258Z-c1-p1-repack`
- Matched parameters: `{"seed_rows": 250000, "toast_rows": 25000, "duration_s": 180.0, "rate_ops_s": 10.0, "rng_seed": 20261119, "batch_size": 2000, "probe_interval_s": 20.0, "probe_poll_interval_s": 0.5}` (`parameters_matched=True`)
- Overlap confirmed: `True` (`22` writes committed during REPACK)
- Final source read opened strictly after REPACK finish: `True` (`repack_end_utc=2026-11-19T23:54:06.170579Z`, `source_read_opened_utc=2026-11-19T23:56:39.151267Z`)

## 1. CDC Convergence Evidence

| Check | Baseline | Concurrent REPACK |
| --- | --- | --- |
| Run outcome | `converged` | `converged` |
| Verified snapshot ID | `7011613701619538094` | `6711568726880290289` |
| Schema check (`schema_id`) | `passed=True` (`schema_id=0`) | `passed=True` (`schema_id=0`) |
| Source / Lake rows | `249994` / `249994` | `249994` / `249994` |
| Source-only / Lake-only / Duplicate lake IDs | `0` / `0` / `0` | `0` / `0` / `0` |
| Value mismatch rows | `0` | `0` |
| Final marker commit-to-visibility | `3954.34 ms` | `4883.16 ms` |
| Final marker commit to verified match | `14301.41 ms` | `14944.94 ms` |
| Full diff duration | `10060.93 ms` | `9777.77 ms` |

## 2. Concurrent REPACK Execution, Sizes and Sampling

- Command: `REPACK (CONCURRENTLY) public.orders` (PID `392`), outcome: `ok`
- Window interval: `30034.61 ms` to `32212.829 ms` (duration `2178.219 ms`)
- Writes overlapping REPACK: `22` committed during REPACK (`22` strictly contained, `22` any overlap)
- Relation file swap confirmed: `heap_relfilenode_changed=True`, `toast_relfilenode_changed=True`
- Table / index sizes before -> after: heap `46661632` -> `46604288` bytes; TOAST `415006720` -> `414359552` bytes; indexes `5636096` -> `5644288` bytes; total `467345408` -> `466649088` bytes
- Progress samples: `35` total (`34` with `pg_stat_progress_repack` row) at `0.05 s` cadence
- Phases observed: `['initializing', 'seq scanning heap', 'rebuilding index']`
- Temporary repack slots observed: `['pg_repack_400']`
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
| Scheduled-to-completed latency | `p50=5.726 ms, p99=20.384 ms, max=81.298 ms (n=1800)` | `p50=5.764 ms, p99=23.812 ms, max=154.714 ms (n=1800)` |
| Execution latency | `p50=5.067 ms, p99=19.451 ms, max=80.613 ms (n=1800)` | `p50=5.171 ms, p99=21.416 ms, max=154.26 ms (n=1800)` |
| Queue delay | `p50=0.711 ms, p99=1.273 ms, max=21.893 ms (n=1800)` | `p50=0.7 ms, p99=1.273 ms, max=54.795 ms (n=1800)` |
| Steady-state reader lag probes | `8/8` visible, `0` errors, `p50=10598.935 ms, p99=13594.041 ms, max=13630.25 ms (n=8)` | `8/8` visible, `0` errors, `p50=9804.735 ms, p99=13262.552 ms, max=13340.2 ms (n=8)` |

### Matched REPACK Interval (`30034.61 ms` to `32212.829 ms`, duration `2178.219 ms`)

| Metric (ops scheduled in interval) | Baseline (matching interval) | Concurrent REPACK |
| --- | --- | --- |
| Attempted / Completed ops | `22` / `22` | `22` / `22` |
| Errors / Timeouts | `0` / `0` | `0` / `0` |
| Completed throughput | `10.1 ops/s` | `10.1 ops/s` |
| Scheduled-to-completed latency | `p50=6.3 ms, p99=79.694 ms, max=81.298 ms (n=22)` | `p50=17.206 ms, p99=35.45 ms, max=35.492 ms (n=22)` |
| Execution latency | `p50=5.429 ms, p99=79.038 ms, max=80.613 ms (n=22)` | `p50=16.737 ms, p99=35.099 ms, max=35.222 ms (n=22)` |
| Queue delay | `p50=0.706 ms, p99=1.128 ms, max=1.167 ms (n=22)` | `p50=0.707 ms, p99=1.098 ms, max=1.128 ms (n=22)` |
