# Draft upstream report: `pg2iceberg run` exits fatally on transient catalog outage during `materializer.cycle()`

Local draft only (not submitted upstream).

## Summary

In `polynyadev/pg2iceberg:0.12.0`, single-process `pg2iceberg run` terminates with exit code `1` on the first `materializer.cycle()` that encounters an unreachable Iceberg REST catalog (for example a 60-second Lakekeeper restart). Terminating the process stops WAL staging and disconnects the PostgreSQL logical replication slot, even though staging to object storage and `_pg2iceberg.log_index` was succeeding using cached vended credentials.

By contrast, `pg2iceberg` distributed `materializer-only` mode logs a warning and retries on the next interval (`crates/pg2iceberg/src/run.rs:478-484`), and `docs/architecture/materializer.md` states:

> A table that fails doesn't stop the others; the cycle reports the failure once they're done, and the next cycle takes the table up again.

## Verified source location (`v0.12.0`)

- `crates/pg2iceberg-validate/src/runtime.rs:71` (`run_materialize_tick`): propagates `MaterializerError` from `materializer.cycle().await?`.
- `crates/pg2iceberg-validate/src/runtime.rs:1404` (`Ticks::finish`): comment states `// cycle failure is fatal (propagates); compaction failure is ...`, causing `run_logical_lifecycle` to return `Err(LifecycleError::Materialize(...))`, which exits `pg2iceberg run` with status `1`.
- `crates/pg2iceberg/src/run.rs:478-484` (`run_materializer_only`): catches `Err(e)` from `materializer.cycle().await`, logs `tracing::warn!(error = %e, "materializer cycle failed; will retry next interval")`, and keeps the worker alive.

## Minimal reproduction observed in this lab

- Environment: PostgreSQL `19.0`, `polynyadev/pg2iceberg:0.12.0` (`sha256:4d76b19dedfdb36adf6b01978bb1c9460d178ab83603fe9c7efadd01b61d3b6b`), Lakekeeper `v0.15.1`, Silo `RELEASE.2026-09-03T13-18-01Z`.
- Evidence runs: `runs/20261119T210838Z-l3-catalog-outage/` and `runs/20261119T212047Z-l3-catalog-outage-recovery/`.
- Steps:
  1. Start `pg2iceberg run` in single-process logical mode (`flush_interval: 2s`, `credential_mode: vended`) under a steady `10 ops/s` write workload.
  2. Stop Lakekeeper for `60 s` while PostgreSQL continues accepting writes.
  3. Observe `_pg2iceberg.log_seq` advance for ~`10 s` (`+5` staged log files uploaded to S3), followed by process exit code `1` on the first materializer tick:
     ```text
     Error: logical lifecycle

     Caused by:
         0: materializer.cycle: catalog: other: catalog: other: Unexpected => Failed to execute http request, source: error sending request for url (http://lakekeeper:8181/catalog/v1/<warehouse-id>/namespaces/public/tables/orders)
     ```
  4. Restarting `pg2iceberg` after Lakekeeper returns healthy resumes cleanly from `pg2iceberg_slot` and `_pg2iceberg.log_index` with zero row loss or duplication, leaving `2` uncommitted Parquet files (from the aborted pre-crash commit) in S3 for orphan cleanup.

## Suggested upstream improvement

In `crates/pg2iceberg-validate/src/runtime.rs` (`Ticks::finish`), treat transient catalog/storage `MaterializerError` from `materializer.cycle()` the same way `run_materializer_only` does (log `WARN`/`ERROR`, update failure metrics, leave `mat_cursor` unchanged, and retry on the next interval with backoff) while keeping the flusher and replication slot running, or expose an explicit config option to choose between retry-in-process and fail-fast exit. Also align `docs/architecture/materializer.md` with single-process `run` behavior if fail-fast exit is intentional.
