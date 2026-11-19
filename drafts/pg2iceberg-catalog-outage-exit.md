# Draft upstream report: `pg2iceberg run` exits fatally on transient catalog outage during `materializer.cycle()`

Local draft only (not submitted upstream).

## Summary

In `polynyadev/pg2iceberg:0.12.0`, single-process `pg2iceberg run` terminates with exit code `1` on the first `materializer.cycle()` that encounters an unreachable Iceberg REST catalog (for example a brief Lakekeeper restart). Terminating the process stops WAL staging and disconnects the PostgreSQL logical replication slot (`pg_replication_slots.active = false`), even though staging to object storage and `_pg2iceberg.log_index` was succeeding using cached vended credentials.

By contrast, `pg2iceberg` split mode (`stream-only` plus `materializer-only --worker-id worker-1`, using the same `pg2iceberg.yaml`) survives the same outage: `stream-only` keeps the replication slot active, `materializer-only` logs a warning and retries on the next interval (`crates/pg2iceberg/src/run.rs:478-484`), and a new Iceberg snapshot is committed automatically once the catalog returns. `docs/architecture/materializer.md` also states:

> A table that fails doesn't stop the others; the cycle reports the failure once they're done, and the next cycle takes the table up again.

## Verified source location (`v0.12.0`)

- `crates/pg2iceberg-validate/src/runtime.rs:71` (`run_materialize_tick`): propagates `MaterializerError` from `materializer.cycle().await?`.
- `crates/pg2iceberg-validate/src/runtime.rs:1404` (`Ticks::finish`): comment states `// cycle failure is fatal (propagates); compaction failure is ...`, causing `run_logical_lifecycle` to return `Err(LifecycleError::Materialize(...))`, which exits `pg2iceberg run` with status `1`.
- `crates/pg2iceberg/src/run.rs:478-484` (`run_materializer_only`): catches `Err(e)` from `materializer.cycle().await`, logs `tracing::warn!(error = %e, "materializer cycle failed; will retry next interval")`, and keeps the worker alive.

## Standalone reproduction

A self-contained reproduction requiring only Docker Compose, `curl`, and POSIX `/bin/sh` is in [`repro/pg2iceberg-catalog-outage/`](../repro/pg2iceberg-catalog-outage/) (`docker-compose.yml`, `pg2iceberg.yaml`, `repro.sh`, `README.md`). It uses only public multi-arch images pinned by digest: PostgreSQL 17 (`postgres:17-bookworm`), Lakekeeper `v0.15.1`, Silo (`RELEASE.2026-09-03T13-18-01Z`), and `polynyadev/pg2iceberg:0.12.0`.

Run from `repro/pg2iceberg-catalog-outage/`:

```sh
./repro.sh down
./repro.sh
```

What it does: starts the stack and bootstraps bucket and warehouse `repro`; creates `public.items` with a primary key and inserts 5 rows; starts `pg2iceberg run` and waits for the first Iceberg snapshot in Lakekeeper; stops `lakekeeper`, inserts 5 rows and updates 1 row, and waits 30 s; prints `docker inspect`, the last 25 log lines of `pg2iceberg`, and `pg_replication_slots`; starts `lakekeeper`, waits 30 s, and prints the same checks again.

Exact output observed across 3 of 3 clean runs (`runs/20261119T232348Z-r1-catalog-outage-repro/`):

```text
Error: logical lifecycle

Caused by:
    0: materializer.cycle: catalog: other: catalog: other: Unexpected => Failed to execute http request, source: error sending request for url (http://lakekeeper:8181/catalog/v1/<warehouse-id>/namespaces/public/tables/items)
```

```text
RESULT: pg2iceberg exited with code 1 while the catalog was down; slot active=false
```

`repro.sh` exits with status `1` when the fatal exit reproduces and `0` if `pg2iceberg` stays running across the outage.

### Split-mode comparison with the same `pg2iceberg.yaml`

Running the same steps with `stream-only` and `materializer-only --worker-id worker-1`:

```sh
./repro.sh down
./repro.sh split
```

Exact output observed (`exit 0`):

```text
RESULT: split mode survived catalog outage (stream=running exit=0, materializer=running exit=0); slot active=true; snapshot_advanced=true
```

Note on split-mode startup: starting `stream-only` and `materializer-only` simultaneously against an empty catalog can fail `materializer-only` during startup with `Error: register public.items` / `catalog: conflict: TableAlreadyExists`. `repro.sh split` starts `stream-only` until the initial snapshot exists, then starts `materializer-only`.

Cleanup removes only Compose project `p2irepro`:

```sh
./repro.sh down
```

### Additional lab evidence

Full workload runs (`25,000` rows, `10 ops/s`, `60 s` Lakekeeper outage on PostgreSQL 19.0) are preserved under `runs/20261119T210838Z-l3-catalog-outage/` and `runs/20261119T212047Z-l3-catalog-outage-recovery/`. Restarting `pg2iceberg` after Lakekeeper returned resumed cleanly from `pg2iceberg_slot` and `_pg2iceberg.log_index` with zero row loss or duplication, leaving `2` uncommitted Parquet files (from the aborted pre-crash commit) in object storage for orphan cleanup.

## Suggested upstream improvement

In `crates/pg2iceberg-validate/src/runtime.rs` (`Ticks::finish`), treat transient catalog/storage `MaterializerError` from `materializer.cycle()` the same way `run_materializer_only` does (log `WARN`/`ERROR`, update failure metrics, leave `mat_cursor` unchanged, and retry on the next interval with backoff) while keeping the flusher and replication slot running, or expose an explicit config option to choose between retry-in-process and fail-fast exit. Also align `docs/architecture/materializer.md` with single-process `run` behavior if fail-fast exit is intentional.
