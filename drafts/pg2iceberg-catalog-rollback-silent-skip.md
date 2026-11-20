# Draft upstream note: `pg2iceberg` silently skips staged offsets and forks table history when the Iceberg catalog rolls back behind `mat_cursor`

Local draft only (not submitted upstream).

## Summary

In `polynyadev/pg2iceberg:0.12.0`, when the Iceberg REST catalog metadata database (Lakekeeper `v0.15.1` on PostgreSQL 17) is restored from an earlier `pg_dump` backup while `pg2iceberg` is running, the catalog's current snapshot (`pg2iceberg.log-end`) moves behind PostgreSQL's `_pg2iceberg.mat_cursor`. On the next materializer cycle, `pg2iceberg` does not detect or reject this backward discrepancy. Instead, it logs zero warnings and zero errors, keeps `/healthz` and `/readyz` healthy, and commits a new Iceberg snapshot branching off the older restored snapshot with `pg2iceberg.log-start` equal to the ahead `mat_cursor`.

Every staged change between the restored snapshot's `pg2iceberg.log-end` and `_pg2iceberg.mat_cursor` is silently skipped, even though the staged Parquet batches and `_pg2iceberg.log_index` entries are still present in object storage and PostgreSQL. Subsequent updates to out-of-line TOAST rows under `REPLICA IDENTITY DEFAULT` also read the stale pre-backup TOAST values from the rolled-back table and combine them with post-restore scalar columns, producing hybrid rows that never existed in PostgreSQL.

## Observed behavior in K2 drill (`pg2iceberg 0.12.0` + Lakekeeper `v0.15.1`)

Raw evidence: `runs/20261120T122551Z-k2-catalog-restore-drill/` (summary in `reports/K2.md`).

1. At `t = 39.26 s` into a `25,000`-row, `10 ops/s` workload, `pg_dump` captured `lakekeeper-db` at snapshot `1376350308479531387` (`sequence_number=8`, `pg2iceberg.log-end=386`, `_pg2iceberg.mat_cursor=386`).
2. `pg2iceberg` committed `3` more snapshots before restore:
   - `682512011106338716` (`seq=9`, `log-start=386..487`)
   - `420035708574599370` (`seq=10`, `log-start=487..588`)
   - `1440282023235694939` (`seq=11`, `log-start=588..688`)
   `_pg2iceberg.mat_cursor` reached `688`.
3. Lakekeeper was stopped, `lakekeeper` DB was dropped, recreated, and restored from the backup at `t = 39.26 s`, and Lakekeeper restarted (`3.29 s` total catalog downtime). Lakekeeper returned at snapshot `1376350308479531387` (`seq=8`, `log-end=386`) while `_pg2iceberg.mat_cursor` remained `688` (`302` offsets ahead).
4. `pg2iceberg` stayed alive across the restore (`running=true`) and emitted zero `WARN` or `ERROR` lines. On its next materializer cycle (`12:27:40.657Z`), it committed snapshot `5716965016479734296` with:
   - `parent_snapshot_id = 1376350308479531387` (`seq=8`, `pg2iceberg.log-end=386`)
   - `sequence_number = 9` (reusing sequence number `9` on a new forked branch)
   - `pg2iceberg.log-start = 688`, `pg2iceberg.log-end = 789`
5. `pg2iceberg` continued committing `11` post-restore snapshots (`seq=9..19`, up to `log-end=1811`) with zero errors in `pg2iceberg_post_drill.log` and zero errors in `lakekeeper_post_drill.log`.
6. Both the independent DuckDB exact verifier (`lab/compare.py`) and `pg2iceberg verify` (`exit code 1`) detected `209` diverged rows across the `302` skipped offsets `(386, 688]`:
   - `39` rows present in PostgreSQL and missing from Iceberg (`38` workload inserts `1000000060..1000000097` plus steady-state probe `900000003`)
   - `48` deleted rows resurrected as ghost rows in Iceberg
   - `122` stale rows in Iceberg, including `6` hybrid TOAST rows (`id=19, 29, 43, 84, 89, 110`) where a post-restore unchanged-TOAST update merged current scalar columns with pre-dump TOAST blobs (`rev=0` instead of `rev=1` on `id=19`)

The same silent skip reproduced on an earlier trial run (`skipped_offset_count=301`, `40` missing, `47` ghost, `121` stale rows).

## Why this happens

`docs.pg2iceberg.dev` documents that before folding work, the materializer inspects `pg2iceberg.log-end` on the Iceberg table's snapshots and fast-forwards `mat_cursor` when Iceberg is ahead of PostgreSQL (protecting against duplicate application after a crash between an Iceberg commit and updating `mat_cursor`). The reverse invariant is not enforced: when the Iceberg table's latest `pg2iceberg.log-end` (`386`) is strictly less than `_pg2iceberg.mat_cursor` (`688`), the materializer still starts reading `_pg2iceberg.log_index` from `mat_cursor` (`688`) and commits directly onto the older Iceberg snapshot (`386`), leaving a permanent gap `(386, 688]`.

## Suggested upstream improvement

Before committing a materializer cycle against an Iceberg table, compare the table's current `pg2iceberg.log-end` (for the active consumer group) against `_pg2iceberg.mat_cursor`:

1. If `iceberg_log_end > mat_cursor`: keep the existing fast-forward behavior.
2. If `iceberg_log_end < mat_cursor`: treat it as catalog metadata rollback / split-brain. Fail the cycle loudly with an explicit `ERROR` (including `table`, `mat_cursor`, `iceberg_log_end`, and `current_snapshot_id`) instead of committing a gapped snapshot that forks the table timeline and permanently drops `(iceberg_log_end, mat_cursor]`.
3. Optionally document a safe replay or recovery flag when `_pg2iceberg.log_index` and staged Parquet files still cover `(iceberg_log_end, mat_cursor]`, or instruct operators to halt writers, re-register the newer `metadata.json` from object storage if un-forked, or run `pg2iceberg cleanup` + catalog table drop + `pg2iceberg snapshot`.
