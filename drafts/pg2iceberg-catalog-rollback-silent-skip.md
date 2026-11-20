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

## Minimal reproduction

`repro/pg2iceberg-catalog-restore/` in this repository reproduces it in about one minute, with 35 rows and no lab code (Docker Compose, `curl`, POSIX `sh`). It backs up Lakekeeper's catalog database after the first snapshots, applies four more transactions, waits for two more snapshots, restores the dump while pg2iceberg keeps running, writes one more change, and compares the snapshot log ranges.

```sh
./repro.sh down
./repro.sh                      # polynyadev/pg2iceberg:0.12.0
./repro.sh down
P2I_VERSION=main ./repro.sh     # polynyadev/pg2iceberg:sha-4344888 (main at 4344888eb6)
```

Both images print the same final line and exit with code 1 (output of three clean runs per image in `reports/R2.md`):

```text
RESULT: gap in the applied log range after the catalog restore: yes; verify reported diffs (exit 1); pg2iceberg warnings or errors since the restore: 0
```

The step before it lists the snapshots, for example `gaps=(6,29]`: the restored snapshot ends at log offset 6, `_pg2iceberg.mat_cursor` stays at 29, and the next commit has `pg2iceberg.log-start=29`. `pg2iceberg verify` reports `pg_only=15`, `iceberg_only=3` and `mismatched=5`. `P2I_CONTROL=1 ./repro.sh` skips the restore and prints `gap ... no; verify reported no diffs (exit 0)`.

## Behaviour on main (`4344888eb6`, 2026-11-17)

Main still skips silently. It records the end of the applied log per consumer group in the table property `pg2iceberg.log-ends` (and in each snapshot's summary), and reads it in `Materializer::begin_unit` (`crates/pg2iceberg-logical/src/materializer.rs`) before it folds a unit:

```rust
let applied = self.sync_file_index(ident).await?;
if let Some(&end) = applied.get(&self.unit_group(unit)) {
    if end > start {
        return Err(MaterializerError::AlreadyApplied { end });
    }
}
```

`AlreadyApplied` moves the cursor forward to `end` (`apply_table`). Nothing handles `end < start`, which is the rolled-back catalog: the unit starts at the cursor and commits on top of the restored snapshot. The other guard, `log_continues`, compares the cursor with the first staged log entry (`LogGap`), so it catches a truncated log but not a table that is behind its cursor. `docs/architecture/materializer.md` says the same: "if a commit already applied past it, it moves the cursor there and goes on from the end."

Suggested change, as above: when the recorded `pg2iceberg.log-end` for the group is less than the cursor, fail the cycle with an error that names the table, the cursor and the recorded end. In the K2 drill on 0.12.0 the staged log and `_pg2iceberg.log_index` still held the skipped range, so replaying from the recorded end may also be possible; R2 did not check that on main.
