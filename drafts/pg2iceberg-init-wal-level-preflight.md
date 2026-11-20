# Draft upstream note: `pg2iceberg init` marks `wal_level = replica` as a failure on PostgreSQL 19

Local draft only (not submitted upstream).

## Summary

On PostgreSQL 19.0 a source with `wal_level = replica` accepts logical decoding as soon as the first logical slot exists: the server raises `effective_wal_level` to `logical` by itself and lowers it again when the last logical slot is dropped. `pg2iceberg run` (0.12.0) works on such a source. `pg2iceberg init` still prints a failing check and tells the operator to change `wal_level` and restart:

```text
✗ wal_level is replica — set wal_level = logical and restart Postgres (RDS / Aurora: rds.logical_replication = 1 in the parameter group; Cloud SQL: the cloudsql.logical_decoding flag)
...
fix what's marked ✗ before starting pg2iceberg
```

The check reads `SHOW wal_level`, so it stays red while the connector is running and `effective_wal_level` is `logical`. Nothing blocks `run`; only the advice is wrong for PostgreSQL 19.

## Verified source location (`v0.12.0`, commit `01c2e172eb5e7be82c60b77f20cb089354c06ce6`)

- `crates/pg2iceberg/src/init.rs:97-107`: `SHOW wal_level`, failing when the value is not `logical`.
- `crates/pg2iceberg/src/init.rs:339` and `:359`: failing checks print as `✗ ... — <fix>`, followed by "fix what's marked ✗ before starting pg2iceberg".
- No other file in the tree reads `wal_level`; `run` creates the slot with `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput NOEXPORT_SNAPSHOT` (`crates/pg2iceberg-pg/src/prod/client.rs:305-314`).
- `docs/getting-started/installation.md:57`, `docs/getting-started/quickstart.md:25` and `docs/usage/configuration.md:81` state `wal_level = logical` as a requirement.

## Evidence

`runs/20261120T134349Z-w1-wal-level-replica/` (report: `reports/W1.md`). Fresh PostgreSQL 19.0, `wal_level = replica`, zero slots, 2,500-row `orders` table: `pg2iceberg run` created its slot, took the initial snapshot, replicated 600 workload operations, and the exact comparison matched (`2,520 = 2,520`). `init` printed the failing check before the slot existed and again while the connector was running with `effective_wal_level = logical`.

## Possible change

Read `effective_wal_level` where the server has it (PostgreSQL 19 and later) and pass when it is `logical`. When `wal_level = replica` on 19 or later, report the check as advisory ("logical decoding turns on when the first logical slot is created") instead of failing it. Servers before 19 keep the current check. The behaviour of `CREATE_REPLICATION_SLOT` at `wal_level = replica` on those servers was not tested here.
