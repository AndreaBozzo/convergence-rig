Title: After a catalog restore, `run` commits on top of the older snapshot and skips the changes since

Hi, another one from the same catalog testing as #154, this time restoring the catalog's database from a backup.

If the REST catalog (Lakekeeper 0.15.1) is restored from a `pg_dump` taken a few commits earlier, pg2iceberg keeps running and its next cycle commits on top of the restored snapshot, starting from its own cursor. The log range in between is never applied, and nothing is logged:

```text
before the restore    seq 4  log-start 21  log-end 29   log-ends={"default":29}  mat_cursor 29
after the restore     seq 2  log-start 0   log-end 6    log-ends={"default":6}   mat_cursor 29
next commit           seq 3  log-start 29  log-end 31   log-ends={"default":31}  mat_cursor 31
```

Offsets 7 to 29 are gone from the table: `pg2iceberg verify` then reports `pg_only=15, iceberg_only=3, mismatched=5`, while the `run` log has no warning or error after the restore. On 0.12.0 a re-snapshot (`cleanup`, drop the table, `snapshot`) recovers an exact match.

Same on 0.12.0 and on main (`sha-4344888`), 3 of 3 runs each. As far as I can tell, `Materializer::begin_unit` (`crates/pg2iceberg-logical/src/materializer.rs`) only handles a table that is ahead of the cursor (`end > start`, `AlreadyApplied`); a table whose recorded `pg2iceberg.log-ends` is behind the cursor goes through. Failing the cycle there, with the table, the recorded end and the cursor in the error, would turn this into a visible stop instead of a silent gap.

Repro (Postgres 17, Lakekeeper 0.15.1, Silo, pg2iceberg pinned by digest; `P2I_VERSION=main` for main): https://github.com/AndreaBozzo/convergence-rig/tree/main/repro/pg2iceberg-catalog-restore

Happy to send a PR for the check if that's the behaviour you want.

Filed 2026-11-20 as https://github.com/polynya-dev/pg2iceberg/issues/156
