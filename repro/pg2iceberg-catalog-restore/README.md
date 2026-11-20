# Minimal Reproduction: pg2iceberg Skips Changes After the Catalog Database Is Restored

Standalone reproduction showing that `pg2iceberg run` keeps committing on top of an older Iceberg snapshot when the Iceberg REST catalog's own database is restored from an earlier backup while pg2iceberg is running. pg2iceberg's cursor in PostgreSQL (`_pg2iceberg.mat_cursor`) stays ahead of the restored table. The next commit starts at the cursor, and the change-log range between the restored snapshot's `pg2iceberg.log-end` and the cursor is never applied. There is no error or warning, and `pg2iceberg verify` reports the diffs.

The script runs against two images: `polynyadev/pg2iceberg:0.12.0` and `polynyadev/pg2iceberg:sha-4344888` (main at commit `4344888eb6`, which also records `pg2iceberg.log-ends` in the table properties). Both show the same result.

Nothing outside this directory is required.

## Requirements

- Docker with Docker Compose v2 (`docker compose`)
- `curl`
- A POSIX `/bin/sh` with `awk` and `sed` (Linux, macOS, or Git Bash on Windows)

A run takes about one minute after the images are pulled. All published ports bind to `127.0.0.1` and default to `35432` (PostgreSQL), `38181` (Lakekeeper), and `39000` (Silo). Override with `P2I_PG_PORT`, `P2I_LAKEKEEPER_PORT`, and `P2I_SILO_PORT`. All credentials are synthetic and local only.

## Pinned images

All images are pinned by multi-arch manifest index digest (`linux/amd64` and `linux/arm64`). The infrastructure images are in `docker-compose.yml`; the pg2iceberg image is chosen by `repro.sh` from `P2I_VERSION`.

| Service | Image reference |
| --- | --- |
| `pg-source`, `lakekeeper-db` | `postgres:17-bookworm@sha256:837bed18782820fe967067cb5328b02b0d9992c1be9a37ee0c9c492cfc03af3b` |
| `silo` | `pgsty/silo:RELEASE.2026-09-03T13-18-01Z@sha256:b616a0cf8cb281e7e6bb3c9b1fb53875b4016a2878223925541c18f82d6c5ca3` |
| `lakekeeper`, `lakekeeper-migrate` | `quay.io/lakekeeper/catalog:v0.15.1@sha256:22dcb6044c106a962c242b70051bd226bd50c9134c763121d8713c37fe9a70ec` |
| `pg2iceberg`, `P2I_VERSION=0.12.0` (default) | `polynyadev/pg2iceberg:0.12.0@sha256:c6b97dc3e4d1e41deee3bcd84c1dc8c655c24b0bbc2061a087afc27b242a7916` |
| `pg2iceberg`, `P2I_VERSION=main` | `polynyadev/pg2iceberg:sha-4344888@sha256:964f0be1f4d038947d5a85905dbbc04733292d20cdcc33eb2224e1dcb663cd68` |

`P2I_PG2ICEBERG_IMAGE` overrides both with any image reference.

## Run

```sh
./repro.sh down
./repro.sh                      # polynyadev/pg2iceberg:0.12.0
./repro.sh down
P2I_VERSION=main ./repro.sh     # polynyadev/pg2iceberg:sha-4344888
```

What `./repro.sh` does, each step printed with a UTC timestamp:

1. Starts PostgreSQL 17 (`wal_level=logical`), Silo and Lakekeeper `v0.15.1` under Compose project `p2irestore`, and bootstraps bucket `repro` and warehouse `repro` (vended credentials).
2. Creates `public.items` with a primary key and 30 rows.
3. Starts `pg2iceberg run` (`flush_interval: 2s`, `materializer_interval: 8s`) and waits for the first snapshot. That snapshot carries no log range.
4. Inserts 5 rows and waits until it is committed, so the backup holds a snapshot with a `pg2iceberg.log-end`.
5. Takes `pg_dump` of the Lakekeeper catalog database.
6. Inserts, updates and deletes rows in four transactions and waits until at least two more snapshots are committed and the cursor, the end of the staged log and the table's `pg2iceberg.log-end` are equal.
7. Stops Lakekeeper, drops and recreates its database, restores the dump, and starts Lakekeeper. pg2iceberg keeps running. Lakekeeper's table is back at the backed-up snapshot while `_pg2iceberg.mat_cursor` is not.
8. Inserts one row and updates another in one transaction, and waits for the next snapshot.
9. Prints the snapshot list with each snapshot's `pg2iceberg.log-start` and `pg2iceberg.log-end`, pg2iceberg's warnings and errors since the restore began, and the output of `pg2iceberg verify`.
10. Ends with one `RESULT:` line.

Exit codes: `1` when a gap in the applied log range exists (the skip reproduces), `0` when it does not, `2` when the run is invalid. A run is invalid when a precondition fails, for example fewer than two snapshots landed after the backup, the catalog did not roll back, or pg2iceberg exited during the restore window. An invalid run prints `RESULT: invalid run (...)` and no verdict.

Expected output, identical for `0.12.0` and `main`:

```text
RESULT: gap in the applied log range after the catalog restore: yes; verify reported diffs (exit 1); pg2iceberg warnings or errors since the restore: 0
```

The line before it names the gap, for example `gaps=(6,29]`: the restored snapshot ended at log offset 6, and the next commit starts at 29. The offsets can differ from run to run with batching. The `pg2iceberg verify` output lists `pg_only=15` (rows written after the backup), `iceberg_only=3` (rows deleted after the backup, back in Iceberg) and `mismatched=5` (rows updated after the backup).

### Control run

`P2I_CONTROL=1 ./repro.sh` skips step 7. Nothing is restored, so the same detector and the same `verify` must report no gap and no diffs:

```text
RESULT: gap in the applied log range after the catalog restore: no; verify reported no diffs (exit 0); pg2iceberg warnings or errors since the restore: 0
```

## Cleanup

Removes only Compose project `p2irestore` (containers, volumes and network):

```sh
./repro.sh down
```
