# Minimal Reproduction: `pg2iceberg run` Exits on Transient Catalog Outage

Standalone reproduction showing that `polynyadev/pg2iceberg:0.12.0` in single-process `run` mode exits with code `1` on the first `materializer.cycle()` against an unreachable Iceberg REST catalog (dropping the PostgreSQL logical replication slot), whereas split mode (`stream-only` plus `materializer-only`) survives the same outage and resumes snapshot commits once the catalog returns.

Nothing outside this directory is required.

## Requirements

- Docker with Docker Compose v2 (`docker compose`)
- `curl`
- A POSIX `/bin/sh` (Linux, macOS, or Git Bash on Windows)

All published ports bind to `127.0.0.1` and default to `25432` (PostgreSQL), `28181` (Lakekeeper), and `29000` (Silo). Override with `P2I_PG_PORT`, `P2I_LAKEKEEPER_PORT`, and `P2I_SILO_PORT` if needed. All credentials in `docker-compose.yml` and `pg2iceberg.yaml` are synthetic local-only values.

## Pinned images

All images are pinned by multi-arch manifest index digest (`linux/amd64` and `linux/arm64`):

| Service | Image reference |
| --- | --- |
| `pg-source`, `lakekeeper-db` | `postgres:17-bookworm@sha256:837bed18782820fe967067cb5328b02b0d9992c1be9a37ee0c9c492cfc03af3b` |
| `silo` | `pgsty/silo:RELEASE.2026-09-03T13-18-01Z@sha256:b616a0cf8cb281e7e6bb3c9b1fb53875b4016a2878223925541c18f82d6c5ca3` |
| `lakekeeper`, `lakekeeper-migrate` | `quay.io/lakekeeper/catalog:v0.15.1@sha256:22dcb6044c106a962c242b70051bd226bd50c9134c763121d8713c37fe9a70ec` |
| `pg2iceberg` (`run`, `stream-only`, `materializer-only`) | `polynyadev/pg2iceberg:0.12.0@sha256:c6b97dc3e4d1e41deee3bcd84c1dc8c655c24b0bbc2061a087afc27b242a7916` |

## Run

Single-process reproduction (`pg2iceberg run`):

```sh
./repro.sh down
./repro.sh
```

What `./repro.sh` does:

1. Starts PostgreSQL 17 (`wal_level=logical`), Silo, and Lakekeeper `v0.15.1` under Compose project `p2irepro`, and bootstraps bucket `repro` and warehouse `repro` (`endpoint: http://silo:9000`, vended STS credentials).
2. Creates `public.items` with a primary key, inserts 5 rows, starts `pg2iceberg run`, and waits until Lakekeeper serves a `current-snapshot-id`.
3. Stops `lakekeeper`, inserts 5 rows and updates 1 row, and waits 30 seconds.
4. Prints `docker inspect` state and exit code, the last 25 log lines of `pg2iceberg`, and `pg_replication_slots`.
5. Starts `lakekeeper`, waits 30 seconds, and prints the same checks again.
6. Ends with a `RESULT:` line. Exits non-zero (`1`) when the reported exit reproduces, and `0` when `pg2iceberg` stays running across the outage.

Expected output on `polynyadev/pg2iceberg:0.12.0`:

```text
RESULT: pg2iceberg exited with code 1 while the catalog was down; slot active=false
```

Split-mode comparison (`pg2iceberg stream-only` plus `pg2iceberg materializer-only --worker-id worker-1`, using the same `pg2iceberg.yaml`):

```sh
./repro.sh down
./repro.sh split
```

Expected output on `polynyadev/pg2iceberg:0.12.0`:

```text
RESULT: split mode survived catalog outage (stream=running exit=0, materializer=running exit=0); slot active=true; snapshot_advanced=true
```

## Cleanup

Removes only Compose project `p2irepro` (containers, volumes, and network):

```sh
./repro.sh down
```
