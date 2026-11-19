# CDC Convergence Lab

Local lab testing whether a PostgreSQL 19 source table (`public.orders`) can undergo `REPACK (CONCURRENTLY)` under steady write load while its Apache Iceberg replica (PostgreSQL 19 -> pg2iceberg -> Lakekeeper -> Silo -> DuckDB) converges to the exact rows, values and schema.

## Layout

- `compose/`: Docker Compose project (`cdclab`), `.env`, PostgreSQL 19.0 tarball Dockerfile, `pg2iceberg` configuration, pinned Spark + Iceberg image (`profile spark`), and Keycloak OIDC realm (`profile authz`).
- `lab/`: bootstrap, exact batched verifier (`compare.py`), seeded TOAST workload (`workload.py`), REPACK runner and sampler (`repack.py`), SVG plot generator (`plots.py`), M0-M3 runners (`m0.py` through `m3.py`), and Lakekeeper follow-up runners (`l1.py`, `l2.py`, `l3.py`).
- `tests/`: fault-injection fixtures for the verifier and unit tests for workload, REPACK analysis, repeated reporting, and L1-L3 adapters/helpers.
- `runs/`: raw committed evidence per run, matched pair, repeated series, and Lakekeeper follow-up drills.
- `reports/`: milestone reports (`M0.md` through `M3.md`, `L1.md` through `L3.md`, and `L1,L2,L3.md`).
- `drafts/`: local drafts for suspected upstream issues.
- `docs/PLAN.md`: lab plan and milestone pass criteria.

## Prerequisites

- Docker Desktop running on `127.0.0.1`.
- `uv` with the locked Python environment (`uv sync`).
- Build `pg-source` once from the official PostgreSQL 19.0 release tarball:
  ```text
  docker compose -p cdclab --env-file compose/.env -f compose/docker-compose.yml build pg-source
  ```

## Documented Entry Points

### 1. Run one fresh matched pair (baseline vs concurrent REPACK)

```text
uv run python -m lab.m2 --seed-rows 250000 --toast-rows 25000 --duration-s 180 --rate-ops-s 10 --repack-delay-s 30 --order baseline-first --label m2
```

Regenerate a single matched pair's summary and Markdown table from two raw run directories:

```text
uv run python -m lab.m2 --render-from-runs <baseline_run_dir> <repack_run_dir> --output-dir <pair_dir>
```

### 2. Run three fresh counterbalanced matched pairs (plus plain REPACK control)

```text
uv run python -m lab.m3 --pairs 3 --seed-rows 250000 --toast-rows 25000 --duration-s 180 --rate-ops-s 10 --repack-delay-s 30 --base-rng-seed 20261119 --with-plain-repack-control --label m3
```

Regenerate the repeated summary JSON, Markdown results table, and all SVG plots from raw run files:

```text
uv run python -m lab.m3 --render-from-manifest <repeat_dir>/manifest.json --output-dir <repeat_dir>
```

### 3. Lakekeeper follow-up milestones (L1-L3)

Build the pinned Spark + Iceberg image once (for L1 and L2):

```text
docker compose -p cdclab --env-file compose/.env -f compose/docker-compose.yml --profile spark build spark
```

Run L1 (second reader: Spark vs DuckDB on the same equality-delete snapshot), L2 (OpenFGA + Keycloak OIDC scoped access and vended STS checks), and L3 (60 s Lakekeeper catalog outage drill):

```text
uv run python -m lab.l1
uv run python -m lab.l2
uv run python -m lab.l3
uv run python -m lab.l3 --restart-connector-after-recovery --label l3-catalog-outage-recovery
```

### 4. Run unit and fault-injection tests

```text
uv run pytest -q
```
