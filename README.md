# CDC Convergence Lab

Local lab testing whether a PostgreSQL 19 source table (`public.orders`) can undergo `REPACK (CONCURRENTLY)` under steady write load while its Apache Iceberg replica (PostgreSQL 19 -> pg2iceberg -> Lakekeeper -> Silo -> DuckDB) converges to the exact rows, values and schema.

## Layout

- `compose/`: Docker Compose project (`cdclab`), `.env`, PostgreSQL 19.0 tarball Dockerfile, and `pg2iceberg` configuration.
- `lab/`: bootstrap, exact batched verifier (`compare.py`), seeded TOAST workload (`workload.py`), REPACK runner and sampler (`repack.py`), SVG plot generator (`plots.py`), and milestone runners (`m0.py`, `m1.py`, `m2.py`, `m3.py`).
- `tests/`: fault-injection fixtures for the verifier and unit tests for workload, REPACK analysis, and repeated reporting.
- `runs/`: raw committed evidence per run, matched pair, and repeated series.
- `reports/`: milestone reports (`M0.md` through `M3.md`).
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

### 3. Run unit and fault-injection tests

```text
uv run pytest -q
```
