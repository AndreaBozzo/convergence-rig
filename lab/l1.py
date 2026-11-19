"""Milestone L1 end-to-end runner: second independent Iceberg reader (Apache Spark) on the same snapshot."""

from __future__ import annotations

import argparse
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import platform
import random
import re
import sys
import time
from typing import Any

import psycopg

from lab.bootstrap import bootstrap_all
from lab.compare import (
    compare_ordered_streams,
    stream_duckdb_orders_at_snapshot,
    stream_postgres_orders,
    verify_iceberg_schema,
)
from lab.config import REPO_ROOT, load_config
from lab.l_common import (
    ALL_LAB_PROFILES,
    collect_digests_for_profiles,
    compose_cmd_with_profiles,
    project_teardown_and_verify,
)
from lab.m0 import snapshot_has_equality_deletes
from lab.pipeline import (
    capture_pg_pipeline_positions,
    extract_iceberg_snapshot_info,
    json_default,
    list_non_project_containers,
    list_project_resources,
    run_cmd,
    utc_now_iso,
    utc_run_stamp,
    wait_for_connector_and_snapshot,
    wait_for_marker_visible,
    write_json,
)
from lab.reader import (
    connect_iceberg,
    fetch_lakekeeper_table_metadata,
    read_metadata_files,
    read_snapshots,
)
from lab.schema import EXPECTED_ICEBERG_FIELDS
from lab.workload import (
    FINAL_MARKER_ID,
    _make_toast_note,
    _make_toast_payload_json,
    commit_marker_row,
    seed_orders,
    verify_toast_storage,
)

_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)
_SPARK_DECIMAL_RE = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$", re.IGNORECASE)
_DUCKDB_DECIMAL_RE = re.compile(r"^DECIMAL\(\s*(\d+)\s*,\s*(\d+)\s*\)$", re.IGNORECASE)


def latest_snapshot_id_from_duckdb(snapshots: Sequence[dict[str, Any]]) -> int | None:
    """Return the snapshot_id with the highest sequence_number regardless of row ordering."""
    if not snapshots:
        return None
    latest = max(snapshots, key=lambda s: int(s["sequence_number"]))
    return int(latest["snapshot_id"])


def canonicalize_spark_type(type_val: Any) -> str:
    """Map Spark StructType JSON field types to canonical Iceberg type names."""
    if not isinstance(type_val, str):
        return json.dumps(type_val, sort_keys=True)
    raw = type_val.strip().lower()
    m = _SPARK_DECIMAL_RE.match(raw)
    if m:
        return f"decimal({int(m.group(1))}, {int(m.group(2))})"
    mapping = {
        "long": "long",
        "string": "string",
        "timestamp": "timestamptz",
        "timestamp_ntz": "timestamp",
    }
    return mapping.get(raw, raw)


def canonicalize_duckdb_type(type_val: Any) -> str:
    """Map DuckDB DESCRIBE column_type values to canonical Iceberg type names."""
    if not isinstance(type_val, str):
        return str(type_val)
    raw = type_val.strip()
    m = _DUCKDB_DECIMAL_RE.match(raw)
    if m:
        return f"decimal({int(m.group(1))}, {int(m.group(2))})"
    upper = raw.upper()
    mapping = {
        "BIGINT": "long",
        "VARCHAR": "string",
        "TIMESTAMP WITH TIME ZONE": "timestamptz",
        "TIMESTAMPTZ": "timestamptz",
        "TIMESTAMP": "timestamp",
    }
    return mapping.get(upper, raw.lower())


def verify_spark_schema(
    spark_schema: dict[str, Any],
    expected_fields: Sequence[dict[str, Any]] = EXPECTED_ICEBERG_FIELDS,
) -> dict[str, Any]:
    """Verify Spark's StructType JSON schema against the explicit expected Iceberg field mapping."""
    actual_fields = list(spark_schema.get("fields") or [])
    mismatches: list[dict[str, Any]] = []
    actual_by_name: dict[str, dict[str, Any]] = {}
    for af in actual_fields:
        name = str(af.get("name"))
        if name in actual_by_name:
            mismatches.append({"kind": "duplicate_field_name", "field": name})
        else:
            actual_by_name[name] = af

    expected_by_name = {str(ef["name"]): ef for ef in expected_fields}
    normalized_actual: list[dict[str, Any]] = []
    for af in actual_fields:
        normalized_actual.append(
            {
                "name": af.get("name"),
                "spark_type": af.get("type"),
                "canonical_type": canonicalize_spark_type(af.get("type")),
                "required": not bool(af.get("nullable", True)),
            }
        )

    for name, ef in expected_by_name.items():
        af = actual_by_name.get(name)
        if af is None:
            mismatches.append({"kind": "missing_column", "column": name, "expected": dict(ef)})
            continue
        act_type = canonicalize_spark_type(af.get("type"))
        exp_type = str(ef["type"])
        if act_type != exp_type:
            mismatches.append(
                {
                    "kind": "type_mismatch",
                    "column": name,
                    "expected_type": exp_type,
                    "actual_type": act_type,
                    "spark_type": af.get("type"),
                }
            )
        exp_req = bool(ef["required"])
        act_req = not bool(af.get("nullable", True))
        if act_req != exp_req:
            mismatches.append(
                {
                    "kind": "required_mismatch",
                    "column": name,
                    "expected_required": exp_req,
                    "actual_required": act_req,
                }
            )

    for name, af in actual_by_name.items():
        if name not in expected_by_name:
            mismatches.append({"kind": "extra_column", "column": name, "actual": af})

    return {
        "passed": len(mismatches) == 0,
        "mismatches": mismatches,
        "actual_fields": normalized_actual,
    }


def verify_duckdb_schema(
    describe_rows: Sequence[dict[str, Any]],
    expected_fields: Sequence[dict[str, Any]] = EXPECTED_ICEBERG_FIELDS,
) -> dict[str, Any]:
    """Verify DuckDB DESCRIBE output against expected Iceberg column names and types."""
    mismatches: list[dict[str, Any]] = []
    actual_by_name: dict[str, dict[str, Any]] = {}
    normalized_actual: list[dict[str, Any]] = []
    for row in describe_rows:
        name = str(row.get("column_name"))
        dtype = row.get("column_type")
        actual_by_name[name] = dict(row)
        normalized_actual.append(
            {
                "name": name,
                "duckdb_type": dtype,
                "canonical_type": canonicalize_duckdb_type(dtype),
                "null": row.get("null"),
            }
        )

    expected_by_name = {str(ef["name"]): ef for ef in expected_fields}
    for name, ef in expected_by_name.items():
        af = actual_by_name.get(name)
        if af is None:
            mismatches.append({"kind": "missing_column", "column": name, "expected": dict(ef)})
            continue
        act_type = canonicalize_duckdb_type(af.get("column_type"))
        exp_type = str(ef["type"])
        if act_type != exp_type:
            mismatches.append(
                {
                    "kind": "type_mismatch",
                    "column": name,
                    "expected_type": exp_type,
                    "actual_type": act_type,
                    "duckdb_type": af.get("column_type"),
                }
            )

    for name, af in actual_by_name.items():
        if name not in expected_by_name:
            mismatches.append({"kind": "extra_column", "column": name, "actual": af})

    return {
        "passed": len(mismatches) == 0,
        "mismatches": mismatches,
        "actual_fields": normalized_actual,
    }


def parse_spark_ndjson_record(raw_line: str) -> tuple[Any, ...]:
    """Parse one NDJSON row exported by compose/spark/scripts/read_snapshot.py into a verifier tuple."""
    obj = json.loads(raw_line)
    row_id = obj["id"]
    if isinstance(row_id, bool) or not isinstance(row_id, int):
        raise TypeError(f"Spark id must be int, got {type(row_id).__name__}: {row_id!r}")

    status = obj["status"]

    amt_raw = obj["amount"]
    if isinstance(amt_raw, (bool, float)):
        raise TypeError(f"Spark amount must be exact decimal string, got {type(amt_raw).__name__}")
    amount = Decimal(str(amt_raw)) if amt_raw is not None else None

    ts_us = obj["updated_at_epoch_us"]
    if ts_us is None:
        updated_at: datetime | None = None
    elif isinstance(ts_us, bool) or not isinstance(ts_us, int):
        raise TypeError(f"Spark updated_at_epoch_us must be int, got {type(ts_us).__name__}")
    else:
        updated_at = _EPOCH_UTC + timedelta(microseconds=ts_us)

    note = obj["note"]
    payload = obj["payload"]

    disc_raw = obj["discount"]
    if isinstance(disc_raw, (bool, float)):
        raise TypeError(f"Spark discount must be exact decimal string, got {type(disc_raw).__name__}")
    discount = Decimal(str(disc_raw)) if disc_raw is not None else None

    return (row_id, status, amount, updated_at, note, payload, discount)


def stream_spark_orders_ndjson(
    path: Path,
    *,
    batch_size: int = 2000,
) -> Iterator[list[tuple[Any, ...]]]:
    """Stream Spark NDJSON output in bounded batches ordered by id for lab.compare."""
    batch: list[tuple[Any, ...]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            batch.append(parse_spark_ndjson_record(stripped))
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if batch:
        yield batch


def run_l1(
    *,
    seed_rows: int = 30,
    toast_rows: int = 10,
    rng_seed: int = 20261119,
    batch_size: int = 10,
    label: str = "l1-spark-reader",
    keep_up: bool = False,
) -> Path:
    """Run L1 end-to-end: PG19 -> pg2iceberg -> Lakekeeper -> DuckDB & Spark on the same snapshot."""
    cfg = load_config()
    run_dir = REPO_ROOT / "runs" / f"{utc_run_stamp()}-{label}"
    run_dir.mkdir(parents=True, exist_ok=True)

    commands_executed: list[str] = []
    non_project_before = list_non_project_containers(cfg.compose_project)

    def logged_cmd(cmd: list[str], *, check: bool = True) -> Any:
        commands_executed.append(" ".join(cmd))
        return run_cmd(cmd, check=check)

    # 1. Clean fresh start across all lab profiles.
    logged_cmd([*compose_cmd_with_profiles(cfg, ALL_LAB_PROFILES), "down", "-v", "--remove-orphans"])
    resources_after_pre_clean = list_project_resources(cfg.compose_project)
    logged_cmd([*compose_cmd_with_profiles(cfg), "up", "-d", "--wait"])

    # 2. Bootstrap bucket, Lakekeeper, warehouse, and public.orders.
    bootstrap_res = bootstrap_all(cfg)
    write_json(run_dir / "bootstrap.json", bootstrap_res)

    # 3. Seed small fixture (12 edge rows + out-of-line TOAST rows + normal rows).
    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        seed_res = seed_orders(
            pg_conn,
            seed_rows=seed_rows,
            toast_row_count=toast_rows,
            rng_seed=rng_seed,
        )

    # 4. Start pg2iceberg and wait for initial snapshot completion.
    logged_cmd([*compose_cmd_with_profiles(cfg, ("connector",)), "up", "-d", "pg2iceberg"])
    initial_snapshot_info = wait_for_connector_and_snapshot(cfg, timeout_s=180.0)

    # 5. Perform deterministic updates (edge 38-digit decimal + unchanged TOAST + changed TOAST)
    #    and deletes (edge row, TOAST row, normal row) plus a final marker row.
    rng_mut = random.Random(rng_seed + 77)
    updated_toast_note = _make_toast_note(rng_mut, 14, rev=1)
    updated_toast_payload = _make_toast_payload_json(rng_mut, 14, rev=1)
    mut_ts = datetime(2026, 11, 19, 22, 0, 0, 123457, tzinfo=timezone.utc)

    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        with pg_conn.cursor() as cur:
            # Update row 1 (edge row: last-digit decimal change, +1us timestamp, JSON reformatted)
            cur.execute(
                """
                UPDATE public.orders
                SET status = %s,
                    amount = %s,
                    updated_at = %s,
                    note = %s,
                    payload = %s::jsonb,
                    discount = %s
                WHERE id = 1
                """,
                (
                    "updated_l1",
                    Decimal("12345678901234567890123456789.123456788"),
                    mut_ts,
                    "trailing space updated  ",
                    '{"a": [1, 2, 3], "b": 3, "exact": 12345678901234567890123456789.123456788}',
                    Decimal("0.000000002"),
                ),
            )
            # Update row 13 (TOAST unchanged)
            cur.execute(
                """
                UPDATE public.orders
                SET status = 'toast_unchanged_l1',
                    amount = %s,
                    updated_at = %s,
                    discount = %s
                WHERE id = 13
                """,
                (Decimal("555.123456789"), mut_ts, Decimal("1.000000001")),
            )
            # Update row 14 (TOAST changed)
            cur.execute(
                """
                UPDATE public.orders
                SET status = 'toast_changed_l1',
                    amount = %s,
                    updated_at = %s,
                    note = %s,
                    payload = %s::jsonb,
                    discount = NULL
                WHERE id = 14
                """,
                (
                    Decimal("777.987654321"),
                    mut_ts,
                    updated_toast_note,
                    updated_toast_payload,
                ),
            )
            # Delete rows 2 (edge row), 15 (TOAST row), 25 (normal row)
            cur.execute("DELETE FROM public.orders WHERE id IN (2, 15, 25)")
        pg_conn.commit()
        toast_after_mut = verify_toast_storage(pg_conn)
        marker_commit_ns, marker_commit_utc = commit_marker_row(
            pg_conn,
            marker_id=FINAL_MARKER_ID,
            status="marker_l1_final",
            note="l1 second-reader verification marker",
        )

    # 6. Wait for marker visibility in DuckDB and confirm live equality-delete files on that snapshot.
    vis = wait_for_marker_visible(
        cfg,
        FINAL_MARKER_ID,
        marker_commit_ns,
        timeout_s=120.0,
        poll_interval_s=0.5,
    )
    if not vis["visible"] or vis["snapshot_id"] is None:
        raise RuntimeError(f"L1 final marker not visible in DuckDB within timeout: {vis}")
    target_snapshot_id = int(vis["snapshot_id"])

    lk_meta = fetch_lakekeeper_table_metadata(cfg, namespace=cfg.lakekeeper_namespace, table="orders")
    assert lk_meta is not None
    write_json(run_dir / "lakekeeper_table_final.json", lk_meta)
    lakekeeper_current_snapshot_id = int((lk_meta.get("metadata") or {}).get("current-snapshot-id"))

    # 7. Capture DuckDB snapshots, metadata, DESCRIBE, and EXPLAIN on target_snapshot_id.
    con_duck, reader_versions = connect_iceberg(cfg)
    try:
        duckdb_snapshots = read_snapshots(con_duck, namespace=cfg.lakekeeper_namespace)
        duckdb_metadata = read_metadata_files(con_duck, namespace=cfg.lakekeeper_namespace)
        desc_cur = con_duck.execute(
            f"DESCRIBE SELECT id, status, amount, updated_at, note, payload, discount "
            f"FROM cdclab.{cfg.lakekeeper_namespace}.orders AT (VERSION => {target_snapshot_id})"
        )
        desc_cols = [d[0] for d in desc_cur.description]
        duckdb_describe = [dict(zip(desc_cols, r)) for r in desc_cur.fetchall()]

        explain_rows = con_duck.execute(
            f"EXPLAIN SELECT id, status, amount, updated_at, note, payload, discount "
            f"FROM cdclab.{cfg.lakekeeper_namespace}.orders AT (VERSION => {target_snapshot_id}) "
            f"ORDER BY id"
        ).fetchall()
        duckdb_explain = [{"key": r[0], "value": r[1]} for r in explain_rows]

        explain_analyze_rows = con_duck.execute(
            f"EXPLAIN ANALYZE SELECT id, status, amount, updated_at, note, payload, discount "
            f"FROM cdclab.{cfg.lakekeeper_namespace}.orders AT (VERSION => {target_snapshot_id}) "
            f"ORDER BY id"
        ).fetchall()
        duckdb_explain_analyze = [{"key": r[0], "value": r[1]} for r in explain_analyze_rows]
    finally:
        con_duck.close()

    write_json(run_dir / "duckdb_snapshots_final.json", duckdb_snapshots)
    write_json(run_dir / "duckdb_metadata_final.json", duckdb_metadata)
    write_json(run_dir / "duckdb_describe_final.json", duckdb_describe)
    write_json(run_dir / "duckdb_explain_final.json", duckdb_explain)
    write_json(run_dir / "duckdb_explain_analyze_final.json", duckdb_explain_analyze)

    duckdb_current_snapshot_id = latest_snapshot_id_from_duckdb(duckdb_snapshots)
    eq_del_check = snapshot_has_equality_deletes(lk_meta, target_snapshot_id, duckdb_metadata)
    if not eq_del_check["confirmed"]:
        raise RuntimeError(
            f"Snapshot {target_snapshot_id} does not hold confirmed live equality deletes: {eq_del_check}"
        )

    # 8. Start on-demand Spark service (profile spark) and read the exact same snapshot ID.
    logged_cmd([*compose_cmd_with_profiles(cfg, ("spark",)), "up", "-d", "spark"])
    digests = collect_digests_for_profiles(cfg, ("connector", "spark"))

    spark_rows_container_path = "/tmp/cdclab-spark/rows.jsonl"
    spark_meta_container_path = "/tmp/cdclab-spark/meta.json"
    spark_t0 = time.monotonic_ns()
    spark_proc = logged_cmd(
        [
            *compose_cmd_with_profiles(cfg, ("spark",)),
            "exec",
            "-T",
            "spark",
            "/opt/spark/bin/spark-submit",
            "--master",
            "local[2]",
            "/opt/cdclab/scripts/read_snapshot.py",
            "--catalog-name",
            "cdclab",
            "--catalog-uri",
            "http://lakekeeper:8181/catalog",
            "--warehouse",
            cfg.lakekeeper_warehouse,
            "--namespace",
            cfg.lakekeeper_namespace,
            "--table",
            "orders",
            "--snapshot-id",
            str(target_snapshot_id),
            "--rows-out",
            spark_rows_container_path,
            "--meta-out",
            spark_meta_container_path,
        ]
    )
    spark_read_ms = (time.monotonic_ns() - spark_t0) / 1_000_000.0
    (run_dir / "spark_submit.stdout.log").write_text(spark_proc.stdout, encoding="utf-8")
    (run_dir / "spark_submit.stderr.log").write_text(spark_proc.stderr, encoding="utf-8")

    # Copy raw Spark outputs out of the container to run_dir.
    spark_rows_host = run_dir / "spark_rows.jsonl"
    spark_meta_host = run_dir / "spark_metadata.json"
    rows_out = run_cmd(
        [*compose_cmd_with_profiles(cfg, ("spark",)), "exec", "-T", "spark", "cat", spark_rows_container_path]
    ).stdout
    meta_out = run_cmd(
        [*compose_cmd_with_profiles(cfg, ("spark",)), "exec", "-T", "spark", "cat", spark_meta_container_path]
    ).stdout
    spark_rows_host.write_text(rows_out, encoding="utf-8")
    spark_meta_host.write_text(meta_out, encoding="utf-8")
    spark_meta = json.loads(meta_out)

    # 9. Verify schemas across Lakekeeper Iceberg metadata, DuckDB, and Spark.
    iceberg_schema_check = verify_iceberg_schema(lk_meta)
    duckdb_schema_check = verify_duckdb_schema(duckdb_describe)
    spark_schema_check = verify_spark_schema(spark_meta["spark_schema"])

    # 10. Run lab.compare.compare_ordered_streams across all three pairs in bounded batches.
    con_cmp, _ = connect_iceberg(cfg)
    try:
        with psycopg.connect(cfg.pg_dsn) as pg_conn:
            pg_conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cmp_pg_vs_duckdb = compare_ordered_streams(
                stream_postgres_orders(pg_conn, batch_size=batch_size),
                stream_duckdb_orders_at_snapshot(
                    con_cmp,
                    target_snapshot_id,
                    namespace=cfg.lakekeeper_namespace,
                    batch_size=batch_size,
                ),
            )
            pg_conn.rollback()

            pg_conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cmp_pg_vs_spark = compare_ordered_streams(
                stream_postgres_orders(pg_conn, batch_size=batch_size),
                stream_spark_orders_ndjson(spark_rows_host, batch_size=batch_size),
            )
            pg_conn.rollback()

        cmp_duckdb_vs_spark = compare_ordered_streams(
            stream_duckdb_orders_at_snapshot(
                con_cmp,
                target_snapshot_id,
                namespace=cfg.lakekeeper_namespace,
                batch_size=batch_size,
            ),
            stream_spark_orders_ndjson(spark_rows_host, batch_size=batch_size),
        )
    finally:
        con_cmp.close()

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as pg_conn:
        pg_pos_final = capture_pg_pipeline_positions(pg_conn)

    pg2_logs = run_cmd(
        [*compose_cmd_with_profiles(cfg, ("connector",)), "logs", "--no-color", "pg2iceberg"],
        check=False,
    ).stdout
    (run_dir / "pg2iceberg.log").write_text(pg2_logs, encoding="utf-8")

    # 11. Project-only cleanup.
    cleanup_result = project_teardown_and_verify(
        cfg,
        logged_cmd,
        non_project_before,
        profiles=ALL_LAB_PROFILES,
        keep_up=keep_up,
    )

    # Check Spark delete file evidence from Iceberg metadata tables.
    spark_delete_files = [
        f
        for f in (spark_meta.get("files") or [])
        if int(f.get("content", 0)) == 2
    ]
    if not spark_delete_files:
        spark_delete_files = [
            f
            for f in (spark_meta.get("all_delete_files") or [])
            if int(f.get("content", 0)) == 2
        ]

    spark_current_snapshot_id = spark_meta.get("spark_current_snapshot_id")
    snapshot_ids_match = (
        target_snapshot_id == lakekeeper_current_snapshot_id
        and target_snapshot_id == duckdb_current_snapshot_id
        and target_snapshot_id == spark_current_snapshot_id
        and bool(spark_meta.get("requested_snapshot_found"))
    )
    schemas_match = (
        iceberg_schema_check.passed
        and duckdb_schema_check["passed"]
        and spark_schema_check["passed"]
    )
    rows_match = (
        cmp_pg_vs_duckdb.passed
        and cmp_pg_vs_spark.passed
        and cmp_duckdb_vs_spark.passed
    )
    cleanup_ok = keep_up or (
        cleanup_result["all_project_resources_removed"]
        and cleanup_result["non_project_containers_unchanged"]
    )
    passed = (
        snapshot_ids_match
        and schemas_match
        and rows_match
        and eq_del_check["confirmed"]
        and len(spark_delete_files) > 0
        and cleanup_ok
    )
    outcome = "converged" if passed else "mismatch at deadline"

    summary = {
        "milestone": "L1",
        "label": label,
        "outcome": outcome,
        "passed": passed,
        "completed_at_utc": utc_now_iso(),
        "parameters": {
            "seed_rows": seed_rows,
            "toast_rows": toast_rows,
            "rng_seed": rng_seed,
            "batch_size": batch_size,
        },
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python_version": sys.version,
        },
        "config": cfg.redacted_summary(),
        "commands_executed": commands_executed,
        "container_digests": digests,
        "reader_versions": {
            "duckdb": reader_versions.duckdb_version,
            "duckdb_extensions": reader_versions.extensions,
            "spark": spark_meta.get("versions"),
        },
        "resources_after_pre_clean": resources_after_pre_clean,
        "seed": seed_res.to_dict(),
        "initial_snapshot": initial_snapshot_info,
        "mutations": {
            "updated_ids": [1, 13, 14],
            "deleted_ids": [2, 15, 25],
            "marker_id": FINAL_MARKER_ID,
            "marker_commit_utc": marker_commit_utc,
            "marker_visibility_ms": vis["visibility_ms"],
            "toast_after_mutations": toast_after_mut,
        },
        "snapshot_comparison": {
            "tested_snapshot_id": target_snapshot_id,
            "lakekeeper_current_snapshot_id": lakekeeper_current_snapshot_id,
            "duckdb_current_snapshot_id": duckdb_current_snapshot_id,
            "spark_current_snapshot_id": spark_current_snapshot_id,
            "spark_requested_snapshot_found": spark_meta.get("requested_snapshot_found"),
            "snapshot_ids_match": snapshot_ids_match,
            "snapshot_info": extract_iceberg_snapshot_info(lk_meta, target_snapshot_id),
        },
        "equality_delete_evidence": {
            "lakekeeper_and_duckdb": eq_del_check,
            "spark_equality_delete_file_count": len(spark_delete_files),
            "spark_equality_delete_files": spark_delete_files,
            "spark_manifests_count": len(spark_meta.get("manifests") or []),
            "spark_read_duration_ms": round(spark_read_ms, 2),
        },
        "schema_comparison": {
            "all_passed": schemas_match,
            "iceberg_metadata": iceberg_schema_check.to_dict(),
            "duckdb_describe": duckdb_schema_check,
            "spark_struct": spark_schema_check,
        },
        "row_comparisons": {
            "all_passed": rows_match,
            "postgres_vs_duckdb": cmp_pg_vs_duckdb.to_dict(),
            "postgres_vs_spark": cmp_pg_vs_spark.to_dict(),
            "duckdb_vs_spark": cmp_duckdb_vs_spark.to_dict(),
        },
        "reader_features": {
            "duckdb": {
                "catalog_attachment": "ATTACH ... (TYPE iceberg, AUTHORIZATION_TYPE none, ENDPOINT ...)",
                "snapshot_pinning": f"AT (VERSION => {target_snapshot_id})",
                "storage_access": "vended credentials (S3 STS via Lakekeeper LoadTable)",
                "explain": duckdb_explain,
                "explain_analyze": duckdb_explain_analyze,
            },
            "spark": {
                "catalog_impl": "org.apache.iceberg.spark.SparkCatalog (type=rest)",
                "snapshot_pinning": f"VERSION AS OF {target_snapshot_id}",
                "storage_access": "vended credentials (header.X-Iceberg-Access-Delegation=vended-credentials, Iceberg AWSFileIO)",
                "explain_formatted": spark_meta.get("explain_formatted"),
                "iceberg_scan_details": spark_meta.get("iceberg_scan_details"),
            },
        },
        "pipeline_positions_final": pg_pos_final,
        "cleanup": cleanup_result,
    }
    write_json(run_dir / "summary.json", summary)

    compact = {
        "run_dir": str(run_dir.relative_to(REPO_ROOT).as_posix()),
        "outcome": outcome,
        "passed": passed,
        "tested_snapshot_id": target_snapshot_id,
        "snapshot_ids_match": snapshot_ids_match,
        "schemas_match": schemas_match,
        "rows_match": rows_match,
        "pg_vs_duckdb_rows": f"{cmp_pg_vs_duckdb.source_row_count}={cmp_pg_vs_duckdb.lake_row_count}",
        "pg_vs_spark_rows": f"{cmp_pg_vs_spark.source_row_count}={cmp_pg_vs_spark.lake_row_count}",
        "duckdb_vs_spark_rows": f"{cmp_duckdb_vs_spark.source_row_count}={cmp_duckdb_vs_spark.lake_row_count}",
        "equality_delete_files_duckdb": len(eq_del_check["duckdb_live_delete_entries"]),
        "equality_delete_files_spark": len(spark_delete_files),
        "spark_read_duration_ms": round(spark_read_ms, 2),
    }
    print(json.dumps(compact, indent=2, default=json_default))
    if not passed:
        raise SystemExit(1)
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Run L1 second-reader (Spark vs DuckDB) verification.")
    parser.add_argument("--seed-rows", type=int, default=30)
    parser.add_argument("--toast-rows", type=int, default=10)
    parser.add_argument("--rng-seed", type=int, default=20261119)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--label", default="l1-spark-reader")
    parser.add_argument("--keep-up", action="store_true")
    args = parser.parse_args()
    run_l1(
        seed_rows=args.seed_rows,
        toast_rows=args.toast_rows,
        rng_seed=args.rng_seed,
        batch_size=args.batch_size,
        label=args.label,
        keep_up=args.keep_up,
    )


if __name__ == "__main__":
    main()
