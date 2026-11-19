"""Shared Compose lifecycle, pipeline-position capture, and snapshot convergence helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import subprocess
import time
from typing import Any
import urllib.request

import psycopg

from lab.compare import (
    ComparisonResult,
    SchemaCheckResult,
    compare_ordered_streams,
    format_decimal_fixed,
    stream_duckdb_orders_at_snapshot,
    stream_postgres_orders,
    verify_iceberg_schema,
)
from lab.config import LabConfig, REPO_ROOT
from lab.reader import (
    connect_iceberg,
    fetch_lakekeeper_table_metadata,
    marker_exists_at_snapshot,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def utc_run_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return format_decimal_fixed(obj)
    if isinstance(obj, datetime):
        return obj.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(obj, Path):
        return str(obj)
    return str(obj)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, default=json_default) + "\n",
        encoding="utf-8",
    )


def run_cmd(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"Command failed ({proc.returncode}): {' '.join(cmd)}\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    return proc


def list_project_resources(project: str) -> dict[str, list[str]]:
    containers = run_cmd(
        [
            "docker",
            "ps",
            "-a",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--format",
            "{{.Names}}",
        ]
    ).stdout.splitlines()
    volumes = run_cmd(
        [
            "docker",
            "volume",
            "ls",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--format",
            "{{.Name}}",
        ]
    ).stdout.splitlines()
    networks = run_cmd(
        [
            "docker",
            "network",
            "ls",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--format",
            "{{.Name}}",
        ]
    ).stdout.splitlines()
    return {
        "containers": sorted(c.strip() for c in containers if c.strip()),
        "volumes": sorted(v.strip() for v in volumes if v.strip()),
        "networks": sorted(n.strip() for n in networks if n.strip()),
    }


def list_non_project_containers(project: str) -> list[str]:
    out = run_cmd(
        [
            "docker",
            "ps",
            "-a",
            "--format",
            '{{.ID}}|{{.Names}}|{{.Label "com.docker.compose.project"}}',
        ]
    ).stdout.splitlines()
    others = []
    for line in out:
        parts = line.strip().split("|")
        if len(parts) == 3 and parts[2] != project:
            others.append(f"{parts[0]}|{parts[1]}|{parts[2]}")
    return sorted(others)


def collect_container_digests(cfg: LabConfig) -> list[dict[str, Any]]:
    ids = run_cmd(
        [*cfg.compose_base_cmd(include_connector=True), "ps", "-q"]
    ).stdout.splitlines()
    ids = [cid.strip() for cid in ids if cid.strip()]
    if not ids:
        return []
    raw = run_cmd(["docker", "inspect", *ids]).stdout
    inspected = json.loads(raw)
    result = []
    for item in inspected:
        img_id = item.get("Image")
        img_info = json.loads(run_cmd(["docker", "image", "inspect", img_id]).stdout)[0]
        result.append(
            {
                "service": (item.get("Config", {}).get("Labels") or {}).get(
                    "com.docker.compose.service"
                ),
                "container_name": item.get("Name", "").lstrip("/"),
                "image_ref": item.get("Config", {}).get("Image"),
                "image_id": img_id,
                "repo_digests": img_info.get("RepoDigests") or [],
                "architecture": img_info.get("Architecture"),
                "os": img_info.get("Os"),
            }
        )
    return sorted(result, key=lambda x: str(x["service"]))


def capture_pg_pipeline_positions(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    """Capture WAL position, replication slot LSNs/byte gaps, table sizes, and _pg2iceberg state."""
    with conn.cursor() as cur:
        cur.execute("SELECT pg_current_wal_lsn()::text")
        current_wal_lsn = cur.fetchone()[0]

        cur.execute(
            """
            SELECT
                slot_name,
                plugin,
                slot_type,
                temporary,
                active,
                active_pid,
                restart_lsn::text,
                confirmed_flush_lsn::text,
                CASE
                    WHEN confirmed_flush_lsn IS NOT NULL
                    THEN pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint
                    ELSE NULL
                END AS wal_gap_bytes_confirmed_flush,
                CASE
                    WHEN restart_lsn IS NOT NULL
                    THEN pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
                    ELSE NULL
                END AS wal_gap_bytes_restart
            FROM pg_replication_slots
            ORDER BY slot_name
            """
        )
        slots = [
            {
                "slot_name": r[0],
                "plugin": r[1],
                "slot_type": r[2],
                "temporary": bool(r[3]),
                "active": bool(r[4]),
                "active_pid": r[5],
                "restart_lsn": r[6],
                "confirmed_flush_lsn": r[7],
                "wal_gap_bytes_confirmed_flush": r[8],
                "wal_gap_bytes_restart": r[9],
            }
            for r in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT
                pg_relation_size('public.orders'::regclass) AS heap_bytes,
                pg_table_size('public.orders'::regclass) AS table_and_toast_bytes,
                pg_indexes_size('public.orders'::regclass) AS indexes_bytes,
                pg_total_relation_size('public.orders'::regclass) AS total_relation_bytes,
                CASE
                    WHEN c.reltoastrelid <> 0 THEN pg_total_relation_size(c.reltoastrelid)
                    ELSE 0
                END AS toast_relation_bytes,
                c.relreplident::text AS replica_identity
            FROM pg_class c
            WHERE c.oid = 'public.orders'::regclass
            """
        )
        sz = cur.fetchone()
        table_sizes = (
            {
                "heap_bytes": int(sz[0]),
                "table_and_toast_bytes": int(sz[1]),
                "indexes_bytes": int(sz[2]),
                "total_relation_bytes": int(sz[3]),
                "toast_relation_bytes": int(sz[4]),
                "replica_identity": sz[5],
            }
            if sz
            else {}
        )

        # Inspect small state tables inside _pg2iceberg coordinator schema if present.
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '_pg2iceberg'
            ORDER BY table_name
            """
        )
        coord_tables = [r[0] for r in cur.fetchall()]
        coordinator_state: dict[str, Any] = {}
        for tbl in (
            "replicated_lsn",
            "flushed_lsn",
            "mat_cursor",
            "snapshot_progress",
            "log_seq",
            "pipeline_meta",
            "table_epoch",
        ):
            if tbl not in coord_tables:
                continue
            try:
                cur.execute(f'SELECT * FROM _pg2iceberg."{tbl}" LIMIT 20')
                cols = [d[0] for d in cur.description]
                coordinator_state[tbl] = [dict(zip(cols, row)) for row in cur.fetchall()]
            except Exception as exc:
                coordinator_state[tbl] = {"error": str(exc)}

        if "log_index" in coord_tables:
            try:
                cur.execute('SELECT COUNT(*) FROM _pg2iceberg."log_index"')
                coordinator_state["log_index_row_count"] = int(cur.fetchone()[0])
            except Exception as exc:
                coordinator_state["log_index_row_count"] = {"error": str(exc)}

    return {
        "captured_at_utc": utc_now_iso(),
        "current_wal_lsn": current_wal_lsn,
        "replication_slots": slots,
        "table_sizes": table_sizes,
        "coordinator_tables": coord_tables,
        "coordinator_state": coordinator_state,
    }


def extract_iceberg_snapshot_info(
    lakekeeper_meta: dict[str, Any] | None,
    snapshot_id: int | None = None,
) -> dict[str, Any] | None:
    if not lakekeeper_meta:
        return None
    meta = lakekeeper_meta.get("metadata") or {}
    target_id = snapshot_id if snapshot_id is not None else meta.get("current-snapshot-id")
    if target_id is None:
        return None
    for snap in meta.get("snapshots") or []:
        if int(snap.get("snapshot-id", -1)) == int(target_id):
            summary = snap.get("summary") or {}
            return {
                "snapshot_id": int(snap["snapshot-id"]),
                "parent_snapshot_id": snap.get("parent-snapshot-id"),
                "sequence_number": snap.get("sequence-number"),
                "timestamp_ms": snap.get("timestamp-ms"),
                "schema_id": snap.get("schema-id"),
                "pg2iceberg_log_start": summary.get("pg2iceberg.log-start"),
                "pg2iceberg_log_end": summary.get("pg2iceberg.log-end"),
                "pg2iceberg_log_ends": summary.get("pg2iceberg.log-ends"),
                "pg2iceberg_log_group": summary.get("pg2iceberg.log-group"),
                "summary": summary,
            }
    return None


def wait_for_connector_and_snapshot(
    cfg: LabConfig,
    *,
    timeout_s: float = 180.0,
    poll_interval_s: float = 1.0,
) -> dict[str, Any]:
    """Wait until pg2iceberg slot is active and its initial snapshot phase completes."""
    t0 = time.monotonic_ns()
    deadline = time.monotonic() + timeout_s
    last_err: str | None = None
    slot_seen = False

    while time.monotonic() < deadline:
        try:
            with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
                pos = capture_pg_pipeline_positions(conn)
            slots = [s for s in pos["replication_slots"] if s["slot_name"] == "pg2iceberg_slot"]
            if slots and slots[0]["active"]:
                slot_seen = True

            # Check logs for "snapshot phase complete"
            logs = run_cmd(
                [*cfg.compose_base_cmd(include_connector=True), "logs", "--no-color", "pg2iceberg"],
                check=False,
            ).stdout
            snapshot_logged_complete = "snapshot phase complete" in logs

            lk_meta = fetch_lakekeeper_table_metadata(
                cfg,
                namespace=cfg.lakekeeper_namespace,
                table="orders",
                allow_not_found=True,
            )
            current_snap_id = (
                (lk_meta.get("metadata") or {}).get("current-snapshot-id")
                if lk_meta
                else None
            )

            health_status: int | None = None
            try:
                with urllib.request.urlopen(f"{cfg.pg2iceberg_metrics_url}/healthz", timeout=3.0) as r:
                    health_status = r.status
            except Exception as exc:
                last_err = f"healthz: {exc}"

            if slot_seen and snapshot_logged_complete and current_snap_id is not None:
                elapsed_ms = (time.monotonic_ns() - t0) / 1_000_000.0
                return {
                    "elapsed_ms": round(elapsed_ms, 2),
                    "healthz_status": health_status,
                    "snapshot_logged_complete": snapshot_logged_complete,
                    "current_snapshot_id": int(current_snap_id),
                    "snapshot_info": extract_iceberg_snapshot_info(lk_meta, int(current_snap_id)),
                    "pipeline_positions": pos,
                }
        except Exception as exc:
            last_err = f"{type(exc).__name__}: {exc}"
        time.sleep(poll_interval_s)

    raise TimeoutError(
        f"pg2iceberg did not complete initial snapshot within {timeout_s}s; "
        f"slot_seen={slot_seen} last_err={last_err}"
    )


def wait_for_marker_visible(
    cfg: LabConfig,
    marker_id: int,
    commit_mono_ns: int,
    *,
    timeout_s: float = 120.0,
    poll_interval_s: float = 0.5,
) -> dict[str, Any]:
    """Poll Lakekeeper + DuckDB (pinned to each new snapshot ID) until marker_id is visible."""
    deadline = time.monotonic() + timeout_s
    checked_snapshots: set[int] = set()
    polls = 0
    duckdb_checks = 0
    errors: list[str] = []

    while time.monotonic() < deadline:
        polls += 1
        try:
            lk_meta = fetch_lakekeeper_table_metadata(
                cfg,
                namespace=cfg.lakekeeper_namespace,
                table="orders",
                allow_not_found=True,
            )
            snap_id = (
                (lk_meta.get("metadata") or {}).get("current-snapshot-id")
                if lk_meta
                else None
            )
            if snap_id is not None:
                snap_int = int(snap_id)
                if snap_int not in checked_snapshots:
                    checked_snapshots.add(snap_int)
                    duckdb_checks += 1
                    con = None
                    try:
                        con, _ = connect_iceberg(cfg)
                        found = marker_exists_at_snapshot(
                            con, snap_int, marker_id, namespace=cfg.lakekeeper_namespace
                        )
                    finally:
                        if con is not None:
                            con.close()
                    if found:
                        obs_ns = time.monotonic_ns()
                        return {
                            "visible": True,
                            "marker_id": marker_id,
                            "snapshot_id": snap_int,
                            "observed_utc": utc_now_iso(),
                            "visibility_ms": round((obs_ns - commit_mono_ns) / 1_000_000.0, 2),
                            "polls": polls,
                            "duckdb_checks": duckdb_checks,
                            "poll_interval_s": poll_interval_s,
                            "errors": errors,
                            "snapshot_info": extract_iceberg_snapshot_info(lk_meta, snap_int),
                            "lakekeeper_metadata": lk_meta,
                        }
        except Exception as exc:
            errors.append(f"{utc_now_iso()} poll={polls}: {type(exc).__name__}: {exc}")
        time.sleep(poll_interval_s)

    return {
        "visible": False,
        "marker_id": marker_id,
        "snapshot_id": None,
        "observed_utc": None,
        "visibility_ms": None,
        "polls": polls,
        "duckdb_checks": duckdb_checks,
        "poll_interval_s": poll_interval_s,
        "errors": errors,
        "snapshot_info": None,
        "lakekeeper_metadata": None,
    }


def verify_snapshot_convergence(
    cfg: LabConfig,
    final_marker_id: int,
    final_marker_commit_mono_ns: int,
    *,
    timeout_s: float = 180.0,
    poll_interval_s: float = 1.0,
    batch_size: int = 2000,
) -> dict[str, Any]:
    """Wait for final marker, then run exact schema + bounded-batch row diff once per eligible snapshot."""
    deadline = time.monotonic() + timeout_s
    marker_checked_snapshots: set[int] = set()
    diffed_snapshots: set[int] = set()
    snapshot_attempts: list[dict[str, Any]] = []
    reader_errors: list[str] = []
    polls = 0

    first_marker_visible_ns: int | None = None
    first_marker_snapshot_id: int | None = None
    first_marker_utc: str | None = None
    last_lk_meta: dict[str, Any] | None = None

    while time.monotonic() < deadline:
        polls += 1
        try:
            lk_meta = fetch_lakekeeper_table_metadata(
                cfg,
                namespace=cfg.lakekeeper_namespace,
                table="orders",
                allow_not_found=True,
            )
            snap_id = (
                (lk_meta.get("metadata") or {}).get("current-snapshot-id")
                if lk_meta
                else None
            )
            if snap_id is None:
                time.sleep(poll_interval_s)
                continue

            snap_int = int(snap_id)
            last_lk_meta = lk_meta

            # Once first marker is seen, every subsequent distinct snapshot is eligible too.
            if first_marker_snapshot_id is None:
                if snap_int in marker_checked_snapshots:
                    time.sleep(poll_interval_s)
                    continue
                marker_checked_snapshots.add(snap_int)
                con = None
                try:
                    con, _ = connect_iceberg(cfg)
                    has_marker = marker_exists_at_snapshot(
                        con, snap_int, final_marker_id, namespace=cfg.lakekeeper_namespace
                    )
                finally:
                    if con is not None:
                        con.close()
                if not has_marker:
                    time.sleep(poll_interval_s)
                    continue
                first_marker_visible_ns = time.monotonic_ns()
                first_marker_snapshot_id = snap_int
                first_marker_utc = utc_now_iso()

            if snap_int in diffed_snapshots:
                time.sleep(poll_interval_s)
                continue
            diffed_snapshots.add(snap_int)

            # Run full schema check + bounded-batch row diff against quiescent source.
            schema_res: SchemaCheckResult = verify_iceberg_schema(lk_meta)
            con = None
            source_read_opened_utc = ""
            try:
                con, _ = connect_iceberg(cfg)
                with psycopg.connect(cfg.pg_dsn) as pg_conn:
                    pg_conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    source_read_opened_utc = utc_now_iso()
                    src_batches = stream_postgres_orders(pg_conn, batch_size=batch_size)
                    lake_batches = stream_duckdb_orders_at_snapshot(
                        con,
                        snap_int,
                        namespace=cfg.lakekeeper_namespace,
                        batch_size=batch_size,
                    )
                    row_res: ComparisonResult = compare_ordered_streams(
                        src_batches,
                        lake_batches,
                        max_examples=20,
                    )
                    pg_conn.rollback()
            finally:
                if con is not None:
                    con.close()

            attempt_passed = schema_res.passed and row_res.passed
            attempt = {
                "snapshot_id": snap_int,
                "source_read_opened_utc": source_read_opened_utc,
                "checked_at_utc": utc_now_iso(),
                "passed": attempt_passed,
                "schema_check": schema_res.to_dict(),
                "row_comparison": row_res.to_dict(),
                "snapshot_info": extract_iceberg_snapshot_info(lk_meta, snap_int),
            }
            snapshot_attempts.append(attempt)

            if attempt_passed:
                verified_ns = time.monotonic_ns()
                return {
                    "outcome": "converged",
                    "passed": True,
                    "final_marker_id": final_marker_id,
                    "first_marker_snapshot_id": first_marker_snapshot_id,
                    "first_marker_observed_utc": first_marker_utc,
                    "final_marker_visibility_ms": round(
                        (first_marker_visible_ns - final_marker_commit_mono_ns) / 1_000_000.0,
                        2,
                    ),
                    "verified_snapshot_id": snap_int,
                    "verified_at_utc": utc_now_iso(),
                    "final_marker_to_verified_match_ms": round(
                        (verified_ns - final_marker_commit_mono_ns) / 1_000_000.0,
                        2,
                    ),
                    "diff_duration_ms": round(row_res.diff_duration_ms, 2),
                    "batch_size": batch_size,
                    "poll_interval_s": poll_interval_s,
                    "polls": polls,
                    "reader_errors": reader_errors,
                    "snapshot_attempts": snapshot_attempts,
                    "final_lakekeeper_metadata": lk_meta,
                }
        except Exception as exc:
            reader_errors.append(f"{utc_now_iso()} poll={polls}: {type(exc).__name__}: {exc}")
        time.sleep(poll_interval_s)

    if first_marker_snapshot_id is None and reader_errors:
        outcome = "reader error"
    else:
        outcome = "mismatch at deadline"
    return {
        "outcome": outcome,
        "passed": False,
        "final_marker_id": final_marker_id,
        "first_marker_snapshot_id": first_marker_snapshot_id,
        "first_marker_observed_utc": first_marker_utc,
        "final_marker_visibility_ms": (
            round((first_marker_visible_ns - final_marker_commit_mono_ns) / 1_000_000.0, 2)
            if first_marker_visible_ns is not None
            else None
        ),
        "verified_snapshot_id": None,
        "verified_at_utc": None,
        "final_marker_to_verified_match_ms": None,
        "diff_duration_ms": (
            snapshot_attempts[-1]["row_comparison"]["diff_duration_ms"]
            if snapshot_attempts
            else None
        ),
        "batch_size": batch_size,
        "poll_interval_s": poll_interval_s,
        "polls": polls,
        "reader_errors": reader_errors,
        "snapshot_attempts": snapshot_attempts,
        "final_lakekeeper_metadata": last_lk_meta,
    }
