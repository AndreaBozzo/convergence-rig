"""Milestone M0 end-to-end runner: prove the reader path with insert, update, delete."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Context, Decimal
import json
from pathlib import Path
import platform
import subprocess
import sys
import time
from typing import Any
import urllib.request

import psycopg

from lab.bootstrap import bootstrap_all
from lab.config import LabConfig, REPO_ROOT, load_config
from lab.reader import (
    connect_iceberg,
    fetch_lakekeeper_table_metadata,
    read_metadata_files,
    read_orders_at_snapshot,
    read_orders_current,
    read_snapshots,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def utc_run_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.astimezone(timezone.utc).isoformat()
    if isinstance(obj, Path):
        return str(obj)
    return str(obj)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, default=_json_default) + "\n",
        encoding="utf-8",
    )


DECIMAL38_CTX = Context(prec=38)


def _format_decimal_9(value: Any) -> str:
    return str(Decimal(str(value)).quantize(Decimal("0.000000001"), context=DECIMAL38_CTX))


def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize a row returned from DuckDB or expected fixture into comparable canonical form."""
    updated_at = row["updated_at"]
    if isinstance(updated_at, datetime):
        updated_iso = updated_at.astimezone(timezone.utc).isoformat(timespec="microseconds")
    else:
        updated_iso = str(updated_at)

    payload = row["payload"]
    if isinstance(payload, str):
        payload_norm: Any = json.loads(payload)
    else:
        payload_norm = payload

    amount = row["amount"]
    discount = row["discount"]
    return {
        "id": int(row["id"]),
        "status": str(row["status"]),
        "amount": _format_decimal_9(amount),
        "updated_at": updated_iso,
        "note": row["note"],
        "payload": payload_norm,
        "discount": None if discount is None else _format_decimal_9(discount),
    }


def snapshot_has_equality_deletes(
    lakekeeper_meta: dict[str, Any],
    snapshot_id: int,
    duckdb_metadata_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Verify equality-delete files are referenced in both Iceberg metadata and DuckDB iceberg_metadata()."""
    meta = lakekeeper_meta.get("metadata") or {}
    snap_summary: dict[str, Any] = {}
    for snap in meta.get("snapshots") or []:
        if int(snap.get("snapshot-id", -1)) == int(snapshot_id):
            snap_summary = snap.get("summary") or {}
            break

    total_eq_deletes = int(snap_summary.get("total-equality-deletes", "0"))
    added_eq_deletes = int(snap_summary.get("added-equality-deletes", "0"))
    total_delete_files = int(snap_summary.get("total-delete-files", "0"))
    added_delete_files = int(snap_summary.get("added-delete-files", "0"))

    eq_delete_files_duckdb = []
    for row in duckdb_metadata_rows:
        content = str(row.get("content", "")).upper()
        manifest_content = str(row.get("manifest_content", "")).upper()
        status = str(row.get("status", "")).upper()
        if "EQUALITY" in content or "DELETE" in manifest_content:
            if status != "DELETED":
                eq_delete_files_duckdb.append(
                    {
                        "manifest_path": row.get("manifest_path"),
                        "manifest_content": row.get("manifest_content"),
                        "file_path": row.get("file_path"),
                        "content": row.get("content"),
                        "status": row.get("status"),
                        "record_count": row.get("record_count"),
                    }
                )

    return {
        "snapshot_id": snapshot_id,
        "snapshot_summary": snap_summary,
        "total_equality_deletes": total_eq_deletes,
        "added_equality_deletes": added_eq_deletes,
        "total_delete_files": total_delete_files,
        "added_delete_files": added_delete_files,
        "duckdb_live_delete_entries": eq_delete_files_duckdb,
        "confirmed": total_eq_deletes > 0 and len(eq_delete_files_duckdb) > 0,
    }


def _run_cmd(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"Command failed ({proc.returncode}): {' '.join(cmd)}\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    return proc


def list_project_resources(project: str) -> dict[str, list[str]]:
    containers = _run_cmd(
        ["docker", "ps", "-a", "--filter", f"label=com.docker.compose.project={project}", "--format", "{{.Names}}"]
    ).stdout.splitlines()
    volumes = _run_cmd(
        ["docker", "volume", "ls", "--filter", f"label=com.docker.compose.project={project}", "--format", "{{.Name}}"]
    ).stdout.splitlines()
    networks = _run_cmd(
        ["docker", "network", "ls", "--filter", f"label=com.docker.compose.project={project}", "--format", "{{.Name}}"]
    ).stdout.splitlines()
    return {
        "containers": sorted(c.strip() for c in containers if c.strip()),
        "volumes": sorted(v.strip() for v in volumes if v.strip()),
        "networks": sorted(n.strip() for n in networks if n.strip()),
    }


def list_non_project_containers(project: str) -> list[str]:
    out = _run_cmd(
        ["docker", "ps", "-a", "--format", '{{.ID}}|{{.Names}}|{{.Label "com.docker.compose.project"}}']
    ).stdout.splitlines()
    others = []
    for line in out:
        parts = line.strip().split("|")
        if len(parts) == 3 and parts[2] != project:
            others.append(f"{parts[0]}|{parts[1]}|{parts[2]}")
    return sorted(others)


def collect_container_digests(cfg: LabConfig) -> list[dict[str, Any]]:
    ids = _run_cmd([*cfg.compose_base_cmd(include_connector=True), "ps", "-q"]).stdout.splitlines()
    ids = [cid.strip() for cid in ids if cid.strip()]
    if not ids:
        return []
    raw = _run_cmd(["docker", "inspect", *ids]).stdout
    inspected = json.loads(raw)
    result = []
    for item in inspected:
        img_id = item.get("Image")
        img_info = json.loads(_run_cmd(["docker", "image", "inspect", img_id]).stdout)[0]
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


def wait_for_connector_ready(cfg: LabConfig, timeout_s: float = 60.0) -> dict[str, Any]:
    """Wait until pg2iceberg creates its replication slot and reports ready/healthy."""
    deadline = time.monotonic() + timeout_s
    last_err: str | None = None
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT slot_name, plugin, slot_type, active, restart_lsn::text, confirmed_flush_lsn::text
                        FROM pg_replication_slots
                        WHERE slot_name = 'pg2iceberg_slot'
                        """
                    )
                    slot = cur.fetchone()
            health_status: int | None = None
            try:
                with urllib.request.urlopen(f"{cfg.pg2iceberg_metrics_url}/healthz", timeout=3.0) as r:
                    health_status = r.status
            except Exception as exc:
                last_err = f"healthz: {exc}"
            if slot is not None and slot[3]:
                return {
                    "slot_name": slot[0],
                    "plugin": slot[1],
                    "slot_type": slot[2],
                    "active": slot[3],
                    "restart_lsn": slot[4],
                    "confirmed_flush_lsn": slot[5],
                    "healthz_status": health_status,
                }
        except Exception as exc:
            last_err = str(exc)
        time.sleep(1.0)
    raise TimeoutError(f"pg2iceberg did not become ready within {timeout_s}s; last_err={last_err}")


def poll_duckdb_until(
    cfg: LabConfig,
    predicate,
    *,
    timeout_s: float = 90.0,
    poll_interval_s: float = 1.0,
) -> tuple[list[dict[str, Any]], int, dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Poll DuckDB on fresh catalog attachments until predicate(normalized_rows) returns True."""
    deadline = time.monotonic() + timeout_s
    errors: list[str] = []
    polls = 0
    while time.monotonic() < deadline:
        polls += 1
        con = None
        try:
            con, _ = connect_iceberg(cfg)
            rows = read_orders_current(con, namespace=cfg.lakekeeper_namespace)
            norm = [normalize_row(r) for r in rows]
            if predicate(norm):
                snapshots = read_snapshots(con, namespace=cfg.lakekeeper_namespace)
                metadata_rows = read_metadata_files(con, namespace=cfg.lakekeeper_namespace)
                lk_meta = fetch_lakekeeper_table_metadata(
                    cfg, namespace=cfg.lakekeeper_namespace, table="orders"
                )
                current_snap_id = int(lk_meta["metadata"]["current-snapshot-id"])
                return norm, current_snap_id, lk_meta, snapshots, metadata_rows, errors
        except Exception as exc:
            errors.append(f"{utc_now_iso()} poll={polls}: {type(exc).__name__}: {exc}")
        finally:
            if con is not None:
                con.close()
        time.sleep(poll_interval_s)
    raise TimeoutError(
        f"Timed out after {timeout_s}s ({polls} polls) waiting for DuckDB condition. "
        f"Recent errors: {errors[-5:]}"
    )


def run_m0(*, keep_up: bool = False, label: str = "m0") -> Path:
    cfg = load_config()
    run_dir = REPO_ROOT / "runs" / f"{utc_run_stamp()}-{label}"
    run_dir.mkdir(parents=True, exist_ok=True)

    commands_executed: list[str] = []
    non_project_before = list_non_project_containers(cfg.compose_project)

    def logged_cmd(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        commands_executed.append(" ".join(cmd))
        return _run_cmd(cmd, check=check)

    # Step 1a: Clean fresh start (infra only, before connector).
    logged_cmd([*cfg.compose_base_cmd(include_connector=True), "down", "-v", "--remove-orphans"])
    resources_after_pre_clean = list_project_resources(cfg.compose_project)

    logged_cmd([*cfg.compose_base_cmd(include_connector=False), "up", "-d", "--wait"])

    # Step 1b: Run bootstrap twice to prove idempotency before starting connector.
    bootstrap_first = bootstrap_all(cfg)
    bootstrap_second = bootstrap_all(cfg)
    write_json(
        run_dir / "bootstrap.json",
        {
            "first_run": bootstrap_first,
            "second_run": bootstrap_second,
            "idempotent": (
                bootstrap_second["lakekeeper"]["action"] == "already_bootstrapped"
                and bootstrap_second["warehouse"]["action"] == "already_exists"
                and bootstrap_second["endpoint_reachability"]["both_reachable"]
            ),
        },
    )

    # Step 1c: Start pg2iceberg connector after bootstrap completes.
    logged_cmd([*cfg.compose_base_cmd(include_connector=True), "up", "-d", "pg2iceberg"])
    connector_ready = wait_for_connector_ready(cfg, timeout_s=60.0)
    digests = collect_container_digests(cfg)

    # Capture DuckDB & extension versions once.
    con_v, reader_versions = connect_iceberg(cfg)
    con_v.close()

    # Step 2a: Insert two known rows.
    row1_initial = {
        "id": 1,
        "status": "created",
        "amount": Decimal("12345678901234567890123456789.123456789"),
        "updated_at": datetime(2026, 11, 19, 18, 0, 0, 123456, tzinfo=timezone.utc),
        "note": "first row",
        "payload": {"k": "v1", "n": 1},
        "discount": Decimal("0.000000001"),
    }
    row2_initial = {
        "id": 2,
        "status": "created",
        "amount": Decimal("42.500000000"),
        "updated_at": datetime(2026, 11, 19, 18, 0, 1, 654321, tzinfo=timezone.utc),
        "note": None,
        "payload": {"k": "v2", "arr": [1, 2]},
        "discount": None,
    }
    expected_after_insert = [normalize_row(row1_initial), normalize_row(row2_initial)]

    insert_t0 = time.monotonic_ns()
    with psycopg.connect(cfg.pg_dsn) as conn:
        with conn.cursor() as cur:
            for r in (row1_initial, row2_initial):
                cur.execute(
                    """
                    INSERT INTO public.orders (id, status, amount, updated_at, note, payload, discount)
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
                    """,
                    (
                        r["id"],
                        r["status"],
                        r["amount"],
                        r["updated_at"],
                        r["note"],
                        json.dumps(r["payload"]),
                        r["discount"],
                    ),
                )
        conn.commit()
    insert_commit_utc = utc_now_iso()

    (
        rows_after_insert,
        snap_after_insert,
        lk_meta_insert,
        snaps_after_insert,
        meta_files_insert,
        insert_poll_errors,
    ) = poll_duckdb_until(cfg, lambda rows: rows == expected_after_insert, timeout_s=90.0)
    insert_visible_ms = (time.monotonic_ns() - insert_t0) / 1_000_000.0

    write_json(run_dir / "lakekeeper_table_after_insert.json", lk_meta_insert)

    # Step 2b: Update row 1 (including last-digit 38-digit decimal change) and delete row 2.
    row1_updated = {
        "id": 1,
        "status": "updated",
        "amount": Decimal("12345678901234567890123456789.123456788"),
        "updated_at": datetime(2026, 11, 19, 18, 5, 0, 987654, tzinfo=timezone.utc),
        "note": "first row updated",
        "payload": {"k": "v1_updated", "n": 2},
        "discount": Decimal("1.250000000"),
    }
    expected_after_mut = [normalize_row(row1_updated)]

    mut_t0 = time.monotonic_ns()
    with psycopg.connect(cfg.pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.orders
                SET status = %s,
                    amount = %s,
                    updated_at = %s,
                    note = %s,
                    payload = %s::jsonb,
                    discount = %s
                WHERE id = %s
                """,
                (
                    row1_updated["status"],
                    row1_updated["amount"],
                    row1_updated["updated_at"],
                    row1_updated["note"],
                    json.dumps(row1_updated["payload"]),
                    row1_updated["discount"],
                    row1_updated["id"],
                ),
            )
            cur.execute("DELETE FROM public.orders WHERE id = %s", (2,))
        conn.commit()
    mut_commit_utc = utc_now_iso()

    (
        rows_after_mut,
        snap_after_mut,
        lk_meta_mut,
        snaps_after_mut,
        meta_files_mut,
        mut_poll_errors,
    ) = poll_duckdb_until(cfg, lambda rows: rows == expected_after_mut, timeout_s=90.0)
    mut_visible_ms = (time.monotonic_ns() - mut_t0) / 1_000_000.0

    write_json(run_dir / "lakekeeper_table_after_update_delete.json", lk_meta_mut)
    write_json(run_dir / "duckdb_metadata_after_update_delete.json", meta_files_mut)
    write_json(run_dir / "duckdb_snapshots_after_update_delete.json", snaps_after_mut)

    # Verify equality-delete files remain referenced by the tested snapshot.
    eq_del_check = snapshot_has_equality_deletes(lk_meta_mut, snap_after_mut, meta_files_mut)

    # Confirm snapshot-pinned reads for BOTH snapshots (latest after update/delete AND historical after insert).
    con_pin, _ = connect_iceberg(cfg)
    try:
        pinned_mut_rows = [
            normalize_row(r)
            for r in read_orders_at_snapshot(
                con_pin, snap_after_mut, namespace=cfg.lakekeeper_namespace
            )
        ]
        pinned_insert_rows = [
            normalize_row(r)
            for r in read_orders_at_snapshot(
                con_pin, snap_after_insert, namespace=cfg.lakekeeper_namespace
            )
        ]
    finally:
        con_pin.close()

    # Capture PG replication slot state and connector logs before cleanup.
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT slot_name, plugin, active, restart_lsn::text, confirmed_flush_lsn::text,
                       pg_current_wal_lsn()::text
                FROM pg_replication_slots
                ORDER BY slot_name
                """
            )
            slot_rows = [
                {
                    "slot_name": r[0],
                    "plugin": r[1],
                    "active": r[2],
                    "restart_lsn": r[3],
                    "confirmed_flush_lsn": r[4],
                    "current_wal_lsn": r[5],
                }
                for r in cur.fetchall()
            ]
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '_pg2iceberg'
                ORDER BY table_name
                """
            )
            coord_tables = [r[0] for r in cur.fetchall()]

    pg2_logs = _run_cmd(
        [*cfg.compose_base_cmd(include_connector=True), "logs", "--no-color", "pg2iceberg"],
        check=False,
    ).stdout
    (run_dir / "pg2iceberg.log").write_text(pg2_logs, encoding="utf-8")

    # Step 3: Project-only cleanup verification.
    cleanup_result: dict[str, Any] = {"skipped_keep_up": keep_up}
    if not keep_up:
        logged_cmd([*cfg.compose_base_cmd(include_connector=True), "down", "-v", "--remove-orphans"])
        resources_after_cleanup = list_project_resources(cfg.compose_project)
        non_project_after = list_non_project_containers(cfg.compose_project)
        cleanup_result = {
            "skipped_keep_up": False,
            "resources_after_cleanup": resources_after_cleanup,
            "all_project_resources_removed": (
                not resources_after_cleanup["containers"]
                and not resources_after_cleanup["volumes"]
                and not resources_after_cleanup["networks"]
            ),
            "non_project_containers_unchanged": non_project_before == non_project_after,
            "non_project_count_before": len(non_project_before),
            "non_project_count_after": len(non_project_after),
        }

    # Check advertised storage endpoint in actual Iceberg REST LoadTable response.
    load_table_s3_endpoint = (lk_meta_mut.get("config") or {}).get("s3.endpoint")
    if not load_table_s3_endpoint:
        for sc in lk_meta_mut.get("storage_credentials") or []:
            ep = (sc.get("config") or {}).get("s3.endpoint")
            if ep:
                load_table_s3_endpoint = ep
                break

    passed = all(
        [
            bootstrap_second["lakekeeper"]["action"] == "already_bootstrapped",
            bootstrap_second["warehouse"]["action"] == "already_exists",
            bootstrap_first["endpoint_reachability"]["both_reachable"],
            rows_after_insert == expected_after_insert,
            rows_after_mut == expected_after_mut,
            eq_del_check["confirmed"],
            pinned_mut_rows == expected_after_mut,
            pinned_insert_rows == expected_after_insert,
            snap_after_mut != snap_after_insert,
            (keep_up or (
                cleanup_result["all_project_resources_removed"]
                and cleanup_result["non_project_containers_unchanged"]
            )),
        ]
    )

    summary = {
        "milestone": "M0",
        "outcome": "converged" if passed else "mismatch_or_failed",
        "passed": passed,
        "completed_at_utc": utc_now_iso(),
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
            "extensions": reader_versions.extensions,
        },
        "resources_after_pre_clean": resources_after_pre_clean,
        "connector_ready": connector_ready,
        "load_table_advertised_s3_endpoint": load_table_s3_endpoint,
        "step2_insert": {
            "commit_utc": insert_commit_utc,
            "visible_ms": round(insert_visible_ms, 2),
            "snapshot_id": snap_after_insert,
            "expected_rows": expected_after_insert,
            "observed_rows": rows_after_insert,
            "match": rows_after_insert == expected_after_insert,
            "poll_errors_before_ready": insert_poll_errors,
        },
        "step2_update_delete": {
            "commit_utc": mut_commit_utc,
            "visible_ms": round(mut_visible_ms, 2),
            "snapshot_id": snap_after_mut,
            "expected_rows": expected_after_mut,
            "observed_rows": rows_after_mut,
            "match": rows_after_mut == expected_after_mut,
            "poll_errors_before_ready": mut_poll_errors,
            "equality_delete_evidence": eq_del_check,
        },
        "step2_snapshot_pinned_reads": {
            "pinned_after_update_delete_snapshot_id": snap_after_mut,
            "pinned_after_update_delete_match": pinned_mut_rows == expected_after_mut,
            "pinned_after_update_delete_rows": pinned_mut_rows,
            "pinned_after_insert_snapshot_id": snap_after_insert,
            "pinned_after_insert_match": pinned_insert_rows == expected_after_insert,
            "pinned_after_insert_rows": pinned_insert_rows,
        },
        "pg_slots_final": slot_rows,
        "coordinator_tables": coord_tables,
        "step3_cleanup": cleanup_result,
    }
    write_json(run_dir / "summary.json", summary)
    print(json.dumps(summary, indent=2, default=_json_default))
    if not passed:
        raise SystemExit(1)
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Run M0 end-to-end verification.")
    parser.add_argument("--keep-up", action="store_true", help="Leave containers running after verification.")
    parser.add_argument("--label", default="m0", help="Label suffix for runs/<timestamp>-<label>/")
    args = parser.parse_args()
    run_m0(keep_up=args.keep_up, label=args.label)


if __name__ == "__main__":
    main()
