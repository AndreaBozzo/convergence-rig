"""Milestone K1 end-to-end runner: orphan file classification and cleanup after a catalog outage."""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import platform
import random
import re
import sys
import threading
import time
from typing import Any

import psycopg

from lab.bootstrap import _http_json, bootstrap_all
from lab.compare import (
    compare_ordered_streams,
    stream_duckdb_orders_at_snapshot,
    stream_postgres_orders,
    verify_iceberg_schema,
)
from lab.config import LabConfig, REPO_ROOT, load_config
from lab.l3 import (
    CatalogOutageController,
    summarize_outage_samples,
    summarize_pg2iceberg_logs,
)
from lab.l_common import (
    ALL_LAB_PROFILES,
    collect_digests_for_profiles,
    compose_cmd_with_profiles,
    project_teardown_and_verify,
)
from lab.m1 import summarize_duckdb_metadata
from lab.pipeline import (
    capture_pg_pipeline_positions,
    json_default,
    list_non_project_containers,
    list_project_resources,
    run_cmd,
    utc_now_iso,
    utc_run_stamp,
    verify_snapshot_convergence,
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
from lab.workload import (
    FINAL_MARKER_ID,
    STATUSES,
    WARMUP_MARKER_ID,
    SeedResult,
    _make_decimal_38_9,
    _make_toast_note,
    _make_toast_payload_json,
    commit_marker_row,
    run_paced_workload,
    seed_orders,
    verify_toast_storage,
)

CONCURRENT_INSERT_ID_START = 2_000_000_001
POST_CONCURRENT_MARKER_ID = 901_000_000


def normalize_s3_uri(uri: str) -> str:
    """Normalize S3 URIs (s3a/s3n -> s3, strip trailing slash, collapse redundant slashes)."""
    cleaned = uri.strip()
    for scheme in ("s3a://", "s3n://"):
        if cleaned.startswith(scheme):
            cleaned = "s3://" + cleaned[len(scheme) :]
    if cleaned.startswith("s3://"):
        rest = re.sub(r"/{2,}", "/", cleaned[5:]).rstrip("/")
        return "s3://" + rest
    return cleaned.rstrip("/")


def probe_lakekeeper_maintenance_api(
    cfg: LabConfig,
    warehouse_id: str,
    *,
    lakekeeper_image: str = (
        "quay.io/lakekeeper/catalog:v0.15.1"
        "@sha256:ca3487f3b781c2a916f64e9eff3e97709b60bab6f473cdaa065f995d02b15b54"
    ),
) -> dict[str, Any]:
    """Probe Lakekeeper v0.15.1 management API and OpenAPI spec for table-maintenance support."""
    base = cfg.lakekeeper_base_url
    checks: dict[str, dict[str, Any]] = {}
    endpoints: list[tuple[str, str, str, dict[str, Any] | None]] = [
        ("info", "GET", f"{base}/management/v1/info", None),
        ("warehouse_detail", "GET", f"{base}/management/v1/warehouse/{warehouse_id}", None),
        (
            "queue_soft_deletion",
            "GET",
            f"{base}/management/v1/warehouse/{warehouse_id}/task-queue/soft_deletion/config",
            None,
        ),
        (
            "queue_tabular_purge",
            "GET",
            f"{base}/management/v1/warehouse/{warehouse_id}/task-queue/tabular_purge/config",
            None,
        ),
        (
            "queue_task_log_cleanup",
            "GET",
            f"{base}/management/v1/project/task-queue/task_log_cleanup/config",
            None,
        ),
        (
            "queue_remove_orphan_files",
            "GET",
            f"{base}/management/v1/warehouse/{warehouse_id}/task-queue/remove_orphan_files/config",
            None,
        ),
        (
            "queue_expire_snapshots",
            "GET",
            f"{base}/management/v1/warehouse/{warehouse_id}/task-queue/expire_snapshots/config",
            None,
        ),
        (
            "queue_optimize",
            "GET",
            f"{base}/management/v1/warehouse/{warehouse_id}/task-queue/optimize/config",
            None,
        ),
        (
            "task_list",
            "POST",
            f"{base}/management/v1/warehouse/{warehouse_id}/task/list",
            {},
        ),
    ]
    for name, method, url, body in endpoints:
        status, payload = _http_json(url, method=method, body=body)
        checks[name] = {
            "method": method,
            "url": url,
            "status": status,
            "body": payload,
        }

    # Inspect management-openapi directly from the pinned Lakekeeper binary.
    openapi_proc = run_cmd(["docker", "run", "--rm", lakekeeper_image, "management-openapi"], check=False)
    openapi_task_queue_paths: list[str] = []
    openapi_maintenance_paths: list[str] = []
    if openapi_proc.returncode == 0:
        for path in re.findall(r"^  (/[^:]+):", openapi_proc.stdout, re.M):
            if "task-queue" in path:
                openapi_task_queue_paths.append(path)
            if any(
                k in path.lower()
                for k in ("task", "maint", "orphan", "expir", "delet", "purge", "optim")
            ):
                openapi_maintenance_paths.append(path)

    return {
        "captured_at_utc": utc_now_iso(),
        "lakekeeper_image": lakekeeper_image,
        "warehouse_id": warehouse_id,
        "api_probes": checks,
        "openapi_task_queue_paths": sorted(openapi_task_queue_paths),
        "openapi_maintenance_paths": sorted(openapi_maintenance_paths),
        "has_orphan_removal_queue": checks["queue_remove_orphan_files"]["status"] == 200,
        "has_expire_snapshots_queue": checks["queue_expire_snapshots"]["status"] == 200,
        "has_optimize_queue": checks["queue_optimize"]["status"] == 200,
    }


def list_silo_objects(cfg: LabConfig, *, prefix: str = "") -> list[dict[str, Any]]:
    """List every object in the synthetic Silo bucket (optionally under prefix) with exact metadata."""
    clean_prefix = prefix.strip("/")
    target = f"local/{cfg.silo_bucket}/{clean_prefix}" if clean_prefix else f"local/{cfg.silo_bucket}"
    shell_cmd = (
        f"mc alias set local http://127.0.0.1:{cfg.silo_port} "
        f"{cfg.silo_root_user} {cfg.silo_root_password} >/dev/null 2>&1 && "
        f"mc ls --recursive --json {target}"
    )
    proc = run_cmd(
        [*compose_cmd_with_profiles(cfg), "exec", "-T", "silo", "sh", "-c", shell_cmd],
        check=True,
    )
    objects: list[dict[str, Any]] = []
    for raw_line in proc.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if entry.get("type") != "file":
            continue
        rel_key = str(entry.get("key") or "").lstrip("/")
        if clean_prefix and not rel_key.startswith(clean_prefix + "/"):
            full_key = f"{clean_prefix}/{rel_key}"
        else:
            full_key = rel_key
        s3_uri = normalize_s3_uri(f"s3://{cfg.silo_bucket}/{full_key}")
        objects.append(
            {
                "key": full_key,
                "s3_uri": s3_uri,
                "size": int(entry.get("size") or 0),
                "last_modified": entry.get("lastModified"),
                "etag": entry.get("etag"),
            }
        )
    objects.sort(key=lambda o: o["key"])
    return objects


def fetch_pg2iceberg_log_index_paths(
    conn: psycopg.Connection[Any],
    *,
    bucket: str,
) -> dict[str, Any]:
    """Read _pg2iceberg.log_index rows to identify pg2iceberg staging object paths precisely."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '_pg2iceberg' AND table_name = 'log_index'
            ORDER BY ordinal_position
            """
        )
        columns = [r[0] for r in cur.fetchall()]
        if not columns:
            return {"columns": [], "row_count": 0, "staging_keys": [], "staging_uris": [], "sample_rows": []}

        cur.execute('SELECT * FROM _pg2iceberg."log_index" ORDER BY 1, 2')
        rows = [dict(zip(columns, r)) for r in cur.fetchall()]

    staging_keys: set[str] = set()
    staging_uris: set[str] = set()
    for row in rows:
        for col, val in row.items():
            if not isinstance(val, str):
                continue
            v = val.strip()
            if not v:
                continue
            if v.startswith("s3://") or v.startswith("s3a://"):
                uri = normalize_s3_uri(v)
                staging_uris.add(uri)
                prefix = f"s3://{bucket}/"
                if uri.startswith(prefix):
                    staging_keys.add(uri[len(prefix) :])
            elif "/" in v and (
                v.endswith(".parquet")
                or "log" in col.lower()
                or "path" in col.lower()
                or "key" in col.lower()
                or "file" in col.lower()
                or "uri" in col.lower()
            ):
                key = v.lstrip("/")
                staging_keys.add(key)
                staging_uris.add(normalize_s3_uri(f"s3://{bucket}/{key}"))

    return {
        "columns": columns,
        "row_count": len(rows),
        "staging_keys": sorted(staging_keys),
        "staging_uris": sorted(staging_uris),
        "sample_rows": rows[:10],
    }


def collect_referenced_s3_uris(
    lakekeeper_meta: Mapping[str, Any],
    spark_inspect: Mapping[str, Any],
) -> dict[str, str]:
    """Collect every S3 URI referenced by any Iceberg snapshot, manifest, or metadata log entry."""
    referenced: dict[str, str] = {}

    def _add(uri: Any, role: str) -> None:
        if not isinstance(uri, str) or not uri.strip():
            return
        norm = normalize_s3_uri(uri)
        # Prefer more specific role if already present? Keep first deterministic priority order.
        if norm not in referenced:
            referenced[norm] = role

    meta = lakekeeper_meta.get("metadata") or {}
    _add(lakekeeper_meta.get("metadata_location"), "metadata_json")
    for entry in meta.get("metadata-log") or []:
        if isinstance(entry, Mapping):
            _add(entry.get("metadata-file"), "metadata_json")
    for entry in spark_inspect.get("metadata_log_entries") or []:
        if isinstance(entry, Mapping):
            _add(entry.get("file"), "metadata_json")

    for snap in meta.get("snapshots") or []:
        if isinstance(snap, Mapping):
            _add(snap.get("manifest-list"), "manifest_list")
    for snap in spark_inspect.get("snapshots") or []:
        if isinstance(snap, Mapping):
            _add(snap.get("manifest_list"), "manifest_list")

    for m in spark_inspect.get("all_manifests") or []:
        if isinstance(m, Mapping):
            _add(m.get("path"), "manifest")

    for f in spark_inspect.get("all_data_files") or []:
        if isinstance(f, Mapping):
            _add(f.get("file_path"), "data_file")
    for f in spark_inspect.get("all_delete_files") or []:
        if isinstance(f, Mapping):
            _add(f.get("file_path"), "delete_file")
    for f in spark_inspect.get("current_files") or []:
        if isinstance(f, Mapping):
            role = "delete_file" if int(f.get("content", 0)) in (1, 2) else "data_file"
            _add(f.get("file_path"), role)

    for stat_key in ("statistics", "partition-statistics"):
        for s in meta.get(stat_key) or []:
            if isinstance(s, Mapping):
                _add(s.get("statistics-path"), "statistics")

    return referenced


def _is_staging_object(
    s3_uri: str,
    rel_path: str,
    staging_uri_set: set[str],
    staging_key_set: set[str],
    full_key: str,
) -> tuple[bool, bool]:
    """Return (is_staging, matched_log_index) for an unreferenced object."""
    if s3_uri in staging_uri_set or full_key in staging_key_set or rel_path in staging_key_set:
        return True, True
    # Check if any log_index key is a suffix of full_key or rel_path (when log_index stores relative paths).
    for sk in staging_key_set:
        if sk and (full_key.endswith("/" + sk) or rel_path == sk):
            return True, True
    # Fallback prefix heuristic for pg2iceberg staging folders outside data/ and metadata/.
    lowered = "/" + rel_path.lower().lstrip("/")
    if (
        not lowered.startswith("/data/")
        and not lowered.startswith("/metadata/")
        and any(
            tok in lowered
            for tok in ("/_pg2iceberg", "/pg2iceberg", "/staged/", "/staging/", "/stage/", "/logs/", "/log/")
        )
    ):
        return True, False
    return False, False


def classify_table_objects(
    silo_objects: Sequence[Mapping[str, Any]],
    table_location: str,
    referenced_by_uri: Mapping[str, str],
    staging_uris: Iterable[str] = (),
    staging_keys: Iterable[str] = (),
) -> dict[str, Any]:
    """Classify every object under table_location as referenced, staging, or unreferenced."""
    norm_loc = normalize_s3_uri(table_location)
    loc_prefix = norm_loc + "/"
    staging_uri_set = {normalize_s3_uri(u) for u in staging_uris if u}
    staging_key_set = {str(k).strip().lstrip("/") for k in staging_keys if k}

    under_table: list[dict[str, Any]] = []
    outside_table: list[dict[str, Any]] = []

    counts: dict[str, int] = {"referenced": 0, "staging": 0, "unreferenced": 0}
    bytes_by_class: dict[str, int] = {"referenced": 0, "staging": 0, "unreferenced": 0}
    by_sub_role: dict[str, dict[str, int]] = {}

    for obj in silo_objects:
        uri = normalize_s3_uri(str(obj["s3_uri"]))
        key = str(obj["key"]).lstrip("/")
        size = int(obj["size"])
        last_mod = obj.get("last_modified")

        if uri.startswith(loc_prefix):
            rel_path = uri[len(loc_prefix) :]
            if uri in referenced_by_uri:
                classification = "referenced"
                sub_role = referenced_by_uri[uri]
                matched_log_index = False
            else:
                is_stg, matched_log_index = _is_staging_object(
                    uri, rel_path, staging_uri_set, staging_key_set, key
                )
                if is_stg:
                    classification = "staging"
                    sub_role = "pg2iceberg_staging"
                else:
                    classification = "unreferenced"
                    if rel_path.startswith("data/"):
                        sub_role = "orphan_data_or_delete"
                    elif rel_path.startswith("metadata/"):
                        sub_role = "orphan_metadata"
                    else:
                        sub_role = "orphan_other"

            counts[classification] += 1
            bytes_by_class[classification] += size
            sr = by_sub_role.setdefault(sub_role, {"count": 0, "bytes": 0})
            sr["count"] += 1
            sr["bytes"] += size
            under_table.append(
                {
                    "key": key,
                    "rel_path": rel_path,
                    "s3_uri": uri,
                    "size": size,
                    "last_modified": last_mod,
                    "classification": classification,
                    "sub_role": sub_role,
                    "matched_log_index": matched_log_index,
                }
            )
        else:
            is_stg, matched_log_index = _is_staging_object(
                uri, key, staging_uri_set, staging_key_set, key
            )
            outside_table.append(
                {
                    "key": key,
                    "s3_uri": uri,
                    "size": size,
                    "last_modified": last_mod,
                    "classification": "staging" if is_stg else "other_outside_table",
                    "matched_log_index": matched_log_index,
                }
            )

    referenced_uris_missing_in_storage = sorted(
        u for u in referenced_by_uri if u.startswith(loc_prefix) and u not in {o["s3_uri"] for o in under_table}
    )

    return {
        "table_location": norm_loc,
        "total_under_table_objects": len(under_table),
        "total_under_table_bytes": sum(o["size"] for o in under_table),
        "counts_by_classification": counts,
        "bytes_by_classification": bytes_by_class,
        "by_sub_role": by_sub_role,
        "referenced_uris_missing_in_storage": referenced_uris_missing_in_storage,
        "unreferenced_objects": [o for o in under_table if o["classification"] == "unreferenced"],
        "staging_objects_under_table": [o for o in under_table if o["classification"] == "staging"],
        "objects_under_table": under_table,
        "outside_table_count": len(outside_table),
        "outside_table_staging_count": sum(1 for o in outside_table if o["classification"] == "staging"),
        "outside_table_objects": outside_table,
    }


def evaluate_cleanup_diff(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify cleanup removed exactly the unreferenced objects and left every referenced/staging object."""
    before_under = {o["s3_uri"]: o for o in before["objects_under_table"]}
    after_under = {o["s3_uri"]: o for o in after["objects_under_table"]}
    before_out = {o["s3_uri"]: o for o in before["outside_table_objects"]}
    after_out = {o["s3_uri"]: o for o in after["outside_table_objects"]}

    removed_uris = sorted(set(before_under) - set(after_under))
    added_uris = sorted(set(after_under) - set(before_under))
    removed_outside_uris = sorted(set(before_out) - set(after_out))

    removed_unreferenced = [u for u in removed_uris if before_under[u]["classification"] == "unreferenced"]
    removed_referenced = [u for u in removed_uris if before_under[u]["classification"] == "referenced"]
    removed_staging_under = [u for u in removed_uris if before_under[u]["classification"] == "staging"]
    removed_staging_outside = [u for u in removed_outside_uris if before_out[u]["classification"] == "staging"]
    removed_staging = sorted(removed_staging_under + removed_staging_outside)

    before_unreferenced = sorted(
        u for u, o in before_under.items() if o["classification"] == "unreferenced"
    )
    remaining_unreferenced = sorted(
        u for u, o in after_under.items() if o["classification"] == "unreferenced"
    )

    reclaimed_bytes = sum(int(before_under[u]["size"]) for u in removed_unreferenced)

    exact_orphan_removal_passed = (
        len(before_unreferenced) > 0
        and removed_unreferenced == before_unreferenced
        and len(removed_referenced) == 0
        and len(removed_staging) == 0
        and len(remaining_unreferenced) == 0
        and len(after.get("referenced_uris_missing_in_storage") or []) == 0
    )

    return {
        "before_unreferenced_count": len(before_unreferenced),
        "before_unreferenced_uris": before_unreferenced,
        "removed_total_under_table": len(removed_uris),
        "removed_unreferenced_count": len(removed_unreferenced),
        "removed_unreferenced_uris": removed_unreferenced,
        "removed_referenced_count": len(removed_referenced),
        "removed_referenced_uris": removed_referenced,
        "removed_staging_count": len(removed_staging),
        "removed_staging_uris": removed_staging,
        "remaining_unreferenced_count": len(remaining_unreferenced),
        "remaining_unreferenced_uris": remaining_unreferenced,
        "added_under_table_count": len(added_uris),
        "added_under_table_uris": added_uris,
        "reclaimed_bytes": reclaimed_bytes,
        "exact_orphan_removal_passed": exact_orphan_removal_passed,
    }


def evaluate_concurrent_cleanup(
    *,
    before_classified: Mapping[str, Any],
    after_classified: Mapping[str, Any],
    spark_deleted_uris: Sequence[str],
    writes_during_cleanup: int,
    snapshots_before: Sequence[int],
    snapshots_after: Sequence[int],
) -> dict[str, Any]:
    """Verify concurrent default-retention remove_orphan_files removed zero live or staging files."""
    before_under = {o["s3_uri"]: o for o in before_classified["objects_under_table"]}
    after_under = {o["s3_uri"]: o for o in after_classified["objects_under_table"]}
    before_out = {o["s3_uri"]: o for o in before_classified["outside_table_objects"]}
    after_out = {o["s3_uri"]: o for o in after_classified["outside_table_objects"]}

    before_referenced = {u for u, o in before_under.items() if o["classification"] == "referenced"}
    removed_referenced = sorted(before_referenced - set(after_under))
    missing_after_referenced = sorted(after_classified.get("referenced_uris_missing_in_storage") or [])
    removed_live_files = sorted(set(removed_referenced) | set(missing_after_referenced))

    before_staging_under = {u for u, o in before_under.items() if o["classification"] == "staging"}
    before_staging_out = {u for u, o in before_out.items() if o["classification"] == "staging"}
    removed_staging = sorted(
        (before_staging_under - set(after_under)) | (before_staging_out - set(after_out))
    )

    new_objects_under_table = sorted(set(after_under) - set(before_under))
    new_snapshots = [sid for sid in snapshots_after if sid not in set(snapshots_before)]

    passed = (
        len(removed_live_files) == 0
        and len(removed_staging) == 0
        and len(spark_deleted_uris) == 0
        and writes_during_cleanup > 0
        and len(new_snapshots) > 0
    )

    return {
        "passed": passed,
        "spark_deleted_count": len(spark_deleted_uris),
        "spark_deleted_uris": sorted(str(u) for u in spark_deleted_uris),
        "removed_live_file_count": len(removed_live_files),
        "removed_live_files": removed_live_files,
        "removed_staging_file_count": len(removed_staging),
        "removed_staging_files": removed_staging,
        "new_objects_created_under_table": len(new_objects_under_table),
        "writes_committed_during_cleanup": writes_during_cleanup,
        "snapshots_before_count": len(snapshots_before),
        "snapshots_after_count": len(snapshots_after),
        "new_snapshots_count": len(new_snapshots),
        "new_snapshot_ids": new_snapshots,
    }


def _run_spark_script(
    cfg: LabConfig,
    logged_cmd: Any,
    *,
    mode: str,
    out_container_path: str,
    out_host_path: Path,
    stdout_log_path: Path,
    stderr_log_path: Path,
    older_than: str = "",
    location: str = "",
    dry_run: str = "omit",
    prefix_mismatch_mode: str = "",
    prefix_listing: str = "omit",
    allow_error: bool = False,
) -> dict[str, Any]:
    cmd = [
        *compose_cmd_with_profiles(cfg, ("spark",)),
        "exec",
        "-T",
        "spark",
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "/opt/cdclab/scripts/orphan_cleanup.py",
        "--mode",
        mode,
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
        "--dry-run",
        dry_run,
        "--prefix-listing",
        prefix_listing,
        "--out-json",
        out_container_path,
    ]
    if older_than:
        cmd.extend(["--older-than", older_than])
    if location:
        cmd.extend(["--location", location])
    if prefix_mismatch_mode:
        cmd.extend(["--prefix-mismatch-mode", prefix_mismatch_mode])

    proc = logged_cmd(cmd, check=False)
    stdout_log_path.write_text(proc.stdout, encoding="utf-8")
    stderr_log_path.write_text(proc.stderr, encoding="utf-8")

    cat_proc = run_cmd(
        [*compose_cmd_with_profiles(cfg, ("spark",)), "exec", "-T", "spark", "cat", out_container_path],
        check=False,
    )
    if cat_proc.returncode != 0 or not cat_proc.stdout.strip():
        raise RuntimeError(
            f"Spark {mode} failed (rc={proc.returncode}) and produced no JSON at {out_container_path}\n"
            f"STDERR:\n{proc.stderr[-2000:]}"
        )
    out_host_path.write_text(cat_proc.stdout, encoding="utf-8")
    payload = json.loads(cat_proc.stdout)
    if not allow_error and (proc.returncode != 0 or not payload.get("ok")):
        raise RuntimeError(
            f"Spark {mode} returned error (rc={proc.returncode}): {payload.get('error')}"
        )
    return payload


class ConcurrentBurstWriter:
    """Paced background writer exercising updates, TOAST updates, inserts and deletes during cleanup."""

    def __init__(
        self,
        cfg: LabConfig,
        *,
        rate_ops_s: float = 10.0,
        rng_seed: int = 20261199,
        ops_jsonl: Path,
        toast_ids: Sequence[int],
    ) -> None:
        self._cfg = cfg
        self._rate_ops_s = rate_ops_s
        self._rng = random.Random(rng_seed)
        self._ops_jsonl = ops_jsonl
        self._toast_ids = list(toast_ids)
        self._stop = threading.Event()
        self._started = threading.Event()
        self._lock = threading.Lock()
        self._ops: list[dict[str, Any]] = []
        self._error: str | None = None
        self._started_utc: str | None = None
        self._stopped_utc: str | None = None
        self._thread = threading.Thread(target=self._run, name="k1-concurrent-writer", daemon=True)

    def start(self) -> None:
        self._thread.start()
        self._started.wait(timeout=30.0)

    def stop_and_collect(self) -> dict[str, Any]:
        self._stop.set()
        self._thread.join(timeout=60.0)
        with self._lock:
            ops = list(self._ops)
        ok_ops = [o for o in ops if o["outcome"] == "ok"]
        err_ops = [o for o in ops if o["outcome"] != "ok"]
        op_counts: dict[str, int] = {}
        for o in ok_ops:
            k = o["op_kind"]
            op_counts[k] = op_counts.get(k, 0) + 1
        return {
            "started_utc": self._started_utc,
            "stopped_utc": self._stopped_utc,
            "total_ops": len(ops),
            "ok_ops": len(ok_ops),
            "error_ops": len(err_ops),
            "op_counts": op_counts,
            "fatal_error": self._error,
            "ops": ops,
        }

    def count_ops_in_mono_window(self, start_ns: int, end_ns: int) -> dict[str, int]:
        with self._lock:
            ops = list(self._ops)
        ok_in = 0
        err_in = 0
        for o in ops:
            c_ns = o.get("complete_mono_ns")
            if isinstance(c_ns, int) and start_ns <= c_ns <= end_ns:
                if o["outcome"] == "ok":
                    ok_in += 1
                else:
                    err_in += 1
        return {"ok": ok_in, "error": err_in}

    def _run(self) -> None:
        interval_ns = int(round(1_000_000_000 / self._rate_ops_s))
        next_insert_id = CONCURRENT_INSERT_ID_START
        toast_rev = 1000
        try:
            with psycopg.connect(self._cfg.pg_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id FROM public.orders WHERE id > 112 AND id < 900000000 ORDER BY id"
                    )
                    mutable_ids = [int(r[0]) for r in cur.fetchall()]
                    cur.execute(
                        "SELECT id FROM public.orders WHERE id BETWEEN 13 AND 112 ORDER BY id"
                    )
                    live_toast_ids = [int(r[0]) for r in cur.fetchall()] or list(self._toast_ids)
                conn.commit()

                self._ops_jsonl.parent.mkdir(parents=True, exist_ok=True)
                t0_ns = time.monotonic_ns()
                self._started_utc = utc_now_iso()
                self._started.set()

                seq = 0
                with self._ops_jsonl.open("w", encoding="utf-8", buffering=1) as fh:
                    while not self._stop.is_set():
                        target_ns = t0_ns + seq * interval_ns
                        rem_s = (target_ns - time.monotonic_ns()) / 1_000_000_000.0
                        if rem_s > 0:
                            if self._stop.wait(timeout=rem_s):
                                break
                        seq += 1
                        u = self._rng.random()
                        if u < 0.45 and mutable_ids:
                            op_kind = "update_normal"
                        elif u < 0.60 and live_toast_ids:
                            op_kind = "update_toast_unchanged"
                        elif u < 0.75 and live_toast_ids:
                            op_kind = "update_toast_changed"
                        elif u < 0.90 or len(mutable_ids) <= 20:
                            op_kind = "insert"
                        else:
                            op_kind = "delete"

                        now_dt = datetime.now(timezone.utc)
                        target_id = -1
                        outcome = "ok"
                        err_msg: str | None = None
                        try:
                            with conn.cursor() as cur:
                                if op_kind == "update_normal":
                                    target_id = mutable_ids[self._rng.randrange(len(mutable_ids))]
                                    cur.execute(
                                        """
                                        UPDATE public.orders
                                        SET status = %s, amount = %s, updated_at = %s, note = %s
                                        WHERE id = %s
                                        """,
                                        (
                                            STATUSES[self._rng.randrange(len(STATUSES))],
                                            _make_decimal_38_9(self._rng),
                                            now_dt,
                                            f"k1-concurrent-seq-{seq}",
                                            target_id,
                                        ),
                                    )
                                elif op_kind == "update_toast_unchanged":
                                    target_id = live_toast_ids[self._rng.randrange(len(live_toast_ids))]
                                    cur.execute(
                                        """
                                        UPDATE public.orders
                                        SET status = %s, amount = %s, updated_at = %s
                                        WHERE id = %s
                                        """,
                                        (
                                            STATUSES[self._rng.randrange(len(STATUSES))],
                                            _make_decimal_38_9(self._rng),
                                            now_dt,
                                            target_id,
                                        ),
                                    )
                                elif op_kind == "update_toast_changed":
                                    target_id = live_toast_ids[self._rng.randrange(len(live_toast_ids))]
                                    toast_rev += 1
                                    cur.execute(
                                        """
                                        UPDATE public.orders
                                        SET status = %s, amount = %s, updated_at = %s,
                                            note = %s, payload = %s::jsonb
                                        WHERE id = %s
                                        """,
                                        (
                                            STATUSES[self._rng.randrange(len(STATUSES))],
                                            _make_decimal_38_9(self._rng),
                                            now_dt,
                                            _make_toast_note(self._rng, target_id, rev=toast_rev),
                                            _make_toast_payload_json(self._rng, target_id, rev=toast_rev),
                                            target_id,
                                        ),
                                    )
                                elif op_kind == "insert":
                                    target_id = next_insert_id
                                    next_insert_id += 1
                                    cur.execute(
                                        """
                                        INSERT INTO public.orders
                                            (id, status, amount, updated_at, note, payload, discount)
                                        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
                                        """,
                                        (
                                            target_id,
                                            STATUSES[self._rng.randrange(len(STATUSES))],
                                            _make_decimal_38_9(self._rng),
                                            now_dt,
                                            f"k1-concurrent-insert-{seq}",
                                            json.dumps({"k1_concurrent": True, "seq": seq}),
                                            None,
                                        ),
                                    )
                                    mutable_ids.append(target_id)
                                else:
                                    idx = self._rng.randrange(len(mutable_ids))
                                    target_id = mutable_ids.pop(idx)
                                    cur.execute("DELETE FROM public.orders WHERE id = %s", (target_id,))
                            conn.commit()
                        except Exception as exc:
                            conn.rollback()
                            outcome = "error"
                            err_msg = f"{type(exc).__name__}: {exc}"

                        complete_ns = time.monotonic_ns()
                        rec = {
                            "seq": seq,
                            "op_kind": op_kind,
                            "target_id": target_id,
                            "outcome": outcome,
                            "error": err_msg,
                            "complete_utc": utc_now_iso(),
                            "complete_mono_ns": complete_ns,
                        }
                        with self._lock:
                            self._ops.append(rec)
                        fh.write(json.dumps({k: v for k, v in rec.items() if k != "complete_mono_ns"}) + "\n")
        except Exception as exc:
            self._error = f"{type(exc).__name__}: {exc}"
            self._started.set()
        finally:
            self._stopped_utc = utc_now_iso()


def run_exact_comparison_at_snapshot(
    cfg: LabConfig,
    snapshot_id: int,
    *,
    batch_size: int = 2000,
) -> dict[str, Any]:
    """Run exact schema + bounded-batch PG-vs-DuckDB comparison at snapshot_id."""
    lk_meta = fetch_lakekeeper_table_metadata(
        cfg, namespace=cfg.lakekeeper_namespace, table="orders"
    )
    assert lk_meta is not None
    schema_res = verify_iceberg_schema(lk_meta)
    con, _ = connect_iceberg(cfg)
    try:
        with psycopg.connect(cfg.pg_dsn) as pg_conn:
            pg_conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            source_read_opened_utc = utc_now_iso()
            row_res = compare_ordered_streams(
                stream_postgres_orders(pg_conn, batch_size=batch_size),
                stream_duckdb_orders_at_snapshot(
                    con,
                    snapshot_id,
                    namespace=cfg.lakekeeper_namespace,
                    batch_size=batch_size,
                ),
                max_examples=20,
            )
            pg_conn.rollback()
    finally:
        con.close()

    return {
        "snapshot_id": snapshot_id,
        "source_read_opened_utc": source_read_opened_utc,
        "checked_at_utc": utc_now_iso(),
        "passed": schema_res.passed and row_res.passed,
        "schema_check": schema_res.to_dict(),
        "row_comparison": row_res.to_dict(),
    }


def run_k1(
    *,
    seed_rows: int = 25000,
    toast_rows: int = 100,
    duration_s: float = 180.0,
    rate_ops_s: float = 10.0,
    rng_seed: int = 20261119,
    batch_size: int = 2000,
    probe_interval_s: float = 20.0,
    probe_poll_interval_s: float = 0.5,
    outage_delay_s: float = 65.0,
    outage_duration_s: float = 60.0,
    sample_interval_s: float = 2.0,
    convergence_timeout_s: float = 240.0,
    concurrent_pre_delay_s: float = 12.0,
    concurrent_post_delay_s: float = 12.0,
    label: str = "k1-orphan-cleanup",
    keep_up: bool = False,
) -> Path:
    """Execute Milestone K1 end to end on a fresh cdclab stack."""
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
    warehouse_id = str(bootstrap_res["warehouse"]["warehouse_id"])

    # 3. Step 1: Probe Lakekeeper v0.15.1 maintenance API and OpenAPI spec.
    lk_maint_probe = probe_lakekeeper_maintenance_api(cfg, warehouse_id)
    write_json(run_dir / "lakekeeper_maintenance_probe.json", lk_maint_probe)

    # 4. Seed 25,000-row fixture (100 TOAST rows), start pg2iceberg, wait for snapshot + warm-up.
    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        seed_res: SeedResult = seed_orders(
            pg_conn,
            seed_rows=seed_rows,
            toast_row_count=toast_rows,
            rng_seed=rng_seed,
        )
        pg_pos_after_seed = capture_pg_pipeline_positions(pg_conn)

    logged_cmd([*compose_cmd_with_profiles(cfg, ("connector",)), "up", "-d", "pg2iceberg"])
    con_v, reader_versions = connect_iceberg(cfg)
    con_v.close()
    initial_snapshot_info = wait_for_connector_and_snapshot(cfg, timeout_s=180.0)

    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        warmup_commit_ns, warmup_commit_utc = commit_marker_row(
            pg_conn,
            marker_id=WARMUP_MARKER_ID,
            status="marker_warmup",
            note="k1 warm-up marker before outage workload",
        )
    warmup_vis = wait_for_marker_visible(
        cfg, WARMUP_MARKER_ID, warmup_commit_ns, timeout_s=120.0, poll_interval_s=0.5
    )
    if not warmup_vis["visible"]:
        raise RuntimeError(f"Warm-up marker not visible within timeout: {warmup_vis}")

    # 5. Step 2: Reproduce the L3 outage on 25,000-row fixture with connector restart so orphans exist.
    controller = CatalogOutageController(
        cfg,
        outage_delay_s=outage_delay_s,
        outage_duration_s=outage_duration_s,
        sample_interval_s=sample_interval_s,
        samples_jsonl=run_dir / "outage_samples.jsonl",
        restart_connector_after_recovery=True,
    )
    workload_summary = run_paced_workload(
        cfg,
        seed_res,
        duration_s=duration_s,
        rate_ops_s=rate_ops_s,
        rng_seed=rng_seed,
        ops_jsonl=run_dir / "ops.jsonl",
        probes_jsonl=run_dir / "steady_state_probes.jsonl",
        probe_interval_s=probe_interval_s,
        probe_poll_interval_s=probe_poll_interval_s,
        on_window_start=controller.start_for_window,
    )
    controller.wait_outage_done(timeout_s=240.0)

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as pg_conn:
        toast_after_workload = verify_toast_storage(pg_conn)

    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        final_commit_ns, final_commit_utc = commit_marker_row(
            pg_conn,
            marker_id=FINAL_MARKER_ID,
            status="marker_final",
            note="k1 pre-cleanup convergence marker",
        )

    convergence_before = verify_snapshot_convergence(
        cfg,
        FINAL_MARKER_ID,
        final_commit_ns,
        timeout_s=convergence_timeout_s,
        poll_interval_s=1.0,
        batch_size=batch_size,
    )
    outage_events, outage_samples = controller.stop_and_collect()
    commands_executed.extend(outage_events.get("commands") or [])
    outage_summary = summarize_outage_samples(outage_samples)

    lk_meta_before = convergence_before.pop("final_lakekeeper_metadata", None)
    if lk_meta_before is None:
        lk_meta_before = fetch_lakekeeper_table_metadata(
            cfg, namespace=cfg.lakekeeper_namespace, table="orders"
        )
    assert lk_meta_before is not None
    write_json(run_dir / "lakekeeper_table_before_cleanup.json", lk_meta_before)

    if not convergence_before["passed"] or convergence_before["verified_snapshot_id"] is None:
        raise RuntimeError(f"Pre-cleanup convergence failed: {convergence_before}")
    verified_snap_before = int(convergence_before["verified_snapshot_id"])
    table_location = normalize_s3_uri(str((lk_meta_before.get("metadata") or {}).get("location") or ""))

    # Start L1 Spark container and capture digests across connector + spark.
    logged_cmd([*compose_cmd_with_profiles(cfg, ("spark",)), "up", "-d", "spark"])
    digests = collect_digests_for_profiles(cfg, ("connector", "spark"))

    # Capture before-cleanup DuckDB metadata, Silo object listing, _pg2iceberg.log_index, and Spark inspect.
    con_duck, _ = connect_iceberg(cfg)
    try:
        duckdb_meta_before = read_metadata_files(con_duck, namespace=cfg.lakekeeper_namespace)
        duckdb_snaps_before = read_snapshots(con_duck, namespace=cfg.lakekeeper_namespace)
    finally:
        con_duck.close()
    write_json(run_dir / "duckdb_metadata_before_cleanup.json", duckdb_meta_before)
    write_json(run_dir / "duckdb_snapshots_before_cleanup.json", duckdb_snaps_before)

    silo_objects_before = list_silo_objects(cfg)
    write_json(run_dir / "silo_objects_before_cleanup.json", silo_objects_before)

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as pg_conn:
        log_index_before = fetch_pg2iceberg_log_index_paths(pg_conn, bucket=cfg.silo_bucket)
        pg_pos_before_cleanup = capture_pg_pipeline_positions(pg_conn)
    write_json(run_dir / "pg_log_index_before_cleanup.json", log_index_before)

    spark_inspect_before = _run_spark_script(
        cfg,
        logged_cmd,
        mode="inspect",
        out_container_path="/tmp/cdclab-spark/k1_inspect_before.json",
        out_host_path=run_dir / "spark_inspect_before.json",
        stdout_log_path=run_dir / "spark_inspect_before.stdout.log",
        stderr_log_path=run_dir / "spark_inspect_before.stderr.log",
    )

    referenced_before = collect_referenced_s3_uris(lk_meta_before, spark_inspect_before)
    classification_before = classify_table_objects(
        silo_objects_before,
        table_location,
        referenced_before,
        staging_uris=log_index_before["staging_uris"],
        staging_keys=log_index_before["staging_keys"],
    )
    write_json(run_dir / "classification_before_cleanup.json", classification_before)

    # Choose an explicit older_than timestamp strictly after the orphan objects' lastModified
    # (and strictly before 'now' if orphans are already >10s old, otherwise now + 5s).
    cutoff_dt = datetime.now(timezone.utc) - timedelta(seconds=5)
    for orphan in classification_before["unreferenced_objects"]:
        lm = orphan.get("last_modified")
        if isinstance(lm, str) and lm:
            parsed = datetime.fromisoformat(lm.replace("Z", "+00:00")).astimezone(timezone.utc)
            if parsed >= cutoff_dt:
                cutoff_dt = parsed + timedelta(seconds=5)
    older_than_sql = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    # 6. Step 3a: Document SQL procedure's 24-hour guard on short older_than, then use Iceberg's
    #    official SparkActions.deleteOrphanFiles API (dry-run root vs /data, then actual deletion).
    proc_short_interval = _run_spark_script(
        cfg,
        logged_cmd,
        mode="remove_orphans",
        older_than=older_than_sql,
        location="",
        dry_run="true",
        prefix_listing="true",
        out_container_path="/tmp/cdclab-spark/k1_proc_short_interval.json",
        out_host_path=run_dir / "spark_procedure_short_interval_rejected.json",
        stdout_log_path=run_dir / "spark_procedure_short_interval_rejected.stdout.log",
        stderr_log_path=run_dir / "spark_procedure_short_interval_rejected.stderr.log",
        allow_error=True,
    )

    dry_run_root = _run_spark_script(
        cfg,
        logged_cmd,
        mode="action_remove_orphans",
        older_than=older_than_sql,
        location="",
        dry_run="true",
        prefix_listing="true",
        out_container_path="/tmp/cdclab-spark/k1_dry_run_root.json",
        out_host_path=run_dir / "spark_dry_run_root.json",
        stdout_log_path=run_dir / "spark_dry_run_root.stdout.log",
        stderr_log_path=run_dir / "spark_dry_run_root.stderr.log",
    )
    data_location = f"{table_location}/data"
    dry_run_data = _run_spark_script(
        cfg,
        logged_cmd,
        mode="action_remove_orphans",
        older_than=older_than_sql,
        location=data_location,
        dry_run="true",
        prefix_listing="true",
        out_container_path="/tmp/cdclab-spark/k1_dry_run_data.json",
        out_host_path=run_dir / "spark_dry_run_data.json",
        stdout_log_path=run_dir / "spark_dry_run_data.stdout.log",
        stderr_log_path=run_dir / "spark_dry_run_data.stderr.log",
    )

    staging_uris_under_table = {
        o["s3_uri"] for o in classification_before["staging_objects_under_table"]
    }
    root_would_delete_staging = sorted(
        normalize_s3_uri(u)
        for u in dry_run_root["orphan_file_locations"]
        if normalize_s3_uri(u) in staging_uris_under_table
    )
    # Scope actual cleanup to <table_location>/data when root scan would touch pg2iceberg staging.
    cleanup_location = data_location if root_would_delete_staging else ""

    # Execute actual deleteOrphanFiles (dry_run => false) using Lakekeeper vended STS credentials.
    cleanup_actual = _run_spark_script(
        cfg,
        logged_cmd,
        mode="action_remove_orphans",
        older_than=older_than_sql,
        location=cleanup_location,
        dry_run="false",
        prefix_listing="true",
        out_container_path="/tmp/cdclab-spark/k1_cleanup_actual.json",
        out_host_path=run_dir / "spark_cleanup_actual.json",
        stdout_log_path=run_dir / "spark_cleanup_actual.stdout.log",
        stderr_log_path=run_dir / "spark_cleanup_actual.stderr.log",
    )

    # Re-list Silo, re-inspect Spark metadata, classify after cleanup, and verify exact PG-vs-DuckDB match.
    silo_objects_after = list_silo_objects(cfg)
    write_json(run_dir / "silo_objects_after_cleanup.json", silo_objects_after)

    lk_meta_after = fetch_lakekeeper_table_metadata(
        cfg, namespace=cfg.lakekeeper_namespace, table="orders"
    )
    assert lk_meta_after is not None
    write_json(run_dir / "lakekeeper_table_after_cleanup.json", lk_meta_after)

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as pg_conn:
        log_index_after = fetch_pg2iceberg_log_index_paths(pg_conn, bucket=cfg.silo_bucket)
    spark_inspect_after = _run_spark_script(
        cfg,
        logged_cmd,
        mode="inspect",
        out_container_path="/tmp/cdclab-spark/k1_inspect_after.json",
        out_host_path=run_dir / "spark_inspect_after.json",
        stdout_log_path=run_dir / "spark_inspect_after.stdout.log",
        stderr_log_path=run_dir / "spark_inspect_after.stderr.log",
    )
    referenced_after = collect_referenced_s3_uris(lk_meta_after, spark_inspect_after)
    classification_after = classify_table_objects(
        silo_objects_after,
        table_location,
        referenced_after,
        staging_uris=log_index_after["staging_uris"],
        staging_keys=log_index_after["staging_keys"],
    )
    write_json(run_dir / "classification_after_cleanup.json", classification_after)

    cleanup_diff = evaluate_cleanup_diff(classification_before, classification_after)
    write_json(run_dir / "cleanup_diff.json", cleanup_diff)

    post_cleanup_comparison = run_exact_comparison_at_snapshot(
        cfg, verified_snap_before, batch_size=batch_size
    )
    write_json(run_dir / "post_cleanup_comparison.json", post_cleanup_comparison)

    # 7. Step 3b: Run remove_orphan_files a second time WHILE pg2iceberg is actively writing,
    #    using default retention (older_than omitted = Iceberg 3-day default).
    snapshots_before_concurrent = [
        int(s["snapshot_id"]) for s in spark_inspect_after.get("snapshots") or []
    ]
    concurrent_writer = ConcurrentBurstWriter(
        cfg,
        rate_ops_s=rate_ops_s,
        rng_seed=rng_seed + 80,
        ops_jsonl=run_dir / "concurrent_ops.jsonl",
        toast_ids=seed_res.toast_ids,
    )
    concurrent_writer.start()
    try:
        # Let writes flow for ~12s so pg2iceberg is actively staging & committing snapshots.
        time.sleep(concurrent_pre_delay_s)

        concurrent_spark_start_ns = time.monotonic_ns()
        concurrent_spark_start_utc = utc_now_iso()
        concurrent_cleanup = _run_spark_script(
            cfg,
            logged_cmd,
            mode="remove_orphans",
            older_than="",
            location=cleanup_location,
            dry_run="omit",
            prefix_listing="true",
            out_container_path="/tmp/cdclab-spark/k1_cleanup_concurrent.json",
            out_host_path=run_dir / "spark_cleanup_concurrent.json",
            stdout_log_path=run_dir / "spark_cleanup_concurrent.stdout.log",
            stderr_log_path=run_dir / "spark_cleanup_concurrent.stderr.log",
        )
        concurrent_spark_end_ns = time.monotonic_ns()
        concurrent_spark_end_utc = utc_now_iso()
        writes_during_spark = concurrent_writer.count_ops_in_mono_window(
            concurrent_spark_start_ns, concurrent_spark_end_ns
        )

        # Continue writing briefly so another materializer cycle commits after cleanup.
        time.sleep(concurrent_post_delay_s)
    finally:
        concurrent_writer_summary = concurrent_writer.stop_and_collect()

    # Commit post-concurrent final marker and verify exact convergence again.
    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        post_conc_commit_ns, post_conc_commit_utc = commit_marker_row(
            pg_conn,
            marker_id=POST_CONCURRENT_MARKER_ID,
            status="marker_post_concurrent",
            note="k1 post-concurrent-cleanup convergence marker",
        )
    convergence_after_concurrent = verify_snapshot_convergence(
        cfg,
        POST_CONCURRENT_MARKER_ID,
        post_conc_commit_ns,
        timeout_s=convergence_timeout_s,
        poll_interval_s=1.0,
        batch_size=batch_size,
    )
    lk_meta_final = convergence_after_concurrent.pop("final_lakekeeper_metadata", None)
    if lk_meta_final is None:
        lk_meta_final = fetch_lakekeeper_table_metadata(
            cfg, namespace=cfg.lakekeeper_namespace, table="orders"
        )
    assert lk_meta_final is not None
    write_json(run_dir / "lakekeeper_table_after_concurrent.json", lk_meta_final)

    silo_objects_after_concurrent = list_silo_objects(cfg)
    write_json(run_dir / "silo_objects_after_concurrent.json", silo_objects_after_concurrent)

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as pg_conn:
        log_index_final = fetch_pg2iceberg_log_index_paths(pg_conn, bucket=cfg.silo_bucket)
        pg_pos_final = capture_pg_pipeline_positions(pg_conn)

    spark_inspect_after_concurrent = _run_spark_script(
        cfg,
        logged_cmd,
        mode="inspect",
        out_container_path="/tmp/cdclab-spark/k1_inspect_after_concurrent.json",
        out_host_path=run_dir / "spark_inspect_after_concurrent.json",
        stdout_log_path=run_dir / "spark_inspect_after_concurrent.stdout.log",
        stderr_log_path=run_dir / "spark_inspect_after_concurrent.stderr.log",
    )
    referenced_final = collect_referenced_s3_uris(lk_meta_final, spark_inspect_after_concurrent)
    classification_after_concurrent = classify_table_objects(
        silo_objects_after_concurrent,
        table_location,
        referenced_final,
        staging_uris=log_index_final["staging_uris"],
        staging_keys=log_index_final["staging_keys"],
    )
    write_json(run_dir / "classification_after_concurrent.json", classification_after_concurrent)

    snapshots_after_concurrent = [
        int(s["snapshot_id"]) for s in spark_inspect_after_concurrent.get("snapshots") or []
    ]
    concurrent_eval = evaluate_concurrent_cleanup(
        before_classified=classification_after,
        after_classified=classification_after_concurrent,
        spark_deleted_uris=concurrent_cleanup["orphan_file_locations"],
        writes_during_cleanup=writes_during_spark["ok"],
        snapshots_before=snapshots_before_concurrent,
        snapshots_after=snapshots_after_concurrent,
    )
    write_json(run_dir / "concurrent_cleanup_eval.json", concurrent_eval)

    # Capture DuckDB final metadata and container logs before teardown.
    con_meta, _ = connect_iceberg(cfg)
    try:
        duckdb_meta_final = read_metadata_files(con_meta, namespace=cfg.lakekeeper_namespace)
        duckdb_snaps_final = read_snapshots(con_meta, namespace=cfg.lakekeeper_namespace)
        duckdb_final_summary = summarize_duckdb_metadata(duckdb_meta_final)
    finally:
        con_meta.close()
    write_json(run_dir / "duckdb_metadata_final.json", duckdb_meta_final)
    write_json(run_dir / "duckdb_snapshots_final.json", duckdb_snaps_final)

    pg2_logs = run_cmd(
        [*compose_cmd_with_profiles(cfg, ("connector",)), "logs", "--no-color", "pg2iceberg"],
        check=False,
    ).stdout
    (run_dir / "pg2iceberg.log").write_text(pg2_logs, encoding="utf-8")
    pg2_log_summary = summarize_pg2iceberg_logs(pg2_logs)

    lk_logs = run_cmd(
        [*compose_cmd_with_profiles(cfg), "logs", "--no-color", "lakekeeper"],
        check=False,
    ).stdout
    (run_dir / "lakekeeper.log").write_text(lk_logs, encoding="utf-8")

    # 8. Project-only cleanup.
    cleanup_result = project_teardown_and_verify(
        cfg,
        logged_cmd,
        non_project_before,
        profiles=ALL_LAB_PROFILES,
        keep_up=keep_up,
    )

    cleanup_ok = keep_up or (
        cleanup_result["all_project_resources_removed"]
        and cleanup_result["non_project_containers_unchanged"]
    )
    passed = (
        convergence_before["passed"]
        and cleanup_diff["exact_orphan_removal_passed"]
        and post_cleanup_comparison["passed"]
        and concurrent_eval["passed"]
        and convergence_after_concurrent["passed"]
        and concurrent_writer_summary["fatal_error"] is None
        and concurrent_writer_summary["error_ops"] == 0
        and cleanup_ok
    )
    outcome = "converged" if passed else "mismatch at deadline"

    persisted_outage_events = {
        k: v for k, v in outage_events.items() if not k.endswith("_mono_ns")
    }
    persisted_concurrent_writer = {
        k: v for k, v in concurrent_writer_summary.items() if k != "ops"
    }

    summary = {
        "milestone": "K1",
        "label": label,
        "outcome": outcome,
        "passed": passed,
        "completed_at_utc": utc_now_iso(),
        "parameters": {
            "seed_rows": seed_rows,
            "toast_rows": toast_rows,
            "duration_s": duration_s,
            "rate_ops_s": rate_ops_s,
            "rng_seed": rng_seed,
            "batch_size": batch_size,
            "probe_interval_s": probe_interval_s,
            "probe_poll_interval_s": probe_poll_interval_s,
            "outage_delay_s": outage_delay_s,
            "outage_duration_s": outage_duration_s,
            "sample_interval_s": sample_interval_s,
            "convergence_timeout_s": convergence_timeout_s,
            "concurrent_pre_delay_s": concurrent_pre_delay_s,
            "concurrent_post_delay_s": concurrent_post_delay_s,
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
            "spark": spark_inspect_before.get("versions"),
        },
        "resources_after_pre_clean": resources_after_pre_clean,
        "lakekeeper_maintenance_probe": {
            "warehouse_id": warehouse_id,
            "openapi_task_queue_paths": lk_maint_probe["openapi_task_queue_paths"],
            "has_orphan_removal_queue": lk_maint_probe["has_orphan_removal_queue"],
            "has_expire_snapshots_queue": lk_maint_probe["has_expire_snapshots_queue"],
            "has_optimize_queue": lk_maint_probe["has_optimize_queue"],
            "probe_statuses": {k: v["status"] for k, v in lk_maint_probe["api_probes"].items()},
        },
        "seed": seed_res.to_dict(),
        "initial_snapshot": initial_snapshot_info,
        "warmup_marker": {
            "marker_id": WARMUP_MARKER_ID,
            "commit_utc": warmup_commit_utc,
            "visibility_ms": warmup_vis["visibility_ms"],
            "snapshot_id": warmup_vis["snapshot_id"],
        },
        "workload": workload_summary,
        "toast_after_workload": toast_after_workload,
        "outage": {
            "events": persisted_outage_events,
            "sample_summary": outage_summary,
            "pg2iceberg_log_summary": pg2_log_summary,
        },
        "pre_cleanup_convergence": {
            "final_marker_commit_utc": final_commit_utc,
            **convergence_before,
        },
        "table_location": table_location,
        "cleanup": {
            "older_than_sql": older_than_sql,
            "procedure_short_interval_rejected": {
                "sql": proc_short_interval.get("sql"),
                "ok": proc_short_interval.get("ok"),
                "error": proc_short_interval.get("error"),
            },
            "root_dry_run_action": dry_run_root.get("action"),
            "root_dry_run_count": dry_run_root["orphan_file_count"],
            "root_dry_run_locations": dry_run_root["orphan_file_locations"],
            "root_dry_run_would_delete_staging_count": len(root_would_delete_staging),
            "root_dry_run_would_delete_staging": root_would_delete_staging,
            "data_dry_run_action": dry_run_data.get("action"),
            "data_dry_run_count": dry_run_data["orphan_file_count"],
            "data_dry_run_locations": dry_run_data["orphan_file_locations"],
            "selected_cleanup_location": cleanup_location or "<table_root>",
            "actual_action": cleanup_actual.get("action"),
            "actual_duration_ms": cleanup_actual["duration_ms"],
            "actual_deleted_count": cleanup_actual["orphan_file_count"],
            "actual_deleted_locations": cleanup_actual["orphan_file_locations"],
            "before_counts": classification_before["counts_by_classification"],
            "before_bytes": classification_before["bytes_by_classification"],
            "before_by_sub_role": classification_before["by_sub_role"],
            "before_outside_table_count": classification_before["outside_table_count"],
            "before_outside_table_staging_count": classification_before["outside_table_staging_count"],
            "after_counts": classification_after["counts_by_classification"],
            "after_bytes": classification_after["bytes_by_classification"],
            "after_by_sub_role": classification_after["by_sub_role"],
            "diff": cleanup_diff,
            "post_cleanup_comparison": post_cleanup_comparison,
        },
        "concurrent_cleanup": {
            "sql": concurrent_cleanup["sql"],
            "spark_started_utc": concurrent_spark_start_utc,
            "spark_completed_utc": concurrent_spark_end_utc,
            "spark_duration_ms": concurrent_cleanup["duration_ms"],
            "writes_during_spark": writes_during_spark,
            "writer_summary": persisted_concurrent_writer,
            "evaluation": concurrent_eval,
            "after_counts": classification_after_concurrent["counts_by_classification"],
            "after_bytes": classification_after_concurrent["bytes_by_classification"],
            "post_concurrent_marker_commit_utc": post_conc_commit_utc,
            "post_concurrent_convergence": convergence_after_concurrent,
        },
        "duckdb_final_snapshot_count": len(duckdb_snaps_final),
        "duckdb_final_metadata_summary": duckdb_final_summary,
        "pipeline_positions": {
            "after_seed": pg_pos_after_seed,
            "before_cleanup": pg_pos_before_cleanup,
            "final": pg_pos_final,
        },
        "teardown": cleanup_result,
    }
    write_json(run_dir / "summary.json", summary)

    compact = {
        "run_dir": str(run_dir.relative_to(REPO_ROOT).as_posix()),
        "outcome": outcome,
        "passed": passed,
        "lakekeeper_has_orphan_removal": lk_maint_probe["has_orphan_removal_queue"],
        "before_counts": classification_before["counts_by_classification"],
        "before_outside_staging": classification_before["outside_table_staging_count"],
        "root_dry_run_count": dry_run_root["orphan_file_count"],
        "root_dry_run_would_delete_staging": len(root_would_delete_staging),
        "selected_cleanup_location": cleanup_location or "<table_root>",
        "actual_deleted_count": cleanup_actual["orphan_file_count"],
        "after_counts": classification_after["counts_by_classification"],
        "exact_orphan_removal_passed": cleanup_diff["exact_orphan_removal_passed"],
        "post_cleanup_comparison_passed": post_cleanup_comparison["passed"],
        "concurrent_writes_during_spark": writes_during_spark["ok"],
        "concurrent_spark_deleted_count": concurrent_cleanup["orphan_file_count"],
        "concurrent_removed_live_files": concurrent_eval["removed_live_file_count"],
        "concurrent_new_snapshots": concurrent_eval["new_snapshots_count"],
        "post_concurrent_convergence_passed": convergence_after_concurrent["passed"],
    }
    print(json.dumps(compact, indent=2, default=json_default))
    if not passed:
        raise SystemExit(1)
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run K1 orphan-file classification and cleanup after a Lakekeeper catalog outage."
    )
    parser.add_argument("--seed-rows", type=int, default=25000)
    parser.add_argument("--toast-rows", type=int, default=100)
    parser.add_argument("--duration-s", type=float, default=180.0)
    parser.add_argument("--rate-ops-s", type=float, default=10.0)
    parser.add_argument("--rng-seed", type=int, default=20261119)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--probe-interval-s", type=float, default=20.0)
    parser.add_argument("--probe-poll-interval-s", type=float, default=0.5)
    parser.add_argument("--outage-delay-s", type=float, default=65.0)
    parser.add_argument("--outage-duration-s", type=float, default=60.0)
    parser.add_argument("--sample-interval-s", type=float, default=2.0)
    parser.add_argument("--convergence-timeout-s", type=float, default=240.0)
    parser.add_argument("--concurrent-pre-delay-s", type=float, default=12.0)
    parser.add_argument("--concurrent-post-delay-s", type=float, default=12.0)
    parser.add_argument("--label", default="k1-orphan-cleanup")
    parser.add_argument("--keep-up", action="store_true")
    args = parser.parse_args()
    run_k1(
        seed_rows=args.seed_rows,
        toast_rows=args.toast_rows,
        duration_s=args.duration_s,
        rate_ops_s=args.rate_ops_s,
        rng_seed=args.rng_seed,
        batch_size=args.batch_size,
        probe_interval_s=args.probe_interval_s,
        probe_poll_interval_s=args.probe_poll_interval_s,
        outage_delay_s=args.outage_delay_s,
        outage_duration_s=args.outage_duration_s,
        sample_interval_s=args.sample_interval_s,
        convergence_timeout_s=args.convergence_timeout_s,
        concurrent_pre_delay_s=args.concurrent_pre_delay_s,
        concurrent_post_delay_s=args.concurrent_post_delay_s,
        label=args.label,
        keep_up=args.keep_up,
    )


if __name__ == "__main__":
    main()
