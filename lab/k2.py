"""Milestone K2 end-to-end runner: Lakekeeper catalog metadata restore drill."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import platform
import random
import sys
import threading
import time
from typing import Any
import urllib.parse

import psycopg

from lab.bootstrap import _http_json, bootstrap_all
from lab.compare import (
    ComparisonResult,
    compare_ordered_streams,
    stream_duckdb_orders_at_snapshot,
    stream_postgres_orders,
    verify_iceberg_schema,
)
from lab.config import LabConfig, REPO_ROOT, load_config
from lab.k1 import (
    fetch_pg2iceberg_log_index_paths,
    list_silo_objects,
    normalize_s3_uri,
)
from lab.l3 import (
    extract_coordinator_offsets,
    inspect_service_container,
    probe_http_status,
    summarize_pg2iceberg_logs,
    wait_for_lakekeeper_healthy,
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

POST_RECOVERY_INSERT_ID = 3_000_000_001
POST_RECOVERY_MARKER_ID = 901_000_000
BACKUP_PATH_IN_CONTAINER = "/tmp/lakekeeper_backup_T.dump"


def _parse_int_or_none(val: Any) -> int | None:
    if val is None or isinstance(val, bool):
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def extract_snapshot_lineage(lakekeeper_meta: Mapping[str, Any] | None) -> dict[str, Any]:
    """Extract ordered snapshot lineage and metadata log from Lakekeeper LoadTable JSON."""
    if not lakekeeper_meta:
        return {
            "found": False,
            "table_uuid": None,
            "location": None,
            "metadata_location": None,
            "current_snapshot_id": None,
            "last_sequence_number": None,
            "snapshot_count": 0,
            "snapshots": [],
            "metadata_log": [],
        }
    meta = lakekeeper_meta.get("metadata") or {}
    raw_snaps = list(meta.get("snapshots") or [])
    raw_snaps.sort(key=lambda s: (int(s.get("sequence-number") or 0), int(s.get("timestamp-ms") or 0)))

    snapshots: list[dict[str, Any]] = []
    for snap in raw_snaps:
        summary = snap.get("summary") or {}
        snapshots.append(
            {
                "snapshot_id": int(snap["snapshot-id"]),
                "parent_snapshot_id": _parse_int_or_none(snap.get("parent-snapshot-id")),
                "sequence_number": _parse_int_or_none(snap.get("sequence-number")),
                "timestamp_ms": _parse_int_or_none(snap.get("timestamp-ms")),
                "schema_id": _parse_int_or_none(snap.get("schema-id")),
                "operation": summary.get("operation"),
                "pg2iceberg_log_start": _parse_int_or_none(summary.get("pg2iceberg.log-start")),
                "pg2iceberg_log_end": _parse_int_or_none(summary.get("pg2iceberg.log-end")),
                "pg2iceberg_log_group": summary.get("pg2iceberg.log-group"),
                "added_records": _parse_int_or_none(summary.get("added-records")),
                "added_data_files": _parse_int_or_none(summary.get("added-data-files")),
                "added_equality_deletes": _parse_int_or_none(summary.get("added-equality-deletes")),
                "added_delete_files": _parse_int_or_none(summary.get("added-delete-files")),
                "total_records": _parse_int_or_none(summary.get("total-records")),
                "total_data_files": _parse_int_or_none(summary.get("total-data-files")),
                "total_delete_files": _parse_int_or_none(summary.get("total-delete-files")),
                "total_equality_deletes": _parse_int_or_none(summary.get("total-equality-deletes")),
                "manifest_list": (
                    normalize_s3_uri(str(snap["manifest-list"]))
                    if snap.get("manifest-list")
                    else None
                ),
            }
        )

    metadata_log = [
        {
            "timestamp_ms": _parse_int_or_none(e.get("timestamp-ms")),
            "metadata_file": (
                normalize_s3_uri(str(e["metadata-file"])) if e.get("metadata-file") else None
            ),
        }
        for e in (meta.get("metadata-log") or [])
        if isinstance(e, Mapping)
    ]

    cur_id = _parse_int_or_none(meta.get("current-snapshot-id"))
    cur_snap = next((s for s in snapshots if s["snapshot_id"] == cur_id), None)

    return {
        "found": True,
        "table_uuid": meta.get("table-uuid"),
        "location": normalize_s3_uri(str(meta["location"])) if meta.get("location") else None,
        "metadata_location": (
            normalize_s3_uri(str(lakekeeper_meta["metadata_location"]))
            if lakekeeper_meta.get("metadata_location")
            else None
        ),
        "current_snapshot_id": cur_id,
        "current_snapshot": cur_snap,
        "last_sequence_number": _parse_int_or_none(meta.get("last-sequence-number")),
        "snapshot_count": len(snapshots),
        "snapshot_ids": [s["snapshot_id"] for s in snapshots],
        "snapshots": snapshots,
        "metadata_log": metadata_log,
    }


def summarize_silo_objects_by_role(
    silo_objects: Sequence[Mapping[str, Any]],
    table_location: str,
) -> dict[str, Any]:
    """Summarize Silo objects under table_location by Iceberg/staging role."""
    norm_loc = normalize_s3_uri(table_location)
    loc_prefix = norm_loc + "/"

    counts: dict[str, int] = {}
    bytes_by_role: dict[str, int] = {}
    metadata_json_uris: list[str] = []
    manifest_list_uris: list[str] = []
    under_table_uris: list[str] = []
    outside_table_count = 0
    total_under_bytes = 0

    for obj in silo_objects:
        uri = normalize_s3_uri(str(obj["s3_uri"]))
        size = int(obj["size"])
        if not uri.startswith(loc_prefix):
            outside_table_count += 1
            continue
        under_table_uris.append(uri)
        total_under_bytes += size
        rel = uri[len(loc_prefix) :]
        if rel.startswith("metadata/") and rel.endswith(".metadata.json"):
            role = "metadata_json"
            metadata_json_uris.append(uri)
        elif rel.startswith("metadata/snap-") and rel.endswith(".avro"):
            role = "manifest_list"
            manifest_list_uris.append(uri)
        elif rel.startswith("metadata/") and rel.endswith(".avro"):
            role = "manifest"
        elif rel.startswith("data/eq-delete-"):
            role = "delete_file"
        elif rel.startswith("data/"):
            role = "data_file"
        elif rel.startswith("staged/"):
            role = "pg2iceberg_staging"
        else:
            role = "other"
        counts[role] = counts.get(role, 0) + 1
        bytes_by_role[role] = bytes_by_role.get(role, 0) + size

    return {
        "table_location": norm_loc,
        "total_under_table_objects": len(under_table_uris),
        "total_under_table_bytes": total_under_bytes,
        "outside_table_count": outside_table_count,
        "counts_by_role": counts,
        "bytes_by_role": bytes_by_role,
        "metadata_json_uris": sorted(metadata_json_uris),
        "manifest_list_uris": sorted(manifest_list_uris),
        "under_table_uris": sorted(under_table_uris),
    }


def diff_silo_object_sets(
    before_objects: Sequence[Mapping[str, Any]],
    after_objects: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Return exact added, removed, and preserved S3 URIs between two Silo listings."""
    before_map = {normalize_s3_uri(str(o["s3_uri"])): int(o["size"]) for o in before_objects}
    after_map = {normalize_s3_uri(str(o["s3_uri"])): int(o["size"]) for o in after_objects}
    added = sorted(set(after_map) - set(before_map))
    removed = sorted(set(before_map) - set(after_map))
    preserved = sorted(set(before_map) & set(after_map))
    return {
        "before_count": len(before_map),
        "after_count": len(after_map),
        "added_count": len(added),
        "added_bytes": sum(after_map[u] for u in added),
        "added_uris": added,
        "removed_count": len(removed),
        "removed_bytes": sum(before_map[u] for u in removed),
        "removed_uris": removed,
        "preserved_count": len(preserved),
    }


def analyze_restore_divergence(
    *,
    lineage_at_dump: Mapping[str, Any],
    lineage_before_restore: Mapping[str, Any],
    lineage_after_restore: Mapping[str, Any],
    lineage_post_drill: Mapping[str, Any],
    silo_before_restore: Sequence[Mapping[str, Any]],
    silo_after_restore: Sequence[Mapping[str, Any]],
    silo_post_drill: Sequence[Mapping[str, Any]],
    pg_offsets_at_dump: Mapping[str, Any],
    pg_offsets_before_restore: Mapping[str, Any],
    pg_offsets_after_restore: Mapping[str, Any],
    pg_offsets_post_drill: Mapping[str, Any],
    pg2_log_summary: Mapping[str, Any],
    pg2_container_after_restore: Mapping[str, Any],
    pg2_container_post_drill: Mapping[str, Any],
) -> dict[str, Any]:
    """Analyze snapshot rollback, timeline fork, skipped log offsets, and connector behavior."""
    dump_ids = set(lineage_at_dump.get("snapshot_ids") or [])
    before_snaps = list(lineage_before_restore.get("snapshots") or [])
    after_ids = set(lineage_after_restore.get("snapshot_ids") or [])
    post_snaps = list(lineage_post_drill.get("snapshots") or [])

    rolled_back_snapshots = [s for s in before_snaps if s["snapshot_id"] not in after_ids]
    committed_between_dump_and_restore = [
        s for s in before_snaps if s["snapshot_id"] not in dump_ids
    ]
    post_restore_new_snapshots = [s for s in post_snaps if s["snapshot_id"] not in after_ids]

    dump_cur_id = lineage_at_dump.get("current_snapshot_id")
    after_cur_id = lineage_after_restore.get("current_snapshot_id")
    before_cur_id = lineage_before_restore.get("current_snapshot_id")
    post_cur_id = lineage_post_drill.get("current_snapshot_id")

    restored_to_dump_snapshot = (
        dump_cur_id is not None and after_cur_id is not None and after_cur_id == dump_cur_id
    )
    restore_rolled_back_catalog = (
        before_cur_id is not None
        and after_cur_id is not None
        and before_cur_id != after_cur_id
        and len(rolled_back_snapshots) > 0
    )

    # Detect Iceberg metadata timeline fork: new post-restore snapshots branching off
    # the restored (older) snapshot while rolled-back snapshots had existed on another branch.
    first_post_snap = post_restore_new_snapshots[0] if post_restore_new_snapshots else None
    fork_detected = bool(
        rolled_back_snapshots
        and first_post_snap is not None
        and first_post_snap.get("parent_snapshot_id") == after_cur_id
    )

    after_cur_snap = lineage_after_restore.get("current_snapshot") or {}
    before_cur_snap = lineage_before_restore.get("current_snapshot") or {}
    restored_log_end = _parse_int_or_none(after_cur_snap.get("pg2iceberg_log_end"))
    before_restore_log_end = _parse_int_or_none(before_cur_snap.get("pg2iceberg_log_end"))
    first_post_log_start = (
        _parse_int_or_none(first_post_snap.get("pg2iceberg_log_start"))
        if first_post_snap
        else None
    )
    first_post_log_end = (
        _parse_int_or_none(first_post_snap.get("pg2iceberg_log_end"))
        if first_post_snap
        else None
    )

    skipped_log_offset_range: dict[str, Any] | None = None
    if (
        restored_log_end is not None
        and first_post_log_start is not None
        and first_post_log_start > restored_log_end
    ):
        skipped_log_offset_range = {
            "from_offset_exclusive": restored_log_end,
            "to_offset_inclusive": first_post_log_start,
            "skipped_offset_count": first_post_log_start - restored_log_end,
        }

    # Check if coordinator mat_cursor was ahead of the restored Iceberg snapshot's log-end.
    mat_cursor_after_restore = _parse_int_or_none(pg_offsets_after_restore.get("applied_mat_cursor"))
    catalog_behind_coordinator_offsets: int | None = None
    if mat_cursor_after_restore is not None and restored_log_end is not None:
        catalog_behind_coordinator_offsets = mat_cursor_after_restore - restored_log_end

    # Check Silo retention across restore: metadata/data files from rolled-back snapshots stay in Silo.
    silo_diff_across_restore = diff_silo_object_sets(silo_before_restore, silo_after_restore)
    silo_post_uris = {normalize_s3_uri(str(o["s3_uri"])) for o in silo_post_drill}
    rolled_back_manifest_lists = [
        s["manifest_list"] for s in rolled_back_snapshots if s.get("manifest_list")
    ]
    rolled_back_manifest_lists_still_in_silo = [
        u for u in rolled_back_manifest_lists if u in silo_post_uris
    ]

    before_meta_loc = lineage_before_restore.get("metadata_location")
    before_meta_loc_still_in_silo = (
        before_meta_loc in silo_post_uris if before_meta_loc else False
    )

    # Classify pg2iceberg behavior after the catalog restore.
    pg2_running_post = bool(pg2_container_post_drill.get("running"))
    err_count = int(pg2_log_summary.get("error_count") or 0) + int(
        pg2_log_summary.get("fatal_text_error_count") or 0
    )
    if not pg2_running_post and len(post_restore_new_snapshots) == 0:
        behavior = "exited_without_post_restore_commits"
    elif fork_detected and skipped_log_offset_range is not None and err_count == 0:
        behavior = "silent_continuation_with_gap"
    elif fork_detected and skipped_log_offset_range is not None and err_count > 0:
        behavior = "continued_with_gap_after_errors"
    elif len(post_restore_new_snapshots) > 0 and skipped_log_offset_range is None:
        behavior = "re_committed_without_log_gap"
    elif err_count > 0:
        behavior = "errored_or_conflicted_without_new_commits"
    else:
        behavior = "stalled_without_errors"

    return {
        "restored_to_dump_snapshot": restored_to_dump_snapshot,
        "restore_rolled_back_catalog": restore_rolled_back_catalog,
        "snapshot_id_at_dump": dump_cur_id,
        "snapshot_id_before_restore": before_cur_id,
        "snapshot_id_after_restore": after_cur_id,
        "snapshot_id_post_drill": post_cur_id,
        "snapshots_committed_between_dump_and_restore_count": len(
            committed_between_dump_and_restore
        ),
        "rolled_back_snapshot_count": len(rolled_back_snapshots),
        "rolled_back_snapshots": rolled_back_snapshots,
        "post_restore_new_snapshot_count": len(post_restore_new_snapshots),
        "post_restore_new_snapshots": post_restore_new_snapshots,
        "fork_detected": fork_detected,
        "restored_snapshot_log_end": restored_log_end,
        "before_restore_snapshot_log_end": before_restore_log_end,
        "first_post_restore_snapshot_log_start": first_post_log_start,
        "first_post_restore_snapshot_log_end": first_post_log_end,
        "skipped_log_offset_range": skipped_log_offset_range,
        "mat_cursor_at_dump": _parse_int_or_none(pg_offsets_at_dump.get("applied_mat_cursor")),
        "mat_cursor_before_restore": _parse_int_or_none(
            pg_offsets_before_restore.get("applied_mat_cursor")
        ),
        "mat_cursor_after_restore": mat_cursor_after_restore,
        "mat_cursor_post_drill": _parse_int_or_none(pg_offsets_post_drill.get("applied_mat_cursor")),
        "catalog_behind_coordinator_offsets": catalog_behind_coordinator_offsets,
        "silo_objects_removed_by_restore": silo_diff_across_restore["removed_count"],
        "before_restore_metadata_location": before_meta_loc,
        "before_restore_metadata_location_still_in_silo": before_meta_loc_still_in_silo,
        "rolled_back_manifest_lists_still_in_silo_count": len(
            rolled_back_manifest_lists_still_in_silo
        ),
        "pg2iceberg_running_after_restore": bool(pg2_container_after_restore.get("running")),
        "pg2iceberg_running_post_drill": pg2_running_post,
        "pg2iceberg_error_or_fatal_count": err_count,
        "connector_behavior": behavior,
    }


def _parse_utc_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def find_log_index_cutoff_utc(
    log_index_rows: Sequence[Mapping[str, Any]],
    target_end_offset: int | None,
) -> str | None:
    """Find the exact created_at UTC timestamp of the _pg2iceberg.log_index row at target_end_offset."""
    if target_end_offset is None:
        return None
    for row in log_index_rows:
        end_off = _parse_int_or_none(row.get("end_offset"))
        if end_off == target_end_offset and row.get("created_at"):
            return _parse_utc_ts(str(row["created_at"])).isoformat(timespec="microseconds")
    return None


def fetch_full_log_index_rows(conn: psycopg.Connection[Any]) -> list[dict[str, Any]]:
    """Read all rows from _pg2iceberg.log_index ordered by start_offset, end_offset."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '_pg2iceberg' AND table_name = 'log_index'
            ORDER BY ordinal_position
            """
        )
        cols = [r[0] for r in cur.fetchall()]
        if not cols:
            return []
        cur.execute('SELECT * FROM _pg2iceberg."log_index" ORDER BY start_offset, end_offset')
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def attribute_divergence_to_workload(
    *,
    full_diff: Mapping[str, Any],
    ops: Sequence[Mapping[str, Any]],
    probes: Sequence[Mapping[str, Any]],
    window_start_utc: str | None = None,
    dump_snapshot_timestamp_ms: int | None,
    before_restore_snapshot_timestamp_ms: int | None,
    after_restore_utc: str,
    rollback_start_utc_override: str | None = None,
    rollback_end_utc_override: str | None = None,
) -> dict[str, Any]:
    """Cross-reference every discrepant row against ops.jsonl and steady_state_probes.jsonl."""
    win_start_dt = _parse_utc_ts(window_start_utc) if window_start_utc else None
    dump_snap_dt = (
        datetime.fromtimestamp(dump_snapshot_timestamp_ms / 1000.0, tz=timezone.utc)
        if dump_snapshot_timestamp_ms is not None
        else None
    )
    before_snap_dt = (
        datetime.fromtimestamp(before_restore_snapshot_timestamp_ms / 1000.0, tz=timezone.utc)
        if before_restore_snapshot_timestamp_ms is not None
        else None
    )
    # Prefer exact _pg2iceberg.log_index staging cutoff timestamps when available,
    # because snapshot commit timestamps lag the last staged PG commit by up to one flush/materializer cycle.
    rb_start_dt = (
        _parse_utc_ts(rollback_start_utc_override) if rollback_start_utc_override else dump_snap_dt
    )
    rb_end_dt = (
        _parse_utc_ts(rollback_end_utc_override) if rollback_end_utc_override else before_snap_dt
    )
    after_restore_dt = _parse_utc_ts(after_restore_utc)

    def _window_for(dt: datetime | None) -> str:
        if dt is None or rb_start_dt is None or rb_end_dt is None:
            return "unknown"
        if dt <= rb_start_dt:
            return "before_dump"
        if dt <= rb_end_dt:
            return "rolled_back_window"
        if dt <= after_restore_dt:
            return "during_restore"
        return "after_restore"

    # Index all workload ops and probes by target_id.
    events_by_id: dict[int, list[dict[str, Any]]] = {}
    for op in ops:
        if op.get("outcome") != "ok":
            continue
        tid = int(op["target_id"])
        op_kind = str(op.get("op") or op.get("op_kind") or "unknown")
        comp_utc = str(op.get("complete_utc") or "")
        comp_dt: datetime | None = None
        if comp_utc:
            comp_dt = _parse_utc_ts(comp_utc)
        elif win_start_dt is not None and op.get("complete_offset_ms") is not None:
            comp_dt = win_start_dt + timedelta(milliseconds=float(op["complete_offset_ms"]))
            comp_utc = comp_dt.isoformat(timespec="microseconds")
        events_by_id.setdefault(tid, []).append(
            {
                "source": "workload",
                "seq": op.get("seq"),
                "op_kind": op_kind,
                "complete_utc": comp_utc,
                "window": _window_for(comp_dt),
            }
        )

    for probe in probes:
        mid = _parse_int_or_none(probe.get("marker_id"))
        c_utc = str(probe.get("commit_utc") or "")
        if mid is None or not c_utc:
            continue
        c_dt = _parse_utc_ts(c_utc)
        events_by_id.setdefault(mid, []).append(
            {
                "source": "steady_state_probe",
                "probe_seq": probe.get("probe_seq"),
                "op_kind": "insert_probe",
                "complete_utc": c_utc,
                "window": _window_for(c_dt),
            }
        )

    # Sort each ID's event history chronologically.
    for evs in events_by_id.values():
        evs.sort(key=lambda e: str(e.get("complete_utc") or ""))

    source_only_ids = [int(e["id"]) for e in full_diff.get("source_only_examples") or []]
    lake_only_ids = [int(e["id"]) for e in full_diff.get("lake_only_examples") or []]
    value_mismatch_ids = [int(e["id"]) for e in full_diff.get("value_mismatch_examples") or []]
    duplicate_lake_ids = [int(e["id"]) for e in full_diff.get("duplicate_lake_examples") or []]

    def _classify_ids(ids: Sequence[int]) -> dict[str, Any]:
        by_window: dict[str, int] = {}
        by_last_op_kind: dict[str, int] = {}
        details: list[dict[str, Any]] = []
        for row_id in ids:
            evs = events_by_id.get(row_id) or []
            windows = sorted({e["window"] for e in evs})
            last_ev = evs[-1] if evs else None
            w_label = (
                "rolled_back_window"
                if "rolled_back_window" in windows
                else (last_ev["window"] if last_ev else "seeded_or_marker")
            )
            op_label = str(last_ev["op_kind"]) if last_ev else "seeded_or_marker"
            by_window[w_label] = by_window.get(w_label, 0) + 1
            by_last_op_kind[op_label] = by_last_op_kind.get(op_label, 0) + 1
            if len(details) < 50:
                details.append(
                    {
                        "id": row_id,
                        "primary_window": w_label,
                        "last_op_kind": op_label,
                        "events": evs,
                    }
                )
        return {
            "count": len(ids),
            "all_ids": list(ids),
            "by_primary_window": by_window,
            "by_last_op_kind": by_last_op_kind,
            "sample_details": details,
        }

    # Summarize all ops in the rolled-back window and count how many were masked by a post-restore op.
    rolled_back_ops_by_kind: dict[str, int] = {}
    discrepant_set = set(source_only_ids) | set(lake_only_ids) | set(value_mismatch_ids) | set(duplicate_lake_ids)
    rolled_back_ids_touched: set[int] = set()
    rolled_back_ids_masked_by_later_op: set[int] = set()
    for row_id, evs in events_by_id.items():
        rb_evs = [e for e in evs if e["window"] == "rolled_back_window"]
        if not rb_evs:
            continue
        rolled_back_ids_touched.add(row_id)
        for ev in rb_evs:
            k = str(ev["op_kind"])
            rolled_back_ops_by_kind[k] = rolled_back_ops_by_kind.get(k, 0) + 1
        later_evs = [e for e in evs if e["window"] in ("during_restore", "after_restore")]
        if later_evs and row_id not in discrepant_set:
            rolled_back_ids_masked_by_later_op.add(row_id)

    return {
        "dump_snapshot_timestamp_utc": (
            dump_snap_dt.isoformat(timespec="microseconds") if dump_snap_dt else None
        ),
        "before_restore_snapshot_timestamp_utc": (
            before_snap_dt.isoformat(timespec="microseconds") if before_snap_dt else None
        ),
        "rollback_staging_start_utc": (
            rb_start_dt.isoformat(timespec="microseconds") if rb_start_dt else None
        ),
        "rollback_staging_end_utc": (
            rb_end_dt.isoformat(timespec="microseconds") if rb_end_dt else None
        ),
        "after_restore_utc": after_restore_utc,
        "rolled_back_window_ops_by_kind": rolled_back_ops_by_kind,
        "rolled_back_distinct_ids_touched": len(rolled_back_ids_touched),
        "rolled_back_ids_masked_by_later_op_count": len(rolled_back_ids_masked_by_later_op),
        "total_discrepant_ids": len(discrepant_set),
        "source_only_missing_from_iceberg": _classify_ids(source_only_ids),
        "lake_only_ghost_rows_in_iceberg": _classify_ids(lake_only_ids),
        "value_mismatch_stale_rows_in_iceberg": _classify_ids(value_mismatch_ids),
        "duplicate_lake_ids": _classify_ids(duplicate_lake_ids),
    }


def inspect_lakekeeper_db_summary(cfg: LabConfig) -> dict[str, Any]:
    """Read high-level table/snapshot row counts from lakekeeper-db for restore proof."""
    sql = (
        "SELECT json_build_object("
        "'database', current_database(), "
        "'tabular_count', (SELECT count(*) FROM public.tabular), "
        "'table_snapshot_count', (SELECT count(*) FROM public.table_snapshot), "
        "'table_metadata_log_count', (SELECT count(*) FROM public.table_metadata_log), "
        "'current_metadata_locations', ("
        "  SELECT COALESCE(json_agg(json_build_object("
        "    'tabular_id', tabular_id, "
        "    'name', name, "
        "    'metadata_location', metadata_location, "
        "    'updated_at', updated_at"
        "  ) ORDER BY name), '[]'::json) "
        "  FROM public.tabular WHERE deleted_at IS NULL"
        ")"
        ")::text;"
    )
    proc = run_cmd(
        [
            *compose_cmd_with_profiles(cfg),
            "exec",
            "-T",
            "lakekeeper-db",
            "psql",
            "-U",
            "postgres",
            "-d",
            "lakekeeper",
            "-t",
            "-A",
            "-c",
            sql,
        ],
        check=False,
    )
    if proc.returncode != 0:
        return {"ok": False, "error": proc.stderr.strip() or proc.stdout.strip()}
    raw = proc.stdout.strip()
    try:
        data = json.loads(raw)
        return {"ok": True, "captured_at_utc": utc_now_iso(), **data}
    except json.JSONDecodeError as exc:
        return {"ok": False, "error": f"JSON parse error: {exc}", "raw": raw[:500]}


def drop_lakekeeper_table(
    cfg: LabConfig,
    *,
    namespace: str,
    table: str,
    purge_requested: bool = False,
) -> dict[str, Any]:
    """Drop an Iceberg table from Lakekeeper via the standard Iceberg REST DELETE endpoint."""
    wh_q = urllib.parse.urlencode({"warehouse": cfg.lakekeeper_warehouse})
    cfg_url = f"{cfg.lakekeeper_catalog_url}/v1/config?{wh_q}"
    status_cfg, catalog_cfg = _http_json(cfg_url)
    if status_cfg != 200 or not isinstance(catalog_cfg, dict):
        raise RuntimeError(f"Failed to read /v1/config: status={status_cfg} body={catalog_cfg!r}")
    prefix = (catalog_cfg.get("overrides") or {}).get("prefix") or (
        catalog_cfg.get("defaults") or {}
    ).get("prefix")
    if not prefix:
        raise RuntimeError(f"No warehouse prefix in /v1/config: {catalog_cfg!r}")

    purge_flag = "true" if purge_requested else "false"
    drop_url = (
        f"{cfg.lakekeeper_catalog_url}/v1/{urllib.parse.quote(str(prefix), safe='')}"
        f"/namespaces/{urllib.parse.quote(namespace, safe='')}"
        f"/tables/{urllib.parse.quote(table, safe='')}"
        f"?purgeRequested={purge_flag}"
    )
    status, body = _http_json(drop_url, method="DELETE")
    return {
        "method": "DELETE",
        "url": drop_url,
        "purge_requested": purge_requested,
        "status": status,
        "body": body,
        "ok": status in (200, 204),
    }


def run_exact_comparison_full(
    cfg: LabConfig,
    snapshot_id: int,
    *,
    batch_size: int = 2000,
    max_examples: int = 10000,
) -> dict[str, Any]:
    """Run exact schema + bounded-batch PG-vs-DuckDB comparison capturing all discrepant IDs."""
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
            row_res: ComparisonResult = compare_ordered_streams(
                stream_postgres_orders(pg_conn, batch_size=batch_size),
                stream_duckdb_orders_at_snapshot(
                    con,
                    snapshot_id,
                    namespace=cfg.lakekeeper_namespace,
                    batch_size=batch_size,
                ),
                max_examples=max_examples,
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


class CatalogRestoreDrillController:
    """Takes a pg_dump at T, waits for >=3 more snapshots, then restores lakekeeper-db to T."""

    def __init__(
        self,
        cfg: LabConfig,
        *,
        run_dir: Path,
        dump_delay_s: float = 35.0,
        min_snapshots_after_dump: int = 3,
        sample_interval_s: float = 2.0,
        samples_jsonl: Path,
    ) -> None:
        self._cfg = cfg
        self._run_dir = run_dir
        self._dump_delay_s = dump_delay_s
        self._min_snapshots_after_dump = min_snapshots_after_dump
        self._sample_interval_s = sample_interval_s
        self._samples_jsonl = samples_jsonl

        self._window_start_ns: int | None = None
        self._phase = "pre_dump"
        self._stop_sampling = threading.Event()
        self._drill_done = threading.Event()
        self._lock = threading.Lock()
        self._samples: list[dict[str, Any]] = []
        self._events: dict[str, Any] = {
            "dump_delay_s": dump_delay_s,
            "min_snapshots_after_dump": min_snapshots_after_dump,
            "commands": [],
            "error": None,
        }
        self._drill_thread: threading.Thread | None = None
        self._sampler_thread: threading.Thread | None = None

    def start_for_window(self, window_start_ns: int, window_start_utc: str) -> None:
        self._window_start_ns = window_start_ns
        self._events["window_start_utc"] = window_start_utc
        self._sampler_thread = threading.Thread(
            target=self._sample_loop, name="k2-drill-sampler", daemon=True
        )
        self._drill_thread = threading.Thread(
            target=self._drill_lifecycle, name="k2-restore-drill", daemon=True
        )
        self._sampler_thread.start()
        self._drill_thread.start()

    def _logged_cmd(self, cmd: list[str], *, check: bool = True) -> Any:
        with self._lock:
            self._events["commands"].append(" ".join(cmd))
        return run_cmd(cmd, check=check)

    def _record_sample(self) -> None:
        assert self._window_start_ns is not None
        now_ns = time.monotonic_ns()
        offset_ms = round((now_ns - self._window_start_ns) / 1_000_000.0, 2)
        with self._lock:
            phase = self._phase
        try:
            with psycopg.connect(self._cfg.pg_dsn, autocommit=True) as conn:
                pg_pos = capture_pg_pipeline_positions(conn)
            offsets = extract_coordinator_offsets(pg_pos)
        except Exception as exc:
            offsets = {"pg_error": f"{type(exc).__name__}: {exc}"}

        lk_cont = inspect_service_container(self._cfg, "lakekeeper", profiles=())
        pg2_cont = inspect_service_container(self._cfg, "pg2iceberg", profiles=("connector",))
        pg2_healthz = probe_http_status(f"{self._cfg.pg2iceberg_metrics_url}/healthz", timeout=1.5)
        pg2_readyz = probe_http_status(f"{self._cfg.pg2iceberg_metrics_url}/readyz", timeout=1.5)

        visible_snap_id: int | None = None
        visible_seq: int | None = None
        visible_log_start: int | None = None
        visible_log_end: int | None = None
        visible_snap_count: int | None = None
        if phase != "restoring":
            try:
                lk_meta = fetch_lakekeeper_table_metadata(
                    self._cfg,
                    namespace=self._cfg.lakekeeper_namespace,
                    table="orders",
                    allow_not_found=True,
                )
                if lk_meta:
                    lin = extract_snapshot_lineage(lk_meta)
                    visible_snap_id = lin["current_snapshot_id"]
                    visible_snap_count = lin["snapshot_count"]
                    cur = lin.get("current_snapshot") or {}
                    visible_seq = cur.get("sequence_number")
                    visible_log_start = cur.get("pg2iceberg_log_start")
                    visible_log_end = cur.get("pg2iceberg_log_end")
            except Exception:
                pass

        entry = {
            "captured_at_utc": utc_now_iso(),
            "offset_ms": offset_ms,
            "phase": phase,
            **offsets,
            "visible_snapshot_id": visible_snap_id,
            "visible_sequence_number": visible_seq,
            "visible_snapshot_count": visible_snap_count,
            "visible_snapshot_log_start": visible_log_start,
            "visible_snapshot_log_end": visible_log_end,
            "lakekeeper_container": lk_cont,
            "pg2iceberg_container": pg2_cont,
            "pg2iceberg_healthz_status": pg2_healthz["status"],
            "pg2iceberg_readyz_status": pg2_readyz["status"],
        }
        with self._lock:
            self._samples.append(entry)
            self._samples_jsonl.parent.mkdir(parents=True, exist_ok=True)
            with self._samples_jsonl.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, default=json_default) + "\n")

    def _sample_loop(self) -> None:
        self._samples_jsonl.parent.mkdir(parents=True, exist_ok=True)
        self._samples_jsonl.write_text("", encoding="utf-8")
        while not self._stop_sampling.is_set():
            t0 = time.monotonic()
            self._record_sample()
            rem = self._sample_interval_s - (time.monotonic() - t0)
            if rem > 0:
                self._stop_sampling.wait(timeout=rem)

    def _drill_lifecycle(self) -> None:
        assert self._window_start_ns is not None
        try:
            # 1. Wait until dump_delay_s after measurement window start.
            dump_target_ns = self._window_start_ns + int(self._dump_delay_s * 1_000_000_000)
            while True:
                rem_s = (dump_target_ns - time.monotonic_ns()) / 1_000_000_000.0
                if rem_s <= 0:
                    break
                time.sleep(min(0.25, rem_s))

            # Wait briefly for a fresh snapshot commit so pg_dump captures a clean boundary
            # right after a commit (reducing any chance of a concurrent commit during pg_dump).
            initial_meta = fetch_lakekeeper_table_metadata(
                self._cfg, namespace=self._cfg.lakekeeper_namespace, table="orders"
            )
            assert initial_meta is not None
            initial_snap_id = extract_snapshot_lineage(initial_meta)["current_snapshot_id"]
            boundary_deadline = time.monotonic() + 15.0
            while time.monotonic() < boundary_deadline:
                probe_meta = fetch_lakekeeper_table_metadata(
                    self._cfg, namespace=self._cfg.lakekeeper_namespace, table="orders"
                )
                if probe_meta:
                    cur_id = extract_snapshot_lineage(probe_meta)["current_snapshot_id"]
                    if cur_id != initial_snap_id:
                        break
                time.sleep(0.3)

            # 2. Take pg_dump of Lakekeeper catalog database at recorded time T.
            dump_start_ns = time.monotonic_ns()
            dump_start_utc = utc_now_iso()
            dump_cmd = [
                *compose_cmd_with_profiles(self._cfg),
                "exec",
                "-T",
                "lakekeeper-db",
                "pg_dump",
                "-U",
                "postgres",
                "-d",
                "lakekeeper",
                "--format=custom",
                f"--file={BACKUP_PATH_IN_CONTAINER}",
            ]
            self._logged_cmd(dump_cmd, check=True)
            dump_end_ns = time.monotonic_ns()
            dump_end_utc = utc_now_iso()

            # Verify dump size, SHA-256, and pg_restore --list inside lakekeeper-db.
            stat_proc = run_cmd(
                [
                    *compose_cmd_with_profiles(self._cfg),
                    "exec",
                    "-T",
                    "lakekeeper-db",
                    "sh",
                    "-c",
                    f"stat -c %s {BACKUP_PATH_IN_CONTAINER} && sha256sum {BACKUP_PATH_IN_CONTAINER}",
                ],
                check=True,
            )
            stat_lines = [ln.strip() for ln in stat_proc.stdout.splitlines() if ln.strip()]
            dump_size_bytes = int(stat_lines[0])
            dump_sha256 = stat_lines[1].split()[0]

            toc_proc = run_cmd(
                [
                    *compose_cmd_with_profiles(self._cfg),
                    "exec",
                    "-T",
                    "lakekeeper-db",
                    "pg_restore",
                    "--list",
                    BACKUP_PATH_IN_CONTAINER,
                ],
                check=True,
            )
            (self._run_dir / "lakekeeper_backup_T_toc.txt").write_text(
                toc_proc.stdout, encoding="utf-8"
            )

            lk_meta_at_dump = fetch_lakekeeper_table_metadata(
                self._cfg, namespace=self._cfg.lakekeeper_namespace, table="orders"
            )
            assert lk_meta_at_dump is not None
            write_json(self._run_dir / "lakekeeper_table_at_dump.json", lk_meta_at_dump)
            lineage_at_dump = extract_snapshot_lineage(lk_meta_at_dump)

            with psycopg.connect(self._cfg.pg_dsn, autocommit=True) as conn:
                pg_pos_at_dump = capture_pg_pipeline_positions(conn)
            silo_at_dump = list_silo_objects(self._cfg)
            write_json(self._run_dir / "silo_objects_at_dump.json", silo_at_dump)
            lk_db_at_dump = inspect_lakekeeper_db_summary(self._cfg)

            self._events["dump"] = {
                "dump_start_utc": dump_start_utc,
                "dump_end_utc": dump_end_utc,
                "dump_offset_ms": round((dump_start_ns - self._window_start_ns) / 1_000_000.0, 2),
                "dump_duration_ms": round((dump_end_ns - dump_start_ns) / 1_000_000.0, 2),
                "dump_path_in_container": BACKUP_PATH_IN_CONTAINER,
                "dump_size_bytes": dump_size_bytes,
                "dump_sha256": dump_sha256,
                "lineage": lineage_at_dump,
                "pg_positions": pg_pos_at_dump,
                "pg_offsets": extract_coordinator_offsets(pg_pos_at_dump),
                "lakekeeper_db_summary": lk_db_at_dump,
                "silo_summary": summarize_silo_objects_by_role(
                    silo_at_dump, str(lineage_at_dump["location"] or "")
                ),
            }
            with self._lock:
                self._phase = "post_dump_waiting_snapshots"

            # 3. Wait for pg2iceberg to commit at least min_snapshots_after_dump (3) more snapshots.
            dump_snap_ids = set(lineage_at_dump["snapshot_ids"])
            wait_deadline = time.monotonic() + 90.0
            lk_meta_before_restore: dict[str, Any] | None = None
            lineage_before_restore: dict[str, Any] | None = None
            while time.monotonic() < wait_deadline:
                candidate = fetch_lakekeeper_table_metadata(
                    self._cfg, namespace=self._cfg.lakekeeper_namespace, table="orders"
                )
                if candidate:
                    lin = extract_snapshot_lineage(candidate)
                    new_ids = [sid for sid in lin["snapshot_ids"] if sid not in dump_snap_ids]
                    if len(new_ids) >= self._min_snapshots_after_dump:
                        lk_meta_before_restore = candidate
                        lineage_before_restore = lin
                        break
                time.sleep(0.3)

            if lk_meta_before_restore is None or lineage_before_restore is None:
                raise TimeoutError(
                    f"Did not observe {self._min_snapshots_after_dump} new snapshots after dump within 90s"
                )

            # Capture exact before_restore state immediately after the 3rd post-dump commit.
            before_restore_utc = utc_now_iso()
            before_restore_ns = time.monotonic_ns()
            write_json(self._run_dir / "lakekeeper_table_before_restore.json", lk_meta_before_restore)
            with psycopg.connect(self._cfg.pg_dsn, autocommit=True) as conn:
                pg_pos_before_restore = capture_pg_pipeline_positions(conn)
                log_index_before_restore = fetch_pg2iceberg_log_index_paths(
                    conn, bucket=self._cfg.silo_bucket
                )
            write_json(self._run_dir / "pg_log_index_before_restore.json", log_index_before_restore)
            silo_before_restore = list_silo_objects(self._cfg)
            write_json(self._run_dir / "silo_objects_before_restore.json", silo_before_restore)
            lk_db_before_restore = inspect_lakekeeper_db_summary(self._cfg)
            pg2_before_restore = inspect_service_container(
                self._cfg, "pg2iceberg", profiles=("connector",)
            )

            self._events["before_restore"] = {
                "captured_at_utc": before_restore_utc,
                "offset_ms": round((before_restore_ns - self._window_start_ns) / 1_000_000.0, 2),
                "lineage": lineage_before_restore,
                "pg_positions": pg_pos_before_restore,
                "pg_offsets": extract_coordinator_offsets(pg_pos_before_restore),
                "lakekeeper_db_summary": lk_db_before_restore,
                "pg2iceberg_container": pg2_before_restore,
                "silo_summary": summarize_silo_objects_by_role(
                    silo_before_restore, str(lineage_before_restore["location"] or "")
                ),
            }

            # 4. Stop Lakekeeper, drop & recreate fresh lakekeeper DB, restore pg_dump, start Lakekeeper.
            with self._lock:
                self._phase = "restoring"

            stop_start_ns = time.monotonic_ns()
            stop_start_utc = utc_now_iso()
            self._logged_cmd([*compose_cmd_with_profiles(self._cfg), "stop", "lakekeeper"], check=True)
            stopped_ns = time.monotonic_ns()

            recreate_start_ns = time.monotonic_ns()
            self._logged_cmd(
                [
                    *compose_cmd_with_profiles(self._cfg),
                    "exec",
                    "-T",
                    "lakekeeper-db",
                    "psql",
                    "-U",
                    "postgres",
                    "-d",
                    "postgres",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-c",
                    "DROP DATABASE lakekeeper WITH (FORCE);",
                    "-c",
                    "CREATE DATABASE lakekeeper;",
                ],
                check=True,
            )
            recreate_end_ns = time.monotonic_ns()

            restore_start_ns = time.monotonic_ns()
            self._logged_cmd(
                [
                    *compose_cmd_with_profiles(self._cfg),
                    "exec",
                    "-T",
                    "lakekeeper-db",
                    "pg_restore",
                    "-U",
                    "postgres",
                    "-d",
                    "lakekeeper",
                    "--exit-on-error",
                    BACKUP_PATH_IN_CONTAINER,
                ],
                check=True,
            )
            restore_end_ns = time.monotonic_ns()

            start_cmd_ns = time.monotonic_ns()
            self._logged_cmd([*compose_cmd_with_profiles(self._cfg), "start", "lakekeeper"], check=True)
            health_res = wait_for_lakekeeper_healthy(self._cfg, timeout_s=90.0)
            healthy_ns = int(health_res["healthy_mono_ns"])
            healthy_utc = str(health_res["healthy_utc"])

            with self._lock:
                self._phase = "post_restore"

            # 5. Immediately capture after_restore state.
            lk_meta_after_restore = fetch_lakekeeper_table_metadata(
                self._cfg, namespace=self._cfg.lakekeeper_namespace, table="orders"
            )
            assert lk_meta_after_restore is not None
            write_json(self._run_dir / "lakekeeper_table_after_restore.json", lk_meta_after_restore)
            lineage_after_restore = extract_snapshot_lineage(lk_meta_after_restore)

            with psycopg.connect(self._cfg.pg_dsn, autocommit=True) as conn:
                pg_pos_after_restore = capture_pg_pipeline_positions(conn)
            silo_after_restore = list_silo_objects(self._cfg)
            write_json(self._run_dir / "silo_objects_after_restore.json", silo_after_restore)
            lk_db_after_restore = inspect_lakekeeper_db_summary(self._cfg)
            pg2_after_restore = inspect_service_container(
                self._cfg, "pg2iceberg", profiles=("connector",)
            )

            self._events["restore"] = {
                "stop_start_utc": stop_start_utc,
                "lakekeeper_healthy_utc": healthy_utc,
                "stop_offset_ms": round((stop_start_ns - self._window_start_ns) / 1_000_000.0, 2),
                "healthy_offset_ms": round((healthy_ns - self._window_start_ns) / 1_000_000.0, 2),
                "stop_duration_ms": round((stopped_ns - stop_start_ns) / 1_000_000.0, 2),
                "recreate_fresh_db_ms": round((recreate_end_ns - recreate_start_ns) / 1_000_000.0, 2),
                "pg_restore_ms": round((restore_end_ns - restore_start_ns) / 1_000_000.0, 2),
                "lakekeeper_start_to_healthy_ms": round(
                    (healthy_ns - start_cmd_ns) / 1_000_000.0, 2
                ),
                "total_catalog_downtime_ms": round((healthy_ns - stopped_ns) / 1_000_000.0, 2),
            }
            self._events["after_restore"] = {
                "captured_at_utc": utc_now_iso(),
                "lineage": lineage_after_restore,
                "pg_positions": pg_pos_after_restore,
                "pg_offsets": extract_coordinator_offsets(pg_pos_after_restore),
                "lakekeeper_db_summary": lk_db_after_restore,
                "pg2iceberg_container": pg2_after_restore,
                "silo_summary": summarize_silo_objects_by_role(
                    silo_after_restore, str(lineage_after_restore["location"] or "")
                ),
            }
        except Exception as exc:
            self._events["error"] = f"{type(exc).__name__}: {exc}"
        finally:
            self._drill_done.set()

    def wait_drill_done(self, timeout_s: float = 240.0) -> None:
        self._drill_done.wait(timeout=timeout_s)

    def stop_and_collect(self) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        self._stop_sampling.set()
        if self._drill_thread is not None:
            self._drill_thread.join(timeout=30.0)
        if self._sampler_thread is not None:
            self._sampler_thread.join(timeout=30.0)
        with self._lock:
            return dict(self._events), list(self._samples)


def run_pg2iceberg_oneshot(
    cfg: LabConfig,
    logged_cmd: Any,
    subcommand: list[str],
    *,
    log_path: Path,
) -> dict[str, Any]:
    """Execute a documented one-shot pg2iceberg CLI subcommand (verify, cleanup, snapshot)."""
    cmd = [
        *compose_cmd_with_profiles(cfg, ("connector",)),
        "run",
        "--rm",
        "--no-deps",
        "pg2iceberg",
        *subcommand,
        "--config",
        "/etc/pg2iceberg/config.yaml",
    ]
    t0 = time.monotonic_ns()
    started_utc = utc_now_iso()
    proc = logged_cmd(cmd, check=False)
    duration_ms = round((time.monotonic_ns() - t0) / 1_000_000.0, 2)
    combined = proc.stdout + ("\n--- STDERR ---\n" + proc.stderr if proc.stderr else "")
    log_path.write_text(combined, encoding="utf-8")
    return {
        "command": " ".join(cmd),
        "started_utc": started_utc,
        "duration_ms": duration_ms,
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def run_documented_resnapshot_recovery(
    cfg: LabConfig,
    logged_cmd: Any,
    *,
    run_dir: Path,
    seed_res: SeedResult,
    batch_size: int = 2000,
    convergence_timeout_s: float = 180.0,
) -> dict[str, Any]:
    """Run documented pg2iceberg 0.12.0 cleanup + catalog drop + snapshot + run recovery."""
    recovery_t0 = time.monotonic_ns()
    recovery_started_utc = utc_now_iso()

    # 1. Stop running pg2iceberg consumer (required by `pg2iceberg cleanup`: slot must be inactive).
    stop_t0 = time.monotonic_ns()
    logged_cmd([*compose_cmd_with_profiles(cfg, ("connector",)), "stop", "pg2iceberg"], check=True)
    stop_ms = round((time.monotonic_ns() - stop_t0) / 1_000_000.0, 2)

    # 2. Run documented `pg2iceberg cleanup` (drops slot, publication, _pg2iceberg schema).
    cleanup_res = run_pg2iceberg_oneshot(
        cfg, logged_cmd, ["cleanup"], log_path=run_dir / "recovery_pg2iceberg_cleanup.log"
    )
    if cleanup_res["returncode"] != 0:
        raise RuntimeError(f"pg2iceberg cleanup failed: {cleanup_res}")

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        pg_pos_after_cleanup = capture_pg_pipeline_positions(conn)

    # 3. Drop the diverged Iceberg table out-of-band in Lakekeeper (`purgeRequested=false`).
    drop_t0 = time.monotonic_ns()
    drop_res = drop_lakekeeper_table(
        cfg, namespace=cfg.lakekeeper_namespace, table="orders", purge_requested=False
    )
    drop_res["duration_ms"] = round((time.monotonic_ns() - drop_t0) / 1_000_000.0, 2)
    if not drop_res["ok"]:
        raise RuntimeError(f"Lakekeeper dropTable failed: {drop_res}")

    # 4. Run documented one-shot `pg2iceberg snapshot` to rebuild table and pin fresh slot.
    snapshot_res = run_pg2iceberg_oneshot(
        cfg, logged_cmd, ["snapshot"], log_path=run_dir / "recovery_pg2iceberg_snapshot.log"
    )
    if snapshot_res["returncode"] != 0:
        raise RuntimeError(f"pg2iceberg snapshot failed: {snapshot_res}")

    lk_meta_after_snapshot = fetch_lakekeeper_table_metadata(
        cfg, namespace=cfg.lakekeeper_namespace, table="orders"
    )
    assert lk_meta_after_snapshot is not None
    write_json(run_dir / "lakekeeper_table_after_recovery_snapshot.json", lk_meta_after_snapshot)
    lineage_after_snapshot = extract_snapshot_lineage(lk_meta_after_snapshot)
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        pg_pos_after_snapshot = capture_pg_pipeline_positions(conn)

    # 5. Start `pg2iceberg run` to resume CDC from flushed_lsn.
    logged_cmd(
        [*compose_cmd_with_profiles(cfg, ("connector",)), "up", "-d", "pg2iceberg"], check=True
    )

    # Wait briefly for pg2iceberg_slot to become active again before committing post-recovery ops.
    slot_deadline = time.monotonic() + 60.0
    slot_active_recovered = False
    while time.monotonic() < slot_deadline:
        with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
            pos_now = capture_pg_pipeline_positions(conn)
        slots = [s for s in pos_now["replication_slots"] if s["slot_name"] == "pg2iceberg_slot"]
        if slots and slots[0]["active"]:
            slot_active_recovered = True
            break
        time.sleep(0.5)
    if not slot_active_recovered:
        raise RuntimeError("pg2iceberg_slot did not become active after restarting pg2iceberg run")

    # 6. Commit post-recovery mutations (normal update, TOAST update, insert) + recovery marker
    #    to verify live CDC works end-to-end on the freshly rebuilt table.
    rng = random.Random(20261199)
    now_dt = datetime.now(timezone.utc)
    toast_target = seed_res.toast_ids[0]
    with psycopg.connect(cfg.pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.orders
                SET status = %s, amount = %s, updated_at = %s, note = %s, payload = %s::jsonb
                WHERE id = %s
                """,
                (
                    "settled",
                    _make_decimal_38_9(rng),
                    now_dt,
                    _make_toast_note(rng, toast_target, rev=999),
                    _make_toast_payload_json(rng, toast_target, rev=999),
                    toast_target,
                ),
            )
            cur.execute(
                """
                INSERT INTO public.orders (id, status, amount, updated_at, note, payload, discount)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
                """,
                (
                    POST_RECOVERY_INSERT_ID,
                    "created",
                    Decimal("12345.678900001"),
                    now_dt,
                    "k2-post-recovery-insert",
                    json.dumps({"k2_recovery": True}),
                    Decimal("0.000000001"),
                ),
            )
        conn.commit()
        rec_marker_ns, rec_marker_utc = commit_marker_row(
            conn,
            marker_id=POST_RECOVERY_MARKER_ID,
            status="marker_post_recovery",
            note="k2 post-recovery convergence marker",
        )

    rec_convergence = verify_snapshot_convergence(
        cfg,
        POST_RECOVERY_MARKER_ID,
        rec_marker_ns,
        timeout_s=convergence_timeout_s,
        poll_interval_s=1.0,
        batch_size=batch_size,
    )
    lk_meta_post_rec = rec_convergence.pop("final_lakekeeper_metadata", None)
    if lk_meta_post_rec is None:
        lk_meta_post_rec = fetch_lakekeeper_table_metadata(
            cfg, namespace=cfg.lakekeeper_namespace, table="orders"
        )
    assert lk_meta_post_rec is not None
    write_json(run_dir / "lakekeeper_table_post_recovery.json", lk_meta_post_rec)

    # 7. Also run `pg2iceberg verify` as a secondary day-2 check after recovery.
    verify_post_rec = run_pg2iceberg_oneshot(
        cfg, logged_cmd, ["verify"], log_path=run_dir / "recovery_pg2iceberg_verify.log"
    )

    total_recovery_ms = round((time.monotonic_ns() - recovery_t0) / 1_000_000.0, 2)
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        pg_pos_post_rec = capture_pg_pipeline_positions(conn)
    silo_post_rec = list_silo_objects(cfg)
    write_json(run_dir / "silo_objects_post_recovery.json", silo_post_rec)

    con_duck, _ = connect_iceberg(cfg)
    try:
        duckdb_meta_post_rec = read_metadata_files(con_duck, namespace=cfg.lakekeeper_namespace)
        duckdb_snaps_post_rec = read_snapshots(con_duck, namespace=cfg.lakekeeper_namespace)
        duckdb_post_rec_summary = summarize_duckdb_metadata(duckdb_meta_post_rec)
    finally:
        con_duck.close()
    write_json(run_dir / "duckdb_metadata_post_recovery.json", duckdb_meta_post_rec)
    write_json(run_dir / "duckdb_snapshots_post_recovery.json", duckdb_snaps_post_rec)

    lineage_post_rec = extract_snapshot_lineage(lk_meta_post_rec)
    return {
        "procedure": "pg2iceberg cleanup -> Lakekeeper DELETE table (purgeRequested=false) -> pg2iceberg snapshot -> pg2iceberg run",
        "started_utc": recovery_started_utc,
        "total_recovery_to_verified_ms": total_recovery_ms,
        "stop_connector_ms": stop_ms,
        "cleanup": {
            "returncode": cleanup_res["returncode"],
            "duration_ms": cleanup_res["duration_ms"],
            "stdout_tail": cleanup_res["stdout"].strip()[-500:],
            "pg_slots_after_cleanup": pg_pos_after_cleanup["replication_slots"],
            "coordinator_tables_after_cleanup": pg_pos_after_cleanup["coordinator_tables"],
        },
        "drop_table": drop_res,
        "snapshot": {
            "returncode": snapshot_res["returncode"],
            "duration_ms": snapshot_res["duration_ms"],
            "stdout_tail": snapshot_res["stdout"].strip()[-500:],
            "new_table_uuid": lineage_after_snapshot["table_uuid"],
            "new_table_location": lineage_after_snapshot["location"],
            "initial_snapshot_id": lineage_after_snapshot["current_snapshot_id"],
            "pg_offsets_after_snapshot": extract_coordinator_offsets(pg_pos_after_snapshot),
        },
        "post_recovery_mutations": {
            "updated_toast_id": toast_target,
            "inserted_id": POST_RECOVERY_INSERT_ID,
            "marker_id": POST_RECOVERY_MARKER_ID,
            "marker_commit_utc": rec_marker_utc,
        },
        "post_recovery_convergence": rec_convergence,
        "pg2iceberg_verify_after_recovery": {
            "returncode": verify_post_rec["returncode"],
            "duration_ms": verify_post_rec["duration_ms"],
            "stdout": verify_post_rec["stdout"].strip(),
            "stderr": verify_post_rec["stderr"].strip(),
        },
        "lineage_post_recovery": lineage_post_rec,
        "duckdb_post_recovery_metadata_summary": duckdb_post_rec_summary,
        "pg_positions_post_recovery": pg_pos_post_rec,
        "silo_post_recovery_summary": summarize_silo_objects_by_role(
            silo_post_rec, str(lineage_post_rec["location"] or "")
        ),
    }


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def run_k2(
    *,
    seed_rows: int = 25000,
    toast_rows: int = 100,
    duration_s: float = 180.0,
    rate_ops_s: float = 10.0,
    rng_seed: int = 20261119,
    batch_size: int = 2000,
    probe_interval_s: float = 20.0,
    probe_poll_interval_s: float = 0.5,
    dump_delay_s: float = 35.0,
    min_snapshots_after_dump: int = 3,
    sample_interval_s: float = 2.0,
    convergence_timeout_s: float = 90.0,
    recovery_timeout_s: float = 180.0,
    label: str = "k2-catalog-restore-drill",
    keep_up: bool = False,
) -> Path:
    """Execute Milestone K2 catalog metadata restore drill end to end on a fresh cdclab stack."""
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

    # 3. Seed M1 fixture (25,000 rows, 100 TOAST rows), start pg2iceberg, wait for snapshot + warm-up.
    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        seed_res = seed_orders(
            pg_conn,
            seed_rows=seed_rows,
            toast_row_count=toast_rows,
            rng_seed=rng_seed,
        )
        pg_pos_after_seed = capture_pg_pipeline_positions(pg_conn)

    logged_cmd([*compose_cmd_with_profiles(cfg, ("connector",)), "up", "-d", "pg2iceberg"])
    digests = collect_digests_for_profiles(cfg, ("connector",))
    con_v, reader_versions = connect_iceberg(cfg)
    con_v.close()
    initial_snapshot_info = wait_for_connector_and_snapshot(cfg, timeout_s=180.0)

    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        warmup_commit_ns, warmup_commit_utc = commit_marker_row(
            pg_conn,
            marker_id=WARMUP_MARKER_ID,
            status="marker_warmup",
            note="k2 warm-up marker before restore drill",
        )
    warmup_vis = wait_for_marker_visible(
        cfg, WARMUP_MARKER_ID, warmup_commit_ns, timeout_s=120.0, poll_interval_s=0.5
    )
    if not warmup_vis["visible"]:
        raise RuntimeError(f"Warm-up marker not visible within timeout: {warmup_vis}")

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as pg_conn:
        pg_pos_window_start = capture_pg_pipeline_positions(pg_conn)

    # 4. Run M1 workload while CatalogRestoreDrillController dumps at T, waits >=3 snaps, restores DB.
    controller = CatalogRestoreDrillController(
        cfg,
        run_dir=run_dir,
        dump_delay_s=dump_delay_s,
        min_snapshots_after_dump=min_snapshots_after_dump,
        sample_interval_s=sample_interval_s,
        samples_jsonl=run_dir / "drill_samples.jsonl",
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
    controller.wait_drill_done(timeout_s=240.0)

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as pg_conn:
        toast_after_workload = verify_toast_storage(pg_conn)
        pg_pos_window_end = capture_pg_pipeline_positions(pg_conn)

    # 5. Stop writes, commit final marker, and run exact convergence check with a deadline.
    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        final_commit_ns, final_commit_utc = commit_marker_row(
            pg_conn,
            marker_id=FINAL_MARKER_ID,
            status="marker_final",
            note="k2 post-restore convergence marker",
        )
        pg_pos_after_final_marker = capture_pg_pipeline_positions(pg_conn)

    convergence = verify_snapshot_convergence(
        cfg,
        FINAL_MARKER_ID,
        final_commit_ns,
        timeout_s=convergence_timeout_s,
        poll_interval_s=1.0,
        batch_size=batch_size,
    )
    drill_events, drill_samples = controller.stop_and_collect()
    commands_executed.extend(drill_events.get("commands") or [])
    if drill_events.get("error"):
        raise RuntimeError(f"CatalogRestoreDrillController error: {drill_events['error']}")

    lk_meta_post_drill = convergence.pop("final_lakekeeper_metadata", None)
    if lk_meta_post_drill is None:
        lk_meta_post_drill = fetch_lakekeeper_table_metadata(
            cfg, namespace=cfg.lakekeeper_namespace, table="orders"
        )
    assert lk_meta_post_drill is not None
    write_json(run_dir / "lakekeeper_table_post_drill.json", lk_meta_post_drill)
    lineage_post_drill = extract_snapshot_lineage(lk_meta_post_drill)

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as pg_conn:
        pg_pos_post_drill = capture_pg_pipeline_positions(pg_conn)
        log_index_post_drill = fetch_pg2iceberg_log_index_paths(pg_conn, bucket=cfg.silo_bucket)
        log_index_all_rows = fetch_full_log_index_rows(pg_conn)
    write_json(run_dir / "pg_log_index_post_drill.json", log_index_post_drill)
    write_json(run_dir / "pg_log_index_all_rows_post_drill.json", log_index_all_rows)

    silo_post_drill = list_silo_objects(cfg)
    write_json(run_dir / "silo_objects_post_drill.json", silo_post_drill)

    pg2_cont_post_drill = inspect_service_container(cfg, "pg2iceberg", profiles=("connector",))
    pg2_logs_post_drill = run_cmd(
        [*compose_cmd_with_profiles(cfg, ("connector",)), "logs", "--no-color", "pg2iceberg"],
        check=False,
    ).stdout
    (run_dir / "pg2iceberg_post_drill.log").write_text(pg2_logs_post_drill, encoding="utf-8")
    pg2_log_summary_post_drill = summarize_pg2iceberg_logs(pg2_logs_post_drill)

    lk_logs_post_drill = run_cmd(
        [*compose_cmd_with_profiles(cfg), "logs", "--no-color", "lakekeeper"],
        check=False,
    ).stdout
    (run_dir / "lakekeeper_post_drill.log").write_text(lk_logs_post_drill, encoding="utf-8")

    con_duck, _ = connect_iceberg(cfg)
    try:
        duckdb_meta_post_drill = read_metadata_files(con_duck, namespace=cfg.lakekeeper_namespace)
        duckdb_snaps_post_drill = read_snapshots(con_duck, namespace=cfg.lakekeeper_namespace)
        duckdb_post_drill_summary = summarize_duckdb_metadata(duckdb_meta_post_drill)
    finally:
        con_duck.close()
    write_json(run_dir / "duckdb_metadata_post_drill.json", duckdb_meta_post_drill)
    write_json(run_dir / "duckdb_snapshots_post_drill.json", duckdb_snaps_post_drill)

    # Also capture pg2iceberg's own built-in `verify` output against the post-drill table.
    pg2_verify_post_drill = run_pg2iceberg_oneshot(
        cfg, logged_cmd, ["verify"], log_path=run_dir / "pg2iceberg_verify_post_drill.log"
    )

    # 6. Forensic analysis of snapshot rollback, fork, skipped offsets, and exact row loss.
    silo_before_restore = json.loads(
        (run_dir / "silo_objects_before_restore.json").read_text(encoding="utf-8")
    )
    silo_after_restore = json.loads(
        (run_dir / "silo_objects_after_restore.json").read_text(encoding="utf-8")
    )

    divergence = analyze_restore_divergence(
        lineage_at_dump=drill_events["dump"]["lineage"],
        lineage_before_restore=drill_events["before_restore"]["lineage"],
        lineage_after_restore=drill_events["after_restore"]["lineage"],
        lineage_post_drill=lineage_post_drill,
        silo_before_restore=silo_before_restore,
        silo_after_restore=silo_after_restore,
        silo_post_drill=silo_post_drill,
        pg_offsets_at_dump=drill_events["dump"]["pg_offsets"],
        pg_offsets_before_restore=drill_events["before_restore"]["pg_offsets"],
        pg_offsets_after_restore=drill_events["after_restore"]["pg_offsets"],
        pg_offsets_post_drill=extract_coordinator_offsets(pg_pos_post_drill),
        pg2_log_summary=pg2_log_summary_post_drill,
        pg2_container_after_restore=drill_events["after_restore"]["pg2iceberg_container"],
        pg2_container_post_drill=pg2_cont_post_drill,
    )
    write_json(run_dir / "divergence_analysis.json", divergence)

    post_drill_full_diff: dict[str, Any] | None = None
    loss_attribution: dict[str, Any] | None = None
    if not convergence["passed"] and lineage_post_drill["current_snapshot_id"] is not None:
        post_drill_full_diff = run_exact_comparison_full(
            cfg,
            int(lineage_post_drill["current_snapshot_id"]),
            batch_size=batch_size,
            max_examples=10000,
        )
        write_json(run_dir / "post_drill_full_diff.json", post_drill_full_diff)

        ops_rows = _load_jsonl(run_dir / "ops.jsonl")
        probe_rows = _load_jsonl(run_dir / "steady_state_probes.jsonl")
        dump_cur_snap = drill_events["dump"]["lineage"].get("current_snapshot") or {}
        before_cur_snap = drill_events["before_restore"]["lineage"].get("current_snapshot") or {}
        rb_start_utc = find_log_index_cutoff_utc(
            log_index_all_rows, divergence.get("restored_snapshot_log_end")
        )
        rb_end_utc = find_log_index_cutoff_utc(
            log_index_all_rows, divergence.get("before_restore_snapshot_log_end")
        )
        loss_attribution = attribute_divergence_to_workload(
            full_diff=post_drill_full_diff["row_comparison"],
            ops=ops_rows,
            probes=probe_rows,
            window_start_utc=workload_summary.get("window_start_utc"),
            dump_snapshot_timestamp_ms=dump_cur_snap.get("timestamp_ms"),
            before_restore_snapshot_timestamp_ms=before_cur_snap.get("timestamp_ms"),
            after_restore_utc=drill_events["restore"]["lakekeeper_healthy_utc"],
            rollback_start_utc_override=rb_start_utc,
            rollback_end_utc_override=rb_end_utc,
        )
        write_json(run_dir / "loss_attribution.json", loss_attribution)

    # 7. Step 3: If restore left the table behind/diverged, test one documented recovery procedure.
    recovery_result: dict[str, Any] | None = None
    if not convergence["passed"]:
        recovery_result = run_documented_resnapshot_recovery(
            cfg,
            logged_cmd,
            run_dir=run_dir,
            seed_res=seed_res,
            batch_size=batch_size,
            convergence_timeout_s=recovery_timeout_s,
        )
        write_json(run_dir / "recovery_result.json", recovery_result)

        # Save final logs after recovery too.
        pg2_logs_post_rec = run_cmd(
            [*compose_cmd_with_profiles(cfg, ("connector",)), "logs", "--no-color", "pg2iceberg"],
            check=False,
        ).stdout
        (run_dir / "pg2iceberg_post_recovery.log").write_text(pg2_logs_post_rec, encoding="utf-8")
        lk_logs_post_rec = run_cmd(
            [*compose_cmd_with_profiles(cfg), "logs", "--no-color", "lakekeeper"],
            check=False,
        ).stdout
        (run_dir / "lakekeeper_post_recovery.log").write_text(lk_logs_post_rec, encoding="utf-8")

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

    drill_outcome = str(convergence["outcome"])
    drill_valid = (
        divergence["restored_to_dump_snapshot"]
        and divergence["rolled_back_snapshot_count"] >= min_snapshots_after_dump
        and int(workload_summary["error_count"]) == 0
        and cleanup_ok
    )
    recovery_passed = True if recovery_result is None else bool(
        recovery_result["post_recovery_convergence"]["passed"]
    )

    summary = {
        "milestone": "K2",
        "label": label,
        "drill_outcome": drill_outcome,
        "drill_valid": drill_valid,
        "recovery_executed": recovery_result is not None,
        "recovery_outcome": (
            recovery_result["post_recovery_convergence"]["outcome"] if recovery_result else None
        ),
        "recovery_passed": recovery_passed,
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
            "dump_delay_s": dump_delay_s,
            "min_snapshots_after_dump": min_snapshots_after_dump,
            "sample_interval_s": sample_interval_s,
            "convergence_timeout_s": convergence_timeout_s,
            "recovery_timeout_s": recovery_timeout_s,
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
        },
        "resources_after_pre_clean": resources_after_pre_clean,
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
        "drill": {
            "sample_count": len(drill_samples),
            "events": drill_events,
            "divergence_analysis": divergence,
            "pg2iceberg_log_summary_post_drill": pg2_log_summary_post_drill,
            "pg2iceberg_container_post_drill": pg2_cont_post_drill,
            "pg2iceberg_verify_post_drill": {
                "returncode": pg2_verify_post_drill["returncode"],
                "duration_ms": pg2_verify_post_drill["duration_ms"],
                "stdout": pg2_verify_post_drill["stdout"].strip(),
                "stderr": pg2_verify_post_drill["stderr"].strip(),
            },
            "silo_post_drill_summary": summarize_silo_objects_by_role(
                silo_post_drill, str(lineage_post_drill["location"] or "")
            ),
            "duckdb_post_drill_snapshot_count": len(duckdb_snaps_post_drill),
            "duckdb_post_drill_metadata_summary": duckdb_post_drill_summary,
        },
        "post_drill_convergence": {
            "final_marker_commit_utc": final_commit_utc,
            **convergence,
        },
        "post_drill_full_diff_summary": (
            {
                "snapshot_id": post_drill_full_diff["snapshot_id"],
                "passed": post_drill_full_diff["passed"],
                "source_row_count": post_drill_full_diff["row_comparison"]["source_row_count"],
                "lake_row_count": post_drill_full_diff["row_comparison"]["lake_row_count"],
                "exact_match_row_count": post_drill_full_diff["row_comparison"]["exact_match_row_count"],
                "source_only_count": post_drill_full_diff["row_comparison"]["source_only_count"],
                "lake_only_count": post_drill_full_diff["row_comparison"]["lake_only_count"],
                "duplicate_lake_id_count": post_drill_full_diff["row_comparison"]["duplicate_lake_id_count"],
                "value_mismatch_row_count": post_drill_full_diff["row_comparison"]["value_mismatch_row_count"],
                "value_mismatch_column_counts": post_drill_full_diff["row_comparison"]["value_mismatch_column_counts"],
            }
            if post_drill_full_diff
            else None
        ),
        "loss_attribution_summary": (
            {
                "total_discrepant_ids": loss_attribution["total_discrepant_ids"],
                "rolled_back_window_ops_by_kind": loss_attribution["rolled_back_window_ops_by_kind"],
                "source_only_count": loss_attribution["source_only_missing_from_iceberg"]["count"],
                "source_only_by_window": loss_attribution["source_only_missing_from_iceberg"]["by_primary_window"],
                "source_only_by_op": loss_attribution["source_only_missing_from_iceberg"]["by_last_op_kind"],
                "lake_only_count": loss_attribution["lake_only_ghost_rows_in_iceberg"]["count"],
                "lake_only_by_window": loss_attribution["lake_only_ghost_rows_in_iceberg"]["by_primary_window"],
                "lake_only_by_op": loss_attribution["lake_only_ghost_rows_in_iceberg"]["by_last_op_kind"],
                "value_mismatch_count": loss_attribution["value_mismatch_stale_rows_in_iceberg"]["count"],
                "value_mismatch_by_window": loss_attribution["value_mismatch_stale_rows_in_iceberg"]["by_primary_window"],
                "value_mismatch_by_op": loss_attribution["value_mismatch_stale_rows_in_iceberg"]["by_last_op_kind"],
            }
            if loss_attribution
            else None
        ),
        "recovery": recovery_result,
        "pipeline_positions": {
            "after_seed": pg_pos_after_seed,
            "window_start": pg_pos_window_start,
            "window_end": pg_pos_window_end,
            "after_final_marker": pg_pos_after_final_marker,
            "post_drill": pg_pos_post_drill,
        },
        "teardown": cleanup_result,
    }
    write_json(run_dir / "summary.json", summary)

    compact = {
        "run_dir": str(run_dir.relative_to(REPO_ROOT).as_posix()),
        "drill_outcome": drill_outcome,
        "drill_valid": drill_valid,
        "rolled_back_snapshot_count": divergence["rolled_back_snapshot_count"],
        "post_restore_new_snapshot_count": divergence["post_restore_new_snapshot_count"],
        "fork_detected": divergence["fork_detected"],
        "skipped_log_offset_range": divergence["skipped_log_offset_range"],
        "connector_behavior": divergence["connector_behavior"],
        "post_drill_full_diff_summary": summary["post_drill_full_diff_summary"],
        "pg2iceberg_verify_post_drill_rc": pg2_verify_post_drill["returncode"],
        "recovery_executed": recovery_result is not None,
        "recovery_passed": recovery_passed,
        "cleanup_ok": cleanup_ok,
    }
    print(json.dumps(compact, indent=2, default=json_default))
    if not drill_valid or not recovery_passed:
        raise SystemExit(1)
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run K2 Lakekeeper catalog metadata restore drill."
    )
    parser.add_argument("--seed-rows", type=int, default=25000)
    parser.add_argument("--toast-rows", type=int, default=100)
    parser.add_argument("--duration-s", type=float, default=180.0)
    parser.add_argument("--rate-ops-s", type=float, default=10.0)
    parser.add_argument("--rng-seed", type=int, default=20261119)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--probe-interval-s", type=float, default=20.0)
    parser.add_argument("--probe-poll-interval-s", type=float, default=0.5)
    parser.add_argument("--dump-delay-s", type=float, default=35.0)
    parser.add_argument("--min-snapshots-after-dump", type=int, default=3)
    parser.add_argument("--sample-interval-s", type=float, default=2.0)
    parser.add_argument("--convergence-timeout-s", type=float, default=90.0)
    parser.add_argument("--recovery-timeout-s", type=float, default=180.0)
    parser.add_argument("--label", default="k2-catalog-restore-drill")
    parser.add_argument("--keep-up", action="store_true")
    args = parser.parse_args()
    run_k2(
        seed_rows=args.seed_rows,
        toast_rows=args.toast_rows,
        duration_s=args.duration_s,
        rate_ops_s=args.rate_ops_s,
        rng_seed=args.rng_seed,
        batch_size=args.batch_size,
        probe_interval_s=args.probe_interval_s,
        probe_poll_interval_s=args.probe_poll_interval_s,
        dump_delay_s=args.dump_delay_s,
        min_snapshots_after_dump=args.min_snapshots_after_dump,
        sample_interval_s=args.sample_interval_s,
        convergence_timeout_s=args.convergence_timeout_s,
        recovery_timeout_s=args.recovery_timeout_s,
        label=args.label,
        keep_up=args.keep_up,
    )


if __name__ == "__main__":
    main()
