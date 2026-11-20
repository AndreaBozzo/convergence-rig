"""Milestone C1 runner: M3 REPACK pairs with pg2iceberg compaction enabled, compaction audit,
and reproducible C1-vs-M3 comparison tables and SVG chart."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
from html import escape
import json
import os
from pathlib import Path
import re
from typing import Any, Iterator
import urllib.request

from lab.config import REPO_ROOT, load_config
from lab.m2 import render_pair_from_dirs, run_single_experiment
from lab.m3 import (
    _extract_run_metrics,
    _rel_str,
    _resolve_run_path,
    aggregate_repeated_runs,
    build_paired_metric,
    default_orders_for_count,
    median_and_range,
)
from lab.pipeline import (
    json_default,
    list_non_project_containers,
    list_project_resources,
    run_cmd,
    utc_run_stamp,
    write_json,
)
from lab.plots import (
    BASELINE_COLOR,
    BG_COLOR,
    BORDER_COLOR,
    GRID_COLOR,
    MUTED_COLOR,
    PANEL_BG,
    REPACK_COLOR,
    TEXT_COLOR,
    _nice_upper,
    _svg_open,
    render_latency_comparison_svg,
    render_reader_lag_svg,
    render_repack_window_timeseries_svg,
)

DEFAULT_COMPACTION_CONFIG = "pg2iceberg.compaction.yaml"
DEFAULT_M3_MANIFEST = REPO_ROOT / "runs" / "20261119T200004Z-m3-repeat" / "manifest.json"

EXPECTED_STARTUP_WARN_SUBSTRING = (
    "vended-router: table not yet in catalog at startup; will be registered lazily"
)
LOG_CONFLICT_RE = re.compile(r"\b(conflict|stale|precondition|optimistic|cas|alreadyexists)\b", re.I)
LOG_RETRY_RE = re.compile(r"\b(retr(?:y|ied|ying)|backoff|reattempt)\b", re.I)
LOG_ERROR_RE = re.compile(r"\b(error|failed|failure|panic|fatal|abort)\b", re.I)


def parse_utc_iso_to_ms(ts: str | None) -> int | None:
    """Parse an ISO-8601 UTC timestamp string into integer epoch milliseconds."""
    if not ts:
        return None
    s = ts.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(round(dt.timestamp() * 1000.0))


def epoch_ms_to_utc_iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _safe_int(val: Any) -> int | None:
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def parse_pg2iceberg_compaction_metrics(metrics_text: str) -> dict[str, Any]:
    """Extract pg2iceberg_compaction_* Prometheus samples from /metrics text."""
    runs: list[dict[str, Any]] = []
    files_rewritten: list[dict[str, Any]] = []
    duration_count: float | None = None
    duration_sum: float | None = None

    for raw in metrics_text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("pg2iceberg_compaction_runs_total"):
            parts = line.rsplit(" ", 1)
            if len(parts) == 2:
                try:
                    runs.append({"series": parts[0], "value": float(parts[1])})
                except ValueError:
                    pass
        elif line.startswith("pg2iceberg_compaction_files_rewritten_total"):
            parts = line.rsplit(" ", 1)
            if len(parts) == 2:
                try:
                    files_rewritten.append({"series": parts[0], "value": float(parts[1])})
                except ValueError:
                    pass
        elif line.startswith("pg2iceberg_compaction_duration_seconds_count"):
            parts = line.rsplit(" ", 1)
            if len(parts) == 2:
                try:
                    duration_count = float(parts[1])
                except ValueError:
                    pass
        elif line.startswith("pg2iceberg_compaction_duration_seconds_sum"):
            parts = line.rsplit(" ", 1)
            if len(parts) == 2:
                try:
                    duration_sum = float(parts[1])
                except ValueError:
                    pass

    return {
        "compaction_runs_total": runs,
        "compaction_files_rewritten_total": files_rewritten,
        "compaction_duration_seconds_count": duration_count,
        "compaction_duration_seconds_sum": duration_sum,
    }


def analyze_pg2iceberg_log(log_text: str) -> dict[str, Any]:
    """Audit pg2iceberg JSON logs for conflicts, retries, errors, and non-startup warnings."""
    total_lines = 0
    expected_startup_warnings = 0
    errors: list[dict[str, Any]] = []
    unexpected_warnings: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    retries: list[dict[str, Any]] = []
    compaction_lines: list[dict[str, Any]] = []

    for raw in log_text.splitlines():
        line = raw.strip()
        if not line:
            continue
        total_lines += 1
        # Strip docker compose prefix ("pg2iceberg-1  | ") if present.
        payload = line.split("|", 1)[1].strip() if "|" in line and "{" in line.split("|", 1)[1] else line
        parsed: dict[str, Any] | None = None
        if payload.startswith("{"):
            try:
                parsed = json.loads(payload)
            except json.JSONDecodeError:
                parsed = None

        if parsed is not None:
            level = str(parsed.get("level") or "").upper()
            msg = str(parsed.get("message") or "")
            ts = parsed.get("timestamp")
            target = parsed.get("target")
            full_text = json.dumps(parsed, sort_keys=True)
            entry = {"timestamp": ts, "level": level, "target": target, "message": msg}
        else:
            level = "ERROR" if LOG_ERROR_RE.search(payload) else "RAW"
            msg = payload
            full_text = payload
            entry = {"timestamp": None, "level": level, "target": None, "message": msg}

        if EXPECTED_STARTUP_WARN_SUBSTRING in msg:
            expected_startup_warnings += 1
            continue

        if "compact" in full_text.lower():
            compaction_lines.append(entry)

        if level in ("ERROR", "FATAL"):
            errors.append(entry)
        elif level == "WARN":
            unexpected_warnings.append(entry)
        elif parsed is None and LOG_ERROR_RE.search(msg):
            errors.append(entry)

        if LOG_CONFLICT_RE.search(msg) or (parsed is not None and LOG_CONFLICT_RE.search(str(parsed.get("error", "")))):
            conflicts.append(entry)
        if LOG_RETRY_RE.search(msg) or (parsed is not None and LOG_RETRY_RE.search(str(parsed.get("error", "")))):
            retries.append(entry)

    return {
        "total_log_lines": total_lines,
        "expected_startup_warning_count": expected_startup_warnings,
        "error_count": len(errors),
        "unexpected_warning_count": len(unexpected_warnings),
        "conflict_count": len(conflicts),
        "retry_count": len(retries),
        "compaction_log_line_count": len(compaction_lines),
        "errors": errors[:20],
        "unexpected_warnings": unexpected_warnings[:20],
        "conflicts": conflicts[:20],
        "retries": retries[:20],
        "compaction_log_lines": compaction_lines[:20],
        "clean": (
            len(errors) == 0
            and len(unexpected_warnings) == 0
            and len(conflicts) == 0
            and len(retries) == 0
        ),
    }


def analyze_compaction_evidence(
    summary: dict[str, Any],
    lakekeeper_meta: dict[str, Any] | None,
    *,
    pg2iceberg_log_text: str = "",
    duckdb_snapshots_final: list[dict[str, Any]] | None = None,
    metrics_text: str = "",
) -> dict[str, Any]:
    """Extract authoritative compaction evidence from Iceberg snapshot metadata and logs."""
    workload = summary.get("workload") or {}
    window_start_utc = workload.get("window_start_utc")
    window_end_utc = workload.get("window_end_utc")
    window_start_ms = parse_utc_iso_to_ms(window_start_utc)
    window_end_ms = parse_utc_iso_to_ms(window_end_utc)

    repack = summary.get("repack") or None
    repack_start_ms = parse_utc_iso_to_ms(repack.get("start_utc")) if repack else None
    repack_end_ms = parse_utc_iso_to_ms(repack.get("end_utc")) if repack else None

    final_marker_commit_utc = summary.get("final_marker_commit_utc")
    final_marker_commit_ms = parse_utc_iso_to_ms(final_marker_commit_utc)

    conv = summary.get("convergence") or {}
    verified_snap_id = conv.get("verified_snapshot_id")
    first_marker_snap_id = conv.get("first_marker_snapshot_id")

    raw_snaps = ((lakekeeper_meta or {}).get("metadata") or {}).get("snapshots") or []
    sorted_snaps = sorted(raw_snaps, key=lambda s: int(s.get("sequence-number", 0)))
    snap_by_id: dict[int, dict[str, Any]] = {int(s["snapshot-id"]): s for s in sorted_snaps}

    verified_snap = snap_by_id.get(int(verified_snap_id)) if verified_snap_id is not None else None
    verified_seq = int(verified_snap["sequence-number"]) if verified_snap is not None else None

    compaction_commits: list[dict[str, Any]] = []
    compaction_in_window: list[dict[str, Any]] = []
    compaction_before_window: list[dict[str, Any]] = []
    compaction_after_window: list[dict[str, Any]] = []
    repack_commit_inside: list[dict[str, Any]] = []
    repack_interval_overlap: list[dict[str, Any]] = []
    after_final_marker_before_verified: list[dict[str, Any]] = []

    for sn in sorted_snaps:
        seq = int(sn.get("sequence-number", 0))
        if verified_seq is not None and seq > verified_seq:
            continue
        sm = sn.get("summary") or {}
        op = str(sm.get("operation") or "").lower()
        pg2_op = str(sm.get("pg2iceberg.operation") or "").lower()
        is_compaction = op == "replace" or pg2_op == "compaction"
        if not is_compaction:
            continue

        snap_id = int(sn["snapshot-id"])
        parent_id = _safe_int(sn.get("parent-snapshot-id"))
        ts_ms = _safe_int(sn.get("timestamp-ms"))
        parent_sn = snap_by_id.get(parent_id) if parent_id is not None else None
        parent_ts_ms = _safe_int(parent_sn.get("timestamp-ms")) if parent_sn is not None else None

        removed_del_files = _safe_int(sm.get("removed-delete-files")) or 0
        removed_eq_del_files = _safe_int(sm.get("removed-equality-delete-files")) or 0
        removed_eq_deletes = _safe_int(sm.get("removed-equality-deletes")) or 0
        kind = "delete_rewrite" if (removed_del_files > 0 or removed_eq_del_files > 0) else "small_file_merge"

        in_window = (
            ts_ms is not None
            and window_start_ms is not None
            and window_end_ms is not None
            and window_start_ms <= ts_ms <= window_end_ms
        )
        before_window = (
            ts_ms is not None and window_start_ms is not None and ts_ms < window_start_ms
        )
        after_window = (
            ts_ms is not None and window_end_ms is not None and ts_ms > window_end_ms
        )

        commit_in_repack = (
            ts_ms is not None
            and repack_start_ms is not None
            and repack_end_ms is not None
            and repack_start_ms <= ts_ms <= repack_end_ms
        )
        # Background compaction spans [parent_ts_ms, ts_ms]; check overlap with [repack_start_ms, repack_end_ms].
        span_start = parent_ts_ms if parent_ts_ms is not None else ts_ms
        interval_overlaps_repack = (
            span_start is not None
            and ts_ms is not None
            and repack_start_ms is not None
            and repack_end_ms is not None
            and span_start <= repack_end_ms
            and ts_ms >= repack_start_ms
        )

        between_final_and_verified = (
            ts_ms is not None
            and final_marker_commit_ms is not None
            and ts_ms >= final_marker_commit_ms
        )

        rec = {
            "snapshot_id": snap_id,
            "parent_snapshot_id": parent_id,
            "sequence_number": seq,
            "timestamp_ms": ts_ms,
            "timestamp_utc": epoch_ms_to_utc_iso(ts_ms),
            "parent_timestamp_ms": parent_ts_ms,
            "operation": op,
            "pg2iceberg_operation": sm.get("pg2iceberg.operation"),
            "compaction_kind": kind,
            "added_data_files": _safe_int(sm.get("added-data-files")) or 0,
            "deleted_data_files": _safe_int(sm.get("deleted-data-files")) or 0,
            "removed_delete_files": removed_del_files,
            "removed_equality_delete_files": removed_eq_del_files,
            "removed_equality_deletes": removed_eq_deletes,
            "total_data_files": _safe_int(sm.get("total-data-files")),
            "total_delete_files": _safe_int(sm.get("total-delete-files")),
            "total_equality_deletes": _safe_int(sm.get("total-equality-deletes")),
            "total_records": _safe_int(sm.get("total-records")),
            "total_files_size": _safe_int(sm.get("total-files-size")),
            "in_measurement_window": in_window,
            "commit_inside_repack_window": commit_in_repack,
            "background_span_overlaps_repack_window": interval_overlaps_repack,
            "between_final_marker_and_verified": between_final_and_verified,
            "summary": sm,
        }
        compaction_commits.append(rec)
        if in_window:
            compaction_in_window.append(rec)
        elif before_window:
            compaction_before_window.append(rec)
        elif after_window:
            compaction_after_window.append(rec)
        if commit_in_repack:
            repack_commit_inside.append(rec)
        if interval_overlaps_repack:
            repack_interval_overlap.append(rec)
        if between_final_and_verified:
            after_final_marker_before_verified.append(rec)

    # Check if verified snapshot or first-marker snapshot is itself a compaction commit, or has a compaction parent.
    verified_sm = (verified_snap.get("summary") or {}) if verified_snap is not None else {}
    verified_op = str(verified_sm.get("operation") or "").lower()
    verified_parent_id = _safe_int(verified_snap.get("parent-snapshot-id")) if verified_snap else None
    verified_parent = snap_by_id.get(verified_parent_id) if verified_parent_id is not None else None
    verified_parent_op = (
        str((verified_parent.get("summary") or {}).get("operation") or "").lower()
        if verified_parent
        else None
    )

    first_marker_snap = (
        snap_by_id.get(int(first_marker_snap_id)) if first_marker_snap_id is not None else None
    )
    first_marker_op = (
        str((first_marker_snap.get("summary") or {}).get("operation") or "").lower()
        if first_marker_snap
        else None
    )

    # Post-verification snapshots captured by DuckDB after diff completed (informational only).
    post_verified_compactions: list[dict[str, Any]] = []
    if duckdb_snapshots_final and verified_seq is not None:
        for ds in sorted(duckdb_snapshots_final, key=lambda x: int(x.get("sequence_number", 0))):
            dseq = int(ds.get("sequence_number", 0))
            if dseq <= verified_seq:
                continue
            dsm = ds.get("summary") or {}
            dop = str(ds.get("operation") or dsm.get("operation") or "").lower()
            if dop == "replace" or str(dsm.get("pg2iceberg.operation") or "").lower() == "compaction":
                post_verified_compactions.append(
                    {
                        "sequence_number": dseq,
                        "snapshot_id": _safe_int(ds.get("snapshot_id")),
                        "operation": dop,
                        "total_data_files": _safe_int(dsm.get("total-data-files")),
                        "total_delete_files": _safe_int(dsm.get("total-delete-files")),
                    }
                )

    verified_file_counts = {
        "verified_snapshot_id": int(verified_snap_id) if verified_snap_id is not None else None,
        "verified_sequence_number": verified_seq,
        "verified_operation": verified_op or None,
        "live_data_files": _safe_int(verified_sm.get("total-data-files")),
        "live_equality_delete_files": _safe_int(verified_sm.get("total-delete-files")),
        "live_equality_delete_records": _safe_int(verified_sm.get("total-equality-deletes")),
        "live_position_delete_records": _safe_int(verified_sm.get("total-position-deletes")),
        "total_records": _safe_int(verified_sm.get("total-records")),
        "total_files_size_bytes": _safe_int(verified_sm.get("total-files-size")),
    }

    log_audit = analyze_pg2iceberg_log(pg2iceberg_log_text)
    prom_metrics = parse_pg2iceberg_compaction_metrics(metrics_text) if metrics_text else None

    return {
        "compaction_ran_in_window": len(compaction_in_window) > 0,
        "total_snapshots_up_to_verified": len(
            [s for s in sorted_snaps if verified_seq is None or int(s.get("sequence-number", 0)) <= verified_seq]
        ),
        "compaction_commits_up_to_verified": len(compaction_commits),
        "compaction_commits_in_window": len(compaction_in_window),
        "compaction_commits_before_window": len(compaction_before_window),
        "compaction_commits_after_window_up_to_verified": len(compaction_after_window),
        "small_file_merge_commits_in_window": sum(
            1 for c in compaction_in_window if c["compaction_kind"] == "small_file_merge"
        ),
        "delete_rewrite_commits_in_window": sum(
            1 for c in compaction_in_window if c["compaction_kind"] == "delete_rewrite"
        ),
        "delete_rewrite_commits_up_to_verified": sum(
            1 for c in compaction_commits if c["compaction_kind"] == "delete_rewrite"
        ),
        "total_data_files_replaced_up_to_verified": sum(
            c["deleted_data_files"] for c in compaction_commits
        ),
        "total_delete_files_removed_up_to_verified": sum(
            c["removed_delete_files"] for c in compaction_commits
        ),
        "total_equality_deletes_removed_up_to_verified": sum(
            c["removed_equality_deletes"] for c in compaction_commits
        ),
        "verified_snapshot_files": verified_file_counts,
        "repack_overlap": {
            "applicable": repack is not None,
            "commit_inside_repack_window": len(repack_commit_inside) > 0,
            "commit_inside_repack_count": len(repack_commit_inside),
            "commit_inside_repack_snapshots": [
                {
                    "sequence_number": c["sequence_number"],
                    "snapshot_id": c["snapshot_id"],
                    "timestamp_utc": c["timestamp_utc"],
                    "compaction_kind": c["compaction_kind"],
                }
                for c in repack_commit_inside
            ],
            "background_span_overlaps_repack": len(repack_interval_overlap) > 0,
            "background_span_overlap_count": len(repack_interval_overlap),
            "background_span_overlap_snapshots": [
                {
                    "sequence_number": c["sequence_number"],
                    "snapshot_id": c["snapshot_id"],
                    "timestamp_utc": c["timestamp_utc"],
                    "compaction_kind": c["compaction_kind"],
                }
                for c in repack_interval_overlap
            ],
        },
        "final_marker_overlap": {
            "verified_snapshot_is_compaction": verified_op == "replace",
            "first_marker_snapshot_is_compaction": first_marker_op == "replace",
            "parent_of_verified_is_compaction": verified_parent_op == "replace",
            "compaction_between_final_marker_and_verified": len(after_final_marker_before_verified) > 0,
            "compaction_between_final_marker_and_verified_snapshots": [
                {
                    "sequence_number": c["sequence_number"],
                    "snapshot_id": c["snapshot_id"],
                    "timestamp_utc": c["timestamp_utc"],
                    "compaction_kind": c["compaction_kind"],
                }
                for c in after_final_marker_before_verified
            ],
        },
        "post_verified_compactions_during_diff": post_verified_compactions,
        "compaction_commits": compaction_commits,
        "pg2iceberg_log_audit": log_audit,
        "prometheus_compaction_metrics": prom_metrics,
    }


@contextmanager
def _pg2iceberg_config_env(config_file: str) -> Iterator[None]:
    prev = os.environ.get("PG2ICEBERG_CONFIG_FILE")
    os.environ["PG2ICEBERG_CONFIG_FILE"] = config_file
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop("PG2ICEBERG_CONFIG_FILE", None)
        else:
            os.environ["PG2ICEBERG_CONFIG_FILE"] = prev


def _scrape_metrics(url: str) -> str:
    try:
        with urllib.request.urlopen(f"{url}/metrics", timeout=5.0) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception:
        return ""


def run_c1_single_experiment(
    *,
    run_kind: str,
    compaction_config: str = DEFAULT_COMPACTION_CONFIG,
    seed_rows: int = 250000,
    toast_rows: int = 25000,
    duration_s: float = 180.0,
    rate_ops_s: float = 10.0,
    rng_seed: int = 20261119,
    batch_size: int = 2000,
    probe_interval_s: float = 20.0,
    probe_poll_interval_s: float = 0.5,
    repack_delay_s: float = 30.0,
    repack_sample_interval_s: float = 0.05,
    snapshot_timeout_s: float = 300.0,
    convergence_timeout_s: float = 300.0,
    label: str | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Run one fresh C1 experiment with compaction enabled, audit snapshots/logs, and verify cleanup."""
    cfg_path = REPO_ROOT / "compose" / compaction_config
    if not cfg_path.exists():
        raise FileNotFoundError(f"Compaction config not found: {cfg_path}")
    cfg_bytes = cfg_path.read_bytes()
    cfg_sha256 = hashlib.sha256(cfg_bytes).hexdigest()

    cfg = load_config()
    non_project_before = list_non_project_containers(cfg.compose_project)

    with _pg2iceberg_config_env(compaction_config):
        run_dir, summary = run_single_experiment(
            run_kind=run_kind,
            seed_rows=seed_rows,
            toast_rows=toast_rows,
            duration_s=duration_s,
            rate_ops_s=rate_ops_s,
            rng_seed=rng_seed,
            batch_size=batch_size,
            probe_interval_s=probe_interval_s,
            probe_poll_interval_s=probe_poll_interval_s,
            repack_delay_s=repack_delay_s,
            repack_sample_interval_s=repack_sample_interval_s,
            snapshot_timeout_s=snapshot_timeout_s,
            convergence_timeout_s=convergence_timeout_s,
            label=label or f"c1-{run_kind}",
            milestone="C1",
            keep_up=True,
        )

        # Scrape Prometheus metrics before teardown so compaction counters are preserved.
        metrics_text = _scrape_metrics(cfg.pg2iceberg_metrics_url)
        if metrics_text:
            (run_dir / "pg2iceberg_metrics.txt").write_text(metrics_text, encoding="utf-8")

        # Project-only cleanup and verification.
        down_cmd = [*cfg.compose_base_cmd(include_connector=True), "down", "-v", "--remove-orphans"]
        run_cmd(down_cmd, check=True)
        summary["commands_executed"].append(" ".join(down_cmd))
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
        summary["cleanup"] = cleanup_result

    lk_path = run_dir / "lakekeeper_table_final.json"
    lk_meta = json.loads(lk_path.read_text(encoding="utf-8")) if lk_path.exists() else None
    ds_path = run_dir / "duckdb_snapshots_final.json"
    duckdb_snaps = json.loads(ds_path.read_text(encoding="utf-8")) if ds_path.exists() else None
    log_path = run_dir / "pg2iceberg.log"
    log_text = log_path.read_text(encoding="utf-8") if log_path.exists() else ""

    compaction_ev = analyze_compaction_evidence(
        summary,
        lk_meta,
        pg2iceberg_log_text=log_text,
        duckdb_snapshots_final=duckdb_snaps,
        metrics_text=metrics_text,
    )

    summary["pg2iceberg_config"] = {
        "config_file": f"compose/{compaction_config}",
        "env_var": f"PG2ICEBERG_CONFIG_FILE={compaction_config}",
        "sha256": cfg_sha256,
    }
    summary["compaction"] = compaction_ev

    # Step 3 validity gate: a run in which no compaction commit happened inside the
    # measurement window is an invalid harness run.
    cleanup_ok = (
        cleanup_result["all_project_resources_removed"]
        and cleanup_result["non_project_containers_unchanged"]
    )
    if not compaction_ev["compaction_ran_in_window"]:
        if summary["outcome"] == "converged":
            summary["outcome"] = "invalid harness run"
        summary["passed"] = False
    else:
        summary["passed"] = bool(summary["passed"] and cleanup_ok)

    write_json(run_dir / "summary.json", summary)
    return run_dir, summary


def run_c1_matched_pair(
    *,
    compaction_config: str = DEFAULT_COMPACTION_CONFIG,
    seed_rows: int = 250000,
    toast_rows: int = 25000,
    duration_s: float = 180.0,
    rate_ops_s: float = 10.0,
    rng_seed: int = 20261119,
    batch_size: int = 2000,
    probe_interval_s: float = 20.0,
    probe_poll_interval_s: float = 0.5,
    repack_delay_s: float = 30.0,
    repack_sample_interval_s: float = 0.05,
    snapshot_timeout_s: float = 300.0,
    convergence_timeout_s: float = 300.0,
    order: str = "baseline-first",
    label: str = "c1",
) -> tuple[Path, dict[str, Any]]:
    """Run one fresh C1 matched pair (baseline + concurrent REPACK) with compaction enabled."""
    if order not in ("baseline-first", "repack-first"):
        raise ValueError(f"order must be 'baseline-first' or 'repack-first', got {order!r}")
    run_order = ("baseline", "repack") if order == "baseline-first" else ("repack", "baseline")
    run_dirs: dict[str, Path] = {}
    summaries: dict[str, dict[str, Any]] = {}

    for kind in run_order:
        r_dir, r_sum = run_c1_single_experiment(
            run_kind=kind,
            compaction_config=compaction_config,
            seed_rows=seed_rows,
            toast_rows=toast_rows,
            duration_s=duration_s,
            rate_ops_s=rate_ops_s,
            rng_seed=rng_seed,
            batch_size=batch_size,
            probe_interval_s=probe_interval_s,
            probe_poll_interval_s=probe_poll_interval_s,
            repack_delay_s=repack_delay_s,
            repack_sample_interval_s=repack_sample_interval_s,
            snapshot_timeout_s=snapshot_timeout_s,
            convergence_timeout_s=convergence_timeout_s,
            label=f"{label}-{kind}",
        )
        run_dirs[kind] = r_dir
        summaries[kind] = r_sum

    pair_dir = REPO_ROOT / "runs" / f"{utc_run_stamp()}-{label}-pair"
    _, pair = render_pair_from_dirs(run_dirs["baseline"], run_dirs["repack"], output_dir=pair_dir)
    pair["run_order"] = list(run_order)
    write_json(pair_dir / "pair_summary.json", pair)

    compact = {
        "pair_dir": pair_dir.relative_to(REPO_ROOT).as_posix(),
        "run_order": list(run_order),
        "baseline_run_dir": pair["baseline_run_dir"],
        "repack_run_dir": pair["repack_run_dir"],
        "baseline_outcome": summaries["baseline"]["outcome"],
        "repack_outcome": summaries["repack"]["outcome"],
        "baseline_compactions_in_window": summaries["baseline"]["compaction"]["compaction_commits_in_window"],
        "repack_compactions_in_window": summaries["repack"]["compaction"]["compaction_commits_in_window"],
        "overlap_confirmed": pair["overlap_confirmed"],
        "committed_during_repack_count": pair["committed_during_repack_count"],
    }
    print(json.dumps(compact, indent=2, default=json_default), flush=True)
    return pair_dir, pair


def _load_compaction_for_run(run_dir: Path, summary: dict[str, Any]) -> dict[str, Any]:
    """Recompute compaction evidence strictly from raw run files (never trusting cached summaries)."""
    lk_path = run_dir / "lakekeeper_table_final.json"
    lk_meta = json.loads(lk_path.read_text(encoding="utf-8")) if lk_path.exists() else None
    ds_path = run_dir / "duckdb_snapshots_final.json"
    duckdb_snaps = json.loads(ds_path.read_text(encoding="utf-8")) if ds_path.exists() else None
    log_path = run_dir / "pg2iceberg.log"
    log_text = log_path.read_text(encoding="utf-8") if log_path.exists() else ""
    m_path = run_dir / "pg2iceberg_metrics.txt"
    metrics_text = m_path.read_text(encoding="utf-8") if m_path.exists() else ""
    return analyze_compaction_evidence(
        summary,
        lk_meta,
        pg2iceberg_log_text=log_text,
        duckdb_snapshots_final=duckdb_snaps,
        metrics_text=metrics_text,
    )


def _extract_pipeline_positions_compact(summary: dict[str, Any]) -> dict[str, Any]:
    pp = summary.get("pipeline_positions") or {}
    out: dict[str, Any] = {}
    for cp in ("window_start", "window_end", "final"):
        node = pp.get(cp) or {}
        cdc = next(
            (s for s in (node.get("replication_slots") or []) if s.get("slot_name") == "pg2iceberg_slot"),
            {},
        )
        cs = node.get("coordinator_state") or {}
        mat_rows = cs.get("mat_cursor") or []
        seq_rows = cs.get("log_seq") or []
        default_mat = next(
            (r for r in mat_rows if isinstance(r, dict) and r.get("group_name") == "default"),
            mat_rows[0] if mat_rows and isinstance(mat_rows[0], dict) else None,
        )
        first_seq = seq_rows[0] if seq_rows and isinstance(seq_rows[0], dict) else None
        out[cp] = {
            "current_wal_lsn": node.get("current_wal_lsn"),
            "confirmed_flush_lsn": cdc.get("confirmed_flush_lsn"),
            "restart_lsn": cdc.get("restart_lsn"),
            "wal_gap_bytes_confirmed_flush": cdc.get("wal_gap_bytes_confirmed_flush"),
            "wal_gap_bytes_restart": cdc.get("wal_gap_bytes_restart"),
            "mat_cursor": default_mat.get("last_offset") if default_mat else None,
            "log_seq": first_seq.get("next_offset") if first_seq else None,
        }
    return out


def aggregate_c1_and_compare_m3(
    c1_manifest: dict[str, Any],
    *,
    m3_manifest: dict[str, Any] | None = None,
    repo_root: Path = REPO_ROOT,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Recompute all C1 metrics, compaction audits, and M3 side-by-side comparisons from raw files."""
    c1_agg, c1_series, _ = aggregate_repeated_runs(c1_manifest, repo_root=repo_root)
    c1_agg["milestone"] = "C1"

    compaction_runs: list[dict[str, Any]] = []
    all_compaction_ran_in_window = True
    all_logs_clean = True
    total_repack_commit_overlaps = 0
    total_repack_span_overlaps = 0
    total_final_marker_is_compaction = 0
    total_final_marker_parent_compaction = 0

    for p in c1_agg["pairs"]:
        for side_key in ("baseline", "repack"):
            run_rel = p[f"{side_key}_run_dir"]
            run_dir = _resolve_run_path(run_rel, repo_root)
            r_sum = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            comp = _load_compaction_for_run(run_dir, r_sum)
            vf = comp["verified_snapshot_files"]

            # Enforce C1 Step 3 validity gate on recomputed raw evidence too.
            effective_outcome = p[side_key]["outcome"]
            if not comp["compaction_ran_in_window"]:
                all_compaction_ran_in_window = False
                if effective_outcome == "converged":
                    effective_outcome = "invalid harness run"
                p[side_key]["outcome"] = effective_outcome
                p[side_key]["passed"] = False

            if not comp["pg2iceberg_log_audit"]["clean"]:
                all_logs_clean = False
            if comp["repack_overlap"]["commit_inside_repack_window"]:
                total_repack_commit_overlaps += 1
            if comp["repack_overlap"]["background_span_overlaps_repack"]:
                total_repack_span_overlaps += 1
            if comp["final_marker_overlap"]["verified_snapshot_is_compaction"]:
                total_final_marker_is_compaction += 1
            if comp["final_marker_overlap"]["parent_of_verified_is_compaction"]:
                total_final_marker_parent_compaction += 1

            # Replace post-diff duckdb live file counts with verified-snapshot live file counts.
            p[side_key]["verified_live_data_files"] = vf["live_data_files"]
            p[side_key]["verified_live_equality_delete_files"] = vf["live_equality_delete_files"]
            p[side_key]["verified_live_equality_delete_records"] = vf["live_equality_delete_records"]
            p[side_key]["compaction"] = comp
            p[side_key]["pipeline_positions_compact"] = _extract_pipeline_positions_compact(r_sum)

            compaction_runs.append(
                {
                    "pair_index": p["pair_index"],
                    "side": side_key,
                    "rng_seed": p["rng_seed"],
                    "run_dir": run_rel,
                    "outcome": effective_outcome,
                    "compaction_ran_in_window": comp["compaction_ran_in_window"],
                    "compaction_commits_in_window": comp["compaction_commits_in_window"],
                    "small_file_merge_commits_in_window": comp["small_file_merge_commits_in_window"],
                    "delete_rewrite_commits_in_window": comp["delete_rewrite_commits_in_window"],
                    "compaction_commits_up_to_verified": comp["compaction_commits_up_to_verified"],
                    "verified_live_data_files": vf["live_data_files"],
                    "verified_live_equality_delete_files": vf["live_equality_delete_files"],
                    "verified_live_equality_delete_records": vf["live_equality_delete_records"],
                    "repack_commit_inside": comp["repack_overlap"]["commit_inside_repack_window"],
                    "repack_span_overlap": comp["repack_overlap"]["background_span_overlaps_repack"],
                    "verified_is_compaction": comp["final_marker_overlap"]["verified_snapshot_is_compaction"],
                    "parent_of_verified_is_compaction": comp["final_marker_overlap"]["parent_of_verified_is_compaction"],
                    "compaction_between_final_marker_and_verified": comp["final_marker_overlap"]["compaction_between_final_marker_and_verified"],
                    "log_clean": comp["pg2iceberg_log_audit"]["clean"],
                    "log_errors": comp["pg2iceberg_log_audit"]["error_count"],
                    "log_conflicts": comp["pg2iceberg_log_audit"]["conflict_count"],
                    "log_retries": comp["pg2iceberg_log_audit"]["retry_count"],
                }
            )

    # Recompute convergence counts after enforcing the compaction validity gate.
    cs = c1_agg["convergence_summary"]
    cs["baseline_converged_count"] = sum(
        1 for p in c1_agg["pairs"] if p["baseline"]["outcome"] == "converged"
    )
    cs["repack_converged_count"] = sum(
        1 for p in c1_agg["pairs"] if p["repack"]["outcome"] == "converged"
    )
    cs["all_converged"] = all(
        p["baseline"]["outcome"] == "converged" and p["repack"]["outcome"] == "converged"
        for p in c1_agg["pairs"]
    )
    cs["all_compaction_ran_in_window"] = all_compaction_ran_in_window
    cs["all_valid_runs"] = bool(
        cs["all_converged"]
        and cs["all_overlaps_confirmed"]
        and cs["all_parameters_matched_within_pair"]
        and cs["all_source_reads_after_repack"]
        and cs["all_cleanups_ok"]
        and all_compaction_ran_in_window
    )

    c1_agg["compaction_summary"] = {
        "compaction_config_file": c1_manifest.get("compaction_config", DEFAULT_COMPACTION_CONFIG),
        "all_runs_had_compaction_in_window": all_compaction_ran_in_window,
        "all_pg2iceberg_logs_clean": all_logs_clean,
        "commits_in_window": median_and_range(
            [r["compaction_commits_in_window"] for r in compaction_runs], decimals=1
        ),
        "delete_rewrites_in_window": median_and_range(
            [r["delete_rewrite_commits_in_window"] for r in compaction_runs], decimals=1
        ),
        "verified_live_data_files": median_and_range(
            [r["verified_live_data_files"] for r in compaction_runs], decimals=1
        ),
        "verified_live_equality_delete_files": median_and_range(
            [r["verified_live_equality_delete_files"] for r in compaction_runs], decimals=1
        ),
        "repack_commit_inside_count": total_repack_commit_overlaps,
        "repack_background_span_overlap_count": total_repack_span_overlaps,
        "final_marker_verified_is_compaction_count": total_final_marker_is_compaction,
        "final_marker_parent_is_compaction_count": total_final_marker_parent_compaction,
        "per_run": compaction_runs,
    }

    # Optional M3 comparison matched on the exact same seeds.
    m3_comparison: dict[str, Any] | None = None
    if m3_manifest is not None:
        m3_agg, _, _ = aggregate_repeated_runs(m3_manifest, repo_root=repo_root)
        # Also extract verified-snapshot file counts for M3 directly from lakekeeper_table_final.json.
        m3_by_seed: dict[int, dict[str, Any]] = {}
        for mp in m3_agg["pairs"]:
            for side_key in ("baseline", "repack"):
                mdir = _resolve_run_path(mp[f"{side_key}_run_dir"], repo_root)
                msum = json.loads((mdir / "summary.json").read_text(encoding="utf-8"))
                mcomp = _load_compaction_for_run(mdir, msum)
                mp[side_key]["verified_live_data_files"] = mcomp["verified_snapshot_files"]["live_data_files"]
                mp[side_key]["verified_live_equality_delete_files"] = mcomp["verified_snapshot_files"]["live_equality_delete_files"]
                mp[side_key]["verified_live_equality_delete_records"] = mcomp["verified_snapshot_files"]["live_equality_delete_records"]
                mp[side_key]["compaction_commits_in_window"] = mcomp["compaction_commits_in_window"]
            m3_by_seed[int(mp["rng_seed"])] = mp

        per_run_rows: list[dict[str, Any]] = []
        matched_seeds = True
        for cp in c1_agg["pairs"]:
            seed = int(cp["rng_seed"])
            mp = m3_by_seed.get(seed)
            if mp is None:
                matched_seeds = False
                continue
            for side_key in ("baseline", "repack"):
                cm = cp[side_key]
                mm = mp[side_key]
                per_run_rows.append(
                    {
                        "pair_index": cp["pair_index"],
                        "order": cp["order"],
                        "rng_seed": seed,
                        "side": side_key,
                        "c1": {
                            "run_dir": cm["run_dir"],
                            "outcome": cm["outcome"],
                            "source_row_count": cm["source_row_count"],
                            "lake_row_count": cm["lake_row_count"],
                            "compaction_commits_in_window": cm["compaction"]["compaction_commits_in_window"],
                            "delete_rewrites_in_window": cm["compaction"]["delete_rewrite_commits_in_window"],
                            "verified_live_data_files": cm["verified_live_data_files"],
                            "verified_live_equality_delete_files": cm["verified_live_equality_delete_files"],
                            "final_marker_visibility_ms": cm["final_marker_visibility_ms"],
                            "final_marker_to_verified_match_ms": cm["final_marker_to_verified_match_ms"],
                            "diff_duration_ms": cm["diff_duration_ms"],
                            "probe_p50_ms": cm["probe_p50_ms"],
                            "probe_max_ms": cm["probe_max_ms"],
                            "int_exec_p50_ms": cm["int_exec_p50_ms"],
                            "int_s2c_p50_ms": cm["int_s2c_p50_ms"],
                            "int_s2c_p99_ms": cm["int_s2c_p99_ms"],
                            "full_exec_p50_ms": cm["full_exec_p50_ms"],
                            "full_s2c_p50_ms": cm["full_s2c_p50_ms"],
                            "full_s2c_p99_ms": cm["full_s2c_p99_ms"],
                            "final_cdc_wal_gap_bytes": cm["final_cdc_wal_gap_bytes"],
                        },
                        "m3": {
                            "run_dir": mm["run_dir"],
                            "outcome": mm["outcome"],
                            "source_row_count": mm["source_row_count"],
                            "lake_row_count": mm["lake_row_count"],
                            "compaction_commits_in_window": mm["compaction_commits_in_window"],
                            "verified_live_data_files": mm["verified_live_data_files"],
                            "verified_live_equality_delete_files": mm["verified_live_equality_delete_files"],
                            "final_marker_visibility_ms": mm["final_marker_visibility_ms"],
                            "final_marker_to_verified_match_ms": mm["final_marker_to_verified_match_ms"],
                            "diff_duration_ms": mm["diff_duration_ms"],
                            "probe_p50_ms": mm["probe_p50_ms"],
                            "probe_max_ms": mm["probe_max_ms"],
                            "int_exec_p50_ms": mm["int_exec_p50_ms"],
                            "int_s2c_p50_ms": mm["int_s2c_p50_ms"],
                            "int_s2c_p99_ms": mm["int_s2c_p99_ms"],
                            "full_exec_p50_ms": mm["full_exec_p50_ms"],
                            "full_s2c_p50_ms": mm["full_s2c_p50_ms"],
                            "full_s2c_p99_ms": mm["full_s2c_p99_ms"],
                            "final_cdc_wal_gap_bytes": mm["final_cdc_wal_gap_bytes"],
                        },
                    }
                )

        def _agg_cmp(side_filter: str | None, key: str, *, decimals: int = 2) -> dict[str, Any]:
            rows = [r for r in per_run_rows if side_filter is None or r["side"] == side_filter]
            m3_vals = [r["m3"].get(key) for r in rows]
            c1_vals = [r["c1"].get(key) for r in rows]
            deltas = [
                float(c) - float(m)
                for m, c in zip(m3_vals, c1_vals)
                if m is not None and c is not None
            ]
            return {
                "m3": median_and_range(m3_vals, decimals=decimals),
                "c1": median_and_range(c1_vals, decimals=decimals),
                "delta_c1_minus_m3": median_and_range(deltas, decimals=decimals),
            }

        m3_comparison = {
            "m3_manifest_label": m3_manifest.get("label"),
            "all_seeds_matched": matched_seeds and len(per_run_rows) == 2 * len(c1_agg["pairs"]),
            "m3_summary": {
                "convergence_summary": m3_agg["convergence_summary"],
                "repack_execution_summary": m3_agg["repack_execution_summary"],
                "host_performance_summary": m3_agg["host_performance_summary"],
            },
            "aggregate_c1_vs_m3": {
                "all_6_runs": {
                    "verified_live_data_files": _agg_cmp(None, "verified_live_data_files", decimals=1),
                    "verified_live_equality_delete_files": _agg_cmp(None, "verified_live_equality_delete_files", decimals=1),
                    "final_marker_visibility_ms": _agg_cmp(None, "final_marker_visibility_ms"),
                    "final_marker_to_verified_match_ms": _agg_cmp(None, "final_marker_to_verified_match_ms"),
                    "diff_duration_ms": _agg_cmp(None, "diff_duration_ms"),
                    "probe_p50_ms": _agg_cmp(None, "probe_p50_ms"),
                    "probe_max_ms": _agg_cmp(None, "probe_max_ms"),
                    "full_exec_p50_ms": _agg_cmp(None, "full_exec_p50_ms"),
                    "full_s2c_p99_ms": _agg_cmp(None, "full_s2c_p99_ms"),
                },
                "baseline_runs": {
                    "int_exec_p50_ms": _agg_cmp("baseline", "int_exec_p50_ms"),
                    "int_s2c_p50_ms": _agg_cmp("baseline", "int_s2c_p50_ms"),
                    "full_exec_p50_ms": _agg_cmp("baseline", "full_exec_p50_ms"),
                    "probe_p50_ms": _agg_cmp("baseline", "probe_p50_ms"),
                    "diff_duration_ms": _agg_cmp("baseline", "diff_duration_ms"),
                    "final_marker_visibility_ms": _agg_cmp("baseline", "final_marker_visibility_ms"),
                    "final_marker_to_verified_match_ms": _agg_cmp("baseline", "final_marker_to_verified_match_ms"),
                },
                "repack_runs": {
                    "int_exec_p50_ms": _agg_cmp("repack", "int_exec_p50_ms"),
                    "int_s2c_p50_ms": _agg_cmp("repack", "int_s2c_p50_ms"),
                    "full_exec_p50_ms": _agg_cmp("repack", "full_exec_p50_ms"),
                    "probe_p50_ms": _agg_cmp("repack", "probe_p50_ms"),
                    "diff_duration_ms": _agg_cmp("repack", "diff_duration_ms"),
                    "final_marker_visibility_ms": _agg_cmp("repack", "final_marker_visibility_ms"),
                    "final_marker_to_verified_match_ms": _agg_cmp("repack", "final_marker_to_verified_match_ms"),
                },
            },
            "per_run_matched_by_seed": per_run_rows,
        }

    c1_agg["m3_comparison"] = m3_comparison
    return c1_agg, c1_series


def _fmt_mr(stat: dict[str, Any] | None, *, unit: str = "ms", decimals: int = 2) -> str:
    if not stat or stat.get("median") is None:
        return "n/a"
    fmt = f"{{:.{decimals}f}}"
    suffix = f" {unit}" if unit else ""
    return f"{fmt.format(stat['median'])}{suffix} [{fmt.format(stat['min'])}, {fmt.format(stat['max'])}]"


def _fmt_mr_signed(stat: dict[str, Any] | None, *, unit: str = "ms", decimals: int = 2) -> str:
    if not stat or stat.get("median") is None:
        return "n/a"
    fmt = f"{{:+.{decimals}f}}"
    suffix = f" {unit}" if unit else ""
    return f"{fmt.format(stat['median'])}{suffix} [{fmt.format(stat['min'])}, {fmt.format(stat['max'])}]"


def _fmt_int(val: Any) -> str:
    if val is None:
        return "n/a"
    return f"{int(val):,}"


def _fmt_ms(val: Any, decimals: int = 2) -> str:
    if val is None:
        return "n/a"
    return f"{float(val):.{decimals}f} ms"


def render_c1_results_markdown(agg: dict[str, Any]) -> str:
    """Render deterministic C1 results tables and C1-vs-M3 comparison Markdown from raw runs."""
    cs = agg["convergence_summary"]
    rx = agg["repack_execution_summary"]
    hp = agg["host_performance_summary"]
    cmp_sum = agg["compaction_summary"]
    pairs = agg["pairs"]
    m3_cmp = agg.get("m3_comparison")

    lines = [
        "# C1 Results: Concurrent REPACK Pairs with pg2iceberg Compaction Enabled",
        "",
        f"- Matched pairs: `{agg['pair_count']}` (orders: `{cs['orders']}`, counterbalanced: `{cs['counterbalanced_orders']}`)",
        f"- Compaction config: `compose/{cmp_sum['compaction_config_file']}`",
        f"- Outcomes: Baseline `{cs['baseline_converged_count']}/{cs['pair_count']}` converged, Concurrent REPACK `{cs['repack_converged_count']}/{cs['pair_count']}` converged (`all_valid_runs={cs['all_valid_runs']}`)",
        f"- Compaction ran inside measurement window in all runs: `{cmp_sum['all_runs_had_compaction_in_window']}`",
        f"- `pg2iceberg.log` free of conflicts, retries, errors and unexpected warnings in all runs: `{cmp_sum['all_pg2iceberg_logs_clean']}`",
        f"- Concurrent write overlap confirmed in all pairs: `{cs['all_overlaps_confirmed']}`",
        f"- Final source read opened strictly after REPACK finish in all pairs: `{cs['all_source_reads_after_repack']}`",
        f"- Project-only cleanup verified in all runs: `{cs['all_cleanups_ok']}`",
        f"- Container digests identical across all runs: `{agg['container_digests_identical_across_all_runs']}`",
        "",
        "## 1. Per-Run Convergence and Compaction Proof",
        "",
        "| Run | Order | Seed | Outcome | Verified Snapshot ID | Rows (Src = Lake) | Diffs | Compactions in Window (Merge / Del-Rewrite) | Verified Live Data / Eq-Del Files (Eq-Del Recs) | Final Vis | Commit-to-Match | Diff Time |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]

    for p in pairs:
        for side_label, side_key in (("Baseline", "baseline"), ("REPACK (CONC)", "repack")):
            m = p[side_key]
            c = m["compaction"]
            diffs = (
                f"{m['source_only_count']}/{m['lake_only_count']}/"
                f"{m['duplicate_lake_id_count']}/{m['value_mismatch_row_count']}"
            )
            comp_str = (
                f"{c['compaction_commits_in_window']} "
                f"({c['small_file_merge_commits_in_window']} / {c['delete_rewrite_commits_in_window']})"
            )
            vf = c["verified_snapshot_files"]
            files_str = (
                f"{vf['live_data_files']} / {vf['live_equality_delete_files']} "
                f"({_fmt_int(vf['live_equality_delete_records'])})"
            )
            lines.append(
                f"| P{p['pair_index']} {side_label} | `{p['order']}` | `{p['rng_seed']}` | "
                f"`{m['outcome']}` | `{m['verified_snapshot_id']}` | "
                f"`{_fmt_int(m['source_row_count'])}` = `{_fmt_int(m['lake_row_count'])}` | "
                f"`{diffs}` | `{comp_str}` | `{files_str}` | "
                f"`{_fmt_ms(m['final_marker_visibility_ms'])}` | "
                f"`{_fmt_ms(m['final_marker_to_verified_match_ms'])}` | "
                f"`{_fmt_ms(m['diff_duration_ms'])}` |"
            )

    lines.extend(
        [
            "",
            "## 2. Compaction Overlap and pg2iceberg Log Audit",
            "",
            "| Run | Total Compactions (<= Verified) | Data Files Replaced / Del Files Removed | Overlapped REPACK (Commit Inside / Span Overlap) | Final-Marker Overlap (Verified=Replace / Parent=Replace / Between Commit & Verified) | Log Audit (Err / Conflict / Retry / Unexp Warn) |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )

    for p in pairs:
        for side_label, side_key in (("Baseline", "baseline"), ("REPACK (CONC)", "repack")):
            c = p[side_key]["compaction"]
            ro = c["repack_overlap"]
            fo = c["final_marker_overlap"]
            la = c["pg2iceberg_log_audit"]
            repack_str = (
                f"{ro['commit_inside_repack_window']} ({ro['commit_inside_repack_count']}) / "
                f"{ro['background_span_overlaps_repack']} ({ro['background_span_overlap_count']})"
                if ro["applicable"]
                else "n/a"
            )
            fm_str = (
                f"{fo['verified_snapshot_is_compaction']} / "
                f"{fo['parent_of_verified_is_compaction']} / "
                f"{fo['compaction_between_final_marker_and_verified']}"
            )
            log_str = (
                f"{la['error_count']} / {la['conflict_count']} / "
                f"{la['retry_count']} / {la['unexpected_warning_count']}"
            )
            lines.append(
                f"| P{p['pair_index']} {side_label} | `{c['compaction_commits_up_to_verified']}` | "
                f"`{c['total_data_files_replaced_up_to_verified']} / {c['total_delete_files_removed_up_to_verified']}` | "
                f"`{repack_str}` | `{fm_str}` | `{log_str}` |"
            )

    lines.extend(
        [
            "",
            "## 3. Concurrent REPACK Execution Across C1 Pairs",
            "",
            "| Pair | Order | Seed | Duration | Committed Ops During REPACK | Heap/TOAST Swap | Total Size Delta | Peak CDC Confirmed-Flush WAL Gap | Blocked Waiters / Final Lock |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for p in pairs:
        r = p["repack_execution"]
        lines.append(
            f"| Pair {p['pair_index']} | `{p['order']}` | `{p['rng_seed']}` | "
            f"`{_fmt_ms(r['duration_ms'])}` | `{r['committed_during_repack_count']}` | "
            f"`{r['heap_relfilenode_changed']}/{r['toast_relfilenode_changed']}` | "
            f"`{_fmt_int(r['total_size_delta_bytes'])} B` | "
            f"`{_fmt_int(r['peak_cdc_confirmed_flush_wal_gap_bytes'])} B` | "
            f"`{r['waiting_locks_observed_count']}` / `{r['final_lock_observed']}` |"
        )
    lines.append(
        f"| **Median [min, max]** | `n={agg['pair_count']}` | `matched` | "
        f"`{_fmt_mr(rx['duration_ms'])}` | `{_fmt_mr(rx['committed_during_repack_count'], unit='ops', decimals=1)}` | "
        f"`{agg['pair_count']}/{agg['pair_count']} swapped` | "
        f"`{_fmt_mr(rx['total_size_delta_bytes'], unit='B', decimals=0)}` | "
        f"`{_fmt_mr(rx['peak_cdc_confirmed_flush_wal_gap_bytes'], unit='B', decimals=0)}` | "
        f"`{rx['waiting_locks_observed_total']}` total / `{rx['final_lock_observed_count']}/{agg['pair_count']}` |"
    )

    lines.extend(
        [
            "",
            "## 4. C1 Host Performance Summary (Median and Range Across Pairs)",
            "",
            "| Metric | Baseline Median [min, max] | Concurrent REPACK Median [min, max] | Paired Delta (REPACK - Baseline) Median [min, max] |",
            "| --- | --- | --- | --- |",
            f"| Matched interval: Exec p50 | `{_fmt_mr(hp['matched_interval']['exec_p50_ms']['baseline'])}` | `{_fmt_mr(hp['matched_interval']['exec_p50_ms']['repack'])}` | `{_fmt_mr_signed(hp['matched_interval']['exec_p50_ms']['paired_delta_repack_minus_baseline'])}` |",
            f"| Matched interval: Exec p99 | `{_fmt_mr(hp['matched_interval']['exec_p99_ms']['baseline'])}` | `{_fmt_mr(hp['matched_interval']['exec_p99_ms']['repack'])}` | `{_fmt_mr_signed(hp['matched_interval']['exec_p99_ms']['paired_delta_repack_minus_baseline'])}` |",
            f"| Matched interval: S2C p50 | `{_fmt_mr(hp['matched_interval']['sched_to_complete_p50_ms']['baseline'])}` | `{_fmt_mr(hp['matched_interval']['sched_to_complete_p50_ms']['repack'])}` | `{_fmt_mr_signed(hp['matched_interval']['sched_to_complete_p50_ms']['paired_delta_repack_minus_baseline'])}` |",
            f"| Matched interval: S2C p99 | `{_fmt_mr(hp['matched_interval']['sched_to_complete_p99_ms']['baseline'])}` | `{_fmt_mr(hp['matched_interval']['sched_to_complete_p99_ms']['repack'])}` | `{_fmt_mr_signed(hp['matched_interval']['sched_to_complete_p99_ms']['paired_delta_repack_minus_baseline'])}` |",
            f"| Matched interval: S2C max | `{_fmt_mr(hp['matched_interval']['sched_to_complete_max_ms']['baseline'])}` | `{_fmt_mr(hp['matched_interval']['sched_to_complete_max_ms']['repack'])}` | `{_fmt_mr_signed(hp['matched_interval']['sched_to_complete_max_ms']['paired_delta_repack_minus_baseline'])}` |",
            f"| Full window (180 s): Exec p50 | `{_fmt_mr(hp['full_window']['exec_p50_ms']['baseline'])}` | `{_fmt_mr(hp['full_window']['exec_p50_ms']['repack'])}` | `{_fmt_mr_signed(hp['full_window']['exec_p50_ms']['paired_delta_repack_minus_baseline'])}` |",
            f"| Full window (180 s): Exec p99 | `{_fmt_mr(hp['full_window']['exec_p99_ms']['baseline'])}` | `{_fmt_mr(hp['full_window']['exec_p99_ms']['repack'])}` | `{_fmt_mr_signed(hp['full_window']['exec_p99_ms']['paired_delta_repack_minus_baseline'])}` |",
            f"| Full window (180 s): S2C p50 | `{_fmt_mr(hp['full_window']['sched_to_complete_p50_ms']['baseline'])}` | `{_fmt_mr(hp['full_window']['sched_to_complete_p50_ms']['repack'])}` | `{_fmt_mr_signed(hp['full_window']['sched_to_complete_p50_ms']['paired_delta_repack_minus_baseline'])}` |",
            f"| Full window (180 s): S2C p99 | `{_fmt_mr(hp['full_window']['sched_to_complete_p99_ms']['baseline'])}` | `{_fmt_mr(hp['full_window']['sched_to_complete_p99_ms']['repack'])}` | `{_fmt_mr_signed(hp['full_window']['sched_to_complete_p99_ms']['paired_delta_repack_minus_baseline'])}` |",
            f"| Full window (180 s): S2C max | `{_fmt_mr(hp['full_window']['sched_to_complete_max_ms']['baseline'])}` | `{_fmt_mr(hp['full_window']['sched_to_complete_max_ms']['repack'])}` | `{_fmt_mr_signed(hp['full_window']['sched_to_complete_max_ms']['paired_delta_repack_minus_baseline'])}` |",
            f"| Steady-state probe lag: p50 | `{_fmt_mr(hp['full_window']['steady_state_probe_p50_ms']['baseline'])}` | `{_fmt_mr(hp['full_window']['steady_state_probe_p50_ms']['repack'])}` | `{_fmt_mr_signed(hp['full_window']['steady_state_probe_p50_ms']['paired_delta_repack_minus_baseline'])}` |",
            f"| Steady-state probe lag: max | `{_fmt_mr(hp['full_window']['steady_state_probe_max_ms']['baseline'])}` | `{_fmt_mr(hp['full_window']['steady_state_probe_max_ms']['repack'])}` | `{_fmt_mr_signed(hp['full_window']['steady_state_probe_max_ms']['paired_delta_repack_minus_baseline'])}` |",
            f"| Final marker commit-to-visibility | `{_fmt_mr(cs['final_marker_visibility_ms']['baseline'])}` | `{_fmt_mr(cs['final_marker_visibility_ms']['repack'])}` | `{_fmt_mr_signed(cs['final_marker_visibility_ms']['paired_delta_repack_minus_baseline'])}` |",
            f"| Final marker commit-to-match | `{_fmt_mr(cs['final_marker_to_verified_match_ms']['baseline'])}` | `{_fmt_mr(cs['final_marker_to_verified_match_ms']['repack'])}` | `{_fmt_mr_signed(cs['final_marker_to_verified_match_ms']['paired_delta_repack_minus_baseline'])}` |",
            f"| Full diff duration | `{_fmt_mr(cs['diff_duration_ms']['baseline'])}` | `{_fmt_mr(cs['diff_duration_ms']['repack'])}` | `{_fmt_mr_signed(cs['diff_duration_ms']['paired_delta_repack_minus_baseline'])}` |",
            "",
            "## 5. Per-Run C1 Host Performance and Pipeline Positions",
            "",
            "| Run | Attempted/Comp | Err/TO | Missed Sched/Harness | Matched Int Exec p50/p99 | Matched Int S2C p50/p99/max | Full Win S2C p50/p99/max | Probe Lag p50/max | Final WAL Gap (Flush / Restart) | Final `mat_cursor` / `log_seq` |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for p in pairs:
        for side_label, side_key in (("Baseline", "baseline"), ("REPACK (CONC)", "repack")):
            m = p[side_key]
            ppf = (m.get("pipeline_positions_compact") or {}).get("final") or {}
            lines.append(
                f"| P{p['pair_index']} {side_label} | `{m['attempted_ops']}/{m['completed_ops']}` | "
                f"`{m['error_count']}/{m['timeout_count']}` | "
                f"`{m['missed_schedule_count']}/{m['harness_missed_arrival_count']}` | "
                f"`{m['int_exec_p50_ms']} / {m['int_exec_p99_ms']} ms` | "
                f"`{m['int_s2c_p50_ms']} / {m['int_s2c_p99_ms']} / {m['int_s2c_max_ms']} ms` | "
                f"`{m['full_s2c_p50_ms']} / {m['full_s2c_p99_ms']} / {m['full_s2c_max_ms']} ms` | "
                f"`{m['probe_p50_ms']} / {m['probe_max_ms']} ms` | "
                f"`{_fmt_int(ppf.get('wal_gap_bytes_confirmed_flush'))} B / {_fmt_int(ppf.get('wal_gap_bytes_restart'))} B` | "
                f"`{ppf.get('mat_cursor')} / {ppf.get('log_seq')}` |"
            )

    if m3_cmp is not None:
        all6 = m3_cmp["aggregate_c1_vs_m3"]["all_6_runs"]
        b_cmp = m3_cmp["aggregate_c1_vs_m3"]["baseline_runs"]
        r_cmp = m3_cmp["aggregate_c1_vs_m3"]["repack_runs"]
        lines.extend(
            [
                "",
                "## 6. Side-by-Side Comparison: C1 (Compaction On) vs M3 (Compaction Off, Same Seeds)",
                "",
                f"- All seeds matched (`20261119`, `20261120`, `20261121`): `{m3_cmp['all_seeds_matched']}`",
                "",
                "### Aggregate across all 6 matched runs (n=6) and by role (n=3)",
                "",
                "| Metric | M3 (Compaction Off) Median [min, max] | C1 (Compaction On) Median [min, max] | Paired Delta (C1 - M3) Median [min, max] |",
                "| --- | --- | --- | --- |",
                f"| Verified live data files (all 6) | `{_fmt_mr(all6['verified_live_data_files']['m3'], unit='files', decimals=1)}` | `{_fmt_mr(all6['verified_live_data_files']['c1'], unit='files', decimals=1)}` | `{_fmt_mr_signed(all6['verified_live_data_files']['delta_c1_minus_m3'], unit='files', decimals=1)}` |",
                f"| Verified live eq-delete files (all 6) | `{_fmt_mr(all6['verified_live_equality_delete_files']['m3'], unit='files', decimals=1)}` | `{_fmt_mr(all6['verified_live_equality_delete_files']['c1'], unit='files', decimals=1)}` | `{_fmt_mr_signed(all6['verified_live_equality_delete_files']['delta_c1_minus_m3'], unit='files', decimals=1)}` |",
                f"| Full diff duration (all 6) | `{_fmt_mr(all6['diff_duration_ms']['m3'])}` | `{_fmt_mr(all6['diff_duration_ms']['c1'])}` | `{_fmt_mr_signed(all6['diff_duration_ms']['delta_c1_minus_m3'])}` |",
                f"| Final marker visibility (all 6) | `{_fmt_mr(all6['final_marker_visibility_ms']['m3'])}` | `{_fmt_mr(all6['final_marker_visibility_ms']['c1'])}` | `{_fmt_mr_signed(all6['final_marker_visibility_ms']['delta_c1_minus_m3'])}` |",
                f"| Final marker commit-to-match (all 6) | `{_fmt_mr(all6['final_marker_to_verified_match_ms']['m3'])}` | `{_fmt_mr(all6['final_marker_to_verified_match_ms']['c1'])}` | `{_fmt_mr_signed(all6['final_marker_to_verified_match_ms']['delta_c1_minus_m3'])}` |",
                f"| Steady-state probe lag p50 (all 6) | `{_fmt_mr(all6['probe_p50_ms']['m3'])}` | `{_fmt_mr(all6['probe_p50_ms']['c1'])}` | `{_fmt_mr_signed(all6['probe_p50_ms']['delta_c1_minus_m3'])}` |",
                f"| Steady-state probe lag max (all 6) | `{_fmt_mr(all6['probe_max_ms']['m3'])}` | `{_fmt_mr(all6['probe_max_ms']['c1'])}` | `{_fmt_mr_signed(all6['probe_max_ms']['delta_c1_minus_m3'])}` |",
                f"| Full window Exec p50 (all 6) | `{_fmt_mr(all6['full_exec_p50_ms']['m3'])}` | `{_fmt_mr(all6['full_exec_p50_ms']['c1'])}` | `{_fmt_mr_signed(all6['full_exec_p50_ms']['delta_c1_minus_m3'])}` |",
                f"| Matched interval Exec p50 (Baseline, n=3) | `{_fmt_mr(b_cmp['int_exec_p50_ms']['m3'])}` | `{_fmt_mr(b_cmp['int_exec_p50_ms']['c1'])}` | `{_fmt_mr_signed(b_cmp['int_exec_p50_ms']['delta_c1_minus_m3'])}` |",
                f"| Matched interval Exec p50 (REPACK, n=3) | `{_fmt_mr(r_cmp['int_exec_p50_ms']['m3'])}` | `{_fmt_mr(r_cmp['int_exec_p50_ms']['c1'])}` | `{_fmt_mr_signed(r_cmp['int_exec_p50_ms']['delta_c1_minus_m3'])}` |",
                "",
                "### Per-run side-by-side (matched on seed and role)",
                "",
                "| Run (Seed) | Outcome (M3 / C1) | Live Data/Del Files (M3 -> C1) | Final Vis (M3 -> C1) | Commit-to-Match (M3 -> C1) | Diff Time (M3 -> C1) | Probe p50 (M3 -> C1) | Matched Int Exec p50 (M3 -> C1) | Full Win Exec p50 (M3 -> C1) |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in m3_cmp["per_run_matched_by_seed"]:
            role = "Baseline" if row["side"] == "baseline" else "REPACK"
            m3r = row["m3"]
            c1r = row["c1"]
            lines.append(
                f"| P{row['pair_index']} {role} (`{row['rng_seed']}`) | "
                f"`{m3r['outcome']}` / `{c1r['outcome']}` | "
                f"`{m3r['verified_live_data_files']}/{m3r['verified_live_equality_delete_files']}` -> `{c1r['verified_live_data_files']}/{c1r['verified_live_equality_delete_files']}` | "
                f"`{_fmt_ms(m3r['final_marker_visibility_ms'])}` -> `{_fmt_ms(c1r['final_marker_visibility_ms'])}` | "
                f"`{_fmt_ms(m3r['final_marker_to_verified_match_ms'])}` -> `{_fmt_ms(c1r['final_marker_to_verified_match_ms'])}` | "
                f"`{_fmt_ms(m3r['diff_duration_ms'])}` -> `{_fmt_ms(c1r['diff_duration_ms'])}` | "
                f"`{_fmt_ms(m3r['probe_p50_ms'])}` -> `{_fmt_ms(c1r['probe_p50_ms'])}` | "
                f"`{_fmt_ms(m3r['int_exec_p50_ms'])}` -> `{_fmt_ms(c1r['int_exec_p50_ms'])}` | "
                f"`{_fmt_ms(m3r['full_exec_p50_ms'])}` -> `{_fmt_ms(c1r['full_exec_p50_ms'])}` |"
            )

    lines.append("")
    return "\n".join(lines)


def render_c1_vs_m3_svg(agg: dict[str, Any]) -> str:
    """Render deterministic side-by-side SVG comparing C1 (compaction on) vs M3 (compaction off)."""
    width = 960
    height = 460
    m3_cmp = agg.get("m3_comparison")
    lines = _svg_open(width, height, "C1 (Compaction On) vs M3 (Compaction Off) Comparison")
    lines.append(
        f'<text x="24" y="30" font-size="18" font-weight="700" fill="{TEXT_COLOR}">'
        "C1 (Compaction On) vs M3 (Compaction Off) on Matched Seeds</text>"
    )
    lines.append(
        f'<text x="24" y="49" font-size="12" fill="{MUTED_COLOR}">'
        "Bars show median across 6 matched runs (3 baseline + 3 concurrent REPACK); "
        "circles show individual runs (this host only).</text>"
    )
    m3_color = "#64748b"
    c1_color = "#059669"
    lx = 620
    lines.append(f'<rect x="{lx}" y="18" width="12" height="12" rx="2" fill="{m3_color}" />')
    lines.append(f'<text x="{lx + 18}" y="28" font-size="12" fill="{TEXT_COLOR}">M3 (Compaction Off)</text>')
    lines.append(f'<rect x="{lx + 165}" y="18" width="12" height="12" rx="2" fill="{c1_color}" />')
    lines.append(f'<text x="{lx + 183}" y="28" font-size="12" fill="{TEXT_COLOR}">C1 (Compaction On)</text>')

    if not m3_cmp:
        lines.append(f'<text x="24" y="100" font-size="14" fill="{MUTED_COLOR}">No M3 manifest supplied.</text>')
        lines.append("</svg>\n")
        return "\n".join(lines)

    all6 = m3_cmp["aggregate_c1_vs_m3"]["all_6_runs"]
    panels = [
        (
            "Reader and Verification Timings (seconds, n=6 runs)",
            [
                ("Probe p50", all6["probe_p50_ms"], 1000.0),
                ("Final Vis", all6["final_marker_visibility_ms"], 1000.0),
                ("Diff Time", all6["diff_duration_ms"], 1000.0),
                ("Commit->Match", all6["final_marker_to_verified_match_ms"], 1000.0),
            ],
            24,
            70,
            560,
            360,
            "s",
        ),
        (
            "Verified Snapshot Live Files (count, n=6 runs)",
            [
                ("Data Files", all6["verified_live_data_files"], 1.0),
                ("Eq-Del Files", all6["verified_live_equality_delete_files"], 1.0),
            ],
            604,
            70,
            332,
            360,
            "",
        ),
    ]

    for title, metrics, px, py, pw, ph, unit in panels:
        lines.append(
            f'<rect x="{px}" y="{py}" width="{pw}" height="{ph}" rx="6" '
            f'fill="{PANEL_BG}" stroke="{BORDER_COLOR}" />'
        )
        lines.append(
            f'<text x="{px + 14}" y="{py + 22}" font-size="13" font-weight="600" fill="{TEXT_COLOR}">'
            f"{escape(title)}</text>"
        )
        all_vals: list[float] = []
        for _, m, div in metrics:
            all_vals.extend(float(v) / div for v in (m["m3"].get("values") or []))
            all_vals.extend(float(v) / div for v in (m["c1"].get("values") or []))
        y_max = _nice_upper(max(all_vals) if all_vals else 10.0, min_upper=5.0)
        plot_x = px + 50
        plot_y = py + 40
        plot_w = pw - 68
        plot_h = ph - 82

        for tick_i in range(5):
            frac = tick_i / 4.0
            val = y_max * frac
            gy = plot_y + plot_h * (1.0 - frac)
            lines.append(
                f'<line x1="{plot_x}" y1="{gy:.1f}" x2="{plot_x + plot_w}" y2="{gy:.1f}" stroke="{GRID_COLOR}" />'
            )
            lbl = f"{val:.0f}{unit}" if y_max >= 10 else f"{val:.1f}{unit}"
            lines.append(
                f'<text x="{plot_x - 8}" y="{gy + 4:.1f}" text-anchor="end" font-size="10" fill="{MUTED_COLOR}">'
                f"{escape(lbl)}</text>"
            )

        slot_w = plot_w / max(len(metrics), 1)
        bar_w = min(34.0, slot_w * 0.28)
        for idx, (label, m, div) in enumerate(metrics):
            cx = plot_x + (idx + 0.5) * slot_w
            for offset_mult, key, color in ((-0.55, "m3", m3_color), (0.55, "c1", c1_color)):
                stat = m[key]
                med = (float(stat["median"]) / div) if stat.get("median") is not None else 0.0
                bx = cx + offset_mult * bar_w - bar_w / 2.0
                bh = (med / y_max) * plot_h if y_max > 0 else 0.0
                by = plot_y + plot_h - bh
                lines.append(
                    f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_w:.1f}" height="{bh:.1f}" '
                    f'rx="3" fill="{color}" fill-opacity="0.85" />'
                )
                for v in stat.get("values") or []:
                    vy = plot_y + plot_h - ((float(v) / div) / y_max) * plot_h
                    lines.append(
                        f'<circle cx="{bx + bar_w / 2.0:.1f}" cy="{vy:.1f}" r="3" '
                        f'fill="#ffffff" stroke="{color}" stroke-width="1.6" />'
                    )
            lines.append(
                f'<text x="{cx:.1f}" y="{plot_y + plot_h + 18}" text-anchor="middle" '
                f'font-size="11" fill="{TEXT_COLOR}">{escape(label)}</text>'
            )

    lines.append("</svg>\n")
    return "\n".join(lines)


def render_c1_from_manifest(
    c1_manifest_path: Path,
    *,
    output_dir: Path,
    m3_manifest_path: Path | None = DEFAULT_M3_MANIFEST,
    repo_root: Path = REPO_ROOT,
) -> dict[str, Any]:
    """Regenerate c1_summary.json, c1_results.md, and SVG charts deterministically from raw runs."""
    c1_manifest = json.loads(c1_manifest_path.read_text(encoding="utf-8"))
    m3_manifest: dict[str, Any] | None = None
    if m3_manifest_path is not None and m3_manifest_path.exists():
        m3_manifest = json.loads(m3_manifest_path.read_text(encoding="utf-8"))

    agg, c1_series = aggregate_c1_and_compare_m3(
        c1_manifest,
        m3_manifest=m3_manifest,
        repo_root=repo_root,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / "c1_summary.json", agg)
    (output_dir / "c1_results.md").write_text(render_c1_results_markdown(agg), encoding="utf-8")
    (output_dir / "c1_vs_m3_comparison.svg").write_text(render_c1_vs_m3_svg(agg), encoding="utf-8")
    (output_dir / "latency_comparison.svg").write_text(
        render_latency_comparison_svg(agg), encoding="utf-8"
    )
    (output_dir / "repack_window_timeseries.svg").write_text(
        render_repack_window_timeseries_svg(c1_series, control_series=None), encoding="utf-8"
    )
    (output_dir / "reader_lag.svg").write_text(render_reader_lag_svg(agg), encoding="utf-8")
    return agg


def run_c1_repeated_pairs(
    *,
    n_pairs: int = 3,
    compaction_config: str = DEFAULT_COMPACTION_CONFIG,
    seed_rows: int = 250000,
    toast_rows: int = 25000,
    duration_s: float = 180.0,
    rate_ops_s: float = 10.0,
    base_rng_seed: int = 20261119,
    batch_size: int = 2000,
    probe_interval_s: float = 20.0,
    probe_poll_interval_s: float = 0.5,
    repack_delay_s: float = 30.0,
    repack_sample_interval_s: float = 0.05,
    snapshot_timeout_s: float = 300.0,
    convergence_timeout_s: float = 300.0,
    m3_manifest_path: Path | None = DEFAULT_M3_MANIFEST,
    label: str = "c1",
) -> tuple[Path, dict[str, Any]]:
    """Run N fresh counterbalanced C1 matched pairs (skipping plain REPACK) and render comparison."""
    orders = default_orders_for_count(n_pairs)
    repeat_dir = REPO_ROOT / "runs" / f"{utc_run_stamp()}-{label}-repeat"
    repeat_dir.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, Any] = {
        "milestone": "C1",
        "label": label,
        "compaction_config": compaction_config,
        "n_pairs": n_pairs,
        "orders": orders,
        "base_rng_seed": base_rng_seed,
        "distinct_pair_seeds": True,
        "with_plain_repack_control": False,
        "m3_reference_manifest": (
            _rel_str(m3_manifest_path, REPO_ROOT)
            if m3_manifest_path and m3_manifest_path.exists()
            else None
        ),
        "parameters": {
            "seed_rows": seed_rows,
            "toast_rows": toast_rows,
            "duration_s": duration_s,
            "rate_ops_s": rate_ops_s,
            "batch_size": batch_size,
            "probe_interval_s": probe_interval_s,
            "probe_poll_interval_s": probe_poll_interval_s,
            "repack_delay_s": repack_delay_s,
            "repack_sample_interval_s": repack_sample_interval_s,
        },
        "pairs": [],
        "plain_repack_control": None,
    }
    manifest_path = repeat_dir / "manifest.json"
    write_json(manifest_path, manifest)

    for idx, order in enumerate(orders, start=1):
        pair_seed = base_rng_seed + (idx - 1)
        print(
            f"=== C1 Pair {idx}/{n_pairs}: order={order}, rng_seed={pair_seed}, config={compaction_config} ===",
            flush=True,
        )
        pair_dir, pair = run_c1_matched_pair(
            compaction_config=compaction_config,
            seed_rows=seed_rows,
            toast_rows=toast_rows,
            duration_s=duration_s,
            rate_ops_s=rate_ops_s,
            rng_seed=pair_seed,
            batch_size=batch_size,
            probe_interval_s=probe_interval_s,
            probe_poll_interval_s=probe_poll_interval_s,
            repack_delay_s=repack_delay_s,
            repack_sample_interval_s=repack_sample_interval_s,
            snapshot_timeout_s=snapshot_timeout_s,
            convergence_timeout_s=convergence_timeout_s,
            order=order,
            label=f"{label}-p{idx}",
        )
        manifest["pairs"].append(
            {
                "pair_index": idx,
                "order": order,
                "rng_seed": pair_seed,
                "pair_dir": _rel_str(pair_dir, REPO_ROOT),
                "baseline_run_dir": pair["baseline_run_dir"],
                "repack_run_dir": pair["repack_run_dir"],
            }
        )
        write_json(manifest_path, manifest)

    agg = render_c1_from_manifest(
        manifest_path,
        output_dir=repeat_dir,
        m3_manifest_path=m3_manifest_path,
        repo_root=REPO_ROOT,
    )
    compact = {
        "repeat_dir": _rel_str(repeat_dir, REPO_ROOT),
        "pair_count": agg["pair_count"],
        "all_valid_runs": agg["convergence_summary"]["all_valid_runs"],
        "all_converged": agg["convergence_summary"]["all_converged"],
        "all_compaction_ran_in_window": agg["compaction_summary"]["all_runs_had_compaction_in_window"],
        "all_pg2iceberg_logs_clean": agg["compaction_summary"]["all_pg2iceberg_logs_clean"],
        "all_overlaps_confirmed": agg["convergence_summary"]["all_overlaps_confirmed"],
    }
    print(json.dumps(compact, indent=2, default=json_default), flush=True)
    return repeat_dir, agg


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run C1 counterbalanced REPACK pairs with pg2iceberg compaction enabled, "
            "or regenerate C1 tables and M3 side-by-side comparison from raw runs."
        )
    )
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--compaction-config", default=DEFAULT_COMPACTION_CONFIG)
    parser.add_argument("--seed-rows", type=int, default=250000)
    parser.add_argument("--toast-rows", type=int, default=25000)
    parser.add_argument("--duration-s", type=float, default=180.0)
    parser.add_argument("--rate-ops-s", type=float, default=10.0)
    parser.add_argument("--base-rng-seed", type=int, default=20261119)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--probe-interval-s", type=float, default=20.0)
    parser.add_argument("--probe-poll-interval-s", type=float, default=0.5)
    parser.add_argument("--repack-delay-s", type=float, default=30.0)
    parser.add_argument("--repack-sample-interval-s", type=float, default=0.05)
    parser.add_argument("--snapshot-timeout-s", type=float, default=300.0)
    parser.add_argument("--convergence-timeout-s", type=float, default=300.0)
    parser.add_argument("--label", default="c1")
    parser.add_argument(
        "--m3-manifest",
        type=Path,
        default=DEFAULT_M3_MANIFEST,
        help="Path to M3 manifest.json for side-by-side comparison.",
    )
    parser.add_argument(
        "--render-from-manifest",
        type=Path,
        default=None,
        help="Regenerate c1_summary.json, c1_results.md, and SVG charts from C1 manifest.json.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory when using --render-from-manifest (defaults to manifest parent dir).",
    )
    args = parser.parse_args()

    if args.render_from_manifest is not None:
        out_dir = args.output_dir or args.render_from_manifest.parent
        agg = render_c1_from_manifest(
            args.render_from_manifest,
            output_dir=out_dir,
            m3_manifest_path=args.m3_manifest,
        )
        print(
            json.dumps(
                {
                    "output_dir": str(out_dir),
                    "pair_count": agg["pair_count"],
                    "all_valid_runs": agg["convergence_summary"]["all_valid_runs"],
                    "all_compaction_ran_in_window": agg["compaction_summary"][
                        "all_runs_had_compaction_in_window"
                    ],
                },
                indent=2,
            )
        )
        return

    run_c1_repeated_pairs(
        n_pairs=args.pairs,
        compaction_config=args.compaction_config,
        seed_rows=args.seed_rows,
        toast_rows=args.toast_rows,
        duration_s=args.duration_s,
        rate_ops_s=args.rate_ops_s,
        base_rng_seed=args.base_rng_seed,
        batch_size=args.batch_size,
        probe_interval_s=args.probe_interval_s,
        probe_poll_interval_s=args.probe_poll_interval_s,
        repack_delay_s=args.repack_delay_s,
        repack_sample_interval_s=args.repack_sample_interval_s,
        snapshot_timeout_s=args.snapshot_timeout_s,
        convergence_timeout_s=args.convergence_timeout_s,
        m3_manifest_path=args.m3_manifest,
        label=args.label,
    )


if __name__ == "__main__":
    main()
