"""Milestone L3 end-to-end runner: Lakekeeper catalog outage drill during M1 baseline workload."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import json
from pathlib import Path
import platform
import subprocess
import sys
import threading
import time
from typing import Any
import urllib.error
import urllib.request

import psycopg

from lab.bootstrap import bootstrap_all
from lab.config import LabConfig, REPO_ROOT, load_config
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
    commit_marker_row,
    run_paced_workload,
    seed_orders,
    verify_toast_storage,
)


def inspect_service_container(
    cfg: LabConfig,
    service: str,
    *,
    profiles: Sequence[str] = ("connector",),
) -> dict[str, Any]:
    """Inspect a single Compose service container's state, restart count, and health."""
    proc = run_cmd(
        [*compose_cmd_with_profiles(cfg, profiles), "ps", "-a", "-q", service],
        check=False,
    )
    cid = proc.stdout.strip().splitlines()[0].strip() if proc.stdout.strip() else ""
    if not cid:
        return {"service": service, "found": False}
    raw = run_cmd(["docker", "inspect", cid], check=False).stdout
    if not raw.strip():
        return {"service": service, "found": False}
    info = json.loads(raw)[0]
    state = info.get("State") or {}
    health = state.get("Health") or {}
    return {
        "service": service,
        "found": True,
        "container_id": cid[:12],
        "status": state.get("Status"),
        "running": bool(state.get("Running")),
        "restart_count": int(info.get("RestartCount") or 0),
        "exit_code": state.get("ExitCode"),
        "oom_killed": bool(state.get("OOMKilled")),
        "started_at": state.get("StartedAt"),
        "finished_at": state.get("FinishedAt"),
        "health_status": health.get("Status"),
    }


def probe_http_status(url: str, timeout: float = 2.0) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace").strip()
            return {"status": resp.status, "body": body[:200], "error": None}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace").strip()
        return {"status": exc.code, "body": body[:200], "error": str(exc)}
    except Exception as exc:
        return {"status": None, "body": None, "error": f"{type(exc).__name__}: {exc}"}


def measure_silo_bucket_usage(cfg: LabConfig) -> dict[str, Any]:
    """List objects in the synthetic Silo bucket and break down counts/bytes by top-level category."""
    shell_cmd = (
        f"mc alias set local http://127.0.0.1:{cfg.silo_port} "
        f"{cfg.silo_root_user} {cfg.silo_root_password} >/dev/null 2>&1 && "
        f"mc ls --recursive --json local/{cfg.silo_bucket}"
    )
    proc = run_cmd(
        [*compose_cmd_with_profiles(cfg), "exec", "-T", "silo", "sh", "-c", shell_cmd],
        check=False,
    )
    if proc.returncode != 0:
        return {"ok": False, "error": proc.stderr.strip() or proc.stdout.strip()}

    total_objects = 0
    total_bytes = 0
    by_category: dict[str, dict[str, int]] = {}
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if entry.get("type") != "file":
            continue
        key = str(entry.get("key") or "")
        size = int(entry.get("size") or 0)
        total_objects += 1
        total_bytes += size
        if "/metadata/" in key or key.startswith("metadata/"):
            cat = "iceberg_metadata"
        elif "/data/" in key:
            cat = "iceberg_data_and_deletes"
        elif "log" in key or "stage" in key or "_pg2iceberg" in key:
            cat = "pg2iceberg_staging"
        else:
            cat = "other"
        bucket_cat = by_category.setdefault(cat, {"objects": 0, "bytes": 0})
        bucket_cat["objects"] += 1
        bucket_cat["bytes"] += size

    return {
        "ok": True,
        "captured_at_utc": utc_now_iso(),
        "total_objects": total_objects,
        "total_bytes": total_bytes,
        "by_category": by_category,
    }


def extract_coordinator_offsets(pg_pos: dict[str, Any]) -> dict[str, Any]:
    """Extract staged log_seq, applied mat_cursor, pending backlog, and LSNs from pipeline positions."""
    coord = pg_pos.get("coordinator_state") or {}
    staged_next_offset: int | None = None
    for row in coord.get("log_seq") or []:
        if isinstance(row, dict) and row.get("table_name") == "public.orders":
            val = row.get("next_offset")
            if val is not None:
                staged_next_offset = int(val)
            break

    applied_last_offset: int | None = None
    for row in coord.get("mat_cursor") or []:
        if isinstance(row, dict) and row.get("group_name") == "default" and row.get("table_name") == "public.orders":
            val = row.get("last_offset")
            if val is not None:
                applied_last_offset = int(val)
            break

    replicated_lsn: int | None = None
    for row in coord.get("replicated_lsn") or []:
        if isinstance(row, dict) and row.get("lsn") is not None:
            replicated_lsn = int(row["lsn"])
            break

    flushed_lsn: int | None = None
    for row in coord.get("flushed_lsn") or []:
        if isinstance(row, dict) and row.get("lsn") is not None:
            flushed_lsn = int(row["lsn"])
            break

    pending_offsets: int | None = None
    if staged_next_offset is not None and applied_last_offset is not None:
        pending_offsets = max(0, staged_next_offset - applied_last_offset)

    cdc_slot: dict[str, Any] | None = None
    for slot in pg_pos.get("replication_slots") or []:
        if slot.get("slot_name") == "pg2iceberg_slot":
            cdc_slot = slot
            break

    return {
        "current_wal_lsn": pg_pos.get("current_wal_lsn"),
        "slot_active": cdc_slot.get("active") if cdc_slot else None,
        "slot_restart_lsn": cdc_slot.get("restart_lsn") if cdc_slot else None,
        "slot_confirmed_flush_lsn": cdc_slot.get("confirmed_flush_lsn") if cdc_slot else None,
        "wal_gap_bytes_confirmed_flush": (
            cdc_slot.get("wal_gap_bytes_confirmed_flush") if cdc_slot else None
        ),
        "wal_gap_bytes_restart": cdc_slot.get("wal_gap_bytes_restart") if cdc_slot else None,
        "replicated_lsn": replicated_lsn,
        "flushed_lsn": flushed_lsn,
        "staged_log_seq": staged_next_offset,
        "applied_mat_cursor": applied_last_offset,
        "pending_unapplied_offsets": pending_offsets,
        "log_index_row_count": coord.get("log_index_row_count"),
    }


def summarize_pg2iceberg_logs(raw_logs: str, *, max_examples: int = 20) -> dict[str, Any]:
    """Parse JSON and plain-text fatal error lines from pg2iceberg container logs."""
    level_counts: dict[str, int] = {}
    error_warn_groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    error_examples: list[dict[str, Any]] = []
    warn_examples: list[dict[str, Any]] = []
    non_json_lines = 0
    non_json_text_lines: list[str] = []
    parsed_lines = 0

    for raw_line in raw_logs.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        json_start = line.find("{")
        if json_start == -1:
            non_json_lines += 1
            if len(non_json_text_lines) < max_examples:
                non_json_text_lines.append(line)
            continue
        try:
            obj = json.loads(line[json_start:])
        except json.JSONDecodeError:
            non_json_lines += 1
            if len(non_json_text_lines) < max_examples:
                non_json_text_lines.append(line)
            continue
        if not isinstance(obj, dict):
            non_json_lines += 1
            if len(non_json_text_lines) < max_examples:
                non_json_text_lines.append(line)
            continue
        parsed_lines += 1
        level = str(obj.get("level", "UNKNOWN")).upper()
        level_counts[level] = level_counts.get(level, 0) + 1
        if level in ("WARN", "ERROR"):
            target = str(obj.get("target") or "")
            msg = str(obj.get("message") or "")
            ts = str(obj.get("timestamp") or "")
            key = (level, target, msg)
            grp = error_warn_groups.get(key)
            if grp is None:
                error_warn_groups[key] = {
                    "level": level,
                    "target": target,
                    "message": msg,
                    "count": 1,
                    "first_timestamp": ts,
                    "last_timestamp": ts,
                }
            else:
                grp["count"] += 1
                grp["last_timestamp"] = ts
            if level == "ERROR" and len(error_examples) < max_examples:
                error_examples.append(obj)
            elif level == "WARN" and len(warn_examples) < max_examples:
                warn_examples.append(obj)

    fatal_text_errors = [
        ln for ln in non_json_text_lines if ("Error:" in ln or "Caused by:" in ln or "materializer.cycle" in ln)
    ]
    grouped_list = sorted(
        error_warn_groups.values(),
        key=lambda g: (-int(g["count"]), g["level"], g["target"], g["message"]),
    )
    return {
        "parsed_json_lines": parsed_lines,
        "non_json_lines": non_json_lines,
        "non_json_text_lines": non_json_text_lines,
        "fatal_text_error_count": len(fatal_text_errors),
        "fatal_text_errors": fatal_text_errors,
        "level_counts": level_counts,
        "error_count": level_counts.get("ERROR", 0),
        "warn_count": level_counts.get("WARN", 0),
        "error_warn_groups": grouped_list,
        "error_examples": error_examples,
        "warn_examples": warn_examples,
    }


def summarize_outage_samples(samples: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Summarize WAL retained, staging vs applied progress, and container restarts during the outage."""
    outage_samples = [s for s in samples if s.get("phase") == "outage"]
    post_samples = [s for s in samples if s.get("phase") == "post_restart"]

    def _int_vals(rows: Sequence[dict[str, Any]], key: str) -> list[int]:
        out: list[int] = []
        for r in rows:
            v = r.get(key)
            if isinstance(v, int) and not isinstance(v, bool):
                out.append(v)
        return out

    flush_gaps = _int_vals(outage_samples, "wal_gap_bytes_confirmed_flush")
    restart_gaps = _int_vals(outage_samples, "wal_gap_bytes_restart")
    staged_vals = _int_vals(outage_samples, "staged_log_seq")
    applied_vals = _int_vals(outage_samples, "applied_mat_cursor")
    pending_vals = _int_vals(outage_samples, "pending_unapplied_offsets")
    log_idx_vals = _int_vals(outage_samples, "log_index_row_count")

    pg2_restarts = [
        int((s.get("pg2iceberg_container") or {}).get("restart_count") or 0)
        for s in samples
        if isinstance(s.get("pg2iceberg_container"), dict) and s["pg2iceberg_container"].get("found")
    ]
    pg2_all_running = all(
        bool((s.get("pg2iceberg_container") or {}).get("running"))
        for s in samples
        if isinstance(s.get("pg2iceberg_container"), dict) and s["pg2iceberg_container"].get("found")
    )

    staging_advanced_during_outage = (
        len(staged_vals) >= 2 and staged_vals[-1] > staged_vals[0]
    )
    applied_frozen_during_outage = (
        len(applied_vals) >= 2 and min(applied_vals) == max(applied_vals)
    )

    outage_end_staged = staged_vals[-1] if staged_vals else None
    backlog_drained_sample: dict[str, Any] | None = None
    if outage_end_staged is not None:
        for s in post_samples:
            app = s.get("applied_mat_cursor")
            if isinstance(app, int) and app >= outage_end_staged:
                backlog_drained_sample = s
                break

    return {
        "total_samples": len(samples),
        "outage_sample_count": len(outage_samples),
        "post_restart_sample_count": len(post_samples),
        "staging_advanced_during_outage": staging_advanced_during_outage,
        "applied_frozen_during_outage": applied_frozen_during_outage,
        "outage_staged_log_seq_start": staged_vals[0] if staged_vals else None,
        "outage_staged_log_seq_end": staged_vals[-1] if staged_vals else None,
        "outage_staged_delta_offsets": (
            (staged_vals[-1] - staged_vals[0]) if len(staged_vals) >= 2 else None
        ),
        "outage_applied_mat_cursor_min": min(applied_vals) if applied_vals else None,
        "outage_applied_mat_cursor_max": max(applied_vals) if applied_vals else None,
        "outage_max_pending_unapplied_offsets": max(pending_vals) if pending_vals else None,
        "outage_log_index_row_count_start": log_idx_vals[0] if log_idx_vals else None,
        "outage_log_index_row_count_max": max(log_idx_vals) if log_idx_vals else None,
        "outage_wal_gap_confirmed_flush_min_bytes": min(flush_gaps) if flush_gaps else None,
        "outage_wal_gap_confirmed_flush_max_bytes": max(flush_gaps) if flush_gaps else None,
        "outage_wal_gap_restart_min_bytes": min(restart_gaps) if restart_gaps else None,
        "outage_wal_gap_restart_max_bytes": max(restart_gaps) if restart_gaps else None,
        "pg2iceberg_all_samples_running": pg2_all_running,
        "pg2iceberg_max_restart_count": max(pg2_restarts) if pg2_restarts else 0,
        "backlog_drained_observed_utc": (
            backlog_drained_sample.get("captured_at_utc") if backlog_drained_sample else None
        ),
        "backlog_drained_offset_ms": (
            backlog_drained_sample.get("offset_ms") if backlog_drained_sample else None
        ),
    }


def wait_for_lakekeeper_healthy(cfg: LabConfig, *, timeout_s: float = 90.0) -> dict[str, Any]:
    """Poll Lakekeeper /management/v1/info until HTTP 200 returns."""
    t0 = time.monotonic_ns()
    deadline = time.monotonic() + timeout_s
    last_err: str | None = None
    info_url = f"{cfg.lakekeeper_base_url}/management/v1/info"
    while time.monotonic() < deadline:
        res = probe_http_status(info_url, timeout=3.0)
        if res["status"] == 200:
            now_ns = time.monotonic_ns()
            return {
                "healthy": True,
                "healthy_mono_ns": now_ns,
                "healthy_utc": utc_now_iso(),
                "wait_ms": round((now_ns - t0) / 1_000_000.0, 2),
                "last_error": last_err,
            }
        last_err = res["error"] or f"HTTP {res['status']}"
        time.sleep(0.5)
    raise TimeoutError(f"Lakekeeper did not become healthy within {timeout_s}s: {last_err}")


class CatalogOutageController:
    """Stops Lakekeeper for a fixed interval during a running workload, sampling backlog throughout."""

    def __init__(
        self,
        cfg: LabConfig,
        *,
        outage_delay_s: float,
        outage_duration_s: float,
        sample_interval_s: float,
        samples_jsonl: Path,
        restart_connector_after_recovery: bool = False,
    ) -> None:
        self._cfg = cfg
        self._outage_delay_s = outage_delay_s
        self._outage_duration_s = outage_duration_s
        self._sample_interval_s = sample_interval_s
        self._samples_jsonl = samples_jsonl
        self._restart_connector_after_recovery = restart_connector_after_recovery

        self._window_start_ns: int | None = None
        self._phase = "pre_outage"
        self._stop_sampling = threading.Event()
        self._outage_done = threading.Event()
        self._lock = threading.Lock()
        self._samples: list[dict[str, Any]] = []
        self._events: dict[str, Any] = {
            "outage_delay_s": outage_delay_s,
            "outage_duration_s": outage_duration_s,
            "restart_connector_after_recovery": restart_connector_after_recovery,
            "commands": [],
            "error": None,
        }
        self._outage_thread: threading.Thread | None = None
        self._sampler_thread: threading.Thread | None = None

    def start_for_window(self, window_start_ns: int, window_start_utc: str) -> None:
        self._window_start_ns = window_start_ns
        self._events["window_start_utc"] = window_start_utc
        self._sampler_thread = threading.Thread(
            target=self._sample_loop, name="l3-outage-sampler", daemon=True
        )
        self._outage_thread = threading.Thread(
            target=self._outage_lifecycle, name="l3-outage-controller", daemon=True
        )
        self._sampler_thread.start()
        self._outage_thread.start()

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
        visible_log_end: str | None = None
        if phase != "outage":
            try:
                lk_meta = fetch_lakekeeper_table_metadata(
                    self._cfg,
                    namespace=self._cfg.lakekeeper_namespace,
                    table="orders",
                    allow_not_found=True,
                )
                if lk_meta and isinstance(lk_meta.get("metadata"), dict):
                    snap_id = lk_meta["metadata"].get("current-snapshot-id")
                    if snap_id is not None:
                        visible_snap_id = int(snap_id)
                        for s in lk_meta["metadata"].get("snapshots") or []:
                            if int(s.get("snapshot-id", -1)) == visible_snap_id:
                                visible_log_end = (s.get("summary") or {}).get("pg2iceberg.log-end")
                                break
            except Exception:
                pass

        entry = {
            "captured_at_utc": utc_now_iso(),
            "offset_ms": offset_ms,
            "phase": phase,
            **offsets,
            "visible_snapshot_id": visible_snap_id,
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

    def _outage_lifecycle(self) -> None:
        assert self._window_start_ns is not None
        try:
            # Wait until outage_delay_s after measurement window start.
            stop_target_ns = self._window_start_ns + int(self._outage_delay_s * 1_000_000_000)
            while True:
                rem_s = (stop_target_ns - time.monotonic_ns()) / 1_000_000_000.0
                if rem_s <= 0:
                    break
                time.sleep(min(0.25, rem_s))

            # Capture pre-outage snapshot & Silo usage immediately before stopping Lakekeeper.
            pre_silo = measure_silo_bucket_usage(self._cfg)
            pre_lk_meta = fetch_lakekeeper_table_metadata(
                self._cfg, namespace=self._cfg.lakekeeper_namespace, table="orders", allow_not_found=True
            )
            pre_snap_id = (
                int(pre_lk_meta["metadata"]["current-snapshot-id"])
                if pre_lk_meta and (pre_lk_meta.get("metadata") or {}).get("current-snapshot-id") is not None
                else None
            )
            self._events["silo_before_outage"] = pre_silo
            self._events["pre_outage_snapshot_id"] = pre_snap_id

            # Stop Lakekeeper container.
            stop_cmd = [*compose_cmd_with_profiles(self._cfg), "stop", "lakekeeper"]
            self._events["commands"].append(" ".join(stop_cmd))
            stop_cmd_start_ns = time.monotonic_ns()
            self._events["stop_command_start_utc"] = utc_now_iso()
            self._events["stop_command_start_offset_ms"] = round(
                (stop_cmd_start_ns - self._window_start_ns) / 1_000_000.0, 2
            )
            run_cmd(stop_cmd, check=True)
            stopped_ns = time.monotonic_ns()
            with self._lock:
                self._phase = "outage"
            self._events["outage_started_mono_ns"] = stopped_ns
            self._events["outage_started_utc"] = utc_now_iso()
            self._events["outage_started_offset_ms"] = round(
                (stopped_ns - self._window_start_ns) / 1_000_000.0, 2
            )
            self._events["stop_command_duration_ms"] = round(
                (stopped_ns - stop_cmd_start_ns) / 1_000_000.0, 2
            )

            # Hold Lakekeeper stopped for outage_duration_s while source writes continue.
            restart_target_ns = stopped_ns + int(self._outage_duration_s * 1_000_000_000)
            while True:
                rem_s = (restart_target_ns - time.monotonic_ns()) / 1_000_000_000.0
                if rem_s <= 0:
                    break
                time.sleep(min(0.25, rem_s))

            # Capture exact end-of-outage backlog before restarting Lakekeeper.
            with psycopg.connect(self._cfg.pg_dsn, autocommit=True) as conn:
                self._events["pg_positions_at_outage_end"] = capture_pg_pipeline_positions(conn)
            self._events["silo_at_outage_end"] = measure_silo_bucket_usage(self._cfg)
            self._events["pg2iceberg_container_at_outage_end"] = inspect_service_container(
                self._cfg, "pg2iceberg", profiles=("connector",)
            )

            # Restart Lakekeeper and wait until healthy.
            with self._lock:
                self._phase = "restarting"
            start_cmd = [*compose_cmd_with_profiles(self._cfg), "start", "lakekeeper"]
            self._events["commands"].append(" ".join(start_cmd))
            start_cmd_ns = time.monotonic_ns()
            self._events["restart_command_start_mono_ns"] = start_cmd_ns
            self._events["restart_command_start_utc"] = utc_now_iso()
            self._events["restart_command_start_offset_ms"] = round(
                (start_cmd_ns - self._window_start_ns) / 1_000_000.0, 2
            )
            self._events["actual_stopped_duration_ms"] = round(
                (start_cmd_ns - stopped_ns) / 1_000_000.0, 2
            )
            run_cmd(start_cmd, check=True)
            health_res = wait_for_lakekeeper_healthy(self._cfg, timeout_s=90.0)
            healthy_ns = int(health_res["healthy_mono_ns"])
            with self._lock:
                self._phase = "post_restart"

            self._events["lakekeeper_healthy_mono_ns"] = healthy_ns
            self._events["lakekeeper_healthy_utc"] = health_res["healthy_utc"]
            self._events["lakekeeper_healthy_offset_ms"] = round(
                (healthy_ns - self._window_start_ns) / 1_000_000.0, 2
            )
            self._events["lakekeeper_restart_to_healthy_ms"] = round(
                (healthy_ns - start_cmd_ns) / 1_000_000.0, 2
            )
            self._events["total_unavailable_ms"] = round(
                (healthy_ns - stopped_ns) / 1_000_000.0, 2
            )

            # Check if pg2iceberg exited during the outage; optionally restart it when configured.
            pg2_state_post = inspect_service_container(
                self._cfg, "pg2iceberg", profiles=("connector",)
            )
            self._events["pg2iceberg_exited_during_outage"] = not bool(pg2_state_post.get("running"))
            self._events["pg2iceberg_state_when_lakekeeper_healthy"] = pg2_state_post
            connector_restart_ns: int | None = None
            if self._restart_connector_after_recovery and not bool(pg2_state_post.get("running")):
                restart_pg2_cmd = [
                    *compose_cmd_with_profiles(self._cfg, ("connector",)),
                    "start",
                    "pg2iceberg",
                ]
                self._events["commands"].append(" ".join(restart_pg2_cmd))
                connector_restart_ns = time.monotonic_ns()
                self._events["connector_restart_utc"] = utc_now_iso()
                self._events["connector_restart_offset_ms"] = round(
                    (connector_restart_ns - self._window_start_ns) / 1_000_000.0, 2
                )
                self._events["lakekeeper_healthy_to_connector_restart_ms"] = round(
                    (connector_restart_ns - healthy_ns) / 1_000_000.0, 2
                )
                run_cmd(restart_pg2_cmd, check=True)
                self._events["connector_restarted"] = True
            else:
                self._events["connector_restarted"] = False

            # Observe first post-outage Iceberg snapshot committed after Lakekeeper returns.
            snap_deadline = time.monotonic() + 120.0
            while time.monotonic() < snap_deadline:
                try:
                    post_meta = fetch_lakekeeper_table_metadata(
                        self._cfg,
                        namespace=self._cfg.lakekeeper_namespace,
                        table="orders",
                        allow_not_found=True,
                    )
                    post_snap_id = (
                        int(post_meta["metadata"]["current-snapshot-id"])
                        if post_meta
                        and (post_meta.get("metadata") or {}).get("current-snapshot-id") is not None
                        else None
                    )
                    if post_snap_id is not None and post_snap_id != pre_snap_id:
                        obs_ns = time.monotonic_ns()
                        self._events["first_post_outage_snapshot_id"] = post_snap_id
                        self._events["first_post_outage_snapshot_utc"] = utc_now_iso()
                        self._events["lakekeeper_healthy_to_first_snapshot_ms"] = round(
                            (obs_ns - healthy_ns) / 1_000_000.0, 2
                        )
                        if connector_restart_ns is not None:
                            self._events["connector_restart_to_first_snapshot_ms"] = round(
                                (obs_ns - connector_restart_ns) / 1_000_000.0, 2
                            )
                        break
                except Exception:
                    pass
                time.sleep(0.5)
        except Exception as exc:
            self._events["error"] = f"{type(exc).__name__}: {exc}"
        finally:
            self._outage_done.set()

    def wait_outage_done(self, timeout_s: float = 240.0) -> None:
        self._outage_done.wait(timeout=timeout_s)

    def stop_and_collect(self) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        self._stop_sampling.set()
        if self._outage_thread is not None:
            self._outage_thread.join(timeout=30.0)
        if self._sampler_thread is not None:
            self._sampler_thread.join(timeout=30.0)
        with self._lock:
            return dict(self._events), list(self._samples)


def run_l3(
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
    restart_connector_after_recovery: bool = False,
    label: str = "l3-catalog-outage",
    keep_up: bool = False,
) -> Path:
    """Execute Milestone L3 catalog outage drill end to end on a fresh cdclab stack."""
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

    # 3. Seed M1 baseline fixture (25,000 rows, 100 TOAST rows).
    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        seed_res = seed_orders(
            pg_conn,
            seed_rows=seed_rows,
            toast_row_count=toast_rows,
            rng_seed=rng_seed,
        )
        pg_pos_after_seed = capture_pg_pipeline_positions(pg_conn)

    # 4. Start pg2iceberg and wait for initial snapshot completion.
    logged_cmd([*compose_cmd_with_profiles(cfg, ("connector",)), "up", "-d", "pg2iceberg"])
    digests = collect_digests_for_profiles(cfg, ("connector",))
    con_v, reader_versions = connect_iceberg(cfg)
    con_v.close()

    initial_snapshot_info = wait_for_connector_and_snapshot(cfg, timeout_s=180.0)

    # 5. Commit warm-up marker and wait until visible before measurement window.
    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        warmup_commit_ns, warmup_commit_utc = commit_marker_row(
            pg_conn,
            marker_id=WARMUP_MARKER_ID,
            status="marker_warmup",
            note="l3 warm-up marker before measurement window",
        )
    warmup_vis = wait_for_marker_visible(
        cfg,
        WARMUP_MARKER_ID,
        warmup_commit_ns,
        timeout_s=120.0,
        poll_interval_s=0.5,
    )
    if not warmup_vis["visible"]:
        raise RuntimeError(f"Warm-up marker not visible within timeout: {warmup_vis}")

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as pg_conn:
        pg_pos_window_start = capture_pg_pipeline_positions(pg_conn)

    # 6. Run M1 baseline workload while CatalogOutageController stops Lakekeeper for 60s.
    controller = CatalogOutageController(
        cfg,
        outage_delay_s=outage_delay_s,
        outage_duration_s=outage_duration_s,
        sample_interval_s=sample_interval_s,
        samples_jsonl=run_dir / "outage_samples.jsonl",
        restart_connector_after_recovery=restart_connector_after_recovery,
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
        pg_pos_window_end = capture_pg_pipeline_positions(pg_conn)

    # 7. Commit unique final marker row and run full exact convergence verifier.
    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        final_commit_ns, final_commit_utc = commit_marker_row(
            pg_conn,
            marker_id=FINAL_MARKER_ID,
            status="marker_final",
            note="l3 catalog outage final convergence marker",
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

    outage_events, outage_samples = controller.stop_and_collect()
    commands_executed.extend(outage_events.get("commands") or [])
    outage_summary = summarize_outage_samples(outage_samples)

    # Compute recovery timing from Lakekeeper healthy to backlog drain and to verified match.
    healthy_offset_ms = outage_events.get("lakekeeper_healthy_offset_ms")
    drained_offset_ms = outage_summary.get("backlog_drained_offset_ms")
    if isinstance(healthy_offset_ms, (int, float)) and isinstance(drained_offset_ms, (int, float)):
        outage_summary["lakekeeper_healthy_to_backlog_drained_ms"] = round(
            max(0.0, float(drained_offset_ms) - float(healthy_offset_ms)), 2
        )
    else:
        outage_summary["lakekeeper_healthy_to_backlog_drained_ms"] = None

    final_lk_meta = convergence.pop("final_lakekeeper_metadata", None)
    if final_lk_meta is not None:
        write_json(run_dir / "lakekeeper_table_final.json", final_lk_meta)

    # 8. Capture final DuckDB snapshots/metadata, Silo usage, PG positions, and logs.
    duckdb_snapshots_final: list[dict[str, Any]] = []
    duckdb_metadata_summary: dict[str, Any] = {}
    con_meta = None
    try:
        con_meta, _ = connect_iceberg(cfg)
        duckdb_snapshots_final = read_snapshots(con_meta, namespace=cfg.lakekeeper_namespace)
        meta_rows = read_metadata_files(con_meta, namespace=cfg.lakekeeper_namespace)
        duckdb_metadata_summary = summarize_duckdb_metadata(meta_rows)
        write_json(run_dir / "duckdb_snapshots_final.json", duckdb_snapshots_final)
        write_json(run_dir / "duckdb_metadata_final.json", meta_rows)
    except Exception as exc:
        duckdb_metadata_summary = {"error": f"{type(exc).__name__}: {exc}"}
    finally:
        if con_meta is not None:
            con_meta.close()

    silo_final = measure_silo_bucket_usage(cfg)
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as pg_conn:
        pg_pos_final = capture_pg_pipeline_positions(pg_conn)

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

    # Measure writes committed during the outage window from ops.jsonl.
    outage_start_off = outage_events.get("outage_started_offset_ms")
    outage_end_off = outage_events.get("restart_command_start_offset_ms")
    ops_during_outage = 0
    ops_errors_during_outage = 0
    if isinstance(outage_start_off, (int, float)) and isinstance(outage_end_off, (int, float)):
        with (run_dir / "ops.jsonl").open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                c_off = rec.get("complete_offset_ms")
                if isinstance(c_off, (int, float)) and outage_start_off <= c_off <= outage_end_off:
                    if rec.get("outcome") == "ok":
                        ops_during_outage += 1
                    else:
                        ops_errors_during_outage += 1

    # 9. Project-only cleanup.
    cleanup_result = project_teardown_and_verify(
        cfg,
        logged_cmd,
        non_project_before,
        profiles=ALL_LAB_PROFILES,
        keep_up=keep_up,
    )

    # Explicit outcome per docs/PLAN.md.
    schedule_ok = workload_summary["arrival_schedule"]["schedule_valid"]
    toast_ok = (
        seed_res.toast_verification["both_toasted_rows"] >= len(seed_res.toast_ids)
        and toast_after_workload["both_toasted_rows"] >= len(seed_res.toast_ids)
        and workload_summary["toast_exercise"]["unchanged_toast_updates"] > 0
        and workload_summary["toast_exercise"]["changed_toast_updates"] > 0
    )
    outage_ok = (
        outage_events.get("error") is None
        and isinstance(outage_events.get("actual_stopped_duration_ms"), (int, float))
        and float(outage_events["actual_stopped_duration_ms"]) >= outage_duration_s * 900.0
        and ops_during_outage > 0
    )
    cleanup_ok = keep_up or (
        cleanup_result["all_project_resources_removed"]
        and cleanup_result["non_project_containers_unchanged"]
    )

    if not schedule_ok or not toast_ok or not outage_ok:
        outcome = "invalid harness run"
    else:
        outcome = convergence["outcome"]

    passed = (
        outcome == "converged"
        and convergence["passed"]
        and schedule_ok
        and toast_ok
        and outage_ok
        and cleanup_ok
    )

    # Strip internal monotonic timestamps from persisted outage_events.
    persisted_outage_events = {
        k: v for k, v in outage_events.items() if not k.endswith("_mono_ns")
    }
    persisted_outage_events["ops_committed_during_outage"] = ops_during_outage
    persisted_outage_events["ops_errors_during_outage"] = ops_errors_during_outage
    persisted_outage_events["silo_final"] = silo_final

    summary = {
        "milestone": "L3",
        "run_kind": "catalog_outage",
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
            "restart_connector_after_recovery": restart_connector_after_recovery,
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
            "extensions": reader_versions.extensions,
        },
        "resources_after_pre_clean": resources_after_pre_clean,
        "seed": seed_res.to_dict(),
        "initial_snapshot": initial_snapshot_info,
        "warmup_marker": {
            "marker_id": WARMUP_MARKER_ID,
            "commit_utc": warmup_commit_utc,
            "visible": warmup_vis["visible"],
            "visibility_ms": warmup_vis["visibility_ms"],
            "snapshot_id": warmup_vis["snapshot_id"],
            "polls": warmup_vis["polls"],
            "duckdb_checks": warmup_vis["duckdb_checks"],
            "errors": warmup_vis["errors"],
            "snapshot_info": warmup_vis["snapshot_info"],
        },
        "workload": workload_summary,
        "toast_after_workload": toast_after_workload,
        "outage": {
            "events": persisted_outage_events,
            "sample_summary": outage_summary,
            "pg2iceberg_log_summary": pg2_log_summary,
        },
        "final_marker_commit_utc": final_commit_utc,
        "convergence": convergence,
        "duckdb_final_snapshot_count": len(duckdb_snapshots_final),
        "duckdb_final_metadata_summary": duckdb_metadata_summary,
        "pipeline_positions": {
            "after_seed": pg_pos_after_seed,
            "window_start": pg_pos_window_start,
            "window_end": pg_pos_window_end,
            "after_final_marker": pg_pos_after_final_marker,
            "final": pg_pos_final,
        },
        "cleanup": cleanup_result,
    }
    write_json(run_dir / "summary.json", summary)

    compact = {
        "run_dir": str(run_dir.relative_to(REPO_ROOT).as_posix()),
        "outcome": outcome,
        "passed": passed,
        "actual_stopped_duration_ms": persisted_outage_events.get("actual_stopped_duration_ms"),
        "total_unavailable_ms": persisted_outage_events.get("total_unavailable_ms"),
        "ops_committed_during_outage": ops_during_outage,
        "staging_advanced_during_outage": outage_summary["staging_advanced_during_outage"],
        "applied_frozen_during_outage": outage_summary["applied_frozen_during_outage"],
        "outage_max_pending_unapplied_offsets": outage_summary["outage_max_pending_unapplied_offsets"],
        "outage_wal_gap_confirmed_flush_max_bytes": outage_summary["outage_wal_gap_confirmed_flush_max_bytes"],
        "outage_wal_gap_restart_max_bytes": outage_summary["outage_wal_gap_restart_max_bytes"],
        "pg2iceberg_all_samples_running": outage_summary["pg2iceberg_all_samples_running"],
        "pg2iceberg_max_restart_count": outage_summary["pg2iceberg_max_restart_count"],
        "pg2iceberg_exited_during_outage": persisted_outage_events.get(
            "pg2iceberg_exited_during_outage"
        ),
        "connector_restarted": persisted_outage_events.get("connector_restarted"),
        "pg2iceberg_error_count": pg2_log_summary["error_count"],
        "pg2iceberg_fatal_text_error_count": pg2_log_summary["fatal_text_error_count"],
        "pg2iceberg_warn_count": pg2_log_summary["warn_count"],
        "lakekeeper_healthy_to_first_snapshot_ms": persisted_outage_events.get(
            "lakekeeper_healthy_to_first_snapshot_ms"
        ),
        "connector_restart_to_first_snapshot_ms": persisted_outage_events.get(
            "connector_restart_to_first_snapshot_ms"
        ),
        "lakekeeper_healthy_to_backlog_drained_ms": outage_summary.get(
            "lakekeeper_healthy_to_backlog_drained_ms"
        ),
        "final_marker_visibility_ms": convergence["final_marker_visibility_ms"],
        "final_marker_to_verified_match_ms": convergence["final_marker_to_verified_match_ms"],
        "verified_snapshot_id": convergence["verified_snapshot_id"],
    }
    print(json.dumps(compact, indent=2, default=json_default))
    if not passed:
        raise SystemExit(1)
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Run L3 catalog outage drill during M1 baseline workload.")
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
    parser.add_argument(
        "--restart-connector-after-recovery",
        action="store_true",
        help="Restart pg2iceberg if it exited during the Lakekeeper outage once Lakekeeper returns healthy.",
    )
    parser.add_argument("--label", default="l3-catalog-outage")
    parser.add_argument("--keep-up", action="store_true")
    args = parser.parse_args()
    run_l3(
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
        restart_connector_after_recovery=args.restart_connector_after_recovery,
        label=args.label,
        keep_up=args.keep_up,
    )


if __name__ == "__main__":
    main()
