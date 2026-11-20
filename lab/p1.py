"""Milestone P1 end-to-end runner: Apache Polaris 1.9.0 comparison against Lakekeeper (M1, L1, L2 + telemetry)."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
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
import urllib.request

import psycopg

from lab.bootstrap import ensure_bucket, ensure_source_fixture, verify_storage_endpoint_reachability
from lab.compare import (
    ComparisonResult,
    SchemaCheckResult,
    compare_ordered_streams,
    stream_duckdb_orders_at_snapshot,
    stream_postgres_orders,
    verify_iceberg_schema,
)
from lab.config import LabConfig, REPO_ROOT, load_config
from lab.l1 import (
    latest_snapshot_id_from_duckdb,
    stream_spark_orders_ndjson,
    verify_duckdb_schema,
    verify_spark_schema,
)
from lab.l2 import (
    extract_vended_s3_credentials,
    redact_load_table_response,
    sigv4_s3_get,
    verify_distinct_storage_prefixes,
)
from lab.l_common import (
    collect_digests_for_profiles,
    compose_cmd_with_profiles,
    project_teardown_and_verify,
)
from lab.m0 import snapshot_has_equality_deletes
from lab.m1 import summarize_duckdb_metadata
from lab.pipeline import (
    capture_pg_pipeline_positions,
    extract_iceberg_snapshot_info,
    json_default,
    list_non_project_containers,
    list_project_resources,
    run_cmd,
    utc_now_iso,
    utc_run_stamp,
    write_json,
)
from lab.polaris import (
    ALL_P1_PROFILES,
    PolarisConfig,
    add_grant_to_catalog_role,
    assign_catalog_role_to_principal_role,
    assign_principal_role,
    connect_polaris_iceberg,
    create_catalog_role,
    create_principal,
    create_principal_role,
    create_s3_catalog,
    duckdb_scan_polaris_table,
    ensure_polaris_namespace,
    fetch_polaris_table_metadata,
    fetch_polaris_token,
    grant_root_catalog_content_access,
    load_polaris_config,
    rest_attempt_table_commit_polaris,
    rest_load_table_polaris,
    sample_container_stats,
)
from lab.reader import marker_exists_at_snapshot, read_metadata_files, read_snapshots
from lab.workload import (
    FINAL_MARKER_ID,
    INSERT_ID_START,
    PROBE_MARKER_ID_START,
    STATUSES,
    WARMUP_MARKER_ID,
    SeedResult,
    _make_decimal_38_9,
    _make_toast_note,
    _make_toast_payload_json,
    commit_marker_row,
    percentile,
    seed_orders,
    summarize_latency,
    verify_toast_storage,
)


def _redact_cmd(cmd: list[str]) -> str:
    safe_parts: list[str] = []
    skip_next = False
    for part in cmd:
        if skip_next:
            safe_parts.append("<redacted>")
            skip_next = False
            continue
        safe_parts.append(part)
        if part == "--client-secret":
            skip_next = True
    return " ".join(safe_parts)


def _summarize_memory_samples(samples: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_container: dict[str, list[float]] = {}
    for s in samples:
        for cname, info in (s.get("containers") or {}).items():
            mib = info.get("mem_used_mib")
            if isinstance(mib, (int, float)):
                by_container.setdefault(cname, []).append(float(mib))
    out: dict[str, dict[str, Any]] = {}
    for cname, vals in sorted(by_container.items()):
        s_vals = sorted(vals)
        out[cname] = {
            "count": len(s_vals),
            "min_mib": round(s_vals[0], 2),
            "p50_mib": round(percentile(s_vals, 50.0) or 0.0, 2),
            "mean_mib": round(sum(s_vals) / len(s_vals), 2),
            "p95_mib": round(percentile(s_vals, 95.0) or 0.0, 2),
            "max_mib": round(s_vals[-1], 2),
        }
    return out


class ContainerStatsSampler:
    """Background docker stats sampler writing JSONL samples during a workload window."""

    def __init__(
        self,
        container_names: list[str],
        *,
        interval_s: float = 10.0,
        out_jsonl: Path,
    ) -> None:
        self._container_names = list(container_names)
        self._interval_s = interval_s
        self._out_jsonl = out_jsonl
        self._stop_event = threading.Event()
        self._samples: list[dict[str, Any]] = []
        self._thread = threading.Thread(target=self._run, name="p1-docker-stats", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop_and_join(self, timeout_s: float = 30.0) -> list[dict[str, Any]]:
        self._stop_event.set()
        self._thread.join(timeout=timeout_s)
        return list(self._samples)

    def _run(self) -> None:
        self._out_jsonl.parent.mkdir(parents=True, exist_ok=True)
        with self._out_jsonl.open("w", encoding="utf-8", buffering=1) as fh:
            while not self._stop_event.is_set():
                sample = sample_container_stats(self._container_names)
                self._samples.append(sample)
                fh.write(json.dumps(sample, default=json_default) + "\n")
                self._stop_event.wait(timeout=self._interval_s)


def _sample_idle_memory(
    container_names: list[str],
    *,
    samples: int = 3,
    interval_s: float = 2.0,
) -> dict[str, Any]:
    raw: list[dict[str, Any]] = []
    for idx in range(samples):
        raw.append(sample_container_stats(container_names))
        if idx + 1 < samples:
            time.sleep(interval_s)
    return {
        "samples": raw,
        "summary": _summarize_memory_samples(raw),
    }


def _parse_quarkus_startup_s(polaris_logs: str) -> float | None:
    m = re.search(r"polaris-server\s+[^\n]*started in\s+([0-9]+(?:\.[0-9]+)?)s", polaris_logs)
    return float(m.group(1)) if m else None


def bootstrap_polaris_m1(
    lab_cfg: LabConfig,
    polaris_cfg: PolarisConfig,
    *,
    catalog_name: str | None = None,
    key_prefix: str = "polaris-warehouse",
    include_source_fixture: bool = True,
) -> dict[str, Any]:
    """Idempotently bootstrap Silo bucket, Polaris S3 catalog with STS, namespace, and PG fixture."""
    cat = catalog_name or polaris_cfg.catalog_name
    bucket_res = ensure_bucket(lab_cfg)
    tok, tok_info = fetch_polaris_token(
        polaris_cfg.host_token_url,
        polaris_cfg.root_client_id,
        polaris_cfg.root_client_secret,
        scope="PRINCIPAL_ROLE:ALL",
        realm=polaris_cfg.realm,
    )
    cat_res = create_s3_catalog(
        polaris_cfg,
        lab_cfg,
        tok,
        catalog_name=cat,
        key_prefix=key_prefix,
        sts_unavailable=False,
        kms_unavailable=True,
    )
    grant_res = grant_root_catalog_content_access(polaris_cfg, tok, catalog_name=cat)
    ns_res = ensure_polaris_namespace(
        polaris_cfg,
        tok,
        catalog_name=cat,
        namespace=lab_cfg.lakekeeper_namespace,
    )
    storage_ep = (
        (cat_res.get("catalog") or {}).get("storageConfigInfo", {}).get("endpoint")
        or lab_cfg.silo_advertised_endpoint
    )
    reach_res: dict[str, Any] | None = None
    pg_res: dict[str, Any] | None = None
    if include_source_fixture:
        reach_res = verify_storage_endpoint_reachability(lab_cfg, storage_ep)
        pg_res = ensure_source_fixture(lab_cfg)
    return {
        "bucket": bucket_res,
        "token_claims_sub": (tok_info.get("claims") or {}).get("sub"),
        "token_expires_in": tok_info.get("expires_in"),
        "catalog": cat_res,
        "grant": grant_res,
        "namespace": ns_res,
        "endpoint_reachability": reach_res,
        "source_fixture": pg_res,
    }


def wait_for_polaris_connector_and_snapshot(
    lab_cfg: LabConfig,
    polaris_cfg: PolarisConfig,
    *,
    timeout_s: float = 180.0,
    poll_interval_s: float = 1.0,
) -> dict[str, Any]:
    """Wait until pg2iceberg-polaris slot is active and its initial snapshot completes on Polaris."""
    t0 = time.monotonic_ns()
    deadline = time.monotonic() + timeout_s
    last_err: str | None = None
    slot_seen = False

    while time.monotonic() < deadline:
        try:
            with psycopg.connect(lab_cfg.pg_dsn, autocommit=True) as conn:
                pos = capture_pg_pipeline_positions(conn)
            slots = [s for s in pos["replication_slots"] if s["slot_name"] == "pg2iceberg_slot"]
            if slots and slots[0]["active"]:
                slot_seen = True

            logs = run_cmd(
                [
                    *compose_cmd_with_profiles(lab_cfg, ("polaris", "polaris-connector")),
                    "logs",
                    "--no-color",
                    "pg2iceberg-polaris",
                ],
                check=False,
            ).stdout
            snapshot_logged_complete = "snapshot phase complete" in logs

            p_meta = fetch_polaris_table_metadata(
                polaris_cfg,
                catalog_name=polaris_cfg.catalog_name,
                namespace=lab_cfg.lakekeeper_namespace,
                table="orders",
                allow_not_found=True,
            )
            current_snap_id = (
                (p_meta.get("metadata") or {}).get("current-snapshot-id")
                if p_meta
                else None
            )

            health_status: int | None = None
            try:
                with urllib.request.urlopen(
                    f"{polaris_cfg.pg2iceberg_polaris_metrics_url}/healthz", timeout=3.0
                ) as r:
                    health_status = r.status
            except Exception as exc:
                last_err = f"healthz: {exc}"

            if (
                slot_seen
                and snapshot_logged_complete
                and current_snap_id is not None
                and int(current_snap_id) != -1
            ):
                elapsed_ms = (time.monotonic_ns() - t0) / 1_000_000.0
                return {
                    "elapsed_ms": round(elapsed_ms, 2),
                    "healthz_status": health_status,
                    "snapshot_logged_complete": snapshot_logged_complete,
                    "current_snapshot_id": int(current_snap_id),
                    "snapshot_info": extract_iceberg_snapshot_info(p_meta, int(current_snap_id)),
                    "pipeline_positions": pos,
                }
        except Exception as exc:
            last_err = f"{type(exc).__name__}: {exc}"
        time.sleep(poll_interval_s)

    raise TimeoutError(
        f"pg2iceberg-polaris did not complete initial snapshot within {timeout_s}s; "
        f"slot_seen={slot_seen} last_err={last_err}"
    )


def wait_for_polaris_marker_visible(
    lab_cfg: LabConfig,
    polaris_cfg: PolarisConfig,
    marker_id: int,
    commit_mono_ns: int,
    *,
    timeout_s: float = 120.0,
    poll_interval_s: float = 0.5,
) -> dict[str, Any]:
    """Poll Polaris + DuckDB (pinned to each new snapshot ID) until marker_id is visible."""
    deadline = time.monotonic() + timeout_s
    checked_snapshots: set[int] = set()
    polls = 0
    duckdb_checks = 0
    errors: list[str] = []

    while time.monotonic() < deadline:
        polls += 1
        try:
            p_meta = fetch_polaris_table_metadata(
                polaris_cfg,
                catalog_name=polaris_cfg.catalog_name,
                namespace=lab_cfg.lakekeeper_namespace,
                table="orders",
                allow_not_found=True,
            )
            snap_id = (
                (p_meta.get("metadata") or {}).get("current-snapshot-id")
                if p_meta
                else None
            )
            if snap_id is not None and int(snap_id) != -1:
                snap_int = int(snap_id)
                if snap_int not in checked_snapshots:
                    checked_snapshots.add(snap_int)
                    duckdb_checks += 1
                    con = None
                    try:
                        con, _ = connect_polaris_iceberg(polaris_cfg)
                        found = marker_exists_at_snapshot(
                            con, snap_int, marker_id, namespace=lab_cfg.lakekeeper_namespace
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
                            "snapshot_info": extract_iceberg_snapshot_info(p_meta, snap_int),
                            "polaris_metadata": p_meta,
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
        "polaris_metadata": None,
    }


def verify_polaris_snapshot_convergence(
    lab_cfg: LabConfig,
    polaris_cfg: PolarisConfig,
    final_marker_id: int,
    final_marker_commit_mono_ns: int,
    *,
    timeout_s: float = 180.0,
    poll_interval_s: float = 1.0,
    batch_size: int = 2000,
) -> dict[str, Any]:
    """Wait for final marker on Polaris, then run exact schema + bounded-batch row diff."""
    deadline = time.monotonic() + timeout_s
    marker_checked_snapshots: set[int] = set()
    diffed_snapshots: set[int] = set()
    snapshot_attempts: list[dict[str, Any]] = []
    reader_errors: list[str] = []
    polls = 0

    first_marker_visible_ns: int | None = None
    first_marker_snapshot_id: int | None = None
    first_marker_utc: str | None = None
    last_meta: dict[str, Any] | None = None

    while time.monotonic() < deadline:
        polls += 1
        try:
            p_meta = fetch_polaris_table_metadata(
                polaris_cfg,
                catalog_name=polaris_cfg.catalog_name,
                namespace=lab_cfg.lakekeeper_namespace,
                table="orders",
                allow_not_found=True,
            )
            snap_id = (
                (p_meta.get("metadata") or {}).get("current-snapshot-id")
                if p_meta
                else None
            )
            if snap_id is None or int(snap_id) == -1:
                time.sleep(poll_interval_s)
                continue

            snap_int = int(snap_id)
            last_meta = p_meta

            if first_marker_snapshot_id is None:
                if snap_int in marker_checked_snapshots:
                    time.sleep(poll_interval_s)
                    continue
                marker_checked_snapshots.add(snap_int)
                con = None
                try:
                    con, _ = connect_polaris_iceberg(polaris_cfg)
                    has_marker = marker_exists_at_snapshot(
                        con, snap_int, final_marker_id, namespace=lab_cfg.lakekeeper_namespace
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

            schema_res: SchemaCheckResult = verify_iceberg_schema(p_meta)
            con = None
            source_read_opened_utc = ""
            try:
                con, _ = connect_polaris_iceberg(polaris_cfg)
                with psycopg.connect(lab_cfg.pg_dsn) as pg_conn:
                    pg_conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    source_read_opened_utc = utc_now_iso()
                    src_batches = stream_postgres_orders(pg_conn, batch_size=batch_size)
                    lake_batches = stream_duckdb_orders_at_snapshot(
                        con,
                        snap_int,
                        namespace=lab_cfg.lakekeeper_namespace,
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
                "snapshot_info": extract_iceberg_snapshot_info(p_meta, snap_int),
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
                    "final_polaris_metadata": p_meta,
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
        "final_polaris_metadata": last_meta,
    }


class PolarisSteadyStateLagProber:
    """Background thread inserting steady-state probes and polling DuckDB via Polaris."""

    def __init__(
        self,
        lab_cfg: LabConfig,
        polaris_cfg: PolarisConfig,
        *,
        window_start_mono_ns: int,
        duration_s: float,
        probe_interval_s: float = 20.0,
        poll_interval_s: float = 0.5,
        out_jsonl: Path,
    ) -> None:
        self._lab_cfg = lab_cfg
        self._polaris_cfg = polaris_cfg
        self._window_start_mono_ns = window_start_mono_ns
        self._duration_s = duration_s
        self._probe_interval_s = probe_interval_s
        self._poll_interval_s = poll_interval_s
        self._out_jsonl = out_jsonl
        self._stop_event = threading.Event()
        self._results: list[dict[str, Any]] = []
        self._thread = threading.Thread(
            target=self._run, name="polaris-steady-state-lag-prober", daemon=True
        )

    def start(self) -> None:
        self._thread.start()

    def stop_and_join(self, timeout_s: float = 60.0) -> list[dict[str, Any]]:
        self._stop_event.set()
        self._thread.join(timeout=timeout_s)
        return list(self._results)

    def _run(self) -> None:
        first_offset_s = min(10.0, max(2.0, self._duration_s / 3.0))
        probe_offsets: list[float] = []
        t = first_offset_s
        cutoff_s = max(first_offset_s + 0.1, self._duration_s - 10.0)
        while t < cutoff_s:
            probe_offsets.append(t)
            t += self._probe_interval_s

        self._out_jsonl.parent.mkdir(parents=True, exist_ok=True)
        with self._out_jsonl.open("w", encoding="utf-8", buffering=1) as fh:
            for idx, offset_s in enumerate(probe_offsets, start=1):
                target_ns = self._window_start_mono_ns + int(offset_s * 1_000_000_000)
                while not self._stop_event.is_set():
                    rem_s = (target_ns - time.monotonic_ns()) / 1_000_000_000.0
                    if rem_s <= 0:
                        break
                    self._stop_event.wait(timeout=min(0.25, rem_s))
                if self._stop_event.is_set():
                    break

                marker_id = PROBE_MARKER_ID_START + idx - 1
                status = f"marker_probe_{idx:03d}"
                try:
                    with psycopg.connect(self._lab_cfg.pg_dsn) as pg_conn:
                        commit_ns, commit_utc = commit_marker_row(
                            pg_conn,
                            marker_id=marker_id,
                            status=status,
                            note=f"steady-state probe {idx}",
                        )
                        pg_pos = capture_pg_pipeline_positions(pg_conn)

                    vis = wait_for_polaris_marker_visible(
                        self._lab_cfg,
                        self._polaris_cfg,
                        marker_id,
                        commit_ns,
                        timeout_s=45.0,
                        poll_interval_s=self._poll_interval_s,
                    )
                    entry = {
                        "probe_seq": idx,
                        "marker_id": marker_id,
                        "scheduled_offset_s": round(offset_s, 2),
                        "commit_offset_s": round(
                            (commit_ns - self._window_start_mono_ns) / 1_000_000_000.0, 3
                        ),
                        "commit_utc": commit_utc,
                        "visible": vis["visible"],
                        "visibility_ms": vis["visibility_ms"],
                        "observed_utc": vis["observed_utc"],
                        "snapshot_id": vis["snapshot_id"],
                        "polls": vis["polls"],
                        "duckdb_checks": vis["duckdb_checks"],
                        "poll_interval_s": self._poll_interval_s,
                        "reader_errors": vis["errors"],
                        "snapshot_info": vis["snapshot_info"],
                        "pg_positions_at_commit": {
                            "current_wal_lsn": pg_pos["current_wal_lsn"],
                            "replication_slots": pg_pos["replication_slots"],
                        },
                    }
                except Exception as exc:
                    entry = {
                        "probe_seq": idx,
                        "marker_id": marker_id,
                        "scheduled_offset_s": round(offset_s, 2),
                        "visible": False,
                        "visibility_ms": None,
                        "error": f"{type(exc).__name__}: {exc}",
                        "reader_errors": [f"{type(exc).__name__}: {exc}"],
                    }

                self._results.append(entry)
                fh.write(json.dumps(entry, default=json_default) + "\n")


def run_paced_workload_polaris(
    lab_cfg: LabConfig,
    polaris_cfg: PolarisConfig,
    seed_res: SeedResult,
    *,
    duration_s: float = 180.0,
    rate_ops_s: float = 10.0,
    rng_seed: int = 20261119,
    ops_jsonl: Path,
    probes_jsonl: Path,
    probe_interval_s: float = 20.0,
    probe_poll_interval_s: float = 0.5,
    statement_timeout_ms: int = 15000,
) -> dict[str, Any]:
    """Execute the exact M1 deterministic RNG workload with Polaris steady-state lag probes."""
    rng = random.Random(rng_seed + 1000)
    toast_ids = list(seed_res.toast_ids)
    mutable_ids = list(seed_res.mutable_ids)
    toast_rev: dict[int, int] = {tid: 0 for tid in toast_ids}
    next_insert_id = INSERT_ID_START
    insert_counter = 0

    planned_ops = max(1, int(round(duration_s * rate_ops_s)))
    interval_ns = int(round(1_000_000_000 / rate_ops_s))
    miss_threshold_ms = max(250.0, 2.0 * (1000.0 / rate_ops_s))

    op_counts: dict[str, int] = {
        "update_normal": 0,
        "update_toast_unchanged": 0,
        "update_toast_changed": 0,
        "insert": 0,
        "delete": 0,
    }
    affected_rows_by_op: dict[str, int] = {k: 0 for k in op_counts}
    sched_to_complete_ms_list: list[float] = []
    exec_latency_ms_list: list[float] = []
    queue_delay_ms_list: list[float] = []
    harness_dispatch_lag_ms_list: list[float] = []
    backpressure_delay_ms_list: list[float] = []

    attempted = 0
    completed = 0
    errors = 0
    timeouts = 0
    missed_schedule_count = 0
    harness_missed_arrival_count = 0
    backpressure_delayed_count = 0
    sample_errors: list[dict[str, Any]] = []
    toast_unchanged_ids_touched: set[int] = set()
    toast_changed_ids_touched: set[int] = set()

    ops_jsonl.parent.mkdir(parents=True, exist_ok=True)
    window_start_utc = utc_now_iso()
    window_start_ns = time.monotonic_ns()

    prober = PolarisSteadyStateLagProber(
        lab_cfg,
        polaris_cfg,
        window_start_mono_ns=window_start_ns,
        duration_s=duration_s,
        probe_interval_s=probe_interval_s,
        poll_interval_s=probe_poll_interval_s,
        out_jsonl=probes_jsonl,
    )
    prober.start()

    try:
        with (
            psycopg.connect(lab_cfg.pg_dsn) as conn,
            ops_jsonl.open("w", encoding="utf-8", buffering=1) as fh,
        ):
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {int(statement_timeout_ms)};")
            conn.commit()

            prev_complete_ns = window_start_ns

            for seq in range(planned_ops):
                scheduled_ns = window_start_ns + seq * interval_ns
                while True:
                    now_ns = time.monotonic_ns()
                    rem_ns = scheduled_ns - now_ns
                    if rem_ns <= 0:
                        break
                    if rem_ns > 2_000_000:
                        time.sleep((rem_ns - 1_500_000) / 1_000_000_000.0)
                    else:
                        time.sleep(0.0005)

                start_ns = time.monotonic_ns()
                attempted += 1

                u = rng.random()
                if u < 0.40 and mutable_ids:
                    op_kind = "update_normal"
                elif u < 0.55 and toast_ids:
                    op_kind = "update_toast_unchanged"
                elif u < 0.70 and toast_ids:
                    op_kind = "update_toast_changed"
                elif u < 0.85 or len(mutable_ids) <= 10:
                    op_kind = "insert"
                else:
                    op_kind = "delete"

                op_counts[op_kind] += 1
                target_id = -1
                affected_rows = 0
                outcome = "ok"
                err_msg: str | None = None
                now_utc_dt = datetime.now(timezone.utc)

                try:
                    with conn.cursor() as cur:
                        if op_kind == "update_normal":
                            target_id = mutable_ids[rng.randrange(len(mutable_ids))]
                            new_status = STATUSES[rng.randrange(len(STATUSES))]
                            new_amount = _make_decimal_38_9(rng, extreme=(seq % 200 == 0))
                            r_note = rng.random()
                            if r_note < 0.1:
                                new_note: str | None = None
                            elif r_note < 0.2:
                                new_note = ""
                            else:
                                new_note = f"upd-{seq}-id-{target_id} "
                            new_payload = (
                                "null"
                                if rng.random() < 0.05
                                else json.dumps(
                                    {"seq": seq, "op": "upd", "arr": [seq % 5, target_id % 7]},
                                    separators=(",", ":"),
                                )
                            )
                            new_discount = None if rng.random() < 0.3 else _make_decimal_38_9(rng)
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
                                    new_status,
                                    new_amount,
                                    now_utc_dt,
                                    new_note,
                                    new_payload,
                                    new_discount,
                                    target_id,
                                ),
                            )
                            affected_rows = cur.rowcount

                        elif op_kind == "update_toast_unchanged":
                            target_id = toast_ids[rng.randrange(len(toast_ids))]
                            new_status = STATUSES[rng.randrange(len(STATUSES))]
                            new_amount = _make_decimal_38_9(rng)
                            new_discount = None if rng.random() < 0.3 else _make_decimal_38_9(rng)
                            cur.execute(
                                """
                                UPDATE public.orders
                                SET status = %s,
                                    amount = %s,
                                    updated_at = %s,
                                    discount = %s
                                WHERE id = %s
                                """,
                                (
                                    new_status,
                                    new_amount,
                                    now_utc_dt,
                                    new_discount,
                                    target_id,
                                ),
                            )
                            affected_rows = cur.rowcount
                            toast_unchanged_ids_touched.add(target_id)

                        elif op_kind == "update_toast_changed":
                            target_id = toast_ids[rng.randrange(len(toast_ids))]
                            toast_rev[target_id] = toast_rev.get(target_id, 0) + 1
                            rev = toast_rev[target_id]
                            new_status = STATUSES[rng.randrange(len(STATUSES))]
                            new_amount = _make_decimal_38_9(rng)
                            new_note_t = _make_toast_note(rng, target_id, rev=rev)
                            new_payload_t = _make_toast_payload_json(rng, target_id, rev=rev)
                            new_discount = None if rng.random() < 0.3 else _make_decimal_38_9(rng)
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
                                    new_status,
                                    new_amount,
                                    now_utc_dt,
                                    new_note_t,
                                    new_payload_t,
                                    new_discount,
                                    target_id,
                                ),
                            )
                            affected_rows = cur.rowcount
                            toast_changed_ids_touched.add(target_id)

                        elif op_kind == "insert":
                            target_id = next_insert_id
                            next_insert_id += 1
                            insert_counter += 1
                            new_status = STATUSES[rng.randrange(len(STATUSES))]
                            new_amount = _make_decimal_38_9(rng, extreme=(insert_counter % 50 == 0))
                            new_discount = None if rng.random() < 0.3 else _make_decimal_38_9(rng)
                            if insert_counter % 20 == 0:
                                ins_note: str | None = _make_toast_note(rng, target_id, rev=0)
                                ins_payload: str | None = _make_toast_payload_json(
                                    rng, target_id, rev=0
                                )
                            else:
                                ins_note = f"ins-{target_id}"
                                ins_payload = json.dumps(
                                    {"seq": seq, "op": "ins", "id": target_id},
                                    separators=(",", ":"),
                                )
                            cur.execute(
                                """
                                INSERT INTO public.orders (id, status, amount, updated_at, note, payload, discount)
                                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
                                """,
                                (
                                    target_id,
                                    new_status,
                                    new_amount,
                                    now_utc_dt,
                                    ins_note,
                                    ins_payload,
                                    new_discount,
                                ),
                            )
                            affected_rows = cur.rowcount
                            mutable_ids.append(target_id)

                        elif op_kind == "delete":
                            idx = rng.randrange(len(mutable_ids))
                            target_id = mutable_ids[idx]
                            mutable_ids[idx] = mutable_ids[-1]
                            mutable_ids.pop()
                            cur.execute("DELETE FROM public.orders WHERE id = %s", (target_id,))
                            affected_rows = cur.rowcount

                    conn.commit()
                    completed += 1
                    affected_rows_by_op[op_kind] += affected_rows
                except psycopg.errors.QueryCanceled as exc:
                    conn.rollback()
                    timeouts += 1
                    errors += 1
                    outcome = "timeout"
                    err_msg = str(exc)
                    if len(sample_errors) < 20:
                        sample_errors.append({"seq": seq, "op": op_kind, "error": err_msg})
                except Exception as exc:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    errors += 1
                    outcome = "error"
                    err_msg = f"{type(exc).__name__}: {exc}"
                    if len(sample_errors) < 20:
                        sample_errors.append({"seq": seq, "op": op_kind, "error": err_msg})

                complete_ns = time.monotonic_ns()
                queue_ms = max(0.0, (start_ns - scheduled_ns) / 1_000_000.0)
                exec_ms = (complete_ns - start_ns) / 1_000_000.0
                s2c_ms = (complete_ns - scheduled_ns) / 1_000_000.0

                backpressure_ms = max(0.0, (prev_complete_ns - scheduled_ns) / 1_000_000.0)
                harness_lag_ms = max(0.0, queue_ms - backpressure_ms)
                prev_complete_ns = complete_ns

                queue_delay_ms_list.append(queue_ms)
                exec_latency_ms_list.append(exec_ms)
                sched_to_complete_ms_list.append(s2c_ms)
                backpressure_delay_ms_list.append(backpressure_ms)
                harness_dispatch_lag_ms_list.append(harness_lag_ms)

                if queue_ms > miss_threshold_ms:
                    missed_schedule_count += 1
                if backpressure_ms > 0.0:
                    backpressure_delayed_count += 1
                if harness_lag_ms > miss_threshold_ms:
                    harness_missed_arrival_count += 1

                record = {
                    "seq": seq,
                    "op": op_kind,
                    "id": target_id,
                    "outcome": outcome,
                    "affected_rows": affected_rows,
                    "scheduled_offset_ms": round((scheduled_ns - window_start_ns) / 1_000_000.0, 3),
                    "start_offset_ms": round((start_ns - window_start_ns) / 1_000_000.0, 3),
                    "complete_offset_ms": round((complete_ns - window_start_ns) / 1_000_000.0, 3),
                    "queue_delay_ms": round(queue_ms, 3),
                    "backpressure_delay_ms": round(backpressure_ms, 3),
                    "harness_dispatch_lag_ms": round(harness_lag_ms, 3),
                    "exec_latency_ms": round(exec_ms, 3),
                    "sched_to_complete_ms": round(s2c_ms, 3),
                    "error": err_msg,
                }
                fh.write(json.dumps(record) + "\n")
    finally:
        window_end_ns = time.monotonic_ns()
        window_end_utc = utc_now_iso()
        probe_results = prober.stop_and_join(timeout_s=60.0)

    actual_duration_s = (window_end_ns - window_start_ns) / 1_000_000_000.0
    completed_throughput = completed / actual_duration_s if actual_duration_s > 0 else 0.0
    attempted_throughput = attempted / actual_duration_s if actual_duration_s > 0 else 0.0
    missed_fraction = missed_schedule_count / attempted if attempted else 0.0
    harness_missed_fraction = harness_missed_arrival_count / attempted if attempted else 0.0

    visible_probe_lags = [
        float(p["visibility_ms"])
        for p in probe_results
        if p.get("visible") and p.get("visibility_ms") is not None
    ]
    probe_reader_errors = sum(len(p.get("reader_errors") or []) for p in probe_results)

    return {
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
        "planned_duration_s": duration_s,
        "actual_duration_s": round(actual_duration_s, 3),
        "offered_rate_ops_s": rate_ops_s,
        "planned_ops": planned_ops,
        "attempted_ops": attempted,
        "completed_ops": completed,
        "error_count": errors,
        "timeout_count": timeouts,
        "sample_errors": sample_errors,
        "attempted_throughput_ops_s": round(attempted_throughput, 3),
        "completed_throughput_ops_s": round(completed_throughput, 3),
        "op_counts": op_counts,
        "affected_rows_by_op": affected_rows_by_op,
        "total_affected_rows": sum(affected_rows_by_op.values()),
        "toast_exercise": {
            "unchanged_toast_updates": op_counts["update_toast_unchanged"],
            "changed_toast_updates": op_counts["update_toast_changed"],
            "distinct_unchanged_toast_ids": len(toast_unchanged_ids_touched),
            "distinct_changed_toast_ids": len(toast_changed_ids_touched),
        },
        "arrival_schedule": {
            "miss_threshold_ms": miss_threshold_ms,
            "missed_schedule_count": missed_schedule_count,
            "missed_schedule_fraction": round(missed_fraction, 6),
            "harness_missed_arrival_count": harness_missed_arrival_count,
            "harness_missed_arrival_fraction": round(harness_missed_fraction, 6),
            "backpressure_delayed_count": backpressure_delayed_count,
            "schedule_valid": (attempted == planned_ops) and (harness_missed_fraction <= 0.05),
        },
        "latency_sched_to_complete_ms": summarize_latency(sched_to_complete_ms_list),
        "latency_exec_ms": summarize_latency(exec_latency_ms_list),
        "latency_queue_delay_ms": summarize_latency(queue_delay_ms_list),
        "latency_backpressure_delay_ms": summarize_latency(backpressure_delay_ms_list),
        "latency_harness_dispatch_lag_ms": summarize_latency(harness_dispatch_lag_ms_list),
        "steady_state_probes": {
            "probe_interval_s": probe_interval_s,
            "poll_interval_s": probe_poll_interval_s,
            "total_probes": len(probe_results),
            "visible_probes": len(visible_probe_lags),
            "reader_error_count": probe_reader_errors,
            "visibility_lag_ms": summarize_latency(visible_probe_lags),
            "probes": probe_results,
        },
    }


def evaluate_p1_l2_checks(
    *,
    prefix_disjoint: bool,
    reader_curated_load: dict[str, Any],
    reader_duckdb_curated: dict[str, Any],
    reader_s3_curated_positive: dict[str, Any],
    reader_raw_load: dict[str, Any],
    reader_duckdb_raw: dict[str, Any],
    writer_raw_load: dict[str, Any],
    reader_s3_raw_denied: dict[str, Any],
    reader_s3_new_raw_denied: dict[str, Any],
    writer_s3_raw_allowed: dict[str, Any],
    writer_init: dict[str, Any],
    writer_append: dict[str, Any],
    reader_duckdb_curated_after_append: dict[str, Any],
    reader_curated_write_denied: dict[str, Any],
) -> dict[str, bool]:
    """Evaluate all four L2 scoped-access checks on Polaris (pure function for fault-injection tests)."""
    check1_passed = (
        bool(reader_curated_load.get("allowed"))
        and bool(reader_duckdb_curated.get("allowed"))
        and int(reader_duckdb_curated.get("row_count") or 0) == 2
        and int(reader_s3_curated_positive.get("http_status") or 0) == 200
        and bool(reader_s3_curated_positive.get("parquet_magic_valid"))
    )
    check2_passed = (
        not bool(reader_raw_load.get("allowed"))
        and int(reader_raw_load.get("http_status") or 0) in (401, 403, 404)
        and not bool(reader_duckdb_raw.get("allowed"))
        and bool(writer_raw_load.get("allowed"))
    )
    check3_passed = (
        not bool(reader_s3_raw_denied.get("allowed"))
        and int(reader_s3_raw_denied.get("http_status") or 0) == 403
        and bool(writer_s3_raw_allowed.get("allowed"))
        and int(writer_s3_raw_allowed.get("http_status") or 0) == 200
        and bool(writer_s3_raw_allowed.get("parquet_magic_valid"))
        and not bool(reader_s3_new_raw_denied.get("allowed"))
        and int(reader_s3_new_raw_denied.get("http_status") or 0) == 403
    )
    check4_passed = (
        int(writer_append["raw"]["row_count"]) == 3
        and int(writer_append["curated"]["row_count"]) == 3
        and writer_append["raw"]["current_snapshot_id"] != writer_init["raw"]["current_snapshot_id"]
        and writer_append["curated"]["current_snapshot_id"]
        != writer_init["curated"]["current_snapshot_id"]
        and bool(reader_duckdb_curated_after_append.get("allowed"))
        and int(reader_duckdb_curated_after_append.get("row_count") or 0) == 3
        and not bool(reader_curated_write_denied.get("allowed"))
        and int(reader_curated_write_denied.get("http_status") or 0) == 403
    )
    return {
        "prefix_disjoint": bool(prefix_disjoint),
        "check1_reader_curated": check1_passed,
        "check2_catalog_denies_raw": check2_passed,
        "check3_s3_denies_raw": check3_passed,
        "check4_writer_writes_both_and_commit_denied": check4_passed,
        "all_passed": (
            bool(prefix_disjoint)
            and check1_passed
            and check2_passed
            and check3_passed
            and check4_passed
        ),
    }


def run_p1_m1(
    *,
    seed_rows: int = 25000,
    toast_rows: int = 100,
    duration_s: float = 180.0,
    rate_ops_s: float = 10.0,
    rng_seed: int = 20261119,
    batch_size: int = 2000,
    probe_interval_s: float = 20.0,
    probe_poll_interval_s: float = 0.5,
    convergence_timeout_s: float = 180.0,
    label: str = "p1-polaris-m1-baseline",
    keep_up: bool = False,
) -> Path:
    """Run the M1 25,000-row 180s baseline on Apache Polaris 1.9.0 with startup and memory telemetry."""
    lab_cfg = load_config()
    polaris_cfg = load_polaris_config()
    run_dir = REPO_ROOT / "runs" / f"{utc_run_stamp()}-{label}"
    run_dir.mkdir(parents=True, exist_ok=True)

    commands_executed: list[str] = []
    non_project_before = list_non_project_containers(lab_cfg.compose_project)

    def logged_cmd(cmd: list[str], *, check: bool = True) -> Any:
        commands_executed.append(_redact_cmd(cmd))
        return run_cmd(cmd, check=check)

    # 1. Fresh stack: clean everything, bring up pg-source + silo, then time Polaris startup alone.
    logged_cmd(
        [*compose_cmd_with_profiles(lab_cfg, ALL_P1_PROFILES), "down", "-v", "--remove-orphans"]
    )
    resources_after_pre_clean = list_project_resources(lab_cfg.compose_project)

    logged_cmd([*compose_cmd_with_profiles(lab_cfg), "up", "-d", "--wait", "pg-source", "silo"])

    polaris_start_t0 = time.monotonic_ns()
    logged_cmd(
        [*compose_cmd_with_profiles(lab_cfg, ("polaris",)), "up", "-d", "--wait", "polaris"]
    )
    polaris_stack_startup_ms = round((time.monotonic_ns() - polaris_start_t0) / 1_000_000.0, 2)

    # Sample idle memory right after Polaris becomes healthy (before catalog/workload activity).
    idle_memory = _sample_idle_memory(
        ["cdclab-polaris-1", "cdclab-polaris-db-1", "cdclab-silo-1"],
        samples=3,
        interval_s=2.0,
    )

    # 2. Bootstrap bucket, Polaris catalog (with STS + kmsUnavailable), namespace, PG fixture,
    #    and verify idempotent second bootstrap.
    bootstrap_first = bootstrap_polaris_m1(lab_cfg, polaris_cfg)
    bootstrap_second = bootstrap_polaris_m1(lab_cfg, polaris_cfg)
    write_json(
        run_dir / "bootstrap.json",
        {
            "first": bootstrap_first,
            "second": bootstrap_second,
            "idempotent": (
                bootstrap_second["catalog"]["action"] == "already_exists"
                and bootstrap_second["namespace"]["action"] == "already_exists"
            ),
        },
    )

    # 3. Seed 25,000 rows BEFORE starting pg2iceberg-polaris.
    with psycopg.connect(lab_cfg.pg_dsn) as pg_conn:
        seed_res = seed_orders(
            pg_conn,
            seed_rows=seed_rows,
            toast_row_count=toast_rows,
            rng_seed=rng_seed,
        )
        pg_pos_after_seed = capture_pg_pipeline_positions(pg_conn)

    # 4. Start pg2iceberg-polaris and wait for initial snapshot completion.
    logged_cmd(
        [
            *compose_cmd_with_profiles(lab_cfg, ("polaris", "polaris-connector")),
            "up",
            "-d",
            "pg2iceberg-polaris",
        ]
    )
    digests = collect_digests_for_profiles(lab_cfg, ("polaris", "polaris-connector"))
    con_v, reader_versions = connect_polaris_iceberg(polaris_cfg)
    con_v.close()

    initial_snapshot_info = wait_for_polaris_connector_and_snapshot(
        lab_cfg, polaris_cfg, timeout_s=180.0
    )

    # 5. Warm-up marker visible in DuckDB via Polaris before measurement window.
    with psycopg.connect(lab_cfg.pg_dsn) as pg_conn:
        warmup_commit_ns, warmup_commit_utc = commit_marker_row(
            pg_conn,
            marker_id=WARMUP_MARKER_ID,
            status="marker_warmup",
            note="p1 polaris warm-up marker before measurement window",
        )
    warmup_vis = wait_for_polaris_marker_visible(
        lab_cfg,
        polaris_cfg,
        WARMUP_MARKER_ID,
        warmup_commit_ns,
        timeout_s=120.0,
        poll_interval_s=0.5,
    )
    if not warmup_vis["visible"]:
        raise RuntimeError(f"Polaris warm-up marker not visible within timeout: {warmup_vis}")

    with psycopg.connect(lab_cfg.pg_dsn, autocommit=True) as pg_conn:
        pg_pos_window_start = capture_pg_pipeline_positions(pg_conn)

    # 6. Run measured 180s paced workload while sampling Polaris memory every 10s.
    stats_sampler = ContainerStatsSampler(
        [
            "cdclab-polaris-1",
            "cdclab-polaris-db-1",
            "cdclab-pg2iceberg-polaris-1",
            "cdclab-silo-1",
        ],
        interval_s=10.0,
        out_jsonl=run_dir / "memory_samples.jsonl",
    )
    stats_sampler.start()
    try:
        workload_summary = run_paced_workload_polaris(
            lab_cfg,
            polaris_cfg,
            seed_res,
            duration_s=duration_s,
            rate_ops_s=rate_ops_s,
            rng_seed=rng_seed,
            ops_jsonl=run_dir / "ops.jsonl",
            probes_jsonl=run_dir / "steady_state_probes.jsonl",
            probe_interval_s=probe_interval_s,
            probe_poll_interval_s=probe_poll_interval_s,
        )
    finally:
        workload_mem_samples = stats_sampler.stop_and_join(timeout_s=30.0)
    workload_memory_summary = _summarize_memory_samples(workload_mem_samples)

    with psycopg.connect(lab_cfg.pg_dsn, autocommit=True) as pg_conn:
        toast_after_workload = verify_toast_storage(pg_conn)
        pg_pos_window_end = capture_pg_pipeline_positions(pg_conn)

    # 7. Commit unique final marker row.
    with psycopg.connect(lab_cfg.pg_dsn) as pg_conn:
        final_commit_ns, final_commit_utc = commit_marker_row(
            pg_conn,
            marker_id=FINAL_MARKER_ID,
            status="marker_final",
            note="p1 polaris final convergence marker",
        )
        pg_pos_after_final_marker = capture_pg_pipeline_positions(pg_conn)

    # 8. Verify exact snapshot convergence on Polaris via DuckDB.
    convergence = verify_polaris_snapshot_convergence(
        lab_cfg,
        polaris_cfg,
        FINAL_MARKER_ID,
        final_commit_ns,
        timeout_s=convergence_timeout_s,
        poll_interval_s=1.0,
        batch_size=batch_size,
    )
    final_polaris_meta = convergence.pop("final_polaris_metadata", None)
    if final_polaris_meta is not None:
        write_json(run_dir / "polaris_table_final.json", final_polaris_meta)

    # 9. Capture final DuckDB snapshots/metadata, PG positions, and logs.
    duckdb_snapshots_final: list[dict[str, Any]] = []
    duckdb_metadata_summary: dict[str, Any] = {}
    con_meta = None
    try:
        con_meta, _ = connect_polaris_iceberg(polaris_cfg)
        duckdb_snapshots_final = read_snapshots(con_meta, namespace=lab_cfg.lakekeeper_namespace)
        meta_rows = read_metadata_files(con_meta, namespace=lab_cfg.lakekeeper_namespace)
        duckdb_metadata_summary = summarize_duckdb_metadata(meta_rows)
        write_json(run_dir / "duckdb_snapshots_final.json", duckdb_snapshots_final)
        write_json(run_dir / "duckdb_metadata_final.json", meta_rows)
    except Exception as exc:
        duckdb_metadata_summary = {"error": f"{type(exc).__name__}: {exc}"}
    finally:
        if con_meta is not None:
            con_meta.close()

    with psycopg.connect(lab_cfg.pg_dsn, autocommit=True) as pg_conn:
        pg_pos_final = capture_pg_pipeline_positions(pg_conn)

    pg2_logs = run_cmd(
        [
            *compose_cmd_with_profiles(lab_cfg, ("polaris", "polaris-connector")),
            "logs",
            "--no-color",
            "pg2iceberg-polaris",
        ],
        check=False,
    ).stdout
    (run_dir / "pg2iceberg_polaris.log").write_text(pg2_logs, encoding="utf-8")

    polaris_logs = run_cmd(
        [*compose_cmd_with_profiles(lab_cfg, ("polaris",)), "logs", "--no-color", "polaris"],
        check=False,
    ).stdout
    (run_dir / "polaris.log").write_text(polaris_logs, encoding="utf-8")
    quarkus_startup_s = _parse_quarkus_startup_s(polaris_logs)

    # 10. Project-only cleanup.
    cleanup_result = project_teardown_and_verify(
        lab_cfg,
        logged_cmd,
        non_project_before,
        profiles=ALL_P1_PROFILES,
        keep_up=keep_up,
    )

    schedule_ok = workload_summary["arrival_schedule"]["schedule_valid"]
    toast_ok = (
        seed_res.toast_verification["both_toasted_rows"] >= len(seed_res.toast_ids)
        and toast_after_workload["both_toasted_rows"] >= len(seed_res.toast_ids)
        and workload_summary["toast_exercise"]["unchanged_toast_updates"] > 0
        and workload_summary["toast_exercise"]["changed_toast_updates"] > 0
    )
    cleanup_ok = keep_up or (
        cleanup_result["all_project_resources_removed"]
        and cleanup_result["non_project_containers_unchanged"]
    )
    if not schedule_ok or not toast_ok:
        outcome = "invalid harness run"
    else:
        outcome = convergence["outcome"]
    passed = (
        outcome == "converged"
        and convergence["passed"]
        and schedule_ok
        and toast_ok
        and cleanup_ok
    )

    summary = {
        "milestone": "P1",
        "subcheck": "M1_baseline",
        "catalog": "apache-polaris-1.9.0",
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
            "convergence_timeout_s": convergence_timeout_s,
        },
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python_version": sys.version,
        },
        "config": {
            "lab": lab_cfg.redacted_summary(),
            "polaris": polaris_cfg.redacted_summary(),
        },
        "commands_executed": commands_executed,
        "container_digests": digests,
        "reader_versions": {
            "duckdb": reader_versions.duckdb_version,
            "extensions": reader_versions.extensions,
        },
        "resources_after_pre_clean": resources_after_pre_clean,
        "telemetry": {
            "polaris_stack_startup_ms": polaris_stack_startup_ms,
            "polaris_quarkus_started_in_s": quarkus_startup_s,
            "idle_memory": idle_memory,
            "workload_memory_sample_count": len(workload_mem_samples),
            "workload_memory_summary": workload_memory_summary,
        },
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
        "subcheck": "M1_baseline",
        "outcome": outcome,
        "passed": passed,
        "polaris_stack_startup_ms": polaris_stack_startup_ms,
        "polaris_quarkus_started_in_s": quarkus_startup_s,
        "polaris_idle_mem_mib": idle_memory["summary"].get("cdclab-polaris-1"),
        "polaris_workload_mem_mib": workload_memory_summary.get("cdclab-polaris-1"),
        "workload_completed_ops": workload_summary["completed_ops"],
        "workload_errors": workload_summary["error_count"],
        "steady_state_probe_lag_ms": workload_summary["steady_state_probes"]["visibility_lag_ms"],
        "final_marker_visibility_ms": convergence["final_marker_visibility_ms"],
        "final_marker_to_verified_match_ms": convergence["final_marker_to_verified_match_ms"],
        "diff_duration_ms": convergence["diff_duration_ms"],
        "verified_snapshot_id": convergence["verified_snapshot_id"],
        "duckdb_final_metadata_summary": duckdb_metadata_summary,
    }
    print(json.dumps(compact, indent=2, default=json_default))
    if not passed:
        raise SystemExit(1)
    return run_dir


def run_p1_l1(
    *,
    seed_rows: int = 30,
    toast_rows: int = 10,
    rng_seed: int = 20261119,
    batch_size: int = 10,
    label: str = "p1-polaris-l1-spark-reader",
    keep_up: bool = False,
) -> Path:
    """Repeat the L1 two-reader check (DuckDB + Spark on the same equality-delete snapshot) on Polaris."""
    lab_cfg = load_config()
    polaris_cfg = load_polaris_config()
    run_dir = REPO_ROOT / "runs" / f"{utc_run_stamp()}-{label}"
    run_dir.mkdir(parents=True, exist_ok=True)

    commands_executed: list[str] = []
    non_project_before = list_non_project_containers(lab_cfg.compose_project)

    def logged_cmd(cmd: list[str], *, check: bool = True) -> Any:
        commands_executed.append(_redact_cmd(cmd))
        return run_cmd(cmd, check=check)

    # 1. Clean fresh start.
    logged_cmd(
        [*compose_cmd_with_profiles(lab_cfg, ALL_P1_PROFILES), "down", "-v", "--remove-orphans"]
    )
    resources_after_pre_clean = list_project_resources(lab_cfg.compose_project)
    logged_cmd(
        [*compose_cmd_with_profiles(lab_cfg, ("polaris",)), "up", "-d", "--wait", "pg-source", "silo", "polaris"]
    )

    # 2. Bootstrap Polaris catalog + PG fixture.
    bootstrap_res = bootstrap_polaris_m1(lab_cfg, polaris_cfg)
    write_json(run_dir / "bootstrap.json", bootstrap_res)

    # 3. Seed 30 rows (identical to L1).
    with psycopg.connect(lab_cfg.pg_dsn) as pg_conn:
        seed_res = seed_orders(
            pg_conn,
            seed_rows=seed_rows,
            toast_row_count=toast_rows,
            rng_seed=rng_seed,
        )

    # 4. Start pg2iceberg-polaris and wait for initial snapshot.
    logged_cmd(
        [
            *compose_cmd_with_profiles(lab_cfg, ("polaris", "polaris-connector")),
            "up",
            "-d",
            "pg2iceberg-polaris",
        ]
    )
    initial_snapshot_info = wait_for_polaris_connector_and_snapshot(
        lab_cfg, polaris_cfg, timeout_s=180.0
    )

    # 5. Apply identical L1 mutations (3 updates + 3 deletes + final marker -> 28 live rows).
    rng_mut = random.Random(rng_seed + 77)
    updated_toast_note = _make_toast_note(rng_mut, 14, rev=1)
    updated_toast_payload = _make_toast_payload_json(rng_mut, 14, rev=1)
    mut_ts = datetime(2026, 11, 19, 22, 0, 0, 123457, tzinfo=timezone.utc)

    with psycopg.connect(lab_cfg.pg_dsn) as pg_conn:
        with pg_conn.cursor() as cur:
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
            cur.execute("DELETE FROM public.orders WHERE id IN (2, 15, 25)")
        pg_conn.commit()
        toast_after_mut = verify_toast_storage(pg_conn)
        marker_commit_ns, marker_commit_utc = commit_marker_row(
            pg_conn,
            marker_id=FINAL_MARKER_ID,
            status="marker_l1_final",
            note="p1 polaris second-reader verification marker",
        )

    # 6. Wait for marker visibility in DuckDB via Polaris and verify live equality deletes.
    vis = wait_for_polaris_marker_visible(
        lab_cfg,
        polaris_cfg,
        FINAL_MARKER_ID,
        marker_commit_ns,
        timeout_s=120.0,
        poll_interval_s=0.5,
    )
    if not vis["visible"] or vis["snapshot_id"] is None:
        raise RuntimeError(f"P1 L1 marker not visible in DuckDB on Polaris: {vis}")
    target_snapshot_id = int(vis["snapshot_id"])

    p_meta = fetch_polaris_table_metadata(
        polaris_cfg,
        catalog_name=polaris_cfg.catalog_name,
        namespace=lab_cfg.lakekeeper_namespace,
        table="orders",
    )
    assert p_meta is not None
    write_json(run_dir / "polaris_table_final.json", p_meta)
    polaris_current_snapshot_id = int((p_meta.get("metadata") or {}).get("current-snapshot-id"))

    con_duck, reader_versions = connect_polaris_iceberg(polaris_cfg)
    try:
        duckdb_snapshots = read_snapshots(con_duck, namespace=lab_cfg.lakekeeper_namespace)
        duckdb_metadata = read_metadata_files(con_duck, namespace=lab_cfg.lakekeeper_namespace)
        desc_cur = con_duck.execute(
            f"DESCRIBE SELECT id, status, amount, updated_at, note, payload, discount "
            f"FROM cdclab.{lab_cfg.lakekeeper_namespace}.orders AT (VERSION => {target_snapshot_id})"
        )
        desc_cols = [d[0] for d in desc_cur.description]
        duckdb_describe = [dict(zip(desc_cols, r)) for r in desc_cur.fetchall()]

        explain_rows = con_duck.execute(
            f"EXPLAIN SELECT id, status, amount, updated_at, note, payload, discount "
            f"FROM cdclab.{lab_cfg.lakekeeper_namespace}.orders AT (VERSION => {target_snapshot_id}) "
            f"ORDER BY id"
        ).fetchall()
        duckdb_explain = [{"key": r[0], "value": r[1]} for r in explain_rows]

        explain_analyze_rows = con_duck.execute(
            f"EXPLAIN ANALYZE SELECT id, status, amount, updated_at, note, payload, discount "
            f"FROM cdclab.{lab_cfg.lakekeeper_namespace}.orders AT (VERSION => {target_snapshot_id}) "
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
    eq_del_check = snapshot_has_equality_deletes(p_meta, target_snapshot_id, duckdb_metadata)
    if not eq_del_check["confirmed"]:
        raise RuntimeError(
            f"Polaris snapshot {target_snapshot_id} does not hold confirmed live equality deletes: {eq_del_check}"
        )

    # 7. Start Spark and read the exact same snapshot ID from Polaris via OAuth2 + vended creds.
    logged_cmd([*compose_cmd_with_profiles(lab_cfg, ("spark",)), "up", "-d", "spark"])
    digests = collect_digests_for_profiles(lab_cfg, ("polaris", "polaris-connector", "spark"))

    spark_rows_container = "/tmp/cdclab-spark/p1_l1_rows.jsonl"
    spark_meta_container = "/tmp/cdclab-spark/p1_l1_meta.json"
    spark_t0 = time.monotonic_ns()
    spark_proc = logged_cmd(
        [
            *compose_cmd_with_profiles(lab_cfg, ("spark",)),
            "exec",
            "-T",
            "spark",
            "/opt/spark/bin/spark-submit",
            "--master",
            "local[2]",
            "/opt/cdclab/scripts/read_snapshot.py",
            "--catalog-name",
            polaris_cfg.catalog_name,
            "--catalog-uri",
            polaris_cfg.container_catalog_url,
            "--warehouse",
            polaris_cfg.catalog_name,
            "--namespace",
            lab_cfg.lakekeeper_namespace,
            "--table",
            "orders",
            "--snapshot-id",
            str(target_snapshot_id),
            "--oauth2-server-uri",
            polaris_cfg.container_token_url,
            "--client-id",
            polaris_cfg.root_client_id,
            "--client-secret",
            polaris_cfg.root_client_secret,
            "--oauth2-scope",
            "PRINCIPAL_ROLE:ALL",
            "--rows-out",
            spark_rows_container,
            "--meta-out",
            spark_meta_container,
        ]
    )
    spark_read_ms = (time.monotonic_ns() - spark_t0) / 1_000_000.0
    (run_dir / "spark_submit.stdout.log").write_text(spark_proc.stdout, encoding="utf-8")
    (run_dir / "spark_submit.stderr.log").write_text(spark_proc.stderr, encoding="utf-8")

    spark_rows_host = run_dir / "spark_rows.jsonl"
    spark_meta_host = run_dir / "spark_metadata.json"
    rows_out = run_cmd(
        [*compose_cmd_with_profiles(lab_cfg, ("spark",)), "exec", "-T", "spark", "cat", spark_rows_container]
    ).stdout
    meta_out = run_cmd(
        [*compose_cmd_with_profiles(lab_cfg, ("spark",)), "exec", "-T", "spark", "cat", spark_meta_container]
    ).stdout
    spark_rows_host.write_text(rows_out, encoding="utf-8")
    spark_meta_host.write_text(meta_out, encoding="utf-8")
    spark_meta = json.loads(meta_out)

    # 8. Verify schemas and all three bounded-batch exact row comparisons.
    iceberg_schema_check = verify_iceberg_schema(p_meta)
    duckdb_schema_check = verify_duckdb_schema(duckdb_describe)
    spark_schema_check = verify_spark_schema(spark_meta["spark_schema"])

    con_cmp, _ = connect_polaris_iceberg(polaris_cfg)
    try:
        with psycopg.connect(lab_cfg.pg_dsn) as pg_conn:
            pg_conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cmp_pg_vs_duckdb = compare_ordered_streams(
                stream_postgres_orders(pg_conn, batch_size=batch_size),
                stream_duckdb_orders_at_snapshot(
                    con_cmp,
                    target_snapshot_id,
                    namespace=lab_cfg.lakekeeper_namespace,
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
                namespace=lab_cfg.lakekeeper_namespace,
                batch_size=batch_size,
            ),
            stream_spark_orders_ndjson(spark_rows_host, batch_size=batch_size),
        )
    finally:
        con_cmp.close()

    with psycopg.connect(lab_cfg.pg_dsn, autocommit=True) as pg_conn:
        pg_pos_final = capture_pg_pipeline_positions(pg_conn)

    pg2_logs = run_cmd(
        [
            *compose_cmd_with_profiles(lab_cfg, ("polaris", "polaris-connector")),
            "logs",
            "--no-color",
            "pg2iceberg-polaris",
        ],
        check=False,
    ).stdout
    (run_dir / "pg2iceberg_polaris.log").write_text(pg2_logs, encoding="utf-8")

    cleanup_result = project_teardown_and_verify(
        lab_cfg,
        logged_cmd,
        non_project_before,
        profiles=ALL_P1_PROFILES,
        keep_up=keep_up,
    )

    spark_delete_files = [
        f for f in (spark_meta.get("files") or []) if int(f.get("content", 0)) == 2
    ]
    if not spark_delete_files:
        spark_delete_files = [
            f for f in (spark_meta.get("all_delete_files") or []) if int(f.get("content", 0)) == 2
        ]

    spark_current_snapshot_id = spark_meta.get("spark_current_snapshot_id")
    snapshot_ids_match = (
        target_snapshot_id == polaris_current_snapshot_id
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
        cmp_pg_vs_duckdb.passed and cmp_pg_vs_spark.passed and cmp_duckdb_vs_spark.passed
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
        "milestone": "P1",
        "subcheck": "L1_two_reader",
        "catalog": "apache-polaris-1.9.0",
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
        "config": {
            "lab": lab_cfg.redacted_summary(),
            "polaris": polaris_cfg.redacted_summary(),
        },
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
            "polaris_current_snapshot_id": polaris_current_snapshot_id,
            "duckdb_current_snapshot_id": duckdb_current_snapshot_id,
            "spark_current_snapshot_id": spark_current_snapshot_id,
            "spark_requested_snapshot_found": spark_meta.get("requested_snapshot_found"),
            "snapshot_ids_match": snapshot_ids_match,
            "snapshot_info": extract_iceberg_snapshot_info(p_meta, target_snapshot_id),
        },
        "equality_delete_evidence": {
            "polaris_and_duckdb": eq_del_check,
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
        "pipeline_positions_final": pg_pos_final,
        "cleanup": cleanup_result,
    }
    write_json(run_dir / "summary.json", summary)

    compact = {
        "run_dir": str(run_dir.relative_to(REPO_ROOT).as_posix()),
        "subcheck": "L1_two_reader",
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


def _run_spark_polaris_writer(
    lab_cfg: LabConfig,
    polaris_cfg: PolarisConfig,
    logged_cmd,
    *,
    catalog_name: str,
    client_id: str,
    client_secret: str,
    scope: str,
    mode: str,
    out_container_path: str,
    out_host_path: Path,
) -> dict[str, Any]:
    proc = logged_cmd(
        [
            *compose_cmd_with_profiles(lab_cfg, ("spark",)),
            "exec",
            "-T",
            "spark",
            "/opt/spark/bin/spark-submit",
            "--master",
            "local[2]",
            "/opt/cdclab/scripts/authz_writer.py",
            "--mode",
            mode,
            "--catalog-name",
            catalog_name,
            "--catalog-uri",
            polaris_cfg.container_catalog_url,
            "--warehouse",
            catalog_name,
            "--oauth2-server-uri",
            polaris_cfg.container_token_url,
            "--client-id",
            client_id,
            "--client-secret",
            client_secret,
            "--oauth2-scope",
            scope,
            "--out-json",
            out_container_path,
        ]
    )
    out_host_path.with_suffix(".stdout.log").write_text(proc.stdout, encoding="utf-8")
    out_host_path.with_suffix(".stderr.log").write_text(proc.stderr, encoding="utf-8")
    raw = run_cmd(
        [*compose_cmd_with_profiles(lab_cfg, ("spark",)), "exec", "-T", "spark", "cat", out_container_path]
    ).stdout
    out_host_path.write_text(raw, encoding="utf-8")
    return json.loads(raw)


def run_p1_l2(
    *,
    label: str = "p1-polaris-l2-scoped-access",
    keep_up: bool = False,
) -> Path:
    """Repeat the L2 scoped-access checks on Apache Polaris 1.9.0 using Polaris's built-in RBAC + STS."""
    lab_cfg = load_config()
    polaris_cfg = load_polaris_config()
    authz_catalog = polaris_cfg.authz_catalog_name
    run_dir = REPO_ROOT / "runs" / f"{utc_run_stamp()}-{label}"
    run_dir.mkdir(parents=True, exist_ok=True)

    commands_executed: list[str] = []
    non_project_before = list_non_project_containers(lab_cfg.compose_project)

    def logged_cmd(cmd: list[str], *, check: bool = True) -> Any:
        commands_executed.append(_redact_cmd(cmd))
        return run_cmd(cmd, check=check)

    # 1. Fresh stack: silo + polaris (built-in OAuth2 + RBAC, no Keycloak/OpenFGA) + spark writer.
    logged_cmd(
        [*compose_cmd_with_profiles(lab_cfg, ALL_P1_PROFILES), "down", "-v", "--remove-orphans"]
    )
    resources_after_pre_clean = list_project_resources(lab_cfg.compose_project)

    logged_cmd([*compose_cmd_with_profiles(lab_cfg), "up", "-d", "--wait", "silo"])
    polaris_t0 = time.monotonic_ns()
    logged_cmd(
        [
            *compose_cmd_with_profiles(lab_cfg, ("polaris", "spark")),
            "up",
            "-d",
            "--wait",
            "polaris",
            "spark",
        ]
    )
    polaris_authz_startup_ms = round((time.monotonic_ns() - polaris_t0) / 1_000_000.0, 2)
    digests = collect_digests_for_profiles(lab_cfg, ("polaris", "spark"))

    bucket_res = ensure_bucket(lab_cfg)

    # 2. Authenticate as built-in root admin and create authz catalog with Silo STS + kmsUnavailable.
    admin_token, admin_tok_info = fetch_polaris_token(
        polaris_cfg.host_token_url,
        polaris_cfg.root_client_id,
        polaris_cfg.root_client_secret,
        scope="PRINCIPAL_ROLE:ALL",
        realm=polaris_cfg.realm,
    )
    cat_res = create_s3_catalog(
        polaris_cfg,
        lab_cfg,
        admin_token,
        catalog_name=authz_catalog,
        key_prefix="polaris-authz",
        sts_unavailable=False,
        kms_unavailable=True,
    )

    # 3. Create distinct writer and reader principals using Polaris's 4-tier RBAC model:
    #    Principal -> PrincipalRole -> CatalogRole -> Grants.
    writer_created = create_principal(polaris_cfg, admin_token, name="cdclab_writer")
    reader_created = create_principal(polaris_cfg, admin_token, name="cdclab_reader")
    writer_cid = str(writer_created["credentials"]["clientId"])
    writer_csec = str(writer_created["credentials"]["clientSecret"])
    reader_cid = str(reader_created["credentials"]["clientId"])
    reader_csec = str(reader_created["credentials"]["clientSecret"])

    create_principal_role(polaris_cfg, admin_token, name="writer_pr")
    create_principal_role(polaris_cfg, admin_token, name="reader_pr")
    assign_principal_role(
        polaris_cfg, admin_token, principal_name="cdclab_writer", principal_role_name="writer_pr"
    )
    assign_principal_role(
        polaris_cfg, admin_token, principal_name="cdclab_reader", principal_role_name="reader_pr"
    )

    create_catalog_role(
        polaris_cfg, admin_token, catalog_name=authz_catalog, role_name="writer_cr"
    )
    create_catalog_role(
        polaris_cfg, admin_token, catalog_name=authz_catalog, role_name="reader_cr"
    )
    assign_catalog_role_to_principal_role(
        polaris_cfg,
        admin_token,
        principal_role_name="writer_pr",
        catalog_name=authz_catalog,
        catalog_role_name="writer_cr",
    )
    assign_catalog_role_to_principal_role(
        polaris_cfg,
        admin_token,
        principal_role_name="reader_pr",
        catalog_name=authz_catalog,
        catalog_role_name="reader_cr",
    )

    # Grant writer CATALOG_MANAGE_CONTENT on authz_catalog (equivalent to Lakekeeper create + modify).
    writer_grant = add_grant_to_catalog_role(
        polaris_cfg,
        admin_token,
        catalog_name=authz_catalog,
        catalog_role_name="writer_cr",
        grant={"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"},
    )

    writer_token, writer_tok_info = fetch_polaris_token(
        polaris_cfg.host_token_url,
        writer_cid,
        writer_csec,
        scope="PRINCIPAL_ROLE:writer_pr",
        realm=polaris_cfg.realm,
    )
    reader_token, reader_tok_info = fetch_polaris_token(
        polaris_cfg.host_token_url,
        reader_cid,
        reader_csec,
        scope="PRINCIPAL_ROLE:reader_pr",
        realm=polaris_cfg.realm,
    )

    admin_sub = str((admin_tok_info.get("claims") or {}).get("sub") or "root")
    writer_sub = str((writer_tok_info.get("claims") or {}).get("sub") or "cdclab_writer")
    reader_sub = str((reader_tok_info.get("claims") or {}).get("sub") or "cdclab_reader")
    if len({admin_sub, writer_sub, reader_sub}) != 3:
        raise RuntimeError(
            f"Expected 3 distinct Polaris principals, got {admin_sub}, {writer_sub}, {reader_sub}"
        )

    identities_evidence = {
        "admin": {
            "principal_name": "root",
            "principal_role": "service_admin",
            "token_sub": admin_sub,
            "principal_id": (admin_tok_info.get("claims") or {}).get("principalId"),
            "scope": (admin_tok_info.get("claims") or {}).get("scope"),
        },
        "writer": {
            "principal_name": "cdclab_writer",
            "principal_role": "writer_pr",
            "catalog_role": "writer_cr",
            "token_sub": writer_sub,
            "principal_id": (writer_tok_info.get("claims") or {}).get("principalId"),
            "scope": (writer_tok_info.get("claims") or {}).get("scope"),
        },
        "reader": {
            "principal_name": "cdclab_reader",
            "principal_role": "reader_pr",
            "catalog_role": "reader_cr",
            "token_sub": reader_sub,
            "principal_id": (reader_tok_info.get("claims") or {}).get("principalId"),
            "scope": (reader_tok_info.get("claims") or {}).get("scope"),
        },
    }

    # 4. Writer creates raw.orders and curated.orders in distinct S3 prefixes and inserts 2 rows each.
    writer_init = _run_spark_polaris_writer(
        lab_cfg,
        polaris_cfg,
        logged_cmd,
        catalog_name=authz_catalog,
        client_id=writer_cid,
        client_secret=writer_csec,
        scope="PRINCIPAL_ROLE:writer_pr",
        mode="init",
        out_container_path="/tmp/cdclab-spark/p1_l2_init.json",
        out_host_path=run_dir / "spark_writer_init.json",
    )

    writer_raw_load = rest_load_table_polaris(
        polaris_cfg,
        catalog_name=authz_catalog,
        namespace="raw",
        table="orders",
        access_token=writer_token,
    )
    writer_curated_load = rest_load_table_polaris(
        polaris_cfg,
        catalog_name=authz_catalog,
        namespace="curated",
        table="orders",
        access_token=writer_token,
    )
    if not writer_raw_load["allowed"] or not writer_curated_load["allowed"]:
        raise RuntimeError(
            f"Polaris writer could not load its own tables: "
            f"raw={writer_raw_load['http_status']} curated={writer_curated_load['http_status']}"
        )

    raw_meta = writer_raw_load["body"]["metadata"]
    curated_meta = writer_curated_load["body"]["metadata"]
    raw_table_uuid = str(raw_meta["table-uuid"])
    curated_table_uuid = str(curated_meta["table-uuid"])
    raw_location = str(raw_meta["location"])
    curated_location = str(curated_meta["location"])
    prefix_check = verify_distinct_storage_prefixes(raw_location, curated_location)
    if not prefix_check["distinct_and_disjoint"]:
        raise RuntimeError(f"Polaris table storage prefixes are not disjoint: {prefix_check}")

    raw_data_file_uri = str(writer_init["raw"]["data_files"][0]["file_path"])
    curated_data_file_uri = str(writer_init["curated"]["data_files"][0]["file_path"])

    # 5. Grant reader TABLE_READ_DATA on curated.orders ONLY (zero grants on raw).
    reader_grant = add_grant_to_catalog_role(
        polaris_cfg,
        admin_token,
        catalog_name=authz_catalog,
        catalog_role_name="reader_cr",
        grant={
            "type": "table",
            "namespace": ["curated"],
            "tableName": "orders",
            "privilege": "TABLE_READ_DATA",
        },
    )

    # 6. Check 1: Reader loads curated.orders via REST, scans in DuckDB, and fetches Parquet via Silo.
    reader_curated_load = rest_load_table_polaris(
        polaris_cfg,
        catalog_name=authz_catalog,
        namespace="curated",
        table="orders",
        access_token=reader_token,
    )
    if not reader_curated_load["allowed"]:
        raise RuntimeError(f"Polaris reader failed to load curated.orders: {reader_curated_load}")
    reader_curated_creds = extract_vended_s3_credentials(reader_curated_load["body"])
    writer_raw_creds = extract_vended_s3_credentials(writer_raw_load["body"])

    reader_duckdb_curated = duckdb_scan_polaris_table(
        polaris_cfg,
        catalog_name=authz_catalog,
        client_id=reader_cid,
        client_secret=reader_csec,
        scope="PRINCIPAL_ROLE:reader_pr",
        namespace="curated",
        table="orders",
    )
    reader_s3_curated_positive = sigv4_s3_get(
        endpoint=reader_curated_creds["endpoint"] or lab_cfg.silo_advertised_endpoint,
        s3_uri=curated_data_file_uri,
        region=reader_curated_creds["region"],
        access_key=reader_curated_creds["access_key"],
        secret_key=reader_curated_creds["secret_key"],
        session_token=reader_curated_creds["session_token"],
    )

    # 7. Check 2: Catalog denies raw.orders to reader (REST + DuckDB), while writer succeeds.
    reader_raw_load = rest_load_table_polaris(
        polaris_cfg,
        catalog_name=authz_catalog,
        namespace="raw",
        table="orders",
        access_token=reader_token,
    )
    reader_duckdb_raw = duckdb_scan_polaris_table(
        polaris_cfg,
        catalog_name=authz_catalog,
        client_id=reader_cid,
        client_secret=reader_csec,
        scope="PRINCIPAL_ROLE:reader_pr",
        namespace="raw",
        table="orders",
    )

    # 8. Check 3: Reader's curated-vended STS creds are refused by Silo on raw Parquet file (403),
    #    while writer's raw-vended STS creds fetch the exact same raw Parquet file (200).
    reader_s3_raw_denied = sigv4_s3_get(
        endpoint=reader_curated_creds["endpoint"] or lab_cfg.silo_advertised_endpoint,
        s3_uri=raw_data_file_uri,
        region=reader_curated_creds["region"],
        access_key=reader_curated_creds["access_key"],
        secret_key=reader_curated_creds["secret_key"],
        session_token=reader_curated_creds["session_token"],
    )
    writer_s3_raw_allowed = sigv4_s3_get(
        endpoint=writer_raw_creds["endpoint"] or lab_cfg.silo_advertised_endpoint,
        s3_uri=raw_data_file_uri,
        region=writer_raw_creds["region"],
        access_key=writer_raw_creds["access_key"],
        secret_key=writer_raw_creds["secret_key"],
        session_token=writer_raw_creds["session_token"],
    )

    # 9. Check 4: Writer appends row 3 to both raw and curated; reader sees 3 rows in curated;
    #    reader commit on curated is refused (403); reader vended creds refused on new raw Parquet.
    writer_append = _run_spark_polaris_writer(
        lab_cfg,
        polaris_cfg,
        logged_cmd,
        catalog_name=authz_catalog,
        client_id=writer_cid,
        client_secret=writer_csec,
        scope="PRINCIPAL_ROLE:writer_pr",
        mode="append",
        out_container_path="/tmp/cdclab-spark/p1_l2_append.json",
        out_host_path=run_dir / "spark_writer_append.json",
    )
    reader_duckdb_curated_after_append = duckdb_scan_polaris_table(
        polaris_cfg,
        catalog_name=authz_catalog,
        client_id=reader_cid,
        client_secret=reader_csec,
        scope="PRINCIPAL_ROLE:reader_pr",
        namespace="curated",
        table="orders",
    )
    reader_curated_write_denied = rest_attempt_table_commit_polaris(
        polaris_cfg,
        prefix=str(reader_curated_load["prefix"]),
        namespace="curated",
        table="orders",
        access_token=reader_token,
    )

    new_raw_files = [
        str(f["file_path"])
        for f in writer_append["raw"]["data_files"]
        if str(f["file_path"]) != raw_data_file_uri
    ]
    new_raw_file_uri = new_raw_files[0] if new_raw_files else raw_data_file_uri
    reader_s3_new_raw_denied = sigv4_s3_get(
        endpoint=reader_curated_creds["endpoint"] or lab_cfg.silo_advertised_endpoint,
        s3_uri=new_raw_file_uri,
        region=reader_curated_creds["region"],
        access_key=reader_curated_creds["access_key"],
        secret_key=reader_curated_creds["secret_key"],
        session_token=reader_curated_creds["session_token"],
    )

    write_json(
        run_dir / "load_table_responses.json",
        {
            "writer_raw": {
                "http_status": writer_raw_load["http_status"],
                "url": writer_raw_load["url"],
                "redacted": redact_load_table_response(writer_raw_load["body"]),
            },
            "writer_curated": {
                "http_status": writer_curated_load["http_status"],
                "url": writer_curated_load["url"],
                "redacted": redact_load_table_response(writer_curated_load["body"]),
            },
            "reader_curated": {
                "http_status": reader_curated_load["http_status"],
                "url": reader_curated_load["url"],
                "redacted": redact_load_table_response(reader_curated_load["body"]),
            },
            "reader_raw_denied": {
                "http_status": reader_raw_load["http_status"],
                "url": reader_raw_load["url"],
                "body": reader_raw_load["body"],
                "raw_body": reader_raw_load["raw_body"],
            },
        },
    )

    polaris_logs = run_cmd(
        [*compose_cmd_with_profiles(lab_cfg, ("polaris",)), "logs", "--no-color", "polaris"],
        check=False,
    ).stdout
    (run_dir / "polaris.log").write_text(polaris_logs, encoding="utf-8")

    cleanup_result = project_teardown_and_verify(
        lab_cfg,
        logged_cmd,
        non_project_before,
        profiles=ALL_P1_PROFILES,
        keep_up=keep_up,
    )

    eval_res = evaluate_p1_l2_checks(
        prefix_disjoint=prefix_check["distinct_and_disjoint"],
        reader_curated_load=reader_curated_load,
        reader_duckdb_curated=reader_duckdb_curated,
        reader_s3_curated_positive=reader_s3_curated_positive,
        reader_raw_load=reader_raw_load,
        reader_duckdb_raw=reader_duckdb_raw,
        writer_raw_load=writer_raw_load,
        reader_s3_raw_denied=reader_s3_raw_denied,
        reader_s3_new_raw_denied=reader_s3_new_raw_denied,
        writer_s3_raw_allowed=writer_s3_raw_allowed,
        writer_init=writer_init,
        writer_append=writer_append,
        reader_duckdb_curated_after_append=reader_duckdb_curated_after_append,
        reader_curated_write_denied=reader_curated_write_denied,
    )
    cleanup_ok = keep_up or (
        cleanup_result["all_project_resources_removed"]
        and cleanup_result["non_project_containers_unchanged"]
    )
    passed = eval_res["all_passed"] and cleanup_ok
    outcome = "converged" if passed else "mismatch at deadline"

    summary = {
        "milestone": "P1",
        "subcheck": "L2_scoped_access",
        "catalog": "apache-polaris-1.9.0",
        "label": label,
        "outcome": outcome,
        "passed": passed,
        "completed_at_utc": utc_now_iso(),
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python_version": sys.version,
        },
        "config": {
            "lab": lab_cfg.redacted_summary(),
            "polaris": polaris_cfg.redacted_summary(),
        },
        "commands_executed": commands_executed,
        "container_digests": digests,
        "resources_after_pre_clean": resources_after_pre_clean,
        "polaris_authz_startup_ms": polaris_authz_startup_ms,
        "bucket": bucket_res,
        "catalog_created": cat_res,
        "identities": identities_evidence,
        "grants": {
            "writer_grant": writer_grant,
            "reader_grant": reader_grant,
            "reader_raw_grants": [],
        },
        "tables": {
            "raw": {
                "table_uuid": raw_table_uuid,
                "location": raw_location,
                "init_snapshot_id": writer_init["raw"]["current_snapshot_id"],
                "append_snapshot_id": writer_append["raw"]["current_snapshot_id"],
                "init_data_file": raw_data_file_uri,
                "append_data_file": new_raw_file_uri,
            },
            "curated": {
                "table_uuid": curated_table_uuid,
                "location": curated_location,
                "init_snapshot_id": writer_init["curated"]["current_snapshot_id"],
                "append_snapshot_id": writer_append["curated"]["current_snapshot_id"],
                "init_data_file": curated_data_file_uri,
            },
            "prefix_disjointness": prefix_check,
        },
        "checks": {
            "evaluation": eval_res,
            "check1_reader_loads_and_scans_curated": {
                "passed": eval_res["check1_reader_curated"],
                "rest_load_table_status": reader_curated_load["http_status"],
                "vended_credential_prefix": reader_curated_creds["prefix"],
                "vended_credential_endpoint": reader_curated_creds["endpoint"],
                "duckdb_scan": reader_duckdb_curated,
                "direct_s3_curated_positive_control": reader_s3_curated_positive,
            },
            "check2_catalog_denies_raw_to_reader": {
                "passed": eval_res["check2_catalog_denies_raw"],
                "reader_rest_load_table": {
                    "url": reader_raw_load["url"],
                    "http_status": reader_raw_load["http_status"],
                    "body": reader_raw_load["body"],
                    "raw_body": reader_raw_load["raw_body"],
                },
                "reader_duckdb_scan_raw": reader_duckdb_raw,
                "writer_rest_load_table_control": {
                    "url": writer_raw_load["url"],
                    "http_status": writer_raw_load["http_status"],
                    "allowed": writer_raw_load["allowed"],
                    "vended_credential_prefix": writer_raw_creds["prefix"],
                },
            },
            "check3_reader_vended_creds_denied_raw_s3": {
                "passed": eval_res["check3_s3_denies_raw"],
                "reader_vended_creds_on_raw_init_file": reader_s3_raw_denied,
                "reader_vended_creds_on_raw_append_file": reader_s3_new_raw_denied,
                "writer_vended_creds_on_raw_init_file_control": writer_s3_raw_allowed,
            },
            "check4_writer_writes_both_and_reader_write_denied": {
                "passed": eval_res["check4_writer_writes_both_and_commit_denied"],
                "writer_append_raw_rows": writer_append["raw"]["row_count"],
                "writer_append_curated_rows": writer_append["curated"]["row_count"],
                "reader_duckdb_curated_after_append": reader_duckdb_curated_after_append,
                "reader_commit_curated_denied": reader_curated_write_denied,
            },
        },
        "cleanup": cleanup_result,
    }
    write_json(run_dir / "summary.json", summary)

    compact = {
        "run_dir": str(run_dir.relative_to(REPO_ROOT).as_posix()),
        "subcheck": "L2_scoped_access",
        "outcome": outcome,
        "passed": passed,
        "evaluation": eval_res,
        "reader_raw_http_status": reader_raw_load["http_status"],
        "reader_raw_s3_status": reader_s3_raw_denied["http_status"],
        "writer_raw_s3_status": writer_s3_raw_allowed["http_status"],
        "reader_commit_curated_status": reader_curated_write_denied["http_status"],
    }
    print(json.dumps(compact, indent=2, default=json_default))
    if not passed:
        raise SystemExit(1)
    return run_dir


def run_lakekeeper_telemetry(
    *,
    label: str = "p1-lakekeeper-telemetry",
) -> Path:
    """Measure Lakekeeper unauth & authz startup time and idle memory only (never re-runs M1/L1/L2)."""
    lab_cfg = load_config()
    run_dir = REPO_ROOT / "runs" / f"{utc_run_stamp()}-{label}"
    run_dir.mkdir(parents=True, exist_ok=True)

    commands_executed: list[str] = []
    non_project_before = list_non_project_containers(lab_cfg.compose_project)

    def logged_cmd(cmd: list[str], *, check: bool = True) -> Any:
        commands_executed.append(_redact_cmd(cmd))
        return run_cmd(cmd, check=check)

    # 1. Measure unauth Lakekeeper stack (lakekeeper-db + lakekeeper-migrate + lakekeeper) after silo is healthy.
    logged_cmd(
        [*compose_cmd_with_profiles(lab_cfg, ALL_P1_PROFILES), "down", "-v", "--remove-orphans"]
    )
    logged_cmd([*compose_cmd_with_profiles(lab_cfg), "up", "-d", "--wait", "silo"])
    t0_unauth = time.monotonic_ns()
    logged_cmd([*compose_cmd_with_profiles(lab_cfg), "up", "-d", "--wait", "lakekeeper"])
    lk_unauth_startup_ms = round((time.monotonic_ns() - t0_unauth) / 1_000_000.0, 2)
    lk_unauth_idle = _sample_idle_memory(
        ["cdclab-lakekeeper-1", "cdclab-lakekeeper-db-1", "cdclab-silo-1"],
        samples=3,
        interval_s=2.0,
    )

    # 2. Measure authz Lakekeeper stack (keycloak + openfga-db + openfga + lakekeeper-authz-db + lakekeeper-authz).
    logged_cmd(
        [*compose_cmd_with_profiles(lab_cfg, ALL_P1_PROFILES), "down", "-v", "--remove-orphans"]
    )
    logged_cmd([*compose_cmd_with_profiles(lab_cfg), "up", "-d", "--wait", "silo"])
    t0_authz = time.monotonic_ns()
    logged_cmd(
        [
            *compose_cmd_with_profiles(lab_cfg, ("authz",)),
            "up",
            "-d",
            "--wait",
            "keycloak",
            "openfga",
            "lakekeeper-authz",
        ]
    )
    lk_authz_startup_ms = round((time.monotonic_ns() - t0_authz) / 1_000_000.0, 2)
    lk_authz_idle = _sample_idle_memory(
        [
            "cdclab-lakekeeper-authz-1",
            "cdclab-lakekeeper-authz-db-1",
            "cdclab-keycloak-1",
            "cdclab-openfga-1",
            "cdclab-openfga-db-1",
            "cdclab-silo-1",
        ],
        samples=3,
        interval_s=2.0,
    )

    cleanup_result = project_teardown_and_verify(
        lab_cfg,
        logged_cmd,
        non_project_before,
        profiles=ALL_P1_PROFILES,
        keep_up=False,
    )

    summary = {
        "milestone": "P1",
        "subcheck": "lakekeeper_startup_and_idle_telemetry",
        "note": "Measures startup time and idle container memory only; M1/L1/L2 Lakekeeper checks were NOT re-run.",
        "completed_at_utc": utc_now_iso(),
        "commands_executed": commands_executed,
        "lakekeeper_unauth": {
            "startup_ms": lk_unauth_startup_ms,
            "idle_memory": lk_unauth_idle,
        },
        "lakekeeper_authz": {
            "startup_ms": lk_authz_startup_ms,
            "idle_memory": lk_authz_idle,
        },
        "cleanup": cleanup_result,
    }
    write_json(run_dir / "summary.json", summary)
    print(
        json.dumps(
            {
                "run_dir": str(run_dir.relative_to(REPO_ROOT).as_posix()),
                "lakekeeper_unauth_startup_ms": lk_unauth_startup_ms,
                "lakekeeper_unauth_idle_summary": lk_unauth_idle["summary"],
                "lakekeeper_authz_startup_ms": lk_authz_startup_ms,
                "lakekeeper_authz_idle_summary": lk_authz_idle["summary"],
                "cleanup_ok": (
                    cleanup_result["all_project_resources_removed"]
                    and cleanup_result["non_project_containers_unchanged"]
                ),
            },
            indent=2,
            default=json_default,
        )
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Milestone P1 Apache Polaris checks (M1 baseline, L1 two-reader, L2 scoped access)."
    )
    parser.add_argument(
        "--mode",
        choices=("all", "m1", "l1", "l2", "telemetry-lakekeeper"),
        default="all",
    )
    parser.add_argument("--seed-rows", type=int, default=25000)
    parser.add_argument("--toast-rows", type=int, default=100)
    parser.add_argument("--duration-s", type=float, default=180.0)
    parser.add_argument("--rate-ops-s", type=float, default=10.0)
    parser.add_argument("--rng-seed", type=int, default=20261119)
    parser.add_argument("--keep-up", action="store_true")
    args = parser.parse_args()

    if args.mode in ("m1", "all"):
        run_p1_m1(
            seed_rows=args.seed_rows,
            toast_rows=args.toast_rows,
            duration_s=args.duration_s,
            rate_ops_s=args.rate_ops_s,
            rng_seed=args.rng_seed,
            keep_up=args.keep_up and args.mode == "m1",
        )
    if args.mode in ("l1", "all"):
        run_p1_l1(
            rng_seed=args.rng_seed,
            keep_up=args.keep_up and args.mode == "l1",
        )
    if args.mode in ("l2", "all"):
        run_p1_l2(keep_up=args.keep_up and args.mode == "l2")
    if args.mode in ("telemetry-lakekeeper", "all"):
        run_lakekeeper_telemetry()


if __name__ == "__main__":
    main()
