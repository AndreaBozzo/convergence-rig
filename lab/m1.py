"""Milestone M1 end-to-end runner: baseline 25,000-row run with TOAST, lag probes, and exact diff."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import platform
import sys
from typing import Any

import psycopg

from lab.bootstrap import bootstrap_all
from lab.config import REPO_ROOT, load_config
from lab.pipeline import (
    capture_pg_pipeline_positions,
    collect_container_digests,
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
from lab.reader import connect_iceberg, read_metadata_files, read_snapshots
from lab.workload import (
    FINAL_MARKER_ID,
    WARMUP_MARKER_ID,
    commit_marker_row,
    run_paced_workload,
    seed_orders,
    verify_toast_storage,
)


def summarize_duckdb_metadata(rows: list[dict[str, Any]]) -> dict[str, Any]:
    data_files = 0
    eq_delete_files = 0
    pos_delete_files = 0
    data_records = 0
    eq_delete_records = 0
    pos_delete_records = 0
    for r in rows:
        status = str(r.get("status", "")).upper()
        if status == "DELETED":
            continue
        content = str(r.get("content", "")).upper()
        rec = int(r.get("record_count") or 0)
        if "EQUALITY" in content:
            eq_delete_files += 1
            eq_delete_records += rec
        elif "POSITION" in content:
            pos_delete_files += 1
            pos_delete_records += rec
        else:
            data_files += 1
            data_records += rec
    return {
        "total_live_entries": data_files + eq_delete_files + pos_delete_files,
        "live_data_files": data_files,
        "live_equality_delete_files": eq_delete_files,
        "live_position_delete_files": pos_delete_files,
        "live_data_records": data_records,
        "live_equality_delete_records": eq_delete_records,
        "live_position_delete_records": pos_delete_records,
    }


def run_m1_baseline(
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
    label: str = "m1-baseline",
    keep_up: bool = False,
) -> Path:
    cfg = load_config()
    run_dir = REPO_ROOT / "runs" / f"{utc_run_stamp()}-{label}"
    run_dir.mkdir(parents=True, exist_ok=True)

    commands_executed: list[str] = []
    non_project_before = list_non_project_containers(cfg.compose_project)

    def logged_cmd(cmd: list[str], *, check: bool = True) -> Any:
        commands_executed.append(" ".join(cmd))
        return run_cmd(cmd, check=check)

    # 1. Clean fresh start (infra only, before connector).
    logged_cmd([*cfg.compose_base_cmd(include_connector=True), "down", "-v", "--remove-orphans"])
    resources_after_pre_clean = list_project_resources(cfg.compose_project)
    logged_cmd([*cfg.compose_base_cmd(include_connector=False), "up", "-d", "--wait"])

    # 2. Bootstrap bucket, Lakekeeper, warehouse, and public.orders DDL.
    bootstrap_res = bootstrap_all(cfg)
    write_json(run_dir / "bootstrap.json", bootstrap_res)

    # 3. Seed public.orders BEFORE starting pg2iceberg so initial snapshot runs on full dataset.
    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        seed_res = seed_orders(
            pg_conn,
            seed_rows=seed_rows,
            toast_row_count=toast_rows,
            rng_seed=rng_seed,
        )
        pg_pos_after_seed = capture_pg_pipeline_positions(pg_conn)

    # 4. Start pg2iceberg and wait for initial snapshot completion.
    logged_cmd([*cfg.compose_base_cmd(include_connector=True), "up", "-d", "pg2iceberg"])
    digests = collect_container_digests(cfg)
    con_v, reader_versions = connect_iceberg(cfg)
    con_v.close()

    initial_snapshot_info = wait_for_connector_and_snapshot(cfg, timeout_s=180.0)

    # 5. Commit warm-up marker and wait until DuckDB sees it before measurement window.
    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        warmup_commit_ns, warmup_commit_utc = commit_marker_row(
            pg_conn,
            marker_id=WARMUP_MARKER_ID,
            status="marker_warmup",
            note="m1 warm-up marker before measurement window",
        )
    warmup_vis = wait_for_marker_visible(
        cfg,
        WARMUP_MARKER_ID,
        warmup_commit_ns,
        timeout_s=120.0,
        poll_interval_s=0.5,
    )
    if not warmup_vis["visible"]:
        raise RuntimeError(f"Warm-up marker {WARMUP_MARKER_ID} was not visible within timeout: {warmup_vis}")

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as pg_conn:
        pg_pos_window_start = capture_pg_pipeline_positions(pg_conn)

    # 6. Run measured paced workload with steady-state lag probes.
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
    )

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as pg_conn:
        toast_after_workload = verify_toast_storage(pg_conn)
        pg_pos_window_end = capture_pg_pipeline_positions(pg_conn)

    # 7. Commit unique final marker row after workload drains.
    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        final_commit_ns, final_commit_utc = commit_marker_row(
            pg_conn,
            marker_id=FINAL_MARKER_ID,
            status="marker_final",
            note="m1 final convergence marker",
        )
        pg_pos_after_final_marker = capture_pg_pipeline_positions(pg_conn)

    # 8. Wait for final marker visibility and run exact schema + bounded-batch row verification.
    convergence = verify_snapshot_convergence(
        cfg,
        FINAL_MARKER_ID,
        final_commit_ns,
        timeout_s=convergence_timeout_s,
        poll_interval_s=1.0,
        batch_size=batch_size,
    )

    final_lk_meta = convergence.pop("final_lakekeeper_metadata", None)
    if final_lk_meta is not None:
        write_json(run_dir / "lakekeeper_table_final.json", final_lk_meta)

    # 9. Capture final DuckDB snapshots/metadata, PG pipeline positions, and pg2iceberg logs.
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

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as pg_conn:
        pg_pos_final = capture_pg_pipeline_positions(pg_conn)

    pg2_logs = run_cmd(
        [*cfg.compose_base_cmd(include_connector=True), "logs", "--no-color", "pg2iceberg"],
        check=False,
    ).stdout
    (run_dir / "pg2iceberg.log").write_text(pg2_logs, encoding="utf-8")

    # 10. Project-only cleanup verification.
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

    # Explicit run outcome per docs/PLAN.md.
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
        "milestone": "M1",
        "run_kind": "baseline",
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

    # Print compact top-level summary so stdout stays readable.
    compact = {
        "run_dir": str(run_dir.relative_to(REPO_ROOT).as_posix()),
        "outcome": outcome,
        "passed": passed,
        "seed_rows": seed_rows,
        "toast_after_seed": seed_res.toast_verification["both_toasted_rows"],
        "toast_after_workload": toast_after_workload["both_toasted_rows"],
        "workload_completed_ops": workload_summary["completed_ops"],
        "workload_errors": workload_summary["error_count"],
        "workload_missed_schedule": workload_summary["arrival_schedule"]["missed_schedule_count"],
        "latency_sched_to_complete_ms": workload_summary["latency_sched_to_complete_ms"],
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Run M1 baseline workload and exact verification.")
    parser.add_argument("--seed-rows", type=int, default=25000)
    parser.add_argument("--toast-rows", type=int, default=100)
    parser.add_argument("--duration-s", type=float, default=180.0)
    parser.add_argument("--rate-ops-s", type=float, default=10.0)
    parser.add_argument("--rng-seed", type=int, default=20261119)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--probe-interval-s", type=float, default=20.0)
    parser.add_argument("--probe-poll-interval-s", type=float, default=0.5)
    parser.add_argument("--convergence-timeout-s", type=float, default=180.0)
    parser.add_argument("--label", default="m1-baseline")
    parser.add_argument("--keep-up", action="store_true")
    args = parser.parse_args()
    run_m1_baseline(
        seed_rows=args.seed_rows,
        toast_rows=args.toast_rows,
        duration_s=args.duration_s,
        rate_ops_s=args.rate_ops_s,
        rng_seed=args.rng_seed,
        batch_size=args.batch_size,
        probe_interval_s=args.probe_interval_s,
        probe_poll_interval_s=args.probe_poll_interval_s,
        convergence_timeout_s=args.convergence_timeout_s,
        label=args.label,
        keep_up=args.keep_up,
    )


if __name__ == "__main__":
    main()
