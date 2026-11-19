"""Milestone M2 runner: matched baseline vs concurrent REPACK pair and raw-evidence reporting."""

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
from lab.m1 import summarize_duckdb_metadata
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
from lab.repack import (
    ConcurrentRepackRunner,
    PLAIN_REPACK_SQL,
    REPACK_SQL,
    build_pair_comparison,
    capture_table_and_index_sizes,
    load_ops_jsonl,
    render_pair_results_markdown,
)
from lab.workload import (
    FINAL_MARKER_ID,
    WARMUP_MARKER_ID,
    commit_marker_row,
    run_paced_workload,
    seed_orders,
    verify_toast_storage,
)


def run_single_experiment(
    *,
    run_kind: str,
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
    repack_timeout_s: float = 600.0,
    label: str | None = None,
    milestone: str = "M2",
    keep_up: bool = False,
) -> tuple[Path, dict[str, Any]]:
    """Run one fresh baseline, concurrent-REPACK, or plain-REPACK experiment and save raw evidence."""
    if run_kind not in ("baseline", "repack", "plain_repack"):
        raise ValueError(
            f"run_kind must be 'baseline', 'repack', or 'plain_repack', got {run_kind!r}"
        )

    cfg = load_config()
    run_label = label or f"{milestone.lower()}-{run_kind}"
    run_dir = REPO_ROOT / "runs" / f"{utc_run_stamp()}-{run_label}"
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
        pg_conn.commit()

    # 4. Start pg2iceberg and wait for initial snapshot completion.
    logged_cmd([*cfg.compose_base_cmd(include_connector=True), "up", "-d", "pg2iceberg"])
    digests = collect_container_digests(cfg)
    con_v, reader_versions = connect_iceberg(cfg)
    con_v.close()

    initial_snapshot_info = wait_for_connector_and_snapshot(cfg, timeout_s=snapshot_timeout_s)

    # 5. Commit warm-up marker and wait until DuckDB sees it before measurement window.
    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        warmup_commit_ns, warmup_commit_utc = commit_marker_row(
            pg_conn,
            marker_id=WARMUP_MARKER_ID,
            status="marker_warmup",
            note=f"{milestone.lower()} {run_kind} warm-up marker",
        )
    warmup_vis = wait_for_marker_visible(
        cfg,
        WARMUP_MARKER_ID,
        warmup_commit_ns,
        timeout_s=180.0,
        poll_interval_s=0.5,
    )
    if not warmup_vis["visible"]:
        raise RuntimeError(f"Warm-up marker {WARMUP_MARKER_ID} not visible within timeout: {warmup_vis}")

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as pg_conn:
        pg_pos_window_start = capture_pg_pipeline_positions(pg_conn)
        sizes_window_start = capture_table_and_index_sizes(pg_conn)

    # 6. Run measured paced workload (with REPACK anchored to window start if applicable).
    repack_runner: ConcurrentRepackRunner | None = None
    on_window_start = None
    if run_kind in ("repack", "plain_repack"):
        repack_cmd = REPACK_SQL if run_kind == "repack" else PLAIN_REPACK_SQL
        repack_runner = ConcurrentRepackRunner(
            cfg,
            repack_delay_s=repack_delay_s,
            sample_interval_s=repack_sample_interval_s,
            samples_jsonl=run_dir / "repack_samples.jsonl",
            command=repack_cmd,
        )
        on_window_start = repack_runner.start_anchored_to_window

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
        on_window_start=on_window_start,
    )

    # Ensure REPACK has fully completed before committing final marker or opening final source read.
    repack_res: dict[str, Any] | None = None
    if repack_runner is not None:
        repack_res = repack_runner.wait_for_completion(timeout_s=repack_timeout_s)

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as pg_conn:
        toast_after_workload = verify_toast_storage(pg_conn)
        pg_pos_window_end = capture_pg_pipeline_positions(pg_conn)
        sizes_window_end = capture_table_and_index_sizes(pg_conn)

    # 7. Commit unique final marker row strictly after workload drains and REPACK returns.
    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        final_commit_ns, final_commit_utc = commit_marker_row(
            pg_conn,
            marker_id=FINAL_MARKER_ID,
            status="marker_final",
            note=f"{milestone.lower()} {run_kind} final convergence marker",
        )
        pg_pos_after_final_marker = capture_pg_pipeline_positions(pg_conn)
        pg_conn.commit()

    # 8. Wait for final marker visibility and run exact schema + bounded-batch row verification.
    # Final REPEATABLE READ READ ONLY source transaction opens inside verify_snapshot_convergence
    # strictly after REPACK has returned.
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

    # 9. Capture final DuckDB snapshots/metadata, PG pipeline positions, and container logs.
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

    pg_logs = run_cmd(
        [*cfg.compose_base_cmd(include_connector=False), "logs", "--no-color", "pg-source"],
        check=False,
    ).stdout
    (run_dir / "pg_source.log").write_text(pg_logs, encoding="utf-8")

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

    schedule_ok = bool(workload_summary["arrival_schedule"]["schedule_valid"])
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
    repack_ok = repack_res is None or repack_res["outcome"] == "ok"

    # Explicit outcome per docs/PLAN.md.
    if not repack_ok:
        outcome = "REPACK failed"
    elif not schedule_ok or not toast_ok:
        outcome = "invalid harness run"
    else:
        outcome = convergence["outcome"]

    passed = (
        outcome == "converged"
        and convergence["passed"]
        and repack_ok
        and schedule_ok
        and toast_ok
        and cleanup_ok
    )

    summary: dict[str, Any] = {
        "milestone": milestone,
        "run_kind": run_kind,
        "label": run_label,
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
            "repack_delay_s": repack_delay_s if run_kind in ("repack", "plain_repack") else None,
            "repack_sample_interval_s": (
                repack_sample_interval_s if run_kind in ("repack", "plain_repack") else None
            ),
            "repack_command": (
                REPACK_SQL
                if run_kind == "repack"
                else (PLAIN_REPACK_SQL if run_kind == "plain_repack" else None)
            ),
            "snapshot_timeout_s": snapshot_timeout_s,
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
        "sizes_window_start": sizes_window_start,
        "sizes_window_end": sizes_window_end,
        "workload": workload_summary,
        "repack": repack_res,
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
    return run_dir, summary


def render_pair_from_dirs(
    baseline_dir: Path,
    repack_dir: Path,
    *,
    output_dir: Path,
) -> tuple[Path, dict[str, Any]]:
    """Build pair_summary.json and results_table.md reproducibly from raw run directories."""
    b_summary = json.loads((baseline_dir / "summary.json").read_text(encoding="utf-8"))
    r_summary = json.loads((repack_dir / "summary.json").read_text(encoding="utf-8"))
    b_ops = load_ops_jsonl(baseline_dir / "ops.jsonl")
    r_ops = load_ops_jsonl(repack_dir / "ops.jsonl")

    def _rel(p: Path) -> str:
        try:
            return p.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
        except ValueError:
            return p.as_posix()

    pair = build_pair_comparison(
        b_summary,
        r_summary,
        baseline_ops=b_ops,
        repack_ops=r_ops,
        baseline_run_dir=_rel(baseline_dir),
        repack_run_dir=_rel(repack_dir),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / "pair_summary.json", pair)
    md = render_pair_results_markdown(pair)
    (output_dir / "results_table.md").write_text(md, encoding="utf-8")
    return output_dir, pair


def run_matched_pair(
    *,
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
    label: str = "m2",
    milestone: str = "M2",
) -> tuple[Path, dict[str, Any]]:
    """Run one matched pair (baseline + concurrent REPACK) from fresh state and render results."""
    if order not in ("baseline-first", "repack-first"):
        raise ValueError(f"order must be 'baseline-first' or 'repack-first', got {order!r}")

    run_order = ("baseline", "repack") if order == "baseline-first" else ("repack", "baseline")
    run_dirs: dict[str, Path] = {}
    summaries: dict[str, dict[str, Any]] = {}

    for kind in run_order:
        r_dir, r_sum = run_single_experiment(
            run_kind=kind,
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
            milestone=milestone,
            keep_up=False,
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
        "parameters_matched": pair["parameters_matched"],
        "overlap_confirmed": pair["overlap_confirmed"],
        "committed_during_repack_count": pair["committed_during_repack_count"],
        "repack_duration_ms": (summaries["repack"].get("repack") or {}).get("duration_ms"),
        "source_read_opened_after_repack": pair["source_read_opened_after_repack"],
        "final_lock_observed": pair["host_performance"]["final_lock_observed"],
    }
    print(json.dumps(compact, indent=2, default=json_default), flush=True)
    return pair_dir, pair


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run M2 matched pair (baseline vs concurrent REPACK) or render from existing raw runs."
    )
    parser.add_argument("--seed-rows", type=int, default=250000)
    parser.add_argument("--toast-rows", type=int, default=25000)
    parser.add_argument("--duration-s", type=float, default=180.0)
    parser.add_argument("--rate-ops-s", type=float, default=10.0)
    parser.add_argument("--rng-seed", type=int, default=20261119)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--probe-interval-s", type=float, default=20.0)
    parser.add_argument("--probe-poll-interval-s", type=float, default=0.5)
    parser.add_argument("--repack-delay-s", type=float, default=30.0)
    parser.add_argument("--repack-sample-interval-s", type=float, default=0.05)
    parser.add_argument("--snapshot-timeout-s", type=float, default=300.0)
    parser.add_argument("--convergence-timeout-s", type=float, default=300.0)
    parser.add_argument("--order", choices=("baseline-first", "repack-first"), default="baseline-first")
    parser.add_argument("--label", default="m2")
    parser.add_argument(
        "--only",
        choices=("pair", "baseline", "repack"),
        default="pair",
        help="Run a full matched pair (default) or a single baseline/repack run.",
    )
    parser.add_argument(
        "--render-from-runs",
        nargs=2,
        metavar=("BASELINE_DIR", "REPACK_DIR"),
        help="Regenerate pair_summary.json and results_table.md from two existing run directories.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory when using --render-from-runs.",
    )
    args = parser.parse_args()

    if args.render_from_runs:
        b_dir = Path(args.render_from_runs[0])
        r_dir = Path(args.render_from_runs[1])
        out_dir = args.output_dir or (REPO_ROOT / "runs" / f"{utc_run_stamp()}-{args.label}-pair")
        _, pair = render_pair_from_dirs(b_dir, r_dir, output_dir=out_dir)
        print(json.dumps({"pair_dir": str(out_dir), "overlap_confirmed": pair["overlap_confirmed"]}, indent=2))
        return

    if args.only in ("baseline", "repack"):
        r_dir, r_sum = run_single_experiment(
            run_kind=args.only,
            seed_rows=args.seed_rows,
            toast_rows=args.toast_rows,
            duration_s=args.duration_s,
            rate_ops_s=args.rate_ops_s,
            rng_seed=args.rng_seed,
            batch_size=args.batch_size,
            probe_interval_s=args.probe_interval_s,
            probe_poll_interval_s=args.probe_poll_interval_s,
            repack_delay_s=args.repack_delay_s,
            repack_sample_interval_s=args.repack_sample_interval_s,
            snapshot_timeout_s=args.snapshot_timeout_s,
            convergence_timeout_s=args.convergence_timeout_s,
            label=f"{args.label}-{args.only}",
        )
        print(json.dumps({"run_dir": r_dir.relative_to(REPO_ROOT).as_posix(), "outcome": r_sum["outcome"], "passed": r_sum["passed"]}, indent=2))
        return

    run_matched_pair(
        seed_rows=args.seed_rows,
        toast_rows=args.toast_rows,
        duration_s=args.duration_s,
        rate_ops_s=args.rate_ops_s,
        rng_seed=args.rng_seed,
        batch_size=args.batch_size,
        probe_interval_s=args.probe_interval_s,
        probe_poll_interval_s=args.probe_poll_interval_s,
        repack_delay_s=args.repack_delay_s,
        repack_sample_interval_s=args.repack_sample_interval_s,
        snapshot_timeout_s=args.snapshot_timeout_s,
        convergence_timeout_s=args.convergence_timeout_s,
        order=args.order,
        label=args.label,
    )


if __name__ == "__main__":
    main()
