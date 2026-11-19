"""Milestone M3 runner: three fresh counterbalanced matched pairs, optional plain REPACK control,
and reproducible table + SVG plot generation from raw run files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import statistics
from typing import Any

from lab.config import REPO_ROOT
from lab.m2 import run_matched_pair, run_single_experiment
from lab.pipeline import json_default, utc_run_stamp, write_json
from lab.plots import (
    render_latency_comparison_svg,
    render_reader_lag_svg,
    render_repack_window_timeseries_svg,
)
from lab.repack import analyze_ops_intervals, build_pair_comparison, load_ops_jsonl


def default_orders_for_count(n_pairs: int) -> list[str]:
    """Return alternating counterbalanced orders starting with baseline-first."""
    return ["baseline-first" if i % 2 == 0 else "repack-first" for i in range(n_pairs)]


def median_and_range(values: list[float | int | None], *, decimals: int = 3) -> dict[str, Any]:
    """Compute exact n, median, min, max, and raw values list (ignoring None)."""
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return {"n": 0, "median": None, "min": None, "max": None, "values": []}
    return {
        "n": len(clean),
        "median": round(float(statistics.median(clean)), decimals),
        "min": round(min(clean), decimals),
        "max": round(max(clean), decimals),
        "values": [round(v, decimals) for v in clean],
    }


def build_paired_metric(
    baseline_vals: list[float | int | None],
    repack_vals: list[float | int | None],
    *,
    decimals: int = 3,
) -> dict[str, Any]:
    """Summarize baseline, repack, and per-pair (repack - baseline) deltas with median and range."""
    deltas: list[float] = []
    for b, r in zip(baseline_vals, repack_vals):
        if b is not None and r is not None:
            deltas.append(float(r) - float(b))
    return {
        "baseline": median_and_range(baseline_vals, decimals=decimals),
        "repack": median_and_range(repack_vals, decimals=decimals),
        "paired_delta_repack_minus_baseline": median_and_range(deltas, decimals=decimals),
    }


def _resolve_run_path(raw_path: str, repo_root: Path) -> Path:
    p = Path(raw_path)
    if p.is_absolute():
        return p
    return repo_root / p


def _rel_str(p: Path, repo_root: Path) -> str:
    try:
        return p.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return p.as_posix()


def _extract_run_metrics(
    summary: dict[str, Any],
    interval_slice: dict[str, Any],
    full_slice: dict[str, Any],
    run_dir_rel: str,
) -> dict[str, Any]:
    conv = summary.get("convergence") or {}
    attempts = conv.get("snapshot_attempts") or []
    last_rc = attempts[-1].get("row_comparison") or {} if attempts else {}
    last_sc = attempts[-1].get("schema_check") or {} if attempts else {}
    workload = summary.get("workload") or {}
    probes = workload.get("steady_state_probes") or {}
    probe_lat = probes.get("visibility_lag_ms") or {}
    arr = workload.get("arrival_schedule") or {}
    cleanup = summary.get("cleanup") or {}
    meta_sum = summary.get("duckdb_final_metadata_summary") or {}
    pos_final = (summary.get("pipeline_positions") or {}).get("final") or {}
    final_slots = pos_final.get("replication_slots") or []
    cdc_final_gap = None
    for sl in final_slots:
        if sl.get("slot_name") == "pg2iceberg_slot":
            cdc_final_gap = sl.get("wal_gap_bytes_confirmed_flush")
            break

    return {
        "run_dir": run_dir_rel,
        "run_kind": summary.get("run_kind"),
        "label": summary.get("label"),
        "rng_seed": (summary.get("parameters") or {}).get("rng_seed"),
        "outcome": summary.get("outcome"),
        "passed": bool(summary.get("passed")),
        "verified_snapshot_id": conv.get("verified_snapshot_id"),
        "schema_passed": bool(last_sc.get("passed")),
        "schema_id": last_sc.get("schema_id"),
        "source_row_count": last_rc.get("source_row_count"),
        "lake_row_count": last_rc.get("lake_row_count"),
        "source_only_count": last_rc.get("source_only_count"),
        "lake_only_count": last_rc.get("lake_only_count"),
        "duplicate_lake_id_count": last_rc.get("duplicate_lake_id_count"),
        "value_mismatch_row_count": last_rc.get("value_mismatch_row_count"),
        "final_marker_visibility_ms": conv.get("final_marker_visibility_ms"),
        "final_marker_to_verified_match_ms": conv.get("final_marker_to_verified_match_ms"),
        "diff_duration_ms": conv.get("diff_duration_ms"),
        "live_data_files": meta_sum.get("live_data_files"),
        "live_equality_delete_files": meta_sum.get("live_equality_delete_files"),
        "live_equality_delete_records": meta_sum.get("live_equality_delete_records"),
        "final_cdc_wal_gap_bytes": cdc_final_gap,
        "attempted_ops": full_slice.get("attempted_ops"),
        "completed_ops": full_slice.get("completed_ops"),
        "error_count": full_slice.get("error_count"),
        "timeout_count": full_slice.get("timeout_count"),
        "missed_schedule_count": full_slice.get("missed_schedule_count"),
        "harness_missed_arrival_count": full_slice.get("harness_missed_arrival_count"),
        "backpressure_delayed_count": arr.get("backpressure_delayed_count"),
        "schedule_valid": bool(arr.get("schedule_valid")),
        "completed_throughput_ops_s": full_slice.get("completed_throughput_ops_s"),
        "full_s2c_p50_ms": (full_slice.get("latency_sched_to_complete_ms") or {}).get("p50_ms"),
        "full_s2c_p99_ms": (full_slice.get("latency_sched_to_complete_ms") or {}).get("p99_ms"),
        "full_s2c_max_ms": (full_slice.get("latency_sched_to_complete_ms") or {}).get("max_ms"),
        "full_exec_p50_ms": (full_slice.get("latency_exec_ms") or {}).get("p50_ms"),
        "full_exec_p99_ms": (full_slice.get("latency_exec_ms") or {}).get("p99_ms"),
        "full_exec_max_ms": (full_slice.get("latency_exec_ms") or {}).get("max_ms"),
        "full_queue_p50_ms": (full_slice.get("latency_queue_delay_ms") or {}).get("p50_ms"),
        "full_queue_p99_ms": (full_slice.get("latency_queue_delay_ms") or {}).get("p99_ms"),
        "full_queue_max_ms": (full_slice.get("latency_queue_delay_ms") or {}).get("max_ms"),
        "int_attempted_ops": interval_slice.get("attempted_ops"),
        "int_completed_ops": interval_slice.get("completed_ops"),
        "int_error_count": interval_slice.get("error_count"),
        "int_timeout_count": interval_slice.get("timeout_count"),
        "int_s2c_p50_ms": (interval_slice.get("latency_sched_to_complete_ms") or {}).get("p50_ms"),
        "int_s2c_p99_ms": (interval_slice.get("latency_sched_to_complete_ms") or {}).get("p99_ms"),
        "int_s2c_max_ms": (interval_slice.get("latency_sched_to_complete_ms") or {}).get("max_ms"),
        "int_exec_p50_ms": (interval_slice.get("latency_exec_ms") or {}).get("p50_ms"),
        "int_exec_p99_ms": (interval_slice.get("latency_exec_ms") or {}).get("p99_ms"),
        "int_exec_max_ms": (interval_slice.get("latency_exec_ms") or {}).get("max_ms"),
        "int_queue_p50_ms": (interval_slice.get("latency_queue_delay_ms") or {}).get("p50_ms"),
        "int_queue_p99_ms": (interval_slice.get("latency_queue_delay_ms") or {}).get("p99_ms"),
        "int_queue_max_ms": (interval_slice.get("latency_queue_delay_ms") or {}).get("max_ms"),
        "probe_count": probes.get("probe_count"),
        "probe_visible_count": probes.get("visible_count"),
        "probe_error_count": probes.get("reader_error_count"),
        "probe_min_ms": probe_lat.get("min_ms"),
        "probe_p50_ms": probe_lat.get("p50_ms"),
        "probe_p99_ms": probe_lat.get("p99_ms"),
        "probe_max_ms": probe_lat.get("max_ms"),
        "cleanup_ok": bool(
            cleanup.get("all_project_resources_removed")
            and cleanup.get("non_project_containers_unchanged")
        ),
    }


def aggregate_repeated_runs(
    manifest: dict[str, Any],
    *,
    repo_root: Path = REPO_ROOT,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any] | None]:
    """Recompute all per-pair, aggregate (median + range), and control metrics strictly from raw runs."""
    pair_entries = manifest.get("pairs") or []
    if not pair_entries:
        raise ValueError("Manifest contains no pairs")

    pairs_out: list[dict[str, Any]] = []
    pair_ops_series: list[dict[str, Any]] = []
    orders_seen: list[str] = []
    baseline_by_seed: dict[int, tuple[str, dict[str, Any], list[dict[str, Any]]]] = {}
    digests_seen: list[dict[str, Any]] = []

    for idx, entry in enumerate(pair_entries, start=1):
        pair_index = int(entry.get("pair_index", idx))
        order = str(entry.get("order", "baseline-first"))
        orders_seen.append(order)

        b_dir = _resolve_run_path(entry["baseline_run_dir"], repo_root)
        r_dir = _resolve_run_path(entry["repack_run_dir"], repo_root)
        b_rel = _rel_str(b_dir, repo_root)
        r_rel = _rel_str(r_dir, repo_root)

        b_sum = json.loads((b_dir / "summary.json").read_text(encoding="utf-8"))
        r_sum = json.loads((r_dir / "summary.json").read_text(encoding="utf-8"))
        b_ops = load_ops_jsonl(b_dir / "ops.jsonl")
        r_ops = load_ops_jsonl(r_dir / "ops.jsonl")

        if b_sum.get("container_digests"):
            digests_seen.append(b_sum["container_digests"])
        if r_sum.get("container_digests"):
            digests_seen.append(r_sum["container_digests"])

        b_seed = (b_sum.get("parameters") or {}).get("rng_seed")
        if b_seed is not None and int(b_seed) not in baseline_by_seed:
            baseline_by_seed[int(b_seed)] = (b_rel, b_sum, b_ops)

        pair_comp = build_pair_comparison(
            b_sum,
            r_sum,
            baseline_ops=b_ops,
            repack_ops=r_ops,
            baseline_run_dir=b_rel,
            repack_run_dir=r_rel,
        )

        perf = pair_comp["host_performance"]
        b_metrics = _extract_run_metrics(
            b_sum,
            perf["baseline_intervals"]["in_interval_scheduled"],
            perf["baseline_intervals"]["full_window"],
            b_rel,
        )
        r_metrics = _extract_run_metrics(
            r_sum,
            perf["repack_intervals"]["in_interval_scheduled"],
            perf["repack_intervals"]["full_window"],
            r_rel,
        )

        repack_info = r_sum.get("repack") or {}
        sampling = repack_info.get("sampling") or {}
        size_deltas = repack_info.get("size_deltas") or {}
        cdc_act = sampling.get("cdc_slot_activity") or {}

        repack_exec_summary = {
            "command": repack_info.get("command"),
            "outcome": repack_info.get("outcome"),
            "repack_pid": repack_info.get("repack_pid"),
            "start_offset_ms": repack_info.get("start_offset_ms"),
            "end_offset_ms": repack_info.get("end_offset_ms"),
            "duration_ms": repack_info.get("duration_ms"),
            "committed_during_repack_count": pair_comp["committed_during_repack_count"],
            "strictly_contained_count": perf["repack_intervals"]["in_interval_strictly_contained"][
                "completed_ops"
            ],
            "overlap_confirmed": pair_comp["overlap_confirmed"],
            "source_read_opened_after_repack": pair_comp["source_read_opened_after_repack"],
            "heap_relfilenode_changed": size_deltas.get("heap_relfilenode_changed"),
            "toast_relfilenode_changed": size_deltas.get("toast_relfilenode_changed"),
            "total_size_before_bytes": (repack_info.get("sizes_before") or {}).get(
                "total_relation_bytes"
            ),
            "total_size_after_bytes": (repack_info.get("sizes_after") or {}).get(
                "total_relation_bytes"
            ),
            "total_size_delta_bytes": (size_deltas.get("byte_deltas") or {}).get(
                "total_relation_bytes"
            ),
            "phases_observed": [p["phase"] for p in sampling.get("phases_observed") or []],
            "repack_slots_observed": [
                s["slot_name"] for s in sampling.get("repack_slots_observed") or []
            ],
            "sample_count": sampling.get("sample_count"),
            "progress_sample_count": sampling.get("progress_sample_count"),
            "waiting_locks_observed_count": sampling.get("waiting_locks_observed_count"),
            "final_lock_observed": bool(sampling.get("final_lock_observed")),
            "final_lock_evidence": sampling.get("final_lock_evidence"),
            "peak_cdc_confirmed_flush_wal_gap_bytes": cdc_act.get(
                "max_wal_gap_bytes_confirmed_flush"
            ),
            "peak_cdc_restart_wal_gap_bytes": cdc_act.get("max_wal_gap_bytes_restart"),
        }

        pairs_out.append(
            {
                "pair_index": pair_index,
                "order": order,
                "rng_seed": pair_comp["parameters"].get("rng_seed"),
                "parameters_matched": pair_comp["parameters_matched"],
                "parameters": pair_comp["parameters"],
                "baseline_run_dir": b_rel,
                "repack_run_dir": r_rel,
                "baseline": b_metrics,
                "repack": r_metrics,
                "repack_execution": repack_exec_summary,
            }
        )

        pair_ops_series.append(
            {
                "pair_index": pair_index,
                "order": order,
                "rng_seed": pair_comp["parameters"].get("rng_seed"),
                "repack_start_s": float(repack_info.get("start_offset_ms") or 0.0) / 1000.0,
                "repack_end_s": float(repack_info.get("end_offset_ms") or 0.0) / 1000.0,
                "repack_duration_ms": float(repack_info.get("duration_ms") or 0.0),
                "committed_during_repack": pair_comp["committed_during_repack_count"],
                "waiting_locks_observed_count": int(sampling.get("waiting_locks_observed_count") or 0),
                "baseline_ops": b_ops,
                "repack_ops": r_ops,
            }
        )

    # Aggregate across pairs: median + [min, max] + paired deltas (repack - baseline)
    def _col(side: str, key: str) -> list[Any]:
        return [p[side].get(key) for p in pairs_out]

    convergence_summary = {
        "pair_count": len(pairs_out),
        "baseline_converged_count": sum(1 for p in pairs_out if p["baseline"]["outcome"] == "converged"),
        "repack_converged_count": sum(1 for p in pairs_out if p["repack"]["outcome"] == "converged"),
        "all_converged": all(
            p["baseline"]["outcome"] == "converged" and p["repack"]["outcome"] == "converged"
            for p in pairs_out
        ),
        "all_parameters_matched_within_pair": all(p["parameters_matched"] for p in pairs_out),
        "all_overlaps_confirmed": all(p["repack_execution"]["overlap_confirmed"] for p in pairs_out),
        "all_source_reads_after_repack": all(
            p["repack_execution"]["source_read_opened_after_repack"] for p in pairs_out
        ),
        "all_cleanups_ok": all(
            p["baseline"]["cleanup_ok"] and p["repack"]["cleanup_ok"] for p in pairs_out
        ),
        "counterbalanced_orders": len(set(orders_seen)) >= 2 if len(orders_seen) >= 2 else False,
        "orders": orders_seen,
        "final_marker_visibility_ms": build_paired_metric(
            _col("baseline", "final_marker_visibility_ms"),
            _col("repack", "final_marker_visibility_ms"),
        ),
        "final_marker_to_verified_match_ms": build_paired_metric(
            _col("baseline", "final_marker_to_verified_match_ms"),
            _col("repack", "final_marker_to_verified_match_ms"),
        ),
        "diff_duration_ms": build_paired_metric(
            _col("baseline", "diff_duration_ms"),
            _col("repack", "diff_duration_ms"),
        ),
    }

    repack_exec_agg = {
        "duration_ms": median_and_range([p["repack_execution"]["duration_ms"] for p in pairs_out]),
        "committed_during_repack_count": median_and_range(
            [p["repack_execution"]["committed_during_repack_count"] for p in pairs_out], decimals=1
        ),
        "peak_cdc_confirmed_flush_wal_gap_bytes": median_and_range(
            [p["repack_execution"]["peak_cdc_confirmed_flush_wal_gap_bytes"] for p in pairs_out],
            decimals=0,
        ),
        "total_size_delta_bytes": median_and_range(
            [p["repack_execution"]["total_size_delta_bytes"] for p in pairs_out], decimals=0
        ),
        "final_lock_observed_count": sum(
            1 for p in pairs_out if p["repack_execution"]["final_lock_observed"]
        ),
        "waiting_locks_observed_total": sum(
            int(p["repack_execution"]["waiting_locks_observed_count"] or 0) for p in pairs_out
        ),
    }

    host_perf_summary = {
        "full_window": {
            "sched_to_complete_p50_ms": build_paired_metric(
                _col("baseline", "full_s2c_p50_ms"), _col("repack", "full_s2c_p50_ms")
            ),
            "sched_to_complete_p99_ms": build_paired_metric(
                _col("baseline", "full_s2c_p99_ms"), _col("repack", "full_s2c_p99_ms")
            ),
            "sched_to_complete_max_ms": build_paired_metric(
                _col("baseline", "full_s2c_max_ms"), _col("repack", "full_s2c_max_ms")
            ),
            "exec_p50_ms": build_paired_metric(
                _col("baseline", "full_exec_p50_ms"), _col("repack", "full_exec_p50_ms")
            ),
            "exec_p99_ms": build_paired_metric(
                _col("baseline", "full_exec_p99_ms"), _col("repack", "full_exec_p99_ms")
            ),
            "exec_max_ms": build_paired_metric(
                _col("baseline", "full_exec_max_ms"), _col("repack", "full_exec_max_ms")
            ),
            "queue_p50_ms": build_paired_metric(
                _col("baseline", "full_queue_p50_ms"), _col("repack", "full_queue_p50_ms")
            ),
            "queue_p99_ms": build_paired_metric(
                _col("baseline", "full_queue_p99_ms"), _col("repack", "full_queue_p99_ms")
            ),
            "queue_max_ms": build_paired_metric(
                _col("baseline", "full_queue_max_ms"), _col("repack", "full_queue_max_ms")
            ),
            "steady_state_probe_p50_ms": build_paired_metric(
                _col("baseline", "probe_p50_ms"), _col("repack", "probe_p50_ms")
            ),
            "steady_state_probe_p99_ms": build_paired_metric(
                _col("baseline", "probe_p99_ms"), _col("repack", "probe_p99_ms")
            ),
            "steady_state_probe_max_ms": build_paired_metric(
                _col("baseline", "probe_max_ms"), _col("repack", "probe_max_ms")
            ),
        },
        "matched_interval": {
            "sched_to_complete_p50_ms": build_paired_metric(
                _col("baseline", "int_s2c_p50_ms"), _col("repack", "int_s2c_p50_ms")
            ),
            "sched_to_complete_p99_ms": build_paired_metric(
                _col("baseline", "int_s2c_p99_ms"), _col("repack", "int_s2c_p99_ms")
            ),
            "sched_to_complete_max_ms": build_paired_metric(
                _col("baseline", "int_s2c_max_ms"), _col("repack", "int_s2c_max_ms")
            ),
            "exec_p50_ms": build_paired_metric(
                _col("baseline", "int_exec_p50_ms"), _col("repack", "int_exec_p50_ms")
            ),
            "exec_p99_ms": build_paired_metric(
                _col("baseline", "int_exec_p99_ms"), _col("repack", "int_exec_p99_ms")
            ),
            "exec_max_ms": build_paired_metric(
                _col("baseline", "int_exec_max_ms"), _col("repack", "int_exec_max_ms")
            ),
            "queue_p50_ms": build_paired_metric(
                _col("baseline", "int_queue_p50_ms"), _col("repack", "int_queue_p50_ms")
            ),
            "queue_p99_ms": build_paired_metric(
                _col("baseline", "int_queue_p99_ms"), _col("repack", "int_queue_p99_ms")
            ),
            "queue_max_ms": build_paired_metric(
                _col("baseline", "int_queue_max_ms"), _col("repack", "int_queue_max_ms")
            ),
        },
    }

    # Optional plain REPACK control
    control_out: dict[str, Any] | None = None
    control_series: dict[str, Any] | None = None
    ctrl_entry = manifest.get("plain_repack_control")
    if ctrl_entry:
        c_dir = _resolve_run_path(ctrl_entry["run_dir"], repo_root)
        c_rel = _rel_str(c_dir, repo_root)
        c_sum = json.loads((c_dir / "summary.json").read_text(encoding="utf-8"))
        c_ops = load_ops_jsonl(c_dir / "ops.jsonl")
        if c_sum.get("container_digests"):
            digests_seen.append(c_sum["container_digests"])

        c_rep = c_sum.get("repack") or {}
        c_start_ms = float(c_rep.get("start_offset_ms") or 0.0)
        c_end_ms = float(c_rep.get("end_offset_ms") or 0.0)
        c_total_ms = float((c_sum.get("workload") or {}).get("actual_duration_s", 0.0)) * 1000.0
        c_miss_thresh = float(
            (c_sum.get("workload") or {}).get("arrival_schedule", {}).get("miss_threshold_ms", 250.0)
        )
        c_intervals = analyze_ops_intervals(
            c_ops,
            interval_start_ms=c_start_ms,
            interval_end_ms=c_end_ms,
            total_window_duration_ms=c_total_ms,
            miss_threshold_ms=c_miss_thresh,
        )
        c_metrics = _extract_run_metrics(
            c_sum,
            c_intervals["in_interval_scheduled"],
            c_intervals["full_window"],
            c_rel,
        )
        c_sampling = c_rep.get("sampling") or {}
        c_deltas = c_rep.get("size_deltas") or {}
        c_cdc_act = c_sampling.get("cdc_slot_activity") or {}

        c_seed = (c_sum.get("parameters") or {}).get("rng_seed")
        matched_base_ops: list[dict[str, Any]] = []
        matched_base_dir: str | None = None
        if c_seed is not None and int(c_seed) in baseline_by_seed:
            matched_base_dir, _, matched_base_ops = baseline_by_seed[int(c_seed)]

        control_out = {
            "run_dir": c_rel,
            "matched_baseline_run_dir": matched_base_dir,
            "rng_seed": c_seed,
            "outcome": c_sum.get("outcome"),
            "passed": bool(c_sum.get("passed")),
            "backpressure_delayed_count": (c_sum.get("workload") or {})
            .get("arrival_schedule", {})
            .get("backpressure_delayed_count"),
            "missed_schedule_count": c_intervals["full_window"]["missed_schedule_count"],
            "harness_missed_arrival_count": c_intervals["full_window"][
                "harness_missed_arrival_count"
            ],
            "repack_execution": {
                "command": c_rep.get("command"),
                "outcome": c_rep.get("outcome"),
                "repack_pid": c_rep.get("repack_pid"),
                "start_offset_ms": c_rep.get("start_offset_ms"),
                "end_offset_ms": c_rep.get("end_offset_ms"),
                "duration_ms": c_rep.get("duration_ms"),
                "committed_during_repack_count": c_intervals["in_interval_committed"][
                    "completed_ops"
                ],
                "heap_relfilenode_changed": c_deltas.get("heap_relfilenode_changed"),
                "toast_relfilenode_changed": c_deltas.get("toast_relfilenode_changed"),
                "total_size_delta_bytes": (c_deltas.get("byte_deltas") or {}).get(
                    "total_relation_bytes"
                ),
                "phases_observed": [p["phase"] for p in c_sampling.get("phases_observed") or []],
                "repack_slots_observed": [
                    s["slot_name"] for s in c_sampling.get("repack_slots_observed") or []
                ],
                "exclusive_lock_observed": bool(c_sampling.get("final_lock_observed")),
                "exclusive_lock_evidence": c_sampling.get("final_lock_evidence"),
                "waiting_locks_observed_count": c_sampling.get("waiting_locks_observed_count"),
                "waiting_locks_examples": (c_sampling.get("waiting_locks_examples") or [])[:5],
                "peak_cdc_confirmed_flush_wal_gap_bytes": c_cdc_act.get(
                    "max_wal_gap_bytes_confirmed_flush"
                ),
            },
            "matched_interval": c_intervals["in_interval_scheduled"],
            "full_window": c_intervals["full_window"],
            "run_metrics": c_metrics,
        }

        control_series = {
            "rng_seed": c_seed,
            "repack_start_s": c_start_ms / 1000.0,
            "repack_end_s": c_end_ms / 1000.0,
            "repack_duration_ms": float(c_rep.get("duration_ms") or 0.0),
            "waiting_locks_observed_count": int(
                c_sampling.get("waiting_locks_observed_count") or 0
            ),
            "baseline_ops": matched_base_ops,
            "repack_ops": c_ops,
        }

    digests_identical = (
        all(d == digests_seen[0] for d in digests_seen) if digests_seen else False
    )

    agg = {
        "milestone": "M3",
        "manifest_label": manifest.get("label"),
        "pair_count": len(pairs_out),
        "container_digests_identical_across_all_runs": digests_identical,
        "container_digests": digests_seen[0] if digests_seen else {},
        "convergence_summary": convergence_summary,
        "repack_execution_summary": repack_exec_agg,
        "host_performance_summary": host_perf_summary,
        "pairs": pairs_out,
        "plain_repack_control": control_out,
    }
    return agg, pair_ops_series, control_series


def _fmt_mr(stat: dict[str, Any], *, unit: str = "ms", decimals: int = 2) -> str:
    if not stat or stat.get("median") is None:
        return "n/a"
    fmt = f"{{:.{decimals}f}}"
    med = fmt.format(stat["median"])
    lo = fmt.format(stat["min"])
    hi = fmt.format(stat["max"])
    suffix = f" {unit}" if unit else ""
    return f"{med}{suffix} [{lo}, {hi}]"


def _fmt_mr_signed(stat: dict[str, Any], *, unit: str = "ms", decimals: int = 2) -> str:
    if not stat or stat.get("median") is None:
        return "n/a"
    fmt = f"{{:+.{decimals}f}}"
    med = fmt.format(stat["median"])
    lo = fmt.format(stat["min"])
    hi = fmt.format(stat["max"])
    suffix = f" {unit}" if unit else ""
    return f"{med}{suffix} [{lo}, {hi}]"


def _fmt_int(val: Any) -> str:
    if val is None:
        return "n/a"
    return f"{int(val):,}"


def _fmt_ms(val: Any, decimals: int = 2) -> str:
    if val is None:
        return "n/a"
    return f"{float(val):.{decimals}f} ms"


def render_repeated_results_markdown(agg: dict[str, Any]) -> str:
    """Render deterministic Markdown tables from aggregated raw M3 run evidence."""
    cs = agg["convergence_summary"]
    rx = agg["repack_execution_summary"]
    hp = agg["host_performance_summary"]
    pairs = agg["pairs"]
    ctrl = agg.get("plain_repack_control")

    lines = [
        "# Repeated Matched-Pair Results (Baseline vs Concurrent REPACK)",
        "",
        f"- Matched pairs: `{agg['pair_count']}` (orders: `{cs['orders']}`, counterbalanced: `{cs['counterbalanced_orders']}`)",
        f"- All parameters matched within each pair: `{cs['all_parameters_matched_within_pair']}`",
        f"- Outcomes: Baseline `{cs['baseline_converged_count']}/{cs['pair_count']}` converged, Concurrent REPACK `{cs['repack_converged_count']}/{cs['pair_count']}` converged (`all_converged={cs['all_converged']}`)",
        f"- Concurrent write overlap confirmed in all pairs: `{cs['all_overlaps_confirmed']}`",
        f"- Final source read opened strictly after REPACK finish in all pairs: `{cs['all_source_reads_after_repack']}`",
        f"- Project-only cleanup verified in all runs: `{cs['all_cleanups_ok']}`",
        f"- Container digests identical across all runs: `{agg['container_digests_identical_across_all_runs']}`",
        "",
        "## 1. Per-Run CDC Convergence Evidence",
        "",
        "| Pair / Role | Order | Seed | Outcome | Snapshot ID | Rows (Src = Lake) | Diffs (Src-only / Lake-only / Dup / Val) | Final Vis | Commit-to-Match | Diff Time | Raw Run |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]

    for p in pairs:
        for side_label, side_key in (("Baseline", "baseline"), ("REPACK (CONC)", "repack")):
            m = p[side_key]
            diffs = (
                f"{m['source_only_count']}/{m['lake_only_count']}/"
                f"{m['duplicate_lake_id_count']}/{m['value_mismatch_row_count']}"
            )
            lines.append(
                f"| P{p['pair_index']} {side_label} | `{p['order']}` | `{p['rng_seed']}` | "
                f"`{m['outcome']}` | `{m['verified_snapshot_id']}` | "
                f"`{_fmt_int(m['source_row_count'])}` = `{_fmt_int(m['lake_row_count'])}` | "
                f"`{diffs}` | `{_fmt_ms(m['final_marker_visibility_ms'])}` | "
                f"`{_fmt_ms(m['final_marker_to_verified_match_ms'])}` | "
                f"`{_fmt_ms(m['diff_duration_ms'])}` | `{m['run_dir']}` |"
            )

    if ctrl:
        cm = ctrl["run_metrics"]
        diffs = (
            f"{cm['source_only_count']}/{cm['lake_only_count']}/"
            f"{cm['duplicate_lake_id_count']}/{cm['value_mismatch_row_count']}"
        )
        lines.append(
            f"| Control Plain REPACK | `control` | `{ctrl['rng_seed']}` | "
            f"`{cm['outcome']}` | `{cm['verified_snapshot_id']}` | "
            f"`{_fmt_int(cm['source_row_count'])}` = `{_fmt_int(cm['lake_row_count'])}` | "
            f"`{diffs}` | `{_fmt_ms(cm['final_marker_visibility_ms'])}` | "
            f"`{_fmt_ms(cm['final_marker_to_verified_match_ms'])}` | "
            f"`{_fmt_ms(cm['diff_duration_ms'])}` | `{cm['run_dir']}` |"
        )

    lines.extend(
        [
            "",
            "## 2. Concurrent REPACK Execution Across Pairs",
            "",
            "| Pair | Order | Seed | Duration | Committed Ops During REPACK | Heap/TOAST Swap | Total Size Delta | Peak CDC Confirmed-Flush WAL Gap | Phases Observed | Blocked Waiters / Final Lock |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )

    for p in pairs:
        r = p["repack_execution"]
        swap = f"{r['heap_relfilenode_changed']}/{r['toast_relfilenode_changed']}"
        lines.append(
            f"| Pair {p['pair_index']} | `{p['order']}` | `{p['rng_seed']}` | "
            f"`{_fmt_ms(r['duration_ms'])}` | `{r['committed_during_repack_count']}` | "
            f"`{swap}` | `{_fmt_int(r['total_size_delta_bytes'])} B` | "
            f"`{_fmt_int(r['peak_cdc_confirmed_flush_wal_gap_bytes'])} B` | "
            f"`{r['phases_observed']}` | `{r['waiting_locks_observed_count']}` / `{r['final_lock_observed']}` |"
        )

    swapped_count = sum(
        1
        for p in pairs
        if p["repack_execution"]["heap_relfilenode_changed"]
        and p["repack_execution"]["toast_relfilenode_changed"]
    )
    lines.append(
        f"| **Median [min, max]** | `n={agg['pair_count']}` | `matched` | "
        f"`{_fmt_mr(rx['duration_ms'])}` | `{_fmt_mr(rx['committed_during_repack_count'], unit='ops', decimals=1)}` | "
        f"`{swapped_count}/{agg['pair_count']} swapped` | `{_fmt_mr(rx['total_size_delta_bytes'], unit='B', decimals=0)}` | "
        f"`{_fmt_mr(rx['peak_cdc_confirmed_flush_wal_gap_bytes'], unit='B', decimals=0)}` | "
        f"`n/a` | `{rx['waiting_locks_observed_total']}` total / `{rx['final_lock_observed_count']}/{agg['pair_count']}` |"
    )

    lines.extend(
        [
            "",
            "## 3. Host Performance Summary: Median and Range Across Pairs (This Host Only)",
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
            "",
            "## 4. Per-Run Host Performance Breakdown",
            "",
            "| Run | Attempted/Completed | Err/TO | Missed Sched / Harness | Matched Int Exec p50 / p99 | Matched Int S2C p50 / p99 / max | Full Win S2C p50 / p99 / max | Probe Lag p50 / max |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )

    for p in pairs:
        for side_label, side_key in (("Baseline", "baseline"), ("REPACK (CONC)", "repack")):
            m = p[side_key]
            lines.append(
                f"| P{p['pair_index']} {side_label} | `{m['attempted_ops']}/{m['completed_ops']}` | "
                f"`{m['error_count']}/{m['timeout_count']}` | "
                f"`{m['missed_schedule_count']}/{m['harness_missed_arrival_count']}` | "
                f"`{m['int_exec_p50_ms']} / {m['int_exec_p99_ms']} ms` | "
                f"`{m['int_s2c_p50_ms']} / {m['int_s2c_p99_ms']} / {m['int_s2c_max_ms']} ms` | "
                f"`{m['full_s2c_p50_ms']} / {m['full_s2c_p99_ms']} / {m['full_s2c_max_ms']} ms` | "
                f"`{m['probe_p50_ms']} / {m['probe_max_ms']} ms` |"
            )

    if ctrl:
        cm = ctrl["run_metrics"]
        cr = ctrl["repack_execution"]
        lines.extend(
            [
                f"| Control Plain REPACK | `{cm['attempted_ops']}/{cm['completed_ops']}` | "
                f"`{cm['error_count']}/{cm['timeout_count']}` | "
                f"`{cm['missed_schedule_count']}/{cm['harness_missed_arrival_count']}` | "
                f"`{cm['int_exec_p50_ms']} / {cm['int_exec_p99_ms']} ms` | "
                f"`{cm['int_s2c_p50_ms']} / {cm['int_s2c_p99_ms']} / {cm['int_s2c_max_ms']} ms` | "
                f"`{cm['full_s2c_p50_ms']} / {cm['full_s2c_p99_ms']} / {cm['full_s2c_max_ms']} ms` | "
                f"`{cm['probe_p50_ms']} / {cm['probe_max_ms']} ms` |",
                "",
                "## 5. Plain REPACK Control (`REPACK public.orders`)",
                "",
                f"- Raw evidence: `{ctrl['run_dir']}` (same 250k fixture and seed `{ctrl['rng_seed']}` as Pair 1)",
                f"- Outcome: `{ctrl['outcome']}`, duration `{_fmt_ms(cr['duration_ms'])}`, heap/TOAST swap `{cr['heap_relfilenode_changed']}/{cr['toast_relfilenode_changed']}`",
                f"- `AccessExclusiveLock` observed by sampler: `{cr['exclusive_lock_observed']}` (evidence: `{json.dumps(cr['exclusive_lock_evidence'])}`)",
                f"- Blocked waiter samples on `public.orders`: `{cr['waiting_locks_observed_count']}`",
                f"- Backpressure-delayed ops (full window): `{ctrl['backpressure_delayed_count']}` (`missed_schedule_count={ctrl['missed_schedule_count']}`, `harness_missed_arrival_count={ctrl['harness_missed_arrival_count']}`)",
                f"- Peak CDC slot confirmed-flush WAL gap: `{_fmt_int(cr['peak_cdc_confirmed_flush_wal_gap_bytes'])} B`",
                f"- Phases observed: `{cr['phases_observed']}`, temporary repack slots: `{cr['repack_slots_observed']}`",
            ]
        )

    lines.extend(
        [
            "",
            "## Reproducible Artifacts",
            "",
            "- `repeated_summary.json`: aggregated per-run, per-pair, and median/range JSON",
            "- `repeated_results.md`: this results table",
            "- `latency_comparison.svg`: matched-interval and full-window writer latency comparison",
            "- `repack_window_timeseries.svg`: per-operation latency traces around `t = 20 s..45 s`",
            "- `reader_lag.svg`: steady-state probe lag and final-marker convergence timing",
            "",
        ]
    )
    return "\n".join(lines)


def render_from_manifest(
    manifest_path: Path,
    *,
    output_dir: Path,
    repo_root: Path = REPO_ROOT,
) -> dict[str, Any]:
    """Regenerate repeated_summary.json, repeated_results.md, and all SVG plots from raw runs."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    agg, pair_series, control_series = aggregate_repeated_runs(manifest, repo_root=repo_root)

    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / "repeated_summary.json", agg)
    (output_dir / "repeated_results.md").write_text(
        render_repeated_results_markdown(agg), encoding="utf-8"
    )
    (output_dir / "latency_comparison.svg").write_text(
        render_latency_comparison_svg(agg), encoding="utf-8"
    )
    (output_dir / "repack_window_timeseries.svg").write_text(
        render_repack_window_timeseries_svg(pair_series, control_series=control_series),
        encoding="utf-8",
    )
    (output_dir / "reader_lag.svg").write_text(
        render_reader_lag_svg(agg), encoding="utf-8"
    )
    return agg


def run_repeated_pairs(
    *,
    n_pairs: int = 3,
    seed_rows: int = 250000,
    toast_rows: int = 25000,
    duration_s: float = 180.0,
    rate_ops_s: float = 10.0,
    base_rng_seed: int = 20261119,
    distinct_pair_seeds: bool = True,
    batch_size: int = 2000,
    probe_interval_s: float = 20.0,
    probe_poll_interval_s: float = 0.5,
    repack_delay_s: float = 30.0,
    repack_sample_interval_s: float = 0.05,
    snapshot_timeout_s: float = 300.0,
    convergence_timeout_s: float = 300.0,
    with_plain_repack_control: bool = True,
    label: str = "m3",
) -> tuple[Path, dict[str, Any]]:
    """Run N fresh counterbalanced matched pairs (+ optional plain REPACK control) and render report."""
    orders = default_orders_for_count(n_pairs)
    repeat_dir = REPO_ROOT / "runs" / f"{utc_run_stamp()}-{label}-repeat"
    repeat_dir.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, Any] = {
        "milestone": "M3",
        "label": label,
        "n_pairs": n_pairs,
        "orders": orders,
        "base_rng_seed": base_rng_seed,
        "distinct_pair_seeds": distinct_pair_seeds,
        "with_plain_repack_control": with_plain_repack_control,
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
        pair_seed = base_rng_seed + (idx - 1) if distinct_pair_seeds else base_rng_seed
        print(
            f"=== M3 Pair {idx}/{n_pairs}: order={order}, rng_seed={pair_seed} ===",
            flush=True,
        )
        pair_dir, pair = run_matched_pair(
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
            milestone="M3",
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

    if with_plain_repack_control:
        print(
            f"=== M3 Control: plain REPACK public.orders, rng_seed={base_rng_seed} ===",
            flush=True,
        )
        c_dir, c_sum = run_single_experiment(
            run_kind="plain_repack",
            seed_rows=seed_rows,
            toast_rows=toast_rows,
            duration_s=duration_s,
            rate_ops_s=rate_ops_s,
            rng_seed=base_rng_seed,
            batch_size=batch_size,
            probe_interval_s=probe_interval_s,
            probe_poll_interval_s=probe_poll_interval_s,
            repack_delay_s=repack_delay_s,
            repack_sample_interval_s=repack_sample_interval_s,
            snapshot_timeout_s=snapshot_timeout_s,
            convergence_timeout_s=convergence_timeout_s,
            label=f"{label}-control-plain-repack",
            milestone="M3",
            keep_up=False,
        )
        manifest["plain_repack_control"] = {
            "rng_seed": base_rng_seed,
            "run_dir": _rel_str(c_dir, REPO_ROOT),
            "outcome": c_sum["outcome"],
            "passed": c_sum["passed"],
        }
        write_json(manifest_path, manifest)

    agg = render_from_manifest(manifest_path, output_dir=repeat_dir, repo_root=REPO_ROOT)
    compact = {
        "repeat_dir": _rel_str(repeat_dir, REPO_ROOT),
        "pair_count": agg["pair_count"],
        "all_converged": agg["convergence_summary"]["all_converged"],
        "all_overlaps_confirmed": agg["convergence_summary"]["all_overlaps_confirmed"],
        "all_parameters_matched_within_pair": agg["convergence_summary"][
            "all_parameters_matched_within_pair"
        ],
        "counterbalanced_orders": agg["convergence_summary"]["counterbalanced_orders"],
        "plain_repack_control_outcome": (
            agg["plain_repack_control"]["outcome"] if agg["plain_repack_control"] else None
        ),
    }
    print(json.dumps(compact, indent=2, default=json_default), flush=True)
    return repeat_dir, agg


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run M3 repeated counterbalanced matched pairs (+ optional plain REPACK control) "
            "or regenerate tables and SVG plots reproducibly from a manifest."
        )
    )
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--seed-rows", type=int, default=250000)
    parser.add_argument("--toast-rows", type=int, default=25000)
    parser.add_argument("--duration-s", type=float, default=180.0)
    parser.add_argument("--rate-ops-s", type=float, default=10.0)
    parser.add_argument("--base-rng-seed", type=int, default=20261119)
    parser.add_argument(
        "--same-seed-all-pairs",
        action="store_true",
        help="Reuse --base-rng-seed for all pairs instead of base_rng_seed + pair_index.",
    )
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--probe-interval-s", type=float, default=20.0)
    parser.add_argument("--probe-poll-interval-s", type=float, default=0.5)
    parser.add_argument("--repack-delay-s", type=float, default=30.0)
    parser.add_argument("--repack-sample-interval-s", type=float, default=0.05)
    parser.add_argument("--snapshot-timeout-s", type=float, default=300.0)
    parser.add_argument("--convergence-timeout-s", type=float, default=300.0)
    parser.add_argument(
        "--with-plain-repack-control",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include one fresh plain REPACK public.orders control run (default: enabled).",
    )
    parser.add_argument("--label", default="m3")
    parser.add_argument(
        "--render-from-manifest",
        type=Path,
        default=None,
        help="Regenerate repeated_summary.json, repeated_results.md, and SVG plots from manifest.json.",
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
        agg = render_from_manifest(args.render_from_manifest, output_dir=out_dir)
        print(
            json.dumps(
                {
                    "output_dir": str(out_dir),
                    "pair_count": agg["pair_count"],
                    "all_converged": agg["convergence_summary"]["all_converged"],
                },
                indent=2,
            )
        )
        return

    run_repeated_pairs(
        n_pairs=args.pairs,
        seed_rows=args.seed_rows,
        toast_rows=args.toast_rows,
        duration_s=args.duration_s,
        rate_ops_s=args.rate_ops_s,
        base_rng_seed=args.base_rng_seed,
        distinct_pair_seeds=not args.same_seed_all_pairs,
        batch_size=args.batch_size,
        probe_interval_s=args.probe_interval_s,
        probe_poll_interval_s=args.probe_poll_interval_s,
        repack_delay_s=args.repack_delay_s,
        repack_sample_interval_s=args.repack_sample_interval_s,
        snapshot_timeout_s=args.snapshot_timeout_s,
        convergence_timeout_s=args.convergence_timeout_s,
        with_plain_repack_control=args.with_plain_repack_control,
        label=args.label,
    )


if __name__ == "__main__":
    main()
