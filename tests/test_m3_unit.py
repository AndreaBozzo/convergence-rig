"""Unit tests for M3 repeated matched-pair aggregation, plain REPACK control, and reproducible plots."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from lab.m3 import (
    aggregate_repeated_runs,
    build_paired_metric,
    default_orders_for_count,
    median_and_range,
    render_from_manifest,
)


def _write_synthetic_run(
    run_dir: Path,
    *,
    run_kind: str,
    rng_seed: int,
    exec_ms: float,
    int_exec_ms: float,
    probe_p50_ms: float,
    final_vis_ms: float,
    diff_ms: float,
    repack_duration_ms: float = 4000.0,
    repack_waiters: int = 0,
    exclusive_lock: bool = False,
    rate_ops_s: float = 10.0,
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    repack_start_ms = 30000.0
    repack_end_ms = repack_start_ms + repack_duration_ms

    ops: list[dict[str, Any]] = []
    for seq in range(500):
        sched_ms = seq * 100.0
        in_repack = repack_start_ms <= sched_ms <= repack_end_ms
        if run_kind == "plain_repack" and in_repack:
            # Simulate plain REPACK blocking until repack_end_ms + 50 ms
            start_ms = sched_ms + 1.0 if seq == 300 else max(sched_ms + 1.0, repack_end_ms + (seq - 300) * 15.0)
            ex = (repack_end_ms + 20.0 - start_ms) if seq == 300 else 15.0
            comp_ms = start_ms + ex
            q_ms = start_ms - sched_ms
            bp_ms = max(0.0, q_ms - 1.0)
            s2c_ms = comp_ms - sched_ms
        else:
            ex = int_exec_ms if in_repack else exec_ms
            q_ms = 1.0
            bp_ms = 0.0
            start_ms = sched_ms + q_ms
            comp_ms = start_ms + ex
            s2c_ms = comp_ms - sched_ms
        ops.append(
            {
                "seq": seq,
                "op": "update_normal",
                "target_id": 1000 + seq,
                "scheduled_offset_ms": round(sched_ms, 3),
                "start_offset_ms": round(start_ms, 3),
                "complete_offset_ms": round(comp_ms, 3),
                "queue_delay_ms": round(q_ms, 3),
                "harness_dispatch_lag_ms": 1.0,
                "backpressure_delay_ms": round(bp_ms, 3),
                "exec_latency_ms": round(ex, 3),
                "sched_to_complete_ms": round(s2c_ms, 3),
                "affected_rows": 1,
                "outcome": "ok",
                "error": None,
            }
        )

    with (run_dir / "ops.jsonl").open("w", encoding="utf-8") as fh:
        for row in ops:
            fh.write(json.dumps(row) + "\n")

    repack_block: dict[str, Any] | None = None
    if run_kind in ("repack", "plain_repack"):
        repack_block = {
            "command": "REPACK (CONCURRENTLY) public.orders" if run_kind == "repack" else "REPACK public.orders",
            "repack_pid": 500,
            "start_offset_ms": repack_start_ms,
            "end_offset_ms": repack_end_ms,
            "duration_ms": repack_duration_ms,
            "start_utc": "2026-11-19T21:00:30.000000Z",
            "end_utc": "2026-11-19T21:00:34.000000Z",
            "outcome": "ok",
            "sizes_before": {
                "heap_bytes": 46_000_000,
                "toast_relation_bytes": 415_000_000,
                "indexes_bytes": 5_600_000,
                "total_relation_bytes": 466_600_000,
            },
            "sizes_after": {
                "heap_bytes": 46_200_000,
                "toast_relation_bytes": 414_500_000,
                "indexes_bytes": 5_600_000,
                "total_relation_bytes": 466_300_000,
            },
            "size_deltas": {
                "byte_deltas": {"total_relation_bytes": -300_000},
                "heap_relfilenode_changed": True,
                "toast_relfilenode_changed": True,
            },
            "sampling": {
                "sample_interval_s": 0.05,
                "sample_count": 50,
                "progress_sample_count": 48,
                "phases_observed": [{"phase": "seq scanning heap"}, {"phase": "rebuilding index"}],
                "repack_slots_observed": [{"slot_name": "pg_repack_500"}] if run_kind == "repack" else [],
                "waiting_locks_observed_count": repack_waiters,
                "waiting_locks_examples": (
                    [{"offset_ms": 30100.0, "pid": 501, "mode": "RowExclusiveLock", "blocking_pids": [500]}]
                    if repack_waiters > 0
                    else []
                ),
                "final_lock_observed": exclusive_lock,
                "final_lock_evidence": (
                    {
                        "triggers": ["repack_lock:orders:AccessExclusiveLock:granted=True"],
                        "sample_hits": 40,
                        "first_offset_ms": 30010.0,
                        "last_offset_ms": 33990.0,
                        "observed_span_ms": 3980.0,
                    }
                    if exclusive_lock
                    else None
                ),
                "cdc_slot_activity": {
                    "slot_name": "pg2iceberg_slot",
                    "sample_count": 50,
                    "active_sample_count": 50,
                    "min_wal_gap_bytes_confirmed_flush": 1000,
                    "max_wal_gap_bytes_confirmed_flush": 400_000_000,
                    "min_wal_gap_bytes_restart": 2000,
                    "max_wal_gap_bytes_restart": 400_100_000,
                },
            },
        }

    summary = {
        "milestone": "M3",
        "run_kind": run_kind,
        "label": f"synth-{run_kind}",
        "outcome": "converged",
        "passed": True,
        "parameters": {
            "seed_rows": 250000,
            "toast_rows": 25000,
            "duration_s": 50.0,
            "rate_ops_s": rate_ops_s,
            "rng_seed": rng_seed,
            "batch_size": 2000,
            "probe_interval_s": 20.0,
            "probe_poll_interval_s": 0.5,
        },
        "container_digests": {"pg-source": "sha256:aaa", "pg2iceberg": "sha256:bbb"},
        "workload": {
            "actual_duration_s": 50.0,
            "arrival_schedule": {
                "miss_threshold_ms": 250.0,
                "backpressure_delayed_count": 40 if run_kind == "plain_repack" else 0,
                "schedule_valid": True,
            },
            "steady_state_probes": {
                "probe_count": 2,
                "visible_count": 2,
                "reader_error_count": 0,
                "visibility_lag_ms": {
                    "count": 2,
                    "min_ms": probe_p50_ms - 1000.0,
                    "p50_ms": probe_p50_ms,
                    "p99_ms": probe_p50_ms + 1000.0,
                    "max_ms": probe_p50_ms + 1000.0,
                },
            },
        },
        "repack": repack_block,
        "convergence": {
            "outcome": "converged",
            "passed": True,
            "verified_snapshot_id": 100000 + rng_seed,
            "final_marker_visibility_ms": final_vis_ms,
            "final_marker_to_verified_match_ms": final_vis_ms + diff_ms,
            "diff_duration_ms": diff_ms,
            "snapshot_attempts": [
                {
                    "source_read_opened_utc": "2026-11-19T21:05:00.000000Z",
                    "schema_check": {"passed": True, "schema_id": 0},
                    "row_comparison": {
                        "source_row_count": 250000,
                        "lake_row_count": 250000,
                        "source_only_count": 0,
                        "lake_only_count": 0,
                        "duplicate_lake_id_count": 0,
                        "value_mismatch_row_count": 0,
                    },
                }
            ],
        },
        "duckdb_final_metadata_summary": {
            "live_data_files": 40,
            "live_equality_delete_files": 15,
            "live_equality_delete_records": 1500,
        },
        "pipeline_positions": {
            "final": {
                "replication_slots": [
                    {"slot_name": "pg2iceberg_slot", "wal_gap_bytes_confirmed_flush": 1200}
                ]
            }
        },
        "cleanup": {
            "all_project_resources_removed": True,
            "non_project_containers_unchanged": True,
        },
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")


def test_default_orders_and_median_range_math() -> None:
    assert default_orders_for_count(3) == ["baseline-first", "repack-first", "baseline-first"]
    assert median_and_range([]) == {"n": 0, "median": None, "min": None, "max": None, "values": []}
    assert median_and_range([30.0, 10.0, 20.0]) == {
        "n": 3,
        "median": 20.0,
        "min": 10.0,
        "max": 30.0,
        "values": [30.0, 10.0, 20.0],
    }
    pm = build_paired_metric([10.0, 12.0, 14.0], [20.0, 25.0, 24.0])
    assert pm["baseline"]["median"] == 12.0
    assert pm["repack"]["median"] == 24.0
    assert pm["paired_delta_repack_minus_baseline"]["median"] == 10.0
    assert pm["paired_delta_repack_minus_baseline"]["min"] == 10.0
    assert pm["paired_delta_repack_minus_baseline"]["max"] == 13.0


def test_render_from_manifest_aggregates_three_pairs_control_and_is_byte_reproducible(
    tmp_path: Path,
) -> None:
    runs = tmp_path / "runs"
    pair_specs = [
        (1, "baseline-first", 20261119, 14.0, 18.0, 28.0, 10500.0, 11000.0, 18000.0),
        (2, "repack-first", 20261120, 15.0, 20.0, 32.0, 11200.0, 10000.0, 17500.0),
        (3, "baseline-first", 20261121, 13.0, 19.0, 30.0, 10800.0, 12000.0, 19000.0),
    ]
    manifest_pairs = []
    for idx, order, seed, base_ex, base_int, rep_int, probe_p50, fvis, diff_ms in pair_specs:
        b_dir = runs / f"p{idx}-baseline"
        r_dir = runs / f"p{idx}-repack"
        _write_synthetic_run(
            b_dir,
            run_kind="baseline",
            rng_seed=seed,
            exec_ms=base_ex,
            int_exec_ms=base_int,
            probe_p50_ms=probe_p50,
            final_vis_ms=fvis,
            diff_ms=diff_ms,
        )
        _write_synthetic_run(
            r_dir,
            run_kind="repack",
            rng_seed=seed,
            exec_ms=base_ex + 0.5,
            int_exec_ms=rep_int,
            probe_p50_ms=probe_p50 + 300.0,
            final_vis_ms=fvis - 500.0,
            diff_ms=diff_ms - 200.0,
        )
        manifest_pairs.append(
            {
                "pair_index": idx,
                "order": order,
                "rng_seed": seed,
                "baseline_run_dir": f"runs/p{idx}-baseline",
                "repack_run_dir": f"runs/p{idx}-repack",
            }
        )

    ctrl_dir = runs / "control-plain-repack"
    _write_synthetic_run(
        ctrl_dir,
        run_kind="plain_repack",
        rng_seed=20261119,
        exec_ms=14.0,
        int_exec_ms=14.0,
        probe_p50_ms=11000.0,
        final_vis_ms=11500.0,
        diff_ms=18000.0,
        repack_duration_ms=3500.0,
        repack_waiters=65,
        exclusive_lock=True,
    )

    manifest = {
        "milestone": "M3",
        "label": "m3-synth",
        "pairs": manifest_pairs,
        "plain_repack_control": {"rng_seed": 20261119, "run_dir": "runs/control-plain-repack"},
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    agg1 = render_from_manifest(manifest_path, output_dir=out1, repo_root=tmp_path)
    agg2 = render_from_manifest(manifest_path, output_dir=out2, repo_root=tmp_path)

    assert agg1 == agg2
    assert agg1["pair_count"] == 3
    assert agg1["convergence_summary"]["all_converged"] is True
    assert agg1["convergence_summary"]["all_parameters_matched_within_pair"] is True
    assert agg1["convergence_summary"]["counterbalanced_orders"] is True
    assert agg1["convergence_summary"]["all_overlaps_confirmed"] is True
    assert agg1["convergence_summary"]["all_cleanups_ok"] is True
    assert agg1["container_digests_identical_across_all_runs"] is True

    # Median matched-interval exec p50: baseline [18, 20, 19] -> 19.0; repack [28, 32, 30] -> 30.0; delta [10, 12, 11] -> 11.0
    int_exec = agg1["host_performance_summary"]["matched_interval"]["exec_p50_ms"]
    assert int_exec["baseline"]["median"] == 19.0
    assert int_exec["repack"]["median"] == 30.0
    assert int_exec["paired_delta_repack_minus_baseline"]["median"] == 11.0

    # Plain REPACK control must show exclusive lock, blocked waiters, and 0 ops committed during REPACK
    ctrl = agg1["plain_repack_control"]
    assert ctrl is not None
    assert ctrl["repack_execution"]["exclusive_lock_observed"] is True
    assert ctrl["repack_execution"]["waiting_locks_observed_count"] == 65
    assert ctrl["repack_execution"]["committed_during_repack_count"] == 0

    # Byte-for-byte reproducibility of Markdown, JSON, and all 3 SVG plots
    for fname in (
        "repeated_summary.json",
        "repeated_results.md",
        "latency_comparison.svg",
        "repack_window_timeseries.svg",
        "reader_lag.svg",
    ):
        b1 = (out1 / fname).read_bytes()
        b2 = (out2 / fname).read_bytes()
        assert b1 == b2, f"{fname} was not byte-for-byte reproducible"
        assert len(b1) > 200
        if fname.endswith(".svg"):
            text = b1.decode("utf-8")
            assert text.startswith("<svg ")
            assert "</svg>" in text


def test_aggregate_repeated_runs_flags_within_pair_parameter_mismatch(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    _write_synthetic_run(
        runs / "b1",
        run_kind="baseline",
        rng_seed=20261119,
        exec_ms=15.0,
        int_exec_ms=15.0,
        probe_p50_ms=10000.0,
        final_vis_ms=11000.0,
        diff_ms=15000.0,
        rate_ops_s=10.0,
    )
    _write_synthetic_run(
        runs / "r1",
        run_kind="repack",
        rng_seed=20261199,  # Mismatched seed!
        exec_ms=15.0,
        int_exec_ms=25.0,
        probe_p50_ms=10000.0,
        final_vis_ms=11000.0,
        diff_ms=15000.0,
        rate_ops_s=10.0,
    )
    manifest = {
        "pairs": [
            {
                "pair_index": 1,
                "order": "baseline-first",
                "baseline_run_dir": "runs/b1",
                "repack_run_dir": "runs/r1",
            }
        ]
    }
    agg, _, _ = aggregate_repeated_runs(manifest, repo_root=tmp_path)
    assert agg["convergence_summary"]["all_parameters_matched_within_pair"] is False
