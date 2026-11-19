"""Unit tests for M2 REPACK sampling summary, matched-interval analysis, and pair reporting."""

from __future__ import annotations

from lab.repack import (
    analyze_ops_intervals,
    build_pair_comparison,
    compute_size_deltas,
    render_pair_results_markdown,
    summarize_repack_samples,
)


def test_compute_size_deltas_detects_relfilenode_swaps_and_byte_changes() -> None:
    before = {
        "heap_relfilenode": 16391,
        "toast_relfilenode": 16394,
        "heap_bytes": 50_000_000,
        "toast_relation_bytes": 400_000_000,
        "table_and_toast_bytes": 450_000_000,
        "indexes_bytes": 6_000_000,
        "total_relation_bytes": 456_000_000,
        "indexes": [
            {"index_name": "orders_pkey", "index_oid": 16398, "index_relfilenode": 16398, "size_bytes": 6_000_000}
        ],
    }
    after = {
        "heap_relfilenode": 24576,
        "toast_relfilenode": 24579,
        "heap_bytes": 46_000_000,
        "toast_relation_bytes": 395_000_000,
        "table_and_toast_bytes": 441_000_000,
        "indexes_bytes": 5_500_000,
        "total_relation_bytes": 446_500_000,
        "indexes": [
            {"index_name": "orders_pkey", "index_oid": 16398, "index_relfilenode": 24583, "size_bytes": 5_500_000}
        ],
    }
    d = compute_size_deltas(before, after)
    assert d["heap_relfilenode_changed"] is True
    assert d["toast_relfilenode_changed"] is True
    assert d["byte_deltas"]["heap_bytes"] == -4_000_000
    assert d["byte_deltas"]["total_relation_bytes"] == -9_500_000
    assert d["indexes"]["orders_pkey"]["delta_bytes"] == -500_000
    assert d["indexes"]["orders_pkey"]["relfilenode_changed"] is True


def test_summarize_repack_samples_without_final_lock_observed() -> None:
    samples = [
        {
            "offset_ms": 30050.0,
            "progress": {
                "pid": 500,
                "command": "REPACK",
                "phase": "initializing",
                "heap_blks_total": 5000,
                "heap_blks_scanned": 0,
                "heap_tuples_scanned": 0,
                "heap_tuples_inserted": 0,
                "heap_tuples_updated": 0,
                "heap_tuples_deleted": 0,
                "index_rebuild_count": 0,
            },
            "repack_locks": [
                {"locktype": "relation", "relation_name": "orders", "mode": "ShareUpdateExclusiveLock", "granted": True}
            ],
            "waiting_locks_on_orders": [],
            "replication_slots": [
                {
                    "slot_name": "pg2iceberg_slot",
                    "plugin": "pgoutput",
                    "slot_type": "logical",
                    "temporary": False,
                    "active": True,
                    "wal_gap_bytes_confirmed_flush": 1200,
                    "wal_gap_bytes_restart": 4000,
                },
                {
                    "slot_name": "pg_repack_500",
                    "plugin": "pgrepack",
                    "slot_type": "logical",
                    "temporary": True,
                    "active": True,
                    "wal_gap_bytes_confirmed_flush": 500,
                    "wal_gap_bytes_restart": 1000,
                },
            ],
        },
        {
            "offset_ms": 31500.0,
            "progress": {
                "pid": 500,
                "command": "REPACK",
                "phase": "seq scanning heap",
                "heap_blks_total": 5000,
                "heap_blks_scanned": 4200,
                "heap_tuples_scanned": 210000,
                "heap_tuples_inserted": 210000,
                "heap_tuples_updated": 0,
                "heap_tuples_deleted": 0,
                "index_rebuild_count": 0,
            },
            "repack_locks": [
                {"locktype": "relation", "relation_name": "orders", "mode": "ShareUpdateExclusiveLock", "granted": True}
            ],
            "waiting_locks_on_orders": [],
            "replication_slots": [
                {
                    "slot_name": "pg2iceberg_slot",
                    "plugin": "pgoutput",
                    "slot_type": "logical",
                    "temporary": False,
                    "active": True,
                    "wal_gap_bytes_confirmed_flush": 48000,
                    "wal_gap_bytes_restart": 96000,
                },
                {
                    "slot_name": "pg_repack_500",
                    "plugin": "pgrepack",
                    "slot_type": "logical",
                    "temporary": True,
                    "active": True,
                    "wal_gap_bytes_confirmed_flush": 22000,
                    "wal_gap_bytes_restart": 44000,
                },
            ],
        },
    ]

    summary = summarize_repack_samples(samples, sample_interval_s=0.05)
    assert summary["sample_count"] == 2
    assert summary["progress_sample_count"] == 2
    assert [p["phase"] for p in summary["phases_observed"]] == ["initializing", "seq scanning heap"]
    assert summary["final_lock_observed"] is False
    assert summary["final_lock_evidence"] is None
    assert len(summary["repack_slots_observed"]) == 1
    assert summary["repack_slots_observed"][0]["slot_name"] == "pg_repack_500"
    assert summary["repack_slots_observed"][0]["max_wal_gap_bytes_confirmed_flush"] == 22000
    assert summary["cdc_slot_activity"]["min_wal_gap_bytes_confirmed_flush"] == 1200
    assert summary["cdc_slot_activity"]["max_wal_gap_bytes_confirmed_flush"] == 48000


def test_summarize_repack_samples_detects_final_lock_and_blocked_waiters() -> None:
    samples = [
        {
            "offset_ms": 34000.0,
            "progress": {"pid": 500, "command": "REPACK", "phase": "swapping relation files"},
            "repack_locks": [
                {"locktype": "relation", "relation_name": "orders", "mode": "AccessExclusiveLock", "granted": True}
            ],
            "waiting_locks_on_orders": [
                {"pid": 510, "mode": "RowExclusiveLock", "granted": False, "blocking_pids": [500]}
            ],
            "replication_slots": [],
        },
        {
            "offset_ms": 34060.0,
            "progress": {"pid": 500, "command": "REPACK", "phase": "swapping relation files"},
            "repack_locks": [
                {"locktype": "relation", "relation_name": "orders", "mode": "AccessExclusiveLock", "granted": True}
            ],
            "waiting_locks_on_orders": [],
            "replication_slots": [],
        },
    ]
    summary = summarize_repack_samples(samples, sample_interval_s=0.05)
    assert summary["final_lock_observed"] is True
    assert summary["final_lock_evidence"] is not None
    assert summary["final_lock_evidence"]["first_offset_ms"] == 34000.0
    assert summary["final_lock_evidence"]["last_offset_ms"] == 34060.0
    assert summary["final_lock_evidence"]["observed_span_ms"] == 60.0
    assert summary["waiting_locks_observed_count"] == 1


def test_analyze_ops_intervals_slices_and_counts_accurately() -> None:
    ops = [
        # Pre-interval
        {
            "seq": 0,
            "op": "update_normal",
            "scheduled_offset_ms": 100.0,
            "start_offset_ms": 101.0,
            "complete_offset_ms": 120.0,
            "queue_delay_ms": 1.0,
            "harness_dispatch_lag_ms": 1.0,
            "backpressure_delay_ms": 0.0,
            "exec_latency_ms": 19.0,
            "sched_to_complete_ms": 20.0,
            "affected_rows": 1,
            "outcome": "ok",
        },
        # Started before REPACK, completed during REPACK
        {
            "seq": 1,
            "op": "insert",
            "scheduled_offset_ms": 990.0,
            "start_offset_ms": 992.0,
            "complete_offset_ms": 1020.0,
            "queue_delay_ms": 2.0,
            "harness_dispatch_lag_ms": 2.0,
            "backpressure_delay_ms": 0.0,
            "exec_latency_ms": 28.0,
            "sched_to_complete_ms": 30.0,
            "affected_rows": 1,
            "outcome": "ok",
        },
        # Strictly inside REPACK [1000, 2000]
        {
            "seq": 2,
            "op": "update_toast_unchanged",
            "scheduled_offset_ms": 1100.0,
            "start_offset_ms": 1105.0,
            "complete_offset_ms": 1155.0,
            "queue_delay_ms": 5.0,
            "harness_dispatch_lag_ms": 5.0,
            "backpressure_delay_ms": 0.0,
            "exec_latency_ms": 50.0,
            "sched_to_complete_ms": 55.0,
            "affected_rows": 1,
            "outcome": "ok",
        },
        # Strictly inside REPACK, delayed by backpressure (> 250ms) but NOT harness lag
        {
            "seq": 3,
            "op": "delete",
            "scheduled_offset_ms": 1200.0,
            "start_offset_ms": 1500.0,
            "complete_offset_ms": 1540.0,
            "queue_delay_ms": 300.0,
            "harness_dispatch_lag_ms": 2.0,
            "backpressure_delay_ms": 298.0,
            "exec_latency_ms": 40.0,
            "sched_to_complete_ms": 340.0,
            "affected_rows": 1,
            "outcome": "ok",
        },
        # Timeout inside REPACK
        {
            "seq": 4,
            "op": "update_normal",
            "scheduled_offset_ms": 1600.0,
            "start_offset_ms": 1601.0,
            "complete_offset_ms": 1900.0,
            "queue_delay_ms": 1.0,
            "harness_dispatch_lag_ms": 1.0,
            "backpressure_delay_ms": 0.0,
            "exec_latency_ms": 299.0,
            "sched_to_complete_ms": 300.0,
            "affected_rows": 0,
            "outcome": "timeout",
        },
        # Post-interval
        {
            "seq": 5,
            "op": "update_toast_changed",
            "scheduled_offset_ms": 2500.0,
            "start_offset_ms": 2501.0,
            "complete_offset_ms": 2525.0,
            "queue_delay_ms": 1.0,
            "harness_dispatch_lag_ms": 1.0,
            "backpressure_delay_ms": 0.0,
            "exec_latency_ms": 24.0,
            "sched_to_complete_ms": 25.0,
            "affected_rows": 1,
            "outcome": "ok",
        },
    ]

    res = analyze_ops_intervals(
        ops,
        interval_start_ms=1000.0,
        interval_end_ms=2000.0,
        total_window_duration_ms=3000.0,
        miss_threshold_ms=250.0,
    )
    assert res["full_window"]["attempted_ops"] == 6
    assert res["full_window"]["completed_ops"] == 5
    assert res["full_window"]["error_count"] == 1
    assert res["full_window"]["timeout_count"] == 1
    assert res["full_window"]["missed_schedule_count"] == 1
    assert res["full_window"]["harness_missed_arrival_count"] == 0

    assert res["in_interval_scheduled"]["attempted_ops"] == 3
    assert res["in_interval_scheduled"]["completed_ops"] == 2
    assert res["in_interval_scheduled"]["timeout_count"] == 1
    assert res["in_interval_scheduled"]["latency_sched_to_complete_ms"]["count"] == 2
    assert res["in_interval_scheduled"]["latency_sched_to_complete_ms"]["max_ms"] == 340.0

    # seq 1, 2, 3 committed inside [1000, 2000] (seq 4 timed out)
    assert res["in_interval_committed"]["completed_ops"] == 3
    # seq 2, 3, 4 strictly contained in [1000, 2000] (2 ok + 1 timeout)
    assert res["in_interval_strictly_contained"]["attempted_ops"] == 3
    assert res["in_interval_strictly_contained"]["completed_ops"] == 2
    # seq 1, 2, 3, 4 any overlap
    assert res["in_interval_any_overlap"]["attempted_ops"] == 4


def test_build_pair_comparison_and_markdown_respects_final_lock_flag() -> None:
    base_params = {
        "seed_rows": 250000,
        "toast_rows": 25000,
        "duration_s": 180.0,
        "rate_ops_s": 10.0,
        "rng_seed": 20261119,
        "batch_size": 2000,
        "probe_interval_s": 20.0,
        "probe_poll_interval_s": 0.5,
    }
    b_sum = {
        "outcome": "converged",
        "passed": True,
        "parameters": base_params,
        "workload": {
            "actual_duration_s": 10.0,
            "arrival_schedule": {"miss_threshold_ms": 250.0},
            "steady_state_probes": {
                "probe_count": 1,
                "visible_count": 1,
                "reader_error_count": 0,
                "visibility_lag_ms": {"count": 1, "p50_ms": 10000.0, "p99_ms": 10000.0, "max_ms": 10000.0},
            },
        },
        "convergence": {
            "verified_snapshot_id": 111,
            "final_marker_visibility_ms": 12000.0,
            "final_marker_to_verified_match_ms": 15000.0,
            "diff_duration_ms": 2500.0,
            "snapshot_attempts": [
                {
                    "source_read_opened_utc": "2026-11-19T20:10:00.000000Z",
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
    }
    r_sum = {
        **b_sum,
        "convergence": {
            **b_sum["convergence"],
            "verified_snapshot_id": 222,
            "snapshot_attempts": [
                {
                    **b_sum["convergence"]["snapshot_attempts"][0],
                    "source_read_opened_utc": "2026-11-19T20:15:00.000000Z",
                }
            ],
        },
        "repack": {
            "command": "REPACK (CONCURRENTLY) public.orders",
            "repack_pid": 999,
            "start_offset_ms": 1000.0,
            "end_offset_ms": 2000.0,
            "duration_ms": 1000.0,
            "start_utc": "2026-11-19T20:11:00.000000Z",
            "end_utc": "2026-11-19T20:11:01.000000Z",
            "outcome": "ok",
            "sizes_before": {"heap_bytes": 100, "toast_relation_bytes": 200, "indexes_bytes": 50, "total_relation_bytes": 350},
            "sizes_after": {"heap_bytes": 90, "toast_relation_bytes": 190, "indexes_bytes": 45, "total_relation_bytes": 325},
            "size_deltas": {"heap_relfilenode_changed": True, "toast_relfilenode_changed": True},
            "sampling": {
                "sample_interval_s": 0.05,
                "sample_count": 20,
                "progress_sample_count": 18,
                "phases_observed": [{"phase": "seq scanning heap"}],
                "repack_slots_observed": [{"slot_name": "pg_repack_999"}],
                "final_lock_observed": False,
                "final_lock_evidence": None,
            },
        },
    }
    ops = [
        {
            "seq": 0,
            "op": "update_normal",
            "scheduled_offset_ms": 1200.0,
            "start_offset_ms": 1201.0,
            "complete_offset_ms": 1220.0,
            "queue_delay_ms": 1.0,
            "harness_dispatch_lag_ms": 1.0,
            "backpressure_delay_ms": 0.0,
            "exec_latency_ms": 19.0,
            "sched_to_complete_ms": 20.0,
            "affected_rows": 1,
            "outcome": "ok",
        }
    ]
    pair = build_pair_comparison(
        b_sum,
        r_sum,
        baseline_ops=ops,
        repack_ops=ops,
        baseline_run_dir="runs/b",
        repack_run_dir="runs/r",
    )
    assert pair["parameters_matched"] is True
    assert pair["overlap_confirmed"] is True
    assert pair["committed_during_repack_count"] == 1
    assert pair["source_read_opened_after_repack"] is True
    assert pair["host_performance"]["final_lock_observed"] is False
    assert pair["host_performance"]["final_lock_ops"] is None

    md = render_pair_results_markdown(pair)
    assert "## 1. CDC Convergence Evidence" in md
    assert "## 3. Host Performance Observations" in md
    assert "Final-lock-specific figures are omitted" in md
