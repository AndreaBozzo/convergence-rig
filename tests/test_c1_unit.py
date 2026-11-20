"""Unit and fault-injection tests for C1 compaction evidence, validity gates, and M3 comparison."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from lab.c1 import (
    DEFAULT_COMPACTION_CONFIG,
    EXPECTED_STARTUP_WARN_SUBSTRING,
    aggregate_c1_and_compare_m3,
    analyze_compaction_evidence,
    analyze_pg2iceberg_log,
    parse_pg2iceberg_compaction_metrics,
    parse_utc_iso_to_ms,
    render_c1_from_manifest,
)
from lab.config import REPO_ROOT
from tests.test_m3_unit import _write_synthetic_run


def _make_lk_meta(
    snapshots: list[dict[str, Any]],
) -> dict[str, Any]:
    return {"metadata": {"snapshots": snapshots}}


def _snap(
    seq: int,
    snap_id: int,
    parent_id: int | None,
    ts_utc: str,
    op: str,
    *,
    tot_data: int,
    tot_del: int,
    tot_eq: int = 0,
    add_data: int = 1,
    del_data: int = 0,
    rem_del_files: int = 0,
    rem_eq_dels: int = 0,
    pg2_op: str | None = None,
) -> dict[str, Any]:
    sm: dict[str, str] = {
        "operation": op,
        "added-data-files": str(add_data),
        "total-data-files": str(tot_data),
        "total-delete-files": str(tot_del),
        "total-equality-deletes": str(tot_eq),
        "total-position-deletes": "0",
        "total-records": "250000",
        "total-files-size": "340000000",
    }
    if del_data:
        sm["deleted-data-files"] = str(del_data)
    if rem_del_files:
        sm["removed-delete-files"] = str(rem_del_files)
        sm["removed-equality-delete-files"] = str(rem_del_files)
    if rem_eq_dels:
        sm["removed-equality-deletes"] = str(rem_eq_dels)
    if pg2_op:
        sm["pg2iceberg.operation"] = pg2_op
    return {
        "sequence-number": seq,
        "snapshot-id": snap_id,
        "parent-snapshot-id": parent_id,
        "timestamp-ms": parse_utc_iso_to_ms(ts_utc),
        "summary": sm,
    }


def test_compose_defaults_preserved_and_compaction_config_valid() -> None:
    compose_text = (REPO_ROOT / "compose" / "docker-compose.yml").read_text(encoding="utf-8")
    assert "${PG2ICEBERG_CONFIG_FILE:-pg2iceberg.yaml}:/etc/pg2iceberg/config.yaml:ro" in compose_text

    default_cfg = (REPO_ROOT / "compose" / "pg2iceberg.yaml").read_text(encoding="utf-8")
    assert "target_file_size: 0" in default_cfg

    comp_cfg = (REPO_ROOT / "compose" / DEFAULT_COMPACTION_CONFIG).read_text(encoding="utf-8")
    assert "target_file_size: 134217728" in comp_cfg
    assert "compaction_data_files: 4" in comp_cfg
    assert "compaction_delete_files: 6" in comp_cfg
    assert "compaction_delete_ratio: 0.1" in comp_cfg


def test_compaction_evidence_detects_in_window_merges_delete_rewrites_and_overlaps() -> None:
    summary = {
        "workload": {
            "window_start_utc": "2026-11-20T00:00:00.000000Z",
            "window_end_utc": "2026-11-20T00:03:00.000000Z",
        },
        "repack": {
            "start_utc": "2026-11-20T00:00:30.000000Z",
            "end_utc": "2026-11-20T00:00:35.000000Z",
        },
        "final_marker_commit_utc": "2026-11-20T00:03:00.200000Z",
        "convergence": {
            "first_marker_snapshot_id": 106,
            "verified_snapshot_id": 106,
        },
    }
    lk = _make_lk_meta(
        [
            _snap(1, 100, None, "2026-11-19T23:59:50.000000Z", "append", tot_data=25, tot_del=0),
            # Small-file merge overlapping REPACK window at t=32s
            _snap(
                2,
                101,
                100,
                "2026-11-20T00:00:32.000000Z",
                "replace",
                tot_data=4,
                tot_del=2,
                tot_eq=150,
                add_data=1,
                del_data=2,
                pg2_op="compaction",
            ),
            _snap(3, 102, 101, "2026-11-20T00:01:00.000000Z", "append", tot_data=5, tot_del=6, tot_eq=500),
            # Full delete-retiring compaction at t=70s
            _snap(
                4,
                103,
                102,
                "2026-11-20T00:01:10.000000Z",
                "replace",
                tot_data=3,
                tot_del=0,
                tot_eq=0,
                add_data=3,
                del_data=5,
                rem_del_files=6,
                rem_eq_dels=500,
                pg2_op="compaction",
            ),
            _snap(5, 105, 103, "2026-11-20T00:03:04.000000Z", "replace", tot_data=4, tot_del=2, tot_eq=120, add_data=1, del_data=2, pg2_op="compaction"),
            _snap(6, 106, 105, "2026-11-20T00:03:05.000000Z", "append", tot_data=5, tot_del=3, tot_eq=180),
        ]
    )
    ev = analyze_compaction_evidence(summary, lk)
    assert ev["compaction_ran_in_window"] is True
    assert ev["compaction_commits_in_window"] == 2
    assert ev["small_file_merge_commits_in_window"] == 1
    assert ev["delete_rewrite_commits_in_window"] == 1
    assert ev["compaction_commits_up_to_verified"] == 3
    assert ev["verified_snapshot_files"]["live_data_files"] == 5
    assert ev["verified_snapshot_files"]["live_equality_delete_files"] == 3
    assert ev["verified_snapshot_files"]["live_equality_delete_records"] == 180
    assert ev["repack_overlap"]["commit_inside_repack_window"] is True
    assert ev["repack_overlap"]["commit_inside_repack_count"] == 1
    assert ev["final_marker_overlap"]["verified_snapshot_is_compaction"] is False
    assert ev["final_marker_overlap"]["parent_of_verified_is_compaction"] is True
    assert ev["final_marker_overlap"]["compaction_between_final_marker_and_verified"] is True


def test_fault_zero_compaction_in_window_marks_run_invalid(tmp_path: Path) -> None:
    """Fault test: a run with zero compaction commits inside the measurement window must fail as invalid."""
    runs = tmp_path / "runs"
    for kind in ("baseline", "repack"):
        rdir = runs / f"p1-{kind}"
        _write_synthetic_run(
            rdir,
            run_kind=kind,
            rng_seed=20261119,
            exec_ms=14.0,
            int_exec_ms=20.0 if kind == "baseline" else 30.0,
            probe_p50_ms=10000.0,
            final_vis_ms=9000.0,
            diff_ms=10000.0,
        )
        # Add window timestamps and append-only Lakekeeper metadata (NO compaction!).
        s = json.loads((rdir / "summary.json").read_text(encoding="utf-8"))
        s["workload"]["window_start_utc"] = "2026-11-20T00:00:00.000000Z"
        s["workload"]["window_end_utc"] = "2026-11-20T00:03:00.000000Z"
        s["convergence"]["verified_snapshot_id"] = 200
        (rdir / "summary.json").write_text(json.dumps(s, indent=2), encoding="utf-8")
        lk = _make_lk_meta(
            [
                _snap(1, 199, None, "2026-11-20T00:01:00.000000Z", "append", tot_data=26, tot_del=1),
                _snap(2, 200, 199, "2026-11-20T00:03:05.000000Z", "append", tot_data=27, tot_del=2),
            ]
        )
        (rdir / "lakekeeper_table_final.json").write_text(json.dumps(lk, indent=2), encoding="utf-8")
        (rdir / "pg2iceberg.log").write_text("", encoding="utf-8")

    manifest = {
        "milestone": "C1",
        "label": "c1-fault",
        "pairs": [
            {
                "pair_index": 1,
                "order": "baseline-first",
                "rng_seed": 20261119,
                "baseline_run_dir": "runs/p1-baseline",
                "repack_run_dir": "runs/p1-repack",
            }
        ],
    }
    agg, _ = aggregate_c1_and_compare_m3(manifest, m3_manifest=None, repo_root=tmp_path)
    assert agg["compaction_summary"]["all_runs_had_compaction_in_window"] is False
    assert agg["convergence_summary"]["all_valid_runs"] is False
    assert agg["pairs"][0]["baseline"]["outcome"] == "invalid harness run"
    assert agg["pairs"][0]["repack"]["outcome"] == "invalid harness run"


def test_log_audit_and_metrics_parser() -> None:
    clean_log = (
        f'pg2iceberg-1  | {{"timestamp":"2026-11-20T00:00:01Z","level":"WARN","message":"{EXPECTED_STARTUP_WARN_SUBSTRING}"}}\n'
        'pg2iceberg-1  | {"timestamp":"2026-11-20T00:00:05Z","level":"INFO","message":"snapshot phase complete"}\n'
    )
    res_clean = analyze_pg2iceberg_log(clean_log)
    assert res_clean["clean"] is True
    assert res_clean["expected_startup_warning_count"] == 1
    assert res_clean["error_count"] == 0
    assert res_clean["conflict_count"] == 0
    assert res_clean["retry_count"] == 0

    noisy_log = (
        'pg2iceberg-1  | {"timestamp":"2026-11-20T00:01:00Z","level":"WARN","message":"commit conflict; retrying next interval"}\n'
        'pg2iceberg-1  | {"timestamp":"2026-11-20T00:01:10Z","level":"ERROR","message":"compaction task failed"}\n'
    )
    res_noisy = analyze_pg2iceberg_log(noisy_log)
    assert res_noisy["clean"] is False
    assert res_noisy["error_count"] == 1
    assert res_noisy["unexpected_warning_count"] == 1
    assert res_noisy["conflict_count"] == 1
    assert res_noisy["retry_count"] == 1

    metrics_txt = """
# HELP pg2iceberg_compaction_runs_total Compactions
pg2iceberg_compaction_runs_total{table="public.orders",outcome="rewritten"} 9
pg2iceberg_compaction_files_rewritten_total{table="public.orders"} 42
pg2iceberg_compaction_duration_seconds_sum{table="public.orders"} 12.5
pg2iceberg_compaction_duration_seconds_count{table="public.orders"} 9
"""
    pm = parse_pg2iceberg_compaction_metrics(metrics_txt)
    assert pm["compaction_runs_total"][0]["value"] == 9.0
    assert pm["compaction_files_rewritten_total"][0]["value"] == 42.0
    assert pm["compaction_duration_seconds_count"] == 9.0
    assert pm["compaction_duration_seconds_sum"] == 12.5


def test_extract_pipeline_positions_compact_reads_default_mat_cursor_and_log_seq() -> None:
    from lab.c1 import _extract_pipeline_positions_compact

    summary = {
        "pipeline_positions": {
            "final": {
                "current_wal_lsn": "0/19EC1E58",
                "replication_slots": [
                    {
                        "slot_name": "pg2iceberg_slot",
                        "confirmed_flush_lsn": "0/19EC17E0",
                        "restart_lsn": "0/19E21BC0",
                        "wal_gap_bytes_confirmed_flush": 1656,
                        "wal_gap_bytes_restart": 656024,
                    }
                ],
                "coordinator_state": {
                    "mat_cursor": [
                        {"group_name": "default#snapshot-lsn", "last_offset": 426316240},
                        {"group_name": "default", "last_offset": 1811},
                    ],
                    "log_seq": [{"table_name": "public.orders", "next_offset": 1811}],
                },
            }
        }
    }
    compact = _extract_pipeline_positions_compact(summary)
    assert compact["final"]["mat_cursor"] == 1811
    assert compact["final"]["log_seq"] == 1811
    assert compact["final"]["wal_gap_bytes_confirmed_flush"] == 1656


def test_render_c1_from_manifest_with_m3_comparison_is_byte_reproducible(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    c1_pairs = []
    m3_pairs = []
    for idx, (order, seed) in enumerate(
        [("baseline-first", 20261119), ("repack-first", 20261120), ("baseline-first", 20261121)],
        start=1,
    ):
        for track, diff_ms, tot_d, tot_del, with_comp in (
            ("m3", 22000.0, 43, 17, False),
            ("c1", 9500.0, 5, 2, True),
        ):
            for kind in ("baseline", "repack"):
                rdir = runs / f"{track}-p{idx}-{kind}"
                _write_synthetic_run(
                    rdir,
                    run_kind=kind,
                    rng_seed=seed,
                    exec_ms=14.0,
                    int_exec_ms=19.0 if kind == "baseline" else 31.0,
                    probe_p50_ms=10500.0 if track == "m3" else 9800.0,
                    final_vis_ms=10000.0 if track == "m3" else 8500.0,
                    diff_ms=diff_ms,
                )
                s = json.loads((rdir / "summary.json").read_text(encoding="utf-8"))
                s["workload"]["window_start_utc"] = "2026-11-20T00:00:00.000000Z"
                s["workload"]["window_end_utc"] = "2026-11-20T00:03:00.000000Z"
                s["final_marker_commit_utc"] = "2026-11-20T00:03:00.100000Z"
                s["convergence"]["verified_snapshot_id"] = 500 + idx
                (rdir / "summary.json").write_text(json.dumps(s, indent=2), encoding="utf-8")

                snaps = [
                    _snap(1, 500, None, "2026-11-19T23:59:50.000000Z", "append", tot_data=25, tot_del=0),
                ]
                if with_comp:
                    snaps.append(
                        _snap(
                            2,
                            500 + idx - 10,
                            500,
                            "2026-11-20T00:01:00.000000Z",
                            "replace",
                            tot_data=3,
                            tot_del=0,
                            add_data=3,
                            del_data=8,
                            rem_del_files=6,
                            rem_eq_dels=500,
                            pg2_op="compaction",
                        )
                    )
                snaps.append(
                    _snap(
                        3,
                        500 + idx,
                        snaps[-1]["snapshot-id"],
                        "2026-11-20T00:03:08.000000Z",
                        "append",
                        tot_data=tot_d,
                        tot_del=tot_del,
                        tot_eq=150 if with_comp else 1500,
                    )
                )
                (rdir / "lakekeeper_table_final.json").write_text(
                    json.dumps(_make_lk_meta(snaps), indent=2), encoding="utf-8"
                )
                (rdir / "pg2iceberg.log").write_text("", encoding="utf-8")

            entry = {
                "pair_index": idx,
                "order": order,
                "rng_seed": seed,
                "baseline_run_dir": f"runs/{track}-p{idx}-baseline",
                "repack_run_dir": f"runs/{track}-p{idx}-repack",
            }
            if track == "c1":
                c1_pairs.append(entry)
            else:
                m3_pairs.append(entry)

    c1_mpath = tmp_path / "c1_manifest.json"
    m3_mpath = tmp_path / "m3_manifest.json"
    c1_mpath.write_text(json.dumps({"milestone": "C1", "label": "c1", "pairs": c1_pairs}), encoding="utf-8")
    m3_mpath.write_text(json.dumps({"milestone": "M3", "label": "m3", "pairs": m3_pairs}), encoding="utf-8")

    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    agg1 = render_c1_from_manifest(c1_mpath, output_dir=out1, m3_manifest_path=m3_mpath, repo_root=tmp_path)
    agg2 = render_c1_from_manifest(c1_mpath, output_dir=out2, m3_manifest_path=m3_mpath, repo_root=tmp_path)

    assert agg1 == agg2
    assert agg1["convergence_summary"]["all_valid_runs"] is True
    assert agg1["compaction_summary"]["all_runs_had_compaction_in_window"] is True
    assert agg1["m3_comparison"]["all_seeds_matched"] is True
    assert agg1["m3_comparison"]["aggregate_c1_vs_m3"]["all_6_runs"]["diff_duration_ms"]["delta_c1_minus_m3"]["median"] == -12500.0

    for fname in (
        "c1_summary.json",
        "c1_results.md",
        "c1_vs_m3_comparison.svg",
        "latency_comparison.svg",
        "repack_window_timeseries.svg",
        "reader_lag.svg",
    ):
        assert (out1 / fname).read_bytes() == (out2 / fname).read_bytes()
        assert len((out1 / fname).read_bytes()) > 200
