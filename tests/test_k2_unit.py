"""Unit and fault-injection tests for K2 catalog metadata restore drill analysis."""

from __future__ import annotations

from lab.k2 import (
    analyze_restore_divergence,
    attribute_divergence_to_workload,
    diff_silo_object_sets,
    extract_snapshot_lineage,
    find_log_index_cutoff_utc,
    summarize_silo_objects_by_role,
)

TABLE_LOC = "s3://cdclab/warehouse/01a1f900-0000-7000-8000-000000000001"
PREFIX = "warehouse/01a1f900-0000-7000-8000-000000000001"


def _mk_snap(
    snap_id: int,
    parent_id: int | None,
    seq: int,
    ts_ms: int,
    log_start: int | None,
    log_end: int | None,
) -> dict:
    summary = {"operation": "append", "total-records": "25000"}
    if log_start is not None:
        summary["pg2iceberg.log-start"] = str(log_start)
    if log_end is not None:
        summary["pg2iceberg.log-end"] = str(log_end)
    snap = {
        "snapshot-id": snap_id,
        "sequence-number": seq,
        "timestamp-ms": ts_ms,
        "schema-id": 0,
        "manifest-list": f"{TABLE_LOC}/metadata/snap-{snap_id}.avro",
        "summary": summary,
    }
    if parent_id is not None:
        snap["parent-snapshot-id"] = parent_id
    return snap


def _mk_lk_meta(cur_id: int, meta_file: str, snaps: list[dict]) -> dict:
    return {
        "metadata_location": f"{TABLE_LOC}/metadata/{meta_file}",
        "metadata": {
            "table-uuid": "01a1f900-0000-7000-8000-000000000001",
            "location": TABLE_LOC,
            "current-snapshot-id": cur_id,
            "last-sequence-number": max(s["sequence-number"] for s in snaps),
            "snapshots": snaps,
            "metadata-log": [],
        },
    }


def test_analyze_restore_divergence_detects_rollback_fork_and_skipped_offsets() -> None:
    s1 = _mk_snap(100, None, 1, 1_795_180_000_000, None, None)
    s2 = _mk_snap(200, 100, 2, 1_795_180_035_000, 0, 350)
    # 3 post-dump snapshots committed before restore:
    s3 = _mk_snap(300, 200, 3, 1_795_180_045_000, 350, 450)
    s4 = _mk_snap(400, 300, 4, 1_795_180_055_000, 450, 550)
    s5 = _mk_snap(500, 400, 5, 1_795_180_065_000, 550, 650)
    # Post-restore forked snapshot branching off s2 with skipped offsets (350..650]:
    s6_fork = _mk_snap(600, 200, 3, 1_795_180_080_000, 650, 750)

    lin_dump = extract_snapshot_lineage(_mk_lk_meta(200, "00002-dump.gz.metadata.json", [s1, s2]))
    lin_before = extract_snapshot_lineage(
        _mk_lk_meta(500, "00005-before.gz.metadata.json", [s1, s2, s3, s4, s5])
    )
    lin_after = extract_snapshot_lineage(_mk_lk_meta(200, "00002-dump.gz.metadata.json", [s1, s2]))
    lin_post = extract_snapshot_lineage(
        _mk_lk_meta(600, "00003-fork.gz.metadata.json", [s1, s2, s6_fork])
    )

    silo_before = [
        {"key": f"{PREFIX}/metadata/00002-dump.gz.metadata.json", "s3_uri": f"{TABLE_LOC}/metadata/00002-dump.gz.metadata.json", "size": 1000},
        {"key": f"{PREFIX}/metadata/00005-before.gz.metadata.json", "s3_uri": f"{TABLE_LOC}/metadata/00005-before.gz.metadata.json", "size": 1200},
        {"key": f"{PREFIX}/metadata/snap-300.avro", "s3_uri": f"{TABLE_LOC}/metadata/snap-300.avro", "size": 500},
        {"key": f"{PREFIX}/metadata/snap-400.avro", "s3_uri": f"{TABLE_LOC}/metadata/snap-400.avro", "size": 500},
        {"key": f"{PREFIX}/metadata/snap-500.avro", "s3_uri": f"{TABLE_LOC}/metadata/snap-500.avro", "size": 500},
        {"key": f"{PREFIX}/staged/batch-1.parquet", "s3_uri": f"{TABLE_LOC}/staged/batch-1.parquet", "size": 2000},
    ]
    silo_after = list(silo_before)
    silo_post = [
        *silo_after,
        {"key": f"{PREFIX}/metadata/00003-fork.gz.metadata.json", "s3_uri": f"{TABLE_LOC}/metadata/00003-fork.gz.metadata.json", "size": 1100},
        {"key": f"{PREFIX}/metadata/snap-600.avro", "s3_uri": f"{TABLE_LOC}/metadata/snap-600.avro", "size": 500},
    ]

    div = analyze_restore_divergence(
        lineage_at_dump=lin_dump,
        lineage_before_restore=lin_before,
        lineage_after_restore=lin_after,
        lineage_post_drill=lin_post,
        silo_before_restore=silo_before,
        silo_after_restore=silo_after,
        silo_post_drill=silo_post,
        pg_offsets_at_dump={"applied_mat_cursor": 350},
        pg_offsets_before_restore={"applied_mat_cursor": 650},
        pg_offsets_after_restore={"applied_mat_cursor": 650},
        pg_offsets_post_drill={"applied_mat_cursor": 750},
        pg2_log_summary={"error_count": 0, "fatal_text_error_count": 0},
        pg2_container_after_restore={"running": True},
        pg2_container_post_drill={"running": True},
    )

    assert div["restored_to_dump_snapshot"] is True
    assert div["restore_rolled_back_catalog"] is True
    assert div["rolled_back_snapshot_count"] == 3
    assert div["post_restore_new_snapshot_count"] == 1
    assert div["fork_detected"] is True
    assert div["catalog_behind_coordinator_offsets"] == 300
    assert div["skipped_log_offset_range"] == {
        "from_offset_exclusive": 350,
        "to_offset_inclusive": 650,
        "skipped_offset_count": 300,
    }
    assert div["before_restore_metadata_location_still_in_silo"] is True
    assert div["rolled_back_manifest_lists_still_in_silo_count"] == 3
    assert div["connector_behavior"] == "silent_continuation_with_gap"

    # Fault check: if restore did NOT roll back the catalog, restore_rolled_back_catalog is False.
    no_rb = analyze_restore_divergence(
        lineage_at_dump=lin_dump,
        lineage_before_restore=lin_before,
        lineage_after_restore=lin_before,
        lineage_post_drill=lin_before,
        silo_before_restore=silo_before,
        silo_after_restore=silo_before,
        silo_post_drill=silo_before,
        pg_offsets_at_dump={"applied_mat_cursor": 350},
        pg_offsets_before_restore={"applied_mat_cursor": 650},
        pg_offsets_after_restore={"applied_mat_cursor": 650},
        pg_offsets_post_drill={"applied_mat_cursor": 650},
        pg2_log_summary={"error_count": 0, "fatal_text_error_count": 0},
        pg2_container_after_restore={"running": True},
        pg2_container_post_drill={"running": True},
    )
    assert no_rb["restored_to_dump_snapshot"] is False
    assert no_rb["restore_rolled_back_catalog"] is False
    assert no_rb["rolled_back_snapshot_count"] == 0


def test_attribute_divergence_to_workload_maps_lost_ghost_and_stale_rows() -> None:
    # Window starts at 12:00:00Z; log_index cutoffs at 12:00:34Z (offset 388) and 12:01:04Z (offset 689).
    window_start_utc = "2026-11-20T12:00:00.000000Z"
    dump_ms = 1_795_176_035_000
    before_ms = 1_795_176_065_000
    after_utc = "2026-11-20T12:01:10.000000Z"

    log_rows = [
        {"start_offset": 360, "end_offset": 388, "created_at": "2026-11-20T12:00:34.000000+00:00"},
        {"start_offset": 660, "end_offset": 689, "created_at": "2026-11-20T12:01:04.000000+00:00"},
    ]
    rb_start = find_log_index_cutoff_utc(log_rows, 388)
    rb_end = find_log_index_cutoff_utc(log_rows, 689)
    assert rb_start == "2026-11-20T12:00:34.000000+00:00"
    assert rb_end == "2026-11-20T12:01:04.000000+00:00"

    # Match real lab/workload.py ops.jsonl schema: "op" and "complete_offset_ms".
    ops = [
        {"seq": 345, "op": "insert", "target_id": 1000000010, "outcome": "ok", "complete_offset_ms": 34500.0},
        {"seq": 500, "op": "delete", "target_id": 500, "outcome": "ok", "complete_offset_ms": 50000.0},
        {"seq": 550, "op": "update_toast_changed", "target_id": 25, "outcome": "ok", "complete_offset_ms": 55000.0},
        {"seq": 560, "op": "update_normal", "target_id": 777, "outcome": "ok", "complete_offset_ms": 56000.0},
        {"seq": 800, "op": "update_normal", "target_id": 777, "outcome": "ok", "complete_offset_ms": 80000.0},
        {"seq": 810, "op": "insert", "target_id": 1000000099, "outcome": "ok", "complete_offset_ms": 81000.0},
    ]
    probes = [
        {"probe_seq": 3, "marker_id": 900000003, "commit_utc": "2026-11-20T12:00:50.500000Z"},
    ]
    full_diff = {
        "source_only_examples": [{"id": 900000003}, {"id": 1000000010}],
        "lake_only_examples": [{"id": 500}],
        "value_mismatch_examples": [{"id": 25}],
        "duplicate_lake_examples": [],
    }
    attr = attribute_divergence_to_workload(
        full_diff=full_diff,
        ops=ops,
        probes=probes,
        window_start_utc=window_start_utc,
        dump_snapshot_timestamp_ms=dump_ms,
        before_restore_snapshot_timestamp_ms=before_ms,
        after_restore_utc=after_utc,
        rollback_start_utc_override=rb_start,
        rollback_end_utc_override=rb_end,
    )
    assert attr["total_discrepant_ids"] == 4
    assert attr["rolled_back_distinct_ids_touched"] == 5
    assert attr["rolled_back_ids_masked_by_later_op_count"] == 1
    assert attr["source_only_missing_from_iceberg"]["count"] == 2
    assert attr["source_only_missing_from_iceberg"]["by_primary_window"] == {"rolled_back_window": 2}
    assert attr["source_only_missing_from_iceberg"]["by_last_op_kind"] == {
        "insert": 1,
        "insert_probe": 1,
    }
    assert attr["lake_only_ghost_rows_in_iceberg"]["by_last_op_kind"] == {"delete": 1}
    assert attr["value_mismatch_stale_rows_in_iceberg"]["by_last_op_kind"] == {
        "update_toast_changed": 1
    }


def test_silo_role_summary_and_diff() -> None:
    objs_before = [
        {"key": f"{PREFIX}/metadata/00001.gz.metadata.json", "s3_uri": f"{TABLE_LOC}/metadata/00001.gz.metadata.json", "size": 500},
        {"key": f"{PREFIX}/data/data-1.parquet", "s3_uri": f"{TABLE_LOC}/data/data-1.parquet", "size": 10000},
        {"key": f"{PREFIX}/data/eq-delete-1.parquet", "s3_uri": f"{TABLE_LOC}/data/eq-delete-1.parquet", "size": 700},
        {"key": f"{PREFIX}/staged/s1.parquet", "s3_uri": f"{TABLE_LOC}/staged/s1.parquet", "size": 3000},
    ]
    summary = summarize_silo_objects_by_role(objs_before, TABLE_LOC)
    assert summary["counts_by_role"] == {
        "metadata_json": 1,
        "data_file": 1,
        "delete_file": 1,
        "pg2iceberg_staging": 1,
    }
    objs_after = [
        *objs_before,
        {"key": f"{PREFIX}/metadata/00002.gz.metadata.json", "s3_uri": f"{TABLE_LOC}/metadata/00002.gz.metadata.json", "size": 600},
    ]
    diff = diff_silo_object_sets(objs_before, objs_after)
    assert diff["added_count"] == 1
    assert diff["removed_count"] == 0
    assert diff["preserved_count"] == 4
