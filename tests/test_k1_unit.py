"""Unit and fault-injection tests for K1 orphan-file classification and cleanup evaluation."""

from __future__ import annotations

from lab.k1 import (
    classify_table_objects,
    collect_referenced_s3_uris,
    evaluate_cleanup_diff,
    evaluate_concurrent_cleanup,
    normalize_s3_uri,
)

TABLE_LOC = "s3://cdclab/warehouse/01a1f5af-4056-73c2-bab7-7cd652917b72"
PREFIX = "warehouse/01a1f5af-4056-73c2-bab7-7cd652917b72"


def _sample_metadata_and_objects():
    lk_meta = {
        "metadata_location": f"{TABLE_LOC}/metadata/00002-cur.gz.metadata.json",
        "metadata": {
            "location": TABLE_LOC,
            "metadata-log": [
                {"metadata-file": f"{TABLE_LOC}/metadata/00001-prev.gz.metadata.json", "timestamp-ms": 1}
            ],
            "snapshots": [
                {"snapshot-id": 100, "manifest-list": f"{TABLE_LOC}/metadata/snap-100.avro"},
                {"snapshot-id": 200, "manifest-list": f"{TABLE_LOC}/metadata/snap-200.avro"},
            ],
        },
    }
    spark_inspect = {
        "metadata_log_entries": [
            {"file": f"{TABLE_LOC}/metadata/00001-prev.gz.metadata.json"},
            {"file": f"{TABLE_LOC}/metadata/00002-cur.gz.metadata.json"},
        ],
        "snapshots": [
            {"snapshot_id": 100, "manifest_list": f"{TABLE_LOC}/metadata/snap-100.avro"},
            {"snapshot_id": 200, "manifest_list": f"{TABLE_LOC}/metadata/snap-200.avro"},
        ],
        "all_manifests": [
            {"path": f"{TABLE_LOC}/metadata/m-100.avro"},
            {"path": f"{TABLE_LOC}/metadata/m-200-del.avro"},
        ],
        "all_data_files": [
            {"file_path": f"{TABLE_LOC}/data/data-live-1.parquet"},
        ],
        "all_delete_files": [
            {"file_path": f"{TABLE_LOC}/data/eq-delete-live-1.parquet"},
        ],
        "current_files": [
            {"content": 0, "file_path": f"{TABLE_LOC}/data/data-live-1.parquet"},
            {"content": 2, "file_path": f"{TABLE_LOC}/data/eq-delete-live-1.parquet"},
        ],
    }
    silo_objects = [
        {"key": f"{PREFIX}/metadata/00001-prev.gz.metadata.json", "s3_uri": f"{TABLE_LOC}/metadata/00001-prev.gz.metadata.json", "size": 1000, "last_modified": "2026-11-20T10:00:00Z"},
        {"key": f"{PREFIX}/metadata/00002-cur.gz.metadata.json", "s3_uri": f"{TABLE_LOC}/metadata/00002-cur.gz.metadata.json", "size": 1200, "last_modified": "2026-11-20T10:01:00Z"},
        {"key": f"{PREFIX}/metadata/snap-100.avro", "s3_uri": f"{TABLE_LOC}/metadata/snap-100.avro", "size": 500, "last_modified": "2026-11-20T10:00:00Z"},
        {"key": f"{PREFIX}/metadata/snap-200.avro", "s3_uri": f"{TABLE_LOC}/metadata/snap-200.avro", "size": 500, "last_modified": "2026-11-20T10:01:00Z"},
        {"key": f"{PREFIX}/metadata/m-100.avro", "s3_uri": f"{TABLE_LOC}/metadata/m-100.avro", "size": 600, "last_modified": "2026-11-20T10:00:00Z"},
        {"key": f"{PREFIX}/metadata/m-200-del.avro", "s3_uri": f"{TABLE_LOC}/metadata/m-200-del.avro", "size": 600, "last_modified": "2026-11-20T10:01:00Z"},
        {"key": f"{PREFIX}/data/data-live-1.parquet", "s3_uri": f"{TABLE_LOC}/data/data-live-1.parquet", "size": 50000, "last_modified": "2026-11-20T10:00:00Z"},
        {"key": f"{PREFIX}/data/eq-delete-live-1.parquet", "s3_uri": f"{TABLE_LOC}/data/eq-delete-live-1.parquet", "size": 2000, "last_modified": "2026-11-20T10:01:00Z"},
        # 2 unreferenced outage orphans:
        {"key": f"{PREFIX}/data/data-orphan-outage.parquet", "s3_uri": f"{TABLE_LOC}/data/data-orphan-outage.parquet", "size": 300000, "last_modified": "2026-11-20T10:00:30Z"},
        {"key": f"{PREFIX}/data/eq-delete-orphan-outage.parquet", "s3_uri": f"{TABLE_LOC}/data/eq-delete-orphan-outage.parquet", "size": 58000, "last_modified": "2026-11-20T10:00:30Z"},
        # pg2iceberg staging file under table prefix:
        {"key": f"{PREFIX}/staged/batch-0001.parquet", "s3_uri": f"{TABLE_LOC}/staged/batch-0001.parquet", "size": 70000, "last_modified": "2026-11-20T10:00:20Z"},
    ]
    staging_uris = [f"{TABLE_LOC}/staged/batch-0001.parquet"]
    staging_keys = [f"{PREFIX}/staged/batch-0001.parquet"]
    return lk_meta, spark_inspect, silo_objects, staging_uris, staging_keys


def test_normalize_s3_uri_handles_s3a_and_redundant_slashes() -> None:
    assert normalize_s3_uri("s3a://cdclab//warehouse/t//data/f.parquet/") == (
        "s3://cdclab/warehouse/t/data/f.parquet"
    )


def test_classify_table_objects_and_clean_orphan_removal_pass() -> None:
    lk_meta, spark_inspect, silo_before, staging_uris, staging_keys = _sample_metadata_and_objects()
    referenced = collect_referenced_s3_uris(lk_meta, spark_inspect)

    before = classify_table_objects(
        silo_before, TABLE_LOC, referenced, staging_uris=staging_uris, staging_keys=staging_keys
    )
    assert before["counts_by_classification"] == {
        "referenced": 8,
        "staging": 1,
        "unreferenced": 2,
    }
    assert before["referenced_uris_missing_in_storage"] == []
    assert before["staging_objects_under_table"][0]["matched_log_index"] is True

    # Clean cleanup removes ONLY the 2 unreferenced files.
    silo_after = [o for o in silo_before if "orphan" not in o["key"]]
    after = classify_table_objects(
        silo_after, TABLE_LOC, referenced, staging_uris=staging_uris, staging_keys=staging_keys
    )
    diff = evaluate_cleanup_diff(before, after)
    assert diff["exact_orphan_removal_passed"] is True
    assert diff["removed_unreferenced_count"] == 2
    assert diff["removed_referenced_count"] == 0
    assert diff["removed_staging_count"] == 0
    assert diff["remaining_unreferenced_count"] == 0
    assert diff["reclaimed_bytes"] == 358000


def test_evaluate_cleanup_diff_fails_on_injected_faults() -> None:
    lk_meta, spark_inspect, silo_before, staging_uris, staging_keys = _sample_metadata_and_objects()
    referenced = collect_referenced_s3_uris(lk_meta, spark_inspect)
    before = classify_table_objects(
        silo_before, TABLE_LOC, referenced, staging_uris=staging_uris, staging_keys=staging_keys
    )

    # Fault 1: one orphan file left behind.
    silo_partial = [o for o in silo_before if "data-orphan-outage" not in o["key"]]
    after_partial = classify_table_objects(
        silo_partial, TABLE_LOC, referenced, staging_uris=staging_uris, staging_keys=staging_keys
    )
    diff_partial = evaluate_cleanup_diff(before, after_partial)
    assert diff_partial["exact_orphan_removal_passed"] is False
    assert diff_partial["remaining_unreferenced_count"] == 1

    # Fault 2: a live referenced data file was accidentally removed.
    silo_lost_live = [
        o for o in silo_before if "orphan" not in o["key"] and "data-live-1" not in o["key"]
    ]
    after_lost_live = classify_table_objects(
        silo_lost_live, TABLE_LOC, referenced, staging_uris=staging_uris, staging_keys=staging_keys
    )
    diff_lost_live = evaluate_cleanup_diff(before, after_lost_live)
    assert diff_lost_live["exact_orphan_removal_passed"] is False
    assert diff_lost_live["removed_referenced_count"] == 1
    assert f"{TABLE_LOC}/data/data-live-1.parquet" in diff_lost_live["removed_referenced_uris"]

    # Fault 3: pg2iceberg staging file was accidentally removed.
    silo_lost_stg = [
        o for o in silo_before if "orphan" not in o["key"] and "/staged/" not in o["key"]
    ]
    after_lost_stg = classify_table_objects(
        silo_lost_stg, TABLE_LOC, referenced, staging_uris=staging_uris, staging_keys=staging_keys
    )
    diff_lost_stg = evaluate_cleanup_diff(before, after_lost_stg)
    assert diff_lost_stg["exact_orphan_removal_passed"] is False
    assert diff_lost_stg["removed_staging_count"] == 1


def test_evaluate_concurrent_cleanup_passes_clean_and_fails_on_live_file_loss() -> None:
    lk_meta, spark_inspect, silo_before, staging_uris, staging_keys = _sample_metadata_and_objects()
    referenced = collect_referenced_s3_uris(lk_meta, spark_inspect)
    silo_clean = [o for o in silo_before if "orphan" not in o["key"]]
    before = classify_table_objects(
        silo_clean, TABLE_LOC, referenced, staging_uris=staging_uris, staging_keys=staging_keys
    )

    # Clean concurrent run: new data file added, zero files removed, active writes + new snapshot.
    new_data_uri = f"{TABLE_LOC}/data/data-live-2.parquet"
    ref_after = dict(referenced)
    ref_after[new_data_uri] = "data_file"
    silo_after_ok = [
        *silo_clean,
        {"key": f"{PREFIX}/data/data-live-2.parquet", "s3_uri": new_data_uri, "size": 12000, "last_modified": "2026-11-20T10:02:00Z"},
    ]
    after_ok = classify_table_objects(
        silo_after_ok, TABLE_LOC, ref_after, staging_uris=staging_uris, staging_keys=staging_keys
    )
    ok_res = evaluate_concurrent_cleanup(
        before_classified=before,
        after_classified=after_ok,
        spark_deleted_uris=[],
        writes_during_cleanup=150,
        snapshots_before=[100, 200],
        snapshots_after=[100, 200, 300],
    )
    assert ok_res["passed"] is True
    assert ok_res["removed_live_file_count"] == 0
    assert ok_res["new_snapshots_count"] == 1

    # Fault: live file removed during concurrent run must fail loudly.
    silo_after_bad = [o for o in silo_after_ok if "eq-delete-live-1" not in o["key"]]
    after_bad = classify_table_objects(
        silo_after_bad, TABLE_LOC, ref_after, staging_uris=staging_uris, staging_keys=staging_keys
    )
    bad_res = evaluate_concurrent_cleanup(
        before_classified=before,
        after_classified=after_bad,
        spark_deleted_uris=[f"{TABLE_LOC}/data/eq-delete-live-1.parquet"],
        writes_during_cleanup=150,
        snapshots_before=[100, 200],
        snapshots_after=[100, 200, 300],
    )
    assert bad_res["passed"] is False
    assert bad_res["removed_live_file_count"] == 1
    assert bad_res["spark_deleted_count"] == 1

    # Fault: zero writes during cleanup must fail.
    no_writes = evaluate_concurrent_cleanup(
        before_classified=before,
        after_classified=after_ok,
        spark_deleted_uris=[],
        writes_during_cleanup=0,
        snapshots_before=[100, 200],
        snapshots_after=[100, 200, 300],
    )
    assert no_writes["passed"] is False
