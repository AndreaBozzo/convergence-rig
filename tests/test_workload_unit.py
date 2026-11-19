"""Unit tests for M1 workload helpers, latency stats, and metadata summarization."""

from __future__ import annotations

from datetime import datetime, timezone
import random

from lab.m1 import summarize_duckdb_metadata
from lab.pipeline import extract_iceberg_snapshot_info
from lab.workload import (
    _make_incompressible_blob,
    build_edge_case_rows,
    percentile,
    summarize_latency,
)


def test_percentile_and_summarize_latency() -> None:
    assert percentile([], 50.0) is None
    assert percentile([4.2], 99.0) == 4.2
    vals = [10.0, 20.0, 30.0, 40.0, 50.0]
    stats = summarize_latency(vals)
    assert stats["count"] == 5
    assert stats["min_ms"] == 10.0
    assert stats["p50_ms"] == 30.0
    assert stats["max_ms"] == 50.0
    assert stats["mean_ms"] == 30.0

    empty = summarize_latency([])
    assert empty["count"] == 0
    assert empty["p50_ms"] is None


def test_incompressible_blob_is_deterministic_and_above_toast_threshold() -> None:
    b1 = _make_incompressible_blob(random.Random(42))
    b2 = _make_incompressible_blob(random.Random(42))
    b3 = _make_incompressible_blob(random.Random(43))
    assert b1 == b2
    assert b1 != b3
    assert len(b1) == 6144


def test_build_edge_case_rows_covers_required_sql_and_json_cases() -> None:
    rows = build_edge_case_rows(datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc))
    assert len(rows) == 12
    ids = [r[0] for r in rows]
    assert ids == list(range(1, 13))
    # Row 2 has SQL NULL note and payload
    assert rows[1][4] is None and rows[1][5] is None
    # Row 3 has empty text note and JSON null payload
    assert rows[2][4] == "" and rows[2][5] == "null"
    # Row 4 has multibyte UTF-8 note and JSON empty string payload
    assert "日本語" in rows[3][4] and rows[3][5] == '""'


def test_summarize_duckdb_metadata_and_extract_snapshot_info() -> None:
    meta_rows = [
        {"status": "ADDED", "content": "DATA", "record_count": 100},
        {"status": "EXISTING", "content": "DATA", "record_count": 200},
        {"status": "ADDED", "content": "EQUALITY_DELETES", "record_count": 15},
        {"status": "DELETED", "content": "EQUALITY_DELETES", "record_count": 99},
    ]
    s = summarize_duckdb_metadata(meta_rows)
    assert s["live_data_files"] == 2
    assert s["live_data_records"] == 300
    assert s["live_equality_delete_files"] == 1
    assert s["live_equality_delete_records"] == 15
    assert s["live_position_delete_files"] == 0

    lk_meta = {
        "metadata": {
            "current-snapshot-id": 555,
            "snapshots": [
                {
                    "snapshot-id": 555,
                    "sequence-number": 4,
                    "schema-id": 0,
                    "summary": {
                        "pg2iceberg.log-start": "10",
                        "pg2iceberg.log-end": "25",
                        "pg2iceberg.log-ends": '{"default":25}',
                        "pg2iceberg.log-group": "default",
                    },
                }
            ],
        }
    }
    info = extract_iceberg_snapshot_info(lk_meta)
    assert info is not None
    assert info["snapshot_id"] == 555
    assert info["pg2iceberg_log_start"] == "10"
    assert info["pg2iceberg_log_end"] == "25"
