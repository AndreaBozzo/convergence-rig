"""Unit tests for M0 configuration, row normalization, and equality-delete evidence checks."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from lab.config import load_config, parse_env_file
from lab.m0 import normalize_row, snapshot_has_equality_deletes


def test_parse_env_file_and_load_config(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "PG_SOURCE_PORT=25432",
                "LAKEKEEPER_PORT=28181",
                "SILO_PORT=29000",
                "LAKEKEEPER_WAREHOUSE='custom_wh'",
            ]
        ),
        encoding="utf-8",
    )
    parsed = parse_env_file(env_file)
    assert parsed["PG_SOURCE_PORT"] == "25432"
    assert parsed["LAKEKEEPER_WAREHOUSE"] == "custom_wh"

    cfg = load_config(env_file=env_file)
    assert cfg.pg_source_host == "127.0.0.1"
    assert cfg.pg_source_port == 25432
    assert cfg.lakekeeper_catalog_url == "http://127.0.0.1:28181/catalog"
    assert cfg.silo_advertised_endpoint == "http://silo.localhost:29000"
    assert cfg.lakekeeper_warehouse == "custom_wh"


def test_normalize_row_preserves_38_digit_decimal_and_utc_microseconds() -> None:
    cet = timezone(timedelta(hours=1))
    row = {
        "id": 1,
        "status": "updated",
        "amount": Decimal("12345678901234567890123456789.123456788"),
        "updated_at": datetime(2026, 11, 19, 19, 5, 0, 987654, tzinfo=cet),
        "note": None,
        "payload": '{"b": 2, "a": 1}',
        "discount": None,
    }
    norm = normalize_row(row)
    assert norm["amount"] == "12345678901234567890123456789.123456788"
    assert norm["updated_at"] == "2026-11-19T18:05:00.987654+00:00"
    assert norm["note"] is None
    assert norm["payload"] == {"a": 1, "b": 2}
    assert norm["discount"] is None


def test_snapshot_has_equality_deletes_detects_present_and_absent() -> None:
    lk_meta = {
        "metadata": {
            "snapshots": [
                {
                    "snapshot-id": 100,
                    "summary": {
                        "total-equality-deletes": "0",
                        "added-equality-deletes": "0",
                        "total-delete-files": "0",
                        "added-delete-files": "0",
                    },
                },
                {
                    "snapshot-id": 200,
                    "summary": {
                        "total-equality-deletes": "2",
                        "added-equality-deletes": "2",
                        "total-delete-files": "1",
                        "added-delete-files": "1",
                    },
                },
            ]
        }
    }
    duckdb_meta_no_del = [
        {"manifest_content": "DATA", "content": "DATA", "status": "ADDED", "file_path": "s3://b/d1.parquet"}
    ]
    duckdb_meta_with_del = [
        {"manifest_content": "DATA", "content": "DATA", "status": "EXISTING", "file_path": "s3://b/d1.parquet"},
        {
            "manifest_content": "DELETES",
            "content": "EQUALITY_DELETES",
            "status": "ADDED",
            "file_path": "s3://b/eq1.parquet",
            "record_count": 2,
        },
    ]

    assert not snapshot_has_equality_deletes(lk_meta, 100, duckdb_meta_no_del)["confirmed"]
    assert not snapshot_has_equality_deletes(lk_meta, 200, duckdb_meta_no_del)["confirmed"]
    res = snapshot_has_equality_deletes(lk_meta, 200, duckdb_meta_with_del)
    assert res["confirmed"]
    assert res["total_equality_deletes"] == 2
    assert len(res["duckdb_live_delete_entries"]) == 1
