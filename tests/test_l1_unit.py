"""Unit and fault-injection tests for L1 Spark NDJSON adapter and reader schema verification."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
from typing import Any

import pytest

from lab.compare import compare_ordered_streams
from lab.l1 import (
    latest_snapshot_id_from_duckdb,
    parse_spark_ndjson_record,
    stream_spark_orders_ndjson,
    verify_duckdb_schema,
    verify_spark_schema,
)


def _write_ndjson(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n",
        encoding="utf-8",
    )


def _sample_records() -> tuple[list[tuple[Any, ...]], list[dict[str, Any]]]:
    ts1 = datetime(2026, 11, 19, 22, 0, 0, 123456, tzinfo=timezone.utc)
    ts2 = datetime(2026, 11, 19, 22, 0, 1, 999999, tzinfo=timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    us1 = int((ts1 - epoch).total_seconds() * 1_000_000)
    us2 = int((ts2 - epoch).total_seconds() * 1_000_000)

    pg_rows = [
        (
            1,
            "updated",
            Decimal("12345678901234567890123456789.123456789"),
            ts1,
            "trailing  ",
            '{"a": [1, 2], "exact": 12345678901234567890123456789.123456789}',
            Decimal("0.000000001"),
        ),
        (
            3,
            "settled",
            Decimal("-99999999999999999999999999999.999999999"),
            ts2,
            None,
            "null",
            None,
        ),
    ]
    spark_records = [
        {
            "id": 1,
            "status": "updated",
            "amount": "12345678901234567890123456789.123456789",
            "updated_at_epoch_us": us1,
            "note": "trailing  ",
            "payload": '{"exact": 12345678901234567890123456789.123456789, "a": [1, 2]}',
            "discount": "0.000000001",
        },
        {
            "id": 3,
            "status": "settled",
            "amount": "-99999999999999999999999999999.999999999",
            "updated_at_epoch_us": us2,
            "note": None,
            "payload": "null",
            "discount": None,
        },
    ]
    return pg_rows, spark_records


def test_spark_ndjson_stream_passes_clean_and_fails_injected_faults(tmp_path: Path) -> None:
    pg_rows, spark_records = _sample_records()
    clean_path = tmp_path / "clean.jsonl"
    _write_ndjson(clean_path, spark_records)

    res_clean = compare_ordered_streams(
        [pg_rows[:1], pg_rows[1:]],
        stream_spark_orders_ndjson(clean_path, batch_size=1),
    )
    assert res_clean.passed is True
    assert res_clean.exact_match_row_count == 2
    assert res_clean.batches_read_lake == 2

    # Fault 1: last-digit 38-digit decimal diff + 1us timestamp diff
    bad_val = [dict(r) for r in spark_records]
    bad_val[0]["amount"] = "12345678901234567890123456789.123456788"
    bad_val[1]["updated_at_epoch_us"] = int(bad_val[1]["updated_at_epoch_us"]) - 1
    bad_val_path = tmp_path / "bad_val.jsonl"
    _write_ndjson(bad_val_path, bad_val)
    res_bad_val = compare_ordered_streams([pg_rows], stream_spark_orders_ndjson(bad_val_path, batch_size=1))
    assert res_bad_val.passed is False
    assert res_bad_val.value_mismatch_row_count == 2
    assert res_bad_val.value_mismatch_column_counts == {"amount": 1, "updated_at": 1}

    # Fault 2: duplicate row across batch boundary
    dup_recs = [spark_records[0], dict(spark_records[0]), spark_records[1]]
    dup_path = tmp_path / "dup.jsonl"
    _write_ndjson(dup_path, dup_recs)
    res_dup = compare_ordered_streams([pg_rows], stream_spark_orders_ndjson(dup_path, batch_size=1))
    assert res_dup.passed is False
    assert res_dup.duplicate_lake_id_count == 1
    assert res_dup.duplicate_lake_examples[0]["id"] == 1

    # Fault 3: float decimal in NDJSON must be rejected loudly
    with pytest.raises(TypeError, match="exact decimal string"):
        parse_spark_ndjson_record(json.dumps({**spark_records[0], "amount": 1.5}))


def test_verify_spark_and_duckdb_schema_pass_valid_and_fail_faults() -> None:
    valid_spark = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": False, "metadata": {}},
            {"name": "status", "type": "string", "nullable": False, "metadata": {}},
            {"name": "amount", "type": "decimal(38,9)", "nullable": False, "metadata": {}},
            {"name": "updated_at", "type": "timestamp", "nullable": False, "metadata": {}},
            {"name": "note", "type": "string", "nullable": True, "metadata": {}},
            {"name": "payload", "type": "string", "nullable": True, "metadata": {}},
            {"name": "discount", "type": "decimal(38,9)", "nullable": True, "metadata": {}},
        ],
    }
    assert verify_spark_schema(valid_spark)["passed"] is True

    # Inject Spark timestamp_ntz (wrong tz semantics) and decimal(38,8) and nullable mismatch
    bad_spark = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": False},
            {"name": "status", "type": "string", "nullable": True},
            {"name": "amount", "type": "decimal(38,8)", "nullable": False},
            {"name": "updated_at", "type": "timestamp_ntz", "nullable": False},
            {"name": "note", "type": "string", "nullable": True},
            {"name": "payload", "type": "string", "nullable": True},
            {"name": "discount", "type": "decimal(38,9)", "nullable": True},
        ],
    }
    res_bad_spark = verify_spark_schema(bad_spark)
    assert res_bad_spark["passed"] is False
    cols = {(m["column"], m["kind"]) for m in res_bad_spark["mismatches"]}
    assert cols == {
        ("status", "required_mismatch"),
        ("amount", "type_mismatch"),
        ("updated_at", "type_mismatch"),
    }

    valid_duckdb = [
        {"column_name": "id", "column_type": "BIGINT", "null": "YES"},
        {"column_name": "status", "column_type": "VARCHAR", "null": "YES"},
        {"column_name": "amount", "column_type": "DECIMAL(38,9)", "null": "YES"},
        {"column_name": "updated_at", "column_type": "TIMESTAMP WITH TIME ZONE", "null": "YES"},
        {"column_name": "note", "column_type": "VARCHAR", "null": "YES"},
        {"column_name": "payload", "column_type": "VARCHAR", "null": "YES"},
        {"column_name": "discount", "column_type": "DECIMAL(38,9)", "null": "YES"},
    ]
    assert verify_duckdb_schema(valid_duckdb)["passed"] is True

    bad_duckdb = [dict(r) for r in valid_duckdb]
    bad_duckdb[3]["column_type"] = "TIMESTAMP"
    res_bad_duck = verify_duckdb_schema(bad_duckdb)
    assert res_bad_duck["passed"] is False
    assert res_bad_duck["mismatches"][0]["column"] == "updated_at"


def test_latest_snapshot_id_from_duckdb_uses_highest_sequence_number() -> None:
    desc = [
        {"sequence_number": 2, "snapshot_id": 222},
        {"sequence_number": 1, "snapshot_id": 111},
    ]
    asc = list(reversed(desc))
    assert latest_snapshot_id_from_duckdb(desc) == 222
    assert latest_snapshot_id_from_duckdb(asc) == 222
    assert latest_snapshot_id_from_duckdb([]) is None
