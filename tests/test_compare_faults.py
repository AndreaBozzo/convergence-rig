"""Deterministic fault-injection tests for lab.compare (M1 Step 1)."""

from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from lab.compare import (
    compare_ordered_streams,
    format_decimal_fixed,
    verify_iceberg_schema,
)


def _base_rows() -> list[dict[str, Any]]:
    return [
        {
            "id": 1,
            "status": "created",
            "amount": Decimal("12345678901234567890123456789.123456789"),
            "updated_at": datetime(2026, 11, 19, 18, 0, 0, 123456, tzinfo=timezone.utc),
            "note": "alpha",
            "payload": '{"b": 2, "a": [1, 2, 3], "exact": 12345678901234567890123456789.123456789}',
            "discount": Decimal("0.000000001"),
        },
        {
            "id": 2,
            "status": "updated",
            "amount": Decimal("0.000000000"),
            "updated_at": datetime(2026, 11, 19, 18, 0, 1, 0, tzinfo=timezone.utc),
            "note": None,
            "payload": None,
            "discount": None,
        },
        {
            "id": 3,
            "status": "settled",
            "amount": Decimal("-99999999999999999999999999999.999999999"),
            "updated_at": datetime(2026, 11, 19, 18, 0, 2, 999999, tzinfo=timezone.utc),
            "note": "",
            "payload": "null",
            "discount": Decimal("100.000000000"),
        },
        {
            "id": 4,
            "status": "closed",
            "amount": Decimal("42.500000000"),
            "updated_at": datetime(2026, 11, 19, 18, 0, 3, 500000, tzinfo=timezone.utc),
            "note": "caffè ☕ 日本語",
            "payload": '{"flag": true, "nested": {"x": "", "y": null}}',
            "discount": Decimal("1.250000000"),
        },
    ]


def _chunk(rows: list[Any], size: int) -> list[list[Any]]:
    return [rows[i : i + size] for i in range(0, len(rows), size)]


def _valid_iceberg_metadata() -> dict[str, Any]:
    return {
        "metadata": {
            "format-version": 2,
            "current-schema-id": 0,
            "schemas": [
                {
                    "schema-id": 0,
                    "identifier-field-ids": [1],
                    "type": "struct",
                    "fields": [
                        {"id": 1, "name": "id", "required": True, "type": "long"},
                        {"id": 2, "name": "status", "required": True, "type": "string"},
                        {"id": 3, "name": "amount", "required": True, "type": "decimal(38, 9)"},
                        {"id": 4, "name": "updated_at", "required": True, "type": "timestamptz"},
                        {"id": 5, "name": "note", "required": False, "type": "string"},
                        {"id": 6, "name": "payload", "required": False, "type": "string"},
                        {"id": 7, "name": "discount", "required": False, "type": "decimal(38, 9)"},
                    ],
                }
            ],
        }
    }


def test_clean_control_fixture_passes_across_small_batches() -> None:
    src = _base_rows()
    lake = copy.deepcopy(src)
    # Different timezone offset representing exact same UTC instant must pass.
    lake[0]["updated_at"] = lake[0]["updated_at"].astimezone(timezone(timedelta(hours=2)))
    # Semantically identical JSON with different formatting and key order must pass.
    lake[0]["payload"] = (
        '{\n  "a": [1,2,3],\n  "exact": 12345678901234567890123456789.123456789,\n  "b": 2\n}'
    )

    res = compare_ordered_streams(_chunk(src, 2), _chunk(lake, 3))
    assert res.passed is True
    assert res.source_row_count == 4
    assert res.lake_row_count == 4
    assert res.matched_id_count == 4
    assert res.exact_match_row_count == 4
    assert res.source_only_count == 0
    assert res.lake_only_count == 0
    assert res.duplicate_lake_id_count == 0
    assert res.value_mismatch_row_count == 0
    assert res.batches_read_source == 2
    assert res.batches_read_lake == 2


def test_fault_missing_row_source_only() -> None:
    src = _base_rows()
    lake = [r for r in copy.deepcopy(src) if r["id"] != 3]

    res = compare_ordered_streams(_chunk(src, 2), _chunk(lake, 2))
    assert res.passed is False
    assert res.source_only_count == 1
    assert res.lake_only_count == 0
    assert res.source_only_examples[0]["id"] == 3
    assert res.exact_match_row_count == 3


def test_fault_extra_row_lake_only() -> None:
    src = _base_rows()
    lake = copy.deepcopy(src)
    extra = copy.deepcopy(src[0])
    extra["id"] = 99
    lake.append(extra)

    res = compare_ordered_streams(_chunk(src, 2), _chunk(lake, 2))
    assert res.passed is False
    assert res.lake_only_count == 1
    assert res.source_only_count == 0
    assert res.lake_only_examples[0]["id"] == 99


def test_fault_duplicate_lake_row_identical_values_across_batch_boundary() -> None:
    src = _base_rows()
    lake = copy.deepcopy(src)
    # Inject exact identical duplicate for id=2 so index 1 and 2 straddle batch size 2.
    lake.insert(2, copy.deepcopy(src[1]))

    res = compare_ordered_streams(_chunk(src, 2), _chunk(lake, 2))
    assert res.passed is False
    assert res.duplicate_lake_id_count == 1
    assert res.duplicate_lake_extra_row_count == 1
    assert res.duplicate_lake_examples[0]["id"] == 2
    assert res.duplicate_lake_examples[0]["occurrences"] == 2
    assert res.duplicate_lake_examples[0]["present_in_source"] is True
    assert res.duplicate_lake_examples[0]["all_rows_identical"] is True
    # Other 3 rows still matched cleanly without misalignment.
    assert res.exact_match_row_count == 3


def test_fault_duplicate_lake_row_differing_values() -> None:
    src = _base_rows()
    lake = copy.deepcopy(src)
    dup = copy.deepcopy(src[0])
    dup["status"] = "stale_duplicate"
    lake.insert(1, dup)

    res = compare_ordered_streams(_chunk(src, 2), _chunk(lake, 2))
    assert res.passed is False
    assert res.duplicate_lake_id_count == 1
    assert res.duplicate_lake_examples[0]["id"] == 1
    assert res.duplicate_lake_examples[0]["all_rows_identical"] is False


def test_fault_decimal_38_digit_last_digit_difference() -> None:
    src = _base_rows()
    lake = copy.deepcopy(src)
    lake[0]["amount"] = Decimal("12345678901234567890123456789.123456788")
    lake[0]["discount"] = Decimal("0.000000002")

    res = compare_ordered_streams([src], [lake])
    assert res.passed is False
    assert res.value_mismatch_row_count == 1
    assert res.value_mismatch_column_counts == {"amount": 1, "discount": 1}
    diff = res.value_mismatch_examples[0]["diffs"]
    assert diff["amount"] == {
        "source": "12345678901234567890123456789.123456789",
        "lake": "12345678901234567890123456789.123456788",
    }
    # Fixed-point formatting must not emit scientific notation (1E-9).
    assert diff["discount"] == {
        "source": "0.000000001",
        "lake": "0.000000002",
    }
    assert format_decimal_fixed(Decimal("0.000000001")) == "0.000000001"


def test_fault_decimal_float_rejected() -> None:
    src = _base_rows()
    lake = copy.deepcopy(src)
    lake[3]["amount"] = 42.5  # float must never be accepted silently

    res = compare_ordered_streams([src], [lake])
    assert res.passed is False
    assert res.value_mismatch_column_counts == {"amount": 1}
    assert "no float" in res.value_mismatch_examples[0]["diffs"]["amount"]["error"]


def test_fault_timestamp_microsecond_difference_and_naive_rejected() -> None:
    src = _base_rows()
    lake = copy.deepcopy(src)
    lake[0]["updated_at"] = datetime(2026, 11, 19, 18, 0, 0, 123457, tzinfo=timezone.utc)
    lake[1]["updated_at"] = datetime(2026, 11, 19, 18, 0, 1, 0)  # naive

    res = compare_ordered_streams([src], [lake])
    assert res.passed is False
    assert res.value_mismatch_row_count == 2
    assert res.value_mismatch_column_counts == {"updated_at": 2}
    assert res.value_mismatch_examples[0]["id"] == 1
    assert res.value_mismatch_examples[0]["diffs"]["updated_at"] == {
        "source": "2026-11-19T18:00:00.123456+00:00",
        "lake": "2026-11-19T18:00:00.123457+00:00",
    }
    assert res.value_mismatch_examples[1]["id"] == 2
    assert "naive" in res.value_mismatch_examples[1]["diffs"]["updated_at"]["error"]


def test_fault_trailing_text_whitespace() -> None:
    src = _base_rows()
    lake = copy.deepcopy(src)
    lake[0]["note"] = "alpha "
    lake[1]["status"] = "updated "

    res = compare_ordered_streams([src], [lake])
    assert res.passed is False
    assert res.value_mismatch_row_count == 2
    assert res.value_mismatch_column_counts == {"note": 1, "status": 1}
    assert res.value_mismatch_examples[0]["diffs"]["note"] == {
        "source": "alpha",
        "lake": "alpha ",
    }


def test_fault_null_vs_empty_text_and_json_null() -> None:
    src = _base_rows()
    lake = copy.deepcopy(src)
    # Row 2: note is SQL NULL in source, empty text in lake; payload is SQL NULL vs JSON null.
    lake[1]["note"] = ""
    lake[1]["payload"] = "null"
    # Row 3: note is "" in source, SQL NULL in lake; payload is JSON null vs empty text "".
    lake[2]["note"] = None
    lake[2]["payload"] = ""

    res = compare_ordered_streams([src], [lake])
    assert res.passed is False
    assert res.value_mismatch_row_count == 2
    assert res.value_mismatch_column_counts == {"note": 2, "payload": 2}
    assert res.value_mismatch_examples[0]["diffs"]["note"] == {"source": None, "lake": ""}
    assert res.value_mismatch_examples[0]["diffs"]["payload"] == {"source": None, "lake": "null"}
    assert res.value_mismatch_examples[1]["diffs"]["note"] == {"source": "", "lake": None}
    assert res.value_mismatch_examples[1]["diffs"]["payload"] == {"source": "null", "lake": ""}


def test_fault_json_changed_values_array_order_exact_number_and_bool_vs_int() -> None:
    src = _base_rows()
    lake = copy.deepcopy(src)
    # Array order changed + exact large number last-digit changed in row 1
    lake[0]["payload"] = '{"a": [1, 3, 2], "b": 2, "exact": 12345678901234567890123456789.123456789}'
    # JSON null vs JSON empty string "" in row 3
    lake[2]["payload"] = '""'
    # bool true vs int 1 in row 4
    lake[3]["payload"] = '{"flag": 1, "nested": {"x": "", "y": null}}'

    res = compare_ordered_streams([src], [lake])
    assert res.passed is False
    assert res.value_mismatch_row_count == 3
    assert res.value_mismatch_column_counts == {"payload": 3}

    # Also test exact large JSON number last-digit change alone
    lake_num = copy.deepcopy(src)
    lake_num[0]["payload"] = (
        '{"b": 2, "a": [1, 2, 3], "exact": 12345678901234567890123456789.123456788}'
    )
    res_num = compare_ordered_streams([src], [lake_num])
    assert res_num.passed is False
    assert res_num.value_mismatch_row_count == 1
    assert res_num.value_mismatch_examples[0]["id"] == 1


def test_schema_verification_passes_valid_and_fails_mismatches() -> None:
    valid = _valid_iceberg_metadata()
    ok = verify_iceberg_schema(valid)
    assert ok.passed is True
    assert ok.schema_id == 0
    assert ok.format_version == 2
    assert ok.identifier_field_ids == [1]
    assert ok.mismatches == []

    # 1. Decimal precision/scale mismatch
    bad_dec = copy.deepcopy(valid)
    bad_dec["metadata"]["schemas"][0]["fields"][2]["type"] = "decimal(38, 8)"
    res_dec = verify_iceberg_schema(bad_dec)
    assert res_dec.passed is False
    assert res_dec.mismatches[0]["kind"] == "type_mismatch"
    assert res_dec.mismatches[0]["column"] == "amount"

    # 2. Timestamp semantics mismatch (timestamp without tz vs timestamptz)
    bad_ts = copy.deepcopy(valid)
    bad_ts["metadata"]["schemas"][0]["fields"][3]["type"] = "timestamp"
    res_ts = verify_iceberg_schema(bad_ts)
    assert res_ts.passed is False
    assert res_ts.mismatches[0]["kind"] == "type_mismatch"
    assert res_ts.mismatches[0]["column"] == "updated_at"

    # 3. Required column mismatch
    bad_req = copy.deepcopy(valid)
    bad_req["metadata"]["schemas"][0]["fields"][1]["required"] = False
    res_req = verify_iceberg_schema(bad_req)
    assert res_req.passed is False
    assert res_req.mismatches[0]["kind"] == "required_mismatch"
    assert res_req.mismatches[0]["column"] == "status"

    # 4. Missing and extra columns
    bad_cols = copy.deepcopy(valid)
    bad_cols["metadata"]["schemas"][0]["fields"].pop()  # remove discount
    bad_cols["metadata"]["schemas"][0]["fields"].append(
        {"id": 8, "name": "unexpected_col", "required": False, "type": "string"}
    )
    res_cols = verify_iceberg_schema(bad_cols)
    assert res_cols.passed is False
    kinds = {m["kind"] for m in res_cols.mismatches}
    assert kinds == {"missing_column", "extra_column"}


def test_examples_capped_at_20_and_out_of_order_rejected() -> None:
    base_ts = datetime(2026, 11, 19, 18, 0, 0, 0, tzinfo=timezone.utc)
    src = [
        (i, "s", Decimal("1.000000000"), base_ts, "n", '{"i": 1}', None)
        for i in range(1, 31)
    ]
    res = compare_ordered_streams(_chunk(src, 7), [[]], max_examples=20)
    assert res.passed is False
    assert res.source_only_count == 30
    assert len(res.source_only_examples) == 20

    unordered = [src[1], src[0]]
    with pytest.raises(ValueError, match="out of order"):
        compare_ordered_streams([unordered], [src])
