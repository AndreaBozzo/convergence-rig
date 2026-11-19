"""Exact schema and bounded-batch ordered row comparison for PostgreSQL -> Iceberg verification."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Context, Decimal, InvalidOperation
import hashlib
import json
import re
import time
from typing import Any

from lab.schema import EXPECTED_ICEBERG_FIELDS

COLUMNS: tuple[str, ...] = (
    "id",
    "status",
    "amount",
    "updated_at",
    "note",
    "payload",
    "discount",
)

DECIMAL38_CTX = Context(prec=38)
_DECIMAL_TYPE_RE = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")
_MAX_PREVIEW_CHARS = 200


def _canonical_iceberg_type(type_val: Any) -> str:
    if not isinstance(type_val, str):
        return json.dumps(type_val, sort_keys=True)
    raw = type_val.strip()
    m = _DECIMAL_TYPE_RE.match(raw)
    if m:
        return f"decimal({int(m.group(1))}, {int(m.group(2))})"
    return raw


@dataclass(frozen=True)
class SchemaCheckResult:
    passed: bool
    schema_id: int | None
    format_version: int | None
    identifier_field_ids: list[int]
    mismatches: list[dict[str, Any]]
    actual_fields: list[dict[str, Any]]
    expected_fields: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "schema_id": self.schema_id,
            "format_version": self.format_version,
            "identifier_field_ids": self.identifier_field_ids,
            "mismatches": self.mismatches,
            "actual_fields": self.actual_fields,
            "expected_fields": self.expected_fields,
        }


def verify_iceberg_schema(
    table_meta_or_response: dict[str, Any],
    expected_fields: Sequence[dict[str, Any]] = EXPECTED_ICEBERG_FIELDS,
    expected_identifier_names: Sequence[str] = ("id",),
) -> SchemaCheckResult:
    """Verify the current Iceberg table schema against the explicit expected mapping."""
    metadata = (
        table_meta_or_response.get("metadata")
        if "metadata" in table_meta_or_response and isinstance(table_meta_or_response.get("metadata"), dict)
        else table_meta_or_response
    )
    format_version = metadata.get("format-version")
    current_schema_id = metadata.get("current-schema-id")
    schemas = metadata.get("schemas") or []

    mismatches: list[dict[str, Any]] = []
    expected_list = [dict(f) for f in expected_fields]

    current_schema: dict[str, Any] | None = None
    for s in schemas:
        if s.get("schema-id") == current_schema_id:
            current_schema = s
            break

    if current_schema is None:
        mismatches.append(
            {
                "kind": "missing_current_schema",
                "current_schema_id": current_schema_id,
                "available_schema_ids": [s.get("schema-id") for s in schemas],
            }
        )
        return SchemaCheckResult(
            passed=False,
            schema_id=current_schema_id,
            format_version=format_version,
            identifier_field_ids=[],
            mismatches=mismatches,
            actual_fields=[],
            expected_fields=expected_list,
        )

    actual_fields = list(current_schema.get("fields") or [])
    identifier_field_ids = [int(x) for x in (current_schema.get("identifier-field-ids") or [])]

    actual_by_name: dict[str, dict[str, Any]] = {}
    for af in actual_fields:
        name = af.get("name")
        if name in actual_by_name:
            mismatches.append({"kind": "duplicate_field_name", "field": name})
        else:
            actual_by_name[str(name)] = af

    expected_by_name = {str(ef["name"]): ef for ef in expected_list}

    for name, ef in expected_by_name.items():
        af = actual_by_name.get(name)
        if af is None:
            mismatches.append({"kind": "missing_column", "column": name, "expected": ef})
            continue
        exp_type = _canonical_iceberg_type(ef["type"])
        act_type = _canonical_iceberg_type(af.get("type"))
        if act_type != exp_type:
            mismatches.append(
                {
                    "kind": "type_mismatch",
                    "column": name,
                    "expected_type": exp_type,
                    "actual_type": act_type,
                }
            )
        exp_req = bool(ef["required"])
        act_req = bool(af.get("required"))
        if act_req != exp_req:
            mismatches.append(
                {
                    "kind": "required_mismatch",
                    "column": name,
                    "expected_required": exp_req,
                    "actual_required": act_req,
                }
            )

    for name, af in actual_by_name.items():
        if name not in expected_by_name:
            mismatches.append({"kind": "extra_column", "column": name, "actual": af})

    expected_id_field_ids = [
        int(actual_by_name[n]["id"])
        for n in expected_identifier_names
        if n in actual_by_name and "id" in actual_by_name[n]
    ]
    if len(expected_id_field_ids) == len(expected_identifier_names):
        if sorted(identifier_field_ids) != sorted(expected_id_field_ids):
            mismatches.append(
                {
                    "kind": "identifier_field_ids_mismatch",
                    "expected_identifier_field_ids": expected_id_field_ids,
                    "actual_identifier_field_ids": identifier_field_ids,
                }
            )

    return SchemaCheckResult(
        passed=len(mismatches) == 0,
        schema_id=int(current_schema_id) if current_schema_id is not None else None,
        format_version=int(format_version) if format_version is not None else None,
        identifier_field_ids=identifier_field_ids,
        mismatches=mismatches,
        actual_fields=actual_fields,
        expected_fields=expected_list,
    )


def _to_row_tuple(row: Any) -> tuple[Any, ...]:
    if isinstance(row, dict):
        return tuple(row[c] for c in COLUMNS)
    if isinstance(row, (tuple, list)):
        if len(row) != len(COLUMNS):
            raise ValueError(f"Expected {len(COLUMNS)} columns, got {len(row)}")
        return tuple(row)
    raise TypeError(f"Unsupported row type: {type(row).__name__}")


def _parse_int_id(val: Any) -> int:
    if isinstance(val, bool) or not isinstance(val, int):
        raise TypeError(f"id must be int, got {type(val).__name__}: {val!r}")
    return val


def _parse_exact_decimal(val: Any) -> Decimal:
    if isinstance(val, bool) or isinstance(val, float):
        raise TypeError(f"Exact decimal required (no float/bool), got {type(val).__name__}: {val!r}")
    if isinstance(val, Decimal):
        d = val
    elif isinstance(val, (int, str)):
        try:
            d = Decimal(val)
        except InvalidOperation as exc:
            raise ValueError(f"Invalid decimal value: {val!r}") from exc
    else:
        raise TypeError(f"Unsupported decimal value type {type(val).__name__}: {val!r}")
    if not d.is_finite():
        raise ValueError(f"Non-finite decimal value not allowed: {val!r}")
    return d


def format_decimal_fixed(val: Decimal | None) -> str | None:
    if val is None:
        return None
    return format(val, "f")


def _parse_utc_timestamp(val: Any) -> datetime:
    if isinstance(val, str):
        s = val.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
    elif isinstance(val, datetime):
        dt = val
    else:
        raise TypeError(f"Unsupported timestamp type {type(val).__name__}: {val!r}")
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"Timestamp must be timezone-aware (timestamptz), got naive {val!r}")
    return dt.astimezone(timezone.utc)


def _utc_microsecond_key(dt: datetime) -> tuple[int, int, int, int, int, int, int]:
    return (
        dt.year,
        dt.month,
        dt.day,
        dt.hour,
        dt.minute,
        dt.second,
        dt.microsecond,
    )


def _canonicalize_parsed_json(obj: Any) -> Any:
    if obj is None:
        return ("json_null",)
    if isinstance(obj, bool):
        return ("bool", obj)
    if isinstance(obj, Decimal):
        return ("number", obj)
    if isinstance(obj, int):
        return ("number", Decimal(obj))
    if isinstance(obj, str):
        return ("string", obj.encode("utf-8"))
    if isinstance(obj, list):
        return ("array", tuple(_canonicalize_parsed_json(item) for item in obj))
    if isinstance(obj, dict):
        return (
            "object",
            tuple(
                sorted(
                    (k.encode("utf-8"), _canonicalize_parsed_json(v))
                    for k, v in obj.items()
                )
            ),
        )
    raise TypeError(f"Unexpected JSON value type {type(obj).__name__}: {obj!r}")


def _canonicalize_json_column(val: Any) -> Any:
    """Distinguish SQL NULL (None), JSON null ('null'), empty/invalid text (''), and exact JSON."""
    if val is None:
        return ("sql_null",)
    if not isinstance(val, str):
        raise TypeError(
            f"JSON column must be raw JSON string or None (SQL NULL) to preserve exact numbers "
            f"and NULL semantics, got {type(val).__name__}"
        )
    try:
        parsed = json.loads(val, parse_float=Decimal, parse_int=Decimal)
    except json.JSONDecodeError:
        return ("invalid_json_text", val.encode("utf-8"))
    return _canonicalize_parsed_json(parsed)


def _preview_value(col: str, val: Any) -> Any:
    if val is None:
        return None
    if col in ("amount", "discount"):
        try:
            return format_decimal_fixed(_parse_exact_decimal(val))
        except Exception:
            return repr(val)
    if col == "updated_at":
        try:
            return _parse_utc_timestamp(val).isoformat(timespec="microseconds")
        except Exception:
            return repr(val)
    if isinstance(val, str):
        raw_bytes = val.encode("utf-8")
        if len(val) > _MAX_PREVIEW_CHARS:
            return {
                "preview": val[:_MAX_PREVIEW_CHARS] + "...",
                "char_len": len(val),
                "utf8_bytes": len(raw_bytes),
                "sha256": hashlib.sha256(raw_bytes).hexdigest(),
            }
        return val
    return val


def _format_row_preview(row: tuple[Any, ...]) -> dict[str, Any]:
    return {col: _preview_value(col, val) for col, val in zip(COLUMNS, row)}


def compare_row_values(
    src_row: tuple[Any, ...],
    lake_row: tuple[Any, ...],
) -> dict[str, dict[str, Any]]:
    """Compare every column of two matched rows; return dict of differing columns."""
    diffs: dict[str, dict[str, Any]] = {}

    # 1. status: required text, UTF-8 byte-for-byte
    s_status, l_status = src_row[1], lake_row[1]
    if not (
        isinstance(s_status, str)
        and isinstance(l_status, str)
        and s_status.encode("utf-8") == l_status.encode("utf-8")
    ):
        diffs["status"] = {
            "source": _preview_value("status", s_status),
            "lake": _preview_value("status", l_status),
        }

    # 2. amount: required numeric(38, 9), exact Decimal
    s_amt_raw, l_amt_raw = src_row[2], lake_row[2]
    try:
        s_amt = _parse_exact_decimal(s_amt_raw)
        l_amt = _parse_exact_decimal(l_amt_raw)
        if s_amt != l_amt:
            diffs["amount"] = {
                "source": format_decimal_fixed(s_amt),
                "lake": format_decimal_fixed(l_amt),
            }
    except Exception as exc:
        diffs["amount"] = {
            "source": _preview_value("amount", s_amt_raw),
            "lake": _preview_value("amount", l_amt_raw),
            "error": str(exc),
        }

    # 3. updated_at: required timestamptz, UTC instant at microsecond precision
    s_ts_raw, l_ts_raw = src_row[3], lake_row[3]
    try:
        s_ts = _parse_utc_timestamp(s_ts_raw)
        l_ts = _parse_utc_timestamp(l_ts_raw)
        if _utc_microsecond_key(s_ts) != _utc_microsecond_key(l_ts):
            diffs["updated_at"] = {
                "source": s_ts.isoformat(timespec="microseconds"),
                "lake": l_ts.isoformat(timespec="microseconds"),
            }
    except Exception as exc:
        diffs["updated_at"] = {
            "source": _preview_value("updated_at", s_ts_raw),
            "lake": _preview_value("updated_at", l_ts_raw),
            "error": str(exc),
        }

    # 4. note: nullable text, distinguish NULL vs "" and compare UTF-8 byte-for-byte
    s_note, l_note = src_row[4], lake_row[4]
    if s_note is None and l_note is None:
        pass
    elif (
        isinstance(s_note, str)
        and isinstance(l_note, str)
        and s_note.encode("utf-8") == l_note.encode("utf-8")
    ):
        pass
    else:
        diffs["note"] = {
            "source": _preview_value("note", s_note),
            "lake": _preview_value("note", l_note),
        }

    # 5. payload: nullable jsonb/json string, structural comparison with exact numbers
    s_pay, l_pay = src_row[5], lake_row[5]
    try:
        s_pay_c = _canonicalize_json_column(s_pay)
        l_pay_c = _canonicalize_json_column(l_pay)
        if s_pay_c != l_pay_c:
            diffs["payload"] = {
                "source": _preview_value("payload", s_pay),
                "lake": _preview_value("payload", l_pay),
            }
    except Exception as exc:
        diffs["payload"] = {
            "source": _preview_value("payload", s_pay),
            "lake": _preview_value("payload", l_pay),
            "error": str(exc),
        }

    # 6. discount: nullable numeric(38, 9), exact Decimal
    s_disc_raw, l_disc_raw = src_row[6], lake_row[6]
    if s_disc_raw is None and l_disc_raw is None:
        pass
    elif s_disc_raw is None or l_disc_raw is None:
        diffs["discount"] = {
            "source": _preview_value("discount", s_disc_raw),
            "lake": _preview_value("discount", l_disc_raw),
        }
    else:
        try:
            s_disc = _parse_exact_decimal(s_disc_raw)
            l_disc = _parse_exact_decimal(l_disc_raw)
            if s_disc != l_disc:
                diffs["discount"] = {
                    "source": format_decimal_fixed(s_disc),
                    "lake": format_decimal_fixed(l_disc),
                }
        except Exception as exc:
            diffs["discount"] = {
                "source": _preview_value("discount", s_disc_raw),
                "lake": _preview_value("discount", l_disc_raw),
                "error": str(exc),
            }

    return diffs


class _GroupedIdStream:
    """Stream rows ordered non-decreasingly by id in bounded batches, grouping identical IDs."""

    def __init__(self, batches: Iterable[Sequence[Any]], *, side_name: str) -> None:
        self._batches = iter(batches)
        self._side_name = side_name
        self._buf: Sequence[Any] = ()
        self._idx = 0
        self.batches_read = 0
        self.rows_read = 0
        self._prev_id: int | None = None
        self._peeked: tuple[int, tuple[Any, ...]] | None = None
        self._exhausted = False

    def _next_raw(self) -> tuple[int, tuple[Any, ...]] | None:
        if self._peeked is not None:
            item = self._peeked
            self._peeked = None
            return item
        if self._exhausted:
            return None
        while self._idx >= len(self._buf):
            try:
                batch = next(self._batches)
            except StopIteration:
                self._exhausted = True
                return None
            self.batches_read += 1
            self._buf = batch
            self._idx = 0
        raw_row = self._buf[self._idx]
        self._idx += 1
        self.rows_read += 1
        row_tuple = _to_row_tuple(raw_row)
        row_id = _parse_int_id(row_tuple[0])
        if self._prev_id is not None and row_id < self._prev_id:
            raise ValueError(
                f"{self._side_name} stream out of order: saw id={row_id} after id={self._prev_id}"
            )
        self._prev_id = row_id
        return row_id, row_tuple

    def next_group(self) -> tuple[int, list[tuple[Any, ...]]] | None:
        first = self._next_raw()
        if first is None:
            return None
        group_id, first_row = first
        rows = [first_row]
        while True:
            nxt = self._next_raw()
            if nxt is None:
                break
            nxt_id, nxt_row = nxt
            if nxt_id == group_id:
                rows.append(nxt_row)
            else:
                self._peeked = (nxt_id, nxt_row)
                break
        return group_id, rows


@dataclass
class ComparisonResult:
    passed: bool
    source_row_count: int
    lake_row_count: int
    matched_id_count: int
    exact_match_row_count: int
    source_only_count: int
    source_only_examples: list[dict[str, Any]] = field(default_factory=list)
    lake_only_count: int = 0
    lake_only_examples: list[dict[str, Any]] = field(default_factory=list)
    duplicate_lake_id_count: int = 0
    duplicate_lake_extra_row_count: int = 0
    duplicate_lake_examples: list[dict[str, Any]] = field(default_factory=list)
    duplicate_source_id_count: int = 0
    duplicate_source_examples: list[dict[str, Any]] = field(default_factory=list)
    value_mismatch_row_count: int = 0
    value_mismatch_column_counts: dict[str, int] = field(default_factory=dict)
    value_mismatch_examples: list[dict[str, Any]] = field(default_factory=list)
    batches_read_source: int = 0
    batches_read_lake: int = 0
    diff_duration_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "source_row_count": self.source_row_count,
            "lake_row_count": self.lake_row_count,
            "matched_id_count": self.matched_id_count,
            "exact_match_row_count": self.exact_match_row_count,
            "source_only_count": self.source_only_count,
            "source_only_examples": self.source_only_examples,
            "lake_only_count": self.lake_only_count,
            "lake_only_examples": self.lake_only_examples,
            "duplicate_lake_id_count": self.duplicate_lake_id_count,
            "duplicate_lake_extra_row_count": self.duplicate_lake_extra_row_count,
            "duplicate_lake_examples": self.duplicate_lake_examples,
            "duplicate_source_id_count": self.duplicate_source_id_count,
            "duplicate_source_examples": self.duplicate_source_examples,
            "value_mismatch_row_count": self.value_mismatch_row_count,
            "value_mismatch_column_counts": self.value_mismatch_column_counts,
            "value_mismatch_examples": self.value_mismatch_examples,
            "batches_read_source": self.batches_read_source,
            "batches_read_lake": self.batches_read_lake,
            "diff_duration_ms": round(self.diff_duration_ms, 2),
        }


def _all_rows_identical(rows: list[tuple[Any, ...]]) -> bool:
    first = rows[0]
    return all(len(compare_row_values(first, other)) == 0 for other in rows[1:])


def compare_ordered_streams(
    source_batches: Iterable[Sequence[Any]],
    lake_batches: Iterable[Sequence[Any]],
    *,
    max_examples: int = 20,
) -> ComparisonResult:
    """Merge-compare two id-ordered row streams in bounded batches, detecting duplicates first."""
    t0 = time.monotonic_ns()
    src_stream = _GroupedIdStream(source_batches, side_name="source")
    lake_stream = _GroupedIdStream(lake_batches, side_name="lake")

    matched_id_count = 0
    exact_match_row_count = 0
    source_only_count = 0
    source_only_examples: list[dict[str, Any]] = []
    lake_only_count = 0
    lake_only_examples: list[dict[str, Any]] = []
    duplicate_lake_id_count = 0
    duplicate_lake_extra_row_count = 0
    duplicate_lake_examples: list[dict[str, Any]] = []
    duplicate_source_id_count = 0
    duplicate_source_examples: list[dict[str, Any]] = []
    value_mismatch_row_count = 0
    value_mismatch_column_counts: dict[str, int] = {}
    value_mismatch_examples: list[dict[str, Any]] = []

    src_group = src_stream.next_group()
    lake_group = lake_stream.next_group()

    while src_group is not None or lake_group is not None:
        if lake_group is None:
            src_id, src_rows = src_group  # type: ignore[misc]
            if len(src_rows) > 1:
                duplicate_source_id_count += 1
                if len(duplicate_source_examples) < max_examples:
                    duplicate_source_examples.append(
                        {"id": src_id, "occurrences": len(src_rows)}
                    )
            source_only_count += 1
            if len(source_only_examples) < max_examples:
                source_only_examples.append(
                    {"id": src_id, "source_row": _format_row_preview(src_rows[0])}
                )
            src_group = src_stream.next_group()
            continue

        if src_group is None:
            lake_id, lake_rows = lake_group
            if len(lake_rows) > 1:
                duplicate_lake_id_count += 1
                duplicate_lake_extra_row_count += len(lake_rows) - 1
                if len(duplicate_lake_examples) < max_examples:
                    duplicate_lake_examples.append(
                        {
                            "id": lake_id,
                            "occurrences": len(lake_rows),
                            "present_in_source": False,
                            "all_rows_identical": _all_rows_identical(lake_rows),
                            "sample_rows": [_format_row_preview(r) for r in lake_rows[:2]],
                        }
                    )
            lake_only_count += 1
            if len(lake_only_examples) < max_examples:
                lake_only_examples.append(
                    {
                        "id": lake_id,
                        "occurrences": len(lake_rows),
                        "lake_row": _format_row_preview(lake_rows[0]),
                    }
                )
            lake_group = lake_stream.next_group()
            continue

        src_id, src_rows = src_group
        lake_id, lake_rows = lake_group

        if src_id < lake_id:
            if len(src_rows) > 1:
                duplicate_source_id_count += 1
                if len(duplicate_source_examples) < max_examples:
                    duplicate_source_examples.append(
                        {"id": src_id, "occurrences": len(src_rows)}
                    )
            source_only_count += 1
            if len(source_only_examples) < max_examples:
                source_only_examples.append(
                    {"id": src_id, "source_row": _format_row_preview(src_rows[0])}
                )
            src_group = src_stream.next_group()
            continue

        if lake_id < src_id:
            if len(lake_rows) > 1:
                duplicate_lake_id_count += 1
                duplicate_lake_extra_row_count += len(lake_rows) - 1
                if len(duplicate_lake_examples) < max_examples:
                    duplicate_lake_examples.append(
                        {
                            "id": lake_id,
                            "occurrences": len(lake_rows),
                            "present_in_source": False,
                            "all_rows_identical": _all_rows_identical(lake_rows),
                            "sample_rows": [_format_row_preview(r) for r in lake_rows[:2]],
                        }
                    )
            lake_only_count += 1
            if len(lake_only_examples) < max_examples:
                lake_only_examples.append(
                    {
                        "id": lake_id,
                        "occurrences": len(lake_rows),
                        "lake_row": _format_row_preview(lake_rows[0]),
                    }
                )
            lake_group = lake_stream.next_group()
            continue

        # src_id == lake_id: detect duplicates on Iceberg (and source) BEFORE pairing rows.
        has_dup = False
        if len(src_rows) > 1:
            has_dup = True
            duplicate_source_id_count += 1
            if len(duplicate_source_examples) < max_examples:
                duplicate_source_examples.append(
                    {"id": src_id, "occurrences": len(src_rows)}
                )
        if len(lake_rows) > 1:
            has_dup = True
            duplicate_lake_id_count += 1
            duplicate_lake_extra_row_count += len(lake_rows) - 1
            if len(duplicate_lake_examples) < max_examples:
                duplicate_lake_examples.append(
                    {
                        "id": lake_id,
                        "occurrences": len(lake_rows),
                        "present_in_source": True,
                        "all_rows_identical": _all_rows_identical(lake_rows),
                        "sample_rows": [_format_row_preview(r) for r in lake_rows[:2]],
                    }
                )

        if not has_dup:
            matched_id_count += 1
            diffs = compare_row_values(src_rows[0], lake_rows[0])
            if not diffs:
                exact_match_row_count += 1
            else:
                value_mismatch_row_count += 1
                for col in diffs:
                    value_mismatch_column_counts[col] = (
                        value_mismatch_column_counts.get(col, 0) + 1
                    )
                if len(value_mismatch_examples) < max_examples:
                    value_mismatch_examples.append({"id": src_id, "diffs": diffs})

        src_group = src_stream.next_group()
        lake_group = lake_stream.next_group()

    diff_duration_ms = (time.monotonic_ns() - t0) / 1_000_000.0
    passed = (
        source_only_count == 0
        and lake_only_count == 0
        and duplicate_lake_id_count == 0
        and duplicate_source_id_count == 0
        and value_mismatch_row_count == 0
    )
    return ComparisonResult(
        passed=passed,
        source_row_count=src_stream.rows_read,
        lake_row_count=lake_stream.rows_read,
        matched_id_count=matched_id_count,
        exact_match_row_count=exact_match_row_count,
        source_only_count=source_only_count,
        source_only_examples=source_only_examples,
        lake_only_count=lake_only_count,
        lake_only_examples=lake_only_examples,
        duplicate_lake_id_count=duplicate_lake_id_count,
        duplicate_lake_extra_row_count=duplicate_lake_extra_row_count,
        duplicate_lake_examples=duplicate_lake_examples,
        duplicate_source_id_count=duplicate_source_id_count,
        duplicate_source_examples=duplicate_source_examples,
        value_mismatch_row_count=value_mismatch_row_count,
        value_mismatch_column_counts=value_mismatch_column_counts,
        value_mismatch_examples=value_mismatch_examples,
        batches_read_source=src_stream.batches_read,
        batches_read_lake=lake_stream.batches_read,
        diff_duration_ms=diff_duration_ms,
    )


def stream_postgres_orders(
    conn: Any,
    *,
    batch_size: int = 2000,
) -> Iterator[list[tuple[Any, ...]]]:
    """Stream public.orders from PostgreSQL ordered by id via a server-side cursor.

    Note: selects payload::text so psycopg does not convert JSON numbers to Python floats
    and so SQL NULL (None) remains distinct from JSON null ('null').
    """
    with conn.cursor(name="cdclab_orders_verify_scan") as cur:
        cur.itersize = batch_size
        cur.execute(
            """
            SELECT id, status, amount, updated_at, note, payload::text AS payload, discount
            FROM public.orders
            ORDER BY id
            """
        )
        while True:
            batch = cur.fetchmany(batch_size)
            if not batch:
                break
            yield batch


def stream_duckdb_orders_at_snapshot(
    con: Any,
    snapshot_id: int,
    *,
    namespace: str = "public",
    batch_size: int = 2000,
) -> Iterator[list[tuple[Any, ...]]]:
    """Stream orders from DuckDB Iceberg at an explicit snapshot ID in bounded batches."""
    cur = con.execute(
        f"SELECT id, status, amount, updated_at, note, payload, discount "
        f"FROM cdclab.{namespace}.orders AT (VERSION => {int(snapshot_id)}) "
        f"ORDER BY id"
    )
    while True:
        batch = cur.fetchmany(batch_size)
        if not batch:
            break
        yield batch
