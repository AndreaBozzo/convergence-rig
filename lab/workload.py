"""Deterministic seeding (with verifiable out-of-line TOAST) and paced CDC workload generator."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import math
from pathlib import Path
import random
import threading
import time
from typing import Any

import psycopg

from lab.config import LabConfig
from lab.pipeline import (
    capture_pg_pipeline_positions,
    json_default,
    utc_now_iso,
    wait_for_marker_visible,
)

STATUSES = ("created", "pending", "processing", "shipped", "settled", "cancelled")
EDGE_CASE_MAX_ID = 12
INSERT_ID_START = 1_000_000_001
WARMUP_MARKER_ID = 900_000_000
PROBE_MARKER_ID_START = 900_000_001
FINAL_MARKER_ID = 900_999_999

# 4608 pseudo-random bytes -> 6144 URL-safe base64 ASCII chars: incompressible by pglz/lz4,
# reliably exceeding PostgreSQL's ~2 KB TOAST_TUPLE_THRESHOLD so values live out-of-line.
TOAST_RAW_BYTES = 4608


def _make_incompressible_blob(rng: random.Random, raw_bytes: int = TOAST_RAW_BYTES) -> str:
    return base64.urlsafe_b64encode(rng.randbytes(raw_bytes)).decode("ascii")


def _make_toast_note(rng: random.Random, row_id: int, rev: int) -> str:
    return f"toast-note-id={row_id}-rev={rev}-" + _make_incompressible_blob(rng)


def _make_toast_payload_json(rng: random.Random, row_id: int, rev: int) -> str:
    return json.dumps(
        {
            "toast": True,
            "row_id": row_id,
            "rev": rev,
            "exact_num": "12345678901234567890123456789.123456789",
            "blob": _make_incompressible_blob(rng),
        },
        separators=(",", ":"),
    )


def _make_decimal_38_9(rng: random.Random, *, extreme: bool = False) -> Decimal:
    if extreme:
        sign = "-" if rng.random() < 0.2 else ""
        int_part = "".join(str(rng.randint(0, 9)) for _ in range(29)).lstrip("0") or "1"
        frac_part = f"{rng.randint(0, 999_999_999):09d}"
        return Decimal(f"{sign}{int_part}.{frac_part}")
    whole = rng.randint(0, 999_999)
    frac = rng.randint(0, 999_999_999)
    sign = "-" if rng.random() < 0.05 else ""
    return Decimal(f"{sign}{whole}.{frac:09d}")


def _make_timestamp(base_utc: datetime, offset_us: int) -> datetime:
    return base_utc + timedelta(microseconds=offset_us)


def build_edge_case_rows(base_utc: datetime) -> list[tuple[Any, ...]]:
    """Return 12 deterministic edge-case rows exercising decimals, timestamps, text, NULLs, JSON."""
    return [
        (
            1,
            "created",
            Decimal("12345678901234567890123456789.123456789"),
            _make_timestamp(base_utc, 123456),
            "trailing space   ",
            '{"b": 2, "a": [1, 2, 3], "exact": 12345678901234567890123456789.123456789}',
            Decimal("0.000000001"),
        ),
        (
            2,
            "pending",
            Decimal("0.000000000"),
            _make_timestamp(base_utc, 0),
            None,
            None,
            None,
        ),
        (
            3,
            "settled",
            Decimal("-99999999999999999999999999999.999999999"),
            _make_timestamp(base_utc, 999999),
            "",
            "null",
            Decimal("100.000000000"),
        ),
        (
            4,
            "shipped",
            Decimal("99999999999999999999999999999.999999999"),
            _make_timestamp(base_utc, 1),
            "caffè ☕ 日本語",
            '""',
            Decimal("-0.000000001"),
        ),
        (
            5,
            "processing",
            Decimal("42.500000000"),
            _make_timestamp(base_utc, 500000),
            "   leading and trailing   ",
            '{"flag": true, "zero": 0, "arr": [3, 1, 2], "empty": "", "nil": null}',
            Decimal("99999999999999999999999999999.999999999"),
        ),
        (
            6,
            "cancelled",
            Decimal("-0.000000001"),
            _make_timestamp(base_utc, 2),
            "\tline1\nline2\r\n",
            "{}",
            Decimal("0.000000000"),
        ),
        (
            7,
            "created",
            Decimal("12345678901234567890123456789.123456788"),
            _make_timestamp(base_utc, 123457),
            "non-empty",
            "[]",
            Decimal("-99999999999999999999999999999.999999999"),
        ),
        (
            8,
            "pending",
            Decimal("1.000000000"),
            _make_timestamp(base_utc, 987654),
            " ",
            '[null, true, false, 0, 1, "null", ""]',
            None,
        ),
        (
            9,
            "processing",
            Decimal("100000000000000000000.000000001"),
            _make_timestamp(base_utc, 654321),
            None,
            '{"nested": {"deep": [1, {"k": "v"}]}}',
            Decimal("0.123456789"),
        ),
        (
            10,
            "shipped",
            Decimal("0.000000001"),
            _make_timestamp(base_utc, 111111),
            "",
            None,
            Decimal("12345.678900000"),
        ),
        (
            11,
            "settled",
            Decimal("77777777777777777777777777777.777777777"),
            _make_timestamp(base_utc, 222222),
            "quote\"and\\slash",
            '{"quote": "a\\"b", "slash": "c\\\\d"}',
            Decimal("0.500000000"),
        ),
        (
            12,
            "created",
            Decimal("888.000000008"),
            _make_timestamp(base_utc, 333333),
            "edge-12",
            '{"false_val": false, "int_zero": 0}',
            None,
        ),
    ]


def verify_toast_storage(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    """Verify out-of-line TOAST storage on public.orders using PG19 pg_column_toast_chunk_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE pg_column_toast_chunk_id(note) IS NOT NULL) AS note_toasted_rows,
                COUNT(*) FILTER (WHERE pg_column_toast_chunk_id(payload) IS NOT NULL) AS payload_toasted_rows,
                COUNT(*) FILTER (
                    WHERE pg_column_toast_chunk_id(note) IS NOT NULL
                      AND pg_column_toast_chunk_id(payload) IS NOT NULL
                ) AS both_toasted_rows,
                COALESCE(MAX(octet_length(note)), 0) AS max_note_bytes,
                COALESCE(MAX(octet_length(payload::text)), 0) AS max_payload_bytes
            FROM public.orders
            """
        )
        row = cur.fetchone()
        cur.execute(
            """
            SELECT
                pg_relation_size('public.orders'::regclass) AS heap_bytes,
                pg_table_size('public.orders'::regclass) AS table_and_toast_bytes,
                pg_indexes_size('public.orders'::regclass) AS indexes_bytes,
                pg_total_relation_size('public.orders'::regclass) AS total_relation_bytes,
                CASE
                    WHEN c.reltoastrelid <> 0 THEN pg_total_relation_size(c.reltoastrelid)
                    ELSE 0
                END AS toast_relation_bytes
            FROM pg_class c
            WHERE c.oid = 'public.orders'::regclass
            """
        )
        sz = cur.fetchone()
    return {
        "total_rows": int(row[0]),
        "note_toasted_rows": int(row[1]),
        "payload_toasted_rows": int(row[2]),
        "both_toasted_rows": int(row[3]),
        "max_note_bytes": int(row[4]),
        "max_payload_bytes": int(row[5]),
        "heap_bytes": int(sz[0]),
        "table_and_toast_bytes": int(sz[1]),
        "indexes_bytes": int(sz[2]),
        "total_relation_bytes": int(sz[3]),
        "toast_relation_bytes": int(sz[4]),
    }


@dataclass(frozen=True)
class SeedResult:
    seed_rows: int
    rng_seed: int
    toast_ids: list[int]
    mutable_ids: list[int]
    edge_ids: list[int]
    seed_duration_ms: float
    toast_verification: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "seed_rows": self.seed_rows,
            "rng_seed": self.rng_seed,
            "edge_row_count": len(self.edge_ids),
            "toast_row_count": len(self.toast_ids),
            "toast_id_range": [min(self.toast_ids), max(self.toast_ids)] if self.toast_ids else [],
            "mutable_row_count": len(self.mutable_ids),
            "seed_duration_ms": round(self.seed_duration_ms, 2),
            "toast_verification": self.toast_verification,
        }


def seed_orders(
    conn: psycopg.Connection[Any],
    *,
    seed_rows: int = 25000,
    toast_row_count: int = 100,
    rng_seed: int = 20261119,
) -> SeedResult:
    """Seed public.orders deterministically using COPY, then verify out-of-line TOAST storage."""
    if seed_rows < 30:
        raise ValueError("seed_rows must be at least 30")
    actual_toast_count = min(toast_row_count, max(10, seed_rows // 10))
    toast_start_id = EDGE_CASE_MAX_ID + 1
    toast_end_id = toast_start_id + actual_toast_count - 1
    toast_id_set = set(range(toast_start_id, toast_end_id + 1))

    rng = random.Random(rng_seed)
    base_utc = datetime(2026, 11, 19, 12, 0, 0, 0, tzinfo=timezone.utc)
    edge_rows = build_edge_case_rows(base_utc)

    t0 = time.monotonic_ns()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE public.orders;")
        with cur.copy(
            "COPY public.orders (id, status, amount, updated_at, note, payload, discount) FROM STDIN"
        ) as copy:
            for row in edge_rows:
                copy.write_row(row)

            for row_id in range(EDGE_CASE_MAX_ID + 1, seed_rows + 1):
                status = STATUSES[rng.randrange(len(STATUSES))]
                amount = _make_decimal_38_9(rng, extreme=(row_id % 500 == 0))
                updated_at = _make_timestamp(base_utc, row_id * 1000 + rng.randint(0, 999))
                discount = None if rng.random() < 0.3 else _make_decimal_38_9(rng)

                if row_id in toast_id_set:
                    note: str | None = _make_toast_note(rng, row_id, rev=0)
                    payload: str | None = _make_toast_payload_json(rng, row_id, rev=0)
                else:
                    r_note = rng.random()
                    if r_note < 0.10:
                        note = None
                    elif r_note < 0.15:
                        note = ""
                    elif r_note < 0.20:
                        note = f"order-{row_id}   "
                    else:
                        note = f"order-{row_id}-batch-{row_id // 1000}"

                    r_pay = rng.random()
                    if r_pay < 0.08:
                        payload = None
                    elif r_pay < 0.12:
                        payload = "null"
                    else:
                        payload = json.dumps(
                            {
                                "order_id": row_id,
                                "items": [rng.randint(1, 50), rng.randint(1, 50)],
                                "priority": rng.random() < 0.25,
                                "tag": f"t{row_id % 17}",
                            },
                            separators=(",", ":"),
                        )

                copy.write_row((row_id, status, amount, updated_at, note, payload, discount))
    conn.commit()
    seed_duration_ms = (time.monotonic_ns() - t0) / 1_000_000.0

    toast_ver = verify_toast_storage(conn)
    if (
        toast_ver["both_toasted_rows"] < actual_toast_count
        or toast_ver["toast_relation_bytes"] <= 0
    ):
        raise RuntimeError(
            f"Out-of-line TOAST verification failed after seed: expected >= {actual_toast_count} "
            f"both-toasted rows and toast_relation_bytes > 0, got {toast_ver!r}"
        )

    toast_ids = list(range(toast_start_id, toast_end_id + 1))
    mutable_ids = list(range(toast_end_id + 1, seed_rows + 1))
    edge_ids = list(range(1, EDGE_CASE_MAX_ID + 1))
    return SeedResult(
        seed_rows=seed_rows,
        rng_seed=rng_seed,
        toast_ids=toast_ids,
        mutable_ids=mutable_ids,
        edge_ids=edge_ids,
        seed_duration_ms=seed_duration_ms,
        toast_verification=toast_ver,
    )


def commit_marker_row(
    conn: psycopg.Connection[Any],
    *,
    marker_id: int,
    status: str,
    note: str,
) -> tuple[int, str]:
    """Commit a unique marker row into public.orders and return (commit_mono_ns, commit_utc)."""
    now_dt = datetime.now(timezone.utc)
    payload = json.dumps(
        {"marker": True, "marker_id": marker_id, "status": status},
        separators=(",", ":"),
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.orders (id, status, amount, updated_at, note, payload, discount)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
            """,
            (
                marker_id,
                status,
                Decimal("0.000000001"),
                now_dt,
                note,
                payload,
                None,
            ),
        )
    conn.commit()
    return time.monotonic_ns(), utc_now_iso()


def percentile(sorted_values: list[float], pct: float) -> float | None:
    """Nearest-rank / linear interpolated percentile on a pre-sorted list of floats."""
    n = len(sorted_values)
    if n == 0:
        return None
    if n == 1:
        return round(sorted_values[0], 3)
    rank = (pct / 100.0) * (n - 1)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return round(sorted_values[lo], 3)
    frac = rank - lo
    val = sorted_values[lo] + frac * (sorted_values[hi] - sorted_values[lo])
    return round(val, 3)


def summarize_latency(values: list[float]) -> dict[str, Any]:
    if not values:
        return {
            "count": 0,
            "min_ms": None,
            "mean_ms": None,
            "p50_ms": None,
            "p95_ms": None,
            "p99_ms": None,
            "max_ms": None,
        }
    s = sorted(values)
    return {
        "count": len(s),
        "min_ms": round(s[0], 3),
        "mean_ms": round(sum(s) / len(s), 3),
        "p50_ms": percentile(s, 50.0),
        "p95_ms": percentile(s, 95.0),
        "p99_ms": percentile(s, 99.0),
        "max_ms": round(s[-1], 3),
    }


class SteadyStateLagProber:
    """Background thread that inserts occasional unique markers and measures DuckDB visibility lag."""

    def __init__(
        self,
        cfg: LabConfig,
        *,
        window_start_mono_ns: int,
        duration_s: float,
        probe_interval_s: float = 20.0,
        poll_interval_s: float = 0.5,
        out_jsonl: Path,
    ) -> None:
        self._cfg = cfg
        self._window_start_mono_ns = window_start_mono_ns
        self._duration_s = duration_s
        self._probe_interval_s = probe_interval_s
        self._poll_interval_s = poll_interval_s
        self._out_jsonl = out_jsonl
        self._stop_event = threading.Event()
        self._results: list[dict[str, Any]] = []
        self._thread = threading.Thread(target=self._run, name="steady-state-lag-prober", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop_and_join(self, timeout_s: float = 60.0) -> list[dict[str, Any]]:
        self._stop_event.set()
        self._thread.join(timeout=timeout_s)
        return list(self._results)

    def _run(self) -> None:
        # Schedule first probe at min(10s, duration_s / 3), then every probe_interval_s
        # stopping at least 10s before window end so probes represent steady state.
        first_offset_s = min(10.0, max(2.0, self._duration_s / 3.0))
        probe_offsets: list[float] = []
        t = first_offset_s
        cutoff_s = max(first_offset_s + 0.1, self._duration_s - 10.0)
        while t < cutoff_s:
            probe_offsets.append(t)
            t += self._probe_interval_s

        self._out_jsonl.parent.mkdir(parents=True, exist_ok=True)
        with self._out_jsonl.open("w", encoding="utf-8", buffering=1) as fh:
            for idx, offset_s in enumerate(probe_offsets, start=1):
                target_ns = self._window_start_mono_ns + int(offset_s * 1_000_000_000)
                while not self._stop_event.is_set():
                    rem_s = (target_ns - time.monotonic_ns()) / 1_000_000_000.0
                    if rem_s <= 0:
                        break
                    self._stop_event.wait(timeout=min(0.25, rem_s))
                if self._stop_event.is_set():
                    break

                marker_id = PROBE_MARKER_ID_START + idx - 1
                status = f"marker_probe_{idx:03d}"
                try:
                    with psycopg.connect(self._cfg.pg_dsn) as pg_conn:
                        commit_ns, commit_utc = commit_marker_row(
                            pg_conn,
                            marker_id=marker_id,
                            status=status,
                            note=f"steady-state probe {idx}",
                        )
                        pg_pos = capture_pg_pipeline_positions(pg_conn)

                    vis = wait_for_marker_visible(
                        self._cfg,
                        marker_id,
                        commit_ns,
                        timeout_s=45.0,
                        poll_interval_s=self._poll_interval_s,
                    )
                    entry = {
                        "probe_seq": idx,
                        "marker_id": marker_id,
                        "scheduled_offset_s": round(offset_s, 2),
                        "commit_offset_s": round(
                            (commit_ns - self._window_start_mono_ns) / 1_000_000_000.0, 3
                        ),
                        "commit_utc": commit_utc,
                        "visible": vis["visible"],
                        "visibility_ms": vis["visibility_ms"],
                        "observed_utc": vis["observed_utc"],
                        "snapshot_id": vis["snapshot_id"],
                        "polls": vis["polls"],
                        "duckdb_checks": vis["duckdb_checks"],
                        "poll_interval_s": self._poll_interval_s,
                        "reader_errors": vis["errors"],
                        "snapshot_info": vis["snapshot_info"],
                        "pg_positions_at_commit": {
                            "current_wal_lsn": pg_pos["current_wal_lsn"],
                            "replication_slots": pg_pos["replication_slots"],
                        },
                    }
                except Exception as exc:
                    entry = {
                        "probe_seq": idx,
                        "marker_id": marker_id,
                        "scheduled_offset_s": round(offset_s, 2),
                        "visible": False,
                        "visibility_ms": None,
                        "error": f"{type(exc).__name__}: {exc}",
                        "reader_errors": [f"{type(exc).__name__}: {exc}"],
                    }

                self._results.append(entry)
                fh.write(json.dumps(entry, default=json_default) + "\n")


def run_paced_workload(
    cfg: LabConfig,
    seed_res: SeedResult,
    *,
    duration_s: float = 180.0,
    rate_ops_s: float = 20.0,
    rng_seed: int = 20261119,
    ops_jsonl: Path,
    probes_jsonl: Path,
    probe_interval_s: float = 20.0,
    probe_poll_interval_s: float = 0.5,
    statement_timeout_ms: int = 15000,
) -> dict[str, Any]:
    """Execute a seeded update/insert/delete workload at a fixed offered rate."""
    rng = random.Random(rng_seed + 1000)
    toast_ids = list(seed_res.toast_ids)
    mutable_ids = list(seed_res.mutable_ids)
    toast_rev: dict[int, int] = {tid: 0 for tid in toast_ids}
    next_insert_id = INSERT_ID_START
    insert_counter = 0

    planned_ops = max(1, int(round(duration_s * rate_ops_s)))
    interval_ns = int(round(1_000_000_000 / rate_ops_s))
    # Arrival-schedule miss threshold: > max(250 ms, 2 * inter-arrival period).
    miss_threshold_ms = max(250.0, 2.0 * (1000.0 / rate_ops_s))

    op_counts: dict[str, int] = {
        "update_normal": 0,
        "update_toast_unchanged": 0,
        "update_toast_changed": 0,
        "insert": 0,
        "delete": 0,
    }
    affected_rows_by_op: dict[str, int] = {k: 0 for k in op_counts}
    sched_to_complete_ms_list: list[float] = []
    exec_latency_ms_list: list[float] = []
    queue_delay_ms_list: list[float] = []

    attempted = 0
    completed = 0
    errors = 0
    timeouts = 0
    missed_schedule_count = 0
    sample_errors: list[dict[str, Any]] = []
    toast_unchanged_ids_touched: set[int] = set()
    toast_changed_ids_touched: set[int] = set()

    ops_jsonl.parent.mkdir(parents=True, exist_ok=True)
    window_start_utc = utc_now_iso()
    window_start_ns = time.monotonic_ns()

    prober = SteadyStateLagProber(
        cfg,
        window_start_mono_ns=window_start_ns,
        duration_s=duration_s,
        probe_interval_s=probe_interval_s,
        poll_interval_s=probe_poll_interval_s,
        out_jsonl=probes_jsonl,
    )
    prober.start()

    try:
        with (
            psycopg.connect(cfg.pg_dsn) as conn,
            ops_jsonl.open("w", encoding="utf-8", buffering=1) as fh,
        ):
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {int(statement_timeout_ms)};")
            conn.commit()

            for seq in range(planned_ops):
                scheduled_ns = window_start_ns + seq * interval_ns
                while True:
                    now_ns = time.monotonic_ns()
                    rem_ns = scheduled_ns - now_ns
                    if rem_ns <= 0:
                        break
                    if rem_ns > 2_000_000:
                        time.sleep((rem_ns - 1_500_000) / 1_000_000_000.0)
                    else:
                        time.sleep(0.0005)

                start_ns = time.monotonic_ns()
                attempted += 1

                # Deterministic weighted operation choice.
                u = rng.random()
                if u < 0.40 and mutable_ids:
                    op_kind = "update_normal"
                elif u < 0.55 and toast_ids:
                    op_kind = "update_toast_unchanged"
                elif u < 0.70 and toast_ids:
                    op_kind = "update_toast_changed"
                elif u < 0.85 or len(mutable_ids) <= 10:
                    op_kind = "insert"
                else:
                    op_kind = "delete"

                op_counts[op_kind] += 1
                target_id = -1
                affected_rows = 0
                outcome = "ok"
                err_msg: str | None = None
                now_utc_dt = datetime.now(timezone.utc)

                try:
                    with conn.cursor() as cur:
                        if op_kind == "update_normal":
                            target_id = mutable_ids[rng.randrange(len(mutable_ids))]
                            new_status = STATUSES[rng.randrange(len(STATUSES))]
                            new_amount = _make_decimal_38_9(rng, extreme=(seq % 200 == 0))
                            r_note = rng.random()
                            if r_note < 0.1:
                                new_note: str | None = None
                            elif r_note < 0.2:
                                new_note = ""
                            else:
                                new_note = f"upd-{seq}-id-{target_id} "
                            new_payload = (
                                "null"
                                if rng.random() < 0.05
                                else json.dumps(
                                    {"seq": seq, "op": "upd", "arr": [seq % 5, target_id % 7]},
                                    separators=(",", ":"),
                                )
                            )
                            new_discount = None if rng.random() < 0.3 else _make_decimal_38_9(rng)
                            cur.execute(
                                """
                                UPDATE public.orders
                                SET status = %s,
                                    amount = %s,
                                    updated_at = %s,
                                    note = %s,
                                    payload = %s::jsonb,
                                    discount = %s
                                WHERE id = %s
                                """,
                                (
                                    new_status,
                                    new_amount,
                                    now_utc_dt,
                                    new_note,
                                    new_payload,
                                    new_discount,
                                    target_id,
                                ),
                            )
                            affected_rows = cur.rowcount

                        elif op_kind == "update_toast_unchanged":
                            # Deliberately leave out-of-line TOAST columns (note, payload) untouched.
                            target_id = toast_ids[rng.randrange(len(toast_ids))]
                            new_status = STATUSES[rng.randrange(len(STATUSES))]
                            new_amount = _make_decimal_38_9(rng)
                            new_discount = None if rng.random() < 0.3 else _make_decimal_38_9(rng)
                            cur.execute(
                                """
                                UPDATE public.orders
                                SET status = %s,
                                    amount = %s,
                                    updated_at = %s,
                                    discount = %s
                                WHERE id = %s
                                """,
                                (
                                    new_status,
                                    new_amount,
                                    now_utc_dt,
                                    new_discount,
                                    target_id,
                                ),
                            )
                            affected_rows = cur.rowcount
                            toast_unchanged_ids_touched.add(target_id)

                        elif op_kind == "update_toast_changed":
                            target_id = toast_ids[rng.randrange(len(toast_ids))]
                            toast_rev[target_id] = toast_rev.get(target_id, 0) + 1
                            rev = toast_rev[target_id]
                            new_status = STATUSES[rng.randrange(len(STATUSES))]
                            new_amount = _make_decimal_38_9(rng)
                            new_note_t = _make_toast_note(rng, target_id, rev=rev)
                            new_payload_t = _make_toast_payload_json(rng, target_id, rev=rev)
                            new_discount = None if rng.random() < 0.3 else _make_decimal_38_9(rng)
                            cur.execute(
                                """
                                UPDATE public.orders
                                SET status = %s,
                                    amount = %s,
                                    updated_at = %s,
                                    note = %s,
                                    payload = %s::jsonb,
                                    discount = %s
                                WHERE id = %s
                                """,
                                (
                                    new_status,
                                    new_amount,
                                    now_utc_dt,
                                    new_note_t,
                                    new_payload_t,
                                    new_discount,
                                    target_id,
                                ),
                            )
                            affected_rows = cur.rowcount
                            toast_changed_ids_touched.add(target_id)

                        elif op_kind == "insert":
                            target_id = next_insert_id
                            next_insert_id += 1
                            insert_counter += 1
                            new_status = STATUSES[rng.randrange(len(STATUSES))]
                            new_amount = _make_decimal_38_9(rng, extreme=(insert_counter % 50 == 0))
                            new_discount = None if rng.random() < 0.3 else _make_decimal_38_9(rng)
                            if insert_counter % 20 == 0:
                                ins_note: str | None = _make_toast_note(rng, target_id, rev=0)
                                ins_payload: str | None = _make_toast_payload_json(rng, target_id, rev=0)
                            else:
                                ins_note = f"ins-{target_id}"
                                ins_payload = json.dumps(
                                    {"seq": seq, "op": "ins", "id": target_id},
                                    separators=(",", ":"),
                                )
                            cur.execute(
                                """
                                INSERT INTO public.orders (id, status, amount, updated_at, note, payload, discount)
                                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
                                """,
                                (
                                    target_id,
                                    new_status,
                                    new_amount,
                                    now_utc_dt,
                                    ins_note,
                                    ins_payload,
                                    new_discount,
                                ),
                            )
                            affected_rows = cur.rowcount
                            mutable_ids.append(target_id)

                        elif op_kind == "delete":
                            idx = rng.randrange(len(mutable_ids))
                            # O(1) swap-pop
                            target_id = mutable_ids[idx]
                            mutable_ids[idx] = mutable_ids[-1]
                            mutable_ids.pop()
                            cur.execute("DELETE FROM public.orders WHERE id = %s", (target_id,))
                            affected_rows = cur.rowcount

                    conn.commit()
                    completed += 1
                    affected_rows_by_op[op_kind] += affected_rows
                except Exception as exc:
                    conn.rollback()
                    outcome = "timeout" if "statement timeout" in str(exc).lower() else "error"
                    err_msg = f"{type(exc).__name__}: {exc}"
                    errors += 1
                    if outcome == "timeout":
                        timeouts += 1
                    if len(sample_errors) < 20:
                        sample_errors.append(
                            {"seq": seq, "op": op_kind, "target_id": target_id, "error": err_msg}
                        )

                complete_ns = time.monotonic_ns()
                queue_delay_ms = (start_ns - scheduled_ns) / 1_000_000.0
                exec_latency_ms = (complete_ns - start_ns) / 1_000_000.0
                sched_to_complete_ms = (complete_ns - scheduled_ns) / 1_000_000.0

                if queue_delay_ms > miss_threshold_ms:
                    missed_schedule_count += 1

                if outcome == "ok":
                    queue_delay_ms_list.append(queue_delay_ms)
                    exec_latency_ms_list.append(exec_latency_ms)
                    sched_to_complete_ms_list.append(sched_to_complete_ms)

                fh.write(
                    json.dumps(
                        {
                            "seq": seq,
                            "op": op_kind,
                            "target_id": target_id,
                            "scheduled_offset_ms": round(
                                (scheduled_ns - window_start_ns) / 1_000_000.0, 3
                            ),
                            "start_offset_ms": round(
                                (start_ns - window_start_ns) / 1_000_000.0, 3
                            ),
                            "complete_offset_ms": round(
                                (complete_ns - window_start_ns) / 1_000_000.0, 3
                            ),
                            "queue_delay_ms": round(queue_delay_ms, 3),
                            "exec_latency_ms": round(exec_latency_ms, 3),
                            "sched_to_complete_ms": round(sched_to_complete_ms, 3),
                            "affected_rows": affected_rows,
                            "outcome": outcome,
                            "error": err_msg,
                        }
                    )
                    + "\n"
                )
    finally:
        probe_results = prober.stop_and_join(timeout_s=60.0)

    window_end_ns = time.monotonic_ns()
    window_end_utc = utc_now_iso()
    elapsed_s = (window_end_ns - window_start_ns) / 1_000_000_000.0

    visible_probe_lags = [
        float(p["visibility_ms"])
        for p in probe_results
        if p.get("visible") and p.get("visibility_ms") is not None
    ]
    probe_reader_errors = [
        err for p in probe_results for err in (p.get("reader_errors") or [])
    ]

    missed_schedule_pct = round((missed_schedule_count / attempted) * 100.0, 3) if attempted > 0 else 0.0
    schedule_valid = errors == 0 and attempted == planned_ops and missed_schedule_pct <= 1.0
    return {
        "rng_seed": rng_seed,
        "offered_rate_ops_s": rate_ops_s,
        "planned_duration_s": duration_s,
        "actual_duration_s": round(elapsed_s, 3),
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
        "planned_ops": planned_ops,
        "attempted_ops": attempted,
        "completed_ops": completed,
        "error_count": errors,
        "timeout_count": timeouts,
        "sample_errors": sample_errors,
        "achieved_throughput_ops_s": round(completed / elapsed_s, 3) if elapsed_s > 0 else 0.0,
        "op_counts": op_counts,
        "affected_rows_by_op": affected_rows_by_op,
        "total_affected_rows": sum(affected_rows_by_op.values()),
        "toast_exercise": {
            "unchanged_toast_updates": op_counts["update_toast_unchanged"],
            "changed_toast_updates": op_counts["update_toast_changed"],
            "distinct_toast_ids_unchanged_updated": len(toast_unchanged_ids_touched),
            "distinct_toast_ids_changed_updated": len(toast_changed_ids_touched),
        },
        "arrival_schedule": {
            "miss_threshold_ms": round(miss_threshold_ms, 2),
            "missed_schedule_count": missed_schedule_count,
            "missed_schedule_pct": missed_schedule_pct,
            "schedule_valid": schedule_valid,
        },
        "latency_sched_to_complete_ms": summarize_latency(sched_to_complete_ms_list),
        "latency_exec_ms": summarize_latency(exec_latency_ms_list),
        "latency_queue_delay_ms": summarize_latency(queue_delay_ms_list),
        "steady_state_probes": {
            "probe_interval_s": probe_interval_s,
            "poll_interval_s": probe_poll_interval_s,
            "probe_count": len(probe_results),
            "visible_count": len(visible_probe_lags),
            "reader_error_count": len(probe_reader_errors),
            "visibility_lag_ms": summarize_latency(visible_probe_lags),
            "probes": probe_results,
        },
    }
