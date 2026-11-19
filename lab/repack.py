"""PostgreSQL 19 concurrent REPACK runner, progress/lock/slot sampler, and matched-interval analysis."""

from __future__ import annotations

import json
from pathlib import Path
import threading
import time
from typing import Any

import psycopg

from lab.config import LabConfig
from lab.pipeline import json_default, utc_now_iso
from lab.workload import summarize_latency

REPACK_SQL = "REPACK (CONCURRENTLY) public.orders"
PLAIN_REPACK_SQL = "REPACK public.orders"


def capture_table_and_index_sizes(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    """Record public.orders heap, TOAST, index sizes, relfilenodes, and tuple stats."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                c.oid::bigint AS table_oid,
                c.relfilenode::bigint AS heap_relfilenode,
                c.reltoastrelid::bigint AS toast_oid,
                COALESCE(tc.relfilenode, 0)::bigint AS toast_relfilenode,
                pg_relation_size(c.oid) AS heap_bytes,
                pg_table_size(c.oid) AS table_and_toast_bytes,
                pg_indexes_size(c.oid) AS indexes_bytes,
                pg_total_relation_size(c.oid) AS total_relation_bytes,
                CASE
                    WHEN c.reltoastrelid <> 0 THEN pg_total_relation_size(c.reltoastrelid)
                    ELSE 0
                END AS toast_relation_bytes,
                c.relpages::bigint AS relpages,
                c.reltuples::double precision AS reltuples,
                c.relreplident::text AS replica_identity
            FROM pg_class c
            LEFT JOIN pg_class tc ON tc.oid = c.reltoastrelid
            WHERE c.oid = 'public.orders'::regclass
            """
        )
        trow = cur.fetchone()
        if trow is None:
            raise RuntimeError("public.orders not found in pg_class")

        cur.execute(
            """
            SELECT
                ic.relname AS index_name,
                ic.oid::bigint AS index_oid,
                ic.relfilenode::bigint AS index_relfilenode,
                pg_relation_size(ic.oid) AS size_bytes
            FROM pg_index i
            JOIN pg_class ic ON ic.oid = i.indexrelid
            WHERE i.indrelid = 'public.orders'::regclass
            ORDER BY ic.relname
            """
        )
        indexes = [
            {
                "index_name": r[0],
                "index_oid": int(r[1]),
                "index_relfilenode": int(r[2]),
                "size_bytes": int(r[3]),
            }
            for r in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT
                n_live_tup,
                n_dead_tup,
                n_tup_ins,
                n_tup_upd,
                n_tup_del,
                n_tup_hot_upd
            FROM pg_stat_user_tables
            WHERE schemaname = 'public' AND relname = 'orders'
            """
        )
        srow = cur.fetchone()
        stat_info = (
            {
                "n_live_tup": int(srow[0] or 0),
                "n_dead_tup": int(srow[1] or 0),
                "n_tup_ins": int(srow[2] or 0),
                "n_tup_upd": int(srow[3] or 0),
                "n_tup_del": int(srow[4] or 0),
                "n_tup_hot_upd": int(srow[5] or 0),
            }
            if srow
            else {}
        )

    return {
        "captured_at_utc": utc_now_iso(),
        "table_oid": int(trow[0]),
        "heap_relfilenode": int(trow[1]),
        "toast_oid": int(trow[2]),
        "toast_relfilenode": int(trow[3]),
        "heap_bytes": int(trow[4]),
        "table_and_toast_bytes": int(trow[5]),
        "indexes_bytes": int(trow[6]),
        "total_relation_bytes": int(trow[7]),
        "toast_relation_bytes": int(trow[8]),
        "relpages": int(trow[9]),
        "reltuples": float(trow[10]),
        "replica_identity": trow[11],
        "indexes": indexes,
        "pg_stat_user_tables": stat_info,
    }


def compute_size_deltas(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Compute exact byte deltas and relfilenode swap proof between before and after REPACK."""
    keys = (
        "heap_bytes",
        "toast_relation_bytes",
        "table_and_toast_bytes",
        "indexes_bytes",
        "total_relation_bytes",
    )
    byte_deltas = {
        k: int(after.get(k, 0)) - int(before.get(k, 0))
        for k in keys
    }

    before_idx = {i["index_name"]: i for i in before.get("indexes") or []}
    after_idx = {i["index_name"]: i for i in after.get("indexes") or []}
    index_deltas: dict[str, dict[str, Any]] = {}
    for name in sorted(set(before_idx) | set(after_idx)):
        b = before_idx.get(name, {})
        a = after_idx.get(name, {})
        index_deltas[name] = {
            "before_bytes": b.get("size_bytes"),
            "after_bytes": a.get("size_bytes"),
            "delta_bytes": (
                int(a["size_bytes"]) - int(b["size_bytes"])
                if "size_bytes" in a and "size_bytes" in b
                else None
            ),
            "before_relfilenode": b.get("index_relfilenode"),
            "after_relfilenode": a.get("index_relfilenode"),
            "relfilenode_changed": (
                b.get("index_relfilenode") != a.get("index_relfilenode")
                if "index_relfilenode" in a and "index_relfilenode" in b
                else None
            ),
        }

    return {
        "byte_deltas": byte_deltas,
        "heap_relfilenode_before": before.get("heap_relfilenode"),
        "heap_relfilenode_after": after.get("heap_relfilenode"),
        "heap_relfilenode_changed": before.get("heap_relfilenode") != after.get("heap_relfilenode"),
        "toast_relfilenode_before": before.get("toast_relfilenode"),
        "toast_relfilenode_after": after.get("toast_relfilenode"),
        "toast_relfilenode_changed": before.get("toast_relfilenode") != after.get("toast_relfilenode"),
        "indexes": index_deltas,
    }


def summarize_repack_samples(
    samples: list[dict[str, Any]],
    *,
    sample_interval_s: float,
    cdc_slot_name: str = "pg2iceberg_slot",
) -> dict[str, Any]:
    """Summarize raw pg_stat_progress_repack, pg_locks, and pg_replication_slots samples."""
    progress_sample_count = 0
    phases_order: list[str] = []
    phase_stats: dict[str, dict[str, Any]] = {}
    repack_locks_set: set[tuple[str | None, str | None, str, bool]] = set()
    waiting_locks_examples: list[dict[str, Any]] = []
    waiting_locks_total = 0

    final_lock_offsets: list[float] = []
    final_lock_triggers: set[str] = set()

    repack_slots: dict[str, dict[str, Any]] = {}
    cdc_slot_samples = 0
    cdc_slot_active_samples = 0
    cdc_confirmed_gaps: list[int] = []
    cdc_restart_gaps: list[int] = []

    for s in samples:
        offset_ms = float(s["offset_ms"])
        prog = s.get("progress")
        if prog is not None:
            progress_sample_count += 1
            phase = str(prog.get("phase") or "unknown")
            if phase not in phase_stats:
                phases_order.append(phase)
                phase_stats[phase] = {
                    "phase": phase,
                    "sample_count": 0,
                    "first_offset_ms": offset_ms,
                    "last_offset_ms": offset_ms,
                    "max_heap_blks_scanned": None,
                    "max_heap_blks_total": None,
                    "max_heap_tuples_scanned": None,
                    "max_heap_tuples_inserted": None,
                    "max_heap_tuples_updated": None,
                    "max_heap_tuples_deleted": None,
                    "max_index_rebuild_count": None,
                }
            ps = phase_stats[phase]
            ps["sample_count"] += 1
            ps["last_offset_ms"] = offset_ms
            for field_name, stat_key in (
                ("heap_blks_scanned", "max_heap_blks_scanned"),
                ("heap_blks_total", "max_heap_blks_total"),
                ("heap_tuples_scanned", "max_heap_tuples_scanned"),
                ("heap_tuples_inserted", "max_heap_tuples_inserted"),
                ("heap_tuples_updated", "max_heap_tuples_updated"),
                ("heap_tuples_deleted", "max_heap_tuples_deleted"),
                ("index_rebuild_count", "max_index_rebuild_count"),
            ):
                val = prog.get(field_name)
                if val is not None:
                    ival = int(val)
                    cur_max = ps[stat_key]
                    if cur_max is None or ival > cur_max:
                        ps[stat_key] = ival

            if phase == "swapping relation files":
                final_lock_offsets.append(offset_ms)
                final_lock_triggers.add("phase:swapping relation files")

        for lk in s.get("repack_locks") or []:
            rel = lk.get("relation_name")
            ltype = lk.get("locktype")
            mode = str(lk.get("mode") or "")
            granted = bool(lk.get("granted"))
            repack_locks_set.add((ltype, rel, mode, granted))
            if rel == "orders" and mode == "AccessExclusiveLock":
                final_lock_offsets.append(offset_ms)
                final_lock_triggers.add(f"repack_lock:orders:AccessExclusiveLock:granted={granted}")

        waiters = s.get("waiting_locks_on_orders") or []
        if waiters:
            waiting_locks_total += len(waiters)
            final_lock_offsets.append(offset_ms)
            final_lock_triggers.add("blocked_waiter_on_orders")
            for w in waiters:
                if len(waiting_locks_examples) < 20:
                    waiting_locks_examples.append({"offset_ms": offset_ms, **w})

        for slot in s.get("replication_slots") or []:
            sname = str(slot.get("slot_name") or "")
            plugin = str(slot.get("plugin") or "")
            if sname == cdc_slot_name:
                cdc_slot_samples += 1
                if slot.get("active"):
                    cdc_slot_active_samples += 1
                if slot.get("wal_gap_bytes_confirmed_flush") is not None:
                    cdc_confirmed_gaps.append(int(slot["wal_gap_bytes_confirmed_flush"]))
                if slot.get("wal_gap_bytes_restart") is not None:
                    cdc_restart_gaps.append(int(slot["wal_gap_bytes_restart"]))
            elif plugin == "pgrepack" or sname.startswith("pg_repack_"):
                if sname not in repack_slots:
                    repack_slots[sname] = {
                        "slot_name": sname,
                        "plugin": plugin,
                        "slot_type": slot.get("slot_type"),
                        "temporary": bool(slot.get("temporary")),
                        "sample_count": 0,
                        "active_sample_count": 0,
                        "first_offset_ms": offset_ms,
                        "last_offset_ms": offset_ms,
                        "max_wal_gap_bytes_confirmed_flush": None,
                        "max_wal_gap_bytes_restart": None,
                    }
                rs = repack_slots[sname]
                rs["sample_count"] += 1
                if slot.get("active"):
                    rs["active_sample_count"] += 1
                rs["last_offset_ms"] = offset_ms
                for gk, sk in (
                    ("wal_gap_bytes_confirmed_flush", "max_wal_gap_bytes_confirmed_flush"),
                    ("wal_gap_bytes_restart", "max_wal_gap_bytes_restart"),
                ):
                    gv = slot.get(gk)
                    if gv is not None:
                        igv = int(gv)
                        if rs[sk] is None or igv > rs[sk]:
                            rs[sk] = igv

    phases_observed = []
    for p in phases_order:
        st = dict(phase_stats[p])
        st["observed_span_ms"] = round(st["last_offset_ms"] - st["first_offset_ms"], 3)
        phases_observed.append(st)

    repack_locks_observed = [
        {"locktype": lt, "relation_name": rn, "mode": md, "granted": gr}
        for lt, rn, md, gr in sorted(
            repack_locks_set,
            key=lambda x: (str(x[1] or ""), str(x[0] or ""), x[2], x[3]),
        )
    ]

    final_lock_observed = len(final_lock_offsets) > 0
    final_lock_evidence: dict[str, Any] | None = None
    if final_lock_observed:
        f_start = min(final_lock_offsets)
        f_end = max(final_lock_offsets)
        final_lock_evidence = {
            "triggers": sorted(final_lock_triggers),
            "sample_hits": len(final_lock_offsets),
            "first_offset_ms": round(f_start, 3),
            "last_offset_ms": round(f_end, 3),
            "observed_span_ms": round(f_end - f_start, 3),
        }

    return {
        "sample_interval_s": sample_interval_s,
        "sample_count": len(samples),
        "progress_sample_count": progress_sample_count,
        "phases_observed": phases_observed,
        "repack_locks_observed": repack_locks_observed,
        "waiting_locks_observed_count": waiting_locks_total,
        "waiting_locks_examples": waiting_locks_examples,
        "final_lock_observed": final_lock_observed,
        "final_lock_evidence": final_lock_evidence,
        "repack_slots_observed": [
            {
                **v,
                "observed_span_ms": round(v["last_offset_ms"] - v["first_offset_ms"], 3),
            }
            for v in repack_slots.values()
        ],
        "cdc_slot_activity": {
            "slot_name": cdc_slot_name,
            "sample_count": cdc_slot_samples,
            "active_sample_count": cdc_slot_active_samples,
            "min_wal_gap_bytes_confirmed_flush": min(cdc_confirmed_gaps) if cdc_confirmed_gaps else None,
            "max_wal_gap_bytes_confirmed_flush": max(cdc_confirmed_gaps) if cdc_confirmed_gaps else None,
            "min_wal_gap_bytes_restart": min(cdc_restart_gaps) if cdc_restart_gaps else None,
            "max_wal_gap_bytes_restart": max(cdc_restart_gaps) if cdc_restart_gaps else None,
        },
    }


class ConcurrentRepackRunner:
    """Runs REPACK (CONCURRENTLY) public.orders in its own autocommit session with live sampling."""

    def __init__(
        self,
        cfg: LabConfig,
        *,
        repack_delay_s: float = 30.0,
        sample_interval_s: float = 0.05,
        samples_jsonl: Path,
        command: str = REPACK_SQL,
    ) -> None:
        self._cfg = cfg
        self._repack_delay_s = repack_delay_s
        self._sample_interval_s = sample_interval_s
        self._samples_jsonl = samples_jsonl
        self._command = command
        self._window_start_ns: int | None = None
        self._window_start_utc: str | None = None
        self._cancel_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._result: dict[str, Any] | None = None

    def start_anchored_to_window(self, window_start_ns: int, window_start_utc: str) -> None:
        """Start the background REPACK thread anchored to the workload window monotonic clock."""
        self._window_start_ns = window_start_ns
        self._window_start_utc = window_start_utc
        self._thread = threading.Thread(
            target=self._run,
            name="concurrent-repack-runner",
            daemon=True,
        )
        self._thread.start()

    def wait_for_completion(self, timeout_s: float = 600.0) -> dict[str, Any]:
        """Block until REPACK has finished (and its sampler has flushed) and return the summary."""
        if self._thread is None:
            raise RuntimeError("ConcurrentRepackRunner was never started")
        self._thread.join(timeout=timeout_s)
        if self._thread.is_alive():
            raise TimeoutError(f"REPACK thread did not finish within {timeout_s}s")
        if self._result is None:
            raise RuntimeError("REPACK thread exited without producing a result")
        return self._result

    def _poll_once(
        self,
        monitor_conn: psycopg.Connection[Any],
        repack_pid: int,
        sample_seq: int,
        repack_start_ns: int,
    ) -> dict[str, Any]:
        assert self._window_start_ns is not None
        now_ns = time.monotonic_ns()
        offset_ms = round((now_ns - self._window_start_ns) / 1_000_000.0, 3)
        repack_elapsed_ms = round((now_ns - repack_start_ns) / 1_000_000.0, 3)

        progress_row: dict[str, Any] | None = None
        repack_locks: list[dict[str, Any]] = []
        waiting_locks: list[dict[str, Any]] = []
        slots: list[dict[str, Any]] = []
        current_wal_lsn: str | None = None
        sample_error: str | None = None

        try:
            with monitor_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        pid,
                        command,
                        phase,
                        repack_index_relid::bigint,
                        heap_tuples_scanned,
                        heap_tuples_inserted,
                        heap_tuples_updated,
                        heap_tuples_deleted,
                        heap_blks_total,
                        heap_blks_scanned,
                        index_rebuild_count
                    FROM pg_stat_progress_repack
                    WHERE pid = %s
                    """,
                    (repack_pid,),
                )
                pr = cur.fetchone()
                if pr is not None:
                    progress_row = {
                        "pid": int(pr[0]),
                        "command": pr[1],
                        "phase": pr[2],
                        "repack_index_relid": int(pr[3]) if pr[3] is not None else None,
                        "heap_tuples_scanned": int(pr[4]) if pr[4] is not None else None,
                        "heap_tuples_inserted": int(pr[5]) if pr[5] is not None else None,
                        "heap_tuples_updated": int(pr[6]) if pr[6] is not None else None,
                        "heap_tuples_deleted": int(pr[7]) if pr[7] is not None else None,
                        "heap_blks_total": int(pr[8]) if pr[8] is not None else None,
                        "heap_blks_scanned": int(pr[9]) if pr[9] is not None else None,
                        "index_rebuild_count": int(pr[10]) if pr[10] is not None else None,
                    }

                cur.execute(
                    """
                    SELECT
                        l.locktype,
                        c.relname AS relation_name,
                        l.mode,
                        l.granted,
                        l.fastpath
                    FROM pg_locks l
                    LEFT JOIN pg_class c ON c.oid = l.relation
                    WHERE l.pid = %s
                      AND (
                        l.relation IS NOT NULL
                        OR NOT l.granted
                      )
                    ORDER BY c.relname NULLS LAST, l.mode
                    """,
                    (repack_pid,),
                )
                repack_locks = [
                    {
                        "locktype": r[0],
                        "relation_name": r[1],
                        "mode": r[2],
                        "granted": bool(r[3]),
                        "fastpath": bool(r[4]),
                    }
                    for r in cur.fetchall()
                ]

                cur.execute(
                    """
                    SELECT
                        l.pid,
                        a.state,
                        left(a.query, 120) AS query_prefix,
                        l.locktype,
                        c.relname AS relation_name,
                        l.mode,
                        l.granted,
                        pg_blocking_pids(l.pid) AS blocking_pids
                    FROM pg_locks l
                    LEFT JOIN pg_class c ON c.oid = l.relation
                    LEFT JOIN pg_stat_activity a ON a.pid = l.pid
                    WHERE NOT l.granted
                      AND (
                        l.relation = 'public.orders'::regclass
                        OR %s = ANY(pg_blocking_pids(l.pid))
                      )
                    ORDER BY l.pid
                    """,
                    (repack_pid,),
                )
                waiting_locks = [
                    {
                        "pid": int(r[0]),
                        "state": r[1],
                        "query_prefix": r[2],
                        "locktype": r[3],
                        "relation_name": r[4],
                        "mode": r[5],
                        "granted": bool(r[6]),
                        "blocking_pids": [int(x) for x in (r[7] or [])],
                    }
                    for r in cur.fetchall()
                ]

                cur.execute("SELECT pg_current_wal_lsn()::text")
                current_wal_lsn = cur.fetchone()[0]

                cur.execute(
                    """
                    SELECT
                        slot_name,
                        plugin,
                        slot_type,
                        temporary,
                        active,
                        active_pid,
                        restart_lsn::text,
                        confirmed_flush_lsn::text,
                        CASE
                            WHEN confirmed_flush_lsn IS NOT NULL
                            THEN pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint
                            ELSE NULL
                        END AS wal_gap_bytes_confirmed_flush,
                        CASE
                            WHEN restart_lsn IS NOT NULL
                            THEN pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
                            ELSE NULL
                        END AS wal_gap_bytes_restart
                    FROM pg_replication_slots
                    ORDER BY slot_name
                    """
                )
                slots = [
                    {
                        "slot_name": r[0],
                        "plugin": r[1],
                        "slot_type": r[2],
                        "temporary": bool(r[3]),
                        "active": bool(r[4]),
                        "active_pid": int(r[5]) if r[5] is not None else None,
                        "restart_lsn": r[6],
                        "confirmed_flush_lsn": r[7],
                        "wal_gap_bytes_confirmed_flush": int(r[8]) if r[8] is not None else None,
                        "wal_gap_bytes_restart": int(r[9]) if r[9] is not None else None,
                    }
                    for r in cur.fetchall()
                ]
        except Exception as exc:
            sample_error = f"{type(exc).__name__}: {exc}"

        return {
            "sample_seq": sample_seq,
            "captured_at_utc": utc_now_iso(),
            "offset_ms": offset_ms,
            "repack_elapsed_ms": repack_elapsed_ms,
            "current_wal_lsn": current_wal_lsn,
            "progress": progress_row,
            "repack_locks": repack_locks,
            "waiting_locks_on_orders": waiting_locks,
            "replication_slots": slots,
            "sample_error": sample_error,
        }

    def _run(self) -> None:
        assert self._window_start_ns is not None
        target_start_ns = self._window_start_ns + int(self._repack_delay_s * 1_000_000_000)
        while not self._cancel_event.is_set():
            rem_s = (target_start_ns - time.monotonic_ns()) / 1_000_000_000.0
            if rem_s <= 0:
                break
            self._cancel_event.wait(timeout=min(0.2, rem_s))

        samples: list[dict[str, Any]] = []
        stop_sampling = threading.Event()
        repack_pid: int | None = None
        sizes_before: dict[str, Any] = {}
        sizes_after: dict[str, Any] = {}
        start_ns = 0
        end_ns = 0
        start_utc = ""
        end_utc = ""
        outcome = "failed"
        err_msg: str | None = None

        self._samples_jsonl.parent.mkdir(parents=True, exist_ok=True)
        try:
            with (
                psycopg.connect(self._cfg.pg_dsn, autocommit=True) as repack_conn,
                psycopg.connect(self._cfg.pg_dsn, autocommit=True) as monitor_conn,
                self._samples_jsonl.open("w", encoding="utf-8", buffering=1) as fh,
            ):
                repack_pid = int(repack_conn.execute("SELECT pg_backend_pid()").fetchone()[0])
                sizes_before = capture_table_and_index_sizes(monitor_conn)

                start_ns = time.monotonic_ns()
                start_utc = utc_now_iso()

                def _sampler_loop() -> None:
                    seq = 0
                    while not stop_sampling.is_set():
                        seq += 1
                        sample = self._poll_once(monitor_conn, repack_pid, seq, start_ns)
                        samples.append(sample)
                        fh.write(json.dumps(sample, default=json_default) + "\n")
                        stop_sampling.wait(timeout=self._sample_interval_s)

                sampler_thread = threading.Thread(
                    target=_sampler_loop,
                    name="repack-progress-sampler",
                    daemon=True,
                )
                sampler_thread.start()

                try:
                    repack_conn.execute(self._command)
                    outcome = "ok"
                except Exception as exc:
                    outcome = "failed"
                    err_msg = f"{type(exc).__name__}: {exc}"
                finally:
                    end_ns = time.monotonic_ns()
                    end_utc = utc_now_iso()
                    stop_sampling.set()
                    sampler_thread.join(timeout=15.0)
                    # Take one final post-REPACK sample to confirm slot cleanup and zero progress row.
                    final_sample = self._poll_once(
                        monitor_conn, repack_pid, len(samples) + 1, start_ns
                    )
                    final_sample["post_repack_check"] = True
                    samples.append(final_sample)
                    fh.write(json.dumps(final_sample, default=json_default) + "\n")
                    sizes_after = capture_table_and_index_sizes(monitor_conn)
        except Exception as exc:
            if not start_ns:
                start_ns = time.monotonic_ns()
                start_utc = utc_now_iso()
            if not end_ns:
                end_ns = time.monotonic_ns()
                end_utc = utc_now_iso()
            outcome = "failed"
            err_msg = f"{type(exc).__name__}: {exc}"

        start_offset_ms = round((start_ns - self._window_start_ns) / 1_000_000.0, 3)
        end_offset_ms = round((end_ns - self._window_start_ns) / 1_000_000.0, 3)
        duration_ms = round((end_ns - start_ns) / 1_000_000.0, 3)

        # Exclude the post-REPACK check sample from active-REPACK phase/slot summary.
        active_samples = [s for s in samples if not s.get("post_repack_check")]
        sampling_summary = summarize_repack_samples(
            active_samples,
            sample_interval_s=self._sample_interval_s,
        )
        size_deltas = (
            compute_size_deltas(sizes_before, sizes_after)
            if sizes_before and sizes_after
            else {}
        )

        self._result = {
            "command": self._command,
            "repack_pid": repack_pid,
            "scheduled_delay_s": self._repack_delay_s,
            "start_utc": start_utc,
            "end_utc": end_utc,
            "start_offset_ms": start_offset_ms,
            "end_offset_ms": end_offset_ms,
            "duration_ms": duration_ms,
            "outcome": outcome,
            "error": err_msg,
            "sizes_before": sizes_before,
            "sizes_after": sizes_after,
            "size_deltas": size_deltas,
            "sampling": sampling_summary,
            "post_repack_slots": (
                samples[-1].get("replication_slots", [])
                if samples and samples[-1].get("post_repack_check")
                else []
            ),
        }


def load_ops_jsonl(path: Path) -> list[dict[str, Any]]:
    """Load per-operation raw log rows from ops.jsonl."""
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            s = line.strip()
            if not s:
                continue
            rows.append(json.loads(s))
    return rows


def summarize_ops_slice(
    ops: list[dict[str, Any]],
    *,
    duration_ms: float | None = None,
    miss_threshold_ms: float = 250.0,
) -> dict[str, Any]:
    """Summarize attempted, completed, error, timeout, affected rows, throughput, and latencies."""
    attempted = len(ops)
    completed = 0
    errors = 0
    timeouts = 0
    total_affected = 0
    missed_schedule = 0
    harness_missed = 0
    op_counts: dict[str, int] = {}
    sched_lat: list[float] = []
    exec_lat: list[float] = []
    queue_lat: list[float] = []
    backpressure_lat: list[float] = []

    for row in ops:
        op_kind = str(row.get("op", "unknown"))
        op_counts[op_kind] = op_counts.get(op_kind, 0) + 1
        outcome = row.get("outcome")
        qd = float(row.get("queue_delay_ms", 0.0))
        if qd > miss_threshold_ms:
            missed_schedule += 1
        if "harness_dispatch_lag_ms" in row and float(row["harness_dispatch_lag_ms"]) > miss_threshold_ms:
            harness_missed += 1

        if outcome == "ok":
            completed += 1
            total_affected += int(row.get("affected_rows") or 0)
            sched_lat.append(float(row["sched_to_complete_ms"]))
            exec_lat.append(float(row["exec_latency_ms"]))
            queue_lat.append(qd)
            if "backpressure_delay_ms" in row:
                backpressure_lat.append(float(row["backpressure_delay_ms"]))
        else:
            errors += 1
            if outcome == "timeout":
                timeouts += 1

    throughput_ops_s: float | None = None
    if duration_ms is not None and duration_ms > 0:
        throughput_ops_s = round(completed / (duration_ms / 1000.0), 3)

    return {
        "duration_ms": round(duration_ms, 3) if duration_ms is not None else None,
        "attempted_ops": attempted,
        "completed_ops": completed,
        "error_count": errors,
        "timeout_count": timeouts,
        "total_affected_rows": total_affected,
        "op_counts": op_counts,
        "completed_throughput_ops_s": throughput_ops_s,
        "missed_schedule_count": missed_schedule,
        "harness_missed_arrival_count": harness_missed,
        "latency_sched_to_complete_ms": summarize_latency(sched_lat),
        "latency_exec_ms": summarize_latency(exec_lat),
        "latency_queue_delay_ms": summarize_latency(queue_lat),
        "latency_backpressure_delay_ms": summarize_latency(backpressure_lat),
    }


def analyze_ops_intervals(
    ops: list[dict[str, Any]],
    *,
    interval_start_ms: float,
    interval_end_ms: float,
    total_window_duration_ms: float,
    miss_threshold_ms: float = 250.0,
) -> dict[str, Any]:
    """Slice ops.jsonl into full window, pre-interval, matched interval, and post-interval."""
    interval_duration_ms = max(0.0, interval_end_ms - interval_start_ms)

    pre_ops = [o for o in ops if float(o["scheduled_offset_ms"]) < interval_start_ms]
    sched_in = [
        o for o in ops
        if interval_start_ms <= float(o["scheduled_offset_ms"]) <= interval_end_ms
    ]
    committed_in = [
        o for o in ops
        if o.get("outcome") == "ok"
        and interval_start_ms <= float(o["complete_offset_ms"]) <= interval_end_ms
    ]
    strictly_contained = [
        o for o in ops
        if float(o["start_offset_ms"]) >= interval_start_ms
        and float(o["complete_offset_ms"]) <= interval_end_ms
    ]
    any_overlap = [
        o for o in ops
        if float(o["complete_offset_ms"]) >= interval_start_ms
        and float(o["start_offset_ms"]) <= interval_end_ms
    ]
    post_ops = [o for o in ops if float(o["scheduled_offset_ms"]) > interval_end_ms]

    return {
        "interval_start_ms": round(interval_start_ms, 3),
        "interval_end_ms": round(interval_end_ms, 3),
        "interval_duration_ms": round(interval_duration_ms, 3),
        "full_window": summarize_ops_slice(
            ops,
            duration_ms=total_window_duration_ms,
            miss_threshold_ms=miss_threshold_ms,
        ),
        "pre_interval": summarize_ops_slice(
            pre_ops,
            duration_ms=max(0.0, interval_start_ms),
            miss_threshold_ms=miss_threshold_ms,
        ),
        "in_interval_scheduled": summarize_ops_slice(
            sched_in,
            duration_ms=interval_duration_ms,
            miss_threshold_ms=miss_threshold_ms,
        ),
        "in_interval_committed": summarize_ops_slice(
            committed_in,
            duration_ms=interval_duration_ms,
            miss_threshold_ms=miss_threshold_ms,
        ),
        "in_interval_strictly_contained": summarize_ops_slice(
            strictly_contained,
            duration_ms=interval_duration_ms,
            miss_threshold_ms=miss_threshold_ms,
        ),
        "in_interval_any_overlap": summarize_ops_slice(
            any_overlap,
            duration_ms=interval_duration_ms,
            miss_threshold_ms=miss_threshold_ms,
        ),
        "post_interval": summarize_ops_slice(
            post_ops,
            duration_ms=max(0.0, total_window_duration_ms - interval_end_ms),
            miss_threshold_ms=miss_threshold_ms,
        ),
    }


def _extract_last_row_comparison(summary: dict[str, Any]) -> dict[str, Any]:
    attempts = (summary.get("convergence") or {}).get("snapshot_attempts") or []
    if not attempts:
        return {}
    return attempts[-1].get("row_comparison") or {}


def _extract_last_schema_check(summary: dict[str, Any]) -> dict[str, Any]:
    attempts = (summary.get("convergence") or {}).get("snapshot_attempts") or []
    if not attempts:
        return {}
    return attempts[-1].get("schema_check") or {}


def _extract_first_source_read_opened_utc(summary: dict[str, Any]) -> str | None:
    attempts = (summary.get("convergence") or {}).get("snapshot_attempts") or []
    if not attempts:
        return None
    return attempts[0].get("source_read_opened_utc")


def build_pair_comparison(
    baseline_summary: dict[str, Any],
    repack_summary: dict[str, Any],
    *,
    baseline_ops: list[dict[str, Any]],
    repack_ops: list[dict[str, Any]],
    baseline_run_dir: str,
    repack_run_dir: str,
) -> dict[str, Any]:
    """Build a structured matched-pair comparison from raw baseline and REPACK evidence."""
    repack_info = repack_summary.get("repack") or {}
    r_start_ms = float(repack_info.get("start_offset_ms") or 0.0)
    r_end_ms = float(repack_info.get("end_offset_ms") or 0.0)

    b_miss_thresh = float(
        (baseline_summary.get("workload") or {})
        .get("arrival_schedule", {})
        .get("miss_threshold_ms", 250.0)
    )
    r_miss_thresh = float(
        (repack_summary.get("workload") or {})
        .get("arrival_schedule", {})
        .get("miss_threshold_ms", 250.0)
    )
    b_total_ms = float((baseline_summary.get("workload") or {}).get("actual_duration_s", 0.0)) * 1000.0
    r_total_ms = float((repack_summary.get("workload") or {}).get("actual_duration_s", 0.0)) * 1000.0

    baseline_intervals = analyze_ops_intervals(
        baseline_ops,
        interval_start_ms=r_start_ms,
        interval_end_ms=r_end_ms,
        total_window_duration_ms=b_total_ms,
        miss_threshold_ms=b_miss_thresh,
    )
    repack_intervals = analyze_ops_intervals(
        repack_ops,
        interval_start_ms=r_start_ms,
        interval_end_ms=r_end_ms,
        total_window_duration_ms=r_total_ms,
        miss_threshold_ms=r_miss_thresh,
    )

    sampling = repack_info.get("sampling") or {}
    final_lock_observed = bool(sampling.get("final_lock_observed"))
    final_lock_ops: dict[str, Any] | None = None
    if final_lock_observed and sampling.get("final_lock_evidence"):
        fl = sampling["final_lock_evidence"]
        fl_start = float(fl["first_offset_ms"])
        fl_end = float(fl["last_offset_ms"])
        final_lock_ops = {
            "baseline": analyze_ops_intervals(
                baseline_ops,
                interval_start_ms=fl_start,
                interval_end_ms=fl_end,
                total_window_duration_ms=b_total_ms,
                miss_threshold_ms=b_miss_thresh,
            )["in_interval_any_overlap"],
            "repack": analyze_ops_intervals(
                repack_ops,
                interval_start_ms=fl_start,
                interval_end_ms=fl_end,
                total_window_duration_ms=r_total_ms,
                miss_threshold_ms=r_miss_thresh,
            )["in_interval_any_overlap"],
            "evidence": fl,
        }

    committed_during_repack = repack_intervals["in_interval_committed"]["completed_ops"]
    overlap_confirmed = (
        repack_info.get("outcome") == "ok"
        and float(repack_info.get("duration_ms") or 0.0) > 0.0
        and committed_during_repack > 0
    )

    repack_end_utc = repack_info.get("end_utc")
    repack_final_src_read_utc = _extract_first_source_read_opened_utc(repack_summary)
    source_read_after_repack = bool(
        repack_end_utc
        and repack_final_src_read_utc
        and repack_final_src_read_utc > repack_end_utc
    )

    # Check matched parameters across the pair.
    param_keys = (
        "seed_rows",
        "toast_rows",
        "duration_s",
        "rate_ops_s",
        "rng_seed",
        "batch_size",
        "probe_interval_s",
        "probe_poll_interval_s",
    )
    b_params = baseline_summary.get("parameters") or {}
    r_params = repack_summary.get("parameters") or {}
    params_matched = all(b_params.get(k) == r_params.get(k) for k in param_keys)

    return {
        "generated_at_utc": utc_now_iso(),
        "baseline_run_dir": baseline_run_dir,
        "repack_run_dir": repack_run_dir,
        "parameters_matched": params_matched,
        "parameters": {k: b_params.get(k) for k in param_keys},
        "overlap_confirmed": overlap_confirmed,
        "committed_during_repack_count": committed_during_repack,
        "source_read_opened_after_repack": source_read_after_repack,
        "repack_end_utc": repack_end_utc,
        "repack_final_source_read_opened_utc": repack_final_src_read_utc,
        "cdc_convergence": {
            "baseline": {
                "outcome": baseline_summary.get("outcome"),
                "passed": baseline_summary.get("passed"),
                "schema_check": _extract_last_schema_check(baseline_summary),
                "row_comparison": _extract_last_row_comparison(baseline_summary),
                "final_marker_visibility_ms": (baseline_summary.get("convergence") or {}).get(
                    "final_marker_visibility_ms"
                ),
                "final_marker_to_verified_match_ms": (baseline_summary.get("convergence") or {}).get(
                    "final_marker_to_verified_match_ms"
                ),
                "diff_duration_ms": (baseline_summary.get("convergence") or {}).get(
                    "diff_duration_ms"
                ),
                "verified_snapshot_id": (baseline_summary.get("convergence") or {}).get(
                    "verified_snapshot_id"
                ),
                "duckdb_final_metadata_summary": baseline_summary.get("duckdb_final_metadata_summary"),
                "toast_after_seed": (baseline_summary.get("seed") or {}).get("toast_verification"),
                "toast_after_workload": baseline_summary.get("toast_after_workload"),
            },
            "repack": {
                "outcome": repack_summary.get("outcome"),
                "passed": repack_summary.get("passed"),
                "schema_check": _extract_last_schema_check(repack_summary),
                "row_comparison": _extract_last_row_comparison(repack_summary),
                "final_marker_visibility_ms": (repack_summary.get("convergence") or {}).get(
                    "final_marker_visibility_ms"
                ),
                "final_marker_to_verified_match_ms": (repack_summary.get("convergence") or {}).get(
                    "final_marker_to_verified_match_ms"
                ),
                "diff_duration_ms": (repack_summary.get("convergence") or {}).get(
                    "diff_duration_ms"
                ),
                "verified_snapshot_id": (repack_summary.get("convergence") or {}).get(
                    "verified_snapshot_id"
                ),
                "duckdb_final_metadata_summary": repack_summary.get("duckdb_final_metadata_summary"),
                "toast_after_seed": (repack_summary.get("seed") or {}).get("toast_verification"),
                "toast_after_workload": repack_summary.get("toast_after_workload"),
            },
        },
        "repack_execution": repack_info,
        "host_performance": {
            "baseline_intervals": baseline_intervals,
            "repack_intervals": repack_intervals,
            "baseline_steady_state_probes": (
                (baseline_summary.get("workload") or {}).get("steady_state_probes") or {}
            ),
            "repack_steady_state_probes": (
                (repack_summary.get("workload") or {}).get("steady_state_probes") or {}
            ),
            "final_lock_observed": final_lock_observed,
            "final_lock_ops": final_lock_ops,
        },
    }


def _fmt_lat(lat: dict[str, Any] | None) -> str:
    if not lat or not lat.get("count"):
        return "n/a (n=0)"
    return (
        f"p50={lat['p50_ms']} ms, p99={lat['p99_ms']} ms, "
        f"max={lat['max_ms']} ms (n={lat['count']})"
    )


def _fmt_opt_ms(val: Any) -> str:
    if val is None:
        return "n/a"
    return f"{val} ms"


def render_pair_results_markdown(pair: dict[str, Any]) -> str:
    """Render a reproducible Markdown results report separating CDC convergence from host perf."""
    conv_b = pair["cdc_convergence"]["baseline"]
    conv_r = pair["cdc_convergence"]["repack"]
    rc_b = conv_b.get("row_comparison") or {}
    rc_r = conv_r.get("row_comparison") or {}
    sc_b = conv_b.get("schema_check") or {}
    sc_r = conv_r.get("schema_check") or {}

    perf = pair["host_performance"]
    b_full = perf["baseline_intervals"]["full_window"]
    r_full = perf["repack_intervals"]["full_window"]
    b_int = perf["baseline_intervals"]["in_interval_scheduled"]
    r_int = perf["repack_intervals"]["in_interval_scheduled"]
    r_comm = perf["repack_intervals"]["in_interval_committed"]
    b_probes = perf["baseline_steady_state_probes"]
    r_probes = perf["repack_steady_state_probes"]

    repack = pair["repack_execution"]
    sizes_b = repack.get("sizes_before") or {}
    sizes_a = repack.get("sizes_after") or {}
    deltas = repack.get("size_deltas") or {}
    sampling = repack.get("sampling") or {}

    lines = [
        "# Matched Pair Results (Baseline vs Concurrent REPACK)",
        "",
        f"- Baseline evidence: `{pair['baseline_run_dir']}`",
        f"- REPACK evidence: `{pair['repack_run_dir']}`",
        f"- Matched parameters: `{json.dumps(pair['parameters'])}` (`parameters_matched={pair['parameters_matched']}`)",
        f"- Overlap confirmed: `{pair['overlap_confirmed']}` (`{pair['committed_during_repack_count']}` writes committed during REPACK)",
        f"- Final source read opened strictly after REPACK finish: `{pair['source_read_opened_after_repack']}` (`repack_end_utc={pair['repack_end_utc']}`, `source_read_opened_utc={pair['repack_final_source_read_opened_utc']}`)",
        "",
        "## 1. CDC Convergence Evidence",
        "",
        "| Check | Baseline | Concurrent REPACK |",
        "| --- | --- | --- |",
        f"| Run outcome | `{conv_b.get('outcome')}` | `{conv_r.get('outcome')}` |",
        f"| Verified snapshot ID | `{conv_b.get('verified_snapshot_id')}` | `{conv_r.get('verified_snapshot_id')}` |",
        f"| Schema check (`schema_id`) | `passed={sc_b.get('passed')}` (`schema_id={sc_b.get('schema_id')}`) | `passed={sc_r.get('passed')}` (`schema_id={sc_r.get('schema_id')}`) |",
        f"| Source / Lake rows | `{rc_b.get('source_row_count')}` / `{rc_b.get('lake_row_count')}` | `{rc_r.get('source_row_count')}` / `{rc_r.get('lake_row_count')}` |",
        f"| Source-only / Lake-only / Duplicate lake IDs | `{rc_b.get('source_only_count')}` / `{rc_b.get('lake_only_count')}` / `{rc_b.get('duplicate_lake_id_count')}` | `{rc_r.get('source_only_count')}` / `{rc_r.get('lake_only_count')}` / `{rc_r.get('duplicate_lake_id_count')}` |",
        f"| Value mismatch rows | `{rc_b.get('value_mismatch_row_count')}` | `{rc_r.get('value_mismatch_row_count')}` |",
        f"| Final marker commit-to-visibility | `{_fmt_opt_ms(conv_b.get('final_marker_visibility_ms'))}` | `{_fmt_opt_ms(conv_r.get('final_marker_visibility_ms'))}` |",
        f"| Final marker commit to verified match | `{_fmt_opt_ms(conv_b.get('final_marker_to_verified_match_ms'))}` | `{_fmt_opt_ms(conv_r.get('final_marker_to_verified_match_ms'))}` |",
        f"| Full diff duration | `{_fmt_opt_ms(conv_b.get('diff_duration_ms'))}` | `{_fmt_opt_ms(conv_r.get('diff_duration_ms'))}` |",
        "",
        "## 2. Concurrent REPACK Execution, Sizes and Sampling",
        "",
        f"- Command: `{repack.get('command')}` (PID `{repack.get('repack_pid')}`), outcome: `{repack.get('outcome')}`",
        f"- Window interval: `{repack.get('start_offset_ms')} ms` to `{repack.get('end_offset_ms')} ms` (duration `{repack.get('duration_ms')} ms`)",
        f"- Writes overlapping REPACK: `{r_comm['completed_ops']}` committed during REPACK (`{perf['repack_intervals']['in_interval_strictly_contained']['completed_ops']}` strictly contained, `{perf['repack_intervals']['in_interval_any_overlap']['completed_ops']}` any overlap)",
        f"- Relation file swap confirmed: `heap_relfilenode_changed={deltas.get('heap_relfilenode_changed')}`, `toast_relfilenode_changed={deltas.get('toast_relfilenode_changed')}`",
        f"- Table / index sizes before -> after: heap `{sizes_b.get('heap_bytes')}` -> `{sizes_a.get('heap_bytes')}` bytes; TOAST `{sizes_b.get('toast_relation_bytes')}` -> `{sizes_a.get('toast_relation_bytes')}` bytes; indexes `{sizes_b.get('indexes_bytes')}` -> `{sizes_a.get('indexes_bytes')}` bytes; total `{sizes_b.get('total_relation_bytes')}` -> `{sizes_a.get('total_relation_bytes')}` bytes",
        f"- Progress samples: `{sampling.get('sample_count')}` total (`{sampling.get('progress_sample_count')}` with `pg_stat_progress_repack` row) at `{sampling.get('sample_interval_s')} s` cadence",
        f"- Phases observed: `{[p['phase'] for p in sampling.get('phases_observed') or []]}`",
        f"- Temporary repack slots observed: `{[s['slot_name'] for s in sampling.get('repack_slots_observed') or []]}`",
        f"- Final lock interval observed: `{sampling.get('final_lock_observed')}`",
    ]

    if perf.get("final_lock_observed") and perf.get("final_lock_ops"):
        fl_ev = perf["final_lock_ops"]["evidence"]
        fl_r = perf["final_lock_ops"]["repack"]
        fl_b = perf["final_lock_ops"]["baseline"]
        lines.extend(
            [
                f"  - Observed final-lock triggers: `{fl_ev['triggers']}`, span `{fl_ev['first_offset_ms']} ms` to `{fl_ev['last_offset_ms']} ms` (`{fl_ev['observed_span_ms']} ms`, `{fl_ev['sample_hits']}` sample hits)",
                f"  - Overlapping ops during observed final lock: REPACK `{_fmt_lat(fl_r['latency_sched_to_complete_ms'])}` vs Baseline `{_fmt_lat(fl_b['latency_sched_to_complete_ms'])}`",
            ]
        )
    else:
        lines.append(
            "  - Final-lock-specific figures are omitted because the brief `AccessExclusiveLock` / `swapping relation files` interval was not observed in sampler polls."
        )

    lines.extend(
        [
            "",
            "## 3. Host Performance Observations (This Host Only)",
            "",
            "### Full Measurement Window",
            "",
            "| Metric | Baseline | Concurrent REPACK |",
            "| --- | --- | --- |",
            f"| Attempted / Completed ops | `{b_full['attempted_ops']}` / `{b_full['completed_ops']}` | `{r_full['attempted_ops']}` / `{r_full['completed_ops']}` |",
            f"| Errors / Timeouts | `{b_full['error_count']}` / `{b_full['timeout_count']}` | `{r_full['error_count']}` / `{r_full['timeout_count']}` |",
            f"| Missed schedule (`queue > thresh`) / Harness missed | `{b_full['missed_schedule_count']}` / `{b_full['harness_missed_arrival_count']}` | `{r_full['missed_schedule_count']}` / `{r_full['harness_missed_arrival_count']}` |",
            f"| Completed throughput | `{b_full['completed_throughput_ops_s']} ops/s` | `{r_full['completed_throughput_ops_s']} ops/s` |",
            f"| Scheduled-to-completed latency | `{_fmt_lat(b_full['latency_sched_to_complete_ms'])}` | `{_fmt_lat(r_full['latency_sched_to_complete_ms'])}` |",
            f"| Execution latency | `{_fmt_lat(b_full['latency_exec_ms'])}` | `{_fmt_lat(r_full['latency_exec_ms'])}` |",
            f"| Queue delay | `{_fmt_lat(b_full['latency_queue_delay_ms'])}` | `{_fmt_lat(r_full['latency_queue_delay_ms'])}` |",
            f"| Steady-state reader lag probes | `{b_probes.get('visible_count')}/{b_probes.get('probe_count')}` visible, `{b_probes.get('reader_error_count')}` errors, `{_fmt_lat(b_probes.get('visibility_lag_ms'))}` | `{r_probes.get('visible_count')}/{r_probes.get('probe_count')}` visible, `{r_probes.get('reader_error_count')}` errors, `{_fmt_lat(r_probes.get('visibility_lag_ms'))}` |",
            "",
            f"### Matched REPACK Interval (`{repack.get('start_offset_ms')} ms` to `{repack.get('end_offset_ms')} ms`, duration `{repack.get('duration_ms')} ms`)",
            "",
            "| Metric (ops scheduled in interval) | Baseline (matching interval) | Concurrent REPACK |",
            "| --- | --- | --- |",
            f"| Attempted / Completed ops | `{b_int['attempted_ops']}` / `{b_int['completed_ops']}` | `{r_int['attempted_ops']}` / `{r_int['completed_ops']}` |",
            f"| Errors / Timeouts | `{b_int['error_count']}` / `{b_int['timeout_count']}` | `{r_int['error_count']}` / `{r_int['timeout_count']}` |",
            f"| Completed throughput | `{b_int['completed_throughput_ops_s']} ops/s` | `{r_int['completed_throughput_ops_s']} ops/s` |",
            f"| Scheduled-to-completed latency | `{_fmt_lat(b_int['latency_sched_to_complete_ms'])}` | `{_fmt_lat(r_int['latency_sched_to_complete_ms'])}` |",
            f"| Execution latency | `{_fmt_lat(b_int['latency_exec_ms'])}` | `{_fmt_lat(r_int['latency_exec_ms'])}` |",
            f"| Queue delay | `{_fmt_lat(b_int['latency_queue_delay_ms'])}` | `{_fmt_lat(r_int['latency_queue_delay_ms'])}` |",
            "",
        ]
    )
    return "\n".join(lines)
