"""Unit tests for L3 outage sample aggregation, coordinator offset extraction, and log parsing."""

from __future__ import annotations

from lab.l3 import (
    extract_coordinator_offsets,
    summarize_outage_samples,
    summarize_pg2iceberg_logs,
)


def test_extract_coordinator_offsets_computes_pending_backlog() -> None:
    pg_pos = {
        "current_wal_lsn": "0/03000000",
        "replication_slots": [
            {
                "slot_name": "pg2iceberg_slot",
                "active": True,
                "restart_lsn": "0/02F00000",
                "confirmed_flush_lsn": "0/02FF0000",
                "wal_gap_bytes_confirmed_flush": 65536,
                "wal_gap_bytes_restart": 1048576,
            }
        ],
        "coordinator_state": {
            "replicated_lsn": [{"id": 1, "lsn": 50331648}],
            "flushed_lsn": [{"id": 1, "lsn": 50266112}],
            "log_seq": [{"table_name": "public.orders", "next_offset": 1250}],
            "mat_cursor": [
                {"group_name": "default#snapshot-lsn", "table_name": "public.orders", "last_offset": 30000000},
                {"group_name": "default", "table_name": "public.orders", "last_offset": 620},
            ],
            "log_index_row_count": 32,
        },
    }
    res = extract_coordinator_offsets(pg_pos)
    assert res["staged_log_seq"] == 1250
    assert res["applied_mat_cursor"] == 620
    assert res["pending_unapplied_offsets"] == 630
    assert res["wal_gap_bytes_confirmed_flush"] == 65536
    assert res["wal_gap_bytes_restart"] == 1048576
    assert res["log_index_row_count"] == 32


def test_summarize_outage_samples_tracks_staging_backlog_and_drain() -> None:
    samples = [
        {
            "phase": "pre_outage",
            "offset_ms": 60000.0,
            "captured_at_utc": "2026-11-19T21:10:00.000000Z",
            "staged_log_seq": 600,
            "applied_mat_cursor": 600,
            "pending_unapplied_offsets": 0,
            "log_index_row_count": 1,
            "wal_gap_bytes_confirmed_flush": 1000,
            "wal_gap_bytes_restart": 50000,
            "pg2iceberg_container": {"found": True, "running": True, "restart_count": 0},
        },
        {
            "phase": "outage",
            "offset_ms": 70000.0,
            "captured_at_utc": "2026-11-19T21:10:10.000000Z",
            "staged_log_seq": 700,
            "applied_mat_cursor": 600,
            "pending_unapplied_offsets": 100,
            "log_index_row_count": 5,
            "wal_gap_bytes_confirmed_flush": 120000,
            "wal_gap_bytes_restart": 500000,
            "pg2iceberg_container": {"found": True, "running": True, "restart_count": 0},
        },
        {
            "phase": "outage",
            "offset_ms": 124000.0,
            "captured_at_utc": "2026-11-19T21:11:04.000000Z",
            "staged_log_seq": 1200,
            "applied_mat_cursor": 600,
            "pending_unapplied_offsets": 600,
            "log_index_row_count": 30,
            "wal_gap_bytes_confirmed_flush": 240000,
            "wal_gap_bytes_restart": 1800000,
            "pg2iceberg_container": {"found": True, "running": True, "restart_count": 0},
        },
        {
            "phase": "post_restart",
            "offset_ms": 135000.0,
            "captured_at_utc": "2026-11-19T21:11:15.000000Z",
            "staged_log_seq": 1310,
            "applied_mat_cursor": 1280,
            "pending_unapplied_offsets": 30,
            "log_index_row_count": 2,
            "wal_gap_bytes_confirmed_flush": 5000,
            "wal_gap_bytes_restart": 100000,
            "pg2iceberg_container": {"found": True, "running": True, "restart_count": 0},
        },
    ]
    s = summarize_outage_samples(samples)
    assert s["staging_advanced_during_outage"] is True
    assert s["applied_frozen_during_outage"] is True
    assert s["outage_staged_delta_offsets"] == 500
    assert s["outage_max_pending_unapplied_offsets"] == 600
    assert s["outage_wal_gap_confirmed_flush_max_bytes"] == 240000
    assert s["outage_wal_gap_restart_max_bytes"] == 1800000
    assert s["pg2iceberg_all_samples_running"] is True
    assert s["pg2iceberg_max_restart_count"] == 0
    assert s["backlog_drained_offset_ms"] == 135000.0


def test_summarize_pg2iceberg_logs_groups_warnings_and_errors() -> None:
    raw = "\n".join(
        [
            'pg2iceberg-1  | {"timestamp":"2026-11-19T21:10:00Z","level":"INFO","message":"ok","target":"pg2iceberg::run"}',
            'pg2iceberg-1  | {"timestamp":"2026-11-19T21:10:15Z","level":"ERROR","message":"catalog commit failed","target":"pg2iceberg_logical::materializer"}',
            'pg2iceberg-1  | {"timestamp":"2026-11-19T21:10:25Z","level":"ERROR","message":"catalog commit failed","target":"pg2iceberg_logical::materializer"}',
            'pg2iceberg-1  | {"timestamp":"2026-11-19T21:10:26Z","level":"WARN","message":"retrying cycle","target":"pg2iceberg_logical::materializer"}',
            "pg2iceberg-1  | Error: logical lifecycle",
            "pg2iceberg-1  | Caused by:",
            "pg2iceberg-1  |     0: materializer.cycle: catalog: other: Unexpected => Failed to execute http request",
        ]
    )
    summary = summarize_pg2iceberg_logs(raw)
    assert summary["parsed_json_lines"] == 4
    assert summary["non_json_lines"] == 3
    assert summary["fatal_text_error_count"] == 3
    assert summary["error_count"] == 2
    assert summary["warn_count"] == 1
    assert summary["error_warn_groups"][0]["count"] == 2
    assert summary["error_warn_groups"][0]["message"] == "catalog commit failed"
