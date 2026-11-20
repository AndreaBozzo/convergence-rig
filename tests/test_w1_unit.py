"""Unit and fault-injection tests for the W1 wal_level=replica analysis helpers."""

from __future__ import annotations

from pathlib import Path

from lab.w1 import (
    classify_w1,
    parse_init_checks,
    quote_source_lines,
    summarize_log_levels,
    wal_level_check,
)

CROSS = "✗"
TICK = "✓"
DASH = "—"

PREFLIGHT_REPLICA = (
    f"{CROSS} wal_level is replica {DASH} set wal_level = logical and restart Postgres "
    f"(RDS / Aurora: rds.logical_replication = 1 in the parameter group)\n"
    f"{TICK} role postgres can replicate\n"
    f"{TICK} 16 replication slot(s) free for pg2iceberg_slot\n"
    f"{TICK} pg2iceberg can keep its state in schema _pg2iceberg\n"
    f"{TICK} pg2iceberg can create publication pg2iceberg_pub\n"
    "1 table(s) to replicate, 0 left out\n"
    "fix what's marked " + CROSS + " before starting pg2iceberg\n"
)
PREFLIGHT_LOGICAL = PREFLIGHT_REPLICA.replace(
    f"{CROSS} wal_level is replica {DASH} set wal_level = logical and restart Postgres "
    f"(RDS / Aurora: rds.logical_replication = 1 in the parameter group)",
    f"{TICK} wal_level is logical",
)

CLEAN = {"wal_level": "replica", "effective_wal_level": "replica", "logical_slot_count": 0}
RAISED = {"wal_level": "replica", "effective_wal_level": "logical", "logical_slot_count": 1}


def test_parse_marks_replica_as_failing_with_fix() -> None:
    checks = parse_init_checks(PREFLIGHT_REPLICA)
    assert len(checks) == 5
    wal = wal_level_check(checks)
    assert wal is not None
    assert wal["ok"] is False
    assert wal["what"] == "wal_level is replica"
    assert wal["fix"] is not None and wal["fix"].startswith("set wal_level = logical")
    assert all(c["ok"] for c in checks if c is not wal)


def test_parse_marks_logical_as_passing() -> None:
    wal = wal_level_check(parse_init_checks(PREFLIGHT_LOGICAL))
    assert wal == {"ok": True, "what": "wal_level is logical", "fix": None}


def test_parse_ignores_other_lines_and_empty_input() -> None:
    assert parse_init_checks("") == []
    assert parse_init_checks("1 table(s) to replicate\nwrote x\n") == []
    assert wal_level_check([]) is None


def test_accepted_run_contradicts_the_preflight() -> None:
    res = classify_w1(
        before=CLEAN,
        after_slot=RAISED,
        preflight_before=parse_init_checks(PREFLIGHT_REPLICA),
        preflight_after=parse_init_checks(PREFLIGHT_REPLICA),
        run_started=True,
        convergence_outcome="converged",
    )
    assert res["outcome"] == "accepted and converged"
    assert res["effective_wal_level_raised_by_slot"] is True
    assert res["wal_level_stayed_replica"] is True
    assert res["init_preflight_flags_replica_before_slot"] is True
    assert res["init_preflight_contradicts_running_connector"] is True


def test_refused_when_connector_did_not_start() -> None:
    res = classify_w1(
        before=CLEAN,
        after_slot=CLEAN,
        preflight_before=parse_init_checks(PREFLIGHT_REPLICA),
        preflight_after=[],
        run_started=False,
        convergence_outcome=None,
    )
    assert res["outcome"] == "refused"
    assert res["init_preflight_contradicts_running_connector"] is False


def test_start_with_an_existing_logical_slot_is_invalid() -> None:
    dirty = {**CLEAN, "effective_wal_level": "logical", "logical_slot_count": 1}
    res = classify_w1(
        before=dirty,
        after_slot=RAISED,
        preflight_before=[],
        preflight_after=[],
        run_started=True,
        convergence_outcome="converged",
    )
    assert res["outcome"] == "invalid harness run"
    assert res["clean_start"] is False


def test_start_with_wal_level_logical_is_invalid() -> None:
    logical = {"wal_level": "logical", "effective_wal_level": "logical", "logical_slot_count": 0}
    res = classify_w1(
        before=logical,
        after_slot=logical,
        preflight_before=[],
        preflight_after=[],
        run_started=True,
        convergence_outcome="converged",
    )
    assert res["outcome"] == "invalid harness run"


def test_mismatch_after_start_is_reported_not_hidden() -> None:
    res = classify_w1(
        before=CLEAN,
        after_slot=RAISED,
        preflight_before=[],
        preflight_after=[],
        run_started=True,
        convergence_outcome="mismatch at deadline",
    )
    assert res["outcome"] == "accepted, mismatch at deadline"


def test_quote_source_lines_checks_expected_text(tmp_path: Path) -> None:
    f = tmp_path / "a.rs"
    f.write_text("line1\nSHOW wal_level\nline3\n", encoding="utf-8")
    ok = quote_source_lines(tmp_path, "a.rs", 2, 3, "SHOW wal_level")
    assert ok["found_expected_text"] is True
    assert ok["text"] == "SHOW wal_level\nline3"
    moved = quote_source_lines(tmp_path, "a.rs", 1, 1, "SHOW wal_level")
    assert moved["found_expected_text"] is False


def test_summarize_log_levels() -> None:
    log = (
        '{"level":"INFO","msg":"a"}\n'
        '{"level":"ERROR","msg":"b"}\n'
        '{"level":"WARN","msg":"c"}\n'
        "\n"
    )
    assert summarize_log_levels(log) == {"error": 1, "warn": 1, "lines": 3}
    assert summarize_log_levels("") == {"error": 0, "warn": 0, "lines": 0}
