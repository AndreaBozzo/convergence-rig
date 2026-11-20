"""Milestone W1 runner: start CDC with wal_level=replica on a fresh PostgreSQL 19.0 source.

The source starts with ``wal_level=replica`` and zero logical slots. The runner records
``wal_level`` and ``effective_wal_level`` before the first logical slot exists, runs
pg2iceberg's own preflight (``pg2iceberg init``) and ``connect-pg``, starts ``pg2iceberg run``,
and records both settings again once the slot exists. A short M1-style workload and the exact
comparison show whether the replica converged. The runner then restarts the source, drops the
slot, and creates and drops a native slot to show the settings moving in both directions.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import time
from typing import Any

import psycopg

from lab.bootstrap import bootstrap_all
from lab.config import REPO_ROOT, LabConfig, load_config
from lab.l_common import (
    ALL_LAB_PROFILES,
    compose_cmd_with_profiles,
    project_teardown_and_verify,
)
from lab.pipeline import (
    capture_pg_pipeline_positions,
    collect_container_digests,
    json_default,
    list_non_project_containers,
    list_project_resources,
    run_cmd,
    utc_now_iso,
    utc_run_stamp,
    verify_snapshot_convergence,
    wait_for_connector_and_snapshot,
    wait_for_marker_visible,
    write_json,
)
from lab.reader import connect_iceberg
from lab.workload import (
    FINAL_MARKER_ID,
    WARMUP_MARKER_ID,
    commit_marker_row,
    run_paced_workload,
    seed_orders,
    verify_toast_storage,
)

WAL_LEVEL_ENV = "PG_SOURCE_WAL_LEVEL"
PG2ICEBERG_TAG = "v0.12.0"
PG2ICEBERG_COMMIT = "01c2e172eb5e7be82c60b77f20cb089354c06ce6"
UPSTREAM_DIR = REPO_ROOT / "upstream" / "pg2iceberg-v0.12.0"
NATIVE_SLOT = "w1_native_probe"
CONNECTOR_SLOT = "pg2iceberg_slot"

# (relative path, first line, last line, substring that must appear in the range)
SOURCE_QUOTES: tuple[tuple[str, int, int, str], ...] = (
    ("crates/pg2iceberg/src/init.rs", 97, 107, 'SHOW wal_level'),
    ("crates/pg2iceberg/src/init.rs", 333, 350, 'eprintln!("✗ {} — {fix}"'),
    ("crates/pg2iceberg/src/init.rs", 1, 5, "pg2iceberg init"),
    ("crates/pg2iceberg/src/main.rs", 148, 157, "Command::Init"),
    ("crates/pg2iceberg/src/config.rs", 936, 950, "wal_level=logical"),
    ("crates/pg2iceberg-pg/src/prod/client.rs", 305, 314, "CREATE_REPLICATION_SLOT"),
    ("crates/pg2iceberg-validate/src/runtime.rs", 531, 545, "create_slot"),
)


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested)
# ---------------------------------------------------------------------------

_CHECK_RE = re.compile(r"^(?P<mark>[✓✗])\s+(?P<what>.*?)(?:\s+—\s+(?P<fix>.*))?$")


def parse_init_checks(stderr_text: str) -> list[dict[str, Any]]:
    """Parse the check lines ``pg2iceberg init`` prints on stderr.

    A passing line starts with a check mark, a failing one with a cross and ends with
    ``" — <fix>"``. Other lines are ignored.
    """
    checks: list[dict[str, Any]] = []
    for raw in stderr_text.splitlines():
        m = _CHECK_RE.match(raw.strip())
        if not m:
            continue
        ok = m.group("mark") == "✓"
        checks.append(
            {
                "ok": ok,
                "what": m.group("what").strip(),
                "fix": None if ok else (m.group("fix") or "").strip() or None,
            }
        )
    return checks


def wal_level_check(checks: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Return the ``wal_level is ...`` check from a parsed preflight, if present."""
    for c in checks:
        if c["what"].startswith("wal_level is "):
            return c
    return None


def settings_snapshot(rows: list[tuple[str, str]]) -> dict[str, str | None]:
    """Map pg_settings (name, setting) rows to the two WAL level settings."""
    d = dict(rows)
    return {"wal_level": d.get("wal_level"), "effective_wal_level": d.get("effective_wal_level")}


def classify_w1(
    *,
    before: dict[str, Any],
    after_slot: dict[str, Any],
    preflight_before: list[dict[str, Any]],
    preflight_after: list[dict[str, Any]],
    run_started: bool,
    convergence_outcome: str | None,
) -> dict[str, Any]:
    """Decide the W1 outcome from the recorded states.

    ``accepted`` means pg2iceberg ran against ``wal_level=replica`` and the exact comparison
    converged. ``refused`` means the connector did not start. ``invalid`` means the starting
    state was not the clean-start case the experiment needs.
    """
    clean_start = (
        before.get("wal_level") == "replica"
        and before.get("effective_wal_level") == "replica"
        and before.get("logical_slot_count") == 0
    )
    pre = wal_level_check(preflight_before)
    post = wal_level_check(preflight_after)
    preflight_flags_replica = bool(pre and not pre["ok"])
    if not clean_start:
        outcome = "invalid harness run"
    elif not run_started:
        outcome = "refused"
    elif convergence_outcome == "converged":
        outcome = "accepted and converged"
    else:
        outcome = f"accepted, {convergence_outcome or 'no comparison'}"
    return {
        "outcome": outcome,
        "clean_start": clean_start,
        "wal_level_stayed_replica": after_slot.get("wal_level") == "replica",
        "effective_wal_level_after_slot": after_slot.get("effective_wal_level"),
        "effective_wal_level_raised_by_slot": (
            before.get("effective_wal_level") == "replica"
            and after_slot.get("effective_wal_level") == "logical"
        ),
        "init_preflight_flags_replica_before_slot": preflight_flags_replica,
        "init_preflight_still_flags_after_slot": bool(post and not post["ok"]),
        "init_preflight_contradicts_running_connector": bool(
            run_started and post and not post["ok"]
            and after_slot.get("effective_wal_level") == "logical"
        ),
    }


def quote_source_lines(base: Path, rel: str, first: int, last: int, must_contain: str) -> dict[str, Any]:
    """Read ``rel`` lines ``first..last`` (1-based, inclusive) and check the expected text is there."""
    lines = (base / rel).read_text(encoding="utf-8").splitlines()
    chunk = lines[first - 1 : last]
    text = "\n".join(chunk)
    return {
        "file": rel,
        "first_line": first,
        "last_line": last,
        "found_expected_text": must_contain in text,
        "expected_text": must_contain,
        "text": text,
    }


# ---------------------------------------------------------------------------
# Docker and PostgreSQL helpers
# ---------------------------------------------------------------------------


def _run_utf8(cmd: list[str], *, timeout: float = 120.0) -> dict[str, Any]:
    proc = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        timeout=timeout,
    )
    return {
        "cmd": " ".join(cmd),
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def pg_wal_state(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    """Record wal_level, effective_wal_level, their pg_settings rows and the replication slots."""
    with conn.cursor() as cur:
        cur.execute("SHOW wal_level")
        show_wal = cur.fetchone()[0]
        cur.execute("SHOW effective_wal_level")
        show_eff = cur.fetchone()[0]
        cur.execute(
            "SELECT name, setting, context, short_desc FROM pg_settings "
            "WHERE name IN ('wal_level', 'effective_wal_level') ORDER BY name"
        )
        settings_rows = cur.fetchall()
        cur.execute(
            "SELECT slot_name, plugin, slot_type, active, wal_status, "
            "restart_lsn::text, confirmed_flush_lsn::text "
            "FROM pg_replication_slots ORDER BY slot_name"
        )
        slots = [
            {
                "slot_name": r[0],
                "plugin": r[1],
                "slot_type": r[2],
                "active": r[3],
                "wal_status": r[4],
                "restart_lsn": r[5],
                "confirmed_flush_lsn": r[6],
            }
            for r in cur.fetchall()
        ]
    return {
        "captured_at_utc": utc_now_iso(),
        "wal_level": show_wal,
        "effective_wal_level": show_eff,
        "pg_settings": [
            {"name": r[0], "setting": r[1], "context": r[2], "short_desc": r[3]} for r in settings_rows
        ],
        "slots": slots,
        "logical_slot_count": sum(1 for s in slots if s["slot_type"] == "logical"),
    }


def wait_effective_wal_level(
    cfg: LabConfig,
    want: str,
    *,
    timeout_s: float = 60.0,
    interval_s: float = 0.25,
    checkpoint_after_s: float | None = None,
) -> dict[str, Any]:
    """Poll ``effective_wal_level`` until it equals ``want``; optionally force a CHECKPOINT later."""
    t0 = time.monotonic()
    samples: list[dict[str, Any]] = []
    checkpointed = False
    while True:
        elapsed = time.monotonic() - t0
        with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW effective_wal_level")
                value = cur.fetchone()[0]
            if (
                checkpoint_after_s is not None
                and not checkpointed
                and value != want
                and elapsed >= checkpoint_after_s
            ):
                conn.execute("CHECKPOINT")
                checkpointed = True
                samples.append({"t_s": round(elapsed, 3), "event": "CHECKPOINT issued"})
        if not samples or samples[-1].get("value") != value:
            samples.append({"t_s": round(elapsed, 3), "value": value})
        if value == want:
            return {
                "wanted": want,
                "reached": True,
                "elapsed_ms": round(elapsed * 1000.0, 1),
                "checkpoint_forced": checkpointed,
                "samples": samples,
            }
        if elapsed >= timeout_s:
            return {
                "wanted": want,
                "reached": False,
                "elapsed_ms": round(elapsed * 1000.0, 1),
                "checkpoint_forced": checkpointed,
                "samples": samples,
            }
        time.sleep(interval_s)


def run_init_preflight(cfg: LabConfig, label: str, run_dir: Path) -> dict[str, Any]:
    """Run ``pg2iceberg init --output -`` in a one-off container and keep its raw output."""
    url = (
        f"postgres://{cfg.pg_source_user}:{cfg.pg_source_password}"
        f"@pg-source:5432/{cfg.pg_source_db}?sslmode=disable"
    )
    cmd = [
        *compose_cmd_with_profiles(cfg, ("connector",)),
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "-e",
        f"POSTGRES_URL={url}",
        "pg2iceberg",
        "init",
        "--output",
        "-",
    ]
    res = _run_utf8(cmd)
    res["cmd"] = res["cmd"].replace(cfg.pg_source_password, "<redacted>")
    checks = parse_init_checks(res["stderr"])
    (run_dir / f"init_{label}.stdout.yaml").write_text(res["stdout"], encoding="utf-8")
    (run_dir / f"init_{label}.stderr.txt").write_text(res["stderr"], encoding="utf-8")
    return {
        "label": label,
        "cmd": res["cmd"],
        "returncode": res["returncode"],
        "checks": checks,
        "stderr_file": f"init_{label}.stderr.txt",
        "stdout_file": f"init_{label}.stdout.yaml",
    }


def run_connect_pg(cfg: LabConfig, label: str, run_dir: Path) -> dict[str, Any]:
    cmd = [
        *compose_cmd_with_profiles(cfg, ("connector",)),
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "pg2iceberg",
        "connect-pg",
        "--config",
        "/etc/pg2iceberg/config.yaml",
    ]
    res = _run_utf8(cmd)
    (run_dir / f"connect_pg_{label}.txt").write_text(
        res["stdout"] + "\n--- stderr ---\n" + res["stderr"], encoding="utf-8"
    )
    return {
        "label": label,
        "cmd": res["cmd"],
        "returncode": res["returncode"],
        "stdout": res["stdout"].strip(),
        "stderr_tail": res["stderr"].strip().splitlines()[-5:],
    }


_LEVEL_RE = re.compile(r'"level":"(?P<level>[A-Za-z]+)"')


def summarize_log_levels(log_text: str) -> dict[str, int]:
    """Count pg2iceberg JSON log lines by level (the ``level`` field)."""
    counts = {"error": 0, "warn": 0, "lines": 0}
    for line in log_text.splitlines():
        if not line.strip():
            continue
        counts["lines"] += 1
        m = _LEVEL_RE.search(line)
        if not m:
            continue
        level = m.group("level").lower()
        if level == "error":
            counts["error"] += 1
        elif level in ("warn", "warning"):
            counts["warn"] += 1
    return counts


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_w1(
    *,
    seed_rows: int = 2500,
    toast_rows: int = 50,
    duration_s: float = 60.0,
    rate_ops_s: float = 10.0,
    rng_seed: int = 20261119,
    batch_size: int = 2000,
    convergence_timeout_s: float = 180.0,
    label: str = "w1-wal-level-replica",
    keep_up: bool = False,
) -> Path:
    os.environ[WAL_LEVEL_ENV] = "replica"
    cfg = load_config()
    run_dir = REPO_ROOT / "runs" / f"{utc_run_stamp()}-{label}"
    run_dir.mkdir(parents=True, exist_ok=True)

    commands_executed: list[str] = []
    non_project_before = list_non_project_containers(cfg.compose_project)

    def logged_cmd(cmd: list[str], *, check: bool = True) -> Any:
        commands_executed.append(" ".join(cmd))
        return run_cmd(cmd, check=check)

    all_profiles = (*ALL_LAB_PROFILES, "polaris", "polaris-connector")

    # 0. Source quotes from the pinned checkout.
    head = _run_utf8(["git", "-C", str(UPSTREAM_DIR), "rev-parse", "HEAD"])["stdout"].strip()
    quotes = [quote_source_lines(UPSTREAM_DIR, *q) for q in SOURCE_QUOTES]
    write_json(
        run_dir / "pg2iceberg_source_quotes.json",
        {"tag": PG2ICEBERG_TAG, "expected_commit": PG2ICEBERG_COMMIT, "checkout_head": head, "quotes": quotes},
    )

    # 1. Fresh start with wal_level=replica (compose reads PG_SOURCE_WAL_LEVEL from the process env).
    logged_cmd([*compose_cmd_with_profiles(cfg, all_profiles), "down", "-v", "--remove-orphans"])
    resources_after_pre_clean = list_project_resources(cfg.compose_project)
    logged_cmd([*cfg.compose_base_cmd(include_connector=False), "up", "-d", "--wait"])

    bootstrap_res = bootstrap_all(cfg)
    write_json(run_dir / "bootstrap.json", bootstrap_res)

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        state_fresh = pg_wal_state(conn)
    write_json(run_dir / "wal_state_1_fresh_before_seed.json", state_fresh)

    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        seed_res = seed_orders(
            pg_conn, seed_rows=seed_rows, toast_row_count=toast_rows, rng_seed=rng_seed
        )
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        state_before = pg_wal_state(conn)
        pos_before = capture_pg_pipeline_positions(conn)
    write_json(run_dir / "wal_state_2_before_first_slot.json", state_before)

    # 2. pg2iceberg's own preflight and connection check, before any slot exists.
    preflight_before = run_init_preflight(cfg, "before_slot", run_dir)
    connect_before = run_connect_pg(cfg, "before_slot", run_dir)
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        state_after_preflight = pg_wal_state(conn)
    write_json(run_dir / "wal_state_3_after_preflight_no_slot.json", state_after_preflight)

    # 3. Start pg2iceberg run and wait for the slot and the initial snapshot.
    logged_cmd([*cfg.compose_base_cmd(include_connector=True), "up", "-d", "pg2iceberg"])
    digests = collect_container_digests(cfg)
    con_v, reader_versions = connect_iceberg(cfg)
    con_v.close()

    run_started = False
    initial_snapshot_info: dict[str, Any] = {}
    start_error: str | None = None
    try:
        initial_snapshot_info = wait_for_connector_and_snapshot(cfg, timeout_s=180.0)
        run_started = True
    except Exception as exc:  # a refusal is a result, keep the evidence
        start_error = f"{type(exc).__name__}: {exc}"

    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        state_after_slot = pg_wal_state(conn)
    write_json(run_dir / "wal_state_4_after_slot_connector_running.json", state_after_slot)

    convergence: dict[str, Any] = {}
    workload_summary: dict[str, Any] = {}
    toast_after_workload: dict[str, Any] = {}
    preflight_after: dict[str, Any] = {"checks": []}
    warmup_vis: dict[str, Any] = {}
    final_commit_utc = None
    pos_window_start = pos_window_end = pos_final = {}

    if run_started:
        # 4. The same preflight while the slot exists and effective_wal_level is logical.
        preflight_after = run_init_preflight(cfg, "after_slot", run_dir)

        with psycopg.connect(cfg.pg_dsn) as pg_conn:
            warmup_ns, _ = commit_marker_row(
                pg_conn,
                marker_id=WARMUP_MARKER_ID,
                status="marker_warmup",
                note="w1 warm-up marker before measurement window",
            )
        warmup_vis = wait_for_marker_visible(
            cfg, WARMUP_MARKER_ID, warmup_ns, timeout_s=120.0, poll_interval_s=0.5
        )
        with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
            pos_window_start = capture_pg_pipeline_positions(conn)

        workload_summary = run_paced_workload(
            cfg,
            seed_res,
            duration_s=duration_s,
            rate_ops_s=rate_ops_s,
            rng_seed=rng_seed,
            ops_jsonl=run_dir / "ops.jsonl",
            probes_jsonl=run_dir / "steady_state_probes.jsonl",
            probe_interval_s=20.0,
            probe_poll_interval_s=0.5,
        )
        with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
            toast_after_workload = verify_toast_storage(conn)
            pos_window_end = capture_pg_pipeline_positions(conn)

        with psycopg.connect(cfg.pg_dsn) as pg_conn:
            final_ns, final_commit_utc = commit_marker_row(
                pg_conn,
                marker_id=FINAL_MARKER_ID,
                status="marker_final",
                note="w1 final convergence marker",
            )
        convergence = verify_snapshot_convergence(
            cfg,
            FINAL_MARKER_ID,
            final_ns,
            timeout_s=convergence_timeout_s,
            poll_interval_s=1.0,
            batch_size=batch_size,
        )
        lk_meta = convergence.pop("final_lakekeeper_metadata", None)
        if lk_meta is not None:
            write_json(run_dir / "lakekeeper_table_final.json", lk_meta)
        with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
            pos_final = capture_pg_pipeline_positions(conn)

    pg2_logs = run_cmd(
        [*cfg.compose_base_cmd(include_connector=True), "logs", "--no-color", "pg2iceberg"],
        check=False,
    ).stdout
    (run_dir / "pg2iceberg.log").write_text(pg2_logs, encoding="utf-8")
    log_levels = summarize_log_levels(pg2_logs)

    # 5. Stop the connector (the slot stays), restart the source, and watch effective_wal_level.
    restart_info: dict[str, Any] = {}
    drop_info: dict[str, Any] = {}
    native_info: dict[str, Any] = {}
    if run_started:
        logged_cmd([*cfg.compose_base_cmd(include_connector=True), "stop", "pg2iceberg"])
        logged_cmd([*cfg.compose_base_cmd(), "restart", "pg-source"])
        # wait for the healthcheck
        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline:
            try:
                with psycopg.connect(cfg.pg_dsn, autocommit=True, connect_timeout=2) as conn:
                    state_after_restart = pg_wal_state(conn)
                break
            except Exception:
                time.sleep(0.5)
        else:
            raise RuntimeError("pg-source did not come back after restart")
        restart_info = {
            "wal_level_setting_still_replica": state_after_restart["wal_level"] == "replica",
            "state": state_after_restart,
        }
        write_json(run_dir / "wal_state_5_after_source_restart_slot_kept.json", state_after_restart)

        # 6. Drop the connector slot and watch the effective level fall back.
        with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
            conn.execute("SELECT pg_drop_replication_slot(%s)", (CONNECTOR_SLOT,))
        fell = wait_effective_wal_level(cfg, "replica", timeout_s=30.0, checkpoint_after_s=10.0)
        with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
            state_after_drop = pg_wal_state(conn)
        drop_info = {"slot_dropped": CONNECTOR_SLOT, "fallback": fell, "state": state_after_drop}
        write_json(run_dir / "wal_state_6_after_slot_drop.json", state_after_drop)

        # 7. Native slot creation on the same instance, outside pg2iceberg.
        with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
            t_ns = time.monotonic_ns()
            conn.execute(
                "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (NATIVE_SLOT,)
            )
            create_ms = (time.monotonic_ns() - t_ns) / 1e6
            state_native = pg_wal_state(conn)
            conn.execute("SELECT pg_drop_replication_slot(%s)", (NATIVE_SLOT,))
        native_fell = wait_effective_wal_level(cfg, "replica", timeout_s=30.0, checkpoint_after_s=10.0)
        native_info = {
            "slot": NATIVE_SLOT,
            "create_ms": round(create_ms, 2),
            "state_with_native_slot": state_native,
            "fallback_after_drop": native_fell,
        }
        write_json(run_dir / "wal_state_7_native_slot.json", native_info)

    # 8. Classify and clean up.
    classification = classify_w1(
        before=state_before,
        after_slot=state_after_slot,
        preflight_before=preflight_before["checks"],
        preflight_after=preflight_after["checks"],
        run_started=run_started,
        convergence_outcome=convergence.get("outcome") if run_started else None,
    )

    cleanup_result = project_teardown_and_verify(
        cfg, logged_cmd, non_project_before, profiles=all_profiles, keep_up=keep_up
    )
    cleanup_ok = keep_up or (
        cleanup_result["all_project_resources_removed"]
        and cleanup_result["non_project_containers_unchanged"]
    )

    schedule_ok = bool(workload_summary) and workload_summary["arrival_schedule"]["schedule_valid"]
    passed = (
        classification["outcome"] == "accepted and converged"
        and bool(convergence.get("passed"))
        and schedule_ok
        and cleanup_ok
    )

    summary = {
        "milestone": "W1",
        "label": label,
        "outcome": classification["outcome"],
        "passed": passed,
        "completed_at_utc": utc_now_iso(),
        "parameters": {
            "wal_level_env": f"{WAL_LEVEL_ENV}=replica",
            "seed_rows": seed_rows,
            "toast_rows": toast_rows,
            "duration_s": duration_s,
            "rate_ops_s": rate_ops_s,
            "rng_seed": rng_seed,
            "batch_size": batch_size,
            "convergence_timeout_s": convergence_timeout_s,
        },
        "host": {"platform": platform.platform(), "machine": platform.machine(), "python_version": sys.version},
        "config": cfg.redacted_summary(),
        "commands_executed": commands_executed,
        "container_digests": digests,
        "reader_versions": {
            "duckdb": reader_versions.duckdb_version,
            "extensions": reader_versions.extensions,
        },
        "resources_after_pre_clean": resources_after_pre_clean,
        "classification": classification,
        "states": {
            "fresh_before_seed": state_fresh,
            "before_first_slot": state_before,
            "after_preflight_no_slot": state_after_preflight,
            "after_slot_connector_running": state_after_slot,
        },
        "preflight_before_slot": preflight_before,
        "connect_pg_before_slot": connect_before,
        "preflight_after_slot": preflight_after,
        "run_started": run_started,
        "start_error": start_error,
        "initial_snapshot": initial_snapshot_info,
        "seed": seed_res.to_dict(),
        "warmup_marker": {
            k: warmup_vis.get(k)
            for k in ("visible", "visibility_ms", "snapshot_id", "polls", "errors")
        },
        "workload": workload_summary,
        "toast_after_workload": toast_after_workload,
        "final_marker_commit_utc": final_commit_utc,
        "convergence": convergence,
        "pg2iceberg_log_levels": log_levels,
        "source_restart": restart_info,
        "slot_drop": drop_info,
        "native_slot": native_info,
        "pipeline_positions": {
            "before_slot": pos_before,
            "window_start": pos_window_start,
            "window_end": pos_window_end,
            "final": pos_final,
        },
        "upstream_source": {"tag": PG2ICEBERG_TAG, "commit": head, "expected_commit": PG2ICEBERG_COMMIT},
        "cleanup": cleanup_result,
    }
    write_json(run_dir / "summary.json", summary)

    compact = {
        "run_dir": str(run_dir.relative_to(REPO_ROOT).as_posix()),
        "outcome": classification["outcome"],
        "passed": passed,
        "classification": classification,
        "wal_before": {k: state_before[k] for k in ("wal_level", "effective_wal_level", "logical_slot_count")},
        "wal_after_slot": {
            k: state_after_slot[k] for k in ("wal_level", "effective_wal_level", "logical_slot_count")
        },
        "init_before": [c for c in preflight_before["checks"] if c["what"].startswith("wal_level")],
        "init_after": [c for c in preflight_after["checks"] if c["what"].startswith("wal_level")],
        "final_marker_visibility_ms": convergence.get("final_marker_visibility_ms"),
        "diff_duration_ms": convergence.get("diff_duration_ms"),
        "log_levels": log_levels,
        "cleanup_ok": cleanup_ok,
    }
    print(json.dumps(compact, indent=2, default=json_default))
    if not passed:
        raise SystemExit(1)
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="W1: start CDC with wal_level=replica.")
    parser.add_argument("--seed-rows", type=int, default=2500)
    parser.add_argument("--toast-rows", type=int, default=50)
    parser.add_argument("--duration-s", type=float, default=60.0)
    parser.add_argument("--rate-ops-s", type=float, default=10.0)
    parser.add_argument("--rng-seed", type=int, default=20261119)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--convergence-timeout-s", type=float, default=180.0)
    parser.add_argument("--label", default="w1-wal-level-replica")
    parser.add_argument("--keep-up", action="store_true")
    args = parser.parse_args()
    run_w1(
        seed_rows=args.seed_rows,
        toast_rows=args.toast_rows,
        duration_s=args.duration_s,
        rate_ops_s=args.rate_ops_s,
        rng_seed=args.rng_seed,
        batch_size=args.batch_size,
        convergence_timeout_s=args.convergence_timeout_s,
        label=args.label,
        keep_up=args.keep_up,
    )


if __name__ == "__main__":
    main()
