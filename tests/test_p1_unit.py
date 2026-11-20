"""Unit and fault-injection tests for Milestone P1 (Apache Polaris helpers and checks)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from lab.config import load_config
from lab.p1 import _parse_quarkus_startup_s, _summarize_memory_samples, evaluate_p1_l2_checks
from lab.polaris import (
    SYNTHETIC_SILO_ROLE_ARN,
    create_s3_catalog,
    load_polaris_config,
    parse_docker_mem_mib,
)


def test_parse_docker_mem_mib_and_summary() -> None:
    assert parse_docker_mem_mib("389.8MiB / 15.38GiB") == 389.8
    assert parse_docker_mem_mib("1.5GiB / 16GiB") == 1536.0
    assert parse_docker_mem_mib("512KiB / 16GiB") == 0.5
    assert parse_docker_mem_mib("not-a-number / 16GiB") is None

    summary = _summarize_memory_samples(
        [
            {"containers": {"cdclab-polaris-1": {"mem_used_mib": 380.0}}},
            {"containers": {"cdclab-polaris-1": {"mem_used_mib": 400.0}}},
            {"containers": {"cdclab-polaris-1": {"mem_used_mib": 420.0}}},
        ]
    )
    assert summary["cdclab-polaris-1"]["count"] == 3
    assert summary["cdclab-polaris-1"]["min_mib"] == 380.0
    assert summary["cdclab-polaris-1"]["p50_mib"] == 400.0
    assert summary["cdclab-polaris-1"]["max_mib"] == 420.0


def test_parse_quarkus_startup_seconds() -> None:
    log = (
        "2026-11-20 12:58:19,580 INFO  [io.quarkus] (main) "
        "polaris-server 1.9.0 on JVM (powered by Quarkus 3.40.1.2) started in 6.721s."
    )
    assert _parse_quarkus_startup_s(log) == 6.721
    assert _parse_quarkus_startup_s("no startup line here") is None


def test_create_s3_catalog_payload_enables_sts_and_disables_kms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, dict[str, Any] | None]] = []

    def fake_http(url: str, *, method: str = "GET", body=None, headers=None, timeout=20.0):
        calls.append((method, url, body))
        if method == "GET" and len(calls) == 1:
            return 404, {"error": {"code": 404}}, ""
        if method == "POST":
            return 201, {"name": "cdclab_polaris"}, ""
        return 200, {"name": "cdclab_polaris", "storageConfigInfo": {"endpoint": "http://silo.localhost:19000"}}, ""

    monkeypatch.setattr("lab.polaris.http_json_raw", fake_http)
    lab_cfg = load_config()
    polaris_cfg = load_polaris_config()
    res = create_s3_catalog(
        polaris_cfg,
        lab_cfg,
        "dummy-token",
        catalog_name="cdclab_polaris",
        key_prefix="polaris-warehouse",
    )
    assert res["action"] == "created"
    post_body = calls[1][2]
    assert post_body is not None
    sc = post_body["catalog"]["storageConfigInfo"]
    assert sc["storageType"] == "S3"
    assert sc["pathStyleAccess"] is True
    assert sc["stsUnavailable"] is False
    assert sc["kmsUnavailable"] is True
    assert sc["roleArn"] == SYNTHETIC_SILO_ROLE_ARN
    assert sc["endpoint"] == "http://silo.localhost:19000"
    assert sc["stsEndpoint"] == "http://silo.localhost:19000"


def _passing_l2_inputs() -> dict[str, Any]:
    return {
        "prefix_disjoint": True,
        "reader_curated_load": {"allowed": True, "http_status": 200},
        "reader_duckdb_curated": {"allowed": True, "row_count": 2},
        "reader_s3_curated_positive": {"allowed": True, "http_status": 200, "parquet_magic_valid": True},
        "reader_raw_load": {"allowed": False, "http_status": 403},
        "reader_duckdb_raw": {"allowed": False, "row_count": 0},
        "writer_raw_load": {"allowed": True, "http_status": 200},
        "reader_s3_raw_denied": {"allowed": False, "http_status": 403},
        "reader_s3_new_raw_denied": {"allowed": False, "http_status": 403},
        "writer_s3_raw_allowed": {"allowed": True, "http_status": 200, "parquet_magic_valid": True},
        "writer_init": {
            "raw": {"row_count": 2, "current_snapshot_id": 100},
            "curated": {"row_count": 2, "current_snapshot_id": 200},
        },
        "writer_append": {
            "raw": {"row_count": 3, "current_snapshot_id": 101},
            "curated": {"row_count": 3, "current_snapshot_id": 201},
        },
        "reader_duckdb_curated_after_append": {"allowed": True, "row_count": 3},
        "reader_curated_write_denied": {"allowed": False, "http_status": 403},
    }


def test_evaluate_p1_l2_checks_passes_clean_and_fails_each_fault() -> None:
    clean = evaluate_p1_l2_checks(**_passing_l2_inputs())
    assert clean["all_passed"] is True

    # Fault 1: reader allowed to load raw table in catalog
    f1 = _passing_l2_inputs()
    f1["reader_raw_load"] = {"allowed": True, "http_status": 200}
    r1 = evaluate_p1_l2_checks(**f1)
    assert r1["all_passed"] is False
    assert r1["check2_catalog_denies_raw"] is False

    # Fault 2: reader vended STS credentials allowed on raw Parquet file in Silo
    f2 = _passing_l2_inputs()
    f2["reader_s3_raw_denied"] = {"allowed": True, "http_status": 200}
    r2 = evaluate_p1_l2_checks(**f2)
    assert r2["all_passed"] is False
    assert r2["check3_s3_denies_raw"] is False

    # Fault 3: reader commit allowed on curated table
    f3 = _passing_l2_inputs()
    f3["reader_curated_write_denied"] = {"allowed": True, "http_status": 200}
    r3 = evaluate_p1_l2_checks(**f3)
    assert r3["all_passed"] is False
    assert r3["check4_writer_writes_both_and_commit_denied"] is False

    # Fault 4: non-disjoint table storage prefixes
    f4 = _passing_l2_inputs()
    f4["prefix_disjoint"] = False
    r4 = evaluate_p1_l2_checks(**f4)
    assert r4["all_passed"] is False
    assert r4["prefix_disjoint"] is False


def test_p1_polaris_compose_files_present() -> None:
    repo = Path(__file__).resolve().parent.parent
    assert (repo / "compose" / "pg2iceberg.polaris.yaml").is_file()
    assert (repo / "compose" / "polaris" / "init-schema.sql").is_file()
    polaris_cfg = load_polaris_config()
    assert polaris_cfg.polaris_port == 18183
    assert polaris_cfg.polaris_mgmt_port == 18184
    assert polaris_cfg.catalog_name == "cdclab_polaris"
    assert polaris_cfg.authz_catalog_name == "cdclab_polaris_authz"
