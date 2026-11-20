"""Unit and fault-injection tests for the D1 helpers: redaction, classification and cleanup planning."""

from __future__ import annotations

import json

from lab.d1 import (
    Redactor,
    classify_pg2iceberg_attempt,
    partition_tables,
    scan_for_secrets,
    strip_credentials,
    summarize_table_response,
)

# Fake values with the real shapes. None of these is a credential.
FAKE_TOKEN = "d" + "api" + "0123456789abcdef" * 2
FAKE_HOST = "https://dbc-12345678-abcd.cloud.databricks.com"
FAKE_KEY_ID = "ASIA" + "ABCDEFGHIJKLMNOP"
FAKE_SESSION = "IQoJ" + "b3JpZ2luX2Vj" * 12
FAKE_STORAGE = (
    "s3://dbstorage-prod-abcde/uc/"
    "11111111-2222-3333-4444-555555555555/66666666-7777-8888-9999-000000000000/"
    "__unitystorage/catalogs/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/tables/"
    "12345678-1234-1234-1234-123456789012/_iceberg/metadata/00001.json"
)


def _red() -> Redactor:
    return Redactor(token=FAKE_TOKEN, host=FAKE_HOST)


def test_redactor_removes_every_credential_shape() -> None:
    text = (
        f"Authorization: Bearer {FAKE_TOKEN}\n"
        f"url={FAKE_HOST}/api/x host=dbc-12345678-abcd.cloud.databricks.com\n"
        f'"s3.access-key-id": "{FAKE_KEY_ID}", "s3.session-token": "{FAKE_SESSION}"\n'
        f'headers: {{"x-databricks-org-id": "1234567890123456"}}\n'
        f"{FAKE_STORAGE}\n"
    )
    out = _red().apply(text)
    for secret in (FAKE_TOKEN, FAKE_HOST, "dbc-12345678", FAKE_KEY_ID, FAKE_SESSION, "1234567890123456", "dbstorage-prod-abcde", "11111111-2222"):
        assert secret not in out, secret
    assert "<managed-storage>" in out
    assert "/tables/12345678-1234-1234-1234-123456789012/" in out


def test_scan_flags_unredacted_text_and_passes_redacted_text() -> None:
    raw = f"debug dump {FAKE_TOKEN} and {FAKE_KEY_ID}"
    assert scan_for_secrets(raw, [FAKE_TOKEN]) != []
    assert scan_for_secrets(_red().apply(raw), [FAKE_TOKEN]) == []


def test_scan_catches_token_shape_even_without_the_exact_value() -> None:
    assert "token-pattern" in scan_for_secrets("x d" + "api" + "ab" * 10, [])


def test_redactor_apply_obj_round_trips_json() -> None:
    obj = {"a": [FAKE_TOKEN, {"b": FAKE_HOST}], "n": 3}
    out = _red().apply_obj(obj)
    assert out == {"a": ["<redacted-token>", {"b": "<databricks-host>"}], "n": 3}


def test_strip_credentials_blanks_secret_values_but_keeps_expiry() -> None:
    cfg = {
        "s3.access-key-id": FAKE_KEY_ID,
        "s3.secret-access-key": "x",
        "s3.session-token": FAKE_SESSION,
        "s3.session-token-expires-at-ms": "1795186190000",
        "client.region": "us-east-2",
    }
    out = strip_credentials(cfg)
    assert out["s3.access-key-id"] == "<redacted>"
    assert out["s3.secret-access-key"] == "<redacted>"
    assert out["s3.session-token"] == "<redacted>"
    assert out["s3.session-token-expires-at-ms"] == "1795186190000"
    assert out["client.region"] == "us-east-2"


def test_summarize_table_response_never_carries_credential_values() -> None:
    payload = {
        "metadata-location": FAKE_STORAGE,
        "metadata": {"format-version": 2, "current-snapshot-id": -1, "schemas": [{"schema-id": 0}]},
        "config": {"s3.access-key-id": FAKE_KEY_ID, "s3.session-token": FAKE_SESSION},
        "storage-credentials": [{"prefix": "s3://b/", "config": {"s3.session-token": FAKE_SESSION}}],
    }
    dumped = json.dumps(summarize_table_response(payload))
    assert FAKE_KEY_ID not in dumped and FAKE_SESSION not in dumped
    s = summarize_table_response(payload)
    assert s["has_s3_credentials_in_config"] is True
    assert s["storage_credentials"][0]["config_keys"] == ["s3.session-token"]
    assert summarize_table_response(None) == {"present": False}


def test_classify_identifier_column_refusal() -> None:
    log = (
        "Error: logical lifecycle\nCaused by:\n catalog: other: Unexpected, context: { status: 400 Bad Request, "
        'json: {"error":{"message":"Table with identifier columns is not allowed. [ErrorCode: 2014] '
        '(request_id: x)","type":"BadRequestException","code":400}} }'
    )
    res = classify_pg2iceberg_attempt(exit_code=1, log_text=log, snapshot_complete=False)
    assert res["outcome"] == "refused by catalog"
    assert res["uc_error_code"] == 2014
    assert res["http_status"] == 400


def test_classify_orphaned_table_refusal() -> None:
    log = "startup validation failed:\n OrphanedTables {\n tables: [\"convergence_rig.orders\"] }"
    res = classify_pg2iceberg_attempt(exit_code=1, log_text=log, snapshot_complete=False)
    assert res["outcome"] == "refused by pg2iceberg startup validation"


def test_classify_running_and_unknown() -> None:
    assert classify_pg2iceberg_attempt(exit_code=None, log_text="snapshot phase complete", snapshot_complete=True)["outcome"] == "running"
    assert classify_pg2iceberg_attempt(exit_code=2, log_text="boom", snapshot_complete=False)["outcome"] == "exited"
    assert classify_pg2iceberg_attempt(exit_code=None, log_text="", snapshot_complete=False)["outcome"] == "unknown"
    # a run that logged the snapshot but then died must not count as running
    assert classify_pg2iceberg_attempt(exit_code=1, log_text="snapshot phase complete boom", snapshot_complete=True)["outcome"] != "running"


def test_partition_tables_keeps_only_the_demo_table() -> None:
    drop, kept = partition_tables(["orders", "orders_demo", "d1_probe"], ["orders_demo"])
    assert drop == ["d1_probe", "orders"]
    assert kept == ["orders_demo"]
    assert partition_tables([], ["orders_demo"]) == ([], [])
    drop, kept = partition_tables(["orders"], ["orders_demo"])
    assert (drop, kept) == (["orders"], [])


# ---- row hashes, S3 denial and INSERT builder ------------------------------

from pathlib import Path  # noqa: E402

from lab.d1 import (  # noqa: E402
    classify_s3_denial,
    compare_row_hashes,
    demo_insert_statements,
    row_hash,
    row_hash_text,
)


def test_row_hash_text_distinguishes_null_from_empty_and_literal_marker() -> None:
    base = (1, "new", "1.500000000", 5)
    assert row_hash_text(*base, None, None, None) != row_hash_text(*base, "", None, None)
    assert row_hash_text(*base, None, None, None).endswith("<null>|<null>|<null>")
    assert row_hash(*base, "a", "{}", "0.000000001") != row_hash(*base, "a", "{}", "0.000000002")
    assert row_hash(*base, "a", "{}", None) == row_hash(*base, "a", "{}", None)


def test_compare_row_hashes_passes_on_equal_and_fails_on_each_fault() -> None:
    exp = {1: "a", 2: "b", 3: "c"}
    assert compare_row_hashes(exp, [(1, "a"), (2, "b"), (3, "c")])["passed"] is True
    missing = compare_row_hashes(exp, [(1, "a"), (2, "b")])
    assert missing["passed"] is False and missing["missing_examples"] == [3]
    extra = compare_row_hashes(exp, [(1, "a"), (2, "b"), (3, "c"), (4, "d")])
    assert extra["passed"] is False and extra["extra_examples"] == [4]
    dup = compare_row_hashes(exp, [(1, "a"), (2, "b"), (3, "c"), (3, "c")])
    assert dup["passed"] is False and dup["duplicate_examples"] == [3]
    diff = compare_row_hashes(exp, [(1, "a"), (2, "x"), (3, "c")])
    assert diff["passed"] is False and diff["differing_examples"] == [2]


def test_classify_s3_denial_reads_the_aws_message() -> None:
    msg = (
        "AccessDenied: User: <redacted-assumed-role> is not authorized to perform: s3:PutObject on resource: "
        "x with an explicit deny in a resource-based policy"
    )
    res = classify_s3_denial(msg)
    assert res == {"denied": True, "action": "s3:putobject", "explicit_deny_in_resource_policy": True}
    assert classify_s3_denial("all fine") == {"denied": False, "action": None, "explicit_deny_in_resource_policy": False}


def test_insert_builder_hex_encodes_text_and_batches(tmp_path: Path) -> None:
    rows = []
    for i in range(1, 4):
        rows.append(
            {
                "id": i,
                "status": "it's",
                "amount": "1.500000000",
                "updated_at_epoch_us": 1700000000000000 + i,
                "note": None if i == 2 else "a'b" + chr(92) + "c" + chr(10),
                "payload": "{}",
                "discount": None if i == 1 else "0.000000001",
            }
        )
    f = tmp_path / "r.jsonl"
    f.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    stmts = list(demo_insert_statements(f, "t", batch_rows=2))
    assert len(stmts) == 2
    assert all(s.startswith("INSERT INTO t SELECT ") for s in stmts)
    assert "it's" not in stmts[0]  # quotes never appear unescaped in the SQL text
    assert "'a'b" not in stmts[0]
    assert "('" + "it's".encode().hex() + "'" not in stmts[0]  # hex sits inside the tuple, not first
    assert "'" + "it's".encode().hex() + "'" in stmts[0]
    assert "FROM VALUES (1," in stmts[0] and "FROM VALUES (3," in stmts[1]
    assert "NULL" in stmts[0] and "'0.000000001'" in stmts[0]
    assert "decode(unhex(CAST(note_h AS STRING)), 'UTF-8')" in stmts[0]


def test_redactor_covers_aws_role_arn_and_bucket_arn() -> None:
    text = (
        "User: arn:aws:sts::123456789012:assumed-role/dbrole-prod-xyz/m=AbC,s=Def is not authorized on "
        "arn:aws:s3:::dbstorage-prod-abcde/uc/11111111-2222-3333-4444-555555555555/"
        "66666666-7777-8888-9999-000000000000/__unitystorage/catalogs/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/tables/t"
    )
    out = _red().apply(text)
    assert "123456789012" not in out and "dbstorage-prod-abcde" not in out and "11111111-2222" not in out
    assert "arn:aws:s3:::<managed-storage>/tables/t" in out


def test_classify_s3_denial_recognises_a_bare_403_from_the_aws_sdk() -> None:
    msg = "software.amazon.awssdk.services.s3.model.S3Exception: Forbidden (Service: S3, Status Code: 403, Request ID: x)"
    res = classify_s3_denial(msg)
    assert res["denied"] is True and res["explicit_deny_in_resource_policy"] is False
    assert classify_s3_denial("S3Exception: Not Found (Service: S3, Status Code: 404)")["denied"] is False
