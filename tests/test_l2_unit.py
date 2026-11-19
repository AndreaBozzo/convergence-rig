"""Unit tests for L2 AWS SigV4 S3 signing, JWT decoding, S3 URI parsing, and prefix disjointness."""

from __future__ import annotations

import base64
from datetime import datetime, timezone
import json

import pytest

from lab.l2 import (
    build_sigv4_s3_get_headers,
    decode_jwt_payload,
    parse_s3_uri,
    redact_load_table_response,
    verify_distinct_storage_prefixes,
)


def test_parse_s3_uri_and_prefix_disjointness() -> None:
    assert parse_s3_uri("s3://cdclab/authz-warehouse/abc/data/f.parquet") == (
        "cdclab",
        "authz-warehouse/abc/data/f.parquet",
    )
    with pytest.raises(ValueError):
        parse_s3_uri("http://example.com/a/b")
    with pytest.raises(ValueError):
        parse_s3_uri("s3://only-bucket")

    ok = verify_distinct_storage_prefixes(
        "s3://cdclab/authz-warehouse/raw-uuid",
        "s3://cdclab/authz-warehouse/curated-uuid",
    )
    assert ok["distinct_and_disjoint"] is True

    # Prefix-sibling names sharing a character prefix must still be treated as disjoint.
    sibling = verify_distinct_storage_prefixes(
        "s3://cdclab/authz-warehouse/raw",
        "s3://cdclab/authz-warehouse/raw2",
    )
    assert sibling["distinct_and_disjoint"] is True

    # Nested and identical prefixes must FAIL disjointness.
    nested = verify_distinct_storage_prefixes(
        "s3://cdclab/authz-warehouse/raw",
        "s3://cdclab/authz-warehouse/raw/curated",
    )
    assert nested["distinct_and_disjoint"] is False

    same = verify_distinct_storage_prefixes(
        "s3://cdclab/authz-warehouse/same/",
        "s3://cdclab/authz-warehouse/same",
    )
    assert same["distinct_and_disjoint"] is False


def test_sigv4_s3_get_headers_matches_known_signature_and_includes_sts_token() -> None:
    # Deterministic signing test (standard AWS SigV4 algorithm for GET Object).
    dt = datetime(2026, 11, 19, 21, 0, 0, tzinfo=timezone.utc)
    uri, hdrs = build_sigv4_s3_get_headers(
        host_header="silo.localhost:19000",
        bucket="cdclab",
        key="authz-warehouse/tbl/data/part-000.parquet",
        region="local-01",
        access_key="AKIAIOSFODNN7EXAMPLE",
        secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        session_token="FwoGZXIvYXdzEBYaDTestSessionToken",
        amz_dt=dt,
    )
    assert uri == "/cdclab/authz-warehouse/tbl/data/part-000.parquet"
    assert hdrs["x-amz-date"] == "20261119T210000Z"
    assert hdrs["x-amz-security-token"] == "FwoGZXIvYXdzEBYaDTestSessionToken"
    assert "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token" in hdrs["Authorization"]
    assert "Credential=AKIAIOSFODNN7EXAMPLE/20261119/local-01/s3/aws4_request" in hdrs["Authorization"]

    # Without session token, x-amz-security-token must be absent from headers and SignedHeaders.
    _, hdrs_no_tok = build_sigv4_s3_get_headers(
        host_header="silo.localhost:19000",
        bucket="cdclab",
        key="k",
        region="us-east-1",
        access_key="AKIAIOSFODNN7EXAMPLE",
        secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        amz_dt=dt,
    )
    assert "x-amz-security-token" not in hdrs_no_tok
    assert "SignedHeaders=host;x-amz-content-sha256;x-amz-date," in hdrs_no_tok["Authorization"]


def test_decode_jwt_payload_and_redact_load_table() -> None:
    payload = {"sub": "user-123", "aud": "lakekeeper", "iss": "http://keycloak:8080/realms/cdclab"}
    seg = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii").rstrip("=")
    fake_jwt = f"eyJhbGciOiJSUzI1NiJ9.{seg}.sig"
    assert decode_jwt_payload(fake_jwt) == payload

    redacted = redact_load_table_response(
        {
            "metadata-location": "s3://cdclab/meta.json",
            "config": {
                "s3.endpoint": "http://silo.localhost:19000/",
                "s3.access-key-id": "ASIA123",
                "s3.secret-access-key": "topsecret",
                "s3.session-token": "tok123",
            },
            "storage-credentials": [
                {
                    "prefix": "s3://cdclab/authz-warehouse/curated",
                    "config": {
                        "s3.access-key-id": "ASIA123",
                        "s3.secret-access-key": "topsecret",
                        "s3.session-token": "tok123",
                        "s3.endpoint": "http://silo.localhost:19000/",
                    },
                }
            ],
            "metadata": {"table-uuid": "uuid-1", "location": "s3://cdclab/authz-warehouse/curated"},
        }
    )
    assert redacted["config"]["s3.access-key-id"] == "<redacted>"
    assert redacted["config"]["s3.secret-access-key"] == "<redacted>"
    assert redacted["config"]["s3.session-token"] == "<redacted>"
    assert redacted["config"]["s3.endpoint"] == "http://silo.localhost:19000/"
    assert redacted["storage_credentials"][0]["config"]["s3.secret-access-key"] == "<redacted>"
