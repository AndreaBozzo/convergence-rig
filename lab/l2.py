"""Milestone L2 end-to-end runner: Lakekeeper OpenFGA + Keycloak OIDC scoped access & vended STS checks."""

from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import hmac
import json
import os
from pathlib import Path
import platform
import sys
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

import duckdb

from lab.bootstrap import ensure_bucket
from lab.config import (
    DEFAULT_ENV_FILE,
    LOCAL_ENV_FILE,
    LabConfig,
    REPO_ROOT,
    load_config,
    parse_env_file,
)
from lab.l_common import (
    ALL_LAB_PROFILES,
    collect_digests_for_profiles,
    compose_cmd_with_profiles,
    project_teardown_and_verify,
)
from lab.pipeline import (
    json_default,
    list_non_project_containers,
    list_project_resources,
    run_cmd,
    utc_now_iso,
    utc_run_stamp,
    write_json,
)

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


@dataclass(frozen=True)
class AuthzConfig:
    keycloak_port: int
    keycloak_realm: str
    lakekeeper_authz_port: int
    lakekeeper_authz_warehouse: str
    admin_client_id: str
    admin_client_secret: str
    writer_client_id: str
    writer_client_secret: str
    reader_client_id: str
    reader_client_secret: str

    @property
    def host_token_url(self) -> str:
        return (
            f"http://127.0.0.1:{self.keycloak_port}"
            f"/realms/{self.keycloak_realm}/protocol/openid-connect/token"
        )

    @property
    def container_token_url(self) -> str:
        return (
            f"http://keycloak:8080/realms/{self.keycloak_realm}/protocol/openid-connect/token"
        )

    @property
    def host_lakekeeper_base_url(self) -> str:
        return f"http://127.0.0.1:{self.lakekeeper_authz_port}"

    @property
    def host_lakekeeper_catalog_url(self) -> str:
        return f"http://127.0.0.1:{self.lakekeeper_authz_port}/catalog"

    @property
    def container_lakekeeper_catalog_url(self) -> str:
        return "http://lakekeeper-authz:8181/catalog"

    def redacted_summary(self) -> dict[str, Any]:
        return {
            "keycloak_port": self.keycloak_port,
            "keycloak_realm": self.keycloak_realm,
            "host_token_url": self.host_token_url,
            "container_token_url": self.container_token_url,
            "lakekeeper_authz_port": self.lakekeeper_authz_port,
            "host_lakekeeper_base_url": self.host_lakekeeper_base_url,
            "host_lakekeeper_catalog_url": self.host_lakekeeper_catalog_url,
            "container_lakekeeper_catalog_url": self.container_lakekeeper_catalog_url,
            "lakekeeper_authz_warehouse": self.lakekeeper_authz_warehouse,
            "admin_client_id": self.admin_client_id,
            "writer_client_id": self.writer_client_id,
            "reader_client_id": self.reader_client_id,
        }


def load_authz_config(env_file: Path | None = None) -> AuthzConfig:
    primary = env_file or DEFAULT_ENV_FILE
    merged = parse_env_file(primary)
    if env_file is None and LOCAL_ENV_FILE.exists():
        merged.update(parse_env_file(LOCAL_ENV_FILE))
    for key in list(merged.keys()):
        if key in os.environ:
            merged[key] = os.environ[key]
    return AuthzConfig(
        keycloak_port=int(merged.get("KEYCLOAK_PORT", "18080")),
        keycloak_realm=merged.get("KEYCLOAK_REALM", "cdclab"),
        lakekeeper_authz_port=int(merged.get("LAKEKEEPER_AUTHZ_PORT", "18182")),
        lakekeeper_authz_warehouse=merged.get("LAKEKEEPER_AUTHZ_WAREHOUSE", "cdclab_authz"),
        admin_client_id=merged.get("AUTHZ_ADMIN_CLIENT_ID", "cdclab-admin"),
        admin_client_secret=merged.get("AUTHZ_ADMIN_CLIENT_SECRET", "cdclab-admin-secret"),
        writer_client_id=merged.get("AUTHZ_WRITER_CLIENT_ID", "cdclab-writer"),
        writer_client_secret=merged.get("AUTHZ_WRITER_CLIENT_SECRET", "cdclab-writer-secret"),
        reader_client_id=merged.get("AUTHZ_READER_CLIENT_ID", "cdclab-reader"),
        reader_client_secret=merged.get("AUTHZ_READER_CLIENT_SECRET", "cdclab-reader-secret"),
    )


def decode_jwt_payload(token: str) -> dict[str, Any]:
    """Decode the payload segment of a JWT without signature verification (for inspecting sub/aud/iss)."""
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected 3-segment JWT, got {len(parts)} segments")
    padded = parts[1] + "=" * ((4 - len(parts[1]) % 4) % 4)
    raw = base64.urlsafe_b64decode(padded.encode("ascii"))
    return json.loads(raw.decode("utf-8"))


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an s3://bucket/key URI into (bucket, key)."""
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got {uri!r}")
    rest = uri[len("s3://") :]
    if "/" not in rest:
        raise ValueError(f"S3 URI missing object key: {uri!r}")
    bucket, key = rest.split("/", 1)
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {uri!r}")
    return bucket, key


def verify_distinct_storage_prefixes(raw_location: str, curated_location: str) -> dict[str, Any]:
    """Verify two table storage locations are non-empty, disjoint s3:// prefixes."""
    raw_norm = raw_location.rstrip("/")
    cur_norm = curated_location.rstrip("/")
    raw_bucket, raw_prefix = parse_s3_uri(raw_norm)
    cur_bucket, cur_prefix = parse_s3_uri(cur_norm)
    overlapping = raw_bucket == cur_bucket and (
        raw_prefix == cur_prefix
        or raw_prefix.startswith(cur_prefix + "/")
        or cur_prefix.startswith(raw_prefix + "/")
    )
    return {
        "raw_location": raw_norm,
        "curated_location": cur_norm,
        "raw_bucket": raw_bucket,
        "raw_prefix": raw_prefix,
        "curated_bucket": cur_bucket,
        "curated_prefix": cur_prefix,
        "distinct_and_disjoint": not overlapping,
    }


def _hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def build_sigv4_s3_get_headers(
    *,
    host_header: str,
    bucket: str,
    key: str,
    region: str,
    access_key: str,
    secret_key: str,
    session_token: str | None = None,
    amz_dt: datetime | None = None,
) -> tuple[str, dict[str, str]]:
    """Build canonical path and AWS SigV4 signed headers for a path-style S3 GET request."""
    now = (amz_dt or datetime.now(timezone.utc)).astimezone(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    canonical_uri = urllib.parse.quote(f"/{bucket}/{key.lstrip('/')}", safe="/-_.~")
    headers_to_sign: dict[str, str] = {
        "host": host_header,
        "x-amz-content-sha256": EMPTY_SHA256,
        "x-amz-date": amz_date,
    }
    if session_token:
        headers_to_sign["x-amz-security-token"] = session_token

    sorted_keys = sorted(headers_to_sign.keys())
    canonical_headers = "".join(f"{k}:{headers_to_sign[k].strip()}\n" for k in sorted_keys)
    signed_headers = ";".join(sorted_keys)

    canonical_request = "\n".join(
        [
            "GET",
            canonical_uri,
            "",
            canonical_headers,
            signed_headers,
            EMPTY_SHA256,
        ]
    )
    credential_scope = f"{date_stamp}/{region}/s3/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )

    k_date = _hmac_sha256(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = _hmac_sha256(k_date, region)
    k_service = _hmac_sha256(k_region, "s3")
    k_signing = _hmac_sha256(k_service, "aws4_request")
    signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    authorization = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )

    out_headers = {
        "Host": host_header,
        "x-amz-content-sha256": EMPTY_SHA256,
        "x-amz-date": amz_date,
        "Authorization": authorization,
    }
    if session_token:
        out_headers["x-amz-security-token"] = session_token
    return canonical_uri, out_headers


def sigv4_s3_get(
    *,
    endpoint: str,
    s3_uri: str,
    region: str,
    access_key: str,
    secret_key: str,
    session_token: str | None = None,
    timeout: float = 15.0,
) -> dict[str, Any]:
    """Execute a real AWS SigV4 signed GET against Silo/S3 and return status/body evidence."""
    bucket, key = parse_s3_uri(s3_uri)
    parsed_ep = urllib.parse.urlparse(endpoint.rstrip("/"))
    host_header = parsed_ep.netloc
    canonical_uri, headers = build_sigv4_s3_get_headers(
        host_header=host_header,
        bucket=bucket,
        key=key,
        region=region,
        access_key=access_key,
        secret_key=secret_key,
        session_token=session_token,
    )
    url = f"{parsed_ep.scheme}://{host_header}{canonical_uri}"
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            parquet_magic = len(body) >= 8 and body[:4] == b"PAR1" and body[-4:] == b"PAR1"
            return {
                "url": url,
                "s3_uri": s3_uri,
                "http_status": resp.status,
                "allowed": True,
                "body_bytes": len(body),
                "parquet_magic_valid": parquet_magic,
                "body_sha256": hashlib.sha256(body).hexdigest(),
                "error_body": None,
                "content_type": resp.headers.get("Content-Type"),
            }
    except urllib.error.HTTPError as exc:
        err_text = exc.read().decode("utf-8", errors="replace")
        return {
            "url": url,
            "s3_uri": s3_uri,
            "http_status": exc.code,
            "allowed": False,
            "body_bytes": len(err_text.encode("utf-8")),
            "parquet_magic_valid": False,
            "body_sha256": None,
            "error_body": err_text,
            "content_type": exc.headers.get("Content-Type") if exc.headers else None,
        }


def _http_json_raw(
    url: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 15.0,
) -> tuple[int, Any, str]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req_headers = {"Accept": "application/json"}
    if data is not None:
        req_headers["Content-Type"] = "application/json"
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, (json.loads(raw) if raw.strip() else None), raw
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            parsed: Any = json.loads(raw) if raw.strip() else None
        except json.JSONDecodeError:
            parsed = raw
        return exc.code, parsed, raw


def fetch_oidc_token(
    token_url: str,
    client_id: str,
    client_secret: str,
    *,
    scope: str = "lakekeeper",
    timeout: float = 15.0,
) -> tuple[str, dict[str, Any]]:
    """Request an OAuth2 client_credentials access token from Keycloak and return (token, claims)."""
    form = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        token_url,
        data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    access_token = payload["access_token"]
    claims = decode_jwt_payload(access_token)
    return access_token, claims


def redact_load_table_response(payload: dict[str, Any]) -> dict[str, Any]:
    """Redact temporary STS credentials from a Lakekeeper LoadTable JSON payload before saving."""
    redacted_cfg: dict[str, Any] = {}
    for k, v in (payload.get("config") or {}).items():
        if any(tok in k.lower() for tok in ("secret", "token", "key-id")):
            redacted_cfg[k] = "<redacted>" if v else ""
        else:
            redacted_cfg[k] = v
    redacted_sc = []
    for entry in payload.get("storage-credentials") or []:
        c_cfg = {}
        for k, v in (entry.get("config") or {}).items():
            if any(tok in k.lower() for tok in ("secret", "token", "key-id")):
                c_cfg[k] = "<redacted>" if v else ""
            else:
                c_cfg[k] = v
        redacted_sc.append({"prefix": entry.get("prefix"), "config": c_cfg})
    return {
        "metadata_location": payload.get("metadata-location"),
        "config": redacted_cfg,
        "storage_credentials": redacted_sc,
        "table_uuid": (payload.get("metadata") or {}).get("table-uuid"),
        "location": (payload.get("metadata") or {}).get("location"),
        "current_snapshot_id": (payload.get("metadata") or {}).get("current-snapshot-id"),
        "format_version": (payload.get("metadata") or {}).get("format-version"),
    }


def extract_vended_s3_credentials(load_table_payload: dict[str, Any]) -> dict[str, Any]:
    """Extract live STS credentials and endpoint from a Lakekeeper LoadTable response."""
    cfg = load_table_payload.get("config") or {}
    sc_list = load_table_payload.get("storage-credentials") or []
    sc_cfg = (sc_list[0].get("config") or {}) if sc_list else {}
    sc_prefix = sc_list[0].get("prefix") if sc_list else None

    def pick(key: str) -> str | None:
        return sc_cfg.get(key) or cfg.get(key)

    access_key = pick("s3.access-key-id")
    secret_key = pick("s3.secret-access-key")
    session_token = pick("s3.session-token")
    endpoint = pick("s3.endpoint")
    region = pick("client.region") or pick("s3.region") or "local-01"
    if not access_key or not secret_key:
        raise RuntimeError(
            f"LoadTable response did not include vended S3 credentials: "
            f"{redact_load_table_response(load_table_payload)!r}"
        )
    return {
        "prefix": sc_prefix,
        "endpoint": endpoint,
        "region": region,
        "access_key": access_key,
        "secret_key": secret_key,
        "session_token": session_token,
    }


def rest_load_table(
    catalog_url: str,
    warehouse: str,
    namespace: str,
    table: str,
    access_token: str,
    *,
    request_vended_credentials: bool = True,
) -> dict[str, Any]:
    """Call Lakekeeper /v1/config then Iceberg REST LoadTable as an authenticated identity."""
    auth_headers = {"Authorization": f"Bearer {access_token}"}
    wh_q = urllib.parse.urlencode({"warehouse": warehouse})
    cfg_url = f"{catalog_url}/v1/config?{wh_q}"
    cfg_status, cfg_body, cfg_raw = _http_json_raw(cfg_url, headers=auth_headers)
    if cfg_status != 200 or not isinstance(cfg_body, dict):
        return {
            "stage": "catalog_config",
            "url": cfg_url,
            "http_status": cfg_status,
            "allowed": False,
            "body": cfg_body,
            "raw_body": cfg_raw,
        }

    prefix = (cfg_body.get("overrides") or {}).get("prefix") or (
        cfg_body.get("defaults") or {}
    ).get("prefix")
    table_url = (
        f"{catalog_url}/v1/{urllib.parse.quote(str(prefix), safe='')}"
        f"/namespaces/{urllib.parse.quote(namespace, safe='')}"
        f"/tables/{urllib.parse.quote(table, safe='')}"
    )
    tbl_headers = dict(auth_headers)
    if request_vended_credentials:
        tbl_headers["X-Iceberg-Access-Delegation"] = "vended-credentials"
    tbl_status, tbl_body, tbl_raw = _http_json_raw(table_url, headers=tbl_headers)
    return {
        "stage": "load_table",
        "url": table_url,
        "prefix": prefix,
        "http_status": tbl_status,
        "allowed": tbl_status == 200,
        "body": tbl_body,
        "raw_body": tbl_raw if tbl_status != 200 else None,
    }


def rest_attempt_table_commit(
    catalog_url: str,
    prefix: str,
    namespace: str,
    table: str,
    access_token: str,
) -> dict[str, Any]:
    """Attempt a no-op Iceberg REST table commit to prove write authorization enforcement."""
    url = (
        f"{catalog_url}/v1/{urllib.parse.quote(prefix, safe='')}"
        f"/namespaces/{urllib.parse.quote(namespace, safe='')}"
        f"/tables/{urllib.parse.quote(table, safe='')}"
    )
    status, body, raw = _http_json_raw(
        url,
        method="POST",
        headers={"Authorization": f"Bearer {access_token}"},
        body={
            "requirements": [],
            "updates": [{"action": "set-properties", "updates": {"cdclab.reader_write_attempt": "1"}}],
        },
    )
    return {
        "url": url,
        "http_status": status,
        "allowed": 200 <= status < 300,
        "body": body,
        "raw_body": raw,
    }


def duckdb_scan_authz_table(
    authz_cfg: AuthzConfig,
    *,
    client_id: str,
    client_secret: str,
    namespace: str,
    table: str,
) -> dict[str, Any]:
    """Authenticate DuckDB to Lakekeeper via OAuth2 client_credentials and scan namespace.table."""
    con = duckdb.connect()
    try:
        for ext in ("iceberg", "httpfs"):
            try:
                con.execute(f"LOAD {ext};")
            except Exception:
                con.execute(f"INSTALL {ext};")
                con.execute(f"LOAD {ext};")

        con.execute(
            f"""
            CREATE SECRET lk_authz_secret (
                TYPE ICEBERG,
                CLIENT_ID '{client_id}',
                CLIENT_SECRET '{client_secret}',
                OAUTH2_SCOPE 'lakekeeper',
                OAUTH2_SERVER_URI '{authz_cfg.host_token_url}'
            );
            """
        )
        con.execute(
            f"""
            ATTACH '{authz_cfg.lakekeeper_authz_warehouse}' AS lk_authz (
                TYPE iceberg,
                ENDPOINT '{authz_cfg.host_lakekeeper_catalog_url}',
                SECRET lk_authz_secret
            );
            """
        )
        cur = con.execute(f"SELECT * FROM lk_authz.{namespace}.{table} ORDER BY id")
        cols = [d[0] for d in cur.description]
        rows = [
            {
                col: (format(val, "f") if isinstance(val, Decimal) else val)
                for col, val in zip(cols, row)
            }
            for row in cur.fetchall()
        ]
        return {
            "client_id": client_id,
            "target": f"{namespace}.{table}",
            "allowed": True,
            "row_count": len(rows),
            "rows": rows,
            "error": None,
        }
    except Exception as exc:
        return {
            "client_id": client_id,
            "target": f"{namespace}.{table}",
            "allowed": False,
            "row_count": 0,
            "rows": [],
            "error": f"{type(exc).__name__}: {exc}",
        }
    finally:
        con.close()


def _run_spark_writer(
    cfg: LabConfig,
    authz_cfg: AuthzConfig,
    logged_cmd,
    *,
    mode: str,
    out_container_path: str,
    out_host_path: Path,
) -> dict[str, Any]:
    proc = logged_cmd(
        [
            *compose_cmd_with_profiles(cfg, ("spark",)),
            "exec",
            "-T",
            "spark",
            "/opt/spark/bin/spark-submit",
            "--master",
            "local[2]",
            "/opt/cdclab/scripts/authz_writer.py",
            "--mode",
            mode,
            "--catalog-name",
            authz_cfg.lakekeeper_authz_warehouse,
            "--catalog-uri",
            authz_cfg.container_lakekeeper_catalog_url,
            "--warehouse",
            authz_cfg.lakekeeper_authz_warehouse,
            "--oauth2-server-uri",
            authz_cfg.container_token_url,
            "--client-id",
            authz_cfg.writer_client_id,
            "--client-secret",
            authz_cfg.writer_client_secret,
            "--oauth2-scope",
            "lakekeeper",
            "--out-json",
            out_container_path,
        ]
    )
    out_host_path.with_suffix(".stdout.log").write_text(proc.stdout, encoding="utf-8")
    out_host_path.with_suffix(".stderr.log").write_text(proc.stderr, encoding="utf-8")
    raw = run_cmd(
        [*compose_cmd_with_profiles(cfg, ("spark",)), "exec", "-T", "spark", "cat", out_container_path]
    ).stdout
    out_host_path.write_text(raw, encoding="utf-8")
    return json.loads(raw)


def run_l2(
    *,
    label: str = "l2-authz-scoped-access",
    keep_up: bool = False,
) -> Path:
    """Execute Milestone L2 end-to-end on a fresh cdclab stack with authz + spark profiles."""
    cfg = load_config()
    authz_cfg = load_authz_config()
    run_dir = REPO_ROOT / "runs" / f"{utc_run_stamp()}-{label}"
    run_dir.mkdir(parents=True, exist_ok=True)

    commands_executed: list[str] = []
    non_project_before = list_non_project_containers(cfg.compose_project)

    def logged_cmd(cmd: list[str], *, check: bool = True) -> Any:
        # Redact client secrets if present in logged command strings.
        safe_parts: list[str] = []
        skip_next = False
        for part in cmd:
            if skip_next:
                safe_parts.append("<redacted>")
                skip_next = False
                continue
            safe_parts.append(part)
            if part == "--client-secret":
                skip_next = True
        commands_executed.append(" ".join(safe_parts))
        return run_cmd(cmd, check=check)

    # 1. Clean fresh start across all lab profiles.
    logged_cmd([*compose_cmd_with_profiles(cfg, ALL_LAB_PROFILES), "down", "-v", "--remove-orphans"])
    resources_after_pre_clean = list_project_resources(cfg.compose_project)

    # Start Silo + authz profile services (Keycloak, OpenFGA, Lakekeeper-authz) + Spark writer.
    logged_cmd(
        [
            *compose_cmd_with_profiles(cfg, ("authz", "spark")),
            "up",
            "-d",
            "--wait",
            "silo",
            "keycloak",
            "openfga",
            "lakekeeper-authz",
            "spark",
        ]
    )
    digests = collect_digests_for_profiles(cfg, ("authz", "spark"))

    # 2. Ensure Silo bucket exists.
    bucket_res = ensure_bucket(cfg)

    # 3. Acquire distinct OIDC tokens from Keycloak for admin, writer, and reader.
    admin_token, admin_claims = fetch_oidc_token(
        authz_cfg.host_token_url, authz_cfg.admin_client_id, authz_cfg.admin_client_secret
    )
    writer_token, writer_claims = fetch_oidc_token(
        authz_cfg.host_token_url, authz_cfg.writer_client_id, authz_cfg.writer_client_secret
    )
    reader_token, reader_claims = fetch_oidc_token(
        authz_cfg.host_token_url, authz_cfg.reader_client_id, authz_cfg.reader_client_secret
    )

    admin_user_id = f"oidc~{admin_claims['sub']}"
    writer_user_id = f"oidc~{writer_claims['sub']}"
    reader_user_id = f"oidc~{reader_claims['sub']}"
    if len({admin_user_id, writer_user_id, reader_user_id}) != 3:
        raise RuntimeError(
            f"Expected 3 distinct OIDC identities, got admin={admin_user_id}, "
            f"writer={writer_user_id}, reader={reader_user_id}"
        )

    identities_evidence = {
        "admin": {
            "client_id": authz_cfg.admin_client_id,
            "lakekeeper_user_id": admin_user_id,
            "iss": admin_claims.get("iss"),
            "aud": admin_claims.get("aud"),
            "preferred_username": admin_claims.get("preferred_username"),
        },
        "writer": {
            "client_id": authz_cfg.writer_client_id,
            "lakekeeper_user_id": writer_user_id,
            "iss": writer_claims.get("iss"),
            "aud": writer_claims.get("aud"),
            "preferred_username": writer_claims.get("preferred_username"),
        },
        "reader": {
            "client_id": authz_cfg.reader_client_id,
            "lakekeeper_user_id": reader_user_id,
            "iss": reader_claims.get("iss"),
            "aud": reader_claims.get("aud"),
            "preferred_username": reader_claims.get("preferred_username"),
        },
    }

    mgmt_url = f"{authz_cfg.host_lakekeeper_base_url}/management/v1"
    admin_hdrs = {"Authorization": f"Bearer {admin_token}"}

    # 4. Bootstrap Lakekeeper with OpenFGA enabled as admin.
    info_pre_status, info_pre, _ = _http_json_raw(f"{mgmt_url}/info", headers=admin_hdrs)
    if info_pre_status != 200 or not isinstance(info_pre, dict):
        raise RuntimeError(f"Lakekeeper authz /info failed: {info_pre_status} {info_pre!r}")

    boot_status, boot_body, _ = _http_json_raw(
        f"{mgmt_url}/bootstrap",
        method="POST",
        headers=admin_hdrs,
        body={"accept-terms-of-use": True},
    )
    if boot_status not in (200, 204):
        raise RuntimeError(f"Lakekeeper authz bootstrap failed: {boot_status} {boot_body!r}")

    info_post_status, info_post, _ = _http_json_raw(f"{mgmt_url}/info", headers=admin_hdrs)
    if info_post_status != 200 or not isinstance(info_post, dict) or not info_post.get("bootstrapped"):
        raise RuntimeError(f"Lakekeeper authz not bootstrapped: {info_post_status} {info_post!r}")

    # Provision writer and reader users in Lakekeeper.
    for role_name, tok in (("writer", writer_token), ("reader", reader_token)):
        u_status, u_body, _ = _http_json_raw(
            f"{mgmt_url}/user",
            method="POST",
            headers={"Authorization": f"Bearer {tok}"},
            body={"update-if-exists": True},
        )
        if u_status not in (200, 201, 204):
            raise RuntimeError(f"Failed to register {role_name} user: {u_status} {u_body!r}")

    # 5. Create warehouse cdclab_authz pointing at Silo with STS vended credentials.
    wh_payload = {
        "warehouse-name": authz_cfg.lakekeeper_authz_warehouse,
        "storage-profile": {
            "type": "s3",
            "bucket": cfg.silo_bucket,
            "key-prefix": "authz-warehouse",
            "assume-role-arn": None,
            "endpoint": cfg.silo_advertised_endpoint,
            "sts-endpoint": cfg.silo_advertised_endpoint,
            "region": cfg.silo_region,
            "path-style-access": True,
            "flavor": "s3-compat",
            "sts-enabled": True,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "access-key-id": cfg.silo_root_user,
            "secret-access-key": cfg.silo_root_password,
        },
    }
    wh_status, wh_created, _ = _http_json_raw(
        f"{mgmt_url}/warehouse", method="POST", headers=admin_hdrs, body=wh_payload
    )
    if wh_status not in (200, 201) or not isinstance(wh_created, dict):
        raise RuntimeError(f"Failed to create authz warehouse: {wh_status} {wh_created!r}")
    warehouse_id = str(wh_created.get("warehouse-id") or wh_created.get("id"))

    # 6. Grant writer create + modify on the warehouse (so writer can create & write both tables).
    writer_grant_body = {
        "writes": [
            {"type": "create", "user": writer_user_id},
            {"type": "modify", "user": writer_user_id},
        ]
    }
    wg_status, wg_body, _ = _http_json_raw(
        f"{mgmt_url}/permissions/warehouse/{warehouse_id}/assignments",
        method="POST",
        headers=admin_hdrs,
        body=writer_grant_body,
    )
    if wg_status not in (200, 204):
        raise RuntimeError(f"Failed to grant warehouse permissions to writer: {wg_status} {wg_body!r}")

    # 7. Writer creates raw.orders and curated.orders in distinct prefixes and writes initial rows.
    writer_init = _run_spark_writer(
        cfg,
        authz_cfg,
        logged_cmd,
        mode="init",
        out_container_path="/tmp/cdclab-spark/l2_init.json",
        out_host_path=run_dir / "spark_writer_init.json",
    )

    # Load both tables as writer to inspect exact metadata/table-uuid and confirm writer access.
    writer_raw_load = rest_load_table(
        authz_cfg.host_lakekeeper_catalog_url,
        authz_cfg.lakekeeper_authz_warehouse,
        "raw",
        "orders",
        writer_token,
    )
    writer_curated_load = rest_load_table(
        authz_cfg.host_lakekeeper_catalog_url,
        authz_cfg.lakekeeper_authz_warehouse,
        "curated",
        "orders",
        writer_token,
    )
    if not writer_raw_load["allowed"] or not writer_curated_load["allowed"]:
        raise RuntimeError(
            f"Writer could not load its own tables: raw={writer_raw_load['http_status']} "
            f"curated={writer_curated_load['http_status']}"
        )

    raw_meta = writer_raw_load["body"]["metadata"]
    curated_meta = writer_curated_load["body"]["metadata"]
    raw_table_uuid = str(raw_meta["table-uuid"])
    curated_table_uuid = str(curated_meta["table-uuid"])
    raw_location = str(raw_meta["location"])
    curated_location = str(curated_meta["location"])
    prefix_check = verify_distinct_storage_prefixes(raw_location, curated_location)
    if not prefix_check["distinct_and_disjoint"]:
        raise RuntimeError(f"Table storage prefixes are not disjoint: {prefix_check}")

    raw_data_file_uri = str(writer_init["raw"]["data_files"][0]["file_path"])
    curated_data_file_uri = str(writer_init["curated"]["data_files"][0]["file_path"])

    # 8. Grant reader SELECT on curated.orders ONLY (zero grants on raw namespace or raw.orders).
    reader_grant_body = {
        "writes": [
            {"type": "select", "user": reader_user_id},
        ]
    }
    rg_status, rg_body, _ = _http_json_raw(
        f"{mgmt_url}/permissions/warehouse/{warehouse_id}/table/{curated_table_uuid}/assignments",
        method="POST",
        headers=admin_hdrs,
        body=reader_grant_body,
    )
    if rg_status not in (200, 204):
        raise RuntimeError(
            f"Failed to grant select on curated.orders to reader: {rg_status} {rg_body!r}"
        )

    # 9. Check 1: Reader can load and scan curated (REST LoadTable + DuckDB scan + positive S3 control).
    reader_curated_load = rest_load_table(
        authz_cfg.host_lakekeeper_catalog_url,
        authz_cfg.lakekeeper_authz_warehouse,
        "curated",
        "orders",
        reader_token,
    )
    if not reader_curated_load["allowed"]:
        raise RuntimeError(f"Reader failed to load curated.orders: {reader_curated_load}")
    reader_curated_creds = extract_vended_s3_credentials(reader_curated_load["body"])
    writer_raw_creds = extract_vended_s3_credentials(writer_raw_load["body"])

    reader_duckdb_curated = duckdb_scan_authz_table(
        authz_cfg,
        client_id=authz_cfg.reader_client_id,
        client_secret=authz_cfg.reader_client_secret,
        namespace="curated",
        table="orders",
    )
    reader_s3_curated_positive = sigv4_s3_get(
        endpoint=reader_curated_creds["endpoint"] or cfg.silo_advertised_endpoint,
        s3_uri=curated_data_file_uri,
        region=reader_curated_creds["region"],
        access_key=reader_curated_creds["access_key"],
        secret_key=reader_curated_creds["secret_key"],
        session_token=reader_curated_creds["session_token"],
    )

    # 10. Check 2: Catalog denies loading raw to reader (REST + DuckDB), while writer succeeds.
    reader_raw_load = rest_load_table(
        authz_cfg.host_lakekeeper_catalog_url,
        authz_cfg.lakekeeper_authz_warehouse,
        "raw",
        "orders",
        reader_token,
    )
    reader_duckdb_raw = duckdb_scan_authz_table(
        authz_cfg,
        client_id=authz_cfg.reader_client_id,
        client_secret=authz_cfg.reader_client_secret,
        namespace="raw",
        table="orders",
    )

    # 11. Check 3: Reader's vended credentials CANNOT fetch known raw data file directly from Silo,
    #     while writer's vended credentials CAN fetch that exact same raw data file.
    reader_s3_raw_denied = sigv4_s3_get(
        endpoint=reader_curated_creds["endpoint"] or cfg.silo_advertised_endpoint,
        s3_uri=raw_data_file_uri,
        region=reader_curated_creds["region"],
        access_key=reader_curated_creds["access_key"],
        secret_key=reader_curated_creds["secret_key"],
        session_token=reader_curated_creds["session_token"],
    )
    writer_s3_raw_allowed = sigv4_s3_get(
        endpoint=writer_raw_creds["endpoint"] or cfg.silo_advertised_endpoint,
        s3_uri=raw_data_file_uri,
        region=writer_raw_creds["region"],
        access_key=writer_raw_creds["access_key"],
        secret_key=writer_raw_creds["secret_key"],
        session_token=writer_raw_creds["session_token"],
    )

    # 12. Check 4: Writer can still write BOTH raw and curated (after reader grants are active),
    #     reader sees the new curated row, reader cannot commit to curated, and reader still cannot
    #     fetch the newly appended raw Parquet file.
    writer_append = _run_spark_writer(
        cfg,
        authz_cfg,
        logged_cmd,
        mode="append",
        out_container_path="/tmp/cdclab-spark/l2_append.json",
        out_host_path=run_dir / "spark_writer_append.json",
    )
    reader_duckdb_curated_after_append = duckdb_scan_authz_table(
        authz_cfg,
        client_id=authz_cfg.reader_client_id,
        client_secret=authz_cfg.reader_client_secret,
        namespace="curated",
        table="orders",
    )
    reader_curated_write_denied = rest_attempt_table_commit(
        authz_cfg.host_lakekeeper_catalog_url,
        str(reader_curated_load["prefix"]),
        "curated",
        "orders",
        reader_token,
    )

    new_raw_files = [
        str(f["file_path"])
        for f in writer_append["raw"]["data_files"]
        if str(f["file_path"]) != raw_data_file_uri
    ]
    new_raw_file_uri = new_raw_files[0] if new_raw_files else raw_data_file_uri
    reader_s3_new_raw_denied = sigv4_s3_get(
        endpoint=reader_curated_creds["endpoint"] or cfg.silo_advertised_endpoint,
        s3_uri=new_raw_file_uri,
        region=reader_curated_creds["region"],
        access_key=reader_curated_creds["access_key"],
        secret_key=reader_curated_creds["secret_key"],
        session_token=reader_curated_creds["session_token"],
    )

    # Save redacted Lakekeeper load responses and service logs before teardown.
    write_json(
        run_dir / "load_table_responses.json",
        {
            "writer_raw": {
                "http_status": writer_raw_load["http_status"],
                "url": writer_raw_load["url"],
                "redacted": redact_load_table_response(writer_raw_load["body"]),
            },
            "writer_curated": {
                "http_status": writer_curated_load["http_status"],
                "url": writer_curated_load["url"],
                "redacted": redact_load_table_response(writer_curated_load["body"]),
            },
            "reader_curated": {
                "http_status": reader_curated_load["http_status"],
                "url": reader_curated_load["url"],
                "redacted": redact_load_table_response(reader_curated_load["body"]),
            },
            "reader_raw_denied": {
                "http_status": reader_raw_load["http_status"],
                "url": reader_raw_load["url"],
                "body": reader_raw_load["body"],
                "raw_body": reader_raw_load["raw_body"],
            },
        },
    )

    lk_authz_logs = run_cmd(
        [*compose_cmd_with_profiles(cfg, ("authz",)), "logs", "--no-color", "lakekeeper-authz"],
        check=False,
    ).stdout
    (run_dir / "lakekeeper_authz.log").write_text(lk_authz_logs, encoding="utf-8")

    # 13. Project-only cleanup.
    cleanup_result = project_teardown_and_verify(
        cfg,
        logged_cmd,
        non_project_before,
        profiles=ALL_LAB_PROFILES,
        keep_up=keep_up,
    )

    check1_passed = (
        reader_curated_load["allowed"]
        and reader_duckdb_curated["allowed"]
        and reader_duckdb_curated["row_count"] == 2
        and reader_s3_curated_positive["http_status"] == 200
        and reader_s3_curated_positive["parquet_magic_valid"]
    )
    check2_passed = (
        not reader_raw_load["allowed"]
        and reader_raw_load["http_status"] in (401, 403, 404)
        and not reader_duckdb_raw["allowed"]
        and writer_raw_load["allowed"]
    )
    check3_passed = (
        not reader_s3_raw_denied["allowed"]
        and reader_s3_raw_denied["http_status"] == 403
        and writer_s3_raw_allowed["allowed"]
        and writer_s3_raw_allowed["http_status"] == 200
        and writer_s3_raw_allowed["parquet_magic_valid"]
        and not reader_s3_new_raw_denied["allowed"]
        and reader_s3_new_raw_denied["http_status"] == 403
    )
    check4_passed = (
        writer_append["raw"]["row_count"] == 3
        and writer_append["curated"]["row_count"] == 3
        and writer_append["raw"]["current_snapshot_id"] != writer_init["raw"]["current_snapshot_id"]
        and writer_append["curated"]["current_snapshot_id"]
        != writer_init["curated"]["current_snapshot_id"]
        and reader_duckdb_curated_after_append["allowed"]
        and reader_duckdb_curated_after_append["row_count"] == 3
        and not reader_curated_write_denied["allowed"]
        and reader_curated_write_denied["http_status"] == 403
    )
    cleanup_ok = keep_up or (
        cleanup_result["all_project_resources_removed"]
        and cleanup_result["non_project_containers_unchanged"]
    )

    passed = (
        prefix_check["distinct_and_disjoint"]
        and check1_passed
        and check2_passed
        and check3_passed
        and check4_passed
        and cleanup_ok
    )
    outcome = "converged" if passed else "mismatch at deadline"

    summary = {
        "milestone": "L2",
        "label": label,
        "outcome": outcome,
        "passed": passed,
        "completed_at_utc": utc_now_iso(),
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python_version": sys.version,
        },
        "config": {
            "lab": cfg.redacted_summary(),
            "authz": authz_cfg.redacted_summary(),
        },
        "commands_executed": commands_executed,
        "container_digests": digests,
        "resources_after_pre_clean": resources_after_pre_clean,
        "bucket": bucket_res,
        "lakekeeper_authz_info": {
            "before_bootstrap": info_pre,
            "after_bootstrap": info_post,
            "warehouse_id": warehouse_id,
            "warehouse_name": authz_cfg.lakekeeper_authz_warehouse,
        },
        "identities": identities_evidence,
        "grants": {
            "writer_warehouse_grant": {
                "endpoint": f"/management/v1/permissions/warehouse/{warehouse_id}/assignments",
                "http_status": wg_status,
                "writes": writer_grant_body["writes"],
            },
            "reader_curated_table_grant": {
                "endpoint": f"/management/v1/permissions/warehouse/{warehouse_id}/table/{curated_table_uuid}/assignments",
                "http_status": rg_status,
                "writes": reader_grant_body["writes"],
            },
            "reader_raw_grants": [],
        },
        "tables": {
            "raw": {
                "table_uuid": raw_table_uuid,
                "location": raw_location,
                "init_snapshot_id": writer_init["raw"]["current_snapshot_id"],
                "append_snapshot_id": writer_append["raw"]["current_snapshot_id"],
                "init_data_file": raw_data_file_uri,
                "append_data_file": new_raw_file_uri,
            },
            "curated": {
                "table_uuid": curated_table_uuid,
                "location": curated_location,
                "init_snapshot_id": writer_init["curated"]["current_snapshot_id"],
                "append_snapshot_id": writer_append["curated"]["current_snapshot_id"],
                "init_data_file": curated_data_file_uri,
            },
            "prefix_disjointness": prefix_check,
        },
        "checks": {
            "check1_reader_loads_and_scans_curated": {
                "passed": check1_passed,
                "rest_load_table_status": reader_curated_load["http_status"],
                "vended_credential_prefix": reader_curated_creds["prefix"],
                "vended_credential_endpoint": reader_curated_creds["endpoint"],
                "duckdb_scan": reader_duckdb_curated,
                "direct_s3_curated_positive_control": reader_s3_curated_positive,
            },
            "check2_catalog_denies_raw_to_reader": {
                "passed": check2_passed,
                "reader_rest_load_table": {
                    "url": reader_raw_load["url"],
                    "http_status": reader_raw_load["http_status"],
                    "body": reader_raw_load["body"],
                    "raw_body": reader_raw_load["raw_body"],
                },
                "reader_duckdb_scan_raw": reader_duckdb_raw,
                "writer_rest_load_table_control": {
                    "url": writer_raw_load["url"],
                    "http_status": writer_raw_load["http_status"],
                    "allowed": writer_raw_load["allowed"],
                    "vended_credential_prefix": writer_raw_creds["prefix"],
                },
            },
            "check3_reader_vended_creds_denied_raw_s3": {
                "passed": check3_passed,
                "reader_vended_creds_on_raw_init_file": reader_s3_raw_denied,
                "reader_vended_creds_on_raw_append_file": reader_s3_new_raw_denied,
                "writer_vended_creds_on_raw_init_file_control": writer_s3_raw_allowed,
            },
            "check4_writer_writes_both_and_reader_write_denied": {
                "passed": check4_passed,
                "writer_append_raw_rows": writer_append["raw"]["row_count"],
                "writer_append_curated_rows": writer_append["curated"]["row_count"],
                "writer_raw_snapshot_advanced": (
                    writer_append["raw"]["current_snapshot_id"]
                    != writer_init["raw"]["current_snapshot_id"]
                ),
                "writer_curated_snapshot_advanced": (
                    writer_append["curated"]["current_snapshot_id"]
                    != writer_init["curated"]["current_snapshot_id"]
                ),
                "reader_duckdb_curated_after_append": reader_duckdb_curated_after_append,
                "reader_commit_curated_denied": reader_curated_write_denied,
            },
        },
        "cleanup": cleanup_result,
    }
    write_json(run_dir / "summary.json", summary)

    compact = {
        "run_dir": str(run_dir.relative_to(REPO_ROOT).as_posix()),
        "outcome": outcome,
        "passed": passed,
        "prefix_disjoint": prefix_check["distinct_and_disjoint"],
        "check1_reader_curated": check1_passed,
        "check2_catalog_denies_raw": check2_passed,
        "check2_reader_raw_http_status": reader_raw_load["http_status"],
        "check3_s3_denies_raw": check3_passed,
        "check3_reader_raw_s3_status": reader_s3_raw_denied["http_status"],
        "check3_writer_raw_s3_status": writer_s3_raw_allowed["http_status"],
        "check4_writer_writes_both": check4_passed,
        "check4_reader_write_curated_status": reader_curated_write_denied["http_status"],
    }
    print(json.dumps(compact, indent=2, default=json_default))
    if not passed:
        raise SystemExit(1)
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run L2 Lakekeeper OpenFGA + Keycloak scoped access verification."
    )
    parser.add_argument("--label", default="l2-authz-scoped-access")
    parser.add_argument("--keep-up", action="store_true")
    args = parser.parse_args()
    run_l2(label=args.label, keep_up=args.keep_up)


if __name__ == "__main__":
    main()
