"""Milestone D1 runner: Databricks Free Edition through Unity Catalog's Iceberg REST endpoint.

Order of work, all inside schema ``convergence_rig`` of the catalog named in ``compose/.env.local``:

1. Feasibility from the host: REST configuration, schema creation, an external ``createTable`` with
   vended credentials, and the same request with identifier columns (what pg2iceberg sends).
2. pg2iceberg 0.12.0 against the endpoint: a fresh run, then a run against a pre-created table.
3. Read-write path with standard engines: write a small ``orders_demo`` table with Spark through the
   endpoint, read it back with DuckDB and Spark, and compare both with PostgreSQL exactly.
4. Cleanup: drop every table created here except ``orders_demo``.

The token is read from the gitignored env file inside this module. It is never printed, passed on a
command line or written to evidence: every file goes through :class:`Redactor`.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import time
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

import duckdb
import psycopg

from lab.bootstrap import ensure_source_fixture
from lab.compare import (
    ComparisonResult,
    compare_ordered_streams,
    verify_iceberg_schema,
)
from lab.config import LOCAL_ENV_FILE, REPO_ROOT, LabConfig, load_config, parse_env_file
from lab.l1 import stream_spark_orders_ndjson, verify_spark_schema
from lab.l_common import compose_cmd_with_profiles, project_teardown_and_verify
from lab.pipeline import (
    json_default,
    list_non_project_containers,
    list_project_resources,
    utc_now_iso,
    utc_run_stamp,
)
from lab.reader import ReaderVersions, _load_or_install_extension
from lab.workload import seed_orders

NAMESPACE = "convergence_rig"
DEMO_TABLE = "orders_demo"
DEMO_MAX_ID = 1000
REST_BASE = "/api/2.1/unity-catalog/iceberg-rest"
D1_PROFILES: tuple[str, ...] = ("uc-connector", "uc-spark")
ALL_PROFILES: tuple[str, ...] = (
    "connector",
    "spark",
    "authz",
    "polaris",
    "polaris-connector",
    "uc-connector",
    "uc-spark",
)
SPARK_TMP = "/tmp/cdclab-spark"

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Redaction (unit-tested)

# Written in two parts so that no source file contains the literal prefix of a Databricks token.
TOKEN_PREFIX = "d" + "api"
# ---------------------------------------------------------------------------

_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(TOKEN_PREFIX + r"[0-9a-fA-F]{16,}"), "<redacted-token>"),
    (re.compile(r"\bASIA[A-Z0-9]{12,}\b"), "<redacted-aws-key-id>"),
    (re.compile(r"\bIQoJ[A-Za-z0-9+/=]{60,}"), "<redacted-aws-session-token>"),
    (re.compile(r'("x-databricks-org-id"\s*:\s*")\d+(")'), r"\1<org-id>\2"),
    (re.compile(r"(x-databricks-org-id[=:]\s*)\d+"), r"\1<org-id>"),
    (
        re.compile(rf"(s3://)dbstorage-[a-z0-9-]+/uc/{_UUID}/{_UUID}/__unitystorage/catalogs/{_UUID}"),
        r"\1<managed-storage>",
    ),
    (re.compile(r"dbc-[0-9a-f]{8}-[0-9a-f]{4}\.cloud\.databricks\.com"), "<databricks-host>"),
    (re.compile(r"arn:aws:sts::\d{12}:assumed-role/[^\s\",]+"), "<redacted-assumed-role>"),
    (
        re.compile(rf"(arn:aws:s3:::)dbstorage-[a-z0-9-]+/uc/{_UUID}/{_UUID}/__unitystorage/catalogs/{_UUID}"),
        r"\1<managed-storage>",
    ),
    (re.compile(r"(Extended Request ID: )[A-Za-z0-9+/=]+"), r"\1<id>"),
)


class Redactor:
    """Replace the token, the workspace host and common credential shapes in any text."""

    def __init__(self, *, token: str = "", host: str = "") -> None:
        self._exact: list[tuple[str, str]] = []
        if token:
            self._exact.append((token, "<redacted-token>"))
        if host:
            bare = host.split("://", 1)[-1].rstrip("/")
            self._exact.append((host.rstrip("/"), "<databricks-host>"))
            self._exact.append((bare, "<databricks-host>"))

    def apply(self, text: str) -> str:
        for needle, repl in self._exact:
            if needle:
                text = text.replace(needle, repl)
        for pat, repl in _PATTERNS:
            text = pat.sub(repl, text)
        return text

    def apply_obj(self, obj: Any) -> Any:
        return json.loads(self.apply(json.dumps(obj, default=json_default)))


_SECRET_KEY_PARTS = ("secret", "token", "key-id", "access-key", "session")


def strip_credentials(config: dict[str, Any] | None) -> dict[str, Any]:
    """Return a REST ``config`` block with credential values blanked and the key names kept."""
    out: dict[str, Any] = {}
    for key, value in (config or {}).items():
        if any(part in key.lower() for part in _SECRET_KEY_PARTS) and "expires" not in key.lower():
            out[key] = "<redacted>" if value else ""
        else:
            out[key] = value
    return out


def summarize_table_response(payload: dict[str, Any] | None) -> dict[str, Any]:
    """Describe a LoadTable/CreateTable response without credential values."""
    if not isinstance(payload, dict):
        return {"present": False}
    meta = payload.get("metadata") or {}
    creds = []
    for entry in payload.get("storage-credentials") or []:
        creds.append(
            {
                "prefix_present": bool(entry.get("prefix")),
                "config_keys": sorted((entry.get("config") or {}).keys()),
            }
        )
    cfg = payload.get("config") or {}
    return {
        "present": True,
        "format_version": meta.get("format-version"),
        "current_snapshot_id": meta.get("current-snapshot-id"),
        "identifier_field_ids": [
            s.get("identifier-field-ids") for s in meta.get("schemas") or []
        ],
        "config_keys": sorted(cfg.keys()),
        "config_values": strip_credentials(cfg),
        "storage_credentials": creds,
        "has_s3_credentials_in_config": any(
            "access-key-id" in k or "session-token" in k for k in cfg
        ),
        "metadata_location_is_s3": str(payload.get("metadata-location", "")).startswith("s3://"),
    }


def classify_pg2iceberg_attempt(
    *,
    exit_code: int | None,
    log_text: str,
    snapshot_complete: bool,
) -> dict[str, Any]:
    """Name what stopped pg2iceberg from the container exit code and its log."""
    low = log_text.lower()
    if snapshot_complete and exit_code in (None, 0):
        return {"outcome": "running", "reason": None}
    if "identifier columns is not allowed" in low:
        m = re.search(r"errorcode:\s*(\d+)", low)
        return {
            "outcome": "refused by catalog",
            "reason": "createTable with identifier columns",
            "uc_error_code": int(m.group(1)) if m else None,
            "http_status": 400 if "400 bad request" in low else None,
        }
    if "orphanedtables" in low:
        return {
            "outcome": "refused by pg2iceberg startup validation",
            "reason": "Iceberg table exists but pg2iceberg holds no state for it (OrphanedTables)",
        }
    if exit_code not in (None, 0):
        return {"outcome": "exited", "reason": f"exit code {exit_code}"}
    return {"outcome": "unknown", "reason": None}


def row_hash_text(
    row_id: int,
    status: str,
    amount: str,
    updated_at_epoch_us: int,
    note: str | None,
    payload: str | None,
    discount: str | None,
) -> str:
    """Canonical text of one order row, identical to the SQL ``concat_ws`` used on Databricks."""
    return "|".join(
        [
            str(row_id),
            status,
            amount,
            str(updated_at_epoch_us),
            "<null>" if note is None else note,
            "<null>" if payload is None else payload,
            "<null>" if discount is None else discount,
        ]
    )


def row_hash(*args: Any) -> str:
    return hashlib.md5(row_hash_text(*args).encode("utf-8")).hexdigest()  # noqa: S324 - fingerprint only


def compare_row_hashes(
    expected: dict[int, str],
    actual: Sequence[tuple[int, str]],
    *,
    max_examples: int = 20,
) -> dict[str, Any]:
    """Compare per-row hashes by id: missing, extra, duplicate ids and differing hashes."""
    seen: dict[int, str] = {}
    duplicates: list[int] = []
    for rid, h in actual:
        if rid in seen:
            duplicates.append(rid)
        seen[rid] = h
    missing = sorted(set(expected) - set(seen))
    extra = sorted(set(seen) - set(expected))
    differing = sorted(i for i in set(expected) & set(seen) if expected[i] != seen[i])
    return {
        "passed": not (missing or extra or duplicates or differing),
        "expected_rows": len(expected),
        "actual_rows": len(actual),
        "missing_count": len(missing),
        "missing_examples": missing[:max_examples],
        "extra_count": len(extra),
        "extra_examples": extra[:max_examples],
        "duplicate_count": len(duplicates),
        "duplicate_examples": duplicates[:max_examples],
        "differing_count": len(differing),
        "differing_examples": differing[:max_examples],
    }


def classify_s3_denial(text: str) -> dict[str, Any]:
    """Describe an S3 failure message from a vended-credential read or write."""
    low = text.lower()
    action = None
    for act in ("s3:getobject", "s3:putobject", "s3:listbucket"):
        if act in low:
            action = act
            break
    return {
        "denied": ("accessdenied" in low)
        or ("not authorized" in low)
        or ("s3exception" in low and "status code: 403" in low),
        "action": action,
        "explicit_deny_in_resource_policy": "explicit deny in a resource-based policy" in low,
    }


def partition_tables(existing: Sequence[str], keep: Sequence[str]) -> tuple[list[str], list[str]]:
    """Split table names into (to drop, to keep)."""
    keep_set = set(keep)
    drop = sorted(t for t in existing if t not in keep_set)
    kept = sorted(t for t in existing if t in keep_set)
    return drop, kept


def scan_for_secrets(text: str, secrets: Sequence[str]) -> list[str]:
    """Return which of the named needles occur in ``text`` (for the pre-commit guard)."""
    hits = [s for s in secrets if s and s in text]
    if re.search(TOKEN_PREFIX + r"[0-9a-fA-F]{8,}", text):
        hits.append("token-pattern")
    if re.search(r"\bASIA[A-Z0-9]{12,}\b", text):
        hits.append("aws-key-id-pattern")
    return hits


# ---------------------------------------------------------------------------
# Environment and REST client
# ---------------------------------------------------------------------------


def load_d1_env() -> dict[str, str] | None:
    """Return host, token, catalog from compose/.env.local, or None when any is missing."""
    values = parse_env_file(LOCAL_ENV_FILE)
    host = values.get("DATABRICKS_HOST", "").rstrip("/")
    token = values.get("DATABRICKS_TOKEN", "")
    catalog = values.get("DATABRICKS_CATALOG", "")
    if not (host and token and catalog):
        return None
    if "://" not in host:
        host = "https://" + host
    return {"host": host, "token": token, "catalog": catalog}


class UcClient:
    """Minimal Unity Catalog Iceberg REST client. Never raises on HTTP errors."""

    def __init__(self, env: dict[str, str], redactor: Redactor) -> None:
        self._host = env["host"]
        self._token = env["token"]
        self.catalog = env["catalog"]
        self._red = redactor
        self.prefix_path = f"{REST_BASE}/v1/catalogs/{self.catalog}"

    def call(
        self,
        method: str,
        path: str,
        body: Any | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 60.0,
    ) -> dict[str, Any]:
        hdr = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        data = None
        if body is not None:
            hdr["Content-Type"] = "application/json"
            data = json.dumps(body).encode("utf-8")
        if headers:
            hdr.update(headers)
        req = urllib.request.Request(self._host + path, data=data, headers=hdr, method=method)
        t0 = time.monotonic_ns()
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                status, raw = resp.status, resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            status, raw = exc.code, exc.read().decode("utf-8", errors="replace")
        except Exception as exc:  # network failure is a result too
            return {
                "method": method,
                "path": path,
                "status": None,
                "error": self._red.apply(f"{type(exc).__name__}: {exc}"),
                "json": None,
                "elapsed_ms": round((time.monotonic_ns() - t0) / 1e6, 2),
            }
        try:
            parsed = json.loads(raw) if raw.strip() else None
        except json.JSONDecodeError:
            parsed = None
        return {
            "method": method,
            "path": path,
            "status": status,
            "json": parsed,
            "text_head": self._red.apply(raw[:300]) if parsed is None else None,
            "elapsed_ms": round((time.monotonic_ns() - t0) / 1e6, 2),
        }

    def config(self) -> dict[str, Any]:
        q = urllib.parse.urlencode({"warehouse": self.catalog})
        return self.call("GET", f"{REST_BASE}/v1/config?{q}")

    def list_namespaces(self) -> dict[str, Any]:
        return self.call("GET", f"{self.prefix_path}/namespaces")

    def create_namespace(self, name: str) -> dict[str, Any]:
        return self.call("POST", f"{self.prefix_path}/namespaces", {"namespace": [name], "properties": {}})

    def list_tables(self) -> list[str]:
        r = self.call("GET", f"{self.prefix_path}/namespaces/{NAMESPACE}/tables")
        if r["status"] != 200 or not r["json"]:
            return []
        return [i["name"] for i in r["json"].get("identifiers", [])]

    def create_table(self, name: str, fields: list[dict[str, Any]], identifier_ids: list[int] | None = None) -> dict[str, Any]:
        schema: dict[str, Any] = {"type": "struct", "schema-id": 0, "fields": fields}
        if identifier_ids:
            schema["identifier-field-ids"] = identifier_ids
        return self.call(
            "POST",
            f"{self.prefix_path}/namespaces/{NAMESPACE}/tables",
            {"name": name, "schema": schema, "properties": {}},
            {"X-Iceberg-Access-Delegation": "vended-credentials"},
        )

    def load_table(self, name: str, vended: bool = True) -> dict[str, Any]:
        h = {"X-Iceberg-Access-Delegation": "vended-credentials"} if vended else None
        return self.call("GET", f"{self.prefix_path}/namespaces/{NAMESPACE}/tables/{name}", headers=h)

    def table_credentials(self, name: str) -> dict[str, Any]:
        return self.call("GET", f"{self.prefix_path}/namespaces/{NAMESPACE}/tables/{name}/credentials")

    def drop_table(self, name: str) -> dict[str, Any]:
        return self.call("DELETE", f"{self.prefix_path}/namespaces/{NAMESPACE}/tables/{name}")


PROBE_FIELDS: list[dict[str, Any]] = [
    {"id": 1, "name": "id", "required": True, "type": "long"},
    {"id": 2, "name": "v", "required": False, "type": "string"},
]
ORDERS_FIELDS: list[dict[str, Any]] = [
    {"id": 1, "name": "id", "required": True, "type": "long"},
    {"id": 2, "name": "status", "required": True, "type": "string"},
    {"id": 3, "name": "amount", "required": True, "type": "decimal(38, 9)"},
    {"id": 4, "name": "updated_at", "required": True, "type": "timestamptz"},
    {"id": 5, "name": "note", "required": False, "type": "string"},
    {"id": 6, "name": "payload", "required": False, "type": "string"},
    {"id": 7, "name": "discount", "required": False, "type": "decimal(38, 9)"},
]


# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------


def _run(cmd: list[str], red: Redactor, *, timeout: float = 600.0, check: bool = False) -> dict[str, Any]:
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
    res = {
        "cmd": red.apply(" ".join(cmd)),
        "returncode": proc.returncode,
        "stdout": red.apply(proc.stdout),
        "stderr": red.apply(proc.stderr),
    }
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {res['cmd']}\n{res['stderr'][-2000:]}")
    return res


def _write_json(path: Path, payload: Any, red: Redactor) -> None:
    text = json.dumps(payload, indent=2, default=json_default) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(red.apply(text), encoding="utf-8")


def _write_text(path: Path, text: str, red: Redactor) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(red.apply(text), encoding="utf-8")


def pg2iceberg_uc_attempt(
    cfg: LabConfig,
    red: Redactor,
    run_dir: Path,
    label: str,
    commands: list[str],
    *,
    wait_s: float = 90.0,
) -> dict[str, Any]:
    """Start pg2iceberg-uc, wait for it to exit or finish its snapshot, and keep its log."""
    base = compose_cmd_with_profiles(cfg, ("uc-connector",))
    up = [*base, "up", "-d", "pg2iceberg-uc"]
    commands.append(" ".join(up))
    _run(up, red, check=True)
    deadline = time.monotonic() + wait_s
    exit_code: int | None = None
    snapshot_complete = False
    log_text = ""
    while time.monotonic() < deadline:
        time.sleep(2.0)
        logs = _run([*base, "logs", "--no-color", "pg2iceberg-uc"], red)
        log_text = logs["stdout"] + logs["stderr"]
        snapshot_complete = "snapshot phase complete" in log_text
        ps = _run([*base, "ps", "-a", "--format", "json", "pg2iceberg-uc"], red)
        for line in ps["stdout"].splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            info = json.loads(line)
            if info.get("State") == "exited":
                exit_code = int(info.get("ExitCode", 1))
        if exit_code is not None or snapshot_complete:
            break
    _write_text(run_dir / f"pg2iceberg_uc_{label}.log", log_text, red)
    verdict = classify_pg2iceberg_attempt(
        exit_code=exit_code, log_text=log_text, snapshot_complete=snapshot_complete
    )
    return {
        "label": label,
        "exit_code": exit_code,
        "snapshot_complete": snapshot_complete,
        "log_file": f"pg2iceberg_uc_{label}.log",
        "log_lines": len([ln for ln in log_text.splitlines() if ln.strip()]),
        **verdict,
    }


# ---------------------------------------------------------------------------
# Demo rows, DuckDB and Spark readers
# ---------------------------------------------------------------------------


def _epoch_us(dt: datetime) -> int:
    return (dt - _EPOCH) // timedelta(microseconds=1)


def export_demo_rows(pg_dsn: str, path: Path, max_id: int = DEMO_MAX_ID) -> dict[str, Any]:
    """Write ``public.orders`` rows with ``id <= max_id`` as NDJSON in the Spark helper's input shape."""
    n = 0
    h = hashlib.sha256()
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur, path.open("w", encoding="utf-8", newline="\n") as fh:
        cur.execute(
            "SELECT id, status, amount, updated_at, note, payload::text, discount "
            "FROM public.orders WHERE id <= %s ORDER BY id",
            (max_id,),
        )
        for row in cur:
            rec = {
                "id": int(row[0]),
                "status": row[1],
                "amount": format(row[2], "f"),
                "updated_at_epoch_us": _epoch_us(row[3]),
                "note": row[4],
                "payload": row[5],
                "discount": None if row[6] is None else format(row[6], "f"),
            }
            line = json.dumps(rec, ensure_ascii=False) + "\n"
            fh.write(line)
            h.update(line.encode("utf-8"))
            n += 1
    return {"rows": n, "sha256": h.hexdigest(), "max_id": max_id}


def stream_postgres_demo(pg_dsn: str, max_id: int = DEMO_MAX_ID, batch_size: int = 500) -> Iterator[list[tuple[Any, ...]]]:
    with psycopg.connect(pg_dsn) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        with conn.cursor(name="d1_demo_scan") as cur:
            cur.itersize = batch_size
            cur.execute(
                "SELECT id, status, amount, updated_at, note, payload::text AS payload, discount "
                "FROM public.orders WHERE id <= %s ORDER BY id",
                (max_id,),
            )
            while True:
                batch = cur.fetchmany(batch_size)
                if not batch:
                    break
                yield batch


def connect_duckdb_uc(env: dict[str, str], red: Redactor) -> tuple[duckdb.DuckDBPyConnection, ReaderVersions]:
    """Attach the Unity Catalog Iceberg REST endpoint in DuckDB under the alias ``cdclab``."""
    con = duckdb.connect()
    _load_or_install_extension(con, "iceberg")
    _load_or_install_extension(con, "httpfs")
    rows = con.execute(
        "SELECT extension_name, loaded, installed, extension_version, install_mode FROM duckdb_extensions() "
        "WHERE extension_name IN ('iceberg','httpfs','parquet') ORDER BY extension_name"
    ).fetchall()
    exts = [
        {"name": r[0], "loaded": bool(r[1]), "installed": bool(r[2]), "version": r[3], "install_mode": r[4]}
        for r in rows
    ]
    try:
        tok = env["token"].replace("'", "''")
        con.execute(f"CREATE SECRET uc_secret (TYPE iceberg, TOKEN '{tok}')")
        endpoint = f"{env['host']}{REST_BASE}".replace("'", "''")
        cat = env["catalog"].replace("'", "''")
        con.execute(f"ATTACH '{cat}' AS cdclab (TYPE iceberg, SECRET uc_secret, ENDPOINT '{endpoint}')")
    except Exception as exc:
        con.close()
        raise RuntimeError(red.apply(f"{type(exc).__name__}: {exc}")) from None
    return con, ReaderVersions(duckdb_version=duckdb.__version__, extensions=exts)


def stream_duckdb_table(
    con: duckdb.DuckDBPyConnection,
    snapshot_id: int,
    table: str,
    batch_size: int = 500,
) -> Iterator[list[tuple[Any, ...]]]:
    cur = con.execute(
        f"SELECT id, status, amount, updated_at, note, payload, discount "
        f"FROM cdclab.{NAMESPACE}.{table} AT (VERSION => {int(snapshot_id)}) ORDER BY id"
    )
    while True:
        batch = cur.fetchmany(batch_size)
        if not batch:
            break
        yield batch


def rows_from_df(cur: Any) -> list[dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


class DatabricksSql:
    """Run statements on the workspace's SQL warehouse through the Statement Execution API."""

    def __init__(self, uc: UcClient, red: Redactor) -> None:
        self._uc = uc
        self._red = red
        r = uc.call("GET", "/api/2.0/sql/warehouses")
        whs = ((r["json"] or {}).get("warehouses") or []) if r["status"] == 200 else []
        self.warehouse_id: str | None = whs[0]["id"] if whs else None

    def run(self, statement: str, *, timeout_s: float = 300.0) -> dict[str, Any]:
        assert self.warehouse_id
        r = self._uc.call(
            "POST",
            "/api/2.0/sql/statements",
            {
                "warehouse_id": self.warehouse_id,
                "statement": statement,
                "wait_timeout": "50s",
                "on_wait_timeout": "CONTINUE",
            },
            timeout=120.0,
        )
        resp = r["json"] or {}
        deadline = time.monotonic() + timeout_s
        while (resp.get("status") or {}).get("state") in ("PENDING", "RUNNING") and time.monotonic() < deadline:
            time.sleep(3.0)
            resp = self._uc.call("GET", f"/api/2.0/sql/statements/{resp['statement_id']}")["json"] or {}
        state = (resp.get("status") or {}).get("state", "UNKNOWN")
        rows: list[list[Any]] = []
        result = resp.get("result") or {}
        rows.extend(result.get("data_array") or [])
        nxt = result.get("next_chunk_index")
        while nxt is not None:
            ch = (
                self._uc.call("GET", f"/api/2.0/sql/statements/{resp['statement_id']}/result/chunks/{nxt}")["json"]
                or {}
            )
            rows.extend(ch.get("data_array") or [])
            nxt = ch.get("next_chunk_index")
        error = self._red.apply(json.dumps((resp.get("status") or {}).get("error"))) if state == "FAILED" else None
        return {"state": state, "rows": rows, "error": error}


def _sql_hex(value: str | None) -> str:
    """A VALUES literal for text: hex-encoded UTF-8, or NULL."""
    if value is None:
        return "NULL"
    return f"'{value.encode('utf-8').hex()}'"


_INSERT_SELECT = (
    "SELECT CAST(id AS BIGINT), decode(unhex(CAST(status_h AS STRING)), 'UTF-8'), "
    "CAST(CAST(amount_s AS STRING) AS DECIMAL(38,9)), timestamp_micros(CAST(ts AS BIGINT)), "
    "decode(unhex(CAST(note_h AS STRING)), 'UTF-8'), decode(unhex(CAST(payload_h AS STRING)), 'UTF-8'), "
    "CAST(CAST(disc_s AS STRING) AS DECIMAL(38,9)) "
    "FROM VALUES {values} AS v(id, status_h, amount_s, ts, note_h, payload_h, disc_s)"
)


def demo_insert_statements(rows_in: Path, fq_table: str, batch_rows: int = 50) -> Iterator[str]:
    """INSERT statements for the NDJSON demo rows. Text travels as hex and decimals as text, so it is exact.

    Databricks rejects expressions inside a bare VALUES list, so the values are literals only and the
    decoding happens in the SELECT around them.
    """
    batch: list[str] = []

    def build() -> str:
        return f"INSERT INTO {fq_table} " + _INSERT_SELECT.format(values=", ".join(batch))

    with rows_in.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            r = json.loads(line)
            disc = "NULL" if r["discount"] is None else f"'{r['discount']}'"
            batch.append(
                f"({int(r['id'])}, {_sql_hex(r['status'])}, '{r['amount']}', {int(r['updated_at_epoch_us'])}, "
                f"{_sql_hex(r['note'])}, {_sql_hex(r['payload'])}, {disc})"
            )
            if len(batch) >= batch_rows:
                yield build()
                batch = []
    if batch:
        yield build()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_d1(*, seed_rows: int = 2500, toast_rows: int = 50, rng_seed: int = 20261119, label: str = "d1-databricks-uc") -> Path:
    env = load_d1_env()
    if env is None:
        print("D1 skipped: DATABRICKS_HOST, DATABRICKS_TOKEN or DATABRICKS_CATALOG missing in compose/.env.local")
        raise SystemExit(2)
    red = Redactor(token=env["token"], host=env["host"])
    uc = UcClient(env, red)
    cfg = load_config()
    run_dir = REPO_ROOT / "runs" / f"{utc_run_stamp()}-{label}"
    run_dir.mkdir(parents=True, exist_ok=True)
    commands: list[str] = []
    non_project_before = list_non_project_containers(cfg.compose_project)
    created: list[str] = []  # tables this run created

    def logged(cmd: list[str], *, check: bool = True) -> dict[str, Any]:
        commands.append(red.apply(" ".join(cmd)))
        return _run(cmd, red, check=check)

    # ---- 1. Feasibility -------------------------------------------------
    feas: dict[str, Any] = {}
    cfg_resp = uc.config()
    _write_json(run_dir / "uc_config.json", cfg_resp, red)
    cj = cfg_resp.get("json") or {}
    endpoints = cj.get("endpoints") or []
    feas["config"] = {
        "status": cfg_resp["status"],
        "prefix": (cj.get("overrides") or {}).get("prefix"),
        "defaults": cj.get("defaults"),
        "endpoint_count": len(endpoints),
        "advertises": {
            "create_namespace": any(e.startswith("POST") and e.endswith("/namespaces") for e in endpoints),
            "create_table": any(e.startswith("POST") and e.endswith("/namespaces/{namespace}/tables") for e in endpoints),
            "commit_table": any(e.startswith("POST") and e.endswith("/tables/{table}") for e in endpoints),
            "drop_table": any(e.startswith("DELETE") and "/tables/" in e for e in endpoints),
            "table_credentials": any("/credentials" in e for e in endpoints),
            "multi_table_transactions": any("transactions/commit" in e for e in endpoints),
        },
    }
    ns_before = uc.list_namespaces()
    _write_json(run_dir / "uc_namespaces_before.json", ns_before, red)
    names_before = [n[0] for n in (ns_before.get("json") or {}).get("namespaces", [])]
    if NAMESPACE in names_before:
        schema_action = {"action": "already_exists"}
    else:
        r = uc.create_namespace(NAMESPACE)
        schema_action = {"action": "created", "status": r["status"], "body": r["json"]}
    feas["schema"] = {"name": NAMESPACE, **schema_action}
    tables_before = uc.list_tables()
    feas["tables_before_run"] = tables_before
    # A leftover `orders` from an earlier exploratory run would turn attempt A into an
    # OrphanedTables refusal, so remove it and say so.
    stale_dropped = []
    for name in tables_before:
        if name in ("orders", DEMO_TABLE, "d1_probe", "d1_ident_probe", "d1_noident_probe"):
            stale_dropped.append({"table": name, "status": uc.drop_table(name)["status"]})
    feas["stale_tables_dropped_at_start"] = stale_dropped

    probe = uc.create_table("d1_probe", PROBE_FIELDS)
    if probe["status"] == 200:
        created.append("d1_probe")
    feas["external_create_table"] = {
        "status": probe["status"],
        **summarize_table_response(probe["json"]),
    }
    load = uc.load_table("d1_probe")
    feas["external_load_table_vended"] = {"status": load["status"], **summarize_table_response(load["json"])}
    creds = uc.table_credentials("d1_probe")
    feas["table_credentials_endpoint"] = {
        "status": creds["status"],
        "keys": sorted((creds["json"] or {}).keys()) if isinstance(creds["json"], dict) else None,
        "credential_entries": [
            {"prefix_present": bool(e.get("prefix")), "config_keys": sorted((e.get("config") or {}).keys())}
            for e in ((creds["json"] or {}).get("storage-credentials") or [])
        ]
        if isinstance(creds["json"], dict)
        else None,
    }
    drop_probe = uc.drop_table("d1_probe")
    if drop_probe["status"] in (200, 204) and "d1_probe" in created:
        created.remove("d1_probe")
    feas["drop_table"] = {"status": drop_probe["status"]}

    ident = uc.create_table("d1_ident_probe", ORDERS_FIELDS, identifier_ids=[1])
    if ident["status"] == 200:
        created.append("d1_ident_probe")
    feas["create_table_with_identifier_columns"] = {
        "status": ident["status"],
        "error": red.apply_obj(ident["json"]) if ident["status"] != 200 else None,
    }
    no_ident = uc.create_table("d1_noident_probe", ORDERS_FIELDS)
    if no_ident["status"] == 200:
        created.append("d1_noident_probe")
    feas["create_table_without_identifier_columns"] = {"status": no_ident["status"]}
    for name in ("d1_ident_probe", "d1_noident_probe"):
        if name in created and uc.drop_table(name)["status"] in (200, 204):
            created.remove(name)
    _write_json(run_dir / "feasibility.json", feas, red)

    # ---- 2. pg2iceberg against the endpoint ------------------------------
    teardown_all = [*compose_cmd_with_profiles(cfg, ALL_PROFILES), "down", "-v", "--remove-orphans"]
    logged(teardown_all)
    attempts: list[dict[str, Any]] = []
    for attempt_label, precreate in (("a_fresh", False), ("b_precreated_without_identifier", True)):
        logged([*compose_cmd_with_profiles(cfg, ()), "up", "-d", "--wait", "pg-source"])
        ensure_source_fixture(cfg)
        with psycopg.connect(cfg.pg_dsn) as pg_conn:
            seed_orders(pg_conn, seed_rows=seed_rows, toast_row_count=toast_rows, rng_seed=rng_seed)
        pre: dict[str, Any] | None = None
        if precreate:
            pre = uc.create_table("orders", ORDERS_FIELDS)
            if pre["status"] == 200:
                created.append("orders")
        res = pg2iceberg_uc_attempt(cfg, red, run_dir, attempt_label, commands)
        res["precreated_table_status"] = pre["status"] if pre else None
        with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
            cur = conn.execute("SELECT slot_name, active FROM pg_replication_slots ORDER BY slot_name")
            res["slots_after"] = [{"slot_name": r[0], "active": r[1]} for r in cur.fetchall()]
        res["tables_after"] = uc.list_tables()
        attempts.append(res)
        if "orders" in created and uc.drop_table("orders")["status"] in (200, 204):
            created.remove("orders")
        logged(teardown_all)
    _write_json(run_dir / "pg2iceberg_attempts.json", attempts, red)

    # ---- 3. Standard engines and Databricks SQL on orders_demo -------------
    logged([*compose_cmd_with_profiles(cfg, ()), "up", "-d", "--wait", "pg-source"])
    ensure_source_fixture(cfg)
    with psycopg.connect(cfg.pg_dsn) as pg_conn:
        seed_res = seed_orders(pg_conn, seed_rows=seed_rows, toast_row_count=toast_rows, rng_seed=rng_seed)
    rows_in = run_dir / "orders_demo_rows.jsonl"
    export_info = export_demo_rows(cfg.pg_dsn, rows_in)
    base_spark = compose_cmd_with_profiles(cfg, ("uc-spark",))
    logged([*base_spark, "up", "-d", "spark-uc"])
    logged([*base_spark, "exec", "-T", "spark-uc", "mkdir", "-p", SPARK_TMP])
    logged([*base_spark, "cp", rows_in.relative_to(REPO_ROOT).as_posix(), f"spark-uc:{SPARK_TMP}/rows_in.jsonl"])

    readback: dict[str, Any] = {}

    # 3a. External write with Spark through the endpoint (vended credentials).
    write_cmd = [
        *base_spark, "exec", "-T", "spark-uc", "/opt/spark/bin/spark-submit", "--master", "local[2]",
        "/opt/cdclab/scripts/uc_orders.py", "--mode", "create-write", "--table", DEMO_TABLE,
        "--rows-in", f"{SPARK_TMP}/rows_in.jsonl", "--result-out", f"{SPARK_TMP}/write_result.json",
    ]
    t0 = time.monotonic_ns()
    write_run = logged(write_cmd, check=False)
    write_ms = (time.monotonic_ns() - t0) / 1e6
    _write_text(run_dir / "spark_write.stderr.txt", write_run["stderr"][-20000:], red)
    _write_text(run_dir / "spark_write.stdout.txt", write_run["stdout"][-20000:], red)
    table_exists = DEMO_TABLE in uc.list_tables()
    if table_exists:
        created.append(DEMO_TABLE)
    readback["spark_external_write"] = {
        "returncode": write_run["returncode"],
        "elapsed_ms": round(write_ms, 2),
        "table_created_through_rest": table_exists,
        "s3_failure": classify_s3_denial(write_run["stderr"] + write_run["stdout"]),
    }

    # 3b. Databricks SQL: the engine that can write this storage.
    sql = DatabricksSql(uc, red)
    fq = f"`{env['catalog']}`.{NAMESPACE}.{DEMO_TABLE}"
    sql_info: dict[str, Any] = {"warehouse_found": bool(sql.warehouse_id)}
    if sql.warehouse_id:
        if not table_exists:
            r = sql.run(
                f"CREATE TABLE {fq} (id BIGINT NOT NULL, status STRING NOT NULL, amount DECIMAL(38,9) NOT NULL, "
                "updated_at TIMESTAMP NOT NULL, note STRING, payload STRING, discount DECIMAL(38,9)) USING ICEBERG"
            )
            sql_info["create_table_state"] = r["state"]
            if r["state"] == "SUCCEEDED":
                created.append(DEMO_TABLE)
        count0 = sql.run(f"SELECT count(*) FROM {fq}")
        n0 = int(count0["rows"][0][0]) if count0["rows"] else None
        sql_info["rows_before_insert"] = n0
        insert_states = []
        if n0 == 0:
            for stmt in demo_insert_statements(rows_in, fq):
                res_ins = sql.run(stmt)
                insert_states.append(res_ins["state"])
                if res_ins["error"] and "first_insert_error" not in sql_info:
                    sql_info["first_insert_error"] = res_ins["error"][:800]
        sql_info["insert_statements"] = len(insert_states)
        sql_info["insert_states"] = sorted(set(insert_states))
        hashed = sql.run(
            "SELECT id, md5(concat_ws('|', CAST(id AS STRING), status, CAST(amount AS STRING), "
            "CAST(unix_micros(updated_at) AS STRING), coalesce(note, '<null>'), coalesce(payload, '<null>'), "
            f"coalesce(CAST(discount AS STRING), '<null>'))) AS h FROM {fq} ORDER BY id"
        )
        expected: dict[int, str] = {}
        with psycopg.connect(cfg.pg_dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id, status, amount, updated_at, note, payload::text, discount "
                "FROM public.orders WHERE id <= %s ORDER BY id",
                (DEMO_MAX_ID,),
            )
            for r_ in cur:
                expected[int(r_[0])] = row_hash(
                    int(r_[0]),
                    r_[1],
                    format(r_[2], "f"),
                    _epoch_us(r_[3]),
                    r_[4],
                    r_[5],
                    None if r_[6] is None else format(r_[6], "f"),
                )
        actual = [(int(r_[0]), str(r_[1])) for r_ in hashed["rows"]]
        sql_info["hash_query_state"] = hashed["state"]
        sql_info["hash_comparison"] = compare_row_hashes(expected, actual)
        sql_info["sample_rows"] = sql.run(f"SELECT id, status, amount, updated_at FROM {fq} ORDER BY id LIMIT 3")["rows"]
        sql_info["final_count"] = sql.run(f"SELECT count(*) FROM {fq}")["rows"]
    readback["databricks_sql"] = sql_info

    # 3c. External reads through the endpoint after Databricks wrote the rows.
    demo_load = uc.load_table(DEMO_TABLE)
    demo_meta = (demo_load["json"] or {}).get("metadata") if demo_load["status"] == 200 else None
    _write_json(
        run_dir / "orders_demo_table_response.json",
        {"status": demo_load["status"], "summary": summarize_table_response(demo_load["json"]), "metadata": demo_meta},
        red,
    )
    if demo_meta:
        readback["rest_schema_check"] = verify_iceberg_schema(demo_meta, expected_identifier_names=()).to_dict()
        readback["rest_current_snapshot_id"] = demo_meta.get("current-snapshot-id")
        readback["rest_snapshot_count"] = len(demo_meta.get("snapshots") or [])
    snap_id = int((demo_meta or {}).get("current-snapshot-id") or -1)

    duck_res: dict[str, Any]
    try:
        con, versions = connect_duckdb_uc(env, red)
        try:
            try:
                cmp_duck: ComparisonResult = compare_ordered_streams(
                    stream_postgres_demo(cfg.pg_dsn),
                    stream_duckdb_table(con, snap_id, DEMO_TABLE),
                    max_examples=20,
                )
                duck_res = {"attached": True, "passed": cmp_duck.passed, "comparison": cmp_duck.to_dict()}
            except Exception as exc:
                msg = red.apply(f"{type(exc).__name__}: {exc}")
                duck_res = {
                    "attached": True,
                    "passed": False,
                    "error": msg[:1500],
                    "s3_failure": classify_s3_denial(msg),
                }
        finally:
            con.close()
        duck_res["reader_versions"] = {"duckdb": versions.duckdb_version, "extensions": versions.extensions}
    except Exception as exc:
        duck_res = {"attached": False, "passed": False, "error": red.apply(f"{type(exc).__name__}: {exc}")[:1500]}
    readback["duckdb_external_read"] = duck_res

    read_cmd = [
        *base_spark, "exec", "-T", "spark-uc", "/opt/spark/bin/spark-submit", "--master", "local[2]",
        "/opt/cdclab/scripts/uc_orders.py", "--mode", "read", "--table", DEMO_TABLE,
        "--rows-out", f"{SPARK_TMP}/rows_out.jsonl", "--meta-out", f"{SPARK_TMP}/meta_out.json",
        "--result-out", f"{SPARK_TMP}/read_result.json",
    ]
    read_run = logged(read_cmd, check=False)
    _write_text(run_dir / "spark_read.stderr.txt", read_run["stderr"][-20000:], red)
    _write_text(run_dir / "spark_read.stdout.txt", read_run["stdout"][-20000:], red)
    readback["spark_external_read"] = {
        "returncode": read_run["returncode"],
        "passed": read_run["returncode"] == 0,
        "s3_failure": classify_s3_denial(read_run["stderr"] + read_run["stdout"]),
    }
    _write_json(run_dir / "readback.json", readback, red)

    # ---- 4. Cleanup: keep only orders_demo ---------------------------------
    existing = uc.list_tables()
    drop_names, kept_names = partition_tables(existing, [DEMO_TABLE])
    drops = []
    for name in drop_names:
        r = uc.drop_table(name)
        drops.append({"table": name, "status": r["status"]})
    final_tables = uc.list_tables()
    final_demo = uc.load_table(DEMO_TABLE, vended=False)
    cleanup_tables = {
        "tables_before_run": tables_before,
        "existing_before_cleanup": existing,
        "dropped": drops,
        "kept": kept_names,
        "final_tables": final_tables,
        "demo_table_status": final_demo["status"],
    }
    _write_json(run_dir / "uc_cleanup.json", cleanup_tables, red)

    docker_cleanup = project_teardown_and_verify(
        cfg, lambda c: logged(c), non_project_before, profiles=ALL_PROFILES
    )

    # ---- Summary --------------------------------------------------------
    hash_cmp = (readback.get("databricks_sql") or {}).get("hash_comparison") or {}
    demo_ok = bool(hash_cmp.get("passed")) and hash_cmp.get("expected_rows") == export_info["rows"]
    kept_ok = final_tables == [DEMO_TABLE]
    external_write_ok = readback["spark_external_write"]["returncode"] == 0
    external_read_ok = bool(duck_res.get("passed")) and bool(readback["spark_external_read"]["passed"])
    summary = {
        "milestone": "D1",
        "label": label,
        "completed_at_utc": utc_now_iso(),
        "outcome": {
            "rest_metadata_and_table_creation": "answered (see feasibility)",
            "external_data_write_with_vended_credentials": "allowed" if external_write_ok else "denied",
            "external_data_read_with_vended_credentials": "allowed" if external_read_ok else "denied",
            "pg2iceberg_write": [
                {k: a.get(k) for k in ("label", "outcome", "reason", "uc_error_code", "http_status")}
                for a in attempts
            ],
            "demo_table_exact_in_databricks_sql": "converged" if demo_ok else "not converged",
        },
        "passed": demo_ok and kept_ok and docker_cleanup.get("all_project_resources_removed", False),
        "parameters": {"seed_rows": seed_rows, "toast_rows": toast_rows, "rng_seed": rng_seed, "demo_max_id": DEMO_MAX_ID},
        "host": {"platform": platform.platform(), "machine": platform.machine(), "python_version": sys.version},
        "feasibility": feas,
        "pg2iceberg_attempts": attempts,
        "demo_rows_export": export_info,
        "seed": seed_res.to_dict(),
        "readback": readback,
        "cleanup_tables": cleanup_tables,
        "cleanup_docker": docker_cleanup,
        "commands_executed": commands,
    }
    _write_json(run_dir / "summary.json", summary, red)
    print(
        json.dumps(
            {
                "run_dir": str(run_dir.relative_to(REPO_ROOT).as_posix()),
                "passed": summary["passed"],
                "outcome": summary["outcome"],
                "final_tables": final_tables,
            },
            indent=2,
        )
    )
    if not summary["passed"]:
        raise SystemExit(1)
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="D1: Databricks Free Edition through Unity Catalog's Iceberg REST endpoint.")
    parser.add_argument("--seed-rows", type=int, default=2500)
    parser.add_argument("--toast-rows", type=int, default=50)
    parser.add_argument("--rng-seed", type=int, default=20261119)
    parser.add_argument("--label", default="d1-databricks-uc")
    args = parser.parse_args()
    run_d1(seed_rows=args.seed_rows, toast_rows=args.toast_rows, rng_seed=args.rng_seed, label=args.label)


if __name__ == "__main__":
    main()
