"""DuckDB Iceberg REST catalog reader and Lakekeeper REST inspection helpers."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

import duckdb

from lab.config import LabConfig


@dataclass(frozen=True)
class ReaderVersions:
    duckdb_version: str
    extensions: list[dict[str, Any]]


def _load_or_install_extension(con: duckdb.DuckDBPyConnection, name: str) -> None:
    try:
        con.execute(f"LOAD {name};")
    except Exception:
        con.execute(f"INSTALL {name};")
        con.execute(f"LOAD {name};")


def connect_iceberg(cfg: LabConfig) -> tuple[duckdb.DuckDBPyConnection, ReaderVersions]:
    """Open an in-memory DuckDB connection attached to Lakekeeper's Iceberg REST catalog."""
    con = duckdb.connect()
    _load_or_install_extension(con, "iceberg")
    _load_or_install_extension(con, "httpfs")

    ext_rows = con.execute(
        """
        SELECT extension_name, loaded, installed, extension_version, install_mode
        FROM duckdb_extensions()
        WHERE extension_name IN ('iceberg', 'httpfs', 'parquet')
        ORDER BY extension_name
        """
    ).fetchall()
    extensions = [
        {
            "name": row[0],
            "loaded": bool(row[1]),
            "installed": bool(row[2]),
            "version": row[3],
            "install_mode": row[4],
        }
        for row in ext_rows
    ]

    # Lakekeeper runs without auth in M0-M3; DuckDB requests vended credentials by default.
    con.execute(
        f"""
        ATTACH '{cfg.lakekeeper_warehouse}' AS cdclab (
            TYPE iceberg,
            AUTHORIZATION_TYPE none,
            ENDPOINT '{cfg.lakekeeper_catalog_url}'
        );
        """
    )
    return con, ReaderVersions(duckdb_version=duckdb.__version__, extensions=extensions)


def read_orders_current(con: duckdb.DuckDBPyConnection, namespace: str = "public") -> list[dict[str, Any]]:
    """Read current rows of orders via DuckDB ordered by id."""
    cur = con.execute(
        f"SELECT id, status, amount, updated_at, note, payload, discount "
        f"FROM cdclab.{namespace}.orders ORDER BY id"
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def read_orders_at_snapshot(
    con: duckdb.DuckDBPyConnection,
    snapshot_id: int,
    namespace: str = "public",
) -> list[dict[str, Any]]:
    """Read rows of orders pinned to an explicit Iceberg snapshot ID."""
    cur = con.execute(
        f"SELECT id, status, amount, updated_at, note, payload, discount "
        f"FROM cdclab.{namespace}.orders AT (VERSION => {int(snapshot_id)}) ORDER BY id"
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def marker_exists_at_snapshot(
    con: duckdb.DuckDBPyConnection,
    snapshot_id: int,
    marker_id: int,
    namespace: str = "public",
) -> bool:
    """Point-lookup check for a single marker row at an explicit Iceberg snapshot ID."""
    row = con.execute(
        f"SELECT id FROM cdclab.{namespace}.orders AT (VERSION => {int(snapshot_id)}) "
        f"WHERE id = ?",
        [int(marker_id)],
    ).fetchone()
    return row is not None


def read_snapshots(con: duckdb.DuckDBPyConnection, namespace: str = "public") -> list[dict[str, Any]]:
    """Read Iceberg snapshot history via DuckDB iceberg_snapshots()."""
    cur = con.execute(f"SELECT * FROM iceberg_snapshots(cdclab.{namespace}.orders)")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def read_metadata_files(con: duckdb.DuckDBPyConnection, namespace: str = "public") -> list[dict[str, Any]]:
    """Read live manifest/file metadata via DuckDB iceberg_metadata()."""
    cur = con.execute(f"SELECT * FROM iceberg_metadata(cdclab.{namespace}.orders)")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_lakekeeper_table_metadata(
    cfg: LabConfig,
    namespace: str = "public",
    table: str = "orders",
    *,
    request_vended_credentials: bool = True,
    allow_not_found: bool = False,
) -> dict[str, Any] | None:
    """Call Lakekeeper's Iceberg REST LoadTable endpoint directly to inspect metadata and config."""
    wh_q = urllib.parse.urlencode({"warehouse": cfg.lakekeeper_warehouse})
    cfg_url = f"{cfg.lakekeeper_catalog_url}/v1/config?{wh_q}"
    with urllib.request.urlopen(cfg_url, timeout=15.0) as resp:
        catalog_cfg = json.loads(resp.read().decode("utf-8"))

    prefix = (catalog_cfg.get("overrides") or {}).get("prefix") or (
        catalog_cfg.get("defaults") or {}
    ).get("prefix")
    if not prefix:
        raise RuntimeError(f"Lakekeeper /v1/config did not return a prefix: {catalog_cfg!r}")

    table_url = (
        f"{cfg.lakekeeper_catalog_url}/v1/{urllib.parse.quote(prefix, safe='')}"
        f"/namespaces/{urllib.parse.quote(namespace, safe='')}"
        f"/tables/{urllib.parse.quote(table, safe='')}"
    )
    headers = {"Accept": "application/json"}
    if request_vended_credentials:
        headers["X-Iceberg-Access-Delegation"] = "vended-credentials"
    req = urllib.request.Request(table_url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15.0) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if allow_not_found and exc.code == 404:
            return None
        raise

    # Redact temporary STS credentials before returning/persisting evidence.
    redacted_config = {}
    for k, v in (payload.get("config") or {}).items():
        if any(tok in k.lower() for tok in ("secret", "token", "key-id")):
            redacted_config[k] = "<redacted>" if v else ""
        else:
            redacted_config[k] = v

    redacted_storage_creds = []
    for entry in payload.get("storage-credentials") or []:
        c_cfg = {}
        for k, v in (entry.get("config") or {}).items():
            if any(tok in k.lower() for tok in ("secret", "token", "key-id")):
                c_cfg[k] = "<redacted>" if v else ""
            else:
                c_cfg[k] = v
        redacted_storage_creds.append({"prefix": entry.get("prefix"), "config": c_cfg})

    return {
        "catalog_config": catalog_cfg,
        "table_url": table_url,
        "metadata_location": payload.get("metadata-location"),
        "config": redacted_config,
        "storage_credentials": redacted_storage_creds,
        "metadata": payload.get("metadata"),
    }
