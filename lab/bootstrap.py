"""Idempotent bootstrap for Silo bucket, Lakekeeper warehouse, and PG19 source schema."""

from __future__ import annotations

import json
import subprocess
from typing import Any
import urllib.error
import urllib.request

import psycopg

from lab.config import LabConfig, REPO_ROOT, load_config
from lab.schema import ORDERS_DDL

DEFAULT_PROJECT_ID = "00000000-0000-0000-0000-000000000000"


def _http_json(
    url: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 15.0,
) -> tuple[int, Any]:
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
            return resp.status, json.loads(raw) if raw.strip() else None
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            parsed: Any = json.loads(raw) if raw.strip() else None
        except json.JSONDecodeError:
            parsed = raw
        return exc.code, parsed


def ensure_bucket(cfg: LabConfig) -> dict[str, Any]:
    """Create the synthetic S3 bucket in Silo idempotently."""
    shell_cmd = (
        f"mc alias set local http://127.0.0.1:{cfg.silo_port} "
        f"{cfg.silo_root_user} {cfg.silo_root_password} >/dev/null && "
        f"mc mb --ignore-existing local/{cfg.silo_bucket}"
    )
    cmd = [*cfg.compose_base_cmd(), "exec", "-T", "silo", "sh", "-c", shell_cmd]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            f"Failed to ensure Silo bucket {cfg.silo_bucket!r}: "
            f"rc={proc.returncode} stdout={proc.stdout!r} stderr={proc.stderr!r}"
        )
    return {
        "bucket": cfg.silo_bucket,
        "stdout": proc.stdout.strip(),
        "ok": True,
    }


def ensure_lakekeeper_bootstrapped(cfg: LabConfig) -> dict[str, Any]:
    """Bootstrap Lakekeeper if not already bootstrapped."""
    info_url = f"{cfg.lakekeeper_base_url}/management/v1/info"
    status, info = _http_json(info_url)
    if status != 200 or not isinstance(info, dict):
        raise RuntimeError(f"Lakekeeper info check failed: status={status} body={info!r}")

    if info.get("bootstrapped"):
        return {
            "action": "already_bootstrapped",
            "version": info.get("version"),
            "default_project_id": info.get("default-project-id", DEFAULT_PROJECT_ID),
            "info": info,
        }

    boot_url = f"{cfg.lakekeeper_base_url}/management/v1/bootstrap"
    b_status, b_body = _http_json(
        boot_url,
        method="POST",
        body={"accept-terms-of-use": True},
    )
    if b_status not in (200, 204):
        raise RuntimeError(f"Lakekeeper bootstrap failed: status={b_status} body={b_body!r}")

    status2, info2 = _http_json(info_url)
    if status2 != 200 or not isinstance(info2, dict) or not info2.get("bootstrapped"):
        raise RuntimeError(f"Lakekeeper not bootstrapped after POST: status={status2} body={info2!r}")

    return {
        "action": "bootstrapped",
        "http_status": b_status,
        "version": info2.get("version"),
        "default_project_id": info2.get("default-project-id", DEFAULT_PROJECT_ID),
        "info": info2,
    }


def ensure_warehouse(cfg: LabConfig, project_id: str = DEFAULT_PROJECT_ID) -> dict[str, Any]:
    """Create the Lakekeeper warehouse pointing at the shared storage endpoint, idempotently."""
    list_url = f"{cfg.lakekeeper_base_url}/management/v1/warehouse"
    status, listing = _http_json(list_url)
    if status != 200 or not isinstance(listing, dict):
        raise RuntimeError(f"Failed to list warehouses: status={status} body={listing!r}")

    for wh in listing.get("warehouses", []):
        if wh.get("name") == cfg.lakekeeper_warehouse:
            profile = wh.get("storage-profile", {})
            advertised = profile.get("endpoint")
            if (advertised or "").rstrip("/") != cfg.silo_advertised_endpoint.rstrip("/"):
                raise RuntimeError(
                    f"Existing warehouse {cfg.lakekeeper_warehouse!r} has endpoint "
                    f"{advertised!r}, expected {cfg.silo_advertised_endpoint!r}"
                )
            return {
                "action": "already_exists",
                "warehouse_id": wh.get("id") or wh.get("warehouse-id"),
                "warehouse_name": wh.get("name"),
                "advertised_endpoint": advertised,
                "sts_endpoint": profile.get("sts-endpoint"),
                "sts_enabled": profile.get("sts-enabled"),
            }

    payload = {
        "warehouse-name": cfg.lakekeeper_warehouse,
        "project-id": project_id,
        "storage-profile": {
            "type": "s3",
            "bucket": cfg.silo_bucket,
            "key-prefix": "warehouse",
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
    c_status, created = _http_json(list_url, method="POST", body=payload)
    if c_status not in (200, 201) or not isinstance(created, dict):
        raise RuntimeError(f"Failed to create warehouse: status={c_status} body={created!r}")

    wh_id = created.get("warehouse-id") or created.get("id")
    detail_status, detail = _http_json(f"{list_url}/{wh_id}")
    if detail_status != 200 or not isinstance(detail, dict):
        raise RuntimeError(f"Failed to read warehouse detail: status={detail_status} body={detail!r}")
    profile = detail.get("storage-profile", {})
    return {
        "action": "created",
        "warehouse_id": wh_id,
        "warehouse_name": cfg.lakekeeper_warehouse,
        "advertised_endpoint": profile.get("endpoint"),
        "sts_endpoint": profile.get("sts-endpoint"),
        "sts_enabled": profile.get("sts-enabled"),
    }


def ensure_source_fixture(cfg: LabConfig) -> dict[str, Any]:
    """Create public.orders on PostgreSQL 19 idempotently and record server settings."""
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(ORDERS_DDL)
            cur.execute("SELECT version();")
            pg_version = cur.fetchone()[0]
            settings = {}
            for name in (
                "server_version",
                "wal_level",
                "max_replication_slots",
                "max_wal_senders",
                "max_repack_replication_slots",
            ):
                cur.execute("SHOW " + name + ";")
                settings[name] = cur.fetchone()[0]
    return {
        "table": "public.orders",
        "pg_version": pg_version,
        "settings": settings,
    }


def verify_storage_endpoint_reachability(cfg: LabConfig, advertised_endpoint: str) -> dict[str, Any]:
    """Verify the endpoint advertised by Lakekeeper is reachable from host and from a container."""
    health_url = f"{advertised_endpoint.rstrip('/')}/minio/health/live"
    req = urllib.request.Request(health_url, method="GET")
    with urllib.request.urlopen(req, timeout=10.0) as resp:
        host_status = resp.status

    # Probe the exact same advertised hostname and port from another container (pg-source)
    # on the cdclab Docker network using bash /dev/tcp.
    host_port = advertised_endpoint.split("://", 1)[1].rstrip("/")
    host_name, port_str = host_port.rsplit(":", 1)
    probe_script = (
        f"exec 3<>/dev/tcp/{host_name}/{port_str} && "
        f'printf "GET /minio/health/live HTTP/1.1\\r\\nHost: {host_name}\\r\\nConnection: close\\r\\n\\r\\n" >&3 && '
        f"head -n 1 <&3"
    )
    cmd = [*cfg.compose_base_cmd(), "exec", "-T", "pg-source", "bash", "-c", probe_script]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    if proc.returncode != 0 or "200" not in proc.stdout:
        raise RuntimeError(
            f"Container-side probe to {advertised_endpoint} failed: "
            f"rc={proc.returncode} stdout={proc.stdout!r} stderr={proc.stderr!r}"
        )

    return {
        "advertised_endpoint": advertised_endpoint,
        "host_probe_url": health_url,
        "host_http_status": host_status,
        "container_probe_service": "pg-source",
        "container_http_line": proc.stdout.strip(),
        "both_reachable": host_status == 200 and "200" in proc.stdout,
    }


def bootstrap_all(cfg: LabConfig | None = None) -> dict[str, Any]:
    """Run all idempotent bootstrap steps in required order before starting the connector."""
    cfg = cfg or load_config()
    bucket_res = ensure_bucket(cfg)
    lk_res = ensure_lakekeeper_bootstrapped(cfg)
    wh_res = ensure_warehouse(cfg, project_id=lk_res["default_project_id"])
    reach_res = verify_storage_endpoint_reachability(cfg, wh_res["advertised_endpoint"])
    pg_res = ensure_source_fixture(cfg)
    return {
        "bucket": bucket_res,
        "lakekeeper": lk_res,
        "warehouse": wh_res,
        "endpoint_reachability": reach_res,
        "source_fixture": pg_res,
    }


def main() -> None:
    result = bootstrap_all()
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
