"""Configuration loader for the CDC convergence lab."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENV_FILE = REPO_ROOT / "compose" / ".env"
LOCAL_ENV_FILE = REPO_ROOT / "compose" / ".env.local"
COMPOSE_FILE = REPO_ROOT / "compose" / "docker-compose.yml"
COMPOSE_PROJECT = "cdclab"


def parse_env_file(path: Path) -> dict[str, str]:
    """Parse a KEY=VALUE dotenv file without mutating process environment."""
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
            val = val[1:-1]
        values[key] = val
    return values


@dataclass(frozen=True)
class LabConfig:
    pg_source_host: str
    pg_source_port: int
    pg_source_user: str
    pg_source_password: str
    pg_source_db: str
    lakekeeper_port: int
    lakekeeper_warehouse: str
    lakekeeper_namespace: str
    silo_port: int
    silo_console_port: int
    silo_root_user: str
    silo_root_password: str
    silo_bucket: str
    silo_region: str
    pg2iceberg_metrics_port: int
    compose_project: str = COMPOSE_PROJECT
    compose_file: Path = COMPOSE_FILE
    compose_env_file: Path = DEFAULT_ENV_FILE

    @property
    def pg_dsn(self) -> str:
        return (
            f"postgresql://{self.pg_source_user}:{self.pg_source_password}"
            f"@{self.pg_source_host}:{self.pg_source_port}/{self.pg_source_db}"
        )

    @property
    def lakekeeper_base_url(self) -> str:
        return f"http://127.0.0.1:{self.lakekeeper_port}"

    @property
    def lakekeeper_catalog_url(self) -> str:
        return f"http://127.0.0.1:{self.lakekeeper_port}/catalog"

    @property
    def silo_advertised_endpoint(self) -> str:
        # silo.localhost resolves to 127.0.0.1 on the Windows host (RFC 6761)
        # and to the silo container on the cdclab Docker network via network alias.
        return f"http://silo.localhost:{self.silo_port}"

    @property
    def pg2iceberg_metrics_url(self) -> str:
        return f"http://127.0.0.1:{self.pg2iceberg_metrics_port}"

    def compose_base_cmd(self, *, include_connector: bool = False) -> list[str]:
        cmd = [
            "docker",
            "compose",
            "-p",
            self.compose_project,
            "--env-file",
            str(self.compose_env_file.relative_to(REPO_ROOT).as_posix()),
            "-f",
            str(self.compose_file.relative_to(REPO_ROOT).as_posix()),
        ]
        if include_connector:
            cmd.extend(["--profile", "connector"])
        return cmd

    def redacted_summary(self) -> dict[str, object]:
        return {
            "compose_project": self.compose_project,
            "pg_source_host": self.pg_source_host,
            "pg_source_port": self.pg_source_port,
            "pg_source_db": self.pg_source_db,
            "lakekeeper_base_url": self.lakekeeper_base_url,
            "lakekeeper_catalog_url": self.lakekeeper_catalog_url,
            "lakekeeper_warehouse": self.lakekeeper_warehouse,
            "lakekeeper_namespace": self.lakekeeper_namespace,
            "silo_port": self.silo_port,
            "silo_advertised_endpoint": self.silo_advertised_endpoint,
            "silo_bucket": self.silo_bucket,
            "silo_region": self.silo_region,
            "pg2iceberg_metrics_port": self.pg2iceberg_metrics_port,
        }


def load_config(
    env_file: Path | None = None,
    overrides: dict[str, str] | None = None,
) -> LabConfig:
    """Load LabConfig from compose/.env, optional compose/.env.local, and process env."""
    primary = env_file or DEFAULT_ENV_FILE
    merged = parse_env_file(primary)
    if env_file is None and LOCAL_ENV_FILE.exists():
        merged.update(parse_env_file(LOCAL_ENV_FILE))
    for key in list(merged.keys()):
        if key in os.environ:
            merged[key] = os.environ[key]
    if overrides:
        merged.update(overrides)

    return LabConfig(
        pg_source_host="127.0.0.1",
        pg_source_port=int(merged.get("PG_SOURCE_PORT", "15432")),
        pg_source_user=merged.get("PG_SOURCE_USER", "postgres"),
        pg_source_password=merged.get("PG_SOURCE_PASSWORD", "cdclab_pg_pass"),
        pg_source_db=merged.get("PG_SOURCE_DB", "cdclab"),
        lakekeeper_port=int(merged.get("LAKEKEEPER_PORT", "18181")),
        lakekeeper_warehouse=merged.get("LAKEKEEPER_WAREHOUSE", "cdclab"),
        lakekeeper_namespace=merged.get("LAKEKEEPER_NAMESPACE", "public"),
        silo_port=int(merged.get("SILO_PORT", "19000")),
        silo_console_port=int(merged.get("SILO_CONSOLE_PORT", "19001")),
        silo_root_user=merged.get("SILO_ROOT_USER", "cdclab-silo-user"),
        silo_root_password=merged.get("SILO_ROOT_PASSWORD", "cdclab-silo-pass"),
        silo_bucket=merged.get("SILO_BUCKET", "cdclab"),
        silo_region=merged.get("SILO_REGION", "local-01"),
        pg2iceberg_metrics_port=int(merged.get("PG2ICEBERG_METRICS_PORT", "19090")),
        compose_env_file=primary,
    )
