"""PySpark authenticated Iceberg writer for L2 (writes raw.orders and curated.orders via Lakekeeper OAuth2)."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return format(obj, "f")
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.astimezone(timezone.utc).isoformat(timespec="microseconds")
    return str(obj)


def build_spark(args: argparse.Namespace) -> SparkSession:
    cat = args.catalog_name
    spark = (
        SparkSession.builder.master("local[2]")
        .appName(f"cdclab-l2-{args.mode}")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{cat}.type", "rest")
        .config(f"spark.sql.catalog.{cat}.uri", args.catalog_uri)
        .config(f"spark.sql.catalog.{cat}.warehouse", args.warehouse)
        .config(f"spark.sql.catalog.{cat}.oauth2-server-uri", args.oauth2_server_uri)
        .config(f"spark.sql.catalog.{cat}.credential", f"{args.client_id}:{args.client_secret}")
        .config(f"spark.sql.catalog.{cat}.scope", args.oauth2_scope)
        .config(
            f"spark.sql.catalog.{cat}.header.X-Iceberg-Access-Delegation",
            "vended-credentials",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _inspect_table(spark: SparkSession, full_table: str) -> dict[str, Any]:
    desc = [r.asDict(recursive=True) for r in spark.sql(f"DESCRIBE EXTENDED {full_table}").collect()]
    location = None
    for row in desc:
        if row.get("col_name") == "Location":
            location = row.get("data_type")
            break
    snaps = [
        r.asDict(recursive=True)
        for r in spark.sql(f"SELECT * FROM {full_table}.snapshots ORDER BY committed_at").collect()
    ]
    files = [
        r.asDict(recursive=True)
        for r in spark.sql(f"SELECT file_path, content, record_count, file_size_in_bytes FROM {full_table}.files").collect()
    ]
    rows = [
        r.asDict(recursive=True)
        for r in spark.sql(f"SELECT * FROM {full_table} ORDER BY id").collect()
    ]
    return {
        "table": full_table,
        "location": location,
        "current_snapshot_id": int(snaps[-1]["snapshot_id"]) if snaps else None,
        "snapshot_count": len(snaps),
        "data_files": files,
        "row_count": len(rows),
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Authenticated Spark writer for L2 raw and curated tables.")
    parser.add_argument("--mode", choices=("init", "append"), required=True)
    parser.add_argument("--catalog-name", default="cdclab_authz")
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--oauth2-server-uri", required=True)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--client-secret", required=True)
    parser.add_argument("--oauth2-scope", default="lakekeeper")
    parser.add_argument("--out-json", required=True)
    args = parser.parse_args()

    spark = build_spark(args)
    cat = args.catalog_name
    raw_tbl = f"{cat}.raw.orders"
    curated_tbl = f"{cat}.curated.orders"

    try:
        if args.mode == "init":
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.raw")
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.curated")
            spark.sql(
                f"""
                CREATE TABLE {raw_tbl} (
                    id BIGINT,
                    status STRING,
                    amount DECIMAL(38, 9),
                    pii_note STRING
                ) USING iceberg
                TBLPROPERTIES ('format-version'='2')
                """
            )
            spark.sql(
                f"""
                CREATE TABLE {curated_tbl} (
                    id BIGINT,
                    status STRING,
                    amount DECIMAL(38, 9),
                    tier STRING
                ) USING iceberg
                TBLPROPERTIES ('format-version'='2')
                """
            )
            spark.sql(
                f"""
                INSERT INTO {raw_tbl} VALUES
                    (1, 'created', CAST(100.123456789 AS DECIMAL(38,9)), 'raw-secret-alpha'),
                    (2, 'settled', CAST(250.000000001 AS DECIMAL(38,9)), 'raw-secret-beta')
                """
            )
            spark.sql(
                f"""
                INSERT INTO {curated_tbl} VALUES
                    (1, 'created', CAST(100.123456789 AS DECIMAL(38,9)), 'standard'),
                    (2, 'settled', CAST(250.000000001 AS DECIMAL(38,9)), 'premium')
                """
            )
        elif args.mode == "append":
            spark.sql(
                f"""
                INSERT INTO {raw_tbl} VALUES
                    (3, 'shipped', CAST(420.500000000 AS DECIMAL(38,9)), 'raw-secret-gamma')
                """
            )
            spark.sql(
                f"""
                INSERT INTO {curated_tbl} VALUES
                    (3, 'shipped', CAST(420.500000000 AS DECIMAL(38,9)), 'enterprise')
                """
            )

        result = {
            "mode": args.mode,
            "client_id": args.client_id,
            "raw": _inspect_table(spark, raw_tbl),
            "curated": _inspect_table(spark, curated_tbl),
        }
        out_path = Path(args.out_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, indent=2, default=_json_default) + "\n", encoding="utf-8")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
