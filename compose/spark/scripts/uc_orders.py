"""PySpark helper for D1: write and read the demo orders table through Unity Catalog's Iceberg REST endpoint.

Credentials come from the container environment (DATABRICKS_HOST, DATABRICKS_TOKEN,
DATABRICKS_CATALOG), never from the command line. The output files hold rows and metadata only.

Modes:
  create-write  create `<namespace>.<table>` (if missing) and append the rows of --rows-in
  read          read the current snapshot and export exact NDJSON plus Spark-side metadata
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import sys
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATALOG_NAME = "uc"


def _fmt_decimal(val: Decimal | None) -> str | None:
    return None if val is None else format(val, "f")


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return format(obj, "f")
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.astimezone(timezone.utc).isoformat(timespec="microseconds")
    return str(obj)


def build_spark(region: str) -> SparkSession:
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    warehouse = os.environ["DATABRICKS_CATALOG"]
    cat = f"spark.sql.catalog.{CATALOG_NAME}"
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("cdclab-d1-uc-orders")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(cat, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{cat}.type", "rest")
        .config(f"{cat}.uri", f"{host}/api/2.1/unity-catalog/iceberg-rest")
        .config(f"{cat}.warehouse", warehouse)
        .config(f"{cat}.token", token)
        .config(f"{cat}.header.X-Iceberg-Access-Delegation", "vended-credentials")
        .config(f"{cat}.client.region", region)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_write(spark: SparkSession, namespace: str, table: str, rows_in: Path) -> dict[str, Any]:
    full = f"{CATALOG_NAME}.{namespace}.{table}"
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {full} ("
        "id BIGINT NOT NULL, status STRING NOT NULL, amount DECIMAL(38,9) NOT NULL, "
        "updated_at TIMESTAMP NOT NULL, note STRING, payload STRING, discount DECIMAL(38,9)"
        ") USING iceberg TBLPROPERTIES ('format-version'='2')"
    )
    raw = spark.read.json(str(rows_in))
    df = raw.select(
        F.col("id").cast("bigint").alias("id"),
        F.col("status").cast("string").alias("status"),
        F.col("amount").cast("decimal(38,9)").alias("amount"),
        F.expr("timestamp_micros(updated_at_epoch_us)").alias("updated_at"),
        F.col("note").cast("string").alias("note"),
        F.col("payload").cast("string").alias("payload"),
        F.col("discount").cast("decimal(38,9)").alias("discount"),
    ).orderBy("id")
    df.writeTo(full).append()
    snaps = [r.asDict(recursive=True) for r in spark.sql(f"SELECT * FROM {full}.snapshots ORDER BY committed_at").collect()]
    return {"table": full, "snapshots_after_write": snaps}


def read_table(spark: SparkSession, namespace: str, table: str, rows_out: Path, meta_out: Path) -> dict[str, Any]:
    full = f"{CATALOG_NAME}.{namespace}.{table}"
    jvm = spark._jvm
    snaps = [r.asDict(recursive=True) for r in spark.sql(f"SELECT * FROM {full}.snapshots ORDER BY committed_at").collect()]
    current = int(snaps[-1]["snapshot_id"]) if snaps else None
    df = spark.sql(
        f"SELECT id, status, amount, updated_at, note, payload, discount FROM {full} VERSION AS OF {current} ORDER BY id"
    )
    schema = df.schema.jsonValue()
    export = df.select(
        F.col("id"),
        F.col("status"),
        F.col("amount"),
        F.unix_micros(F.col("updated_at")).alias("updated_at_epoch_us"),
        F.col("note"),
        F.col("payload"),
        F.col("discount"),
    )
    rows_out.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with rows_out.open("w", encoding="utf-8") as fh:
        for row in export.toLocalIterator():
            fh.write(
                json.dumps(
                    {
                        "id": int(row["id"]),
                        "status": row["status"],
                        "amount": _fmt_decimal(row["amount"]),
                        "updated_at_epoch_us": (
                            int(row["updated_at_epoch_us"]) if row["updated_at_epoch_us"] is not None else None
                        ),
                        "note": row["note"],
                        "payload": row["payload"],
                        "discount": _fmt_decimal(row["discount"]),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            count += 1
    meta = {
        "versions": {
            "spark_version": str(spark.version),
            "scala_version": str(jvm.scala.util.Properties.versionNumberString()),
            "java_version": str(jvm.java.lang.System.getProperty("java.version")),
            "iceberg_version": str(jvm.org.apache.iceberg.IcebergBuild.version()),
            "python_version": sys.version,
        },
        "table": full,
        "spark_current_snapshot_id": current,
        "row_count": count,
        "spark_schema": schema,
        "snapshots": snaps,
        "files": [r.asDict(recursive=True) for r in spark.sql(f"SELECT * FROM {full}.files").collect()],
        "all_delete_files": [r.asDict(recursive=True) for r in spark.sql(f"SELECT * FROM {full}.all_delete_files").collect()],
    }
    meta_out.parent.mkdir(parents=True, exist_ok=True)
    meta_out.write_text(json.dumps(meta, indent=2, default=_json_default) + "\n", encoding="utf-8")
    return {"row_count": count, "current_snapshot_id": current}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["create-write", "read"], required=True)
    ap.add_argument("--namespace", default="convergence_rig")
    ap.add_argument("--table", default="orders_demo")
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--rows-in")
    ap.add_argument("--rows-out")
    ap.add_argument("--meta-out")
    ap.add_argument("--result-out", required=True)
    args = ap.parse_args()
    spark = build_spark(args.region)
    try:
        if args.mode == "create-write":
            result = create_write(spark, args.namespace, args.table, Path(args.rows_in))
        else:
            result = read_table(spark, args.namespace, args.table, Path(args.rows_out), Path(args.meta_out))
        out = Path(args.result_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(result, indent=2, default=_json_default) + "\n", encoding="utf-8")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
