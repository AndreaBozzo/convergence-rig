"""PySpark reader for L1: read an explicit Iceberg snapshot from Lakekeeper and emit exact NDJSON."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _format_decimal_fixed(val: Decimal | None) -> str | None:
    if val is None:
        return None
    return format(val, "f")


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return format(obj, "f")
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.astimezone(timezone.utc).isoformat(timespec="microseconds")
    return str(obj)


def build_spark_session(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("cdclab-l1-spark-reader")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{args.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{args.catalog_name}.type", "rest")
        .config(f"spark.sql.catalog.{args.catalog_name}.uri", args.catalog_uri)
        .config(f"spark.sql.catalog.{args.catalog_name}.warehouse", args.warehouse)
        .config(
            f"spark.sql.catalog.{args.catalog_name}.header.X-Iceberg-Access-Delegation",
            "vended-credentials",
        )
    )
    if args.oauth2_server_uri:
        builder = (
            builder.config(
                f"spark.sql.catalog.{args.catalog_name}.oauth2-server-uri",
                args.oauth2_server_uri,
            )
            .config(
                f"spark.sql.catalog.{args.catalog_name}.credential",
                f"{args.client_id}:{args.client_secret}",
            )
            .config(f"spark.sql.catalog.{args.catalog_name}.scope", args.oauth2_scope)
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Read Iceberg snapshot from Lakekeeper via Spark.")
    parser.add_argument("--catalog-name", default="cdclab")
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--namespace", default="public")
    parser.add_argument("--table", default="orders")
    parser.add_argument("--snapshot-id", type=int, required=True)
    parser.add_argument("--rows-out", required=True)
    parser.add_argument("--meta-out", required=True)
    parser.add_argument("--oauth2-server-uri", default="")
    parser.add_argument("--client-id", default="")
    parser.add_argument("--client-secret", default="")
    parser.add_argument("--oauth2-scope", default="lakekeeper")
    args = parser.parse_args()

    spark = build_spark_session(args)
    try:
        full_table = f"{args.catalog_name}.{args.namespace}.{args.table}"

        # Capture runtime versions directly from JVM and Spark.
        jvm = spark._jvm
        iceberg_version = str(jvm.org.apache.iceberg.IcebergBuild.version())
        iceberg_git_id = str(jvm.org.apache.iceberg.IcebergBuild.gitCommitShortId())
        spark_version = str(spark.version)
        scala_version = str(jvm.scala.util.Properties.versionNumberString())
        java_version = str(jvm.java.lang.System.getProperty("java.version"))

        # Inspect snapshots, manifests, files, and table properties.
        snapshots_rows = [
            row.asDict(recursive=True)
            for row in spark.sql(f"SELECT * FROM {full_table}.snapshots ORDER BY committed_at").collect()
        ]
        current_snapshot_id = int(snapshots_rows[-1]["snapshot_id"]) if snapshots_rows else None

        files_rows = [
            row.asDict(recursive=True)
            for row in spark.sql(f"SELECT * FROM {full_table}.files").collect()
        ]
        all_delete_files_rows = [
            row.asDict(recursive=True)
            for row in spark.sql(f"SELECT * FROM {full_table}.all_delete_files").collect()
        ]
        manifests_rows = [
            row.asDict(recursive=True)
            for row in spark.sql(f"SELECT * FROM {full_table}.manifests").collect()
        ]
        describe_rows = [
            row.asDict(recursive=True)
            for row in spark.sql(f"DESCRIBE EXTENDED {full_table}").collect()
        ]

        # Read the exact pinned snapshot using Iceberg SQL time travel syntax (VERSION AS OF).
        read_sql = (
            f"SELECT id, status, amount, updated_at, note, payload, discount "
            f"FROM {full_table} VERSION AS OF {int(args.snapshot_id)} "
            f"ORDER BY id"
        )
        df = spark.sql(read_sql)
        spark_schema = df.schema.jsonValue()
        explain_formatted = "\n".join(
            str(r[0]) for r in spark.sql(f"EXPLAIN FORMATTED {read_sql}").collect()
        )
        explain_extended = "\n".join(
            str(r[0]) for r in spark.sql(f"EXPLAIN EXTENDED {read_sql}").collect()
        )

        # Extract updated_at as exact UTC microseconds since epoch (BIGINT) in SQL to avoid
        # any Python local timezone ambiguity while preserving full microsecond precision.
        df_export = df.select(
            F.col("id"),
            F.col("status"),
            F.col("amount"),
            F.unix_micros(F.col("updated_at")).alias("updated_at_epoch_us"),
            F.col("note"),
            F.col("payload"),
            F.col("discount"),
        )

        rows_path = Path(args.rows_out)
        rows_path.parent.mkdir(parents=True, exist_ok=True)
        row_count = 0
        with rows_path.open("w", encoding="utf-8") as fh:
            for row in df_export.toLocalIterator():
                record = {
                    "id": int(row["id"]),
                    "status": row["status"],
                    "amount": _format_decimal_fixed(row["amount"]),
                    "updated_at_epoch_us": (
                        int(row["updated_at_epoch_us"])
                        if row["updated_at_epoch_us"] is not None
                        else None
                    ),
                    "note": row["note"],
                    "payload": row["payload"],
                    "discount": _format_decimal_fixed(row["discount"]),
                }
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                row_count += 1

        # Collect executed Iceberg BatchScan metrics and reader details after materialization.
        iceberg_scan_details: dict[str, Any] = {}
        try:
            qe = df_export._jdf.queryExecution()
            plan = qe.executedPlan()
            nodes = [plan]
            scan_nodes: list[dict[str, Any]] = []
            while nodes:
                node = nodes.pop(0)
                cls_name = str(node.getClass().getName())
                if "BatchScanExec" in cls_name:
                    scan_obj = node.scan()
                    metrics_map = node.metrics()
                    metrics_dict: dict[str, Any] = {}
                    it = metrics_map.iterator()
                    while it.hasNext():
                        tup = it.next()
                        k = str(tup._1())
                        v = tup._2()
                        metrics_dict[k] = {
                            "name": str(v.name().get()) if v.name().isDefined() else k,
                            "value": int(v.value()),
                        }
                    scan_nodes.append(
                        {
                            "exec_class": cls_name,
                            "scan_class": str(scan_obj.getClass().getName()),
                            "scan_description": str(scan_obj.description()),
                            "read_schema": str(scan_obj.readSchema().simpleString()),
                            "metrics": metrics_dict,
                        }
                    )
                if "AdaptiveSparkPlanExec" in cls_name:
                    nodes.append(node.executedPlan())
                children = node.children()
                for idx in range(int(children.size())):
                    nodes.append(children.apply(idx))
            iceberg_scan_details = {
                "scan_nodes": scan_nodes,
                "parquet_vectorization_conf": spark.conf.get(
                    "spark.sql.iceberg.vectorization.enabled", "default (true)"
                ),
            }
        except Exception as exc:
            iceberg_scan_details = {"capture_error": f"{type(exc).__name__}: {exc}"}

        # Also verify the exact snapshot ID exists in Spark's snapshots table.
        matching_snaps = [
            s for s in snapshots_rows if int(s["snapshot_id"]) == int(args.snapshot_id)
        ]

        meta = {
            "versions": {
                "spark_version": spark_version,
                "scala_version": scala_version,
                "java_version": java_version,
                "iceberg_version": iceberg_version,
                "iceberg_git_commit_short": iceberg_git_id,
                "python_version": sys.version,
            },
            "catalog_config": {
                "catalog_name": args.catalog_name,
                "catalog_uri": args.catalog_uri,
                "warehouse": args.warehouse,
                "namespace": args.namespace,
                "table": args.table,
                "access_delegation": "vended-credentials",
            },
            "requested_snapshot_id": int(args.snapshot_id),
            "spark_current_snapshot_id": current_snapshot_id,
            "requested_snapshot_found": len(matching_snaps) == 1,
            "requested_snapshot_record": matching_snaps[0] if matching_snaps else None,
            "row_count": row_count,
            "spark_schema": spark_schema,
            "snapshots": snapshots_rows,
            "files": files_rows,
            "all_delete_files": all_delete_files_rows,
            "manifests": manifests_rows,
            "describe_extended": describe_rows,
            "explain_formatted": explain_formatted,
            "explain_extended": explain_extended,
            "iceberg_scan_details": iceberg_scan_details,
        }
        meta_path = Path(args.meta_out)
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        meta_path.write_text(
            json.dumps(meta, indent=2, default=_json_default) + "\n",
            encoding="utf-8",
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
