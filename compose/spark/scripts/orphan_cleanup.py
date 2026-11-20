"""PySpark Iceberg maintenance script for K1: metadata inspection and orphan file removal."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys
import time
from typing import Any

from pyspark.sql import SparkSession


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return format(obj, "f")
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.astimezone(timezone.utc).isoformat(timespec="microseconds")
    return str(obj)


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _parse_utc_timestamp_ms(value: str) -> int:
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def build_spark_session(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName(f"cdclab-k1-{args.mode}")
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
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def capture_versions(spark: SparkSession) -> dict[str, str]:
    jvm = spark._jvm
    return {
        "spark_version": str(spark.version),
        "scala_version": str(jvm.scala.util.Properties.versionNumberString()),
        "java_version": str(jvm.java.lang.System.getProperty("java.version")),
        "iceberg_version": str(jvm.org.apache.iceberg.IcebergBuild.version()),
        "iceberg_git_commit_short": str(jvm.org.apache.iceberg.IcebergBuild.gitCommitShortId()),
        "python_version": sys.version,
    }


def inspect_table_metadata(spark: SparkSession, full_table: str) -> dict[str, Any]:
    metadata_log_entries = [
        row.asDict(recursive=True)
        for row in spark.sql(f"SELECT * FROM {full_table}.metadata_log_entries ORDER BY timestamp").collect()
    ]
    snapshots = [
        row.asDict(recursive=True)
        for row in spark.sql(f"SELECT * FROM {full_table}.snapshots ORDER BY committed_at").collect()
    ]
    all_manifests = [
        row.asDict(recursive=True)
        for row in spark.sql(
            f"SELECT content, path, length, partition_spec_id, added_snapshot_id, "
            f"added_data_files_count, existing_data_files_count, deleted_data_files_count "
            f"FROM {full_table}.all_manifests"
        ).collect()
    ]
    all_data_files = [
        row.asDict(recursive=True)
        for row in spark.sql(
            f"SELECT content, file_path, file_format, record_count, file_size_in_bytes "
            f"FROM {full_table}.all_data_files"
        ).collect()
    ]
    all_delete_files = [
        row.asDict(recursive=True)
        for row in spark.sql(
            f"SELECT content, file_path, file_format, record_count, file_size_in_bytes, equality_ids "
            f"FROM {full_table}.all_delete_files"
        ).collect()
    ]
    current_files = [
        row.asDict(recursive=True)
        for row in spark.sql(
            f"SELECT content, file_path, file_format, record_count, file_size_in_bytes, equality_ids "
            f"FROM {full_table}.files"
        ).collect()
    ]
    describe_extended = [
        row.asDict(recursive=True)
        for row in spark.sql(f"DESCRIBE EXTENDED {full_table}").collect()
    ]
    current_snapshot_id = int(snapshots[-1]["snapshot_id"]) if snapshots else None
    return {
        "captured_at_utc": _utc_now_iso(),
        "full_table": full_table,
        "current_snapshot_id": current_snapshot_id,
        "snapshot_count": len(snapshots),
        "metadata_log_entries": metadata_log_entries,
        "snapshots": snapshots,
        "all_manifests": all_manifests,
        "all_data_files": all_data_files,
        "all_delete_files": all_delete_files,
        "current_files": current_files,
        "describe_extended": describe_extended,
    }


def build_remove_orphan_files_sql(
    *,
    catalog_name: str,
    namespace: str,
    table: str,
    older_than: str | None = None,
    location: str | None = None,
    dry_run: bool | None = None,
    prefix_mismatch_mode: str | None = None,
    prefix_listing: bool | None = None,
) -> str:
    params: list[str] = [f"table => {_sql_quote(f'{namespace}.{table}')}"]
    if older_than:
        params.append(f"older_than => TIMESTAMP {_sql_quote(older_than)}")
    if location:
        params.append(f"location => {_sql_quote(location)}")
    if dry_run is not None:
        params.append(f"dry_run => {'true' if dry_run else 'false'}")
    if prefix_mismatch_mode:
        params.append(f"prefix_mismatch_mode => {_sql_quote(prefix_mismatch_mode)}")
    if prefix_listing is not None:
        params.append(f"prefix_listing => {'true' if prefix_listing else 'false'}")
    return f"CALL {catalog_name}.system.remove_orphan_files({', '.join(params)})"


def _parse_tri_bool(value: str) -> bool | None:
    if value == "true":
        return True
    if value == "false":
        return False
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect Iceberg table references or run Spark orphan-file removal via Lakekeeper."
    )
    parser.add_argument(
        "--mode",
        choices=("inspect", "remove_orphans", "action_remove_orphans"),
        required=True,
    )
    parser.add_argument("--catalog-name", default="cdclab")
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--namespace", default="public")
    parser.add_argument("--table", default="orders")
    parser.add_argument("--older-than", default="")
    parser.add_argument("--location", default="")
    parser.add_argument("--dry-run", choices=("true", "false", "omit"), default="omit")
    parser.add_argument("--prefix-mismatch-mode", default="")
    parser.add_argument("--prefix-listing", choices=("true", "false", "omit"), default="omit")
    parser.add_argument("--out-json", required=True)
    args = parser.parse_args()

    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    full_table = f"{args.catalog_name}.{args.namespace}.{args.table}"

    spark = build_spark_session(args)
    try:
        versions = capture_versions(spark)
        if args.mode == "inspect":
            t0 = time.monotonic_ns()
            meta = inspect_table_metadata(spark, full_table)
            duration_ms = round((time.monotonic_ns() - t0) / 1_000_000.0, 2)
            result: dict[str, Any] = {
                "mode": "inspect",
                "ok": True,
                "duration_ms": duration_ms,
                "versions": versions,
                "catalog_config": {
                    "catalog_name": args.catalog_name,
                    "catalog_uri": args.catalog_uri,
                    "warehouse": args.warehouse,
                    "namespace": args.namespace,
                    "table": args.table,
                    "access_delegation": "vended-credentials",
                },
                **meta,
            }
            out_path.write_text(json.dumps(result, indent=2, default=_json_default) + "\n", encoding="utf-8")
            return

        dry_run_val = _parse_tri_bool(args.dry_run)
        prefix_listing_val = _parse_tri_bool(args.prefix_listing)

        snapshots_before = [
            int(r["snapshot_id"])
            for r in spark.sql(f"SELECT snapshot_id FROM {full_table}.snapshots ORDER BY committed_at").collect()
        ]
        started_utc = _utc_now_iso()
        t0 = time.monotonic_ns()

        if args.mode == "remove_orphans":
            sql = build_remove_orphan_files_sql(
                catalog_name=args.catalog_name,
                namespace=args.namespace,
                table=args.table,
                older_than=args.older_than or None,
                location=args.location or None,
                dry_run=dry_run_val,
                prefix_mismatch_mode=args.prefix_mismatch_mode or None,
                prefix_listing=prefix_listing_val,
            )
            try:
                rows = spark.sql(sql).collect()
                duration_ms = round((time.monotonic_ns() - t0) / 1_000_000.0, 2)
                completed_utc = _utc_now_iso()
                orphan_locations = sorted(str(r[0]) for r in rows)
                snapshots_after = [
                    int(r["snapshot_id"])
                    for r in spark.sql(
                        f"SELECT snapshot_id FROM {full_table}.snapshots ORDER BY committed_at"
                    ).collect()
                ]
                result = {
                    "mode": "remove_orphans",
                    "ok": True,
                    "error": None,
                    "sql": sql,
                    "parameters": {
                        "table": f"{args.namespace}.{args.table}",
                        "older_than": args.older_than or None,
                        "location": args.location or None,
                        "dry_run": dry_run_val,
                        "prefix_mismatch_mode": args.prefix_mismatch_mode or None,
                        "prefix_listing": prefix_listing_val,
                    },
                    "started_at_utc": started_utc,
                    "completed_at_utc": completed_utc,
                    "duration_ms": duration_ms,
                    "versions": versions,
                    "orphan_file_count": len(orphan_locations),
                    "orphan_file_locations": orphan_locations,
                    "snapshots_before": snapshots_before,
                    "snapshots_after": snapshots_after,
                }
                out_path.write_text(json.dumps(result, indent=2, default=_json_default) + "\n", encoding="utf-8")
                return
            except Exception as exc:
                duration_ms = round((time.monotonic_ns() - t0) / 1_000_000.0, 2)
                result = {
                    "mode": "remove_orphans",
                    "ok": False,
                    "error": f"{type(exc).__name__}: {exc}",
                    "sql": sql,
                    "parameters": {
                        "table": f"{args.namespace}.{args.table}",
                        "older_than": args.older_than or None,
                        "location": args.location or None,
                        "dry_run": dry_run_val,
                        "prefix_mismatch_mode": args.prefix_mismatch_mode or None,
                        "prefix_listing": prefix_listing_val,
                    },
                    "started_at_utc": started_utc,
                    "completed_at_utc": _utc_now_iso(),
                    "duration_ms": duration_ms,
                    "versions": versions,
                    "orphan_file_count": 0,
                    "orphan_file_locations": [],
                    "snapshots_before": snapshots_before,
                    "snapshots_after": [],
                }
                out_path.write_text(json.dumps(result, indent=2, default=_json_default) + "\n", encoding="utf-8")
                raise

        # mode == "action_remove_orphans": Iceberg's official SparkActions.deleteOrphanFiles API
        # (documented by RemoveOrphanFilesProcedure itself for intervals shorter than 24 hours).
        jvm = spark._jvm
        desc_parts = [f"SparkActions.get(spark).deleteOrphanFiles({full_table})"]
        try:
            iceberg_table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
                spark._jsparkSession, full_table
            )
            action = jvm.org.apache.iceberg.spark.actions.SparkActions.get(
                spark._jsparkSession
            ).deleteOrphanFiles(iceberg_table)

            older_than_ms: int | None = None
            if args.older_than:
                older_than_ms = _parse_utc_timestamp_ms(args.older_than)
                action = action.olderThan(older_than_ms)
                desc_parts.append(f".olderThan({older_than_ms} /* {args.older_than} UTC */)")

            if args.location:
                action = action.location(args.location)
                desc_parts.append(f".location({_sql_quote(args.location)})")

            if prefix_listing_val is not None:
                action = action.usePrefixListing(bool(prefix_listing_val))
                desc_parts.append(f".usePrefixListing({str(bool(prefix_listing_val)).lower()})")

            if args.prefix_mismatch_mode:
                mode_enum = jvm.org.apache.iceberg.actions.DeleteOrphanFiles.PrefixMismatchMode.fromString(
                    args.prefix_mismatch_mode
                )
                action = action.prefixMismatchMode(mode_enum)
                desc_parts.append(f".prefixMismatchMode({args.prefix_mismatch_mode})")

            if dry_run_val is True:
                # Standard JDK Stream.Builder<String> implements java.util.function.Consumer<String>
                # without deleting any objects in storage.
                noop_consumer = jvm.java.util.stream.Stream.builder()
                action = action.deleteWith(noop_consumer)
                desc_parts.append(".deleteWith(Stream.builder() /* dry-run no-op */)")

            desc_parts.append(".execute()")
            action_desc = "".join(desc_parts)

            exec_result = action.execute()
            duration_ms = round((time.monotonic_ns() - t0) / 1_000_000.0, 2)
            completed_utc = _utc_now_iso()

            orphan_locations: list[str] = []
            it = exec_result.orphanFileLocations().iterator()
            while it.hasNext():
                orphan_locations.append(str(it.next()))
            orphan_locations.sort()

            snapshots_after = [
                int(r["snapshot_id"])
                for r in spark.sql(
                    f"SELECT snapshot_id FROM {full_table}.snapshots ORDER BY committed_at"
                ).collect()
            ]
            result = {
                "mode": "action_remove_orphans",
                "ok": True,
                "error": None,
                "action": action_desc,
                "parameters": {
                    "table": f"{args.namespace}.{args.table}",
                    "older_than": args.older_than or None,
                    "older_than_ms": older_than_ms,
                    "location": args.location or None,
                    "dry_run": bool(dry_run_val is True),
                    "prefix_mismatch_mode": args.prefix_mismatch_mode or None,
                    "prefix_listing": prefix_listing_val,
                },
                "started_at_utc": started_utc,
                "completed_at_utc": completed_utc,
                "duration_ms": duration_ms,
                "versions": versions,
                "orphan_file_count": len(orphan_locations),
                "orphan_file_locations": orphan_locations,
                "snapshots_before": snapshots_before,
                "snapshots_after": snapshots_after,
            }
            out_path.write_text(json.dumps(result, indent=2, default=_json_default) + "\n", encoding="utf-8")
        except Exception as exc:
            duration_ms = round((time.monotonic_ns() - t0) / 1_000_000.0, 2)
            result = {
                "mode": "action_remove_orphans",
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                "action": "".join(desc_parts),
                "parameters": {
                    "table": f"{args.namespace}.{args.table}",
                    "older_than": args.older_than or None,
                    "location": args.location or None,
                    "dry_run": bool(dry_run_val is True),
                    "prefix_mismatch_mode": args.prefix_mismatch_mode or None,
                    "prefix_listing": prefix_listing_val,
                },
                "started_at_utc": started_utc,
                "completed_at_utc": _utc_now_iso(),
                "duration_ms": duration_ms,
                "versions": versions,
                "orphan_file_count": 0,
                "orphan_file_locations": [],
                "snapshots_before": snapshots_before,
                "snapshots_after": [],
            }
            out_path.write_text(json.dumps(result, indent=2, default=_json_default) + "\n", encoding="utf-8")
            raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
