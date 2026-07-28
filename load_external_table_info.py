#!/usr/bin/env python3
"""Focused loader for the analyzer's external_table_info_all dataset.

Keep this file beside the current runner.py. It reuses the runner's bundled
Redshift SQL, environment handling, DuckDB locking, backup, and schema rules,
but it fetches and promotes only external_table_info_all.

Usage:
  python load_external_table_info.py --duckdb-path C:\\path\\redshift.duckdb
  python load_external_table_info.py --status --duckdb-path C:\\path\\redshift.duckdb
  python load_external_table_info.py --promote --duckdb-path C:\\path\\redshift.duckdb
"""
from __future__ import annotations

import argparse
from datetime import datetime
import os
from pathlib import Path
import sys
import uuid


TABLE_NAME = "external_table_info_all"


def _active_external_summary_sql(days: float, hours: int | None = None) -> str:
    """One safe Redshift statement over one SYS view; returns no raw rows."""
    lookback_days = max(1, int(round(float(days or 7.0))))
    lookback_hours = max(1, int(hours)) if hours is not None else None
    window = (
        f"DATEADD(hour, -{lookback_hours}, GETDATE())"
        if lookback_hours is not None
        else f"DATEADD(day, -{lookback_days}, GETDATE())"
    )
    return f"""
SELECT
  LOWER(TRIM(table_name)) AS external_table_key,
  MAX(TRIM(table_name)) AS table_name,
  MAX(NULLIF(TRIM(file_location), '')) AS s3_location,
  'ACTIVE_EXTERNAL_TABLE'::VARCHAR AS table_type,
  MIN(start_time) AS observation_start_time,
  MAX(end_time) AS observation_end_time,
  COUNT(DISTINCT query_id) AS query_count,
  COUNT(*) AS external_segment_count,
  COALESCE(SUM(returned_bytes), 0) AS gross_scan_bytes,
  COALESCE(SUM(returned_bytes), 0)::DOUBLE PRECISION / 1073741824.0 AS gross_scan_gb,
  COALESCE(SUM(returned_rows), 0) AS gross_scan_rows,
  'ACTIVE_SCAN_SUMMARY'::VARCHAR AS filtering_assessment,
  COALESCE(SUM(total_partitions), 0) AS total_partitions_considered,
  COALESCE(SUM(qualified_partitions), 0) AS qualified_partitions_scanned,
  CASE WHEN SUM(total_partitions) > 0 THEN
    100.0 * (1.0 - SUM(qualified_partitions)::DOUBLE PRECISION /
      NULLIF(SUM(total_partitions), 0)::DOUBLE PRECISION)
  END AS partition_pruning_pct,
  COALESCE(SUM(scanned_files), 0) AS scanned_files,
  AVG(scanned_files::DOUBLE PRECISION) AS avg_files_per_segment,
  MAX(scanned_files) AS max_files_per_segment,
  COALESCE(SUM(duration), 0)::DOUBLE PRECISION / 1000000.0 AS external_duration_s,
  AVG(duration::DOUBLE PRECISION) / 1000000.0 AS avg_external_duration_s,
  MAX(duration)::DOUBLE PRECISION / 1000000.0 AS max_external_duration_s,
  COALESCE(SUM(s3list_time), 0) AS s3list_time_ms,
  AVG(s3list_time::DOUBLE PRECISION) AS avg_s3list_time_ms,
  MAX(s3list_time) AS max_s3list_time_ms,
  COALESCE(SUM(get_partition_time), 0) AS get_partition_time_total_raw,
  AVG(get_partition_time::DOUBLE PRECISION) AS avg_get_partition_time_raw,
  MAX(get_partition_time) AS max_get_partition_time_raw,
  'AWS_NOT_DOCUMENTED'::VARCHAR AS get_partition_time_unit,
  SUM(CASE WHEN NULLIF(TRIM(warning_message), '') IS NOT NULL THEN 1 ELSE 0 END) AS warning_event_count,
  MAX(NULLIF(TRIM(warning_message), '')) AS warning_example,
  MAX(NULLIF(TRIM(file_format), '')) AS observed_file_format
FROM sys_external_query_detail
WHERE LOWER(TRIM(source_type)) = 's3'
  AND start_time >= {window}
  AND NULLIF(TRIM(table_name), '') IS NOT NULL
GROUP BY LOWER(TRIM(table_name))
"""


def _active_external_window_sql(start_hours_ago: int, end_hours_ago: int) -> str:
    """Return one bounded hour slice, oldest boundary inclusive."""
    start = max(1, int(start_hours_ago))
    end = max(0, min(int(end_hours_ago), start - 1))
    sql = _active_external_summary_sql(1, start)
    return sql.replace(
        "AND NULLIF(TRIM(table_name), '') IS NOT NULL",
        f"AND start_time < DATEADD(hour, -{end}, GETDATE())\n"
        "  AND NULLIF(TRIM(table_name), '') IS NOT NULL",
        1,
    )


def _recent_query_ids_sql(hours: int) -> str:
    lookback = max(1, int(hours or 1))
    return f"""
SELECT query_id
FROM sys_query_history
WHERE start_time >= DATEADD(hour, -{lookback}, GETDATE())
ORDER BY start_time DESC, query_id DESC
"""


def _active_external_query_batch_sql(query_ids) -> str:
    ids = tuple(dict.fromkeys(int(value) for value in query_ids))
    if not ids:
        raise ValueError("At least one query id is required")
    predicate = ", ".join(str(value) for value in ids)
    sql = _active_external_summary_sql(1, 1)
    return sql.replace(
        "AND start_time >= DATEADD(hour, -1, GETDATE())",
        f"AND query_id IN ({predicate})",
        1,
    )


def _rollup_hourly_summaries(frames, pd_module):
    """Merge already summarized hourly results to one row per table."""
    usable = [frame for frame in frames if frame is not None and not frame.empty]
    if not usable:
        return pd_module.DataFrame()
    combined = pd_module.concat(usable, ignore_index=True)
    sums = (
        "query_count", "external_segment_count", "gross_scan_bytes", "gross_scan_rows",
        "total_partitions_considered", "qualified_partitions_scanned", "scanned_files",
        "external_duration_s", "s3list_time_ms", "get_partition_time_total_raw",
        "warning_event_count",
    )
    maxima = (
        "max_files_per_segment", "max_external_duration_s", "max_s3list_time_ms",
        "max_get_partition_time_raw",
    )
    for column in sums + maxima:
        if column in combined.columns:
            combined[column] = pd_module.to_numeric(combined[column], errors="coerce")
    aggregations = {
        "table_name": "first", "s3_location": "first", "table_type": "first",
        "observation_start_time": "min", "observation_end_time": "max",
        "filtering_assessment": "first", "get_partition_time_unit": "first",
        "warning_example": "first", "observed_file_format": "first",
    }
    aggregations.update({column: "sum" for column in sums if column in combined.columns})
    aggregations.update({column: "max" for column in maxima if column in combined.columns})
    aggregations = {key: value for key, value in aggregations.items() if key in combined.columns}
    rolled = combined.groupby("external_table_key", as_index=False, dropna=False).agg(aggregations)
    rolled["gross_scan_gb"] = rolled.get("gross_scan_bytes", 0) / 1073741824.0
    segments = pd_module.to_numeric(
        rolled.get("external_segment_count", 0), errors="coerce"
    ).replace(0, float("nan"))
    rolled["avg_files_per_segment"] = rolled.get("scanned_files", 0) / segments
    rolled["avg_external_duration_s"] = rolled.get("external_duration_s", 0) / segments
    rolled["avg_s3list_time_ms"] = rolled.get("s3list_time_ms", 0) / segments
    rolled["avg_get_partition_time_raw"] = rolled.get("get_partition_time_total_raw", 0) / segments
    total = pd_module.to_numeric(
        rolled.get("total_partitions_considered", 0), errors="coerce"
    ).replace(0, float("nan"))
    qualified = pd_module.to_numeric(
        rolled.get("qualified_partitions_scanned", 0), errors="coerce"
    )
    rolled["partition_pruning_pct"] = 100.0 * (1.0 - qualified / total)
    return rolled.sort_values("gross_scan_bytes", ascending=False, na_position="last").reset_index(drop=True)


def _runner():
    try:
        import runner
    except Exception as exc:
        raise SystemExit(
            "This focused loader must be kept beside the current runner.py. "
            f"The runner could not be imported: {exc}"
        ) from exc
    required = (
        "EXTERNAL_TABLE_INFO_SQL", "EXTERNAL_CATALOG_STAGE_SQL",
        "EXTERNAL_COLUMN_STATS_STAGE_SQL", "external_segments_stage_sql",
        "external_steps_stage_sql", "external_errors_stage_sql",
        "external_query_ids", "minimal_external_catalog_from_segments",
        "assemble_external_table_info", "EXPECTED_COLUMNS", "PERFORMANCE_INDEXES",
        "build_configs", "fetch_frame", "open_duck", "write_tmp_table",
        "stamp_cluster_namespace",
    )
    missing = [name for name in required if not hasattr(runner, name)]
    if missing:
        raise SystemExit(
            "runner.py is older than this loader. Replace it with the newly generated runner.py. "
            f"Missing: {', '.join(missing)}"
        )
    return runner


def _args(argv=None):
    parser = argparse.ArgumentParser(
        description="Load only external-table catalog and Spectrum execution health into DuckDB."
    )
    parser.add_argument("--duckdb-path", default=None, help="Analyzer DuckDB file.")
    parser.add_argument("--lock-wait-seconds", type=float, default=600)
    parser.add_argument("--days", type=float, default=7.0, help="Recent telemetry lookback (default: 7 days).")
    parser.add_argument("--hours", type=int, default=6, help="Recent hours to summarize (default: 6).")
    parser.add_argument("--chunk-hours", type=int, default=1, help="Hours per bounded Redshift query (default: 1).")
    parser.add_argument("--query-batch-size", type=int, default=100, help="Recent query IDs per external summary query (default: 100).")
    parser.add_argument(
        "--statement-timeout-seconds",
        type=int,
        default=600,
        help="Timeout per independent Redshift stage (default: 600 seconds / 10 minutes).",
    )
    parser.add_argument(
        "--include-errors",
        action="store_true",
        help="Also capture sampled SYS_EXTERNAL_QUERY_ERROR rows. Off by default for speed.",
    )
    parser.add_argument("--promote", action="store_true", help="Promote only external_table_info_all_tmp.")
    parser.add_argument("--status", action="store_true", help="Show focused staging/live row counts.")
    parser.add_argument("--no-backup", action="store_true", help="Skip the pre-promotion DuckDB backup.")
    return parser.parse_args(argv)


def _base_args(args):
    return argparse.Namespace(
        duckdb_path=args.duckdb_path,
        table_databases=None,
        days=max(1.0, float(args.days)),
        floor_seconds=600.0,
        floor_basis="execution_time",
    )


def _paths(args, runner):
    runner._load_dotenv_if_present()
    path = args.duckdb_path or os.environ.get("REDSHIFT_DUCKDB_PATH")
    if not path:
        path = str(runner.resolve_default_duckdb_path())
    args.duckdb_path = path
    return Path(path)


def _timeout_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(marker in text for marker in (
        "statement timeout", "canceling statement", "system requested abort",
        "abort query", "query timeout", "timed out",
    ))


def _optional_stage(runner, cfg, database: str, sql: str, stage: str):
    while True:
        try:
            return runner.fetch_frame(cfg, database, sql, stage=stage)
        except Exception as exc:
            if _timeout_error(exc) and sys.stdin.isatty():
                print(f"\nStage reached its 10-minute timeout: {stage}\n{exc}")
                answer = input(
                    "Retry for another 10 minutes [R], or move to the next step [N]? "
                ).strip().lower()
                if answer in {"r", "retry", "c", "continue"}:
                    continue
            print(f"    STAGE SKIPPED: {stage}: {exc}")
            return runner.pd.DataFrame()


def _capture_database_stages(args, runner, cfg, database: str):
    """Capture each Redshift view independently; perform no source-side joins."""
    label = f"{cfg.namespace_id}/{database}"
    # Preserve the smallest, most valuable result first.  Everything after the
    # catalog is optional enrichment and may be skipped after its timeout.
    catalog = _optional_stage(
        runner, cfg, database, runner.EXTERNAL_CATALOG_STAGE_SQL,
        f"external table catalog [{label}]",
    )
    segments = _optional_stage(
        runner, cfg, database,
        runner.external_segments_stage_sql(args.days, args.hours),
        f"external scan segments [{label}]",
    )
    if catalog.empty:
        catalog = runner.minimal_external_catalog_from_segments(segments, database)
        if not catalog.empty:
            print(f"    Built {len(catalog):,} minimal catalog row(s) from active scans")
    query_ids = runner.external_query_ids(segments)
    history = runner.pd.DataFrame({
        "query_id": query_ids,
        "database_name": [database] * len(query_ids),
    })
    steps = []
    errors = []
    batch_size = max(1, int(args.query_batch_size or 100))
    batches = [query_ids[offset:offset + batch_size] for offset in range(0, len(query_ids), batch_size)]
    for number, batch in enumerate(batches, 1):
        steps.append(_optional_stage(
            runner, cfg, database, runner.external_steps_stage_sql(batch),
            f"external output metrics [{label}; batch {number}/{len(batches)}]",
        ))
        if args.include_errors:
            errors.append(_optional_stage(
                runner, cfg, database, runner.external_errors_stage_sql(batch),
                f"external sampled errors [{label}; batch {number}/{len(batches)}]",
            ))
    # Partition-key names are useful but nonessential.  Keep this last so a
    # slow catalog view never prevents the table list and usage rollup.
    columns = _optional_stage(
        runner, cfg, database, runner.EXTERNAL_COLUMN_STATS_STAGE_SQL,
        f"external partition-key metadata [{label}]",
    )
    return {
        "svv_external_tables": catalog,
        "external_column_stats": columns,
        "sys_query_history": history,
        "sys_external_query_detail": segments,
        "sys_query_detail": runner.pd.concat(steps, ignore_index=True) if steps else runner.pd.DataFrame(),
        "sys_external_query_error": runner.pd.concat(errors, ignore_index=True) if errors else runner.pd.DataFrame(),
    }


def load(args, runner) -> int:
    path = _paths(args, runner)
    if not path.is_file():
        raise SystemExit(f"DuckDB file not found: {path}")
    configs = runner.build_configs(_base_args(args))
    namespace_frames = []
    print("Validating and capturing external-table information")
    for cfg in configs:
        cfg.table_databases = ""
        cfg.statement_timeout_ms = max(1, int(args.statement_timeout_seconds)) * 1000
        databases = runner.resolve_table_databases(cfg)
        print(f"  Namespace {cfg.namespace_id} ({cfg.cluster_role})")
        profile_frames = []
        for database in databases:
            print(f"    Staging independent source views from database {database}")
            stages = _capture_database_stages(args, runner, cfg, database)
            profile_frames.append(runner.assemble_external_table_info(stages))
        profile_summary = runner.pd.concat(profile_frames, ignore_index=True) if profile_frames else runner.pd.DataFrame()
        namespace_frames.append(runner.stamp_cluster_namespace(profile_summary, cfg))
    combined = runner.pd.concat(namespace_frames, ignore_index=True) if namespace_frames else runner.pd.DataFrame()
    if not combined.empty:
        combined = combined.drop_duplicates(["namespace_id", "external_table_key"], keep="first")
    print(f"  Rolled up to {len(combined):,} active external table row(s)")
    snapshot_id = str(uuid.uuid4())
    summary_sql = (
        f"-- Independent Redshift source staging; local DuckDB joins; hours={args.hours}; "
        f"query_batch_size={args.query_batch_size}; include_errors={args.include_errors}\n"
        + runner.external_segments_stage_sql(args.days, args.hours)
    )
    runner.write_tmp_table(
        path,
        TABLE_NAME,
        combined,
        snapshot_id,
        summary_sql,
        args.lock_wait_seconds,
    )
    print(
        "\nFocused load complete. Live data was not changed. Promote with:\n\n"
        f'  "{sys.executable}" "{Path(__file__).resolve()}" --promote --duckdb-path "{path}"'
    )
    return 0


def promote(args, runner) -> int:
    path = _paths(args, runner)
    target = runner.tmp_name(TABLE_NAME)
    if not args.no_backup:
        print(f"Backup written: {runner._backup_file(path, args.lock_wait_seconds)}")
    con = runner.open_duck(path, args.lock_wait_seconds)
    try:
        existing = {
            str(row[0]).lower()
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
            ).fetchall()
        }
        if target.lower() not in existing:
            raise SystemExit(f"{target} does not exist. Run the focused loader first.")
        snapshot_row = con.execute(
            f"SELECT snapshot_id FROM {runner.quote_ident(target)} "
            "WHERE snapshot_id IS NOT NULL LIMIT 1"
        ).fetchone()
        snapshot_id = str(snapshot_row[0]) if snapshot_row and snapshot_row[0] else str(uuid.uuid4())
        try:
            source_row = con.execute(
                f"SELECT sql_text FROM {runner.SQL_STASH_TABLE} WHERE table_name = ?",
                [TABLE_NAME],
            ).fetchone()
        except Exception:
            source_row = None
        summary_sql = (
            str(source_row[0]) if source_row and source_row[0]
            else _active_external_summary_sql(args.days, args.hours)
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS analyzer_source_sql (table_name VARCHAR, snapshot_id VARCHAR, "
            "sql_hash VARCHAR, sql_text VARCHAR, recorded_at TIMESTAMP, source VARCHAR, "
            "PRIMARY KEY (table_name, snapshot_id))"
        )
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(f"DROP TABLE IF EXISTS {runner.quote_ident(TABLE_NAME)}")
            con.execute(
                f"ALTER TABLE {runner.quote_ident(target)} RENAME TO {runner.quote_ident(TABLE_NAME)}"
            )
            # Do not register an external-only refresh as the application's
            # global latest workload snapshot. The external tab resolves this
            # table's snapshot independently.
            con.execute(
                "INSERT OR REPLACE INTO analyzer_source_sql VALUES (?, ?, ?, ?, ?, ?)",
                [
                    TABLE_NAME,
                    snapshot_id,
                    runner.sql_hash(summary_sql),
                    summary_sql,
                    datetime.now(),
                    "external-table-loader",
                ],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        runner.ensure_table(con, TABLE_NAME, runner.EXPECTED_COLUMNS[TABLE_NAME])
        runner.ensure_indexes(con)
        rows = con.execute(f"SELECT COUNT(*) FROM {runner.quote_ident(TABLE_NAME)}").fetchone()[0]
        print(f"Promoted {int(rows or 0):,} external table row(s) into {TABLE_NAME}.")
    finally:
        con.close()
    return 0


def status(args, runner) -> int:
    path = _paths(args, runner)
    con = runner.open_duck(path, args.lock_wait_seconds)
    try:
        existing = {
            str(row[0]).lower()
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
            ).fetchall()
        }
        for table in (TABLE_NAME, runner.tmp_name(TABLE_NAME)):
            if table.lower() not in existing:
                print(f"{table}: not present")
            else:
                rows = con.execute(f"SELECT COUNT(*) FROM {runner.quote_ident(table)}").fetchone()[0]
                print(f"{table}: {int(rows or 0):,} row(s)")
    finally:
        con.close()
    return 0


def main(argv=None) -> int:
    args = _args(argv)
    if args.promote and args.status:
        raise SystemExit("Choose either --promote or --status.")
    runner = _runner()
    if args.promote:
        return promote(args, runner)
    if args.status:
        return status(args, runner)
    return load(args, runner)


if __name__ == "__main__":
    raise SystemExit(main())
