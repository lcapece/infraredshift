"""DuckDB storage and SQL analytics for Redshift slow-query snapshots."""
from __future__ import annotations

import os
import re
import shutil
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

from .settings import sql_hash


COMMON_COLUMNS = ("snapshot_id", "captured_at", "namespace_id")


def producer_namespace_id() -> str:
    """Namespace assigned to legacy/single-cluster rows."""
    return (
        os.environ.get("REDSHIFT_NAMESPACE")
        or os.environ.get("REDSHIFT_PRODUCER_NAMESPACE_ID")
        or os.environ.get("REDSHIFT_NAMESPACE_ID")
        or "producer"
    ).strip() or "producer"

# Every DuckDB connection runs the lightweight schema migration below.  The
# desktop capture worker and the GUI can open connections at the same time;
# without process-local serialization they can both reach CREATE OR REPLACE
# VIEW and DuckDB rejects one with a catalog write-write conflict.  An RLock is
# intentional because install_schema() calls install_analytics_views().
_SCHEMA_INSTALL_LOCK = threading.RLock()
_ANALYTICS_STATE_KEY = "analytics_sql_hash"


EXPECTED_COLUMNS: dict[str, tuple[str, ...]] = {
    "query_details": (
        "query_id", "total_spill", "input_bytes", "output_bytes", "blocks_read",
        "blocks_write", "local_read_io", "remote_read_io", "input_rows",
        "output_rows", "selectivity_ratio", "total_step_duration",
        "max_step_duration", "avg_step_duration", "total_steps",
        "max_data_skewness", "max_time_skewness", "segments_used",
        "streams_used", "scan_steps", "join_steps", "sort_steps",
        "agg_steps", "tables_touched", "alert_count", "external_steps",
        "s3_steps", "external_input_bytes", "external_input_rows",
        "external_duration", "external_duration_pct", "remote_io_ratio",
        "external_selectivity", "external_spill_blocks",
        "external_tables_touched", "external_data_skew",
    ),
    "query_health": (
        "query_id", "seq_scan_cnt", "s3_scan_cnt", "partition_loop_cnt",
        "dist_both_cnt", "bcast_cnt", "dist_total_cnt", "has_nested_loop",
        "hash_join_cnt", "subquery_cnt", "network_cnt", "missing_stats_flag",
        "max_est_rows", "max_cost", "cost_score", "dominant_issue", "cost_tier",
    ),
    "query_explain": (
        "userid", "user_id", "query_id", "child_query_sequence",
        "plan_node_id", "plan_parent_id", "plan_node", "plan_info",
    ),
    "query_detail_flow": (
        "user_id", "query_id", "child_query_sequence", "metrics_level",
        "step_name", "step_id", "step_attribute", "stream_id", "segment_id",
        "plan_node_id", "plan_parent_id", "start_time", "end_time", "duration",
        "table_id", "table_name", "source", "is_rrscan", "input_bytes",
        "input_rows", "output_bytes", "output_rows", "blocks_read",
        "blocks_write", "local_read_io", "remote_read_io",
        "spilled_block_local_disk", "spilled_block_remote_disk",
        "data_skewness", "time_skewness", "alert", "detail_row_count",
        "max_duration", "alert_count", "pain_score", "dominant_pain",
        "pain_points", "detail_row_rank", "pain_rank",
    ),
    "query_text": (
        "query_id", "sequence", "sequence_num", "query_text", "text", "sql_text",
    ),
    "child_query_text": (
        "user_id", "query_id", "child_query_sequence", "sequence", "text",
    ),
    "user_info": (
        "user_id", "usesysid", "user_name", "usename",
    ),
    "table_scan_info": (
        "query_id", "full_table_name", "table_database", "schema_name",
        "table_name", "queries", "duration_s", "input_rows_m", "output_rows_m",
        "rrscan_queries", "non_rrscan_queries",
    ),
    "view_definitions": (
        "database", "schema", "view_name", "source_definition",
    ),
    "procedure_definitions": (
        "database", "schema", "procedure_name", "procedure_oid", "procedure_key",
        "owner_id", "owner_name", "argument_names", "argument_modes",
        "argument_type_oids", "all_argument_type_oids", "source_length",
        "source_definition",
    ),
    "external_tables_catalog": (
        "external_table_key", "source_db", "redshift_database_name", "schema_name",
        "table_name", "sortkey",
    ),
    "external_table_metadata": (
        "external_table_key", "source_db", "redshift_database_name", "schema_name",
        "table_name", "column_name", "data_type", "column_number",
        "partition_key_ordinal", "is_nullable",
    ),
    "user_roster": (
        "user_id", "user_name", "email", "first_name", "middle_name",
        "middle_initial", "last_name", "domain", "parsed",
    ),
    "query_group_assignments": (
        "repeat_group_key", "user_name", "engineer_display", "assigned_at",
        # The engineer who OWNS the fix and the business user the query is
        # ASSOCIATED with are different people and are tracked separately.
        # Added after the original two columns; ensure_table adds them to an
        # existing warehouse rather than requiring a reload.
        "associated_user_name", "associated_user_display", "associated_at",
    ),
    "svv_table_info_all": (
        "database", "source_db", "schema", "table_id", "table", "encoded",
        "diststyle", "sortkey1", "max_varchar", "sortkey1_enc", "sortkey_num",
        "size", "pct_used", "empty", "unsorted", "stats_off", "tbl_rows",
        "skew_sortkey1", "skew_rows", "estimated_visible_rows", "risk_event",
        "vacuum_sort_benefit", "create_time",
    ),
    "external_table_info_all": (
        "external_table_key", "redshift_database_name", "schema_name", "table_name",
        "s3_location", "table_type", "input_format", "output_format", "serialization_lib",
        "serde_parameters", "compressed", "table_parameters", "column_count",
        "partition_key_count", "partition_key_columns", "catalog_partition_count", "observation_start_time",
        "observation_end_time", "query_count", "external_segment_count", "user_count",
        "gross_scan_bytes", "gross_scan_gb", "gross_output_bytes", "gross_output_gb",
        "gross_scan_rows", "gross_output_rows", "output_metric_match_count",
        "output_to_scan_byte_ratio", "byte_reduction_pct_estimate",
        "output_to_scan_row_ratio", "row_filter_efficiency_pct", "filtering_assessment",
        "total_partitions_considered", "qualified_partitions_scanned", "partition_pruning_pct",
        "pruning_event_count", "no_pruning_event_count", "scanned_files",
        "avg_files_per_segment", "max_files_per_segment", "external_duration_s",
        "avg_external_duration_s", "max_external_duration_s", "s3list_time_ms",
        "avg_s3list_time_ms", "max_s3list_time_ms", "get_partition_time_total_raw",
        "avg_get_partition_time_raw", "max_get_partition_time_raw", "get_partition_time_unit",
        "recursive_scan_count", "nested_scan_count", "warning_event_count",
        "security_warning_count", "partition_warning_count", "schema_format_warning_count",
        "file_location_warning_count", "connectivity_warning_count", "other_warning_count",
        "warning_example", "sampled_error_count", "queries_with_sampled_errors",
        "truncation_error_count", "overflow_error_count", "invalid_data_error_count",
        "null_handling_error_count", "other_error_count", "distinct_error_code_count",
        "affected_column_count", "max_data_skewness", "max_time_skewness",
        "external_spill_blocks", "observed_file_format",
    ),
    "query_history": (
        "query_id", "user_id", "user_name", "database", "database_name",
        "transaction_id", "session_id", "service_class", "service_class_name",
        "query_type", "status", "aborted", "result_cache_hit", "start_time",
        "end_time", "elapsed_time", "execution_time", "queue_time",
        "planning_time", "compile_time", "lock_wait_time", "rows",
        "returned_rows", "wlm_query_slot_count", "query_label", "error_message",
        # SYS_QUERY_HISTORY repeat-pattern hashes (newer Redshift versions;
        # NULL on clusters that do not emit them). generic_query_hash strips
        # literals, so it groups parameterized repeats across users.
        "user_query_hash", "generic_query_hash",
    ),
    "query_history_all": (
        "query_id", "user_id", "user_name", "database", "database_name",
        "transaction_id", "session_id", "service_class", "service_class_name",
        "query_type", "status", "aborted", "result_cache_hit", "start_time",
        "end_time", "elapsed_time", "execution_time", "queue_time",
        "planning_time", "compile_time", "lock_wait_time", "rows",
        "returned_rows", "wlm_query_slot_count", "query_label", "error_message",
        "user_query_hash", "generic_query_hash",
    ),
}

# Namespace is a storage-level identity field on every captured dataset. Keep
# it in one canonical position without repeating it in every declaration.
EXPECTED_COLUMNS = {
    table_name: tuple(dict.fromkeys(("namespace_id", *columns)))
    for table_name, columns in EXPECTED_COLUMNS.items()
}


INSIGHT_RULE_COUNT = 32

# Reference tables that persist across data reloads. They are populated by
# their own action and cleared only by an explicit manual truncate, so a
# normal capture/full-refresh must never wipe them.
PRESERVED_TABLES = frozenset({"user_roster", "query_group_assignments"})


TABLE_SOURCE_QUERIES = {
    "query_details": "sys_query_detail slow-query aggregate",
    "query_health": "sys_query_explain slow-query plan aggregate",
    "query_explain": "full sys_query_explain rows for bounded slow-query plan analysis",
    "query_detail_flow": "bounded sys_query_detail step rows for execution-flow evidence",
    "query_text": "sys_query_text slow-query SQL text",
    "child_query_text": "sys_child_query_text optimizer-rewritten child SQL text",
    "user_info": "SVV_USER_INFO user id to user name lookup",
    "table_scan_info": "sys_query_detail table scan summary",
    "view_definitions": "pg_views chunked source definitions across discovered databases",
    "procedure_definitions": "pg_proc_info stored procedure bodies across discovered databases",
    "svv_table_info_all": "SVV_TABLE_INFO across discovered databases",
    "external_tables_catalog": "Legacy summarized external-table catalog",
    "external_table_metadata": "SVV_EXTERNAL_COLUMNS metadata (producer, per database)",
    "user_roster": "Parsed name roster from SVV_USER_INFO (producer, enterprise_data_warehouse); persists across loads until manually cleared",
    "query_group_assignments": "Repeat-group -> responsible engineer, keyed on durable repeat_group_key; persists across loads",
    "external_table_info_all": "Spectrum catalog plus SYS_EXTERNAL_QUERY_DETAIL table-level telemetry",
    "query_history": "sys_query_history filtered slow queries",
    "query_history_all": "sys_query_history raw slow-query backup",
}


PERFORMANCE_INDEXES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("snapshot_runs", ("captured_at",)),
    ("query_details", ("snapshot_id", "query_id")),
    ("query_details", ("query_id",)),
    ("query_health", ("snapshot_id", "query_id")),
    ("query_health", ("query_id",)),
    ("query_explain", ("snapshot_id", "query_id")),
    ("query_explain", ("query_id",)),
    ("query_explain", ("snapshot_id", "query_id", "plan_node_id")),
    ("query_detail_flow", ("snapshot_id", "query_id")),
    ("query_detail_flow", ("query_id",)),
    ("query_detail_flow", ("snapshot_id", "query_id", "child_query_sequence", "stream_id", "segment_id", "step_id")),
    ("query_detail_flow", ("snapshot_id", "query_id", "plan_node_id")),
    ("query_text", ("snapshot_id", "query_id")),
    ("query_text", ("query_id",)),
    ("child_query_text", ("snapshot_id", "query_id")),
    ("child_query_text", ("query_id",)),
    ("child_query_text", ("snapshot_id", "query_id", "child_query_sequence", "sequence")),
    ("user_info", ("snapshot_id", "user_id")),
    ("user_info", ("user_id",)),
    ("query_history", ("snapshot_id", "query_id")),
    ("query_history", ("query_id",)),
    ("query_history_all", ("snapshot_id", "query_id")),
    ("query_history_all", ("query_id",)),
    ("table_scan_info", ("snapshot_id", "table_database", "schema_name", "table_name")),
    ("table_scan_info", ("table_name",)),
    ("table_scan_info", ("table_database", "schema_name", "table_name")),
    ("svv_table_info_all", ("snapshot_id", "source_db", "database", "schema", "table")),
    ("svv_table_info_all", ("table",)),
    ("svv_table_info_all", ("source_db", "schema", "table")),
    ("svv_table_info_all", ("database", "schema", "table")),
    ("external_table_info_all", ("snapshot_id", "external_table_key")),
    ("external_table_info_all", ("external_table_key",)),
    ("external_table_info_all", ("redshift_database_name", "schema_name", "table_name")),
    ("external_table_info_all", ("table_name",)),
    ("view_definitions", ("snapshot_id", "database", "schema", "view_name")),
    ("view_definitions", ("view_name",)),
    ("view_definitions", ("database", "schema", "view_name")),
    ("procedure_definitions", ("snapshot_id", "database", "schema", "procedure_name")),
    ("procedure_definitions", ("procedure_key",)),
    ("procedure_definitions", ("database", "schema", "procedure_name")),
)

PERFORMANCE_INDEXES = tuple(
    (
        table_name,
        tuple(dict.fromkeys(("namespace_id", *columns)))
        if table_name in EXPECTED_COLUMNS
        else columns,
    )
    for table_name, columns in PERFORMANCE_INDEXES
)

QUERY_EVIDENCE_TABLES: tuple[str, ...] = (
    "query_history",
    "query_history_all",
    "query_details",
    "query_health",
    "query_explain",
    "query_detail_flow",
    "query_text",
    "child_query_text",
)


def default_duckdb_path() -> Path:
    from .runtime_paths import resolve_runtime_paths

    # Corporate deployments retain ~/RQP/data unless explicitly configured.
    # Relative overrides are launcher-relative, never working-directory-relative.
    return resolve_runtime_paths().duckdb_file


def adopt_legacy_duckdb_file(legacy_path: Path, per_cluster_path: Path) -> bool:
    """First-run migration: when a per-cluster file does not exist yet but the
    legacy default file does and holds data, rename the legacy file to the
    per-cluster name so the current cluster keeps its already-captured rows
    instead of loading blank. Returns True when a file was adopted.

    Safe/idempotent: does nothing if the per-cluster file already exists, if the
    two paths are the same, or if the legacy file is missing/empty."""
    legacy_path = Path(legacy_path)
    per_cluster_path = Path(per_cluster_path)
    if legacy_path == per_cluster_path:
        return False
    if per_cluster_path.exists():
        return False
    if not legacy_path.exists():
        return False
    try:
        if legacy_path.stat().st_size <= 0:
            return False
    except OSError:
        return False
    try:
        per_cluster_path.parent.mkdir(parents=True, exist_ok=True)
        legacy_path.replace(per_cluster_path)
    except OSError:
        return False
    return True


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _raw_store_value(value) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _raw_target_type(col: str) -> str:
    return "TIMESTAMP" if col == "captured_at" else "VARCHAR"


def _column_type_matches(actual: str, expected: str) -> bool:
    actual = (actual or "").upper()
    expected = expected.upper()
    if expected == "VARCHAR":
        return actual in {"VARCHAR", "CHAR", "TEXT"} or actual.startswith("VARCHAR")
    if expected == "TIMESTAMP":
        return actual.startswith("TIMESTAMP")
    return actual == expected


def _typed_expr(col: str, dtype: str) -> str:
    raw = f"NULLIF(NULLIF(NULLIF(TRIM(CAST({quote_ident(col)} AS VARCHAR)), ''), 'None'), 'NULL')"
    return f"TRY_CAST({raw} AS {dtype})"


def _string_expr(col: str) -> str:
    return f"NULLIF(NULLIF(NULLIF(TRIM(CAST({quote_ident(col)} AS VARCHAR)), ''), 'None'), 'NULL')"


def _text_fragment_expr(col: str) -> str:
    value = f"CAST({quote_ident(col)} AS VARCHAR)"
    normalized = f"TRIM({value})"
    return (
        f"CASE WHEN {normalized} IN ('', 'None', 'NULL') "
        f"THEN NULL ELSE {value} END"
    )


def _missing_sortkey_sql(expr: str) -> str:
    normalized = (
        f"REGEXP_REPLACE(UPPER(TRIM(COALESCE(CAST({expr} AS VARCHAR), ''))), "
        "'\\s+', '', 'g')"
    )
    return (
        f"{normalized} IN ("
        "'', '-', 'NONE', 'NULL', 'NAN', 'SORTKEY', '(SORTKEY)', "
        "'AUTO(SORTKEY)', '(AUTO(SORTKEY))'"
        ")"
    )


def _duration_seconds_expr(expr: str) -> str:
    value = f"({expr})"
    cap_seconds = 31_536_000
    return (
        "CASE "
        f"WHEN {value} IS NULL THEN NULL "
        f"WHEN ABS({value}) / 1000000.0 BETWEEN 0 AND {cap_seconds} THEN {value} / 1000000.0 "
        f"WHEN ABS({value}) / 1000000000.0 BETWEEN 0 AND {cap_seconds} THEN {value} / 1000000000.0 "
        f"WHEN ABS({value}) / 1000.0 BETWEEN 0 AND {cap_seconds} THEN {value} / 1000.0 "
        f"WHEN ABS({value}) BETWEEN 0 AND {cap_seconds} THEN {value} "
        f"ELSE {value} / 1000000.0 "
        "END"
    )


def _timestamp_duration_seconds_expr(start_expr: str, end_expr: str) -> str:
    start = f"({start_expr})"
    end = f"({end_expr})"
    diff_ms = f"DATE_DIFF('millisecond', {start}, {end})"
    cap_ms = 31_536_000_000
    return (
        "CASE "
        f"WHEN {start} IS NOT NULL AND {end} IS NOT NULL AND {end} >= {start} "
        f"AND {diff_ms} BETWEEN 0 AND {cap_ms} "
        f"THEN {diff_ms} / 1000.0 "
        "ELSE NULL "
        "END"
    )


def _aliased_string_expr(alias: str, col: str) -> str:
    value = f"{quote_ident(alias)}.{quote_ident(col)}"
    return f"NULLIF(NULLIF(NULLIF(TRIM(CAST({value} AS VARCHAR)), ''), 'None'), 'NULL')"


def _aliased_query_id_expr(alias: str) -> str:
    return f"TRY_CAST({_aliased_string_expr(alias, 'query_id')} AS BIGINT)"


def _query_keep_exists_sql(alias: str) -> str:
    snapshot_expr = _aliased_string_expr(alias, "snapshot_id")
    namespace_expr = _aliased_string_expr(alias, "namespace_id")
    query_expr = _aliased_query_id_expr(alias)
    return (
        "EXISTS ("
        "SELECT 1 FROM _query_trim_keep k "
        f"WHERE k.snapshot_id IS NOT DISTINCT FROM {snapshot_expr} "
        f"AND k.namespace_id IS NOT DISTINCT FROM {namespace_expr} "
        f"AND k.query_id = {query_expr}"
        ")"
    )


@dataclass(frozen=True)
class SnapshotRun:
    snapshot_id: str
    captured_at: datetime
    label: str


class DuckDBStore:
    def __init__(self, path: str | os.PathLike | None = None) -> None:
        self.path = Path(path) if path else default_duckdb_path()

    def connect(self) -> duckdb.DuckDBPyConnection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(self.path))
        try:
            with _SCHEMA_INSTALL_LOCK:
                self.install_schema(con)
        except Exception:
            con.close()
            raise
        return con

    def new_snapshot(self, label: str = "manual") -> SnapshotRun:
        return SnapshotRun(
            snapshot_id=str(uuid.uuid4()),
            captured_at=datetime.now(),
            label=label or "manual",
        )

    def latest_snapshot(self, con: duckdb.DuckDBPyConnection) -> SnapshotRun | None:
        row = con.execute(
            """
SELECT snapshot_id, captured_at, label
FROM snapshot_runs
ORDER BY captured_at DESC
LIMIT 1
"""
        ).fetchone()
        if not row:
            return None
        return SnapshotRun(snapshot_id=row[0], captured_at=row[1], label=row[2] or "manual")

    def backup_database(self, label: str = "backup") -> Path:
        if not self.path.exists():
            raise FileNotFoundError(f"DuckDB file does not exist: {self.path}")
        safe_label = re.sub(r"[^a-zA-Z0-9_-]+", "-", label or "backup").strip("-") or "backup"
        backup_dir = self.path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = backup_dir / f"{self.path.stem}.{safe_label}.{stamp}{self.path.suffix}"
        # A raw copy while DuckDB still has uncheckpointed WAL data is not a
        # backup.  Fail closed if CHECKPOINT cannot complete rather than
        # producing a silently incomplete file that appears recoverable.
        try:
            con = duckdb.connect(str(self.path))
            try:
                con.execute("CHECKPOINT")
            finally:
                con.close()
        except Exception as exc:
            raise RuntimeError(
                f"Could not checkpoint DuckDB before backup; no backup was created: {exc}"
            ) from exc
        shutil.copy2(self.path, backup_path)
        if backup_path.stat().st_size != self.path.stat().st_size:
            backup_path.unlink(missing_ok=True)
            raise RuntimeError("DuckDB backup byte count did not match the source file.")
        # Opening the copy read-only catches truncated/invalid artifacts before
        # the caller is told that a rollback point exists.
        try:
            check = duckdb.connect(str(backup_path), read_only=True)
            try:
                check.execute("SELECT COUNT(*) FROM information_schema.tables").fetchone()
            finally:
                check.close()
        except Exception as exc:
            backup_path.unlink(missing_ok=True)
            raise RuntimeError(f"DuckDB backup verification failed: {exc}") from exc
        return backup_path

    def restore_database_backup(self, backup_path: str | os.PathLike) -> Path | None:
        source = Path(backup_path)
        if not source.exists():
            raise FileNotFoundError(f"Backup file does not exist: {source}")
        current_backup = self.backup_database("before-restore") if self.path.exists() else None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Windows will raise PermissionError if any live connection still holds
        # the target file. Checkpoint + close a throwaway connection first, then
        # replace. Callers must also close their own handles before restore.
        if self.path.exists():
            try:
                con = duckdb.connect(str(self.path))
                try:
                    con.execute("CHECKPOINT")
                finally:
                    con.close()
            except Exception:
                # Best-effort release; the copy itself will fail closed if locked.
                pass
            self._remove_wal_sidecar(self.path)
        shutil.copy2(source, self.path)
        # Drop any leftover WAL that belonged to the previous file identity so
        # DuckDB cannot mix old WAL with the restored main file.
        self._remove_wal_sidecar(self.path)
        return current_backup

    @staticmethod
    def _remove_wal_sidecar(db_path: Path) -> None:
        wal_path = Path(str(db_path) + ".wal")
        if wal_path.exists():
            try:
                wal_path.unlink()
            except OSError:
                pass

    def install_schema(self, con: duckdb.DuckDBPyConnection) -> None:
        con.execute(
            """
CREATE TABLE IF NOT EXISTS snapshot_runs (
  snapshot_id VARCHAR PRIMARY KEY,
  captured_at TIMESTAMP,
  label VARCHAR,
  source VARCHAR
)
"""
        )
        con.execute(
            """
CREATE TABLE IF NOT EXISTS analyzer_source_sql (
  table_name VARCHAR,
  snapshot_id VARCHAR,
  sql_hash VARCHAR,
  sql_text VARCHAR,
  recorded_at TIMESTAMP,
  source VARCHAR,
  PRIMARY KEY (table_name, snapshot_id)
)
"""
        )
        con.execute(
            """
CREATE TABLE IF NOT EXISTS analyzer_schema_state (
  state_key VARCHAR PRIMARY KEY,
  state_value VARCHAR,
  updated_at TIMESTAMP
)
"""
        )
        con.execute(
            """
CREATE TABLE IF NOT EXISTS snapshot_cluster_runs (
  snapshot_id VARCHAR,
  namespace_id VARCHAR,
  cluster_role VARCHAR,
  cluster_name VARCHAR,
  cluster_host VARCHAR,
  primary_database VARCHAR,
  captured_at TIMESTAMP,
  PRIMARY KEY (snapshot_id, namespace_id)
)
"""
        )
        registry_columns = {
            str(row[1]).lower()
            for row in con.execute("PRAGMA table_info('snapshot_cluster_runs')").fetchall()
        }
        if "cluster_name" not in registry_columns:
            con.execute("ALTER TABLE snapshot_cluster_runs ADD COLUMN cluster_name VARCHAR")
        self.recover_missing_base_tables_from_tmp(con)
        for table_name, columns in EXPECTED_COLUMNS.items():
            self.ensure_table(con, table_name, columns)
            # Legacy single-cluster files predate namespace stamping. Backfill
            # only when blank rows actually exist, so a plain GUI open of a
            # healthy warehouse stays write-free (and cannot contend with a
            # loader holding the write lock).
            needs_backfill = con.execute(
                f"SELECT 1 FROM {quote_ident(table_name)} "
                "WHERE NULLIF(TRIM(CAST(namespace_id AS VARCHAR)), '') IS NULL LIMIT 1"
            ).fetchone()
            if needs_backfill:
                con.execute(
                    f"UPDATE {quote_ident(table_name)} SET namespace_id = ? "
                    "WHERE NULLIF(TRIM(CAST(namespace_id AS VARCHAR)), '') IS NULL",
                    [producer_namespace_id()],
                )
        if not _analytics_views_current(con):
            install_analytics_views(con)

    def recover_missing_base_tables_from_tmp(
        self,
        con: duckdb.DuckDBPyConnection,
    ) -> tuple[str, ...]:
        """Recover an interrupted first load where only ``<table>_tmp`` exists.

        Existing base tables are never replaced here. The staging table is
        retained as a rollback/recovery copy.
        """
        existing = {
            str(row[0])
            for row in con.execute(
                """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = current_schema()
"""
            ).fetchall()
        }
        recovered: list[str] = []
        for table_name in EXPECTED_COLUMNS:
            tmp_name = f"{table_name}_tmp"
            if table_name in existing or tmp_name not in existing:
                continue
            # Only materialize a COHERENT staged load: one snapshot stamp.
            # A tmp with no snapshot_id column was not staged by the loader,
            # and one mixing several snapshots is debris from aborted runs —
            # neither may silently become the live table.
            try:
                distinct_snapshots = int(
                    con.execute(
                        f"SELECT COUNT(DISTINCT snapshot_id) FROM {quote_ident(tmp_name)}"
                    ).fetchone()[0]
                    or 0
                )
            except Exception:
                continue
            if distinct_snapshots > 1:
                continue
            con.execute(
                f"CREATE TABLE {quote_ident(table_name)} AS "
                f"SELECT * FROM {quote_ident(tmp_name)}"
            )
            existing.add(table_name)
            recovered.append(table_name)
        return tuple(recovered)

    def ensure_indexes(self, con: duckdb.DuckDBPyConnection) -> None:
        for table_name, columns in PERFORMANCE_INDEXES:
            existing_columns = {
                row[0]
                for row in con.execute(
                    """
SELECT column_name
FROM information_schema.columns
WHERE table_name = ?
""",
                    [table_name],
                ).fetchall()
            }
            index_columns = tuple(col for col in columns if col in existing_columns)
            if not index_columns:
                continue
            index_name = _index_name(table_name, index_columns)
            con.execute(
                f"""
CREATE INDEX IF NOT EXISTS {quote_ident(index_name)}
ON {quote_ident(table_name)} ({", ".join(quote_ident(col) for col in index_columns)})
"""
            )

    def rebuild_indexes(self, con: duckdb.DuckDBPyConnection) -> None:
        """Ensure expected performance indexes exist without dropping current indexes."""
        self.ensure_indexes(con)

    def index_summary(self, con: duckdb.DuckDBPyConnection) -> dict[str, dict[str, object]]:
        try:
            rows = con.execute(
                """
SELECT table_name, index_name, expressions
FROM duckdb_indexes()
"""
            ).fetchall()
        except Exception:
            return {}
        summary: dict[str, dict[str, object]] = {}
        for table_name, index_name, expressions in rows:
            table = str(table_name)
            entry = summary.setdefault(table, {"index_count": 0, "indexed_columns": []})
            entry["index_count"] = int(entry["index_count"]) + 1
            expr_text = _format_index_expression(expressions)
            if expr_text:
                entry["indexed_columns"].append(expr_text)
        return summary

    def ensure_table(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        columns: Iterable[str],
    ) -> None:
        con.execute(
            f"""
CREATE TABLE IF NOT EXISTS {quote_ident(table_name)} (
  snapshot_id VARCHAR,
  captured_at TIMESTAMP
)
"""
        )
        existing = {
            row[0]: row[1]
            for row in con.execute(
                """
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = ?
""",
                [table_name],
            ).fetchall()
        }
        for col in tuple(COMMON_COLUMNS) + tuple(columns):
            if col in existing:
                target_dtype = _raw_target_type(col)
                if not _column_type_matches(existing[col], target_dtype):
                    con.execute(
                        f"ALTER TABLE {quote_ident(table_name)} "
                        f"ALTER COLUMN {quote_ident(col)} SET DATA TYPE {target_dtype}"
                    )
                    existing[col] = target_dtype
                continue
            dtype = _raw_target_type(col)
            con.execute(f"ALTER TABLE {quote_ident(table_name)} ADD COLUMN {quote_ident(col)} {dtype}")
            existing[col] = dtype

    def record_snapshot(
        self,
        con: duckdb.DuckDBPyConnection,
        run: SnapshotRun,
        source: str = "redshift",
    ) -> None:
        con.execute(
            """
INSERT OR REPLACE INTO snapshot_runs (snapshot_id, captured_at, label, source)
VALUES (?, ?, ?, ?)
""",
            [run.snapshot_id, run.captured_at, run.label, source],
        )

    def record_source_sql(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        run: SnapshotRun,
        sql_text: str,
        source: str = "redshift",
    ) -> None:
        con.execute(
            """
INSERT OR REPLACE INTO analyzer_source_sql
  (table_name, snapshot_id, sql_hash, sql_text, recorded_at, source)
VALUES (?, ?, ?, ?, ?, ?)
""",
            [table_name, run.snapshot_id, sql_hash(sql_text), sql_text, datetime.now(), source],
        )

    def latest_source_sql_hashes(self, con: duckdb.DuckDBPyConnection) -> dict[str, str]:
        rows = con.execute(
            """
SELECT table_name, sql_hash
FROM (
  SELECT
    table_name,
    sql_hash,
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY recorded_at DESC) AS rn
  FROM analyzer_source_sql
)
WHERE rn = 1
"""
        ).fetchall()
        return {str(table): str(hash_value or "") for table, hash_value in rows}

    def replace_table_from_frame(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        frame: pd.DataFrame,
        run: SnapshotRun,
    ) -> None:
        self._write_table_from_frame(con, table_name, frame, run, replace=True)

    def append_table_from_frame(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        frame: pd.DataFrame,
        run: SnapshotRun,
        *,
        dedupe_keys: Iterable[str] | None = None,
    ) -> None:
        self._write_table_from_frame(con, table_name, frame, run, replace=False, dedupe_keys=dedupe_keys)

    def query_high_water_mark(self, con: duckdb.DuckDBPyConnection) -> tuple[str, object] | None:
        for query in (
            """
SELECT query_id, start_time
FROM (
  SELECT query_id, start_time FROM v_query_history WHERE query_id IS NOT NULL
  UNION ALL
  SELECT query_id, start_time FROM v_query_history_all WHERE query_id IS NOT NULL
)
ORDER BY start_time DESC NULLS LAST, query_id DESC
LIMIT 1
""",
            """
SELECT query_id, start_time
FROM (
  SELECT TRY_CAST(query_id AS BIGINT) AS query_id, TRY_CAST(start_time AS TIMESTAMP) AS start_time
  FROM query_history
  WHERE TRY_CAST(query_id AS BIGINT) IS NOT NULL
  UNION ALL
  SELECT TRY_CAST(query_id AS BIGINT) AS query_id, TRY_CAST(start_time AS TIMESTAMP) AS start_time
  FROM query_history_all
  WHERE TRY_CAST(query_id AS BIGINT) IS NOT NULL
)
ORDER BY start_time DESC NULLS LAST, query_id DESC
LIMIT 1
""",
        ):
            try:
                row = con.execute(query).fetchone()
            except Exception:
                continue
            if row:
                return str(row[0]), row[1]
        return None

    def _write_table_from_frame(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        frame: pd.DataFrame,
        run: SnapshotRun,
        *,
        replace: bool,
        dedupe_keys: Iterable[str] | None = None,
    ) -> None:
        df = frame.copy()
        df.columns = [str(c).strip() for c in df.columns]
        df = df.loc[:, ~df.columns.duplicated()].copy()
        # Imported/exported frames may already carry warehouse bookkeeping
        # columns.  They belong to the destination snapshot, not the source
        # file, so replace them instead of crashing on DataFrame.insert().
        df = df.drop(columns=[name for name in ("snapshot_id", "captured_at") if name in df.columns])
        # Non-runner import paths are the legacy producer path.  Stamp them at
        # the storage boundary so no Redshift-sourced row can enter DuckDB
        # without its cluster identity.
        if "namespace_id" not in df.columns:
            df.insert(0, "namespace_id", producer_namespace_id())
        else:
            namespace = df["namespace_id"].astype("string").str.strip()
            df["namespace_id"] = namespace.mask(namespace.isna() | namespace.eq(""), producer_namespace_id())
        expected = tuple(EXPECTED_COLUMNS.get(table_name, ())) + tuple(df.columns)
        self.ensure_table(con, table_name, expected)
        for col in df.columns:
            df[col] = df[col].map(_raw_store_value)
        # Refresh/import paths reuse the latest snapshot run, so run.captured_at
        # can be days old; stamp rows with the actual write time instead.
        df.insert(0, "captured_at", datetime.now())
        df.insert(0, "snapshot_id", run.snapshot_id)

        columns = list(dict.fromkeys(df.columns))
        registered = f"incoming_{uuid.uuid4().hex}"
        con.register(registered, df)
        transaction_started = False
        try:
            con.execute("BEGIN TRANSACTION")
            transaction_started = True
            if replace:
                # Replace only this batch's namespaces within the snapshot.
                # Snapshots are shared across clusters, so an unscoped delete
                # would erase other namespaces' (or legacy) rows that this
                # batch is not replacing.
                namespaces = sorted({
                    str(value) for value in df["namespace_id"].tolist() if value is not None
                }) or [producer_namespace_id()]
                placeholders = ", ".join("?" for _ in namespaces)
                con.execute(
                    f"DELETE FROM {quote_ident(table_name)} "
                    f"WHERE snapshot_id = ? "
                    f"AND COALESCE(CAST(namespace_id AS VARCHAR), '') IN ({placeholders})",
                    [run.snapshot_id, *namespaces],
                )
            elif dedupe_keys:
                keys = [
                    key
                    for key in dict.fromkeys(("snapshot_id", "namespace_id", *dedupe_keys))
                    if key in columns
                ]
                if keys:
                    conditions = " AND ".join(
                        f"CAST(t.{quote_ident(key)} AS VARCHAR) IS NOT DISTINCT FROM CAST(i.{quote_ident(key)} AS VARCHAR)"
                        for key in keys
                    )
                    con.execute(
                        f"""
DELETE FROM {quote_ident(table_name)} AS t
USING {quote_ident(registered)} AS i
WHERE {conditions}
"""
                    )
            select_list = []
            for col in columns:
                if col == "captured_at":
                    select_list.append(f"CAST({quote_ident(col)} AS TIMESTAMP) AS {quote_ident(col)}")
                else:
                    select_list.append(f"CAST({quote_ident(col)} AS VARCHAR) AS {quote_ident(col)}")
            con.execute(
                f"""
INSERT INTO {quote_ident(table_name)} ({", ".join(quote_ident(c) for c in columns)})
SELECT {", ".join(select_list)}
FROM {quote_ident(registered)}
"""
            )
            con.execute("COMMIT")
            transaction_started = False
        except Exception:
            if transaction_started:
                try:
                    con.execute("ROLLBACK")
                except Exception:
                    pass
            raise
        finally:
            con.unregister(registered)

    def table_counts(
        self,
        con: duckdb.DuckDBPyConnection,
        expected_sql_hashes: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        latest = self.latest_snapshot(con)
        captured_sql_hashes = self.latest_source_sql_hashes(con)
        index_summary = self.index_summary(con)
        expected_sql_hashes = expected_sql_hashes or {}
        rows = []
        for table_name in sorted(EXPECTED_COLUMNS):
            self.ensure_table(con, table_name, EXPECTED_COLUMNS[table_name])
            count, snapshots, max_captured_at = con.execute(
                f"""
SELECT COUNT(*), COUNT(DISTINCT snapshot_id), MAX(captured_at)
FROM {quote_ident(table_name)}
"""
            ).fetchone()
            latest_snapshot_rows = 0
            if latest is not None:
                latest_snapshot_rows = con.execute(
                    f"SELECT COUNT(*) FROM {quote_ident(table_name)} WHERE snapshot_id = ?",
                    [latest.snapshot_id],
                ).fetchone()[0]
            status = _freshness_status(
                row_count=int(count or 0),
                latest_snapshot_rows=int(latest_snapshot_rows or 0),
                max_captured_at=max_captured_at,
                latest_snapshot_id=latest.snapshot_id if latest else None,
            )
            rows.append(
                {
                    "table_name": table_name,
                    "index_status": "indexed" if int(index_summary.get(table_name, {}).get("index_count") or 0) else "not_indexed",
                    "index_count": int(index_summary.get(table_name, {}).get("index_count") or 0),
                    "indexed_columns": "; ".join(index_summary.get(table_name, {}).get("indexed_columns") or []),
                    "record_count": int(count or 0),
                    "snapshot_count": int(snapshots or 0),
                    "latest_snapshot_rows": int(latest_snapshot_rows or 0),
                    "latest_captured_at": max_captured_at,
                    "source_query": TABLE_SOURCE_QUERIES.get(table_name, ""),
                    "coverage_status": _sql_status(
                        row_count=int(count or 0),
                        captured_hash=captured_sql_hashes.get(table_name, ""),
                        expected_hash=expected_sql_hashes.get(table_name, ""),
                        fallback=status,
                    ),
                    "sql_status": _sql_status(
                        row_count=int(count or 0),
                        captured_hash=captured_sql_hashes.get(table_name, ""),
                        expected_hash=expected_sql_hashes.get(table_name, ""),
                        fallback="untracked",
                    ),
                    "captured_sql_hash": captured_sql_hashes.get(table_name, ""),
                    "expected_sql_hash": expected_sql_hashes.get(table_name, ""),
                    "is_empty": int(count or 0) == 0,
                }
            )
        return pd.DataFrame(rows)

    def preview_query_trim(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        min_elapsed_minutes: int = 30,
        keep_top_n: int = 0,
    ) -> dict[str, Any]:
        min_elapsed_seconds = max(0, int(min_elapsed_minutes or 0)) * 60
        keep_top_n = max(0, int(keep_top_n or 0))
        self._create_query_trim_keep_table(
            con,
            min_elapsed_seconds=min_elapsed_seconds,
            keep_top_n=keep_top_n,
        )
        return self._query_trim_stats(
            con,
            min_elapsed_minutes=min_elapsed_minutes,
            keep_top_n=keep_top_n,
        )

    def trim_query_evidence(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        min_elapsed_minutes: int = 30,
        keep_top_n: int = 0,
    ) -> dict[str, Any]:
        preview = self.preview_query_trim(
            con,
            min_elapsed_minutes=min_elapsed_minutes,
            keep_top_n=keep_top_n,
        )
        con.execute("BEGIN TRANSACTION")
        try:
            for table_name in QUERY_EVIDENCE_TABLES:
                self.ensure_table(con, table_name, EXPECTED_COLUMNS[table_name])
                con.execute(
                    f"""
DELETE FROM {quote_ident(table_name)} AS t
WHERE NOT {_query_keep_exists_sql("t")}
"""
                )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        install_analytics_views(con)
        try:
            con.execute("CHECKPOINT")
        except Exception:
            pass
        return preview

    def _create_query_trim_keep_table(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        min_elapsed_seconds: int,
        keep_top_n: int,
    ) -> None:
        for table_name in QUERY_EVIDENCE_TABLES:
            self.ensure_table(con, table_name, EXPECTED_COLUMNS[table_name])
        install_analytics_views(con)
        con.execute("DROP TABLE IF EXISTS _query_trim_keep")
        con.execute(
            """
CREATE TEMP TABLE _query_trim_keep AS
SELECT snapshot_id, namespace_id, query_id
FROM (
  SELECT
    snapshot_id,
    namespace_id,
    query_id,
    ROW_NUMBER() OVER (
      ORDER BY
        COALESCE(risk_score, 0) DESC,
        COALESCE(total_spill, 0) DESC,
        COALESCE(elapsed_s, 0) DESC,
        query_id
    ) AS rn
  FROM v_slow_queries
  WHERE query_id IS NOT NULL
    AND COALESCE(elapsed_s, 0) >= ?
)
WHERE (? <= 0 OR rn <= ?)
""",
            [min_elapsed_seconds, keep_top_n, keep_top_n],
        )

    def _query_trim_stats(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        min_elapsed_minutes: int,
        keep_top_n: int,
    ) -> dict[str, Any]:
        query_count_before = int(
            con.execute(
                """
SELECT COUNT(*)
FROM (
  SELECT DISTINCT snapshot_id, query_id
  FROM v_slow_queries
  WHERE query_id IS NOT NULL
)
"""
            ).fetchone()[0]
            or 0
        )
        query_count_after = int(con.execute("SELECT COUNT(*) FROM _query_trim_keep").fetchone()[0] or 0)
        table_rows: list[dict[str, Any]] = []
        total_rows_before = 0
        total_rows_after = 0
        for table_name in QUERY_EVIDENCE_TABLES:
            self.ensure_table(con, table_name, EXPECTED_COLUMNS[table_name])
            before = int(con.execute(f"SELECT COUNT(*) FROM {quote_ident(table_name)}").fetchone()[0] or 0)
            after = int(
                con.execute(
                    f"""
SELECT COUNT(*)
FROM {quote_ident(table_name)} AS t
WHERE {_query_keep_exists_sql("t")}
"""
                ).fetchone()[0]
                or 0
            )
            total_rows_before += before
            total_rows_after += after
            table_rows.append(
                {
                    "table_name": table_name,
                    "rows_before": before,
                    "rows_after": after,
                    "rows_removed": max(0, before - after),
                }
            )
        return {
            "min_elapsed_minutes": int(min_elapsed_minutes or 0),
            "keep_top_n": int(keep_top_n or 0),
            "query_count_before": query_count_before,
            "query_count_after": query_count_after,
            "query_count_removed": max(0, query_count_before - query_count_after),
            "query_reduction_pct": _pct_reduction(query_count_before, query_count_after),
            "total_rows_before": total_rows_before,
            "total_rows_after": total_rows_after,
            "total_rows_removed": max(0, total_rows_before - total_rows_after),
            "row_reduction_pct": _pct_reduction(total_rows_before, total_rows_after),
            "table_rows": table_rows,
        }

    def table_is_empty(self, con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
        if table_name not in EXPECTED_COLUMNS:
            raise ValueError(f"Unknown analyzer DuckDB table: {table_name}")
        self.ensure_table(con, table_name, EXPECTED_COLUMNS[table_name])
        count = con.execute(f"SELECT COUNT(*) FROM {quote_ident(table_name)}").fetchone()[0]
        return int(count or 0) == 0

    def snapshot_row_count(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        snapshot_id: str,
    ) -> int:
        if table_name not in EXPECTED_COLUMNS:
            raise ValueError(f"Unknown analyzer DuckDB table: {table_name}")
        self.ensure_table(con, table_name, EXPECTED_COLUMNS[table_name])
        row = con.execute(
            f"SELECT COUNT(*) FROM {quote_ident(table_name)} WHERE snapshot_id = ?",
            [snapshot_id],
        ).fetchone()
        return int(row[0] or 0) if row else 0

    def truncate_table(self, con: duckdb.DuckDBPyConnection, table_name: str) -> None:
        if table_name not in EXPECTED_COLUMNS:
            raise ValueError(f"Unknown analyzer DuckDB table: {table_name}")
        con.execute(f"DELETE FROM {quote_ident(table_name)}")
        install_analytics_views(con)

    def truncate_all_tables(self, con: duckdb.DuckDBPyConnection) -> None:
        for table_name, columns in EXPECTED_COLUMNS.items():
            self.ensure_table(con, table_name, columns)
            # Persistent reference tables (e.g. the user roster) survive a
            # full reload; they are cleared only by their own manual action.
            if table_name in PRESERVED_TABLES:
                continue
            con.execute(f"DELETE FROM {quote_ident(table_name)}")
        con.execute("DELETE FROM analyzer_source_sql")
        con.execute("DELETE FROM snapshot_runs")
        install_analytics_views(con)


def install_analytics_views(con: duckdb.DuckDBPyConnection) -> None:
    with _SCHEMA_INSTALL_LOCK:
        con.execute(_ANALYTICS_SQL)
        con.execute(
            """
INSERT OR REPLACE INTO analyzer_schema_state (state_key, state_value, updated_at)
VALUES (?, ?, ?)
""",
            [_ANALYTICS_STATE_KEY, sql_hash(_ANALYTICS_SQL), datetime.now()],
        )


def _analytics_views_current(con: duckdb.DuckDBPyConnection) -> bool:
    """Return True when this database already has the current view bundle.

    Replaying every CREATE OR REPLACE VIEW on every read-only GUI connection is
    both expensive and unsafe under concurrency.  The SQL hash invalidates the
    bundle whenever any view definition changes; sentinel checks also recover
    automatically if a user drops an important view manually.
    """
    try:
        row = con.execute(
            "SELECT state_value FROM analyzer_schema_state WHERE state_key = ?",
            [_ANALYTICS_STATE_KEY],
        ).fetchone()
        if not row or str(row[0] or "") != sql_hash(_ANALYTICS_SQL):
            return False
        existing = {
            str(item[0]).lower()
            for item in con.execute(
                """
SELECT view_name
FROM duckdb_views()
WHERE schema_name = 'main'
  AND view_name IN ('v_query_details', 'v_child_query_text', 'v_slow_queries', 'v_action_queue')
"""
            ).fetchall()
        }
        return existing == {"v_query_details", "v_child_query_text", "v_slow_queries", "v_action_queue"}
    except Exception:
        return False


def _freshness_status(
    *,
    row_count: int,
    latest_snapshot_rows: int,
    max_captured_at,
    latest_snapshot_id: str | None,
) -> str:
    if row_count <= 0:
        return "empty"
    if latest_snapshot_id and latest_snapshot_rows <= 0:
        return "partial"
    return "current"


def _sql_status(*, row_count: int, captured_hash: str, expected_hash: str, fallback: str) -> str:
    if row_count <= 0:
        return fallback
    if not expected_hash:
        return fallback
    if not captured_hash:
        return "sql_not_recorded"
    if captured_hash != expected_hash:
        return "sql_changed"
    return fallback


def _pct_reduction(before: int, after: int) -> float:
    if before <= 0:
        return 0.0
    return round(max(0, before - after) * 100.0 / before, 2)


def _format_index_expression(expressions) -> str:
    text = str(expressions or "").strip()
    if not text:
        return ""
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    parts = []
    for part in text.split(","):
        cleaned = part.strip().strip("'").strip('"')
        if cleaned.startswith('"') and cleaned.endswith('"'):
            cleaned = cleaned[1:-1]
        cleaned = cleaned.replace('""', '"')
        if cleaned:
            parts.append(cleaned)
    return ", ".join(parts)


def _index_name(table_name: str, columns: tuple[str, ...]) -> str:
    return "idx_" + re.sub(r"[^a-z0-9_]+", "_", f"{table_name}_{'_'.join(columns)}".lower())


_ANALYTICS_SQL = f"""
CREATE OR REPLACE VIEW v_query_details AS
SELECT
  {_string_expr("snapshot_id")} AS snapshot_id,
  {_typed_expr("captured_at", "TIMESTAMP")} AS captured_at,
  {_string_expr("namespace_id")} AS namespace_id,
  {_typed_expr("query_id", "BIGINT")} AS query_id,
  {_typed_expr("total_spill", "DOUBLE")} AS total_spill,
  {_typed_expr("input_bytes", "DOUBLE")} AS input_bytes,
  {_typed_expr("output_bytes", "DOUBLE")} AS output_bytes,
  {_typed_expr("blocks_read", "DOUBLE")} AS blocks_read,
  {_typed_expr("blocks_write", "DOUBLE")} AS blocks_write,
  {_typed_expr("local_read_io", "DOUBLE")} AS local_read_io,
  {_typed_expr("remote_read_io", "DOUBLE")} AS remote_read_io,
  {_typed_expr("input_rows", "DOUBLE")} AS input_rows,
  {_typed_expr("output_rows", "DOUBLE")} AS output_rows,
  {_typed_expr("selectivity_ratio", "DOUBLE")} AS selectivity_ratio,
  {_typed_expr("total_step_duration", "DOUBLE")} AS total_step_duration_us,
  {_typed_expr("max_step_duration", "DOUBLE")} AS max_step_duration_us,
  {_typed_expr("avg_step_duration", "DOUBLE")} AS avg_step_duration_us,
  {_typed_expr("total_steps", "DOUBLE")} AS total_steps,
  {_typed_expr("max_data_skewness", "DOUBLE")} AS max_data_skewness,
  {_typed_expr("max_time_skewness", "DOUBLE")} AS max_time_skewness,
  {_typed_expr("segments_used", "DOUBLE")} AS segments_used,
  {_typed_expr("streams_used", "DOUBLE")} AS streams_used,
  {_typed_expr("scan_steps", "DOUBLE")} AS scan_steps,
  {_typed_expr("join_steps", "DOUBLE")} AS join_steps,
  {_typed_expr("sort_steps", "DOUBLE")} AS sort_steps,
  {_typed_expr("agg_steps", "DOUBLE")} AS agg_steps,
  {_typed_expr("tables_touched", "DOUBLE")} AS tables_touched,
  {_typed_expr("alert_count", "DOUBLE")} AS alert_count,
  {_typed_expr("external_steps", "DOUBLE")} AS external_steps,
  {_typed_expr("s3_steps", "DOUBLE")} AS s3_steps,
  {_typed_expr("external_input_bytes", "DOUBLE")} AS external_input_bytes,
  {_typed_expr("external_input_rows", "DOUBLE")} AS external_input_rows,
  {_typed_expr("external_duration", "DOUBLE")} AS external_duration_us,
  {_typed_expr("external_duration_pct", "DOUBLE")} AS external_duration_pct,
  {_typed_expr("remote_io_ratio", "DOUBLE")} AS remote_io_ratio,
  {_typed_expr("external_selectivity", "DOUBLE")} AS external_selectivity,
  {_typed_expr("external_spill_blocks", "DOUBLE")} AS external_spill_blocks,
  {_typed_expr("external_tables_touched", "DOUBLE")} AS external_tables_touched,
  {_typed_expr("external_data_skew", "DOUBLE")} AS external_data_skew
FROM query_details;

CREATE OR REPLACE VIEW v_query_health_capture AS
SELECT
  {_string_expr("snapshot_id")} AS snapshot_id,
  {_typed_expr("captured_at", "TIMESTAMP")} AS captured_at,
  {_string_expr("namespace_id")} AS namespace_id,
  {_typed_expr("query_id", "BIGINT")} AS query_id,
  {_typed_expr("seq_scan_cnt", "DOUBLE")} AS seq_scan_cnt,
  {_typed_expr("s3_scan_cnt", "DOUBLE")} AS s3_scan_cnt,
  {_typed_expr("partition_loop_cnt", "DOUBLE")} AS partition_loop_cnt,
  {_typed_expr("dist_both_cnt", "DOUBLE")} AS dist_both_cnt,
  {_typed_expr("bcast_cnt", "DOUBLE")} AS bcast_cnt,
  {_typed_expr("dist_total_cnt", "DOUBLE")} AS dist_total_cnt,
  {_typed_expr("has_nested_loop", "DOUBLE")} AS has_nested_loop,
  {_typed_expr("hash_join_cnt", "DOUBLE")} AS hash_join_cnt,
  {_typed_expr("subquery_cnt", "DOUBLE")} AS subquery_cnt,
  {_typed_expr("network_cnt", "DOUBLE")} AS network_cnt,
  {_typed_expr("missing_stats_flag", "DOUBLE")} AS missing_stats_flag,
  {_typed_expr("max_est_rows", "DOUBLE")} AS max_est_rows,
  {_typed_expr("max_cost", "DOUBLE")} AS max_cost,
  {_typed_expr("cost_score", "DOUBLE")} AS cost_score,
  {_string_expr("dominant_issue")} AS dominant_issue,
  {_string_expr("cost_tier")} AS cost_tier
FROM query_health;

CREATE OR REPLACE VIEW v_query_explain AS
SELECT
  {_string_expr("snapshot_id")} AS snapshot_id,
  {_typed_expr("captured_at", "TIMESTAMP")} AS captured_at,
  {_string_expr("namespace_id")} AS namespace_id,
  COALESCE({_typed_expr("user_id", "BIGINT")}, {_typed_expr("userid", "BIGINT")}) AS user_id,
  {_typed_expr("query_id", "BIGINT")} AS query_id,
  {_typed_expr("child_query_sequence", "BIGINT")} AS child_query_sequence,
  {_typed_expr("plan_node_id", "BIGINT")} AS plan_node_id,
  {_typed_expr("plan_parent_id", "BIGINT")} AS plan_parent_id,
  {_text_fragment_expr("plan_node")} AS plan_node,
  {_text_fragment_expr("plan_info")} AS plan_info,
  LOWER(
    COALESCE({_text_fragment_expr("plan_node")}, '')
    || ' '
    || COALESCE({_text_fragment_expr("plan_info")}, '')
  ) AS plan_text_lc
FROM query_explain
WHERE {_typed_expr("query_id", "BIGINT")} IS NOT NULL;

CREATE OR REPLACE VIEW v_query_detail_flow AS
SELECT
  {_string_expr("snapshot_id")} AS snapshot_id,
  {_typed_expr("captured_at", "TIMESTAMP")} AS captured_at,
  {_string_expr("namespace_id")} AS namespace_id,
  {_typed_expr("user_id", "BIGINT")} AS user_id,
  {_typed_expr("query_id", "BIGINT")} AS query_id,
  {_typed_expr("child_query_sequence", "BIGINT")} AS child_query_sequence,
  {_string_expr("metrics_level")} AS metrics_level,
  {_string_expr("step_name")} AS step_name,
  {_typed_expr("step_id", "BIGINT")} AS step_id,
  {_string_expr("step_attribute")} AS step_attribute,
  {_typed_expr("stream_id", "BIGINT")} AS stream_id,
  {_typed_expr("segment_id", "BIGINT")} AS segment_id,
  {_typed_expr("plan_node_id", "BIGINT")} AS plan_node_id,
  {_typed_expr("plan_parent_id", "BIGINT")} AS plan_parent_id,
  {_typed_expr("start_time", "TIMESTAMP")} AS start_time,
  {_typed_expr("end_time", "TIMESTAMP")} AS end_time,
  {_typed_expr("duration", "DOUBLE")} AS duration_us,
  {_duration_seconds_expr(_typed_expr("duration", "DOUBLE"))} AS duration_s,
  {_typed_expr("table_id", "BIGINT")} AS table_id,
  {_string_expr("table_name")} AS table_name,
  {_string_expr("source")} AS source,
  {_string_expr("is_rrscan")} AS is_rrscan,
  {_typed_expr("input_bytes", "DOUBLE")} AS input_bytes,
  {_typed_expr("input_rows", "DOUBLE")} AS input_rows,
  {_typed_expr("output_bytes", "DOUBLE")} AS output_bytes,
  {_typed_expr("output_rows", "DOUBLE")} AS output_rows,
  {_typed_expr("blocks_read", "DOUBLE")} AS blocks_read,
  {_typed_expr("blocks_write", "DOUBLE")} AS blocks_write,
  {_typed_expr("local_read_io", "DOUBLE")} AS local_read_io,
  {_typed_expr("remote_read_io", "DOUBLE")} AS remote_read_io,
  {_typed_expr("spilled_block_local_disk", "DOUBLE")} AS spilled_block_local_disk,
  {_typed_expr("spilled_block_remote_disk", "DOUBLE")} AS spilled_block_remote_disk,
  {_typed_expr("data_skewness", "DOUBLE")} AS data_skewness,
  {_typed_expr("time_skewness", "DOUBLE")} AS time_skewness,
  {_string_expr("alert")} AS alert,
  {_typed_expr("detail_row_count", "BIGINT")} AS detail_row_count,
  {_typed_expr("max_duration", "DOUBLE")} AS max_duration_us,
  {_duration_seconds_expr(_typed_expr("max_duration", "DOUBLE"))} AS max_duration_s,
  {_typed_expr("alert_count", "DOUBLE")} AS alert_count,
  {_typed_expr("pain_score", "DOUBLE")} AS pain_score,
  {_string_expr("dominant_pain")} AS dominant_pain,
  {_string_expr("pain_points")} AS pain_points,
  {_typed_expr("detail_row_rank", "BIGINT")} AS detail_row_rank,
  {_typed_expr("pain_rank", "BIGINT")} AS pain_rank
FROM query_detail_flow
WHERE {_typed_expr("query_id", "BIGINT")} IS NOT NULL;

CREATE OR REPLACE VIEW v_query_explain_health AS
WITH p AS (
  SELECT *
  FROM v_query_explain
),
agg AS (
  SELECT
    snapshot_id,
    namespace_id,
    query_id,
    MAX(captured_at) AS captured_at,
    COUNT(*) AS plan_node_count,
    SUM(CASE WHEN plan_text_lc LIKE '%seq scan%' THEN 1 ELSE 0 END) AS seq_scan_cnt,
    SUM(CASE WHEN plan_text_lc LIKE '%s3 seq scan%' THEN 1 ELSE 0 END) AS s3_scan_cnt,
    SUM(CASE WHEN plan_text_lc LIKE '%partition loop%' THEN 1 ELSE 0 END) AS partition_loop_cnt,
    SUM(CASE WHEN plan_text_lc LIKE '%ds_dist_both%' THEN 1 ELSE 0 END) AS dist_both_cnt,
    SUM(CASE WHEN plan_text_lc LIKE '%ds_bcast_inner%' THEN 1 ELSE 0 END) AS bcast_cnt,
    SUM(CASE WHEN plan_text_lc LIKE '%ds_dist_%' THEN 1 ELSE 0 END) AS dist_total_cnt,
    MAX(CASE WHEN plan_text_lc LIKE '%nested loop%' THEN 1 ELSE 0 END) AS has_nested_loop,
    SUM(CASE WHEN plan_text_lc LIKE '%hash join%' THEN 1 ELSE 0 END) AS hash_join_cnt,
    SUM(CASE WHEN plan_text_lc LIKE '%subquery scan%' THEN 1 ELSE 0 END) AS subquery_cnt,
    SUM(CASE WHEN plan_text_lc LIKE '%network%' THEN 1 ELSE 0 END) AS network_cnt,
    SUM(CASE WHEN plan_text_lc LIKE '%sort%' THEN 1 ELSE 0 END) AS sort_node_cnt,
    SUM(CASE WHEN plan_text_lc LIKE '%agg%' OR plan_text_lc LIKE '%aggregate%' THEN 1 ELSE 0 END) AS agg_node_cnt,
    SUM(CASE WHEN plan_text_lc LIKE '%filter:%' OR plan_text_lc LIKE '%join filter:%' THEN 1 ELSE 0 END) AS filter_node_cnt,
    MAX(CASE WHEN plan_text_lc LIKE '%missing statistics%' THEN 1 ELSE 0 END) AS missing_stats_flag,
    MAX(TRY_CAST(NULLIF(REGEXP_EXTRACT(plan_text_lc, 'rows=([0-9]+)', 1), '') AS DOUBLE)) AS max_est_rows,
    MAX(TRY_CAST(NULLIF(REGEXP_EXTRACT(plan_text_lc, 'cost=[0-9.]+\\.\\.([0-9.]+)', 1), '') AS DOUBLE)) AS max_cost
  FROM p
  GROUP BY snapshot_id, namespace_id, query_id
)
SELECT
  snapshot_id,
  namespace_id,
  captured_at,
  query_id,
  seq_scan_cnt,
  s3_scan_cnt,
  partition_loop_cnt,
  dist_both_cnt,
  bcast_cnt,
  dist_total_cnt,
  has_nested_loop,
  hash_join_cnt,
  subquery_cnt,
  network_cnt,
  missing_stats_flag,
  max_est_rows,
  max_cost,
  (
    COALESCE(seq_scan_cnt, 0) * 1
    + COALESCE(s3_scan_cnt, 0) * 3
    + COALESCE(dist_both_cnt, 0) * 5
    + COALESCE(bcast_cnt, 0) * 2
    + COALESCE(has_nested_loop, 0) * 10
    + COALESCE(missing_stats_flag, 0) * 8
    + COALESCE(partition_loop_cnt, 0) * 2
    + COALESCE(filter_node_cnt, 0)
    + COALESCE(sort_node_cnt, 0)
  ) AS cost_score,
  CASE
    WHEN COALESCE(dist_both_cnt, 0) > 0 THEN 'BAD_DISTRIBUTION'
    WHEN COALESCE(has_nested_loop, 0) = 1 THEN 'NESTED_LOOP_RISK'
    WHEN COALESCE(s3_scan_cnt, 0) > 0 THEN 'S3_HEAVY'
    WHEN COALESCE(missing_stats_flag, 0) = 1 THEN 'MISSING_STATS'
    WHEN COALESCE(seq_scan_cnt, 0) > 5 THEN 'SCAN_HEAVY'
    WHEN COALESCE(filter_node_cnt, 0) > 3 THEN 'FILTER_HEAVY'
    ELSE 'GENERAL'
  END AS dominant_issue,
  CASE
    WHEN (
      COALESCE(dist_both_cnt, 0) * 5
      + COALESCE(has_nested_loop, 0) * 10
      + COALESCE(filter_node_cnt, 0)
      + COALESCE(sort_node_cnt, 0)
    ) > 50 THEN 'VERY_EXPENSIVE'
    WHEN (COALESCE(seq_scan_cnt, 0) + COALESCE(s3_scan_cnt, 0) * 3) > 20 THEN 'EXPENSIVE'
    WHEN COALESCE(seq_scan_cnt, 0) + COALESCE(s3_scan_cnt, 0) > 10 THEN 'MODERATE'
    ELSE 'LIGHT'
  END AS cost_tier,
  plan_node_count,
  sort_node_cnt,
  agg_node_cnt,
  filter_node_cnt,
  1 AS full_explain_available
FROM agg;

CREATE OR REPLACE VIEW v_query_health AS
SELECT
  COALESCE(h.snapshot_id, e.snapshot_id) AS snapshot_id,
  COALESCE(h.captured_at, e.captured_at) AS captured_at,
  COALESCE(h.namespace_id, e.namespace_id) AS namespace_id,
  COALESCE(h.query_id, e.query_id) AS query_id,
  COALESCE(h.seq_scan_cnt, e.seq_scan_cnt, 0) AS seq_scan_cnt,
  COALESCE(h.s3_scan_cnt, e.s3_scan_cnt, 0) AS s3_scan_cnt,
  COALESCE(h.partition_loop_cnt, e.partition_loop_cnt, 0) AS partition_loop_cnt,
  COALESCE(h.dist_both_cnt, e.dist_both_cnt, 0) AS dist_both_cnt,
  COALESCE(h.bcast_cnt, e.bcast_cnt, 0) AS bcast_cnt,
  COALESCE(h.dist_total_cnt, e.dist_total_cnt, 0) AS dist_total_cnt,
  COALESCE(h.has_nested_loop, e.has_nested_loop, 0) AS has_nested_loop,
  COALESCE(h.hash_join_cnt, e.hash_join_cnt, 0) AS hash_join_cnt,
  COALESCE(h.subquery_cnt, e.subquery_cnt, 0) AS subquery_cnt,
  COALESCE(h.network_cnt, e.network_cnt, 0) AS network_cnt,
  COALESCE(h.missing_stats_flag, e.missing_stats_flag, 0) AS missing_stats_flag,
  COALESCE(h.max_est_rows, e.max_est_rows, 0) AS max_est_rows,
  COALESCE(h.max_cost, e.max_cost, 0) AS max_cost,
  COALESCE(h.cost_score, e.cost_score, 0) AS cost_score,
  COALESCE(h.dominant_issue, e.dominant_issue) AS dominant_issue,
  COALESCE(h.cost_tier, e.cost_tier) AS cost_tier,
  COALESCE(e.plan_node_count, 0) AS plan_node_count,
  COALESCE(e.sort_node_cnt, 0) AS sort_node_cnt,
  COALESCE(e.agg_node_cnt, 0) AS agg_node_cnt,
  COALESCE(e.filter_node_cnt, 0) AS filter_node_cnt,
  COALESCE(e.full_explain_available, 0) AS full_explain_available,
  COALESCE(e.cost_score, 0) AS full_explain_cost_score,
  e.dominant_issue AS full_explain_dominant_issue,
  e.cost_tier AS full_explain_cost_tier
FROM v_query_health_capture h
FULL OUTER JOIN v_query_explain_health e
  ON e.snapshot_id IS NOT DISTINCT FROM h.snapshot_id
 AND e.namespace_id IS NOT DISTINCT FROM h.namespace_id
 AND e.query_id = h.query_id;

CREATE OR REPLACE VIEW v_user_info AS
WITH normalized AS (
  SELECT
    {_string_expr("snapshot_id")} AS snapshot_id,
    {_typed_expr("captured_at", "TIMESTAMP")} AS captured_at,
    {_string_expr("namespace_id")} AS namespace_id,
    COALESCE({_typed_expr("user_id", "BIGINT")}, {_typed_expr("usesysid", "BIGINT")}) AS user_id,
    NULLIF(TRIM(COALESCE({_string_expr("user_name")}, {_string_expr("usename")})), '') AS user_name
  FROM user_info
)
SELECT
  MAX(snapshot_id) AS snapshot_id,
  MAX(captured_at) AS captured_at,
  namespace_id,
  user_id,
  MAX(user_name) AS user_name
FROM normalized
WHERE user_id IS NOT NULL
GROUP BY namespace_id, user_id;

CREATE OR REPLACE VIEW v_query_history AS
WITH base AS (
SELECT
  {_string_expr("snapshot_id")} AS snapshot_id,
  {_typed_expr("captured_at", "TIMESTAMP")} AS captured_at,
  {_string_expr("namespace_id")} AS namespace_id,
  {_typed_expr("query_id", "BIGINT")} AS query_id,
  {_typed_expr("user_id", "BIGINT")} AS user_id,
  COALESCE({_string_expr("database_name")}, {_string_expr("database")}) AS database_name,
  {_string_expr("user_name")} AS raw_user_name,
  {_string_expr("query_type")} AS query_type,
  {_string_expr("status")} AS status,
  {_string_expr("aborted")} AS aborted,
  {_typed_expr("start_time", "TIMESTAMP")} AS start_time,
  {_typed_expr("end_time", "TIMESTAMP")} AS end_time,
  {_typed_expr("elapsed_time", "DOUBLE")} AS elapsed_time_us,
  {_typed_expr("execution_time", "DOUBLE")} AS execution_time_us,
  {_typed_expr("queue_time", "DOUBLE")} AS queue_time_us,
  {_typed_expr("planning_time", "DOUBLE")} AS planning_time_us,
  {_typed_expr("compile_time", "DOUBLE")} AS compile_time_us,
  {_typed_expr("lock_wait_time", "DOUBLE")} AS lock_wait_time_us,
  {_typed_expr("rows", "DOUBLE")} AS rows,
  {_typed_expr("returned_rows", "DOUBLE")} AS returned_rows,
  {_typed_expr("wlm_query_slot_count", "DOUBLE")} AS wlm_query_slot_count,
  {_string_expr("service_class_name")} AS service_class_name,
  {_string_expr("query_label")} AS query_label,
  {_string_expr("error_message")} AS error_message,
  {_string_expr("user_query_hash")} AS user_query_hash,
  {_string_expr("generic_query_hash")} AS generic_query_hash
FROM query_history
)
SELECT
  b.snapshot_id,
  b.captured_at,
  b.namespace_id,
  b.query_id,
  b.user_id,
  b.database_name,
  COALESCE(
    CASE
      WHEN NULLIF(TRIM(b.raw_user_name), '') IS NOT NULL
       AND NOT REGEXP_MATCHES(TRIM(b.raw_user_name), '^[0-9]+$')
      THEN b.raw_user_name
      ELSE NULL
    END,
    ui.user_name,
    CAST(b.user_id AS VARCHAR)
  ) AS user_name,
  b.query_type,
  b.status,
  b.aborted,
  b.start_time,
  b.end_time,
  b.elapsed_time_us,
  b.execution_time_us,
  b.queue_time_us,
  b.planning_time_us,
  b.compile_time_us,
  b.lock_wait_time_us,
  b.rows,
  b.returned_rows,
  b.wlm_query_slot_count,
  b.service_class_name,
  b.query_label,
  b.error_message,
  b.user_query_hash,
  b.generic_query_hash
FROM base b
LEFT JOIN v_user_info ui
  ON ui.namespace_id IS NOT DISTINCT FROM b.namespace_id
 AND ui.user_id = b.user_id;

CREATE OR REPLACE VIEW v_query_history_all AS
WITH base AS (
SELECT
  {_string_expr("snapshot_id")} AS snapshot_id,
  {_typed_expr("captured_at", "TIMESTAMP")} AS captured_at,
  {_string_expr("namespace_id")} AS namespace_id,
  {_typed_expr("query_id", "BIGINT")} AS query_id,
  {_typed_expr("user_id", "BIGINT")} AS user_id,
  COALESCE({_string_expr("database_name")}, {_string_expr("database")}) AS database_name,
  {_string_expr("user_name")} AS raw_user_name,
  {_string_expr("query_type")} AS query_type,
  {_string_expr("status")} AS status,
  {_string_expr("aborted")} AS aborted,
  {_typed_expr("start_time", "TIMESTAMP")} AS start_time,
  {_typed_expr("end_time", "TIMESTAMP")} AS end_time,
  {_typed_expr("elapsed_time", "DOUBLE")} AS elapsed_time_us,
  {_typed_expr("execution_time", "DOUBLE")} AS execution_time_us,
  {_typed_expr("queue_time", "DOUBLE")} AS queue_time_us,
  {_typed_expr("planning_time", "DOUBLE")} AS planning_time_us,
  {_typed_expr("compile_time", "DOUBLE")} AS compile_time_us,
  {_typed_expr("lock_wait_time", "DOUBLE")} AS lock_wait_time_us,
  {_typed_expr("rows", "DOUBLE")} AS rows,
  {_typed_expr("returned_rows", "DOUBLE")} AS returned_rows,
  {_typed_expr("wlm_query_slot_count", "DOUBLE")} AS wlm_query_slot_count,
  {_string_expr("service_class_name")} AS service_class_name,
  {_string_expr("query_label")} AS query_label,
  {_string_expr("error_message")} AS error_message,
  {_string_expr("user_query_hash")} AS user_query_hash,
  {_string_expr("generic_query_hash")} AS generic_query_hash
FROM query_history_all
)
SELECT
  b.snapshot_id,
  b.captured_at,
  b.namespace_id,
  b.query_id,
  b.user_id,
  b.database_name,
  COALESCE(
    CASE
      WHEN NULLIF(TRIM(b.raw_user_name), '') IS NOT NULL
       AND NOT REGEXP_MATCHES(TRIM(b.raw_user_name), '^[0-9]+$')
      THEN b.raw_user_name
      ELSE NULL
    END,
    ui.user_name,
    CAST(b.user_id AS VARCHAR)
  ) AS user_name,
  b.query_type,
  b.status,
  b.aborted,
  b.start_time,
  b.end_time,
  b.elapsed_time_us,
  b.execution_time_us,
  b.queue_time_us,
  b.planning_time_us,
  b.compile_time_us,
  b.lock_wait_time_us,
  b.rows,
  b.returned_rows,
  b.wlm_query_slot_count,
  b.service_class_name,
  b.query_label,
  b.error_message,
  b.user_query_hash,
  b.generic_query_hash
FROM base b
LEFT JOIN v_user_info ui
  ON ui.namespace_id IS NOT DISTINCT FROM b.namespace_id
 AND ui.user_id = b.user_id;

CREATE OR REPLACE VIEW v_history_union AS
SELECT * FROM v_query_history
UNION ALL
SELECT a.*
FROM v_query_history_all a
WHERE NOT EXISTS (
  SELECT 1
  FROM v_query_history h
  WHERE h.snapshot_id IS NOT DISTINCT FROM a.snapshot_id
    AND h.namespace_id IS NOT DISTINCT FROM a.namespace_id
    AND h.query_id = a.query_id
);

CREATE OR REPLACE VIEW v_query_text AS
WITH fragments AS (
  SELECT
    {_string_expr("snapshot_id")} AS snapshot_id,
    {_typed_expr("captured_at", "TIMESTAMP")} AS captured_at,
    {_string_expr("namespace_id")} AS namespace_id,
    {_typed_expr("query_id", "BIGINT")} AS query_id,
    COALESCE({_typed_expr("sequence", "BIGINT")}, {_typed_expr("sequence_num", "BIGINT")}, 0) AS sequence_num,
    COALESCE({_text_fragment_expr("query_text")}, {_text_fragment_expr("sql_text")}, {_text_fragment_expr("text")}) AS fragment
  FROM query_text
)
SELECT
  snapshot_id,
  namespace_id,
  MAX(captured_at) AS captured_at,
  query_id,
  STRING_AGG(COALESCE(fragment, ''), '' ORDER BY sequence_num) AS sql_text
FROM fragments
WHERE query_id IS NOT NULL
GROUP BY snapshot_id, namespace_id, query_id;

CREATE OR REPLACE VIEW v_child_query_text AS
WITH fragments AS (
  SELECT
    {_string_expr("snapshot_id")} AS snapshot_id,
    {_typed_expr("captured_at", "TIMESTAMP")} AS captured_at,
    {_string_expr("namespace_id")} AS namespace_id,
    {_typed_expr("user_id", "BIGINT")} AS user_id,
    {_typed_expr("query_id", "BIGINT")} AS query_id,
    {_typed_expr("child_query_sequence", "BIGINT")} AS child_query_sequence,
    {_typed_expr("sequence", "BIGINT")} AS sequence_num,
    {_text_fragment_expr("text")} AS fragment
  FROM child_query_text
)
SELECT
  snapshot_id,
  namespace_id,
  MAX(captured_at) AS captured_at,
  MAX(user_id) AS user_id,
  query_id,
  child_query_sequence,
  STRING_AGG(COALESCE(fragment, ''), '' ORDER BY sequence_num) AS child_sql_text,
  COUNT(*) AS fragment_count
FROM fragments
WHERE query_id IS NOT NULL
  AND child_query_sequence IS NOT NULL
GROUP BY snapshot_id, namespace_id, query_id, child_query_sequence;

CREATE OR REPLACE VIEW v_table_info AS
SELECT
  {_string_expr("snapshot_id")} AS snapshot_id,
  {_typed_expr("captured_at", "TIMESTAMP")} AS captured_at,
  {_string_expr("namespace_id")} AS namespace_id,
  COALESCE({_string_expr("source_db")}, {_string_expr("database")}) AS source_db,
  {_string_expr("database")} AS database_name,
  {_string_expr("schema")} AS schema_name,
  {_typed_expr("table_id", "BIGINT")} AS table_id,
  {_string_expr("table")} AS table_name,
  LOWER(COALESCE({_string_expr("namespace_id")}, '') || '.' || COALESCE({_string_expr("source_db")}, {_string_expr("database")}, '') || '.' || COALESCE({_string_expr("schema")}, '') || '.' || COALESCE({_string_expr("table")}, '')) AS table_key,
  {_string_expr("encoded")} AS encoded,
  {_string_expr("diststyle")} AS diststyle,
  {_string_expr("sortkey1")} AS sortkey1,
  {_typed_expr("sortkey_num", "DOUBLE")} AS sortkey_num,
  {_typed_expr("size", "DOUBLE")} AS size_mb,
  {_typed_expr("pct_used", "DOUBLE")} AS pct_used,
  {_typed_expr("empty", "DOUBLE")} AS empty_blocks,
  {_typed_expr("unsorted", "DOUBLE")} AS unsorted_pct,
  {_typed_expr("stats_off", "DOUBLE")} AS stats_off,
  {_typed_expr("tbl_rows", "DOUBLE")} AS tbl_rows,
  {_typed_expr("skew_sortkey1", "DOUBLE")} AS skew_sortkey1,
  {_typed_expr("skew_rows", "DOUBLE")} AS skew_rows,
  {_typed_expr("estimated_visible_rows", "DOUBLE")} AS estimated_visible_rows,
  {_string_expr("risk_event")} AS risk_event,
  {_typed_expr("vacuum_sort_benefit", "DOUBLE")} AS vacuum_sort_benefit,
  {_typed_expr("create_time", "TIMESTAMP")} AS create_time
FROM svv_table_info_all;

CREATE OR REPLACE VIEW v_external_table_info AS
SELECT
  {_string_expr("snapshot_id")} AS snapshot_id,
  {_typed_expr("captured_at", "TIMESTAMP")} AS captured_at,
  {_string_expr("namespace_id")} AS namespace_id,
  LOWER(COALESCE({_string_expr("namespace_id")}, '') || '.' || COALESCE({_string_expr("external_table_key")}, '')) AS external_table_key,
  {_string_expr("redshift_database_name")} AS redshift_database_name,
  {_string_expr("schema_name")} AS schema_name,
  {_string_expr("table_name")} AS table_name,
  {_string_expr("s3_location")} AS s3_location,
  {_string_expr("table_type")} AS table_type,
  {_string_expr("input_format")} AS input_format,
  {_string_expr("output_format")} AS output_format,
  {_string_expr("serialization_lib")} AS serialization_lib,
  {_string_expr("serde_parameters")} AS serde_parameters,
  {_typed_expr("compressed", "BIGINT")} AS compressed,
  {_string_expr("table_parameters")} AS table_parameters,
  {_typed_expr("column_count", "BIGINT")} AS column_count,
  {_typed_expr("partition_key_count", "BIGINT")} AS partition_key_count,
  {_string_expr("partition_key_columns")} AS partition_key_columns,
  {_typed_expr("catalog_partition_count", "BIGINT")} AS catalog_partition_count,
  {_typed_expr("observation_start_time", "TIMESTAMP")} AS observation_start_time,
  {_typed_expr("observation_end_time", "TIMESTAMP")} AS observation_end_time,
  {_typed_expr("query_count", "BIGINT")} AS query_count,
  {_typed_expr("external_segment_count", "BIGINT")} AS external_segment_count,
  {_typed_expr("user_count", "BIGINT")} AS user_count,
  {_typed_expr("gross_scan_bytes", "DOUBLE")} AS gross_scan_bytes,
  {_typed_expr("gross_scan_gb", "DOUBLE")} AS gross_scan_gb,
  {_typed_expr("gross_output_bytes", "DOUBLE")} AS gross_output_bytes,
  {_typed_expr("gross_output_gb", "DOUBLE")} AS gross_output_gb,
  {_typed_expr("gross_scan_rows", "DOUBLE")} AS gross_scan_rows,
  {_typed_expr("gross_output_rows", "DOUBLE")} AS gross_output_rows,
  {_typed_expr("output_metric_match_count", "BIGINT")} AS output_metric_match_count,
  {_typed_expr("output_to_scan_byte_ratio", "DOUBLE")} AS output_to_scan_byte_ratio,
  {_typed_expr("byte_reduction_pct_estimate", "DOUBLE")} AS byte_reduction_pct_estimate,
  {_typed_expr("output_to_scan_row_ratio", "DOUBLE")} AS output_to_scan_row_ratio,
  {_typed_expr("row_filter_efficiency_pct", "DOUBLE")} AS row_filter_efficiency_pct,
  {_string_expr("filtering_assessment")} AS filtering_assessment,
  {_typed_expr("total_partitions_considered", "DOUBLE")} AS total_partitions_considered,
  {_typed_expr("qualified_partitions_scanned", "DOUBLE")} AS qualified_partitions_scanned,
  {_typed_expr("partition_pruning_pct", "DOUBLE")} AS partition_pruning_pct,
  {_typed_expr("pruning_event_count", "BIGINT")} AS pruning_event_count,
  {_typed_expr("no_pruning_event_count", "BIGINT")} AS no_pruning_event_count,
  {_typed_expr("scanned_files", "DOUBLE")} AS scanned_files,
  {_typed_expr("avg_files_per_segment", "DOUBLE")} AS avg_files_per_segment,
  {_typed_expr("max_files_per_segment", "DOUBLE")} AS max_files_per_segment,
  {_typed_expr("external_duration_s", "DOUBLE")} AS external_duration_s,
  {_typed_expr("avg_external_duration_s", "DOUBLE")} AS avg_external_duration_s,
  {_typed_expr("max_external_duration_s", "DOUBLE")} AS max_external_duration_s,
  {_typed_expr("s3list_time_ms", "DOUBLE")} AS s3list_time_ms,
  {_typed_expr("avg_s3list_time_ms", "DOUBLE")} AS avg_s3list_time_ms,
  {_typed_expr("max_s3list_time_ms", "DOUBLE")} AS max_s3list_time_ms,
  {_typed_expr("get_partition_time_total_raw", "DOUBLE")} AS get_partition_time_total_raw,
  {_typed_expr("avg_get_partition_time_raw", "DOUBLE")} AS avg_get_partition_time_raw,
  {_typed_expr("max_get_partition_time_raw", "DOUBLE")} AS max_get_partition_time_raw,
  {_string_expr("get_partition_time_unit")} AS get_partition_time_unit,
  {_typed_expr("recursive_scan_count", "BIGINT")} AS recursive_scan_count,
  {_typed_expr("nested_scan_count", "BIGINT")} AS nested_scan_count,
  {_typed_expr("warning_event_count", "BIGINT")} AS warning_event_count,
  {_typed_expr("security_warning_count", "BIGINT")} AS security_warning_count,
  {_typed_expr("partition_warning_count", "BIGINT")} AS partition_warning_count,
  {_typed_expr("schema_format_warning_count", "BIGINT")} AS schema_format_warning_count,
  {_typed_expr("file_location_warning_count", "BIGINT")} AS file_location_warning_count,
  {_typed_expr("connectivity_warning_count", "BIGINT")} AS connectivity_warning_count,
  {_typed_expr("other_warning_count", "BIGINT")} AS other_warning_count,
  {_string_expr("warning_example")} AS warning_example,
  {_typed_expr("sampled_error_count", "BIGINT")} AS sampled_error_count,
  {_typed_expr("queries_with_sampled_errors", "BIGINT")} AS queries_with_sampled_errors,
  {_typed_expr("truncation_error_count", "BIGINT")} AS truncation_error_count,
  {_typed_expr("overflow_error_count", "BIGINT")} AS overflow_error_count,
  {_typed_expr("invalid_data_error_count", "BIGINT")} AS invalid_data_error_count,
  {_typed_expr("null_handling_error_count", "BIGINT")} AS null_handling_error_count,
  {_typed_expr("other_error_count", "BIGINT")} AS other_error_count,
  {_typed_expr("distinct_error_code_count", "BIGINT")} AS distinct_error_code_count,
  {_typed_expr("affected_column_count", "BIGINT")} AS affected_column_count,
  {_typed_expr("max_data_skewness", "DOUBLE")} AS max_data_skewness,
  {_typed_expr("max_time_skewness", "DOUBLE")} AS max_time_skewness,
  {_typed_expr("external_spill_blocks", "DOUBLE")} AS external_spill_blocks,
  {_string_expr("observed_file_format")} AS observed_file_format
FROM external_table_info_all;

CREATE OR REPLACE VIEW v_table_scan_info AS
SELECT
  {_string_expr("snapshot_id")} AS snapshot_id,
  {_typed_expr("captured_at", "TIMESTAMP")} AS captured_at,
  {_string_expr("namespace_id")} AS namespace_id,
  {_typed_expr("query_id", "BIGINT")} AS query_id,
  {_string_expr("full_table_name")} AS full_table_name,
  {_string_expr("table_database")} AS table_database,
  {_string_expr("schema_name")} AS schema_name,
  {_string_expr("table_name")} AS table_name,
  LOWER(COALESCE({_string_expr("namespace_id")}, '') || '.' || COALESCE({_string_expr("table_database")}, '') || '.' || COALESCE({_string_expr("schema_name")}, '') || '.' || COALESCE({_string_expr("table_name")}, '')) AS table_key,
  {_typed_expr("queries", "DOUBLE")} AS scan_query_count,
  {_typed_expr("duration_s", "DOUBLE")} AS scan_duration_s,
  {_typed_expr("input_rows_m", "DOUBLE")} AS scan_input_rows_m,
  {_typed_expr("output_rows_m", "DOUBLE")} AS scan_output_rows_m,
  {_typed_expr("rrscan_queries", "DOUBLE")} AS rrscan_query_count,
  {_typed_expr("non_rrscan_queries", "DOUBLE")} AS non_rrscan_query_count,
  CASE
    WHEN {_typed_expr("queries", "DOUBLE")} > 0
    THEN {_typed_expr("rrscan_queries", "DOUBLE")} / {_typed_expr("queries", "DOUBLE")}
    ELSE NULL
  END AS rrscan_query_pct,
  CASE
    WHEN {_typed_expr("queries", "DOUBLE")} > 0
    THEN {_typed_expr("non_rrscan_queries", "DOUBLE")} / {_typed_expr("queries", "DOUBLE")}
    ELSE NULL
  END AS full_scan_query_pct
FROM table_scan_info;

CREATE OR REPLACE VIEW v_view_definitions AS
SELECT
  {_string_expr("snapshot_id")} AS snapshot_id,
  {_typed_expr("captured_at", "TIMESTAMP")} AS captured_at,
  {_string_expr("namespace_id")} AS namespace_id,
  {_string_expr("database")} AS database,
  {_string_expr("schema")} AS schema,
  {_string_expr("view_name")} AS view_name,
  LOWER(COALESCE({_string_expr("namespace_id")}, '') || '.' || COALESCE({_string_expr("database")}, '') || '.' || COALESCE({_string_expr("schema")}, '') || '.' || COALESCE({_string_expr("view_name")}, '')) AS view_key,
  {_text_fragment_expr("source_definition")} AS source_definition
FROM view_definitions;

CREATE OR REPLACE VIEW v_procedure_definitions AS
SELECT
  {_string_expr("snapshot_id")} AS snapshot_id,
  {_typed_expr("captured_at", "TIMESTAMP")} AS captured_at,
  {_string_expr("namespace_id")} AS namespace_id,
  {_string_expr("database")} AS database,
  {_string_expr("schema")} AS schema,
  {_string_expr("procedure_name")} AS procedure_name,
  CASE
    WHEN NULLIF(TRIM({_string_expr("procedure_key")}), '') IS NOT NULL
      THEN LOWER(COALESCE({_string_expr("namespace_id")}, '') || '.' || {_string_expr("procedure_key")})
    ELSE LOWER(COALESCE({_string_expr("namespace_id")}, '') || '.' || COALESCE({_string_expr("database")}, '') || '.' || COALESCE({_string_expr("schema")}, '') || '.' || COALESCE({_string_expr("procedure_name")}, ''))
  END AS procedure_key,
  {_string_expr("procedure_oid")} AS procedure_oid,
  {_string_expr("owner_id")} AS owner_id,
  {_string_expr("owner_name")} AS owner_name,
  {_string_expr("argument_names")} AS argument_names,
  {_string_expr("argument_modes")} AS argument_modes,
  {_string_expr("argument_type_oids")} AS argument_type_oids,
  {_string_expr("all_argument_type_oids")} AS all_argument_type_oids,
  {_typed_expr("source_length", "DOUBLE")} AS source_length,
  {_text_fragment_expr("source_definition")} AS source_definition
FROM procedure_definitions;

CREATE OR REPLACE VIEW v_slow_queries AS
WITH ids AS (
  SELECT snapshot_id, namespace_id, query_id FROM v_history_union WHERE query_id IS NOT NULL
  UNION
  SELECT snapshot_id, namespace_id, query_id FROM v_query_details WHERE query_id IS NOT NULL
  UNION
  SELECT snapshot_id, namespace_id, query_id FROM v_query_health WHERE query_id IS NOT NULL
  UNION
  SELECT snapshot_id, namespace_id, query_id FROM v_query_text WHERE query_id IS NOT NULL
)
SELECT
  ids.snapshot_id,
  ids.namespace_id,
  ids.query_id,
  COALESCE(h.captured_at, d.captured_at, qh.captured_at, qt.captured_at) AS captured_at,
  h.database_name,
  h.user_name,
  h.query_type,
  h.status,
  h.start_time,
  h.end_time,
  COALESCE(
    {_timestamp_duration_seconds_expr("h.start_time", "h.end_time")},
    {_duration_seconds_expr("h.elapsed_time_us")},
    {_duration_seconds_expr("d.total_step_duration_us")}
  ) AS elapsed_s,
  {_duration_seconds_expr("h.execution_time_us")} AS execution_s,
  {_duration_seconds_expr("h.queue_time_us")} AS queue_s,
  {_duration_seconds_expr("h.planning_time_us")} AS planning_s,
  {_duration_seconds_expr("h.compile_time_us")} AS compile_s,
  d.total_spill,
  d.input_bytes,
  d.output_bytes,
  d.blocks_read,
  d.blocks_write,
  d.local_read_io,
  d.remote_read_io,
  d.input_rows,
  d.output_rows,
  d.selectivity_ratio,
  {_duration_seconds_expr("d.total_step_duration_us")} AS total_step_duration_s,
  {_duration_seconds_expr("d.max_step_duration_us")} AS max_step_duration_s,
  {_duration_seconds_expr("d.avg_step_duration_us")} AS avg_step_duration_s,
  d.total_steps,
  d.max_data_skewness,
  d.max_time_skewness,
  d.segments_used,
  d.streams_used,
  d.scan_steps,
  d.join_steps,
  d.sort_steps,
  d.agg_steps,
  d.tables_touched,
  d.alert_count,
  d.external_steps,
  d.s3_steps,
  d.external_input_bytes,
  d.external_input_rows,
  {_duration_seconds_expr("d.external_duration_us")} AS external_duration_s,
  d.external_duration_pct,
  d.remote_io_ratio,
  d.external_selectivity,
  d.external_spill_blocks,
  d.external_tables_touched,
  d.external_data_skew,
  qh.seq_scan_cnt,
  qh.s3_scan_cnt,
  qh.partition_loop_cnt,
  qh.dist_both_cnt,
  qh.bcast_cnt,
  qh.dist_total_cnt,
  qh.has_nested_loop,
  qh.hash_join_cnt,
  qh.subquery_cnt,
  qh.network_cnt,
  qh.missing_stats_flag,
  qh.max_est_rows,
  qh.max_cost,
  qh.cost_score,
  qh.dominant_issue,
  qh.cost_tier,
  qh.plan_node_count,
  qh.sort_node_cnt,
  qh.agg_node_cnt,
  qh.filter_node_cnt,
  qh.full_explain_available,
  qh.full_explain_cost_score,
  qh.full_explain_dominant_issue,
  qh.full_explain_cost_tier,
  qt.sql_text,
  h.user_query_hash,
  h.generic_query_hash,
  COALESCE(qh.cost_score, 0)
    + COALESCE(d.total_spill, 0) / 10000.0
    + COALESCE(d.remote_io_ratio, 0) * 20.0
    + COALESCE(d.max_data_skewness, 0) * 2.0
    + COALESCE(d.max_time_skewness, 0) * 2.0
    + COALESCE(d.external_duration_pct, 0) * 10.0
    + CASE WHEN COALESCE(qh.has_nested_loop, 0) > 0 THEN 15 ELSE 0 END
    + CASE WHEN COALESCE(qh.dist_both_cnt, 0) > 0 THEN 12 ELSE 0 END
    AS risk_score
FROM ids
LEFT JOIN v_history_union h
  ON h.snapshot_id IS NOT DISTINCT FROM ids.snapshot_id AND h.query_id = ids.query_id
 AND h.namespace_id IS NOT DISTINCT FROM ids.namespace_id
LEFT JOIN v_query_details d
  ON d.snapshot_id IS NOT DISTINCT FROM ids.snapshot_id AND d.query_id = ids.query_id
 AND d.namespace_id IS NOT DISTINCT FROM ids.namespace_id
LEFT JOIN v_query_health qh
  ON qh.snapshot_id IS NOT DISTINCT FROM ids.snapshot_id AND qh.query_id = ids.query_id
 AND qh.namespace_id IS NOT DISTINCT FROM ids.namespace_id
LEFT JOIN v_query_text qt
  ON qt.snapshot_id IS NOT DISTINCT FROM ids.snapshot_id AND qt.query_id = ids.query_id
 AND qt.namespace_id IS NOT DISTINCT FROM ids.namespace_id;

CREATE OR REPLACE VIEW v_insights AS
WITH q AS (
  SELECT * FROM v_slow_queries
),
t AS (
  SELECT * FROM v_table_info
)
SELECT snapshot_id, 'Q01_REMOTE_SPILL' AS insight_id, 'query' AS scope, 'Memory' AS family,
  CASE WHEN COALESCE(total_spill, 0) >= 100000 THEN 'crit' ELSE 'warn' END AS severity,
  'Disk spill in slow query' AS title,
  CAST(query_id AS VARCHAR) AS subject, query_id, CAST(NULL AS VARCHAR) AS table_key,
  COALESCE(total_spill, 0) AS metric_value, 95 + LEAST(COALESCE(total_spill, 0) / 100000.0, 25) AS impact_score,
  'Hash or sort work exceeded memory and spilled to disk.' AS evidence,
  'Review WLM memory, join fan-out, sort width, and earlier filtering.' AS recommendation
FROM q WHERE COALESCE(total_spill, 0) > 0
UNION ALL
SELECT snapshot_id, 'Q02_EXTERNAL_SPILL', 'query', 'External Data',
  'crit', 'External step spilled remotely', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(external_spill_blocks, 0), 94 + LEAST(COALESCE(external_spill_blocks, 0) / 10000.0, 20),
  'External-table work spilled remote blocks.', 'Reduce external scan width or stage data into optimized Redshift tables.'
FROM q WHERE COALESCE(external_spill_blocks, 0) > 0
UNION ALL
SELECT snapshot_id, 'Q03_REMOTE_IO_RATIO', 'query', 'I/O',
  CASE WHEN COALESCE(remote_io_ratio, 0) >= 0.5 THEN 'crit' ELSE 'warn' END,
  'High remote I/O ratio', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(remote_io_ratio, 0), 88 + COALESCE(remote_io_ratio, 0) * 20,
  'A large share of reads came from remote storage rather than local cache.', 'Check hot data locality, concurrency pressure, and scan volume.'
FROM q WHERE COALESCE(remote_io_ratio, 0) >= 0.25
UNION ALL
SELECT snapshot_id, 'Q04_DATA_SKEW_STEP', 'query', 'Skew',
  CASE WHEN COALESCE(max_data_skewness, 0) >= 8 THEN 'crit' ELSE 'warn' END,
  'Severe data skew during execution', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(max_data_skewness, 0), 86 + LEAST(COALESCE(max_data_skewness, 0), 20),
  'One or more steps processed materially more rows on some slices.', 'Inspect distkeys for the joined tables and avoid low-cardinality distribution columns.'
FROM q WHERE COALESCE(max_data_skewness, 0) >= 4
UNION ALL
SELECT snapshot_id, 'Q05_TIME_SKEW_STEP', 'query', 'Skew',
  CASE WHEN COALESCE(max_time_skewness, 0) >= 8 THEN 'crit' ELSE 'warn' END,
  'Severe time skew during execution', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(max_time_skewness, 0), 84 + LEAST(COALESCE(max_time_skewness, 0), 20),
  'Some slices ran much longer than peers.', 'Look for skewed joins, uneven distribution, and serial bottleneck steps.'
FROM q WHERE COALESCE(max_time_skewness, 0) >= 4
UNION ALL
SELECT snapshot_id, 'Q06_BROADCAST_JOIN', 'query', 'Distribution',
  CASE WHEN COALESCE(bcast_cnt, 0) >= 3 THEN 'crit' ELSE 'warn' END,
  'Broadcast join activity', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(bcast_cnt, 0), 78 + COALESCE(bcast_cnt, 0) * 3,
  'The plan copied inner relations across slices.', 'Confirm broadcasted tables are small; otherwise align DISTKEYs or use ALL only for true dimensions.'
FROM q WHERE COALESCE(bcast_cnt, 0) > 0
UNION ALL
SELECT snapshot_id, 'Q07_DS_DIST_BOTH', 'query', 'Distribution',
  'crit', 'Both sides redistributed', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(dist_both_cnt, 0), 92 + COALESCE(dist_both_cnt, 0) * 5,
  'DS_DIST_BOTH means neither side was co-located for the join.', 'Choose a shared join-key DISTKEY for the dominant fact path.'
FROM q WHERE COALESCE(dist_both_cnt, 0) > 0
UNION ALL
SELECT snapshot_id, 'Q08_DIST_HEAVY', 'query', 'Distribution',
  CASE WHEN COALESCE(dist_total_cnt, 0) >= 6 THEN 'crit' ELSE 'warn' END,
  'Redistribution-heavy plan', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(dist_total_cnt, 0), 75 + COALESCE(dist_total_cnt, 0) * 2,
  'Multiple redistribution operators appeared in the plan.', 'Map join columns to table diststyles and remove avoidable inter-slice movement.'
FROM q WHERE COALESCE(dist_total_cnt, 0) >= 3
UNION ALL
SELECT snapshot_id, 'Q09_NESTED_LOOP', 'query', 'Planner',
  'crit', 'Nested loop risk', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(has_nested_loop, 0), 96,
  'The optimizer selected or exposed nested loop behavior.', 'Check join predicates, missing equality joins, and stale statistics.'
FROM q WHERE COALESCE(has_nested_loop, 0) > 0
UNION ALL
SELECT snapshot_id, 'Q10_MISSING_STATS_PLAN', 'query', 'Planner',
  'crit', 'Missing statistics in plan', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(missing_stats_flag, 0), 89,
  'The explain output reports missing statistics.', 'Run ANALYZE on referenced tables and re-check row estimates.'
FROM q WHERE COALESCE(missing_stats_flag, 0) > 0
UNION ALL
SELECT snapshot_id, 'Q11_S3_SCAN', 'query', 'External Data',
  CASE WHEN COALESCE(s3_scan_cnt, 0) + COALESCE(s3_steps, 0) >= 3 THEN 'crit' ELSE 'warn' END,
  'S3 scan in slow query', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(s3_scan_cnt, 0) + COALESCE(s3_steps, 0), 72 + (COALESCE(s3_scan_cnt, 0) + COALESCE(s3_steps, 0)) * 3,
  'The query reads external S3 data.', 'Partition external data tightly or materialize recurring slow paths into Redshift.'
FROM q WHERE COALESCE(s3_scan_cnt, 0) + COALESCE(s3_steps, 0) > 0
UNION ALL
SELECT snapshot_id, 'Q12_EXTERNAL_SHARE', 'query', 'External Data',
  CASE WHEN COALESCE(external_duration_pct, 0) >= 0.5 THEN 'crit' ELSE 'warn' END,
  'External work dominates runtime', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(external_duration_pct, 0), 74 + COALESCE(external_duration_pct, 0) * 30,
  'A large runtime share was spent outside internal Redshift tables.', 'Check external table partitioning, file sizing, and recurring materialization candidates.'
FROM q WHERE COALESCE(external_duration_pct, 0) >= 0.25
UNION ALL
SELECT snapshot_id, 'Q13_EXTERNAL_LOW_SELECTIVITY', 'query', 'External Data',
  'warn', 'External scan has poor selectivity', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(external_selectivity, 0), 70 + LEAST(COALESCE(external_input_rows, 0) / 10000000.0, 20),
  'External input rows were high but few rows survived.', 'Push filters to partitions/files or stage pruned data locally.'
FROM q WHERE COALESCE(external_input_rows, 0) >= 1000000 AND COALESCE(external_selectivity, 1) <= 0.1
UNION ALL
SELECT snapshot_id, 'Q14_SEQ_SCAN_HEAVY', 'query', 'Scan',
  CASE WHEN COALESCE(seq_scan_cnt, 0) >= 10 THEN 'crit' ELSE 'warn' END,
  'Sequential-scan-heavy plan', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(seq_scan_cnt, 0), 64 + COALESCE(seq_scan_cnt, 0) * 2,
  'The plan contains many sequential scans.', 'Confirm predicates match leading sort keys and tables have current stats.'
FROM q WHERE COALESCE(seq_scan_cnt, 0) >= 5
UNION ALL
SELECT snapshot_id, 'Q15_PARTITION_LOOP', 'query', 'Planner',
  'warn', 'Partition loop present', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(partition_loop_cnt, 0), 65 + COALESCE(partition_loop_cnt, 0) * 2,
  'The plan includes partition-loop behavior.', 'Check external partition pruning and join predicates.'
FROM q WHERE COALESCE(partition_loop_cnt, 0) > 0
UNION ALL
SELECT snapshot_id, 'Q16_NETWORK_NODE', 'query', 'Network',
  'warn', 'Network operator present', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(network_cnt, 0), 62 + COALESCE(network_cnt, 0) * 2,
  'The plan includes explicit network movement.', 'Reduce intermediate width before network exchange and check join distribution.'
FROM q WHERE COALESCE(network_cnt, 0) > 0
UNION ALL
SELECT snapshot_id, 'Q17_HIGH_COST_SCORE', 'query', 'Planner',
  CASE WHEN COALESCE(cost_score, 0) >= 75 THEN 'crit' ELSE 'warn' END,
  'High plan risk score', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(cost_score, 0), 60 + LEAST(COALESCE(cost_score, 0), 60),
  'The plan combines several expensive operators.', 'Open the query detail and prioritize the highest-impact issue family.'
FROM q WHERE COALESCE(cost_score, 0) >= 35
UNION ALL
SELECT snapshot_id, 'Q18_ALERT_COUNT', 'query', 'Planner',
  CASE WHEN COALESCE(alert_count, 0) >= 5 THEN 'crit' ELSE 'warn' END,
  'Planner alerts emitted', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(alert_count, 0), 58 + COALESCE(alert_count, 0) * 3,
  'Redshift emitted one or more query-detail alerts.', 'Inspect alert text in source SYS tables and correlate to the slowest operators.'
FROM q WHERE COALESCE(alert_count, 0) > 0
UNION ALL
SELECT snapshot_id, 'Q19_ROW_EXPANSION', 'query', 'Rows',
  CASE WHEN COALESCE(selectivity_ratio, 0) >= 20 THEN 'crit' ELSE 'warn' END,
  'Large row expansion', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(selectivity_ratio, 0), 68 + LEAST(COALESCE(selectivity_ratio, 0), 30),
  'Output rows greatly exceed input rows, usually from join fan-out.', 'Check join cardinality, duplicate keys, and missing predicates.'
FROM q WHERE COALESCE(selectivity_ratio, 0) >= 5
UNION ALL
SELECT snapshot_id, 'Q20_INPUT_WASTE', 'query', 'Scan',
  'warn', 'Large input discarded late', CAST(query_id AS VARCHAR), query_id, NULL,
  COALESCE(input_rows, 0), 55 + LEAST(COALESCE(input_rows, 0) / 100000000.0, 25),
  'The query read many rows to produce a small output.', 'Move filters earlier and align sort keys to common predicates.'
FROM q WHERE COALESCE(input_rows, 0) >= 10000000 AND COALESCE(selectivity_ratio, 1) <= 0.05
UNION ALL
SELECT snapshot_id, 'Q21_SELECT_STAR', 'query', 'SQL Shape',
  'info', 'SELECT * in slow SQL', CAST(query_id AS VARCHAR), query_id, NULL,
  1, 35,
  'The SQL text appears to project all columns.', 'Select only required columns to reduce scan, network, and spill width.'
FROM q WHERE REGEXP_MATCHES(LOWER(COALESCE(sql_text, '')), 'select[[:space:]]+\\*')
UNION ALL
SELECT snapshot_id, 'Q22_LEADING_WILDCARD', 'query', 'SQL Shape',
  'warn', 'Leading-wildcard LIKE', CAST(query_id AS VARCHAR), query_id, NULL,
  1, 42,
  'A LIKE predicate begins with a wildcard.', 'Avoid leading wildcards on large scans; precompute searchable terms when needed.'
FROM q WHERE REGEXP_MATCHES(LOWER(COALESCE(sql_text, '')), 'like[[:space:]]+''%')
UNION ALL
SELECT snapshot_id, 'Q23_UNION_DISTINCT', 'query', 'SQL Shape',
  'info', 'UNION forces distinct work', CAST(query_id AS VARCHAR), query_id, NULL,
  1, 32,
  'The SQL uses UNION without ALL, which can add sort/dedup work.', 'Use UNION ALL when duplicate elimination is not required.'
FROM q WHERE POSITION('union ' IN LOWER(COALESCE(sql_text, ''))) > 0 AND POSITION('union all' IN LOWER(COALESCE(sql_text, ''))) = 0
UNION ALL
SELECT snapshot_id, 'Q24_CROSS_JOIN', 'query', 'SQL Shape',
  'crit', 'Cross join in slow SQL', CAST(query_id AS VARCHAR), query_id, NULL,
  1, 90,
  'The SQL text contains a CROSS JOIN.', 'Confirm this is intentional; otherwise add the missing join predicate.'
FROM q WHERE POSITION('cross join' IN LOWER(COALESCE(sql_text, ''))) > 0
UNION ALL
SELECT snapshot_id, 'Q25_ORDER_BY_NO_LIMIT', 'query', 'SQL Shape',
  'warn', 'ORDER BY without LIMIT', CAST(query_id AS VARCHAR), query_id, NULL,
  1, 40,
  'The SQL orders a result set without an obvious LIMIT.', 'Avoid cluster-wide final sorts unless the full ordered result is required.'
FROM q WHERE POSITION('order by' IN LOWER(COALESCE(sql_text, ''))) > 0 AND POSITION('limit ' IN LOWER(COALESCE(sql_text, ''))) = 0
UNION ALL
SELECT snapshot_id, 'T26_TABLE_SKEW', 'table', 'Physical Design',
  CASE WHEN COALESCE(skew_rows, 0) >= 5 THEN 'crit' ELSE 'warn' END,
  'Table has severe row skew', source_db || '.' || schema_name || '.' || table_name, NULL, table_key,
  COALESCE(skew_rows, 0), 80 + LEAST(COALESCE(skew_rows, 0) * 3, 25),
  'Rows are unevenly distributed across slices.', 'Choose a higher-cardinality DISTKEY or switch to EVEN for this table.'
FROM t WHERE COALESCE(skew_rows, 0) >= 3
UNION ALL
SELECT snapshot_id, 'T27_UNSORTED', 'table', 'Physical Design',
  CASE WHEN COALESCE(unsorted_pct, 0) >= 50 THEN 'crit' ELSE 'warn' END,
  'Table is heavily unsorted', source_db || '.' || schema_name || '.' || table_name, NULL, table_key,
  COALESCE(unsorted_pct, 0), 76 + LEAST(COALESCE(unsorted_pct, 0) / 2, 25),
  'Unsorted rows weaken zone-map pruning.', 'Run VACUUM SORT or review load pattern and sort key choice.'
FROM t WHERE COALESCE(unsorted_pct, 0) >= 20
UNION ALL
SELECT snapshot_id, 'T28_STATS_OFF', 'table', 'Physical Design',
  CASE WHEN COALESCE(stats_off, 0) >= 25 THEN 'crit' ELSE 'warn' END,
  'Table statistics are stale', source_db || '.' || schema_name || '.' || table_name, NULL, table_key,
  COALESCE(stats_off, 0), 78 + LEAST(COALESCE(stats_off, 0) / 2, 20),
  'Optimizer statistics are out of date.', 'Run ANALYZE and verify auto-analyze coverage for this table.'
FROM t WHERE COALESCE(stats_off, 0) >= 10
UNION ALL
SELECT snapshot_id, 'T29_VACUUM_BENEFIT', 'table', 'Physical Design',
  'warn', 'Vacuum sort benefit available', source_db || '.' || schema_name || '.' || table_name, NULL, table_key,
  COALESCE(vacuum_sort_benefit, 0), 64 + LEAST(COALESCE(vacuum_sort_benefit, 0), 20),
  'SVV_TABLE_INFO reports material vacuum sort benefit.', 'Schedule vacuum work for high-benefit tables used by slow queries.'
FROM t WHERE COALESCE(vacuum_sort_benefit, 0) >= 10
UNION ALL
SELECT snapshot_id, 'T30_LARGE_ALL_TABLE', 'table', 'Physical Design',
  'crit', 'Large table uses DISTSTYLE ALL', source_db || '.' || schema_name || '.' || table_name, NULL, table_key,
  COALESCE(size_mb, 0), 82 + LEAST(COALESCE(size_mb, 0) / 1000.0, 20),
  'A large ALL table is replicated to every node.', 'Use ALL only for small stable dimensions; choose KEY or EVEN for large relations.'
FROM t WHERE UPPER(COALESCE(diststyle, '')) LIKE 'ALL%' AND COALESCE(size_mb, 0) >= 1024
UNION ALL
SELECT snapshot_id, 'T31_LARGE_EVEN_TABLE', 'table', 'Physical Design',
  'info', 'Large table uses EVEN distribution', source_db || '.' || schema_name || '.' || table_name, NULL, table_key,
  COALESCE(tbl_rows, 0), 45 + LEAST(COALESCE(tbl_rows, 0) / 1000000000.0, 15),
  'A very large EVEN table may require redistribution for frequent joins.', 'If this table dominates joins, evaluate a KEY diststyle on the main join column.'
FROM t WHERE UPPER(COALESCE(diststyle, '')) LIKE 'EVEN%' AND COALESCE(tbl_rows, 0) >= 100000000
UNION ALL
SELECT snapshot_id, 'T32_NO_SORTKEY_LARGE', 'table', 'Physical Design',
  'warn', 'Large table has no sort key', source_db || '.' || schema_name || '.' || table_name, NULL, table_key,
  COALESCE(size_mb, 0), 66 + LEAST(COALESCE(size_mb, 0) / 1000.0, 20),
  'A large table has no leading sort key.', 'Add a sort key that matches the most common date/range filters.'
FROM t WHERE {_missing_sortkey_sql("sortkey1")} AND (COALESCE(size_mb, 0) >= 1024 OR COALESCE(tbl_rows, 0) >= 100000000);

CREATE OR REPLACE VIEW v_insight_family_summary AS
SELECT
  snapshot_id,
  namespace_id,
  family,
  COUNT(*) AS issue_count,
  SUM(CASE severity WHEN 'crit' THEN 1 ELSE 0 END) AS critical_count,
  SUM(CASE severity WHEN 'warn' THEN 1 ELSE 0 END) AS warning_count,
  SUM(CASE severity WHEN 'info' THEN 1 ELSE 0 END) AS info_count,
  AVG(impact_score) AS avg_impact,
  MAX(impact_score) AS max_impact
FROM v_insights
GROUP BY snapshot_id, namespace_id, family;

CREATE OR REPLACE VIEW v_table_risk AS
SELECT
  snapshot_id,
  namespace_id,
  source_db,
  schema_name,
  table_name,
  table_key,
  diststyle,
  sortkey1,
  size_mb,
  tbl_rows,
  unsorted_pct,
  stats_off,
  skew_rows,
  vacuum_sort_benefit,
  risk_event,
  (
    CASE WHEN COALESCE(skew_rows, 0) >= 3 THEN LEAST(COALESCE(skew_rows, 0) * 8, 40) ELSE 0 END
    + CASE WHEN COALESCE(unsorted_pct, 0) >= 20 THEN LEAST(COALESCE(unsorted_pct, 0) / 2, 30) ELSE 0 END
    + CASE WHEN COALESCE(stats_off, 0) >= 10 THEN LEAST(COALESCE(stats_off, 0) / 2, 30) ELSE 0 END
    + CASE WHEN COALESCE(vacuum_sort_benefit, 0) >= 10 THEN LEAST(COALESCE(vacuum_sort_benefit, 0), 20) ELSE 0 END
    + CASE WHEN UPPER(COALESCE(diststyle, '')) LIKE 'ALL%' AND COALESCE(size_mb, 0) >= 1024 THEN 25 ELSE 0 END
    + CASE WHEN {_missing_sortkey_sql("sortkey1")} AND COALESCE(size_mb, 0) >= 1024 THEN 20 ELSE 0 END
  ) AS table_risk_score
FROM v_table_info;

CREATE OR REPLACE VIEW v_rewrite_opportunities AS
WITH q AS (
  SELECT * FROM v_slow_queries
),
t AS (
  SELECT * FROM v_table_risk
)
SELECT
  snapshot_id,
  'R01_TIME_SERIES_UNION_ALL_PARTS' AS rewrite_id,
  1 AS opportunity_no,
  'crit' AS severity,
  'Split broad time-series scans into UNION ALL parts' AS title,
  CAST(query_id AS VARCHAR) AS subject,
  query_id,
  CAST(NULL AS VARCHAR) AS table_key,
  96 + LEAST(COALESCE(input_rows, 0) / 100000000.0, 20) AS impact_score,
  'Large time-range query appears to read many rows before filtering.' AS trigger,
  'Break the date range into explicit UNION ALL branches by month, quarter, or hot/cold period so Redshift can prune each branch and parallelize smaller scans.' AS rewrite_shape,
  'Use when a massive time-series fact table is scanned for a wide range and each period can be filtered independently.' AS why_it_matters,
  'SELECT ... FROM fact WHERE event_dt >= ? AND event_dt < ? UNION ALL SELECT ... FROM fact_archive WHERE event_dt >= ? AND event_dt < ?' AS candidate_sql
FROM q
WHERE COALESCE(input_rows, 0) >= 10000000
  AND COALESCE(selectivity_ratio, 1) <= 0.25
  AND REGEXP_MATCHES(LOWER(COALESCE(sql_text, '')), '(date|time|timestamp|between|_dt|_date)')
UNION ALL
SELECT snapshot_id, 'R02_PUSH_FILTERS_BEFORE_JOINS', 2,
  CASE WHEN COALESCE(input_rows, 0) >= 100000000 THEN 'crit' ELSE 'warn' END,
  'Push filters before high-cardinality joins', CAST(query_id AS VARCHAR), query_id, NULL,
  90 + LEAST(COALESCE(input_rows, 0) / 100000000.0, 20),
  'Many input rows are read but few rows survive.',
  'Materialize or CTE-filter the largest table first, then join the reduced row set.',
  'Pre-filtering reduces hash table size, redistribution volume, and spill risk.',
  'WITH filtered_fact AS (SELECT needed_cols FROM fact WHERE selective_predicate) SELECT ... FROM filtered_fact JOIN dim ...'
FROM q
WHERE COALESCE(input_rows, 0) >= 10000000 AND COALESCE(selectivity_ratio, 1) <= 0.05
UNION ALL
SELECT snapshot_id, 'R03_PRE_AGGREGATE_FACT', 3,
  CASE WHEN COALESCE(agg_steps, 0) > 0 AND COALESCE(join_steps, 0) > 0 THEN 'warn' ELSE 'info' END,
  'Pre-aggregate fact rows before dimension joins', CAST(query_id AS VARCHAR), query_id, NULL,
  74 + LEAST(COALESCE(input_rows, 0) / 100000000.0, 18),
  'Query joins and aggregates large row sets.',
  'Aggregate the fact table at the join/reporting grain before joining low-cardinality dimensions.',
  'This shrinks join inputs and can remove spill caused by wide post-join grouping.',
  'WITH fact_rollup AS (SELECT key, date_key, SUM(metric) metric FROM fact WHERE ... GROUP BY 1,2) SELECT ... FROM fact_rollup JOIN dim ...'
FROM q
WHERE COALESCE(input_rows, 0) >= 10000000 AND COALESCE(agg_steps, 0) > 0 AND COALESCE(join_steps, 0) > 0
UNION ALL
SELECT snapshot_id, 'R04_STAGE_DISTRIBUTED_TEMP_TABLE', 4,
  'crit',
  'Stage large join inputs with explicit DISTKEY/SORTKEY', CAST(query_id AS VARCHAR), query_id, NULL,
  88 + COALESCE(dist_total_cnt, 0) * 4,
  'Plan contains redistribution or broadcast around large joins.',
  'Create a temp table for the heavy input using the join key as DISTKEY and the range predicate as SORTKEY, then ANALYZE before the final join.',
  'Staging lets DBAs force co-location for a problematic query without redesigning every source table immediately.',
  'CREATE TEMP TABLE stage_fact DISTKEY(join_key) SORTKEY(event_dt) AS SELECT ...; ANALYZE stage_fact; SELECT ... FROM stage_fact JOIN ...'
FROM q
WHERE COALESCE(dist_total_cnt, 0) >= 2 OR COALESCE(dist_both_cnt, 0) > 0
UNION ALL
SELECT snapshot_id, 'R05_MATERIALIZE_EXTERNAL_PATH', 5,
  CASE WHEN COALESCE(external_duration_pct, 0) >= 0.5 THEN 'crit' ELSE 'warn' END,
  'Materialize recurring external/S3 scans', CAST(query_id AS VARCHAR), query_id, NULL,
  82 + COALESCE(external_duration_pct, 0) * 25,
  'External scan work is a large share of runtime.',
  'Persist the filtered external data into an optimized local Redshift table for repeated workloads.',
  'Local encoded/sorted/distributed storage usually beats repeated external file scans for hot paths.',
  'CREATE TABLE mart.optimized DISTKEY(join_key) SORTKEY(event_dt) AS SELECT needed_cols FROM spectrum_table WHERE ...; ANALYZE mart.optimized;'
FROM q
WHERE COALESCE(external_duration_pct, 0) >= 0.25 OR COALESCE(s3_scan_cnt, 0) > 0
UNION ALL
SELECT snapshot_id, 'R06_COLUMN_PRUNE_SELECT_STAR', 6,
  'warn',
  'Replace SELECT * with column projection', CAST(query_id AS VARCHAR), query_id, NULL,
  60 + LEAST(COALESCE(input_bytes, 0) / 1000000000.0, 15),
  'SQL projects all columns in a slow query.',
  'List only required columns, especially before joins, sorts, broadcasts, and temp staging.',
  'Column pruning lowers scan bytes, network width, hash memory, and spill probability.',
  'SELECT col_a, col_b, metric FROM ...'
FROM q
WHERE REGEXP_MATCHES(LOWER(COALESCE(sql_text, '')), 'select[[:space:]]+\\*')
UNION ALL
SELECT snapshot_id, 'R07_UNION_ALL_INSTEAD_OF_UNION', 7,
  'info',
  'Use UNION ALL when deduplication is not required', CAST(query_id AS VARCHAR), query_id, NULL,
  50 + LEAST(COALESCE(input_rows, 0) / 100000000.0, 12),
  'SQL uses UNION without ALL.',
  'Change UNION to UNION ALL when the business logic does not require distinct rows.',
  'UNION can force global sort/hash deduplication on large intermediate data.',
  'SELECT ... FROM a UNION ALL SELECT ... FROM b'
FROM q
WHERE POSITION('union ' IN LOWER(COALESCE(sql_text, ''))) > 0
  AND POSITION('union all' IN LOWER(COALESCE(sql_text, ''))) = 0
UNION ALL
SELECT snapshot_id, 'R08_AVOID_LEADING_WILDCARD', 8,
  'warn',
  'Replace leading-wildcard LIKE on large scans', CAST(query_id AS VARCHAR), query_id, NULL,
  58 + LEAST(COALESCE(input_rows, 0) / 100000000.0, 12),
  'A LIKE predicate begins with a wildcard.',
  'Use pre-tokenized search columns, normalized lookup tables, or anchored predicates instead of LIKE ''%value'' on a large table.',
  'Leading wildcards prevent pruning and force broad scans.',
  'WHERE normalized_term = ? OR anchored_column LIKE ''value%'''
FROM q
WHERE REGEXP_MATCHES(LOWER(COALESCE(sql_text, '')), 'like[[:space:]]+''%')
UNION ALL
SELECT snapshot_id, 'R09_ORDER_BY_LIMIT_TOP_N', 9,
  'warn',
  'Convert full final sort to Top-N pattern', CAST(query_id AS VARCHAR), query_id, NULL,
  56 + COALESCE(sort_steps, 0) * 4,
  'SQL orders the output without an obvious LIMIT.',
  'Add LIMIT when only top results are needed, or rank within a reduced candidate set before final ordering.',
  'Full cluster-wide sorts are expensive late-stage operators.',
  'SELECT ... FROM (...) q ORDER BY metric DESC LIMIT 1000'
FROM q
WHERE POSITION('order by' IN LOWER(COALESCE(sql_text, ''))) > 0
  AND POSITION('limit ' IN LOWER(COALESCE(sql_text, ''))) = 0
UNION ALL
SELECT snapshot_id, 'R10_REMOVE_CROSS_OR_FANOUT_JOIN', 10,
  'crit',
  'Eliminate accidental fan-out joins', CAST(query_id AS VARCHAR), query_id, NULL,
  94 + LEAST(COALESCE(selectivity_ratio, 0), 20),
  'The query shows cross join text or major row expansion.',
  'Validate join keys and move one-to-many expansions after aggregation when possible.',
  'Fan-out joins multiply rows, memory, network movement, and final aggregation cost.',
  'JOIN dim ON fact.dim_key = dim.dim_key -- verify uniqueness or pre-deduplicate dim'
FROM q
WHERE POSITION('cross join' IN LOWER(COALESCE(sql_text, ''))) > 0 OR COALESCE(selectivity_ratio, 0) >= 5
UNION ALL
SELECT snapshot_id, 'R11_BREAK_MONSTER_QUERY_INTO_ANALYZED_STAGES', 11,
  CASE WHEN COALESCE(total_spill, 0) > 0 OR COALESCE(has_nested_loop, 0) > 0 THEN 'crit' ELSE 'warn' END,
  'Break monster query into analyzed stages', CAST(query_id AS VARCHAR), query_id, NULL,
  84 + LEAST(COALESCE(total_spill, 0) / 10000.0, 20),
  'The query combines multiple expensive symptoms such as spill, nested loop risk, skew, or high plan score.',
  'Split the query into temp tables at natural reduction points and ANALYZE each stage.',
  'Stage boundaries give the optimizer real row counts and isolate which phase consumes memory or network.',
  'CREATE TEMP TABLE stage_1 AS SELECT ...; ANALYZE stage_1; CREATE TEMP TABLE stage_2 AS SELECT ... FROM stage_1 JOIN ...; ANALYZE stage_2;'
FROM q
WHERE COALESCE(total_spill, 0) > 0
   OR COALESCE(has_nested_loop, 0) > 0
   OR COALESCE(cost_score, 0) >= 50
   OR (COALESCE(max_data_skewness, 0) >= 4 AND COALESCE(dist_total_cnt, 0) > 0)
UNION ALL
SELECT
  snapshot_id,
  'R01_TIME_SERIES_UNION_ALL_PARTS' AS rewrite_id,
  1 AS opportunity_no,
  CASE WHEN COALESCE(table_risk_score, 0) >= 80 THEN 'crit' ELSE 'warn' END AS severity,
  'Split large time-series table family into UNION ALL parts' AS title,
  source_db || '.' || schema_name || '.' || table_name AS subject,
  CAST(NULL AS BIGINT) AS query_id,
  table_key,
  70 + LEAST(COALESCE(size_mb, 0) / 1000.0, 20) AS impact_score,
  'Large table has a date/time-looking sort key or name and material physical risk.',
  'Partition old and current ranges into separate optimized tables or views with UNION ALL over explicit range predicates.',
  'This is useful when a single huge table mixes hot recent data, cold history, and different access patterns.',
  'CREATE VIEW fact_all AS SELECT ... FROM fact_current WHERE event_dt >= current_cutoff UNION ALL SELECT ... FROM fact_history WHERE event_dt < current_cutoff;'
FROM t
WHERE (LOWER(COALESCE(sortkey1, '') || ' ' || COALESCE(table_name, '')) ~ '(date|time|dt|day|month|year)')
  AND (COALESCE(size_mb, 0) >= 10240 OR COALESCE(tbl_rows, 0) >= 1000000000 OR COALESCE(table_risk_score, 0) >= 70);

CREATE OR REPLACE VIEW v_query_table_refs AS
WITH candidate_refs AS (
  SELECT
    q.snapshot_id,
    q.namespace_id,
    q.query_id,
    q.elapsed_s,
    q.risk_score,
    q.total_spill,
    q.dist_total_cnt,
    q.dist_both_cnt,
    q.bcast_cnt,
    q.max_data_skewness,
    q.max_time_skewness,
    q.dominant_issue,
    q.sql_text,
    t.table_key,
    t.source_db,
    t.schema_name,
    t.table_name,
    t.table_risk_score,
    t.diststyle,
    t.sortkey1,
    t.size_mb,
    t.tbl_rows,
    t.unsorted_pct,
    t.stats_off,
    t.skew_rows,
    t.vacuum_sort_benefit,
    LOWER(COALESCE(q.sql_text, '')) AS sql_text_lc,
    LOWER(COALESCE(q.database_name, '')) AS query_database_lc,
    ' ' || REGEXP_REPLACE(LOWER(COALESCE(q.sql_text, '')), '[^a-z0-9_]+', ' ', 'g') || ' ' AS sql_tokens,
    LOWER(COALESCE(t.source_db, '') || '.' || COALESCE(t.schema_name, '') || '.' || COALESCE(t.table_name, '')) AS db_schema_table_lc,
    LOWER(COALESCE(t.schema_name, '') || '.' || COALESCE(t.table_name, '')) AS schema_table_lc,
    TRIM(REGEXP_REPLACE(LOWER(COALESCE(t.table_name, '')), '[^a-z0-9_]+', ' ', 'g')) AS table_tokens,
    LENGTH(TRIM(REGEXP_REPLACE(LOWER(COALESCE(t.table_name, '')), '[^a-z0-9_]+', ' ', 'g'))) AS table_token_len
  FROM v_slow_queries q
  JOIN v_table_risk t
    ON q.snapshot_id IS NOT DISTINCT FROM t.snapshot_id
   AND q.namespace_id IS NOT DISTINCT FROM t.namespace_id
   AND q.sql_text IS NOT NULL
   -- Prefer same database when the query carries database_name; still allow
   -- unmatched catalog rows when database_name is blank.
   AND (
     NULLIF(TRIM(COALESCE(q.database_name, '')), '') IS NULL
     OR NULLIF(TRIM(COALESCE(t.source_db, '')), '') IS NULL
     OR LOWER(TRIM(q.database_name)) = LOWER(TRIM(t.source_db))
   )
)
SELECT
  snapshot_id,
  namespace_id,
  query_id,
  elapsed_s,
  risk_score,
  total_spill,
  dist_total_cnt,
  dist_both_cnt,
  bcast_cnt,
  max_data_skewness,
  max_time_skewness,
  dominant_issue,
  sql_text,
  table_key,
  source_db,
  schema_name,
  table_name,
  table_risk_score,
  diststyle,
  sortkey1,
  size_mb,
  tbl_rows,
  unsorted_pct,
  stats_off,
  skew_rows,
  vacuum_sort_benefit
FROM candidate_refs
WHERE
  -- Strongest: fully-qualified db.schema.table appears in SQL
  (
    LENGTH(db_schema_table_lc) > 2
    AND POSITION(db_schema_table_lc IN sql_text_lc) > 0
  )
  -- Strong: schema.table token appears
  OR (
    LENGTH(schema_table_lc) > 2
    AND POSITION(schema_table_lc IN sql_text_lc) > 0
  )
  -- Weak bare table name: only when name is long enough to avoid stopwords
  -- (e.g. "user", "date", "order") and appears as a whole token.
  OR (
    table_token_len >= 4
    AND POSITION(' ' || table_tokens || ' ' IN sql_tokens) > 0
  )
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_id, namespace_id, query_id, table_key
  ORDER BY
    CASE
      WHEN LENGTH(db_schema_table_lc) > 2 AND POSITION(db_schema_table_lc IN sql_text_lc) > 0 THEN 0
      WHEN LENGTH(schema_table_lc) > 2 AND POSITION(schema_table_lc IN sql_text_lc) > 0 THEN 1
      ELSE 2
    END,
    table_risk_score DESC
) = 1;

CREATE OR REPLACE VIEW v_table_query_impact AS
SELECT
  r.snapshot_id,
  r.namespace_id,
  r.table_key,
  r.source_db,
  r.schema_name,
  r.table_name,
  ANY_VALUE(r.diststyle) AS diststyle,
  ANY_VALUE(r.sortkey1) AS sortkey1,
  MAX(r.size_mb) AS size_mb,
  MAX(r.tbl_rows) AS tbl_rows,
  MAX(r.unsorted_pct) AS unsorted_pct,
  MAX(r.stats_off) AS stats_off,
  MAX(r.skew_rows) AS skew_rows,
  MAX(r.vacuum_sort_benefit) AS vacuum_sort_benefit,
  MAX(r.table_risk_score) AS table_risk_score,
  COUNT(DISTINCT r.query_id) AS slow_query_count,
  SUM(COALESCE(r.elapsed_s, 0)) AS total_runtime_s,
  MAX(COALESCE(r.elapsed_s, 0)) AS worst_runtime_s,
  AVG(COALESCE(r.risk_score, 0)) AS avg_query_risk,
  MAX(COALESCE(r.risk_score, 0)) AS max_query_risk,
  SUM(COALESCE(r.total_spill, 0)) AS total_spill_blocks,
  SUM(CASE WHEN COALESCE(r.dist_both_cnt, 0) > 0 OR COALESCE(r.dist_total_cnt, 0) >= 2 THEN 1 ELSE 0 END) AS redistribution_query_count,
  SUM(CASE WHEN COALESCE(r.bcast_cnt, 0) > 0 THEN 1 ELSE 0 END) AS broadcast_query_count,
  SUM(CASE WHEN COALESCE(r.max_data_skewness, 0) >= 4 OR COALESCE(r.max_time_skewness, 0) >= 4 THEN 1 ELSE 0 END) AS skewed_query_count,
  STRING_AGG(CAST(r.query_id AS VARCHAR), ', ' ORDER BY COALESCE(r.elapsed_s, 0) DESC) AS query_ids,
  (
    MAX(r.table_risk_score)
    + LEAST(COUNT(DISTINCT r.query_id) * 2.5, 35)
    + LEAST(SUM(COALESCE(r.elapsed_s, 0)) / 3600.0, 35)
    + LEAST(SUM(COALESCE(r.total_spill, 0)) / 100000.0, 20)
    + LEAST(SUM(CASE WHEN COALESCE(r.dist_both_cnt, 0) > 0 OR COALESCE(r.dist_total_cnt, 0) >= 2 THEN 1 ELSE 0 END) * 3, 25)
  ) AS blast_radius_score
FROM v_query_table_refs r
GROUP BY r.snapshot_id, r.namespace_id, r.table_key, r.source_db, r.schema_name, r.table_name;

CREATE OR REPLACE VIEW v_table_review AS
SELECT
  t.snapshot_id,
  t.namespace_id,
  t.source_db,
  t.schema_name,
  t.table_name,
  t.table_key,
  t.table_id,
  t.diststyle,
  t.sortkey1,
  t.size_mb,
  t.tbl_rows,
  t.unsorted_pct,
  t.stats_off,
  t.skew_rows,
  t.vacuum_sort_benefit,
  t.risk_event,
  COALESCE(i.slow_query_count, 0) AS slow_query_count,
  COALESCE(i.total_runtime_s, 0) AS slow_query_runtime_s,
  COALESCE(i.redistribution_query_count, 0) AS redistribution_query_count,
  COALESCE(i.broadcast_query_count, 0) AS broadcast_query_count,
  COALESCE(i.skewed_query_count, 0) AS skewed_query_count,
  COALESCE(i.blast_radius_score, 0) AS blast_radius_score,
  COALESCE(s.scan_query_count, 0) AS scan_query_count,
  COALESCE(s.scan_duration_s, 0) AS scan_duration_s,
  COALESCE(s.scan_input_rows_m, 0) AS scan_input_rows_m,
  COALESCE(s.scan_output_rows_m, 0) AS scan_output_rows_m,
  COALESCE(s.rrscan_query_count, 0) AS rrscan_query_count,
  COALESCE(s.non_rrscan_query_count, 0) AS non_rrscan_query_count,
  COALESCE(s.rrscan_query_pct, 0) AS rrscan_query_pct,
  COALESCE(s.full_scan_query_pct, 0) AS full_scan_query_pct,
  CASE
    WHEN {_missing_sortkey_sql("t.sortkey1")} THEN 0
    ELSE ROUND(COALESCE(s.rrscan_query_pct, 0) * 100, 1)
  END AS sort_key_usage_score,
  LEAST(
    COALESCE(s.full_scan_query_pct, 0) * 60
      + LEAST(COALESCE(s.non_rrscan_query_count, 0) * 6, 30)
      + LEAST(COALESCE(s.scan_input_rows_m, 0) / 100.0, 30),
    120
  ) AS full_scan_score,
  LEAST(
    COALESCE(i.redistribution_query_count, 0) * 10
      + COALESCE(i.broadcast_query_count, 0) * 8
      + COALESCE(i.skewed_query_count, 0) * 8
      + LEAST(COALESCE(t.skew_rows, 0) * 6, 35),
    120
  ) AS distribution_usage_score,
  LEAST(
    CASE WHEN {_missing_sortkey_sql("t.sortkey1")} AND COALESCE(s.scan_query_count, 0) > 0 THEN 30 ELSE 0 END
      + COALESCE(t.unsorted_pct, 0) * 0.7
      + COALESCE(t.vacuum_sort_benefit, 0) * 0.7
      + (1 - COALESCE(s.rrscan_query_pct, 0)) * 35,
    120
  ) AS sort_attention_score,
  LEAST(
    COALESCE(i.blast_radius_score, 0)
      + LEAST(COALESCE(s.scan_duration_s, 0) / 3600.0, 30)
      + LEAST(COALESCE(s.non_rrscan_query_count, 0) * 4, 30)
      + LEAST(COALESCE(s.scan_input_rows_m, 0) / 120.0, 25),
    180
  ) AS table_attention_score
FROM v_table_info t
LEFT JOIN v_table_query_impact i
  ON i.snapshot_id IS NOT DISTINCT FROM t.snapshot_id
 AND i.table_key = t.table_key
LEFT JOIN (
  SELECT
    snapshot_id,
    table_key,
    SUM(scan_query_count) AS scan_query_count,
    SUM(scan_duration_s) AS scan_duration_s,
    SUM(scan_input_rows_m) AS scan_input_rows_m,
    SUM(scan_output_rows_m) AS scan_output_rows_m,
    SUM(rrscan_query_count) AS rrscan_query_count,
    SUM(non_rrscan_query_count) AS non_rrscan_query_count,
    CASE WHEN SUM(scan_query_count) > 0 THEN SUM(rrscan_query_count) / SUM(scan_query_count) ELSE NULL END AS rrscan_query_pct,
    CASE WHEN SUM(scan_query_count) > 0 THEN SUM(non_rrscan_query_count) / SUM(scan_query_count) ELSE NULL END AS full_scan_query_pct
  FROM v_table_scan_info
  GROUP BY snapshot_id, table_key
) s
  ON s.snapshot_id IS NOT DISTINCT FROM t.snapshot_id
 AND s.table_key = t.table_key;

CREATE OR REPLACE VIEW v_action_queue AS
WITH table_impact AS (
  SELECT * FROM v_table_query_impact
),
rewrite_impact AS (
  SELECT * FROM v_rewrite_opportunities
),
query_impact AS (
  SELECT * FROM v_slow_queries
),
raw_actions AS (
  SELECT
    snapshot_id,
    'A01_TABLE_BLAST_RADIUS' AS action_id,
    'Table Blast Radius' AS action_type,
    CASE WHEN COALESCE(blast_radius_score, 0) >= 110 THEN 'crit' ELSE 'warn' END AS severity,
    source_db || '.' || schema_name || '.' || table_name AS subject,
    CAST(NULL AS BIGINT) AS query_id,
    table_key,
    blast_radius_score AS action_score,
    'Fix the physical design for this table first.' AS what_to_do,
    'It combines table risk with repeated slow-query references.' AS why_now,
    'slow_queries=' || CAST(slow_query_count AS VARCHAR)
      || ', runtime=' || CAST(ROUND(total_runtime_s / 60.0, 1) AS VARCHAR) || ' min'
      || ', table_risk=' || CAST(ROUND(table_risk_score, 1) AS VARCHAR)
      || ', sample_queries=' || LEFT(COALESCE(query_ids, ''), 120) AS evidence,
    'Review DISTSTYLE/SORTKEY/stats/vacuum together; this table has workload blast radius.' AS sql_hint
  FROM table_impact
  WHERE COALESCE(slow_query_count, 0) >= 2 AND COALESCE(blast_radius_score, 0) >= 60
  UNION ALL
  SELECT
    snapshot_id,
    'A02_ANALYZE_HIGH_IMPACT_TABLE',
    'Maintenance',
    CASE WHEN COALESCE(stats_off, 0) >= 35 OR COALESCE(slow_query_count, 0) >= 8 THEN 'crit' ELSE 'warn' END,
    source_db || '.' || schema_name || '.' || table_name,
    NULL,
    table_key,
    72 + LEAST(COALESCE(stats_off, 0), 40) + LEAST(COALESCE(slow_query_count, 0) * 2, 25),
    'Run ANALYZE on this high-impact table.',
    'Stale stats affect optimizer row estimates on slow queries that reference it.',
    'stats_off=' || CAST(ROUND(stats_off, 1) AS VARCHAR)
      || ', slow_queries=' || CAST(slow_query_count AS VARCHAR)
      || ', sample_queries=' || LEFT(COALESCE(query_ids, ''), 120),
    'ANALYZE ' || schema_name || '.' || table_name || ';'
  FROM table_impact
  WHERE COALESCE(stats_off, 0) >= 10 AND COALESCE(slow_query_count, 0) > 0
  UNION ALL
  SELECT
    snapshot_id,
    'A03_VACUUM_SORT_HIGH_IMPACT_TABLE',
    'Maintenance',
    CASE WHEN COALESCE(unsorted_pct, 0) >= 50 OR COALESCE(vacuum_sort_benefit, 0) >= 35 THEN 'crit' ELSE 'warn' END,
    source_db || '.' || schema_name || '.' || table_name,
    NULL,
    table_key,
    70 + LEAST(COALESCE(unsorted_pct, 0) / 1.2, 45) + LEAST(COALESCE(slow_query_count, 0) * 2, 25),
    'Run VACUUM SORT or correct the load/sort pattern.',
    'Unsorted rows weaken zone-map pruning for slow queries referencing this table.',
    'unsorted=' || CAST(ROUND(unsorted_pct, 1) AS VARCHAR) || '%'
      || ', vacuum_benefit=' || CAST(ROUND(vacuum_sort_benefit, 1) AS VARCHAR)
      || ', slow_queries=' || CAST(slow_query_count AS VARCHAR),
    'VACUUM SORT ' || schema_name || '.' || table_name || ';'
  FROM table_impact
  WHERE (COALESCE(unsorted_pct, 0) >= 20 OR COALESCE(vacuum_sort_benefit, 0) >= 10)
    AND COALESCE(slow_query_count, 0) > 0
  UNION ALL
  SELECT
    snapshot_id,
    'A04_REDESIGN_DISTRIBUTION',
    'Physical Design',
    CASE WHEN COALESCE(redistribution_query_count, 0) >= 5 OR COALESCE(skew_rows, 0) >= 5 THEN 'crit' ELSE 'warn' END,
    source_db || '.' || schema_name || '.' || table_name,
    NULL,
    table_key,
    78 + LEAST(COALESCE(redistribution_query_count, 0) * 5, 35) + LEAST(COALESCE(skew_rows, 0) * 4, 25),
    'Evaluate DISTKEY/DISTSTYLE redesign for this table.',
    'Queries referencing this table show redistribution, broadcast, or slice skew.',
    'diststyle=' || COALESCE(diststyle, '')
      || ', redistribution_queries=' || CAST(redistribution_query_count AS VARCHAR)
      || ', skew_rows=' || CAST(ROUND(skew_rows, 2) AS VARCHAR)
      || ', sample_queries=' || LEFT(COALESCE(query_ids, ''), 120),
    'Check dominant join columns; use KEY for co-location, EVEN for balanced standalone scans, ALL only for small dimensions.'
  FROM table_impact
  WHERE COALESCE(redistribution_query_count, 0) > 0 OR COALESCE(skew_rows, 0) >= 3
  UNION ALL
  SELECT
    snapshot_id,
    'A05_REWRITE_TOP_QUERY',
    'Rewrite',
    severity,
    subject,
    query_id,
    table_key,
    impact_score,
    title,
    rewrite_shape,
    trigger,
    candidate_sql
  FROM rewrite_impact
  WHERE query_id IS NOT NULL AND COALESCE(impact_score, 0) >= 80
  UNION ALL
  SELECT
    snapshot_id,
    'A06_STAGE_DISTRIBUTED_JOIN',
    'Rewrite',
    'crit',
    CAST(query_id AS VARCHAR),
    query_id,
    NULL,
    86 + LEAST(COALESCE(dist_total_cnt, 0) * 4, 30),
    'Stage this distributed join path into an analyzed temp table.',
    'The plan repeatedly moves data between slices before joins.',
    'dist_total=' || CAST(COALESCE(dist_total_cnt, 0) AS VARCHAR)
      || ', bcast=' || CAST(COALESCE(bcast_cnt, 0) AS VARCHAR)
      || ', dist_both=' || CAST(COALESCE(dist_both_cnt, 0) AS VARCHAR),
    'CREATE TEMP TABLE stage_x DISTKEY(join_key) SORTKEY(date_key) AS SELECT ...; ANALYZE stage_x;'
  FROM query_impact
  WHERE COALESCE(dist_both_cnt, 0) > 0 OR COALESCE(dist_total_cnt, 0) >= 4
  UNION ALL
  SELECT
    snapshot_id,
    'A07_MATERIALIZE_EXTERNAL_SCAN',
    'External Data',
    CASE WHEN COALESCE(external_duration_pct, 0) >= 0.5 THEN 'crit' ELSE 'warn' END,
    CAST(query_id AS VARCHAR),
    query_id,
    NULL,
    80 + COALESCE(external_duration_pct, 0) * 35,
    'Materialize or repartition this external scan path.',
    'External/S3 work consumes a material share of the slow-query runtime.',
    'external_duration_pct=' || CAST(ROUND(COALESCE(external_duration_pct, 0) * 100, 1) AS VARCHAR) || '%'
      || ', s3_scans=' || CAST(COALESCE(s3_scan_cnt, 0) AS VARCHAR),
    'CREATE TABLE optimized_local DISTKEY(join_key) SORTKEY(date_key) AS SELECT ... FROM external_table WHERE ...;'
  FROM query_impact
  WHERE COALESCE(external_duration_pct, 0) >= 0.25 OR COALESCE(s3_scan_cnt, 0) > 0
  UNION ALL
  SELECT
    snapshot_id,
    'A08_FIX_FANOUT_OR_CROSS_JOIN',
    'Rewrite',
    'crit',
    CAST(query_id AS VARCHAR),
    query_id,
    NULL,
    88 + LEAST(COALESCE(selectivity_ratio, 0), 25),
    'Fix fan-out join cardinality before tuning hardware or WLM.',
    'The query expands rows or contains cross-join/fan-out risk.',
    'selectivity_ratio=' || CAST(ROUND(COALESCE(selectivity_ratio, 0), 4) AS VARCHAR)
      || ', input_rows=' || CAST(COALESCE(input_rows, 0) AS VARCHAR)
      || ', output_rows=' || CAST(COALESCE(output_rows, 0) AS VARCHAR),
    'Verify join predicates and pre-deduplicate one-to-many dimensions before the join.'
  FROM query_impact
  WHERE COALESCE(selectivity_ratio, 0) >= 5 OR POSITION('cross join' IN LOWER(COALESCE(sql_text, ''))) > 0
  UNION ALL
  SELECT
    snapshot_id,
    'A09_REVIEW_WLM_QUEUE_PRESSURE',
    'Workload Management',
    CASE WHEN COALESCE(queue_s, 0) >= 60 THEN 'crit' ELSE 'warn' END,
    CAST(query_id AS VARCHAR),
    query_id,
    NULL,
    60 + LEAST(COALESCE(queue_s, 0) / 2, 40),
    'Review WLM queue pressure for this query/workload.',
    'Queue time is a large share of a slow-query experience and may not be SQL design alone.',
    'queue_s=' || CAST(ROUND(COALESCE(queue_s, 0), 1) AS VARCHAR)
      || ', execution_s=' || CAST(ROUND(COALESCE(execution_s, 0), 1) AS VARCHAR)
      || ', user=' || COALESCE(user_name, ''),
    'Inspect service class concurrency, slot count, and workload owner scheduling.'
  FROM query_impact
  WHERE COALESCE(queue_s, 0) >= 20
  UNION ALL
  SELECT
    snapshot_id,
    'A10_INVESTIGATE_MEMORY_STARVATION',
    'Memory',
    CASE WHEN COALESCE(total_spill, 0) >= 100000 THEN 'crit' ELSE 'warn' END,
    CAST(query_id AS VARCHAR),
    query_id,
    NULL,
    82 + LEAST(COALESCE(total_spill, 0) / 10000.0, 35),
    'Investigate memory starvation and intermediate width.',
    'Disk spill means hash/sort work exceeded memory and likely dominates elapsed time.',
    'spill_blocks=' || CAST(COALESCE(total_spill, 0) AS VARCHAR)
      || ', max_step_s=' || CAST(ROUND(COALESCE(max_step_duration_s, 0), 1) AS VARCHAR)
      || ', input_rows=' || CAST(COALESCE(input_rows, 0) AS VARCHAR),
    'Reduce row width before joins/sorts, push filters earlier, or adjust WLM memory for the workload.'
  FROM query_impact
  WHERE COALESCE(total_spill, 0) > 0
)
SELECT
  ROW_NUMBER() OVER (PARTITION BY snapshot_id ORDER BY action_score DESC NULLS LAST) AS priority_rank,
  *
FROM raw_actions
ORDER BY snapshot_id, priority_rank;
"""

# Every insight row must retain the source cluster identity. Keeping this
# transformation beside the generated SQL avoids 32 hand-maintained copies of
# the same namespace column while preserving identical UNION schemas.
_ANALYTICS_SQL = (
    _ANALYTICS_SQL
    .replace("SELECT snapshot_id, 'Q", "SELECT snapshot_id, namespace_id, 'Q")
    .replace("SELECT snapshot_id, 'T", "SELECT snapshot_id, namespace_id, 'T")
)
