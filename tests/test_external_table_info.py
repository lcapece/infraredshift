from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pandas as pd

import load_external_table_info as focused_loader
import runner
from analyzer import ingest_redshift
from analyzer.duckdb_store import DuckDBStore, EXPECTED_COLUMNS
from analyzer.redshift_queries import (
    EXTERNAL_CATALOG_STAGE_SQL,
    EXTERNAL_COLUMN_STATS_STAGE_SQL,
    EXTERNAL_TABLE_INFO_SQL,
    SOURCE_REQUIREMENTS,
    assemble_external_table_info,
    external_errors_stage_sql,
    external_segments_stage_sql,
    external_steps_stage_sql,
    external_table_summary_sql,
)


def _external_row() -> dict:
    return {
        "external_table_key": "dev.spectrum.sales",
        "redshift_database_name": "dev",
        "schema_name": "spectrum",
        "table_name": "sales",
        "s3_location": "s3://bucket/sales/",
        "gross_scan_bytes": 10 * 1024**3,
        "gross_scan_gb": 10,
        "gross_output_bytes": 1024**3,
        "gross_output_gb": 1,
        "gross_scan_rows": 1_000_000,
        "gross_output_rows": 10_000,
        "row_filter_efficiency_pct": 99,
        "filtering_assessment": "HIGHLY_SELECTIVE",
        "partition_pruning_pct": 90,
        "s3list_time_ms": 250,
        "get_partition_time_total_raw": 1234,
        "get_partition_time_unit": "AWS_NOT_DOCUMENTED",
        "warning_event_count": 2,
        "partition_warning_count": 1,
        "sampled_error_count": 3,
    }


def test_external_query_detail_requirements_match_all_documented_columns_used() -> None:
    assert SOURCE_REQUIREMENTS["sys_external_query_detail"] == {
        "user_id", "query_id", "transaction_id", "child_query_sequence", "segment_id",
        "source_type", "start_time", "end_time", "duration", "total_partitions",
        "qualified_partitions", "scanned_files", "returned_rows", "returned_bytes",
        "file_format", "file_location", "external_query_text", "warning_message",
        "table_name", "is_recursive", "is_nested", "s3list_time", "get_partition_time",
    }


def test_external_table_sql_preserves_aws_units_and_builds_health_metrics() -> None:
    sql = EXTERNAL_TABLE_INFO_SQL.lower()
    assert "e.returned_bytes as scanned_bytes" in sql
    assert "e.returned_rows as scanned_rows" in sql
    assert "d.output_bytes" in sql and "d.output_rows" in sql
    assert "row_filter_efficiency_pct" in sql
    assert "partition_pruning_pct" in sql
    assert "s3list_time_ms" in sql
    assert "get_partition_time_total_raw" in sql
    assert "aws_not_documented" in sql
    assert "security_warning_count" in sql
    assert "schema_format_warning_count" in sql
    assert "sampled_error_count" in sql


def test_focused_summary_is_bounded_and_skips_partition_catalog_enumeration() -> None:
    sql = external_table_summary_sql(3).lower()
    assert "from svv_external_partitions" not in sql
    assert "dateadd(day, -3, getdate())" in sql
    assert "total_partitions" in sql
    assert "qualified_partitions" in sql


def test_focused_loader_defaults_are_demo_safe() -> None:
    args = focused_loader._args([])
    assert args.days == 7.0
    assert args.hours == 6
    assert args.chunk_hours == 1
    assert args.query_batch_size == 100
    assert args.statement_timeout_seconds == 600
    assert not hasattr(args, "table_databases")


def test_focused_loader_preserves_catalog_before_optional_enrichment() -> None:
    calls: list[str] = []

    class FakeRunner:
        pd = pd
        EXTERNAL_CATALOG_STAGE_SQL = "catalog"
        EXTERNAL_COLUMN_STATS_STAGE_SQL = "columns"

        @staticmethod
        def external_segments_stage_sql(_days, _hours):
            return "segments"

        @staticmethod
        def external_query_ids(_frame):
            return [10]

        @staticmethod
        def external_steps_stage_sql(_query_ids):
            return "steps"

        @staticmethod
        def fetch_frame(_cfg, _database, sql, stage=""):
            del stage
            calls.append(sql)
            if sql == "segments":
                return pd.DataFrame([{"query_id": 10}])
            return pd.DataFrame()

        @staticmethod
        def minimal_external_catalog_from_segments(_segments, _database):
            return pd.DataFrame()

    args = SimpleNamespace(
        days=7.0, hours=6, query_batch_size=100, include_errors=False,
    )
    cfg = SimpleNamespace(namespace_id="producer")

    focused_loader._capture_database_stages(args, FakeRunner(), cfg, "dev")

    assert calls == ["catalog", "segments", "steps", "columns"]


def test_emergency_focused_sql_uses_only_active_external_detail() -> None:
    sql = focused_loader._active_external_summary_sql(2).lower()
    assert "from sys_external_query_detail" in sql
    assert "svv_external" not in sql
    assert "sys_query_detail" not in sql
    assert "sys_query_history" not in sql
    assert "sys_external_query_error" not in sql
    assert "group by lower(trim(table_name))" in sql
    assert "dateadd(day, -2, getdate())" in sql
    hourly = focused_loader._active_external_summary_sql(7, 6).lower()
    assert "dateadd(hour, -6, getdate())" in hourly
    assert "ilike" not in hourly
    window = focused_loader._active_external_window_sql(6, 5).lower()
    assert "start_time >= dateadd(hour, -6, getdate())" in window
    assert "start_time < dateadd(hour, -5, getdate())" in window
    candidates = focused_loader._recent_query_ids_sql(6).lower()
    assert "from sys_query_history" in candidates
    assert "dateadd(hour, -6, getdate())" in candidates
    batch = focused_loader._active_external_query_batch_sql([30, 20, 30]).lower()
    assert "query_id in (30, 20)" in batch
    assert "start_time >=" not in batch


def test_staging_queries_touch_one_redshift_view_and_never_enumerate_partitions() -> None:
    statements = (
        EXTERNAL_CATALOG_STAGE_SQL,
        EXTERNAL_COLUMN_STATS_STAGE_SQL,
        external_segments_stage_sql(7),
        external_steps_stage_sql([10, 20]),
        external_errors_stage_sql([10, 20]),
    )
    for sql in statements:
        lowered = sql.lower()
        assert " join " not in lowered
        assert "svv_external_partitions" not in lowered
    assert "from svv_external_tables" in EXTERNAL_CATALOG_STAGE_SQL.lower()
    assert "from svv_external_columns" in EXTERNAL_COLUMN_STATS_STAGE_SQL.lower()
    assert "from sys_external_query_detail" in external_segments_stage_sql(7).lower()


def test_independent_stages_are_joined_locally_with_partition_key_names() -> None:
    stages = {
        "svv_external_tables": pd.DataFrame([{
            "redshift_database_name": "dev", "schemaname": "spectrum", "tablename": "sales",
            "tabletype": "TABLE", "location": "s3://bucket/sales/", "input_format": "parquet",
            "output_format": "", "serialization_lib": "", "serde_parameters": "",
            "compressed": 1, "parameters": "",
        }]),
        "external_column_stats": pd.DataFrame([
            {"external_table_key": "dev.spectrum.sales", "columnname": "sale_id", "part_key": 0},
            {"external_table_key": "dev.spectrum.sales", "columnname": "sale_date", "part_key": 1},
            {"external_table_key": "dev.spectrum.sales", "columnname": "region", "part_key": 2},
        ]),
        "sys_query_history": pd.DataFrame([{"query_id": 10, "database_name": "dev"}]),
        "sys_external_query_detail": pd.DataFrame([{
            "user_id": 1, "query_id": 10, "transaction_id": 2, "child_query_sequence": 0,
            "segment_id": 3, "source_type": "S3", "start_time": "2026-01-01 00:00:00",
            "end_time": "2026-01-01 00:00:01", "duration": 1_000_000,
            "total_partitions": 100, "qualified_partitions": 10, "scanned_files": 2,
            "returned_rows": 1000, "returned_bytes": 10000, "file_format": "parquet",
            "file_location": "s3://bucket/sales/day=1/file.parquet", "external_query_text": "",
            "warning_message": "", "table_name": "dev.spectrum.sales", "is_recursive": "f",
            "is_nested": "f", "s3list_time": 5, "get_partition_time": 10,
        }]),
        "sys_query_detail": pd.DataFrame([{
            "query_id": 10, "segment_id": 3, "table_name": "dev.spectrum.sales",
            "output_bytes": 1000, "output_rows": 100, "data_skewness": 0, "time_skewness": 0,
            "spilled_block_local_disk": 0, "spilled_block_remote_disk": 0,
            "step_id": 1, "step_name": "scan", "source": "s3",
        }]),
        "sys_external_query_error": pd.DataFrame(),
    }

    result = assemble_external_table_info(stages).iloc[0]

    assert result["external_table_key"] == "dev.spectrum.sales"
    assert result["partition_key_columns"] == "sale_date, region"
    assert result["partition_key_count"] == 2
    assert result["total_partitions_considered"] == 100
    assert result["qualified_partitions_scanned"] == 10
    assert result["gross_scan_bytes"] == 10000
    assert result["gross_output_bytes"] == 1000


def test_external_table_storage_has_unique_key_and_typed_view(tmp_path) -> None:
    store = DuckDBStore(tmp_path / "external.duckdb")
    with store.connect() as con:
        run = store.new_snapshot("external test")
        store.record_snapshot(con, run, source="test")
        store.replace_table_from_frame(con, "external_table_info_all", pd.DataFrame([_external_row()]), run)
        row = con.execute(
            "SELECT external_table_key, gross_scan_gb, row_filter_efficiency_pct, "
            "partition_pruning_pct, warning_event_count "
            "FROM v_external_table_info"
        ).fetchone()
        assert row == ("producer.dev.spectrum.sales", 10.0, 99.0, 90.0, 2)
        store.rebuild_indexes(con)
        indexes = con.execute(
            "SELECT COUNT(*) FROM duckdb_indexes() WHERE table_name = 'external_table_info_all'"
        ).fetchone()[0]
        assert indexes >= 1


def test_external_table_sql_aggregates_scans_output_pruning_warnings_and_errors() -> None:
    con = duckdb.connect(":memory:")
    try:
        database = con.execute("SELECT CURRENT_DATABASE()").fetchone()[0]
        tables = {
            "svv_external_tables": pd.DataFrame([{
                "redshift_database_name": database, "schemaname": "spectrum", "tablename": "sales",
                "tabletype": "TABLE", "location": "s3://bucket/sales/", "input_format": "parquet",
                "output_format": "", "serialization_lib": "", "serde_parameters": "",
                "compressed": 1, "parameters": "",
            }]),
            "svv_external_columns": pd.DataFrame([
                {"redshift_database_name": database, "schemaname": "spectrum", "tablename": "sales",
                 "columnname": "sale_id", "external_type": "bigint", "columnnum": 1, "part_key": 0,
                 "is_nullable": "true"},
                {"redshift_database_name": database, "schemaname": "spectrum", "tablename": "sales",
                 "columnname": "sale_date", "external_type": "date", "columnnum": 2, "part_key": 1,
                 "is_nullable": "true"},
            ]),
            "sys_query_history": pd.DataFrame([{"query_id": 10, "database_name": database}]),
            "sys_external_query_detail": pd.DataFrame([{
                "user_id": 100, "query_id": 10, "transaction_id": 20, "child_query_sequence": 0,
                "segment_id": 4, "source_type": "S3", "start_time": "2026-01-01 00:00:00",
                "end_time": "2026-01-01 00:00:02", "duration": 2_000_000,
                "total_partitions": 100, "qualified_partitions": 10, "scanned_files": 20,
                "returned_rows": 1_000, "returned_bytes": 10_000, "file_format": "parquet",
                "file_location": "s3://bucket/sales/day=2026-01-01/file.parquet",
                "external_query_text": "", "warning_message": "partition metadata warning",
                "table_name": f"{database}.spectrum.sales", "is_recursive": "t", "is_nested": "f",
                "s3list_time": 25, "get_partition_time": 90,
            }]),
            "sys_query_detail": pd.DataFrame([{
                "query_id": 10, "segment_id": 4, "table_name": f"{database}.spectrum.sales",
                "output_bytes": 1_000, "output_rows": 100, "data_skewness": 5, "time_skewness": 7,
                "spilled_block_local_disk": 2, "spilled_block_remote_disk": 3, "step_id": 0,
                "step_name": "scan", "source": "s3",
            }]),
            "sys_external_query_error": pd.DataFrame([{
                "query_id": 10, "file_location": "s3://bucket/sales/day=2026-01-01/file.parquet",
                "rowid": "0:0:1", "column_name": "amount", "trigger": "UNSPECIFIED",
                "action": "OVERFLOW_VALUE", "error_code": 199,
            }]),
        }
        for name, frame in tables.items():
            con.register(f"incoming_{name}", frame)
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM incoming_{name}")
        result = con.execute(EXTERNAL_TABLE_INFO_SQL).fetchdf().iloc[0]
        assert result["external_table_key"] == f"{database}.spectrum.sales"
        assert result["gross_scan_bytes"] == 10_000
        assert result["gross_output_bytes"] == 1_000
        assert result["row_filter_efficiency_pct"] == 90
        assert result["partition_pruning_pct"] == 90
        assert result["partition_warning_count"] == 1
        assert result["overflow_error_count"] == 1
        assert result["external_spill_blocks"] == 5
    finally:
        con.close()


def test_general_runner_includes_external_table_info() -> None:
    assert "external_table_info_all" in runner.LIVE_REFRESH_TABLES
    assert runner.table_sql(
        SimpleNamespace(
            minutes=10,
            evidence_parent_limit=0,
            rank_by="elapsed_time",
            floor_seconds=600,
            floor_basis="execution_time",
            days=7,
            detail_flow_rows=300,
        ),
        "external_table_info_all",
    ) == runner.EXTERNAL_TABLE_INFO_SQL
    assert tuple(runner.EXPECTED_COLUMNS["external_table_info_all"]) == EXPECTED_COLUMNS["external_table_info_all"]
    assert runner.LIVE_REFRESH_TABLES[-1] == "external_table_info_all"
    assert ingest_redshift.LIVE_REFRESH_TABLES[-1] == "external_table_info_all"


def test_optional_external_sources_do_not_block_general_preflight(monkeypatch) -> None:
    validated: list[str] = []
    monkeypatch.setattr(
        ingest_redshift,
        "validate_source_view",
        lambda *_args, **_kwargs: validated.append(str(_args[-1])),
    )

    ingest_redshift.validate_sources(
        SimpleNamespace(),
        "dev",
        "user",
        "password",
        "jdbc:redshift://example/dev",
        (),
        include_sys=True,
        include_svv=True,
        included_tables={"external_table_info_all"},
    )

    assert validated == []


def test_external_timeout_can_retry_or_move_to_next_stage(monkeypatch) -> None:
    attempts = {"count": 0}

    def flaky(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("system requested abort: statement timeout")
        return pd.DataFrame([{"ok": 1}])

    monkeypatch.setattr(runner, "fetch_frame", flaky)
    cfg = SimpleNamespace(timeout_decision_callback=lambda _stage, _error: "continue")
    retried = runner.fetch_external_stage(cfg, "dev", "SELECT 1", "external partition keys")
    assert attempts["count"] == 2
    assert len(retried) == 1

    monkeypatch.setattr(
        runner,
        "fetch_frame",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("system requested abort: statement timeout")
        ),
    )
    cfg.timeout_decision_callback = lambda _stage, _error: "next"
    skipped = runner.fetch_external_stage(cfg, "dev", "SELECT 1", "external partition keys")
    assert skipped.empty


def test_focused_loader_promotes_only_external_table(tmp_path) -> None:
    path = tmp_path / "focused.duckdb"
    store = DuckDBStore(path)
    with store.connect() as con:
        con.execute("CREATE TABLE unrelated_tmp(marker VARCHAR)")
        con.execute("INSERT INTO unrelated_tmp VALUES ('preserve me')")
    runner.write_tmp_table(
        path,
        "external_table_info_all",
        pd.DataFrame([_external_row()]),
        "focused-snapshot",
        runner.EXTERNAL_TABLE_INFO_SQL,
        1,
    )
    args = SimpleNamespace(
        duckdb_path=str(path),
        table_databases=None,
        lock_wait_seconds=1,
        no_backup=True,
    )

    assert focused_loader.promote(args, runner) == 0

    con = duckdb.connect(str(path))
    try:
        assert con.execute("SELECT table_name FROM external_table_info_all").fetchone()[0] == "sales"
        assert con.execute("SELECT marker FROM unrelated_tmp").fetchone()[0] == "preserve me"
        assert not con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = 'external_table_info_all_tmp'"
        ).fetchone()[0]
        assert con.execute("SELECT COUNT(*) FROM snapshot_runs").fetchone()[0] == 0
    finally:
        con.close()
