from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest

import runner
from analyzer.duckdb_store import DuckDBStore, EXPECTED_COLUMNS
from analyzer.cluster_analyze import load_cluster_report
from analyzer.settings import AnalyzerSettings, save_settings


def _args() -> SimpleNamespace:
    return SimpleNamespace(
        table_databases=None,
        days=7.0,
        floor_seconds=600.0,
        floor_basis="execution_time",
    )


def _producer_env(monkeypatch) -> None:
    monkeypatch.setenv("REDSHIFT_HOST", "producer.example")
    monkeypatch.setenv("REDSHIFT_DATABASE", "dev")
    monkeypatch.setenv("REDSHIFT_USER", "producer_user")
    monkeypatch.setenv("REDSHIFT_PASSWORD", "producer_password")
    monkeypatch.setenv("REDSHIFT_NAMESPACE_ID", "ns-producer")


def test_profiles_support_producer_plus_seven_consumers(monkeypatch) -> None:
    _producer_env(monkeypatch)
    for number in range(1, 8):
        prefix = f"REDSHIFT_CONSUMER_{number}"
        monkeypatch.setenv(f"{prefix}_HOST", f"consumer-{number}.example")
        monkeypatch.setenv(f"{prefix}_DATABASE", "dev")
        monkeypatch.setenv(f"{prefix}_USER", f"consumer_{number}")
        monkeypatch.setenv(f"{prefix}_PASSWORD", f"password_{number}")
        monkeypatch.setenv(f"{prefix}_NAMESPACE_ID", f"ns-consumer-{number}")
        monkeypatch.setenv(f"{prefix}_DISPLAY_NAME", f"Friendly Consumer {number}")

    profiles = runner.build_configs(_args())

    assert len(profiles) == 8
    assert profiles[0].cluster_role == "producer"
    assert profiles[0].namespace_id == "ns-producer"
    assert profiles[-1].cluster_role == "consumer"
    assert profiles[-1].cluster_ordinal == 7
    assert profiles[-1].cluster_name == "Friendly Consumer 7"
    assert len({profile.namespace_id for profile in profiles}) == 8


def test_direct_redshift_reader_pool_defaults_to_four_and_is_capped(
    monkeypatch,
) -> None:
    profiles = [object() for _ in range(12)]
    monkeypatch.delenv("INFRAREDSHIFT_PARALLEL_LOAD", raising=False)
    monkeypatch.delenv("INFRAREDSHIFT_REDSHIFT_READ_WORKERS", raising=False)

    assert runner._redshift_read_worker_count(profiles) == 4

    monkeypatch.setenv("INFRAREDSHIFT_REDSHIFT_READ_WORKERS", "20")
    assert runner._redshift_read_worker_count(profiles) == 8


def test_direct_redshift_reader_pool_can_be_reduced_or_disabled(monkeypatch) -> None:
    profiles = [object() for _ in range(4)]
    monkeypatch.setenv("INFRAREDSHIFT_REDSHIFT_READ_WORKERS", "2")
    assert runner._redshift_read_worker_count(profiles) == 2

    monkeypatch.setenv("INFRAREDSHIFT_PARALLEL_LOAD", "0")
    assert runner._redshift_read_worker_count(profiles) == 1


def test_configured_consumer_requires_namespace(monkeypatch) -> None:
    _producer_env(monkeypatch)
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_HOST", "consumer.example")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_USER", "consumer")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_PASSWORD", "password")

    with pytest.raises(SystemExit, match="REDSHIFT_CONSUMER_1_NAMESPACE_ID"):
        runner.build_configs(_args())


def test_configured_producer_requires_real_namespace(monkeypatch) -> None:
    monkeypatch.delenv("REDSHIFT_PRODUCER_NAMESPACE_ID", raising=False)
    monkeypatch.delenv("REDSHIFT_NAMESPACE_ID", raising=False)
    monkeypatch.setenv("REDSHIFT_HOST", "producer.example")
    monkeypatch.setenv("REDSHIFT_DATABASE", "dev")
    monkeypatch.setenv("REDSHIFT_USER", "producer_user")
    monkeypatch.setenv("REDSHIFT_PASSWORD", "producer_password")

    with pytest.raises(SystemExit, match="REDSHIFT_PRODUCER_NAMESPACE_ID"):
        runner.build_configs(_args())


def test_profile_database_lists_are_discovered_not_read_from_environment(monkeypatch) -> None:
    _producer_env(monkeypatch)
    monkeypatch.setenv("REDSHIFT_PRODUCER_TABLE_DATABASES", "stale_one,stale_two")

    profile = runner.build_configs(_args())[0]

    assert profile.table_databases == ""


def test_database_cycled_catalogs_use_emergency_fixed_scope(monkeypatch) -> None:
    _producer_env(monkeypatch)
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_ENABLED", "true")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_HOST", "consumer.example")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_USER", "consumer")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_PASSWORD", "password")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_NAMESPACE_ID", "ns-consumer")
    calls = []

    def fake_fetch(cfg, database, sql, *, stage):
        calls.append((cfg.namespace_id, database, sql, stage))
        raise AssertionError("database discovery must remain disabled")

    monkeypatch.setattr(runner, "fetch_frame", fake_fetch)
    profiles = runner.build_configs(_args())

    scoped = {cfg.namespace_id: runner.resolve_table_databases(cfg) for cfg in profiles}

    assert scoped == {
        "ns-producer": (
            "datalake_cl",
            "enterprise_datawarehouse",
            "businesslayer",
            "datalake_wealth",
            "datalake_cl1",
            "datalake_cl2",
            "datalake",
            "byod",
            "investors_bank",
            "smart_leads",
        ),
        "ns-consumer": ("enterprise_datawarehouse",),
    }
    assert calls == []


def test_ingest_database_cycled_catalogs_use_producer_emergency_scope() -> None:
    from analyzer import ingest_redshift

    args = SimpleNamespace(table_databases="dev,businesslayer")
    scoped = ingest_redshift.resolve_table_databases(
        args,
        settings=None,
        user="",
        password="",
        jdbc_url="",
        jars=(),
    )

    assert scoped == (
        "datalake_cl",
        "enterprise_datawarehouse",
        "businesslayer",
        "datalake_wealth",
        "datalake_cl1",
        "datalake_cl2",
        "datalake",
        "byod",
        "investors_bank",
        "smart_leads",
    )


def test_namespaced_load_resumes_from_table_checkpoint(tmp_path) -> None:
    path = tmp_path / "resume.duckdb"
    duckdb.connect(str(path)).close()
    cfg = SimpleNamespace(namespace_id="ns-producer")
    args = SimpleNamespace(
        resume=False,
        lock_wait_seconds=1,
        days=7.0,
        floor_seconds=300.0,
        floor_basis="execution_time",
    )

    snapshot_id, completed = runner._resolve_multi_run(args, path, [cfg])
    assert completed == {}
    runner.save_state(
        path,
        1,
        {
            "snapshot_id": snapshot_id,
            "status": "loading",
            "days": "7.0",
            "floor_seconds": "300.0",
            "floor_basis": "execution_time",
            "namespace_ids": "ns-producer",
            "catalog_database_scope": runner.CATALOG_DATABASE_SCOPE_KEY,
        },
    )
    runner._mark_namespace_table_complete(path, 1, snapshot_id, "ns-producer", "query_history", 414)

    args.resume = True
    resumed_snapshot, resumed = runner._resolve_multi_run(args, path, [cfg])

    assert resumed_snapshot == snapshot_id
    assert resumed == {("ns-producer", "query_history"): 414}


def test_resume_preserves_workload_but_invalidates_old_catalog_scope(
    tmp_path,
) -> None:
    path = tmp_path / "catalog-scope-change.duckdb"
    duckdb.connect(str(path)).close()
    cfg = SimpleNamespace(namespace_id="ns-producer")
    args = SimpleNamespace(
        resume=False,
        lock_wait_seconds=1,
        days=7.0,
        floor_seconds=300.0,
        floor_basis="execution_time",
    )
    snapshot_id, _ = runner._resolve_multi_run(args, path, [cfg])
    runner.save_state(
        path,
        1,
        {
            "snapshot_id": snapshot_id,
            "status": "loading",
            "days": "7.0",
            "floor_seconds": "300.0",
            "floor_basis": "execution_time",
            "namespace_ids": "ns-producer",
            "catalog_database_scope": "dev,businesslayer",
        },
    )
    runner._mark_namespace_table_complete(
        path, 1, snapshot_id, "ns-producer", "query_history", 414
    )
    runner._mark_namespace_table_complete(
        path, 1, snapshot_id, "ns-producer", "svv_table_info_all", 99
    )
    con = duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE query_history_tmp "
            "(snapshot_id VARCHAR, namespace_id VARCHAR)"
        )
        con.execute(
            "CREATE TABLE svv_table_info_all_tmp "
            "(snapshot_id VARCHAR, namespace_id VARCHAR)"
        )
        con.execute(
            f"INSERT INTO {runner.SQL_STASH_TABLE} VALUES "
            "('query_history', 'query sql'), "
            "('svv_table_info_all', 'catalog sql')"
        )
    finally:
        con.close()

    args.resume = True
    resumed_snapshot, resumed = runner._resolve_multi_run(args, path, [cfg])

    assert resumed_snapshot == snapshot_id
    assert resumed == {("ns-producer", "query_history"): 414}
    con = duckdb.connect(str(path), read_only=True)
    try:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        assert "query_history_tmp" in tables
        assert "svv_table_info_all_tmp" not in tables
        assert con.execute(
            f"SELECT table_name FROM {runner.SQL_STASH_TABLE}"
        ).fetchall() == [("query_history",)]
    finally:
        con.close()


def test_new_snapshot_discards_stale_staging_and_state(tmp_path) -> None:
    path = tmp_path / "fresh.duckdb"
    con = duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE query_history_tmp "
            "(snapshot_id VARCHAR, namespace_id VARCHAR, query_id VARCHAR)"
        )
        con.execute(
            "INSERT INTO query_history_tmp VALUES ('old-snapshot', 'old-ns', '7')"
        )
        con.execute(
            "CREATE TABLE _tmp_refresh_state "
            "(state_key VARCHAR, state_value VARCHAR)"
        )
        con.execute(
            "INSERT INTO _tmp_refresh_state VALUES "
            "('status', 'loading'), ('failure_count', '3')"
        )
    finally:
        con.close()
    cfg = SimpleNamespace(namespace_id="ns-producer")
    args = SimpleNamespace(
        resume=False,
        lock_wait_seconds=1,
        days=7.0,
        floor_seconds=300.0,
        floor_basis="execution_time",
    )

    snapshot_id, completed = runner._resolve_multi_run(args, path, [cfg])

    assert snapshot_id != "old-snapshot"
    assert completed == {}
    con = duckdb.connect(str(path), read_only=True)
    try:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        assert "query_history_tmp" not in tables
        assert runner.read_state(con) == {}
    finally:
        con.close()


def test_direct_runner_uses_namespaced_loader_for_one_cluster(monkeypatch) -> None:
    cfg = SimpleNamespace(namespace_id="ns-producer")
    args = SimpleNamespace()
    calls = []
    monkeypatch.setattr(runner, "build_configs", lambda value: (cfg,))
    monkeypatch.setattr(
        runner,
        "run_multi_load",
        lambda value, configs: calls.append((value, configs)) or 0,
    )

    assert runner.run_load(args) == 0
    assert calls == [(args, (cfg,))]


def test_resume_retry_removes_only_uncheckpointed_namespace_slice(tmp_path) -> None:
    path = tmp_path / "retry.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE query_history_tmp(namespace_id VARCHAR, query_id BIGINT)")
    con.execute("INSERT INTO query_history_tmp VALUES ('ns-producer', 1), ('ns-consumer', 2)")
    con.close()

    runner._clear_staged_namespace_rows(path, 1, "query_history", "ns-consumer")

    con = duckdb.connect(str(path))
    try:
        assert con.execute("SELECT namespace_id, query_id FROM query_history_tmp").fetchall() == [("ns-producer", 1)]
    finally:
        con.close()


def test_duplicate_namespace_is_rejected(monkeypatch) -> None:
    _producer_env(monkeypatch)
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_HOST", "consumer.example")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_USER", "consumer")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_PASSWORD", "password")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_NAMESPACE_ID", "NS-PRODUCER")

    with pytest.raises(SystemExit, match="unique NAMESPACE_ID"):
        runner.build_configs(_args())


def test_cluster_enabled_flags_choose_which_profiles_load(monkeypatch) -> None:
    _producer_env(monkeypatch)
    monkeypatch.setenv("REDSHIFT_PRODUCER_ENABLED", "false")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_ENABLED", "true")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_HOST", "consumer.example")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_DATABASE", "dev")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_USER", "consumer")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_PASSWORD", "password")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_NAMESPACE_ID", "ns-consumer")

    profiles = runner.build_configs(_args())

    assert [(profile.cluster_role, profile.namespace_id) for profile in profiles] == [
        ("consumer", "ns-consumer")
    ]


def test_at_least_one_cluster_must_be_checked(monkeypatch) -> None:
    _producer_env(monkeypatch)
    monkeypatch.setenv("REDSHIFT_PRODUCER_ENABLED", "false")

    with pytest.raises(SystemExit, match="No Redshift cluster is checked"):
        runner.build_configs(_args())


def test_fresh_load_allows_runner_to_create_new_duckdb(monkeypatch, tmp_path) -> None:
    _producer_env(monkeypatch)
    monkeypatch.setattr(runner, "_IMPORT_ERROR", None)
    monkeypatch.setattr(runner.sys, "version_info", (3, 12, 0))
    args = SimpleNamespace(
        duckdb_path=str(tmp_path / "new" / "redshift.duckdb"),
        swap=False,
        status=False,
        backup_only=False,
    )

    problems = [problem for problem in runner.sense_environment(args) if "redshift_connector" not in problem]

    assert problems == []
    assert (tmp_path / "new").is_dir()


def test_namespace_is_present_in_every_captured_table_schema() -> None:
    assert EXPECTED_COLUMNS
    assert all("namespace_id" in columns for columns in EXPECTED_COLUMNS.values())
    assert "namespace_id" in runner.COMMON_COLUMNS


def test_legacy_rows_are_backfilled_as_producer(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("REDSHIFT_PRODUCER_NAMESPACE_ID", "legacy-producer-ns")
    path = tmp_path / "legacy.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE query_history(snapshot_id VARCHAR, captured_at TIMESTAMP, query_id VARCHAR)")
    con.execute("INSERT INTO query_history VALUES ('old', CURRENT_TIMESTAMP, '42')")
    con.close()

    store = DuckDBStore(path)
    with store.connect() as migrated:
        row = migrated.execute("SELECT namespace_id FROM query_history").fetchone()

    assert row == ("legacy-producer-ns",)


def test_same_query_and_table_names_remain_separate_by_namespace(tmp_path) -> None:
    store = DuckDBStore(tmp_path / "namespaces.duckdb")
    run = store.new_snapshot("namespace collision")
    histories = pd.DataFrame([
        {
            "namespace_id": "producer-ns", "query_id": 100, "user_id": 7,
            "database_name": "dev", "start_time": "2026-07-16 01:00:00",
            "end_time": "2026-07-16 01:10:00", "elapsed_time": 600_000_000,
        },
        {
            "namespace_id": "consumer-ns", "query_id": 100, "user_id": 7,
            "database_name": "dev", "start_time": "2026-07-16 02:00:00",
            "end_time": "2026-07-16 02:20:00", "elapsed_time": 1_200_000_000,
        },
    ])
    text = pd.DataFrame([
        {"namespace_id": "producer-ns", "query_id": 100, "sequence": 0, "text": "select 'producer'"},
        {"namespace_id": "consumer-ns", "query_id": 100, "sequence": 0, "text": "select 'consumer'"},
    ])
    tables = pd.DataFrame([
        {"namespace_id": "producer-ns", "source_db": "dev", "database": "dev", "schema": "sales", "table": "orders"},
        {"namespace_id": "consumer-ns", "source_db": "dev", "database": "dev", "schema": "sales", "table": "orders"},
    ])
    with store.connect() as con:
        store.record_snapshot(con, run, source="test")
        store.replace_table_from_frame(con, "query_history", histories, run)
        store.replace_table_from_frame(con, "query_text", text, run)
        store.replace_table_from_frame(con, "svv_table_info_all", tables, run)
        queries = con.execute(
            "SELECT namespace_id, query_id, sql_text FROM v_slow_queries ORDER BY namespace_id"
        ).fetchall()
        table_keys = con.execute(
            "SELECT namespace_id, table_key FROM v_table_info ORDER BY namespace_id"
        ).fetchall()

    assert queries == [
        ("consumer-ns", 100, "select 'consumer'"),
        ("producer-ns", 100, "select 'producer'"),
    ]
    assert table_keys == [
        ("consumer-ns", "consumer-ns.dev.sales.orders"),
        ("producer-ns", "producer-ns.dev.sales.orders"),
    ]


def test_analysis_scope_can_focus_one_consumer_without_reloading(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(tmp_path / "settings"))
    path = tmp_path / "scope.duckdb"
    store = DuckDBStore(path)
    run = store.new_snapshot("scope")
    # Two runs of the same SQL per cluster: the analyzer keeps repeating
    # patterns only, so one-off fixtures would be filtered out inside DuckDB.
    histories = pd.DataFrame([
        {
            "namespace_id": "producer-ns", "query_id": 100, "database_name": "dev",
            "elapsed_time": 600_000_000, "execution_time": 600_000_000,
        },
        {
            "namespace_id": "producer-ns", "query_id": 101, "database_name": "dev",
            "elapsed_time": 600_000_000, "execution_time": 600_000_000,
        },
        {
            "namespace_id": "reporting-consumer-ns", "query_id": 100, "database_name": "dev",
            "elapsed_time": 1_200_000_000, "execution_time": 1_200_000_000,
        },
        {
            "namespace_id": "reporting-consumer-ns", "query_id": 101, "database_name": "dev",
            "elapsed_time": 1_200_000_000, "execution_time": 1_200_000_000,
        },
    ])
    text = pd.DataFrame([
        {"namespace_id": "producer-ns", "query_id": 100, "sequence": 0, "text": "select 'producer'"},
        {"namespace_id": "producer-ns", "query_id": 101, "sequence": 0, "text": "select 'producer'"},
        {"namespace_id": "reporting-consumer-ns", "query_id": 100, "sequence": 0, "text": "select 'consumer'"},
        {"namespace_id": "reporting-consumer-ns", "query_id": 101, "sequence": 0, "text": "select 'consumer'"},
    ])
    with store.connect() as con:
        store.record_snapshot(con, run, source="test")
        store.replace_table_from_frame(con, "query_history", histories, run)
        store.replace_table_from_frame(con, "query_text", text, run)

    save_settings(AnalyzerSettings(analysis_namespace_filter=["reporting-consumer-ns"]))
    report = load_cluster_report(path, snapshot_id=run.snapshot_id, areas=["slow_queries"])

    assert report.slow_queries["namespace_id"].tolist() == ["reporting-consumer-ns"] * 2
    assert report.slow_queries["sql_text"].tolist() == ["select 'consumer'"] * 2


def test_multi_cluster_runner_appends_namespaces_into_shared_staging_tables(monkeypatch, tmp_path) -> None:
    _producer_env(monkeypatch)
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_HOST", "consumer.example")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_DATABASE", "dev")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_USER", "consumer")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_PASSWORD", "password")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_NAMESPACE_ID", "ns-consumer")
    path = tmp_path / "multi.duckdb"
    store = DuckDBStore(path)
    with store.connect():
        pass
    # Reproduce the demo-blocking condition: a prior snapshot left a selected
    # staging table behind before the operator explicitly started over.
    con = duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE query_history_tmp "
            "(snapshot_id VARCHAR, namespace_id VARCHAR, query_id VARCHAR)"
        )
        con.execute(
            "INSERT INTO query_history_tmp VALUES "
            "('old-snapshot', 'stale-namespace', '999')"
        )
    finally:
        con.close()
    args = SimpleNamespace(
        duckdb_path=str(path), table_databases="dev", days=1.0,
        floor_seconds=300.0, floor_basis="execution_time", lock_wait_seconds=1,
        resume=False,
    )
    profiles = runner.build_configs(args)

    monkeypatch.setattr(runner, "validate_primary_sources", lambda cfg: None)
    monkeypatch.setattr(runner, "resolve_table_databases", lambda cfg: ("dev",))
    monkeypatch.setattr(runner, "table_sql", lambda cfg, table_name, target_ids=None: table_name)
    monkeypatch.setattr(runner, "normalize_view_definitions", lambda frame, database: frame)
    monkeypatch.setattr(runner, "normalize_procedure_definitions", lambda frame, database: frame)

    def fake_fetch(cfg, database, sql, stage=""):
        if sql == runner.TABLE_INFO_SQL:
            return pd.DataFrame([{"database": "dev", "schema": "sales", "table": "orders"}])
        if sql == runner.VIEW_DEFINITIONS_SQL:
            return pd.DataFrame([{"database": "dev", "schema": "sales", "view_name": "orders_v", "source_definition": "select 1"}])
        if sql == runner.PROCEDURE_DEFINITIONS_SQL:
            return pd.DataFrame([{"database": "dev", "schema": "admin", "procedure_name": "p", "source_definition": "select 1"}])
        if sql == "query_text":
            return pd.DataFrame([{"query_id": 10, "sequence": 0, "text": f"select '{cfg.namespace_id}'"}])
        if sql in {"query_history", "query_history_all"}:
            return pd.DataFrame([{"query_id": 10, "execution_time": 600_000_000}])
        if sql == "user_info":
            return pd.DataFrame([{"user_id": 1, "user_name": cfg.namespace_id}])
        return pd.DataFrame([{"query_id": 10}])

    monkeypatch.setattr(runner, "fetch_frame", fake_fetch)

    assert runner.run_multi_load(args, profiles) == 0

    con = duckdb.connect(str(path))
    try:
        assert set(row[0] for row in con.execute(
            "SELECT DISTINCT namespace_id FROM query_history_tmp"
        ).fetchall()) == {"ns-producer", "ns-consumer"}
        assert con.execute(
            "SELECT COUNT(DISTINCT snapshot_id) FROM query_history_tmp"
        ).fetchone()[0] == 1
        assert set(row[0] for row in con.execute(
            "SELECT DISTINCT namespace_id FROM svv_table_info_all_tmp"
        ).fetchall()) == {"ns-producer", "ns-consumer"}
        assert con.execute(
            "SELECT COUNT(*) FROM _tmp_snapshot_cluster_runs"
        ).fetchone()[0] == 2
        # External-table capture is excluded in this version: the loader must
        # not stage (or query Redshift for) external metadata at all.
        staged_tables = {
            str(row[0]).lower()
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        assert "external_table_info_all_tmp" not in staged_tables
    finally:
        con.close()


def test_one_button_load_never_halts_and_catalogs_failures(monkeypatch, tmp_path) -> None:
    """A failing table must be recorded and skipped, the run must finish (return 0),
    auto-promotion must be blocked (status stays 'loading'), and a report is written."""
    _producer_env(monkeypatch)
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_HOST", "consumer.example")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_DATABASE", "dev")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_USER", "consumer")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_PASSWORD", "password")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_NAMESPACE_ID", "ns-consumer")
    path = tmp_path / "multi.duckdb"
    store = DuckDBStore(path)
    with store.connect():
        pass
    args = SimpleNamespace(
        duckdb_path=str(path), table_databases="dev", days=1.0,
        floor_seconds=300.0, floor_basis="execution_time", lock_wait_seconds=1,
        resume=False,
    )
    profiles = runner.build_configs(args)

    monkeypatch.setattr(runner, "validate_primary_sources", lambda cfg: None)
    monkeypatch.setattr(runner, "resolve_table_databases", lambda cfg: ("dev",))
    monkeypatch.setattr(runner, "table_sql", lambda cfg, table_name, target_ids=None: table_name)
    monkeypatch.setattr(runner, "normalize_view_definitions", lambda frame, database: frame)
    monkeypatch.setattr(runner, "normalize_procedure_definitions", lambda frame, database: frame)

    def fake_fetch(cfg, database, sql, stage=""):
        # user_info always explodes on the consumer namespace: one bad table
        # on one cluster must not stop the rest of the load.
        if sql == "user_info" and cfg.namespace_id == "ns-consumer":
            raise RuntimeError("boom: user_info source column missing")
        if sql == runner.TABLE_INFO_SQL:
            return pd.DataFrame([{"database": "dev", "schema": "sales", "table": "orders"}])
        if sql == runner.VIEW_DEFINITIONS_SQL:
            return pd.DataFrame([{"database": "dev", "schema": "sales", "view_name": "orders_v", "source_definition": "select 1"}])
        if sql == runner.PROCEDURE_DEFINITIONS_SQL:
            return pd.DataFrame([{"database": "dev", "schema": "admin", "procedure_name": "p", "source_definition": "select 1"}])
        if sql == "query_text":
            return pd.DataFrame([{"query_id": 10, "sequence": 0, "text": f"select '{cfg.namespace_id}'"}])
        if sql in {"query_history", "query_history_all"}:
            return pd.DataFrame([{"query_id": 10, "execution_time": 600_000_000}])
        return pd.DataFrame([{"query_id": 10}])

    monkeypatch.setattr(runner, "fetch_frame", fake_fetch)

    # The run completes without raising, despite the failing table.
    assert runner.run_multi_load(args, profiles) == 0

    # A report was written next to the warehouse and names the failed table.
    report_txt = (path.parent / "load_report.txt").read_text(encoding="utf-8")
    assert "user_info" in report_txt
    assert "did NOT load" in report_txt
    import json as _json
    payload = _json.loads((path.parent / "load_report.json").read_text(encoding="utf-8"))
    assert payload["clean"] is False
    assert payload["failure_count"] >= 1
    assert any(f["table"] == "user_info" for f in payload["failures"])

    con = duckdb.connect(str(path))
    try:
        # Every other table still loaded for BOTH namespaces (non-halting).
        assert set(row[0] for row in con.execute(
            "SELECT DISTINCT namespace_id FROM query_history_tmp"
        ).fetchall()) == {"ns-producer", "ns-consumer"}
        # Promotion interlock: a partial load leaves status 'loading', not 'loaded'.
        state = runner.read_state(con)
        assert state.get("status") == "loading"
    finally:
        con.close()
