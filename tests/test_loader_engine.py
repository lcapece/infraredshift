from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import duckdb
import pandas as pd

import runner
from analyzer import secrets_store
from analyzer.loader.engine import (
    LoaderAlreadyRunningError,
    LoaderEngine,
    LoaderRequest,
    _ProcessLock,
    build_loader_command,
    build_promote_command,
    loader_script_path,
)


def _request(tmp_path: Path, **changes) -> LoaderRequest:
    values = {
        "duckdb_path": tmp_path / "warehouse.duckdb",
        "days": 7.0,
        "floor_seconds": 300.0,
        "resume": True,
    }
    values.update(changes)
    return LoaderRequest(**values)


def test_refresh_uses_one_namespaced_backend_and_promotes_only_after_success(
    monkeypatch, tmp_path,
) -> None:
    calls = []
    events = []
    profile = SimpleNamespace(namespace_id="producer-ns")
    monkeypatch.setattr(runner, "_load_dotenv_if_present", lambda: calls.append("environment"))
    monkeypatch.setattr(runner, "sense_environment", lambda args: [])
    monkeypatch.setattr(runner, "build_configs", lambda args: (profile,))

    def load(args, configs):
        calls.append(("load", args.resume, args.include_external, configs))
        runner.emit_progress("producer-ns", "query_history", 414, 414, 1, 14, "Staged")
        # A clean load flips durable status to "loaded"; the promote gate reads it.
        runner.save_state(args.duckdb_path, args.lock_wait_seconds, {"status": "loaded"})
        return 0

    monkeypatch.setattr(runner, "run_multi_load", load)
    monkeypatch.setattr(runner, "run_swap", lambda args: calls.append(("swap", args.no_backup)) or 0)

    request = _request(
        tmp_path, promote=True, include_external=False,
        include_tables=("query_history", "query_text"),
    )
    assert LoaderEngine(events.append).refresh(request) == 0

    assert calls[0] == "environment"
    assert calls[1] == ("load", True, False, (profile,))
    assert calls[2] == ("swap", False)
    assert [event.event for event in events] == [
        "started", "progress", "staged", "promoting", "completed"
    ]
    assert events[1].table_name == "query_history"
    assert events[1].source_rows == 414


def test_failed_capture_never_promotes_and_next_run_can_reacquire_lock(
    monkeypatch, tmp_path,
) -> None:
    swaps = []
    profile = SimpleNamespace(namespace_id="producer-ns")
    monkeypatch.setattr(runner, "_load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(runner, "sense_environment", lambda args: [])
    monkeypatch.setattr(runner, "build_configs", lambda args: (profile,))
    monkeypatch.setattr(runner, "run_multi_load", lambda args, configs: (_ for _ in ()).throw(RuntimeError("network stopped")))
    monkeypatch.setattr(runner, "run_swap", lambda args: swaps.append(True) or 0)
    request = _request(tmp_path, promote=True)

    with pytest.raises(RuntimeError, match="network stopped"):
        LoaderEngine().refresh(request)
    assert swaps == []

    # The operating-system lock is released even after an exception.
    with _ProcessLock(request.duckdb_path):
        pass


def test_failed_promotion_is_reported_and_staging_remains_recoverable(
    monkeypatch, tmp_path,
) -> None:
    events = []
    profile = SimpleNamespace(namespace_id="producer-ns")
    monkeypatch.setattr(runner, "_load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(runner, "sense_environment", lambda args: [])
    monkeypatch.setattr(runner, "build_configs", lambda args: (profile,))

    def clean_load(args, configs):
        # Clean load ⇒ status "loaded" ⇒ the promote gate proceeds to run_swap.
        runner.save_state(args.duckdb_path, args.lock_wait_seconds, {"status": "loaded"})
        return 0

    monkeypatch.setattr(runner, "run_multi_load", clean_load)
    monkeypatch.setattr(
        runner, "run_swap",
        lambda args: (_ for _ in ()).throw(RuntimeError("DuckDB promotion stopped")),
    )

    with pytest.raises(RuntimeError, match="promotion stopped"):
        LoaderEngine(events.append).refresh(_request(tmp_path, promote=True))

    assert [event.event for event in events] == [
        "started", "staged", "promoting", "failed"
    ]


def test_partial_load_through_refresh_returns_zero_and_does_not_promote(
    monkeypatch, tmp_path,
) -> None:
    """The real one-button entry point: a load with skipped items must finish
    cleanly (return 0, no raised exception) and must NOT promote (status stayed
    'loading'), so partial data never overwrites live tables and LOAD.cmd shows
    a graceful RC=0 rather than a crash."""
    events = []
    swaps = []
    profile = SimpleNamespace(namespace_id="producer-ns")
    monkeypatch.setattr(runner, "_load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(runner, "sense_environment", lambda args: [])
    monkeypatch.setattr(runner, "build_configs", lambda args: (profile,))

    def partial_load(args, configs):
        # A partial (non-halting) load leaves durable status "loading".
        runner.save_state(args.duckdb_path, args.lock_wait_seconds, {"status": "loading"})
        return 0

    monkeypatch.setattr(runner, "run_multi_load", partial_load)
    monkeypatch.setattr(runner, "run_swap", lambda args: swaps.append(True) or 0)

    result = LoaderEngine(events.append).refresh(_request(tmp_path, promote=True))

    assert result == 0                 # graceful finish, no exception
    assert swaps == []                 # promotion was skipped
    assert "promoting" not in [e.event for e in events]
    assert events[-1].event == "partial"
    assert "resume" in events[-1].message.lower()


def test_process_lock_rejects_a_second_loader_for_same_warehouse(tmp_path) -> None:
    target = (tmp_path / "warehouse.duckdb").resolve()
    with _ProcessLock(target):
        with pytest.raises(LoaderAlreadyRunningError, match="already running"):
            with _ProcessLock(target):
                pass


def test_scheduler_command_is_absolute_and_contains_no_credentials(tmp_path) -> None:
    request = _request(
        tmp_path, promote=True, include_external=False,
        include_tables=("query_history", "query_text"),
    )
    command = build_loader_command(
        request, python_executable="python.exe", json_events=True,
    )
    joined = " ".join(command).lower()

    assert Path(command[1]).is_absolute()
    assert command[2] == "refresh"
    assert "--promote" in command
    assert "--skip-external" in command
    assert "--external-timeout-action" in command
    assert command.count("--table") == 2
    assert "--json-events" in command
    assert "username" not in joined
    assert "password" not in joined
    assert "secret" not in joined


def test_single_file_application_relaunches_itself_for_loader(monkeypatch, tmp_path) -> None:
    launcher = tmp_path / "redshift_analyzer_text.py"
    launcher.write_text("# launcher\n", encoding="utf-8")
    monkeypatch.setenv("REDSHIFT_ANALYZER_LAUNCH_PATH", str(launcher))

    command = build_loader_command(_request(tmp_path), python_executable="python.exe")

    assert command[:4] == ["python.exe", str(launcher.resolve()), "--loader", "refresh"]


def test_selected_refresh_plan_includes_external_table_metadata() -> None:
    args = SimpleNamespace(
        include_tables=("query_text", "query_history", "query_text"),
        include_external=True,
    )
    assert runner.selected_refresh_tables(args) == ("query_history", "query_text")

    args.include_tables = ("external_table_metadata",)
    args.include_external = False
    assert runner.selected_refresh_tables(args) == ("external_table_metadata",)

    args.include_tables = ("external_table_info_all",)
    args.include_external = False
    with pytest.raises(ValueError, match="at least one"):
        runner.selected_refresh_tables(args)

    args.include_tables = ("not_a_loader_table",)
    args.include_external = True
    with pytest.raises(ValueError, match="Unknown loader table"):
        runner.selected_refresh_tables(args)


def test_multi_loader_executes_only_the_selected_table_plan(monkeypatch, tmp_path) -> None:
    path = tmp_path / "selected.duckdb"
    duckdb.connect(str(path)).close()
    args = SimpleNamespace(
        duckdb_path=str(path), days=1.0, floor_seconds=300.0,
        floor_basis="execution_time", lock_wait_seconds=1, resume=False,
        include_external=True, include_tables=("query_history",),
    )
    config = SimpleNamespace(
        namespace_id="producer-ns", cluster_name="Producer",
        cluster_role="producer", cluster_ordinal=0, host="example",
        primary_database="dev", evidence_parent_limit=0,
        floor_basis="execution_time",
    )
    fetched = []
    monkeypatch.setattr(runner, "validate_primary_sources", lambda cfg: None)
    monkeypatch.setattr(runner, "table_sql", lambda cfg, table, target_ids=None: table)

    def fetch(_cfg, _database, sql, stage=""):
        fetched.append((sql, stage))
        return pd.DataFrame([{"query_id": 10, "execution_time": 600_000_000}])

    monkeypatch.setattr(runner, "fetch_frame", fetch)

    assert runner.run_multi_load(args, (config,)) == 0
    assert [sql for sql, _stage in fetched] == ["query_history"]
    con = duckdb.connect(str(path), read_only=True)
    try:
        tables = {
            row[0] for row in con.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        assert "query_history_tmp" in tables
        assert "query_text_tmp" not in tables
        state = dict(con.execute(
            "SELECT state_key, state_value FROM _tmp_refresh_state"
        ).fetchall())
        assert state["selected_tables"] == "query_history"
        assert state["status"] == "loaded"
    finally:
        con.close()


def test_consumer_with_no_qualifying_queries_completes_and_can_promote(
    monkeypatch, tmp_path,
) -> None:
    path = tmp_path / "zero-query-consumer.duckdb"
    duckdb.connect(str(path)).close()
    args = SimpleNamespace(
        duckdb_path=str(path),
        days=1.0,
        floor_seconds=30.0,
        floor_basis="execution_time",
        lock_wait_seconds=1,
        resume=False,
        include_external=False,
        include_tables=("query_history",),
    )
    producer = SimpleNamespace(
        namespace_id="producer-ns",
        cluster_name="Producer",
        cluster_role="producer",
        cluster_ordinal=0,
        host="producer.example",
        primary_database="dev",
    )
    consumer = SimpleNamespace(
        namespace_id="consumer-ns",
        cluster_name="FAR",
        cluster_role="consumer",
        cluster_ordinal=1,
        host="consumer.example",
        primary_database="dev",
    )
    events = []
    monkeypatch.setenv("INFRAREDSHIFT_PARALLEL_LOAD", "0")
    monkeypatch.setattr(runner, "validate_primary_sources", lambda cfg: None)
    monkeypatch.setattr(
        runner, "table_sql",
        lambda cfg, table_name, target_ids=None: table_name,
    )

    def fetch(cfg, _database, _sql, stage=""):
        if cfg.namespace_id == "consumer-ns":
            return pd.DataFrame(columns=["query_id", "execution_time"])
        return pd.DataFrame([{"query_id": 10, "execution_time": 60_000_000}])

    monkeypatch.setattr(runner, "fetch_frame", fetch)
    runner.set_progress_hook(lambda *event: events.append(event))
    try:
        assert runner.run_multi_load(args, (producer, consumer)) == 0
    finally:
        runner.set_progress_hook(None)

    consumer_events = [
        event for event in events
        if event[0] == "consumer-ns" and event[1] == "query_history"
    ]
    assert consumer_events[-1][2:4] == (0, 0)
    assert consumer_events[-1][-1] == runner.NO_QUALIFYING_QUERY_STATUS

    con = duckdb.connect(str(path), read_only=True)
    try:
        state = runner.read_state(con)
        assert state["status"] == "loaded"
        assert runner.staging_checkpoint_progress(
            con, state, ("query_history",)
        ) == (2, 2, [])
        checkpoint = con.execute(
            "SELECT source_rows, status FROM _tmp_namespace_refresh_state "
            "WHERE snapshot_id = ? AND namespace_id = 'consumer-ns' "
            "AND table_name = 'query_history'",
            [state["snapshot_id"]],
        ).fetchone()
        assert checkpoint == (0, "complete")
    finally:
        con.close()


def test_external_table_metadata_loads_once_from_producer(monkeypatch, tmp_path) -> None:
    path = tmp_path / "external-metadata.duckdb"
    duckdb.connect(str(path)).close()
    args = SimpleNamespace(
        duckdb_path=str(path),
        days=1.0,
        floor_seconds=300.0,
        floor_basis="execution_time",
        lock_wait_seconds=1,
        resume=False,
        include_external=False,
        include_tables=("external_table_metadata",),
    )
    producer = SimpleNamespace(
        namespace_id="producer-ns",
        cluster_name="Producer",
        cluster_role="producer",
        cluster_ordinal=0,
        host="producer.example",
        primary_database="dev",
        evidence_parent_limit=0,
        floor_basis="execution_time",
    )
    consumer = SimpleNamespace(
        namespace_id="consumer-ns",
        cluster_name="Consumer",
        cluster_role="consumer",
        cluster_ordinal=1,
        host="consumer.example",
        primary_database="dev",
        evidence_parent_limit=0,
        floor_basis="execution_time",
    )
    fetched = []

    def unexpected_workload_validation(_cfg):
        raise AssertionError(
            "Producer external metadata must not depend on workload-view validation"
        )

    monkeypatch.setattr(
        runner, "validate_primary_sources", unexpected_workload_validation,
    )
    monkeypatch.setattr(runner, "resolve_table_databases", lambda cfg: ("dev",))

    def fetch(cfg, database, sql, stage=""):
        fetched.append((cfg.namespace_id, database, sql, stage))
        return pd.DataFrame([{
            "external_table_key": "dev.spectrum.sales",
            "redshift_database_name": "dev",
            "schema_name": "spectrum",
            "table_name": "sales",
            "column_name": "sale_date",
            "data_type": "date",
            "column_number": 4,
            "partition_key_ordinal": 1,
            "is_nullable": "true",
        }])

    monkeypatch.setattr(runner, "fetch_frame", fetch)

    assert runner.run_multi_load(args, (producer, consumer)) == 0
    assert len(fetched) == 1
    assert fetched[0][0] == "producer-ns"
    assert "FROM svv_external_columns" in fetched[0][2]
    assert "columnname" in fetched[0][2]

    con = duckdb.connect(str(path), read_only=True)
    try:
        assert con.execute(
            "SELECT COUNT(*) FROM external_table_metadata_tmp"
        ).fetchone()[0] == 1
        state = dict(
            con.execute(
                "SELECT state_key, state_value FROM _tmp_refresh_state"
            ).fetchall()
        )
        assert state["status"] == "loaded"
    finally:
        con.close()


def test_later_catalog_allocation_error_skips_whole_producer_entry(
    monkeypatch, tmp_path,
) -> None:
    path = tmp_path / "producer-later-query-memory-error.duckdb"
    duckdb.connect(str(path)).close()
    args = SimpleNamespace(
        duckdb_path=str(path),
        days=1.0,
        floor_seconds=300.0,
        floor_basis="execution_time",
        lock_wait_seconds=1,
        resume=True,
        include_external=False,
        include_tables=("svv_table_info_all", "external_table_metadata"),
    )
    producer = SimpleNamespace(
        namespace_id="producer-ns",
        cluster_name="Producer",
        cluster_role="producer",
        cluster_ordinal=0,
        host="producer.example",
        primary_database="dev",
        evidence_parent_limit=0,
        floor_basis="execution_time",
    )
    events = []
    attempts = []
    monkeypatch.setattr(
        runner,
        "validate_primary_sources",
        lambda _cfg: (_ for _ in ()).throw(
            AssertionError("catalog-only retry must skip workload validation")
        ),
    )
    monkeypatch.setattr(
        runner, "resolve_table_databases",
        lambda _cfg: ("datalake_cl", "enterprise_datawarehouse"),
    )

    def fetch(_cfg, database, sql, stage=""):
        attempts.append((database, sql, stage))
        if database == "datalake_cl" and sql == runner.TABLE_INFO_SQL:
            raise RuntimeError(
                "Invalid memory allocation request Size 1073742848"
            )
        if sql == runner.TABLE_INFO_SQL:
            return pd.DataFrame(columns=["database", "schema", "table"])
        assert sql == runner.EXTERNAL_TABLE_METADATA_SQL
        return pd.DataFrame([{
            "external_table_key": f"{database}.spectrum.sales",
            "redshift_database_name": database,
            "schema_name": "spectrum",
            "table_name": "sales",
            "column_name": "sale_date",
            "data_type": "date",
            "column_number": 4,
            "partition_key_ordinal": 1,
            "is_nullable": "true",
        }])

    monkeypatch.setattr(runner, "fetch_frame", fetch)
    runner.set_progress_hook(lambda *event: events.append(event))
    try:
        assert runner.run_multi_load(args, (producer,)) == 0
    finally:
        runner.set_progress_hook(None)

    assert [(database, sql) for database, sql, _stage in attempts] == [
        ("datalake_cl", runner.EXTERNAL_TABLE_METADATA_SQL),
        ("datalake_cl", runner.TABLE_INFO_SQL),
        ("enterprise_datawarehouse", runner.EXTERNAL_TABLE_METADATA_SQL),
        ("enterprise_datawarehouse", runner.TABLE_INFO_SQL),
    ]
    assert any(
        event[1] == "svv_table_info_all"
        and event[-1] == "Skipped datalake_cl — unavailable or data share"
        for event in events
    )

    con = duckdb.connect(str(path), read_only=True)
    try:
        state = runner.read_state(con)
        assert state["status"] == "loaded"
        assert state["failure_count"] == "0"
        checkpoints = {
            (namespace, table)
            for namespace, table in con.execute(
                "SELECT namespace_id, table_name "
                "FROM _tmp_namespace_refresh_state "
                "WHERE status = 'complete'"
            ).fetchall()
        }
        assert ("producer-ns", "external_table_metadata") in checkpoints
        assert ("producer-ns", "svv_table_info_all") in checkpoints
        assert con.execute(
            "SELECT COUNT(*) FROM external_table_metadata_tmp"
        ).fetchone()[0] == 1
        assert con.execute(
            "SELECT DISTINCT redshift_database_name "
            "FROM external_table_metadata_tmp"
        ).fetchone()[0] == "enterprise_datawarehouse"
    finally:
        con.close()

    report = json.loads(
        (path.parent / "load_report.json").read_text(encoding="utf-8")
    )
    assert report["failures"] == []


def test_missing_fixed_database_on_consumer_is_zero_row_success(
    monkeypatch, tmp_path,
) -> None:
    path = tmp_path / "consumer-without-edw.duckdb"
    duckdb.connect(str(path)).close()
    args = SimpleNamespace(
        duckdb_path=str(path),
        days=1.0,
        floor_seconds=300.0,
        floor_basis="execution_time",
        lock_wait_seconds=1,
        resume=True,
        include_external=False,
        include_tables=(
            "svv_table_info_all",
            "view_definitions",
            "procedure_definitions",
            "external_table_metadata",
        ),
    )
    producer = SimpleNamespace(
        namespace_id="producer-ns",
        cluster_name="Producer",
        cluster_role="producer",
        cluster_ordinal=0,
        host="producer.example",
        primary_database="dev",
        evidence_parent_limit=0,
        floor_basis="execution_time",
    )
    consumer = SimpleNamespace(
        namespace_id="consumer-without-edw",
        cluster_name="Consumer without EDW",
        cluster_role="consumer",
        cluster_ordinal=1,
        host="consumer.example",
        primary_database="dev",
        evidence_parent_limit=0,
        floor_basis="execution_time",
    )
    attempts = []
    events = []
    monkeypatch.setattr(runner, "_redshift_read_worker_count", lambda _configs: 1)
    monkeypatch.setattr(
        runner,
        "validate_primary_sources",
        lambda _cfg: (_ for _ in ()).throw(
            AssertionError("catalog-only load must skip workload validation")
        ),
    )
    monkeypatch.setattr(
        runner, "resolve_table_databases",
        lambda _cfg: ("enterprise_datawarehouse",),
    )

    def fetch(cfg, database, sql, stage=""):
        attempts.append((cfg.namespace_id, database, sql, stage))
        if cfg.cluster_role == "consumer":
            raise RuntimeError(
                "{'S': 'FATAL', 'C': '3D000', "
                "'M': 'database \"enterprise_datawarehouse\" does not exist'}"
            )
        if sql == runner.EXTERNAL_TABLE_METADATA_SQL:
            return pd.DataFrame([{
                "external_table_key": "edw.spectrum.sales",
                "redshift_database_name": "enterprise_datawarehouse",
                "schema_name": "spectrum",
                "table_name": "sales",
                "column_name": "sale_date",
                "data_type": "date",
                "column_number": 4,
                "partition_key_ordinal": 1,
                "is_nullable": "true",
            }])
        return pd.DataFrame()

    monkeypatch.setattr(runner, "fetch_frame", fetch)
    runner.set_progress_hook(lambda *event: events.append(event))
    try:
        assert runner.run_multi_load(args, (producer, consumer)) == 0
    finally:
        runner.set_progress_hook(None)

    consumer_attempts = [
        attempt for attempt in attempts if attempt[0] == consumer.namespace_id
    ]
    assert len(consumer_attempts) == 1
    assert consumer_attempts[0][2] == runner.TABLE_INFO_SQL
    consumer_catalog_events = [
        event for event in events
        if event[0] == consumer.namespace_id
        and event[1] in {
            "svv_table_info_all", "view_definitions", "procedure_definitions",
        }
    ]
    assert {
        event[1] for event in consumer_catalog_events
        if event[-1] == runner.CONSUMER_CATALOG_DATABASE_NOT_PRESENT_STATUS
    } == {
        "svv_table_info_all", "view_definitions", "procedure_definitions",
    }

    con = duckdb.connect(str(path), read_only=True)
    try:
        state = runner.read_state(con)
        assert state["status"] == "loaded"
        assert state["failure_count"] == "0"
        checkpoints = {
            (namespace, table, rows)
            for namespace, table, rows in con.execute(
                "SELECT namespace_id, table_name, source_rows "
                "FROM _tmp_namespace_refresh_state "
                "WHERE snapshot_id = ? AND status = 'complete'",
                [state["snapshot_id"]],
            ).fetchall()
        }
        assert {
            (consumer.namespace_id, table, 0)
            for table in (
                "svv_table_info_all",
                "view_definitions",
                "procedure_definitions",
            )
        }.issubset(checkpoints)
        assert (
            producer.namespace_id,
            "external_table_metadata",
            1,
        ) in checkpoints
    finally:
        con.close()

    report = json.loads(
        (path.parent / "load_report.json").read_text(encoding="utf-8")
    )
    assert report["failures"] == []


def test_missing_fixed_database_on_producer_still_blocks_external_metadata(
    monkeypatch, tmp_path,
) -> None:
    path = tmp_path / "producer-without-edw.duckdb"
    duckdb.connect(str(path)).close()
    args = SimpleNamespace(
        duckdb_path=str(path),
        days=1.0,
        floor_seconds=300.0,
        floor_basis="execution_time",
        lock_wait_seconds=1,
        resume=True,
        include_external=False,
        include_tables=("external_table_metadata",),
    )
    producer = SimpleNamespace(
        namespace_id="producer-ns",
        cluster_name="Producer",
        cluster_role="producer",
        cluster_ordinal=0,
        host="producer.example",
        primary_database="dev",
        evidence_parent_limit=0,
        floor_basis="execution_time",
    )
    monkeypatch.setattr(
        runner, "resolve_table_databases",
        lambda _cfg: ("enterprise_datawarehouse",),
    )
    monkeypatch.setattr(
        runner,
        "fetch_frame",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError(
            "{'S': 'FATAL', 'C': '3D000', "
            "'M': 'database \"enterprise_datawarehouse\" does not exist'}"
        )),
    )

    assert runner.run_multi_load(args, (producer,)) == 0

    con = duckdb.connect(str(path), read_only=True)
    try:
        state = runner.read_state(con)
        assert state["status"] == "loading"
        assert state["failure_count"] == "1"
        checkpoint = con.execute(
            "SELECT COUNT(*) FROM _tmp_namespace_refresh_state "
            "WHERE snapshot_id = ? AND namespace_id = ? "
            "AND table_name = 'external_table_metadata' "
            "AND status = 'complete'",
            [state["snapshot_id"], producer.namespace_id],
        ).fetchone()[0]
        assert checkpoint == 0
    finally:
        con.close()

    report = json.loads(
        (path.parent / "load_report.json").read_text(encoding="utf-8")
    )
    assert [item["table"] for item in report["failures"]] == [
        "external_table_metadata"
    ]


def test_unavailable_producer_entry_is_skipped_and_next_database_loads(
    monkeypatch, tmp_path,
) -> None:
    path = tmp_path / "producer-data-share-skip.duckdb"
    duckdb.connect(str(path)).close()
    args = SimpleNamespace(
        duckdb_path=str(path),
        days=1.0,
        floor_seconds=300.0,
        floor_basis="execution_time",
        lock_wait_seconds=1,
        resume=True,
        include_external=False,
        include_tables=("external_table_metadata",),
    )
    producer = SimpleNamespace(
        namespace_id="producer-ns",
        cluster_name="Producer",
        cluster_role="producer",
        cluster_ordinal=0,
        host="producer.example",
        primary_database="dev",
        evidence_parent_limit=0,
        floor_basis="execution_time",
    )
    attempts = []
    events = []
    monkeypatch.setattr(
        runner,
        "resolve_table_databases",
        lambda _cfg: ("datalake_cl", "enterprise_datawarehouse"),
    )

    def fetch(_cfg, database, _sql, stage=""):
        attempts.append((database, stage))
        if database == "datalake_cl":
            raise RuntimeError("database cannot be opened")
        return pd.DataFrame([{
            "external_table_key": "edw.spectrum.sales",
            "redshift_database_name": "enterprise_datawarehouse",
            "schema_name": "spectrum",
            "table_name": "sales",
            "column_name": "sale_date",
            "data_type": "date",
            "column_number": 4,
            "partition_key_ordinal": 1,
            "is_nullable": "true",
        }])

    monkeypatch.setattr(runner, "fetch_frame", fetch)
    runner.set_progress_hook(lambda *event: events.append(event))
    try:
        assert runner.run_multi_load(args, (producer,)) == 0
    finally:
        runner.set_progress_hook(None)

    assert [database for database, _stage in attempts] == [
        "datalake_cl",
        "enterprise_datawarehouse",
    ]
    assert any(
        event[1] == "external_table_metadata"
        and event[-1]
        == "Skipped datalake_cl — unavailable or data share"
        for event in events
    )
    assert events[-1][-1] == "Staged in DuckDB"

    con = duckdb.connect(str(path), read_only=True)
    try:
        state = runner.read_state(con)
        assert state["status"] == "loaded"
        assert state["failure_count"] == "0"
        assert con.execute(
            "SELECT COUNT(*) FROM external_table_metadata_tmp"
        ).fetchone()[0] == 1
    finally:
        con.close()

    report = json.loads(
        (path.parent / "load_report.json").read_text(encoding="utf-8")
    )
    assert report["failures"] == []


def test_partial_promotion_preserves_unchecked_live_tables(tmp_path) -> None:
    path = tmp_path / "partial.duckdb"
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE TABLE query_history (snapshot_id VARCHAR, marker VARCHAR)")
        con.execute("INSERT INTO query_history VALUES ('old', 'old history')")
        con.execute("CREATE TABLE query_text (snapshot_id VARCHAR, marker VARCHAR)")
        con.execute("INSERT INTO query_text VALUES ('old', 'live text must remain')")
        con.execute("CREATE TABLE query_history_tmp (snapshot_id VARCHAR, marker VARCHAR)")
        con.execute("INSERT INTO query_history_tmp VALUES ('new', 'new history')")
        # Simulate debris from an older wider staging run. The selected plan
        # must prevent it from replacing the unchecked live table.
        con.execute("CREATE TABLE query_text_tmp (snapshot_id VARCHAR, marker VARCHAR)")
        con.execute("INSERT INTO query_text_tmp VALUES ('stale', 'stale text')")
    finally:
        con.close()
    runner.save_state(path, 1, {
        "snapshot_id": "new",
        "status": "loaded",
        "label": "partial",
        "selected_tables": "query_history",
    })
    args = SimpleNamespace(
        duckdb_path=str(path), lock_wait_seconds=1, no_backup=True,
    )

    assert runner.run_swap(args) == 0

    con = duckdb.connect(str(path), read_only=True)
    try:
        assert con.execute("SELECT marker FROM query_history").fetchone()[0] == "new history"
        assert con.execute("SELECT marker FROM query_text").fetchone()[0] == "live text must remain"
    finally:
        con.close()


def test_direct_promotion_refuses_incomplete_staging(tmp_path) -> None:
    path = tmp_path / "incomplete.duckdb"
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE TABLE query_history_tmp (snapshot_id VARCHAR)")
        con.execute("INSERT INTO query_history_tmp VALUES ('partial')")
    finally:
        con.close()
    runner.save_state(path, 1, {
        "snapshot_id": "partial",
        "status": "loading",
        "selected_tables": "query_history",
    })

    with pytest.raises(SystemExit, match="not complete"):
        runner.run_swap(SimpleNamespace(
            duckdb_path=str(path), lock_wait_seconds=1, no_backup=True,
        ))


def test_promotion_recovers_complete_checkpointed_staging(tmp_path) -> None:
    path = tmp_path / "recoverable.duckdb"
    con = duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE query_history_tmp "
            "(snapshot_id VARCHAR, namespace_id VARCHAR, marker VARCHAR)"
        )
        con.execute(
            "INSERT INTO query_history_tmp VALUES "
            "('snap-recover', 'producer-ns', 'new')"
        )
        con.execute(
            "CREATE TABLE _tmp_refresh_state "
            "(state_key VARCHAR, state_value VARCHAR)"
        )
        con.executemany(
            "INSERT INTO _tmp_refresh_state VALUES (?, ?)",
            [
                ("snapshot_id", "snap-recover"),
                ("status", "loading"),
                ("selected_tables", "query_history"),
                ("namespace_ids", "producer-ns"),
                ("failure_count", "0"),
            ],
        )
        con.execute(
            "CREATE TABLE _tmp_namespace_refresh_state "
            "(snapshot_id VARCHAR, namespace_id VARCHAR, table_name VARCHAR, "
            "source_rows BIGINT, status VARCHAR, completed_at TIMESTAMP)"
        )
        con.execute(
            "INSERT INTO _tmp_namespace_refresh_state VALUES "
            "('snap-recover', 'producer-ns', 'query_history', 1, 'complete', CURRENT_TIMESTAMP)"
        )
        con.execute(
            "CREATE TABLE _tmp_snapshot_cluster_runs "
            "(snapshot_id VARCHAR, namespace_id VARCHAR, cluster_role VARCHAR, "
            "cluster_name VARCHAR, cluster_host VARCHAR, primary_database VARCHAR, "
            "captured_at TIMESTAMP)"
        )
        con.execute(
            "INSERT INTO _tmp_snapshot_cluster_runs VALUES "
            "('snap-recover', 'producer-ns', 'producer', 'Producer', '', 'dev', CURRENT_TIMESTAMP)"
        )
    finally:
        con.close()

    events = []
    result = LoaderEngine(events.append).promote(
        path,
        backup_before_promote=False,
        lock_wait_seconds=1,
    )

    assert result == 0
    assert events[-1].event == "completed"
    con = duckdb.connect(str(path), read_only=True)
    try:
        assert con.execute(
            "SELECT marker FROM query_history"
        ).fetchone()[0] == "new"
        tables = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        assert "_tmp_refresh_state" not in tables
    finally:
        con.close()


def test_backup_fails_closed_when_duckdb_checkpoint_fails(monkeypatch, tmp_path) -> None:
    copied = []

    class BrokenConnection:
        def execute(self, _sql):
            raise RuntimeError("checkpoint unavailable")

        def close(self):
            pass

    monkeypatch.setattr(runner, "open_duck", lambda path, wait: BrokenConnection())
    monkeypatch.setattr(runner.shutil, "copy2", lambda *args: copied.append(args))

    with pytest.raises(SystemExit, match="CHECKPOINT failed"):
        runner._backup_file(tmp_path / "warehouse.duckdb", 1)
    assert copied == []


def test_promote_command_and_engine_require_a_completed_staging_state(
    tmp_path,
) -> None:
    path = tmp_path / "not-ready.duckdb"
    duckdb.connect(str(path)).close()
    events = []

    with pytest.raises(RuntimeError, match="No completed staged load"):
        LoaderEngine(events.append).promote(path, backup_before_promote=False)
    assert [event.event for event in events] == ["promoting", "failed"]

    command = build_promote_command(
        path, python_executable="python.exe", json_events=True,
    )
    assert command[:3] == ["python.exe", str(loader_script_path()), "promote"]
    assert "--json-events" in command


def test_external_timeout_decision_crosses_the_process_engine_contract(
    monkeypatch, tmp_path,
) -> None:
    events = []
    decisions = []
    profile = SimpleNamespace(namespace_id="producer-ns")
    monkeypatch.setattr(runner, "_load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(runner, "sense_environment", lambda args: [])
    monkeypatch.setattr(runner, "build_configs", lambda args: (profile,))

    def load(_args, configs):
        decision = configs[0].timeout_decision_callback("external catalog [dev]", "statement timeout")
        decisions.append(decision)
        return 0

    monkeypatch.setattr(runner, "run_multi_load", load)
    request = _request(tmp_path, external_timeout_action="ask")
    engine = LoaderEngine(
        events.append,
        timeout_decider=lambda stage, error: "retry",
    )

    assert engine.refresh(request) == 0
    assert decisions == ["continue"]
    assert any(event.event == "external_timeout" for event in events)


def test_loader_events_redact_credentials() -> None:
    events = []
    engine = LoaderEngine(events.append)

    engine._emit("failed", "connection failed password=hunter2 username=someone")

    assert "hunter2" not in events[0].message
    assert "someone" not in events[0].message
    assert events[0].message.count("<redacted>") == 2


def test_runner_profile_resolves_credentials_from_protected_session(
    monkeypatch,
) -> None:
    protected = {
        "REDSHIFT_PRODUCER_HOST": "producer.example",
        "REDSHIFT_PRODUCER_USER": "db-user",
        "REDSHIFT_PRODUCER_PASSWORD": "db-password",
    }
    for key in protected:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("REDSHIFT_ENABLED", "true")
    monkeypatch.setenv("REDSHIFT_NAMESPACE", "producer-ns")
    monkeypatch.setattr(
        secrets_store, "session_secret",
        lambda name, default=None: protected.get(name, default),
    )
    args = SimpleNamespace(
        days=7.0, floor_seconds=300.0, floor_basis="execution_time",
    )

    profile = runner._profile_config(args, "REDSHIFT_PRODUCER", "producer")

    assert profile.host == "producer.example"
    assert profile.user == "db-user"
    assert profile.password == "db-password"


def test_direct_runner_unlocks_saved_local_credentials(monkeypatch) -> None:
    unlocks = []
    monkeypatch.setattr(secrets_store, "session_secrets", lambda: {})
    monkeypatch.setattr(
        secrets_store,
        "unlock_scheduled_secrets_session",
        lambda: unlocks.append("saved credentials unlocked"),
    )

    runner._load_dotenv_if_present()

    assert unlocks == ["saved credentials unlocked"]


def test_runner_discovers_consumer_stored_only_in_protected_session(
    monkeypatch,
) -> None:
    protected = {
        "REDSHIFT_CONSUMER_7_HOST": "consumer-seven.example",
        "REDSHIFT_CONSUMER_7_USER": "db-user",
        "REDSHIFT_CONSUMER_7_PASSWORD": "db-password",
    }
    monkeypatch.setenv("REDSHIFT_PRODUCER_ENABLED", "false")
    monkeypatch.setenv("REDSHIFT_CONSUMER_7_NAMESPACE_ID", "consumer-seven-ns")
    monkeypatch.setattr(
        secrets_store,
        "session_secrets",
        lambda: dict(protected),
    )
    monkeypatch.setattr(
        secrets_store,
        "session_secret",
        lambda name, default=None: protected.get(name, default),
    )
    args = SimpleNamespace(
        days=7.0, floor_seconds=300.0, floor_basis="execution_time",
    )

    profiles = runner.build_configs(args)

    assert [profile.namespace_id for profile in profiles] == ["consumer-seven-ns"]
    assert profiles[0].host == "consumer-seven.example"


def test_portable_manifest_excludes_stale_protected_consumer(
    monkeypatch,
) -> None:
    protected = {}
    monkeypatch.setenv(
        "INFRAREDSHIFT_ACTIVE_PROFILE_PREFIXES",
        "REDSHIFT_CONSUMER_1,REDSHIFT_CONSUMER_2,REDSHIFT_CONSUMER_3",
    )
    monkeypatch.setenv("REDSHIFT_PRODUCER_ENABLED", "false")
    for ordinal, name in (
        (1, "FAR"),
        (2, "Commercial"),
        (3, "Consumer"),
        (4, "Stale Consumer"),
    ):
        prefix = f"REDSHIFT_CONSUMER_{ordinal}"
        monkeypatch.setenv(f"{prefix}_ENABLED", "true")
        monkeypatch.setenv(f"{prefix}_NAMESPACE_ID", f"consumer-{ordinal}-ns")
        monkeypatch.setenv(f"{prefix}_DISPLAY_NAME", name)
        protected[f"{prefix}_HOST"] = f"consumer-{ordinal}.example"
        protected[f"{prefix}_USER"] = "db-user"
        protected[f"{prefix}_PASSWORD"] = "db-password"
    monkeypatch.setattr(
        secrets_store, "session_secrets", lambda: dict(protected)
    )
    monkeypatch.setattr(
        secrets_store,
        "session_secret",
        lambda name, default=None: protected.get(name, default),
    )
    args = SimpleNamespace(
        days=7.0, floor_seconds=30.0, floor_basis="execution_time",
    )

    profiles = runner.build_configs(args)

    assert [profile.cluster_name for profile in profiles] == [
        "FAR",
        "Commercial",
        "Consumer",
    ]
    assert all(profile.cluster_ordinal != 4 for profile in profiles)


def test_entry_script_help_runs_from_an_unrelated_working_directory(tmp_path) -> None:
    completed = subprocess.run(
        [sys.executable, str(loader_script_path()), "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "Recoverable Infraredshift" in completed.stdout
    assert "refresh" in completed.stdout
