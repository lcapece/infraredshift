"""Behavioral tests for the standalone Infraredshift Loader window."""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
from PySide6.QtWidgets import QApplication

from analyzer.loader.engine import build_loader_gui_command
from analyzer.loader.gui import (
    LoaderWindow,
    read_load_failures,
    read_no_qualifying_query_namespaces,
    read_staging_checkpoint_details,
    read_staging_checkpoint_progress,
    read_staging_state,
)


_APP = QApplication.instance() or QApplication([])


def _staged_warehouse(path: Path, status: str, *, days: str | None = None) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE TABLE _tmp_refresh_state (state_key VARCHAR, state_value VARCHAR)")
        con.execute(
            "INSERT INTO _tmp_refresh_state VALUES ('status', ?), ('snapshot_id', 'snap-1')",
            [status],
        )
        if days is not None:
            con.execute(
                "INSERT INTO _tmp_refresh_state VALUES ('days', ?)",
                [days],
            )
    finally:
        con.close()


def test_window_offers_promotion_only_for_a_completed_staging(tmp_path) -> None:
    ready = tmp_path / "ready.duckdb"
    _staged_warehouse(ready, "loaded")
    window = LoaderWindow(str(ready))
    window.refresh_idle_state()
    assert window._promote.isEnabled()
    assert window._primary.text() == "Start Safe Load"
    window.deleteLater()

    interrupted = tmp_path / "interrupted.duckdb"
    _staged_warehouse(interrupted, "loading")
    window = LoaderWindow(str(interrupted))
    window.refresh_idle_state()
    assert window._promote.isEnabled()
    assert window._primary.text() == "Resume Safe Load"
    assert window._promote.text() == "Validate & Promote"
    window.deleteLater()


def test_missing_warehouse_reads_as_no_staging_without_creating_it(tmp_path) -> None:
    path = tmp_path / "absent.duckdb"
    assert read_staging_state(path) == {}
    assert not path.exists()


def test_resume_restores_the_staged_capture_window(tmp_path) -> None:
    path = tmp_path / "resume-window.duckdb"
    _staged_warehouse(path, "loading", days="2")

    window = LoaderWindow(str(path), embedded=True)
    window.refresh_idle_state()

    assert window._days.value() == 2
    assert window._request().days == 2
    window.deleteLater()


def test_staging_progress_counts_required_namespace_checkpoints(tmp_path) -> None:
    path = tmp_path / "checkpoint-progress.duckdb"
    con = duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE _tmp_refresh_state "
            "(state_key VARCHAR, state_value VARCHAR)"
        )
        con.executemany(
            "INSERT INTO _tmp_refresh_state VALUES (?, ?)",
            [
                ("status", "loading"),
                ("snapshot_id", "snap-progress"),
                ("namespace_ids", "producer-ns,consumer-ns"),
                ("selected_tables", "query_history,external_table_metadata"),
            ],
        )
        con.execute(
            "CREATE TABLE _tmp_namespace_refresh_state "
            "(snapshot_id VARCHAR, namespace_id VARCHAR, table_name VARCHAR, "
            "source_rows BIGINT, status VARCHAR, completed_at TIMESTAMP)"
        )
        con.executemany(
            "INSERT INTO _tmp_namespace_refresh_state VALUES "
            "(?, ?, ?, 1, 'complete', CURRENT_TIMESTAMP)",
            [
                ("snap-progress", "producer-ns", "query_history"),
                ("snap-progress", "consumer-ns", "query_history"),
            ],
        )
        con.execute(
            "CREATE TABLE _tmp_snapshot_cluster_runs "
            "(snapshot_id VARCHAR, namespace_id VARCHAR, cluster_role VARCHAR)"
        )
        con.execute(
            "INSERT INTO _tmp_snapshot_cluster_runs VALUES "
            "('snap-progress', 'producer-ns', 'producer')"
        )
    finally:
        con.close()

    state = read_staging_state(path)

    assert read_staging_checkpoint_progress(path, state) == (2, 3)
    window = LoaderWindow(str(path), embedded=True)
    window.refresh_idle_state()
    assert window._progress.value() == 67
    window.deleteLater()


def test_partial_state_shows_exact_external_metadata_retry_and_error(
    monkeypatch, tmp_path,
) -> None:
    path = tmp_path / "external-retry.duckdb"
    report_path = tmp_path / "load_report.txt"
    con = duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE _tmp_refresh_state "
            "(state_key VARCHAR, state_value VARCHAR)"
        )
        con.executemany(
            "INSERT INTO _tmp_refresh_state VALUES (?, ?)",
            [
                ("status", "loading"),
                ("snapshot_id", "snap-external"),
                ("namespace_ids", "producer-ns"),
                (
                    "selected_tables",
                    "svv_table_info_all,external_table_metadata",
                ),
                ("failure_count", "1"),
                ("load_report", str(report_path)),
            ],
        )
        con.execute(
            "CREATE TABLE _tmp_namespace_refresh_state "
            "(snapshot_id VARCHAR, namespace_id VARCHAR, table_name VARCHAR, "
            "source_rows BIGINT, status VARCHAR, completed_at TIMESTAMP)"
        )
        con.execute(
            "INSERT INTO _tmp_namespace_refresh_state VALUES "
            "('snap-external', 'producer-ns', 'svv_table_info_all', "
            "13, 'complete', CURRENT_TIMESTAMP)"
        )
        con.execute(
            "CREATE TABLE _tmp_snapshot_cluster_runs "
            "(snapshot_id VARCHAR, namespace_id VARCHAR, cluster_role VARCHAR)"
        )
        con.execute(
            "INSERT INTO _tmp_snapshot_cluster_runs VALUES "
            "('snap-external', 'producer-ns', 'producer')"
        )
    finally:
        con.close()
    (tmp_path / "load_report.json").write_text(
        json.dumps({
            "snapshot_id": "snap-external",
            "failures": [{
                "namespace_id": "producer-ns",
                "table": "external_table_metadata",
                "scope": "catalog",
                "error_type": "RuntimeError",
                "error": "permission denied for svv_external_columns",
            }],
        }),
        encoding="utf-8",
    )

    state = read_staging_state(path)
    assert read_staging_checkpoint_details(path, state) == (
        1,
        2,
        ["external_table_metadata: producer-ns"],
    )
    failures = read_load_failures(path, state)
    assert failures[0]["table"] == "external_table_metadata"

    monkeypatch.setattr(
        LoaderWindow,
        "_source_profiles",
        lambda _self: [{
            "prefix": "REDSHIFT_PRODUCER",
            "role": "Producer",
            "ordinal": 0,
            "friendly": "Main Warehouse",
            "namespace": "producer-ns",
            "enabled": True,
            "ready": True,
            "status": "Ready",
        }],
    )
    window = LoaderWindow(str(path), embedded=True)
    window.refresh_idle_state()

    assert "1 checkpoint" in window._status.text()
    assert "external_table_metadata: producer-ns" in window._status_detail.text()
    assert "permission denied for svv_external_columns" in (
        window._status_detail.text()
    )
    metadata_item = window._table_items[
        ("producer-ns", "external_table_metadata")
    ]
    assert "Retry required" in metadata_item.text(2)
    assert window._primary.text() == "Resume Safe Load"
    window.deleteLater()


def test_progress_events_drive_the_namespace_tree_and_bar(tmp_path) -> None:
    window = LoaderWindow(str(tmp_path / "loader.duckdb"))
    window.handle_event({
        "event": "progress", "message": "Staged in DuckDB",
        "namespace_id": "producer-ns", "table_name": "query_history",
        "duckdb_rows": 1200, "completed": 3, "total": 12,
    })
    window.handle_event({
        "event": "progress", "message": "Loading from Redshift",
        "namespace_id": "consumer-ns", "table_name": "query_text",
        "duckdb_rows": 0, "completed": 4, "total": 12,
    })

    producer = window._namespace_items["producer-ns"]
    assert producer.child(0).text(0) == "query_history"
    assert producer.child(0).text(1) == "1,200"
    assert producer.child(0).text(2) == "Staged in DuckDB"
    assert window._progress.value() == 33
    window.deleteLater()


def test_zero_qualifying_queries_is_yellow_completed_success(tmp_path) -> None:
    window = LoaderWindow(str(tmp_path / "loader.duckdb"))
    window.handle_event({
        "event": "progress",
        "message": "Complete — no qualifying queries",
        "namespace_id": "consumer-ns",
        "table_name": "query_history",
        "source_rows": 0,
        "duckdb_rows": 0,
        "completed": 5,
        "total": 10,
    })

    consumer = window._namespace_items["consumer-ns"]
    query_history = consumer.child(0)
    assert query_history.text(1) == "0"
    assert query_history.text(2) == "Complete — no qualifying queries"
    assert consumer.text(2) == "Success — no qualifying queries"
    assert window._progress.property("emptySuccess") == "true"
    assert window._progress.value() == 50
    window.deleteLater()


def test_consumer_without_fixed_catalog_database_is_yellow_completed_success(
    tmp_path,
) -> None:
    window = LoaderWindow(str(tmp_path / "loader.duckdb"))
    status = "Complete — enterprise_datawarehouse not present on this consumer"
    window.handle_event({
        "event": "progress",
        "message": status,
        "namespace_id": "consumer-ns",
        "table_name": "svv_table_info_all",
        "source_rows": 0,
        "duckdb_rows": 0,
        "completed": 6,
        "total": 10,
    })

    table_info = window._table_items[
        ("consumer-ns", "svv_table_info_all")
    ]
    assert table_info.text(1) == "0"
    assert table_info.text(2) == status
    assert table_info.background(0).color().name() == "#fff2b2"
    assert window._progress.property("emptySuccess") == "true"
    assert window._progress.value() == 60
    window.deleteLater()


def test_zero_query_success_is_recovered_from_durable_checkpoint(tmp_path) -> None:
    path = tmp_path / "zero-query.duckdb"
    con = duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE _tmp_namespace_refresh_state "
            "(snapshot_id VARCHAR, namespace_id VARCHAR, table_name VARCHAR, "
            "source_rows BIGINT, status VARCHAR, completed_at TIMESTAMP)"
        )
        con.execute(
            "INSERT INTO _tmp_namespace_refresh_state VALUES "
            "('snap-zero', 'consumer-ns', 'query_history', 0, 'complete', "
            "CURRENT_TIMESTAMP)"
        )
    finally:
        con.close()

    assert read_no_qualifying_query_namespaces(
        path, {"snapshot_id": "snap-zero"}
    ) == {"consumer-ns"}


def test_embedded_loader_exposes_credentials_and_all_sources(
    monkeypatch, tmp_path,
) -> None:
    from analyzer.secrets_store import set_session_secret

    # Exercise the legacy environment-only path; the manifest-specific
    # behavior is covered separately below.
    monkeypatch.setenv("REDSHIFT_ANALYZER_LAUNCH_DIR", str(tmp_path))
    monkeypatch.setenv(
        "REDSHIFT_ANALYZER_PROFILE_PATH",
        str(tmp_path / "missing-profiles.json"),
    )
    monkeypatch.setenv("REDSHIFT_ENABLED", "true")
    monkeypatch.setenv("REDSHIFT_NAMESPACE", "producer-ns")
    monkeypatch.setenv("REDSHIFT_PRODUCER_FRIENDLY", "Main Warehouse")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_ENABLED", "true")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_NAMESPACE_ID", "consumer-ns")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_FRIENDLY", "FAR")
    monkeypatch.setenv("REDSHIFT_CONSUMER_2_ENABLED", "true")
    monkeypatch.setenv("REDSHIFT_CONSUMER_2_NAMESPACE_ID", "commercial-ns")
    monkeypatch.setenv("REDSHIFT_CONSUMER_2_FRIENDLY", "Commercial")
    monkeypatch.setenv("REDSHIFT_CONSUMER_3_ENABLED", "true")
    monkeypatch.setenv("REDSHIFT_CONSUMER_3_NAMESPACE_ID", "consumer-banking-ns")
    monkeypatch.setenv("REDSHIFT_CONSUMER_3_FRIENDLY", "Consumer")
    for number in range(4, 8):
        monkeypatch.setenv(f"REDSHIFT_CONSUMER_{number}_ENABLED", "false")
    for prefix in (
        "REDSHIFT_PRODUCER",
        "REDSHIFT_CONSUMER_1",
        "REDSHIFT_CONSUMER_2",
        "REDSHIFT_CONSUMER_3",
    ):
        set_session_secret(f"{prefix}_HOST", f"{prefix.lower()}.example")
        set_session_secret(f"{prefix}_USER", "db-user")
        set_session_secret(f"{prefix}_PASSWORD", "db-password")
    opened = []

    window = LoaderWindow(
        str(tmp_path / "loader.duckdb"),
        embedded=True,
        credentials_callback=lambda: opened.append(True),
    )
    window.refresh_idle_state()
    labels = [
        window._tree.topLevelItem(index).text(0)
        for index in range(window._tree.topLevelItemCount())
    ]
    window._credentials.click()

    assert "Producer — Main Warehouse" in labels
    assert "Consumer 1 — FAR" in labels
    assert "Consumer 2 — Commercial" in labels
    assert "Consumer 3 — Consumer" in labels
    assert len(labels) == 4
    assert not any("Consumer 4" in label for label in labels)
    assert not any("External table information" in label for label in labels)
    producer = next(
        window._tree.topLevelItem(index)
        for index in range(window._tree.topLevelItemCount())
        if window._tree.topLevelItem(index).text(0) == "Producer — Main Warehouse"
    )
    assert producer.child(0).text(0) == "External table metadata"
    assert producer.child(0).text(2) == "Included in safe load"
    assert window._primary.isEnabled()
    assert opened == [True]
    window.deleteLater()


def test_portable_manifest_limits_sources_and_uses_display_names(
    monkeypatch, tmp_path,
) -> None:
    from analyzer.secrets_store import clear_session_secrets, set_session_secret

    profile_path = tmp_path / "redshift_cluster_profiles.json"
    profile_path.write_text(
        json.dumps(
            {
                "format": "redshift-query-anatomy-cluster-profiles",
                "version": 1,
                "contains_credentials": False,
                "profiles": [
                    {
                        "profile": "REDSHIFT_PRODUCER",
                        "enabled": "true",
                        "display_name": "Main Warehouse",
                        "namespace_id": "producer-ns",
                    },
                    {
                        "profile": "REDSHIFT_CONSUMER_1",
                        "enabled": "true",
                        "display_name": "FAR",
                        "namespace_id": "far-ns",
                    },
                    {
                        "profile": "REDSHIFT_CONSUMER_2",
                        "enabled": "true",
                        "display_name": "Commercial",
                        "namespace_id": "commercial-ns",
                    },
                    {
                        "profile": "REDSHIFT_CONSUMER_3",
                        "enabled": "true",
                        "display_name": "Consumer",
                        "namespace_id": "consumer-ns",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("REDSHIFT_ANALYZER_PROFILE_PATH", str(profile_path))
    monkeypatch.setenv("REDSHIFT_ANALYZER_LAUNCH_DIR", str(tmp_path))
    # Simulate stale non-secret and protected values from a previously
    # configured fourth consumer. The portable manifest must win.
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_FRIENDLY", "Old FAR Label")
    monkeypatch.setenv("REDSHIFT_CONSUMER_4_ENABLED", "true")
    monkeypatch.setenv("REDSHIFT_CONSUMER_4_NAMESPACE_ID", "stale-ns")
    monkeypatch.setenv("REDSHIFT_CONSUMER_4_FRIENDLY", "Stale Consumer")
    clear_session_secrets()
    for prefix in (
        "REDSHIFT_PRODUCER",
        "REDSHIFT_CONSUMER_1",
        "REDSHIFT_CONSUMER_2",
        "REDSHIFT_CONSUMER_3",
        "REDSHIFT_CONSUMER_4",
    ):
        set_session_secret(f"{prefix}_HOST", f"{prefix.lower()}.example")
        set_session_secret(f"{prefix}_USER", "db-user")
        set_session_secret(f"{prefix}_PASSWORD", "db-password")

    try:
        window = LoaderWindow(str(tmp_path / "manifest.duckdb"))
        window.refresh_idle_state()
        labels = [
            window._tree.topLevelItem(index).text(0)
            for index in range(window._tree.topLevelItemCount())
        ]
    finally:
        clear_session_secrets()

    assert labels == [
        "Producer — Main Warehouse",
        "Consumer 1 — FAR",
        "Consumer 2 — Commercial",
        "Consumer 3 — Consumer",
    ]
    assert not any("Consumer 4" in label for label in labels)
    window.deleteLater()


def test_external_catalog_progress_uses_metadata_name(tmp_path) -> None:
    window = LoaderWindow(str(tmp_path / "loader.duckdb"))

    window.handle_event({
        "event": "progress",
        "message": "Staged in DuckDB",
        "namespace_id": "producer-ns",
        "table_name": "external_table_metadata",
        "duckdb_rows": 88,
        "completed": 1,
        "total": 1,
    })

    producer = window._namespace_items["producer-ns"]
    assert producer.child(0).text(0) == "External table metadata"
    assert producer.child(0).text(2) == "Staged in DuckDB"
    window.deleteLater()


def test_embedded_loader_blocks_raw_start_until_credentials_exist(
    monkeypatch, tmp_path,
) -> None:
    monkeypatch.setenv("REDSHIFT_ENABLED", "true")
    monkeypatch.setenv("REDSHIFT_NAMESPACE", "producer-ns")

    window = LoaderWindow(
        str(tmp_path / "loader.duckdb"),
        embedded=True,
        credentials_callback=lambda: None,
    )
    window.refresh_idle_state()

    assert not window._primary.isEnabled()
    assert "credentials are required" in window._status.text().lower()
    assert "Required" in window._credentials.text()
    window.deleteLater()


def test_loader_gui_command_relaunches_the_single_file_application(monkeypatch, tmp_path) -> None:
    launcher = tmp_path / "Infraredshift.py"
    launcher.write_text("# launcher\n", encoding="utf-8")
    monkeypatch.setenv("REDSHIFT_ANALYZER_LAUNCH_PATH", str(launcher))

    command = build_loader_gui_command(tmp_path / "db.duckdb", python_executable="python.exe")

    assert command[:3] == ["python.exe", str(launcher.resolve()), "--loader-gui"]
    assert command[3] == "--duckdb-path"


def test_loader_window_never_imports_the_dashboard_monolith() -> None:
    # The delineation is the product requirement: the loader must stay a
    # small, separate app. Guard it at the source level.
    source = (Path(__file__).resolve().parents[1] / "analyzer" / "loader" / "gui.py").read_text(
        encoding="utf-8"
    )
    assert "cluster_dashboard" not in source
    imports = [
        line.strip() for line in source.splitlines()
        if line.strip().startswith(("import ", "from "))
    ]
    assert not any("widgets" in line for line in imports), imports
