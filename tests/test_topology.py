from __future__ import annotations

import duckdb
from PySide6.QtWidgets import QApplication, QPushButton

from analyzer.topology import REQUIRED_DATASETS, load_topology_snapshot
from analyzer.widgets.topology import TopologyPage


def _create_dataset(con, table_name: str) -> None:
    extra = ", start_time TIMESTAMP" if table_name in {"query_history", "query_history_all"} else ""
    con.execute(
        f'CREATE TABLE "{table_name}"(namespace_id VARCHAR, query_id BIGINT{extra})'
    )


def test_topology_uses_friendly_names_and_namespace_query_ranges(tmp_path) -> None:
    path = tmp_path / "topology.duckdb"
    con = duckdb.connect(str(path))
    try:
        for spec in REQUIRED_DATASETS:
            _create_dataset(con, spec.table_name)
            if spec.table_name in {"query_history", "query_history_all"}:
                con.execute(
                    f'INSERT INTO "{spec.table_name}" VALUES '
                    "('producer-ns', 101, TIMESTAMP '2026-07-10 10:00:00'), "
                    "('producer-ns', 199, TIMESTAMP '2026-07-17 09:00:00')"
                )
            else:
                con.execute(f'INSERT INTO "{spec.table_name}" VALUES (\'producer-ns\', 101)')
        con.execute(
            "CREATE TABLE snapshot_cluster_runs(snapshot_id VARCHAR, namespace_id VARCHAR, "
            "cluster_role VARCHAR, cluster_name VARCHAR, cluster_host VARCHAR, captured_at TIMESTAMP)"
        )
        con.execute(
            "INSERT INTO snapshot_cluster_runs VALUES "
            "('s1','producer-ns','producer','Old Producer Name','host',CURRENT_TIMESTAMP)"
        )
    finally:
        con.close()

    env = {
        "REDSHIFT_PRODUCER_HOST": "producer.example",
        "REDSHIFT_PRODUCER_NAMESPACE_ID": "producer-ns",
        "REDSHIFT_PRODUCER_DISPLAY_NAME": "Business Producer Cluster",
        "REDSHIFT_PRODUCER_ENABLED": "true",
    }
    snapshot = load_topology_snapshot(path, env)
    producer = snapshot.clusters[0]

    assert producer.friendly_name == "Business Producer Cluster"
    assert producer.namespace_id == "producer-ns"
    assert producer.min_query_id == 101
    assert producer.max_query_id == 199
    assert producer.query_count == 2
    assert producer.ready_dataset_count == len(REQUIRED_DATASETS)
    assert producer.severity == "complete"


def test_topology_marks_partial_amber_and_missing_history_red(tmp_path) -> None:
    path = tmp_path / "partial.duckdb"
    con = duckdb.connect(str(path))
    try:
        _create_dataset(con, "query_history")
        con.execute("INSERT INTO query_history VALUES ('partial-ns', 10, CURRENT_TIMESTAMP)")
        _create_dataset(con, "query_text")
        con.execute("INSERT INTO query_text VALUES ('partial-ns', 10)")
    finally:
        con.close()

    env = {
        "REDSHIFT_PRODUCER_HOST": "p",
        "REDSHIFT_PRODUCER_NAMESPACE_ID": "partial-ns",
        "REDSHIFT_PRODUCER_DISPLAY_NAME": "Partial Producer",
        "REDSHIFT_CONSUMER_1_HOST": "c",
        "REDSHIFT_CONSUMER_1_NAMESPACE_ID": "empty-ns",
        "REDSHIFT_CONSUMER_1_DISPLAY_NAME": "Empty Reporting Consumer",
    }
    snapshot = load_topology_snapshot(path, env)
    by_name = {cluster.friendly_name: cluster for cluster in snapshot.clusters}

    assert by_name["Partial Producer"].severity == "amber"
    assert by_name["Empty Reporting Consumer"].severity == "red"
    assert by_name["Empty Reporting Consumer"].query_count == 0


def test_disabled_configured_cluster_is_visible_but_excluded(tmp_path) -> None:
    path = tmp_path / "empty.duckdb"
    con = duckdb.connect(str(path))
    con.close()
    env = {
        "REDSHIFT_PRODUCER_HOST": "p",
        "REDSHIFT_PRODUCER_NAMESPACE_ID": "p-ns",
        "REDSHIFT_CONSUMER_1_HOST": "c",
        "REDSHIFT_CONSUMER_1_NAMESPACE_ID": "c-ns",
        "REDSHIFT_CONSUMER_1_DISPLAY_NAME": "Finance Consumer",
        "REDSHIFT_CONSUMER_1_ENABLED": "false",
    }

    snapshot = load_topology_snapshot(path, env)
    consumer = next(cluster for cluster in snapshot.clusters if cluster.friendly_name == "Finance Consumer")
    assert consumer.severity == "disabled"
    assert not consumer.enabled


def test_portable_manifest_hides_historical_fourth_consumer(tmp_path) -> None:
    path = tmp_path / "manifest-topology.duckdb"
    con = duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE snapshot_cluster_runs("
            "snapshot_id VARCHAR, namespace_id VARCHAR, cluster_role VARCHAR, "
            "cluster_name VARCHAR, cluster_host VARCHAR, captured_at TIMESTAMP)"
        )
        con.execute(
            "INSERT INTO snapshot_cluster_runs VALUES "
            "('old','stale-ns','consumer','Stale Consumer','host',CURRENT_TIMESTAMP)"
        )
    finally:
        con.close()

    env = {
        "INFRAREDSHIFT_ACTIVE_PROFILE_PREFIXES": (
            "REDSHIFT_PRODUCER,REDSHIFT_CONSUMER_1,"
            "REDSHIFT_CONSUMER_2,REDSHIFT_CONSUMER_3"
        ),
        "REDSHIFT_PRODUCER_NAMESPACE_ID": "producer-ns",
        "REDSHIFT_PRODUCER_DISPLAY_NAME": "Main Warehouse",
        "REDSHIFT_PRODUCER_ENABLED": "true",
        "REDSHIFT_CONSUMER_1_NAMESPACE_ID": "far-ns",
        "REDSHIFT_CONSUMER_1_DISPLAY_NAME": "FAR",
        "REDSHIFT_CONSUMER_1_ENABLED": "true",
        "REDSHIFT_CONSUMER_2_NAMESPACE_ID": "commercial-ns",
        "REDSHIFT_CONSUMER_2_DISPLAY_NAME": "Commercial",
        "REDSHIFT_CONSUMER_2_ENABLED": "true",
        "REDSHIFT_CONSUMER_3_NAMESPACE_ID": "consumer-ns",
        "REDSHIFT_CONSUMER_3_DISPLAY_NAME": "Consumer",
        "REDSHIFT_CONSUMER_3_ENABLED": "true",
        "REDSHIFT_CONSUMER_4_NAMESPACE_ID": "stale-ns",
        "REDSHIFT_CONSUMER_4_DISPLAY_NAME": "Stale Consumer",
        "REDSHIFT_CONSUMER_4_ENABLED": "true",
    }

    snapshot = load_topology_snapshot(path, env)

    assert [cluster.friendly_name for cluster in snapshot.clusters] == [
        "Main Warehouse",
        "Commercial",
        "Consumer",
        "FAR",
    ]
    assert not any(
        cluster.friendly_name == "Stale Consumer"
        for cluster in snapshot.clusters
    )


def test_topology_exposes_data_loader_button(monkeypatch, tmp_path) -> None:
    app = QApplication.instance() or QApplication([])
    monkeypatch.setattr("analyzer.widgets.topology.QTimer.singleShot", lambda *_args: None)
    page = TopologyPage(str(tmp_path / "topology.duckdb"))
    labels = {button.text() for button in page.findChildren(QPushButton)}
    assert "Open Data Loader" in labels
    page.close()
    _ = app
