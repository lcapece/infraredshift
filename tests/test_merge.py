"""Per-cluster merge: completeness/freshness gates, dedup, registry, locking."""
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest

import runner
from analyzer.loader.engine import LoaderAlreadyRunningError, _ProcessLock
from analyzer.loader.merge import merge_cluster_files


NOW = datetime(2026, 7, 20, 12, 0, 0)


def _make_cluster_file(
    path: Path,
    namespace: str,
    role: str,
    *,
    snapshot: str = "snap-1",
    captured_at: datetime = NOW,
    query_ids=(1, 2),
    promoted: bool = True,
    with_registry: bool = True,
    cluster_name: str = "",
) -> None:
    assert "query_history" in runner.LIVE_REFRESH_TABLES
    con = duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE query_history (snapshot_id VARCHAR, namespace_id VARCHAR, query_id BIGINT)"
        )
        for query_id in query_ids:
            con.execute(
                "INSERT INTO query_history VALUES (?, ?, ?)", [snapshot, namespace, query_id]
            )
        if promoted:
            con.execute(
                "CREATE TABLE snapshot_runs "
                "(snapshot_id VARCHAR PRIMARY KEY, captured_at TIMESTAMP, label VARCHAR, source VARCHAR)"
            )
            con.execute(
                "INSERT INTO snapshot_runs VALUES (?, ?, 'load', 'test')",
                [snapshot, captured_at],
            )
            if with_registry:
                con.execute(
                    "CREATE TABLE snapshot_cluster_runs "
                    "(snapshot_id VARCHAR, namespace_id VARCHAR, cluster_role VARCHAR, "
                    "cluster_name VARCHAR, cluster_host VARCHAR, primary_database VARCHAR, "
                    "captured_at TIMESTAMP)"
                )
                con.execute(
                    "INSERT INTO snapshot_cluster_runs VALUES (?, ?, ?, ?, 'host', 'dev', ?)",
                    [snapshot, namespace, role, cluster_name or role.title(), captured_at],
                )
    finally:
        con.close()


def _query(target: Path, sql: str):
    con = duckdb.connect(str(target), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_merge_combines_clusters_and_publishes_registry(tmp_path):
    producer = tmp_path / "redshift.producer.duckdb"
    consumer = tmp_path / "redshift.consumer_1.duckdb"
    _make_cluster_file(producer, "ns-prod", "producer", query_ids=(1, 2), cluster_name="Prod")
    _make_cluster_file(consumer, "ns-c1", "consumer_1", query_ids=(3,), cluster_name="C1")
    target = tmp_path / "redshift.duckdb"

    result = merge_cluster_files([producer, consumer], target)

    assert result["tables"] == {"query_history": 3}
    assert result["namespaces"] == ["ns-c1", "ns-prod"]
    rows = _query(target, "SELECT DISTINCT snapshot_id FROM query_history")
    assert len(rows) == 1 and rows[0][0] == result["snapshot_id"]
    registry = _query(
        target,
        "SELECT namespace_id, cluster_role, cluster_name FROM snapshot_cluster_runs ORDER BY namespace_id",
    )
    assert registry == [("ns-c1", "consumer_1", "C1"), ("ns-prod", "producer", "Prod")]


def test_merge_refuses_missing_source_unless_allow_partial(tmp_path):
    producer = tmp_path / "redshift.producer.duckdb"
    _make_cluster_file(producer, "ns-prod", "producer")
    missing = tmp_path / "redshift.consumer_1.duckdb"
    target = tmp_path / "redshift.duckdb"

    with pytest.raises(SystemExit, match="missing per-cluster file"):
        merge_cluster_files([producer, missing], target)

    result = merge_cluster_files([producer, missing], target, allow_partial=True)
    assert result["missing_sources"] == [str(missing)]
    assert result["namespaces"] == ["ns-prod"]


def test_merge_refuses_unpromoted_source_unless_allow_stale(tmp_path):
    producer = tmp_path / "redshift.producer.duckdb"
    consumer = tmp_path / "redshift.consumer_1.duckdb"
    _make_cluster_file(producer, "ns-prod", "producer")
    _make_cluster_file(consumer, "ns-c1", "consumer_1", promoted=False)
    target = tmp_path / "redshift.duckdb"

    with pytest.raises(SystemExit, match="no promoted snapshot"):
        merge_cluster_files([producer, consumer], target)

    result = merge_cluster_files([producer, consumer], target, allow_stale=True)
    assert set(result["namespaces"]) == {"ns-prod", "ns-c1"}


def test_merge_refuses_stale_skew_unless_allow_stale(tmp_path):
    producer = tmp_path / "redshift.producer.duckdb"
    consumer = tmp_path / "redshift.consumer_1.duckdb"
    _make_cluster_file(producer, "ns-prod", "producer", captured_at=NOW)
    _make_cluster_file(
        consumer, "ns-c1", "consumer_1", captured_at=NOW - timedelta(hours=30)
    )
    target = tmp_path / "redshift.duckdb"

    with pytest.raises(SystemExit, match="stale"):
        merge_cluster_files([producer, consumer], target)

    result = merge_cluster_files([producer, consumer], target, allow_stale=True)
    assert set(result["namespaces"]) == {"ns-prod", "ns-c1"}


def test_merge_handles_apostrophe_in_path(tmp_path):
    quoted_dir = tmp_path / "O'Brien"
    quoted_dir.mkdir()
    producer = quoted_dir / "redshift.producer.duckdb"
    _make_cluster_file(producer, "ns-prod", "producer")
    target = quoted_dir / "redshift.duckdb"

    result = merge_cluster_files([producer], target)
    assert result["tables"] == {"query_history": 2}


def test_merge_takes_each_namespace_from_one_source_only(tmp_path):
    producer = tmp_path / "redshift.producer.duckdb"
    consumer = tmp_path / "redshift.consumer_1.duckdb"
    _make_cluster_file(producer, "ns-shared", "producer", query_ids=(1, 2))
    # The consumer file also carries a stale copy of ns-shared plus its own rows.
    con = duckdb.connect(str(consumer))
    try:
        con.execute(
            "CREATE TABLE query_history (snapshot_id VARCHAR, namespace_id VARCHAR, query_id BIGINT)"
        )
        for namespace, query_id in (
            ("ns-shared", 91), ("ns-shared", 92), ("ns-shared", 93),
            ("ns-c1", 10), ("ns-c1", 11),
        ):
            con.execute("INSERT INTO query_history VALUES ('snap-2', ?, ?)", [namespace, query_id])
        con.execute(
            "CREATE TABLE snapshot_runs "
            "(snapshot_id VARCHAR PRIMARY KEY, captured_at TIMESTAMP, label VARCHAR, source VARCHAR)"
        )
        con.execute("INSERT INTO snapshot_runs VALUES ('snap-2', ?, 'load', 'test')", [NOW])
    finally:
        con.close()
    target = tmp_path / "redshift.duckdb"

    result = merge_cluster_files([producer, consumer], target)

    counts = dict(
        _query(target, "SELECT namespace_id, COUNT(*) FROM query_history GROUP BY namespace_id")
    )
    assert counts == {"ns-shared": 2, "ns-c1": 2}
    assert result["tables"]["query_history"] == 4


def test_merge_synthesizes_registry_from_filename_when_source_has_none(tmp_path):
    producer = tmp_path / "redshift.producer.duckdb"
    _make_cluster_file(producer, "ns-prod", "producer", with_registry=False)
    target = tmp_path / "redshift.duckdb"

    merge_cluster_files([producer], target)

    registry = _query(target, "SELECT namespace_id, cluster_role FROM snapshot_cluster_runs")
    assert registry == [("ns-prod", "producer")]


def test_merge_backs_up_existing_target(tmp_path):
    producer = tmp_path / "redshift.producer.duckdb"
    _make_cluster_file(producer, "ns-prod", "producer")
    target = tmp_path / "redshift.duckdb"
    con = duckdb.connect(str(target))
    con.execute("CREATE TABLE keepsake (v INTEGER)")
    con.close()

    merge_cluster_files([producer], target)

    backups = list((tmp_path / "backups").glob("*.duckdb"))
    assert len(backups) == 1


def test_merge_refuses_to_run_beside_an_active_loader(tmp_path):
    producer = tmp_path / "redshift.producer.duckdb"
    _make_cluster_file(producer, "ns-prod", "producer")
    target = tmp_path / "redshift.duckdb"

    with _ProcessLock(target):
        with pytest.raises(LoaderAlreadyRunningError):
            merge_cluster_files([producer], target)
