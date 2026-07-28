from __future__ import annotations

import duckdb
import pandas as pd

from analyzer.cluster_analyze import load_cluster_report
from analyzer.duckdb_store import DuckDBStore


def _warehouse(tmp_path):
    path = tmp_path / "loaderpoison.duckdb"
    store = DuckDBStore(path)
    run = store.new_snapshot("loaderpoison")
    hist = pd.DataFrame(
        [
            {"namespace_id": "ns", "query_id": i, "user_name": "svc",
             "database_name": "dev", "elapsed_time": 700_000_000,
             "execution_time": 700_000_000}
            for i in (1, 2, 3, 4)
        ]
    )
    text = pd.DataFrame(
        [
            {"namespace_id": "ns", "query_id": 1, "sequence": 0, "text": "SELECT a FROM t WHERE x=1"},
            {"namespace_id": "ns", "query_id": 2, "sequence": 0, "text": "SELECT a FROM t WHERE x=1"},
            {"namespace_id": "ns", "query_id": 3, "sequence": 0, "text": "SELECT b FROM u WHERE y=2"},
            {"namespace_id": "ns", "query_id": 4, "sequence": 0, "text": "SELECT b FROM u WHERE y=2"},
        ]
    )
    with store.connect() as con:
        store.record_snapshot(con, run, source="test")
        store.replace_table_from_frame(con, "query_history", hist, run)
        store.replace_table_from_frame(con, "query_text", text, run)
    return path, run


def test_varchar_loader_grouping_tables_cannot_crash_or_blank_the_load(tmp_path, monkeypatch) -> None:
    """Regression for the corp-laptop failure: loader-materialized repeat
    tables with VARCHAR numerics must neither crash sums (int+str) nor leave
    the triage empty."""
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(tmp_path / "home"))
    path, run = _warehouse(tmp_path)

    groups = pd.DataFrame(
        [{
            "snapshot_id": str(run.snapshot_id), "repeat_group_id": "RQ1",
            "repeat_group_key": "Gk1", "query_count": "2",
            "total_runtime_s": "1400.5", "users": "svc", "sql_shape": "select a",
        }]
    )
    members = pd.DataFrame(
        [
            {"snapshot_id": str(run.snapshot_id), "repeat_group_id": "RQ1",
             "query_id": "1", "similarity_score": "1.0"},
            {"snapshot_id": str(run.snapshot_id), "repeat_group_id": "RQ1",
             "query_id": "2", "similarity_score": "1.0"},
        ]
    )
    con = duckdb.connect(str(path))
    con.register("g", groups)
    con.register("m", members)
    con.execute('CREATE OR REPLACE TABLE "loader_repeat_groups" AS SELECT * FROM g')
    con.execute('CREATE OR REPLACE TABLE "loader_repeat_members" AS SELECT * FROM m')
    con.close()

    report = load_cluster_report(path, snapshot_id=run.snapshot_id, areas=["repeat_queries"])

    assert not report.slow_queries.empty
    assert not any("unsupported operand" in err for err in report.load_errors), report.load_errors
    # Numeric coercion must land: sums are real numbers, not concatenations.
    assert report.summary.get("repeat_query_count") in (2, 4)
    assert isinstance(report.summary.get("repeat_runtime_s"), float)
    assert not report.repeat_groups.empty
    assert pd.api.types.is_numeric_dtype(report.repeat_groups["query_count"])


def test_empty_loader_grouping_tables_fall_through_to_fresh_grouping(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(tmp_path / "home"))
    path, run = _warehouse(tmp_path)
    con = duckdb.connect(str(path))
    con.execute(
        'CREATE OR REPLACE TABLE "loader_repeat_groups" '
        "(snapshot_id VARCHAR, repeat_group_id VARCHAR, repeat_group_key VARCHAR, "
        "query_count VARCHAR, total_runtime_s VARCHAR)"
    )
    con.execute(
        'CREATE OR REPLACE TABLE "loader_repeat_members" '
        "(snapshot_id VARCHAR, repeat_group_id VARCHAR, query_id VARCHAR, similarity_score VARCHAR)"
    )
    con.close()

    report = load_cluster_report(path, snapshot_id=run.snapshot_id, areas=["repeat_queries"])

    assert not report.slow_queries.empty
    # Fresh grouping must have produced the exact-duplicate patterns; empty
    # loader tables must never blank the triage (whether the loader reader
    # rejects them itself or the empty-cached guard discards them).
    assert not report.repeat_groups.empty, "empty loader tables must not blank the triage"
