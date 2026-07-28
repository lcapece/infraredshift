from __future__ import annotations

import duckdb
import pandas as pd

from analyzer.cluster_analyze import _sql_feature_key, load_cluster_report
from analyzer.duckdb_store import DuckDBStore


def _warehouse_with_repeats(tmp_path):
    path = tmp_path / "poisoned.duckdb"
    store = DuckDBStore(path)
    run = store.new_snapshot("poisoned")
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


def test_varchar_poisoned_sql_feature_cache_cannot_crash_the_load(tmp_path, monkeypatch) -> None:
    """Regression: a cache whose numeric feature columns round-tripped as
    VARCHAR must not abort the load with int+str TypeErrors."""
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(tmp_path / "home"))
    path, run = _warehouse_with_repeats(tmp_path)

    # Poison the cache with CURRENT-version keys whose feature values are
    # strings (simulating an older build's DuckDB VARCHAR round-trip).
    poisoned = pd.DataFrame(
        [
            {
                "sql_feature_key": _sql_feature_key("SELECT a FROM t WHERE x=1"),
                "sql_table_count": "2",
                "sql_join_count": "1",
            }
        ]
    )
    con = duckdb.connect(str(path))
    con.register("poison", poisoned)
    con.execute('CREATE OR REPLACE TABLE "analysis_cache_sql_features" AS SELECT * FROM poison')
    con.close()

    report = load_cluster_report(path, snapshot_id=run.snapshot_id, areas=["repeat_queries"])

    # The load must complete with queries present; grouping either succeeded
    # or was skipped with a note — but never aborted the report.
    assert not report.slow_queries.empty
    assert not any("Repeat Grouping:" in err and "unsupported operand" in err for err in report.load_errors), (
        "int+str TypeError leaked through the dtype unifier: " + "; ".join(report.load_errors)
    )


def test_legacy_v1_cache_rows_are_orphaned_by_version_bump(tmp_path, monkeypatch) -> None:
    """Old v1-keyed rows must simply never match current lookups."""
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(tmp_path / "home"))
    path, run = _warehouse_with_repeats(tmp_path)
    import hashlib

    legacy_key = hashlib.sha256(b"v1|SELECT a FROM t WHERE x=1").hexdigest()
    legacy = pd.DataFrame([{"sql_feature_key": legacy_key, "sql_table_count": "999"}])
    con = duckdb.connect(str(path))
    con.register("legacy", legacy)
    con.execute('CREATE OR REPLACE TABLE "analysis_cache_sql_features" AS SELECT * FROM legacy')
    con.close()

    report = load_cluster_report(path, snapshot_id=run.snapshot_id, areas=["repeat_queries"])

    assert not report.slow_queries.empty
    assert int(report.summary.get("triage_sql_feature_cache_hits") or 0) == 0
