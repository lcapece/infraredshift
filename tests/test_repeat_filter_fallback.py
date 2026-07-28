from __future__ import annotations

import pandas as pd

from analyzer.cluster_analyze import load_cluster_report
from analyzer.duckdb_store import DuckDBStore


def test_all_one_off_warehouse_falls_back_to_unfiltered(tmp_path, monkeypatch) -> None:
    """The repeat pre-filter must never blank the analysis entirely."""
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(tmp_path / "home"))
    path = tmp_path / "oneoffs.duckdb"
    store = DuckDBStore(path)
    run = store.new_snapshot("oneoffs")
    hist = pd.DataFrame(
        [
            {"namespace_id": "ns", "query_id": i, "user_name": f"user{i}",
             "database_name": "dev", "elapsed_time": 700_000_000,
             "execution_time": 700_000_000}
            for i in range(1, 4)
        ]
    )
    text = pd.DataFrame(
        [
            {"namespace_id": "ns", "query_id": 1, "sequence": 0, "text": "SELECT a FROM t1"},
            {"namespace_id": "ns", "query_id": 2, "sequence": 0, "text": "DELETE FROM t2 WHERE x=9"},
            {"namespace_id": "ns", "query_id": 3, "sequence": 0, "text": "INSERT INTO t3 VALUES (5)"},
        ]
    )
    with store.connect() as con:
        store.record_snapshot(con, run, source="test")
        store.replace_table_from_frame(con, "query_history", hist, run)
        store.replace_table_from_frame(con, "query_text", text, run)

    report = load_cluster_report(path, snapshot_id=run.snapshot_id, areas=["slow_queries"])

    assert len(report.slow_queries) == 3, "fallback must surface the one-off queries"
    assert any("matched no rows" in note for note in report.notes)
