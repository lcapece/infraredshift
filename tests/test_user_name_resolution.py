from pathlib import Path

import duckdb

from analyzer.cluster_analyze import load_cluster_report


def test_user_lookup_crosses_snapshot_boundary(tmp_path) -> None:
    sample = Path(__file__).resolve().parents[1] / "analyzer" / "samples" / "mock_redshift_3300.duckdb"
    db_path = tmp_path / "users.duckdb"
    db_path.write_bytes(sample.read_bytes())

    con = duckdb.connect(str(db_path))
    try:
        snapshot_id, query_id, user_id = con.execute(
            "SELECT snapshot_id, query_id, user_id FROM query_history LIMIT 1"
        ).fetchone()
        con.execute("UPDATE query_history SET user_name = NULL WHERE query_id = ?", [query_id])
        # The production-quality mock now includes a populated current user
        # catalog so Topology can demonstrate 13/13 healthy datasets. Remove
        # this user's current row to isolate the cross-snapshot fallback that
        # this test is specifically intended to prove.
        con.execute("DELETE FROM user_info WHERE user_id = ?", [user_id])
        con.execute(
            "INSERT INTO user_info (snapshot_id, captured_at, user_id, user_name) "
            "VALUES ('older-user-snapshot', CURRENT_TIMESTAMP - INTERVAL 1 DAY, ?, 'resolved_owner')",
            [user_id],
        )
    finally:
        con.close()

    report = load_cluster_report(db_path, snapshot_id=snapshot_id, areas=["slow_queries"])
    row = report.slow_queries[
        report.slow_queries["query_id"].astype(str) == str(query_id)
    ].iloc[0]

    assert row["user_name"] == "resolved_owner"


def test_view_catalog_uses_its_own_latest_snapshot(tmp_path) -> None:
    sample = Path(__file__).resolve().parents[1] / "analyzer" / "samples" / "mock_redshift_3300.duckdb"
    db_path = tmp_path / "views.duckdb"
    db_path.write_bytes(sample.read_bytes())

    con = duckdb.connect(str(db_path))
    try:
        snapshot_id = con.execute("SELECT snapshot_id FROM snapshot_runs ORDER BY captured_at DESC LIMIT 1").fetchone()[0]
        con.execute("UPDATE view_definitions SET snapshot_id = 'older-catalog-snapshot'")
        expected = con.execute("SELECT COUNT(*) FROM view_definitions").fetchone()[0]
    finally:
        con.close()

    report = load_cluster_report(db_path, snapshot_id=snapshot_id, areas=["sql_lens"])

    assert len(report.view_definitions) == expected
