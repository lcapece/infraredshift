from pathlib import Path

import duckdb

from analyzer.cluster_analyze import load_cluster_report


def test_table_review_attributes_captured_query_telemetry_by_table_id(tmp_path) -> None:
    sample = Path(__file__).resolve().parents[1] / "analyzer" / "samples" / "mock_redshift_3300.duckdb"
    db_path = tmp_path / "telemetry.duckdb"
    db_path.write_bytes(sample.read_bytes())

    con = duckdb.connect(str(db_path))
    try:
        snapshot_id, query_id = con.execute(
            "SELECT snapshot_id, query_id FROM query_health WHERE query_id IS NOT NULL LIMIT 1"
        ).fetchone()
        table_id, table_name = con.execute(
            "SELECT table_id, \"table\" FROM svv_table_info_all "
            "WHERE snapshot_id = ? AND table_id IS NOT NULL LIMIT 1",
            [snapshot_id],
        ).fetchone()
        con.execute(
            "UPDATE query_health SET dist_both_cnt = '1', bcast_cnt = '1', dist_total_cnt = '2' "
            "WHERE snapshot_id = ? AND query_id = ?",
            [snapshot_id, query_id],
        )
        con.execute(
            "UPDATE query_details SET max_data_skewness = '5' "
            "WHERE snapshot_id = ? AND query_id = ?",
            [snapshot_id, query_id],
        )
        con.execute(
            "INSERT INTO query_detail_flow "
            "(snapshot_id, captured_at, query_id, metrics_level, step_name, table_id, table_name) "
            "VALUES (?, CURRENT_TIMESTAMP, ?, 'step', 'scan', ?, ?)",
            [snapshot_id, query_id, table_id, table_name],
        )
    finally:
        con.close()

    report = load_cluster_report(db_path, snapshot_id=snapshot_id, areas=["table_review"])
    rows = report.table_review[
        report.table_review["table_id"].astype(str) == str(table_id)
    ]

    assert not rows.empty
    row = rows.iloc[0]
    assert int(row["slow_query_count"]) >= 1
    assert int(row["redistribution_query_count"]) >= 1
    assert int(row["broadcast_query_count"]) >= 1
    assert int(row["skewed_query_count"]) >= 1
