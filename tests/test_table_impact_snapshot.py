from pathlib import Path

import duckdb

from analyzer.cluster_analyze import load_cluster_report


def test_table_impact_uses_catalog_when_query_snapshot_is_newer(tmp_path) -> None:
    sample = Path(__file__).resolve().parents[1] / "analyzer" / "samples" / "mock_redshift_3300.duckdb"
    db_path = tmp_path / "impact.duckdb"
    db_path.write_bytes(sample.read_bytes())

    con = duckdb.connect(str(db_path))
    try:
        snapshot_id = con.execute("SELECT snapshot_id FROM snapshot_runs ORDER BY captured_at DESC LIMIT 1").fetchone()[0]
        con.execute("UPDATE svv_table_info_all SET snapshot_id = 'older-catalog-snapshot'")
    finally:
        con.close()

    report = load_cluster_report(db_path, snapshot_id=snapshot_id, areas=["table_impact"])

    assert not report.table_impact.empty
    assert int(report.table_impact["slow_query_count"].max()) > 0
