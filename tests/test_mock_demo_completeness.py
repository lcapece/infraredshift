from __future__ import annotations

import duckdb

from analyzer.cluster_analyze import load_cluster_report
from analyzer.mock_data import generate_mock_snapshot


def test_clean_demo_contains_plan_flow_and_external_table_evidence(tmp_path) -> None:
    path = tmp_path / "complete-demo.duckdb"
    summary = generate_mock_snapshot(output=path, query_count=24, table_count=36, seed=42)

    assert summary.explain_rows == 24 * 4
    assert summary.detail_flow_rows == 24 * 3
    assert summary.external_table_rows == 12

    con = duckdb.connect(str(path), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM v_query_explain").fetchone()[0] == 96
        assert con.execute("SELECT COUNT(*) FROM v_query_detail_flow").fetchone()[0] == 72
        assert con.execute("SELECT COUNT(*) FROM v_external_table_info").fetchone()[0] == 12
        assert con.execute(
            "SELECT COUNT(*) FROM v_query_explain "
            "WHERE plan_text_lc LIKE '%join%' OR plan_text_lc LIKE '%nested loop%'"
        ).fetchone()[0] == 24
        assert con.execute(
            "SELECT COUNT(*) FROM v_query_detail_flow WHERE input_rows > 0 AND duration_s > 0"
        ).fetchone()[0] == 72
    finally:
        con.close()


def test_clean_demo_populates_major_report_areas(tmp_path) -> None:
    path = tmp_path / "report-demo.duckdb"
    generate_mock_snapshot(output=path, query_count=30, table_count=42, seed=84)

    report = load_cluster_report(str(path), areas={
        "slow_queries", "table_review", "table_heatmap", "external_tables", "sql_lens",
        "repeat_queries",
    })

    assert not report.slow_queries.empty
    assert not report.query_explain.empty
    assert not report.query_detail_flow.empty
    assert not report.table_review.empty
    assert not report.table_heatmap.empty
    assert not report.external_tables.empty
    assert not report.view_definitions.empty
    assert not report.repeat_groups.empty
    assert report.repeat_groups["repeat_group_id"].nunique() >= 4
