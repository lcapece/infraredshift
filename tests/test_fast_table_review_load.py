from __future__ import annotations

from analyzer.cluster_analyze import load_cluster_report
from analyzer.mock_data import generate_mock_snapshot


def test_table_review_fast_path_returns_the_complete_physical_inventory(tmp_path) -> None:
    db_path = tmp_path / "table-review-fast.duckdb"
    generated = generate_mock_snapshot(
        output=db_path,
        query_count=80,
        table_count=45,
        label="fast table review",
    )

    report = load_cluster_report(db_path, areas=["table_review"])

    assert len(report.table_review) == generated.table_rows
    assert report.load_errors == ()
    assert {"table_attention_score", "scan_query_count", "source_db", "table_name"}.issubset(
        report.table_review.columns
    )
    assert report.table_risk.empty
