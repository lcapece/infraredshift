from __future__ import annotations

import duckdb

from analyzer.cluster_analyze import (
    _table_review_zero_row_diagnostic,
    load_cluster_report,
)
from analyzer.mock_data import generate_mock_snapshot


def test_zero_row_diagnostic_counts_promoted_snapshot_and_staging_rows() -> None:
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE svv_table_info_all(snapshot_id VARCHAR, table_name VARCHAR)"
        )
        con.execute(
            "INSERT INTO svv_table_info_all VALUES ('old', 'one'), ('old', 'two')"
        )
        con.execute(
            "CREATE TABLE svv_table_info_all_tmp(snapshot_id VARCHAR, table_name VARCHAR)"
        )
        con.execute(
            "INSERT INTO svv_table_info_all_tmp VALUES "
            "('new', 'one'), ('new', 'two'), ('new', 'three')"
        )

        message, counts = _table_review_zero_row_diagnostic(con, "new")
    finally:
        con.close()

    assert counts == {
        "table_review_direct_source_rows": 2,
        "table_review_direct_selected_snapshot_rows": 0,
        "table_review_direct_staging_rows": 3,
    }
    assert "svv_table_info_all has 2 total row(s)" in message
    assert "0 row(s) match selected snapshot new" in message
    assert "svv_table_info_all_tmp has 3 staged row(s)" in message
    assert "unswapped *_tmp load" in message


def test_zero_row_diagnostic_tolerates_missing_staging_table() -> None:
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE svv_table_info_all(snapshot_id VARCHAR, table_name VARCHAR)"
        )
        message, counts = _table_review_zero_row_diagnostic(con, "new")
    finally:
        con.close()

    assert counts["table_review_direct_source_rows"] == 0
    assert counts["table_review_direct_selected_snapshot_rows"] == 0
    assert "svv_table_info_all_tmp" not in message
    assert "promoted source table also contains zero rows" in message


def test_report_load_falls_back_to_latest_promoted_catalog_snapshot(tmp_path) -> None:
    db_path = tmp_path / "staged.duckdb"
    generated = generate_mock_snapshot(
        output=db_path,
        query_count=8,
        table_count=6,
        label="promoted snapshot",
    )
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            "CREATE TABLE svv_table_info_all_tmp AS SELECT * FROM svv_table_info_all"
        )
        con.execute(
            "UPDATE svv_table_info_all_tmp SET snapshot_id = 'staged-snapshot'"
        )
        con.execute(
            "INSERT INTO snapshot_runs VALUES "
            "('staged-snapshot', CURRENT_TIMESTAMP + INTERVAL 1 HOUR, "
            "'staged snapshot', 'test')"
        )
    finally:
        con.close()

    report = load_cluster_report(db_path, areas=["table_review"])

    assert len(report.table_review) == generated.table_rows
    assert not report.load_errors


def test_report_load_uses_promoted_catalog_rows_with_blank_snapshot_ids(tmp_path) -> None:
    db_path = tmp_path / "blank-catalog-snapshot.duckdb"
    generated = generate_mock_snapshot(
        output=db_path,
        query_count=8,
        table_count=6,
        label="catalog snapshot",
    )
    con = duckdb.connect(str(db_path))
    try:
        con.execute("UPDATE svv_table_info_all SET snapshot_id = NULL")
        con.execute(
            "INSERT INTO snapshot_runs VALUES "
            "('query-repair-anchor', CURRENT_TIMESTAMP + INTERVAL 1 HOUR, "
            "'query repair', 'test')"
        )
    finally:
        con.close()

    report = load_cluster_report(db_path, areas=["table_review"])

    assert len(report.table_review) == generated.table_rows
    assert not report.load_errors
