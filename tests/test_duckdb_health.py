"""DuckDB warehouse health check.

The recurring failure mode in this app has been work that cannot report its own
state: a warehouse that looks loaded but has no SQL text, or rows filed under a
placeholder namespace, produce empty screens with no explanation. These checks
turn that into a named cause.
"""
from __future__ import annotations

import os

import duckdb
import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from analyzer.duckdb_health import (
    FAIL,
    OK,
    WARN,
    check_warehouse,
    format_report,
)


def _warehouse(path, *, sql_text="SELECT 1", namespace="ns-real", rows=1):
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE query_history(query_id BIGINT, namespace_id VARCHAR)")
    con.execute("CREATE TABLE query_text(query_id BIGINT, sql_text VARCHAR)")
    con.execute("CREATE TABLE svv_table_info_all(table_name VARCHAR)")
    for index in range(rows):
        con.execute("INSERT INTO query_history VALUES (?, ?)", [index, namespace])
        con.execute("INSERT INTO query_text VALUES (?, ?)", [index, sql_text])
    con.execute("INSERT INTO svv_table_info_all VALUES ('t')")
    con.close()
    return path


def _check(report, name):
    return next((c for c in report.checks if c.name == name), None)


def test_missing_file_fails_with_a_usable_message(tmp_path):
    report = check_warehouse(tmp_path / "absent.duckdb")

    assert report.status == FAIL
    assert "No file" in report.checks[0].detail
    assert report.checks[0].advice


def test_healthy_warehouse_passes_every_check(tmp_path):
    report = check_warehouse(_warehouse(tmp_path / "good.duckdb"))

    assert report.status in {OK, WARN}
    assert _check(report, "Core tables").status == OK
    assert _check(report, "SQL text").status == OK
    assert _check(report, "Read test").status == OK


def test_captured_rows_without_sql_text_are_a_named_failure(tmp_path):
    """The single most common cause of an empty triage screen.

    Everything else can load and the bubbles still cannot be built, so this
    must be reported as a cause rather than left to look like 'no results'.
    """
    report = check_warehouse(_warehouse(tmp_path / "nosql.duckdb", sql_text=""))

    check = _check(report, "SQL text")
    assert check.status == FAIL
    assert "0 of" in check.detail
    assert "grouping" in check.advice.lower()
    assert report.status == FAIL


def test_placeholder_namespace_is_caught(tmp_path):
    """REPLACE-* namespace ids file every captured row under a fake cluster."""
    report = check_warehouse(
        _warehouse(tmp_path / "ph.duckdb", namespace="REPLACE-PRODUCER-NAMESPACE-ID")
    )

    check = _check(report, "Cluster identity")
    assert check.status == FAIL
    assert "placeholder" in check.detail.lower()
    assert "redshift_cluster_profiles.json" in check.advice


def test_empty_database_reports_missing_core_tables(tmp_path):
    path = tmp_path / "empty.duckdb"
    duckdb.connect(str(path)).close()

    report = check_warehouse(path)

    assert report.status == FAIL
    assert _check(report, "Core tables").status == FAIL
    assert "query_history" in _check(report, "Core tables").detail


def test_check_is_read_only_and_leaves_the_file_unchanged(tmp_path):
    path = _warehouse(tmp_path / "ro.duckdb")
    before = path.read_bytes()

    check_warehouse(path)

    assert path.read_bytes() == before, "a health check must never modify the warehouse"


def test_report_formats_as_plain_text(tmp_path):
    report = check_warehouse(_warehouse(tmp_path / "fmt.duckdb"))

    text = format_report(report)

    assert "DuckDB health check" in text
    assert str(report.path) in text
    for check in report.checks:
        assert check.name in text


def test_health_button_is_in_the_duckdb_panel_and_opens_a_report(tmp_path, monkeypatch):
    from PySide6.QtWidgets import QApplication, QPushButton

    app = QApplication.instance() or QApplication([])
    _ = app
    import analyzer.widgets.cluster_dashboard as module

    captured = {}

    class _Fake:
        def __init__(self, report, parent=None):
            captured["report"] = report

        def exec(self):
            return 0

    monkeypatch.setattr(module, "_HealthCheckDialog", _Fake)

    dashboard = module.ClusterDashboard()
    # The button lives in the DuckDB Tools popup, not on the permanent row -
    # the row was costing vertical space that the analysis content needed.
    assert dashboard._health_btn.text() == "Health"
    assert dashboard._health_btn in dashboard._duckdb_tool_buttons
    assert not dashboard._health_btn.isVisibleTo(dashboard), "should be popup-only"
    # The inline row keeps only the entry point to the popup.
    labels = [button.text() for button in dashboard.findChildren(QPushButton)]
    assert any("DuckDB Tools" in text for text in labels)

    # The panel row must still fit a 1280px viewport without a horizontal page
    # scrollbar; adding this button pushed it to 1356px before it was fixed.
    assert dashboard.minimumSizeHint().width() <= 1276

    dashboard._path.setText(str(_warehouse(tmp_path / "click.duckdb")))
    dashboard._health_btn.click()

    assert "report" in captured, "clicking Health Check must produce a report"
    assert captured["report"].checks
    assert QApplication.overrideCursor() is None, "wait cursor must be restored"


def _schema_only(path, *, snapshots: int = 0, rows: int = 0):
    """A warehouse with the schema in place, optionally loaded."""
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE query_history(query_id BIGINT, namespace_id VARCHAR)")
    con.execute("CREATE TABLE query_text(query_id BIGINT, sql_text VARCHAR)")
    con.execute("CREATE TABLE svv_table_info_all(table_name VARCHAR)")
    con.execute("CREATE TABLE snapshot_runs(snapshot_id VARCHAR, captured_at TIMESTAMP)")
    for index in range(snapshots):
        con.execute("INSERT INTO snapshot_runs VALUES (?, NULL)", [f"s{index}"])
    for index in range(rows):
        con.execute("INSERT INTO query_history VALUES (?, 'ns')", [index])
        con.execute("INSERT INTO query_text VALUES (?, 'SELECT 1')", [index])
        con.execute("INSERT INTO svv_table_info_all VALUES ('t')")
    con.close()
    return path


def test_a_brand_new_install_is_healthy_not_broken(tmp_path):
    """Schema present, nothing captured, no loads run - that is correct.

    Reporting FAIL here would teach users to ignore the health check on the
    one run where they are most likely to look at it.
    """
    report = check_warehouse(_schema_only(tmp_path / "fresh.duckdb"))

    assert report.status == OK
    assert _check(report, "Core tables").status == OK
    assert "new warehouse" in _check(report, "Core tables").detail.lower()


def test_a_load_that_captured_nothing_still_fails(tmp_path):
    """The distinction that makes the fresh-install exemption safe: once a
    load has run, empty core tables are a real failure."""
    report = check_warehouse(
        _schema_only(tmp_path / "broken.duckdb", snapshots=1, rows=0)
    )

    assert report.status == FAIL
    assert _check(report, "Core tables").status == FAIL
    assert "captured nothing" in _check(report, "Core tables").advice


def test_a_loaded_warehouse_reports_its_counts(tmp_path):
    report = check_warehouse(
        _schema_only(tmp_path / "loaded.duckdb", snapshots=1, rows=5)
    )

    assert _check(report, "Core tables").status == OK
    assert "query_history 5" in _check(report, "Core tables").detail
