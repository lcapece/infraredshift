from __future__ import annotations

import pandas as pd
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication

from analyzer.cluster_analyze import ClusterReport, load_cluster_report
from analyzer.duckdb_store import DuckDBStore
from analyzer.widgets.external_tables import (
    ExternalTablesPage,
    _ExternalTableModel,
    _filter_external_rows,
    _metric_severity,
    _partition_severity,
)


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _rows() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "external_table_key": "dev.spectrum.orders",
            "table_name": "dev.spectrum.orders",
            "s3_location": "s3://demo/orders/file.parquet",
            "query_count": 12,
            "gross_scan_gb": 250.0,
            "gross_scan_rows": 2_000_000,
            "partition_pruning_pct": 95.0,
            "total_partitions_considered": 100,
            "qualified_partitions_scanned": 5,
            "scanned_files": 500,
            "avg_files_per_segment": 50,
            "external_duration_s": 7200,
            "warning_event_count": 0,
            "observed_file_format": "parquet",
        },
        {
            "external_table_key": "dev.spectrum.events",
            "table_name": "dev.spectrum.events",
            "s3_location": "s3://demo/events/file.json",
            "query_count": 2,
            "gross_scan_gb": 5.0,
            "gross_scan_rows": 20_000,
            "partition_pruning_pct": 20.0,
            "total_partitions_considered": 10,
            "qualified_partitions_scanned": 8,
            "scanned_files": 4000,
            "avg_files_per_segment": 2000,
            "external_duration_s": 120,
            "warning_event_count": 8,
            "observed_file_format": "json",
        },
    ])


def test_external_filters_search_thresholds_and_warning_focus() -> None:
    filtered = _filter_external_rows(
        _rows(), search="events", min_scan_gb=1, min_queries=1, warnings_only=True
    )
    assert list(filtered["table_name"]) == ["dev.spectrum.events"]


def test_external_heat_severity_uses_partition_scan_runtime_files_and_warnings() -> None:
    orders, events = _rows().iloc[0], _rows().iloc[1]
    assert _partition_severity(orders) == 0
    assert _partition_severity(events) == 2
    assert _metric_severity(orders, "scan") == 2
    assert _metric_severity(orders, "runtime") == 2
    assert _metric_severity(events, "files") == 2
    assert _metric_severity(events, "warnings") == 2


def test_external_grid_sorts_numeric_values_not_formatted_text(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    _app()
    model = _ExternalTableModel()
    model.set_frame(_rows())
    scan_column = [key for key, _ in model._columns].index("gross_scan_gb")
    model.sort(scan_column, Qt.DescendingOrder)
    assert list(model._frame["gross_scan_gb"]) == [250.0, 5.0]


def test_external_area_loads_its_independent_snapshot(tmp_path) -> None:
    path = tmp_path / "external-page.duckdb"
    store = DuckDBStore(path)
    with store.connect() as con:
        workload = store.new_snapshot("workload")
        store.record_snapshot(con, workload, source="test")
        external = store.new_snapshot("external")
        store.replace_table_from_frame(con, "external_table_info_all", _rows(), external)
        con.execute(
            "INSERT INTO snapshot_runs VALUES (?, ?, ?, ?)",
            [external.snapshot_id, external.captured_at, "external refresh", "external-table-loader"],
        )

    report = load_cluster_report(path, areas=["external_tables"])
    assert report.snapshot_id == workload.snapshot_id
    assert len(report.external_tables) == 2
    assert set(report.external_tables["table_name"]) == {
        "dev.spectrum.orders", "dev.spectrum.events",
    }


def test_external_page_has_sortable_grid_and_heatmap(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    _app()
    page = ExternalTablesPage()
    page.set_report(ClusterReport(db_path=tmp_path / "x.duckdb", external_tables=_rows()))
    assert [page._tabs.tabText(i) for i in range(page._tabs.count())] == [
        "Metrics Grid", "Heat Map", "Optimization Queue",
    ]
    assert page._grid.isSortingEnabled()
    assert page.has_data()
    assert page._model.rowCount() == 2
    assert page._action_grid.isSortingEnabled()
    assert page._action_model.rowCount() == 2
    assert "dev.spectrum" in page._action_detail.toPlainText()
    page.deleteLater()
