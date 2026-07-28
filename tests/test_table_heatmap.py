from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QLabel, QScrollArea

from analyzer.app import MainWindow
from analyzer.cluster_analyze import load_cluster_report
from analyzer.duckdb_store import DuckDBStore
from analyzer.mock_data import generate_mock_snapshot
from analyzer.widgets.table_heatmap import (
    TableHeatMap,
    _HeatMapCanvas,
    _aggregate_external_metadata,
    _distribution_health,
    _filter_external_heatmap_rows,
    _filter_heatmap_rows,
    _metric_severity,
    _sort_health,
    _statistics_alert,
    _statistics_fresh_pct,
    _tile_healths,
)


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "namespace_id": "producer-ns", "cluster_name": "Core Producer",
                "source_db": "prod", "schema_name": "sales", "table_name": "large_good",
                "size_mb": 20, "tbl_rows": 2_000_000, "sortkey1": "order_date",
                "sorted_pct": 98, "diststyle": "KEY(customer_id)", "skew_rows": 1.1, "stats_off": 2,
            },
            {
                "namespace_id": "producer-ns", "cluster_name": "Core Producer",
                "source_db": "prod", "schema_name": "sales", "table_name": "too_small",
                "size_mb": 5, "tbl_rows": 2_000_000, "sortkey1": "",
                "sorted_pct": 70, "diststyle": "EVEN", "skew_rows": 3.0, "stats_off": 30,
            },
            {
                "namespace_id": "consumer-ns", "cluster_name": "FAR",
                "source_db": "warehouse", "schema_name": "finance", "table_name": "too_few_rows",
                "size_mb": 30, "tbl_rows": 500_000, "sortkey1": "AUTO(SORTKEY)",
                "sorted_pct": 88, "diststyle": "AUTO(EVEN)", "skew_rows": 1.5, "stats_off": 12,
            },
        ]
    )


def _external_metadata() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "namespace_id": "producer-ns",
                "cluster_name": "Core Producer",
                "source_db": "dev",
                "redshift_database_name": "warehouse",
                "schema_name": "spectrum",
                "table_name": "partitioned_sales",
                "external_table_key": "warehouse.spectrum.partitioned_sales",
                "column_name": "sale_id",
                "data_type": "bigint",
                "column_number": 1,
                "partition_key_ordinal": 0,
                "is_nullable": "false",
            },
            {
                "namespace_id": "producer-ns",
                "cluster_name": "Core Producer",
                "source_db": "dev",
                "redshift_database_name": "warehouse",
                "schema_name": "spectrum",
                "table_name": "partitioned_sales",
                "external_table_key": "warehouse.spectrum.partitioned_sales",
                "column_name": "sale_date",
                "data_type": "date",
                "column_number": 2,
                "partition_key_ordinal": 1,
                "is_nullable": "true",
            },
            {
                "namespace_id": "producer-ns",
                "cluster_name": "Core Producer",
                "source_db": "dev",
                "redshift_database_name": "warehouse",
                "schema_name": "raw",
                "table_name": "unpartitioned_events",
                "external_table_key": "warehouse.raw.unpartitioned_events",
                "column_name": "payload",
                "data_type": "varchar(65535)",
                "column_number": 1,
                "partition_key_ordinal": 0,
                "is_nullable": "true",
            },
        ]
    )


def test_default_filters_are_ten_mb_and_one_million_rows(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    _app()
    heatmap = TableHeatMap()

    assert heatmap._size_value.value() == 10
    assert heatmap._rows_value.value() == 1_000_000
    assert [heatmap._metric.itemText(i) for i in range(heatmap._metric.count())] == [
        "Composite View — All Attributes",
        "Distribution Only",
        "Sort Only",
    ]


def test_size_and_row_thresholds_are_both_required() -> None:
    filtered = _filter_heatmap_rows(
        _sample(),
        metric="sort_key",
        min_size_mb=10,
        min_rows=1_000_000,
        problems_only=False,
    )

    assert list(filtered["table_name"]) == ["large_good"]


def test_problems_only_applies_to_selected_metric() -> None:
    filtered = _filter_heatmap_rows(
        _sample(),
        metric="distribution",
        min_size_mb=0,
        min_rows=0,
        problems_only=True,
    )

    assert set(filtered["table_name"]) == {"too_small", "too_few_rows"}
    assert _metric_severity(_sample().iloc[0], "distribution") == 0


def test_scope_filters_cluster_database_and_schema_together() -> None:
    filtered = _filter_heatmap_rows(
        _sample(),
        metric="combined_health",
        min_size_mb=0,
        min_rows=0,
        problems_only=False,
        cluster="consumer-ns",
        database="warehouse",
        schema="finance",
    )

    assert list(filtered["table_name"]) == ["too_few_rows"]


def test_scope_controls_use_friendly_cluster_names_and_cascade(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    _app()
    heatmap = TableHeatMap()
    heatmap._size_value.setValue(0)
    heatmap._rows_value.setValue(0)
    heatmap.set_report(
        SimpleNamespace(db_path=tmp_path / "scope.duckdb", table_heatmap=_sample())
    )

    cluster_labels = [
        heatmap._cluster_filter.itemText(index)
        for index in range(heatmap._cluster_filter.count())
    ]
    assert "Core Producer — producer-ns" in cluster_labels
    assert "FAR — consumer-ns" in cluster_labels

    heatmap._cluster_filter.setCurrentIndex(
        heatmap._cluster_filter.findData("consumer-ns")
    )
    assert [
        heatmap._database_filter.itemText(index)
        for index in range(heatmap._database_filter.count())
    ] == ["All databases", "warehouse"]
    assert [
        heatmap._schema_filter.itemText(index)
        for index in range(heatmap._schema_filter.count())
    ] == ["All schemas", "finance"]
    assert list(heatmap._canvas._frame["table_name"]) == ["too_few_rows"]


def test_composite_square_is_twice_original_size_and_uses_requested_thresholds() -> None:
    assert _HeatMapCanvas.SQUARE == 24
    green = pd.Series({
        "diststyle": "KEY(customer_id)", "skew_rows": 2.0,
        "sortkey1": "order_date", "sorted_pct": 91,
    })
    yellow = pd.Series({
        "diststyle": "KEY(customer_id)", "skew_rows": 2.01,
        "sortkey1": "order_date", "sorted_pct": 90,
    })
    red = pd.Series({
        "diststyle": "AUTO(EVEN)", "skew_rows": 1.0,
        "sortkey1": "", "sorted_pct": 99,
    })

    assert (_distribution_health(green), _sort_health(green)) == (0, 0)
    assert (_distribution_health(yellow), _sort_health(yellow)) == (1, 1)
    assert (_distribution_health(red), _sort_health(red)) == (2, 2)


def test_statistics_exclamation_uses_freshness_not_raw_stats_off() -> None:
    fresh = pd.Series({"stats_off": 39.9})
    boundary = pd.Series({"stats_off": 40})
    stale = pd.Series({"stats_off": 75})

    assert _statistics_fresh_pct(fresh) == 60.1
    assert not _statistics_alert(fresh)
    assert _statistics_fresh_pct(boundary) == 60
    assert _statistics_alert(boundary)
    assert _statistics_alert(stale)


def test_focus_modes_use_full_tile_health_and_keep_stats_alert_independent() -> None:
    row = pd.Series({
        "diststyle": "KEY(customer_id)", "skew_rows": 1.1,
        "sortkey1": "order_date", "sorted_pct": 40,
        "stats_off": 50,
    })

    assert _tile_healths(row, "combined_health") == (0, 2)
    assert _tile_healths(row, "distribution") == (0, 0)
    assert _tile_healths(row, "sort_key") == (2, 2)
    assert _statistics_alert(row)


def test_heatmap_legend_is_above_canvas_and_explains_both_halves(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    _app()
    heatmap = TableHeatMap()

    labels = [label.text() for label in heatmap._legend.findChildren(type(heatmap._status))]
    assert "TOP HALF — DISTRIBUTION HEALTH" in labels
    assert "BOTTOM HALF — SORT HEALTH" in labels
    assert heatmap.layout().indexOf(heatmap._legend) < heatmap.layout().indexOf(heatmap._scroll)


def test_external_metadata_aggregates_to_binary_partition_tiles_and_tooltip() -> None:
    tables = _aggregate_external_metadata(_external_metadata())
    assert list(tables["table_name"]) == [
        "unpartitioned_events",
        "partitioned_sales",
    ]
    partitioned = tables[tables["table_name"] == "partitioned_sales"].iloc[0]
    plain = tables[tables["table_name"] == "unpartitioned_events"].iloc[0]
    assert bool(partitioned["partition_present"])
    assert partitioned["partition_key_columns"] == "sale_date"
    assert partitioned["sortkey"] == "sale_date"
    assert partitioned["column_count"] == 2
    assert not bool(plain["partition_present"])

    tooltip = _HeatMapCanvas._tooltip(partitioned)
    assert "PARTITIONED (blue)" in tooltip
    assert "Partition key(s): sale_date (1)" in tooltip
    assert "Sort-key equivalent: sale_date" in tooltip
    assert "Columns: 2" in tooltip
    assert "Data types: bigint, date" in tooltip
    assert "Producer SVV_EXTERNAL_COLUMNS" in tooltip


def test_external_heatmap_mode_keeps_cluster_database_schema_filters(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    _app()
    heatmap = TableHeatMap()
    heatmap.set_report(
        SimpleNamespace(
            db_path=tmp_path / "scope.duckdb",
            table_heatmap=_sample(),
            external_table_metadata=_external_metadata(),
        )
    )
    heatmap._view_mode.setCurrentIndex(
        heatmap._view_mode.findData("external")
    )

    assert not heatmap._physical_controls.isVisible()
    assert not heatmap._external_legend.isHidden()
    assert heatmap._canvas._metric == "external_partition"
    assert set(heatmap._canvas._frame["table_name"]) == {
        "partitioned_sales",
        "unpartitioned_events",
    }
    assert [
        heatmap._database_filter.itemText(index)
        for index in range(heatmap._database_filter.count())
    ] == ["All databases", "warehouse"]

    heatmap._schema_filter.setCurrentIndex(
        heatmap._schema_filter.findData("raw")
    )
    assert list(heatmap._canvas._frame["table_name"]) == [
        "unpartitioned_events"
    ]
    assert "1 partitioned (blue)" not in heatmap._status.text()
    assert "1 not partitioned (orange)" in heatmap._status.text()

    filtered = _filter_external_heatmap_rows(
        _aggregate_external_metadata(_external_metadata()),
        cluster="producer-ns",
        database="warehouse",
        schema="spectrum",
    )
    assert list(filtered["table_name"]) == ["partitioned_sales"]


def test_heatmap_report_loads_all_table_info_rows_without_review_limit(tmp_path) -> None:
    path = tmp_path / "heatmap.duckdb"
    generated = generate_mock_snapshot(
        output=path,
        query_count=8,
        table_count=1_025,
        label="heatmap test",
    )

    report = load_cluster_report(path, areas=["table_heatmap"])

    assert len(report.table_heatmap) == generated.table_rows == 1_025
    assert {"source_db", "schema_name", "table_name", "sorted_pct", "diststyle", "skew_rows", "stats_off"}.issubset(
        report.table_heatmap.columns
    )


def test_table_heatmap_area_loads_external_metadata_from_producer_only(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("REDSHIFT_NAMESPACE", "producer-ns")
    path = tmp_path / "external-heatmap.duckdb"
    store = DuckDBStore(path)
    metadata = pd.concat(
        [
            _external_metadata(),
            _external_metadata().assign(
                namespace_id="consumer-ns",
                cluster_name="FAR",
                table_name="consumer_copy",
            ),
        ],
        ignore_index=True,
    )
    with store.connect() as con:
        run = store.new_snapshot("external heatmap")
        store.record_snapshot(con, run, source="test")
        store.replace_table_from_frame(
            con,
            "external_table_metadata",
            metadata,
            run,
        )

    report = load_cluster_report(path, areas=["table_heatmap"])

    assert len(report.external_table_metadata) == len(_external_metadata())
    assert set(report.external_table_metadata["namespace_id"]) == {
        "producer-ns"
    }
    assert "table_heatmap" in report.loaded_areas
    assert any(
        "Producer-only SVV_EXTERNAL_COLUMNS" in note
        for note in report.notes
    )


def test_table_heatmap_is_the_fourth_top_level_tab(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(tmp_path))
    _app()
    window = MainWindow()

    assert [window._tabs.tabText(i) for i in range(window._tabs.count())] == [
        "Load Status",
        "Data Loader",
        "Workload Triage",
        "Table Heat Map",
        "Fix Queue",
        "Single Query Analysis",
    ]
    assert window._table_heatmap_tab == 3
    assert window._action_plan_tab == 4
    assert window._single_query_tab == 5
    # The Fix Queue page was built and fed data but never mounted, so the
    # triage screen pointed at a tab that did not exist.
    assert window._action_plan is window._cluster.action_plan_page()
    assert window._table_heatmap is window._cluster.table_heatmap_page()
    # The dashboard no longer wraps its single page in its own QTabWidget - the
    # main window's tab already names it, and the inner bar was a tab inside a
    # tab that cost vertical space the bubble chart needed.
    assert not hasattr(window._cluster, "_tabs")
    assert window._data_loader._credentials_callback.__name__ == "_edit_local_credentials"
    window.deleteLater()


def test_main_tabs_fit_1280_viewport_without_horizontal_page_scroll(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(tmp_path))
    _app()
    window = MainWindow()
    window.show()
    QApplication.processEvents()
    window.setGeometry(0, 0, 1280, 720)
    window._tabs.blockSignals(True)
    try:
        for index in range(window._tabs.count()):
            window._tabs.setCurrentIndex(index)
            QApplication.processEvents()
            scroll = window._tabs.widget(index)
            assert isinstance(scroll, QScrollArea)
            assert (
                scroll.horizontalScrollBarPolicy()
                == Qt.ScrollBarAlwaysOff
            )
            assert scroll.widget().minimumSizeHint().width() <= 1276
    finally:
        window._tabs.blockSignals(False)
        window.close()
        window.deleteLater()


def test_opening_fourth_tab_lazy_loads_heatmap(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(tmp_path))
    _app()
    window = MainWindow()
    load_calls: list[bool] = []
    window._cluster.load_table_heatmap_if_needed = lambda: load_calls.append(True)

    window._tabs.setCurrentIndex(window._table_heatmap_tab)

    assert load_calls == [True]
    window.deleteLater()


def test_workload_refresh_opens_main_data_loader_tab(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(tmp_path))
    _app()
    window = MainWindow()

    window._cluster._open_refresh_source()

    assert window._tabs.currentIndex() == window._data_loader_tab
    window.deleteLater()


def test_heatmap_thresholds_show_thousands_separators() -> None:
    """Large row/size thresholds must be readable, not a run of bare digits.

    A twelve-digit row threshold rendered as "999999999999 rows" is unreadable
    at a glance, which is precisely when these filters are being tuned.
    """
    _app()
    from analyzer.widgets.table_heatmap import TableHeatMap

    page = TableHeatMap()

    page._rows_value.setValue(999_999_999_999)
    assert page._rows_value.lineEdit().text() == "999,999,999,999 rows"

    page._size_value.setValue(1_000_000)
    assert page._size_value.lineEdit().text() == "1,000,000 MB"

    # The separator must not break the filter: the value still round-trips as
    # a number, and the log-scale slider stays in sync with it.
    page._rows_value.setValue(2_000_000)
    assert page._rows_value.value() == 2_000_000
    assert page._rows_slider.value() == page._rows_to_slider(2_000_000)


def test_external_partition_tiles_are_blue_and_orange(monkeypatch, tmp_path) -> None:
    """Partitioned external tables read blue; unpartitioned read orange.

    Deliberately outside the green/amber/red health ramp the physical heat map
    uses - an unpartitioned external table is a design fact to notice, not a
    severity score, and the health colours read as "this table is failing".
    """
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    _app()
    from analyzer.widgets.table_heatmap import (
        _EXTERNAL_PARTITIONED,
        _EXTERNAL_UNPARTITIONED,
        _health_color,
    )

    heatmap = TableHeatMap()
    heatmap.set_report(
        SimpleNamespace(
            db_path=tmp_path / "colors.duckdb",
            table_heatmap=_sample(),
            external_table_metadata=_external_metadata(),
        )
    )
    heatmap._view_mode.setCurrentIndex(heatmap._view_mode.findData("external"))

    assert heatmap._canvas._metric == "external_partition"

    # The partition palette must not collide with the health ramp.
    ramp = {_health_color(severity).name().lower() for severity in (0, 1, 2)}
    assert _EXTERNAL_PARTITIONED.lower() not in ramp
    assert _EXTERNAL_UNPARTITIONED.lower() not in ramp

    frame = heatmap._canvas._frame
    partitioned = frame[frame["table_name"] == "partitioned_sales"].iloc[0]
    plain = frame[frame["table_name"] == "unpartitioned_events"].iloc[0]
    assert bool(partitioned["partition_present"])
    assert not bool(plain["partition_present"])

    legend_colors = heatmap._external_legend.findChildren(QLabel)
    styles = " ".join(label.styleSheet() for label in legend_colors)
    assert _EXTERNAL_PARTITIONED in styles, "legend must show the blue swatch"
    assert _EXTERNAL_UNPARTITIONED in styles, "legend must show the orange swatch"
