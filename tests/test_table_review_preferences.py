from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pandas as pd
from PySide6.QtWidgets import QApplication

from analyzer.settings import AnalyzerSettings, load_settings, save_settings
from analyzer.widgets import cluster_dashboard
from analyzer.widgets.cluster_dashboard import (
    _TableReviewPage,
    _table_review_intersection_mask,
)

_APP = QApplication.instance() or QApplication([])


def _app() -> QApplication:
    return _APP


def test_table_review_preferences_round_trip(tmp_path) -> None:
    path = tmp_path / "settings.json"
    settings = AnalyzerSettings(
        table_review_visible_cols=["table_name", "schema_name", "source_db"],
        table_review_hide_without_intersection=False,
    )

    save_settings(settings, path)
    loaded = load_settings(path)

    assert loaded.table_review_visible_cols == ["table_name", "schema_name", "source_db"]
    assert loaded.table_review_hide_without_intersection is False
    assert AnalyzerSettings().table_review_hide_without_intersection is True


def test_intersection_mask_uses_query_or_scan_telemetry_and_fails_open() -> None:
    df = pd.DataFrame(
        {
            "table_name": ["slow", "scanned", "unused"],
            "slow_query_count": [2, 0, 0],
            "scan_query_count": [0, 4, 0],
        }
    )

    assert _table_review_intersection_mask(df).tolist() == [True, True, False]
    assert _table_review_intersection_mask(df[["table_name"]]).tolist() == [True, True, True]


def test_table_review_filter_defaults_on_and_column_drag_is_saved(monkeypatch) -> None:
    _app()
    settings = AnalyzerSettings(
        table_review_visible_cols=["source_db", "schema_name", "table_name", "slow_query_count"],
        table_review_hide_without_intersection=True,
    )
    saves: list[list[str]] = []
    monkeypatch.setattr(cluster_dashboard, "load_settings", lambda: settings)
    monkeypatch.setattr(
        cluster_dashboard,
        "save_settings",
        lambda value: saves.append(list(value.table_review_visible_cols)),
    )
    page = _TableReviewPage()
    page.set_dataframe(
        pd.DataFrame(
            [
                {"source_db": "db", "schema_name": "public", "table_name": "used", "slow_query_count": 3},
                {"source_db": "db", "schema_name": "public", "table_name": "unused", "slow_query_count": 0},
            ]
        )
    )

    assert page._hide_without_intersection.isChecked()
    assert page._model is not None and page._model.rowCount() == 1

    page._hide_without_intersection.setChecked(False)
    assert settings.table_review_hide_without_intersection is False
    assert page._model is not None and page._model.rowCount() == 2

    header = page._table.horizontalHeader()
    table_name_logical = page._model.column_index("table_name")
    header.moveSection(header.visualIndex(table_name_logical), 0)

    assert settings.table_review_visible_cols[0] == "table_name"
    assert saves[-1][0] == "table_name"


def test_checked_intersection_filter_never_blanks_a_valid_physical_inventory(monkeypatch) -> None:
    _app()
    settings = AnalyzerSettings(
        table_review_visible_cols=["source_db", "schema_name", "table_name", "scan_query_count"],
        table_review_hide_without_intersection=True,
    )
    monkeypatch.setattr(cluster_dashboard, "load_settings", lambda: settings)
    monkeypatch.setattr(cluster_dashboard, "save_settings", lambda _value: None)
    page = _TableReviewPage()
    page.set_dataframe(
        pd.DataFrame(
            [
                {"source_db": "db", "schema_name": "public", "table_name": "one", "scan_query_count": 0},
                {"source_db": "db", "schema_name": "public", "table_name": "two", "scan_query_count": 0},
            ]
        )
    )

    assert page._model is not None and page._model.rowCount() == 2
    assert page._intersection_filter_fallback is True
    assert "all tables remain visible" in page._status.text()


def test_table_review_renders_nullable_duckdb_strings(monkeypatch) -> None:
    """Nullable DuckDB strings must render instead of evaluating pandas.NA."""
    _app()
    settings = AnalyzerSettings(
        table_review_visible_cols=[
            "source_db",
            "schema_name",
            "table_name",
            "diststyle",
            "distkey",
            "sortkey1",
            "tbl_rows",
        ],
        table_review_hide_without_intersection=False,
    )
    monkeypatch.setattr(cluster_dashboard, "load_settings", lambda: settings)
    monkeypatch.setattr(cluster_dashboard, "save_settings", lambda _value: None)
    page = _TableReviewPage()

    page.set_dataframe(
        pd.DataFrame(
            [
                {
                    "source_db": pd.NA,
                    "schema_name": "public",
                    "table_name": "orders",
                    "diststyle": pd.NA,
                    "distkey": pd.NA,
                    "sortkey1": pd.NA,
                    "tbl_rows": 1_500_000,
                }
            ]
        )
    )

    assert page._model is not None
    assert page._model.rowCount() == 1
    assert page._source_db_filter._values == []
    assert "orders" in page._status.text()
