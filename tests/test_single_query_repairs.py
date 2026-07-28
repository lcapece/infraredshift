from __future__ import annotations

from datetime import datetime, timezone
import time

import duckdb
import pandas as pd
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QPushButton

from analyzer.duckdb_store import DuckDBStore
import analyzer.widgets.cluster_dashboard as dashboard
from analyzer.query_optimizer import optimize_redshift_sql
from analyzer.sql_xray import build_view_map, explode_views_recursive_with_spans
from analyzer.widgets.cluster_dashboard import (
    _SqlLensPage,
    _extract_subquery_rows,
    _fallback_subquery_rows,
    _queries_with_views_frame,
)
from analyzer.widgets.login_dialog import _published_label, _release_version


VIEW_ROWS = [
    {
        "database": "prod",
        "schema": "public",
        "view_name": "v_inner",
        "source_definition": "SELECT id, amount FROM public.fact_sales",
    },
    {
        "database": "prod",
        "schema": "public",
        "view_name": "v_outer",
        "source_definition": "SELECT * FROM public.v_inner WHERE amount > 0",
    },
]


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _button(page: _SqlLensPage, text: str) -> QPushButton:
    return next(button for button in page.findChildren(QPushButton) if button.text() == text)


def _wait_for(predicate, timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    app = _app()
    while time.monotonic() < deadline:
        app.processEvents()
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("Timed out waiting for Qt action")


def test_login_publication_label_uses_build_datetime() -> None:
    stamp = datetime(2026, 7, 14, 15, 42, tzinfo=timezone.utc)

    assert _published_label(stamp) == "Published: July 14, 2026 at 03:42 PM UTC"


def test_login_release_label_shows_immutable_build_id(monkeypatch) -> None:
    stamp = datetime(2026, 7, 24, 15, 42, tzinfo=timezone.utc)
    monkeypatch.setenv("INFRAREDSHIFT_BUILD_ID", "0123456789abcdef")

    assert _release_version(stamp) == "RC-60724.15 · 01234567"


def test_startup_recovers_orphan_view_definitions_tmp(tmp_path) -> None:
    path = tmp_path / "orphan.duckdb"
    raw = duckdb.connect(str(path))
    try:
        raw.execute(
            "CREATE TABLE view_definitions_tmp("
            "snapshot_id VARCHAR, database VARCHAR, schema VARCHAR, "
            "view_name VARCHAR, source_definition VARCHAR)"
        )
        raw.execute(
            "INSERT INTO view_definitions_tmp VALUES "
            "('snap', 'prod', 'public', 'v_orders', 'SELECT 1')"
        )
    finally:
        raw.close()

    store = DuckDBStore(path)
    with store.connect() as con:
        assert con.execute("SELECT COUNT(*) FROM view_definitions").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM view_definitions_tmp").fetchone()[0] == 1


def test_queries_with_views_builds_pick_list_with_counts() -> None:
    known = pd.DataFrame(
        [
            {
                "query_id": 101,
                "sql_text": (
                    "SELECT * FROM public.v_outer a "
                    "JOIN public.v_outer b ON a.id = b.id"
                ),
            },
            {"query_id": 102, "sql_text": "SELECT * FROM public.fact_sales"},
        ]
    )

    rows = _queries_with_views_frame(known, build_view_map(VIEW_ROWS))

    assert list(rows["query_id"]) == ["101"]
    assert list(rows["view_count"]) == [2]
    assert list(rows["views"]) == ["public.v_outer"]


def test_fallback_subquery_extraction_survives_unparseable_outer_sql() -> None:
    sql = "BROKEN OUTER SYNTAX (SELECT id FROM public.events WHERE id IN (SELECT id FROM audit)) trailing"

    rows = _fallback_subquery_rows(sql)

    assert len(rows) == 2
    assert len(_extract_subquery_rows(sql)) == 2
    assert set(rows["kind"]) == {"subquery"}
    assert any("FROM audit" in text for text in rows["sql_text"])


def test_expanded_nested_views_parse_and_fixer_accepts_them() -> None:
    sql = "SELECT * FROM public.v_outer v"
    expanded, exploded, _spans = explode_views_recursive_with_spans(
        sql, build_view_map(VIEW_ROWS)
    )

    assert exploded == ["public.v_outer", "public.v_inner"]
    assert len(_extract_subquery_rows(expanded)) >= 2
    assert optimize_redshift_sql(expanded, pd.DataFrame()).parse_ok


def test_analyze_button_runs_and_explosion_highlights_each_depth(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    _app()
    page = _SqlLensPage()
    page.set_context(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(VIEW_ROWS))
    page._sql.setPlainText("SELECT * FROM public.v_outer v")

    _button(page, "Explode Views").click()
    highlights = page._sql.extraSelections()
    assert len(highlights) == 2
    assert all(selection.cursor.selectedText().startswith("(") for selection in highlights)
    assert highlights[0].format.background().color() != highlights[1].format.background().color()

    _button(page, "Analyze SQL").click()
    _wait_for(lambda: page._analysis is not None and page._analyze_thread is None)

    assert page._analysis.parse_ok
    assert int(page._analysis.summary["table_count"]) >= 1
    page.deleteLater()


def test_show_lineage_analyzes_then_opens_on_the_first_click(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    _app()
    opened: list[str] = []

    class FakeLineageDialog:
        def __init__(self, source_row, *_args, **_kwargs):
            self.source_row = source_row

        def exec(self):
            opened.append(str(self.source_row.get("query_id") or ""))
            return 0

    monkeypatch.setattr(dashboard, "_SlowQueryLineageDialog", FakeLineageDialog)
    monkeypatch.setattr(dashboard, "_resize_dialog_to_screen", lambda *_args, **_kwargs: None)

    page = _SqlLensPage()
    page._sql.setPlainText("SELECT * FROM public.fact_sales WHERE id IN (SELECT id FROM audit)")

    _button(page, "Show Lineage").click()
    _wait_for(lambda: bool(opened))

    assert opened == ["single query"]
    assert page._analysis is not None and page._analysis.parse_ok
    page.deleteLater()
