import os

import pandas as pd

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from analyzer.sql_lens import analyze_console_sql
from analyzer.widgets.triage_home import _GroupQueryHistoryDialog, _QueryVitalsDialog


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_pattern_history_explode_views_is_highlighted_and_reversible() -> None:
    app = _app()
    original = "SELECT o.id FROM public.v_outer o"
    dialog = _GroupQueryHistoryDialog(
        {"repeat_group_id": "RQ024", "query_count": 1},
        pd.DataFrame([{"query_id": "1001", "sql_text_full": original, "elapsed_s": 320.0}]),
        view_definitions=pd.DataFrame(
            [
                {
                    "schema_name": "public",
                    "view_name": "v_outer",
                    "view_definition": "SELECT i.id FROM public.v_inner i",
                },
                {
                    "schema_name": "public",
                    "view_name": "v_inner",
                    "view_definition": "SELECT id FROM public.base_table",
                },
            ]
        ),
    )
    app.processEvents()

    dialog._explode_btn.click()
    app.processEvents()

    assert dialog._explode_btn.text() == "Unexplode Views"
    assert "public.base_table" in dialog._sql_view.toPlainText()
    highlights = dialog._sql_view.extraSelections()
    assert len(highlights) == 2
    assert all(selection.cursor.selectedText().startswith("(") for selection in highlights)
    assert highlights[0].format.background().color().name().upper() == "#FFF59D"
    assert highlights[0].format.background().color() != highlights[1].format.background().color()

    dialog._explode_btn.click()
    app.processEvents()

    assert dialog._explode_btn.text() == "Explode Views"
    assert dialog._sql_view.toPlainText() == original
    assert dialog._sql_view.extraSelections() == []


def test_pattern_history_selecting_another_run_resets_explosion_state() -> None:
    app = _app()
    dialog = _GroupQueryHistoryDialog(
        {"repeat_group_id": "RQ024", "query_count": 2},
        pd.DataFrame(
            [
                {"query_id": "1001", "sql_text_full": "SELECT * FROM public.v_outer", "elapsed_s": 400.0},
                {"query_id": "1002", "sql_text_full": "SELECT * FROM public.base_table", "elapsed_s": 300.0},
            ]
        ),
        view_definitions=pd.DataFrame(
            [{"schema_name": "public", "view_name": "v_outer", "view_definition": "SELECT * FROM public.base_table"}]
        ),
    )
    app.processEvents()
    rows_by_qid = {
        dialog._table.item(row, 0).text(): row
        for row in range(dialog._table.rowCount())
    }
    dialog._table.selectRow(rows_by_qid["1001"])
    app.processEvents()
    dialog._explode_btn.click()
    app.processEvents()
    assert dialog._explode_btn.text() == "Unexplode Views"

    dialog._table.selectRow(rows_by_qid["1002"])
    app.processEvents()

    assert dialog._explode_btn.text() == "Explode Views"
    assert dialog._original_sql is None
    assert dialog._sql_view.extraSelections() == []


def test_query_vitals_reports_join_sides_and_sortkey_intelligence() -> None:
    app = _app()
    metadata = pd.DataFrame(
        [
            {
                "source_db": "edw", "schema_name": "sales", "table_name": "orders",
                "diststyle": "KEY(customer_id)", "sortkey1": "customer_id",
                "size_mb": 20_000, "tbl_rows": 500_000_000,
            },
            {
                "source_db": "edw", "schema_name": "sales", "table_name": "customers",
                "diststyle": "KEY(customer_id)", "sortkey1": "customer_id",
                "size_mb": 5_000, "tbl_rows": 80_000_000,
            },
        ]
    )
    analysis = analyze_console_sql(
        "SELECT * FROM sales.orders o JOIN sales.customers c "
        "ON o.customer_id = c.customer_id WHERE o.customer_id = 42",
        metadata,
    )
    dialog = _QueryVitalsDialog("1001", analysis)
    app.processEvents()

    assert dialog._joins.rowCount() == 1
    assert dialog._joins.item(0, 1).text() == "o.customer_id"
    assert dialog._joins.item(0, 2).text() == "c.customer_id"
    assert dialog._joins.item(0, 4).text() == "OPTIMAL"
    assert dialog._predicates.rowCount() >= 1
    assert "customer_id" in dialog._predicates.item(0, 4).text()
    assert "SORT KEY" in dialog._predicates.item(0, 5).text().upper()
