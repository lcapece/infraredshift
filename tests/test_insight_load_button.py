from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pandas as pd
from PySide6.QtCore import Qt
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication, QPushButton

from analyzer.widgets.cluster_dashboard import _InsightLedgerPage

_APP = QApplication.instance() or QApplication([])


def _app() -> QApplication:
    return _APP


def test_load_insights_button_emits_and_zero_row_completion_is_visible() -> None:
    _app()
    page = _InsightLedgerPage()
    emitted: list[str] = []
    page.loadRequested.connect(emitted.append)
    button = next(
        child for child in page.findChildren(QPushButton) if child.text() == "Load Insights"
    )

    QTest.mouseClick(button, Qt.LeftButton)
    assert emitted == ["insights"]

    page.show_loading()
    assert "Loading Insight Ledger" in page._status.text()
    page.set_dataframe(pd.DataFrame(), loaded=True)
    assert "0 findings returned" in page._status.text()


def test_insight_ledger_groups_rule_then_repeat_group_then_query_id() -> None:
    _app()
    page = _InsightLedgerPage()
    page.set_sql_lookup(pd.DataFrame([
        {"query_id": 102, "repeat_group_id": "RQ024", "sql_text": "SELECT first_representative FROM fact"},
        {"query_id": 101, "repeat_group_id": "RQ024", "sql_text": "SELECT second_member FROM fact"},
        {"query_id": 201, "repeat_group_id": "RQ030", "sql_text": "SELECT another_group FROM dim"},
    ]))
    page.set_dataframe(pd.DataFrame([
        {
            "insight_id": "Q01_REMOTE_SPILL", "title": "Disk spill in slow query",
            "severity": "crit", "query_id": 101, "impact_score": 100,
            "metric_label": "Spilled Blocks", "metric_display": "10", "evidence": "spill",
        },
        {
            "insight_id": "Q01_REMOTE_SPILL", "title": "Disk spill in slow query",
            "severity": "warn", "query_id": 102, "impact_score": 90,
            "metric_label": "Spilled Blocks", "metric_display": "5", "evidence": "spill",
        },
        {
            "insight_id": "Q01_REMOTE_SPILL", "title": "Disk spill in slow query",
            "severity": "warn", "query_id": 201, "impact_score": 80,
            "metric_label": "Spilled Blocks", "metric_display": "2", "evidence": "spill",
        },
    ]))

    category = page._tree.topLevelItem(0)
    assert category.text(0) == "Disk spill in slow query"
    assert category.childCount() == 2
    rq024 = category.child(0)
    assert rq024.text(0) == "Grouped Query ID RQ024"
    assert rq024.childCount() == 2
    assert rq024.text(5).startswith("SELECT first_representative")
    assert {rq024.child(i).text(0) for i in range(2)} == {"Query ID 101", "Query ID 102"}
