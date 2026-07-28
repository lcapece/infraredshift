from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pandas as pd
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QTreeWidget

from analyzer.widgets.cluster_dashboard import (
    _SeverityQueryPage,
    _populate_slow_query_tree,
    _slow_query_parent_row,
)

_APP = QApplication.instance() or QApplication([])


def _app() -> QApplication:
    return _APP


def _queries() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"query_id": "101", "repeat_group_id": "RQ001", "database_name": "sales", "user_name": "one", "severity_score": 10, "elapsed_s": 100, "risk_score": 20, "plan_node_count": 4, "tables_touched": 2, "input_rows": 1000, "output_rows": 10, "total_spill": 0, "dominant_issue": "spill"},
            {"query_id": "102", "repeat_group_id": "RQ001", "database_name": "sales", "user_name": "two", "severity_score": 30, "elapsed_s": 300, "risk_score": 40, "plan_node_count": 8, "tables_touched": 4, "input_rows": 3000, "output_rows": 30, "total_spill": 200, "dominant_issue": "spill"},
            {"query_id": "201", "repeat_group_id": "RQ002", "database_name": "ops", "user_name": "one", "severity_score": 15, "elapsed_s": 75, "risk_score": 10, "plan_node_count": 3, "tables_touched": 1, "input_rows": 500, "output_rows": 5, "total_spill": 0, "dominant_issue": "scan"},
            {"query_id": "301", "repeat_group_id": None, "database_name": "ops", "user_name": "three", "severity_score": 5, "elapsed_s": 25, "risk_score": 5, "plan_node_count": 2, "tables_touched": 1, "input_rows": 100, "output_rows": 1, "total_spill": 0, "dominant_issue": "scan"},
        ]
    )


def test_repeat_group_parent_averages_individual_numeric_values() -> None:
    parent = _slow_query_parent_row("RQ001", _queries().iloc[:2])

    assert parent["severity_score"] == 20
    assert parent["elapsed_s"] == 200
    assert parent["risk_score"] == 30
    assert parent["plan_node_count"] == 6
    assert parent["query_id"] == "102"
    assert parent["_group_query_count"] == 2
    assert parent["_is_group_parent"] is True


def test_tree_groups_queries_and_keeps_ungrouped_branch() -> None:
    _app()
    tree = QTreeWidget()
    tree.setColumnCount(12)
    _populate_slow_query_tree(tree, _queries(), grouped=True)

    assert tree.topLevelItemCount() == 3
    parents = {tree.topLevelItem(i).data(0, Qt.UserRole)["repeat_group_id"]: tree.topLevelItem(i) for i in range(3)}
    assert parents["RQ001"].childCount() == 2
    assert {parents["RQ001"].child(i).data(0, Qt.UserRole)["query_id"] for i in range(2)} == {"101", "102"}
    assert parents["UNGROUPED"].childCount() == 1


def test_slow_query_page_defaults_to_expandable_repeat_groups() -> None:
    _app()
    page = _SeverityQueryPage()
    page.set_dataframe(_queries().drop(columns=["severity_score"]))

    assert page._rollup_check.isChecked()
    assert page._tree.topLevelItemCount() == 3
    first_payload = page._tree.topLevelItem(0).data(0, Qt.UserRole)
    assert first_payload["_is_group_parent"] is True
    assert "groups / 4 queries" in page._status.text()


def test_slow_query_tree_columns_sort_numerically() -> None:
    _app()
    page = _SeverityQueryPage()
    page.set_dataframe(_queries().drop(columns=["severity_score"]))

    page._tree.sortItems(4, Qt.AscendingOrder)
    elapsed = [
        float(page._tree.topLevelItem(i).data(0, Qt.UserRole)["elapsed_s"])
        for i in range(page._tree.topLevelItemCount())
    ]

    assert elapsed == sorted(elapsed)
    assert page._tree.isSortingEnabled()
