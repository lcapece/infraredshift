"""Workload Triage: numeric query-id ordering and sortable List columns."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import pytest

pytest.importorskip("PySide6.QtWidgets")

import os
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt  # noqa: E402
from PySide6.QtWidgets import QApplication  # noqa: E402

from analyzer.widgets.triage_home import (  # noqa: E402
    _SORT_ROLE,
    _SortableTreeItem,
    _query_id_sort_key,
    _query_ids_for_group,
)

_app = QApplication.instance() or QApplication([])


def test_query_id_sort_key_numeric_ascends():
    ids = ["1001", "97", "2500", "13"]
    assert sorted(ids, key=_query_id_sort_key) == ["13", "97", "1001", "2500"]


def test_query_id_sort_key_falls_back_to_text():
    # Non-numeric ids sort after numeric ones, lexically among themselves.
    ids = ["abc", "1001", "13", "aaa"]
    ordered = sorted(ids, key=_query_id_sort_key)
    assert ordered == ["13", "1001", "aaa", "abc"]


def test_query_ids_for_group_returns_numeric_ascending():
    group = {"repeat_group_id": "RQ001"}
    members = pd.DataFrame(
        {
            "repeat_group_id": ["RQ001"] * 4,
            "query_id": ["1001", "97", "2500", "13"],
            "member_rank": [1, 2, 3, 4],
            "elapsed_s": [50.0, 40.0, 30.0, 20.0],
        }
    )
    ids = _query_ids_for_group(group, members)
    # Regardless of member_rank/elapsed order, copied ids come out ascending.
    assert ids == ["13", "97", "1001", "2500"]


def test_copy_one_takes_lowest_id():
    group = {"repeat_group_id": "RQ001"}
    members = pd.DataFrame(
        {
            "repeat_group_id": ["RQ001"] * 3,
            "query_id": ["555", "12", "9000"],
        }
    )
    ids = _query_ids_for_group(group, members)
    assert ids[0] == "12"  # "Copy One" copies ids[0]


def test_sortable_item_sorts_numerically_not_lexically():
    # Formatted cells '1.2K' vs '900' would sort wrong as text; the numeric
    # sort role fixes it.
    big = _SortableTreeItem(["", "pattern-a", "", "1.2K", "", "", "", ""])
    small = _SortableTreeItem(["", "pattern-b", "", "900", "", "", "", ""])
    big.setData(3, _SORT_ROLE, 1200.0)
    small.setData(3, _SORT_ROLE, 900.0)

    from PySide6.QtWidgets import QTreeWidget

    tree = QTreeWidget()
    tree.setColumnCount(8)
    tree.addTopLevelItem(big)
    tree.addTopLevelItem(small)
    tree.setSortingEnabled(True)
    tree.sortByColumn(3, Qt.AscendingOrder)
    # Ascending: 900 (pattern-b) before 1.2K (pattern-a)
    assert tree.topLevelItem(0).text(1) == "pattern-b"
    assert tree.topLevelItem(1).text(1) == "pattern-a"
    tree.sortByColumn(3, Qt.DescendingOrder)
    assert tree.topLevelItem(0).text(1) == "pattern-a"


def test_sortable_item_text_fallback_without_role():
    a = _SortableTreeItem(["", "zebra", "", "", "", "", "", ""])
    b = _SortableTreeItem(["", "apple", "", "", "", "", "", ""])
    from PySide6.QtWidgets import QTreeWidget

    tree = QTreeWidget()
    tree.setColumnCount(8)
    tree.addTopLevelItem(a)
    tree.addTopLevelItem(b)
    tree.setSortingEnabled(True)
    tree.sortByColumn(1, Qt.AscendingOrder)
    assert tree.topLevelItem(0).text(1) == "apple"
