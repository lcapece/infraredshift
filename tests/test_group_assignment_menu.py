from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import duckdb
import pandas as pd
from PySide6.QtWidgets import QApplication

from analyzer.assignments import load_assignments, set_assignment
from analyzer.widgets.triage_home import TriagePage, _AssignEngineerDialog


_APP = QApplication.instance() or QApplication([])


def _roster_db(path) -> str:
    con = duckdb.connect(str(path))
    con.execute(
        "CREATE TABLE user_roster (user_name VARCHAR, first_name VARCHAR, "
        "middle_initial VARCHAR, last_name VARCHAR)"
    )
    con.execute(
        "INSERT INTO user_roster VALUES "
        "('jdoe', 'Jane', 'A', 'Doe'), ('bsmith', 'Bob', '', 'Smith')"
    )
    con.close()
    return str(path)


def _groups_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "repeat_group_id": "RQ1",
                "repeat_group_key": "Gabc123",
                "query_count": 4,
                "total_runtime_s": 100.0,
                "total_input_rows": 10,
                "users": "jdoe",
                "triage_verdict": "FIX QUERY",
                "sql_shape": "select 1",
            }
        ]
    )


class _Report:
    def __init__(self, db_path: str, groups: pd.DataFrame):
        self.repeat_groups = groups
        self.repeat_members = pd.DataFrame()
        self.repeat_group_tables = pd.DataFrame()
        self.action_queue = pd.DataFrame()
        self.table_review = pd.DataFrame()
        self.view_definitions = pd.DataFrame()
        self.slow_queries = pd.DataFrame()
        self.summary = {}
        self.snapshot_id = "s1"
        self.db_path = db_path


def test_assignment_persists_and_reaches_tree_column(tmp_path) -> None:
    db = _roster_db(tmp_path / "assign.duckdb")
    page = TriagePage()
    page.set_report(_Report(db, _groups_frame()))

    group = page._group_by_id("RQ1")
    assert group is not None
    page._write_assignment(group, "jdoe", "Doe, Jane A")

    stored = load_assignments(db)
    assert stored["Gabc123"]["user_name"] == "jdoe"
    assert stored["Gabc123"]["engineer_display"] == "Doe, Jane A"

    item = page._tree.topLevelItem(0)
    assert item.text(8) == "Doe, Jane A"

    # A fresh report load re-attaches the persisted assignment.
    page.set_report(_Report(db, _groups_frame()))
    assert page._tree.topLevelItem(0).text(8) == "Doe, Jane A"
    page.close()


def test_clear_assignment_removes_row(tmp_path) -> None:
    db = _roster_db(tmp_path / "clear.duckdb")
    set_assignment(db, "Gabc123", "jdoe", "Doe, Jane A")
    assert load_assignments(db)

    page = TriagePage()
    page.set_report(_Report(db, _groups_frame()))
    group = page._group_by_id("RQ1")
    page._write_assignment(group, "", "")

    assert load_assignments(db) == {}
    assert page._tree.topLevelItem(0).text(8) == ""
    page.close()


def test_assign_dialog_lists_roster_sorted_and_resolves_user_name(tmp_path) -> None:
    from analyzer.assignments import load_roster_choices

    db = _roster_db(tmp_path / "roster.duckdb")
    choices = load_roster_choices(db)
    assert [choice["display"] for choice in choices] == ["Doe, Jane A", "Smith, Bob"]

    dialog = _AssignEngineerDialog("RQ1", choices)
    dialog._combo.setCurrentIndex(1)
    user_name, display = dialog.selected()
    assert (user_name, display) == ("bsmith", "Smith, Bob")
    dialog.close()


def test_free_text_that_matches_no_roster_entry_is_not_an_assignment(tmp_path) -> None:
    from analyzer.assignments import load_roster_choices

    db = _roster_db(tmp_path / "freetext.duckdb")
    dialog = _AssignEngineerDialog("RQ1", load_roster_choices(db))
    dialog._combo.setCurrentText("Totally Unknown Person")
    assert dialog.selected() == ("", "")
    dialog.close()
