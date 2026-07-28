"""Whether the running user is part of a query pattern's identity."""
from __future__ import annotations

import os

import pandas as pd

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from analyzer.query_similarity import MULTIPLE_USERS_LABEL, _grouped_user_label
from analyzer.settings import AnalyzerSettings


def test_default_includes_the_user_in_the_grouping():
    """Advised starting point: exact attribution, nothing merged unasked."""
    assert AnalyzerSettings().repeat_scope_by_user is True


def test_user_blind_group_spanning_users_reads_multiple_users():
    """Listing the first eight would imply a precision the grouping lacks."""
    users = pd.Series(["alice", "bob", "carol"])

    assert _grouped_user_label(users, scope_by_user=False) == MULTIPLE_USERS_LABEL


def test_user_blind_group_with_one_user_still_names_that_user():
    users = pd.Series(["alice", "alice"])

    assert _grouped_user_label(users, scope_by_user=False) == "alice"


def test_per_user_scoping_names_the_users_exactly():
    """With scoping ON a group is one user by construction."""
    users = pd.Series(["alice", "bob", "carol"])

    label = _grouped_user_label(users, scope_by_user=True)

    assert label != MULTIPLE_USERS_LABEL
    assert "alice" in label


def test_scope_change_invalidates_the_cached_grouping():
    """Changing what counts as one pattern must force a regroup, not reuse
    a cache built under the other rule."""
    import analyzer.cluster_analyze as module

    frame = pd.DataFrame([{"query_id": 1, "sql_text": "SELECT 1"}])
    empty = pd.DataFrame()

    on = module._repeat_cache_key(
        "snap", AnalyzerSettings(repeat_scope_by_user=True), frame, empty, empty
    )
    off = module._repeat_cache_key(
        "snap", AnalyzerSettings(repeat_scope_by_user=False), frame, empty, empty
    )

    assert on != off


def test_settings_dialog_exposes_the_grouping_control():
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])
    _ = app
    from analyzer.widgets.cluster_dashboard import _ConfigDialog

    dialog = _ConfigDialog("test.duckdb")

    assert hasattr(dialog, "_scope_by_user_box")
    tabs = dialog.findChildren(type(dialog).__mro__[0])
    _ = tabs
    text = dialog._scope_by_user_box.text()
    assert "user" in text.lower()
