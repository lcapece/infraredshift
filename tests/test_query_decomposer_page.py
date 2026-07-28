from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from analyzer.app import SingleQueryLab
from analyzer.widgets.title_bar import TitleBar


_APP = QApplication.instance() or QApplication([])


def test_single_query_lab_features_query_decomposer_first() -> None:
    # The lab is no longer a main tab (the app surfaces only Load Status and
    # Workload Triage) but remains available as a component.
    lab = SingleQueryLab(TitleBar())

    labels = [lab._mode_tabs.tabText(index) for index in range(lab._mode_tabs.count())]

    assert labels == ["Query Decomposer", "One-Off SQL", "Plan Paste"]
    assert lab._mode_tabs.currentIndex() == lab._query_decomposer_tab


def test_decomposer_can_copy_sql_from_one_off_lens() -> None:
    lab = SingleQueryLab(TitleBar())
    lab._sql_lens.load_external_sql("SELECT 1", analyze=False)

    lab._copy_one_off_to_decomposer()

    assert lab._query_decomposer.sql_text() == "SELECT 1"
