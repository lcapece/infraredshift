"""The grouped-query collapse tile.

Thousands of "distinct" slow queries collapse into a few dozen shapes. That
ratio is the argument for the whole tool, so the top strip states both sides
rather than a bare pattern count.
"""
from __future__ import annotations

import os

import pandas as pd

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from analyzer.widgets.triage_home import (
    TriagePage,
    _collapse_label,
    _collapse_tooltip,
)


def _app():
    return QApplication.instance() or QApplication([])


def _groups(count: int = 68) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "repeat_group_id": f"RQ{index:03d}",
                "repeat_group_key": f"K{index}",
                "query_count": 195,
                "total_runtime_s": 100.0,
                "total_input_rows": 10,
                "triage_verdict": "FIX QUERY",
                "query_ids": str(1000 + index),
            }
            for index in range(count)
        ]
    )


def test_label_states_both_sides_of_the_collapse():
    """A bare "68" means nothing without the number it came from."""
    assert _collapse_label(13_232, 68) == "13,232 → 68"
    assert _collapse_label(1_500_000, 240) == "1,500,000 → 240"


def test_label_is_blank_when_nothing_is_grouped():
    assert _collapse_label(0, 0) == "-"
    assert _collapse_label(500, 0) == "-"


def test_tooltip_gives_a_checkable_ratio_not_a_percentage():
    """"99.5% reduction" reads as marketing; "1 per 195" can be verified."""
    tooltip = _collapse_tooltip(13_232, 68)

    assert "13,232" in tooltip and "68" in tooltip
    assert "1 pattern per 195" in tooltip
    assert "%" not in tooltip


def test_tooltip_omits_the_ratio_when_there_is_no_collapse():
    tooltip = _collapse_tooltip(5, 5)

    assert "1 pattern per" not in tooltip


def test_tile_uses_the_captured_slow_query_count(monkeypatch):
    _app()
    page = TriagePage()

    page.set_dataframes(
        _groups(68),
        pd.DataFrame(),
        pd.DataFrame(),
        {"total_runtime_s": 20_000.0, "slow_query_count": 13_232},
    )

    assert page._tile_collapse._value.text() == "13,232 → 68"
    assert page._tile_patterns._value.text() == "68"


def test_tile_falls_back_to_summed_runs_when_the_count_is_absent():
    """Never invent a denominator: without slow_query_count, use the runs the
    patterns actually account for."""
    _app()
    page = TriagePage()

    page.set_dataframes(
        _groups(68), pd.DataFrame(), pd.DataFrame(), {"total_runtime_s": 20_000.0}
    )

    # 68 groups x 195 runs each.
    assert page._tile_collapse._value.text() == "13,260 → 68"


def test_tile_resets_with_the_others_when_there_is_no_data():
    _app()
    page = TriagePage()

    page.set_dataframes(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {})

    assert page._tile_collapse._value.text() == "-"


def test_the_strip_stays_one_compact_row():
    """The strip was reduced from 75px to a 28px inline row; adding a tile
    must not push the chart back down."""
    _app()
    page = TriagePage()
    page.set_dataframes(
        _groups(4), pd.DataFrame(), pd.DataFrame(), {"slow_query_count": 900}
    )

    assert page._tile_collapse.parent().maximumHeight() <= 28
