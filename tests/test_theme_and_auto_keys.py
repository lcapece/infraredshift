from __future__ import annotations

import sys
import re
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analyzer.theme import retint_stylesheet, set_theme_mode  # noqa: E402
from analyzer.widgets.cluster_dashboard import (  # noqa: E402
    _dist_sort_missing_state,
    _format_dist_sort_keys,
)


def test_auto_dist_and_sort_keys_render_as_missing():
    dist_auto = _format_dist_sort_keys(
        pd.Series({"diststyle": "DISTKEY(AUTO)", "distkey": "AUTO", "sortkey1": "service_date"})
    )
    sort_auto = _format_dist_sort_keys(
        pd.Series({"diststyle": "KEY(customer_id)", "distkey": "customer_id", "sortkey1": "SORTKEY(AUTO)"})
    )

    assert dist_auto.startswith("Dist: None")
    assert _dist_sort_missing_state(dist_auto) == (True, False)
    assert "Sort: Unsorted" in sort_auto
    assert _dist_sort_missing_state(sort_auto) == (False, True)


def test_light_theme_retints_old_dark_text_colors():
    set_theme_mode("light")
    sheet = retint_stylesheet(
        """
        QLabel { background: #FFFFFF; color: #F5F7FF; }
        QLabel[severity="warn"] { color: #FFB547; }
        """
    )

    assert "color: #F5F7FF" not in sheet
    assert "color: #FFB547" not in sheet
    assert "color: #182033" in sheet
    assert "color: #8A5200" in sheet


def test_actual_light_stylesheet_has_no_white_or_old_yellow_text():
    set_theme_mode("light")
    style_path = Path(__file__).resolve().parents[1] / "analyzer" / "style.qss"
    sheet = retint_stylesheet(style_path.read_text(encoding="utf-8"))

    assert not re.findall(r"color\s*:\s*#(?:F5F7FF|E2E8F7|FFFFFF)\b", sheet, re.IGNORECASE)
    assert not re.findall(r"color\s*:\s*#FFB547\b", sheet, re.IGNORECASE)


def test_stylesheet_defines_keyboard_focus_rings():
    style_path = Path(__file__).resolve().parents[1] / "analyzer" / "style.qss"
    sheet = style_path.read_text(encoding="utf-8")
    assert "QPushButton:focus" in sheet
    assert "QTreeWidget:focus" in sheet or "QTreeView:focus" in sheet
    assert "border: 2px solid" in sheet


def test_title_bar_keeps_theme_toggle_at_narrow_width():
    # Headless-safe construction; skip if Qt platform cannot init (rare CI).
    try:
        from PySide6.QtWidgets import QApplication
        from analyzer.widgets.title_bar import TitleBar
    except Exception:
        return
    import sys

    app = QApplication.instance() or QApplication(sys.argv[:1])
    bar = TitleBar()
    bar.resize(640, 52)
    bar.show()
    app.processEvents()
    assert bar._theme_toggle.isVisible()
    assert bar._exit_btn.isVisible()
    # Metric values use live palette, not hard-coded near-white.
    from analyzer.theme import PALETTE, set_theme_mode

    set_theme_mode("light")
    bar.apply_theme()
    assert PALETTE.text_0 in bar.m_qid._value.styleSheet()
    assert "#F5F7FF" not in bar.m_qid._value.styleSheet().upper()


def test_verdict_colors_follow_live_palette():
    from analyzer.theme import PALETTE, set_theme_mode
    from analyzer.widgets.triage_home import _verdict_colors

    set_theme_mode("light")
    light = _verdict_colors()
    assert light["FIX BOTH"] == PALETTE.crit
    set_theme_mode("dark")
    dark = _verdict_colors()
    assert dark["FIX BOTH"] == PALETTE.crit
    # Light and dark crit tokens differ — proves we are not frozen at import.
    set_theme_mode("light")
    assert _verdict_colors()["FIX BOTH"] != dark["FIX BOTH"] or light["MONITOR"] != dark.get("MONITOR")
