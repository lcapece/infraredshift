from pathlib import Path
import os

import pandas as pd

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QPlainTextEdit, QPushButton, QScrollArea

from analyzer.cluster_analyze import ClusterReport
from analyzer.cluster_analyze import load_cluster_report
from analyzer.widgets.cluster_dashboard import (
    _extract_subquery_rows,
    _SubqueryExtractDialog,
    ClusterDashboard,
)
from analyzer.widgets.triage_home import (
    _AnalysisProcessFlow,
    _SPECTRUM_LABEL,
    TriagePage,
    _display_verdict,
    _is_spectrum_group,
    _query_ids_for_group,
)


def test_triage_process_animation_shows_parallel_display_only_flow():
    app = QApplication.instance() or QApplication([])
    _ = app
    page = TriagePage()
    flow = _AnalysisProcessFlow(scale=0.67)

    assert isinstance(flow, _AnalysisProcessFlow)
    assert len(flow.STEPS) == 10
    assert flow.PARALLEL_STEPS == (6, 7, 8)
    assert page.findChild(_AnalysisProcessFlow) is None
    assert flow.width() == round(1080 * 0.67)
    assert flow.height() == round(214 * 0.67)
    flow._phase = 5
    assert flow._active_steps() == frozenset({6, 7, 8})
    assert "Display-only" in flow.toolTip()
    assert not flow._timer.isActive()


def test_show_all_bubbles_resets_filters_and_loads_missing_triage():
    app = QApplication.instance() or QApplication([])
    _ = app
    page = TriagePage()
    requested = []
    page.loadRequested.connect(requested.append)

    page._scenario_combo.setCurrentIndex(2)
    page._runtime_combo.setCurrentIndex(4)
    page._metric_coverage_combo.setCurrentIndex(1)
    page._show_all_bubbles()

    assert page._scenario_combo.currentIndex() == 0
    assert page._runtime_combo.currentIndex() == 0
    assert page._metric_coverage_combo.currentData() is False
    assert requested == ["repeat_queries"]


def test_zero_metric_patterns_are_visible_in_default_bubble_view():
    app = QApplication.instance() or QApplication([])
    _ = app
    page = TriagePage()
    groups = pd.DataFrame(
        [
            {
                "repeat_group_id": "RQ001",
                "query_count": 2,
                "total_runtime_s": 90.0,
                "total_input_rows": 0,
                "triage_verdict": "FIX QUERY",
            }
        ]
    )

    page.set_dataframes(
        groups,
        pd.DataFrame(),
        pd.DataFrame(),
        {"total_runtime_s": 90.0},
    )

    assert page._metric_coverage_combo.currentData() is False
    assert len(page._chart._points) == 1


def test_triage_sql_view_exposes_format_subqueries_and_lineage_actions():
    app = QApplication.instance() or QApplication([])
    _ = app
    page = TriagePage()
    labels = {button.text() for button in page.findChildren(QPushButton)}

    assert "Format SQL" in labels
    assert "Extract Subqueries" in labels
    assert "Show Lineage" in labels


def test_selecting_extracted_subquery_highlights_source_sql():
    app = QApplication.instance() or QApplication([])
    sql = (
        "WITH recent AS (SELECT id FROM public.events) "
        "SELECT * FROM recent WHERE id IN (SELECT event_id FROM public.audit)"
    )
    editor = QPlainTextEdit()
    editor.setPlainText(sql)
    dialog = _SubqueryExtractDialog(
        pd.Series({"query_id": "123"}),
        _extract_subquery_rows(sql),
        pd.DataFrame(),
        pd.DataFrame(),
        source_editor=editor,
    )

    dialog._table.selectRow(1)
    app.processEvents()

    assert editor.textCursor().selectedText() == "SELECT event_id FROM public.audit"


def test_repeat_queries_area_feeds_workload_triage_chart(tmp_path):
    sample = Path(__file__).resolve().parents[1] / "analyzer" / "samples" / "mock_redshift_3300.duckdb"
    db_path = tmp_path / "mock_redshift_3300.duckdb"
    db_path.write_bytes(sample.read_bytes())

    report = load_cluster_report(db_path, areas=["repeat_queries"])

    assert not report.repeat_groups.empty
    assert not report.repeat_members.empty


def test_triage_diagram_resolves_full_sql_from_report():
    app = QApplication.instance() or QApplication([])
    _ = app
    preview_sql = "SELECT col_001 FROM public.fact_orders WHERE order_date = '2026-06-01'..."
    full_sql = (
        "SELECT "
        + ", ".join(f"col_{i:03d}" for i in range(260))
        + " FROM public.fact_orders WHERE order_date = '2026-06-01'"
    )
    dashboard = ClusterDashboard()
    dashboard._report = ClusterReport(
        db_path=Path("test.duckdb"),
        slow_queries=pd.DataFrame([{"query_id": "12345", "sql_text": full_sql}]),
        repeat_members=pd.DataFrame([{"query_id": "12345", "sql_text": preview_sql}]),
    )

    _row, sql, label = dashboard._resolve_query_diagram_input({"query_id": "12345", "sql_text": preview_sql})

    assert label == "12345"
    assert sql == full_sql


def test_triage_spectrum_groups_are_query_fix_targets():
    group = {
        "repeat_group_id": "RQ001",
        "triage_verdict": "FIX TABLES",
        "avg_s3_scan_cnt": 3,
        "sql_tables": "spectrum.raw_clickstream",
    }

    assert _is_spectrum_group(group)
    assert _display_verdict(group) == _SPECTRUM_LABEL


def test_triage_group_query_ids_prefer_member_order_and_dedupe():
    group = {
        "repeat_group_id": "RQ001",
        "query_ids": "101, 102, 101",
        "example_query_id_1": "103",
    }
    members = pd.DataFrame(
        [
            {"repeat_group_id": "RQ001", "member_rank": 2, "query_id": "102"},
            {"repeat_group_id": "RQ001", "member_rank": 1, "query_id": "101"},
            {"repeat_group_id": "RQ999", "member_rank": 1, "query_id": "999"},
        ]
    )

    assert _query_ids_for_group(group, members) == ["101", "102", "103"]


def test_triage_page_handles_missing_table_flag_column():
    app = QApplication.instance() or QApplication([])
    _ = app
    from analyzer.widgets.triage_home import TriagePage

    page = TriagePage()
    groups = pd.DataFrame(
        [
            {
                "repeat_group_id": "RQ001",
                "query_count": 1,
                "total_runtime_s": 10.0,
                "total_input_rows": 100,
                "triage_verdict": "MONITOR",
                "query_ids": "101",
            }
        ]
    )

    page.set_dataframes(groups, pd.DataFrame(), pd.DataFrame(), {"total_runtime_s": 10.0})

    assert page._tile_tables._value.text() == "0"


def test_triage_page_scrolls_when_viewport_is_short():
    app = QApplication.instance() or QApplication([])
    _ = app
    from analyzer.widgets.triage_home import TriagePage

    page = TriagePage()
    groups = pd.DataFrame(
        [
            {
                "repeat_group_id": f"RQ{i:03d}",
                "query_count": 3,
                "total_runtime_s": 100.0 + i,
                "total_input_rows": 10_000_000 + i,
                "triage_verdict": "FIX QUERY",
                "triage_tables_flagged": 0,
                "query_ids": f"{1000 + i}, {2000 + i}",
            }
            for i in range(1, 8)
        ]
    )

    page.resize(900, 360)
    page.set_dataframes(groups, pd.DataFrame(), pd.DataFrame(), {"total_runtime_s": 800.0})
    page.show()
    app.processEvents()

    assert page._page_scroll.verticalScrollBar().maximum() > 0


def test_command_center_has_no_inner_tab_bar():
    """The dashboard must not wrap its single page in its own QTabWidget.

    The main window already mounts this dashboard under a top-level tab named
    "Workload Triage" (app.py). An inner tab widget drew a second,
    identically-labelled tab bar inside the first - a tab within a tab - and
    cost roughly 35px of height that the bubble chart needs.

    Scroll guarding is not lost: _scroll_guard is applied by the main window at
    the top level, which is where a page-level scrollbar belongs. A second
    guard here produced nested scroll areas.
    """
    from PySide6.QtWidgets import QTabWidget

    app = QApplication.instance() or QApplication([])
    _ = app
    dashboard = ClusterDashboard()

    assert not hasattr(dashboard, "_tabs"), "inner tab widget should be gone"
    inner_tabs = dashboard.findChildren(QTabWidget)
    labels = [
        inner.tabText(i)
        for inner in inner_tabs
        for i in range(inner.count())
    ]
    assert "Workload Triage" not in labels, (
        f"a nested tab still duplicates the top-level tab name: {labels}"
    )


def test_main_app_top_level_tabs_are_page_scroll_guarded():
    app = QApplication.instance() or QApplication([])
    _ = app
    from analyzer.app import MainWindow

    window = MainWindow()

    for index in range(window._tabs.count()):
        assert isinstance(window._tabs.widget(index), QScrollArea)


def test_runtime_filter_discriminates_past_the_producer_capture_floor():
    """The runtime rungs must extend beyond the 300s Producer capture floor.

    Producer capture starts at 300 seconds, so every Producer repeat group
    already clears a 5-minute rung - the filter stopped discriminating exactly
    where the expensive patterns live. These rungs reach 8 hours per run.
    """
    from analyzer.widgets.triage_home import CHART_RUNTIME_FILTERS

    app = QApplication.instance() or QApplication([])
    _ = app
    page = TriagePage()

    thresholds = [seconds for _label, seconds in CHART_RUNTIME_FILTERS]
    for expected in (900.0, 1_800.0, 3_600.0, 7_200.0, 14_400.0, 28_800.0):
        assert expected in thresholds, f"missing a {expected}s rung"
    assert thresholds == sorted(thresholds), "rungs must ascend"

    # Average runtimes from 10 minutes to 10 hours per run.
    averages = (600, 1_200, 2_400, 5_000, 10_000, 20_000, 36_000)
    groups = pd.DataFrame(
        [
            {
                "repeat_group_id": f"RQ{index:03d}",
                "query_count": 1,
                "total_runtime_s": float(average),
                "total_input_rows": 10_000,
                "triage_verdict": "FIX QUERY",
                "query_ids": str(1000 + index),
            }
            for index, average in enumerate(averages)
        ]
    )
    page.set_dataframes(groups, pd.DataFrame(), pd.DataFrame(), {"total_runtime_s": 1.0})

    def bubbles_at(seconds: float) -> int:
        index = thresholds.index(seconds)
        page._runtime_combo.setCurrentIndex(index)
        return len(page._chart._points)

    # Every group clears the old top rung - which is why it needed extending.
    assert bubbles_at(300.0) == len(averages)
    # The new rungs actually narrow the field.
    assert bubbles_at(900.0) == 6
    assert bubbles_at(3_600.0) == 4
    assert bubbles_at(28_800.0) == 1
