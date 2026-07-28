import os

import pandas as pd

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from analyzer.widgets.triage_home import (
    _MAX_CHART_BUBBLES,
    _QuadrantChart,
    _filter_chart_groups,
)


def _groups(count: int = 250) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "repeat_group_id": f"RQ{i:03d}",
                "query_count": 2,
                "total_runtime_s": float(i * 2),
                "total_input_rows": float(i * 200),
                "total_output_rows": float(i * 10),
                "total_input_bytes": float(i * 1000),
                "total_spill_blocks": float(i if i % 2 else 0),
                "total_queue_s": float(i / 10),
                "avg_dist_both_cnt": 1.0 if i % 3 == 0 else 0.0,
                "avg_bcast_cnt": 0.0,
                "avg_max_data_skewness": 5.0 if i % 5 == 0 else 1.0,
                "avg_remote_io_ratio": 0.4 if i % 7 == 0 else 0.0,
                "triage_verdict": "FIX QUERY",
            }
            for i in range(1, count + 1)
        ]
    )


def test_chart_filters_never_return_more_than_200_bubbles():
    filtered, matching = _filter_chart_groups(
        _groups(),
        "rows",
        positive_metric_only=True,
        scenario="overview",
    )

    assert matching == 250
    assert len(filtered) == _MAX_CHART_BUBBLES == 200
    assert filtered.iloc[0]["repeat_group_id"] == "RQ250"


def test_zero_vertical_metrics_are_hidden_by_default():
    groups = _groups(3)
    groups.loc[1, "total_input_rows"] = 0

    filtered, matching = _filter_chart_groups(groups, "rows")

    assert matching == 2
    assert "RQ002" not in set(filtered["repeat_group_id"])


def test_spill_scenario_requires_spill_and_ranks_long_running_patterns():
    groups = _groups(4)

    filtered, matching = _filter_chart_groups(groups, "rows", scenario="spill_slow")

    assert matching == 2
    assert list(filtered["repeat_group_id"]) == ["RQ003", "RQ001"]


def test_overlap_shuffle_promotes_selected_then_whole_successive_layers():
    _app = QApplication.instance() or QApplication([])
    chart = _QuadrantChart()
    laid = [
        {"gid": "A", "cx": 0.0, "cy": 0.0, "r": 10.0},
        {"gid": "B", "cx": 18.0, "cy": 0.0, "r": 10.0},
        {"gid": "C", "cx": 36.0, "cy": 0.0, "r": 10.0},
        {"gid": "D", "cx": 36.0, "cy": 8.0, "r": 10.0},
    ]
    layers = chart._overlap_layers(laid, "A")

    assert layers == {"A": 0, "B": 1, "C": 2, "D": 2}

    chart._shuffle_anchor_gid = "A"
    chart._shuffle_level = 0
    assert [point["gid"] for point in chart._z_ordered_points(laid)][-1:] == ["A"]
    chart._shuffle_level = 2
    assert {point["gid"] for point in chart._z_ordered_points(laid)[-2:]} == {"C", "D"}
