"""Load Status with one configured cluster.

A single-cluster install is a supported setup, not half a missing estate. The
screen used to show "Producer" plus three empty Consumer/Commercial/FAR slots,
which implies a topology that does not exist and reads as three failed loads.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from analyzer.topology import ClusterStatus, TopologySnapshot
from analyzer.widgets.topology import (
    _ESTATE_DESCRIPTION,
    _SINGLE_CLUSTER_DESCRIPTION,
    TopologyPage,
    _assign_display_labels,
)


def _app():
    return QApplication.instance() or QApplication([])


def _cluster(name: str, role: str = "producer") -> ClusterStatus:
    return ClusterStatus(
        friendly_name=name, namespace_id="ns-1", role=role, enabled=True, host="h",
        datasets=(), min_query_id=None, max_query_id=None, query_count=1234,
        first_query_at="", last_query_at="", severity="complete", severity_reason="",
    )


def test_one_cluster_uses_its_own_friendly_name():
    labelled = _assign_display_labels((_cluster("Analytics"),))

    assert len(labelled) == 1
    label, cluster = labelled[0]
    assert label == "Analytics"
    assert cluster.friendly_name == "Analytics"


def test_one_cluster_does_not_invent_empty_sibling_slots():
    """Three blank Consumer/Commercial/FAR slots read as three failed loads."""
    labelled = _assign_display_labels((_cluster("Analytics"),))

    assert [label for label, _ in labelled] == ["Analytics"]
    assert all(cluster is not None for _, cluster in labelled)


def test_a_nameless_single_cluster_still_gets_a_label():
    labelled = _assign_display_labels((_cluster(""),))

    assert labelled[0][0] == "Producer"


def test_multi_cluster_labelling_is_unchanged():
    clusters = (
        _cluster("Core Producer", "producer"),
        _cluster("FAR", "consumer"),
        _cluster("Commercial", "consumer"),
        _cluster("Consumer", "consumer"),
    )

    labelled = _assign_display_labels(clusters)

    assert [label for label, _ in labelled] == [
        "Producer", "Consumer", "Commercial", "FAR",
    ]


def test_single_cluster_page_shows_one_button_and_its_own_wording():
    _app()
    page = TopologyPage()

    page._on_snapshot(
        TopologySnapshot(
            db_path="x.duckdb", clusters=(_cluster("Analytics"),), assessed_at=""
        )
    )

    assert page._single_cluster is True
    assert len(page._selector_buttons) == 1
    assert page._selector_buttons[0].text().startswith("Analytics")
    assert page._description.text() == _SINGLE_CLUSTER_DESCRIPTION
    # "Producer (primary)" implies something to be primary over.
    assert "Single cluster" in page._cluster_line.text()
    assert "Analytics" in page._cluster_line.text()


def test_multi_cluster_page_keeps_the_estate_wording():
    _app()
    page = TopologyPage()

    page._on_snapshot(
        TopologySnapshot(
            db_path="x.duckdb",
            clusters=(_cluster("Core Producer", "producer"), _cluster("FAR", "consumer")),
            assessed_at="",
        )
    )

    assert page._single_cluster is False
    assert page._description.text() == _ESTATE_DESCRIPTION
    assert len(page._selector_buttons) > 1
