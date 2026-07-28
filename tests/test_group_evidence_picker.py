import os

import pandas as pd

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QApplication

from analyzer.theme import PALETTE
from analyzer.widgets.cluster_dashboard import (
    _GroupEvidencePage,
    _group_evidence_kind,
    _group_evidence_picker_label,
)


def test_group_evidence_picker_has_type_runs_and_sql_without_pipe_alignment():
    group = pd.Series(
        {
            "repeat_group_id": "RQ014",
            "query_count": 53,
            "sample_sql": "SELECT order_id, amount FROM spectrum.sales WHERE sale_date >= current_date - 7",
            "avg_s3_scan_cnt": 2,
        }
    )

    label = _group_evidence_picker_label(group)

    assert label.startswith("RQ014 — Spectrum Query — 53 runs — SELECT order_id")
    assert "|" not in label


def test_group_evidence_kind_distinguishes_mixed_and_copy_workloads():
    mixed = pd.Series(
        {
            "sample_sql": "SELECT * FROM local.orders JOIN spectrum.events USING (id)",
            "avg_s3_scan_cnt": 1,
            "triage_tables_matched": 1,
        }
    )
    load = pd.Series({"sample_sql": "COPY public.orders FROM 's3://bucket/orders/'"})
    unload = pd.Series({"sample_sql": "UNLOAD ('SELECT * FROM public.orders') TO 's3://bucket/out/'"})

    assert _group_evidence_kind(mixed) == "Mixed Query"
    assert _group_evidence_kind(load) == "Load COPY"
    assert _group_evidence_kind(unload) == "UNLOAD"


def test_group_evidence_combo_entries_are_blue():
    _app = QApplication.instance() or QApplication([])
    page = _GroupEvidencePage()
    page._groups = pd.DataFrame(
        [
            {
                "repeat_group_id": "RQ001",
                "query_count": 2,
                "sample_sql": "SELECT * FROM public.orders",
            }
        ]
    )

    class Report:
        repeat_groups = page._groups
        repeat_members = pd.DataFrame()
        table_review = pd.DataFrame()
        view_definitions = pd.DataFrame()

    page.set_report(Report())

    assert page._group_combo.itemText(0).startswith("RQ001 — Local Query — 2 runs")
    assert page._group_combo.itemData(0, Qt.ForegroundRole) == QColor(PALETTE.accent)
