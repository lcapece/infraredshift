import os

import pandas as pd

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QLabel, QPushButton, QSplitter

from analyzer.widgets.cluster_dashboard import (
    _ActionPlanPage,
    _build_fix_query_initiatives,
    _fix_query_evidence_frame,
)


def _frames():
    actions = pd.DataFrame(
        [
            {
                "action_id": "A01_ANALYZE_STALE_STATS",
                "action_type": "Maintenance",
                "severity": "crit",
                "subject": "prod.sales.orders",
                "table_key": "prod.sales.orders",
                "action_score": 110,
                "evidence": "stats_off=55%, scans=42",
                "what_to_do": "Run ANALYZE on this table.",
                "sql_hint": "ANALYZE sales.orders;",
            },
            {
                "action_id": "A03_REVIEW_DISTRIBUTION",
                "action_type": "Physical Design",
                "severity": "warn",
                "subject": "prod.sales.line_items",
                "table_key": "prod.sales.line_items",
                "action_score": 92,
                "evidence": "skew_rows=8.2",
                "what_to_do": "Review DISTKEY.",
            },
        ]
    )
    rewrites = pd.DataFrame(
        [
            {
                "query_id": 101,
                "subject": "101",
                "severity": "crit",
                "title": "Spill Reduction Rewrite",
                "impact_score": 118,
                "trigger": "spill_blocks=450000",
                "rewrite_shape": "Pre-aggregate before joining.",
                "candidate_sql": "SELECT ...",
            },
            {
                "query_id": 102,
                "subject": "102",
                "severity": "warn",
                "title": "Distributed Join Rewrite",
                "impact_score": 96,
                "trigger": "dist_total=8",
                "rewrite_shape": "Stage aligned inputs.",
            },
        ]
    )
    slow = pd.DataFrame(
        [
            {"query_id": 101, "elapsed_s": 3600.0},
            {"query_id": 102, "elapsed_s": 1800.0},
        ]
    )
    return actions, rewrites, slow


def test_fix_query_findings_are_consolidated_into_decisions():
    actions, rewrites, slow = _frames()

    initiatives = _build_fix_query_initiatives(actions, rewrites, slow)

    assert set(initiatives["initiative_key"]) == {"statistics", "distribution", "spill"}
    distribution = initiatives[initiatives["initiative_key"] == "distribution"].iloc[0]
    assert distribution["scope"] == "2 finding(s) • 1 query • 1 table(s)"
    assert distribution["runtime_s"] == 1800.0
    assert distribution["readiness"] == "DBA design review"


def test_fix_query_technical_evidence_remains_available_by_initiative():
    actions, rewrites, slow = _frames()
    initiatives = _build_fix_query_initiatives(actions, rewrites, slow)
    distribution = initiatives[initiatives["initiative_key"] == "distribution"].iloc[0]

    evidence = _fix_query_evidence_frame(distribution, actions, rewrites)

    assert set(evidence["source"]) == {"DBA action", "Query rewrite"}
    assert "skew_rows=8.2" in set(evidence["evidence"])
    assert "dist_total=8" in set(evidence["evidence"])


def test_fix_query_page_has_one_brief_without_floating_splitter():
    _app = QApplication.instance() or QApplication([])
    actions, rewrites, slow = _frames()
    page = _ActionPlanPage()
    page.set_dataframes(actions, rewrites, slow)

    labels = {label.text() for label in page.findChildren(QLabel)}
    buttons = {button.text() for button in page.findChildren(QPushButton)}

    assert not page.findChildren(QSplitter)
    assert "RECOMMENDED INITIATIVES" in labels
    assert "Technical Evidence" in buttons
    assert page._brief._initiative_tile._value.text() == "3"


def test_fix_query_accepts_nullable_duckdb_identifiers():
    actions, rewrites, slow = _frames()
    actions["query_id"] = pd.Series([pd.NA, pd.NA], dtype="string")
    actions.loc[0, "table_key"] = pd.NA
    rewrites.loc[0, "query_id"] = pd.NA
    rewrites.loc[0, "table_key"] = pd.NA

    initiatives = _build_fix_query_initiatives(actions, rewrites, slow)

    assert not initiatives.empty
    assert set(initiatives["initiative_key"]) == {"statistics", "distribution", "spill"}
