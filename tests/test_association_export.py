"""Export User Associations - the ownership handoff document.

Covers the two things that make the document trustworthy: a pattern seen on
more than one cluster must say "Multi-Cluster", and the per-cluster query id
list must be capped without silently hiding that it was capped.
"""
from __future__ import annotations

from datetime import datetime
import os

import duckdb
import pandas as pd

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from analyzer.assignments import (
    load_assignments,
    set_assignment,
    set_association,
)
from analyzer.association_export import (
    MAX_QUERY_IDS_PER_CLUSTER,
    MULTI_CLUSTER,
    collect_associations,
    export_markdown,
)


def _groups() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "repeat_group_id": "RQ001", "repeat_group_key": "K1",
                "assigned_engineer": "Smith, John", "associated_user": "Jones, Mary",
                "query_count": 300, "total_runtime_s": 7200.0,
                "triage_verdict": "FIX TABLES",
                "sample_sql": "SELECT a FROM fact_orders WHERE dt = ?",
                "sql_tables": "dev.mart.fact_orders",
            },
            {
                "repeat_group_id": "RQ002", "repeat_group_key": "K2",
                "assigned_engineer": "White, Bob", "associated_user": "",
                "query_count": 40, "total_runtime_s": 600.0,
                "sample_sql": "SELECT 1",
            },
            {
                "repeat_group_id": "RQ003", "repeat_group_key": "K3",
                "assigned_engineer": "", "associated_user": "",
                "query_count": 9, "total_runtime_s": 99.0, "sample_sql": "SELECT 2",
            },
        ]
    )


def _members(producer_ids: int = 25) -> pd.DataFrame:
    rows = [
        {"repeat_group_key": "K1", "query_id": f"P{i}",
         "namespace_id": "ns-prod", "user_name": "svc_etl"}
        for i in range(producer_ids)
    ]
    rows += [
        {"repeat_group_key": "K1", "query_id": f"C{i}",
         "namespace_id": "ns-far", "user_name": "analyst1"}
        for i in range(3)
    ]
    rows += [
        {"repeat_group_key": "K2", "query_id": "S0",
         "namespace_id": "ns-prod", "user_name": "bi_tool"}
    ]
    return pd.DataFrame(rows)


_NAMES = {"ns-prod": "Producer", "ns-far": "FAR"}


def test_only_owned_patterns_are_exported():
    """The document is a handoff of owned work, not a dump of every pattern."""
    records = collect_associations(_groups(), _members(), _NAMES)

    assert [r["repeat_group_id"] for r in records] == ["RQ001", "RQ002"]


def test_pattern_on_two_clusters_is_labelled_multi_cluster():
    records = collect_associations(_groups(), _members(), _NAMES)
    multi = next(r for r in records if r["repeat_group_id"] == "RQ001")
    single = next(r for r in records if r["repeat_group_id"] == "RQ002")

    assert multi["scope"] == MULTI_CLUSTER
    assert multi["is_multi_cluster"]
    assert multi["clusters"] == ["FAR", "Producer"]
    assert single["scope"] == "Producer"
    assert not single["is_multi_cluster"]


def test_query_ids_are_capped_per_cluster_and_the_cap_is_disclosed():
    """Truncating silently would read as 'that is all of them'."""
    records = collect_associations(_groups(), _members(producer_ids=25), _NAMES)
    record = records[0]

    assert len(record["query_ids_by_cluster"]["Producer"]) == MAX_QUERY_IDS_PER_CLUSTER
    assert record["query_id_totals"]["Producer"] == 25
    assert len(record["query_ids_by_cluster"]["FAR"]) == 3

    markdown = export_markdown(_groups(), _members(25), cluster_names=_NAMES)
    assert f"showing {MAX_QUERY_IDS_PER_CLUSTER} of 25" in markdown


def test_records_are_ordered_by_cost():
    records = collect_associations(_groups(), _members(), _NAMES)

    runtimes = [r["total_runtime_s"] for r in records]
    assert runtimes == sorted(runtimes, reverse=True)


def test_markdown_contains_the_handoff_facts():
    markdown = export_markdown(
        _groups(), _members(), cluster_names=_NAMES,
        generated_at=datetime(2026, 7, 28, 9, 0), source="redshift.duckdb",
    )

    assert "Smith, John" in markdown
    assert "Jones, Mary" in markdown
    assert MULTI_CLUSTER in markdown
    assert "SELECT a FROM fact_orders" in markdown
    assert "```sql" in markdown
    assert "2026-07-28 09:00" in markdown
    assert "RQ003" not in markdown, "unowned patterns must not appear"


def test_empty_export_explains_how_to_record_ownership():
    markdown = export_markdown(_groups().assign(assigned_engineer="", associated_user=""),
                               pd.DataFrame())

    assert "No query groups" in markdown
    assert "Assign to Engineer" in markdown


def test_sql_containing_a_code_fence_is_still_fenced_correctly():
    groups = _groups().head(1).copy()
    groups["sample_sql"] = "SELECT '```' AS tricky"

    markdown = export_markdown(groups, _members(), cluster_names=_NAMES)

    assert "````sql" in markdown, "fence must be widened so the block does not break"


def test_engineer_and_associated_user_are_stored_independently(tmp_path):
    """Reassigning the engineer must not silently drop the user association."""
    path = tmp_path / "assoc.duckdb"
    duckdb.connect(str(path)).close()

    set_assignment(path, "K1", "jsmith", "Smith, John")
    set_association(path, "K1", "mjones", "Jones, Mary")
    set_assignment(path, "K1", "bwhite", "White, Bob")

    record = load_assignments(path)["K1"]
    assert record["engineer_display"] == "White, Bob"
    assert record["associated_user_display"] == "Jones, Mary"


def test_association_columns_are_added_to_an_older_warehouse(tmp_path):
    """query_group_assignments is PRESERVED across loads, so it is never
    recreated - the new columns must be migrated in place."""
    path = tmp_path / "old.duckdb"
    con = duckdb.connect(str(path))
    con.execute(
        "CREATE TABLE query_group_assignments (repeat_group_key VARCHAR PRIMARY KEY,"
        " user_name VARCHAR, engineer_display VARCHAR, assigned_at TIMESTAMP)"
    )
    con.execute("INSERT INTO query_group_assignments VALUES ('K1','js','Smith, John',NULL)")
    con.close()

    # Readable before migration.
    assert load_assignments(path)["K1"]["engineer_display"] == "Smith, John"

    set_association(path, "K1", "mj", "Jones, Mary")

    record = load_assignments(path)["K1"]
    assert record["engineer_display"] == "Smith, John"
    assert record["associated_user_display"] == "Jones, Mary"


def test_bubble_context_menu_offers_both_actions():
    from PySide6.QtCore import QPoint
    from PySide6.QtWidgets import QApplication, QMenu

    app = QApplication.instance() or QApplication([])
    _ = app
    import analyzer.widgets.triage_home as module

    seen = {}

    class _Menu(QMenu):
        def exec(self, *args, **kwargs):
            seen["items"] = [a.text() for a in self.actions() if a.text()]
            return None

    original = module.QMenu
    module.QMenu = _Menu
    try:
        page = module.TriagePage()
        groups = _groups().head(1)
        page.set_dataframes(groups, pd.DataFrame(), pd.DataFrame(), {"total_runtime_s": 7200.0})
        page._show_group_context_menu("RQ001", QPoint(0, 0))
    finally:
        module.QMenu = original

    items = seen.get("items", [])
    assert any("Assign to Engineer" in text for text in items)
    assert any("Associate Query to User" in text for text in items)
