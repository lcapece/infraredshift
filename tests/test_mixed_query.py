"""Mixed-query detection: groups touching both external and local tables."""
from __future__ import annotations

import pandas as pd

from analyzer.mixed_query import (
    CLASS_EXTERNAL_ONLY,
    CLASS_LOCAL_ONLY,
    CLASS_MIXED,
    CLASS_UNKNOWN,
    annotate_mixed_queries,
    classify_table_set,
    external_name_index,
)


def _catalog() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"schema_name": "spectrum", "table_name": "events_ext"},
            {"schema_name": "spectrum", "table_name": "clicks_ext"},
        ]
    )


def test_classify_covers_mixed_external_and_local():
    ext = external_name_index(_catalog())
    assert classify_table_set(["public.orders", "spectrum.events_ext"], ext) == CLASS_MIXED
    assert classify_table_set(["spectrum.events_ext"], ext) == CLASS_EXTERNAL_ONLY
    assert classify_table_set(["public.orders"], ext) == CLASS_LOCAL_ONLY
    assert classify_table_set([], ext) == CLASS_UNKNOWN


def test_bare_external_name_matches_unqualified_reference():
    ext = external_name_index(_catalog())
    # Query referenced the external table without its external schema prefix.
    assert classify_table_set(["orders", "events_ext"], ext) == CLASS_MIXED


def test_annotate_adds_class_column_and_leaves_unknown_without_catalog():
    groups = pd.DataFrame(
        [
            {"repeat_group_id": "RQ001", "sql_tables_full": "public.orders, spectrum.events_ext"},
            {"repeat_group_id": "RQ002", "sql_tables_full": "public.orders, public.customers"},
            {"repeat_group_id": "RQ003", "sql_tables_full": "spectrum.clicks_ext"},
        ]
    )
    out = annotate_mixed_queries(groups, _catalog())
    assert list(out["mixed_query_class"]) == [CLASS_MIXED, CLASS_LOCAL_ONLY, CLASS_EXTERNAL_ONLY]

    # No catalog -> nothing is flagged mixed (no false positives).
    out_empty = annotate_mixed_queries(groups, pd.DataFrame())
    assert set(out_empty["mixed_query_class"]) == {CLASS_UNKNOWN}


def test_quadrant_mixed_filter_and_flag(tmp_path):
    from PySide6.QtWidgets import QApplication

    _app = QApplication.instance() or QApplication([])
    from analyzer.widgets.triage_home import (
        TriagePage,
        _filter_chart_groups,
        _is_mixed_query_group,
    )

    groups = pd.DataFrame(
        [
            {"repeat_group_id": "RQ001", "query_count": 5, "total_runtime_s": 50.0,
             "total_input_rows": 100, "triage_verdict": "MONITOR", "mixed_query_class": "mixed"},
            {"repeat_group_id": "RQ002", "query_count": 5, "total_runtime_s": 50.0,
             "total_input_rows": 100, "triage_verdict": "MONITOR", "mixed_query_class": "local-only"},
        ]
    )
    assert _is_mixed_query_group(groups.iloc[0])
    assert not _is_mixed_query_group(groups.iloc[1])

    only_mixed, _count = _filter_chart_groups(groups, "rows", categories={"mixed"})
    assert list(only_mixed["repeat_group_id"]) == ["RQ001"]

    page = TriagePage()
    page.set_dataframes(groups, pd.DataFrame(), pd.DataFrame(), {})
    _app.processEvents()
    page.deleteLater()


def test_external_catalog_sql_derives_sortkey_from_partition_key_one():
    """SVV_EXTERNAL_COLUMNS-sourced catalog: sortkey = part_key=1 column, else NULL."""
    import duckdb

    from analyzer.redshift_queries import EXTERNAL_TABLES_CATALOG_SQL

    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE svv_external_columns (redshift_database_name VARCHAR, "
        "schemaname VARCHAR, tablename VARCHAR, columnname VARCHAR, part_key INTEGER)"
    )
    con.execute(
        "INSERT INTO svv_external_columns VALUES "
        "('dev','spectrum','events','id',0),"
        "('dev','spectrum','events','dt',1),"
        "('dev','spectrum','events','region',2),"
        "('dev','spectrum','lookups','code',0),"
        "('dev','spectrum','lookups','label',0)"
    )
    rows = con.execute(EXTERNAL_TABLES_CATALOG_SQL + " ORDER BY table_name").fetchall()
    cols = [d[0] for d in con.description]
    con.close()
    assert cols == [
        "external_table_key", "redshift_database_name", "schema_name", "table_name", "sortkey",
    ]
    by_table = {r[cols.index("table_name")]: r[cols.index("sortkey")] for r in rows}
    assert by_table["events"] == "dt"          # partition key part_key=1
    assert by_table["lookups"] is None         # unpartitioned -> NULL sortkey


def test_view_usage_flag_via_in_memory_set():
    from analyzer.mixed_query import annotate_view_usage, view_name_index

    views = pd.DataFrame([
        {"database": "edw", "schema": "reporting", "view_name": "daily_sales_v"},
        {"database": "edw", "schema": "reporting", "view_name": "cust_360_v"},
    ])
    idx = view_name_index(views)
    assert {"daily_sales_v", "reporting.daily_sales_v", "edw.reporting.daily_sales_v"} <= idx

    groups = pd.DataFrame([
        {"repeat_group_id": "RQ001", "sql_tables_full": "public.orders, reporting.daily_sales_v"},
        {"repeat_group_id": "RQ002", "sql_tables_full": "public.orders, public.customers"},
        {"repeat_group_id": "RQ003", "sql_tables_full": "cust_360_v"},
    ])
    out = annotate_view_usage(groups, views)
    assert list(out["uses_view"]) == [True, False, True]
    # No views loaded -> nothing flagged (no false positives).
    assert list(annotate_view_usage(groups, pd.DataFrame())["uses_view"]) == [False, False, False]


def test_view_filter_toggle_in_quadrant():
    from PySide6.QtWidgets import QApplication

    _app = QApplication.instance() or QApplication([])
    from analyzer.widgets.triage_home import _filter_chart_groups, _uses_view_group

    groups = pd.DataFrame([
        {"repeat_group_id": "RQ001", "query_count": 5, "total_runtime_s": 50.0,
         "total_input_rows": 100, "triage_verdict": "MONITOR", "uses_view": True},
        {"repeat_group_id": "RQ002", "query_count": 5, "total_runtime_s": 50.0,
         "total_input_rows": 100, "triage_verdict": "MONITOR", "uses_view": False},
    ])
    assert _uses_view_group(groups.iloc[0]) and not _uses_view_group(groups.iloc[1])
    only_view, _n = _filter_chart_groups(groups, "rows", categories={"view"})
    assert list(only_view["repeat_group_id"]) == ["RQ001"]
