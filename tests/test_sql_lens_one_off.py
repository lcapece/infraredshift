from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analyzer.sql_lens import analyze_console_sql  # noqa: E402


def test_where_equality_between_tables_is_treated_as_implicit_join_risk():
    tables = pd.DataFrame(
        [
            {
                "source_db": "edw",
                "schema_name": "sales",
                "table_name": "fact_orders",
                "table_key": "edw.sales.fact_orders",
                "diststyle": "DISTKEY(AUTO)",
                "sortkey1": "SORTKEY(AUTO)",
                "size_mb": 20_000,
                "tbl_rows": 500_000_000,
                "full_scan_score": 90,
                "distribution_usage_score": 92,
                "sort_attention_score": 80,
            },
            {
                "source_db": "edw",
                "schema_name": "sales",
                "table_name": "dim_customer",
                "table_key": "edw.sales.dim_customer",
                "diststyle": "EVEN",
                "sortkey1": "",
                "size_mb": 4_000,
                "tbl_rows": 80_000_000,
                "full_scan_score": 20,
                "distribution_usage_score": 88,
                "sort_attention_score": 10,
            },
        ]
    )
    sql = """
        SELECT *
        FROM sales.fact_orders f, sales.dim_customer d
        WHERE f.customer_id = d.customer_id
          AND f.order_date = DATE '2026-07-07'
    """

    analysis = analyze_console_sql(sql, tables)

    implicit = analysis.predicates[analysis.predicates["predicate_role"] == "implicit join"]
    assert not implicit.empty
    assert "implicit join equality" in str(implicit.iloc[0]["sortkey_alignment"])
    assert str(implicit.iloc[0]["severity"]) == "crit"
    assert "DISTKEY" in str(implicit.iloc[0]["recommendation"]) or "distribution" in str(
        implicit.iloc[0]["recommendation"]
    ).lower()

    fact_role = analysis.tables[analysis.tables["table_name"] == "fact_orders"].iloc[0]["role"]
    assert "join" in str(fact_role).lower()
    assert "filter" in str(fact_role).lower()


def test_auto_keys_do_not_count_as_sort_or_dist_alignment():
    tables = pd.DataFrame(
        [
            {
                "source_db": "edw",
                "schema_name": "sales",
                "table_name": "fact_orders",
                "table_key": "edw.sales.fact_orders",
                "diststyle": "KEY(AUTO)",
                "sortkey1": "AUTO(SORTKEY)",
                "size_mb": 20_000,
                "tbl_rows": 500_000_000,
                "full_scan_score": 90,
            }
        ]
    )

    analysis = analyze_console_sql("SELECT * FROM sales.fact_orders f WHERE f.order_date = DATE '2026-07-07'", tables)

    assert "predicate uses sort key" not in set(analysis.predicates["sortkey_alignment"].astype(str))
    assert "order_date" in str(analysis.predicates.iloc[0]["columns"])


def test_join_and_predicate_visual_signals_are_semantic():
    tables = pd.DataFrame(
        [
            {
                "source_db": "edw", "schema_name": "sales", "table_name": "orders",
                "table_key": "edw.sales.orders", "diststyle": "KEY(customer_id)",
                "sortkey1": "customer_id", "size_mb": 20_000, "tbl_rows": 500_000_000,
            },
            {
                "source_db": "edw", "schema_name": "sales", "table_name": "customers",
                "table_key": "edw.sales.customers", "diststyle": "KEY(customer_id)",
                "sortkey1": "customer_id", "size_mb": 5_000, "tbl_rows": 80_000_000,
            },
        ]
    )
    analysis = analyze_console_sql(
        "SELECT * FROM sales.orders o JOIN sales.customers c "
        "ON o.customer_id = c.customer_id WHERE o.customer_id = 42",
        tables,
    )

    assert analysis.joins.iloc[0]["join_signal"] == "MERGE JOIN CANDIDATE"
    assert analysis.joins.iloc[0]["visual_status"] == "green"
    assert analysis.predicates.iloc[0]["predicate_signal"] == "SORT-KEY ALIGNED FILTER"
    assert analysis.predicates.iloc[0]["visual_status"] == "green"


def test_cross_join_is_red_and_unproven_join_is_amber():
    problematic = analyze_console_sql("SELECT * FROM public.a CROSS JOIN public.b", pd.DataFrame())
    ordinary = analyze_console_sql(
        "SELECT * FROM public.a a JOIN public.b b ON a.id = b.id",
        pd.DataFrame(),
    )

    assert problematic.joins.iloc[0]["visual_status"] == "red"
    assert problematic.joins.iloc[0]["join_signal"] == "PROBLEM JOIN"
    assert ordinary.joins.iloc[0]["visual_status"] == "amber"
    assert "HASH JOIN" in ordinary.joins.iloc[0]["join_signal"]
