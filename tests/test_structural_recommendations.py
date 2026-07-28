from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analyzer.structural_recommendations import (  # noqa: E402
    build_structural_recommendation_script,
    build_structural_recommendations,
)


def test_filter_and_join_workload_emit_sort_and_dist_recommendations():
    slow_queries = pd.DataFrame(
        [
            {
                "query_id": 101,
                "elapsed_s": 900.0,
                "dist_total_cnt": 4,
                "dist_both_cnt": 2,
                "bcast_cnt": 3,
                "total_spill": 0,
                "sql_text": """
                    SELECT *
                    FROM sales.fact_orders o
                    JOIN sales.dim_customer c
                      ON o.customer_id = c.customer_id
                    WHERE o.order_date >= dateadd(day, -7, current_date)
                """,
            }
        ]
    )
    table_review = pd.DataFrame(
        [
            {
                "source_db": "edw",
                "schema_name": "sales",
                "table_name": "fact_orders",
                "table_key": "edw.sales.fact_orders",
                "diststyle": "DISTKEY(AUTO)",
                "sortkey1": "SORTKEY(AUTO)",
                "size_mb": 4096,
                "scan_query_count": 5,
                "rrscan_query_pct": 0.0,
                "full_scan_query_pct": 1.0,
                "sort_attention_score": 70,
                "full_scan_score": 80,
                "distribution_usage_score": 90,
                "redistribution_query_count": 3,
                "broadcast_query_count": 2,
                "skew_rows": 1.0,
            },
            {
                "source_db": "edw",
                "schema_name": "sales",
                "table_name": "dim_customer",
                "table_key": "edw.sales.dim_customer",
                "diststyle": "EVEN",
                "sortkey1": "customer_id",
                "size_mb": 2048,
                "scan_query_count": 1,
                "rrscan_query_pct": 1.0,
                "full_scan_query_pct": 0.0,
                "sort_attention_score": 0,
                "full_scan_score": 0,
                "distribution_usage_score": 80,
                "redistribution_query_count": 3,
                "broadcast_query_count": 2,
                "skew_rows": 1.0,
            },
        ]
    )

    recs = build_structural_recommendations(slow_queries, table_review)

    fact = recs[recs["qualified_table"] == "sales.fact_orders"]
    assert set(fact["recommendation_type"]) >= {"SORTKEY", "DISTKEY"}
    assert "order_date" in fact[fact["recommendation_type"] == "SORTKEY"].iloc[0]["recommended_columns"]
    assert "customer_id" in fact[fact["recommendation_type"] == "DISTKEY"].iloc[0]["recommended_columns"]
    assert fact[fact["recommendation_type"] == "DISTKEY"].iloc[0]["current_distkey"] == ""


def test_structural_script_keeps_alter_statements_commented():
    recs = pd.DataFrame(
        [
            {
                "priority_rank": 1,
                "recommendation_type": "SORTKEY",
                "severity": "crit",
                "priority_score": 100,
                "qualified_table": "sales.fact_orders",
                "current_diststyle": "EVEN",
                "current_distkey": "",
                "current_sortkey": "SORTKEY(AUTO)",
                "recommended_columns": "order_date",
                "evidence": "filters on order_date",
                "query_ids": "101",
                "candidate_sql": "-- ALTER TABLE sales.fact_orders ALTER SORTKEY (order_date);",
            }
        ]
    )

    script = build_structural_recommendation_script(recs)

    # New comment convention: all prose lives in /* */ blocks, and the ONLY
    # '--'-prefixed lines are the executable ALTER statements (commented so
    # nothing runs by default). Exactly one such line for this single rec.
    alter_lines = [ln for ln in script.splitlines() if ln.strip().startswith("--")]
    assert alter_lines == ["-- ALTER TABLE sales.fact_orders ALTER SORTKEY (order_date);"]

    # No bare, runnable DDL: every non-blank line is either inside a /* */ block
    # or the commented '--' ALTER. Verify by stripping block regions.
    in_block = False
    bare_runnable = []
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("/*"):
            in_block = True
        if in_block:
            if stripped.endswith("*/"):
                in_block = False
            continue
        if not stripped.startswith("--"):
            bare_runnable.append(line)
    assert not bare_runnable, bare_runnable

    # Balanced comment delimiters overall.
    assert script.count("/*") == script.count("*/")


def test_structural_script_escapes_comment_delimiters_in_evidence():
    # Evidence text containing '*/' must not prematurely close a block comment.
    recs = pd.DataFrame(
        [
            {
                "priority_rank": 1,
                "recommendation_type": "SORTKEY",
                "severity": "crit",
                "priority_score": 100,
                "qualified_table": "sales.fact_orders",
                "current_diststyle": "EVEN",
                "current_distkey": "",
                "current_sortkey": "SORTKEY(AUTO)",
                "recommended_columns": "order_date",
                "evidence": "predicate uses a/*b*/c and ratio x*/y computation",
                "query_ids": "101",
                "candidate_sql": "-- ALTER TABLE sales.fact_orders ALTER SORTKEY (order_date);",
            }
        ]
    )
    script = build_structural_recommendation_script(recs)
    # Delimiters stay balanced despite the hostile evidence text.
    assert script.count("/*") == script.count("*/")
    # The raw hostile sequences are neutralized in the output.
    assert "*/y" not in script
    # The one executable ALTER is still present and singular.
    alter_lines = [ln for ln in script.splitlines() if ln.strip().startswith("--")]
    assert alter_lines == ["-- ALTER TABLE sales.fact_orders ALTER SORTKEY (order_date);"]


# --- Regression tests for the correctness fixes (session Jul 8 2026) ---

def _tr(**over):
    base = {
        "table_key": "db1.public.orders", "source_db": "db1", "schema_name": "public",
        "table_name": "orders", "diststyle": "EVEN", "current_distkey": "", "distkey": "",
        "sortkey1": "", "size_mb": 5000, "tbl_rows": 900000000,
        "rrscan_query_pct": 0.1, "full_scan_query_pct": 0.8, "scan_query_count": 40,
    }
    base.update(over)
    return base


def test_wrong_database_not_attributed():
    # Same schema.table in two databases; workload runs in db1. Fix must not
    # attribute recommendations to db2's unrelated table.
    tr = pd.DataFrame([
        _tr(table_key="db1.public.orders", source_db="db1", diststyle="KEY", current_distkey="claim_id", size_mb=5000, tbl_rows=900000000),
        _tr(table_key="db2.public.orders", source_db="db2", diststyle="EVEN", size_mb=10, tbl_rows=100, scan_query_count=0),
    ])
    slow = pd.DataFrame([{
        "query_id": "9", "database_name": "db1", "elapsed_s": 480.0,
        "dist_total_cnt": 8, "dist_both_cnt": 4, "bcast_cnt": 2, "total_spill": 0,
        "sql_text": "SELECT o.customer_id FROM public.orders o JOIN public.dim d ON o.customer_id = d.id WHERE o.order_date > current_date - 7",
    }])
    recs = build_structural_recommendations(slow, tr)
    assert not recs.empty
    assert (recs["table_key"] == "db1.public.orders").all()
    assert not recs["table_key"].astype(str).str.contains("db2").any()


def test_diststyle_all_never_gets_blind_distkey():
    tr = pd.DataFrame([_tr(diststyle="ALL", size_mb=200, tbl_rows=500000, table_key="db1.public.dim", table_name="dim")])
    slow = pd.DataFrame([{
        "query_id": "1", "database_name": "db1", "elapsed_s": 300.0,
        "dist_total_cnt": 6, "dist_both_cnt": 2, "bcast_cnt": 2, "total_spill": 0,
        "sql_text": "SELECT d.k FROM public.dim d JOIN public.f f ON d.dim_id = f.dim_id WHERE f.v > 1",
    }])
    recs = build_structural_recommendations(slow, tr)
    dist = recs[recs["recommendation_type"] == "DISTKEY"] if not recs.empty else recs
    assert dist.empty, "DISTSTYLE ALL table must never receive a plain DISTKEY recommendation"


def test_sortkey_size_floor_and_no_current_key_noop():
    # Tiny table missing sortkey -> no SORTKEY. Big table whose sortkey already
    # leads on the filtered column -> no no-op SORTKEY.
    tr = pd.DataFrame([
        _tr(table_key="db1.public.small", table_name="small", size_mb=10, tbl_rows=1000, scan_query_count=1),
        _tr(table_key="db1.public.big", table_name="big", size_mb=5000, sortkey1="created_at"),
    ])
    slow = pd.DataFrame([
        {"query_id": "1", "database_name": "db1", "elapsed_s": 60.0, "dist_total_cnt": 0, "dist_both_cnt": 0, "bcast_cnt": 0, "total_spill": 0,
         "sql_text": "SELECT * FROM public.small s WHERE s.status = 5"},
        {"query_id": "2", "database_name": "db1", "elapsed_s": 200.0, "dist_total_cnt": 0, "dist_both_cnt": 0, "bcast_cnt": 0, "total_spill": 0,
         "sql_text": "SELECT * FROM public.big b WHERE b.created_at > current_date - 30"},
    ])
    recs = build_structural_recommendations(slow, tr)
    if not recs.empty:
        sort = recs[recs["recommendation_type"] == "SORTKEY"]
        assert not (sort["table_key"] == "db1.public.small").any(), "tiny table must not get a SORTKEY"
        assert not (sort["table_key"] == "db1.public.big").any(), "current-sortkey no-op must be suppressed"


def test_using_join_contributes_distkey_evidence():
    tr = pd.DataFrame([
        _tr(table_key="db1.public.orders", table_name="orders", diststyle="EVEN", size_mb=8000),
        _tr(table_key="db1.public.customers", table_name="customers", diststyle="EVEN", size_mb=2000),
    ])
    slow = pd.DataFrame([{
        "query_id": "11",
        "database_name": "db1",
        "elapsed_s": 400.0,
        "dist_total_cnt": 6,
        "dist_both_cnt": 3,
        "bcast_cnt": 1,
        "total_spill": 0,
        "sql_text": """
            SELECT o.order_id
            FROM public.orders o
            JOIN public.customers c USING (customer_id)
            WHERE o.order_date > current_date - 7
        """,
    }])
    recs = build_structural_recommendations(slow, tr)
    assert not recs.empty
    dist = recs[recs["recommendation_type"] == "DISTKEY"]
    assert not dist.empty
    assert dist["recommended_columns"].astype(str).str.contains("customer_id").any()


def test_structural_script_includes_parse_coverage():
    tr = pd.DataFrame([_tr()])
    slow = pd.DataFrame([{
        "query_id": "1", "database_name": "db1", "elapsed_s": 100.0,
        "dist_total_cnt": 0, "dist_both_cnt": 0, "bcast_cnt": 0, "total_spill": 0,
        "sql_text": "SELECT * FROM public.orders WHERE order_date > current_date - 1",
    }])
    recs = build_structural_recommendations(slow, tr)
    script = build_structural_recommendation_script(recs)
    assert "Workload parse coverage" in script


def test_parse_failure_counter_is_reported():
    tr = pd.DataFrame([_tr()])
    slow = pd.DataFrame([
        {
            "query_id": "1",
            "database_name": "db1",
            "elapsed_s": 100.0,
            "dist_total_cnt": 0,
            "dist_both_cnt": 0,
            "bcast_cnt": 0,
            "total_spill": 0,
            "sql_text": "SELECT * FROM public.orders WHERE order_date > current_date - 1",
        },
        {
            "query_id": "2",
            "database_name": "db1",
            "elapsed_s": 100.0,
            "dist_total_cnt": 0,
            "dist_both_cnt": 0,
            "bcast_cnt": 0,
            "total_spill": 0,
            # Deliberately unparseable for sqlglot redshift dialect.
            "sql_text": "SELEC THIS IS NOT VALID SQL !!!! FROM nowhere",
        },
    ])
    recs = build_structural_recommendations(slow, tr)
    assert recs.attrs.get("queries_considered") == 2
    assert recs.attrs.get("parse_failures") == 1
    assert 0.0 < float(recs.attrs.get("parse_ok_rate") or 0) < 1.0
