from __future__ import annotations

import pandas as pd

from analyzer.query_optimizer import build_friendly_fix, optimize_redshift_sql


def _tables(*rows: dict, **single_row) -> pd.DataFrame:
    defaults = {
        "source_db": "edw",
        "schema_name": "sales",
        "diststyle": "EVEN",
        "sortkey1": "",
        "size_mb": 20_000,
        "tbl_rows": 500_000_000,
        "stats_off": 0,
        "unsorted_pct": 0,
        "full_scan_score": 85,
        "rrscan_query_pct": 10,
    }
    if single_row:
        rows = (*rows, single_row)
    return pd.DataFrame([{**defaults, **row} for row in rows])


def test_date_function_on_leading_sortkey_becomes_half_open_range():
    tables = _tables(
        table_name="fact_orders",
        table_key="edw.sales.fact_orders",
        sortkey1="order_date",
    )
    result = optimize_redshift_sql(
        "SELECT f.order_id FROM sales.fact_orders f WHERE DATE(f.order_date) = '2026-07-07'",
        tables,
    )

    assert result.parse_ok and result.changed
    assert "f.order_date >= CAST('2026-07-07' AS DATE)" in result.rewritten_sql
    assert "DATEADD(DAY, 1, CAST('2026-07-07' AS DATE))" in result.rewritten_sql
    assert "DATE(f.order_date)" not in result.rewritten_sql
    assert {change.rule_id for change in result.changes} == {"bare-sortkey-date-range"}
    assert not any(note.startswith("BLOCKED:") for note in result.validation_notes)


def test_year_function_on_leading_sortkey_becomes_year_range():
    tables = _tables(
        table_name="fact_orders",
        table_key="edw.sales.fact_orders",
        sortkey1="order_date",
    )
    result = optimize_redshift_sql(
        "SELECT f.order_id FROM sales.fact_orders f WHERE EXTRACT(YEAR FROM f.order_date) = 2025",
        tables,
    )

    assert result.changed
    assert "f.order_date >= CAST('2025-01-01' AS DATE)" in result.rewritten_sql
    assert "f.order_date < CAST('2026-01-01' AS DATE)" in result.rewritten_sql


def test_inner_join_filter_is_propagated_to_other_tables_sortkey():
    tables = _tables(
        {
            "table_name": "fact_orders",
            "table_key": "edw.sales.fact_orders",
            "diststyle": "KEY(customer_id)",
            "sortkey1": "order_date",
        },
        {
            "table_name": "dim_customer",
            "table_key": "edw.sales.dim_customer",
            "diststyle": "KEY(customer_id)",
            "sortkey1": "customer_id",
            "size_mb": 5_000,
        },
    )
    result = optimize_redshift_sql(
        """
SELECT f.order_id
FROM sales.fact_orders f
JOIN sales.dim_customer d ON f.customer_id = d.customer_id
WHERE f.customer_id = 42
""",
        tables,
    )

    assert result.changed
    assert "d.customer_id = 42" in result.rewritten_sql
    assert "propagate-inner-join-sort-filter" in {change.rule_id for change in result.changes}


def test_filter_is_not_propagated_across_left_join():
    tables = _tables(
        {
            "table_name": "fact_orders",
            "table_key": "edw.sales.fact_orders",
            "sortkey1": "order_date",
        },
        {
            "table_name": "dim_customer",
            "table_key": "edw.sales.dim_customer",
            "sortkey1": "customer_id",
        },
    )
    result = optimize_redshift_sql(
        """
SELECT f.order_id
FROM sales.fact_orders f
LEFT JOIN sales.dim_customer d ON f.customer_id = d.customer_id
WHERE f.customer_id = 42
""",
        tables,
    )

    assert not result.changed
    assert result.rewritten_sql.count("customer_id = 42") == 1


def test_distinct_is_removed_only_when_all_group_keys_are_projected():
    tables = _tables(table_name="fact_orders", table_key="edw.sales.fact_orders")
    result = optimize_redshift_sql(
        """
SELECT DISTINCT f.customer_id, f.order_date, COUNT(*) AS orders
FROM sales.fact_orders f
GROUP BY f.customer_id, f.order_date
""",
        tables,
    )

    assert result.changed
    assert "DISTINCT" not in result.rewritten_sql
    assert "remove-redundant-distinct" in {change.rule_id for change in result.changes}


def test_non_sortkey_function_is_advisory_only_and_not_rewritten():
    tables = _tables(
        table_name="fact_orders",
        table_key="edw.sales.fact_orders",
        sortkey1="order_date",
    )
    result = optimize_redshift_sql(
        "SELECT f.order_id FROM sales.fact_orders f WHERE DATE(f.created_at) = '2026-07-07'",
        tables,
    )

    assert result.parse_ok
    assert not result.changed
    assert "DATE(f.created_at)" in result.rewritten_sql


def test_complex_cte_heart_is_materialized_with_downstream_keys():
    tables = _tables(
        {
            "table_name": "fact_orders",
            "table_key": "edw.sales.fact_orders",
            "diststyle": "KEY(customer_id)",
            "sortkey1": "order_date",
            "size_mb": 80_000,
        },
        {
            "table_name": "dim_customer",
            "table_key": "edw.sales.dim_customer",
            "diststyle": "KEY(customer_id)",
            "sortkey1": "customer_id",
            "size_mb": 5_000,
        },
    )
    result = optimize_redshift_sql(
        """
WITH heart AS (
  SELECT f.customer_id, f.order_date, SUM(f.amount) AS amount
  FROM sales.fact_orders f
  GROUP BY f.customer_id, f.order_date
)
SELECT h.customer_id, h.amount
FROM heart h
JOIN sales.dim_customer d ON h.customer_id = d.customer_id
WHERE h.order_date >= DATE '2026-01-01'
""",
        tables,
    )

    plans = [plan for plan in result.decompositions if plan.plan_id == "cte-heart-staging"]
    assert len(plans) == 1
    script = plans[0].script
    assert "CREATE TEMP TABLE tmp_opt_heart" in script
    assert "DISTKEY (customer_id)" in script
    assert "COMPOUND SORTKEY (order_date, customer_id)" in script
    assert "FROM tmp_opt_heart AS h" in script
    assert "WITH heart AS" not in script


def test_large_filtered_fact_heart_is_projected_and_staged_before_join():
    tables = _tables(
        {
            "table_name": "fact_orders",
            "table_key": "edw.sales.fact_orders",
            "diststyle": "EVEN",
            "sortkey1": "order_date",
            "size_mb": 120_000,
        },
        {
            "table_name": "dim_customer",
            "table_key": "edw.sales.dim_customer",
            "diststyle": "KEY(customer_id)",
            "sortkey1": "customer_id",
            "size_mb": 6_000,
        },
    )
    result = optimize_redshift_sql(
        """
SELECT f.order_id, d.customer_name
FROM sales.fact_orders f
JOIN sales.dim_customer d ON f.customer_id = d.customer_id
WHERE f.order_date >= DATE '2026-01-01'
  AND f.order_date < DATE '2026-02-01'
""",
        tables,
    )

    plans = [plan for plan in result.decompositions if plan.plan_id == "filtered-fact-heart"]
    assert len(plans) == 1
    script = plans[0].script
    assert "CREATE TEMP TABLE tmp_opt_fact_orders_heart" in script
    assert "DISTKEY (customer_id)" in script
    assert "COMPOUND SORTKEY (order_date, customer_id)" in script
    assert "f.customer_id" in script
    assert "f.order_date" in script
    assert "f.order_id" in script
    assert "FROM tmp_opt_fact_orders_heart AS f" in script


def test_friendly_fixer_explains_a_simple_rewrite_without_optimizer_jargon():
    result = optimize_redshift_sql(
        "SELECT f.order_id FROM sales.fact_orders f WHERE DATE(f.order_date) = '2026-07-07'",
        _tables(
            table_name="fact_orders",
            table_key="edw.sales.fact_orders",
            sortkey1="order_date",
        ),
    )

    fix = build_friendly_fix(result)

    assert fix.status == "ready_single"
    assert fix.can_apply_in_editor
    assert not fix.is_multistep
    assert "simpler, faster" in fix.headline.lower()
    assert any("skip unrelated blocks" in reason for reason in fix.why_it_helps)
    assert "DISTKEY" not in " ".join(fix.why_it_helps)


def test_friendly_fixer_recommends_the_large_fact_heart_as_one_clear_choice():
    result = optimize_redshift_sql(
        """
SELECT f.order_id, d.customer_name
FROM sales.fact_orders f
JOIN sales.dim_customer d ON f.customer_id = d.customer_id
WHERE f.order_date >= DATE '2026-01-01'
  AND f.order_date < DATE '2026-02-01'
""",
        _tables(
            {
                "table_name": "fact_orders",
                "table_key": "edw.sales.fact_orders",
                "diststyle": "EVEN",
                "sortkey1": "order_date",
                "size_mb": 120_000,
            },
            {
                "table_name": "dim_customer",
                "table_key": "edw.sales.dim_customer",
                "diststyle": "KEY(customer_id)",
                "sortkey1": "customer_id",
                "size_mb": 6_000,
            },
        ),
    )

    fix = build_friendly_fix(result)

    assert fix.status == "ready_multistep"
    assert fix.is_multistep
    assert not fix.can_apply_in_editor
    assert "CREATE TEMP TABLE tmp_opt_fact_orders_heart" in fix.sql
    assert "largest table" in " ".join(fix.why_it_helps)
    assert any("entire script" in step.lower() for step in fix.next_steps)


def test_friendly_fixer_refuses_to_guess_and_explains_select_star_plainly():
    result = optimize_redshift_sql(
        "SELECT * FROM sales.fact_orders",
        _tables(table_name="fact_orders", table_key="edw.sales.fact_orders"),
    )

    fix = build_friendly_fix(result)

    assert fix.status == "review"
    assert not fix.can_apply_in_editor
    assert "will not guess" in fix.explanation
    assert any("every column" in item for item in fix.why_it_helps)
