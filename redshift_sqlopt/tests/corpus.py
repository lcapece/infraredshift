"""A diverse 100-query Redshift corpus with a deliberately flawed catalog.

This is the package's realism harness. Unit tests prove one rule against one
shape; this proves the whole pipeline against a workload that looks like
something a real BI tool and a real analyst would produce between them —
including the queries nobody is proud of.

Two design choices worth stating:

**The catalog is intentionally bad.** Real warehouses are not uniformly
well-designed: some tables have no distribution key, some have a sort key nobody
filters on, some are skewed, some have stale statistics, some are ``DISTSTYLE
ALL`` at a size where that hurts. Findings that only fire against a clean
catalog are findings that never fire in production.

**Timings are scaled down.** A "slow" query here is ~5s, not ~300s. Findings key
off ratios (estimate error multiples, spill against a floor, broadcast row
counts), so the shapes are preserved while the numbers stay readable. Each
scenario states its own absolute values rather than trusting a global scale.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from redshift_sqlopt import Catalog

# ---------------------------------------------------------------------------
# Catalog: a star schema with realistic physical-design problems
# ---------------------------------------------------------------------------

TABLE_ROWS = [
    # -- well designed: the control group ---------------------------------
    {
        "database": "dw", "schema": "public", "table": "fact_sales",
        "distkey": "cust_id", "diststyle": "KEY", "sortkeys": "sale_date",
        "not_null_columns": "sale_id,cust_id,sale_date",
        "row_count": 40_000_000, "size_mb": 8_600,
        "unsorted_pct": 2.0, "stats_off_pct": 1.0, "skew_ratio": 1.1,
    },
    # -- no distkey AND no sortkey: worst case ----------------------------
    {
        "database": "dw", "schema": "public", "table": "fact_events",
        "distkey": "", "diststyle": "EVEN", "sortkeys": "",
        "row_count": 25_000_000, "size_mb": 5_200,
        "unsorted_pct": 88.0, "stats_off_pct": 64.0, "skew_ratio": 7.5,
    },
    # -- distkey on a low-cardinality column: severe skew -----------------
    {
        "database": "dw", "schema": "public", "table": "fact_clicks",
        "distkey": "channel", "diststyle": "KEY", "sortkeys": "click_ts",
        "row_count": 120_000_000, "size_mb": 19_400,
        "unsorted_pct": 31.0, "stats_off_pct": 12.0, "skew_ratio": 22.0,
    },
    # -- sort key nobody filters on: pruning never happens ----------------
    {
        "database": "dw", "schema": "public", "table": "fact_inventory",
        "distkey": "sku_id", "diststyle": "KEY", "sortkeys": "warehouse_code",
        "not_null_columns": "sku_id",
        "row_count": 8_000_000, "size_mb": 1_900,
        "unsorted_pct": 6.0, "stats_off_pct": 3.0, "skew_ratio": 1.4,
    },
    # -- stale statistics: planner works from fiction ---------------------
    {
        "database": "dw", "schema": "public", "table": "fact_payments",
        "distkey": "cust_id", "diststyle": "KEY", "sortkeys": "paid_at",
        "row_count": 60_000_000, "size_mb": 11_000,
        "unsorted_pct": 41.0, "stats_off_pct": 97.0, "skew_ratio": 1.2,
    },
    # -- DISTSTYLE ALL on a table far too large for it --------------------
    {
        "database": "dw", "schema": "public", "table": "dim_address",
        "distkey": "", "diststyle": "ALL", "sortkeys": "addr_id",
        "row_count": 14_000_000, "size_mb": 3_400,
        "unsorted_pct": 18.0, "stats_off_pct": 22.0,
    },
    # -- dimensions, mostly fine -----------------------------------------
    {
        "database": "dw", "schema": "public", "table": "dim_customer",
        "distkey": "cust_id", "diststyle": "KEY", "sortkeys": "cust_id",
        "not_null_columns": "cust_id", "row_count": 2_000_000, "size_mb": 240,
    },
    {
        "database": "dw", "schema": "public", "table": "dim_product",
        "distkey": "prod_id", "diststyle": "ALL", "sortkeys": "prod_id",
        "not_null_columns": "prod_id", "row_count": 80_000, "size_mb": 12,
    },
    # -- nullable join key: NOT IN rewrites must refuse against this ------
    {
        "database": "dw", "schema": "public", "table": "dim_campaign",
        "distkey": "campaign_id", "diststyle": "KEY", "sortkeys": "campaign_id",
        "row_count": 400_000, "size_mb": 45,
    },
    # -- staging table, no design at all ---------------------------------
    {
        "database": "dw", "schema": "staging", "table": "stg_orders_raw",
        "distkey": "", "diststyle": "EVEN", "sortkeys": "",
        "row_count": 3_000_000, "size_mb": 1_400,
        "unsorted_pct": 100.0, "stats_off_pct": 100.0,
    },
]

VIEW_ROWS = [
    {
        "database": "dw", "schema": "reporting", "view": "v_sales",
        "sql": (
            "SELECT sale_id, cust_id, prod_id, amount, sale_date "
            "FROM dw.public.fact_sales"
        ),
    },
    {
        "database": "dw", "schema": "reporting", "view": "v_sales_enriched",
        "sql": (
            "SELECT s.sale_id, s.cust_id, s.amount, s.sale_date, c.region, c.segment "
            "FROM dw.public.fact_sales s "
            "JOIN dw.public.dim_customer c ON s.cust_id = c.cust_id"
        ),
    },
    # A view containing DISTINCT: a predicate-pushdown barrier.
    {
        "database": "dw", "schema": "reporting", "view": "v_distinct_custs",
        "sql": "SELECT DISTINCT cust_id, region FROM dw.public.dim_customer",
    },
    # Nested view: exercises recursive explosion.
    {
        "database": "dw", "schema": "reporting", "view": "v_nested",
        "sql": "SELECT cust_id, amount FROM dw.reporting.v_sales WHERE amount > 0",
    },
]

CATALOG = Catalog.from_rows(table_rows=TABLE_ROWS, view_rows=VIEW_ROWS)


@dataclass(frozen=True)
class Scenario:
    """One query plus what the optimizer is expected to do with it."""

    name: str
    sql: str
    category: str
    expect_applied: tuple[str, ...] = ()
    """Rule codes that MUST fire."""
    expect_blocked: tuple[str, ...] = ()
    """Rule codes that MUST refuse (soundness cases)."""
    expect_no_rewrite: bool = False
    """True when the query is already fine, or nothing is provable."""
    expect_parse_failure: bool = False
    notes: str = ""
    plan: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# 1. Sargability — function-wrapped sort keys (8)
# ---------------------------------------------------------------------------

SARGABILITY = [
    Scenario(
        "date_wrapper_on_sortkey",
        "SELECT sale_id, amount FROM dw.public.fact_sales "
        "WHERE DATE(sale_date) = '2024-03-15'",
        "sargability",
        expect_applied=("SARGABLE_SORTKEY",),
        notes="DATE() blocks zone-map pruning on the sort key",
    ),
    Scenario(
        "cast_to_date_on_sortkey",
        "SELECT sale_id FROM dw.public.fact_sales "
        "WHERE CAST(sale_date AS DATE) = '2024-03-15'",
        "sargability",
        expect_applied=("SARGABLE_SORTKEY",),
    ),
    Scenario(
        "date_trunc_day_on_sortkey",
        "SELECT sale_id FROM dw.public.fact_sales "
        "WHERE DATE_TRUNC('day', sale_date) = '2024-03-15'",
        "sargability",
        expect_applied=("SARGABLE_SORTKEY",),
    ),
    Scenario(
        "date_trunc_month_must_refuse",
        "SELECT sale_id FROM dw.public.fact_sales "
        "WHERE DATE_TRUNC('month', sale_date) = '2024-03-01'",
        "sargability",
        expect_no_rewrite=True,
        notes="a month is not a day: the one-day range rewrite would be wrong",
    ),
    Scenario(
        "numeric_cast_must_refuse",
        "SELECT sale_id FROM dw.public.fact_sales "
        "WHERE CAST(amount AS VARCHAR) = '100'",
        "sargability",
        expect_no_rewrite=True,
        notes="amount is not a date; DATEADD would be meaningless",
    ),
    Scenario(
        "wrapper_on_non_sortkey_is_ignored",
        "SELECT sale_id FROM dw.public.fact_sales WHERE DATE(created_at) = '2024-03-15'",
        "sargability",
        expect_no_rewrite=True,
        notes="no zone-map benefit to claim on a non-sort-key column",
    ),
    Scenario(
        "wrapper_on_unkeyed_table",
        "SELECT event_id FROM dw.public.fact_events WHERE DATE(event_ts) = '2024-03-15'",
        "sargability",
        expect_no_rewrite=True,
        notes="fact_events has NO sort key, so nothing to make sargable",
    ),
    Scenario(
        "non_date_literal_must_refuse",
        "SELECT sale_id FROM dw.public.fact_sales WHERE DATE(sale_date) = 'garbage'",
        "sargability",
        expect_no_rewrite=True,
    ),
]

# ---------------------------------------------------------------------------
# 2. NULL semantics — NOT IN / NOT EXISTS (8)
# ---------------------------------------------------------------------------

NULL_SEMANTICS = [
    Scenario(
        "qualified_not_in_proven_not_null",
        "SELECT s.sale_id FROM dw.public.fact_sales s WHERE s.cust_id NOT IN "
        "(SELECT d.cust_id FROM dw.public.dim_customer d)",
        "null_semantics",
        expect_applied=("NOT_IN_TO_NOT_EXISTS",),
        notes="dim_customer.cust_id is catalogued NOT NULL",
    ),
    Scenario(
        "unqualified_not_in_must_refuse",
        "SELECT sale_id FROM dw.public.fact_sales WHERE cust_id NOT IN "
        "(SELECT cust_id FROM dw.public.dim_customer)",
        "null_semantics",
        expect_blocked=("NOT_IN_TO_NOT_EXISTS",),
        notes="unqualified would emit `cust_id = cust_id`, a tautology",
    ),
    Scenario(
        "not_in_nullable_column_must_refuse",
        "SELECT s.sale_id FROM dw.public.fact_sales s WHERE s.campaign_id NOT IN "
        "(SELECT c.campaign_id FROM dw.public.dim_campaign c)",
        "null_semantics",
        expect_blocked=("NOT_IN_TO_NOT_EXISTS",),
        notes="dim_campaign.campaign_id nullability is unproven",
    ),
    Scenario(
        "not_in_with_subquery_where",
        "SELECT s.sale_id FROM dw.public.fact_sales s WHERE s.cust_id NOT IN "
        "(SELECT d.cust_id FROM dw.public.dim_customer d WHERE d.region = 'EMEA')",
        "null_semantics",
        expect_applied=("NOT_IN_TO_NOT_EXISTS",),
        notes="existing WHERE must survive the correlation merge",
    ),
    Scenario(
        "not_in_multi_column_must_refuse",
        "SELECT s.sale_id FROM dw.public.fact_sales s WHERE s.cust_id NOT IN "
        "(SELECT d.cust_id, d.region FROM dw.public.dim_customer d)",
        "null_semantics",
        expect_blocked=("NOT_IN_TO_NOT_EXISTS",),
    ),
    Scenario(
        "not_in_multi_source_subquery_must_refuse",
        "SELECT s.sale_id FROM dw.public.fact_sales s WHERE s.cust_id NOT IN "
        "(SELECT d.cust_id FROM dw.public.dim_customer d "
        "JOIN dw.public.dim_address a ON d.cust_id = a.addr_id)",
        "null_semantics",
        expect_blocked=("NOT_IN_TO_NOT_EXISTS",),
        notes="two inner sources make the inner qualifier ambiguous",
    ),
    Scenario(
        "plain_in_is_untouched",
        "SELECT s.sale_id FROM dw.public.fact_sales s WHERE s.cust_id IN "
        "(SELECT d.cust_id FROM dw.public.dim_customer d)",
        "null_semantics",
        expect_no_rewrite=True,
        notes="only NOT IN carries the NULL trap",
    ),
    Scenario(
        "not_in_over_literal_list",
        "SELECT sale_id FROM dw.public.fact_sales WHERE cust_id NOT IN (1, 2, 3)",
        "null_semantics",
        expect_no_rewrite=True,
        notes="a literal list has no subquery to correlate",
    ),
]

# ---------------------------------------------------------------------------
# 3. Window functions over joins — the hard cases (14)
# ---------------------------------------------------------------------------

WINDOWS = [
    Scenario(
        "three_way_join_multi_window",
        "SELECT s.sale_id, c.region, p.category, s.amount, "
        "ROW_NUMBER() OVER (PARTITION BY c.region, p.category ORDER BY s.amount DESC) rk, "
        "SUM(s.amount) OVER (PARTITION BY c.region) region_total, "
        "AVG(s.amount) OVER (PARTITION BY p.category ORDER BY s.sale_date "
        "ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) rolling "
        "FROM dw.public.fact_sales s "
        "JOIN dw.public.dim_customer c ON s.cust_id = c.cust_id "
        "JOIN dw.public.dim_product p ON s.prod_id = p.prod_id "
        "WHERE DATE(s.sale_date) = '2024-03-15'",
        "window",
        expect_applied=("SARGABLE_SORTKEY",),
        notes="three windows with different frames over a 3-way join",
    ),
    Scenario(
        "window_partition_not_distkey",
        "SELECT cust_id, amount, SUM(amount) OVER (PARTITION BY channel) ch_total "
        "FROM dw.public.fact_clicks WHERE DATE(click_ts) = '2024-03-15'",
        "window",
        expect_applied=("SARGABLE_SORTKEY",),
        notes="partitioning on a 22x-skewed distkey",
    ),
    Scenario(
        "lag_lead_over_self_join",
        "SELECT a.sale_id, b.amount, "
        "LAG(a.amount) OVER (PARTITION BY a.cust_id ORDER BY a.sale_date) prev, "
        "LEAD(a.amount) OVER (PARTITION BY a.cust_id ORDER BY a.sale_date) nxt "
        "FROM dw.public.fact_sales a "
        "JOIN dw.public.fact_sales b ON a.cust_id = b.cust_id WHERE a.cust_id = 777",
        "window",
        expect_applied=("PROPAGATE_JOIN_FILTER",),
    ),
    Scenario(
        "window_in_cte_then_filter",
        "WITH ranked AS (SELECT s.cust_id, s.amount, "
        "RANK() OVER (PARTITION BY s.cust_id ORDER BY s.amount DESC) r "
        "FROM dw.public.fact_sales s WHERE CAST(s.sale_date AS DATE) = '2024-01-01') "
        "SELECT cust_id, amount FROM ranked WHERE r <= 3",
        "window",
        expect_applied=("SARGABLE_SORTKEY",),
    ),
    Scenario(
        "nested_window_distinct_cte_chain",
        "WITH ranked AS (SELECT s.cust_id, s.amount, "
        "RANK() OVER (PARTITION BY s.cust_id ORDER BY s.amount DESC) r "
        "FROM dw.public.fact_sales s WHERE DATE(s.sale_date) = '2024-01-01'), "
        "top_n AS (SELECT DISTINCT cust_id, amount FROM ranked WHERE r <= 3 "
        "GROUP BY cust_id, amount) "
        "SELECT t.cust_id, c.region, t.amount FROM top_n t "
        "JOIN dw.public.dim_customer c ON t.cust_id = c.cust_id",
        "window",
        expect_applied=("SARGABLE_SORTKEY", "REDUNDANT_DISTINCT"),
        notes="two rules must compose across a CTE chain",
    ),
    Scenario(
        "ntile_over_view",
        "SELECT * FROM (SELECT v.cust_id, v.amount, "
        "NTILE(4) OVER (PARTITION BY v.cust_id ORDER BY v.amount) q "
        "FROM dw.reporting.v_sales v) z WHERE z.q = 1",
        "window",
        notes="view inlining beneath a window",
    ),
    Scenario(
        "dense_rank_left_join_filter",
        "SELECT s.sale_id, c.region, "
        "DENSE_RANK() OVER (PARTITION BY c.region ORDER BY s.amount) dr "
        "FROM dw.public.fact_sales s "
        "LEFT JOIN dw.public.dim_customer c ON s.cust_id = c.cust_id "
        "WHERE s.cust_id = 555",
        "window",
        expect_blocked=("PROPAGATE_JOIN_FILTER",),
        notes="LEFT JOIN: propagation would drop preserved rows",
    ),
    Scenario(
        "first_last_value_frames",
        "SELECT cust_id, "
        "FIRST_VALUE(amount) OVER (PARTITION BY cust_id ORDER BY sale_date "
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f, "
        "LAST_VALUE(amount) OVER (PARTITION BY cust_id ORDER BY sale_date "
        "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) l "
        "FROM dw.public.fact_sales",
        "window",
        expect_no_rewrite=True,
    ),
    Scenario(
        "window_over_union_all",
        "SELECT cust_id, ROW_NUMBER() OVER (ORDER BY amount DESC) rn FROM "
        "(SELECT cust_id, amount FROM dw.public.fact_sales "
        "UNION ALL SELECT cust_id, amount FROM dw.public.fact_payments) u",
        "window",
        expect_no_rewrite=True,
    ),
    Scenario(
        "percentile_cont_group",
        "SELECT c.region, "
        "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.amount) med "
        "FROM dw.public.fact_sales s "
        "JOIN dw.public.dim_customer c ON s.cust_id = c.cust_id GROUP BY c.region",
        "window",
        expect_no_rewrite=True,
    ),
    Scenario(
        "window_with_qualified_not_in_and_sargable",
        "SELECT s.cust_id, SUM(s.amount) OVER (PARTITION BY s.cust_id) t "
        "FROM dw.public.fact_sales s WHERE DATE(s.sale_date) = '2024-05-05' "
        "AND s.cust_id NOT IN (SELECT d.cust_id FROM dw.public.dim_customer d)",
        "window",
        expect_applied=("SARGABLE_SORTKEY", "NOT_IN_TO_NOT_EXISTS"),
        notes="multi-rule composition on a window query",
    ),
    Scenario(
        "unqualified_not_in_inside_window_query",
        "SELECT cust_id, ROW_NUMBER() OVER (ORDER BY amount DESC) rn "
        "FROM dw.public.fact_sales "
        "WHERE cust_id NOT IN (SELECT cust_id FROM dw.public.dim_customer)",
        "window",
        expect_blocked=("NOT_IN_TO_NOT_EXISTS",),
        notes="tautology trap must still be caught inside a window query",
    ),
    Scenario(
        "cumulative_sum_skewed_table",
        "SELECT channel, click_ts, "
        "SUM(1) OVER (PARTITION BY channel ORDER BY click_ts) running "
        "FROM dw.public.fact_clicks",
        "window",
        expect_no_rewrite=True,
        notes="22x skew on the partition column",
    ),
    Scenario(
        "window_with_redundant_distinct",
        "SELECT DISTINCT cust_id, COUNT(*) OVER (PARTITION BY cust_id) n "
        "FROM dw.public.fact_sales",
        "window",
        notes="DISTINCT without GROUP BY: must be kept",
    ),
]

# ---------------------------------------------------------------------------
# 4. Redundant work (10)
# ---------------------------------------------------------------------------

REDUNDANT = [
    Scenario(
        "distinct_over_group_by",
        "SELECT DISTINCT cust_id, COUNT(*) AS n FROM dw.public.fact_sales GROUP BY cust_id",
        "redundant",
        expect_applied=("REDUNDANT_DISTINCT",),
    ),
    Scenario(
        "distinct_group_by_with_having",
        "SELECT DISTINCT cust_id, COUNT(*) AS n FROM dw.public.fact_sales "
        "GROUP BY cust_id HAVING COUNT(*) > 5",
        "redundant",
        expect_applied=("REDUNDANT_DISTINCT",),
    ),
    Scenario(
        "distinct_with_ungrouped_column_kept",
        "SELECT DISTINCT cust_id, region FROM dw.public.dim_customer GROUP BY cust_id",
        "redundant",
        expect_blocked=("REDUNDANT_DISTINCT",),
        notes="region is neither grouped nor aggregated: DISTINCT is load-bearing",
    ),
    Scenario(
        "distinct_no_group_by_kept",
        "SELECT DISTINCT cust_id FROM dw.public.fact_sales",
        "redundant",
        expect_no_rewrite=True,
    ),
    Scenario(
        "distinct_multi_key_group",
        "SELECT DISTINCT c.region, p.category, COUNT(*) n "
        "FROM dw.public.fact_sales s "
        "JOIN dw.public.dim_customer c ON s.cust_id = c.cust_id "
        "JOIN dw.public.dim_product p ON s.prod_id = p.prod_id "
        "GROUP BY c.region, p.category HAVING COUNT(*) > 10",
        "redundant",
        expect_applied=("REDUNDANT_DISTINCT",),
    ),
    Scenario(
        "count_distinct_single_node",
        "SELECT COUNT(DISTINCT cust_id) FROM dw.public.fact_sales "
        "WHERE sale_date >= '2024-01-01'",
        "redundant",
        expect_no_rewrite=True,
        notes="COUNT(DISTINCT) gathers to one slice; not yet rewritten",
    ),
    Scenario(
        "select_star_through_view",
        "SELECT * FROM dw.reporting.v_sales_enriched WHERE amount > 100",
        "redundant",
        notes="SELECT * across an inlined 2-table view",
    ),
    Scenario(
        "distinct_over_union",
        "SELECT DISTINCT cust_id FROM "
        "(SELECT cust_id FROM dw.public.fact_sales "
        "UNION SELECT cust_id FROM dw.public.fact_payments) u",
        "redundant",
        expect_no_rewrite=True,
        notes="UNION already dedupes; DISTINCT on top is redundant but unhandled",
    ),
    Scenario(
        "group_by_all_projected",
        "SELECT cust_id, sale_date, SUM(amount) FROM dw.public.fact_sales "
        "GROUP BY cust_id, sale_date",
        "redundant",
        expect_no_rewrite=True,
    ),
    Scenario(
        "distinct_star",
        "SELECT DISTINCT * FROM dw.public.dim_product",
        "redundant",
        expect_no_rewrite=True,
    ),
]

# ---------------------------------------------------------------------------
# 5. Join structure and filter propagation (12)
# ---------------------------------------------------------------------------

JOINS = [
    Scenario(
        "inner_join_constant_propagates",
        "SELECT s.sale_id, c.region FROM dw.public.fact_sales s "
        "JOIN dw.public.dim_customer c ON s.cust_id = c.cust_id WHERE s.cust_id = 1000",
        "joins",
        expect_applied=("PROPAGATE_JOIN_FILTER",),
    ),
    Scenario(
        "left_join_constant_refuses",
        "SELECT s.sale_id, c.region FROM dw.public.fact_sales s "
        "LEFT JOIN dw.public.dim_customer c ON s.cust_id = c.cust_id WHERE s.cust_id = 1000",
        "joins",
        expect_blocked=("PROPAGATE_JOIN_FILTER",),
    ),
    Scenario(
        "right_join_constant_refuses",
        "SELECT s.sale_id, c.region FROM dw.public.fact_sales s "
        "RIGHT JOIN dw.public.dim_customer c ON s.cust_id = c.cust_id WHERE s.cust_id = 1000",
        "joins",
        expect_blocked=("PROPAGATE_JOIN_FILTER",),
    ),
    Scenario(
        "full_outer_join_constant_refuses",
        "SELECT s.sale_id, c.region FROM dw.public.fact_sales s "
        "FULL OUTER JOIN dw.public.dim_customer c ON s.cust_id = c.cust_id "
        "WHERE s.cust_id = 1000",
        "joins",
        expect_blocked=("PROPAGATE_JOIN_FILTER",),
    ),
    Scenario(
        "implicit_cross_join_with_filter",
        "SELECT s.sale_id, c.region FROM dw.public.fact_sales s, dw.public.dim_customer c "
        "WHERE s.cust_id = c.cust_id AND s.cust_id = 1000",
        "joins",
        notes="comma join hides the join condition among filters",
    ),
    Scenario(
        "three_way_join_propagation",
        "SELECT s.sale_id FROM dw.public.fact_sales s "
        "JOIN dw.public.dim_customer c ON s.cust_id = c.cust_id "
        "JOIN dw.public.dim_product p ON s.prod_id = p.prod_id WHERE s.cust_id = 42",
        "joins",
        expect_applied=("PROPAGATE_JOIN_FILTER",),
    ),
    Scenario(
        "self_join_propagation",
        "SELECT a.sale_id FROM dw.public.fact_sales a "
        "JOIN dw.public.fact_sales b ON a.cust_id = b.cust_id WHERE a.cust_id = 7",
        "joins",
        expect_applied=("PROPAGATE_JOIN_FILTER",),
    ),
    Scenario(
        "derived_table_join_propagation",
        "SELECT a.sale_id FROM dw.public.fact_sales a "
        "JOIN (SELECT cust_id FROM dw.public.dim_customer) b ON a.cust_id = b.cust_id "
        "WHERE a.cust_id = 42",
        "joins",
        expect_applied=("PROPAGATE_JOIN_FILTER",),
    ),
    Scenario(
        "join_unkeyed_table_broadcast",
        "SELECT e.event_id, c.region FROM dw.public.fact_events e "
        "JOIN dw.public.dim_customer c ON e.cust_id = c.cust_id",
        "joins",
        notes="fact_events is DISTSTYLE EVEN: every join redistributes",
        plan={
            "explain": [
                {"step": 5, "operation": "XN Hash Join DS_BCAST_INNER", "rows": 900,
                 "table_name": "dw.public.fact_events"}
            ],
            "detail": [{"step": 5, "output_rows": 24_000_000, "duration_s": 4.9}],
        },
    ),
    Scenario(
        "cross_join_no_condition",
        "SELECT s.sale_id, p.category FROM dw.public.fact_sales s "
        "CROSS JOIN dw.public.dim_product p",
        "joins",
        expect_no_rewrite=True,
    ),
    Scenario(
        "join_on_cast_column",
        "SELECT s.sale_id FROM dw.public.fact_sales s "
        "JOIN dw.public.dim_customer c ON CAST(s.cust_id AS VARCHAR) = CAST(c.cust_id AS VARCHAR)",
        "joins",
        expect_no_rewrite=True,
        notes="casting both join keys defeats distkey matching",
    ),
    Scenario(
        "join_diststyle_all_large_table",
        "SELECT s.sale_id, a.addr_id FROM dw.public.fact_sales s "
        "JOIN dw.public.dim_address a ON s.cust_id = a.addr_id WHERE s.cust_id = 99",
        "joins",
        expect_applied=("PROPAGATE_JOIN_FILTER",),
        notes="dim_address is DISTSTYLE ALL at 3.4 GB: replicated everywhere",
    ),
]

# ---------------------------------------------------------------------------
# 6. Subqueries (10)
# ---------------------------------------------------------------------------

SUBQUERIES = [
    Scenario(
        "correlated_scalar_in_select",
        "SELECT s.sale_id, (SELECT MAX(x.amount) FROM dw.public.fact_sales x "
        "WHERE x.cust_id = s.cust_id) mx FROM dw.public.fact_sales s WHERE s.cust_id = 42",
        "subquery",
        notes="re-evaluated per outer row; plans as a nested loop",
    ),
    Scenario(
        "correlated_exists",
        "SELECT s.sale_id FROM dw.public.fact_sales s WHERE EXISTS "
        "(SELECT 1 FROM dw.public.dim_customer d WHERE d.cust_id = s.cust_id "
        "AND d.region = 'EMEA')",
        "subquery",
        expect_no_rewrite=True,
    ),
    Scenario(
        "in_subquery_semijoin",
        "SELECT s.sale_id FROM dw.public.fact_sales s WHERE s.cust_id IN "
        "(SELECT d.cust_id FROM dw.public.dim_customer d WHERE d.segment = 'ent')",
        "subquery",
        expect_no_rewrite=True,
    ),
    Scenario(
        "scalar_subquery_in_where",
        "SELECT sale_id FROM dw.public.fact_sales "
        "WHERE amount > (SELECT AVG(amount) FROM dw.public.fact_sales)",
        "subquery",
        expect_no_rewrite=True,
    ),
    Scenario(
        "deeply_nested_subqueries",
        "SELECT sale_id FROM (SELECT sale_id, amount FROM "
        "(SELECT sale_id, amount, cust_id FROM dw.public.fact_sales "
        "WHERE DATE(sale_date) = '2024-01-01') inner1 WHERE amount > 10) inner2",
        "subquery",
        expect_applied=("SARGABLE_SORTKEY",),
        notes="rewrite must reach into a two-deep nest",
    ),
    Scenario(
        "subquery_in_from_with_agg",
        "SELECT t.cust_id, t.total FROM "
        "(SELECT cust_id, SUM(amount) total FROM dw.public.fact_sales GROUP BY cust_id) t "
        "WHERE t.total > 1000",
        "subquery",
        expect_no_rewrite=True,
    ),
    Scenario(
        "cte_referenced_twice",
        "WITH c AS (SELECT cust_id, SUM(amount) t FROM dw.public.fact_sales GROUP BY cust_id) "
        "SELECT a.cust_id, a.t, b.t FROM c a JOIN c b ON a.cust_id = b.cust_id",
        "subquery",
        expect_no_rewrite=True,
        notes="a CTE used twice may be materialized twice",
    ),
    Scenario(
        "recursive_cte",
        "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 10) "
        "SELECT * FROM r",
        "subquery",
        expect_no_rewrite=True,
    ),
    Scenario(
        "exists_with_unkeyed_table",
        "SELECT s.sale_id FROM dw.public.fact_sales s WHERE EXISTS "
        "(SELECT 1 FROM dw.public.fact_events e WHERE e.cust_id = s.cust_id)",
        "subquery",
        expect_no_rewrite=True,
    ),
    Scenario(
        "subquery_over_staging_table",
        "SELECT o.order_id FROM dw.staging.stg_orders_raw o WHERE o.cust_id IN "
        "(SELECT d.cust_id FROM dw.public.dim_customer d)",
        "subquery",
        expect_no_rewrite=True,
        notes="staging table has no design at all",
    ),
]

# ---------------------------------------------------------------------------
# 7. Views (8)
# ---------------------------------------------------------------------------

VIEWS = [
    Scenario(
        "simple_view",
        "SELECT sale_id, amount FROM dw.reporting.v_sales WHERE amount > 100",
        "views",
        notes="single-table view inlined",
    ),
    Scenario(
        "view_with_join",
        "SELECT sale_id, region FROM dw.reporting.v_sales_enriched WHERE amount > 100",
        "views",
        notes="two-table view inlined",
    ),
    Scenario(
        "nested_view",
        "SELECT cust_id, amount FROM dw.reporting.v_nested WHERE amount > 50",
        "views",
        notes="view over a view: recursive explosion",
    ),
    Scenario(
        "view_with_distinct_barrier",
        "SELECT cust_id FROM dw.reporting.v_distinct_custs WHERE region = 'EMEA'",
        "views",
        notes="DISTINCT inside the view blocks predicate pushdown",
    ),
    Scenario(
        "view_joined_to_table",
        "SELECT v.sale_id, p.category FROM dw.reporting.v_sales v "
        "JOIN dw.public.dim_product p ON v.prod_id = p.prod_id",
        "views",
    ),
    Scenario(
        "view_with_sargable_killer",
        "SELECT sale_id FROM dw.reporting.v_sales WHERE DATE(sale_date) = '2024-01-01'",
        "views",
        notes="does the rewrite survive view inlining?",
    ),
    Scenario(
        "unknown_view_left_alone",
        "SELECT a FROM dw.reporting.v_does_not_exist",
        "views",
        expect_no_rewrite=True,
    ),
    Scenario(
        "two_views_joined",
        "SELECT a.sale_id, b.cust_id FROM dw.reporting.v_sales a "
        "JOIN dw.reporting.v_nested b ON a.cust_id = b.cust_id",
        "views",
    ),
]

# ---------------------------------------------------------------------------
# 8. Clean queries — the control group (8)
# ---------------------------------------------------------------------------

CLEAN = [
    Scenario(
        f"clean_{i}",
        sql,
        "clean",
        expect_no_rewrite=True,
        notes="already well written: must not be touched",
    )
    for i, sql in enumerate(
        [
            "SELECT sale_id, amount FROM dw.public.fact_sales "
            "WHERE sale_date >= '2024-01-01' AND sale_date < '2024-02-01'",
            "SELECT cust_id, SUM(amount) FROM dw.public.fact_sales "
            "WHERE sale_date >= '2024-01-01' GROUP BY cust_id",
            "SELECT s.sale_id, c.region FROM dw.public.fact_sales s "
            "JOIN dw.public.dim_customer c ON s.cust_id = c.cust_id "
            "WHERE s.sale_date >= '2024-01-01' AND c.region = 'EMEA'",
            "SELECT prod_id, COUNT(*) FROM dw.public.fact_sales GROUP BY prod_id",
            "SELECT sale_id FROM dw.public.fact_sales WHERE cust_id = 5 "
            "AND sale_date >= '2024-06-01'",
            "SELECT cust_id, MAX(sale_date) FROM dw.public.fact_sales GROUP BY cust_id",
            "SELECT s.sale_id FROM dw.public.fact_sales s WHERE s.amount BETWEEN 10 AND 20",
            "SELECT COUNT(*) FROM dw.public.fact_sales WHERE sale_date >= '2024-01-01'",
        ]
    )
]

# ---------------------------------------------------------------------------
# 9. DML and non-SELECT statements (6)
# ---------------------------------------------------------------------------

DML = [
    Scenario(
        "update_with_sargable_killer",
        "UPDATE dw.public.fact_sales SET amount = 0 WHERE DATE(sale_date) = '2024-01-01'",
        "dml",
        notes="rewrites must be safe on DML too",
    ),
    Scenario(
        "delete_with_subquery",
        "DELETE FROM dw.public.fact_sales WHERE cust_id IN "
        "(SELECT cust_id FROM dw.public.dim_customer WHERE region = 'X')",
        "dml",
    ),
    Scenario(
        "insert_select",
        "INSERT INTO dw.staging.stg_orders_raw "
        "SELECT sale_id, cust_id, amount FROM dw.public.fact_sales "
        "WHERE DATE(sale_date) = '2024-01-01'",
        "dml",
    ),
    Scenario(
        "create_table_as",
        "CREATE TABLE dw.staging.tmp AS SELECT cust_id, SUM(amount) t "
        "FROM dw.public.fact_sales GROUP BY cust_id",
        "dml",
    ),
    Scenario(
        "insert_values_multi_row",
        "INSERT INTO dw.public.dim_product (prod_id, category) "
        "VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        "dml",
        expect_no_rewrite=True,
    ),
    Scenario(
        "unload_wrapped_query",
        "UNLOAD ('SELECT sale_id FROM dw.public.fact_sales') TO 's3://bucket/prefix' "
        "IAM_ROLE 'arn:aws:iam::123456789012:role/x'",
        "dml",
        expect_no_rewrite=True,
    ),
]

# ---------------------------------------------------------------------------
# 10. Malformed input — must degrade, never crash (6)
# ---------------------------------------------------------------------------

MALFORMED = [
    Scenario("empty_ish", "   ", "malformed", expect_parse_failure=True),
    Scenario("garbage_tokens", "SELECT !!! FROM ///", "malformed", expect_parse_failure=True),
    Scenario(
        "truncated_statement",
        "SELECT sale_id FROM dw.public.fact_sales WHERE",
        "malformed",
        expect_parse_failure=True,
    ),
    Scenario(
        "unbalanced_parens",
        "SELECT sale_id FROM (SELECT * FROM dw.public.fact_sales",
        "malformed",
        expect_parse_failure=True,
    ),
    Scenario(
        "only_a_comment",
        "-- just a comment, nothing else",
        "malformed",
        expect_parse_failure=True,
    ),
    Scenario(
        "random_prose",
        "please make my query faster thanks",
        "malformed",
        expect_no_rewrite=True,
        notes="parses as a bare column list in some dialects; must not rewrite",
    ),
]


ALL_SCENARIOS: tuple[Scenario, ...] = tuple(
    SARGABILITY
    + NULL_SEMANTICS
    + WINDOWS
    + REDUNDANT
    + JOINS
    + SUBQUERIES
    + VIEWS
    + CLEAN
    + DML
    + MALFORMED
)


def default_plan(index: int) -> tuple[list[dict], list[dict]]:
    """Scaled-down plan rows: ~5s worst case, ratios preserved."""
    step = 3 + (index % 4)
    broadcast = index % 3 == 0
    spills = index % 5 == 0
    bad_estimate = index % 7 == 0
    return (
        [
            {
                "step": step,
                "operation": (
                    "XN Hash Join DS_BCAST_INNER" if broadcast else "XN Hash Join DS_DIST_NONE"
                ),
                "rows": 500 if bad_estimate else 900_000,
                "table_name": "dw.public.fact_events" if broadcast else "dw.public.fact_sales",
            }
        ],
        [
            {
                "step": step,
                "output_rows": 1_200_000 if broadcast else 800_000,
                "input_bytes": 60_000_000_000 if index % 11 == 0 else 400_000_000,
                "spilled_bytes": 2_000_000_000 if spills else 0,
                "duration_s": round(0.2 + (index % 25) * 0.2, 2),
            }
        ],
    )


def categories() -> dict[str, int]:
    out: dict[str, int] = {}
    for scenario in ALL_SCENARIOS:
        out[scenario.category] = out.get(scenario.category, 0) + 1
    return out


if __name__ == "__main__":  # pragma: no cover - inspection helper
    total = 0
    for category, count in sorted(categories().items(), key=lambda kv: -kv[1]):
        print(f"  {category:<16} {count:>3}")
        total += count
    print(f"  {'TOTAL':<16} {total:>3}")
