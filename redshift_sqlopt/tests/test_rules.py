"""Rewrite rules — including, importantly, the cases where they must refuse.

A rule that fires when it should not is worse than a rule that never fires, so
roughly half of these tests assert that nothing happened.
"""

from __future__ import annotations

import pytest

from redshift_sqlopt import Catalog, optimize

SORTKEY_CATALOG = Catalog.from_rows(
    table_rows=[
        {
            "database": "analytics",
            "schema": "public",
            "table": "fact_orders",
            "distkey": "cust_id",
            "diststyle": "KEY",
            "sortkeys": "order_date",
            "row_count": 2_000_000_000,
        },
        {
            "database": "analytics",
            "schema": "public",
            "table": "dim_customer",
            "distkey": "cust_id",
            "diststyle": "KEY",
            "sortkeys": "cust_id",
            "row_count": 5_000_000,
        },
    ]
)


def codes(items) -> set[str]:
    return {item.code for item in items}


# ---------------------------------------------------------------------------
# sargability
# ---------------------------------------------------------------------------


def test_date_wrapper_on_sortkey_is_rewritten() -> None:
    result = optimize(
        "SELECT order_id FROM analytics.public.fact_orders "
        "WHERE DATE(order_date) = '2024-01-01'",
        catalog=SORTKEY_CATALOG,
    )
    assert "SARGABLE_SORTKEY" in codes(result.applied)
    assert result.has_rewrite
    assert "DATE(" not in result.rewritten_sql.upper().replace("DATEADD(", "")
    assert ">=" in result.rewritten_sql


def test_date_wrapper_on_non_sortkey_is_left_alone() -> None:
    """No zone-map benefit exists, so there is nothing to claim."""
    result = optimize(
        "SELECT order_id FROM analytics.public.fact_orders "
        "WHERE DATE(ship_date) = '2024-01-01'",
        catalog=SORTKEY_CATALOG,
    )
    assert "SARGABLE_SORTKEY" not in codes(result.applied)


NUMERIC_SORTKEY_CATALOG = Catalog.from_rows(
    table_rows=[
        {
            "database": "analytics",
            "schema": "public",
            "table": "fact_orders",
            "distkey": "cust_id",
            "sortkeys": "amount,order_date",
            "row_count": 2_000_000_000,
        }
    ]
)


def test_non_date_cast_on_sortkey_is_not_rewritten() -> None:
    """Regression: the day-range rewrite is only valid for date values.

    Without a type check this produced ``amount >= '100' AND amount <
    DATEADD(day, 1, '100')`` on a numeric sort key — structurally unchanged, so
    the validation gate could not catch it, and meaningless.
    """
    result = optimize(
        "SELECT order_id FROM analytics.public.fact_orders "
        "WHERE CAST(amount AS VARCHAR) = '100'",
        catalog=NUMERIC_SORTKEY_CATALOG,
    )
    assert "SARGABLE_SORTKEY" not in codes(result.applied)
    assert "DATEADD" not in (result.rewritten_sql or "").upper()


def test_cast_to_date_on_sortkey_is_rewritten() -> None:
    """The date-typed cast remains in scope — the guard is narrow, not blanket."""
    result = optimize(
        "SELECT order_id FROM analytics.public.fact_orders "
        "WHERE CAST(order_date AS DATE) = '2024-01-01'",
        catalog=NUMERIC_SORTKEY_CATALOG,
    )
    assert "SARGABLE_SORTKEY" in codes(result.applied)


def test_non_date_literal_is_not_rewritten() -> None:
    """Defence in depth: a date-valued wrapper with a non-date constant."""
    result = optimize(
        "SELECT order_id FROM analytics.public.fact_orders "
        "WHERE DATE(order_date) = 'not-a-date'",
        catalog=NUMERIC_SORTKEY_CATALOG,
    )
    assert "SARGABLE_SORTKEY" not in codes(result.applied)


def test_sargable_rule_blocks_without_catalog() -> None:
    """With no catalog the rule cannot prove the column is a sort key."""
    result = optimize(
        "SELECT order_id FROM analytics.public.fact_orders "
        "WHERE DATE(order_date) = '2024-01-01'"
    )
    assert "SARGABLE_SORTKEY" not in codes(result.applied)


# ---------------------------------------------------------------------------
# NOT IN -> NOT EXISTS
# ---------------------------------------------------------------------------


NOT_NULL_CATALOG = Catalog.from_rows(
    table_rows=[
        {
            "database": "analytics",
            "schema": "public",
            "table": "fact_orders",
            "distkey": "cust_id",
            "sortkeys": "order_date",
            "row_count": 2_000_000_000,
        },
        {
            "database": "analytics",
            "schema": "public",
            "table": "dim_customer",
            "distkey": "cust_id",
            "sortkeys": "cust_id",
            "not_null_columns": "cust_id",
            "row_count": 5_000_000,
        },
    ]
)


def test_not_in_becomes_not_exists_when_column_proven_not_null() -> None:
    """Both sides must be qualified — see the tautology regression tests below."""
    result = optimize(
        "SELECT a FROM analytics.public.fact_orders o "
        "WHERE o.cust_id NOT IN (SELECT i.cust_id FROM analytics.public.dim_customer i)",
        catalog=NOT_NULL_CATALOG,
    )
    assert "NOT_IN_TO_NOT_EXISTS" in codes(result.applied)
    assert "NOT EXISTS" in result.rewritten_sql.upper()


def test_not_in_is_blocked_when_nullability_is_unknown() -> None:
    """NOT IN and NOT EXISTS differ exactly when NULLs are present, so without
    proof the rewrite must be withheld — the policy is refuse-unless-provable."""
    result = optimize(
        "SELECT a FROM analytics.public.fact_orders "
        "WHERE cust_id NOT IN (SELECT cust_id FROM analytics.public.dim_customer)",
        catalog=SORTKEY_CATALOG,  # no not_null_columns declared
    )
    assert "NOT_IN_TO_NOT_EXISTS" not in codes(result.applied)
    blocked = [b for b in result.blocked if b.code == "NOT_IN_TO_NOT_EXISTS"]
    assert blocked
    assert "NOT NULL" in blocked[0].reason


def test_not_in_multi_column_subquery_is_blocked() -> None:
    result = optimize(
        "SELECT a FROM analytics.public.fact_orders "
        "WHERE cust_id NOT IN (SELECT cust_id, region FROM analytics.public.dim_customer)",
        catalog=NOT_NULL_CATALOG,
    )
    assert "NOT_IN_TO_NOT_EXISTS" not in codes(result.applied)
    assert "NOT_IN_TO_NOT_EXISTS" in codes(result.blocked)


def test_plain_in_is_not_touched() -> None:
    """Only NOT IN has the NULL trap; plain IN is left alone."""
    result = optimize(
        "SELECT a FROM analytics.public.fact_orders "
        "WHERE cust_id IN (SELECT cust_id FROM analytics.public.dim_customer)",
        catalog=SORTKEY_CATALOG,
    )
    assert "NOT_IN_TO_NOT_EXISTS" not in codes(result.applied)


# ---------------------------------------------------------------------------
# redundant DISTINCT
# ---------------------------------------------------------------------------


def test_distinct_over_group_by_is_removed() -> None:
    result = optimize(
        "SELECT DISTINCT cust_id, COUNT(*) AS n "
        "FROM analytics.public.fact_orders GROUP BY cust_id",
        catalog=SORTKEY_CATALOG,
    )
    assert "REDUNDANT_DISTINCT" in codes(result.applied)
    assert "DISTINCT" not in result.rewritten_sql.upper()


def test_distinct_without_group_by_is_kept() -> None:
    result = optimize(
        "SELECT DISTINCT cust_id FROM analytics.public.fact_orders",
        catalog=SORTKEY_CATALOG,
    )
    assert "REDUNDANT_DISTINCT" not in codes(result.applied)


def test_distinct_with_ungrouped_projection_is_kept() -> None:
    """A non-grouped, non-aggregate column means DISTINCT is load-bearing."""
    result = optimize(
        "SELECT DISTINCT cust_id, region FROM analytics.public.fact_orders GROUP BY cust_id",
        catalog=SORTKEY_CATALOG,
    )
    assert "REDUNDANT_DISTINCT" not in codes(result.applied)
    assert "REDUNDANT_DISTINCT" in codes(result.blocked)


# ---------------------------------------------------------------------------
# join filter propagation
# ---------------------------------------------------------------------------


def test_constant_propagates_across_inner_join() -> None:
    result = optimize(
        "SELECT o.order_id FROM analytics.public.fact_orders o "
        "JOIN analytics.public.dim_customer c ON o.cust_id = c.cust_id "
        "WHERE o.cust_id = 42",
        catalog=SORTKEY_CATALOG,
    )
    assert "PROPAGATE_JOIN_FILTER" in codes(result.applied)
    assert result.rewritten_sql.count("42") >= 2


def test_constant_does_not_propagate_across_left_join() -> None:
    """Propagating onto the null-supplying side would drop preserved rows."""
    result = optimize(
        "SELECT o.order_id FROM analytics.public.fact_orders o "
        "LEFT JOIN analytics.public.dim_customer c ON o.cust_id = c.cust_id "
        "WHERE o.cust_id = 42",
        catalog=SORTKEY_CATALOG,
    )
    assert "PROPAGATE_JOIN_FILTER" not in codes(result.applied)
    assert "PROPAGATE_JOIN_FILTER" in codes(result.blocked)


# ---------------------------------------------------------------------------
# general guarantees
# ---------------------------------------------------------------------------


def test_clean_query_produces_no_rewrite() -> None:
    result = optimize(
        "SELECT order_id, amount FROM analytics.public.fact_orders "
        "WHERE order_date >= '2024-01-01'",
        catalog=SORTKEY_CATALOG,
    )
    assert not result.has_rewrite
    assert result.applied == []


def test_unparseable_sql_is_terminal_for_rewriting() -> None:
    result = optimize("SELECT !!! FROM ///", catalog=SORTKEY_CATALOG)
    assert not result.parsed
    assert result.parse_failure is not None
    assert not result.has_rewrite


def test_every_rewrite_is_reparseable() -> None:
    """Whatever we emit must itself be valid Redshift SQL."""
    import sqlglot

    result = optimize(
        "SELECT DISTINCT cust_id, COUNT(*) AS n FROM analytics.public.fact_orders "
        "WHERE DATE(order_date) = '2024-01-01' GROUP BY cust_id",
        catalog=SORTKEY_CATALOG,
    )
    if result.has_rewrite:
        assert sqlglot.parse_one(result.rewritten_sql, read="redshift") is not None


# ---------------------------------------------------------------------------
# regression: correlation predicate must be qualified on both sides
# ---------------------------------------------------------------------------


def test_unqualified_not_in_refuses_rather_than_emit_tautology() -> None:
    """Regression for a wrong-results bug.

    With both sides unqualified the generated correlation was ``WHERE k = k``,
    a tautology: unqualified columns inside the subquery resolve to the INNER
    table, so ``NOT EXISTS(SELECT 1 FROM dim WHERE TRUE)`` means "dim is empty"
    rather than an anti-join. Structural validation could not see it — same
    tables, same columns, same projection — so the rule must refuse instead.
    """
    result = optimize(
        "SELECT a FROM analytics.public.fact_orders "
        "WHERE cust_id NOT IN (SELECT cust_id FROM analytics.public.dim_customer)",
        catalog=NOT_NULL_CATALOG,
    )
    assert "NOT_IN_TO_NOT_EXISTS" not in codes(result.applied)
    assert "NOT_IN_TO_NOT_EXISTS" in codes(result.blocked)
    assert not result.has_rewrite


def test_qualified_not_in_builds_a_real_correlation() -> None:
    result = optimize(
        "SELECT a FROM analytics.public.fact_orders o "
        "WHERE o.cust_id NOT IN (SELECT i.cust_id FROM analytics.public.dim_customer i)",
        catalog=NOT_NULL_CATALOG,
    )
    assert "NOT_IN_TO_NOT_EXISTS" in codes(result.applied)
    sql = result.rewritten_sql.replace("\n", " ")
    assert "i.cust_id = o.cust_id" in sql, sql
    assert "cust_id = cust_id" not in sql


def test_not_in_subquery_where_is_preserved_in_correlation() -> None:
    result = optimize(
        "SELECT a FROM analytics.public.fact_orders o "
        "WHERE o.cust_id NOT IN "
        "(SELECT i.cust_id FROM analytics.public.dim_customer i WHERE i.cust_id > 0)",
        catalog=NOT_NULL_CATALOG,
    )
    assert "NOT_IN_TO_NOT_EXISTS" in codes(result.applied)
    sql = result.rewritten_sql.replace("\n", " ")
    assert "i.cust_id > 0" in sql
    assert "i.cust_id = o.cust_id" in sql


def test_multi_source_subquery_refuses() -> None:
    """More than one inner source makes the inner qualifier ambiguous."""
    result = optimize(
        "SELECT a FROM analytics.public.fact_orders o WHERE o.cust_id NOT IN "
        "(SELECT i.cust_id FROM analytics.public.dim_customer i "
        "JOIN analytics.public.fact_orders z ON i.cust_id = z.cust_id)",
        catalog=NOT_NULL_CATALOG,
    )
    assert "NOT_IN_TO_NOT_EXISTS" not in codes(result.applied)
    assert "NOT_IN_TO_NOT_EXISTS" in codes(result.blocked)
