import pandas as pd
import sqlglot
from sqlglot import exp

from analyzer.physical_lineage import PhysicalLineageResolver
from analyzer.sql_lens import analyze_console_sql


def _join_origins(sql: str, views=None):
    tree = sqlglot.parse_one(sql, read="redshift")
    resolver = PhysicalLineageResolver(tree, views)
    join = next(tree.find_all(exp.Join))
    equality = next(join.args["on"].find_all(exp.EQ))
    return resolver.origins_for_expression(equality.left), resolver.origins_for_expression(equality.right)


def test_join_columns_backtrack_through_cte_and_derived_table() -> None:
    sql = """
WITH customer_keys AS (
  SELECT CAST(a.score_customer_account_xid AS VARCHAR(50)) AS account_id
  FROM fraud.raw_authorizations a
), latest AS (
  SELECT p.score_customer_account_xid AS account_id
  FROM fraud.payment_instrument p
  JOIN customer_keys k
    ON CAST(p.score_customer_account_xid AS VARCHAR(50)) = k.account_id
)
SELECT *
FROM fraud.raw_authorizations a
LEFT JOIN latest p
  ON CAST(a.score_customer_account_xid AS VARCHAR(50)) = p.account_id
"""

    left, right = _join_origins(sql)

    assert {origin.column_key for origin in left} == {
        "fraud.raw_authorizations.score_customer_account_xid"
    }
    assert {origin.column_key for origin in right} == {
        "fraud.payment_instrument.score_customer_account_xid"
    }


def test_join_column_backtracks_through_nested_views() -> None:
    views = pd.DataFrame(
        [
            {
                "database": "dev",
                "schema": "reporting",
                "view_name": "customer_view",
                "source_definition": "SELECT n.customer_id FROM reporting.nested_customer_view n",
            },
            {
                "database": "dev",
                "schema": "reporting",
                "view_name": "nested_customer_view",
                "source_definition": "SELECT r.customer_id FROM raw.customer_master r",
            },
        ]
    )
    sql = """
SELECT *
FROM dev.sales.orders o
JOIN dev.reporting.customer_view v
  ON o.customer_id = v.customer_id
"""

    left, right = _join_origins(sql, views)

    assert {origin.column_key for origin in left} == {"dev.sales.orders.customer_id"}
    assert {origin.column_key for origin in right} == {"dev.raw.customer_master.customer_id"}


def test_expression_preserves_multiple_physical_origins() -> None:
    sql = """
SELECT * FROM dev.sales.orders o
JOIN dev.crm.customers c
  ON COALESCE(o.customer_id, o.legacy_customer_id) = c.customer_id
"""

    left, right = _join_origins(sql)

    assert {origin.column for origin in left} == {"customer_id", "legacy_customer_id"}
    assert {origin.column_key for origin in right} == {"dev.crm.customers.customer_id"}


def test_sql_lens_join_rows_publish_physical_sides_instead_of_only_aliases() -> None:
    sql = """
WITH keys AS (
  SELECT a.customer_id FROM dev.raw.authorization_fact a
)
SELECT * FROM keys k
JOIN (SELECT p.customer_id FROM dev.raw.payment_instrument p) latest
  ON k.customer_id = latest.customer_id
"""

    analysis = analyze_console_sql(sql, pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    join = analysis.joins.iloc[-1]

    assert join["left_physical_sources"] == "dev.raw.authorization_fact.customer_id"
    assert join["right_physical_sources"] == "dev.raw.payment_instrument.customer_id"
    assert join["physical_lineage_status"] == "resolved"
