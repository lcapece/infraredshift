"""Tests for catalog discovery and fetch (offline, no live Redshift)."""

from __future__ import annotations

from redshift_decomposer import (
    catalog_from_rows,
    decompose,
    discover_object_refs,
    fetch_catalog_for_sql,
)


def test_discover_skips_ctes():
    refs = discover_object_refs(
        """
        WITH heart AS (SELECT 1 AS id)
        SELECT * FROM heart h JOIN public.orders o ON h.id = o.id
        """
    )
    names = {r.name for r in refs}
    assert "orders" in names
    assert "heart" not in names


def test_discover_nested_and_union():
    refs = discover_object_refs(
        """
        SELECT * FROM a WHERE id IN (SELECT id FROM b)
        UNION ALL
        SELECT * FROM reporting.v1
        """
    )
    identities = {r.identity for r in refs}
    assert "a" in identities
    assert "b" in identities
    assert "reporting.v1" in identities


def test_discover_multi_statement():
    refs = discover_object_refs(
        """
        SELECT * FROM public.t1;
        SELECT * FROM public.t2;
        """
    )
    names = {r.name for r in refs}
    assert "t1" in names
    assert "t2" in names


def test_catalog_from_rows_maps_svv_table_info():
    catalog = catalog_from_rows(
        [
            {
                "database": "analytics",
                "schema": "public",
                "table_name": "fact_orders",
                "diststyle": "KEY(cust_id)",
                "sortkey1": "order_date",
                "size": 420000,
                "tbl_rows": 2_000_000_000,
            }
        ],
        column_rows=[
            {
                "database": "analytics",
                "schema": "public",
                "table_name": "fact_orders",
                "column_name": "order_id",
                "data_type": "bigint",
            },
            {
                "database": "analytics",
                "schema": "public",
                "table_name": "fact_orders",
                "column_name": "cust_id",
                "data_type": "bigint",
            },
            {
                "database": "analytics",
                "schema": "public",
                "table_name": "fact_orders",
                "column_name": "order_date",
                "data_type": "date",
            },
        ],
        view_rows=[
            {
                "database": "analytics",
                "schema": "reporting",
                "view_name": "v_orders",
                "source_definition": "SELECT * FROM public.fact_orders",
            }
        ],
    )
    key = "analytics.public.fact_orders"
    assert key in catalog.tables
    stats = catalog.tables[key]
    assert stats.distkey == "cust_id"
    assert stats.sortkeys == ("order_date",)
    assert stats.size_mb == 420000
    assert "cust_id" in stats.columns
    assert "analytics.reporting.v_orders" in catalog.views


def test_fetch_catalog_for_sql_with_mock_execute():
    sql = """
    SELECT o.order_id
    FROM analytics.reporting.v_orders o
    JOIN public.dim_customer c ON o.cust_id = c.cust_id
    WHERE o.order_date >= DATE '2024-01-01'
    """

    def execute(query: str):
        q = query.lower()
        if "current_database" in q and "pg_views" not in q and "svv_table_info" not in q:
            return [{"database_name": "analytics"}]
        if "svv_table_info" in q:
            return [
                {
                    "database": "analytics",
                    "schema": "public",
                    "table_name": "dim_customer",
                    "diststyle": "ALL",
                    "sortkey1": "",
                    "size": 800,
                    "tbl_rows": 5_000_000,
                },
                {
                    "database": "analytics",
                    "schema": "public",
                    "table_name": "fact_orders",
                    "diststyle": "KEY(cust_id)",
                    "sortkey1": "order_date",
                    "size": 420000,
                    "tbl_rows": 2_000_000_000,
                },
            ]
        if "information_schema.columns" in q:
            return [
                {
                    "database": "analytics",
                    "schema": "public",
                    "table_name": "dim_customer",
                    "column_name": "cust_id",
                    "data_type": "bigint",
                    "ordinal_position": 1,
                },
                {
                    "database": "analytics",
                    "schema": "public",
                    "table_name": "dim_customer",
                    "column_name": "region",
                    "data_type": "character varying",
                    "ordinal_position": 2,
                },
                {
                    "database": "analytics",
                    "schema": "public",
                    "table_name": "fact_orders",
                    "column_name": "order_id",
                    "data_type": "bigint",
                    "ordinal_position": 1,
                },
                {
                    "database": "analytics",
                    "schema": "public",
                    "table_name": "fact_orders",
                    "column_name": "cust_id",
                    "data_type": "bigint",
                    "ordinal_position": 2,
                },
                {
                    "database": "analytics",
                    "schema": "public",
                    "table_name": "fact_orders",
                    "column_name": "order_date",
                    "data_type": "date",
                    "ordinal_position": 3,
                },
                {
                    "database": "analytics",
                    "schema": "public",
                    "table_name": "fact_orders",
                    "column_name": "amount",
                    "data_type": "numeric",
                    "ordinal_position": 4,
                },
            ]
        if "pg_views" in q:
            return [
                {
                    "database": "analytics",
                    "schema": "reporting",
                    "view_name": "v_orders",
                    "source_definition": (
                        "SELECT order_id, cust_id, order_date, amount "
                        "FROM analytics.public.fact_orders WHERE amount > 0"
                    ),
                }
            ]
        return []

    catalog = fetch_catalog_for_sql(sql, execute)
    assert catalog.tables
    assert "analytics.reporting.v_orders" in catalog.views

    plan = decompose(sql, execute=execute, minimum_rows=1, minimum_size_mb=1)
    assert plan.parse_ok, plan.parse_error
    assert any(f.title == "Catalog fetched from Redshift" for f in plan.findings)
    assert plan.stages


def test_decompose_requires_catalog_or_connection():
    plan = decompose("SELECT 1")
    assert not plan.parse_ok
