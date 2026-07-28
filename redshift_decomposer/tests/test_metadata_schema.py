"""Tests for metadata_schema retargeting and summary external partition keys."""

from __future__ import annotations

from redshift_decomposer import (
    catalog_relation,
    decompose,
    fetch_catalog_for_sql,
    normalize_metadata_schema,
)
from redshift_decomposer.fetch import (
    sql_external_partition_keys,
    sql_table_info_for_names,
)


def test_normalize_metadata_schema():
    assert normalize_metadata_schema(None) is None
    assert normalize_metadata_schema("") is None
    assert normalize_metadata_schema("  ") is None
    assert normalize_metadata_schema("My_Meta") == "my_meta"


def test_catalog_relation_system_vs_mirror():
    assert catalog_relation("svv_table_info", None) == "SVV_TABLE_INFO"
    assert catalog_relation("svv_table_info", "my_meta") == "my_meta.svv_table_info"
    assert catalog_relation("columns", None) == "information_schema.columns"
    assert catalog_relation("columns", "my_meta") == "my_meta.columns"
    assert catalog_relation("svv_external_columns", None) == "SVV_EXTERNAL_COLUMNS"
    assert (
        catalog_relation("svv_external_columns", "my_meta")
        == "my_meta.svv_external_columns"
    )


def test_sql_builders_retarget_from_clause():
    sys_sql = sql_table_info_for_names(["fact_orders"])
    assert "FROM SVV_TABLE_INFO" in sys_sql
    assert "my_meta" not in sys_sql

    mirror_sql = sql_table_info_for_names(["fact_orders"], metadata_schema="my_meta")
    assert "FROM my_meta.svv_table_info" in mirror_sql.lower().replace("\n", " ") or (
        "from my_meta.svv_table_info" in mirror_sql.lower()
    )

    ext = sql_external_partition_keys(["ext_events"], metadata_schema="my_meta")
    assert "my_meta.svv_external_columns" in ext.lower()
    assert "part_key = 1" in ext.lower()
    assert "schemaname" in ext.lower()
    assert "tablename" in ext.lower()
    assert "columnname" in ext.lower()


def test_fetch_uses_metadata_schema_tables():
    """Mock execute only answers when queries hit my_meta.* mirrors."""

    def execute(query: str):
        q = query.lower()
        if "current_database" in q and "pg_views" not in q:
            return [{"database_name": "analytics"}]
        if "my_meta.svv_table_info" in q:
            return [
                {
                    "database": "analytics",
                    "schema": "public",
                    "table_name": "fact_orders",
                    "diststyle": "KEY(cust_id)",
                    "sortkey1": "order_date",
                    "size": 1000,
                    "tbl_rows": 5_000_000,
                }
            ]
        if "my_meta.columns" in q:
            return [
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
            ]
        if "my_meta.svv_external_columns" in q:
            # Summary: one row, first partition key only
            return [
                {
                    "database": "analytics",
                    "schema": "spectrum",
                    "table_name": "ext_events",
                    "partition_key": "event_date",
                }
            ]
        if "my_meta.pg_views" in q:
            return []
        # System catalogs intentionally empty / unused
        return []

    sql = "SELECT order_id FROM public.fact_orders WHERE order_date >= DATE '2024-01-01'"
    catalog = fetch_catalog_for_sql(sql, execute, metadata_schema="my_meta")
    assert catalog.coverage.get("metadata_schema") == "my_meta"
    assert any("fact_orders" in k for k in catalog.tables)

    plan = decompose(
        sql,
        execute=execute,
        metadata_schema="my_meta",
        minimum_rows=1,
        minimum_size_mb=1,
    )
    assert plan.parse_ok, plan.parse_error


def test_external_partition_key_applied_from_summary():
    from redshift_decomposer.catalog import Catalog, TableStats
    from redshift_decomposer.fetch import _apply_external_partition_keys

    catalog = Catalog(
        tables={
            "analytics.spectrum.ext_events": TableStats(
                columns={"event_id": "BIGINT", "event_date": "DATE"},
                rows=1e9,
                size_mb=5000,
            )
        },
        default_database="analytics",
    )
    _apply_external_partition_keys(
        catalog,
        [
            {
                "database": "analytics",
                "schema": "spectrum",
                "table_name": "ext_events",
                "partition_key": "event_date",
            }
        ],
        default_database="analytics",
    )
    stats = catalog.tables["analytics.spectrum.ext_events"]
    assert stats.is_external
    assert stats.partition_key == "event_date"
    assert stats.object_type == "external_table"
