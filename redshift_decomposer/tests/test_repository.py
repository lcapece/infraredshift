"""Table repository cache tests (offline)."""

from __future__ import annotations

from pathlib import Path

from redshift_decomposer import (
    TableRepository,
    TableStats,
    build_table_repository,
    decompose,
    fetch_catalog_with_repository,
)
from redshift_decomposer.repository import (
    _sortkeys_from_pg_table_def,
    sql_list_local_databases,
    sql_pg_table_def_all,
    sql_svv_table_info_all,
)


def test_full_sortkeys_from_pg_table_def_order():
    rows = [
        {"column_name": "c", "sortkey": 3},
        {"column_name": "a", "sortkey": 1},
        {"column_name": "b", "sortkey": 2},
        {"column_name": "x", "sortkey": 0},
    ]
    assert _sortkeys_from_pg_table_def(rows) == ["a", "b", "c"]


def test_build_and_cache_first(tmp_path: Path):
    path = tmp_path / "table_repo.sqlite"

    def connect(database: str):
        db = database

        def execute(sql: str):
            q = sql.lower()
            if "svv_redshift_databases" in q:
                return [
                    {"database_name": "analytics", "database_type": "local"},
                    {"database_name": "shared_db", "database_type": "shared"},
                ]
            if "svv_table_info" in q:
                if db != "analytics":
                    return []
                return [
                    {
                        "database": "analytics",
                        "schema": "public",
                        "table_name": "fact_orders",
                        "table_id": "100",
                        "diststyle": "KEY(cust_id)",
                        "sortkey1": "order_date",  # incomplete vs compound
                        "sortkey_num": 2,
                        "size": 420000,
                        "tbl_rows": 2_000_000_000,
                        "encoded": "Y",
                    }
                ]
            if "pg_table_def" in q:
                if db != "analytics":
                    return []
                return [
                    {
                        "schema": "public",
                        "table_name": "fact_orders",
                        "column_name": "order_id",
                        "data_type": "bigint",
                        "encoding": "az64",
                        "distkey": False,
                        "sortkey": 0,
                        "is_not_null": True,
                    },
                    {
                        "schema": "public",
                        "table_name": "fact_orders",
                        "column_name": "cust_id",
                        "data_type": "bigint",
                        "encoding": "az64",
                        "distkey": True,
                        "sortkey": 0,
                        "is_not_null": True,
                    },
                    {
                        "schema": "public",
                        "table_name": "fact_orders",
                        "column_name": "order_date",
                        "data_type": "date",
                        "encoding": "raw",
                        "distkey": False,
                        "sortkey": 1,
                        "is_not_null": True,
                    },
                    {
                        "schema": "public",
                        "table_name": "fact_orders",
                        "column_name": "region_code",
                        "data_type": "varchar",
                        "encoding": "lzo",
                        "distkey": False,
                        "sortkey": 2,
                        "is_not_null": False,
                    },
                    {
                        "schema": "public",
                        "table_name": "fact_orders",
                        "column_name": "amount",
                        "data_type": "numeric",
                        "encoding": "az64",
                        "distkey": False,
                        "sortkey": 0,
                        "is_not_null": False,
                    },
                ]
            return []

        return execute

    report = build_table_repository(
        connect,
        path,
        bootstrap_database="analytics",
    )
    assert "analytics" in report.databases_ok
    assert "shared_db" not in report.databases_planned
    assert report.table_count >= 1

    repo = TableRepository(path)
    stats = repo.get_table("analytics", "public", "fact_orders")
    assert stats is not None
    # Full compound sort key from pg_table_def — not only sortkey1
    assert stats.sortkeys == ("order_date", "region_code")
    assert stats.distkey == "cust_id"
    assert stats.size_mb == 420000
    assert "amount" in stats.columns

    # Cache hit path — no live execute needed for this table
    catalog, misses = repo.catalog_for_refs(
        ["analytics.public.fact_orders"],
        default_database="analytics",
    )
    assert not misses
    assert catalog.tables["analytics.public.fact_orders"].sortkeys == (
        "order_date",
        "region_code",
    )

    # Cache-first catalog for SQL
    live_calls = {"svv": 0}

    def execute_live(sql: str):
        q = sql.lower()
        if "svv_table_info" in q:
            live_calls["svv"] += 1
        return []

    cat = fetch_catalog_with_repository(
        "SELECT amount FROM analytics.public.fact_orders WHERE order_date >= DATE '2024-01-01'",
        repository=repo,
        execute=execute_live,
        database="analytics",
        fetch_views=False,
    )
    assert "analytics.public.fact_orders" in cat.tables
    assert live_calls["svv"] == 0  # served entirely from cache
    assert cat.coverage.get("cache_hits", 0) >= 1

    plan = decompose(
        "SELECT amount FROM analytics.public.fact_orders WHERE order_date >= DATE '2024-01-01'",
        repository=repo,
        database="analytics",
        minimum_rows=1,
        minimum_size_mb=1,
    )
    assert plan.parse_ok, plan.parse_error
    assert plan.stages
    assert any("repository cache-first" in f.title.lower() for f in plan.findings)


def test_cache_miss_live_fill_and_write_through(tmp_path: Path):
    path = tmp_path / "repo.sqlite"
    repo = TableRepository(path)

    def execute(sql: str):
        q = sql.lower()
        if "current_database" in q and "pg_views" not in q and "svv" not in q:
            return [{"database_name": "analytics"}]
        if "svv_table_info" in q:
            return [
                {
                    "database": "analytics",
                    "schema": "public",
                    "table_name": "dim_customer",
                    "diststyle": "ALL",
                    "sortkey1": "cust_id",
                    "size": 800,
                    "tbl_rows": 5_000_000,
                    "table_id": "9",
                }
            ]
        if "pg_table_def" in q:
            return [
                {
                    "schema": "public",
                    "table_name": "dim_customer",
                    "column_name": "cust_id",
                    "data_type": "bigint",
                    "distkey": False,
                    "sortkey": 1,
                },
                {
                    "schema": "public",
                    "table_name": "dim_customer",
                    "column_name": "region",
                    "data_type": "varchar",
                    "distkey": False,
                    "sortkey": 0,
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
                },
                {
                    "database": "analytics",
                    "schema": "public",
                    "table_name": "dim_customer",
                    "column_name": "region",
                    "data_type": "varchar",
                },
            ]
        if "pg_views" in q:
            return []
        return []

    cat = fetch_catalog_with_repository(
        "SELECT region FROM public.dim_customer",
        repository=repo,
        execute=execute,
        database="analytics",
        fetch_views=False,
        write_through=True,
    )
    assert any("dim_customer" in k for k in cat.tables)
    # write-through landed in cache
    assert repo.get_table("analytics", "public", "dim_customer") is not None
    assert repo.get_table("analytics", "public", "dim_customer").sortkeys == ("cust_id",)


def test_sql_helpers_mention_sources():
    assert "SVV_TABLE_INFO" in sql_svv_table_info_all().upper()
    assert "pg_table_def" in sql_pg_table_def_all().lower()
    assert "local" in sql_list_local_databases().lower()


def test_sql_pg_table_def_empty_names_is_valid():
    from redshift_decomposer.repository import sql_pg_table_def_for_tables

    sql = sql_pg_table_def_for_tables([])
    upper = sql.upper()
    assert "WHERE 1=0" in upper
    # Must not append a filter after ORDER BY
    if "ORDER BY" in upper:
        order_pos = upper.index("ORDER BY")
        assert "1=0" not in upper[order_pos:]
