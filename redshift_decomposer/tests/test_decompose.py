"""Golden tests for redshift-decomposer."""

from __future__ import annotations

from redshift_decomposer import Catalog, TableStats, ViewDef, decompose


def _fact_catalog() -> Catalog:
    return Catalog(
        tables={
            "analytics.public.fact_orders": TableStats(
                columns={
                    "order_id": "BIGINT",
                    "cust_id": "BIGINT",
                    "order_date": "DATE",
                    "amount": "DECIMAL",
                },
                diststyle="KEY",
                distkey="cust_id",
                sortkeys=("order_date",),
                rows=2_000_000_000,
                size_mb=420_000,
            ),
            "analytics.public.dim_customer": TableStats(
                columns={
                    "cust_id": "BIGINT",
                    "region": "VARCHAR",
                },
                diststyle="ALL",
                rows=5_000_000,
                size_mb=800,
            ),
        },
        views={
            "analytics.reporting.v_orders": ViewDef(
                sql="""
                SELECT order_id, cust_id, order_date, amount
                FROM analytics.public.fact_orders
                WHERE amount > 0
                """
            ),
        },
    )


def test_parse_empty():
    plan = decompose("", Catalog())
    assert not plan.parse_ok
    assert plan.parse_error


def test_view_explosion_and_physical_stage():
    catalog = _fact_catalog()
    sql = """
    SELECT o.order_id, c.region, o.amount
    FROM analytics.reporting.v_orders o
    JOIN analytics.public.dim_customer c
      ON o.cust_id = c.cust_id
    WHERE o.order_date >= DATE '2024-01-01'
    """
    plan = decompose(sql, catalog, minimum_rows=1_000_000, minimum_size_mb=100)
    assert plan.parse_ok, plan.parse_error
    assert plan.stages, plan.findings
    assert any(s.stage_type == "physical_input" for s in plan.stages)
    assert "CREATE TEMP TABLE" in plan.script
    assert "tmp_rsd_" in plan.script
    # Engineer-facing: imperfect, likelihood shown, skeleton framing
    assert plan.conversion_likelihood >= 0.7
    assert "NOT PERFECT" in plan.script.upper()
    assert "conversion-success likelihood" in plan.script.lower()
    assert any("skeleton" in f.title.lower() or "skeleton" in f.detail.lower()
               for f in plan.findings)
    brief = plan.engineer_brief()
    assert "not perfect" in brief.lower()
    assert "skeleton" in brief.lower()
    # fact should be staged; dim is small and may not stage
    fact_stages = [s for s in plan.stages if "fact_orders" in s.source]
    assert fact_stages
    stage = fact_stages[0]
    assert stage.distkey in {"", "cust_id"} or stage.distkey == "cust_id"
    assert "order_date" in plan.script or "ORDER_DATE" in plan.script.upper()
    # final query should reference temp, not only the view
    assert "v_orders" not in plan.final_sql.lower() or "tmp_rsd_" in plan.final_sql.lower()
    titles = {f.title for f in plan.findings}
    assert any("explod" in t.lower() or "Views exploded" in t for t in titles)


def test_predicate_push_and_sortkey():
    catalog = _fact_catalog()
    sql = """
    SELECT order_id, amount
    FROM analytics.public.fact_orders f
    WHERE f.order_date >= DATE '2024-01-01'
      AND f.order_date < DATE '2025-01-01'
    """
    plan = decompose(sql, catalog, minimum_rows=1, minimum_size_mb=1)
    assert plan.parse_ok
    assert plan.stages
    stage = plan.stages[0]
    assert stage.pushed_predicates or "order_date" in stage.sql.lower()
    assert "SORTKEY" in stage.sql or stage.sortkeys


def test_small_table_not_staged():
    catalog = Catalog(
        tables={
            "public.tiny": TableStats(
                columns={"id": "INT", "v": "VARCHAR"},
                rows=100,
                size_mb=1,
            )
        }
    )
    plan = decompose("SELECT id FROM public.tiny WHERE id = 1", catalog)
    assert plan.parse_ok
    assert not plan.stages
    assert "no stages" in plan.script.lower() or "SELECT" in plan.script


def test_cte_materialization():
    catalog = Catalog(
        tables={
            "public.big_fact": TableStats(
                columns={"id": "BIGINT", "g": "INT", "v": "INT"},
                rows=50_000_000,
                size_mb=20_000,
            )
        }
    )
    sql = """
    WITH heart AS (
      SELECT g, SUM(v) AS total
      FROM public.big_fact
      GROUP BY g
    )
    SELECT a.g, a.total, b.total AS total2
    FROM heart a
    JOIN heart b ON a.g = b.g
    """
    plan = decompose(sql, catalog, minimum_rows=1, minimum_size_mb=1)
    assert plan.parse_ok
    # either physical stage and/or CTE stage
    assert plan.stages
    types = {s.stage_type for s in plan.stages}
    assert "physical_input" in types or "cte" in types


def test_script_is_multi_statement():
    catalog = _fact_catalog()
    plan = decompose(
        "SELECT order_id FROM analytics.public.fact_orders WHERE order_date >= DATE '2024-01-01'",
        catalog,
        minimum_rows=1,
        minimum_size_mb=1,
    )
    assert plan.changed
    assert plan.script.count(";") >= 2


def test_view_body_filter_column_survives_pruning():
    """A column used ONLY in an exploded view's WHERE must stay in the stage.

    Regression: with two physical tables in the query, the unqualified
    filter column inside the view body was attributed to nothing, pruned out
    of the staged temp, and the generated script referenced a column the
    temp did not have.
    """
    from redshift_decomposer import Catalog, TableStats, ViewDef, decompose

    catalog = Catalog(
        tables={
            "analytics.public.fact_orders": TableStats(
                columns={c: "VARCHAR" for c in (
                    "order_id", "cust_id", "order_date", "amount", "status",
                    "channel", "promo_code", "warehouse_id", "carrier",
                    "ship_date", "return_flag", "tax", "discount", "etl_batch_id",
                )},
                diststyle="KEY", distkey="cust_id", sortkeys=("order_date",),
                rows=2_000_000_000, size_mb=420_000,
            ),
            "analytics.public.dim_customer": TableStats(
                columns={"cust_id": "BIGINT", "region": "VARCHAR"},
                diststyle="ALL", rows=5_000_000, size_mb=800,
            ),
        },
        views={
            "analytics.reporting.v_orders": ViewDef(
                sql="SELECT order_id, cust_id, order_date, amount, status "
                    "FROM analytics.public.fact_orders WHERE status <> 'CANCELLED'"
            ),
            "analytics.reporting.v_customer_orders": ViewDef(
                sql="SELECT o.order_id, o.cust_id, o.order_date, o.amount, c.region "
                    "FROM analytics.reporting.v_orders o "
                    "JOIN analytics.public.dim_customer c ON o.cust_id = c.cust_id"
            ),
        },
    )
    plan = decompose(
        "SELECT region, SUM(amount) AS revenue "
        "FROM analytics.reporting.v_customer_orders "
        "WHERE order_date >= DATE '2024-01-01' GROUP BY region",
        catalog,
    )
    assert plan.parse_ok
    fact_stage = next(s for s in plan.stages if "fact_orders" in s.name)
    assert "status" in fact_stage.columns, fact_stage.columns
    assert "channel" not in fact_stage.columns, "pruning should still drop unused columns"


def test_dml_is_blocked():
    catalog = _fact_catalog()
    plan = decompose(
        "DELETE FROM analytics.public.fact_orders WHERE amount < 0",
        catalog,
    )
    assert not plan.parse_ok
    titles = {f.title for f in plan.findings}
    assert "Not a read query" in titles


def test_update_is_blocked():
    catalog = _fact_catalog()
    plan = decompose(
        "UPDATE analytics.public.fact_orders SET amount = 0 WHERE amount < 0",
        catalog,
    )
    assert not plan.parse_ok
    assert any(f.title == "Not a read query" for f in plan.findings)


def test_or_predicate_not_pushed_into_stage():
    """OR filters must stay on the final query, not baked into a shared CTAS."""
    catalog = _fact_catalog()
    sql = """
    SELECT o.order_id, o.amount
    FROM analytics.public.fact_orders o
    WHERE o.order_date >= DATE '2024-01-01'
       OR o.amount > 1000
    """
    plan = decompose(sql, catalog, minimum_rows=1, minimum_size_mb=1)
    assert plan.parse_ok
    fact_stages = [s for s in plan.stages if "fact_orders" in s.source]
    if fact_stages:
        stage_sql = fact_stages[0].sql.upper()
        assert " OR " not in stage_sql, stage_sql


def test_getdate_predicate_not_pushed_into_stage():
    catalog = _fact_catalog()
    sql = """
    SELECT o.order_id
    FROM analytics.public.fact_orders o
    WHERE o.order_date < GETDATE()
    """
    plan = decompose(sql, catalog, minimum_rows=1, minimum_size_mb=1)
    assert plan.parse_ok
    fact_stages = [s for s in plan.stages if "fact_orders" in s.source]
    if fact_stages:
        assert "GETDATE" not in fact_stages[0].sql.upper()
