"""SQL Lens X-ray: click probes, view explosion, recursive footprint."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analyzer.sql_xray import (
    build_table_lookup,
    build_view_map,
    clean_token,
    comparison_at,
    comparison_popup_text,
    explode_views,
    explode_views_recursive,
    explode_views_recursive_with_spans,
    resolve_footprint,
    resolve_table,
    table_popup_text,
    token_at,
)
from analyzer.join_size_highlight import alias_map

_TABLE_ROWS = [
    {
        "source_db": "prod", "schema_name": "public", "table_name": "fact_sales",
        "sortkey1": "sale_date", "diststyle": "KEY(store_id)",
        "unsorted": 18.0, "stats_off": 12.0, "skew_rows": 1.1,
        "tbl_rows": 1.2e9, "size_mb": 92160,
    },
    {
        "source_db": "prod", "schema_name": "public", "table_name": "dim_store",
        "sortkey1": "store_id", "diststyle": "ALL",
        "unsorted": 0.0, "stats_off": 0.0, "skew_rows": 1.0,
        "tbl_rows": 1200, "size_mb": 12,
    },
]
_VIEW_ROWS = [
    {
        "database": "prod", "schema": "public", "view_name": "v_sales_enriched",
        "definition_part_01": "SELECT f.*, s.store_name FROM public.fact_sales f "
                              "JOIN public.dim_store s ON f.store_id = s.store_id;",
    },
    {
        "database": "prod", "schema": "public", "view_name": "v_outer",
        "definition_part_01": "SELECT * FROM public.v_sales_enriched WHERE amount > 0",
    },
]
_SQL = 'select * from public."v_outer" v join public.dim_store d on v.store_id = d.store_id'


def test_clean_token_strips_quotes_and_parens():
    assert clean_token('("public"."fact_sales"),') == "public.fact_sales"
    assert clean_token('"V_OUTER"') == "v_outer"


def test_token_at_expands_identifier():
    text = "select * from public.fact_sales where x = 1"
    offset = text.index("fact_sales") + 3
    assert token_at(text, offset) == "public.fact_sales"


def test_comparison_at_finds_equality_sides():
    text = "on f.store_id = s.store_id"
    hit = comparison_at(text, text.index("=") )
    assert hit == ("f.store_id", "s.store_id", "=")
    assert comparison_at("where a >= 5", 8) is None  # >= is not a match
    ne = "where f.status <> s.status"
    assert comparison_at(ne, ne.index("<>"))[2] == "<>"


def test_table_popup_text_format():
    lookup = build_table_lookup(_TABLE_ROWS)
    text = table_popup_text("fact_sales", lookup["fact_sales"])
    assert "Sortkey: sale_date  Sorted: 82%" in text
    assert "Distkey: store_id  Skew: 1.1" in text
    assert "Stats: 12% stale" in text


def test_resolve_table_via_alias():
    lookup = build_table_lookup(_TABLE_ROWS)
    sql = "select * from public.fact_sales f where f.x = 1"
    resolved = resolve_table("f.x", alias_map(sql), lookup)
    assert resolved is not None and resolved[1]["sortkey1"] == "sale_date"


def test_comparison_popup_covers_both_sides():
    lookup = build_table_lookup(_TABLE_ROWS)
    sql = "select * from public.fact_sales f join public.dim_store s on f.store_id = s.store_id"
    text = comparison_popup_text("f.store_id", "s.store_id", "=", sql, lookup)
    assert "LEFT PHYSICAL ORIGIN" in text and "RIGHT PHYSICAL ORIGIN" in text
    assert text.count("Sortkey:") == 2


def test_comparison_popup_backtracks_cte_alias_to_physical_table():
    sql = """
WITH keys AS (
  SELECT a.customer_id FROM dev.raw.authorization_fact a
)
SELECT * FROM keys k
JOIN dev.crm.customer c ON k.customer_id = c.customer_id
"""
    lookup = build_table_lookup(
        [
            {"source_db": "dev", "schema_name": "raw", "table_name": "authorization_fact", "tbl_rows": 1000},
            {"source_db": "dev", "schema_name": "crm", "table_name": "customer", "tbl_rows": 100},
        ]
    )

    text = comparison_popup_text("k.customer_id", "c.customer_id", "=", sql, lookup)

    assert "dev.raw.authorization_fact.customer_id" in text
    assert "dev.crm.customer.customer_id" in text
    assert "no captured table metadata" not in text


def test_explode_views_one_level_and_alias_kept():
    views = build_view_map(_VIEW_ROWS)
    new_sql, exploded = explode_views(_SQL, views)
    assert exploded == ["public.v_outer"]
    assert "AS v" in new_sql  # original alias preserved
    assert "v_sales_enriched" in new_sql  # inner view now visible
    # second pass opens the nested view
    deeper, exploded2 = explode_views(new_sql, views)
    assert "public.v_sales_enriched" in exploded2
    assert "fact_sales" in deeper


def test_explode_views_recursive_returns_one_valid_query_with_nested_views():
    views = build_view_map(_VIEW_ROWS)

    expanded, exploded = explode_views_recursive(_SQL, views)

    assert exploded == ["public.v_outer", "public.v_sales_enriched"]
    assert "public.v_outer" not in expanded.lower()
    assert "public.v_sales_enriched" not in expanded.lower()
    assert expanded.count("(") >= 2
    assert "public.fact_sales" in expanded.lower()


def test_recursive_explosion_returns_full_parenthesis_spans_by_depth():
    views = build_view_map(_VIEW_ROWS)

    expanded, exploded, spans = explode_views_recursive_with_spans(_SQL, views)

    assert exploded == ["public.v_outer", "public.v_sales_enriched"]
    assert [span["depth"] for span in spans] == [0, 1]
    highlighted = [expanded[span["start"]:span["end"]] for span in spans]
    assert all(text.startswith("(") and text.endswith(")") for text in highlighted)
    assert highlighted[1] in highlighted[0]


def test_build_view_map_strips_create_view_wrapper_and_no_schema_binding():
    views = build_view_map(
        [
            {
                "database": "prod",
                "schema": "public",
                "view_name": "wrapped_view",
                "source_definition": (
                    "CREATE OR REPLACE VIEW public.wrapped_view AS "
                    "SELECT * FROM public.fact_sales WITH NO SCHEMA BINDING;"
                ),
            }
        ]
    )

    assert views["public.wrapped_view"] == "SELECT * FROM public.fact_sales"


def test_footprint_recurses_through_nested_views():
    views = build_view_map(_VIEW_ROWS)
    lookup = build_table_lookup(_TABLE_ROWS)
    rows = resolve_footprint(_SQL, views, lookup)
    by_object = {row["object"]: row for row in rows}
    assert by_object["public.v_outer"]["kind"] == "view"
    assert by_object["public.v_sales_enriched"]["kind"] == "view"
    fact = by_object["public.fact_sales"]
    assert fact["kind"] == "table"
    assert "v_sales_enriched" in fact["via"]  # reached through the view chain
    assert fact["sortkey1"] == "sale_date"
    stores = [row for row in rows if row["object"] == "public.dim_store"]
    assert any(row["via"] == "-" and row["depth"] == 0 for row in stores)  # direct join
    assert any("v_sales_enriched" in row["via"] for row in stores)         # and via the view
