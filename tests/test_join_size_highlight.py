"""Size-aware join/predicate classification and the parse-free SQL reflow."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analyzer.join_size_highlight import (
    alias_map,
    annotate_line,
    build_table_meta,
    classify_equality,
)
from analyzer.sql_soft_format import soft_format_sql

_META_ROWS = [
    {"table_name": "public.fact_sales", "size_mb": 90000, "sortkey1": "sale_date", "diststyle": "key(store_id)"},
    {"table_name": "public.dim_store", "size_mb": 12, "sortkey1": "store_id", "diststyle": "all"},
]
_SQL = (
    "select s.store_name, f.amount from public.fact_sales f "
    "join public.dim_store s on f.store_id = s.store_id "
    "where f.customer_id = 42 and f.sale_date = '2026-01-01'"
)


def _meta():
    return build_table_meta(_META_ROWS)


def test_alias_map_resolves_aliases_and_suffixes():
    aliases = alias_map(_SQL)
    assert aliases["f"] == "public.fact_sales"
    assert aliases["s"] == "public.dim_store"
    assert aliases["fact_sales"] == "public.fact_sales"


def test_large_table_off_sortkey_is_red():
    # fact_sales joined on store_id but its sortkey is sale_date -> red
    assert classify_equality("f.store_id", "s.store_id", alias_map(_SQL), _meta()) == "red"


def test_large_table_on_sortkey_is_yellow():
    assert classify_equality("f.sale_date", "'2026-01-01'", alias_map(_SQL), _meta()) == "yellow"


def test_small_tables_only_is_green():
    assert classify_equality("s.store_id", "7", alias_map(_SQL), _meta()) == "green"


def test_unknown_tables_are_uncolored():
    assert classify_equality("x.a", "y.b", alias_map(_SQL), _meta()) == ""


def test_annotate_line_marks_tables_and_conditions():
    spans = annotate_line(_SQL, alias_map(_SQL), _meta())
    kinds = {kind for _, _, kind in spans}
    assert "large" in kinds       # fact_sales reference highlighted
    assert "small" in kinds       # dim_store / s.* de-emphasized
    assert "red" in kinds         # store_id join off sortkey
    assert "yellow" in kinds      # sale_date filter on the large table's sortkey


def test_single_table_statement_attributes_bare_columns():
    sql = "select * from public.fact_sales where customer_id = 42"
    assert classify_equality("customer_id", "42", alias_map(sql), _meta()) == "red"


def test_soft_format_always_produces_output():
    ugly = "select a,b from t1 join t2 on t1.k=t2.k where t1.x='a b' and t2.y=1 !!broken!!"
    out = soft_format_sql(ugly)
    assert "\nFROM" in out and "\nWHERE" in out and "\n  AND" in out
    assert "'a b'" in out  # string literal untouched
    assert soft_format_sql("") == ""
