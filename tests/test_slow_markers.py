"""Slow-query marker detection: each marker fires on evidence, silent when clean."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analyzer.slow_markers import detect_markers, markers_summary

_TABLES = [
    {
        "source_db": "prod", "schema_name": "public", "table_name": "fact_sales",
        "sortkey1": "sale_date", "diststyle": "KEY(store_id)",
        "unsorted": 35.0, "stats_off": 40.0, "skew_rows": 6.0,
        "tbl_rows": 1.2e9, "size_mb": 92160,
    },
    {
        "source_db": "prod", "schema_name": "public", "table_name": "dim_store",
        "sortkey1": "store_id", "diststyle": "ALL",
        "unsorted": 0.0, "stats_off": 0.0, "skew_rows": 1.0,
        "tbl_rows": 1200, "size_mb": 12,
    },
]
_VIEWS = [
    {"database": "prod", "schema": "public", "view_name": "v_sales",
     "source_definition": "SELECT * FROM public.fact_sales WHERE amount > 0"},
]


def _keys(sql, tables=_TABLES, views=None):
    return {m.key for m in detect_markers(sql, tables, views)}


def test_select_star_flagged():
    assert "select-star" in _keys("select * from public.dim_store")


def test_cross_join_is_critical():
    markers = detect_markers("select * from public.fact_sales f cross join public.dim_store s", _TABLES)
    cross = [m for m in markers if m.key == "cross-join"]
    assert cross and cross[0].severity == "crit"


def test_join_off_distkey_is_critical():
    # fact_sales distributed on store_id, joined on customer_id -> redistribution
    sql = ("select f.amount from public.fact_sales f "
           "join public.dim_store s on f.customer_id = s.customer_id")
    markers = detect_markers(sql, _TABLES)
    hit = [m for m in markers if m.key == "join-off-distkey"]
    assert hit and hit[0].severity == "crit"
    assert "fact_sales" in hit[0].detail and "store_id" in hit[0].detail


def test_join_off_sortkey_flagged():
    sql = ("select f.amount from public.fact_sales f "
           "join public.dim_store s on f.store_id = s.store_id")
    # joined on store_id == distkey (good) but sortkey is sale_date -> sortkey marker
    assert "join-off-sortkey" in _keys(sql)


def test_function_on_sortkey_is_critical():
    sql = "select * from public.fact_sales where trunc(sale_date) = '2026-01-01'"
    markers = detect_markers(sql, _TABLES)
    hit = [m for m in markers if m.key == "func-on-sortkey"]
    assert hit and hit[0].severity == "crit"
    assert "trunc" in hit[0].title.lower()


def test_stale_stats_unsorted_and_skew_all_fire():
    sql = "select f.amount from public.fact_sales f where f.region = 'NE'"
    keys = _keys(sql)
    assert {"stale-stats", "heavily-unsorted", "data-skew"} <= keys


def test_analyze_hint_names_the_table():
    sql = "select count(*) from public.fact_sales"
    stale = [m for m in detect_markers(sql, _TABLES) if m.key == "stale-stats"]
    assert stale and "ANALYZE" in stale[0].fix and "fact_sales" in stale[0].fix


def test_leading_wildcard_like_flagged():
    assert "leading-wildcard-like" in _keys("select * from public.dim_store where name like '%mart'")


def test_hidden_view_join_flagged_only_with_views():
    sql = "select * from public.v_sales v where v.amount > 0"
    assert "hidden-view-joins" not in _keys(sql, views=None)
    assert "hidden-view-joins" in _keys(sql, views=_VIEWS)


def test_many_joins_flagged():
    joins = " ".join(f"join t{i} on t{i}.k = a.k" for i in range(9))
    sql = f"select * from a {joins}"
    assert "many-joins" in _keys(sql, tables=[])


def test_clean_query_is_quiet():
    # small table, join on its own key, no wildcard/function/stale markers
    sql = ("select s.store_name from public.dim_store s "
           "where s.store_id = 42")
    keys = _keys(sql)
    # dim_store is small + fresh: none of the metadata markers should fire
    assert "join-off-distkey" not in keys
    assert "stale-stats" not in keys
    assert "data-skew" not in keys


def test_markers_sorted_critical_first():
    sql = ("select * from public.fact_sales f "
           "cross join public.dim_store s "
           "where trunc(f.sale_date) = '2026-01-01'")
    markers = detect_markers(sql, _TABLES)
    ranks = [m.rank for m in markers]
    assert ranks == sorted(ranks, reverse=True)
    assert markers[0].severity == "crit"


def test_summary_line():
    sql = "select * from public.fact_sales f cross join public.dim_store s"
    assert "critical" in markers_summary(detect_markers(sql, _TABLES))
    assert markers_summary([]) == "No slow-query markers detected."


def test_no_metadata_still_gives_structural_markers():
    # Even with zero table metadata, structural markers must work.
    keys = _keys("select * from a cross join b", tables=[])
    assert "select-star" in keys and "cross-join" in keys
