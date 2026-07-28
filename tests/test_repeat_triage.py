"""Regression tests for repeat-pattern triage table matching."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analyzer.repeat_triage import (  # noqa: E402
    _build_table_index,
    _coverage_note,
    _group_table_names,
    _match_tables,
    _table_side_flags,
    COVERAGE_NONE,
)
from analyzer.redshift_meta import is_missing_sortkey  # noqa: E402


def test_cross_database_schema_table_collision_is_dropped():
    table_review = pd.DataFrame(
        [
            {"table_key": "db_a.sales.orders", "schema_name": "sales", "table_name": "orders", "stats_off": 0.0},
            {"table_key": "db_b.sales.orders", "schema_name": "sales", "table_name": "orders", "stats_off": 40.0},
        ]
    )
    index = _build_table_index(table_review)
    assert "db_a.sales.orders" in index
    assert "db_b.sales.orders" in index
    # Ambiguous short keys must not silently resolve to one database's stats.
    assert "sales.orders" not in index
    assert "orders" not in index
    matched, unmatched = _match_tables(["sales.orders"], index)
    assert not matched
    assert unmatched == ["sales.orders"]


def test_unambiguous_schema_table_still_matches():
    table_review = pd.DataFrame(
        [
            {"table_key": "db_a.sales.orders", "schema_name": "sales", "table_name": "orders", "stats_off": 0.0},
            {"table_key": "db_a.sales.refunds", "schema_name": "sales", "table_name": "refunds", "stats_off": 5.0},
        ]
    )
    index = _build_table_index(table_review)
    matched, unmatched = _match_tables(["sales.orders", "refunds"], index)
    assert len(matched) == 2
    assert not unmatched


def test_group_table_names_prefers_untruncated_list():
    group = pd.Series(
        {
            "repeat_group_id": "RQ001",
            "sql_tables": "a, b",
            "sql_tables_full": "a, b, c, d",
        }
    )
    names = _group_table_names(group, pd.DataFrame())
    assert names == ["a", "b", "c", "d"]


def test_coverage_note_explains_unavailable_table_metadata():
    note = _coverage_note(
        COVERAGE_NONE,
        ["spectrum.raw_clickstream"],
        pd.Series({"databases": "analytics"}),
    )
    assert "Table metadata unavailable for: spectrum.raw_clickstream" in note
    assert "Spectrum/external objects" in note
    assert "No captured table stats" not in note


def test_auto_sortkey_is_missing_not_rarely_pruning():
    assert is_missing_sortkey("auto(sortkey)")
    assert is_missing_sortkey("SORTKEY(AUTO)")
    table_row = {
        "table_name": "orders",
        "sortkey1": "auto(sortkey)",
        "diststyle": "auto",
        "size_mb": 5000,
        "unsorted_pct": 0,
        "stats_off": 0,
        "skew_rows": 1,
        "rrscan_query_pct": 0.1,
        "full_scan_query_pct": 0.9,
        "scan_query_count": 20,
    }
    group = pd.Series({"sql_filter_columns": "order_date", "sql_join_columns": ""})
    flags, _recs = _table_side_flags(table_row, group)
    assert "no sort key" in flags
    assert not any("rarely prunes" in flag for flag in flags)


def test_real_sortkey_can_be_flagged_as_rarely_pruning():
    table_row = {
        "table_name": "orders",
        "sortkey1": "created_at",
        "diststyle": "key(created_at)",
        "size_mb": 5000,
        "unsorted_pct": 0,
        "stats_off": 0,
        "skew_rows": 1,
        "rrscan_query_pct": 0.1,
        "full_scan_query_pct": 0.9,
        "scan_query_count": 20,
    }
    group = pd.Series({"sql_filter_columns": "order_date", "sql_join_columns": ""})
    flags, _recs = _table_side_flags(table_row, group)
    assert any("rarely prunes" in flag for flag in flags)


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as exc:
                failed += 1
                print(f"FAIL {name}: {exc}")
    raise SystemExit(1 if failed else 0)
