"""Tests for the DBA-approvable fix script generator.

The contract: safe maintenance is emitted as runnable SQL; design changes are
ALWAYS commented out; every statement carries evidence.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analyzer.fix_script import build_fix_script  # noqa: E402


def _fixture() -> tuple[pd.DataFrame, pd.DataFrame]:
    groups = pd.DataFrame(
        [
            {
                "repeat_group_id": "RQ001",
                "query_count": 40,
                "sql_filter_columns": "o.order_date, o.region_id",
                "sql_join_columns": "o.customer_id, c.customer_id",
            }
        ]
    )
    group_tables = pd.DataFrame(
        [
            {
                "repeat_group_id": "RQ001",
                "table_key": "edw.sales.fact_orders",
                "schema_name": "sales",
                "table_name": "fact_orders",
                "diststyle": "even",
                "sortkey1": "",
                "size_mb": 5000.0,
                "unsorted_pct": 42.0,
                "stats_off": 25.0,
                "skew_rows": 1.0,
                "scan_query_count": 40.0,
                "scan_duration_s": 3600.0,
                "scan_input_rows_m": 900.0,
                "rrscan_query_pct": 0.1,
                "full_scan_query_pct": 0.9,
                "table_flags": "no sort key",
                "table_recommendation": "",
            }
        ]
    )
    return groups, group_tables


def test_maintenance_statements_are_runnable():
    groups, group_tables = _fixture()
    script = build_fix_script(groups, group_tables, generated_at=datetime(2026, 7, 4, 12, 0))
    lines = [l for l in script.splitlines() if l.strip() and not l.strip().startswith("--")]
    assert "ANALYZE sales.fact_orders;" in lines
    assert "VACUUM SORT ONLY sales.fact_orders;" in lines


def test_design_changes_are_always_commented():
    groups, group_tables = _fixture()
    script = build_fix_script(groups, group_tables)
    runnable = [l for l in script.splitlines() if l.strip() and not l.strip().startswith("--")]
    assert all("ALTER" not in l for l in runnable), "ALTER statements must never be runnable as generated"
    assert "-- ALTER TABLE sales.fact_orders ALTER SORTKEY (order_date, region_id);" in script
    assert "-- ALTER TABLE sales.fact_orders ALTER DISTSTYLE KEY DISTKEY (customer_id);" in script


def test_evidence_comments_present():
    groups, group_tables = _fixture()
    script = build_fix_script(groups, group_tables)
    assert "statistics 25% stale" in script
    assert "42% of rows unsorted" in script
    assert "workload filters most on: order_date, region_id" in script
    assert "verify" in script.lower()


def test_alias_prefixes_are_stripped_from_candidates():
    groups, group_tables = _fixture()
    script = build_fix_script(groups, group_tables)
    assert "o.order_date" not in script.replace("-- ", "")


def test_scan_evidence_not_inflated_by_multiple_groups():
    groups = pd.DataFrame(
        [
            {
                "repeat_group_id": f"RQ00{i}",
                "query_count": 5,
                "sql_filter_columns": "order_date",
                "sql_join_columns": "",
            }
            for i in (1, 2, 3)
        ]
    )
    table = {
        "table_key": "edw.sales.fact_orders",
        "schema_name": "sales",
        "table_name": "fact_orders",
        "diststyle": "key",
        "sortkey1": "order_date",
        "size_mb": 5000.0,
        "unsorted_pct": 0.0,
        "stats_off": 0.0,
        "skew_rows": 1.0,
        "scan_query_count": 1.0,
        "scan_duration_s": 100.0,
        "scan_input_rows_m": 10.0,
        "rrscan_query_pct": 0.1,
        "full_scan_query_pct": 0.9,
        "table_flags": "",
        "table_recommendation": "",
    }
    group_tables = pd.DataFrame([{**table, "repeat_group_id": f"RQ00{i}"} for i in (1, 2, 3)])
    script = build_fix_script(groups, group_tables)
    # The three group rows carry the SAME table-level totals; they must not be
    # summed into scan_query_count=3, which would fake the >=3-scans gate.
    assert "rarely prunes" not in script


def test_empty_findings_yield_safe_script():
    script = build_fix_script(pd.DataFrame(), pd.DataFrame())
    assert "No table findings" in script
    runnable = [l for l in script.splitlines() if l.strip() and not l.strip().startswith("--")]
    assert not runnable


def test_action_queue_fallback_emits_runnable_maintenance():
    action_queue = pd.DataFrame(
        [
            {
                "action_id": "A01_ANALYZE_STALE_STATS",
                "action_type": "Maintenance",
                "severity": "warn",
                "subject": "edw.sales.fact_orders",
                "action_score": 92,
                "what_to_do": "Run ANALYZE on this table.",
                "why_now": "Stats are stale.",
                "evidence": "stats_off=33%",
                "sql_hint": "ANALYZE sales.fact_orders;",
            },
            {
                "action_id": "A03_REVIEW_DISTRIBUTION",
                "action_type": "Physical Design",
                "severity": "crit",
                "subject": "edw.sales.fact_orders",
                "action_score": 88,
                "what_to_do": "Review DISTSTYLE and DISTKEY.",
                "why_now": "Distribution skew is high.",
                "evidence": "skew_rows=6.2",
                "sql_hint": "Check dominant join columns; use KEY for co-location.",
            },
        ]
    )

    script = build_fix_script(pd.DataFrame(), pd.DataFrame(), action_queue=action_queue)
    runnable = [l for l in script.splitlines() if l.strip() and not l.strip().startswith("--")]

    assert "ANALYZE sales.fact_orders;" in runnable
    assert all("Check dominant join columns" not in line for line in runnable)
    assert "Distribution skew is high" in script


def test_table_review_fallback_emits_safe_table_actions():
    table_review = pd.DataFrame(
        [
            {
                "source_db": "edw",
                "schema_name": "sales",
                "table_name": "fact_orders",
                "table_key": "edw.sales.fact_orders",
                "diststyle": "EVEN",
                "sortkey1": "",
                "size_mb": 5000.0,
                "stats_off": 22.0,
                "unsorted_pct": 44.0,
                "skew_rows": 1.0,
                "scan_query_count": 9.0,
                "avg_scan_duration_s": 120.0,
                "rrscan_query_pct": 0.0,
                "full_scan_query_pct": 1.0,
            }
        ]
    )

    script = build_fix_script(pd.DataFrame(), pd.DataFrame(), table_review=table_review)

    assert "ANALYZE sales.fact_orders;" in script
    assert "VACUUM SORT ONLY sales.fact_orders;" in script
    assert "-- ALTER TABLE sales.fact_orders ALTER SORTKEY" in script


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
