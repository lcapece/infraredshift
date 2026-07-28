from __future__ import annotations

import pandas as pd

from analyzer.repeat_triage import build_repeat_triage


def test_repeat_triage_uses_execution_database_to_resolve_ambiguous_schema_table() -> None:
    groups = pd.DataFrame([{
        "repeat_group_id": "RQ001",
        "sql_tables_full": "mart.fact_sales",
        "databases": "dev",
        "query_count": 6,
    }])
    members = pd.DataFrame()
    tables = pd.DataFrame([
        {
            "table_key": "producer.dev.mart.fact_sales",
            "source_db": "dev", "schema_name": "mart", "table_name": "fact_sales",
            "size_mb": 5000, "tbl_rows": 10_000_000, "diststyle": "EVEN", "sortkey1": "",
        },
        {
            "table_key": "producer.reporting.mart.fact_sales",
            "source_db": "reporting", "schema_name": "mart", "table_name": "fact_sales",
            "size_mb": 50, "tbl_rows": 1000, "diststyle": "ALL", "sortkey1": "id",
        },
    ])

    enriched, group_tables = build_repeat_triage(groups, members, tables)

    assert enriched.iloc[0]["triage_stats_coverage"] == "complete"
    assert enriched.iloc[0]["triage_missing_tables"] == ""
    assert len(group_tables) == 1
    assert group_tables.iloc[0]["table_key"] == "producer.dev.mart.fact_sales"
