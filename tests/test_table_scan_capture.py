from __future__ import annotations

from analyzer.ingest_redshift import INCREMENTAL_DEDUPE_KEYS
from analyzer.redshift_queries import table_scan_info_sql


def test_table_scan_capture_keeps_legitimate_temp_and_dev_named_tables() -> None:
    sql = table_scan_info_sql(target_ids=[101]).lower()
    assert "not like '%temp%'" not in sql
    assert "not like 'dev%'" not in sql
    assert "d.query_id" in sql


def test_table_scan_incremental_grain_is_query_and_table() -> None:
    assert INCREMENTAL_DEDUPE_KEYS["table_scan_info"] == (
        "query_id",
        "full_table_name",
    )
