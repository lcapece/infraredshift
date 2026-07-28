"""Canonical column contracts. Every provider must return DataFrames that conform.

Column lists reflect Redshift's SYS_QUERY_DETAIL and SVV_TABLE_INFO, normalized
to snake_case. Parsers translate vendor/format-specific names into these.
"""
from __future__ import annotations
from dataclasses import dataclass


QUERY_DETAIL_COLUMNS: dict[str, str] = {
    "user_id": "Int64",
    "query_id": "Int64",
    "transaction_id": "Int64",
    "session_id": "Int64",
    "database_name": "string",
    "parent_query_id": "Int64",
    "child_query_sequence": "Int64",
    "start_time": "datetime64[ns]",
    "end_time": "datetime64[ns]",
    "elapsed_time": "Int64",
    "aborted": "Int64",
    "step_name": "string",
    "table_id": "Int64",
    "table_name": "string",
    "schema_name": "string",
    "input_bytes": "Int64",
    "input_rows": "Int64",
    "output_bytes": "Int64",
    "output_rows": "Int64",
    "query_text": "string",
    "source_query_id": "Int64",
    "redshift_version": "string",
    "result_cache_hit": "boolean",
    "step_id": "Int64",
    "stream_id": "Int64",
    "is_rrscan": "boolean",
    "segment_id": "Int64",
    "is_child": "boolean",
    "source_type": "string",
    "query_blocks_read": "Int64",
    "spilled_block_local_disk": "Int64",
    "spilled_block_remote_disk": "Int64",
    "data_skewness": "float64",
    "time_skewness": "float64",
    "is_delayed_scan": "boolean",
    "delayed_scan_rows": "Int64",
    "join_type": "string",
    "alert_type": "string",
}

TABLE_INFO_COLUMNS: dict[str, str] = {
    "database": "string",
    "schema": "string",
    "table_id": "Int64",
    "table": "string",
    "encoded": "string",
    "diststyle": "string",
    "sortkey1": "string",
    "max_varchar": "Int64",
    "sortkey1_enc": "string",
    "sortkey_num": "Int64",
    "size": "Int64",
    "pct_used": "float64",
    "empty": "Int64",
    "unsorted": "float64",
    "stats_off": "float64",
    "tbl_rows": "Int64",
    "skew_sortkey1": "float64",
    "skew_rows": "float64",
    "estimated_visible_rows": "Int64",
    "risk_event": "string",
    "vacuum_sort_benefit": "float64",
    "create_time": "datetime64[ns]",
}


# Alternate column names DBeaver / different Redshift versions may emit.
QUERY_DETAIL_ALIASES: dict[str, str] = {
    "userid": "user_id",
    "queryid": "query_id",
    "xid": "transaction_id",
    "pid": "session_id",
    "database": "database_name",
    "parent_queryid": "parent_query_id",
    "child_query_seq": "child_query_sequence",
    "duration": "elapsed_time",
    "operation": "step_name",
    "tableid": "table_id",
    "tbl_name": "table_name",
    "schema": "schema_name",
    "bytes": "input_bytes",
    "rows": "input_rows",
    "out_bytes": "output_bytes",
    "out_rows": "output_rows",
    "sql": "query_text",
    "source_queryid": "source_query_id",
    "rs_version": "redshift_version",
    "cache_hit": "result_cache_hit",
    "stepid": "step_id",
    "streamid": "stream_id",
    "rrscan": "is_rrscan",
    "segmentid": "segment_id",
    "child": "is_child",
    "srctype": "source_type",
    "blocks_read": "query_blocks_read",
    "spill_local": "spilled_block_local_disk",
    "spill_remote": "spilled_block_remote_disk",
    "skewness_data": "data_skewness",
    "skewness_time": "time_skewness",
    "delayed_scan": "is_delayed_scan",
    "delayed_rows": "delayed_scan_rows",
    "jointype": "join_type",
    "alert": "alert_type",
}

TABLE_INFO_ALIASES: dict[str, str] = {
    "db": "database",
    "schemaname": "schema",
    "tableid": "table_id",
    "tbl": "table",
    "tablename": "table",
    "dist_style": "diststyle",
    "sort_key": "sortkey1",
    "sortkey": "sortkey1",
    "sort_key_num": "sortkey_num",
    "size_mb": "size",
    "pct": "pct_used",
    "empty_blocks": "empty",
    "unsorted_pct": "unsorted",
    "stats_stale": "stats_off",
    "rows": "tbl_rows",
    "row_count": "tbl_rows",
    "skew": "skew_rows",
    "est_rows": "estimated_visible_rows",
    "risk": "risk_event",
    "vacuum_benefit": "vacuum_sort_benefit",
    "created": "create_time",
}


@dataclass(frozen=True)
class StepCategory:
    SCAN = "scan"
    JOIN = "join"
    AGGREGATE = "aggregate"
    REDISTRIBUTE = "redistribute"
    SORT = "sort"
    OTHER = "other"


def categorize_step(step_name: str | None) -> str:
    if not step_name:
        return StepCategory.OTHER
    s = step_name.strip().lower()
    if "join" in s or "nested loop" in s:
        return StepCategory.JOIN
    if "aggregate" in s or "agg" == s:
        return StepCategory.AGGREGATE
    if "dist" in s or "bcast" in s or "broadcast" in s or "redistribute" in s:
        return StepCategory.REDISTRIBUTE
    if "sort" in s or "merge" in s:
        return StepCategory.SORT
    if "scan" in s:
        return StepCategory.SCAN
    return StepCategory.OTHER
