"""Redshift extraction SQL used to build the local DuckDB snapshot.

The application is intentionally offline after capture. These statements pull
slow-query evidence from Redshift system views and store it locally in DuckDB.
Connection details and credentials are supplied at runtime only.
"""
from __future__ import annotations

from collections.abc import Iterable


DEFAULT_SLOW_QUERY_MINUTES = 10
DEFAULT_DATABASE_MIN_QUERY_COUNT = 250
DEFAULT_EXPLAIN_QUERY_LIMIT = 0
DEFAULT_DETAIL_FLOW_ROWS_PER_QUERY = 300
DEFAULT_ROOT_MIN_EXECUTION_SECONDS = 30
PROCEDURE_DEFINITION_CHUNKS = 32
TOP_QUERY_RANK_COLUMNS = {
    "elapsed_time": "elapsed_time",
    "execution_time": "execution_time",
}


def definition_chunk_columns(
    expression: str,
    *,
    prefix: str = "definition_part",
    chunks: int = PROCEDURE_DEFINITION_CHUNKS,
) -> str:
    cols = []
    for index in range(1, chunks + 1):
        start = (index - 1) * 65535 + 1
        cols.append(
            f"  SUBSTRING(COALESCE({expression}, ''), {start}, 65535) "
            f"AS {prefix}_{index:02d}"
        )
    return ",\n".join(cols)


def slow_query_filter(minutes: int = DEFAULT_SLOW_QUERY_MINUTES) -> str:
    threshold_seconds = int(minutes) * 60
    return (
        "query_id IN ("
        "SELECT query_id FROM sys_query_history "
        f"WHERE elapsed_time / 1000000 > {threshold_seconds}"
        ")"
    )


ROOT_FLOOR_BASES = ("execution_time", "elapsed_time")


def root_execution_filter(
    min_execution_seconds: float | None = None,
    floor_basis: str = "execution_time",
) -> str:
    """Phase-1 characteristic filter: every execution whose time on the chosen
    basis crosses the floor is captured. execution_time (default) nets out
    queue/wait; elapsed_time includes them. No row-count cap here: parents are
    discovered within the captured set, so frequent mid-weight repeats stay
    visible."""
    floor_value = (
        DEFAULT_ROOT_MIN_EXECUTION_SECONDS
        if min_execution_seconds is None
        else float(min_execution_seconds)
    )
    basis = str(floor_basis or "execution_time").strip().lower()
    if basis not in ROOT_FLOOR_BASES:
        basis = "execution_time"
    return f"{basis} / 1000000.0 >= {floor_value}"


def root_window_filter(window_days: float | None = None, alias: str = "h") -> str:
    """Optional phase-1 lookback window. sys_query_history retains about seven
    days, so the default (None) keeps the historical behavior of taking the
    whole view; a value bounds roots to start_time within the last N days."""
    if window_days is None or float(window_days) <= 0:
        return "TRUE"
    hours = max(1, int(round(float(window_days) * 24)))
    alias = alias.strip() or "h"
    return f"{alias}.start_time >= DATEADD(hour, -{hours}, GETDATE())"


def incremental_history_filter(
    alias: str = "q",
    after_time: object = None,
    after_query_id: object = None,
) -> str:
    query_id = _query_id_floor(after_query_id)
    timestamp_literal = _timestamp_literal(after_time)
    alias = alias.strip() or "q"
    if timestamp_literal and query_id > 0:
        return (
            f"({alias}.start_time > TIMESTAMP {timestamp_literal} "
            f"OR ({alias}.start_time = TIMESTAMP {timestamp_literal} AND {alias}.query_id > {query_id}) "
            f"OR ({alias}.start_time IS NULL AND {alias}.query_id > {query_id}))"
        )
    if timestamp_literal:
        return f"{alias}.start_time > TIMESTAMP {timestamp_literal}"
    if query_id > 0:
        return f"{alias}.query_id > {query_id}"
    return "TRUE"


def _query_id_floor(value: object) -> int:
    try:
        return max(0, int(float(str(value).strip())))
    except (TypeError, ValueError):
        return 0


def _timestamp_literal(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("T", " ").replace("Z", "").replace("'", "''")
    return f"'{text}'"


def target_ids_cte(target_ids) -> str:
    """Phase-2 target set: literal representative query ids, one per parent,
    resolved locally from the captured roots. Keeps every evidence capture on
    the exact same id list."""
    ids = ", ".join(str(int(query_id)) for query_id in target_ids)
    return f"""
target_queries AS (
  SELECT query_id
  FROM sys_query_history
  WHERE query_id IN ({ids})
)"""


def target_query_cte(
    minutes: int = DEFAULT_SLOW_QUERY_MINUTES,
    query_limit: int = DEFAULT_EXPLAIN_QUERY_LIMIT,
    rank_by: str = "elapsed_time",
    *,
    evidence_only: bool = False,
    incremental_after_time: object = None,
    incremental_after_query_id: object = None,
) -> str:
    threshold_seconds = int(minutes) * 60
    limit = max(0, int(query_limit or DEFAULT_EXPLAIN_QUERY_LIMIT))
    rank_col = TOP_QUERY_RANK_COLUMNS.get(str(rank_by or "").strip().lower(), "elapsed_time")
    incremental_filter = incremental_history_filter(
        "q",
        incremental_after_time,
        incremental_after_query_id,
    )
    order_limit_clause = (
        f"\n  ORDER BY rank_metric DESC NULLS LAST, tiebreak_elapsed DESC NULLS LAST\n  LIMIT {limit}"
        if limit > 0
        else ""
    )
    simple_order_limit_clause = (
        f"\n  ORDER BY {rank_col} DESC NULLS LAST, elapsed_time DESC NULLS LAST\n  LIMIT {limit}"
        if limit > 0
        else ""
    )
    # Evidence captures use distinct parent representatives with provable step
    # evidence: scan every above-threshold history row, drop rows that can never
    # have sys_query_detail / sys_query_explain children (PROCEDURE calls,
    # utility statements, result-cache hits, failed queries), prove evidence via
    # a sys_query_detail semi-join, and dedupe repeat executions to one
    # representative per (database, statement text). History/text captures stay
    # inclusive: repeat grouping needs every threshold-qualified execution and
    # the CALL rows. query_limit is an optional safety cap only; 0 means all
    # qualifying representatives.
    if evidence_only:
        return f"""
target_queries AS (
  SELECT query_id
  FROM (
    SELECT
      q.query_id,
      q.{rank_col} AS rank_metric,
      q.elapsed_time AS tiebreak_elapsed,
      ROW_NUMBER() OVER (
        PARTITION BY q.database_name, MD5(COALESCE(q.query_text, ''))
        ORDER BY q.{rank_col} DESC NULLS LAST, q.elapsed_time DESC NULLS LAST, q.query_id
      ) AS duplicate_rank
    FROM sys_query_history q
    WHERE q.elapsed_time / 1000000 > {threshold_seconds}
      AND {incremental_filter}
      AND LOWER(TRIM(q.status::VARCHAR)) = 'success'
      AND COALESCE(q.result_cache_hit, FALSE) = FALSE
      AND UPPER(TRIM(q.query_type::VARCHAR)) NOT IN ('PROCEDURE', 'UTILITY')
      AND q.query_id IN (
        SELECT d.query_id
        FROM sys_query_detail d
      )
  ) ranked
  WHERE duplicate_rank = 1{order_limit_clause}
)"""
    return f"""
target_queries AS (
  SELECT query_id
  FROM sys_query_history q
  WHERE q.elapsed_time / 1000000 > {threshold_seconds}
    AND {incremental_filter}{simple_order_limit_clause}
)"""


def query_details_sql(
    minutes: int = DEFAULT_SLOW_QUERY_MINUTES,
    query_limit: int = DEFAULT_EXPLAIN_QUERY_LIMIT,
    rank_by: str = "elapsed_time",
    target_ids=None,
    incremental_after_time: object = None,
    incremental_after_query_id: object = None,
) -> str:
    cte = target_ids_cte(target_ids) if target_ids else target_query_cte(
        minutes,
        query_limit,
        rank_by,
        evidence_only=True,
        incremental_after_time=incremental_after_time,
        incremental_after_query_id=incremental_after_query_id,
    )
    return f"""
WITH {cte}
SELECT
  d.query_id,
  SUM(d.spilled_block_local_disk + d.spilled_block_remote_disk) AS total_spill,
  SUM(d.input_bytes) AS input_bytes,
  SUM(d.output_bytes) AS output_bytes,
  SUM(d.blocks_read) AS blocks_read,
  SUM(d.blocks_write) AS blocks_write,
  SUM(d.local_read_io) AS local_read_io,
  SUM(d.remote_read_io) AS remote_read_io,
  SUM(d.input_rows) AS input_rows,
  SUM(d.output_rows) AS output_rows,
  CASE
    WHEN SUM(d.input_rows) > 0
    THEN SUM(d.output_rows)::DOUBLE PRECISION / SUM(d.input_rows)::DOUBLE PRECISION
    ELSE NULL::DOUBLE PRECISION
  END AS selectivity_ratio,
  SUM(d.duration) AS total_step_duration,
  MAX(d.duration) AS max_step_duration,
  AVG(d.duration) AS avg_step_duration,
  COUNT(*) AS total_steps,
  MAX(d.data_skewness) AS max_data_skewness,
  MAX(d.time_skewness) AS max_time_skewness,
  COUNT(DISTINCT d.segment_id) AS segments_used,
  COUNT(DISTINCT d.stream_id) AS streams_used,
  COUNT(CASE WHEN d.step_name::TEXT ILIKE '%scan%' THEN 1 ELSE NULL END) AS scan_steps,
  COUNT(CASE WHEN d.step_name::TEXT ILIKE '%join%' THEN 1 ELSE NULL END) AS join_steps,
  COUNT(CASE WHEN d.step_name::TEXT ILIKE '%sort%' THEN 1 ELSE NULL END) AS sort_steps,
  COUNT(CASE WHEN d.step_name::TEXT ILIKE '%agg%' THEN 1 ELSE NULL END) AS agg_steps,
  COUNT(DISTINCT d.table_id) AS tables_touched,
  -- Empty alert strings must not inflate alert_count (COUNT ignores only NULL).
  SUM(CASE WHEN NULLIF(TRIM(d.alert::VARCHAR), '') IS NOT NULL THEN 1 ELSE 0 END) AS alert_count,
  -- External/Spectrum steps are identified POSITIVELY: SYS_QUERY_DETAIL.source
  -- only has a value on scan steps and names the scanned object type, with 's3'
  -- meaning a Redshift Spectrum / external S3 scan. The previous "<> 'internal'"
  -- test was wrong: source is NULL on every non-scan step and holds non-'internal'
  -- values for ordinary LOCAL scans, so it swept nearly all steps into "external"
  -- and made almost every query look Spectrum-dominated. Match 's3'/'external'/
  -- 'spectrum' only.
  COUNT(CASE WHEN LOWER(d.source::TEXT) IN ('s3', 'external', 'spectrum') THEN 1 ELSE NULL END) AS external_steps,
  COUNT(CASE WHEN LOWER(d.source::TEXT) = 's3' THEN 1 ELSE NULL END) AS s3_steps,
  SUM(CASE WHEN LOWER(d.source::TEXT) IN ('s3', 'external', 'spectrum') THEN d.input_bytes ELSE NULL::BIGINT END) AS external_input_bytes,
  SUM(CASE WHEN LOWER(d.source::TEXT) IN ('s3', 'external', 'spectrum') THEN d.input_rows ELSE NULL::BIGINT END) AS external_input_rows,
  SUM(CASE WHEN LOWER(d.source::TEXT) IN ('s3', 'external', 'spectrum') THEN d.duration ELSE NULL::BIGINT END) AS external_duration,
  CASE
    WHEN SUM(d.duration) > 0
    THEN SUM(CASE WHEN LOWER(d.source::TEXT) IN ('s3', 'external', 'spectrum') THEN d.duration ELSE NULL::BIGINT END)::DOUBLE PRECISION
         / SUM(d.duration)::DOUBLE PRECISION
    ELSE NULL::DOUBLE PRECISION
  END AS external_duration_pct,
  CASE
    WHEN SUM(d.local_read_io + d.remote_read_io) > 0
    THEN SUM(d.remote_read_io)::DOUBLE PRECISION
         / SUM(d.local_read_io + d.remote_read_io)::DOUBLE PRECISION
    ELSE NULL::DOUBLE PRECISION
  END AS remote_io_ratio,
  CASE
    WHEN SUM(CASE WHEN LOWER(d.source::TEXT) IN ('s3', 'external', 'spectrum') THEN d.input_rows ELSE NULL::BIGINT END) > 0
    THEN SUM(CASE WHEN LOWER(d.source::TEXT) IN ('s3', 'external', 'spectrum') THEN d.output_rows ELSE NULL::BIGINT END)::DOUBLE PRECISION
         / SUM(CASE WHEN LOWER(d.source::TEXT) IN ('s3', 'external', 'spectrum') THEN d.input_rows ELSE NULL::BIGINT END)::DOUBLE PRECISION
    ELSE NULL::DOUBLE PRECISION
  END AS external_selectivity,
  SUM(CASE WHEN LOWER(d.source::TEXT) IN ('s3', 'external', 'spectrum') THEN d.spilled_block_remote_disk ELSE NULL::BIGINT END) AS external_spill_blocks,
  COUNT(DISTINCT CASE WHEN LOWER(d.source::TEXT) IN ('s3', 'external', 'spectrum') THEN d.table_name ELSE NULL::CHARACTER VARYING END) AS external_tables_touched,
  MAX(CASE WHEN LOWER(d.source::TEXT) IN ('s3', 'external', 'spectrum') THEN d.data_skewness ELSE NULL::INTEGER END) AS external_data_skew
FROM sys_query_detail d
JOIN target_queries t
  ON t.query_id = d.query_id
GROUP BY d.query_id
"""


def query_history_sql(
    minutes: int = DEFAULT_SLOW_QUERY_MINUTES,
    query_limit: int = DEFAULT_EXPLAIN_QUERY_LIMIT,
    rank_by: str = "elapsed_time",
    min_execution_seconds: float | None = None,
    floor_basis: str = "execution_time",
    incremental_after_time: object = None,
    incremental_after_query_id: object = None,
    window_days: float | None = None,
) -> str:
    _ = (minutes, query_limit, rank_by)
    return f"""
SELECT h.*
FROM sys_query_history h
WHERE h.{root_execution_filter(min_execution_seconds, floor_basis)}
  AND {root_window_filter(window_days, "h")}
  AND {incremental_history_filter("h", incremental_after_time, incremental_after_query_id)}
"""


def query_health_sql(
    minutes: int = DEFAULT_SLOW_QUERY_MINUTES,
    query_limit: int = DEFAULT_EXPLAIN_QUERY_LIMIT,
    rank_by: str = "elapsed_time",
    target_ids=None,
    incremental_after_time: object = None,
    incremental_after_query_id: object = None,
) -> str:
    cte = target_ids_cte(target_ids) if target_ids else target_query_cte(
        minutes,
        query_limit,
        rank_by,
        evidence_only=True,
        incremental_after_time=incremental_after_time,
        incremental_after_query_id=incremental_after_query_id,
    )
    return f"""
WITH {cte}
SELECT
  e.query_id,
  SUM((plan_node ILIKE '%Seq Scan%')::BIGINT) AS seq_scan_cnt,
  SUM((plan_node ILIKE '%S3 Seq Scan%')::BIGINT) AS s3_scan_cnt,
  SUM((plan_node ILIKE '%Partition Loop%')::BIGINT) AS partition_loop_cnt,
  SUM((plan_node ILIKE '%DS_DIST_BOTH%')::BIGINT) AS dist_both_cnt,
  SUM((plan_node ILIKE '%DS_BCAST_INNER%')::BIGINT) AS bcast_cnt,
  SUM((plan_node ILIKE '%DS_DIST_%')::BIGINT) AS dist_total_cnt,
  MAX((plan_node ILIKE '%Nested Loop%')::BIGINT) AS has_nested_loop,
  SUM((plan_node ILIKE '%Hash Join%')::BIGINT) AS hash_join_cnt,
  SUM((plan_node ILIKE '%Subquery Scan%')::BIGINT) AS subquery_cnt,
  SUM((plan_node ILIKE '%Network%')::BIGINT) AS network_cnt,
  MAX((plan_node ILIKE '%missing statistics%')::BIGINT) AS missing_stats_flag,
  MAX(TRY_CAST(NULLIF(REGEXP_SUBSTR(plan_node, 'rows=([0-9]+)', 1, 1, 'e'), '') AS BIGINT)) AS max_est_rows,
  MAX(TRY_CAST(NULLIF(REGEXP_SUBSTR(plan_node, 'cost=[0-9.]+\\.\\.([0-9.]+)', 1, 1, 'e'), '') AS FLOAT)) AS max_cost,
  (
    SUM((plan_node ILIKE '%Seq Scan%')::BIGINT) * 1
    + SUM((plan_node ILIKE '%S3 Seq Scan%')::BIGINT) * 3
    + SUM((plan_node ILIKE '%DS_DIST_BOTH%')::BIGINT) * 5
    + SUM((plan_node ILIKE '%DS_BCAST_INNER%')::BIGINT) * 2
    + MAX((plan_node ILIKE '%Nested Loop%')::BIGINT) * 10
    + MAX((plan_node ILIKE '%missing statistics%')::BIGINT) * 8
    + SUM((plan_node ILIKE '%Partition Loop%')::BIGINT) * 2
  ) AS cost_score,
  CASE
    WHEN SUM((plan_node ILIKE '%DS_DIST_BOTH%')::BIGINT) > 0 THEN 'BAD_DISTRIBUTION'
    WHEN MAX((plan_node ILIKE '%Nested Loop%')::BIGINT) = 1 THEN 'NESTED_LOOP_RISK'
    WHEN MAX((plan_node ILIKE '%S3 Seq Scan%')::BIGINT) > 0 THEN 'S3_HEAVY'
    WHEN MAX((plan_node ILIKE '%missing statistics%')::BIGINT) = 1 THEN 'MISSING_STATS'
    WHEN SUM((plan_node ILIKE '%Seq Scan%')::BIGINT) > 5 THEN 'SCAN_HEAVY'
    ELSE 'GENERAL'
  END AS dominant_issue,
  CASE
    WHEN (SUM((plan_node ILIKE '%DS_DIST_BOTH%')::BIGINT) * 5 + MAX((plan_node ILIKE '%Nested Loop%')::BIGINT) * 10) > 50 THEN 'VERY_EXPENSIVE'
    WHEN (SUM((plan_node ILIKE '%Seq Scan%')::BIGINT) + SUM((plan_node ILIKE '%S3 Seq Scan%')::BIGINT) * 3) > 20 THEN 'EXPENSIVE'
    WHEN SUM((plan_node ILIKE '%Seq Scan%')::BIGINT) + SUM((plan_node ILIKE '%S3 Seq Scan%')::BIGINT) > 10 THEN 'MODERATE'
    ELSE 'LIGHT'
  END AS cost_tier
FROM sys_query_explain e
JOIN target_queries t
  ON t.query_id = e.query_id
GROUP BY 1
"""


def query_explain_sql(
    minutes: int = DEFAULT_SLOW_QUERY_MINUTES,
    query_limit: int = DEFAULT_EXPLAIN_QUERY_LIMIT,
    rank_by: str = "elapsed_time",
    target_ids=None,
    incremental_after_time: object = None,
    incremental_after_query_id: object = None,
) -> str:
    cte = target_ids_cte(target_ids) if target_ids else target_query_cte(
        minutes,
        query_limit,
        rank_by,
        evidence_only=True,
        incremental_after_time=incremental_after_time,
        incremental_after_query_id=incremental_after_query_id,
    )
    return f"""
WITH {cte}
SELECT
  userid,
  query_id,
  child_query_sequence,
  plan_node_id,
  plan_parent_id,
  plan_node,
  plan_info
FROM sys_query_explain
WHERE query_id IN (SELECT query_id FROM target_queries)
ORDER BY query_id, child_query_sequence, plan_node_id
"""


def query_detail_flow_sql(
    minutes: int = DEFAULT_SLOW_QUERY_MINUTES,
    query_limit: int = DEFAULT_EXPLAIN_QUERY_LIMIT,
    rows_per_query: int = DEFAULT_DETAIL_FLOW_ROWS_PER_QUERY,
    rank_by: str = "elapsed_time",
    target_ids=None,
    incremental_after_time: object = None,
    incremental_after_query_id: object = None,
) -> str:
    row_limit = max(25, int(rows_per_query or DEFAULT_DETAIL_FLOW_ROWS_PER_QUERY))
    cte = target_ids_cte(target_ids) if target_ids else target_query_cte(
        minutes,
        query_limit,
        rank_by,
        evidence_only=True,
        incremental_after_time=incremental_after_time,
        incremental_after_query_id=incremental_after_query_id,
    )
    return f"""
WITH {cte},
aggregated_flow AS (
  SELECT
    d.user_id,
    d.query_id,
    d.child_query_sequence,
    d.metrics_level,
    d.step_name,
    d.step_id,
    d.step_attribute,
    d.stream_id,
    d.segment_id,
    d.plan_node_id,
    d.plan_parent_id,
    d.table_id,
    d.table_name,
    d.source,
    d.is_rrscan,
    MIN(d.start_time) AS start_time,
    MAX(d.end_time) AS end_time,
    SUM(COALESCE(d.duration, 0)) AS duration,
    MAX(COALESCE(d.duration, 0)) AS max_duration,
    COUNT(*) AS detail_row_count,
    SUM(COALESCE(d.input_bytes, 0)) AS input_bytes,
    SUM(COALESCE(d.input_rows, 0)) AS input_rows,
    SUM(COALESCE(d.output_bytes, 0)) AS output_bytes,
    SUM(COALESCE(d.output_rows, 0)) AS output_rows,
    SUM(COALESCE(d.blocks_read, 0)) AS blocks_read,
    SUM(COALESCE(d.blocks_write, 0)) AS blocks_write,
    SUM(COALESCE(d.local_read_io, 0)) AS local_read_io,
    SUM(COALESCE(d.remote_read_io, 0)) AS remote_read_io,
    SUM(COALESCE(d.spilled_block_local_disk, 0)) AS spilled_block_local_disk,
    SUM(COALESCE(d.spilled_block_remote_disk, 0)) AS spilled_block_remote_disk,
    MAX(COALESCE(d.data_skewness, 0)) AS data_skewness,
    MAX(COALESCE(d.time_skewness, 0)) AS time_skewness,
    MAX(NULLIF(TRIM(d.alert::VARCHAR), '')) AS alert,
    SUM(CASE WHEN NULLIF(TRIM(d.alert::VARCHAR), '') IS NOT NULL THEN 1 ELSE 0 END) AS alert_count
  FROM sys_query_detail d
  JOIN target_queries t
    ON t.query_id = d.query_id
  WHERE
    LOWER(COALESCE(d.metrics_level::TEXT, '')) IN ('step', 'child query')
    OR d.step_id IS NOT NULL
    OR d.plan_node_id IS NOT NULL
  GROUP BY
    d.user_id,
    d.query_id,
    d.child_query_sequence,
    d.metrics_level,
    d.step_name,
    d.step_id,
    d.step_attribute,
    d.stream_id,
    d.segment_id,
    d.plan_node_id,
    d.plan_parent_id,
    d.table_id,
    d.table_name,
    d.source,
    d.is_rrscan
),
scored_flow AS (
  SELECT
    aggregated_flow.*,
    (
      LEAST(COALESCE(spilled_block_remote_disk, 0)::DOUBLE PRECISION / 1000.0, 25.0)
      + LEAST(COALESCE(spilled_block_local_disk, 0)::DOUBLE PRECISION / 1000.0, 15.0)
      + CASE WHEN COALESCE(alert_count, 0) > 0 THEN 20.0 ELSE 0.0 END
      + CASE WHEN COALESCE(data_skewness, 0) >= 3 THEN 15.0 WHEN COALESCE(data_skewness, 0) >= 2 THEN 8.0 ELSE 0.0 END
      + CASE WHEN COALESCE(time_skewness, 0) >= 3 THEN 15.0 WHEN COALESCE(time_skewness, 0) >= 2 THEN 8.0 ELSE 0.0 END
      + CASE WHEN COALESCE(remote_read_io, 0) > COALESCE(local_read_io, 0) AND COALESCE(remote_read_io, 0) > 0 THEN 10.0 ELSE 0.0 END
      + CASE WHEN LOWER(COALESCE(source::VARCHAR, 'internal')) <> 'internal' THEN 8.0 ELSE 0.0 END
      + CASE WHEN LOWER(COALESCE(step_name::VARCHAR, '')) LIKE '%join%' AND COALESCE(input_rows, 0) >= 10000000 THEN 8.0 ELSE 0.0 END
      + CASE WHEN LOWER(COALESCE(step_name::VARCHAR, '')) LIKE '%scan%' AND COALESCE(input_rows, 0) >= 10000000 THEN 6.0 ELSE 0.0 END
    ) AS pain_score,
    CASE
      WHEN COALESCE(alert_count, 0) > 0 THEN 'ALERT'
      WHEN COALESCE(spilled_block_remote_disk, 0) > 0 THEN 'REMOTE_SPILL'
      WHEN COALESCE(spilled_block_local_disk, 0) > 0 THEN 'LOCAL_SPILL'
      WHEN COALESCE(data_skewness, 0) >= 3 THEN 'DATA_SKEW'
      WHEN COALESCE(time_skewness, 0) >= 3 THEN 'TIME_SKEW'
      WHEN COALESCE(remote_read_io, 0) > COALESCE(local_read_io, 0) AND COALESCE(remote_read_io, 0) > 0 THEN 'REMOTE_IO'
      WHEN LOWER(COALESCE(source::VARCHAR, 'internal')) <> 'internal' THEN 'EXTERNAL_SOURCE'
      WHEN LOWER(COALESCE(step_name::VARCHAR, '')) LIKE '%join%' AND COALESCE(input_rows, 0) >= 10000000 THEN 'LARGE_JOIN'
      WHEN LOWER(COALESCE(step_name::VARCHAR, '')) LIKE '%scan%' AND COALESCE(input_rows, 0) >= 10000000 THEN 'LARGE_SCAN'
      ELSE 'NORMAL'
    END AS dominant_pain,
    (
      CASE WHEN COALESCE(alert_count, 0) > 0 THEN 'ALERT;' ELSE '' END
      || CASE WHEN COALESCE(spilled_block_remote_disk, 0) > 0 THEN 'REMOTE_SPILL;' ELSE '' END
      || CASE WHEN COALESCE(spilled_block_local_disk, 0) > 0 THEN 'LOCAL_SPILL;' ELSE '' END
      || CASE WHEN COALESCE(data_skewness, 0) >= 3 THEN 'DATA_SKEW;' ELSE '' END
      || CASE WHEN COALESCE(time_skewness, 0) >= 3 THEN 'TIME_SKEW;' ELSE '' END
      || CASE WHEN COALESCE(remote_read_io, 0) > COALESCE(local_read_io, 0) AND COALESCE(remote_read_io, 0) > 0 THEN 'REMOTE_IO;' ELSE '' END
      || CASE WHEN LOWER(COALESCE(source::VARCHAR, 'internal')) <> 'internal' THEN 'EXTERNAL_SOURCE;' ELSE '' END
      || CASE WHEN LOWER(COALESCE(step_name::VARCHAR, '')) LIKE '%join%' AND COALESCE(input_rows, 0) >= 10000000 THEN 'LARGE_JOIN;' ELSE '' END
      || CASE WHEN LOWER(COALESCE(step_name::VARCHAR, '')) LIKE '%scan%' AND COALESCE(input_rows, 0) >= 10000000 THEN 'LARGE_SCAN;' ELSE '' END
    ) AS pain_points
  FROM aggregated_flow
),
ranked_flow AS (
  SELECT
    scored_flow.*,
    ROW_NUMBER() OVER (
      PARTITION BY query_id
      ORDER BY
        COALESCE(child_query_sequence, 0),
        COALESCE(stream_id, 0),
        COALESCE(segment_id, 0),
        COALESCE(step_id, 0),
        COALESCE(plan_node_id, 0),
        COALESCE(start_time, end_time)
    ) AS detail_row_rank,
    ROW_NUMBER() OVER (
      PARTITION BY query_id
      ORDER BY
        pain_score DESC,
        duration DESC,
        COALESCE(input_rows, 0) DESC
    ) AS pain_rank
  FROM scored_flow
)
SELECT
  user_id,
  query_id,
  child_query_sequence,
  metrics_level,
  step_name,
  step_id,
  step_attribute,
  stream_id,
  segment_id,
  plan_node_id,
  plan_parent_id,
  start_time,
  end_time,
  duration,
  table_id,
  table_name,
  source,
  is_rrscan,
  input_bytes,
  input_rows,
  output_bytes,
  output_rows,
  blocks_read,
  blocks_write,
  local_read_io,
  remote_read_io,
  spilled_block_local_disk,
  spilled_block_remote_disk,
  data_skewness,
  time_skewness,
  alert,
  detail_row_count,
  max_duration,
  alert_count,
  pain_score,
  dominant_pain,
  pain_points,
  detail_row_rank,
  pain_rank
FROM ranked_flow
WHERE detail_row_rank <= {row_limit}
ORDER BY query_id, child_query_sequence, stream_id, segment_id, step_id, plan_node_id
"""


def query_history_all_sql(
    minutes: int = DEFAULT_SLOW_QUERY_MINUTES,
    query_limit: int = DEFAULT_EXPLAIN_QUERY_LIMIT,
    rank_by: str = "elapsed_time",
    min_execution_seconds: float | None = None,
    floor_basis: str = "execution_time",
    incremental_after_time: object = None,
    incremental_after_query_id: object = None,
    window_days: float | None = None,
) -> str:
    _ = (minutes, query_limit, rank_by)
    return f"""
SELECT h.*
FROM sys_query_history h
WHERE h.{root_execution_filter(min_execution_seconds, floor_basis)}
  AND {root_window_filter(window_days, "h")}
  AND {incremental_history_filter("h", incremental_after_time, incremental_after_query_id)}
"""


def query_text_sql(
    minutes: int = DEFAULT_SLOW_QUERY_MINUTES,
    query_limit: int = DEFAULT_EXPLAIN_QUERY_LIMIT,
    rank_by: str = "elapsed_time",
    min_execution_seconds: float | None = None,
    floor_basis: str = "execution_time",
    incremental_after_time: object = None,
    incremental_after_query_id: object = None,
    window_days: float | None = None,
) -> str:
    _ = (minutes, query_limit, rank_by)
    return f"""
SELECT qt.*
FROM sys_query_text qt
WHERE qt.query_id IN (
  SELECT query_id
  FROM sys_query_history h
  WHERE h.{root_execution_filter(min_execution_seconds, floor_basis)}
    AND {root_window_filter(window_days, "h")}
    AND {incremental_history_filter("h", incremental_after_time, incremental_after_query_id)}
)
"""


def child_query_text_sql(
    minutes: int = DEFAULT_SLOW_QUERY_MINUTES,
    query_limit: int = DEFAULT_EXPLAIN_QUERY_LIMIT,
    rank_by: str = "elapsed_time",
    min_execution_seconds: float | None = None,
    floor_basis: str = "execution_time",
    incremental_after_time: object = None,
    incremental_after_query_id: object = None,
    window_days: float | None = None,
) -> str:
    """Capture every optimizer-rewritten child SQL fragment for the same
    threshold-qualified root queries retained in query_history/query_text.

    SYS_CHILD_QUERY_TEXT stores 200-character fragments.  The local analytics
    view reconstructs one statement per (query_id, child_query_sequence) using
    the source sequence number.
    """
    _ = (minutes, query_limit, rank_by)
    return f"""
SELECT cqt.*
FROM sys_child_query_text cqt
WHERE cqt.query_id IN (
  SELECT query_id
  FROM sys_query_history h
  WHERE h.{root_execution_filter(min_execution_seconds, floor_basis)}
    AND {root_window_filter(window_days, "h")}
    AND {incremental_history_filter("h", incremental_after_time, incremental_after_query_id)}
)
"""


def user_info_sql(
    minutes: int = DEFAULT_SLOW_QUERY_MINUTES,
    query_limit: int = DEFAULT_EXPLAIN_QUERY_LIMIT,
    rank_by: str = "elapsed_time",
) -> str:
    _ = minutes
    _ = query_limit
    _ = rank_by
    return """
SELECT
  user_id::BIGINT AS user_id,
  user_name::VARCHAR AS user_name
FROM svv_user_info
ORDER BY user_name
"""


# Raw SVV_USER_INFO rows for the parsed user roster. Producer only, run against the
# enterprise_data_warehouse database. Name parsing happens in Python
# (analyzer.user_roster) rather than SQL.
USER_ROSTER_SQL = """
SELECT
  user_id::BIGINT AS user_id,
  user_name::VARCHAR AS user_name
FROM svv_user_info
ORDER BY user_name
"""


def database_discovery_sql(min_query_count: int = DEFAULT_DATABASE_MIN_QUERY_COUNT) -> str:
    _ = min_query_count
    return """
SELECT
  TRIM(database_name)::VARCHAR AS database_name,
  LOWER(TRIM(database_type))::VARCHAR AS database_type,
  0::BIGINT AS query_count
FROM svv_redshift_databases
WHERE LOWER(TRIM(database_type)) = 'local'
ORDER BY database_name
"""


def table_scan_info_sql(
    minutes: int = DEFAULT_SLOW_QUERY_MINUTES,
    query_limit: int = DEFAULT_EXPLAIN_QUERY_LIMIT,
    rank_by: str = "elapsed_time",
    target_ids=None,
    incremental_after_time: object = None,
    incremental_after_query_id: object = None,
) -> str:
    cte = target_ids_cte(target_ids) if target_ids else target_query_cte(
        minutes,
        query_limit,
        rank_by,
        evidence_only=True,
        incremental_after_time=incremental_after_time,
        incremental_after_query_id=incremental_after_query_id,
    )
    return f"""
WITH {cte}
SELECT
  d.query_id,
  d.table_name AS full_table_name,
  NULLIF(SPLIT_PART(d.table_name, '.', 1), '') AS table_database,
  CASE
    WHEN NULLIF(SPLIT_PART(d.table_name, '.', 3), '') IS NOT NULL
    THEN NULLIF(SPLIT_PART(d.table_name, '.', 2), '')
    ELSE NULL::VARCHAR
  END AS schema_name,
  CASE
    WHEN NULLIF(SPLIT_PART(d.table_name, '.', 3), '') IS NOT NULL
    THEN NULLIF(SPLIT_PART(d.table_name, '.', 3), '')
    ELSE d.table_name
  END AS table_name,
  COUNT(DISTINCT d.query_id) AS queries,
  SUM(d.duration) / 1000000.0 AS duration_s,
  (SUM(d.input_rows) / 1000000.0)::BIGINT AS input_rows_m,
  (SUM(d.output_rows) / 1000000.0)::BIGINT AS output_rows_m,
  COUNT(DISTINCT CASE WHEN d.is_rrscan::VARCHAR = 't' THEN d.query_id ELSE NULL END) AS rrscan_queries,
  COUNT(DISTINCT CASE WHEN d.is_rrscan::VARCHAR = 'f' THEN d.query_id ELSE NULL END) AS non_rrscan_queries
FROM sys_query_detail d
JOIN target_queries t
  ON t.query_id = d.query_id
WHERE LEN(COALESCE(d.table_name, '')) > 3
  AND LOWER(COALESCE(d.step_name::VARCHAR, '')) = 'scan'
  AND LOWER(d.table_name) NOT LIKE '%volt_t%'
GROUP BY 1, 2, 3, 4, 5
ORDER BY duration_s DESC
"""


TABLE_INFO_SQL = "SELECT * FROM SVV_TABLE_INFO"


# One row per fully-qualified Spectrum table. SYS_EXTERNAL_QUERY_DETAIL calls
# its S3 scan volume `returned_bytes`/`returned_rows`; AWS documents those as
# scanned bytes/rows for source_type S3. Post-filter output comes from the
# corresponding step-level SYS_QUERY_DETAIL scan row. get_partition_time is
# intentionally retained as a raw value because AWS does not document its unit;
# s3list_time is documented in milliseconds.
EXTERNAL_TABLE_INFO_SQL = r"""
WITH catalog AS (
  SELECT
    LOWER(TRIM(t.redshift_database_name) || '.' || TRIM(t.schemaname) || '.' || TRIM(t.tablename)) AS external_table_key,
    TRIM(t.redshift_database_name) AS redshift_database_name,
    TRIM(t.schemaname) AS schema_name,
    TRIM(t.tablename) AS table_name,
    TRIM(t.tabletype) AS table_type,
    TRIM(t.location) AS s3_location,
    TRIM(t.input_format) AS input_format,
    TRIM(t.output_format) AS output_format,
    TRIM(t.serialization_lib) AS serialization_lib,
    t.serde_parameters,
    t.compressed,
    t.parameters AS table_parameters
  FROM svv_external_tables t
  WHERE LOWER(TRIM(COALESCE(t.tabletype, 'TABLE'))) IN ('table', '')
),
column_stats AS (
  SELECT
    LOWER(TRIM(redshift_database_name) || '.' || TRIM(schemaname) || '.' || TRIM(tablename)) AS external_table_key,
    COUNT(*) AS column_count,
    SUM(CASE WHEN COALESCE(part_key, 0) > 0 THEN 1 ELSE 0 END) AS partition_key_count
  FROM svv_external_columns
  GROUP BY 1
),
partition_stats AS (
  SELECT external_table_key, 0::BIGINT AS catalog_partition_count
  FROM catalog
),
step_output AS (
  SELECT
    d.query_id,
    d.segment_id,
    LOWER(TRIM(d.table_name)) AS detail_table_name,
    SUM(d.output_bytes) AS output_bytes,
    SUM(d.output_rows) AS output_rows,
    MAX(d.data_skewness) AS max_data_skewness,
    MAX(d.time_skewness) AS max_time_skewness,
    SUM(d.spilled_block_local_disk + d.spilled_block_remote_disk) AS spill_blocks
  FROM sys_query_detail d
  WHERE d.step_id >= 0
    AND LOWER(TRIM(d.step_name)) = 'scan'
    AND LOWER(TRIM(d.source)) IN ('s3', 'external', 'spectrum')
  GROUP BY 1, 2, 3
),
external_segments AS (
  SELECT
    e.*,
    TRIM(h.database_name) AS query_database_name,
    CASE
      WHEN NULLIF(TRIM(e.warning_message), '') IS NULL THEN 'none'
      WHEN e.warning_message ILIKE '%permission%' OR e.warning_message ILIKE '%access denied%'
        OR e.warning_message ILIKE '%authoriz%' OR e.warning_message ILIKE '%credential%' THEN 'security'
      WHEN e.warning_message ILIKE '%partition%' THEN 'partition'
      WHEN e.warning_message ILIKE '%schema%' OR e.warning_message ILIKE '%column%'
        OR e.warning_message ILIKE '%type%' OR e.warning_message ILIKE '%serde%'
        OR e.warning_message ILIKE '%format%' THEN 'schema_or_format'
      WHEN e.warning_message ILIKE '%file%' OR e.warning_message ILIKE '%s3%'
        OR e.warning_message ILIKE '%location%' OR e.warning_message ILIKE '%path%' THEN 'file_or_location'
      WHEN e.warning_message ILIKE '%timeout%' OR e.warning_message ILIKE '%network%'
        OR e.warning_message ILIKE '%retry%' THEN 'connectivity'
      ELSE 'other'
    END AS warning_category
  FROM sys_external_query_detail e
  JOIN sys_query_history h ON h.query_id = e.query_id
  WHERE LOWER(TRIM(e.source_type)) = 's3'
),
mapped_segments AS (
  SELECT
    c.external_table_key,
    e.user_id,
    e.query_id,
    e.child_query_sequence,
    e.segment_id,
    e.start_time,
    e.end_time,
    e.duration,
    e.total_partitions,
    e.qualified_partitions,
    e.scanned_files,
    e.returned_rows AS scanned_rows,
    e.returned_bytes AS scanned_bytes,
    e.file_format AS observed_file_format,
    e.file_location,
    e.warning_message,
    e.warning_category,
    e.is_recursive,
    e.is_nested,
    e.s3list_time,
    e.get_partition_time,
    q.output_bytes,
    q.output_rows,
    q.max_data_skewness,
    q.max_time_skewness,
    q.spill_blocks,
    ROW_NUMBER() OVER (
      PARTITION BY e.query_id, e.child_query_sequence, e.segment_id, TRIM(e.table_name), TRIM(e.file_location)
      ORDER BY
        CASE
          WHEN LOWER(TRIM(e.table_name)) = c.external_table_key THEN 1
          WHEN LOWER(TRIM(e.table_name)) = LOWER(c.schema_name || '.' || c.table_name) THEN 2
          WHEN LOWER(TRIM(e.table_name)) = LOWER(c.table_name) THEN 3
          ELSE 4
        END,
        LEN(c.s3_location) DESC
    ) AS catalog_match_rank
  FROM external_segments e
  JOIN catalog c
    ON LOWER(e.query_database_name) = LOWER(c.redshift_database_name)
   AND (
        LOWER(TRIM(e.table_name)) IN (
          c.external_table_key,
          LOWER(c.schema_name || '.' || c.table_name),
          LOWER(c.table_name)
        )
        OR (
          NULLIF(TRIM(e.file_location), '') IS NOT NULL
          AND NULLIF(TRIM(c.s3_location), '') IS NOT NULL
          AND POSITION(LOWER(TRIM(c.s3_location)) IN LOWER(TRIM(e.file_location))) = 1
        )
   )
  LEFT JOIN step_output q
    ON q.query_id = e.query_id
   AND q.segment_id = e.segment_id
   AND (
        q.detail_table_name = LOWER(TRIM(e.table_name))
        OR q.detail_table_name LIKE '%.' || LOWER(TRIM(e.table_name))
        OR LOWER(TRIM(e.table_name)) LIKE '%.' || q.detail_table_name
   )
  QUALIFY catalog_match_rank = 1
),
activity AS (
  SELECT
    external_table_key,
    MIN(start_time) AS observation_start_time,
    MAX(end_time) AS observation_end_time,
    COUNT(DISTINCT query_id) AS query_count,
    COUNT(*) AS external_segment_count,
    COUNT(DISTINCT user_id) AS user_count,
    SUM(scanned_bytes) AS gross_scan_bytes,
    SUM(output_bytes) AS gross_output_bytes,
    SUM(scanned_rows) AS gross_scan_rows,
    SUM(output_rows) AS gross_output_rows,
    SUM(CASE WHEN output_bytes IS NOT NULL THEN 1 ELSE 0 END) AS output_metric_match_count,
    SUM(total_partitions) AS total_partitions_considered,
    SUM(qualified_partitions) AS qualified_partitions_scanned,
    SUM(CASE WHEN total_partitions > qualified_partitions THEN 1 ELSE 0 END) AS pruning_event_count,
    SUM(CASE WHEN total_partitions > 0 AND total_partitions = qualified_partitions THEN 1 ELSE 0 END) AS no_pruning_event_count,
    SUM(scanned_files) AS scanned_files,
    AVG(scanned_files::DOUBLE PRECISION) AS avg_files_per_segment,
    MAX(scanned_files) AS max_files_per_segment,
    SUM(duration) AS external_duration_us,
    AVG(duration::DOUBLE PRECISION) AS avg_external_duration_us,
    MAX(duration) AS max_external_duration_us,
    SUM(s3list_time) AS s3list_time_ms,
    AVG(s3list_time::DOUBLE PRECISION) AS avg_s3list_time_ms,
    MAX(s3list_time) AS max_s3list_time_ms,
    SUM(get_partition_time) AS get_partition_time_total_raw,
    AVG(get_partition_time::DOUBLE PRECISION) AS avg_get_partition_time_raw,
    MAX(get_partition_time) AS max_get_partition_time_raw,
    SUM(CASE WHEN LOWER(TRIM(is_recursive)) IN ('t', 'true', 'y', 'yes', '1') THEN 1 ELSE 0 END) AS recursive_scan_count,
    SUM(CASE WHEN LOWER(TRIM(is_nested)) IN ('t', 'true', 'y', 'yes', '1') THEN 1 ELSE 0 END) AS nested_scan_count,
    SUM(CASE WHEN warning_category <> 'none' THEN 1 ELSE 0 END) AS warning_event_count,
    SUM(CASE WHEN warning_category = 'security' THEN 1 ELSE 0 END) AS security_warning_count,
    SUM(CASE WHEN warning_category = 'partition' THEN 1 ELSE 0 END) AS partition_warning_count,
    SUM(CASE WHEN warning_category = 'schema_or_format' THEN 1 ELSE 0 END) AS schema_format_warning_count,
    SUM(CASE WHEN warning_category = 'file_or_location' THEN 1 ELSE 0 END) AS file_location_warning_count,
    SUM(CASE WHEN warning_category = 'connectivity' THEN 1 ELSE 0 END) AS connectivity_warning_count,
    SUM(CASE WHEN warning_category = 'other' THEN 1 ELSE 0 END) AS other_warning_count,
    MAX(CASE WHEN warning_category <> 'none' THEN warning_message END) AS warning_example,
    MAX(max_data_skewness) AS max_data_skewness,
    MAX(max_time_skewness) AS max_time_skewness,
    SUM(spill_blocks) AS external_spill_blocks,
    MAX(observed_file_format) AS observed_file_format
  FROM mapped_segments
  GROUP BY 1
),
error_matches AS (
  SELECT
    c.external_table_key,
    er.query_id,
    er.file_location,
    er.rowid,
    er.column_name,
    er.trigger,
    er.action,
    er.error_code,
    CASE
      WHEN er.action ILIKE '%truncat%' OR er.trigger ILIKE '%truncat%' THEN 'truncate'
      WHEN er.action ILIKE '%overflow%' OR er.trigger ILIKE '%overflow%' THEN 'overflow'
      WHEN er.action ILIKE '%invalid%' OR er.trigger ILIKE '%invalid%' THEN 'invalid_data'
      WHEN er.action ILIKE '%null%' OR er.trigger ILIKE '%null%' THEN 'null_handling'
      ELSE 'other'
    END AS error_category,
    ROW_NUMBER() OVER (
      PARTITION BY er.query_id, er.file_location, er.rowid, er.column_name, er.error_code
      ORDER BY LEN(c.s3_location) DESC
    ) AS catalog_match_rank
  FROM sys_external_query_error er
  JOIN sys_query_history h ON h.query_id = er.query_id
  JOIN catalog c
    ON LOWER(TRIM(h.database_name)) = LOWER(c.redshift_database_name)
   AND NULLIF(TRIM(er.file_location), '') IS NOT NULL
   AND NULLIF(TRIM(c.s3_location), '') IS NOT NULL
   AND POSITION(LOWER(TRIM(c.s3_location)) IN LOWER(TRIM(er.file_location))) = 1
  QUALIFY catalog_match_rank = 1
),
errors AS (
  SELECT
    external_table_key,
    COUNT(*) AS sampled_error_count,
    COUNT(DISTINCT query_id) AS queries_with_sampled_errors,
    SUM(CASE WHEN error_category = 'truncate' THEN 1 ELSE 0 END) AS truncation_error_count,
    SUM(CASE WHEN error_category = 'overflow' THEN 1 ELSE 0 END) AS overflow_error_count,
    SUM(CASE WHEN error_category = 'invalid_data' THEN 1 ELSE 0 END) AS invalid_data_error_count,
    SUM(CASE WHEN error_category = 'null_handling' THEN 1 ELSE 0 END) AS null_handling_error_count,
    SUM(CASE WHEN error_category = 'other' THEN 1 ELSE 0 END) AS other_error_count,
    COUNT(DISTINCT error_code) AS distinct_error_code_count,
    COUNT(DISTINCT column_name) AS affected_column_count
  FROM error_matches
  GROUP BY 1
)
SELECT
  c.external_table_key,
  c.redshift_database_name,
  c.schema_name,
  c.table_name,
  c.s3_location,
  c.table_type,
  c.input_format,
  c.output_format,
  c.serialization_lib,
  c.serde_parameters,
  c.compressed,
  c.table_parameters,
  COALESCE(cs.column_count, 0) AS column_count,
  COALESCE(cs.partition_key_count, 0) AS partition_key_count,
  COALESCE(ps.catalog_partition_count, 0) AS catalog_partition_count,
  a.observation_start_time,
  a.observation_end_time,
  COALESCE(a.query_count, 0) AS query_count,
  COALESCE(a.external_segment_count, 0) AS external_segment_count,
  COALESCE(a.user_count, 0) AS user_count,
  COALESCE(a.gross_scan_bytes, 0) AS gross_scan_bytes,
  COALESCE(a.gross_scan_bytes, 0)::DOUBLE PRECISION / 1073741824.0 AS gross_scan_gb,
  a.gross_output_bytes,
  a.gross_output_bytes::DOUBLE PRECISION / 1073741824.0 AS gross_output_gb,
  COALESCE(a.gross_scan_rows, 0) AS gross_scan_rows,
  a.gross_output_rows,
  COALESCE(a.output_metric_match_count, 0) AS output_metric_match_count,
  CASE WHEN a.gross_scan_bytes > 0 AND a.gross_output_bytes IS NOT NULL
    THEN a.gross_output_bytes::DOUBLE PRECISION / a.gross_scan_bytes::DOUBLE PRECISION END AS output_to_scan_byte_ratio,
  CASE WHEN a.gross_scan_bytes > 0 AND a.gross_output_bytes IS NOT NULL
    THEN 100.0 * (1.0 - LEAST(a.gross_output_bytes::DOUBLE PRECISION / a.gross_scan_bytes::DOUBLE PRECISION, 1.0)) END AS byte_reduction_pct_estimate,
  CASE WHEN a.gross_scan_rows > 0 AND a.gross_output_rows IS NOT NULL
    THEN a.gross_output_rows::DOUBLE PRECISION / a.gross_scan_rows::DOUBLE PRECISION END AS output_to_scan_row_ratio,
  CASE WHEN a.gross_scan_rows > 0 AND a.gross_output_rows IS NOT NULL
    THEN 100.0 * (1.0 - LEAST(a.gross_output_rows::DOUBLE PRECISION / a.gross_scan_rows::DOUBLE PRECISION, 1.0)) END AS row_filter_efficiency_pct,
  CASE
    WHEN COALESCE(a.query_count, 0) = 0 THEN 'NO_ACTIVITY'
    WHEN a.gross_output_rows IS NULL THEN 'OUTPUT_UNAVAILABLE'
    WHEN 100.0 * (1.0 - LEAST(a.gross_output_rows::DOUBLE PRECISION / NULLIF(a.gross_scan_rows, 0), 1.0)) >= 90 THEN 'HIGHLY_SELECTIVE'
    WHEN 100.0 * (1.0 - LEAST(a.gross_output_rows::DOUBLE PRECISION / NULLIF(a.gross_scan_rows, 0), 1.0)) >= 50 THEN 'SELECTIVE'
    WHEN 100.0 * (1.0 - LEAST(a.gross_output_rows::DOUBLE PRECISION / NULLIF(a.gross_scan_rows, 0), 1.0)) >= 10 THEN 'LIGHT_FILTERING'
    ELSE 'LOW_FILTERING'
  END AS filtering_assessment,
  COALESCE(a.total_partitions_considered, 0) AS total_partitions_considered,
  COALESCE(a.qualified_partitions_scanned, 0) AS qualified_partitions_scanned,
  CASE WHEN a.total_partitions_considered > 0
    THEN 100.0 * (1.0 - a.qualified_partitions_scanned::DOUBLE PRECISION / a.total_partitions_considered::DOUBLE PRECISION) END AS partition_pruning_pct,
  COALESCE(a.pruning_event_count, 0) AS pruning_event_count,
  COALESCE(a.no_pruning_event_count, 0) AS no_pruning_event_count,
  COALESCE(a.scanned_files, 0) AS scanned_files,
  a.avg_files_per_segment,
  a.max_files_per_segment,
  COALESCE(a.external_duration_us, 0)::DOUBLE PRECISION / 1000000.0 AS external_duration_s,
  a.avg_external_duration_us::DOUBLE PRECISION / 1000000.0 AS avg_external_duration_s,
  a.max_external_duration_us::DOUBLE PRECISION / 1000000.0 AS max_external_duration_s,
  COALESCE(a.s3list_time_ms, 0) AS s3list_time_ms,
  a.avg_s3list_time_ms,
  a.max_s3list_time_ms,
  a.get_partition_time_total_raw,
  a.avg_get_partition_time_raw,
  a.max_get_partition_time_raw,
  'AWS_NOT_DOCUMENTED'::VARCHAR AS get_partition_time_unit,
  COALESCE(a.recursive_scan_count, 0) AS recursive_scan_count,
  COALESCE(a.nested_scan_count, 0) AS nested_scan_count,
  COALESCE(a.warning_event_count, 0) AS warning_event_count,
  COALESCE(a.security_warning_count, 0) AS security_warning_count,
  COALESCE(a.partition_warning_count, 0) AS partition_warning_count,
  COALESCE(a.schema_format_warning_count, 0) AS schema_format_warning_count,
  COALESCE(a.file_location_warning_count, 0) AS file_location_warning_count,
  COALESCE(a.connectivity_warning_count, 0) AS connectivity_warning_count,
  COALESCE(a.other_warning_count, 0) AS other_warning_count,
  a.warning_example,
  COALESCE(e.sampled_error_count, 0) AS sampled_error_count,
  COALESCE(e.queries_with_sampled_errors, 0) AS queries_with_sampled_errors,
  COALESCE(e.truncation_error_count, 0) AS truncation_error_count,
  COALESCE(e.overflow_error_count, 0) AS overflow_error_count,
  COALESCE(e.invalid_data_error_count, 0) AS invalid_data_error_count,
  COALESCE(e.null_handling_error_count, 0) AS null_handling_error_count,
  COALESCE(e.other_error_count, 0) AS other_error_count,
  COALESCE(e.distinct_error_code_count, 0) AS distinct_error_code_count,
  COALESCE(e.affected_column_count, 0) AS affected_column_count,
  a.max_data_skewness,
  a.max_time_skewness,
  COALESCE(a.external_spill_blocks, 0) AS external_spill_blocks,
  a.observed_file_format
FROM catalog c
LEFT JOIN column_stats cs ON cs.external_table_key = c.external_table_key
LEFT JOIN partition_stats ps ON ps.external_table_key = c.external_table_key
LEFT JOIN activity a ON a.external_table_key = c.external_table_key
LEFT JOIN errors e ON e.external_table_key = c.external_table_key
ORDER BY gross_scan_bytes DESC, c.external_table_key
"""


# Redshift cannot reliably join all of the SYS and SVV external-table views in
# one statement because some catalog views are leader-node only.  These staging
# statements deliberately touch exactly one source view apiece.  The joins are
# performed later inside local DuckDB by assemble_external_table_info().
# Isolated, producer-only external-table catalog sourced from
# SVV_EXTERNAL_COLUMNS, cycled per database. One row per external table with a
# single derived attribute: sortkey = the column_name where part_key = 1 (the
# primary partition key). Tables with no partition get a NULL sortkey. Grouping
# collapses the per-column rows to one per table so unpartitioned tables still
# appear.
EXTERNAL_TABLES_CATALOG_SQL = r"""
SELECT
  LOWER(TRIM(redshift_database_name) || '.' || TRIM(schemaname) || '.' || TRIM(tablename)) AS external_table_key,
  TRIM(redshift_database_name) AS redshift_database_name,
  TRIM(schemaname) AS schema_name,
  TRIM(tablename) AS table_name,
  MAX(CASE WHEN part_key = 1 THEN TRIM(columnname) END) AS sortkey
FROM svv_external_columns
GROUP BY 1, 2, 3, 4
"""


EXTERNAL_TABLE_METADATA_SQL = r"""
SELECT
  LOWER(TRIM(redshift_database_name) || '.' || TRIM(schemaname) || '.' || TRIM(tablename)) AS external_table_key,
  TRIM(redshift_database_name) AS redshift_database_name,
  TRIM(schemaname) AS schema_name,
  TRIM(tablename) AS table_name,
  TRIM(columnname) AS column_name,
  TRIM(external_type) AS data_type,
  columnnum AS column_number,
  COALESCE(part_key, 0) AS partition_key_ordinal,
  TRIM(is_nullable) AS is_nullable
FROM svv_external_columns
"""


def _sql_literal(value: str) -> str:
    """Single-quote a value for inline SQL, doubling any embedded quote."""
    return "'" + str(value).replace("'", "''") + "'"


def _glob_to_like(pattern: str) -> str:
    """Translate a shell-style glob to a SQL LIKE pattern.

    Table names are full of underscores (``fact_orders``), and ``_`` is a SQL
    wildcard - so a naive ``*`` -> ``%`` swap leaves every underscore matching
    any character, and ``fact_*`` would also match ``factX_orders``. Underscores
    and percent signs the user typed are escaped so they mean themselves; only
    ``*`` and ``?`` are treated as wildcards.
    """
    out: list[str] = []
    for char in str(pattern):
        if char == "*":
            out.append("%")
        elif char == "?":
            out.append("_")
        elif char in {"%", "_", "\\"}:
            out.append("\\" + char)
        else:
            out.append(char)
    return "".join(out)


def external_capture_predicate(
    schemas: Iterable[str] = (),
    table_patterns: Iterable[str] = (),
) -> str:
    """Build the WHERE clause limiting which external tables are captured.

    SVV_EXTERNAL_COLUMNS is ONE ROW PER COLUMN. A catalog with millions of
    external tables is tens of millions of rows, and capturing it unfiltered is
    what makes the load unusable - so this predicate has to run on Redshift, not
    on the fetched frame. Anything filtered locally has already been paid for.

    ``schemas`` matches schema names exactly (case-insensitively).
    ``table_patterns`` accepts shell-style globs (``fact_*``, ``dim_?``); ``*``
    and ``?`` are the only wildcards, and a literal ``_`` or ``%`` in the
    pattern is escaped so it means itself rather than a SQL wildcard.

    Both are ANDed when both are given: the schema must match AND the table name
    must match. Returns "" when neither is supplied - callers decide whether an
    empty filter is allowed, because "capture everything" is exactly the case
    that has to be opted into deliberately at this scale.
    """
    clauses: list[str] = []

    cleaned_schemas = [str(item).strip().lower() for item in schemas if str(item).strip()]
    if cleaned_schemas:
        joined = ", ".join(_sql_literal(item) for item in sorted(set(cleaned_schemas)))
        clauses.append(f"LOWER(TRIM(schemaname)) IN ({joined})")

    cleaned_patterns = [str(item).strip().lower() for item in table_patterns if str(item).strip()]
    if cleaned_patterns:
        likes: list[str] = []
        for pattern in sorted(set(cleaned_patterns)):
            likes.append(
                "LOWER(TRIM(tablename)) LIKE "
                f"{_sql_literal(_glob_to_like(pattern))} ESCAPE '\\'"
            )
        clauses.append("(" + " OR ".join(likes) + ")")

    return " AND ".join(clauses)


def external_metadata_sql(
    schemas: Iterable[str] = (),
    table_patterns: Iterable[str] = (),
) -> str:
    """EXTERNAL_TABLE_METADATA_SQL narrowed to the configured schemas/patterns."""
    predicate = external_capture_predicate(schemas, table_patterns)
    if not predicate:
        return EXTERNAL_TABLE_METADATA_SQL
    return f"{EXTERNAL_TABLE_METADATA_SQL.rstrip()}\nWHERE {predicate}\n"


def external_catalog_sql(
    schemas: Iterable[str] = (),
    table_patterns: Iterable[str] = (),
) -> str:
    """EXTERNAL_TABLES_CATALOG_SQL narrowed to the configured schemas/patterns.

    The catalog statement aggregates, so the predicate has to land before the
    GROUP BY - filtering the grouped result would still scan everything.
    """
    predicate = external_capture_predicate(schemas, table_patterns)
    if not predicate:
        return EXTERNAL_TABLES_CATALOG_SQL
    head, _, tail = EXTERNAL_TABLES_CATALOG_SQL.partition("GROUP BY")
    return f"{head.rstrip()}\nWHERE {predicate}\nGROUP BY{tail}"


EXTERNAL_CATALOG_STAGE_SQL = r"""
SELECT
  TRIM(redshift_database_name) AS redshift_database_name,
  TRIM(schemaname) AS schemaname,
  TRIM(tablename) AS tablename,
  TRIM(tabletype) AS tabletype,
  TRIM(location) AS location,
  TRIM(input_format) AS input_format,
  TRIM(output_format) AS output_format,
  TRIM(serialization_lib) AS serialization_lib,
  serde_parameters,
  compressed,
  parameters
FROM svv_external_tables
WHERE LOWER(TRIM(COALESCE(tabletype, 'TABLE'))) IN ('table', '')
"""


EXTERNAL_COLUMN_STATS_STAGE_SQL = r"""
SELECT
  LOWER(TRIM(redshift_database_name) || '.' || TRIM(schemaname) || '.' || TRIM(tablename)) AS external_table_key,
  TRIM(columnname) AS columnname,
  COALESCE(part_key, 0) AS part_key
FROM svv_external_columns
"""


def external_segments_stage_sql(days: float = 7.0, hours: int | None = None) -> str:
    """Return a bounded one-view Spectrum scan extraction."""
    lookback_days = max(1, int(round(float(days or 7.0))))
    window = (
        f"DATEADD(hour, -{max(1, int(hours))}, GETDATE())"
        if hours is not None
        else f"DATEADD(day, -{lookback_days}, GETDATE())"
    )
    return f"""
SELECT
  user_id,
  query_id,
  transaction_id,
  child_query_sequence,
  segment_id,
  source_type,
  MIN(start_time) AS start_time,
  MAX(end_time) AS end_time,
  SUM(duration) AS duration,
  SUM(total_partitions) AS total_partitions,
  SUM(qualified_partitions) AS qualified_partitions,
  SUM(scanned_files) AS scanned_files,
  SUM(returned_rows) AS returned_rows,
  SUM(returned_bytes) AS returned_bytes,
  MAX(file_format) AS file_format,
  MAX(file_location) AS file_location,
  MAX(external_query_text) AS external_query_text,
  MAX(NULLIF(TRIM(warning_message), '')) AS warning_message,
  TRIM(table_name) AS table_name,
  MAX(is_recursive) AS is_recursive,
  MAX(is_nested) AS is_nested,
  SUM(s3list_time) AS s3list_time,
  SUM(get_partition_time) AS get_partition_time
FROM sys_external_query_detail
WHERE LOWER(TRIM(source_type)) = 's3'
  AND start_time >= {window}
  AND NULLIF(TRIM(table_name), '') IS NOT NULL
GROUP BY user_id, query_id, transaction_id, child_query_sequence, segment_id,
         source_type, TRIM(table_name)
"""


def _external_id_predicate(query_ids) -> str:
    ids = tuple(dict.fromkeys(int(value) for value in query_ids))
    if not ids:
        raise ValueError("At least one external query id is required")
    return ", ".join(str(value) for value in ids)


def external_steps_stage_sql(query_ids) -> str:
    """Return a single-view step extraction for known external query IDs."""
    ids = _external_id_predicate(query_ids)
    return f"""
SELECT
  query_id, segment_id, table_name, output_bytes, output_rows,
  data_skewness, time_skewness,
  spilled_block_local_disk, spilled_block_remote_disk,
  step_id, step_name, source
FROM sys_query_detail
WHERE query_id IN ({ids})
  AND step_id >= 0
  AND LOWER(TRIM(step_name)) = 'scan'
  AND LOWER(TRIM(source)) IN ('s3', 'external', 'spectrum')
"""


def external_errors_stage_sql(query_ids) -> str:
    """Return sampled external errors without joining another Redshift view."""
    ids = _external_id_predicate(query_ids)
    return f"""
SELECT query_id, file_location, rowid, column_name, trigger, action, error_code
FROM sys_external_query_error
WHERE query_id IN ({ids})
"""


def external_query_ids(frame) -> list[int]:
    """Return unique numeric query IDs from a staged frame."""
    if frame is None or frame.empty:
        return []
    column = next(
        (name for name in frame.columns if str(name).strip().lower() == "query_id"),
        None,
    )
    if column is None:
        return []
    values = []
    for value in frame[column].dropna().tolist():
        try:
            values.append(int(value))
        except (TypeError, ValueError):
            continue
    return list(dict.fromkeys(values))


def minimal_external_catalog_from_segments(segments, database: str):
    """Derive active catalog rows when the optional SVV catalog stage times out."""
    import pandas as pd

    if segments is None or segments.empty or "table_name" not in segments.columns:
        return pd.DataFrame()
    lookup = {str(column).strip().lower(): column for column in segments.columns}
    table_column = lookup.get("table_name")
    location_column = lookup.get("file_location")
    rows = []
    for record in segments.to_dict("records"):
        raw = str(record.get(table_column) or "").strip()
        if not raw:
            continue
        parts = [part.strip(' "') for part in raw.split(".") if part.strip(' "')]
        if len(parts) >= 3:
            source_db, schema_name, table_name = parts[-3:]
        elif len(parts) == 2:
            source_db, schema_name, table_name = database, parts[0], parts[1]
        else:
            source_db, schema_name, table_name = database, "", parts[0]
        rows.append({
            "redshift_database_name": source_db,
            "schemaname": schema_name,
            "tablename": table_name,
            "tabletype": "TABLE",
            "location": str(record.get(location_column) or "") if location_column else "",
            "input_format": "", "output_format": "", "serialization_lib": "",
            "serde_parameters": "", "compressed": None, "parameters": "",
        })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).drop_duplicates(
        ["redshift_database_name", "schemaname", "tablename"], keep="first"
    )


_EXTERNAL_LOCAL_STAGE_SCHEMAS = {
    "svv_external_tables": (
        ("redshift_database_name", "VARCHAR"), ("schemaname", "VARCHAR"),
        ("tablename", "VARCHAR"), ("tabletype", "VARCHAR"), ("location", "VARCHAR"),
        ("input_format", "VARCHAR"), ("output_format", "VARCHAR"),
        ("serialization_lib", "VARCHAR"), ("serde_parameters", "VARCHAR"),
        ("compressed", "BIGINT"), ("parameters", "VARCHAR"),
    ),
    "external_column_stats": (
        ("external_table_key", "VARCHAR"), ("columnname", "VARCHAR"),
        ("part_key", "BIGINT"),
    ),
    "sys_query_history": (("query_id", "BIGINT"), ("database_name", "VARCHAR")),
    "sys_external_query_detail": (
        ("user_id", "BIGINT"), ("query_id", "BIGINT"), ("transaction_id", "BIGINT"),
        ("child_query_sequence", "BIGINT"), ("segment_id", "BIGINT"),
        ("source_type", "VARCHAR"), ("start_time", "TIMESTAMP"), ("end_time", "TIMESTAMP"),
        ("duration", "BIGINT"), ("total_partitions", "BIGINT"),
        ("qualified_partitions", "BIGINT"), ("scanned_files", "BIGINT"),
        ("returned_rows", "BIGINT"), ("returned_bytes", "BIGINT"),
        ("file_format", "VARCHAR"), ("file_location", "VARCHAR"),
        ("external_query_text", "VARCHAR"), ("warning_message", "VARCHAR"),
        ("table_name", "VARCHAR"), ("is_recursive", "VARCHAR"), ("is_nested", "VARCHAR"),
        ("s3list_time", "BIGINT"), ("get_partition_time", "BIGINT"),
    ),
    "sys_query_detail": (
        ("query_id", "BIGINT"), ("segment_id", "BIGINT"), ("table_name", "VARCHAR"),
        ("output_bytes", "BIGINT"), ("output_rows", "BIGINT"),
        ("data_skewness", "BIGINT"), ("time_skewness", "BIGINT"),
        ("spilled_block_local_disk", "BIGINT"), ("spilled_block_remote_disk", "BIGINT"),
        ("step_id", "BIGINT"), ("step_name", "VARCHAR"), ("source", "VARCHAR"),
    ),
    "sys_external_query_error": (
        ("query_id", "BIGINT"), ("file_location", "VARCHAR"), ("rowid", "VARCHAR"),
        ("column_name", "VARCHAR"), ("trigger", "VARCHAR"), ("action", "VARCHAR"),
        ("error_code", "BIGINT"),
    ),
}


def assemble_external_table_info(stage_frames) -> object:
    """Join independently captured external metadata inside local DuckDB."""
    import duckdb
    import pandas as pd

    con = duckdb.connect(":memory:")
    try:
        for table_name, schema in _EXTERNAL_LOCAL_STAGE_SCHEMAS.items():
            definitions = ", ".join(f'"{name}" {kind}' for name, kind in schema)
            con.execute(f'CREATE TABLE "{table_name}" ({definitions})')
            incoming = stage_frames.get(table_name)
            if incoming is None or incoming.empty:
                continue
            columns = [name for name, _ in schema]
            lookup = {str(column).strip().lower(): column for column in incoming.columns}
            normalized = pd.DataFrame({
                name: incoming[lookup[name]] if name in lookup else None
                for name in columns
            }, index=incoming.index)
            con.register("incoming_external_stage", normalized)
            quoted = ", ".join(f'"{name}"' for name in columns)
            con.execute(
                f'INSERT INTO "{table_name}" ({quoted}) '
                f'SELECT {quoted} FROM incoming_external_stage'
            )
            con.unregister("incoming_external_stage")

        column_cte = r"""column_stats AS (
  SELECT
    LOWER(TRIM(redshift_database_name) || '.' || TRIM(schemaname) || '.' || TRIM(tablename)) AS external_table_key,
    COUNT(*) AS column_count,
    SUM(CASE WHEN COALESCE(part_key, 0) > 0 THEN 1 ELSE 0 END) AS partition_key_count
  FROM svv_external_columns
  GROUP BY 1
),"""
        staged_column_cte = r"""column_stats AS (
  SELECT external_table_key, COUNT(*) AS column_count,
         SUM(CASE WHEN COALESCE(part_key, 0) > 0 THEN 1 ELSE 0 END) AS partition_key_count
  FROM external_column_stats
  GROUP BY 1
),"""
        local_sql = EXTERNAL_TABLE_INFO_SQL.replace(column_cte, staged_column_cte)
        result = con.execute(local_sql).fetchdf()
        source_columns = stage_frames.get("external_column_stats")
        result["partition_key_columns"] = ""
        if source_columns is not None and not source_columns.empty:
            lookup = {str(column).strip().lower(): column for column in source_columns.columns}
            required = ["external_table_key", "columnname", "part_key"]
            if set(required).issubset(lookup):
                keys = source_columns[[lookup[name] for name in required]].copy()
                keys.columns = ["external_table_key", "columnname", "part_key"]
                keys["part_key"] = pd.to_numeric(keys["part_key"], errors="coerce").fillna(0)
                keys = keys[keys["part_key"] > 0].sort_values(
                    ["external_table_key", "part_key"], kind="stable"
                )
                if not keys.empty:
                    names = (
                        keys.groupby("external_table_key", as_index=False)["columnname"]
                        .agg(lambda values: ", ".join(str(value) for value in values if str(value).strip()))
                        .rename(columns={"columnname": "partition_key_columns"})
                    )
                    result = result.drop(columns=["partition_key_columns"]).merge(
                        names, on="external_table_key", how="left"
                    )
                    result["partition_key_columns"] = result["partition_key_columns"].fillna("")
        return result
    finally:
        con.close()


def external_table_summary_sql(days: float = 7.0) -> str:
    """Bounded, summary-only variant for the focused demo-safe loader.

    Runtime partition pruning comes from SYS_EXTERNAL_QUERY_DETAIL.
    """
    lookback_days = max(1, int(round(float(days or 7.0))))
    sql = EXTERNAL_TABLE_INFO_SQL
    sql = sql.replace(
        "WHERE d.step_id >= 0",
        "WHERE d.query_id IN (\n"
        "    SELECT query_id FROM sys_external_query_detail\n"
        f"    WHERE LOWER(TRIM(source_type)) = 's3' AND start_time >= DATEADD(day, -{lookback_days}, GETDATE())\n"
        "  )\n  AND d.step_id >= 0",
        1,
    )
    sql = sql.replace(
        "WHERE LOWER(TRIM(e.source_type)) = 's3'",
        "WHERE LOWER(TRIM(e.source_type)) = 's3'\n"
        f"    AND e.start_time >= DATEADD(day, -{lookback_days}, GETDATE())",
        1,
    )
    sql = sql.replace(
        "JOIN sys_query_history h ON h.query_id = er.query_id",
        "JOIN sys_query_history h ON h.query_id = er.query_id\n"
        f"   AND h.start_time >= DATEADD(day, -{lookback_days}, GETDATE())",
        1,
    )
    return sql


VIEW_DEFINITIONS_SQL = """
SELECT
  CURRENT_DATABASE()::VARCHAR AS "database",
  schemaname::VARCHAR AS "schema",
  viewname::VARCHAR AS view_name,
  SUBSTRING(definition, 1, 65535) AS definition_part_01,
  SUBSTRING(definition, 65536, 65535) AS definition_part_02,
  SUBSTRING(definition, 131071, 65535) AS definition_part_03,
  SUBSTRING(definition, 196606, 65535) AS definition_part_04,
  SUBSTRING(definition, 262141, 65535) AS definition_part_05,
  SUBSTRING(definition, 327676, 65535) AS definition_part_06,
  SUBSTRING(definition, 393211, 65535) AS definition_part_07,
  SUBSTRING(definition, 458746, 65535) AS definition_part_08,
  SUBSTRING(definition, 524281, 65535) AS definition_part_09,
  SUBSTRING(definition, 589816, 65535) AS definition_part_10,
  SUBSTRING(definition, 655351, 65535) AS definition_part_11,
  SUBSTRING(definition, 720886, 65535) AS definition_part_12
FROM pg_views
WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'admin')
  AND viewname NOT LIKE 'auto_m%'
  AND LEN(COALESCE(definition, '')) > 0
ORDER BY schemaname DESC, viewname
"""


PROCEDURE_DEFINITIONS_SQL = f"""
SELECT
  CURRENT_DATABASE()::VARCHAR AS "database",
  n.nspname::VARCHAR AS "schema",
  p.proname::VARCHAR AS procedure_name,
  p.prooid::VARCHAR AS procedure_oid,
  LOWER(CURRENT_DATABASE()::VARCHAR || '.' || n.nspname::VARCHAR || '.' || p.proname::VARCHAR) AS procedure_key,
  p.proowner::VARCHAR AS owner_id,
  COALESCE(u.user_name::VARCHAR, '') AS owner_name,
  COALESCE(textin(array_out(p.proargnames)), '') AS argument_names,
  COALESCE(textin(array_out(p.proargmodes)), '') AS argument_modes,
  COALESCE(textin(oidvectorout(p.proargtypes)), '') AS argument_type_oids,
  COALESCE(textin(array_out(p.proallargtypes)), '') AS all_argument_type_oids,
  LEN(COALESCE(p.prosrc, ''))::BIGINT AS source_length,
{definition_chunk_columns("p.prosrc")}
FROM pg_catalog.pg_proc_info p
JOIN pg_catalog.pg_namespace n
  ON n.oid = p.pronamespace
LEFT JOIN svv_user_info u
  ON u.user_id = p.proowner
WHERE p.prokind = 'p'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg_%'
ORDER BY n.nspname, p.proname, textin(oidvectorout(p.proargtypes))
"""


# Dict order IS the capture/refresh order. query_history anchors the analysis
# (evidence tables LEFT-join onto it), query_text feeds repeat grouping,
# child_query_text preserves optimizer rewrites, the evidence tables follow,
# and the per-database catalog tables run last
# (appended in ingest_redshift.LIVE_REFRESH_TABLES). If a run fails partway,
# earlier tables degrade more gracefully than later ones.
EXTRACTIONS = {
    "query_history": query_history_sql,
    "query_text": query_text_sql,
    "child_query_text": child_query_text_sql,
    "query_history_all": query_history_all_sql,
    "query_details": query_details_sql,
    "query_health": query_health_sql,
    "query_explain": query_explain_sql,
    "query_detail_flow": query_detail_flow_sql,
    "table_scan_info": table_scan_info_sql,
    "user_info": user_info_sql,
}


SOURCE_REQUIREMENTS = {
    # Columns below are taken from the extraction SQL, not from memory. The
    # ingestion preflight asks Redshift for SELECT * WHERE 1=0 and compares the
    # actual cursor metadata before running any expensive query.
    "sys_query_detail": {
        "query_id",
        "spilled_block_local_disk",
        "spilled_block_remote_disk",
        "input_bytes",
        "output_bytes",
        "blocks_read",
        "blocks_write",
        "local_read_io",
        "remote_read_io",
        "input_rows",
        "output_rows",
        "duration",
        "data_skewness",
        "time_skewness",
        "segment_id",
        "stream_id",
        "step_name",
        "table_id",
        "alert",
        "source",
        "table_name",
        "is_rrscan",
        "user_id",
        "child_query_sequence",
        "metrics_level",
        "step_id",
        "step_attribute",
        "plan_node_id",
        "plan_parent_id",
        "start_time",
        "end_time",
    },
    "sys_query_history": {
        "query_id",
        "elapsed_time",
        "execution_time",
        "database_name",
        "status",
        "result_cache_hit",
        "query_type",
        "query_text",
    },
    "sys_query_explain": {
        "userid",
        "query_id",
        "child_query_sequence",
        "plan_node_id",
        "plan_parent_id",
        "plan_node",
        "plan_info",
    },
    "sys_query_text": {"query_id"},
    "sys_child_query_text": {
        "user_id",
        "query_id",
        "child_query_sequence",
        "sequence",
        "text",
    },
    "svv_user_info": {"user_id", "user_name"},
    "sys_external_query_detail": {
        "user_id", "query_id", "transaction_id", "child_query_sequence", "segment_id",
        "source_type", "start_time", "end_time", "duration", "total_partitions",
        "qualified_partitions", "scanned_files", "returned_rows", "returned_bytes",
        "file_format", "file_location", "external_query_text", "warning_message",
        "table_name", "is_recursive", "is_nested", "s3list_time", "get_partition_time",
    },
    "sys_external_query_error": {
        "user_id", "query_id", "file_location", "rowid", "column_name", "original_value",
        "modified_value", "trigger", "action", "action_value", "error_code",
    },
    "svv_external_tables": {
        "redshift_database_name", "schemaname", "tablename", "tabletype", "location",
        "input_format", "output_format", "serialization_lib", "serde_parameters",
        "compressed", "parameters",
    },
    "svv_external_columns": {
        "redshift_database_name", "schemaname", "tablename", "columnname", "external_type",
        "columnnum", "part_key", "is_nullable",
    },
    "pg_catalog.pg_proc_info": {
        "prooid",
        "prokind",
        "pronamespace",
        "proname",
        "proowner",
        "prosrc",
        "proargnames",
        "proargmodes",
        "proargtypes",
        "proallargtypes",
    },
    # Redshift OID columns are internal join columns and may not appear in
    # JDBC/ODBC metadata enumeration even though SQL can use them for catalog
    # joins. Validate only the visible namespace name here.
    "pg_catalog.pg_namespace": {"nspname"},
    "SVV_TABLE_INFO": {"table_id", "schema", "table"},
    "pg_views": {"schemaname", "viewname", "definition"},
}
