#!/usr/bin/env python3
"""Standalone zero-downtime refresh of the Redshift analyzer DuckDB tables.

Copy this ONE file anywhere (any folder, any machine) and run it with Python.
It has no dependency on the analyzer source tree; the capture SQL below is
bundled verbatim from the analyzer so the loaded data matches a normal
in-app capture. Requires only:  pip install duckdb pandas redshift-connector

What it does
------------
1. LOAD (default):  pulls a window of data from the cluster - 7 days of
   sys_query_history executions at or above the 10-minute floor - into
   duplicate tables named `<table>_tmp` (query_history_tmp, ...). The
   analyzer app can stay open: all slow cluster work happens with the DuckDB
   file CLOSED, which is only touched in short write bursts (with retry).
2. SWAP (--swap):   renames every `<table>_tmp` over its production name in
   one transaction, registers the snapshot, and rebuilds the analyzer's
   performance indexes. Takes seconds. A file backup is written first.

Usage
-----
    python runner.py              # load 7 days of >= 600s queries into *_tmp
    python runner.py --status     # progress: row counts per *_tmp table
    python runner.py --resume     # continue an interrupted load
    python runner.py --backup-only # checkpoint + preserve the DuckDB; change no tables
    python runner.py --swap       # promote *_tmp -> production tables

Non-secret settings come from environment variables or a .env file next to
this script. When invoked by DataBasix, connection secrets are supplied from
the already-unlocked encrypted .secrets session:

    REDSHIFT_HOST=my-cluster.abc123.us-east-1.redshift.amazonaws.com
    REDSHIFT_PORT=5439                  (optional, default 5439)
    REDSHIFT_DATABASE=dev               (or REDSHIFT_PRIMARY_DATABASE)
    REDSHIFT_USER=analyzer_user
    REDSHIFT_PASSWORD=...               (optional; prompts if missing)
    REDSHIFT_DUCKDB_PATH=...            (optional; defaults to the analyzer's file)
    Database-cycled catalog datasets currently use only enterprise_datawarehouse
    for every enabled cluster under the emergency operating policy.

Multi-cluster profiles (one producer plus any configured consumers):

    REDSHIFT_NAMESPACE=namespace-guid
    REDSHIFT_ENABLED=true
    REDSHIFT_FRIENDLY=Business Producer
    REDSHIFT_PRODUCER_HOST=producer.endpoint
    REDSHIFT_PRODUCER_DATABASE=dev
    REDSHIFT_PRODUCER_USER=producer_user
    REDSHIFT_PRODUCER_PASSWORD=...

    REDSHIFT_CONSUMER_1_NAMESPACE_ID=namespace-guid
    REDSHIFT_CONSUMER_1_ENABLED=true
    REDSHIFT_CONSUMER_1_HOST=consumer.endpoint
    REDSHIFT_CONSUMER_1_DATABASE=dev
    REDSHIFT_CONSUMER_1_USER=consumer_user
    REDSHIFT_CONSUMER_1_PASSWORD=...

Repeat CONSUMER_2 and higher as needed. Set a profile's ENABLED value to
false to keep its configuration while skipping that cluster.
Legacy REDSHIFT_HOST/USER/etc. remain producer fallbacks.

Note: parent-pattern selection here uses a literal-insensitive text
fingerprint instead of the app's sqlglot canonicalizer, so the representative
parent per repeated pattern can occasionally differ from an in-app capture.
The captured evidence semantics are otherwise identical.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import getpass
import hashlib
import json
import os
import re
import shutil
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

MIN_PYTHON = (3, 9)

try:
    import duckdb
    import pandas as pd
except ImportError as _exc:  # sensed in main() with friendly instructions
    duckdb = None
    pd = None
    _IMPORT_ERROR: Exception | None = _exc
else:
    _IMPORT_ERROR = None

DEFAULT_DAYS = 7.0
DEFAULT_FLOOR_SECONDS = 600.0  # 10 minutes
DEFAULT_REDSHIFT_PORT = 5439
DEFAULT_LOCK_WAIT_SECONDS = 600
FETCH_BATCH_ROWS = 25000
STATE_TABLE = "_tmp_refresh_state"
SQL_STASH_TABLE = "_tmp_refresh_sql"
NAMESPACE_STATE_TABLE = "_tmp_namespace_refresh_state"
ACTIVE_PROFILE_PREFIXES_ENV = "INFRAREDSHIFT_ACTIVE_PROFILE_PREFIXES"
_PROGRESS_HOOK = None


def set_progress_hook(hook) -> None:
    """Install an optional in-application multi-cluster progress callback."""
    global _PROGRESS_HOOK
    _PROGRESS_HOOK = hook


def emit_progress(namespace_id: str, table_name: str, source_rows: int, duckdb_rows: int, completed: int, total: int, status: str) -> None:
    hook = _PROGRESS_HOOK
    if hook is not None:
        hook(namespace_id, table_name, int(source_rows), int(duckdb_rows), int(completed), int(total), status)


def _load_dotenv_if_present() -> None:
    """Load non-secret identity plus the authenticated in-app secret session."""
    folders: list[Path] = []
    launch_dir = str(os.environ.get("REDSHIFT_ANALYZER_LAUNCH_DIR") or "").strip()
    for folder in (
        Path(launch_dir).expanduser() if launch_dir else None,
        Path.cwd(),
        Path(__file__).resolve().parent,
    ):
        if folder is not None and folder not in folders:
            folders.append(folder)
    for folder in folders:
        env_path = folder / ".env"
        if not env_path.is_file():
            continue
        try:
            lines = env_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        _apply_env_lines(lines)
        break
    portable_candidates: list[Path] = []
    explicit_profile = str(
        os.environ.get("REDSHIFT_ANALYZER_PROFILE_PATH") or ""
    ).strip()
    if explicit_profile:
        portable_candidates.append(Path(explicit_profile).expanduser())
    portable_candidates.extend(
        folder / "redshift_cluster_profiles.json" for folder in folders
    )
    for portable_path in dict.fromkeys(portable_candidates):
        if not portable_path.is_file():
            continue
        try:
            payload = json.loads(portable_path.read_text(encoding="utf-8"))
            if payload.get("format") != "redshift-query-anatomy-cluster-profiles":
                raise ValueError("unrecognized portable profile format")
            allowed = {
                "enabled", "display_name", "namespace_id", "port",
                "primary_database", "floor_seconds",
                # External (Spectrum) capture scope. SVV_EXTERNAL_COLUMNS is one
                # row per column, so an unfiltered catalog is tens of millions
                # of rows; these keys keep the capture to the schemas and table
                # names that are actually analyzed.
                "external_schemas", "external_table_patterns",
            }
            active_prefixes: list[str] = []
            for profile in payload.get("profiles") or []:
                prefix = str(profile.get("profile") or "").strip().upper()
                if prefix != "REDSHIFT_PRODUCER" and not re.fullmatch(r"REDSHIFT_CONSUMER_\d+", prefix):
                    continue
                if prefix not in active_prefixes:
                    active_prefixes.append(prefix)
                for field in allowed:
                    if field in profile and profile[field] is not None:
                        # Portable identity is authoritative. Credentials stay
                        # in the protected local secret session.
                        os.environ[f"{prefix}_{field.upper()}"] = str(profile[field])
            os.environ[ACTIVE_PROFILE_PREFIXES_ENV] = ",".join(active_prefixes)
            print(f"Loaded portable non-secret cluster profiles from {portable_path}")
        except Exception as exc:
            raise SystemExit(f"Could not read {portable_path}: {exc}") from exc
        break
    try:
        from analyzer.secrets_store import (
            session_secrets,
            unlock_scheduled_secrets_session,
        )

        # A direct runner.py launch starts in a fresh process, so the in-memory
        # credential session is normally empty even after Local Credentials
        # were saved in the app. Unlock the same-Windows-user DPAPI file here;
        # keep decrypted values in the protected session rather than exporting
        # them to child-process-visible environment variables.
        if not session_secrets():
            try:
                unlock_scheduled_secrets_session()
            except FileNotFoundError:
                # Legacy standalone deployments may intentionally provide
                # credentials through their already-established environment.
                pass
    except ImportError:
        # Standalone runner copies without the analyzer package still support
        # ordinary process environment variables.
        pass


def _apply_env_lines(lines) -> None:
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def default_duckdb_path() -> Path:
    """Mirror analyzer.duckdb_store.default_duckdb_path without importing it."""
    base = os.environ.get("REDSHIFT_ANALYZER_HOME")
    if base:
        return Path(base) / "redshift.duckdb"
    return Path.home() / "RQP" / "data" / "redshift.duckdb"


def resolve_default_duckdb_path() -> Path:
    """Find the analyzer's DuckDB file without configuration.

    Search order: the current directory, the folder this script sits in, then
    the analyzer's own data folder. The app renames its file to
    redshift.<cluster>.<endpoint-key>.duckdb once a source cluster is
    configured, so any *.duckdb file counts as a candidate. Auto-pick when a
    folder holds exactly one; list the choices when there are several."""
    searched = []
    script_dir = Path(__file__).resolve().parent
    for folder in dict.fromkeys((Path.cwd(), script_dir, default_duckdb_path().parent)):
        if not folder.is_dir():
            continue
        candidates = sorted(folder.glob("*.duckdb"))
        if len(candidates) == 1:
            print(f"Using DuckDB file found in {folder}: {candidates[0].name}\n")
            return candidates[0]
        if len(candidates) > 1:
            print(f"Multiple DuckDB files exist in {folder} - pass --duckdb-path (or set REDSHIFT_DUCKDB_PATH) to pick one:")
            for candidate in candidates:
                print(f"  {candidate.name}")
            print()
            return default_duckdb_path()
        searched.append(str(folder))
    if searched:
        print("No *.duckdb file found in: " + "; ".join(searched))
    return default_duckdb_path()


# =============================================================================
# SECTION 1 - capture SQL, bundled VERBATIM from analyzer/redshift_queries.py.
# Regenerate with tools/make_runner.py after changing the analyzer's SQL.
# =============================================================================

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


# --- External capture scope -------------------------------------------------
# SVV_EXTERNAL_COLUMNS is ONE ROW PER COLUMN. A catalog with millions of
# external tables is tens of millions of rows, and pulling it unfiltered is what
# makes the load unusable - so the restriction has to run on Redshift, not on
# the fetched frame. Configured per profile in redshift_cluster_profiles.json:
#
#     "external_schemas": "spectrum, raw",
#     "external_table_patterns": "fact_*, dim_*"
#
# runner.py keeps its own copy of these helpers so it stays standalone-
# importable, matching EXTERNAL_CAPTURE_ENABLED.


def _external_sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _external_glob_to_like(pattern: str) -> str:
    """Translate a shell glob to SQL LIKE, escaping literal _ and %.

    Table names are full of underscores and ``_`` is a SQL wildcard, so a naive
    ``*``->``%`` swap would let ``fact_*`` also match ``factX_orders``.
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


def _external_split_list(value: str) -> tuple[str, ...]:
    return tuple(
        item.strip()
        for item in re.split(r"[,;\s]+", str(value or ""))
        if item.strip()
    )


def external_capture_scope(prefix: str = "REDSHIFT_PRODUCER") -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Configured (schemas, table_patterns) limiting external capture."""
    return (
        _external_split_list(os.environ.get(f"{prefix}_EXTERNAL_SCHEMAS", "")),
        _external_split_list(os.environ.get(f"{prefix}_EXTERNAL_TABLE_PATTERNS", "")),
    )


def external_capture_predicate(schemas=(), table_patterns=()) -> str:
    """WHERE clause limiting which external tables are captured."""
    clauses: list[str] = []
    cleaned_schemas = [str(i).strip().lower() for i in schemas if str(i).strip()]
    if cleaned_schemas:
        joined = ", ".join(_external_sql_literal(i) for i in sorted(set(cleaned_schemas)))
        clauses.append(f"LOWER(TRIM(schemaname)) IN ({joined})")
    cleaned_patterns = [str(i).strip().lower() for i in table_patterns if str(i).strip()]
    if cleaned_patterns:
        likes = [
            "LOWER(TRIM(tablename)) LIKE "
            f"{_external_sql_literal(_external_glob_to_like(p))} ESCAPE '\\'"
            for p in sorted(set(cleaned_patterns))
        ]
        clauses.append("(" + " OR ".join(likes) + ")")
    return " AND ".join(clauses)


def external_metadata_sql(schemas=(), table_patterns=()) -> str:
    predicate = external_capture_predicate(schemas, table_patterns)
    if not predicate:
        return EXTERNAL_TABLE_METADATA_SQL
    return f"{EXTERNAL_TABLE_METADATA_SQL.rstrip()}\nWHERE {predicate}\n"


def external_catalog_sql(schemas=(), table_patterns=()) -> str:
    """Predicate must land before the GROUP BY, or it scans everything anyway."""
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


# =============================================================================
# SECTION 2 - table schemas and performance indexes, bundled from
# analyzer/duckdb_store.py. Regenerate with tools/make_runner.py.
# =============================================================================

EXPECTED_COLUMNS = {'query_details': ('namespace_id',
                   'query_id',
                   'total_spill',
                   'input_bytes',
                   'output_bytes',
                   'blocks_read',
                   'blocks_write',
                   'local_read_io',
                   'remote_read_io',
                   'input_rows',
                   'output_rows',
                   'selectivity_ratio',
                   'total_step_duration',
                   'max_step_duration',
                   'avg_step_duration',
                   'total_steps',
                   'max_data_skewness',
                   'max_time_skewness',
                   'segments_used',
                   'streams_used',
                   'scan_steps',
                   'join_steps',
                   'sort_steps',
                   'agg_steps',
                   'tables_touched',
                   'alert_count',
                   'external_steps',
                   's3_steps',
                   'external_input_bytes',
                   'external_input_rows',
                   'external_duration',
                   'external_duration_pct',
                   'remote_io_ratio',
                   'external_selectivity',
                   'external_spill_blocks',
                   'external_tables_touched',
                   'external_data_skew'),
 'query_health': ('namespace_id',
                  'query_id',
                  'seq_scan_cnt',
                  's3_scan_cnt',
                  'partition_loop_cnt',
                  'dist_both_cnt',
                  'bcast_cnt',
                  'dist_total_cnt',
                  'has_nested_loop',
                  'hash_join_cnt',
                  'subquery_cnt',
                  'network_cnt',
                  'missing_stats_flag',
                  'max_est_rows',
                  'max_cost',
                  'cost_score',
                  'dominant_issue',
                  'cost_tier'),
 'query_explain': ('namespace_id',
                   'userid',
                   'user_id',
                   'query_id',
                   'child_query_sequence',
                   'plan_node_id',
                   'plan_parent_id',
                   'plan_node',
                   'plan_info'),
 'query_detail_flow': ('namespace_id',
                       'user_id',
                       'query_id',
                       'child_query_sequence',
                       'metrics_level',
                       'step_name',
                       'step_id',
                       'step_attribute',
                       'stream_id',
                       'segment_id',
                       'plan_node_id',
                       'plan_parent_id',
                       'start_time',
                       'end_time',
                       'duration',
                       'table_id',
                       'table_name',
                       'source',
                       'is_rrscan',
                       'input_bytes',
                       'input_rows',
                       'output_bytes',
                       'output_rows',
                       'blocks_read',
                       'blocks_write',
                       'local_read_io',
                       'remote_read_io',
                       'spilled_block_local_disk',
                       'spilled_block_remote_disk',
                       'data_skewness',
                       'time_skewness',
                       'alert',
                       'detail_row_count',
                       'max_duration',
                       'alert_count',
                       'pain_score',
                       'dominant_pain',
                       'pain_points',
                       'detail_row_rank',
                       'pain_rank'),
 'query_text': ('namespace_id',
                'query_id',
                'sequence',
                'sequence_num',
                'query_text',
                'text',
                'sql_text'),
 'child_query_text': ('namespace_id',
                      'user_id',
                      'query_id',
                      'child_query_sequence',
                      'sequence',
                      'text'),
 'user_info': ('namespace_id', 'user_id', 'usesysid', 'user_name', 'usename'),
 'table_scan_info': ('namespace_id',
                     'query_id',
                     'full_table_name',
                     'table_database',
                     'schema_name',
                     'table_name',
                     'queries',
                     'duration_s',
                     'input_rows_m',
                     'output_rows_m',
                     'rrscan_queries',
                     'non_rrscan_queries'),
 'view_definitions': ('namespace_id', 'database', 'schema', 'view_name', 'source_definition'),
 'procedure_definitions': ('namespace_id',
                           'database',
                           'schema',
                           'procedure_name',
                           'procedure_oid',
                           'procedure_key',
                           'owner_id',
                           'owner_name',
                           'argument_names',
                           'argument_modes',
                           'argument_type_oids',
                           'all_argument_type_oids',
                           'source_length',
                           'source_definition'),
 'external_tables_catalog': ('namespace_id',
                             'external_table_key',
                             'source_db',
                             'redshift_database_name',
                             'schema_name',
                             'table_name',
                             'sortkey'),
 'external_table_metadata': ('namespace_id',
                             'external_table_key',
                             'source_db',
                             'redshift_database_name',
                             'schema_name',
                             'table_name',
                             'column_name',
                             'data_type',
                             'column_number',
                             'partition_key_ordinal',
                             'is_nullable'),
 'user_roster': ('namespace_id',
                 'user_id',
                 'user_name',
                 'email',
                 'first_name',
                 'middle_name',
                 'middle_initial',
                 'last_name',
                 'domain',
                 'parsed'),
 'query_group_assignments': ('namespace_id',
                             'repeat_group_key',
                             'user_name',
                             'engineer_display',
                             'assigned_at'),
 'svv_table_info_all': ('namespace_id',
                        'database',
                        'source_db',
                        'schema',
                        'table_id',
                        'table',
                        'encoded',
                        'diststyle',
                        'sortkey1',
                        'max_varchar',
                        'sortkey1_enc',
                        'sortkey_num',
                        'size',
                        'pct_used',
                        'empty',
                        'unsorted',
                        'stats_off',
                        'tbl_rows',
                        'skew_sortkey1',
                        'skew_rows',
                        'estimated_visible_rows',
                        'risk_event',
                        'vacuum_sort_benefit',
                        'create_time'),
 'external_table_info_all': ('namespace_id',
                             'external_table_key',
                             'redshift_database_name',
                             'schema_name',
                             'table_name',
                             's3_location',
                             'table_type',
                             'input_format',
                             'output_format',
                             'serialization_lib',
                             'serde_parameters',
                             'compressed',
                             'table_parameters',
                             'column_count',
                             'partition_key_count',
                             'partition_key_columns',
                             'catalog_partition_count',
                             'observation_start_time',
                             'observation_end_time',
                             'query_count',
                             'external_segment_count',
                             'user_count',
                             'gross_scan_bytes',
                             'gross_scan_gb',
                             'gross_output_bytes',
                             'gross_output_gb',
                             'gross_scan_rows',
                             'gross_output_rows',
                             'output_metric_match_count',
                             'output_to_scan_byte_ratio',
                             'byte_reduction_pct_estimate',
                             'output_to_scan_row_ratio',
                             'row_filter_efficiency_pct',
                             'filtering_assessment',
                             'total_partitions_considered',
                             'qualified_partitions_scanned',
                             'partition_pruning_pct',
                             'pruning_event_count',
                             'no_pruning_event_count',
                             'scanned_files',
                             'avg_files_per_segment',
                             'max_files_per_segment',
                             'external_duration_s',
                             'avg_external_duration_s',
                             'max_external_duration_s',
                             's3list_time_ms',
                             'avg_s3list_time_ms',
                             'max_s3list_time_ms',
                             'get_partition_time_total_raw',
                             'avg_get_partition_time_raw',
                             'max_get_partition_time_raw',
                             'get_partition_time_unit',
                             'recursive_scan_count',
                             'nested_scan_count',
                             'warning_event_count',
                             'security_warning_count',
                             'partition_warning_count',
                             'schema_format_warning_count',
                             'file_location_warning_count',
                             'connectivity_warning_count',
                             'other_warning_count',
                             'warning_example',
                             'sampled_error_count',
                             'queries_with_sampled_errors',
                             'truncation_error_count',
                             'overflow_error_count',
                             'invalid_data_error_count',
                             'null_handling_error_count',
                             'other_error_count',
                             'distinct_error_code_count',
                             'affected_column_count',
                             'max_data_skewness',
                             'max_time_skewness',
                             'external_spill_blocks',
                             'observed_file_format'),
 'query_history': ('namespace_id',
                   'query_id',
                   'user_id',
                   'user_name',
                   'database',
                   'database_name',
                   'transaction_id',
                   'session_id',
                   'service_class',
                   'service_class_name',
                   'query_type',
                   'status',
                   'aborted',
                   'result_cache_hit',
                   'start_time',
                   'end_time',
                   'elapsed_time',
                   'execution_time',
                   'queue_time',
                   'planning_time',
                   'compile_time',
                   'lock_wait_time',
                   'rows',
                   'returned_rows',
                   'wlm_query_slot_count',
                   'query_label',
                   'error_message',
                   'user_query_hash',
                   'generic_query_hash'),
 'query_history_all': ('namespace_id',
                       'query_id',
                       'user_id',
                       'user_name',
                       'database',
                       'database_name',
                       'transaction_id',
                       'session_id',
                       'service_class',
                       'service_class_name',
                       'query_type',
                       'status',
                       'aborted',
                       'result_cache_hit',
                       'start_time',
                       'end_time',
                       'elapsed_time',
                       'execution_time',
                       'queue_time',
                       'planning_time',
                       'compile_time',
                       'lock_wait_time',
                       'rows',
                       'returned_rows',
                       'wlm_query_slot_count',
                       'query_label',
                       'error_message',
                       'user_query_hash',
                       'generic_query_hash')}

PERFORMANCE_INDEXES = (('snapshot_runs', ('captured_at',)),
 ('query_details', ('namespace_id', 'snapshot_id', 'query_id')),
 ('query_details', ('namespace_id', 'query_id')),
 ('query_health', ('namespace_id', 'snapshot_id', 'query_id')),
 ('query_health', ('namespace_id', 'query_id')),
 ('query_explain', ('namespace_id', 'snapshot_id', 'query_id')),
 ('query_explain', ('namespace_id', 'query_id')),
 ('query_explain', ('namespace_id', 'snapshot_id', 'query_id', 'plan_node_id')),
 ('query_detail_flow', ('namespace_id', 'snapshot_id', 'query_id')),
 ('query_detail_flow', ('namespace_id', 'query_id')),
 ('query_detail_flow',
  ('namespace_id',
   'snapshot_id',
   'query_id',
   'child_query_sequence',
   'stream_id',
   'segment_id',
   'step_id')),
 ('query_detail_flow', ('namespace_id', 'snapshot_id', 'query_id', 'plan_node_id')),
 ('query_text', ('namespace_id', 'snapshot_id', 'query_id')),
 ('query_text', ('namespace_id', 'query_id')),
 ('child_query_text', ('namespace_id', 'snapshot_id', 'query_id')),
 ('child_query_text', ('namespace_id', 'query_id')),
 ('child_query_text',
  ('namespace_id', 'snapshot_id', 'query_id', 'child_query_sequence', 'sequence')),
 ('user_info', ('namespace_id', 'snapshot_id', 'user_id')),
 ('user_info', ('namespace_id', 'user_id')),
 ('query_history', ('namespace_id', 'snapshot_id', 'query_id')),
 ('query_history', ('namespace_id', 'query_id')),
 ('query_history_all', ('namespace_id', 'snapshot_id', 'query_id')),
 ('query_history_all', ('namespace_id', 'query_id')),
 ('table_scan_info',
  ('namespace_id', 'snapshot_id', 'table_database', 'schema_name', 'table_name')),
 ('table_scan_info', ('namespace_id', 'table_name')),
 ('table_scan_info', ('namespace_id', 'table_database', 'schema_name', 'table_name')),
 ('svv_table_info_all',
  ('namespace_id', 'snapshot_id', 'source_db', 'database', 'schema', 'table')),
 ('svv_table_info_all', ('namespace_id', 'table')),
 ('svv_table_info_all', ('namespace_id', 'source_db', 'schema', 'table')),
 ('svv_table_info_all', ('namespace_id', 'database', 'schema', 'table')),
 ('external_table_info_all', ('namespace_id', 'snapshot_id', 'external_table_key')),
 ('external_table_info_all', ('namespace_id', 'external_table_key')),
 ('external_table_info_all',
  ('namespace_id', 'redshift_database_name', 'schema_name', 'table_name')),
 ('external_table_info_all', ('namespace_id', 'table_name')),
 ('view_definitions', ('namespace_id', 'snapshot_id', 'database', 'schema', 'view_name')),
 ('view_definitions', ('namespace_id', 'view_name')),
 ('view_definitions', ('namespace_id', 'database', 'schema', 'view_name')),
 ('procedure_definitions', ('namespace_id', 'snapshot_id', 'database', 'schema', 'procedure_name')),
 ('procedure_definitions', ('namespace_id', 'procedure_key')),
 ('procedure_definitions', ('namespace_id', 'database', 'schema', 'procedure_name')))

COMMON_COLUMNS = ("snapshot_id", "captured_at", "namespace_id")
ROOT_ORDER = ("query_history", "query_text", "child_query_text", "query_history_all")
QUERY_EVIDENCE_TABLES = frozenset(EXTRACTIONS)
NO_QUALIFYING_QUERY_STATUS = "Complete — no qualifying queries"
CONSUMER_CATALOG_DATABASE_NOT_PRESENT_STATUS = (
    "Complete — enterprise_datawarehouse not present on this consumer"
)
# Emergency operating policy. All catalog datasets that normally cycle across
# Redshift databases use these explicit role-specific lists until the operator
# asks to restore database discovery.
PRODUCER_CATALOG_DATABASE_SCOPE = (
    "datalake_cl",
    "enterprise_datawarehouse",
    "businesslayer",
    "datalake_wealth",
    "datalake_cl1",
    "datalake_cl2",
    "datalake",
    "byod",
    "investors_bank",
    "smart_leads",
)
CONSUMER_CATALOG_DATABASE_SCOPE = ("enterprise_datawarehouse",)
# Backward-compatible public name: standalone producer loaders historically
# imported CATALOG_DATABASE_SCOPE.
CATALOG_DATABASE_SCOPE = PRODUCER_CATALOG_DATABASE_SCOPE
CATALOG_DATABASE_SCOPE_KEY = (
    f"producer={','.join(PRODUCER_CATALOG_DATABASE_SCOPE)}|"
    f"consumer={','.join(CONSUMER_CATALOG_DATABASE_SCOPE)}"
)
DATABASE_CYCLED_TABLES = frozenset((
    "svv_table_info_all",
    "view_definitions",
    "procedure_definitions",
    "external_table_metadata",
    "external_table_info_all",
))
LIVE_REFRESH_TABLES = tuple(EXTRACTIONS) + (
    "svv_table_info_all", "view_definitions", "procedure_definitions",
    "external_table_metadata", "external_table_info_all"
)
SCRIPT_NAME = Path(__file__).name
SCRIPT_PATH = Path(__file__).resolve()


def runner_command(*arguments: object) -> str:
    """Copy/paste-safe command that works even when the current folder differs."""
    values = [sys.executable, str(SCRIPT_PATH), *(str(value) for value in arguments)]
    return " ".join('"' + value.replace('"', '\\"') + '"' for value in values)


def tmp_name(table_name: str) -> str:
    return f"{table_name}_tmp"


# External-table capture is EXCLUDED in this version. The SVV_EXTERNAL_*
# stage is disabled everywhere until it can be revalidated against the
# guarded corporate cluster; flip to True to restore the optional final
# stage (the UI and plan validation all key off this one switch).
EXTERNAL_CAPTURE_ENABLED = False

def selected_refresh_tables(args) -> tuple[str, ...]:
    """Return a validated, ordered refresh plan.

    ``None`` means the complete plan.  An explicit selection is kept in the
    canonical dependency order, and external metadata remains an optional
    last step. Evidence tables can run without root tables by using their
    existing live-threshold SQL fallback; parent IDs are derived only when
    both query_history and query_text are in this load.
    """
    requested = getattr(args, "include_tables", None)
    if requested is None:
        selected = set(LIVE_REFRESH_TABLES)
    else:
        selected = {
            str(value).strip()
            for value in requested
            if str(value).strip()
        }
        unknown = sorted(selected - set(LIVE_REFRESH_TABLES))
        if unknown:
            raise ValueError(f"Unknown loader table(s): {', '.join(unknown)}")
    if not EXTERNAL_CAPTURE_ENABLED:
        selected.discard("external_table_info_all")
    if not bool(getattr(args, "include_external", True)):
        selected.discard("external_table_info_all")
    plan = tuple(table for table in LIVE_REFRESH_TABLES if table in selected)
    if not plan:
        raise ValueError("Select at least one DuckDB table to refresh.")
    return plan


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _index_name(table_name: str, columns: tuple[str, ...]) -> str:
    return "idx_" + re.sub(r"[^a-z0-9_]+", "_", f"{table_name}_{'_'.join(columns)}".lower())


def sql_hash(sql: str) -> str:
    normalized = "\n".join(line.rstrip() for line in str(sql or "").strip().splitlines()).strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def _raw_store_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


# ------------------------------------------------------------- duckdb access


def _is_lock_conflict(exc: Exception) -> bool:
    """Only a DuckDB IO error whose text names the file lock is retryable.

    The type gate keeps unrelated errors that merely mention "lock" from
    spinning the retry loop; the wording gate keeps other IO errors (disk
    full, corrupt file) failing fast.
    """
    io_exception = getattr(duckdb, "IOException", None)
    if io_exception is not None and not isinstance(exc, io_exception):
        return False
    message = str(exc).lower()
    return "lock" in message or "being used" in message


def open_duck(path: Path, wait_seconds: float):
    """Open the analyzer DuckDB file, retrying while the app holds the lock."""
    deadline = time.monotonic() + max(0.0, wait_seconds)
    announced = False
    while True:
        try:
            return duckdb.connect(str(path))
        except Exception as exc:
            if not _is_lock_conflict(exc):
                raise
            if time.monotonic() >= deadline:
                raise SystemExit(
                    f"Gave up after {wait_seconds:.0f}s: the DuckDB file is still locked by "
                    "another process (probably the analyzer app mid-query). Re-run when it is idle, "
                    "or raise --lock-wait-seconds."
                )
            if not announced:
                print("  DuckDB file busy (analyzer app is reading); waiting for a free moment ...")
                announced = True
            time.sleep(3)


def ensure_table(con, table_name: str, columns) -> None:
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {quote_ident(table_name)} (snapshot_id VARCHAR, captured_at TIMESTAMP)"
    )
    existing = {
        str(row[0])
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [table_name],
        ).fetchall()
    }
    for col in tuple(COMMON_COLUMNS) + tuple(columns):
        if col in existing:
            continue
        dtype = "TIMESTAMP" if col == "captured_at" else "VARCHAR"
        con.execute(f"ALTER TABLE {quote_ident(table_name)} ADD COLUMN {quote_ident(col)} {dtype}")
        existing.add(col)


def ensure_indexes(con) -> None:
    """The analyzer's canonical performance indexes, applied to the live tables."""
    for table_name, columns in PERFORMANCE_INDEXES:
        existing_columns = {
            row[0]
            for row in con.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
                [table_name],
            ).fetchall()
        }
        index_columns = tuple(col for col in columns if col in existing_columns)
        if not index_columns:
            continue
        con.execute(
            f"CREATE INDEX IF NOT EXISTS {quote_ident(_index_name(table_name, index_columns))} "
            f"ON {quote_ident(table_name)} ({', '.join(quote_ident(c) for c in index_columns)})"
        )


def _ensure_stash_tables(con) -> None:
    con.execute(f"CREATE TABLE IF NOT EXISTS {STATE_TABLE} (state_key VARCHAR, state_value VARCHAR)")
    con.execute(f"CREATE TABLE IF NOT EXISTS {SQL_STASH_TABLE} (table_name VARCHAR, sql_text VARCHAR)")


def save_state(duckdb_path: Path, lock_wait: float, values: dict) -> None:
    con = open_duck(duckdb_path, lock_wait)
    try:
        _ensure_stash_tables(con)
        for key, value in values.items():
            con.execute(f"DELETE FROM {STATE_TABLE} WHERE state_key = ?", [key])
            con.execute(f"INSERT INTO {STATE_TABLE} VALUES (?, ?)", [key, str(value)])
    finally:
        con.close()


def read_state(con) -> dict:
    try:
        rows = con.execute(f"SELECT state_key, state_value FROM {STATE_TABLE}").fetchall()
    except Exception:
        return {}
    return {str(k): str(v) for k, v in rows}


def write_tmp_table(
    duckdb_path: Path, base_table: str, frame, snapshot_id: str, sql_text: str,
    lock_wait: float, *, append: bool = False,
) -> None:
    """One short lock window: rebuild <base_table>_tmp from the fetched frame."""
    target = tmp_name(base_table)
    df = frame.copy()
    df.columns = [str(c).strip() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()].copy()
    for col in df.columns:
        df[col] = df[col].map(_raw_store_value)
    df.insert(0, "captured_at", datetime.now())
    df.insert(0, "snapshot_id", snapshot_id)
    columns = list(dict.fromkeys(df.columns))

    con = open_duck(duckdb_path, lock_wait)
    try:
        if not append:
            con.execute(f"DROP TABLE IF EXISTS {quote_ident(target)}")
        expected = tuple(EXPECTED_COLUMNS.get(base_table, ())) + tuple(c for c in columns if c not in COMMON_COLUMNS)
        ensure_table(con, target, expected)
        registered = f"incoming_{uuid.uuid4().hex}"
        con.register(registered, df)
        try:
            select_list = [
                f"CAST({quote_ident(col)} AS TIMESTAMP) AS {quote_ident(col)}"
                if col == "captured_at"
                else f"CAST({quote_ident(col)} AS VARCHAR) AS {quote_ident(col)}"
                for col in columns
            ]
            con.execute(
                f"INSERT INTO {quote_ident(target)} ({', '.join(quote_ident(c) for c in columns)}) "
                f"SELECT {', '.join(select_list)} FROM {quote_ident(registered)}"
            )
        finally:
            con.unregister(registered)
        _ensure_stash_tables(con)
        if not append:
            con.execute(f"DELETE FROM {SQL_STASH_TABLE} WHERE table_name = ?", [base_table])
            con.execute(f"INSERT INTO {SQL_STASH_TABLE} VALUES (?, ?)", [base_table, sql_text])
        rows = con.execute(f"SELECT COUNT(*) FROM {quote_ident(target)}").fetchone()[0]
    finally:
        con.close()
    print(f"  {target}: {int(rows or 0):,} row(s) written")


def stamp_cluster_namespace(frame, cfg):
    """Attach the supplied namespace to every Redshift-sourced row."""
    df = frame.copy() if frame is not None else pd.DataFrame()
    namespace = str(getattr(cfg, "namespace_id", "") or "producer").strip() or "producer"
    if "namespace_id" in df.columns:
        df["namespace_id"] = namespace
    else:
        df.insert(0, "namespace_id", namespace)
    return df


# ------------------------------------------------------------ redshift access


def connect_redshift(cfg, database: str):
    import redshift_connector

    conn = redshift_connector.connect(
        host=cfg.host,
        port=cfg.port,
        database=database,
        user=cfg.user,
        password=cfg.password,
    )
    conn.autocommit = True
    return conn


def fetch_frame(cfg, database: str, sql: str, stage: str = ""):
    conn = connect_redshift(cfg, database)
    try:
        cur = conn.cursor()
        try:
            timeout_ms = int(getattr(cfg, "statement_timeout_ms", 0) or 0)
            if timeout_ms > 0:
                cur.execute(f"SET statement_timeout TO {timeout_ms}")
            cur.execute(sql)
            columns = [str(d[0]) for d in cur.description]
            rows: list = []
            last_report = 0
            while True:
                batch = cur.fetchmany(FETCH_BATCH_ROWS)
                if not batch:
                    break
                rows.extend(batch)
                if len(rows) - last_report >= 100_000:
                    last_report = len(rows)
                    print(f"    {stage}: {len(rows):,} rows fetched ...")
            return pd.DataFrame.from_records(rows, columns=columns)
        finally:
            cur.close()
    finally:
        conn.close()


def fetch_columns(cfg, database: str, view_name: str) -> list:
    conn = connect_redshift(cfg, database)
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT * FROM {view_name} WHERE 1 = 0")
            return [str(d[0]) for d in cur.description]
        finally:
            cur.close()
    finally:
        conn.close()


def validate_source_columns(cfg, database: str, view_name: str) -> None:
    actual = fetch_columns(cfg, database, view_name)
    normalized = {c.lower() for c in actual}
    required = {c.lower() for c in SOURCE_REQUIREMENTS[view_name]}
    missing = sorted(required - normalized)
    if missing:
        raise SystemExit(
            f"{database}.{view_name} is missing required column(s): {missing}. "
            "The cluster's system views do not match what this capture expects."
        )
    print(f"  {database}.{view_name}: {len(actual)} columns ok")


def _external_timeout_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(marker in text for marker in (
        "statement timeout", "canceling statement", "cancelled on user's request",
        "system requested abort", "abort query", "query timeout", "timed out",
    ))


def _external_timeout_decision(cfg, stage: str, exc: Exception) -> str:
    callback = getattr(cfg, "timeout_decision_callback", None)
    if callable(callback):
        return str(callback(stage, str(exc)) or "next").strip().lower()
    if sys.stdin.isatty():
        print(f"\nExternal stage reached its 10-minute timeout: {stage}\n{exc}")
        answer = input("Retry for another 10 minutes [R], or move to the next step [N]? ").strip().lower()
        return "continue" if answer in {"r", "retry", "c", "continue"} else "next"
    return "next"


def fetch_external_stage(cfg, database: str, sql: str, stage: str, *, optional: bool = True):
    """Fetch one Redshift view with a retry/skip decision at the timeout."""
    while True:
        try:
            return fetch_frame(cfg, database, sql, stage=stage)
        except Exception as exc:
            if _external_timeout_error(exc):
                if _external_timeout_decision(cfg, stage, exc) == "continue":
                    print(f"  Retrying {stage} with a fresh 10-minute window ...")
                    continue
                print(f"  {stage}: skipped after timeout; continuing to the next stage")
                return pd.DataFrame()
            if optional:
                print(f"  OPTIONAL STAGE SKIPPED: {stage}: {exc}")
                return pd.DataFrame()
            raise


def validate_primary_sources(cfg) -> None:
    for view_name in (
        "sys_query_detail",
        "sys_query_history",
        "sys_query_explain",
        "sys_query_text",
        "sys_child_query_text",
        "svv_user_info",
    ):
        validate_source_columns(cfg, cfg.primary_database, view_name)


def resolve_table_databases(cfg) -> tuple:
    namespace = str(getattr(cfg, "namespace_id", "") or "unknown")
    role = str(getattr(cfg, "cluster_role", "") or "").strip().lower()
    scope = (
        CONSUMER_CATALOG_DATABASE_SCOPE
        if role == "consumer"
        else PRODUCER_CATALOG_DATABASE_SCOPE
    )
    print(
        f"Using fixed {role or 'producer'} catalog database scope for "
        f"namespace {namespace}: {', '.join(scope)} "
        "(automatic database discovery disabled by emergency policy)"
    )
    return scope


# --------------------------------------------------- normalizers (from ingest)


def _view_definition_part(value) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value)
    return "" if text in {"None", "NULL", "nan"} else text


def _procedure_body_begin_to_end(value) -> str:
    text = _view_definition_part(value)
    if not text:
        return ""
    begin = re.search(r"\bbegin\b", text, flags=re.IGNORECASE)
    if not begin:
        return text.strip()
    end_matches = list(re.finditer(r"\bend\b\s*;?", text, flags=re.IGNORECASE))
    end_after_begin = [match for match in end_matches if match.start() >= begin.start()]
    if not end_after_begin:
        return text[begin.start():].strip()
    end = end_after_begin[-1]
    return text[begin.start():end.end()].strip()


def normalize_view_definitions(frame, database: str):
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["database", "schema", "view_name", "source_definition"])
    df = frame.copy()
    rename = {}
    for col in df.columns:
        normalized = str(col).strip().lower()
        if normalized == "schemaname":
            rename[col] = "schema"
        elif normalized == "viewname":
            rename[col] = "view_name"
    if rename:
        df = df.rename(columns=rename)
    if "database" not in df.columns:
        df["database"] = database
    part_cols = sorted(
        [col for col in df.columns if re.match(r"definition_part_\d+$", str(col).strip().lower())],
        key=lambda col: int(re.search(r"(\d+)$", str(col)).group(1)),
    )
    if part_cols:
        df["source_definition"] = df[part_cols].apply(
            lambda row: "".join(_view_definition_part(value) for value in row), axis=1
        )
    elif "source_definition" not in df.columns:
        df["source_definition"] = ""
    for col in ("schema", "view_name"):
        if col not in df.columns:
            df[col] = ""
    return df[["database", "schema", "view_name", "source_definition"]]


def normalize_procedure_definitions(frame, database: str):
    out_cols = [
        "database", "schema", "procedure_name", "procedure_oid", "procedure_key",
        "owner_id", "owner_name", "argument_names", "argument_modes",
        "argument_type_oids", "all_argument_type_oids", "source_length", "source_definition",
    ]
    if frame is None or frame.empty:
        return pd.DataFrame(columns=out_cols)
    df = frame.copy()
    rename = {}
    for col in df.columns:
        normalized = str(col).strip().lower()
        if normalized in {"nspname", "schemaname", "schema_name"}:
            rename[col] = "schema"
        elif normalized in {"proname", "procname", "stored_procedure_name"}:
            rename[col] = "procedure_name"
        elif normalized in {"prooid", "oid"}:
            rename[col] = "procedure_oid"
        elif normalized in {"prosrc", "procedure_source", "definition", "procedure_definition"}:
            rename[col] = "source_definition"
    if rename:
        df = df.rename(columns=rename)
    if "database" not in df.columns:
        df["database"] = database
    part_cols = sorted(
        [col for col in df.columns if re.match(r"definition_part_\d+$", str(col).strip().lower())],
        key=lambda col: int(re.search(r"(\d+)$", str(col)).group(1)),
    )
    if part_cols:
        df["source_definition"] = df[part_cols].apply(
            lambda row: "".join(_view_definition_part(value) for value in row), axis=1
        )
    elif "source_definition" not in df.columns:
        df["source_definition"] = ""
    df["source_definition"] = df["source_definition"].map(_procedure_body_begin_to_end)
    df["source_length"] = df["source_definition"].map(lambda value: len(str(value or "")))
    for col in (
        "schema", "procedure_name", "procedure_oid", "owner_id", "owner_name",
        "argument_names", "argument_modes", "argument_type_oids",
        "all_argument_type_oids", "source_length",
    ):
        if col not in df.columns:
            df[col] = ""
    if "procedure_key" not in df.columns:
        df["procedure_key"] = df.apply(
            lambda row: ".".join(
                str(row.get(key) or "").strip().lower()
                for key in ("database", "schema", "procedure_name")
            ),
            axis=1,
        )
    return df[out_cols]


# --------------------------------------------- phase-2 parent representatives


_FP_COMMENT_RE = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)
_FP_STRING_RE = re.compile(r"'(?:[^']|'')*'")
_FP_NUMBER_RE = re.compile(r"\b\d+(?:\.\d+)?\b")
_FP_WS_RE = re.compile(r"\s+")


def simple_fingerprint(sql) -> str:
    """Literal-insensitive grouping key (standalone stand-in for the app's
    sqlglot canonical fingerprint)."""
    text = _FP_COMMENT_RE.sub(" ", str(sql or "").lower())
    text = _FP_STRING_RE.sub("?", text)
    text = _FP_NUMBER_RE.sub("?", text)
    return _FP_WS_RE.sub(" ", text).strip()


def compute_parent_target_ids(con, snapshot_id: str, limit: int, floor_basis: str,
                              history_table: str, text_table: str,
                              namespace_id: str | None = None) -> list:
    """Group the captured _tmp roots into parents by literal-agnostic SQL text
    and return one representative query_id (the heaviest execution) per parent,
    ranked by the parent's TOTAL time on the chosen basis."""
    basis = str(floor_basis or "execution_time").strip().lower()
    if basis not in {"execution_time", "elapsed_time"}:
        basis = "execution_time"
    try:
        history_cols = {
            str(row[0]).lower()
            for row in con.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
                [history_table],
            ).fetchall()
        }
        history_text = "NULLIF(h.query_text, '')" if "query_text" in history_cols else "NULL"
        namespace_filter = " AND namespace_id = ?" if namespace_id else ""
        query_params = [snapshot_id]
        if namespace_id:
            query_params.append(namespace_id)
        query_params.append(snapshot_id)
        if namespace_id:
            query_params.append(namespace_id)
        frame = con.execute(
            f"""
SELECT
  h.query_id,
  COALESCE(TRY_CAST(h.{basis} AS BIGINT), 0) AS execution_time,
  COALESCE(NULLIF(t.sql_text, ''), {history_text}, '') AS sql_text
FROM {quote_ident(history_table)} h
LEFT JOIN (
  SELECT
    query_id,
    STRING_AGG(
      COALESCE(NULLIF(text, ''), NULLIF(sql_text, ''), NULLIF(query_text, ''), ''),
      ''
      ORDER BY COALESCE(TRY_CAST(sequence AS BIGINT), TRY_CAST(sequence_num AS BIGINT), 0)
    ) AS sql_text
  FROM {quote_ident(text_table)}
  WHERE snapshot_id = ?{namespace_filter}
  GROUP BY query_id
) t ON t.query_id = h.query_id
WHERE h.snapshot_id = ?{namespace_filter.replace('namespace_id', 'h.namespace_id')}
""",
            query_params,
        ).fetchdf()
    except Exception:
        return []
    if frame is None or frame.empty:
        return []
    groups: dict = {}
    for row in frame.itertuples(index=False):
        text = str(row.sql_text or "").strip()
        if not text:
            continue
        fingerprint = simple_fingerprint(text) or text
        execution = int(row.execution_time or 0)
        group = groups.setdefault(fingerprint, {"total": 0, "best_execution": -1, "best_id": None})
        group["total"] += execution
        if execution > group["best_execution"]:
            group["best_execution"] = execution
            group["best_id"] = row.query_id
    ranked = sorted(groups.values(), key=lambda group: -group["total"])
    cap = max(0, int(limit or 0))
    selected = ranked if cap <= 0 else ranked[:cap]
    target_ids = []
    for group in selected:
        try:
            target_ids.append(int(str(group["best_id"])))
        except (TypeError, ValueError):
            continue
    return target_ids


# --------------------------------------------------------------- SQL dispatch


ROOT_TABLE_SET = frozenset(ROOT_ORDER)


def table_sql(cfg, table_name: str, target_ids=None) -> str:
    if table_name in ROOT_TABLE_SET:
        return EXTRACTIONS[table_name](
            cfg.minutes,
            cfg.evidence_parent_limit,
            cfg.rank_by,
            min_execution_seconds=cfg.floor_seconds,
            floor_basis=cfg.floor_basis,
            window_days=cfg.days,
        )
    if table_name == "query_detail_flow":
        return EXTRACTIONS[table_name](
            cfg.minutes, cfg.evidence_parent_limit, cfg.detail_flow_rows, cfg.rank_by, target_ids=target_ids
        )
    if table_name == "user_info":
        return EXTRACTIONS[table_name](cfg.minutes, cfg.evidence_parent_limit, cfg.rank_by)
    if table_name in EXTRACTIONS:
        return EXTRACTIONS[table_name](cfg.minutes, cfg.evidence_parent_limit, cfg.rank_by, target_ids=target_ids)
    if table_name == "svv_table_info_all":
        return TABLE_INFO_SQL
    if table_name == "external_table_info_all":
        return EXTERNAL_TABLE_INFO_SQL
    if table_name == "view_definitions":
        return VIEW_DEFINITIONS_SQL
    if table_name == "procedure_definitions":
        return PROCEDURE_DEFINITIONS_SQL
    raise ValueError(f"Unknown table: {table_name}")


# ---------------------------------------------------------------- environment


def sense_environment(args) -> list:
    problems = []
    if sys.version_info < MIN_PYTHON:
        problems.append(
            f"Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ is required (you have "
            f"{sys.version_info.major}.{sys.version_info.minor})."
        )
    if _IMPORT_ERROR is not None:
        problems.append(f"Missing package ({_IMPORT_ERROR}). Fix:  pip install duckdb pandas")
    if not (args.swap or args.status or args.backup_only):
        try:
            __import__("redshift_connector")
        except ImportError:
            problems.append("Missing package 'redshift_connector'. Fix:  pip install redshift-connector")
        configured = bool(
            _credential_value("REDSHIFT_PRODUCER_HOST")
            or _credential_value("REDSHIFT_HOST")
        )
        producer_enabled_name = (
            "REDSHIFT_ENABLED"
            if os.environ.get("REDSHIFT_ENABLED") is not None
            else "REDSHIFT_PRODUCER_ENABLED"
        )
        selected = _env_enabled(producer_enabled_name, default=configured)
        selected = selected or any(
            _env_enabled(f"REDSHIFT_CONSUMER_{ordinal}_ENABLED", default=False)
            or (
                os.environ.get(f"REDSHIFT_CONSUMER_{ordinal}_ENABLED") is None
                and bool(_credential_value(f"REDSHIFT_CONSUMER_{ordinal}_HOST"))
            )
            for ordinal in _configured_consumer_ordinals()
        )
        if not selected:
            problems.append("No Redshift cluster is checked for loading. Set at least one profile's ENABLED value to true.")
    duckdb_path = Path(args.duckdb_path)
    if not duckdb_path.is_file():
        if args.swap or args.status or args.backup_only:
            problems.append(
                f"DuckDB file not found at {duckdb_path}. Run the normal load first, or pass --duckdb-path "
                "to an existing analyzer database."
            )
        else:
            try:
                duckdb_path.parent.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                problems.append(f"DuckDB folder cannot be created at {duckdb_path.parent}: {exc}")
    return problems


def _env_enabled(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return bool(default)
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "y", "on", "checked"}:
        return True
    if value in {"0", "false", "no", "n", "off", "unchecked"}:
        return False
    raise SystemExit(f"{name} must be true or false, got {raw!r}.")


def _configured_consumer_ordinals() -> tuple[int, ...]:
    active = _active_profile_prefixes()
    if active is not None:
        return tuple(sorted(
            int(match.group(1))
            for prefix in active
            if (match := re.fullmatch(r"REDSHIFT_CONSUMER_(\d+)", prefix))
        ))
    keys = set(os.environ)
    try:
        from analyzer.secrets_store import session_secrets

        keys.update(session_secrets())
    except ImportError:
        pass
    return tuple(sorted({
        int(match.group(1))
        for key in keys
        if (match := re.match(r"^REDSHIFT_CONSUMER_(\d+)_", str(key).upper()))
    }))


def _active_profile_prefixes() -> tuple[str, ...] | None:
    """Exact portable-manifest membership, or None for legacy discovery."""
    if ACTIVE_PROFILE_PREFIXES_ENV not in os.environ:
        return None
    result: list[str] = []
    for value in str(os.environ.get(ACTIVE_PROFILE_PREFIXES_ENV) or "").split(","):
        prefix = value.strip().upper()
        if (
            prefix == "REDSHIFT_PRODUCER"
            or re.fullmatch(r"REDSHIFT_CONSUMER_\d+", prefix)
        ) and prefix not in result:
            result.append(prefix)
    return tuple(result)


def _credential_value(name: str) -> str:
    """Resolve a profile value without exporting protected data to the environment."""
    try:
        from analyzer.secrets_store import session_secret

        protected = str(session_secret(name) or "").strip()
        if protected:
            return protected
    except ImportError:
        pass
    # Standalone legacy runner copies may still be supplied with process-only
    # values. The Infraredshift loader resolves its encrypted session first.
    return str(os.environ.get(name) or "").strip()


# Administrator-adjustable per cluster via FLOOR_SECONDS in the portable
# cluster-profiles JSON; these are only the fallbacks when it is not set.
DEFAULT_PRODUCER_FLOOR_SECONDS = 300.0
DEFAULT_CONSUMER_FLOOR_SECONDS = 30.0


def _cluster_floor_seconds(value_fn, default: float) -> float:
    """Per-cluster sys_query_history runtime floor, else the load default."""
    raw = str(value_fn("FLOOR_SECONDS", "REDSHIFT_FLOOR_SECONDS") or "").strip()
    if not raw:
        return default
    try:
        seconds = float(raw)
    except ValueError:
        return default
    return seconds if 0 <= seconds <= 86400 else default


def _floor_fallback(args, role: str) -> float:
    """Explicit --floor-seconds if given, else the role default."""
    explicit = getattr(args, "floor_seconds", None)
    if explicit is not None:
        return float(explicit)
    return DEFAULT_PRODUCER_FLOOR_SECONDS if role == "producer" else DEFAULT_CONSUMER_FLOOR_SECONDS


def _profile_config(args, prefix: str, role: str, ordinal: int = 0) -> argparse.Namespace | None:
    legacy = role == "producer"
    def value(name: str, legacy_name: str | tuple[str, ...] | None = None, default: str = "") -> str:
        found = _credential_value(f"{prefix}_{name}") or None
        if found is None and legacy and legacy_name:
            names = (legacy_name,) if isinstance(legacy_name, str) else legacy_name
            found = next((_credential_value(candidate) for candidate in names if _credential_value(candidate)), None)
        return str(found if found is not None else default).strip()

    host = value("HOST", "REDSHIFT_HOST")
    enabled_name = (
        "REDSHIFT_ENABLED"
        if legacy and os.environ.get("REDSHIFT_ENABLED") is not None
        else f"{prefix}_ENABLED"
    )
    enabled = _env_enabled(enabled_name, default=bool(host))
    if not enabled:
        return None
    if not host:
        raise SystemExit(f"{prefix}_HOST is required when {prefix}_ENABLED=true.")
    namespace_id = (
        str(os.environ.get("REDSHIFT_NAMESPACE") or "").strip()
        if legacy
        else ""
    ) or value("NAMESPACE_ID", "REDSHIFT_NAMESPACE_ID")
    if not namespace_id:
        raise SystemExit(f"{prefix}_NAMESPACE_ID is required when {prefix}_HOST is configured.")
    user = value("USER", "REDSHIFT_USER")
    if not user:
        raise SystemExit(f"{prefix}_USER is required.")
    password = value("PASSWORD", "REDSHIFT_PASSWORD")
    if not password:
        if sys.stdin.isatty():
            password = getpass.getpass(f"Redshift password for {role} {ordinal or ''}: ")
        if not password:
            raise SystemExit(f"{prefix}_PASSWORD is not set and no password was entered.")
    return argparse.Namespace(
        namespace_id=namespace_id,
        cluster_role=role,
        cluster_ordinal=ordinal,
        enabled=True,
        cluster_name=(
            value("DISPLAY_NAME")
            or value("FRIENDLY", "REDSHIFT_FRIENDLY")
            or (str(os.environ.get("REDSHIFT_ENV") or "").strip() if legacy else "")
            or ("Producer" if legacy else f"Consumer {ordinal}")
        ),
        host=host,
        port=int(value("PORT", "REDSHIFT_PORT", str(DEFAULT_REDSHIFT_PORT))),
        user=user,
        password=password,
        primary_database=(
            value("PRIMARY_DATABASE", "REDSHIFT_PRIMARY_DATABASE")
            or value("DATABASE", "REDSHIFT_DATABASE")
            or "dev"
        ),
        table_databases="",
        db_min_query_count=int(value("DATABASE_MIN_QUERY_COUNT", "REDSHIFT_DATABASE_MIN_QUERY_COUNT", "250")),
        days=float(args.days),
        # Floor precedence: FLOOR_SECONDS in the cluster profiles JSON, then an
        # explicit --floor-seconds, then the role default (producer 300s,
        # consumers 30s).
        floor_seconds=_cluster_floor_seconds(value, _floor_fallback(args, role)),
        floor_basis=args.floor_basis,
        # `minutes` is the slow-query threshold used by the evidence fallback
        # CTE when no parent ids resolve - keep it aligned with the floor.
        minutes=max(1, int(round(_cluster_floor_seconds(value, _floor_fallback(args, role)) / 60.0))),
        rank_by=os.environ.get("REDSHIFT_TOP_QUERY_RANK_BY", "elapsed_time").strip().lower() or "elapsed_time",
        evidence_parent_limit=max(0, int(os.environ.get("REDSHIFT_EVIDENCE_PARENT_LIMIT", "0") or 0)),
        detail_flow_rows=int(os.environ.get("REDSHIFT_DETAIL_FLOW_ROWS_PER_QUERY", str(DEFAULT_DETAIL_FLOW_ROWS_PER_QUERY))),
    )


def build_configs(args) -> tuple[argparse.Namespace, ...]:
    active = _active_profile_prefixes()
    profiles = []
    if active is None or "REDSHIFT_PRODUCER" in active:
        profiles.append(
            _profile_config(args, "REDSHIFT_PRODUCER", "producer")
        )
    for ordinal in _configured_consumer_ordinals():
        profile = _profile_config(args, f"REDSHIFT_CONSUMER_{ordinal}", "consumer", ordinal)
        if profile is not None:
            profiles.append(profile)
    configs = tuple(profile for profile in profiles if profile is not None)
    if not configs:
        raise SystemExit("No Redshift cluster is checked for loading. Enable at least one cluster profile.")
    namespaces = [cfg.namespace_id.lower() for cfg in configs]
    if len(namespaces) != len(set(namespaces)):
        raise SystemExit("Every configured cluster must have a unique NAMESPACE_ID.")
    return configs


def build_config(args) -> argparse.Namespace:
    """Backward-compatible first-selected config used by focused tools."""
    return build_configs(args)[0]


# ------------------------------------------------------------------ load mode


def _resolve_run(args, duckdb_path: Path):
    """Fresh run by default. With --resume, continue an interrupted load under
    its original snapshot id, skipping tables whose SQL stash row landed."""
    con = open_duck(duckdb_path, args.lock_wait_seconds)
    try:
        state = read_state(con)
        resumable = (
            args.resume
            and state.get("status") == "loading"
            and bool(state.get("snapshot_id"))
            and state.get("days") == str(args.days)
            and state.get("floor_seconds") == str(args.floor_seconds)
            and state.get("floor_basis") == args.floor_basis
        )
        if args.resume and not resumable:
            print("Nothing resumable (no interrupted load, or its window/floor settings differ); starting fresh.")
        if resumable:
            try:
                stash = {str(row[0]) for row in con.execute(f"SELECT table_name FROM {SQL_STASH_TABLE}").fetchall()}
            except Exception:
                stash = set()
            return state["snapshot_id"], stash
        _ensure_stash_tables(con)
        con.execute(f"DELETE FROM {SQL_STASH_TABLE}")
        con.execute(f"DELETE FROM {STATE_TABLE}")
    finally:
        con.close()
    return str(uuid.uuid4()), set()


def run_load(args) -> int:
    """Use one checkpoint format and one integrity path for every load.

    The former single-cluster implementation did not create namespace
    checkpoints and therefore could not prove that interrupted staging was
    complete.  The namespaced loader works for one cluster as well as many.
    """
    return run_multi_load(args, build_configs(args))


def _legacy_single_cluster_load(args) -> int:
    """Retained only as readable migration history; no entry point calls it."""
    configs = build_configs(args)
    if len(configs) > 1:
        return run_multi_load(args, configs)
    cfg = configs[0]
    duckdb_path = Path(args.duckdb_path)
    snapshot_id, completed_tables = _resolve_run(args, duckdb_path)
    if completed_tables:
        print(f"Resuming interrupted load: {len(completed_tables)} table(s) already finished will be skipped.")

    print(f"== Refresh window: {cfg.days:g} day(s), {cfg.floor_seconds:.0f}s+ on {cfg.floor_basis} ==")
    print(f"== Target file: {duckdb_path} (writes go to *_tmp tables only) ==\n")

    print("Validating Redshift source views")
    validate_primary_sources(cfg)

    save_state(
        duckdb_path,
        args.lock_wait_seconds,
        {
            "snapshot_id": snapshot_id,
            "label": "tmp refresh",
            "days": str(args.days),
            "floor_seconds": str(args.floor_seconds),
            "floor_basis": cfg.floor_basis,
            "catalog_database_scope": CATALOG_DATABASE_SCOPE_KEY,
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "loading",
            "selected_tables": ",".join(selected_refresh_tables(args)),
        },
    )
    # Single-cluster loads must publish the cluster registry too: the per-role
    # files feed the merge, and the merge resolves roles/friendly names from
    # snapshot_cluster_runs promoted out of this staging table.
    _record_tmp_cluster_registry(duckdb_path, args.lock_wait_seconds, snapshot_id, [cfg])

    target_ids: list | None = None

    def capture(table_name: str) -> None:
        if table_name in completed_tables:
            print(f"  {table_name}: already loaded, skipped (resume)")
            return
        sql = table_sql(cfg, table_name, target_ids=target_ids)
        print(f"  {table_name}: fetching from cluster")
        frame = stamp_cluster_namespace(
            fetch_frame(cfg, cfg.primary_database, sql, stage=table_name), cfg
        )
        write_tmp_table(duckdb_path, table_name, frame, snapshot_id, table_sql(cfg, table_name), args.lock_wait_seconds)

    print("\n== Phase 1: root query tables ==")
    for table_name in ROOT_ORDER:
        capture(table_name)

    print("\n== Phase 2: representative parent patterns ==")
    con = open_duck(duckdb_path, args.lock_wait_seconds)
    try:
        target_ids = compute_parent_target_ids(
            con, snapshot_id, cfg.evidence_parent_limit, cfg.floor_basis,
            tmp_name("query_history"), tmp_name("query_text"),
        ) or None
    finally:
        con.close()
    if target_ids:
        print(f"  {len(target_ids)} representative parent query id(s) selected")
    else:
        print("  No parents resolved; evidence tables fall back to live threshold selection")

    print("\n== Phase 3: evidence tables ==")
    for table_name in EXTRACTIONS:
        if table_name not in ROOT_TABLE_SET:
            capture(table_name)

    print("\n== Phase 4: per-database catalog tables ==")
    catalog_pending = [
        t for t in ("svv_table_info_all", "external_table_info_all", "view_definitions", "procedure_definitions")
        if t not in completed_tables
    ]
    for table_name in ("svv_table_info_all", "external_table_info_all", "view_definitions", "procedure_definitions"):
        if table_name not in catalog_pending:
            print(f"  {table_name}: already loaded, skipped (resume)")
    if catalog_pending:
        table_databases = resolve_table_databases(cfg)

    if "svv_table_info_all" in catalog_pending:
        frames = []
        for database in table_databases:
            frame = fetch_frame(cfg, database, TABLE_INFO_SQL, stage=f"svv_table_info_all [{database}]")
            frame["source_db"] = database
            frame = stamp_cluster_namespace(frame, cfg)
            frames.append(frame)
            print(f"  svv_table_info_all [{database}]: {len(frame):,} rows fetched")
        table_info = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        write_tmp_table(duckdb_path, "svv_table_info_all", table_info, snapshot_id, TABLE_INFO_SQL, args.lock_wait_seconds)

    if "view_definitions" in catalog_pending:
        frames = []
        for database in table_databases:
            frame = fetch_frame(cfg, database, VIEW_DEFINITIONS_SQL, stage=f"view_definitions [{database}]")
            frames.append(stamp_cluster_namespace(normalize_view_definitions(frame, database), cfg))
            print(f"  view_definitions [{database}]: {len(frame):,} rows fetched")
        view_defs = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        write_tmp_table(duckdb_path, "view_definitions", view_defs, snapshot_id, VIEW_DEFINITIONS_SQL, args.lock_wait_seconds)

    if "procedure_definitions" in catalog_pending:
        frames = []
        for database in table_databases:
            frame = fetch_frame(cfg, database, PROCEDURE_DEFINITIONS_SQL, stage=f"procedure_definitions [{database}]")
            frames.append(stamp_cluster_namespace(normalize_procedure_definitions(frame, database), cfg))
            print(f"  procedure_definitions [{database}]: {len(frame):,} rows fetched")
        procedure_defs = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        write_tmp_table(duckdb_path, "procedure_definitions", procedure_defs, snapshot_id, PROCEDURE_DEFINITIONS_SQL, args.lock_wait_seconds)

    # External metadata is intentionally the last phase.  Every normal query,
    # catalog, view, and procedure table is already safe in *_tmp before any
    # potentially slow Spectrum metadata view is touched.
    if "external_table_info_all" in catalog_pending:
        print("\n== FINAL PHASE: optional external-table metadata ==")
        frames = []
        prior_timeout = int(getattr(cfg, "statement_timeout_ms", 0) or 0)
        cfg.statement_timeout_ms = 600_000
        try:
            for database in table_databases:
                print(f"  external_table_info_all [{database}]: independent source staging")
                catalog = fetch_external_stage(
                    cfg, database, EXTERNAL_CATALOG_STAGE_SQL,
                    stage=f"external catalog [{database}]",
                )
                segments = fetch_external_stage(
                    cfg, database, external_segments_stage_sql(args.days),
                    stage=f"external scan segments [{database}]",
                )
                if catalog.empty:
                    catalog = minimal_external_catalog_from_segments(segments, database)
                query_ids = external_query_ids(segments)
                history = pd.DataFrame({
                    "query_id": query_ids,
                    "database_name": [database] * len(query_ids),
                })
                batch_size = 100
                step_frames = []
                for offset in range(0, len(query_ids), batch_size):
                    batch = query_ids[offset:offset + batch_size]
                    step_frames.append(fetch_external_stage(
                        cfg, database, external_steps_stage_sql(batch),
                        stage=f"external output metrics [{database}; {offset + 1}-{offset + len(batch)}]",
                    ))
                columns = fetch_external_stage(
                    cfg, database, EXTERNAL_COLUMN_STATS_STAGE_SQL,
                    stage=f"external partition keys [{database}]",
                )
                staged = {
                    "svv_external_tables": catalog,
                    "external_column_stats": columns,
                    "sys_query_history": history,
                    "sys_external_query_detail": segments,
                    "sys_query_detail": pd.concat(step_frames, ignore_index=True) if step_frames else pd.DataFrame(),
                    "sys_external_query_error": pd.DataFrame(),
                }
                frame = assemble_external_table_info(staged)
                frame = stamp_cluster_namespace(frame, cfg)
                frames.append(frame)
                print(f"  external_table_info_all [{database}]: {len(frame):,} rows assembled locally")
        finally:
            cfg.statement_timeout_ms = prior_timeout
        external_info = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if not external_info.empty and "external_table_key" in external_info.columns:
            external_info = external_info.drop_duplicates("external_table_key", keep="first")
        write_tmp_table(
            duckdb_path,
            "external_table_info_all",
            external_info,
            snapshot_id,
            "-- Final external stage; independent Redshift sources; local DuckDB joins\n"
            + external_segments_stage_sql(args.days),
            args.lock_wait_seconds,
        )

    save_state(duckdb_path, args.lock_wait_seconds, {"status": "loaded", "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})

    print(
        "\nDone. All *_tmp tables are loaded; the application never left its live data."
        "\nPreserve the completed load before promotion:"
        f"\n\n    {runner_command('--backup-only', '--duckdb-path', duckdb_path)}"
        "\n\nThen promote it (takes seconds):"
        f"\n\n    {runner_command('--swap', '--duckdb-path', duckdb_path)}\n"
    )
    return 0


def _record_tmp_cluster_registry(duckdb_path: Path, lock_wait: float, snapshot_id: str, configs) -> None:
    con = open_duck(duckdb_path, lock_wait)
    try:
        con.execute(
            "CREATE TABLE IF NOT EXISTS _tmp_snapshot_cluster_runs "
            "(snapshot_id VARCHAR, namespace_id VARCHAR, cluster_role VARCHAR, cluster_name VARCHAR, cluster_host VARCHAR, "
            "primary_database VARCHAR, captured_at TIMESTAMP)"
        )
        tmp_registry_columns = {
            str(row[1]).lower()
            for row in con.execute("PRAGMA table_info('_tmp_snapshot_cluster_runs')").fetchall()
        }
        if "cluster_name" not in tmp_registry_columns:
            con.execute("ALTER TABLE _tmp_snapshot_cluster_runs ADD COLUMN cluster_name VARCHAR")
        con.execute("DELETE FROM _tmp_snapshot_cluster_runs WHERE snapshot_id = ?", [snapshot_id])
        for cfg in configs:
            con.execute(
                "INSERT INTO _tmp_snapshot_cluster_runs "
                "(snapshot_id, namespace_id, cluster_role, cluster_name, cluster_host, primary_database, captured_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [snapshot_id, cfg.namespace_id, cfg.cluster_role, cfg.cluster_name, cfg.host, cfg.primary_database, datetime.now()],
            )
    finally:
        con.close()


def _resolve_multi_run(args, duckdb_path: Path, configs) -> tuple[str, dict[tuple[str, str], int]]:
    """Return resumable snapshot plus completed namespace/table row counts."""
    namespace_ids = ",".join(cfg.namespace_id for cfg in configs)
    con = open_duck(duckdb_path, args.lock_wait_seconds)
    try:
        _ensure_stash_tables(con)
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {NAMESPACE_STATE_TABLE} "
            "(snapshot_id VARCHAR, namespace_id VARCHAR, table_name VARCHAR, source_rows BIGINT, "
            "status VARCHAR, completed_at TIMESTAMP, PRIMARY KEY (snapshot_id, namespace_id, table_name))"
        )
        state = read_state(con)
        resumable = (
            bool(args.resume)
            and state.get("status") == "loading"
            and bool(state.get("snapshot_id"))
            and state.get("days") == str(args.days)
            and state.get("floor_seconds") == str(args.floor_seconds)
            and state.get("floor_basis") == args.floor_basis
            and state.get("namespace_ids") == namespace_ids
        )
        if resumable:
            snapshot_id = state["snapshot_id"]
            prior_catalog_scope = str(
                state.get("catalog_database_scope") or ""
            ).strip()
            if prior_catalog_scope != CATALOG_DATABASE_SCOPE_KEY:
                print(
                    "Catalog database policy changed from "
                    f"{prior_catalog_scope or 'automatic discovery'} to "
                    f"{CATALOG_DATABASE_SCOPE_KEY}; preserving workload "
                    "checkpoints and invalidating only database-cycled "
                    "catalog checkpoints."
                )
                catalog_names = tuple(sorted(DATABASE_CYCLED_TABLES))
                placeholders = ", ".join("?" for _ in catalog_names)
                con.execute(
                    f"DELETE FROM {NAMESPACE_STATE_TABLE} "
                    f"WHERE snapshot_id = ? AND table_name IN ({placeholders})",
                    [snapshot_id, *catalog_names],
                )
                con.execute(
                    f"DELETE FROM {SQL_STASH_TABLE} "
                    f"WHERE table_name IN ({placeholders})",
                    list(catalog_names),
                )
                for table_name in catalog_names:
                    con.execute(
                        f"DROP TABLE IF EXISTS "
                        f"{quote_ident(tmp_name(table_name))}"
                    )
            rows = con.execute(
                f"SELECT namespace_id, table_name, source_rows FROM {NAMESPACE_STATE_TABLE} "
                "WHERE snapshot_id = ? AND status = 'complete'",
                [snapshot_id],
            ).fetchall()
            completed = {(str(ns).lower(), str(table)): int(count or 0) for ns, table, count in rows}
            print(f"Resuming namespaced snapshot {snapshot_id}: {len(completed)} table checkpoint(s) complete.")
            return snapshot_id, completed
        if args.resume and state.get("status") == "loading":
            print("Existing staged load does not match the current namespaces/window; starting a new staged snapshot.")
        # A new snapshot must start with an empty staging area. Parallel loads
        # append namespace slices into shared *_tmp tables, so retaining even
        # one table from an older snapshot would mix snapshot IDs and make a
        # fully completed load impossible to promote.
        for table_name in LIVE_REFRESH_TABLES:
            con.execute(f"DROP TABLE IF EXISTS {quote_ident(tmp_name(table_name))}")
        con.execute("DROP TABLE IF EXISTS _tmp_snapshot_cluster_runs")
        con.execute(f"DELETE FROM {STATE_TABLE}")
        con.execute(f"DELETE FROM {SQL_STASH_TABLE}")
        con.execute(f"DELETE FROM {NAMESPACE_STATE_TABLE}")
        return str(uuid.uuid4()), {}
    finally:
        con.close()


def _mark_namespace_table_complete(
    duckdb_path: Path, lock_wait: float, snapshot_id: str,
    namespace_id: str, table_name: str, source_rows: int,
) -> None:
    con = open_duck(duckdb_path, lock_wait)
    try:
        con.execute(
            f"INSERT OR REPLACE INTO {NAMESPACE_STATE_TABLE} "
            "(snapshot_id, namespace_id, table_name, source_rows, status, completed_at) "
            "VALUES (?, ?, ?, ?, 'complete', ?)",
            [snapshot_id, namespace_id, table_name, int(source_rows), datetime.now()],
        )
    finally:
        con.close()


def _clear_staged_namespace_rows(duckdb_path: Path, lock_wait: float, table_name: str, namespace_id: str) -> None:
    """Remove an uncheckpointed namespace slice before retry to prevent duplicates."""
    con = open_duck(duckdb_path, lock_wait)
    try:
        target = tmp_name(table_name)
        exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE LOWER(table_name) = LOWER(?)",
            [target],
        ).fetchone()[0]
        if exists:
            with contextlib.suppress(Exception):
                con.execute(
                    f"DELETE FROM {quote_ident(target)} WHERE LOWER(namespace_id) = LOWER(?)",
                    [namespace_id],
                )
    finally:
        con.close()


class LoadCatalog:
    """Thread-safe record of everything that failed to load during a run.

    The one-button loader never halts on a per-table or per-cluster failure:
    each problem is appended here and the run continues. A non-empty catalog
    blocks auto-promotion (a partial load must never silently overwrite live
    production data) and is written to disk as load_report.json + .txt.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.failures: list[dict] = []

    def record(self, namespace_id: str, table_name: str, scope: str, error: BaseException) -> None:
        with self._lock:
            self.failures.append({
                "namespace_id": namespace_id,
                "table": table_name,
                "scope": scope,
                "error_type": type(error).__name__,
                "error": _redact_for_catalog(error),
            })

    @property
    def ok(self) -> bool:
        with self._lock:
            return not self.failures

    def snapshot(self) -> list[dict]:
        with self._lock:
            return list(self.failures)


def _redact_for_catalog(value: object) -> str:
    try:
        from analyzer.secrets_store import redact_sensitive_text

        return redact_sensitive_text(str(value))
    except Exception:
        return str(value)


def write_load_report(duckdb_path: Path, catalog: LoadCatalog, configs, snapshot_id: str) -> Path:
    """Write load_report.json + load_report.txt next to the warehouse.

    Returns the path of the human-readable .txt report. The catalog is the
    end-of-run record the one-button loader promises: what loaded clean, and
    every namespace/table that was skipped, without ever halting the run.
    """
    out_dir = Path(duckdb_path).resolve().parent
    failures = catalog.snapshot()
    cluster_labels = [
        getattr(cfg, "cluster_name", None) or getattr(cfg, "namespace_id", "?")
        for cfg in configs
    ]
    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload = {
        "snapshot_id": snapshot_id,
        "finished_at": finished_at,
        "duckdb_path": str(duckdb_path),
        "clusters": cluster_labels,
        "clean": not failures,
        "failure_count": len(failures),
        "external_table_metadata": "included from producer SVV_EXTERNAL_COLUMNS",
        "legacy_external_telemetry": "excluded this version",
        "failures": failures,
    }
    json_path = out_dir / "load_report.json"
    txt_path = out_dir / "load_report.txt"
    with contextlib.suppress(Exception):
        json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "Infraredshift one-button load report",
        f"Finished: {finished_at}",
        f"Snapshot: {snapshot_id}",
        f"Warehouse: {duckdb_path}",
        f"Clusters attempted: {', '.join(cluster_labels) or '(none)'}",
        "External table metadata: included from producer SVV_EXTERNAL_COLUMNS",
        "Legacy external telemetry: excluded this version",
        "",
    ]
    if not failures:
        lines.append("RESULT: CLEAN — every configured namespace/table loaded. Safe to promote.")
    else:
        lines.append(f"RESULT: {len(failures)} item(s) did NOT load (loading continued past each):")
        lines.append("")
        for item in failures:
            lines.append(
                f"  - [{item['scope']}] {item['table']} @ {item['namespace_id']}: "
                f"{item['error_type']}: {item['error']}"
            )
        lines.append("")
        lines.append(
            "Auto-promote was SKIPPED because the load is partial. Live data is untouched.\n"
            "Fix the sources above and resume the safe load. Completed checkpoints are\n"
            "reused; promotion remains blocked until every required checkpoint succeeds."
        )
    with contextlib.suppress(Exception):
        txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return txt_path


def _redshift_read_worker_count(configs) -> int:
    """Return a conservative direct-Redshift reader count.

    ``redshift_connector`` calls are blocking, so independent cluster reads
    overlap efficiently in threads. A worker never receives a connection
    created by another worker: ``fetch_frame`` opens and closes its own
    connection for every query. DuckDB writes remain protected by the one
    writer lock in ``run_multi_load``.
    """
    cluster_count = max(1, len(configs))
    enabled = str(
        os.environ.get("INFRAREDSHIFT_PARALLEL_LOAD", "1")
    ).strip().lower() not in {"0", "false", "no", "off"}
    if not enabled or cluster_count == 1:
        return 1
    try:
        requested = int(
            str(os.environ.get("INFRAREDSHIFT_REDSHIFT_READ_WORKERS", "4")).strip()
        )
    except (TypeError, ValueError):
        requested = 4
    # Four is the operational default; eight is the hard safety ceiling.
    return min(cluster_count, max(1, min(requested, 8)))


def run_multi_load(args, configs) -> int:
    """Capture producer and consumers into one namespaced staging snapshot.

    When more than one cluster is enabled, captures run **in parallel** using
    a conservative direct-reader pool. Redshift I/O overlaps through separate
    per-worker connections; DuckDB writes are serialized with a lock so a
    single warehouse file stays consistent.

    This is the one-button pipeline: it never halts on a per-table or
    per-cluster failure. Each problem is recorded in a LoadCatalog and the
    run continues to the end; a non-empty catalog then blocks auto-promotion
    so a partial load can never silently overwrite live production data.
    """
    duckdb_path = Path(args.duckdb_path)
    refresh_plan = selected_refresh_tables(args)
    refresh_set = set(refresh_plan)
    snapshot_id, completed_tables = _resolve_multi_run(args, duckdb_path, configs)
    workers = _redshift_read_worker_count(configs)
    parallel = workers > 1
    print(f"== Multi-cluster refresh: {len(configs)} cluster(s), one namespaced snapshot ==")
    print(
        f"== Direct Redshift readers: {workers} "
        "(separate connections; DuckDB writes serialized) =="
        if parallel
        else "== Direct Redshift readers: 1 (sequential cluster load) =="
    )
    print(f"== Target file: {duckdb_path} (writes go to *_tmp tables only) ==\n")
    save_state(
        duckdb_path,
        args.lock_wait_seconds,
        {
            "snapshot_id": snapshot_id,
            "label": "multi-cluster tmp refresh",
            "days": str(args.days),
            "floor_seconds": str(args.floor_seconds),
            "floor_basis": args.floor_basis,
            "catalog_database_scope": CATALOG_DATABASE_SCOPE_KEY,
            "status": "loading",
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "namespace_ids": ",".join(cfg.namespace_id for cfg in configs),
            "parallel_workers": str(workers),
            "selected_tables": ",".join(refresh_plan),
            "failure_count": "0",
            "load_report": "",
        },
    )
    _record_tmp_cluster_registry(duckdb_path, args.lock_wait_seconds, snapshot_id, configs)
    external_checkpoints = {
        namespace_id for (namespace_id, table_name) in completed_tables
        if table_name == "external_table_info_all"
    }
    if "external_table_info_all" in refresh_set and not external_checkpoints:
        con = open_duck(duckdb_path, args.lock_wait_seconds)
        try:
            con.execute(f"DROP TABLE IF EXISTS {quote_ident(tmp_name('external_table_info_all'))}")
        finally:
            con.close()

    # A checked/empty-table refresh must never promote a stale *_tmp table
    # left by an older, wider plan. Live production tables are untouched.
    con = open_duck(duckdb_path, args.lock_wait_seconds)
    try:
        for table_name in LIVE_REFRESH_TABLES:
            if table_name not in refresh_set:
                con.execute(f"DROP TABLE IF EXISTS {quote_ident(tmp_name(table_name))}")
    finally:
        con.close()

    progress_tables = list(refresh_plan)
    per_cluster_tables = [
        table for table in progress_tables
        if table != "external_table_metadata"
    ]
    progress_total = max(
        1,
        len(per_cluster_tables) * len(configs)
        + (1 if "external_table_metadata" in refresh_set else 0),
    )
    progress_completed = 0
    progress_lock = threading.Lock()
    write_lock = threading.RLock()
    load_catalog = LoadCatalog()
    if (
        "external_table_metadata" in refresh_set
        and not any(
            str(getattr(cfg, "cluster_role", "")).lower() == "producer"
            for cfg in configs
        )
    ):
        load_catalog.record(
            "producer",
            "external_table_metadata",
            "catalog",
            RuntimeError(
                "External table metadata requires an enabled Producer profile."
            ),
        )

    def bump_progress() -> int:
        nonlocal progress_completed
        with progress_lock:
            progress_completed += 1
            return progress_completed

    def _capture_catalog_tables(cfg, pending_catalogs, append) -> None:
        table_databases = resolve_table_databases(cfg) if pending_catalogs else ()
        # Independent catalog datasets must be independent retry units. In the
        # old block, a table-info or view failure aborted the function before
        # Producer SVV_EXTERNAL_COLUMNS was attempted, so every Resume repeated
        # the same failure loop. External metadata is intentionally first.
        catalog_order = (
            "external_table_metadata",
            "svv_table_info_all",
            "view_definitions",
            "procedure_definitions",
        )
        frames_by_catalog = {
            name: [] for name in catalog_order if name in pending_catalogs
        }
        sql_by_catalog = {
            "external_table_metadata": external_metadata_sql(*external_capture_scope()),
            "svv_table_info_all": TABLE_INFO_SQL,
            "view_definitions": VIEW_DEFINITIONS_SQL,
            "procedure_definitions": PROCEDURE_DEFINITIONS_SQL,
        }
        failed_catalogs: set[str] = set()
        not_applicable_catalogs: set[str] = set()

        def consumer_catalog_database_not_present(
            database: str, exc: BaseException,
        ) -> bool:
            """Recognize the fixed catalog DB being absent on a consumer.

            The producer remains mandatory because it owns external metadata.
            This exemption is deliberately limited to Redshift's invalid
            catalog error for the one emergency-policy database.
            """
            if str(getattr(cfg, "cluster_role", "")).strip().lower() != "consumer":
                return False
            if str(database).strip().lower() not in {
                value.lower() for value in CONSUMER_CATALOG_DATABASE_SCOPE
            }:
                return False
            error = str(exc).lower()
            return (
                "3d000" in error
                or (
                    "database" in error
                    and "enterprise_datawarehouse" in error
                    and "does not exist" in error
                )
            )

        def fail_catalog(catalog_name: str, exc: BaseException) -> None:
            if catalog_name in failed_catalogs:
                return
            failed_catalogs.add(catalog_name)
            load_catalog.record(
                cfg.namespace_id, catalog_name, "catalog", exc,
            )
            done = bump_progress()
            print(
                f"  !! SKIP {catalog_name} [{cfg.namespace_id}]: {exc}",
                flush=True,
            )
            emit_progress(
                cfg.namespace_id,
                catalog_name,
                0,
                0,
                done,
                progress_total,
                f"Retry required — {exc}",
            )

        successful_catalog_databases: set[str] = set()
        skipped_producer_databases: list[tuple[str, str]] = []
        for database in table_databases:
            database_frames: dict[str, object] = {}
            skip_producer_database = False
            for catalog_name in catalog_order:
                if (
                    catalog_name not in frames_by_catalog
                    or catalog_name in failed_catalogs
                ):
                    continue
                action = (
                    "Retrying missing checkpoint"
                    if args.resume and completed_tables
                    else "Loading"
                )
                emit_progress(
                    cfg.namespace_id,
                    catalog_name,
                    0,
                    0,
                    progress_completed,
                    progress_total,
                    f"{action} from {database}",
                )
                try:
                    if catalog_name == "external_table_metadata":
                        frame = fetch_frame(
                            cfg,
                            database,
                            sql_by_catalog["external_table_metadata"],
                            stage=(
                                "external table metadata "
                                f"[{cfg.namespace_id}/{database}]"
                            ),
                        )
                        frame["source_db"] = database
                    elif catalog_name == "svv_table_info_all":
                        frame = fetch_frame(
                            cfg,
                            database,
                            TABLE_INFO_SQL,
                            stage=f"table info [{cfg.namespace_id}/{database}]",
                        )
                        frame["source_db"] = database
                    elif catalog_name == "view_definitions":
                        frame = normalize_view_definitions(
                            fetch_frame(
                                cfg,
                                database,
                                VIEW_DEFINITIONS_SQL,
                                stage=f"views [{cfg.namespace_id}/{database}]",
                            ),
                            database,
                        )
                    else:
                        frame = normalize_procedure_definitions(
                            fetch_frame(
                                cfg,
                                database,
                                PROCEDURE_DEFINITIONS_SQL,
                                stage=f"procedures [{cfg.namespace_id}/{database}]",
                            ),
                            database,
                        )
                    database_frames[catalog_name] = stamp_cluster_namespace(
                        frame, cfg
                    )
                except BaseException as exc:  # noqa: BLE001 — isolate each catalog
                    if consumer_catalog_database_not_present(database, exc):
                        # A consumer without the fixed EDW database has no
                        # applicable catalog rows. Checkpoint every still-
                        # pending consumer catalog as a successful zero-row
                        # result and do not open two more doomed connections.
                        not_applicable_catalogs.update(
                            name
                            for name, frames in frames_by_catalog.items()
                            if name != "external_table_metadata"
                            and not frames
                            and name not in failed_catalogs
                        )
                        print(
                            "  -- Consumer catalog database is not present "
                            f"[{cfg.namespace_id}/{database}]; "
                            "remaining consumer catalog datasets are "
                            "successful zero-row checkpoints.",
                            flush=True,
                        )
                        break
                    if (
                        str(getattr(cfg, "cluster_role", "")).strip().lower()
                        == "producer"
                        and str(database).strip().lower() in {
                            value.lower()
                            for value in PRODUCER_CATALOG_DATABASE_SCOPE
                        }
                    ):
                        # The operator's fixed Producer list can include data
                        # shares that are not connectable Redshift databases.
                        # Treat an error for one named entry as a whole-entry
                        # skip, discard any partial frames from it, and advance.
                        error_summary = str(exc).replace("\r", " ").replace(
                            "\n", " "
                        ).strip()
                        skipped_producer_databases.append(
                            (str(database), error_summary)
                        )
                        skip_producer_database = True
                        print(
                            "  -- SKIP producer entry "
                            f"{database}: unavailable or data share; "
                            f"continuing. Detail: {error_summary}",
                            flush=True,
                        )
                        emit_progress(
                            cfg.namespace_id,
                            catalog_name,
                            0,
                            0,
                            progress_completed,
                            progress_total,
                            f"Skipped {database} — unavailable or data share",
                        )
                        break
                    fail_catalog(catalog_name, exc)
            if not_applicable_catalogs:
                break
            if skip_producer_database:
                continue
            for catalog_name, frame in database_frames.items():
                frames_by_catalog[catalog_name].append(frame)
            if database_frames:
                successful_catalog_databases.add(str(database))

        if (
            str(getattr(cfg, "cluster_role", "")).strip().lower() == "producer"
            and frames_by_catalog
            and not successful_catalog_databases
            and skipped_producer_databases
        ):
            skipped_names = ", ".join(
                database for database, _error in skipped_producer_databases
            )
            for catalog_name in frames_by_catalog:
                fail_catalog(
                    catalog_name,
                    RuntimeError(
                        "No explicit Producer catalog database could be read; "
                        f"skipped: {skipped_names}"
                    ),
                )

        for catalog_name in catalog_order:
            if (
                catalog_name not in frames_by_catalog
                or catalog_name in failed_catalogs
            ):
                continue
            frames = frames_by_catalog[catalog_name]
            try:
                if args.resume:
                    with write_lock:
                        _clear_staged_namespace_rows(
                            duckdb_path,
                            args.lock_wait_seconds,
                            catalog_name,
                            cfg.namespace_id,
                        )
                rows = sum(len(frame) for frame in frames)
                with write_lock:
                    write_tmp_table(
                        duckdb_path,
                        catalog_name,
                        (
                            pd.concat(frames, ignore_index=True)
                            if frames
                            else pd.DataFrame()
                        ),
                        snapshot_id,
                        sql_by_catalog[catalog_name],
                        args.lock_wait_seconds,
                        append=append,
                    )
                    _mark_namespace_table_complete(
                        duckdb_path,
                        args.lock_wait_seconds,
                        snapshot_id,
                        cfg.namespace_id,
                        catalog_name,
                        rows,
                    )
            except BaseException as exc:  # noqa: BLE001 — isolate each catalog
                fail_catalog(catalog_name, exc)
                continue
            done = bump_progress()
            emit_progress(
                cfg.namespace_id,
                catalog_name,
                rows,
                rows,
                done,
                progress_total,
                (
                    CONSUMER_CATALOG_DATABASE_NOT_PRESENT_STATUS
                    if catalog_name in not_applicable_catalogs
                    else "Staged in DuckDB"
                ),
            )

    def load_one_cluster(profile_index: int, cfg) -> None:
        # Parallel clusters always append into shared *_tmp tables (namespace-stamped).
        # Sequential mode keeps historical first-cluster rebuild behavior.
        append = parallel or profile_index > 0
        no_qualifying_queries = (
            completed_tables.get((cfg.namespace_id.lower(), "query_history"))
            == 0
        )
        label = cfg.cluster_name or f"{cfg.cluster_role} {cfg.cluster_ordinal or ''}".strip()
        print(f"\n## START {label}: namespace {cfg.namespace_id} ##", flush=True)
        pending_primary_tables = {
            table_name
            for table_name in QUERY_EVIDENCE_TABLES
            if (
                table_name in refresh_set
                and (cfg.namespace_id.lower(), table_name)
                not in completed_tables
            )
        }
        primary_validation_error: BaseException | None = None
        if pending_primary_tables:
            try:
                validate_primary_sources(cfg)
            except BaseException as exc:  # noqa: BLE001 — catalogs remain independent
                primary_validation_error = exc
                print(
                    f"!! Workload source validation failed for {label} "
                    f"[{cfg.namespace_id}]; independent catalog retries will "
                    f"continue: {exc}",
                    flush=True,
                )
        else:
            print(
                f"  No workload checkpoints pending for {cfg.namespace_id}; "
                "independent catalog retries continue without workload validation.",
                flush=True,
            )

        def capture(table_name: str, target_ids=None) -> None:
            nonlocal no_qualifying_queries
            checkpoint = completed_tables.get((cfg.namespace_id.lower(), table_name))
            if checkpoint is not None:
                if table_name == "query_history" and checkpoint == 0:
                    no_qualifying_queries = True
                done = bump_progress()
                print(f"  {table_name} [{cfg.namespace_id}]: checkpoint complete, skipped", flush=True)
                checkpoint_status = (
                    NO_QUALIFYING_QUERY_STATUS
                    if (
                        no_qualifying_queries
                        and checkpoint == 0
                        and table_name in QUERY_EVIDENCE_TABLES
                    )
                    else "Recovered checkpoint"
                )
                emit_progress(
                    cfg.namespace_id, table_name, checkpoint, checkpoint,
                    done, progress_total, checkpoint_status,
                )
                return
            if primary_validation_error is not None:
                load_catalog.record(
                    cfg.namespace_id,
                    table_name,
                    "table",
                    primary_validation_error,
                )
                done = bump_progress()
                emit_progress(
                    cfg.namespace_id,
                    table_name,
                    0,
                    0,
                    done,
                    progress_total,
                    f"Retry required — source validation: "
                    f"{primary_validation_error}",
                )
                return
            emit_progress(
                cfg.namespace_id, table_name, 0, 0,
                progress_completed, progress_total, "Loading from Redshift",
            )
            try:
                # Redshift fetch outside the DuckDB write lock so clusters overlap.
                if args.resume:
                    with write_lock:
                        _clear_staged_namespace_rows(
                            duckdb_path, args.lock_wait_seconds, table_name, cfg.namespace_id
                        )
                sql = table_sql(cfg, table_name, target_ids=target_ids)
                frame = stamp_cluster_namespace(
                    fetch_frame(cfg, cfg.primary_database, sql, stage=f"{table_name} [{cfg.namespace_id}]"), cfg
                )
                with write_lock:
                    write_tmp_table(
                        duckdb_path, table_name, frame, snapshot_id, sql,
                        args.lock_wait_seconds, append=append,
                    )
                    _mark_namespace_table_complete(
                        duckdb_path, args.lock_wait_seconds, snapshot_id,
                        cfg.namespace_id, table_name, len(frame),
                    )
                if table_name == "query_history" and frame.empty:
                    # Zero threshold-qualified roots is a valid consumer
                    # outcome. Its empty evidence checkpoints are complete,
                    # promotable, and deliberately shown as yellow in the UI.
                    no_qualifying_queries = True
            except BaseException as exc:  # noqa: BLE001 — never halt; record and move on
                load_catalog.record(cfg.namespace_id, table_name, "table", exc)
                done = bump_progress()
                print(f"  !! SKIP {table_name} [{cfg.namespace_id}]: {exc}", flush=True)
                emit_progress(
                    cfg.namespace_id, table_name, 0, 0,
                    done, progress_total, f"Skipped after error: {exc}",
                )
                return
            done = bump_progress()
            completion_status = (
                NO_QUALIFYING_QUERY_STATUS
                if (
                    no_qualifying_queries
                    and frame.empty
                    and table_name in QUERY_EVIDENCE_TABLES
                )
                else "Staged in DuckDB"
            )
            emit_progress(
                cfg.namespace_id, table_name, len(frame), len(frame),
                done, progress_total, completion_status,
            )

        for table_name in ROOT_ORDER:
            if table_name in refresh_set:
                capture(table_name)

        target_ids = None
        if {"query_history", "query_text"} <= refresh_set:
            try:
                with write_lock:
                    con = open_duck(duckdb_path, args.lock_wait_seconds)
                    try:
                        target_ids = compute_parent_target_ids(
                            con, snapshot_id, cfg.evidence_parent_limit, cfg.floor_basis,
                            tmp_name("query_history"), tmp_name("query_text"), cfg.namespace_id,
                        ) or None
                    finally:
                        con.close()
                print(f"  {len(target_ids or [])} representative parent query id(s) for namespace {cfg.namespace_id}", flush=True)
            except BaseException as exc:  # noqa: BLE001 — degrade to no target ids, keep loading
                load_catalog.record(cfg.namespace_id, "(representative parent ids)", "cluster", exc)
                target_ids = None
                print(f"  !! parent-id computation failed [{cfg.namespace_id}], continuing without it: {exc}", flush=True)
        for table_name in EXTRACTIONS:
            if table_name not in ROOT_TABLE_SET and table_name in refresh_set:
                capture(table_name, target_ids)

        catalog_candidates = []
        if (
            str(getattr(cfg, "cluster_role", "")).lower() == "producer"
            and "external_table_metadata" in refresh_set
        ):
            catalog_candidates.append("external_table_metadata")
        catalog_candidates.extend(
            ("svv_table_info_all", "view_definitions", "procedure_definitions")
        )
        catalog_names = tuple(
            name for name in catalog_candidates if name in refresh_set
        )
        for catalog_name in catalog_names:
            checkpoint = completed_tables.get((cfg.namespace_id.lower(), catalog_name))
            if checkpoint is not None:
                done = bump_progress()
                emit_progress(
                    cfg.namespace_id, catalog_name, checkpoint, checkpoint,
                    done, progress_total, "Recovered checkpoint",
                )
        pending_catalogs = {
            name for name in catalog_names
            if (cfg.namespace_id.lower(), name) not in completed_tables
        }
        _capture_catalog_tables(cfg, pending_catalogs, append)
        print(f"## DONE {label}: namespace {cfg.namespace_id} ##", flush=True)

    if parallel:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="cluster-load") as pool:
            futures = {
                pool.submit(load_one_cluster, index, cfg): cfg
                for index, cfg in enumerate(configs)
            }
            for future in concurrent.futures.as_completed(futures):
                cfg = futures[future]
                try:
                    future.result()
                except BaseException as exc:  # noqa: BLE001 — never halt; catalog and continue
                    label = getattr(cfg, "cluster_name", None) or getattr(cfg, "namespace_id", "?")
                    print(f"!! Cluster failed ({label}): {exc}", flush=True)
                    load_catalog.record(getattr(cfg, "namespace_id", "?"), "(whole cluster)", "cluster", exc)
    else:
        for profile_index, cfg in enumerate(configs):
            try:
                load_one_cluster(profile_index, cfg)
            except BaseException as exc:  # noqa: BLE001 — never halt; catalog and continue
                label = getattr(cfg, "cluster_name", None) or getattr(cfg, "namespace_id", "?")
                print(f"!! Cluster failed ({label}): {exc}", flush=True)
                load_catalog.record(getattr(cfg, "namespace_id", "?"), "(whole cluster)", "cluster", exc)

    if "external_table_info_all" in refresh_set:
        print("\n== FINAL GLOBAL PHASE: external-table metadata for all namespaces ==")
        print("All producer/consumer workload and catalog tables are already checkpointed.")
    # External stage also runs in parallel across clusters (fetch-heavy).
    def load_external_one(cfg) -> None:
        nonlocal progress_completed
        table_name = "external_table_info_all"
        checkpoint = completed_tables.get((cfg.namespace_id.lower(), table_name))
        if checkpoint is not None:
            done = bump_progress()
            emit_progress(
                cfg.namespace_id, table_name, checkpoint, checkpoint,
                done, progress_total, "Recovered checkpoint",
            )
            return
        if args.resume:
            with write_lock:
                _clear_staged_namespace_rows(
                    duckdb_path, args.lock_wait_seconds, table_name, cfg.namespace_id
                )
        databases = resolve_table_databases(cfg)
        prior_timeout = int(getattr(cfg, "statement_timeout_ms", 0) or 0)
        cfg.statement_timeout_ms = 600_000
        namespace_frames = []
        try:
            for database in databases:
                emit_progress(
                    cfg.namespace_id, table_name, 0, 0,
                    progress_completed, progress_total,
                    f"External metadata: {database}",
                )
                catalog = fetch_external_stage(
                    cfg, database, EXTERNAL_CATALOG_STAGE_SQL,
                    stage=f"external catalog [{cfg.namespace_id}/{database}]",
                )
                segments = fetch_external_stage(
                    cfg, database, external_segments_stage_sql(args.days),
                    stage=f"external scan segments [{cfg.namespace_id}/{database}]",
                )
                if catalog.empty:
                    catalog = minimal_external_catalog_from_segments(segments, database)
                query_ids = external_query_ids(segments)
                history = pd.DataFrame({
                    "query_id": query_ids,
                    "database_name": [database] * len(query_ids),
                })
                step_frames = []
                for offset in range(0, len(query_ids), 100):
                    batch = query_ids[offset:offset + 100]
                    step_frames.append(fetch_external_stage(
                        cfg, database, external_steps_stage_sql(batch),
                        stage=(
                            f"external output metrics [{cfg.namespace_id}/{database}; "
                            f"{offset + 1}-{offset + len(batch)}]"
                        ),
                    ))
                columns = fetch_external_stage(
                    cfg, database, EXTERNAL_COLUMN_STATS_STAGE_SQL,
                    stage=f"external partition keys [{cfg.namespace_id}/{database}]",
                )
                staged = {
                    "svv_external_tables": catalog,
                    "external_column_stats": columns,
                    "sys_query_history": history,
                    "sys_external_query_detail": segments,
                    "sys_query_detail": pd.concat(step_frames, ignore_index=True) if step_frames else pd.DataFrame(),
                    "sys_external_query_error": pd.DataFrame(),
                }
                namespace_frames.append(stamp_cluster_namespace(
                    assemble_external_table_info(staged), cfg
                ))
        finally:
            cfg.statement_timeout_ms = prior_timeout
        external_info = (
            pd.concat(namespace_frames, ignore_index=True)
            if namespace_frames else pd.DataFrame()
        )
        with write_lock:
            write_tmp_table(
                duckdb_path, table_name, external_info, snapshot_id,
                "-- Final global external stage; independent sources; local DuckDB joins\n"
                + external_segments_stage_sql(args.days),
                args.lock_wait_seconds, append=True,
            )
            _mark_namespace_table_complete(
                duckdb_path, args.lock_wait_seconds, snapshot_id,
                cfg.namespace_id, table_name, len(external_info),
            )
        done = bump_progress()
        emit_progress(
            cfg.namespace_id, table_name, len(external_info), len(external_info),
            done, progress_total, "Staged in DuckDB",
        )

    if "external_table_info_all" in refresh_set:
        if parallel:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="external-load") as pool:
                list(pool.map(load_external_one, configs))
        else:
            for cfg in configs:
                load_external_one(cfg)

    report_path = write_load_report(duckdb_path, load_catalog, configs, snapshot_id)
    failures = load_catalog.snapshot()

    if failures:
        # Partial load: leave status "loading" so run_swap's status=="loaded"
        # gate blocks any auto-promotion. Live production data stays untouched.
        # Checkpoints are preserved, so a rerun resumes and can still complete.
        print(
            f"\n== LOAD FINISHED WITH {len(failures)} SKIPPED ITEM(S) — nothing was halted ==\n"
        )
        for item in failures:
            print(f"  - [{item['scope']}] {item['table']} @ {item['namespace_id']}: "
                  f"{item['error_type']}: {item['error']}")
        print(
            f"\nFull catalog written to:\n    {report_path}\n"
            "\nAuto-promote was SKIPPED (partial load never overwrites live data)."
            "\nFix the sources above, then resume the safe load. Completed "
            "checkpoints will be reused and promotion remains blocked until "
            "every required checkpoint succeeds.\n"
        )
        save_state(
            duckdb_path,
            args.lock_wait_seconds,
            {
                "failure_count": str(len(failures)),
                "load_report": str(report_path),
            },
        )
        return 0

    save_state(
        duckdb_path, args.lock_wait_seconds,
        {
            "status": "loaded",
            "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "failure_count": "0",
            "load_report": str(report_path),
        },
    )
    print(
        "\nDone. CLEAN LOAD — every configured namespace/table staged in *_tmp tables."
        f"\nReport: {report_path}"
        f"\nPromote after review:\n\n    {runner_command('--swap', '--duckdb-path', duckdb_path)}\n"
    )
    return 0


# ------------------------------------------------------------------ swap mode


def _backup_file(duckdb_path: Path, lock_wait: float) -> Path:
    con = open_duck(duckdb_path, lock_wait)
    try:
        con.execute("CHECKPOINT")
    except Exception as exc:
        raise SystemExit(
            f"DuckDB CHECKPOINT failed; backup and promotion were aborted: {exc}"
        ) from exc
    finally:
        con.close()
    backup_dir = duckdb_path.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = backup_dir / f"{duckdb_path.stem}.before-tmp-swap.{stamp}{duckdb_path.suffix}"
    shutil.copy2(duckdb_path, backup_path)
    if backup_path.stat().st_size != duckdb_path.stat().st_size:
        raise SystemExit(
            f"Backup verification failed: {backup_path}. The swap has NOT started; keep the source file unchanged."
        )
    return backup_path


def run_backup_only(args) -> int:
    duckdb_path = Path(args.duckdb_path)
    print("Creating a checkpointed preservation copy. No tables will be renamed or deleted ...")
    backup_path = _backup_file(duckdb_path, args.lock_wait_seconds)
    print(f"  source preserved: {duckdb_path}")
    print(f"  backup verified:  {backup_path} ({backup_path.stat().st_size:,} bytes)")
    print("\nBackup-only complete. The production and *_tmp tables were not changed.")
    print(f"\nWhen ready, use:\n\n    {runner_command('--swap', '--duckdb-path', duckdb_path)}")
    return 0


def _snapshot_from_tmp(con, ready) -> str:
    for table_name in ready:
        try:
            row = con.execute(
                f"SELECT snapshot_id FROM {quote_ident(tmp_name(table_name))} "
                "WHERE snapshot_id IS NOT NULL LIMIT 1"
            ).fetchone()
        except Exception:
            continue
        if row and row[0]:
            return str(row[0])
    return ""


def staging_checkpoint_progress(con, state: dict, planned) -> tuple[int, int, list[str]]:
    """Return completed count, required count, and missing checkpoint labels."""
    snapshot_id = str(state.get("snapshot_id") or "").strip()
    namespace_ids = {
        value.strip().lower()
        for value in str(state.get("namespace_ids") or "").split(",")
        if value.strip()
    }
    if not snapshot_id:
        return 0, 0, ["staging snapshot id is missing"]
    if not namespace_ids:
        return 0, 0, ["staging namespace list is missing"]
    try:
        completed = {
            (str(namespace_id).strip().lower(), str(table_name).strip())
            for namespace_id, table_name in con.execute(
                f"SELECT namespace_id, table_name FROM {NAMESPACE_STATE_TABLE} "
                "WHERE snapshot_id = ? AND status = 'complete'",
                [snapshot_id],
            ).fetchall()
        }
    except Exception:
        return 0, 0, ["namespace checkpoint table is missing"]
    try:
        producer_namespaces = {
            str(namespace_id).strip().lower()
            for namespace_id, in con.execute(
                "SELECT namespace_id FROM _tmp_snapshot_cluster_runs "
                "WHERE snapshot_id = ? AND LOWER(cluster_role) = 'producer'",
                [snapshot_id],
            ).fetchall()
            if str(namespace_id or "").strip()
        }
    except Exception:
        producer_namespaces = set()

    expected: set[tuple[str, str]] = set()
    identity_gaps: list[str] = []
    for table_name in planned:
        expected_namespaces = (
            producer_namespaces
            if table_name == "external_table_metadata"
            else namespace_ids
        )
        if not expected_namespaces:
            identity_gaps.append(
                f"{table_name}: producer checkpoint identity is missing"
            )
            continue
        for namespace_id in sorted(expected_namespaces):
            expected.add((namespace_id, table_name))
    missing_pairs = sorted(expected - completed, key=lambda value: (value[1], value[0]))
    gaps = identity_gaps + [
        f"{table_name}: {namespace_id}"
        for namespace_id, table_name in missing_pairs
    ]
    # An unresolved producer identity is itself one required checkpoint, so a
    # broken profile can never appear as 100% complete.
    total = len(expected) + len(identity_gaps)
    done = len(expected & completed)
    return done, total, gaps


def _staging_checkpoint_gaps(con, state: dict, planned) -> list[str]:
    """Return missing namespace checkpoints for an interrupted finalization."""
    return staging_checkpoint_progress(con, state, planned)[2]


def run_swap(args) -> int:
    duckdb_path = Path(args.duckdb_path)
    if not args.no_backup:
        print("Backing up the DuckDB file before the swap ...")
        print(f"  backup written: {_backup_file(duckdb_path, args.lock_wait_seconds)}")

    con = open_duck(duckdb_path, args.lock_wait_seconds)
    try:
        con.execute(
            "CREATE TABLE IF NOT EXISTS snapshot_runs "
            "(snapshot_id VARCHAR PRIMARY KEY, captured_at TIMESTAMP, label VARCHAR, source VARCHAR)"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS analyzer_source_sql (table_name VARCHAR, snapshot_id VARCHAR, "
            "sql_hash VARCHAR, sql_text VARCHAR, recorded_at TIMESTAMP, source VARCHAR, "
            "PRIMARY KEY (table_name, snapshot_id))"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS snapshot_cluster_runs "
            "(snapshot_id VARCHAR, namespace_id VARCHAR, cluster_role VARCHAR, cluster_name VARCHAR, cluster_host VARCHAR, "
            "primary_database VARCHAR, captured_at TIMESTAMP, "
            "PRIMARY KEY (snapshot_id, namespace_id))"
        )
        registry_columns = {
            str(row[1]).lower()
            for row in con.execute("PRAGMA table_info('snapshot_cluster_runs')").fetchall()
        }
        if "cluster_name" not in registry_columns:
            con.execute("ALTER TABLE snapshot_cluster_runs ADD COLUMN cluster_name VARCHAR")
        state = read_state(con)
        existing = {
            str(row[0]).lower()
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
            ).fetchall()
        }
        selected_state = tuple(
            value.strip() for value in str(state.get("selected_tables") or "").split(",")
            if value.strip()
        )
        planned = selected_state or LIVE_REFRESH_TABLES
        missing = [table for table in planned if tmp_name(table).lower() not in existing]
        if missing and selected_state:
            raise SystemExit(
                "Staged load is incomplete; promotion was aborted. Missing: "
                + ", ".join(f"{table}_tmp" for table in missing)
            )
        if state.get("status") != "loaded":
            try:
                failure_count = int(state.get("failure_count", "0") or 0)
            except ValueError:
                failure_count = 0
            if failure_count:
                raise SystemExit(
                    f"The staged load has {failure_count} incomplete item(s); "
                    "promotion was aborted and no live tables were changed. "
                    "Resume the Infraredshift loader first."
                )
            gaps = _staging_checkpoint_gaps(con, state, planned)
            if gaps:
                raise SystemExit(
                    "The staged load is not complete; promotion was aborted and "
                    "no live tables were changed. Missing checkpoint(s): "
                    + ", ".join(gaps[:12])
                    + (f" and {len(gaps) - 12} more" if len(gaps) > 12 else "")
                )
            con.execute(f"DELETE FROM {STATE_TABLE} WHERE state_key = 'status'")
            con.execute(f"INSERT INTO {STATE_TABLE} VALUES ('status', 'loaded')")
            state["status"] = "loaded"
            print(
                "Recovered a complete staged snapshot from its namespace "
                "checkpoints; promotion validation may continue."
            )
        ready = [t for t in planned if tmp_name(t).lower() in existing]
        if not ready:
            raise SystemExit(
                f"No *_tmp tables found. No tables were changed. Load command: "
                f"{runner_command('--duckdb-path', duckdb_path)}"
            )

        snapshot_id = state.get("snapshot_id") or _snapshot_from_tmp(con, ready)
        if not snapshot_id:
            raise SystemExit("Could not determine the snapshot id of the *_tmp load; aborting swap.")
        label = state.get("label") or "tmp refresh"

        # A leftover *_tmp from an older aborted run must never ride along into
        # production: promote only tmp tables stamped with THIS load's snapshot.
        vetted: list[str] = []
        leftovers: list[str] = []
        for table_name in ready:
            try:
                stamped_ids = {
                    str(row[0])
                    for row in con.execute(
                        f"SELECT DISTINCT snapshot_id FROM {quote_ident(tmp_name(table_name))}"
                    ).fetchall()
                    if row[0]
                }
            except Exception:
                # No snapshot_id column at all: not staged by this loader.
                leftovers.append(table_name)
                continue
            if stamped_ids == {snapshot_id}:
                vetted.append(table_name)
            elif not stamped_ids:
                # Zero rows carry no snapshot stamp. Trust it only when the
                # load plan explicitly named the table; an unattributable empty
                # tmp must not erase a live table.
                if table_name in selected_state:
                    vetted.append(table_name)
                else:
                    leftovers.append(table_name)
            elif table_name in selected_state:
                raise SystemExit(
                    f"Staged table {tmp_name(table_name)} carries snapshot "
                    f"{', '.join(sorted(stamped_ids))} but this load is {snapshot_id}; "
                    "promotion was aborted and no live tables were changed. "
                    "Re-run the Infraredshift loader to restage it."
                )
            else:
                leftovers.append(table_name)
        if leftovers:
            print(
                "Skipping leftover staged table(s) not part of this load: "
                + ", ".join(tmp_name(t) for t in leftovers)
            )
        ready = vetted
        if not ready:
            raise SystemExit(
                "Every staged *_tmp table belongs to a different load; promotion was "
                "aborted and no live tables were changed. Re-run the Infraredshift loader."
            )

        print(f"Swapping {len(ready)} table(s) into production (snapshot {snapshot_id}) ...")
        con.execute("BEGIN TRANSACTION")
        try:
            for table_name in ready:
                con.execute(f"DROP TABLE IF EXISTS {quote_ident(table_name)}")
                con.execute(
                    f"ALTER TABLE {quote_ident(tmp_name(table_name))} RENAME TO {quote_ident(table_name)}"
                )
            # Stamp with the swap moment: the app picks its latest snapshot by
            # captured_at, and promotion is when this data goes live.
            con.execute(
                "INSERT OR REPLACE INTO snapshot_runs (snapshot_id, captured_at, label, source) VALUES (?, ?, ?, ?)",
                [snapshot_id, datetime.now(), label, "runner-tmp-refresh"],
            )
            try:
                stash = con.execute(f"SELECT table_name, sql_text FROM {SQL_STASH_TABLE}").fetchall()
            except Exception:
                stash = []
            for table_name, sql_text in stash:
                if str(table_name) in ready:
                    con.execute(
                        "INSERT OR REPLACE INTO analyzer_source_sql "
                        "(table_name, snapshot_id, sql_hash, sql_text, recorded_at, source) VALUES (?, ?, ?, ?, ?, ?)",
                        [str(table_name), snapshot_id, sql_hash(str(sql_text or "")), str(sql_text or ""), datetime.now(), "runner-tmp-refresh"],
                    )
            try:
                registry_rows = con.execute(
                    "SELECT snapshot_id, namespace_id, cluster_role, cluster_name, cluster_host, "
                    "primary_database, captured_at FROM _tmp_snapshot_cluster_runs "
                    "WHERE snapshot_id = ?",
                    [snapshot_id],
                ).fetchall()
            except Exception:
                registry_rows = []
            for registry_row in registry_rows:
                con.execute(
                    "INSERT OR REPLACE INTO snapshot_cluster_runs "
                    "(snapshot_id, namespace_id, cluster_role, cluster_name, cluster_host, primary_database, captured_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    list(registry_row),
                )
            con.execute(f"DROP TABLE IF EXISTS {STATE_TABLE}")
            con.execute(f"DROP TABLE IF EXISTS {SQL_STASH_TABLE}")
            con.execute(f"DROP TABLE IF EXISTS {NAMESPACE_STATE_TABLE}")
            con.execute("DROP TABLE IF EXISTS _tmp_snapshot_cluster_runs")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        # Align schemas and rebuild the analyzer's canonical performance
        # indexes on the freshly renamed tables (renamed tables carry none).
        for table_name in ready:
            ensure_table(con, table_name, EXPECTED_COLUMNS.get(table_name, ()))
        ensure_indexes(con)
        try:
            con.execute("CHECKPOINT")
        except Exception:
            pass

        print("\nSwap complete. Production tables now hold the new load:")
        for table_name in ready:
            rows = con.execute(f"SELECT COUNT(*) FROM {quote_ident(table_name)}").fetchone()[0]
            print(f"  {table_name}: {int(rows or 0):,} row(s)")
        indexed = {
            str(row[0])
            for row in con.execute("SELECT DISTINCT table_name FROM duckdb_indexes()").fetchall()
        }
        print(f"\nIndexes rebuilt: {sum(1 for t in ready if t in indexed)}/{len(ready)} swapped table(s) carry performance indexes.")
    finally:
        con.close()
    return 0


# ---------------------------------------------------------------- status mode


def run_status(args) -> int:
    duckdb_path = Path(args.duckdb_path)
    con = open_duck(duckdb_path, args.lock_wait_seconds)
    try:
        state = read_state(con)
        if state:
            print(f"Load status: {state.get('status', 'unknown')} (started {state.get('started_at', '?')})")
            floor_value = state.get("floor_seconds", "?")
            floor_display = (
                "per-cluster"
                if str(floor_value).strip().lower() in {"", "none"}
                else f"{floor_value}s"
            )
            print(
                f"Window: {state.get('days', '?')} day(s), floor "
                f"{floor_display} on {state.get('floor_basis', '?')}\n"
            )
            planned = tuple(
                value.strip()
                for value in str(state.get("selected_tables") or "").split(",")
                if value.strip()
            )
            if planned:
                completed, required, gaps = staging_checkpoint_progress(
                    con, state, planned
                )
                print(
                    f"Required dataset checkpoints: {completed}/{required} complete"
                )
                if gaps:
                    print("Missing checkpoints:")
                    for gap in gaps[:20]:
                        print(f"  - {gap}")
                    if len(gaps) > 20:
                        print(f"  - and {len(gaps) - 20} more")
                else:
                    print("Checkpoint validation: complete")
                print()
        existing = {
            str(row[0]).lower()
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
            ).fetchall()
        }
        any_tmp = False
        for table_name in LIVE_REFRESH_TABLES:
            if (
                table_name == "external_table_info_all"
                and not EXTERNAL_CAPTURE_ENABLED
            ):
                continue
            target = tmp_name(table_name)
            if target.lower() not in existing:
                print(f"  {target}: not loaded yet")
                continue
            any_tmp = True
            rows = con.execute(f"SELECT COUNT(*) FROM {quote_ident(target)}").fetchone()[0]
            print(f"  {target}: {int(rows or 0):,} row(s)")
        if not any_tmp:
            print(
                "\nNo staged tables exist. Open Infraredshift and click Start Safe Load "
                "on the Data Loader tab."
            )
    finally:
        con.close()
    return 0


# ----------------------------------------------------------------------- main


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refresh every analyzer DuckDB table through *_tmp duplicates, then swap. Standalone."
    )
    parser.add_argument("--duckdb-path", default=None, help="Analyzer DuckDB file (default: analyzer's own location).")
    parser.add_argument("--days", type=float, default=DEFAULT_DAYS, help=f"Capture window in days (default {DEFAULT_DAYS:g}).")
    parser.add_argument(
        "--floor-seconds", type=float, default=None,
        help=(
            "Load-wide minimum query runtime override. Default: per-cluster "
            "(producer 300s, consumers 30s); FLOOR_SECONDS in the cluster "
            "profiles JSON overrides everything."
        ),
    )
    parser.add_argument("--floor-basis", choices=("execution_time", "elapsed_time"), default="execution_time")
    parser.add_argument(
        "--lock-wait-seconds", type=float, default=DEFAULT_LOCK_WAIT_SECONDS,
        help="How long each short write burst waits for the app to release the DuckDB file.",
    )
    parser.add_argument("--resume", action="store_true", help="Continue an interrupted load, skipping tables that already finished.")
    parser.add_argument("--swap", action="store_true", help="Promote loaded *_tmp tables over the production tables.")
    parser.add_argument("--status", action="store_true", help="Show *_tmp load progress and exit.")
    parser.add_argument(
        "--backup-only", action="store_true",
        help="Checkpoint and copy the DuckDB file without renaming, deleting, or swapping any tables.",
    )
    parser.add_argument("--no-backup", action="store_true", help="Skip the pre-swap backup copy of the DuckDB file.")
    args = parser.parse_args()

    _load_dotenv_if_present()
    args.duckdb_path = args.duckdb_path or os.environ.get("REDSHIFT_DUCKDB_PATH") or str(resolve_default_duckdb_path())

    print("== Environment check ==")
    problems = sense_environment(args)
    if problems:
        print("This machine is not ready yet. Fix the following, then run this script again:\n")
        for number, problem in enumerate(problems, start=1):
            print(f"  {number}. {problem}")
        return 2
    print("  OK - environment ready.\n")

    selected_modes = sum(bool(value) for value in (args.swap, args.status, args.backup_only))
    if selected_modes > 1:
        raise SystemExit("Pick exactly one of --swap, --status, or --backup-only.")
    if args.backup_only:
        return run_backup_only(args)
    if args.status:
        return run_status(args)
    if args.swap:
        return run_swap(args)
    return run_load(args)


if __name__ == "__main__":
    sys.exit(main())
