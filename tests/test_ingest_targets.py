"""Regression tests for phase-2 parent evidence targeting."""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analyzer.ingest_redshift import (  # noqa: E402
    INCREMENTAL_DEDUPE_KEYS,
    LIVE_REFRESH_TABLES,
    ROOT_TABLES,
    compute_parent_target_ids,
)
from analyzer.duckdb_store import DuckDBStore  # noqa: E402
from analyzer.redshift_queries import (  # noqa: E402
    SOURCE_REQUIREMENTS,
    child_query_text_sql,
    query_history_sql,
    query_text_sql,
)


def _connection():
    con = duckdb.connect(":memory:")
    con.execute(
        """
CREATE TABLE query_history (
  snapshot_id VARCHAR,
  query_id BIGINT,
  start_time VARCHAR,
  execution_time BIGINT,
  elapsed_time BIGINT,
  query_text VARCHAR
)
"""
    )
    con.execute(
        """
CREATE TABLE query_text (
  snapshot_id VARCHAR,
  query_id BIGINT,
  sequence BIGINT,
  sequence_num BIGINT,
  text VARCHAR,
  sql_text VARCHAR,
  query_text VARCHAR
)
"""
    )
    return con


def test_parent_target_limit_zero_keeps_every_parent_pattern():
    con = _connection()
    con.executemany(
        "INSERT INTO query_history VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("snap", 101, "2026-07-01 00:00:00", 10, 10, "SELECT * FROM sales WHERE d = '2026-07-01'"),
            ("snap", 102, "2026-07-02 00:00:00", 30, 30, "SELECT * FROM sales WHERE d = '2026-07-02'"),
            ("snap", 201, "2026-07-03 00:00:00", 20, 20, "SELECT * FROM refunds WHERE d = '2026-07-01'"),
        ],
    )

    assert compute_parent_target_ids(con, "snap", limit=0) == [102, 201]


def test_parent_target_positive_limit_is_optional_cap():
    con = _connection()
    con.executemany(
        "INSERT INTO query_history VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("snap", 101, "2026-07-01 00:00:00", 10, 10, "SELECT * FROM sales WHERE d = '2026-07-01'"),
            ("snap", 102, "2026-07-02 00:00:00", 30, 30, "SELECT * FROM sales WHERE d = '2026-07-02'"),
            ("snap", 201, "2026-07-03 00:00:00", 20, 20, "SELECT * FROM refunds WHERE d = '2026-07-01'"),
        ],
    )

    assert compute_parent_target_ids(con, "snap", limit=1) == [102]


def test_parent_targets_incremental_high_water_uses_date_and_query_id():
    con = _connection()
    con.executemany(
        "INSERT INTO query_history VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("snap", 101, "2026-07-01 00:00:00", 10, 10, "SELECT * FROM sales WHERE d = '2026-07-01'"),
            ("snap", 102, "2026-07-02 00:00:00", 30, 30, "SELECT * FROM sales WHERE d = '2026-07-02'"),
            ("snap", 103, "2026-07-02 00:00:00", 40, 40, "SELECT * FROM sales WHERE d = '2026-07-03'"),
            ("snap", 201, "2026-07-03 00:00:00", 20, 20, "SELECT * FROM refunds WHERE d = '2026-07-01'"),
        ],
    )

    assert compute_parent_target_ids(
        con,
        "snap",
        limit=0,
        incremental_after_time="2026-07-02 00:00:00",
        incremental_after_query_id=102,
    ) == [103, 201]


def test_incremental_root_sql_filters_by_date_and_query_id():
    history_sql = query_history_sql(
        min_execution_seconds=30,
        incremental_after_time="2026-07-07 11:11:00",
        incremental_after_query_id=123456,
    )
    text_sql = query_text_sql(
        min_execution_seconds=30,
        incremental_after_time="2026-07-07 11:11:00",
        incremental_after_query_id=123456,
    )
    child_text_sql = child_query_text_sql(
        min_execution_seconds=30,
        incremental_after_time="2026-07-07 11:11:00",
        incremental_after_query_id=123456,
    )

    assert "h.start_time > TIMESTAMP '2026-07-07 11:11:00'" in history_sql
    assert "h.query_id > 123456" in history_sql
    assert "FROM sys_query_history h" in text_sql
    assert "h.start_time > TIMESTAMP '2026-07-07 11:11:00'" in text_sql
    assert "FROM sys_child_query_text cqt" in child_text_sql
    assert "FROM sys_query_history h" in child_text_sql
    assert "h.start_time > TIMESTAMP '2026-07-07 11:11:00'" in child_text_sql


def test_child_query_text_is_a_regular_root_dataset():
    assert "child_query_text" in ROOT_TABLES
    assert "child_query_text" in LIVE_REFRESH_TABLES
    assert INCREMENTAL_DEDUPE_KEYS["child_query_text"] == (
        "query_id",
        "child_query_sequence",
        "sequence",
    )
    assert SOURCE_REQUIREMENTS["sys_child_query_text"] == {
        "user_id",
        "query_id",
        "child_query_sequence",
        "sequence",
        "text",
    }


def test_child_query_text_view_reassembles_fragments_per_child():
    store = DuckDBStore(":memory:")
    with store.connect() as con:
        run = store.new_snapshot("child text")
        store.record_snapshot(con, run)
        store.replace_table_from_frame(
            con,
            "child_query_text",
            pd.DataFrame(
                [
                    {"user_id": 100, "query_id": 42, "child_query_sequence": 1, "sequence": 1, "text": " 1"},
                    {"user_id": 100, "query_id": 42, "child_query_sequence": 2, "sequence": 0, "text": "CREATE TEMP TABLE t AS SELECT 2"},
                    {"user_id": 100, "query_id": 42, "child_query_sequence": 1, "sequence": 0, "text": "SELECT"},
                ]
            ),
            run,
        )

        rows = con.execute(
            """
SELECT query_id, child_query_sequence, child_sql_text, fragment_count
FROM v_child_query_text
ORDER BY child_query_sequence
"""
        ).fetchall()

    assert rows == [
        (42, 1, "SELECT 1", 2),
        (42, 2, "CREATE TEMP TABLE t AS SELECT 2", 1),
    ]


def test_incremental_append_dedupes_query_rows_by_snapshot_and_query_id():
    store = DuckDBStore(":memory:")
    with store.connect() as con:
        run = store.new_snapshot("test")
        store.record_snapshot(con, run)
        store.append_table_from_frame(
            con,
            "query_history",
            pd.DataFrame([{"query_id": 1, "start_time": "2026-07-07 00:00:00"}]),
            run,
            dedupe_keys=("query_id",),
        )
        store.append_table_from_frame(
            con,
            "query_history",
            pd.DataFrame([{"query_id": 1, "start_time": "2026-07-08 00:00:00"}]),
            run,
            dedupe_keys=("query_id",),
        )

        rows = con.execute("SELECT query_id, start_time FROM query_history").fetchall()

    assert rows == [("1", "2026-07-08 00:00:00")]
