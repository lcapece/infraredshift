"""How many runs make a query "repeating".

The threshold is applied inside DuckDB, BEFORE any SQL is parsed. A query
dropped there never reaches grouping, so the grouping-time minimum alone
could not change what the screen shows.
"""
from __future__ import annotations

import os

import duckdb
import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from analyzer.cluster_analyze import _repeating_slow_queries_sql
from analyzer.settings import DEFAULT_REPEAT_MIN_GROUP_SIZE, AnalyzerSettings


def _workload() -> duckdb.DuckDBPyConnection:
    """A=2 runs, B=3, C=4, D=1 (a genuine one-off)."""
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE base(query_id BIGINT, user_name VARCHAR, sql_text VARCHAR,"
        " risk_score DOUBLE, elapsed_s DOUBLE)"
    )
    rows = []
    for text, runs in (("SELECT 1", 2), ("SELECT 2", 3), ("SELECT 3", 4), ("SELECT 9", 1)):
        for _ in range(runs):
            rows.append((len(rows) + 1, "u1", text, 1.0, 1.0))
    con.executemany("INSERT INTO base VALUES (?,?,?,?,?)", rows)
    con.execute("CREATE VIEW v_slow_queries AS SELECT * FROM base")
    return con


def _kept(con, min_runs: int) -> set[str]:
    sql = _repeating_slow_queries_sql("1=1", "1=1", set(), min_runs=min_runs)
    return set(con.execute(sql).df()["sql_text"])


def test_the_default_is_three_runs():
    """Two runs of a shape in a week is often coincidence; three is a habit."""
    assert DEFAULT_REPEAT_MIN_GROUP_SIZE == 3
    assert AnalyzerSettings().repeat_min_group_size == 3


def test_the_threshold_selects_exactly_what_it_says():
    con = _workload()
    try:
        assert _kept(con, 2) == {"SELECT 1", "SELECT 2", "SELECT 3"}
        assert _kept(con, 3) == {"SELECT 2", "SELECT 3"}
        assert _kept(con, 4) == {"SELECT 3"}
        assert _kept(con, 5) == set()
    finally:
        con.close()


def test_a_one_off_query_is_always_dropped():
    con = _workload()
    try:
        for minimum in (2, 3, 4):
            assert "SELECT 9" not in _kept(con, minimum)
    finally:
        con.close()


def test_every_repeat_rule_honours_the_threshold():
    """Exact text, same-user affix shape, and the SYS query hash each gate
    separately - a threshold applied to only some of them would leak."""
    sql = _repeating_slow_queries_sql("1=1", "1=1", {"generic_query_hash"}, min_runs=7)

    assert "_exact_repeats >= 7" in sql
    assert "_shape_repeats >= 7" in sql
    assert "_hash_repeats >= 7" in sql
    assert "> 1" not in sql, "no rule may keep the old hardcoded threshold"


def test_the_setting_is_bounded_below_at_two():
    """A query that ran once cannot repeat."""
    from analyzer.settings import _bounded_int

    assert _bounded_int(1, DEFAULT_REPEAT_MIN_GROUP_SIZE, minimum=2) >= 2
    assert _bounded_int(0, DEFAULT_REPEAT_MIN_GROUP_SIZE, minimum=2) >= 2


def test_changing_the_threshold_invalidates_the_cached_grouping():
    import pandas as pd

    import analyzer.cluster_analyze as module

    frame = pd.DataFrame([{"query_id": 1, "sql_text": "SELECT 1"}])
    empty = pd.DataFrame()
    three = module._repeat_cache_key(
        "snap", AnalyzerSettings(repeat_min_group_size=3), frame, empty, empty
    )
    five = module._repeat_cache_key(
        "snap", AnalyzerSettings(repeat_min_group_size=5), frame, empty, empty
    )

    assert three != five


def test_the_settings_dialog_exposes_the_control():
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])
    _ = app
    from analyzer.widgets.cluster_dashboard import _ConfigDialog

    dialog = _ConfigDialog("test.duckdb")

    assert hasattr(dialog, "_min_runs")
    assert dialog._min_runs.minimum() == 2
    assert dialog._min_runs.value() >= 2
