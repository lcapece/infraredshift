"""Critical Table Insights scenarios.

Each scenario measures physical design against the observed workload. The
recommendations must be checkable, so every row carries the numbers behind
its verdict, and nothing is proposed on the evidence of a single query.
"""
from __future__ import annotations

import os

import pandas as pd
import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from analyzer.table_insights import (
    LARGE_TABLE_ROW_THRESHOLD,
    MIN_PATTERNS_FOR_RECOMMENDATION,
    SCENARIOS,
    _bare_column,
    column_demand,
    run_scenario,
    sort_key_fit_score,
)


def _groups() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "repeat_group_id": "RQ001", "sql_tables": "mart.fact_orders",
                "sql_filter_columns": "f.event_date, region",
                "sql_join_columns": "f.customer_id",
                "query_count": 500, "total_runtime_s": 36_000.0,
            },
            {
                "repeat_group_id": "RQ002", "sql_tables": "mart.fact_orders",
                "sql_filter_columns": "event_date",
                "sql_join_columns": "customer_id",
                "query_count": 200, "total_runtime_s": 7_200.0,
            },
        ]
    )


def _tables(sortkey: str = "load_date", diststyle: str = "EVEN") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "schema": "mart", "table": "fact_orders",
                "tbl_rows": 2_000_000_000, "size": 40_000,
                "sortkey1": sortkey, "diststyle": diststyle,
                "unsorted": 30.0, "stats_off": 25.0,
            }
        ]
    )


def test_table_aliases_are_stripped_from_column_names():
    """"f.event_date" and "event_date" are the same physical column.

    Counting them separately splits the evidence exactly where it needs to
    accumulate, and puts an alias no DDL can use into the proposed SORTKEY.
    """
    assert _bare_column("f.event_date") == "event_date"
    assert _bare_column("event_date") == "event_date"

    demand = column_demand(_groups())["fact_orders"]
    assert "event_date" in demand
    assert not any("." in column for column in demand)


def test_sort_key_fit_scores_a_matching_key_highest():
    score, verdict = sort_key_fit_score("event_date", ["event_date", "region"])

    assert score == 100
    assert "most-filtered" in verdict


def test_sort_key_fit_penalises_an_unused_key():
    score, verdict = sort_key_fit_score("load_date", ["event_date", "region"])

    assert score == 0
    assert "never filtered" in verdict


def test_a_missing_sort_key_scores_zero_and_says_what_to_use():
    score, verdict = sort_key_fit_score("AUTO(SORTKEY)", ["event_date"])

    assert score == 0
    assert "event_date" in verdict


def test_sort_key_scenario_ranks_three_predicates_and_proposes_a_key():
    result = run_scenario("sort_key_fit", groups=_groups(), table_review=_tables())

    assert len(result.rows) == 1
    row = result.rows.iloc[0]
    # event_date is filtered by both patterns and weighted higher than a join.
    assert row["predicate_1"] == "event_date"
    assert row["proposed_sortkey"].startswith("SORTKEY(event_date")
    assert row["fit_score"] == 0
    assert "load_date" in row["verdict"]


def test_a_column_used_by_one_pattern_is_not_recommended():
    """One query is an anecdote, not a workload."""
    groups = _groups().head(1)

    result = run_scenario("sort_key_fit", groups=groups, table_review=_tables())

    assert result.rows.empty
    assert "more than one pattern" in result.note


def test_small_tables_are_not_assessed():
    small = _tables()
    small["tbl_rows"] = LARGE_TABLE_ROW_THRESHOLD - 1

    result = run_scenario("sort_key_fit", groups=_groups(), table_review=small)

    assert result.rows.empty


def test_the_heat_map_column_vocabulary_is_understood():
    """The same facts arrive as table/schema/size or table_name/schema_name/
    size_mb. Reading only one vocabulary produced zero rows from a fully
    populated frame."""
    renamed = _tables().rename(
        columns={"table": "table_name", "schema": "schema_name", "size": "size_mb"}
    )

    result = run_scenario("sort_key_fit", groups=_groups(), table_review=renamed)

    assert len(result.rows) == 1
    assert result.rows.iloc[0]["table"] == "fact_orders"


def test_distribution_scenario_flags_an_even_table_that_is_joined():
    result = run_scenario("distribution_fit", groups=_groups(), table_review=_tables())

    assert len(result.rows) == 1
    row = result.rows.iloc[0]
    assert row["join_column"] == "customer_id"
    assert row["proposed_distkey"] == "DISTKEY(customer_id)"


def test_a_keyed_table_is_not_flagged_for_distribution():
    result = run_scenario(
        "distribution_fit",
        groups=_groups(),
        table_review=_tables(diststyle="KEY(customer_id)"),
    )

    assert result.rows.empty


def test_maintenance_debt_weights_by_workload_not_just_size():
    """Debt on a table nothing queries is not urgent."""
    result = run_scenario("maintenance_debt", groups=_groups(), table_review=_tables())

    assert len(result.rows) == 1
    row = result.rows.iloc[0]
    assert row["action"] == "VACUUM + ANALYZE"
    assert row["captured_runtime_h"] > 0


def test_spectrum_scenarios_explain_that_capture_is_opt_in():
    for key in ("spectrum_scanned", "spectrum_unpartitioned"):
        result = run_scenario(key, external_tables=pd.DataFrame())
        assert "opt-in" in result.note.lower() or "--external-tables" in result.note


def test_spectrum_ranks_by_gigabytes_scanned():
    external = pd.DataFrame(
        [
            {"external_table_key": "s.small", "gross_scan_gb": 5.0, "query_count": 3,
             "partition_key_count": 1},
            {"external_table_key": "s.huge", "gross_scan_gb": 700.0, "query_count": 40,
             "partition_key_count": 0},
        ]
    )

    result = run_scenario("spectrum_scanned", external_tables=external)

    assert list(result.rows["external_table_key"]) == ["s.huge", "s.small"]


def test_spectrum_unpartitioned_only_lists_actively_used_tables():
    external = pd.DataFrame(
        [
            {"external_table_key": "s.used", "gross_scan_gb": 700.0, "query_count": 40,
             "partition_key_count": 0},
            {"external_table_key": "s.idle", "gross_scan_gb": 0.0, "query_count": 0,
             "partition_key_count": 0},
            {"external_table_key": "s.keyed", "gross_scan_gb": 90.0, "query_count": 9,
             "partition_key_count": 2},
        ]
    )

    result = run_scenario("spectrum_unpartitioned", external_tables=external)

    assert list(result.rows["external_table_key"]) == ["s.used"]
    assert "NO partition key" in result.headline


def test_unknown_scenario_raises():
    with pytest.raises(KeyError):
        run_scenario("not-a-scenario")


def test_the_tab_is_mounted_and_every_scenario_runs():
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])
    _ = app
    from analyzer.app import MainWindow
    from analyzer.widgets.table_insights import TableInsightsPage

    window = MainWindow()
    labels = [window._tabs.tabText(i) for i in range(window._tabs.count())]
    assert "Critical Table Insights" in labels
    assert isinstance(window._table_insights, TableInsightsPage)

    page = TableInsightsPage()
    for index in range(page._scenario.count()):
        page._scenario.setCurrentIndex(index)
        assert page._headline.text(), "every scenario must say something"
    assert page._scenario.count() == len(SCENARIOS)
    window.deleteLater()
