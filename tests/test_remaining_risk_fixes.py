"""Regression tests for remaining assessment weaknesses closed on branch grok."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analyzer.cluster_analyze import _build_query_issue_heatmap  # noqa: E402
from analyzer.fix_script import _runnable_maintenance_sql  # noqa: E402
from analyzer.manual_import import import_table_file  # noqa: E402
from analyzer.query_similarity import diagnose_repeat_query_candidates  # noqa: E402


def test_vacuum_sort_only_forced_bare_vacuum_rejected():
    assert _runnable_maintenance_sql("VACUUM SORT public.orders") == "VACUUM SORT ONLY public.orders;"
    assert _runnable_maintenance_sql("VACUUM public.orders") == ""
    assert _runnable_maintenance_sql("ANALYZE public.orders") == "ANALYZE public.orders;"


def test_query_issue_heatmap_built_from_slow_queries():
    slow = pd.DataFrame(
        [
            {"query_id": "101", "risk_score": 90, "dominant_issue": "BAD_DISTRIBUTION"},
            {"query_id": "102", "risk_score": 40, "dominant_issue": "SCAN_HEAVY"},
        ]
    )
    heat = _build_query_issue_heatmap(slow)
    assert not heat.empty
    assert set(heat.columns) >= {"query_id", "family", "severity_rank"}
    assert heat["severity_rank"].max() >= 2


def test_diagnose_accepts_procedure_definitions_without_error():
    slow = pd.DataFrame(
        [
            {
                "query_id": "1",
                "sql_text": "CALL analytics.report_daily(current_date)",
                "elapsed_s": 10,
                "user_name": "u",
                "database_name": "db1",
            },
            {
                "query_id": "2",
                "sql_text": "CALL analytics.report_daily(current_date - 1)",
                "elapsed_s": 12,
                "user_name": "u",
                "database_name": "db1",
            },
        ]
    )
    procs = pd.DataFrame(
        [
            {
                "database_name": "db1",
                "schema_name": "analytics",
                "procedure_name": "report_daily",
                "definition": "BEGIN SELECT 1; END",
            }
        ]
    )
    diag = diagnose_repeat_query_candidates(slow, procedure_definitions=procs)
    assert "repeat_diagnostic_note" in diag
    assert diag["repeat_raw_query_rows"] == 2


def test_manual_import_defaults_to_append(tmp_path, monkeypatch):
    # Confirm the public default is append-safe (multi-file won't wipe prior).
    import inspect

    sig = inspect.signature(import_table_file)
    assert sig.parameters["append"].default is True
