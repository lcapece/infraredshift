from __future__ import annotations

import pandas as pd

from analyzer.spectrum_optimizer import assess_spectrum_tables
from analyzer.widgets.external_tables import _filter_optimization_rows, _metric_severity


def _base_row(**overrides) -> dict:
    row = {
        "external_table_key": "dev.spectrum.events",
        "schema_name": "spectrum",
        "table_name": "events",
        "query_count": 3,
        "gross_scan_gb": 5.0,
        "gross_output_gb": 4.0,
        "external_duration_s": 90.0,
        "total_partitions_considered": 100,
        "qualified_partitions_scanned": 5,
        "partition_pruning_pct": 95.0,
        "partition_key_count": 1,
        "partition_key_columns": "event_date",
        "avg_files_per_segment": 10,
        "scanned_files": 30,
        "s3list_time_ms": 30,
        "row_filter_efficiency_pct": 20.0,
        "output_metric_match_count": 3,
        "warning_event_count": 0,
        "sampled_error_count": 0,
        "external_spill_blocks": 0,
        "observed_file_format": "PARQUET",
        "table_parameters": "{'numRows':'1000000'}",
    }
    row.update(overrides)
    return row


def test_healthy_columnar_table_is_monitor_only() -> None:
    assessed = assess_spectrum_tables(pd.DataFrame([_base_row()])).iloc[0]
    assert assessed["optimization_priority"] == "Healthy"
    assert assessed["optimization_actionable"] is False or not assessed["optimization_actionable"]
    assert assessed["primary_action_code"] == "MONITOR"


def test_poor_format_pruning_and_file_fanout_produce_critical_queue_item() -> None:
    assessed = assess_spectrum_tables(pd.DataFrame([_base_row(
        query_count=30,
        gross_scan_gb=750.0,
        gross_output_gb=5.0,
        external_duration_s=9000,
        partition_pruning_pct=12.0,
        qualified_partitions_scanned=88,
        avg_files_per_segment=1400,
        scanned_files=180000,
        s3list_time_ms=90000,
        row_filter_efficiency_pct=99.0,
        output_metric_match_count=30,
        observed_file_format="JSON",
        table_parameters="{}",
    )])).iloc[0]

    assert assessed["optimization_priority"] == "Critical"
    assert assessed["optimization_score"] == 100
    assert "COLUMNAR_FORMAT" in assessed["recommendation_codes"]
    assert "PARTITION_PREDICATE" in assessed["recommendation_codes"]
    assert "FILE_LAYOUT" in assessed["recommendation_codes"]
    assert "MATERIALIZE_OR_STAGE" in assessed["recommendation_codes"]
    assert "<verified_row_count>" in assessed["review_sql"]
    assert _metric_severity(assessed, "optimization") == 2


def test_no_scan_activity_is_not_an_optimization_failure() -> None:
    assessed = assess_spectrum_tables(pd.DataFrame([_base_row(query_count=0, gross_scan_gb=0)])).iloc[0]
    assert assessed["optimization_priority"] == "No Activity"
    assert not assessed["optimization_actionable"]
    assert _metric_severity(assessed, "optimization") == 3


def test_queue_focus_matches_secondary_recommendations() -> None:
    assessed = assess_spectrum_tables(pd.DataFrame([
        _base_row(
            query_count=15,
            gross_scan_gb=250,
            gross_output_gb=2,
            partition_pruning_pct=20,
            observed_file_format="JSON",
            table_parameters="{}",
        ),
        _base_row(table_name="healthy"),
    ]))
    assert len(_filter_optimization_rows(assessed, focus="statistics")) == 1
    assert len(_filter_optimization_rows(assessed, focus="format")) == 1
    assert len(_filter_optimization_rows(assessed, focus="all")) == 1
