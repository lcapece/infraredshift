"""Cluster-wide DuckDB dashboard widgets."""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import contextlib
import io
import threading
from pathlib import Path

import pandas as pd
from PySide6.QtCore import (
    QAbstractTableModel,
    QEvent,
    QModelIndex,
    QObject,
    QPointF,
    QProcess,
    QPropertyAnimation,
    QRectF,
    Qt,
    QThread,
    QTimer,
    Signal,
)
from PySide6.QtGui import QBrush, QColor, QFont, QPainter, QPainterPath, QPen, QTextCharFormat, QTextCursor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QAbstractScrollArea,
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QFrame,
    QGraphicsOpacityEffect,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMenu,
    QMessageBox,
    QPlainTextEdit,
    QProgressDialog,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QScrollBar,
    QSlider,
    QSpinBox,
    QSizePolicy,
    QSplitter,
    QStackedWidget,
    QTabWidget,
    QTableView,
    QTableWidget,
    QTableWidgetItem,
    QTextBrowser,
    QTextEdit,
    QToolButton,
    QToolTip,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
    QWidgetAction,
)

from ..cluster_analyze import ClusterReport, REPORT_AREA_CHOICES, REPORT_AREA_LABELS
from ..duckdb_store import (
    DuckDBStore,
    EXPECTED_COLUMNS,
    INSIGHT_RULE_COUNT,
    default_duckdb_path,
)
from ..fix_script import build_fix_script
from ..md_render import apply_markdown
from ..manual_import import import_table_file
from ..structural_recommendations import (
    build_structural_recommendation_script,
    build_structural_recommendations,
)
from ..settings import (
    load_settings,
    resolve_source_cluster_config,
    save_settings,
    settings_path,
    source_cluster_configured,
    source_cluster_endpoint_key,
    source_cluster_fingerprint,
    source_cluster_summary,
)
from ..join_size_highlight import alias_map
from ..sql_lens import SQLLensAnalysis, analyze_console_sql
from ..plan_evidence import attach_join_plan_evidence
from ..query_optimizer import build_friendly_fix, optimization_report_text, optimize_redshift_sql
from ..sql_xray import (
    build_table_lookup,
    build_view_map,
    comparison_at,
    comparison_popup_text,
    explode_views,
    explode_views_recursive_with_spans,
    resolve_footprint,
    resolve_table,
    table_popup_text,
    token_at,
)
from ..theme import PALETTE
from .table_heatmap import TableHeatMap
from .triage_home import TriagePage


def _boolean_env_value(value: object, *, default: bool) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return bool(default)
    if text in {"1", "true", "yes", "y", "on", "checked"}:
        return True
    if text in {"0", "false", "no", "n", "off", "unchecked"}:
        return False
    return bool(default)


def _update_dotenv_keys(path: Path, updates: dict[str, str]) -> None:
    """Atomically update non-secret flags and scrub migrated credentials."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = path.read_text(encoding="utf-8-sig") if path.is_file() else ""
    rendered = _updated_dotenv_text(text, updates)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(rendered)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _updated_dotenv_text(text: str, updates: dict[str, str]) -> str:
    from ..secrets_store import is_secret_key

    forbidden = sorted(key for key in updates if is_secret_key(key))
    if forbidden:
        raise ValueError(
            "Credential keys cannot be written to .env: " + ", ".join(forbidden)
        )
    lines = str(text).splitlines()
    remaining = dict(updates)
    output: list[str] = []
    for line in lines:
        match = re.match(r"^(\s*(?:export\s+)?)([A-Za-z_][A-Za-z0-9_]*)(\s*=).*$", line)
        if match and is_secret_key(match.group(2)):
            # The login migration has already preserved these values in the
            # encrypted per-user .secrets file. Never retain a second
            # plaintext copy when Settings writes non-secret configuration.
            continue
        if match and match.group(2) in remaining:
            key = match.group(2)
            output.append(f"{match.group(1)}{key}{match.group(3)}{remaining.pop(key)}")
        else:
            output.append(line)
    if remaining and output and output[-1].strip():
        output.append("")
    if remaining:
        output.append("# Cluster load selection (managed by Infraredshift)")
        output.extend(f"{key}={value}" for key, value in remaining.items())
    return "\n".join(output).rstrip() + "\n"


TABLE_SIZE_ROW_COL = "size_row_count"
TABLE_DIST_SORT_COL = "dist_sort_keys"
TABLE_SORTED_PCT_COL = "sorted_pct"


def _scroll_guard(widget: QWidget) -> QScrollArea:
    scroll = QScrollArea()
    scroll.setObjectName("PageScrollGuard")
    scroll.setWidgetResizable(True)
    scroll.setFrameShape(QFrame.NoFrame)
    scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
    scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
    scroll.setWidget(widget)
    return scroll


HEAT_RANGE_COLUMNS = {
    "severity_score",
    "elapsed_s",
    "risk_score",
    "repeat_group_runtime_s",
    "repeat_group_size",
    "plan_node_count",
    "tables_touched",
    "input_rows",
    "output_rows",
    "total_spill",
    "remote_io_ratio",
    "max_data_skewness",
    "max_time_skewness",
    "dist_total_cnt",
    "dist_both_cnt",
    "bcast_cnt",
    "external_duration_pct",
    "s3_scan_cnt",
    "table_attention_score",
    "table_risk_score",
    "full_scan_score",
    "distribution_usage_score",
    "sort_attention_score",
    "slow_query_count",
    "slow_query_runtime_s",
    "redistribution_query_count",
    "broadcast_query_count",
    "skewed_query_count",
    "avg_scan_duration_s",
    "scan_input_rows_m",
    "query_count",
    "total_runtime_s",
    "worst_runtime_s",
    "avg_runtime_s",
    "impact_score",
    "repeat_priority_score",
}

DISPLAY_COLUMN_LABELS = {
    TABLE_SIZE_ROW_COL: "Size / Row Cnt",
    TABLE_DIST_SORT_COL: "Dist / Sort Keys",
    TABLE_SORTED_PCT_COL: "Sorted Pct",
    "avg_scan_duration_s": "Avg Scan Min",
    "elapsed_s": "Elapsed Time",
    "full_explain_available": "Full Plan Available",
    "distribution_signal": "Distribution Signal",
    "join_scope": "Join Scope",
    "source_definition": "View SQL",
    "avg_runtime_s": "Avg Runtime",
    "total_runtime_s": "Total Runtime",
    "worst_runtime_s": "Worst Runtime",
    "insight_id": "Rule ID",
    "target_type": "Target Type",
    "target_label": "Target",
    "metric_label": "Metric",
    "metric_display": "Metric Value",
    "impact_score": "Impact Score",
    "impact_band": "Priority",
    "action_count": "Actions",
    "critical_count": "Critical",
    "warning_count": "Warning",
    "top_score": "Top Score",
    "top_subject": "Top Subject",
    "top_reason": "Why Now",
    "slow_query_count": "Slow Query Count",
    "slow_query_runtime_s": "Slow Query Run Time (Second)",
    "redistribution_query_count": "Redistribution Query Count",
    "broadcast_query_count": "Broadcast Query Count",
    "dataset": "Dataset",
    "last_capture": "Last Capture",
    "source_info": "Source Info",
    "indexed_cols": "Indexed Cols",
    "database_name": "Database",
    "schema_count": "Schemas",
    "table_count": "Tables",
    "view_count": "Views",
    "procedure_count": "Sprocs",
    "skew_rows": "Skew Rows",
}


QUERY_COLS = [
    "cluster_name", "namespace_id", "query_id", "database_name", "user_name", "elapsed_s", "risk_score",
    "dominant_issue", "cost_tier", "total_spill", "remote_io_ratio",
    "max_data_skewness", "max_time_skewness", "tables_touched",
    "input_rows", "output_rows",
]

SEVERITY_QUERY_COLS = [
    "severity_score", "severity_reason", "cluster_name", "namespace_id", "query_id", "database_name",
    "user_name", "repeat_group_id", "repeat_group_size",
    "repeat_group_runtime_s", "repeat_similarity_score", "elapsed_s",
    "risk_score", "dominant_issue", "cost_tier",
    "sql_parse_status", "sql_table_count", "sql_join_count",
    "sql_predicate_count", "sql_cte_count", "sql_wildcard_count",
    "sql_join_columns", "sql_filter_columns", "sql_projected_columns",
    "total_spill", "remote_io_ratio", "max_data_skewness",
    "max_time_skewness", "dist_total_cnt", "dist_both_cnt", "bcast_cnt",
    "has_nested_loop", "external_duration_pct", "s3_scan_cnt",
    "tables_touched", "input_rows", "output_rows", "sql_tables",
]

SLOW_QUERY_LIST_COLS = [
    "severity_score", "cluster_name", "namespace_id", "query_id", "database_name", "user_name", "elapsed_s",
    "risk_score", "severity_reason", "dominant_issue", "repeat_group_id",
    "full_explain_available", "plan_node_count", "repeat_group_size",
    "repeat_group_runtime_s", "tables_touched",
    "input_rows", "output_rows",
]

SLOW_QUERY_ROLLUP_COLS = [
    "repeat_group_id", "pattern_runs", "pattern_total_elapsed_s",
    "pattern_avg_elapsed_s", "severity_score", "severity_reason",
    "dominant_issue", "pattern_databases", "pattern_users", "query_id",
]

SLOW_QUERY_TREE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("_tree_label", "Repeat Group / Query ID"),
    ("database_name", "Database"),
    ("user_name", "User"),
    ("severity_score", "Severity / Avg"),
    ("elapsed_s", "Elapsed / Avg"),
    ("cluster_name", "Cluster"),
    ("namespace_id", "Namespace"),
    ("risk_score", "Risk / Avg"),
    ("plan_node_count", "Plan Nodes / Avg"),
    ("tables_touched", "Tables / Avg"),
    ("input_rows", "Input Rows / Avg"),
    ("output_rows", "Output Rows / Avg"),
    ("total_spill", "Spill / Avg"),
    ("dominant_issue", "Dominant Issue"),
)

TABLE_COLS = [
    "cluster_name", "namespace_id", "source_db", "schema_name", "table_name", TABLE_SIZE_ROW_COL,
    TABLE_DIST_SORT_COL, "table_risk_score", "unsorted_pct",
    "stats_off", "skew_rows", "vacuum_sort_benefit", "risk_event",
]

INSIGHT_COLS = [
    "severity", "family", "title", "target_type", "target_label",
    "metric_label", "metric_display", "impact_score", "impact_band",
    "evidence", "recommendation", "insight_id", "scope",
]

REWRITE_COLS = [
    "opportunity_no", "severity", "namespace_id", "title", "subject", "impact_score",
    "trigger", "rewrite_shape", "why_it_matters", "candidate_sql",
]

ACTION_COLS = [
    "priority_rank", "severity", "namespace_id", "action_type", "subject", "action_score",
    "what_to_do", "why_now", "evidence", "sql_hint",
]

REPEAT_GROUP_COLS = [
    "repeat_priority_score", "repeat_verdict", "impact_summary", "fix_hint",
    "repeat_group_id", "repeat_group_key", "query_count", "similarity_label", "avg_similarity",
    "parse_success_rate", "total_runtime_s", "worst_runtime_s",
    "elapsed_s", "execution_s", "queue_s", "planning_s", "compile_s",
    "avg_risk_score", "max_risk_score", "query_type", "repeat_kind",
    "example_query_ids", "procedure_key", "table_count", "join_count",
    "predicate_count", "cte_count", "wildcard_count", "users", "databases",
    "repeat_match_basis", "repeat_constraint_key", "sql_length_min",
    "sql_length_max", "sql_length_avg", "predicate_operator_signature",
    "shared_tables", "sql_tables", "sql_join_columns", "sql_filter_columns",
    "sql_projected_columns", "sql_order_columns", "sql_group_columns",
    "sql_joins", "sql_predicates", "sql_predicate_operators",
    "query_ids", "bridge_query_count", "bridge_snapshot_ids", "bridge_query_ids",
    "representative_sql", "procedure_definition", "sample_sql", "sql_shape",
    "sql_tables_full", "mixed_query_class", "uses_view",
]

REPEAT_MEMBER_COLS = [
    "repeat_group_id", "repeat_group_key", "member_rank", "shown_in_tree", "query_id", "match_verdict",
    "snapshot_id", "namespace_id", "bridge_key", "similarity_score", "elapsed_s", "risk_score", "user_name", "database_name",
    "query_type", "repeat_kind", "procedure_key", "sql_length", "start_time",
    "dominant_issue", "sql_parse_status", "sql_table_count",
    "sql_join_count", "sql_predicate_count", "sql_cte_count",
    "sql_wildcard_count", "sql_tables", "sql_join_columns",
    "sql_filter_columns", "sql_projected_columns", "sql_order_columns",
    "sql_group_columns", "sql_joins", "sql_predicates", "sql_predicate_operators",
    "sql_text", "sql_shape",
]

TABLE_IMPACT_COLS = [
    "cluster_name", "namespace_id", "source_db", "schema_name", "table_name", "blast_radius_score",
    "slow_query_count", "avg_runtime_s", "total_runtime_s", "worst_runtime_s",
    "redistribution_query_count", "broadcast_query_count",
    "skewed_query_count", "table_risk_score", "query_ids",
]

TABLE_BLAST_QUERY_IDS_HEADER = "Query IDs  (click on to COPY list of queries to clipboard)"

TABLE_REVIEW_COLS = [
    "cluster_name", "namespace_id", "source_db", "schema_name", "table_name", TABLE_SIZE_ROW_COL,
    TABLE_DIST_SORT_COL, TABLE_SORTED_PCT_COL, "stats_off", "skew_rows",
    "vacuum_sort_benefit", "table_attention_score",
    "full_scan_score", "distribution_usage_score", "sort_key_usage_score",
    "sort_attention_score", "scan_query_count", "non_rrscan_query_count",
    "rrscan_query_count", "full_scan_query_pct", "rrscan_query_pct",
    "avg_scan_duration_s", "scan_input_rows_m", "slow_query_count",
    "slow_query_runtime_s", "redistribution_query_count",
    "broadcast_query_count", "skewed_query_count",
]

COLUMN_TOOLTIPS = {
    "source_db": "Source database where this query or table metadata was captured.",
    "schema_name": "Schema that owns the object.",
    "table_name": "Table or object name.",
    "query_id": "Redshift query id for the captured statement.",
    "user_name": "Database user that ran the query.",
    "database_name": "Database where the query ran.",
    "start_time": "Captured query start time when available.",
    TABLE_SIZE_ROW_COL: "Physical table size and row count from Redshift table metadata.",
    TABLE_DIST_SORT_COL: "Distribution style/key and leading sort key. Yellow means one side is missing; red means both are missing.",
    TABLE_SORTED_PCT_COL: "Estimated sorted percentage. Higher values usually improve zone-map pruning.",
    "stats_off": "Redshift statistics staleness percentage. Higher values mean ANALYZE is more likely needed.",
    "skew_rows": "Row distribution skew across slices. Higher values mean less even distribution.",
    "vacuum_sort_benefit": "Estimated benefit from VACUUM SORT based on table metadata.",
    "table_attention_score": "Combined table priority score from size, physical design, scans, and workload evidence.",
    "full_scan_score": "Pressure from full scans and non-range-restricted scans against this table.",
    "distribution_usage_score": "Pressure from redistribution, broadcast, and distribution skew evidence.",
    "sort_key_usage_score": "How effectively the workload appears to use the table sort key.",
    "sort_attention_score": "Priority from missing or ineffective sort-key behavior and unsorted data.",
    "scan_query_count": "Captured query count that scanned this table.",
    "non_rrscan_query_count": "Scans that were not range-restricted and likely read too broadly.",
    "rrscan_query_count": "Range-restricted scans that likely benefited from sort-key pruning.",
    "full_scan_query_pct": "Share of scans that behaved like broad/full scans.",
    "rrscan_query_pct": "Share of scans that were range-restricted.",
    "avg_scan_duration_s": "Average table scan duration in minutes for captured evidence.",
    "scan_input_rows_m": "Approximate scanned input rows in millions.",
    "slow_query_count": "Slow captured queries associated with this table.",
    "slow_query_runtime_s": "Total slow-query runtime associated with this table, in seconds.",
    "redistribution_query_count": "Queries involving redistribution pressure for this table.",
    "broadcast_query_count": "Queries involving broadcast join pressure for this table.",
    "skewed_query_count": "Queries showing skew evidence tied to this table.",
    "blast_radius_score": "Overall workload impact score for the table.",
    "avg_runtime_s": "Average runtime for related slow queries, in seconds.",
    "total_runtime_s": "Total runtime for related slow queries, in seconds.",
    "worst_runtime_s": "Worst related query runtime, in seconds.",
    "table_risk_score": "Physical table risk score from metadata and workload symptoms.",
    "query_ids": "Related query ids contributing to the impact row.",
    "severity_score": "Combined slow-query severity score.",
    "risk_score": "Query risk score from runtime, skew, spill, movement, and scan symptoms.",
    "elapsed_s": "Elapsed query runtime in seconds.",
    "dominant_issue": "Highest-weight issue detected for the query.",
    "cost_tier": "Runtime or cost bucket used for triage.",
    "repeat_group_id": "Parent repeat pattern id for similar normalized queries.",
    "repeat_group_size": "Number of captured runs in the repeat pattern.",
    "repeat_group_runtime_s": "Total runtime of captured runs in the repeat pattern.",
    "repeat_priority_score": "Priority score for attacking the repeated pattern.",
    "impact_score": "Priority score for the insight or action row.",
    "predicate_role": "Whether this equality/predicate is acting as a filter or an implicit join condition.",
}

TABLE_STATUS_COLS = [
    "coverage_status", "table_name", "index_status", "index_count", "indexed_columns",
    "record_count", "latest_snapshot_rows", "snapshot_count", "latest_captured_at",
    "sql_status", "source_query",
]

SQL_LENS_TABLE_COLS = [
    "object_type", "query_table", "alias", "component_of", "role",
    "match_status", "statement_table_score", "source_db", "schema_name",
    "table_name", TABLE_SIZE_ROW_COL, TABLE_DIST_SORT_COL,
    "stats_off", "unsorted_pct", "table_attention_score",
    "full_scan_score", "distribution_usage_score", "sort_attention_score",
    "scan_query_count", "full_scan_query_pct", "rrscan_query_pct",
    "recommendation", "source_definition",
]

SQL_LENS_JOIN_COLS = [
    "join_no", "join_signal", "join_type", "target_table", "target_alias", "involved_tables",
    "aliases", "left_physical_sources", "right_physical_sources", "physical_column_pairs",
    "physical_lineage_status", "join_columns", "column_pairs", "distribution_alignment", "severity",
    "recommendation", "condition",
]

SQL_LENS_PREDICATE_COLS = [
    "predicate_no", "predicate_signal", "clause", "predicate_type", "predicate_role", "involved_tables",
    "aliases", "physical_sources", "physical_lineage_status", "columns", "sortkey_alignment", "severity", "recommendation",
    "condition",
]

SQL_LENS_REPEAT_COLS = [
    "similarity_score", "repeat_verdict", "query_id", "repeat_group_id",
    "elapsed_s", "risk_score", "user_name", "database_name",
    "dominant_issue", "sql_text",
]

SQL_LENS_STEP_COLS = [
    "rank", "category", "severity", "title", "why", "next_step",
]

SLOW_LINEAGE_OBJECT_COLS = [
    "object_type", "query_table", "alias", "component_of", "role",
    "match_status", TABLE_SIZE_ROW_COL, TABLE_DIST_SORT_COL,
    "stats_off", "unsorted_pct", "table_attention_score", "recommendation",
]

SLOW_LINEAGE_VIEW_COLS = [
    "object_type", "query_table", "alias", "component_of", "match_status",
    "source_db", "schema_name", "table_name", "source_definition",
]

SLOW_LINEAGE_BASE_COLS = [
    "query_table", "source_db", "schema_name", "table_name",
    TABLE_SIZE_ROW_COL, TABLE_DIST_SORT_COL, "stats_off", "unsorted_pct",
    "table_attention_score", "full_scan_score", "distribution_usage_score",
    "recommendation",
]

SLOW_LINEAGE_JOIN_COLS = [
    "join_scope", "join_no", "join_type", "involved_tables",
    "left_physical_sources", "right_physical_sources", "physical_column_pairs",
    "physical_lineage_status", "join_columns", "column_pairs", "distribution_signal",
    "plan_match_status", "plan_node_id", "plan_operator", "actual_step_names", "actual_movement",
    "actual_input_rows", "actual_output_rows", "actual_duration_s",
    "actual_local_spill_blocks", "actual_remote_spill_blocks",
    "distribution_alignment", "severity", "recommendation", "condition",
]

SUBQUERY_EXTRACT_COLS = [
    "subquery_no", "kind", "name", "table_count", "join_count",
    "sql_preview", "sql_text",
]
SUBQUERY_EXTRACT_DATA_COLS = [*SUBQUERY_EXTRACT_COLS, "source_start", "source_end"]

SOURCE_SQL_TABLES = [
    "query_details",
    "query_history",
    "query_health",
    "query_explain",
    "query_detail_flow",
    "query_history_all",
    "query_text",
    "child_query_text",
    "user_info",
    "table_scan_info",
    "svv_table_info_all",
    "external_table_info_all",
    "view_definitions",
    "procedure_definitions",
]


class _HomePage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        header = QHBoxLayout()
        title_box = QVBoxLayout()
        title_box.setSpacing(2)
        title = QLabel("REDSHIFT PERFORMANCE HOME")
        title.setObjectName("SectionHeader")
        subtitle = QLabel("A local, read-only map of the captured workload and the fastest DBA paths through it.")
        subtitle.setObjectName("Caption")
        subtitle.setWordWrap(True)
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        header.addLayout(title_box, 1)
        self._snapshot = QLabel("No snapshot loaded")
        self._snapshot.setObjectName("Mono")
        self._snapshot.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self._snapshot.setStyleSheet(f"color:{PALETTE.text_2}; font-size:11px;")
        header.addWidget(self._snapshot, 0)
        root.addLayout(header)

        self._metrics = _MetricStrip()
        root.addWidget(self._metrics, 0)

        split = QSplitter(Qt.Horizontal)
        split.setChildrenCollapsible(False)
        self._diagram = _QueryCaptureFlowDiagram()
        self._use_cases = _AnimatedUseCasePanel()
        split.addWidget(self._diagram)
        split.addWidget(self._use_cases)
        split.setSizes([900, 520])
        root.addWidget(split, 1)

    def set_report(self, report: ClusterReport) -> None:
        self._snapshot.setText(f"Snapshot: {report.snapshot_id or 'all rows'}")
        self._metrics.set_summary(report.summary, report.rule_count)
        self._diagram.set_report(report)
        self._use_cases.set_report(report)

    def set_loading(self, label: str) -> None:
        self._snapshot.setText(label)

    def set_error(self, message: str) -> None:
        self._snapshot.setText("Last load had errors")
        self._diagram.set_error(message)


class _QueryCaptureFlowDiagram(QFrame):
    _AUX_NODES = [
        ("Scan information", "table_scan_info", ("table_scan_info",), PALETTE.warn),
        ("Filter usage", "query_details + filter plan nodes", ("query_details", "query_explain"), PALETTE.pink),
        ("Explain plan", "query_explain", ("query_explain",), PALETTE.violet),
    ]

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("CardSubtle")
        self.setMinimumHeight(360)
        self._phase = 0.0
        self._parent_limit = 0
        self._counts: dict[str, int] = {}
        self._repeat_groups = 0
        self._repeat_members = 0
        self._summary_text = "Load DuckDB Table Status to populate collection sizes."
        self._timer = QTimer(self)
        self._timer.setInterval(45)
        self._timer.timeout.connect(self._tick)

    def showEvent(self, event) -> None:
        if not self._timer.isActive():
            self._timer.start()
        super().showEvent(event)

    def hideEvent(self, event) -> None:
        self._timer.stop()
        super().hideEvent(event)

    def set_report(self, report: ClusterReport) -> None:
        counts = self._table_counts(report.table_status)
        self._counts = counts
        try:
            settings = load_settings()
            self._parent_limit = max(0, int(settings.capture_query_limit or 0))
        except Exception:
            self._parent_limit = 0
        self._repeat_groups = 0 if report.repeat_groups is None else len(report.repeat_groups)
        self._repeat_members = 0 if report.repeat_members is None else len(report.repeat_members)
        root_rows = self._count_for("query_history")
        raw_ids = self._count_for("query_history_all") or root_rows
        aux_rows = self._count_for("table_scan_info", "query_details", "query_explain")
        bridge_text = (
            f"{_fmt_compact_count(self._repeat_members)} grouped query IDs"
            if self._repeat_members
            else f"{_fmt_compact_count(raw_ids)} captured query IDs"
        )
        cap_text = (
            "all parent patterns"
            if self._parent_limit <= 0
            else f"max {_fmt_int(self._parent_limit)} parent patterns"
        )
        self._summary_text = (
            f"Threshold roots -> {cap_text} | "
            f"{_fmt_compact_count(root_rows)} denormalized rows | "
            f"{bridge_text} | {_fmt_compact_count(aux_rows)} auxiliary rows"
        )
        self.update()

    def set_error(self, message: str) -> None:
        text = str(message or "").strip()
        self._summary_text = f"Last load error: {text[:110]}" if text else "Last load had an error."
        self.update()

    def _tick(self) -> None:
        self._phase = (self._phase + 0.018) % 1.0
        self.update()

    def _table_counts(self, table_status: pd.DataFrame) -> dict[str, int]:
        if table_status is None or table_status.empty or "table_name" not in table_status.columns:
            return {}
        count_col = "record_count" if "record_count" in table_status.columns else "latest_snapshot_rows"
        if count_col not in table_status.columns:
            return {}
        out: dict[str, int] = {}
        for _, row in table_status.iterrows():
            name = str(row.get("table_name") or "").strip()
            if not name:
                continue
            out[name] = out.get(name, 0) + _safe_int(row.get(count_col), 0)
        return out

    def _count_for(self, *tables: str) -> int:
        return sum(self._counts.get(table, 0) for table in tables)

    def paintEvent(self, _) -> None:
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        p.fillRect(self.rect(), QColor(PALETTE.bg_1))

        w = max(1, self.width())
        h = max(1, self.height())
        p.setPen(QColor(PALETTE.text_1))
        p.setFont(QFont("Inter", 10, QFont.Bold))
        p.drawText(QRectF(16, 12, w - 32, 20), Qt.AlignLeft | Qt.AlignVCenter, "QUERY CAPTURE FLOW INTO LOCAL DUCKDB")
        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("Inter", 9))
        p.drawText(QRectF(16, 34, w - 32, 18), Qt.AlignLeft | Qt.AlignVCenter, self._summary_text)

        nodes = self._node_rects()
        self._draw_connectors(p, nodes)
        self._draw_nodes(p, nodes)

        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("Inter", 8, QFont.DemiBold))
        p.drawText(
            QRectF(16, h - 28, w - 32, 18),
            Qt.AlignCenter,
            "Bridge keys preserve the real (snapshot_id, query_id) rows used to pull scan, filter, and plan evidence.",
        )

    def _node_rects(self) -> dict[str, QRectF]:
        w = max(1, self.width())
        h = max(1, self.height())
        gap = 18.0
        margin = 18.0
        aux_w = min(226.0, max(168.0, w * 0.24))
        stage_area_w = max(390.0, w - aux_w - margin * 2 - gap * 3)
        stage_w = max(126.0, min(198.0, stage_area_w / 3.0))
        node_h = 86.0
        stage_y = max(76.0, min(h - node_h - 56.0, h * 0.50 - node_h / 2.0))
        aux_h = 58.0
        aux_gap = 14.0
        aux_total = len(self._AUX_NODES) * aux_h + (len(self._AUX_NODES) - 1) * aux_gap
        aux_x = w - aux_w - margin
        aux_y = max(66.0, min(h - aux_total - 44.0, h * 0.50 - aux_total / 2.0))
        rank = QRectF(margin, stage_y, stage_w, node_h)
        dedupe = QRectF(rank.right() + gap, stage_y, stage_w, node_h)
        bridge = QRectF(dedupe.right() + gap, stage_y, stage_w, node_h)
        if bridge.right() > aux_x - gap:
            usable = max(1.0, aux_x - margin - gap * 3.0)
            stage_w = max(112.0, usable / 3.0)
            rank = QRectF(margin, stage_y, stage_w, node_h)
            dedupe = QRectF(rank.right() + gap, stage_y, stage_w, node_h)
            bridge = QRectF(dedupe.right() + gap, stage_y, stage_w, node_h)
        out = {"rank": rank, "dedupe": dedupe, "bridge": bridge}
        for idx, (_title, _subtitle, _tables, _color) in enumerate(self._AUX_NODES):
            out[f"aux_{idx}"] = QRectF(aux_x, aux_y + idx * (aux_h + aux_gap), aux_w, aux_h)
        return out

    def _draw_connectors(self, p: QPainter, nodes: dict[str, QRectF]) -> None:
        main_color = QColor(PALETTE.accent)
        self._draw_animated_edge(p, nodes["rank"], nodes["dedupe"], main_color, 0.00)
        self._draw_animated_edge(p, nodes["dedupe"], nodes["bridge"], QColor(PALETTE.cyan), 0.18)
        for idx, (_title, _subtitle, _tables, color) in enumerate(self._AUX_NODES):
            self._draw_animated_edge(p, nodes["bridge"], nodes[f"aux_{idx}"], QColor(color), 0.38 + idx * 0.12)

    def _draw_animated_edge(self, p: QPainter, start_rect: QRectF, end_rect: QRectF, color: QColor, delay: float) -> None:
        start = QPointF(start_rect.right(), start_rect.center().y())
        end = QPointF(end_rect.left(), end_rect.center().y())
        path = QPainterPath(start)
        mid = (start.x() + end.x()) / 2.0
        path.cubicTo(QPointF(mid, start.y()), QPointF(mid, end.y()), end)
        base = QColor(color)
        base.setAlpha(78)
        p.setPen(QPen(base, 2.0))
        p.setBrush(Qt.NoBrush)
        p.drawPath(path)

        pulse = QColor(color)
        pulse.setAlpha(230)
        p.setPen(Qt.NoPen)
        p.setBrush(pulse)
        for offset in (0.0, 0.45):
            t = (self._phase + delay + offset) % 1.0
            pt = path.pointAtPercent(t)
            radius = 3.1 + 0.8 * math.sin((self._phase + delay + offset) * math.tau)
            p.drawEllipse(pt, max(2.4, radius), max(2.4, radius))

        arrow = QPainterPath()
        arrow.moveTo(end)
        arrow.lineTo(QPointF(end.x() - 8.0, end.y() - 4.5))
        arrow.lineTo(QPointF(end.x() - 8.0, end.y() + 4.5))
        arrow.closeSubpath()
        p.setBrush(pulse)
        p.drawPath(arrow)

    def _draw_nodes(self, p: QPainter, nodes: dict[str, QRectF]) -> None:
        query_history_rows = self._count_for("query_history")
        raw_id_rows = self._count_for("query_history_all") or query_history_rows
        query_text_rows = self._count_for("query_text")
        bridge_rows = self._repeat_members or raw_id_rows
        groups_text = (
            f"{_fmt_compact_count(self._repeat_groups)} repeat groups"
            if self._repeat_groups
            else f"{_fmt_compact_count(query_text_rows)} SQL text rows"
        )
        self._draw_stage_node(
            p,
            nodes["rank"],
            "1",
            "Threshold roots",
            "all queries over floor",
            "denormalized query rows",
            f"query_history: {_fmt_compact_count(query_history_rows)}",
            QColor(PALETTE.accent),
        )
        self._draw_stage_node(
            p,
            nodes["dedupe"],
            "2",
            "Parent shapes",
            "normalize SQL patterns",
            "query type + SQL shape",
            groups_text,
            QColor(PALETTE.cyan),
        )
        self._draw_stage_node(
            p,
            nodes["bridge"],
            "3",
            "Query-ID bridge",
            "fan out to actual IDs",
            "snapshot_id + query_id",
            f"IDs: {_fmt_compact_count(bridge_rows)}",
            QColor(PALETTE.ok),
        )
        for idx, (title, subtitle, tables, color) in enumerate(self._AUX_NODES):
            row_count = self._count_for(*tables)
            self._draw_aux_node(
                p,
                nodes[f"aux_{idx}"],
                title,
                subtitle,
                f"rows: {_fmt_compact_count(row_count)}",
                row_count,
                QColor(color),
            )

    def _draw_stage_node(
        self,
        p: QPainter,
        rect: QRectF,
        stage: str,
        title: str,
        subtitle: str,
        basis: str,
        metric: str,
        color: QColor,
    ) -> None:
        active = any(_safe_int(self._counts.get(table), 0) for table in self._counts) or self._repeat_members
        fill = QColor(PALETTE.bg_2)
        if active:
            fill = QColor(color)
            fill.setAlpha(34)
        edge = QColor(color)
        edge.setAlpha(210 if active else 128)
        p.setPen(QPen(edge, 1.4))
        p.setBrush(fill)
        p.drawRoundedRect(rect, 8, 8)

        badge = QRectF(rect.left() + 10, rect.top() + 10, 24, 24)
        p.setPen(Qt.NoPen)
        badge_fill = QColor(color)
        badge_fill.setAlpha(210)
        p.setBrush(badge_fill)
        p.drawEllipse(badge)
        p.setPen(QColor(PALETTE.bg_0))
        p.setFont(QFont("JetBrains Mono", 9, QFont.Bold))
        p.drawText(badge, Qt.AlignCenter, stage)

        x = rect.left() + 42
        p.setPen(QColor(PALETTE.text_1))
        p.setFont(QFont("Inter", 9, QFont.Bold))
        p.drawText(QRectF(x, rect.top() + 7, rect.width() - 52, 18), Qt.AlignLeft | Qt.AlignVCenter, title)
        p.setFont(QFont("Inter", 8, QFont.DemiBold))
        p.drawText(QRectF(x, rect.top() + 27, rect.width() - 52, 16), Qt.AlignLeft | Qt.AlignVCenter, subtitle)
        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("Inter", 8))
        p.drawText(QRectF(rect.left() + 12, rect.top() + 49, rect.width() - 24, 15), Qt.AlignLeft | Qt.AlignVCenter, basis)
        p.setFont(QFont("JetBrains Mono", 8))
        p.drawText(QRectF(rect.left() + 12, rect.top() + 66, rect.width() - 24, 15), Qt.AlignLeft | Qt.AlignVCenter, metric)

    def _draw_aux_node(self, p: QPainter, rect: QRectF, title: str, subtitle: str, metric: str, rows: int, color: QColor) -> None:
        fill = QColor(PALETTE.bg_2)
        if rows:
            fill = QColor(color)
            fill.setAlpha(32)
        edge = QColor(color)
        edge.setAlpha(210)
        p.setPen(QPen(edge, 1.2))
        p.setBrush(fill)
        p.drawRoundedRect(rect, 8, 8)

        p.setPen(QColor(PALETTE.text_1))
        p.setFont(QFont("Inter", 9, QFont.Bold))
        p.drawText(rect.adjusted(12, 6, -12, -34), Qt.AlignLeft | Qt.AlignVCenter, title)
        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("Inter", 8))
        p.drawText(rect.adjusted(12, 24, -12, -18), Qt.AlignLeft | Qt.AlignVCenter, subtitle)
        p.setFont(QFont("JetBrains Mono", 8))
        p.drawText(rect.adjusted(12, 39, -12, -4), Qt.AlignLeft | Qt.AlignVCenter, metric)


class _AnimatedUseCasePanel(QFrame):
    _ITEMS = [
        "Triage the worst slow queries before opening SQL.",
        "Trace tables, views, joins, and full plan nodes in one place.",
        "Spot stale stats, unsorted tables, skew, and distribution pain.",
        "Find repeat query patterns by query type and SQL shape.",
        "Turn evidence into a ranked DBA fix queue.",
    ]

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("CardSubtle")
        self.setMinimumWidth(330)
        self.setMinimumHeight(360)
        self._phase = 0.0
        self._caption = "Load top areas to populate the workflow metrics."
        self._timer = QTimer(self)
        self._timer.setInterval(70)
        self._timer.timeout.connect(self._tick)

    def showEvent(self, event) -> None:
        if not self._timer.isActive():
            self._timer.start()
        super().showEvent(event)

    def hideEvent(self, event) -> None:
        self._timer.stop()
        super().hideEvent(event)

    def set_report(self, report: ClusterReport) -> None:
        summary = report.summary or {}
        pieces = [
            f"{_fmt_int(summary.get('slow_query_count'))} slow queries",
            f"{_fmt_int(summary.get('high_risk_table_count'))} risk tables",
            f"{_fmt_int(summary.get('repeat_group_count'))} repeat groups",
        ]
        if report.query_explain is not None and not report.query_explain.empty:
            pieces.append(f"{_fmt_int(len(report.query_explain))} explain rows")
        self._caption = " | ".join(pieces)
        self.update()

    def _tick(self) -> None:
        self._phase = (self._phase + 0.012) % 1.0
        self.update()

    def paintEvent(self, _) -> None:
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        p.fillRect(self.rect(), QColor(PALETTE.bg_1))
        w = max(1, self.width())

        p.setPen(QColor(PALETTE.text_1))
        p.setFont(QFont("Inter", 10, QFont.Bold))
        p.drawText(QRectF(16, 14, w - 32, 20), Qt.AlignLeft | Qt.AlignVCenter, "TOP 5 DBA USE CASES")
        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("Inter", 9))
        p.drawText(QRectF(16, 38, w - 32, 34), Qt.AlignLeft | Qt.TextWordWrap, self._caption)

        active = int(self._phase * len(self._ITEMS)) % len(self._ITEMS)
        top = 86.0
        row_h = max(44.0, min(58.0, (self.height() - top - 28.0) / len(self._ITEMS)))
        for idx, item in enumerate(self._ITEMS):
            y = top + idx * row_h
            is_active = idx == active
            glow = 0.5 + 0.5 * math.sin((self._phase * len(self._ITEMS) - idx) * math.tau)
            card = QRectF(16, y, w - 32, row_h - 8)
            bg = QColor(PALETTE.bg_2)
            if is_active:
                bg = QColor(107, 138, 255, 40 + int(35 * glow))
            p.setPen(QPen(QColor(PALETTE.accent if is_active else PALETTE.border), 1.0))
            p.setBrush(bg)
            p.drawRoundedRect(card, 8, 8)

            dot_color = QColor(PALETTE.accent_bright if is_active else PALETTE.text_2)
            dot_color.setAlpha(240 if is_active else 150)
            p.setPen(Qt.NoPen)
            p.setBrush(dot_color)
            radius = 5.0 + (2.6 * glow if is_active else 0.0)
            p.drawEllipse(QPointF(card.left() + 18, card.center().y()), radius, radius)

            p.setPen(QColor(PALETTE.text_1 if is_active else PALETTE.text_2))
            p.setFont(QFont("Inter", 9, QFont.DemiBold if is_active else QFont.Normal))
            p.drawText(card.adjusted(36, 5, -10, -5), Qt.AlignLeft | Qt.AlignVCenter | Qt.TextWordWrap, item)


def _short_cluster_label(summary: str) -> str:
    """Compact 'Native host:port' / 'JDBC url' summary down to the endpoint for
    the small active-cluster chip; the full summary stays in the tooltip."""
    text = str(summary or "").strip()
    if not text:
        return "-"
    head = text.split(";", 1)[0].strip()
    # 'Native host:port' -> 'host:port'; 'JDBC url' -> 'url'
    for prefix in ("Native ", "JDBC "):
        if head.startswith(prefix):
            head = head[len(prefix):]
            break
    return head[:48] or "-"


def _cluster_identity_key(config: dict[str, str] | None) -> str:
    """Endpoint-only identity used to decide whether Settings may switch files."""
    if not config or not source_cluster_configured(config):
        return ""
    return source_cluster_endpoint_key(config)


class ClusterDashboard(QWidget):
    reloadRequested = Signal(str, object)
    databasePathChanged = Signal(str)
    loaderRequested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._report: ClusterReport | None = None
        self._load_errors: tuple[str, ...] = ()

        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(6)

        source = QFrame()
        source.setObjectName("CardSubtle")
        source_lay = QGridLayout(source)
        source_lay.setContentsMargins(12, 10, 12, 10)
        source_lay.setHorizontalSpacing(8)
        source_lay.setVerticalSpacing(6)
        label = QLabel("DUCKDB")
        label.setObjectName("SectionHeader")
        source_lay.addWidget(label, 0, 0)
        self._path = QLineEdit(str(default_duckdb_path()))
        self._path.setPlaceholderText("Path to redshift.duckdb")
        self._path.setMinimumWidth(0)
        self._path.setReadOnly(True)
        # Annotations must always land in the warehouse being viewed, no
        # matter which code path switches the active per-cluster file.
        from ..sql_annotations import set_active_annotation_db

        set_active_annotation_db(self._path.text())
        self._path.textChanged.connect(lambda text: set_active_annotation_db(text))
        self._path.textChanged.connect(self.databasePathChanged)
        self._path.setToolTip(
            "Active DuckDB file. Each cluster gets its own file automatically, so "
            "switching clusters never flushes another cluster's rows."
        )
        source_lay.addWidget(self._path, 0, 1, 1, 6)
        # Path yields width so the row cannot force a horizontal page scrollbar.
        self._path.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Fixed)
        self._cluster_indicator = QLabel("Cluster: -")
        self._cluster_indicator.setObjectName("Caption")
        self._cluster_indicator.setToolTip("The Redshift cluster whose captured rows are currently loaded.")
        source_lay.addWidget(self._cluster_indicator, 0, 7)
        browse = QPushButton("Browse")
        browse.setObjectName("Ghost")
        config_btn = QPushButton("Settings")
        config_btn.setObjectName("Ghost")
        refresh_source_btn = QPushButton("Refresh Source Data")
        refresh_source_btn.setObjectName("Ghost")
        refresh_source_btn.setToolTip("Reload analyzer tables from Redshift in dependency order.")
        self._error_log_btn = QPushButton("Error Log")
        self._error_log_btn.setObjectName("Ghost")
        self._error_log_btn.setToolTip("Open the DuckDB load error log for the last app load.")
        self._health_btn = QPushButton("Health")
        self._health_btn.setObjectName("Ghost")
        # The DuckDB panel row must stay inside a 1280px viewport without a
        # horizontal page scrollbar, so this button yields width rather than
        # widening the whole dashboard.
        self._health_btn.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Fixed)
        self._health_btn.setMinimumWidth(58)
        self._health_btn.setToolTip(
            "Check this DuckDB warehouse without loading it: core tables, captured "
            "SQL text, cluster identity, and cached analysis."
        )
        self._area = QComboBox()
        for key, label_text in REPORT_AREA_CHOICES:
            if key == "table_risk":
                continue
            self._area.addItem(label_text, key)
        self._area.setCurrentIndex(max(0, self._area.findData("status")))
        self._area.setMinimumWidth(120)
        self._area.setMaximumWidth(210)
        reload_btn = QPushButton("Load Area")
        reload_btn.setObjectName("Primary")
        # The DuckDB controls used to occupy a second full row. That row pushed
        # the analysis content down and off short screens, and everything on it
        # except "Load Area" is occasional-use. The occasional controls now live
        # in a popup; the row that stays is the one an operator actually uses.
        #
        # The buttons are still constructed above and remain reachable by their
        # existing attribute names, so their handlers and tests are unchanged -
        # they are re-parented into the dialog when it opens.
        self._duckdb_tools_btn = QPushButton("DuckDB Tools …")
        self._duckdb_tools_btn.setObjectName("Ghost")
        self._duckdb_tools_btn.setToolTip(
            "Browse, Settings, Health Check, Refresh Source Data and the load "
            "Error Log."
        )
        self._duckdb_tools_btn.clicked.connect(self._open_duckdb_tools)
        self._duckdb_tool_buttons = (
            browse,
            config_btn,
            self._health_btn,
            refresh_source_btn,
            self._error_log_btn,
        )
        for button in self._duckdb_tool_buttons:
            button.setVisible(False)
        source_lay.addWidget(self._duckdb_tools_btn, 0, 8)
        source_lay.addWidget(self._area, 0, 9)
        source_lay.addWidget(reload_btn, 0, 10)
        source_lay.setColumnStretch(1, 1)
        root.addWidget(source)

        self._status = QLabel("Load a DuckDB snapshot to begin.")
        self._status.setObjectName("Caption")
        root.addWidget(self._status)

        # No inner QTabWidget. This dashboard has exactly one visible page, and
        # the main window already mounts it under a top-level tab named
        # "Workload Triage" (app.py). Wrapping a single page in its own tab
        # widget drew a second, identically-labelled tab bar inside the first -
        # a tab within a tab - and cost ~35px of height that the bubble chart
        # needs. The page is mounted directly below.

        self._overview = QWidget()
        overview_lay = QVBoxLayout(self._overview)
        overview_lay.setContentsMargins(0, 0, 0, 0)
        overview_lay.setSpacing(10)

        self._metrics = _MetricStrip()
        overview_lay.addWidget(self._metrics)

        split = QSplitter(Qt.Horizontal)
        split.setChildrenCollapsible(False)
        left = QWidget()
        left_lay = QVBoxLayout(left)
        left_lay.setContentsMargins(0, 0, 0, 0)
        left_lay.setSpacing(10)
        self._family = _FamilyBars()
        self._insights = _InsightCards()
        left_lay.addWidget(self._family, 0)
        left_lay.addWidget(self._insights, 1)
        split.addWidget(left)

        right = QWidget()
        right_lay = QVBoxLayout(right)
        right_lay.setContentsMargins(0, 0, 0, 0)
        right_lay.setSpacing(10)
        self._repeat_overview = _RepeatGroupCards("TOP REPEAT QUERY PATTERNS")
        self._heat = QueryHeatMap()
        right_lay.addWidget(self._repeat_overview, 1)
        right_lay.addWidget(self._heat, 1)
        split.addWidget(right)
        split.setSizes([620, 720])
        overview_lay.addWidget(split, 1)

        self._query_table = _SeverityQueryPage()
        self._table_impact = _TablePage(
            "TABLE BLAST RADIUS",
            TABLE_IMPACT_COLS,
            load_area="table_impact",
            load_label="Load Impact Rows",
            empty_message=(
                "Table Blast Impact has not returned rows. Click Load Impact Rows to compute this local DuckDB area; "
                "it is opt-in because it scans query-table relationships."
            ),
            header_labels={"query_ids": TABLE_BLAST_QUERY_IDS_HEADER},
            copy_column="query_ids",
            filter_columns=[("source_db", "Source DB"), ("schema_name", "Schema")],
        )
        self._insight_table = _InsightLedgerPage()
        self._focus_page = _FocusContributorsPage()
        self._group_evidence_page = _GroupEvidencePage()
        self._script_page = _ScriptPage()
        self._table_review_page = _TableReviewPage()
        self._triage_page = TriagePage()
        self._action_plan_page = _ActionPlanPage()
        self._table_heatmap_page = TableHeatMap()

        # Workload Triage is the dashboard's only visible page. Table Heat Map
        # is owned here for shared report/load wiring but is mounted by the
        # main application as its fourth top-level tab.
        #
        # Mounted directly rather than inside a tab widget: the main window's
        # tab already names this page, so a second tab bar here was redundant
        # chrome stealing vertical space from the chart.
        root.addWidget(self._triage_page, 1)

        browse.clicked.connect(self._browse)
        config_btn.clicked.connect(self._config)
        refresh_source_btn.clicked.connect(self._open_refresh_source)
        self._error_log_btn.clicked.connect(self._open_error_log)
        self._health_btn.clicked.connect(self._open_health_check)
        reload_btn.clicked.connect(self._reload_selected_area)
        for page in (
            self._query_table,
            self._table_impact,
            self._insight_table,
            self._focus_page,
            self._group_evidence_page,
            self._script_page,
            self._table_review_page,
            self._triage_page,
            self._action_plan_page,
            self._table_heatmap_page,
        ):
            page.loadRequested.connect(self._request_area_load)
        self._query_table.queryDiagramRequested.connect(self._open_query_diagram)
        self._triage_page.queryDiagramRequested.connect(self._open_query_diagram)
        self._insight_table.rowActivated.connect(self._open_insight_detail)
        self._update_error_log_button()
        self._settings = load_settings()
        # Resolve the active per-cluster DuckDB file at startup (migrating the
        # legacy default file for the current cluster on first run). No reload
        # here - the app performs its initial area load after construction.
        self._sync_active_cluster_file(reload_after=False)
        self._recover_orphan_tmp_tables_at_startup()

    def active_db_path(self) -> str:
        """Return the exact operator-selected DuckDB path."""
        return self._path.text().strip() or str(default_duckdb_path())

    def table_heatmap_page(self) -> TableHeatMap:
        """Return the shared heat-map page for the main top-level tab."""
        return self._table_heatmap_page

    def action_plan_page(self) -> "_ActionPlanPage":
        """Return the Fix Queue page for the main top-level tab.

        The page was built and fed data (set_dataframes below) but never
        mounted, so the triage screen's "open the Fix Queue tab" message
        pointed at a tab that did not exist. Owned here for the shared
        report/load wiring, mounted by the main window.
        """
        return self._action_plan_page

    def load_table_heatmap_if_needed(self) -> None:
        """Lazy-load physical table health when its top-level tab is opened."""
        if not self._table_heatmap_page.has_data():
            self._request_area_load("table_heatmap")

    def _recover_orphan_tmp_tables_at_startup(self) -> None:
        path = Path(self._path.text().strip() or str(default_duckdb_path()))
        if not path.is_file():
            return
        try:
            with DuckDBStore(path).connect():
                pass
        except Exception as exc:
            self._load_errors = (
                f"Startup DuckDB recovery check failed for {path}: {exc}",
            )
            self._update_error_log_button()

    def set_report(self, report: ClusterReport) -> None:
        self._report = report
        self._load_errors = tuple(report.load_errors or ())
        self._path.setText(str(report.db_path))
        loaded = _area_label(report.loaded_areas)
        if report.is_empty and report.table_status.empty:
            self._status.setText(
                "No captured rows found. Run python -m analyzer.ingest_redshift, then load this DuckDB file."
            )
        else:
            self._status.setText(
                f"{loaded} loaded from local DuckDB: {report.db_path}."
            )
        self._metrics.set_summary(report.summary, report.rule_count)
        self._triage_page.set_report(report)
        self._table_heatmap_page.set_report(report)
        self._family.set_dataframe(report.family_summary)
        self._insights.set_dataframe(report.insights)
        self._repeat_overview.set_dataframe(report.repeat_groups)
        self._heat.set_dataframe(report.query_heatmap)
        self._action_plan_page.set_dataframes(report.action_queue, report.rewrites, report.slow_queries)
        self._table_review_page.set_snapshot_info(report.snapshot_id)
        self._table_review_page.set_table_status(report.table_status)
        self._table_review_page.set_load_errors(report.load_errors)
        self._table_review_page.set_dataframe(report.table_review)
        self._query_table.set_context(report.table_review, report.view_definitions)
        self._query_table.set_explain_dataframe(report.query_explain)
        self._query_table.set_detail_flow_dataframe(report.query_detail_flow)
        self._query_table.set_dataframe(
            report.slow_queries,
            loaded="slow_queries" in set(report.loaded_areas),
        )
        self._table_impact.set_dataframe(report.table_impact)
        self._insight_table.set_sql_lookup(report.slow_queries)
        self._insight_table.set_dataframe(
            report.insights,
            loaded="insights" in set(report.loaded_areas),
        )
        self._focus_page.set_report_frames(report.slow_queries, report.insights, report.table_impact)
        self._group_evidence_page.set_report(report)
        self._script_page.set_report(report)
        self._update_error_log_button()

    def show_error(self, message: str) -> None:
        self._load_errors = (str(message),) if message else ()
        self._status.setText(f"DuckDB load error: {message}")
        self._table_review_page.set_load_error(message)
        self._update_error_log_button()

    def show_area_busy(self, areas: object = None) -> None:
        message = (
            "Another DuckDB area is still loading. Wait for its progress window to finish, "
            "then press this Load button again."
        )
        requested = {
            str(area)
            for area in (areas if isinstance(areas, (list, tuple, set)) else [areas])
            if area
        }
        self._status.setText(message)
        if "slow_queries" in requested:
            self._query_table.show_blocked(message)
        if "table_review" in requested:
            self._table_review_page.show_blocked(message)
        if "insights" in requested:
            self._insight_table.show_blocked(message)

    def show_loading(self, path: str, areas: object = None) -> None:
        target = path or str(default_duckdb_path())
        self._status.setText(f"Loading local DuckDB area {_area_label(areas)} from {target} ...")

    def show_cached(self, areas: object = None) -> None:
        self._status.setText(
            f"{_area_label(areas)} restored instantly from the current snapshot cache; "
            "no SQL rows were re-examined."
        )

    def show_idle(self) -> None:
        self._status.setText(
            "Choose a local DuckDB area and click Load Area. No Redshift refresh runs from this control."
        )

    def _open_error_log(self) -> None:
        _open_load_error_log(self, self._load_errors)

    def _open_duckdb_tools(self) -> None:
        """Occasional DuckDB controls, in a window instead of a permanent row.

        The real buttons are re-parented in rather than duplicated, so each one
        keeps its existing handler, tooltip and object name - a second set of
        buttons would be two things to keep in sync.
        """
        dialog = QDialog(self)
        dialog.setWindowTitle("DuckDB Tools")
        dialog.setMinimumWidth(460)
        layout = QVBoxLayout(dialog)
        layout.setContentsMargins(16, 14, 16, 12)
        layout.setSpacing(9)

        path = QLabel(self._path.text() or "(no DuckDB file selected)")
        path.setObjectName("Caption")
        path.setWordWrap(True)
        path.setTextInteractionFlags(Qt.TextSelectableByMouse)
        layout.addWidget(path)

        for button in self._duckdb_tool_buttons:
            button.setVisible(True)
            button.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
            layout.addWidget(button)

        # Sharing the cluster setup with a teammate is an occasional action and
        # belongs here rather than on the permanent row.
        export_cfg = QPushButton("Export Cluster Config (encrypted) …")
        export_cfg.setObjectName("Ghost")
        export_cfg.setToolTip(
            "Password-encrypted cluster profile for a teammate: namespaces, "
            "ports, databases, floor seconds and capture scope. Never contains "
            "credentials."
        )
        export_cfg.clicked.connect(dialog.accept)
        export_cfg.clicked.connect(self._export_cluster_config)
        layout.addWidget(export_cfg)

        import_cfg = QPushButton("Import Cluster Config …")
        import_cfg.setObjectName("Ghost")
        import_cfg.setToolTip(
            "Load a cluster profile exported by a teammate. You still supply "
            "your own Redshift credentials."
        )
        import_cfg.clicked.connect(dialog.accept)
        import_cfg.clicked.connect(self._import_cluster_config)
        layout.addWidget(import_cfg)

        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        # Every one of these actions either opens its own modal or reloads the
        # page behind this dialog, so close it first to avoid stacked modals.
        for button in self._duckdb_tool_buttons:
            button.clicked.connect(dialog.accept)

        try:
            dialog.exec()
        finally:
            # Hand the buttons back to the hidden row so the dialog can be
            # reopened; a re-parented widget dies with its parent otherwise.
            for button in self._duckdb_tool_buttons:
                try:
                    button.clicked.disconnect(dialog.accept)
                except (RuntimeError, TypeError):
                    pass
                button.setParent(self)
                button.setVisible(False)

    def _cluster_profile_path(self) -> Path | None:
        from ..runtime_paths import resolve_runtime_paths

        try:
            path = Path(resolve_runtime_paths().portable_profile_file)
        except Exception:
            return None
        return path if path.is_file() else None

    def _ask_password(self, title: str, prompt: str) -> str:
        text, ok = QInputDialog.getText(
            self, title, prompt, QLineEdit.Password
        )
        return text if ok else ""

    def _export_cluster_config(self) -> None:
        """Share the cluster setup with a teammate, without credentials."""
        from ..config_export import ConfigExportError, encrypt_document

        source = self._cluster_profile_path()
        if source is None:
            QMessageBox.information(
                self,
                "Export Cluster Config",
                "No redshift_cluster_profiles.json was found next to the app, "
                "so there is no cluster configuration to export.",
            )
            return
        try:
            document = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            QMessageBox.warning(
                self, "Export Cluster Config", f"Could not read {source}:\n{exc}"
            )
            return

        password = self._ask_password(
            "Export Cluster Config",
            "Password to protect this file (share it separately from the file):",
        )
        if not password:
            return
        confirm = self._ask_password("Export Cluster Config", "Re-enter the password:")
        if confirm != password:
            QMessageBox.warning(
                self, "Export Cluster Config", "The passwords did not match."
            )
            return

        try:
            blob = encrypt_document(document, password)
        except ConfigExportError as exc:
            QMessageBox.warning(self, "Export Cluster Config", str(exc))
            return

        target, _filter = QFileDialog.getSaveFileName(
            self,
            "Export Cluster Config",
            str(Path.home() / "infraredshift-clusters.ixcfg"),
            "Infraredshift config (*.ixcfg);;All files (*)",
        )
        if not target:
            return
        try:
            Path(target).write_text(blob, encoding="utf-8")
        except OSError as exc:
            QMessageBox.warning(
                self, "Export Cluster Config", f"Could not write the file:\n{exc}"
            )
            return
        QMessageBox.information(
            self,
            "Export Cluster Config",
            f"Written to:\n{target}\n\n"
            "This file contains cluster identity and capture settings only - "
            "no credentials. Your teammate supplies their own.\n\n"
            "Send the password by a different channel than the file.",
        )

    def _import_cluster_config(self) -> None:
        """Load a teammate's exported cluster profile."""
        from ..config_export import (
            ConfigExportError,
            decrypt_document,
            to_profiles_document,
        )

        source, _filter = QFileDialog.getOpenFileName(
            self,
            "Import Cluster Config",
            str(Path.home()),
            "Infraredshift config (*.ixcfg);;All files (*)",
        )
        if not source:
            return
        try:
            text = Path(source).read_text(encoding="utf-8")
        except OSError as exc:
            QMessageBox.warning(
                self, "Import Cluster Config", f"Could not read the file:\n{exc}"
            )
            return

        password = self._ask_password(
            "Import Cluster Config", "Password for this file:"
        )
        if not password:
            return
        try:
            payload = decrypt_document(text, password)
            document = to_profiles_document(payload)
        except ConfigExportError as exc:
            QMessageBox.warning(self, "Import Cluster Config", str(exc))
            return

        target = self._cluster_profile_path()
        if target is None:
            from ..runtime_paths import resolve_runtime_paths

            target = Path(resolve_runtime_paths().portable_profile_file)

        names = ", ".join(
            str(item.get("display_name") or item.get("profile") or "?")
            for item in document.get("profiles", [])
        )
        confirm = QMessageBox.question(
            self,
            "Import Cluster Config",
            f"Import {len(document.get('profiles', []))} cluster profile(s)?\n\n"
            f"{names}\n\n"
            f"This overwrites:\n{target}\n\n"
            "Your saved Redshift credentials are stored separately and are not "
            "affected. Restart the app afterwards for the new profiles to take "
            "effect.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if confirm != QMessageBox.Yes:
            return
        try:
            # Keep a copy of what was there: this overwrites the file the whole
            # app resolves its clusters from.
            if target.is_file():
                target.with_suffix(target.suffix + ".backup").write_text(
                    target.read_text(encoding="utf-8"), encoding="utf-8"
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
        except OSError as exc:
            QMessageBox.warning(
                self, "Import Cluster Config", f"Could not write {target}:\n{exc}"
            )
            return
        QMessageBox.information(
            self,
            "Import Cluster Config",
            f"Imported to:\n{target}\n\n"
            "Restart the app, then enter your own Redshift credentials under "
            "Local Credentials.",
        )

    def _open_health_check(self) -> None:
        """Check the active warehouse without loading it."""
        from ..duckdb_health import check_warehouse

        path = self._path.text().strip()
        if not path:
            QMessageBox.information(
                self,
                "DuckDB Health Check",
                "No DuckDB file is selected. Use Browse to pick one.",
            )
            return
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            report = check_warehouse(path)
        except Exception as exc:
            QApplication.restoreOverrideCursor()
            QMessageBox.critical(
                self,
                "DuckDB Health Check",
                f"The health check itself failed:\n\n{type(exc).__name__}: {exc}",
            )
            return
        finally:
            if QApplication.overrideCursor() is not None:
                QApplication.restoreOverrideCursor()
        _HealthCheckDialog(report, self).exec()

    def _update_error_log_button(self) -> None:
        count = len(self._load_errors)
        self._error_log_btn.setText(f"Error Log ({count})" if count else "Error Log")
        if count:
            self._error_log_btn.setToolTip(f"Open {count:,} DuckDB load error(s) from the last app load.")
            self._error_log_btn.setStyleSheet(f"color:{PALETTE.crit}; font-weight:700;")
        else:
            self._error_log_btn.setToolTip("No DuckDB load errors recorded for the last app load.")
            self._error_log_btn.setStyleSheet("")

    def _reload_selected_area(self) -> None:
        area = self._area.currentData() or "status"
        path = self._path.text().strip()
        self.show_loading(path, [str(area)])
        QApplication.processEvents()
        self.reloadRequested.emit(path, [str(area)])

    def _request_area_load(self, area: str) -> None:
        area = str(area or "").strip()
        if not area:
            return
        index = self._area.findData(area)
        if index >= 0:
            self._area.setCurrentIndex(index)
        path = self._path.text().strip()
        requested_areas: object = [area]
        if area == "repeat_queries":
            requested_areas = ["repeat_queries", "action_plan"]
        if area == "insights":
            self._insight_table.show_loading()
        if area == "slow_queries":
            self._query_table.show_loading()
        if area == "table_review":
            self._table_review_page.show_loading()
        if area == "table_heatmap":
            self._table_heatmap_page.show_loading()
        self.show_loading(path, requested_areas)
        QApplication.processEvents()
        self.reloadRequested.emit(path, requested_areas)

    def _open_query_diagram(self, row: object) -> None:
        lineage_row, sql, label = self._resolve_query_diagram_input(row)
        if not sql:
            QMessageBox.information(self, "Query Diagram", "The selected query does not include SQL text.")
            return
        if self._report is None:
            QMessageBox.information(self, "Query Diagram", "Load a DuckDB snapshot before opening query lineage.")
            return
        try:
            analysis = analyze_console_sql(
                sql,
                self._report.table_review,
                self._report.slow_queries,
                self._report.view_definitions,
            )
        except Exception as exc:
            QMessageBox.warning(self, "Query Diagram", str(exc))
            return
        if not str(lineage_row.get("query_id") or "").strip():
            lineage_row["query_id"] = label
        if not str(lineage_row.get("sql_text") or "").strip():
            lineage_row["sql_text"] = sql
        dialog = _SlowQueryLineageDialog(
            lineage_row,
            analysis,
            self._report.table_review,
            self._report.view_definitions,
            self,
            explain_rows=_query_evidence_rows(self._report.query_explain, lineage_row),
            detail_rows=_query_evidence_rows(self._report.query_detail_flow, lineage_row),
        )
        _resize_dialog_to_screen(dialog, 0.94)
        dialog.exec()

    def _resolve_query_diagram_input(self, row: object) -> tuple[pd.Series, str, str]:
        if isinstance(row, pd.Series):
            lineage_row = row.copy()
        elif isinstance(row, dict):
            lineage_row = pd.Series(row)
        else:
            text = str(row or "").strip()
            lineage_row = pd.Series({"query_id": text if text.isdigit() else "", "sql_text": "" if text.isdigit() else text})
        query_id = str(lineage_row.get("query_id") or "").strip()
        sql = self._query_row_sql_text(lineage_row)
        if self._report is not None and query_id:
            resolved = self._query_row_for_id(query_id)
            if resolved is not None and not resolved.empty:
                for key, value in resolved.items():
                    if not str(lineage_row.get(key) or "").strip():
                        lineage_row[key] = value
                resolved_sql = self._query_row_sql_text(resolved)
                if resolved_sql and (not sql or sql.endswith("...") or len(resolved_sql) > len(sql)):
                    sql = resolved_sql
                else:
                    sql = self._query_row_sql_text(lineage_row)
        if sql:
            lineage_row["sql_text"] = sql
        label = query_id or str(lineage_row.get("query_id") or "selected query") or "selected query"
        return lineage_row, sql, label

    def _query_row_sql_text(self, row: pd.Series) -> str:
        for key in ("sql_text_full", "full_sql_text", "query_text_full", "sql_text", "query_text", "query_txt", "text"):
            if key in row.index:
                text = str(row.get(key) or "").strip()
                if text and text.lower() != "nan":
                    return text
        return ""

    def _query_row_for_id(self, query_id: str) -> pd.Series | None:
        if self._report is None or not query_id:
            return None
        for frame in (self._report.slow_queries, self._report.repeat_members):
            if frame is None or frame.empty or "query_id" not in frame.columns:
                continue
            rows = frame[frame["query_id"].astype(str).str.strip() == str(query_id).strip()]
            if not rows.empty:
                return rows.iloc[0].copy()
        return None

    def _open_insight_detail(self, row: object) -> None:
        if not isinstance(row, pd.Series) or row.empty:
            return
        dialog = QDialog(self)
        dialog.setWindowTitle(f"Insight Detail - {row.get('insight_id') or row.get('title') or 'selected insight'}")
        root = QVBoxLayout(dialog)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)

        title = QLabel(str(row.get("title") or "Insight"))
        title.setObjectName("SectionHeader")
        title.setWordWrap(True)
        root.addWidget(title)

        metric_rows = [
            ("Rule", row.get("insight_id")),
            ("Severity", str(row.get("severity") or "").upper()),
            ("Family", row.get("family")),
            ("Target", row.get("target_label") or row.get("subject")),
            ("Metric", f"{row.get('metric_label') or 'Observed Value'}: {row.get('metric_display') or _fmt_value('metric_value', row.get('metric_value'))}"),
            ("Impact Score", f"{_fmt_value('impact_score', row.get('impact_score'))} ({row.get('impact_band') or 'Unscored'})"),
        ]
        for label, value in metric_rows:
            item = QLabel(f"{label}: {value if value not in (None, '') else '-'}")
            item.setObjectName("Mono" if label in {"Rule", "Target", "Metric"} else "Caption")
            item.setWordWrap(True)
            root.addWidget(item)

        for label, key in (("Evidence", "evidence"), ("Recommendation", "recommendation")):
            head = QLabel(label.upper())
            head.setObjectName("SectionHeader")
            root.addWidget(head)
            body = QLabel()
            body.setObjectName("Caption")
            apply_markdown(body, str(row.get(key) or "-"))
            root.addWidget(body)

        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.close)
        root.addWidget(buttons)
        _resize_dialog_to_screen(dialog, 0.55)
        dialog.exec()

    def _browse(self) -> None:
        chosen, _ = QFileDialog.getOpenFileName(
            self,
            "Open Redshift DuckDB snapshot",
            str(Path(self._path.text() or str(default_duckdb_path())).parent),
            "DuckDB files (*.duckdb *.db);;All files (*.*)",
        )
        if chosen:
            self._path.setText(chosen)
            self._status.setText("DuckDB path selected. Choose an area and click Load Area.")

    def _config(self) -> None:
        # Settings must inspect the exact file the operator loaded.  Closing
        # this dialog used to rescope every manually browsed DuckDB file back
        # to the automatic AppData per-cluster path, even when the cluster had
        # not changed.  The already-rendered quadrant then remained in memory
        # while every new load/count read a newly created empty database.
        active_path = self._path.text().strip() or str(default_duckdb_path())
        before_config, _before_summary = self._resolve_active_cluster()
        before_identity = _cluster_identity_key(before_config)
        dialog = _ConfigDialog(active_path, self)
        dialog.exec()
        self._settings = load_settings()
        after_config, after_summary = self._resolve_active_cluster()
        after_identity = _cluster_identity_key(after_config)
        if after_identity != before_identity:
            # A real endpoint change (prod <-> dev) still selects that
            # cluster's isolated file and reloads it.
            self._sync_active_cluster_file(reload_after=True)
        else:
            # Capture-selection and SQL-setting edits do not authorize a data
            # file switch. Preserve Browse/current-report identity exactly.
            self._path.setText(active_path)
            if after_config is None:
                self._cluster_indicator.setText("Cluster: (not configured)")
            else:
                self._cluster_indicator.setText(f"Cluster: {_short_cluster_label(after_summary)}")
                self._cluster_indicator.setToolTip(after_summary)
        self._status.setText("Settings closed. Choose an area and click Load Area.")

    def _edit_local_credentials(self) -> None:
        """Open only the protected credential flow used by the Data Loader."""
        active_path = self._path.text().strip() or str(default_duckdb_path())
        dialog = _ConfigDialog(active_path, self)
        dialog._edit_local_configuration()
        self._status.setText("Encrypted cluster credentials closed.")

    # -------------------------------------------------- per-cluster DuckDB file

    def _resolve_active_cluster(self):
        """(config, summary) for the currently configured source cluster, or
        (None, "") when no cluster is configured (demo/mock/manual flows)."""
        try:
            from ..ingest_redshift import load_dotenv, parse_args, resolve_args_from_env

            load_dotenv(None)
            args = parse_args([])
            resolve_args_from_env(args, self._settings)
            config = resolve_source_cluster_config(args)
        except SystemExit:
            return None, ""
        except Exception:
            return None, ""
        if not source_cluster_configured(config):
            return None, ""
        return config, source_cluster_summary(config)

    def _sync_active_cluster_file(self, *, reload_after: bool) -> bool:
        """Refresh the source-cluster label without changing DuckDB identity.

        The operator-selected/current-report file is authoritative. Cluster
        settings control future capture, not the local database path.
        """
        if not hasattr(self, "_settings") or self._settings is None:
            self._settings = load_settings()
        config, summary = self._resolve_active_cluster()
        if config is None:
            self._cluster_indicator.setText("Cluster: (not configured)")
            return False
        self._cluster_indicator.setText(f"Cluster: {_short_cluster_label(summary)}")
        self._cluster_indicator.setToolTip(summary)
        self._settings.last_source_cluster_fingerprint = source_cluster_fingerprint(config)
        self._settings.last_source_cluster_summary = summary
        with contextlib.suppress(Exception):
            save_settings(self._settings)
        return False

    def _open_refresh_source(self) -> None:
        self.loaderRequested.emit()
        self._status.setText("Data Loader opened in the main workspace.")


class _IngestJobsWorker(QObject):
    """Runs a sequence of ingest_redshift.main invocations off the GUI thread,
    emitting step and live row-count progress."""

    stepStarted = Signal(int, int, str)  # 1-based step index, total steps, label
    stepFinished = Signal(int, int, str) # 1-based step index, total steps, label
    rowsFetched = Signal(str, int)       # stage label, rows fetched so far
    timeoutDecisionRequested = Signal(str, str, object)
    finishedOk = Signal(str)             # combined captured stdout
    cancelled = Signal(int, int, str)    # completed steps, total steps, stdout
    failed = Signal(str, str, str)       # failing step label, error, stdout

    def __init__(self, jobs: list[tuple[str, list[str]]]):
        super().__init__()
        self._jobs = list(jobs)
        self._cancel_requested = False

    def request_cancel(self) -> None:
        self._cancel_requested = True

    def run(self) -> None:  # invoked on worker QThread via started signal
        from .. import ingest_redshift

        def hook(stage: str, rows: int) -> None:
            if self._cancel_requested:
                raise ingest_redshift.IngestCancelled()
            self.rowsFetched.emit(stage, rows)

        output = ""
        completed = 0
        total = len(self._jobs)
        ingest_redshift.set_progress_hook(hook)

        def timeout_decision(stage: str, error: str) -> str:
            request = {"event": threading.Event(), "decision": "next"}
            self.timeoutDecisionRequested.emit(str(stage), str(error), request)
            request["event"].wait()
            return str(request.get("decision") or "next")

        ingest_redshift.set_external_timeout_decision_hook(timeout_decision)
        try:
            for index, (label, argv) in enumerate(self._jobs, start=1):
                if self._cancel_requested:
                    self.cancelled.emit(completed, total, output)
                    return
                self.stepStarted.emit(index, total, label)
                buffer = io.StringIO()
                try:
                    with contextlib.redirect_stdout(buffer):
                        ingest_redshift.main(argv)
                finally:
                    output += buffer.getvalue()
                completed += 1
                self.stepFinished.emit(index, total, label)
            self.finishedOk.emit(output)
        except ingest_redshift.IngestCancelled:
            self.cancelled.emit(completed, total, output)
        except (SystemExit, Exception) as exc:
            label = self._jobs[completed][0] if completed < total else ""
            self.failed.emit(label, str(exc), output)
        finally:
            ingest_redshift.set_progress_hook(None)
            ingest_redshift.set_external_timeout_decision_hook(None)


def _ensure_redshift_password_widget(parent, title: str, settings) -> bool:
    try:
        from ..ingest_redshift import load_dotenv, parse_args, resolve_args_from_env

        load_dotenv(None)
        args = parse_args([])
        resolve_args_from_env(args, settings)
    except SystemExit as exc:
        QMessageBox.warning(parent, title, str(exc))
        return False
    except Exception as exc:
        QMessageBox.warning(parent, title, str(exc))
        return False

    if not args.user:
        QMessageBox.warning(parent, title, "Add the Redshift username in Settings → Local Credentials.")
        return False
    if args.connection == "jdbc":
        if not args.jdbc_url or not args.jdbc_jar:
            QMessageBox.warning(parent, title, "JDBC mode needs REDSHIFT_JDBC_URL and REDSHIFT_JDBC_JAR.")
            return False
    elif not args.host:
        QMessageBox.warning(parent, title, "Add the Redshift server address in Settings → Local Credentials.")
        return False

    from ..secrets_store import session_secret, set_session_secret

    if session_secret(args.password_env):
        return True
    password, ok = QInputDialog.getText(
        parent,
        title,
        "Redshift password",
        QLineEdit.Password,
    )
    if not ok or not password:
        return False
    set_session_secret(args.password_env, password)
    return True


class _IngestJobController(QObject):
    """Owns one background ingest run per parent widget: worker thread plus a
    determinate step progress dialog with a live rows-fetched counter."""

    def __init__(self, parent_widget, reload_counts=None):
        super().__init__(parent_widget)
        self._parent = parent_widget
        self._reload_counts = reload_counts
        self._thread: QThread | None = None
        self._worker: _IngestJobsWorker | None = None
        self._dialog: QProgressDialog | None = None
        self._cursor_active = False
        # Per-run state. The worker emits progress/finish signals from its own
        # thread; these handlers are BOUND METHODS of this controller, which
        # lives on the GUI thread, so Qt's AutoConnection queues each call to
        # the GUI thread. That is what keeps every dialog/table repaint on one
        # thread - the previous closure-based handlers ran on the worker thread
        # and corrupted the backing store (recursive repaint, endPaint with
        # active painter, startTimer from another thread).
        self._title = ""
        self._jobs_total = 0
        self._on_success = None
        self._select_table = ""
        self._step = 0
        self._step_label = ""

    def running(self) -> bool:
        return self._thread is not None and self._thread.isRunning()

    # ---- worker-signal handlers: all run on the GUI thread (queued) ----

    def _on_step(self, index: int, total: int, label: str) -> None:
        self._step, self._jobs_total, self._step_label = index, total, label
        if self._dialog is not None:
            self._dialog.setRange(0, total)
            self._dialog.setValue(index - 1)
            self._dialog.setLabelText(f"Step {index}/{total}: refreshing {label} from Redshift ...")

    def _on_rows(self, stage: str, rows: int) -> None:
        if self._dialog is not None:
            self._dialog.setLabelText(
                f"Step {self._step}/{self._jobs_total}: refreshing {self._step_label} from Redshift ...\n"
                f"{stage}: {rows:,} rows fetched"
            )

    def _on_step_finished(self, index: int, total: int, label: str) -> None:
        # Do not open another DuckDB connection between capture steps. The
        # worker starts the next step immediately, and simultaneous connection
        # schema/view installation can cause a DuckDB catalog write-write
        # conflict. _cleanup() refreshes counts once the entire job is done.
        _ = (index, total, label)

    def _on_timeout_decision(self, stage: str, error: str, request: object) -> None:
        if self._cursor_active:
            QApplication.restoreOverrideCursor()
            self._cursor_active = False
        box = QMessageBox(self._parent)
        box.setWindowTitle("External Metadata Stage Timed Out")
        box.setIcon(QMessageBox.Warning)
        box.setText(f"The external metadata stage exceeded 10 minutes:\n\n{stage}")
        box.setInformativeText(
            "Continue retries this stage with another 10-minute window. "
            "Next Step preserves completed work and continues without the stage.\n\n"
            f"Technical message: {error}"
        )
        retry_button = box.addButton("Continue — Retry Stage", QMessageBox.AcceptRole)
        next_button = box.addButton("Next Step — Skip Stage", QMessageBox.DestructiveRole)
        box.setDefaultButton(next_button)
        box.exec()
        if isinstance(request, dict):
            request["decision"] = "continue" if box.clickedButton() is retry_button else "next"
            event = request.get("event")
            if event is not None:
                event.set()
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self._cursor_active = True

    def _cleanup(self) -> None:
        # The worker's C++ object may already be gone (deleteLater fired on
        # finishedOk), so disconnecting its slot can raise SystemError/RuntimeError.
        # Disconnect the dialog's signal wholesale instead - it never fails and
        # achieves the same thing.
        if self._dialog is not None:
            with contextlib.suppress(Exception):
                self._dialog.canceled.disconnect()
        if self._dialog is not None:
            self._dialog.close()
            self._dialog.deleteLater()
            self._dialog = None
        if self._cursor_active:
            QApplication.restoreOverrideCursor()
            self._cursor_active = False
        if self._thread is not None:
            self._thread.quit()
        if self._reload_counts is not None:
            self._reload_counts(self._select_table)

    def _handle_success(self, output: str) -> None:
        if self._dialog is not None:
            self._dialog.setValue(self._jobs_total)
        on_success = self._on_success
        self._cleanup()
        if on_success is not None:
            on_success(output)

    def _handle_cancelled(self, completed: int, total: int, output: str) -> None:
        title = self._title
        self._cleanup()
        QMessageBox.information(
            self._parent,
            title,
            f"Canceled. {completed} of {total} step(s) completed; "
            "tables already refreshed were kept.",
        )

    def _handle_failed(self, label: str, error: str, output: str) -> None:
        title = self._title
        self._cleanup()
        where = f" at step {label!r}" if label else ""
        QMessageBox.warning(self._parent, title, f"Capture failed{where}:\n{error}")

    def start(
        self,
        title: str,
        jobs: list[tuple[str, list[str]]],
        *,
        on_success=None,
        select_table: str = "",
    ) -> None:
        if self.running():
            QMessageBox.information(self._parent, title, "A Redshift refresh is already running.")
            return

        self._title = title
        self._jobs_total = len(jobs)
        self._on_success = on_success
        self._select_table = select_table
        self._step = 0
        self._step_label = ""

        dialog = QProgressDialog("Preparing Redshift capture ...", "Cancel", 0, 0, self._parent)
        dialog.setWindowTitle(title)
        dialog.setWindowModality(Qt.WindowModal)
        dialog.setMinimumDuration(0)
        dialog.setAutoClose(False)
        dialog.setAutoReset(False)
        dialog.setMinimumWidth(420)

        thread = QThread(self._parent)
        worker = _IngestJobsWorker(jobs)
        worker.moveToThread(thread)
        thread.started.connect(worker.run, Qt.QueuedConnection)

        # Bound methods of this GUI-thread controller -> QueuedConnection onto
        # the GUI thread. Do NOT replace with lambdas/closures: those would
        # run on the emitting worker thread.
        worker.stepStarted.connect(self._on_step, Qt.QueuedConnection)
        worker.stepFinished.connect(self._on_step_finished, Qt.QueuedConnection)
        worker.rowsFetched.connect(self._on_rows, Qt.QueuedConnection)
        worker.timeoutDecisionRequested.connect(self._on_timeout_decision, Qt.QueuedConnection)
        worker.finishedOk.connect(self._handle_success, Qt.QueuedConnection)
        worker.cancelled.connect(self._handle_cancelled, Qt.QueuedConnection)
        worker.failed.connect(self._handle_failed, Qt.QueuedConnection)
        dialog.canceled.connect(worker.request_cancel)
        # Always quit the worker thread when the job ends (do not rely only on
        # _cleanup from a queued GUI slot — that left isRunning stuck true).
        worker.finishedOk.connect(thread.quit, Qt.DirectConnection)
        worker.cancelled.connect(thread.quit, Qt.DirectConnection)
        worker.failed.connect(thread.quit, Qt.DirectConnection)
        worker.finishedOk.connect(worker.deleteLater)
        worker.cancelled.connect(worker.deleteLater)
        worker.failed.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(lambda t=thread: self._clear(t))
        self._thread = thread
        self._worker = worker
        self._dialog = dialog
        dialog.show()
        dialog.raise_()
        dialog.activateWindow()
        if not self._cursor_active:
            QApplication.setOverrideCursor(Qt.WaitCursor)
            self._cursor_active = True
        thread.start()

    def _clear(self, thread: QThread) -> None:
        if self._thread is thread:
            self._thread = None
            self._worker = None


# Emoji + font color per table, grouped by utility. Color is never the only
# signal: the Group column carries the same information as text.
_REFRESH_TABLE_STYLE = {
    "query_history": ("\U0001F4DC", "#7FB3FF", "Query History"),
    "query_text": ("\U0001F4DC", "#7FB3FF", "Query History"),
    "child_query_text": ("\U0001F9E9", "#FFD166", "Query Details"),
    "query_history_all": ("\U0001F4DC", "#7FB3FF", "Query History"),
    "query_details": ("\U0001F4CA", "#FFD166", "Query Details"),
    "query_health": ("\U0001F4CA", "#FFD166", "Query Details"),
    "query_explain": ("\U0001F4CA", "#FFD166", "Query Details"),
    "query_detail_flow": ("\U0001F4CA", "#FFD166", "Query Details"),
    "table_scan_info": ("\U0001F4CA", "#FFD166", "Query Details"),
    "user_info": ("\U0001F464", "#6FE3C1", "Users"),
    "svv_table_info_all": ("\U0001F5C4️", "#C9A0FF", "Catalog"),
    "view_definitions": ("\U0001F441️", "#C9A0FF", "Catalog"),
    "procedure_definitions": ("⚙️", "#FF7B72", "Stored Procedures"),
}


def _loaded_within_hours(value: object, hours: int) -> bool:
    timestamp = pd.to_datetime(value, errors="coerce")
    if timestamp is None or pd.isna(timestamp):
        return False
    return (pd.Timestamp.now() - timestamp) <= pd.Timedelta(hours=hours)


def _capture_high_water_datetime(value: object) -> str:
    timestamp = pd.to_datetime(value, errors="coerce")
    if timestamp is None or pd.isna(timestamp):
        return ""
    try:
        return timestamp.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(value or "").replace("T", " ").replace("Z", "").strip()


def _configured_namespace_profiles() -> list[tuple[str, str, str]]:
    """Return enabled configured profiles as (friendly name, namespace, prefix)."""
    from ..portable_config import active_profile_prefixes
    from ..secrets_store import session_secret

    profiles: list[tuple[str, str, str]] = []
    active = active_profile_prefixes()
    if active is None:
        consumer_ordinals = sorted({
            int(match.group(1))
            for key in os.environ
            if (
                match := re.match(
                    r"^REDSHIFT_CONSUMER_(\d+)_", str(key).upper()
                )
            )
        })
        prefixes = (
            "REDSHIFT_PRODUCER",
            *(
                f"REDSHIFT_CONSUMER_{ordinal}"
                for ordinal in consumer_ordinals
            ),
        )
    else:
        prefixes = active
    for prefix in prefixes:
        ordinal_match = re.fullmatch(r"REDSHIFT_CONSUMER_(\d+)", prefix)
        ordinal = int(ordinal_match.group(1)) if ordinal_match else 0
        fallback = "Producer" if ordinal == 0 else f"Consumer {ordinal}"
        host = session_secret(f"{prefix}_HOST")
        namespace = os.environ.get(f"{prefix}_NAMESPACE_ID")
        if ordinal == 0:
            host = host or session_secret("REDSHIFT_HOST")
            namespace = os.environ.get("REDSHIFT_NAMESPACE") or namespace or os.environ.get("REDSHIFT_NAMESPACE_ID")
        configured = bool(str(host or "").strip())
        enabled_raw = (
            os.environ.get("REDSHIFT_ENABLED") if ordinal == 0 else None
        ) or os.environ.get(f"{prefix}_ENABLED")
        if not _boolean_env_value(enabled_raw, default=configured):
            continue
        friendly = str(
            os.environ.get(f"{prefix}_DISPLAY_NAME")
            or (
                os.environ.get("REDSHIFT_FRIENDLY")
                if ordinal == 0
                else os.environ.get(f"{prefix}_FRIENDLY")
            )
            or (os.environ.get("REDSHIFT_ENV") if ordinal == 0 else "")
            or fallback
        ).strip()
        profiles.append((friendly, str(namespace or "").strip(), prefix))
    return profiles


def _friendly_env_key(prefix: str) -> str:
    return "REDSHIFT_FRIENDLY" if prefix == "REDSHIFT_PRODUCER" else f"{prefix}_FRIENDLY"


def _friendly_env_value(prefix: str, fallback: str = "") -> str:
    value = (
        os.environ.get(f"{prefix}_DISPLAY_NAME")
        or os.environ.get(_friendly_env_key(prefix))
    )
    if prefix == "REDSHIFT_PRODUCER":
        value = value or os.environ.get("REDSHIFT_ENV")
    return str(value or fallback).strip()


class _HealthCheckDialog(QDialog):
    """Render a DuckDB health report as a readable verdict.

    Deliberately shows every check, not just the failures: "which checks
    passed" is what tells you the warehouse is genuinely fine rather than
    untested, and an empty problem list on its own is ambiguous.
    """

    _COLORS = {"ok": PALETTE.ok, "warn": PALETTE.warn, "fail": PALETTE.crit}
    _MARKS = {"ok": "PASS", "warn": "WARN", "fail": "FAIL"}

    def __init__(self, report, parent=None):
        super().__init__(parent)
        self._report = report
        self.setWindowTitle("DuckDB Health Check")
        self.setMinimumWidth(720)
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 14, 16, 12)
        root.setSpacing(10)

        headline = QLabel(report.headline)
        headline.setStyleSheet(
            f"font-size:15px; font-weight:800; color:{self._COLORS.get(report.status, PALETTE.text_0)};"
        )
        headline.setWordWrap(True)
        root.addWidget(headline)

        path = QLabel(str(report.path))
        path.setObjectName("Caption")
        path.setWordWrap(True)
        path.setTextInteractionFlags(Qt.TextSelectableByMouse)
        root.addWidget(path)

        grid = QFrame()
        grid.setObjectName("CardSubtle")
        rows = QGridLayout(grid)
        rows.setContentsMargins(12, 10, 12, 10)
        rows.setHorizontalSpacing(12)
        rows.setVerticalSpacing(7)
        for index, check in enumerate(report.checks):
            mark = QLabel(self._MARKS.get(check.status, "?"))
            mark.setStyleSheet(
                f"font-weight:800; color:{self._COLORS.get(check.status, PALETTE.text_0)};"
            )
            rows.addWidget(mark, index, 0, Qt.AlignTop)
            name = QLabel(check.name)
            name.setStyleSheet("font-weight:700;")
            rows.addWidget(name, index, 1, Qt.AlignTop)
            detail_text = check.detail
            if check.advice and check.status != "ok":
                detail_text = f"{check.detail}\n{check.advice}"
            detail = QLabel(detail_text)
            detail.setWordWrap(True)
            detail.setMinimumWidth(0)
            detail.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Preferred)
            if check.status != "ok":
                detail.setObjectName("Caption")
            rows.addWidget(detail, index, 2, Qt.AlignTop)
        rows.setColumnStretch(2, 1)
        root.addWidget(grid)

        footer = QLabel(
            f"Checked in {report.elapsed_s:.2f}s. Read-only - nothing was modified."
        )
        footer.setObjectName("Caption")
        root.addWidget(footer)

        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        copy_btn = buttons.addButton("Copy Report", QDialogButtonBox.ActionRole)
        copy_btn.clicked.connect(self._copy)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)

    def _copy(self) -> None:
        from ..duckdb_health import format_report

        QApplication.clipboard().setText(format_report(self._report))


class RefreshSourceDialog(QDialog):
    """Dedicated Refresh Source Data modal: the full refresh workflow lives
    here (moved out of Settings), bounded to the usable screen."""

    def __init__(self, db_path: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Data Loader — Redshift to DuckDB")
        self._store = DuckDBStore(db_path or None)
        try:
            from ..ingest_redshift import load_dotenv

            load_dotenv(None)
        except Exception:
            pass
        self._settings = load_settings()
        self._loader_process: QProcess | None = None
        self._loader_stdout = ""
        self._loader_stderr = ""
        self._loader_operation = ""
        self._loader_cancel_requested = False
        self._namespace_trees: dict[str, QTreeWidget] = {}
        self._namespace_items: dict[tuple[str, str], QTreeWidgetItem] = {}

        self.setSizeGripEnabled(True)

        lay = QVBoxLayout(self)
        lay.setContentsMargins(14, 14, 14, 14)
        lay.setSpacing(10)

        title = QLabel("Redshift Data Loader")
        title.setObjectName("PageTitle")
        lay.addWidget(title)
        header = QLabel(
            "Load every enabled namespace into a recoverable DuckDB staging snapshot, review row counts, "
            "then promote it when ready. Existing live analysis remains available during collection."
        )
        header.setWordWrap(True)
        header.setObjectName("Caption")
        lay.addWidget(header)

        floor_row = QHBoxLayout()
        floor_label = QLabel("Minimum seconds:")
        floor_label.setToolTip(
            "Phase-1 characteristic floor. Every query whose time on the chosen basis is "
            "at least this many seconds is captured; parent patterns are then found "
            "within the captured set."
        )
        self._min_exec = QSpinBox()
        self._min_exec.setRange(1, 86400)
        self._min_exec.setSuffix(" s")
        self._min_exec.setValue(max(1, int(getattr(self._settings, "root_min_execution_seconds", 30))))
        self._min_exec.setToolTip(floor_label.toolTip())
        basis_label = QLabel("Basis:")
        self._floor_basis = QComboBox()
        self._floor_basis.addItem("execution_time (nets out queue/wait)", "execution_time")
        self._floor_basis.addItem("elapsed_time (wall clock incl. queue)", "elapsed_time")
        saved_basis = str(getattr(self._settings, "root_floor_basis", "execution_time"))
        basis_index = self._floor_basis.findData(saved_basis)
        self._floor_basis.setCurrentIndex(max(0, basis_index))
        self._floor_basis.setToolTip(
            "Time basis for the floor: execution_time counts only actual work; "
            "elapsed_time also includes queue and lock wait."
        )
        floor_row.addWidget(floor_label)
        floor_row.addWidget(self._min_exec)
        floor_row.addWidget(basis_label)
        floor_row.addWidget(self._floor_basis)
        self._incremental_refresh = QCheckBox("Incremental query loads")
        self._incremental_refresh.setChecked(True)
        self._incremental_refresh.setToolTip(
            "Default on: query/evidence tables append only rows newer than the current "
            "query date/id high-water mark. Catalog metadata still refreshes as a point-in-time replacement."
        )
        floor_row.addWidget(self._incremental_refresh)
        floor_row.addStretch(1)
        lay.addLayout(floor_row)

        self._high_water_mark = QLabel("Query Date and ID High Water Mark: unavailable")
        self._high_water_mark.setObjectName("Caption")
        self._high_water_mark.setWordWrap(True)
        lay.addWidget(self._high_water_mark)

        self._loader_mode_tabs = QTabWidget()
        self._loader_mode_tabs.setObjectName("CitizensTabs")
        guided_scroll = QScrollArea()
        guided_scroll.setWidgetResizable(True)
        guided_scroll.setFrameShape(QFrame.NoFrame)
        guided_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        guided_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        guided_page = QWidget()
        guided_lay = QVBoxLayout(guided_page)
        guided_lay.setContentsMargins(12, 12, 12, 12)
        guided_lay.setSpacing(9)
        guided_scroll.setWidget(guided_page)
        advanced_scroll = QScrollArea()
        advanced_scroll.setWidgetResizable(True)
        advanced_scroll.setFrameShape(QFrame.NoFrame)
        advanced_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        advanced_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        advanced_page = QWidget()
        advanced_lay = QVBoxLayout(advanced_page)
        advanced_lay.setContentsMargins(12, 12, 12, 12)
        advanced_lay.setSpacing(9)
        advanced_scroll.setWidget(advanced_page)
        self._loader_mode_tabs.addTab(guided_scroll, "Guided Namespace Load")
        self._loader_mode_tabs.addTab(advanced_scroll, "Advanced Table Refresh")
        self._loader_mode_tabs.setTabToolTip(0, "Recommended: load, review, resume if interrupted, and promote all enabled namespaces.")
        self._loader_mode_tabs.setTabToolTip(1, "Target individual tables or use the legacy empty/all-table refresh controls.")
        lay.addWidget(self._loader_mode_tabs, 1)

        namespace_header = QLabel("LOAD PLAN BY CLUSTER")
        namespace_header.setObjectName("SectionHeader")
        guided_lay.addWidget(namespace_header)
        namespace_hint = QLabel(
            "Select a friendly cluster tab to inspect its tables. Source Rows is the verified Redshift fetch count; "
            "DuckDB Rows shows the current live count, then the staged count as each step completes."
        )
        namespace_hint.setObjectName("Caption")
        namespace_hint.setWordWrap(True)
        guided_lay.addWidget(namespace_hint)
        self._namespace_status = QLabel("Ready. No live DuckDB tables will change until promotion.")
        self._namespace_status.setObjectName("CitizensStatus")
        self._namespace_status.setWordWrap(True)
        self._namespace_status.setContentsMargins(10, 8, 10, 8)
        guided_lay.addWidget(self._namespace_status)
        self._namespace_tabs = QTabWidget()
        self._namespace_tabs.setObjectName("CitizensTabs")
        self._namespace_tabs.setMinimumHeight(180)
        guided_lay.addWidget(self._namespace_tabs, 1)
        self._namespace_progress = QProgressBar()
        self._namespace_progress.setObjectName("CitizensProgress")
        self._namespace_progress.setRange(0, 100)
        self._namespace_progress.setValue(0)
        self._namespace_progress.setFormat("Namespace load progress: %p%")
        guided_lay.addWidget(self._namespace_progress)
        namespace_actions = QHBoxLayout()
        self._cycle_load = QPushButton("Start Safe Load")
        self._cycle_load.setObjectName("CitizensPrimary")
        self._cycle_load.setMinimumHeight(36)
        self._promote_namespaces = QPushButton("Review Complete — Promote")
        self._promote_namespaces.setObjectName("CitizensSecondary")
        self._promote_namespaces.setMinimumHeight(36)
        self._promote_namespaces.setEnabled(False)
        namespace_actions.addWidget(self._cycle_load)
        namespace_actions.addWidget(self._promote_namespaces)
        namespace_actions.addStretch(1)
        guided_lay.addLayout(namespace_actions)

        advanced_notice = QLabel(
            "Advanced controls preserve the original table-by-table workflow. Use these only for a targeted repair; "
            "the Guided Namespace Load is the standard team workflow."
        )
        advanced_notice.setObjectName("Caption")
        advanced_notice.setWordWrap(True)
        advanced_lay.addWidget(advanced_notice)

        self._tree = QTreeWidget()
        self._tree.setColumnCount(5)
        self._tree.setHeaderLabels(["Order", "Table", "Group", "Records", "Last Capture"])
        self._tree.setRootIsDecorated(False)
        self._tree.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self._tree.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._tree.setAlternatingRowColors(True)
        advanced_lay.addWidget(self._tree, 1)

        buttons = QHBoxLayout()
        refresh_selected = QPushButton("Refresh Checked")
        refresh_selected.setObjectName("Ghost")
        refresh_selected.setToolTip(
            "Refresh the checked tables. Tables loaded within the past 12 hours "
            "start unchecked; stale or empty tables start checked."
        )
        refresh_empty = QPushButton("Refresh Empty Tables")
        refresh_empty.setObjectName("Ghost")
        refresh_all = QPushButton("Refresh All")
        refresh_all.setObjectName("Primary")
        rebuild_indexes = QPushButton("Build Missing Indexes")
        rebuild_indexes.setObjectName("Ghost")
        truncate_checked = QPushButton("Truncate Checked")
        truncate_checked.setObjectName("Ghost")
        truncate_checked.setToolTip("Advanced: delete all local DuckDB rows from the checked tables after confirmation.")
        buttons.addWidget(refresh_selected)
        buttons.addWidget(refresh_empty)
        buttons.addWidget(refresh_all)
        buttons.addWidget(rebuild_indexes)
        buttons.addWidget(truncate_checked)
        buttons.addStretch(1)
        advanced_lay.addLayout(buttons)

        footer = QHBoxLayout()
        footer_note = QLabel("Loads are staged and recoverable. Live tables change only after Review Complete — Promote.")
        footer_note.setObjectName("Caption")
        close_btn = QPushButton("Close")
        close_btn.setObjectName("Ghost")
        footer.addWidget(footer_note)
        footer.addStretch(1)
        footer.addWidget(close_btn)
        lay.addLayout(footer)

        refresh_selected.clicked.connect(self._refresh_selected)
        refresh_empty.clicked.connect(self._refresh_empty)
        refresh_all.clicked.connect(self._refresh_all)
        rebuild_indexes.clicked.connect(self._rebuild_indexes)
        truncate_checked.clicked.connect(self._truncate_checked)
        close_btn.clicked.connect(self.reject)
        self._cycle_load.clicked.connect(self._start_namespace_cycle_load)
        self._promote_namespaces.clicked.connect(self._promote_namespace_load)

        self._load_rows()
        self._load_namespace_tabs()
        QTimer.singleShot(0, self._fit_to_available_screen)

    def reject(self) -> None:
        if self._loader_running():
            QMessageBox.information(
                self,
                "Data Loader Running",
                "The loader is still running. Use Cancel Load first, or leave this window open to monitor progress.",
            )
            return
        super().reject()

    def _fit_to_available_screen(self) -> None:
        """Keep every control reachable on small or display-scaled laptops."""
        screen = self.parentWidget().screen() if self.parentWidget() is not None else self.screen()
        screen = screen or QApplication.primaryScreen()
        if screen is None:
            return
        geo = screen.availableGeometry()
        margin = 32
        max_width = max(480, geo.width() - margin)
        max_height = max(360, geo.height() - margin)
        self.setMaximumSize(max_width, max_height)
        width = min(max_width, max(760, int(geo.width() * 0.68)))
        height = min(max_height, max(480, int(geo.height() * 0.72)))
        self.resize(width, height)
        self.move(
            geo.x() + max(0, (geo.width() - width) // 2),
            geo.y() + max(0, (geo.height() - height) // 2),
        )

    def _namespace_table_names(self) -> list[str]:
        try:
            import runner
            return list(runner.ROOT_ORDER) + [
                name for name in runner.EXTRACTIONS if name not in runner.ROOT_TABLE_SET
            ] + [
                "svv_table_info_all", "view_definitions", "procedure_definitions",
                "external_table_info_all",
            ]
        except Exception:
            from ..ingest_redshift import LIVE_REFRESH_TABLES
            return list(LIVE_REFRESH_TABLES)

    def _load_namespace_tabs(self) -> None:
        self._promote_namespaces.setEnabled(False)
        self._namespace_tabs.clear()
        self._namespace_trees.clear()
        self._namespace_items.clear()
        profiles = _configured_namespace_profiles()
        if not profiles:
            empty = QLabel("No enabled namespace clusters are configured. Open Settings -> Data Sources.")
            empty.setWordWrap(True)
            self._namespace_tabs.addTab(empty, "No clusters")
            self._cycle_load.setEnabled(False)
            return
        self._cycle_load.setEnabled(True)
        table_names = self._namespace_table_names()
        all_checkpoints = self._namespace_checkpoint_counts()
        staged_status = self._namespace_staged_status()
        checkpoint_total = 0
        for friendly_name, namespace_id, prefix in profiles:
            tree = QTreeWidget()
            tree.setObjectName("CitizensDataGrid")
            tree.setColumnCount(6)
            tree.setHeaderLabels(["Order", "DuckDB Table", "Redshift Rows", "DuckDB Rows", "Status", "Progress"])
            tree.setRootIsDecorated(False)
            tree.setAlternatingRowColors(True)
            tree.setToolTip(f"{prefix} | namespace_id={namespace_id or 'MISSING'}")
            live_counts = self._namespace_duckdb_counts(
                namespace_id,
                table_names,
                producer=prefix == "REDSHIFT_PRODUCER",
            )
            checkpoints = all_checkpoints.get(namespace_id.lower(), {})
            for index, table_name in enumerate(table_names, start=1):
                checkpoint_rows = checkpoints.get(table_name)
                checkpoint_total += 1 if checkpoint_rows is not None else 0
                item = QTreeWidgetItem([
                    f"{index:02d}", table_name,
                    f"{checkpoint_rows:,}" if checkpoint_rows is not None else "—",
                    f"{checkpoint_rows:,}" if checkpoint_rows is not None else f"{live_counts.get(table_name, 0):,}",
                    "Recovered checkpoint" if checkpoint_rows is not None else "Waiting",
                    "100%" if checkpoint_rows is not None else "0%",
                ])
                item.setData(0, Qt.UserRole, table_name)
                item.setTextAlignment(2, Qt.AlignRight | Qt.AlignVCenter)
                item.setTextAlignment(3, Qt.AlignRight | Qt.AlignVCenter)
                if checkpoint_rows is not None:
                    success_brush = QBrush(QColor("#008555"))
                    item.setForeground(4, success_brush)
                    item.setForeground(5, success_brush)
                tree.addTopLevelItem(item)
                self._namespace_items[(namespace_id.lower(), table_name)] = item
            for column in range(6):
                tree.resizeColumnToContents(column)
            label = friendly_name if namespace_id else f"{friendly_name} (namespace required)"
            self._namespace_tabs.addTab(tree, label)
            self._namespace_tabs.setTabToolTip(self._namespace_tabs.count() - 1, f"Namespace: {namespace_id or 'missing'}")
            self._namespace_trees[namespace_id.lower()] = tree
        total_rows = max(1, len(table_names) * len(profiles))
        if checkpoint_total:
            self._namespace_progress.setValue(int(round((checkpoint_total / total_rows) * 100)))
            if staged_status == "loaded":
                self._namespace_progress.setFormat("Staged namespace load complete: %p%")
                self._cycle_load.setText("Start New Load")
                self._promote_namespaces.setEnabled(True)
                self._namespace_status.setText("Collection complete. Review the counts, then promote when satisfied.")
            else:
                self._namespace_progress.setFormat("Recoverable staged progress: %p%")
                self._cycle_load.setText("Resume Safe Load")
                self._namespace_status.setText(
                    "An interrupted staged load was found. Completed tables are preserved and will be skipped."
                )
        else:
            self._namespace_progress.setValue(0)
            self._namespace_progress.setFormat("Namespace load progress: %p%")
            self._cycle_load.setText("Start Safe Load")
            self._namespace_status.setText("Ready. No live DuckDB tables will change until promotion.")

    def _namespace_staged_status(self) -> str:
        if not self._store.path.is_file():
            return ""
        try:
            import runner
            con = runner.open_duck(self._store.path, 2)
            try:
                return str(runner.read_state(con).get("status") or "")
            finally:
                con.close()
        except Exception:
            return ""

    def _namespace_checkpoint_counts(self) -> dict[str, dict[str, int]]:
        if not self._store.path.is_file():
            return {}
        result: dict[str, dict[str, int]] = {}
        try:
            with self._store.connect() as con:
                exists = con.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE LOWER(table_name) = '_tmp_namespace_refresh_state'"
                ).fetchone()[0]
                if not exists:
                    return {}
                rows = con.execute(
                    "SELECT namespace_id, table_name, source_rows "
                    "FROM _tmp_namespace_refresh_state WHERE status = 'complete'"
                ).fetchall()
            for namespace_id, table_name, source_rows in rows:
                result.setdefault(str(namespace_id).lower(), {})[str(table_name)] = int(source_rows or 0)
        except Exception:
            return {}
        return result

    def _namespace_duckdb_counts(
        self,
        namespace_id: str,
        table_names: list[str],
        *,
        producer: bool = False,
    ) -> dict[str, int]:
        if not namespace_id or not self._store.path.is_file():
            return {}
        counts: dict[str, int] = {}
        try:
            with self._store.connect() as con:
                existing = {
                    str(row[0]).lower() for row in con.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
                    ).fetchall()
                }
                for table_name in table_names:
                    if table_name.lower() not in existing:
                        continue
                    try:
                        table_columns = {
                            str(column[1]).lower()
                            for column in con.execute(f'PRAGMA table_info("{table_name}")').fetchall()
                        }
                        if "namespace_id" not in table_columns:
                            # Tables captured before multi-cluster support are
                            # producer-owned. Never attribute them to consumers.
                            row = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone() if producer else (0,)
                        elif producer:
                            # Historical single-cluster rows were explicitly
                            # stamped "producer". Treat that marker (and legacy
                            # blanks) as the configured producer namespace only.
                            row = con.execute(
                                f'''SELECT COUNT(*) FROM "{table_name}"
                                    WHERE LOWER(COALESCE(NULLIF(TRIM(CAST(namespace_id AS VARCHAR)), ''), 'producer'))
                                          IN (LOWER(?), 'producer')''',
                                [namespace_id],
                            ).fetchone()
                        else:
                            row = con.execute(
                                f'SELECT COUNT(*) FROM "{table_name}" WHERE LOWER(namespace_id) = LOWER(?)',
                                [namespace_id],
                            ).fetchone()
                        counts[table_name] = int(row[0] or 0)
                    except Exception:
                        counts[table_name] = 0
        except Exception:
            return {}
        return counts

    def _rebuild_indexes(self) -> None:
        response = QMessageBox.question(
            self,
            "Build Missing Indexes",
            "Create missing local DuckDB indexes? Existing indexes remain unchanged and Redshift is not queried.",
        )
        if response != QMessageBox.Yes:
            return
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            with self._store.connect() as con:
                before = int(con.execute("SELECT COUNT(*) FROM duckdb_indexes()").fetchone()[0] or 0)
                self._store.rebuild_indexes(con)
                after = int(con.execute("SELECT COUNT(*) FROM duckdb_indexes()").fetchone()[0] or 0)
            QMessageBox.information(
                self,
                "Build Missing Indexes",
                f"Local indexes are ready. Created {max(0, after - before):,}; total indexes: {after:,}.",
            )
        except Exception as exc:
            QMessageBox.warning(self, "Build Missing Indexes", str(exc))
        finally:
            QApplication.restoreOverrideCursor()

    def _truncate_checked(self) -> None:
        tables = self._selected_tables()
        if not tables:
            QMessageBox.information(self, "Truncate Tables", "Check one or more tables first.")
            return
        response = QMessageBox.question(
            self,
            "Truncate Tables",
            f"Delete every local DuckDB row from {len(tables)} checked table(s)?\n\n" + ", ".join(tables),
        )
        if response != QMessageBox.Yes:
            return
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            with self._store.connect() as con:
                for table_name in tables:
                    self._store.truncate_table(con, table_name)
            self._load_rows()
            self._load_namespace_tabs()
        except Exception as exc:
            QMessageBox.warning(self, "Truncate Tables", str(exc))
        finally:
            QApplication.restoreOverrideCursor()

    def _start_namespace_cycle_load(self) -> None:
        if self._loader_running():
            self._cancel_loader()
            return
        missing = [name for name, namespace, _prefix in _configured_namespace_profiles() if not namespace]
        if missing:
            QMessageBox.warning(
                self, "Namespace Cluster Loader",
                f"Every enabled cluster requires a namespace ID. Missing: {', '.join(missing)}",
            )
            return
        response = QMessageBox.question(
            self, "Namespace Cluster Loader",
            "Load every enabled namespace cluster into safe *_tmp staging tables? Existing live tables remain available until promotion.",
        )
        if response != QMessageBox.Yes:
            return
        self._namespace_progress.setValue(0)
        self._namespace_status.setText("Preparing connections and validating enabled namespace clusters …")
        self._promote_namespaces.setEnabled(False)
        self._cycle_load.setEnabled(False)
        self.setCursor(Qt.BusyCursor)
        for item in self._namespace_items.values():
            item.setText(2, "—")
            item.setText(4, "Waiting")
            item.setText(5, "0%")
        from ..loader import LoaderRequest, build_loader_command

        request = LoaderRequest(
            duckdb_path=self._store.path,
            days=7.0,
            floor_seconds=float(self._min_exec.value()),
            floor_basis=str(self._floor_basis.currentData() or "execution_time"),
            resume=True,
            promote=False,
            include_external=False,  # external capture is excluded in this version
            backup_before_promote=True,
            external_timeout_action="ask",
        )
        self._start_loader_process(
            build_loader_command(request, json_events=True),
            operation="guided-stage",
        )

    def _loader_running(self) -> bool:
        process = self._loader_process
        return process is not None and process.state() != QProcess.NotRunning

    def _start_loader_process(self, command: list[str], *, operation: str) -> None:
        if self._loader_running():
            QMessageBox.information(self, "Data Loader", "A Infraredshift load is already running.")
            return
        if not command:
            QMessageBox.warning(self, "Data Loader", "The loader command could not be created.")
            return
        process = QProcess(self)
        process.setProcessChannelMode(QProcess.SeparateChannels)
        process.setProgram(command[0])
        process.setArguments(command[1:])
        if len(command) > 1:
            candidate = Path(command[1])
            if candidate.is_file():
                process.setWorkingDirectory(str(candidate.resolve().parent))
        process.readyReadStandardOutput.connect(self._read_loader_stdout)
        process.readyReadStandardError.connect(self._read_loader_stderr)
        process.finished.connect(self._loader_finished)
        process.errorOccurred.connect(self._loader_process_error)
        self._loader_process = process
        self._loader_stdout = ""
        self._loader_stderr = ""
        self._loader_operation = operation
        self._loader_cancel_requested = False
        self._cycle_load.setText("Cancel Load")
        self._cycle_load.setEnabled(True)
        self._promote_namespaces.setEnabled(False)
        self.setCursor(Qt.BusyCursor)
        process.start()

    def _cancel_loader(self) -> None:
        process = self._loader_process
        if process is None or process.state() == QProcess.NotRunning:
            return
        response = QMessageBox.question(
            self,
            "Cancel Data Load",
            "Stop the active loader? Completed table checkpoints will be preserved and Resume Safe Load will continue from them.",
        )
        if response != QMessageBox.Yes:
            return
        self._loader_cancel_requested = True
        self._namespace_status.setText("Stopping safely … completed checkpoints will be preserved.")
        process.terminate()

        def force_stop() -> None:
            if process.state() != QProcess.NotRunning:
                process.kill()

        QTimer.singleShot(3000, force_stop)

    def _read_loader_stdout(self) -> None:
        process = self._loader_process
        if process is None:
            return
        self._loader_stdout += bytes(process.readAllStandardOutput()).decode("utf-8", errors="replace")
        self._loader_stdout = self._consume_loader_lines(self._loader_stdout)

    def _read_loader_stderr(self) -> None:
        process = self._loader_process
        if process is None:
            return
        self._loader_stderr += bytes(process.readAllStandardError()).decode("utf-8", errors="replace")
        # Keep diagnostics bounded; they are shown only if the process fails.
        if len(self._loader_stderr) > 40_000:
            self._loader_stderr = self._loader_stderr[-40_000:]

    def _consume_loader_lines(self, value: str, *, final: bool = False) -> str:
        lines = value.splitlines(keepends=True)
        if not final and lines and not lines[-1].endswith(("\n", "\r")):
            remainder = lines.pop()
        else:
            remainder = ""
        for raw_line in lines:
            line = raw_line.strip()
            if not line.startswith("INFRAREDSHIFT_EVENT "):
                continue
            try:
                payload = json.loads(line[len("INFRAREDSHIFT_EVENT "):])
            except (TypeError, ValueError):
                continue
            self._handle_loader_event(payload)
        return remainder

    def _handle_loader_event(self, payload: dict) -> None:
        event = str(payload.get("event") or "")
        message = str(payload.get("message") or "")
        if event == "progress":
            self._on_namespace_progress(
                str(payload.get("namespace_id") or ""),
                str(payload.get("table_name") or ""),
                int(payload.get("source_rows") or 0),
                int(payload.get("duckdb_rows") or 0),
                int(payload.get("completed") or 0),
                int(payload.get("total") or 0),
                message,
            )
        elif event == "external_timeout":
            self._on_namespace_timeout_decision(str(payload.get("table_name") or "external metadata"), message)
        elif event in {"started", "staged", "promoting", "completed"}:
            self._namespace_status.setText(message)
            if event == "promoting":
                self._namespace_progress.setFormat("Promoting reviewed staging: %p%")

    def _on_namespace_timeout_decision(self, stage: str, error: str) -> None:
        # The load remains busy, but the choice dialog itself must use a normal
        # pointer so its two actions feel immediately clickable.
        self.unsetCursor()
        box = QMessageBox(self)
        box.setWindowTitle("External Metadata Stage Timed Out")
        box.setIcon(QMessageBox.Warning)
        box.setText(f"The external metadata stage exceeded 10 minutes:\n\n{stage}")
        box.setInformativeText(
            "Continue retries this stage with another 10-minute window. "
            "Next Step preserves completed work and continues without this optional stage.\n\n"
            f"Technical message: {error}"
        )
        retry_button = box.addButton("Continue — Retry Stage", QMessageBox.AcceptRole)
        next_button = box.addButton("Next Step — Skip Stage", QMessageBox.DestructiveRole)
        box.setDefaultButton(next_button)
        box.exec()
        process = self._loader_process
        if process is not None and process.state() != QProcess.NotRunning:
            answer = "retry\n" if box.clickedButton() is retry_button else "skip\n"
            process.write(answer.encode("utf-8"))
        self.setCursor(Qt.BusyCursor)

    def _on_namespace_progress(self, namespace_id: str, table_name: str, source_rows: int, duckdb_rows: int, completed: int, total: int, status: str) -> None:
        item = self._namespace_items.get((namespace_id.lower(), table_name))
        percent = int(round((completed / max(1, total)) * 100))
        self._namespace_progress.setValue(max(0, min(100, percent)))
        if item is not None:
            if status in {"Staged in DuckDB", "Recovered checkpoint"}:
                item.setText(2, f"{source_rows:,}")
                item.setText(3, f"{duckdb_rows:,}")
            item.setText(4, status)
            item.setText(5, "100%" if status in {"Staged in DuckDB", "Recovered checkpoint"} else "Working")
            status_color = "#008555" if status in {"Staged in DuckDB", "Recovered checkpoint"} else "#8A5200"
            item.setForeground(4, QBrush(QColor(status_color)))
            item.setForeground(5, QBrush(QColor(status_color)))
            tree = self._namespace_trees.get(namespace_id.lower())
            if tree is not None:
                tree.scrollToItem(item)
        self._namespace_status.setText(f"{status}: {table_name} ({namespace_id})")

    def _namespace_load_finished(self) -> None:
        self._namespace_progress.setValue(100)
        self._namespace_progress.setFormat("Namespace load staged successfully: %p%")
        self._namespace_status.setText("Collection complete. Review source and staged counts, then promote when satisfied.")
        self._cycle_load.setEnabled(True)
        self.unsetCursor()
        self._promote_namespaces.setEnabled(True)
        QMessageBox.information(
            self, "Namespace Cluster Loader",
            "All enabled namespace clusters were loaded into DuckDB staging tables. Review the counts, then promote the staged load.",
        )

    def _namespace_load_failed(self, error: str) -> None:
        self._namespace_progress.setFormat("Namespace load stopped")
        self._namespace_status.setText("Load stopped safely. Completed checkpoints were preserved; use Resume Safe Load.")
        self._cycle_load.setEnabled(True)
        self.unsetCursor()
        self._load_namespace_tabs()
        QMessageBox.warning(self, "Namespace Cluster Loader", f"Namespace load failed:\n{error}")

    def _loader_process_error(self, _error: QProcess.ProcessError) -> None:
        process = self._loader_process
        if process is None or process.state() != QProcess.NotRunning:
            return
        self._loader_process = None
        process.deleteLater()
        self._finish_loader_ui()
        self._namespace_load_failed(
            "The Infraredshift loader process could not be started. Verify the application files and Python installation."
        )

    def _finish_loader_ui(self) -> None:
        self.unsetCursor()
        self._cycle_load.setEnabled(True)
        self._cycle_load.setText("Resume Safe Load")

    def _loader_finished(self, exit_code: int, _exit_status: QProcess.ExitStatus) -> None:
        process = self._loader_process
        if process is None:
            return
        if process is not None:
            self._loader_stdout += bytes(process.readAllStandardOutput()).decode("utf-8", errors="replace")
            self._loader_stderr += bytes(process.readAllStandardError()).decode("utf-8", errors="replace")
            self._consume_loader_lines(self._loader_stdout, final=True)
        operation = self._loader_operation
        cancelled = self._loader_cancel_requested
        diagnostics = self._loader_stderr.strip()
        self._loader_process = None
        self._loader_operation = ""
        self._finish_loader_ui()
        if process is not None:
            process.deleteLater()
        if cancelled:
            self._namespace_status.setText("Load stopped safely. Completed checkpoints were preserved.")
            self._load_namespace_tabs()
            QMessageBox.information(
                self, "Data Loader", "The load was stopped. Completed table checkpoints were preserved; Resume Safe Load will continue from them."
            )
            return
        if exit_code != 0:
            if not diagnostics:
                diagnostics = f"Loader exited with code {exit_code}."
            self._namespace_load_failed(diagnostics[-4000:])
            return
        self._load_rows()
        self._load_namespace_tabs()
        if operation == "guided-stage":
            self._namespace_load_finished()
        elif operation == "promote":
            self._promote_namespaces.setEnabled(False)
            self._namespace_status.setText("Promotion complete. The reviewed namespace snapshot is now live.")
            QMessageBox.information(self, "Promote Namespace Load", "The namespaced staging tables are now live.")
        else:
            self._namespace_status.setText("Selected tables refreshed and promoted successfully.")
            QMessageBox.information(self, "Refresh Source Data", "Selected tables were refreshed and promoted successfully.")

    def _promote_namespace_load(self) -> None:
        response = QMessageBox.question(
            self, "Promote Namespace Load",
            "Back up the current DuckDB file and promote the reviewed *_tmp tables now?",
        )
        if response != QMessageBox.Yes:
            return
        from ..loader import build_promote_command

        self._start_loader_process(
            build_promote_command(self._store.path, json_events=True),
            operation="promote",
        )

    def _load_rows(self) -> None:
        from ..ingest_redshift import LIVE_REFRESH_TABLES

        counts: dict[str, tuple[int, object]] = {}
        try:
            with self._store.connect() as con:
                frame = self._store.table_counts(con)
            for _, row in frame.iterrows():
                counts[str(row.get("table_name"))] = (
                    int(row.get("record_count") or 0),
                    row.get("latest_captured_at"),
                )
        except Exception:
            counts = {}
        # Live reloads must not clobber checkbox choices the user already made.
        previous_states: dict[str, Qt.CheckState] = {}
        for row_index in range(self._tree.topLevelItemCount()):
            existing = self._tree.topLevelItem(row_index)
            previous_states[str(existing.data(0, Qt.UserRole) or "")] = existing.checkState(0)
        self._tree.clear()
        for index, name in enumerate(LIVE_REFRESH_TABLES, start=1):
            emoji, color, group = _REFRESH_TABLE_STYLE.get(name, ("", "#F5F7FF", ""))
            record_count, last_capture = counts.get(name, (0, None))
            item = QTreeWidgetItem(
                [
                    f"{index:02d}",
                    f"{emoji} {name}".strip(),
                    group,
                    f"{record_count:,}",
                    _fmt_capture_datetime(last_capture),
                ]
            )
            item.setData(0, Qt.UserRole, name)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            if name in previous_states:
                item.setCheckState(0, previous_states[name])
            else:
                item.setCheckState(0, Qt.Unchecked if _loaded_within_hours(last_capture, 12) else Qt.Checked)
            brush = QBrush(QColor(color))
            for col in range(5):
                item.setForeground(col, brush)
            item.setTextAlignment(3, Qt.AlignRight | Qt.AlignVCenter)
            self._tree.addTopLevelItem(item)
        for col in range(5):
            self._tree.resizeColumnToContents(col)
        self._update_high_water_mark()

    def _update_high_water_mark(self) -> None:
        mark = self._query_high_water_mark()
        if mark is None:
            self._high_water_mark.setText("Query Date and ID High Water Mark: unavailable")
            return
        query_id, start_time = mark
        self._high_water_mark.setText(
            f"Query Date and ID High Water Mark: QUERY: {query_id}  Date: {_fmt_capture_datetime(start_time)}"
        )

    def _query_high_water_mark(self) -> tuple[str, object] | None:
        try:
            with self._store.connect() as con:
                return self._store.query_high_water_mark(con)
        except Exception:
            return None

    def _selected_tables(self) -> list[str]:
        return [
            str(item.data(0, Qt.UserRole) or "")
            for item in (self._tree.topLevelItem(i) for i in range(self._tree.topLevelItemCount()))
            if item.checkState(0) == Qt.Checked
        ]

    def _capture_args(self, *, incremental: bool | None = None) -> list[str]:
        self._settings = load_settings()
        floor_value = max(1, int(self._min_exec.value()))
        floor_basis = str(self._floor_basis.currentData() or "execution_time")
        if floor_value != int(getattr(self._settings, "root_min_execution_seconds", 0)) or floor_basis != str(
            getattr(self._settings, "root_floor_basis", "")
        ):
            self._settings.root_min_execution_seconds = floor_value
            self._settings.root_floor_basis = floor_basis
            save_settings(self._settings)
        args = [
            "--evidence-parent-limit",
            str(self._settings.capture_query_limit),
            "--min-execution-seconds",
            str(floor_value),
            "--floor-basis",
            floor_basis,
        ]
        use_incremental = self._incremental_refresh.isChecked() if incremental is None else bool(incremental)
        if use_incremental:
            mark = self._query_high_water_mark()
            if mark is not None:
                query_id, start_time = mark
                args.append("--incremental")
                if query_id:
                    args += ["--incremental-after-query-id", str(query_id)]
                start_arg = _capture_high_water_datetime(start_time)
                if start_arg:
                    args += ["--incremental-after-time", start_arg]
        return args

    def _refresh_selected(self) -> None:
        tables = self._selected_tables()
        if not tables:
            QMessageBox.information(self, "Refresh Source Data", "Check one or more tables first.")
            return
        self._start_tables(tables)

    def _refresh_all(self) -> None:
        from ..ingest_redshift import LIVE_REFRESH_TABLES

        self._start_tables(list(LIVE_REFRESH_TABLES))

    def _start_tables(self, tables: list[str]) -> None:
        from ..ingest_redshift import REFRESH_ORDER

        tables = [name for name in tables if name]
        if "external_table_info_all" in tables:
            tables = [name for name in tables if name != "external_table_info_all"]
            if not tables:
                QMessageBox.information(
                    self, "Refresh Source Data",
                    "External-table capture is excluded in this version.",
                )
                return
        tables.sort(key=lambda name: REFRESH_ORDER.get(name, len(REFRESH_ORDER) + 1))
        if not tables:
            return
        ordered_label = "\n".join(f"{i}. {name}" for i, name in enumerate(tables, start=1))
        response = QMessageBox.question(
            self,
            "Refresh Source Data",
            f"Reload {len(tables)} table(s) from Redshift into the latest DuckDB snapshot, "
            f"in this order?\n\n{ordered_label}",
        )
        if response != QMessageBox.Yes:
            return
        from ..loader import LoaderRequest, build_loader_command

        request = LoaderRequest(
            duckdb_path=self._store.path,
            days=7.0,
            floor_seconds=float(self._min_exec.value()),
            floor_basis=str(self._floor_basis.currentData() or "execution_time"),
            resume=True,
            promote=True,
            include_external=False,  # external capture is excluded in this version
            backup_before_promote=True,
            external_timeout_action="ask",
            include_tables=tuple(tables),
        )
        self._namespace_status.setText("Starting recoverable selected-table refresh …")
        self._start_loader_process(
            build_loader_command(request, json_events=True),
            operation="targeted-refresh",
        )

    def _refresh_empty(self) -> None:
        empty_tables: list[str] = []
        for row_index in range(self._tree.topLevelItemCount()):
            item = self._tree.topLevelItem(row_index)
            table_name = str(item.data(0, Qt.UserRole) or "")
            displayed = str(item.text(3) or "0").replace(",", "").strip()
            try:
                record_count = int(displayed)
            except ValueError:
                record_count = 0
            if table_name and record_count == 0:
                empty_tables.append(table_name)
        include = {name for name in (self._settings.capture_include_tables or []) if name}
        if include:
            empty_tables = [name for name in empty_tables if name in include]
        if not empty_tables:
            QMessageBox.information(self, "Refresh Source Data", "No empty DuckDB tables need refreshing.")
            return
        self._start_tables(empty_tables)


class _ConfigDialog(QDialog):
    def __init__(self, db_path: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Analyzer Configuration")
        self._store = DuckDBStore(db_path or None)
        self._model: _DataFrameModel | None = None
        self._display_df = pd.DataFrame()
        self._settings = load_settings()
        self._busy_depth = 0
        self._busy_dialog: QProgressDialog | None = None
        self._last_selected_table = ""
        self._trim_preview: dict | None = None
        self._ingest = _IngestJobController(
            self, reload_counts=lambda select_table="": self._load_counts(selected_table=select_table)
        )

        root = QVBoxLayout(self)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(10)

        try:
            from ..ingest_redshift import load_dotenv

            load_dotenv(None)
        except Exception:
            pass

        tabs = QTabWidget()
        tabs.setObjectName("AnalyzerConfigurationTabs")
        self._tabs = tabs
        root.addWidget(tabs, 1)

        def add_scroll_tab(label: str) -> QVBoxLayout:
            scroll = QScrollArea()
            scroll.setWidgetResizable(True)
            scroll.setFrameShape(QFrame.NoFrame)
            scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
            scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
            body = QWidget()
            body_lay = QVBoxLayout(body)
            body_lay.setContentsMargins(0, 0, 0, 0)
            body_lay.setSpacing(10)
            scroll.setWidget(body)
            tabs.addTab(scroll, label)
            return body_lay

        data_sources_lay = add_scroll_tab("Data Sources")
        grouping_lay = add_scroll_tab("Query Grouping")
        database_lay = add_scroll_tab("Database Discovery")
        source_statements_lay = add_scroll_tab("Source SQL Statements")
        self._build_grouping_tab(grouping_lay)

        # Retain the internal count model for source-SQL/manual-load refresh
        # callbacks, but present all DuckDB row counts in the dedicated Data
        # Loader. This avoids two competing dataset-status screens.
        self._counts_status = QLabel("")
        self._table = QTableView()
        _configure_table_view(self._table)
        self._table.setSelectionMode(QAbstractItemView.ExtendedSelection)

        secure_env = QFrame()
        secure_env.setObjectName("CardSubtle")
        secure_env_lay = QVBoxLayout(secure_env)
        secure_env_lay.setContentsMargins(12, 10, 12, 10)
        secure_env_lay.setSpacing(7)
        secure_env_head = QLabel("LOCAL CLUSTER CREDENTIALS")
        secure_env_head.setObjectName("SectionHeader")
        secure_env_lay.addWidget(secure_env_head)
        secure_env_note = QLabel(
            "The demonstration-safe .env contains only cluster identity and loader settings. Server addresses, usernames, and passwords "
            "are stored in a Windows-account and Infraredshift-credential protected .secrets file."
        )
        secure_env_note.setObjectName("Caption")
        secure_env_note.setWordWrap(True)
        secure_env_lay.addWidget(secure_env_note)
        edit_secure_env = QPushButton("Edit Local Credentials")
        edit_secure_env.setObjectName("Primary")
        secure_env_lay.addWidget(edit_secure_env, 0, Qt.AlignLeft)
        self._secure_env_status = QLabel(self._environment_status())
        self._secure_env_status.setObjectName("Caption")
        self._secure_env_status.setWordWrap(True)
        secure_env_lay.addWidget(self._secure_env_status)
        data_sources_lay.addWidget(secure_env)

        conn = QFrame()
        conn.setObjectName("CardSubtle")
        conn_lay = QVBoxLayout(conn)
        conn_lay.setContentsMargins(12, 10, 12, 10)
        conn_lay.setSpacing(6)
        head = QLabel("REDSHIFT CONNECTIONS")
        head.setObjectName("SectionHeader")
        conn_lay.addWidget(head)
        conn_lay.addWidget(QLabel(self._connection_summary()))
        data_sources_lay.addWidget(conn)

        clusters = QFrame()
        clusters.setObjectName("CardSubtle")
        clusters_lay = QVBoxLayout(clusters)
        clusters_lay.setContentsMargins(12, 10, 12, 10)
        clusters_lay.setSpacing(7)
        clusters_head = QLabel("CLUSTERS TO LOAD")
        clusters_head.setObjectName("SectionHeader")
        clusters_lay.addWidget(clusters_head)
        clusters_hint = QLabel(
            "Check each configured cluster that the general runner and focused external-table loader should contact. "
            "Unchecked profiles remain in .env but are skipped."
        )
        clusters_hint.setObjectName("Caption")
        clusters_hint.setWordWrap(True)
        clusters_lay.addWidget(clusters_hint)
        self._cluster_checks: dict[str, QCheckBox] = {}
        self._cluster_name_edits: dict[str, QLineEdit] = {}
        for prefix, label, configured, namespace_id, host, enabled in self._cluster_profiles_from_env():
            detail = " • ".join(value for value in (namespace_id, host) if value) or "not configured"
            check = QCheckBox(f"{label} — {detail}")
            check.setChecked(bool(configured and enabled))
            check.setEnabled(configured)
            if not configured:
                check.setToolTip("Add the namespace/friendly name to .env and the server address, username, and password to .secrets.")
            self._cluster_checks[prefix] = check
            name_edit = QLineEdit(_friendly_env_value(prefix))
            name_edit.setPlaceholderText(f"Friendly name, e.g. {label}")
            name_edit.setMaximumWidth(280)
            self._cluster_name_edits[prefix] = name_edit
            configure_cluster = QPushButton("Configure")
            configure_cluster.setObjectName("Ghost")
            configure_cluster.setToolTip("Edit the non-secret environment, friendly name, namespace, port, and database fields.")
            configure_cluster.clicked.connect(
                lambda _checked=False, p=prefix, cluster_label=label: self._edit_cluster_profile(p, cluster_label)
            )
            cluster_row = QHBoxLayout()
            cluster_row.addWidget(check, 1)
            cluster_row.addWidget(QLabel("Name"))
            cluster_row.addWidget(name_edit)
            cluster_row.addWidget(configure_cluster)
            clusters_lay.addLayout(cluster_row)
        cluster_actions = QHBoxLayout()
        save_clusters = QPushButton("Save Cluster Selection")
        save_clusters.setObjectName("Primary")
        cluster_actions.addWidget(save_clusters)
        export_clusters = QPushButton("Export Portable Configuration")
        export_clusters.setObjectName("Ghost")
        export_clusters.setToolTip(
            "Write the non-secret cluster names, namespace IDs, endpoints, ports, databases, and selections beside the application."
        )
        cluster_actions.addWidget(export_clusters)
        cluster_actions.addStretch(1)
        clusters_lay.addLayout(cluster_actions)
        self._cluster_selection_status = QLabel("")
        self._cluster_selection_status.setObjectName("Caption")
        self._cluster_selection_status.setWordWrap(True)
        clusters_lay.addWidget(self._cluster_selection_status)
        data_sources_lay.addWidget(clusters)

        analysis_scope = QFrame()
        analysis_scope.setObjectName("CardSubtle")
        analysis_scope_lay = QVBoxLayout(analysis_scope)
        analysis_scope_lay.setContentsMargins(12, 10, 12, 10)
        analysis_scope_lay.setSpacing(7)
        analysis_scope_head = QLabel("ANALYSIS CLUSTER SCOPE")
        analysis_scope_head.setObjectName("SectionHeader")
        analysis_scope_lay.addWidget(analysis_scope_head)
        analysis_scope_hint = QLabel(
            "Choose which already-loaded namespaces appear in analysis. This does not reload or delete data. "
            "Select one consumer for focused analysis, any combination, or all clusters for an aggregate view."
        )
        analysis_scope_hint.setObjectName("Caption")
        analysis_scope_hint.setWordWrap(True)
        analysis_scope_lay.addWidget(analysis_scope_hint)
        self._analysis_all_clusters = QCheckBox("All loaded clusters (aggregate view)")
        analyze_all = not bool(self._settings.analysis_namespace_filter)
        self._analysis_all_clusters.setChecked(analyze_all)
        analysis_scope_lay.addWidget(self._analysis_all_clusters)
        selected_namespaces = {value.lower() for value in self._settings.analysis_namespace_filter}
        self._analysis_cluster_checks: dict[str, QCheckBox] = {}
        for namespace_id, label in self._available_analysis_namespaces():
            check = QCheckBox(label)
            check.setChecked(analyze_all or namespace_id.lower() in selected_namespaces)
            check.setEnabled(not analyze_all)
            self._analysis_cluster_checks[namespace_id] = check
            analysis_scope_lay.addWidget(check)
        analysis_scope_actions = QHBoxLayout()
        save_analysis_scope = QPushButton("Save Analysis Scope")
        save_analysis_scope.setObjectName("Primary")
        analysis_scope_actions.addWidget(save_analysis_scope)
        analysis_scope_actions.addStretch(1)
        analysis_scope_lay.addLayout(analysis_scope_actions)
        self._analysis_scope_status = QLabel("")
        self._analysis_scope_status.setObjectName("Caption")
        self._analysis_scope_status.setWordWrap(True)
        analysis_scope_lay.addWidget(self._analysis_scope_status)
        data_sources_lay.addWidget(analysis_scope)

        dbs = QFrame()
        dbs.setObjectName("CardSubtle")
        dbs_lay = QVBoxLayout(dbs)
        dbs_lay.setContentsMargins(12, 10, 12, 10)
        dbs_lay.setSpacing(8)
        db_head = QLabel("DATABASE DISCOVERY")
        db_head.setObjectName("SectionHeader")
        dbs_lay.addWidget(db_head)
        self._database_threshold = QLineEdit(str(self._settings.database_min_query_count))
        discovery_rule = QLabel(
            "Safety rule: cycle only databases classified by Redshift as database_type = 'local'. "
            "Inbound datashares, shared databases, and Data Catalog databases are excluded."
        )
        discovery_rule.setObjectName("Caption")
        discovery_rule.setWordWrap(True)
        dbs_lay.addWidget(discovery_rule)
        self._database_sql = QPlainTextEdit()
        self._database_sql.setPlainText(self._settings.database_discovery_sql)
        self._database_sql.setMinimumHeight(118)
        self._database_sql.setObjectName("Mono")
        self._database_sql.setReadOnly(True)
        dbs_lay.addWidget(self._database_sql)
        self._database_status = QLabel(self._database_summary())
        self._database_status.setWordWrap(True)
        self._database_status.setObjectName("Caption")
        dbs_lay.addWidget(self._database_status)
        self._database_overview = QTableView()
        _configure_table_view(self._database_overview)
        self._database_overview.setMinimumHeight(160)
        dbs_lay.addWidget(self._database_overview, 1)
        db_actions = QHBoxLayout()
        save_database_query = QPushButton("Save Query")
        save_database_query.setObjectName("Ghost")
        save_database_query.setEnabled(False)
        save_database_query.setToolTip("Database classification SQL is safety-locked to physical local databases.")
        format_database_query = QPushButton("Format SQL")
        format_database_query.setObjectName("Ghost")
        format_database_query.setEnabled(False)
        format_database_query.clicked.connect(lambda: _apply_format_sql(self._database_sql, self))
        reload_databases = QPushButton("Reload Databases")
        reload_databases.setObjectName("Primary")
        db_actions.addWidget(save_database_query)
        db_actions.addWidget(format_database_query)
        _add_sql_structure_buttons(
            db_actions,
            self._database_sql,
            self,
            pd.Series({"query_id": "database discovery SQL"}),
            pd.DataFrame(),
            pd.DataFrame(),
        )
        db_actions.addWidget(reload_databases)
        db_actions.addStretch(1)
        dbs_lay.addLayout(db_actions)
        database_lay.addWidget(dbs)

        capture = QFrame()
        capture.setObjectName("CardSubtle")
        capture_lay = QVBoxLayout(capture)
        capture_lay.setContentsMargins(12, 10, 12, 10)
        capture_lay.setSpacing(8)
        capture_head = QLabel("CAPTURE SELECTION")
        capture_head.setObjectName("SectionHeader")
        capture_lay.addWidget(capture_head)
        capture_grid = QGridLayout()
        capture_grid.addWidget(QLabel("Parent evidence cap"), 0, 0)
        self._capture_top_n = QLineEdit(str(self._settings.capture_query_limit or 0))
        self._capture_top_n.setMaximumWidth(120)
        self._capture_top_n.setToolTip(
            "Optional cap on representative parent-pattern query IDs used for detail/plan evidence. "
            "Use 0 to include every threshold-qualified parent pattern."
        )
        capture_grid.addWidget(self._capture_top_n, 0, 1)
        capture_hint = QLabel("0 = all threshold-selected patterns")
        capture_hint.setObjectName("Caption")
        capture_grid.addWidget(capture_hint, 0, 2)
        capture_grid.setColumnStretch(4, 1)
        capture_lay.addLayout(capture_grid)
        self._capture_tables = QListWidget()
        self._capture_tables.setMinimumHeight(150)
        selected_tables = set(self._settings.capture_include_tables or SOURCE_SQL_TABLES)
        for table_name in SOURCE_SQL_TABLES:
            item = QListWidgetItem(table_name)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked if table_name in selected_tables else Qt.Unchecked)
            self._capture_tables.addItem(item)
        capture_lay.addWidget(self._capture_tables)
        self._capture_status = QLabel(
            "Checked blocks are included in full reload and Refresh Empty Tables. "
            "Query roots are selected by the minimum-seconds threshold; the evidence cap defaults to all parent patterns."
        )
        self._capture_status.setObjectName("Caption")
        self._capture_status.setWordWrap(True)
        capture_lay.addWidget(self._capture_status)
        capture_actions = QHBoxLayout()
        save_capture = QPushButton("Save Capture Defaults")
        save_capture.setObjectName("Ghost")
        capture_all = QPushButton("All")
        capture_all.setObjectName("Ghost")
        capture_none = QPushButton("None")
        capture_none.setObjectName("Ghost")
        capture_actions.addWidget(save_capture)
        capture_actions.addWidget(capture_all)
        capture_actions.addWidget(capture_none)
        capture_actions.addStretch(1)
        capture_lay.addLayout(capture_actions)
        data_sources_lay.addWidget(capture)

        source_sql = QFrame()
        source_sql.setObjectName("CardSubtle")
        source_sql_lay = QVBoxLayout(source_sql)
        source_sql_lay.setContentsMargins(12, 10, 12, 10)
        source_sql_lay.setSpacing(8)
        source_sql_head = QLabel("BASE SOURCE SQL")
        source_sql_head.setObjectName("SectionHeader")
        source_sql_lay.addWidget(source_sql_head)
        source_sql_top = QHBoxLayout()
        source_sql_top.addWidget(QLabel("Target table"))
        self._source_sql_table = QComboBox()
        self._source_sql_table.addItems(SOURCE_SQL_TABLES)
        source_sql_top.addWidget(self._source_sql_table)
        source_sql_top.addStretch(1)
        source_sql_lay.addLayout(source_sql_top)
        self._source_sql = QPlainTextEdit()
        self._source_sql.setMinimumHeight(118)
        self._source_sql.setObjectName("Mono")
        source_sql_lay.addWidget(self._source_sql)
        self._source_sql_status = QLabel("")
        self._source_sql_status.setWordWrap(True)
        self._source_sql_status.setObjectName("Caption")
        source_sql_lay.addWidget(self._source_sql_status)
        source_sql_actions = QHBoxLayout()
        save_source_sql = QPushButton("Save SQL")
        save_source_sql.setObjectName("Ghost")
        format_source_sql = QPushButton("Format SQL")
        format_source_sql.setObjectName("Ghost")
        format_source_sql.clicked.connect(lambda: _apply_format_sql(self._source_sql, self))
        reset_source_sql = QPushButton("Reset SQL")
        reset_source_sql.setObjectName("Ghost")
        refresh_source_sql = QPushButton("Refresh This Table")
        refresh_source_sql.setObjectName("Primary")
        source_sql_actions.addWidget(save_source_sql)
        source_sql_actions.addWidget(format_source_sql)
        _add_sql_structure_buttons(
            source_sql_actions,
            self._source_sql,
            self,
            pd.Series({"query_id": "source capture SQL"}),
            pd.DataFrame(),
            pd.DataFrame(),
        )
        source_sql_actions.addWidget(reset_source_sql)
        source_sql_actions.addWidget(refresh_source_sql)
        source_sql_actions.addStretch(1)
        source_sql_lay.addLayout(source_sql_actions)
        source_statements_lay.addWidget(source_sql)

        manual = QFrame()
        manual.setObjectName("CardSubtle")
        manual_lay = QVBoxLayout(manual)
        manual_lay.setContentsMargins(12, 10, 12, 10)
        manual_lay.setSpacing(8)
        manual_head = QLabel("MANUAL FILE LOAD")
        manual_head.setObjectName("SectionHeader")
        manual_lay.addWidget(manual_head)
        manual_grid = QGridLayout()
        manual_grid.addWidget(QLabel("Target table"), 0, 0)
        self._manual_table = QComboBox()
        self._manual_table.addItems(sorted(EXPECTED_COLUMNS))
        manual_grid.addWidget(self._manual_table, 0, 1)
        manual_grid.addWidget(QLabel("Source database"), 0, 2)
        self._manual_source_db = QLineEdit()
        self._manual_source_db.setPlaceholderText("Optional for per-database files")
        manual_grid.addWidget(self._manual_source_db, 0, 3)
        manual_grid.addWidget(QLabel("File"), 1, 0)
        self._manual_path = QLineEdit()
        self._manual_path.setPlaceholderText("CSV, TSV, or pipe-delimited export")
        manual_grid.addWidget(self._manual_path, 1, 1, 1, 2)
        browse_import = QPushButton("Browse")
        browse_import.setObjectName("Ghost")
        manual_grid.addWidget(browse_import, 1, 3)
        self._manual_append = QCheckBox("Append to target table")
        self._manual_append.setChecked(True)
        manual_grid.addWidget(self._manual_append, 2, 1, 1, 3)
        manual_lay.addLayout(manual_grid)
        self._manual_status = QLabel("Load one file at a time to measure parse + DuckDB insert time.")
        self._manual_status.setObjectName("Caption")
        self._manual_status.setWordWrap(True)
        manual_lay.addWidget(self._manual_status)
        manual_actions = QHBoxLayout()
        import_file = QPushButton("Load File")
        import_file.setObjectName("Primary")
        manual_actions.addWidget(import_file)
        manual_actions.addStretch(1)
        manual_lay.addLayout(manual_actions)
        data_sources_lay.addWidget(manual)

        # Query-history trimmer: widgets must exist before Preview/Trim slots run.
        trim_card = QFrame()
        trim_card.setObjectName("CardSubtle")
        trim_lay = QVBoxLayout(trim_card)
        trim_lay.setContentsMargins(10, 8, 10, 8)
        trim_title = QLabel("LOCAL QUERY HISTORY TRIM")
        trim_title.setObjectName("SectionHeader")
        trim_lay.addWidget(trim_title)
        trim_hint = QLabel(
            "Optional warehouse hygiene: keep only recent/long-running query evidence "
            "in the local DuckDB file. Always creates a backup first."
        )
        trim_hint.setObjectName("Caption")
        trim_hint.setWordWrap(True)
        trim_lay.addWidget(trim_hint)
        trim_row = QHBoxLayout()
        trim_row.addWidget(QLabel("Min runtime (minutes)"))
        self._trim_minutes = QLineEdit("10")
        self._trim_minutes.setMaximumWidth(80)
        self._trim_minutes.setToolTip("Keep queries with elapsed time at least this many minutes.")
        trim_row.addWidget(self._trim_minutes)
        trim_row.addWidget(QLabel("Keep top N"))
        self._trim_top_n = QLineEdit("5000")
        self._trim_top_n.setMaximumWidth(100)
        self._trim_top_n.setToolTip("Additionally keep the top N longest queries. 0 = no cap.")
        trim_row.addWidget(self._trim_top_n)
        trim_row.addStretch(1)
        trim_lay.addLayout(trim_row)
        trim_actions = QHBoxLayout()
        preview_trim = QPushButton("Preview Trim")
        preview_trim.setObjectName("Ghost")
        apply_trim = QPushButton("Trim Query Evidence")
        apply_trim.setObjectName("Ghost")
        restore_backup = QPushButton("Restore Backup…")
        restore_backup.setObjectName("Ghost")
        trim_actions.addWidget(preview_trim)
        trim_actions.addWidget(apply_trim)
        trim_actions.addWidget(restore_backup)
        trim_actions.addStretch(1)
        trim_lay.addLayout(trim_actions)
        self._trim_status = QLabel("")
        self._trim_status.setObjectName("Caption")
        self._trim_status.setWordWrap(True)
        trim_lay.addWidget(self._trim_status)
        data_sources_lay.addWidget(trim_card)
        preview_trim.clicked.connect(self._preview_query_trim)
        apply_trim.clicked.connect(self._trim_query_tables)
        restore_backup.clicked.connect(self._restore_duckdb_backup)

        close = QPushButton("Close")
        close.setObjectName("Primary")
        data_sources_lay.addStretch(1)
        database_lay.addStretch(1)
        source_statements_lay.addStretch(1)

        footer = QHBoxLayout()
        footer.addStretch(1)
        footer.addWidget(close)
        root.addLayout(footer)

        save_database_query.clicked.connect(self._save_database_settings)
        reload_databases.clicked.connect(self._reload_databases)
        save_capture.clicked.connect(self._save_capture_settings)
        save_clusters.clicked.connect(self._save_cluster_selection)
        export_clusters.clicked.connect(self._export_portable_cluster_profiles)
        edit_secure_env.clicked.connect(self._edit_local_configuration)
        self._analysis_all_clusters.toggled.connect(self._toggle_analysis_cluster_checks)
        save_analysis_scope.clicked.connect(self._save_analysis_scope)
        capture_all.clicked.connect(lambda: self._set_capture_table_checks(True))
        capture_none.clicked.connect(lambda: self._set_capture_table_checks(False))
        self._source_sql_table.currentTextChanged.connect(self._on_source_sql_table_changed)
        save_source_sql.clicked.connect(lambda: self._save_source_sql(offer_refresh=True))
        reset_source_sql.clicked.connect(self._reset_source_sql)
        refresh_source_sql.clicked.connect(lambda: self._refresh_source_table(self._source_sql_table.currentText().strip()))
        browse_import.clicked.connect(self._browse_import_file)
        import_file.clicked.connect(self._import_file)
        close.clicked.connect(self.accept)
        self._load_source_sql()
        self._load_counts()
        QTimer.singleShot(0, self._check_source_cluster_configuration)

    def _selected_table_name(self) -> str:
        tables = self._selected_table_names()
        return tables[0] if tables else ""

    def _selected_table_names(self) -> list[str]:
        if not self._model:
            return []
        selection = self._table.selectionModel()
        selected = selection.selectedRows() if selection is not None else []
        if not selected:
            current = self._table.currentIndex()
            selected = [current] if current.isValid() else []
        table_names: list[str] = []
        for index in sorted(selected, key=lambda item: item.row()):
            if not index.isValid():
                continue
            row = self._model.row_at(index.row())
            table_name = str(row.get("table_name") or "").strip()
            if table_name and table_name not in table_names:
                table_names.append(table_name)
        if table_names:
            self._last_selected_table = table_names[0]
        return table_names

    def _selected_capture_tables(self) -> list[str]:
        tables: list[str] = []
        for row in range(self._capture_tables.count()):
            item = self._capture_tables.item(row)
            if item and item.checkState() == Qt.Checked:
                tables.append(item.text())
        return tables

    def _set_capture_table_checks(self, checked: bool) -> None:
        state = Qt.Checked if checked else Qt.Unchecked
        for row in range(self._capture_tables.count()):
            item = self._capture_tables.item(row)
            if item:
                item.setCheckState(state)

    def _save_capture_settings(self) -> bool:
        try:
            parent_cap = int(self._capture_top_n.text().strip())
            if parent_cap < 0:
                raise ValueError("parent_cap must be non-negative")
        except ValueError:
            QMessageBox.warning(
                self,
                "Capture Selection",
                "Parent evidence cap must be a non-negative integer. Use 0 for all qualifying patterns.",
            )
            return False
        selected = self._selected_capture_tables()
        if not selected:
            QMessageBox.warning(self, "Capture Selection", "Select at least one capture block.")
            return False
        self._settings.capture_query_limit = parent_cap
        self._settings.capture_include_tables = selected
        save_settings(self._settings)
        cap_text = (
            "all threshold-selected parent patterns"
            if parent_cap <= 0
            else f"at most {parent_cap:,} parent patterns"
        )
        self._capture_status.setText(
            f"Saved: {cap_text}; "
            f"{len(selected):,} capture block(s) selected."
        )
        return True

    def _capture_ingest_args(self) -> list[str]:
        if not self._save_capture_settings():
            raise ValueError("Capture selection is not valid.")
        return [
            "--evidence-parent-limit",
            str(self._settings.capture_query_limit),
            "--include-tables",
            ",".join(self._settings.capture_include_tables),
        ]

    def _capture_threshold_args(self) -> list[str]:
        if not self._save_capture_settings():
            raise ValueError("Capture selection is not valid.")
        return [
            "--evidence-parent-limit",
            str(self._settings.capture_query_limit),
        ]

    def _select_table_by_name(self, table_name: str) -> None:
        if not self._model:
            return
        row = self._model.row_for_value("table_name", table_name)
        if row is None:
            return
        index = self._model.index(row, 0)
        if not index.isValid():
            return
        self._table.setCurrentIndex(index)
        self._table.selectRow(row)
        self._table.scrollTo(index)
        self._last_selected_table = table_name

    def _on_table_current_row_changed(self, current: QModelIndex, _previous: QModelIndex) -> None:
        if not current.isValid() or not self._model:
            return
        row = self._model.row_at(current.row())
        table_name = str(row.get("table_name") or "").strip()
        if not table_name:
            return
        self._last_selected_table = table_name
        combo_index = self._source_sql_table.findText(table_name)
        if combo_index >= 0 and combo_index != self._source_sql_table.currentIndex():
            self._source_sql_table.blockSignals(True)
            self._source_sql_table.setCurrentIndex(combo_index)
            self._source_sql_table.blockSignals(False)
            self._load_source_sql()

    def _fit_to_screen(self) -> None:
        screen = None
        parent = self.parent()
        if isinstance(parent, QWidget):
            screen = parent.screen()
        screen = screen or self.screen()
        if screen is None:
            self.resize(920, 680)
            return
        available = screen.availableGeometry()
        width = min(980, max(720, int(available.width() * 0.86)))
        height = min(720, max(520, int(available.height() * 0.84)))
        self.resize(width, height)
        self.setMaximumSize(max(760, int(available.width() * 0.96)), max(560, int(available.height() * 0.92)))

    def _connection_summary(self) -> str:
        from ..secrets_store import session_secret

        native_host = session_secret("REDSHIFT_HOST", "") or ""
        native_user = session_secret("REDSHIFT_USER", "") or ""
        jdbc_url = os.environ.get("REDSHIFT_JDBC_URL", "")
        jdbc_jar = os.environ.get("REDSHIFT_JDBC_JAR", "")
        password_set = "yes" if session_secret("REDSHIFT_PASSWORD") else "no"
        lines = [
            f"Native: {'configured' if native_host and native_user else 'not configured'}",
            f"  host={native_host or '-'}",
            f"  user={native_user or '-'}",
            f"  encrypted password available={password_set}",
            f"JDBC: {'configured' if jdbc_url and jdbc_jar and native_user else 'not configured'}",
            f"  url={jdbc_url or '-'}",
            f"  jar={jdbc_jar or '-'}",
            "",
            "Use Data Loader → Refresh Empty Tables to repopulate empty datasets.",
        ]
        return "\n".join(lines)

    @staticmethod
    def _cluster_profiles_from_env() -> list[tuple[str, str, bool, str, str, bool]]:
        from ..secrets_store import session_secret

        profiles: list[tuple[str, str, bool, str, str, bool]] = []
        configured_ordinals = {
            int(match.group(1))
            for key in os.environ
            if (match := re.match(r"^REDSHIFT_CONSUMER_(\d+)_", str(key).upper()))
        }
        configured_ordinals.update(range(1, 8))
        for ordinal in (0, *sorted(configured_ordinals)):
            if ordinal == 0:
                prefix = "REDSHIFT_PRODUCER"
                label = "Producer"
                host = session_secret(f"{prefix}_HOST") or session_secret("REDSHIFT_HOST", "") or ""
                namespace_id = (
                    os.environ.get("REDSHIFT_NAMESPACE")
                    or os.environ.get(f"{prefix}_NAMESPACE_ID")
                    or os.environ.get("REDSHIFT_NAMESPACE_ID", "")
                )
            else:
                prefix = f"REDSHIFT_CONSUMER_{ordinal}"
                label = f"Consumer {ordinal}"
                host = session_secret(f"{prefix}_HOST", "") or ""
                namespace_id = os.environ.get(f"{prefix}_NAMESPACE_ID", "")
            friendly_name = str(
                os.environ.get(f"{prefix}_DISPLAY_NAME", "")
                or (
                    os.environ.get("REDSHIFT_FRIENDLY")
                    if ordinal == 0
                    else os.environ.get(f"{prefix}_FRIENDLY")
                )
                or (os.environ.get("REDSHIFT_ENV") if ordinal == 0 else "")
            ).strip()
            label = friendly_name or label
            configured = bool(str(host).strip())
            enabled_raw = (
                os.environ.get("REDSHIFT_ENABLED") if ordinal == 0 else None
            ) or os.environ.get(f"{prefix}_ENABLED")
            enabled = _boolean_env_value(enabled_raw, default=configured)
            profiles.append((prefix, label, configured, str(namespace_id).strip(), str(host).strip(), enabled))
        return profiles

    def _cluster_enabled_env_updates(self) -> dict[str, str]:
        """Materialize checkbox state into ENABLED env keys for .env + portable JSON."""
        updates: dict[str, str] = {}
        for prefix, check in self._cluster_checks.items():
            if not check.isEnabled():
                continue
            flag = "true" if check.isChecked() else "false"
            # Canonical producer flag used by loaders/topology.
            if prefix == "REDSHIFT_PRODUCER":
                updates["REDSHIFT_ENABLED"] = flag
                updates["REDSHIFT_PRODUCER_ENABLED"] = flag
            else:
                updates[f"{prefix}_ENABLED"] = flag
        return updates

    def _save_cluster_selection(self) -> None:
        configured = [prefix for prefix, check in self._cluster_checks.items() if check.isEnabled()]
        selected = [prefix for prefix in configured if self._cluster_checks[prefix].isChecked()]
        if not selected:
            QMessageBox.warning(self, "Cluster Selection", "Check at least one configured Redshift cluster.")
            return
        try:
            updates = self._cluster_enabled_env_updates()
            updates.update(
                {
                    _friendly_env_key(prefix): self._cluster_name_edits[prefix].text().strip()
                    for prefix in configured
                }
            )
            env_path = self._persist_environment_updates(updates)
        except Exception as exc:
            QMessageBox.warning(self, "Cluster Selection", f"Could not save cluster selection: {exc}")
            return
        labels = [
            self._cluster_name_edits[prefix].text().strip()
            or prefix.replace("REDSHIFT_", "").replace("_", " ").title()
            for prefix in selected
        ]
        self._cluster_selection_status.setText(
            f"Saved to {env_path}. Next load will include: {', '.join(labels)}."
        )

    def _build_grouping_tab(self, layout) -> None:
        """Whether the running user is part of a query pattern's identity."""
        card = QFrame()
        card.setObjectName("CardSubtle")
        body = QVBoxLayout(card)
        body.setContentsMargins(12, 10, 12, 10)
        body.setSpacing(8)

        title = QLabel("PATTERN IDENTITY")
        title.setObjectName("SectionHeader")
        body.addWidget(title)

        settings = load_settings()
        self._scope_by_user_box = QCheckBox(
            "Group queries separately per user (include the user in the pattern identity)"
        )
        self._scope_by_user_box.setChecked(
            bool(getattr(settings, "repeat_scope_by_user", False))
        )
        body.addWidget(self._scope_by_user_box)

        explain = QLabel(
            "ON (default, and advised at least at first): the same SQL run by two "
            "users stays two patterns. Attribution is exact, every pattern names a "
            "real owner, and nothing is merged that you did not ask to merge.\n\n"
            "OFF: the same SQL shape is ONE pattern no matter who ran it, and its "
            "user column reads \"Multiple Users\". This merges harder - a report SQL "
            "copied into four teams' dashboards becomes one problem instead of four - "
            "but you lose per-user attribution. Turn it off once you trust the "
            "grouping and want the consolidated view."
        )
        explain.setObjectName("Caption")
        explain.setWordWrap(True)
        body.addWidget(explain)

        warning = QLabel(
            "Changing this invalidates the cached grouping and forces a full "
            "regroup on the next analysis load. On a large capture that can take "
            "an hour or more."
        )
        warning.setObjectName("Caption")
        warning.setWordWrap(True)
        warning.setStyleSheet(f"color:{PALETTE.warn}; font-weight:700;")
        body.addWidget(warning)

        self._scope_by_user_box.toggled.connect(self._save_scope_by_user)
        layout.addWidget(card)
        layout.addStretch(1)

    def _save_scope_by_user(self, checked: bool) -> None:
        settings = load_settings()
        if bool(getattr(settings, "repeat_scope_by_user", False)) == bool(checked):
            return
        confirm = QMessageBox.question(
            self,
            "Regroup Required",
            (
                "Grouping queries per user\n\n"
                if checked
                else "Grouping queries across all users\n\n"
            )
            + "This changes what counts as one query pattern, so the cached "
            "grouping is discarded and the next analysis load regroups every "
            "captured query from scratch.\n\n"
            "On a large capture that can take an hour or more. Continue?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if confirm != QMessageBox.Yes:
            self._scope_by_user_box.blockSignals(True)
            self._scope_by_user_box.setChecked(not checked)
            self._scope_by_user_box.blockSignals(False)
            return
        settings.repeat_scope_by_user = bool(checked)
        save_settings(settings)

    def _persist_environment_updates(self, updates: dict[str, str]) -> Path:
        """Persist named values without reading or changing unspecified keys."""
        from ..ingest_redshift import dotenv_candidates

        candidates = dotenv_candidates(None)
        target = next((path for path in candidates if path.is_file()), candidates[0])
        _update_dotenv_keys(target, updates)
        for key, value in updates.items():
            os.environ[key] = value
        self._write_portable_cluster_profiles()
        return target

    @staticmethod
    def _portable_cluster_profile_path() -> Path:
        from ..portable_config import PORTABLE_FILENAME

        launch_dir = os.environ.get("REDSHIFT_ANALYZER_LAUNCH_DIR")
        return Path(launch_dir).resolve() / PORTABLE_FILENAME if launch_dir else Path.cwd() / PORTABLE_FILENAME

    def _write_portable_cluster_profiles(self) -> Path:
        from ..portable_config import export_portable_config

        # Snapshot GUI checkbox state into the process env first so export does
        # not write blank "enabled" when the user has checked clusters but the
        # ENABLED keys were never written to .env.
        if getattr(self, "_cluster_checks", None):
            for key, value in self._cluster_enabled_env_updates().items():
                os.environ[key] = value
        return export_portable_config(self._portable_cluster_profile_path())

    def _export_portable_cluster_profiles(self) -> None:
        try:
            # Persist enabled flags so Export alone does not leave .env out of
            # sync with the checkboxes the operator just set.
            if getattr(self, "_cluster_checks", None):
                updates = self._cluster_enabled_env_updates()
                if updates:
                    self._persist_environment_updates(updates)
            target = self._write_portable_cluster_profiles()
        except Exception as exc:
            QMessageBox.warning(self, "Portable Cluster Configuration", f"Could not export cluster profiles: {exc}")
            return
        self._cluster_selection_status.setText(
            f"Portable configuration exported to {target}. It contains no Redshift usernames or passwords and is safe to copy with the application."
        )

    def _edit_cluster_profile(self, prefix: str, label: str) -> None:
        legacy = prefix == "REDSHIFT_PRODUCER"

        def current(name: str, legacy_name: str = "", default: str = "") -> str:
            value = os.environ.get(f"{prefix}_{name}")
            if value is None and legacy and legacy_name:
                value = os.environ.get(legacy_name)
            return str(value if value is not None else default).strip()

        dialog = QDialog(self)
        dialog.setWindowTitle(f"Configure {label}")
        dialog.setModal(True)
        layout = QVBoxLayout(dialog)
        note = QLabel(
            "Configure cluster identity and routing only. The Redshift username and password are intentionally not displayed and will not be modified."
        )
        note.setWordWrap(True)
        layout.addWidget(note)
        form = QFormLayout()
        display_name = QLineEdit(_friendly_env_value(prefix, label))
        namespace_id = QLineEdit(
            (os.environ.get("REDSHIFT_NAMESPACE") if legacy else "")
            or current("NAMESPACE_ID", "REDSHIFT_NAMESPACE_ID")
        )
        namespace_id.setPlaceholderText("Required unique Redshift namespace ID")
        port = QLineEdit(current("PORT", "REDSHIFT_PORT", "5439"))
        primary_database = QLineEdit(
            current("PRIMARY_DATABASE", "REDSHIFT_PRIMARY_DATABASE")
            or current("DATABASE", "REDSHIFT_DATABASE", "dev")
        )
        environment_name = QLineEdit(str(os.environ.get("REDSHIFT_ENV") or "")) if legacy else None
        if environment_name is not None:
            environment_name.setPlaceholderText("PROD, QA, or DEV")
            form.addRow("Environment", environment_name)
        form.addRow("Friendly name", display_name)
        form.addRow("Namespace ID", namespace_id)
        form.addRow("Port", port)
        form.addRow("Primary database", primary_database)
        discovered_note = QLabel(
            "Physical local databases are discovered from SVV_REDSHIFT_DATABASES each time this cluster is cycle-loaded; shared and Data Catalog databases are excluded."
        )
        discovered_note.setObjectName("Caption")
        discovered_note.setWordWrap(True)
        form.addRow("Database scope", discovered_note)
        layout.addLayout(form)
        credential_note = QLabel("Server address, username, and password: encrypted in .secrets and unchanged here")
        credential_note.setObjectName("Caption")
        layout.addWidget(credential_note)
        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        layout.addWidget(buttons)
        buttons.rejected.connect(dialog.reject)

        def save_profile() -> None:
            from ..secrets_store import session_secret

            namespace = namespace_id.text().strip()
            endpoint = str(
                (session_secret("REDSHIFT_HOST") if legacy else session_secret(f"{prefix}_HOST"))
                or ""
            ).strip()
            try:
                port_number = int(port.text().strip() or "5439")
            except ValueError:
                QMessageBox.warning(dialog, "Configure Cluster", "Port must be an integer.")
                return
            if port_number < 1 or port_number > 65535:
                QMessageBox.warning(dialog, "Configure Cluster", "Port must be between 1 and 65535.")
                return
            if endpoint and not namespace:
                QMessageBox.warning(dialog, "Configure Cluster", "Namespace ID is required when an endpoint is configured.")
                return
            updates = {
                _friendly_env_key(prefix): display_name.text().strip() or label,
                ("REDSHIFT_NAMESPACE" if legacy else f"{prefix}_NAMESPACE_ID"): namespace,
                f"{prefix}_PORT": str(port_number),
                f"{prefix}_PRIMARY_DATABASE": primary_database.text().strip() or "dev",
            }
            if environment_name is not None:
                updates["REDSHIFT_ENV"] = environment_name.text().strip()
            try:
                target = self._persist_environment_updates(updates)
            except Exception as exc:
                QMessageBox.warning(dialog, "Configure Cluster", f"Could not save cluster configuration: {exc}")
                return
            check = self._cluster_checks.get(prefix)
            if check is not None:
                configured = bool(endpoint)
                check.setEnabled(configured)
                visible_name = updates[_friendly_env_key(prefix)]
                check.setText(f"{visible_name} — {' • '.join(value for value in (namespace, endpoint) if value) or 'not configured'}")
                check.setToolTip(f"Configuration slot: {prefix}")
            name_editor = self._cluster_name_edits.get(prefix)
            if name_editor is not None:
                name_editor.setText(updates[_friendly_env_key(prefix)])
            self._cluster_selection_status.setText(
                f"Saved non-secret {label} configuration to {target}. Username and password were unchanged."
            )
            dialog.accept()

        buttons.accepted.connect(save_profile)
        dialog.exec()

    @staticmethod
    def _environment_configuration_path() -> Path:
        from ..ingest_redshift import dotenv_candidates

        candidates = dotenv_candidates(None)
        return next((path for path in candidates if path.is_file()), candidates[0])

    def _environment_status(self) -> str:
        try:
            env_path = self._environment_configuration_path()
            from ..secrets_store import active_secrets_path

            secrets_path = active_secrets_path()
        except Exception as exc:
            return f"Configuration status unavailable: {exc}"
        env_state = f"Demonstration-safe .env: {env_path}" if env_path.is_file() else f".env will be created at {env_path}"
        secret_state = (
            f"Encrypted .secrets: {secrets_path}"
            if secrets_path.is_file()
            else f"Encrypted .secrets will be created at {secrets_path}"
        )
        return f"{env_state}\n{secret_state}"

    def _edit_local_configuration(self) -> None:
        try:
            from ..auth import verify_credentials
            from ..secrets_store import (
                active_secrets_path,
                is_secret_key,
                parse_editor_text,
                plaintext_editor_text,
                read_encrypted_secrets,
                unlock_secrets_session,
                write_encrypted_secrets,
            )

            path = active_secrets_path()
        except Exception as exc:
            QMessageBox.warning(self, "Encrypted Credentials", f"Could not initialize encrypted credentials: {exc}")
            return

        auth_dialog = QDialog(self)
        auth_dialog.setWindowTitle("Unlock Encrypted Credentials")
        auth_layout = QVBoxLayout(auth_dialog)
        auth_note = QLabel(
            "Re-enter the Infraredshift access code and PIN. Successful authentication opens the credential editor for three minutes."
        )
        auth_note.setWordWrap(True)
        auth_layout.addWidget(auth_note)
        auth_form = QFormLayout()
        access_code = QLineEdit()
        access_code.setEchoMode(QLineEdit.Password)
        pin = QLineEdit()
        pin.setEchoMode(QLineEdit.Password)
        auth_form.addRow("Access code", access_code)
        auth_form.addRow("PIN", pin)
        auth_layout.addLayout(auth_form)
        auth_status = QLabel("")
        auth_status.setStyleSheet(f"color:{PALETTE.crit}; font-weight:700;")
        auth_status.setWordWrap(True)
        auth_layout.addWidget(auth_status)
        auth_buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        auth_layout.addWidget(auth_buttons)
        auth_buttons.rejected.connect(auth_dialog.reject)

        def authenticate() -> None:
            if verify_credentials(access_code.text().strip(), pin.text().strip()):
                auth_dialog.accept()
            else:
                auth_status.setText("The access code and PIN do not match.")

        auth_buttons.accepted.connect(authenticate)
        pin.returnPressed.connect(authenticate)
        access_code.setFocus()
        if auth_dialog.exec() != QDialog.Accepted:
            access_code.clear()
            pin.clear()
            return

        access_value = access_code.text().strip()
        pin_value = pin.text().strip()
        access_code.clear()
        pin.clear()
        try:
            if path.is_file():
                values = read_encrypted_secrets(path, access_value, pin_value)
            else:
                from ..secrets_store import session_secrets

                values = {key: value for key, value in session_secrets().items() if is_secret_key(key)}
                if not values:
                    values = {"REDSHIFT_HOST": "", "REDSHIFT_USER": "", "REDSHIFT_PASSWORD": ""}
                    for prefix, _label, _configured, namespace_id, _host, _enabled in self._cluster_profiles_from_env():
                        if prefix != "REDSHIFT_PRODUCER" and namespace_id:
                            values[f"{prefix}_HOST"] = ""
                            values[f"{prefix}_USER"] = ""
                            values[f"{prefix}_PASSWORD"] = ""
        except Exception as exc:
            QMessageBox.warning(self, "Encrypted Credentials", f"Could not unlock {path}: {exc}")
            return

        QMessageBox.information(
            self,
            "Credentials Unlocked for Three Minutes",
            "The editor will remain unlocked for three minutes. The .secrets file on disk remains encrypted and visibly scrambled at all times.",
        )

        dialog = QDialog(self)
        dialog.setWindowTitle("Encrypted Redshift Credentials — Three-Minute Access")
        dialog.setModal(True)
        layout = QVBoxLayout(dialog)
        warning = QLabel(f"Editing decrypted values from {path}. Plaintext exists only in this in-memory editor.")
        warning.setWordWrap(True)
        layout.addWidget(warning)
        countdown = QLabel("Unlocked time remaining: 03:00")
        countdown.setObjectName("CitizensStatus")
        layout.addWidget(countdown)
        editor = QPlainTextEdit()
        editor.setObjectName("Mono")
        editor.setPlainText(plaintext_editor_text(values))
        editor.setMinimumSize(760, 480)
        layout.addWidget(editor, 1)
        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        layout.addWidget(buttons)
        buttons.rejected.connect(dialog.reject)
        remaining = {"seconds": 180, "expired": False}
        timeout = QTimer(dialog)
        timeout.setInterval(1000)

        def tick() -> None:
            remaining["seconds"] -= 1
            seconds = max(0, remaining["seconds"])
            countdown.setText(f"Unlocked time remaining: {seconds // 60:02d}:{seconds % 60:02d}")
            if seconds <= 0:
                remaining["expired"] = True
                editor.clear()
                dialog.reject()

        timeout.timeout.connect(tick)
        timeout.start()

        def save_configuration() -> None:
            config_text = editor.toPlainText().strip() + "\n"
            try:
                updated = parse_editor_text(config_text)
                if not updated:
                    raise ValueError("At least one credential profile is required.")
                write_encrypted_secrets(path, updated, access_value, pin_value)
                unlock_secrets_session(access_value, pin_value)
            except Exception as exc:
                QMessageBox.warning(dialog, "Encrypted Credentials", f"Could not save: {exc}")
                return
            editor.clear()
            dialog.accept()

        buttons.accepted.connect(save_configuration)
        dialog.exec()
        timeout.stop()
        editor.clear()
        access_value = ""
        pin_value = ""
        if remaining["expired"]:
            QMessageBox.information(
                self,
                "Credential Access Expired",
                "The three-minute window expired. Unsaved plaintext was cleared; .secrets remains encrypted.",
            )
        self._secure_env_status.setText(self._environment_status())

    def _available_analysis_namespaces(self) -> list[tuple[str, str]]:
        rows: list[tuple[str, str]] = []
        friendly_names: dict[str, str] = {}
        for prefix, _label, configured, namespace_id, _host, _enabled in self._cluster_profiles_from_env():
            if configured and namespace_id:
                display_name = _friendly_env_value(prefix)
                if display_name:
                    friendly_names[namespace_id.lower()] = display_name
        try:
            if self._store.path.is_file():
                with self._store.connect() as con:
                    rows = [
                        (str(namespace_id).strip(), str(role or "cluster").strip())
                        for namespace_id, role in con.execute(
                            """
SELECT namespace_id, COALESCE(NULLIF(ANY_VALUE(cluster_name), ''), ANY_VALUE(cluster_role))
FROM snapshot_cluster_runs
WHERE NULLIF(TRIM(namespace_id), '') IS NOT NULL
GROUP BY namespace_id
ORDER BY CASE WHEN LOWER(ANY_VALUE(cluster_role)) = 'producer' THEN 0 ELSE 1 END, namespace_id
"""
                        ).fetchall()
                    ]
        except Exception:
            rows = []
        if not rows:
            for prefix, label, configured, namespace_id, _host, _enabled in self._cluster_profiles_from_env():
                if configured and namespace_id:
                    rows.append((namespace_id, label.lower()))
        seen: set[str] = set()
        result: list[tuple[str, str]] = []
        for namespace_id, role in rows:
            key = namespace_id.lower()
            if not namespace_id or key in seen:
                continue
            seen.add(key)
            friendly = friendly_names.get(key) or str(role).title()
            result.append((namespace_id, f"{friendly} — {namespace_id}"))
        return result

    def _toggle_analysis_cluster_checks(self, analyze_all: bool) -> None:
        for check in self._analysis_cluster_checks.values():
            check.setEnabled(not analyze_all)
            if analyze_all:
                check.setChecked(True)

    def _save_analysis_scope(self) -> None:
        if self._analysis_all_clusters.isChecked():
            selected: list[str] = []
            label = "All loaded clusters"
        else:
            selected = [
                namespace_id
                for namespace_id, check in self._analysis_cluster_checks.items()
                if check.isChecked()
            ]
            if not selected:
                QMessageBox.warning(self, "Analysis Cluster Scope", "Select at least one cluster or choose All loaded clusters.")
                return
            label = ", ".join(selected)
        self._settings.analysis_namespace_filter = selected
        save_settings(self._settings)
        self._analysis_scope_status.setText(
            f"Saved analysis scope: {label}. The selection applies the next time an analysis area is loaded."
        )

    def _database_summary(self) -> str:
        databases = ", ".join(self._settings.discovered_databases) or "-"
        when = self._settings.discovered_at or "never"
        return (
            f"Saved database list: {databases}\n"
            f"Last reloaded: {when}\n"
            f"Settings file: {settings_path()}"
        )

    def _resolved_args(self):
        from ..ingest_redshift import parse_args, resolve_args_from_env

        args = parse_args([])
        resolve_args_from_env(args, self._settings)
        return args

    def _ensure_redshift_password(self, title: str) -> bool:
        return _ensure_redshift_password_widget(self, title, self._settings)

    def _current_source_cluster(self):
        from ..ingest_redshift import load_dotenv, parse_args, resolve_args_from_env

        load_dotenv(None)
        args = parse_args([])
        resolve_args_from_env(args, self._settings)
        config = resolve_source_cluster_config(args)
        if not source_cluster_configured(config):
            return None, "", ""
        fingerprint = source_cluster_fingerprint(config)
        summary = source_cluster_summary(config)
        return args, fingerprint, summary

    def _check_source_cluster_configuration(self) -> None:
        try:
            _args, fingerprint, summary = self._current_source_cluster()
        except SystemExit:
            return
        except Exception:
            return
        if not fingerprint:
            return

        previous = self._settings.last_source_cluster_fingerprint
        if not previous:
            self._settings.last_source_cluster_fingerprint = fingerprint
            self._settings.last_source_cluster_summary = summary
            save_settings(self._settings)
            return
        if previous == fingerprint:
            return

        # A different Redshift endpoint changes future source capture only.
        # It must never redirect the active DuckDB data file.
        previous_summary = self._settings.last_source_cluster_summary or "previous saved cluster"
        candidate = self.parent()
        dashboard = candidate if isinstance(candidate, ClusterDashboard) else None
        QMessageBox.information(
            self,
            "Cluster Switched",
            "The configured cluster changed.\n\n"
            f"Previous: {previous_summary}\n"
            f"Current: {summary}\n\n"
            "The active DuckDB file is unchanged. Use Command Center Browse only if you "
            "intentionally want to select a different data file.",
        )
        self._settings.last_source_cluster_fingerprint = fingerprint
        self._settings.last_source_cluster_summary = summary
        save_settings(self._settings)
        if dashboard is not None:
            dashboard._settings = load_settings()
            dashboard._sync_active_cluster_file(reload_after=False)

    def _truncate_all_and_reload_for_cluster_change(self, fingerprint: str, summary: str) -> None:
        if not self._ensure_redshift_password("Reload All Tables"):
            return
        try:
            capture_args = self._capture_threshold_args()
        except Exception as exc:
            QMessageBox.warning(self, "Reload All Tables", str(exc))
            return
        jobs = [
            (
                "all tables (truncate + reload)",
                ["--duckdb-path", str(self._store.path), "--truncate-all-first", *capture_args],
            )
        ]

        def on_success(output: str) -> None:
            self._settings = load_settings()
            self._settings.last_source_cluster_fingerprint = fingerprint
            self._settings.last_source_cluster_summary = summary
            save_settings(self._settings)
            databases = _databases_accessed_from_output(output)
            suffix = f"\n\nDatabases accessed: {', '.join(databases)}" if databases else ""
            QMessageBox.information(
                self,
                "Reload All Tables",
                f"All analyzer tables were truncated and reloaded from the current Redshift cluster.{suffix}",
            )

        self._ingest.start("Reload All Tables", jobs, on_success=on_success)

    def _default_source_sql(self, table_name: str) -> str:
        from ..ingest_redshift import default_table_sql

        return default_table_sql(self._resolved_args(), table_name)

    def _load_source_sql(self) -> None:
        table_name = self._source_sql_table.currentText().strip()
        if not table_name:
            return
        override = self._settings.table_sql_overrides.get(table_name, "").strip()
        self._source_sql.setPlainText(override or self._default_source_sql(table_name))
        self._source_sql_status.setText(
            f"{'Override saved' if override else 'Using built-in SQL'} for {table_name}. "
            "Use {minutes} or {threshold_seconds} placeholders if needed."
        )

    def _on_source_sql_table_changed(self, table_name: str) -> None:
        self._load_source_sql()
        self._select_table_by_name(table_name.strip())

    def _save_source_sql(self, *, offer_refresh: bool = False) -> bool:
        table_name = self._source_sql_table.currentText().strip()
        sql = self._source_sql.toPlainText().strip()
        if not table_name or not sql:
            QMessageBox.warning(self, "Source SQL", "Choose a table and provide SQL.")
            return False
        default_sql = self._default_source_sql(table_name).strip()
        if sql == default_sql:
            self._settings.table_sql_overrides.pop(table_name, None)
            status = "SQL matches the built-in version; override removed."
        else:
            self._settings.table_sql_overrides[table_name] = sql
            status = "SQL override saved."
        save_settings(self._settings)
        self._source_sql_status.setText(status)
        self._load_counts()
        if offer_refresh:
            response = QMessageBox.question(
                self,
                "Source SQL",
                f"Refresh {table_name} now using the saved SQL?",
            )
            if response == QMessageBox.Yes:
                self._refresh_table_by_name(table_name, confirm=False)
        return True

    def _reset_source_sql(self) -> None:
        table_name = self._source_sql_table.currentText().strip()
        if not table_name:
            return
        self._settings.table_sql_overrides.pop(table_name, None)
        save_settings(self._settings)
        self._load_source_sql()
        self._load_counts()

    def _refresh_source_table(self, table_name: str) -> None:
        if not table_name:
            return
        if not self._save_source_sql(offer_refresh=False):
            return
        self._refresh_table_by_name(table_name)

    def _read_database_threshold(self) -> int | None:
        raw = self._database_threshold.text().strip()
        try:
            value = int(raw)
        except ValueError:
            QMessageBox.warning(self, "Database Discovery", "Minimum query rows must be an integer.")
            return None
        if value < 0:
            QMessageBox.warning(self, "Database Discovery", "Minimum query rows cannot be negative.")
            return None
        return value

    def _save_database_settings(self) -> bool:
        from ..settings import DEFAULT_DATABASE_DISCOVERY_SQL, is_safe_local_database_discovery_sql

        threshold = self._read_database_threshold()
        if threshold is None:
            return False
        sql = self._database_sql.toPlainText().strip()
        if not sql:
            QMessageBox.warning(self, "Database Discovery", "Discovery SQL cannot be blank.")
            return False
        if not is_safe_local_database_discovery_sql(sql):
            self._database_sql.setPlainText(DEFAULT_DATABASE_DISCOVERY_SQL)
            QMessageBox.warning(
                self,
                "Database Discovery",
                "Unsafe discovery SQL was rejected. Database cycling must use SVV_REDSHIFT_DATABASES "
                "and explicitly restrict database_type to 'local'. Shared and Data Catalog databases cannot be used for SVV_TABLE_INFO.",
            )
            return False
        self._settings.database_discovery_sql = sql
        self._settings.database_min_query_count = threshold
        save_settings(self._settings)
        self._database_status.setText(self._database_summary())
        return True

    def _reload_databases(self) -> None:
        if not self._save_database_settings():
            return
        try:
            from ..ingest_redshift import (
                load_dotenv,
                parse_args,
                reload_database_cache,
                resolve_args_from_env,
                split_csv,
            )

            load_dotenv(None)
            args = parse_args([])
            resolve_args_from_env(args, self._settings)
            args.database_discovery_sql = self._settings.database_discovery_sql
            args.database_min_query_count = int(self._database_threshold.text().strip())
            args.reload_databases = True

            user = args.user
            if not user:
                QMessageBox.warning(self, "Reload Databases", "Add the Redshift username in Settings → Local Credentials.")
                return
            from ..secrets_store import session_secret, set_session_secret

            password = session_secret(args.password_env)
            if not password:
                password, ok = QInputDialog.getText(
                    self,
                    "Reload Databases",
                    "Redshift password",
                    QLineEdit.Password,
                )
                if not ok or not password:
                    return
                set_session_secret(args.password_env, password)

            if args.connection == "jdbc":
                if not args.jdbc_url or not args.jdbc_jar:
                    QMessageBox.warning(self, "Reload Databases", "JDBC mode needs REDSHIFT_JDBC_URL and REDSHIFT_JDBC_JAR.")
                    return
                jdbc_url = args.jdbc_url
                jar_paths = [str(Path(p).expanduser()) for p in split_csv(args.jdbc_jar)]
            else:
                if not args.host:
                    QMessageBox.warning(self, "Reload Databases", "Add the Redshift server address in Settings → Local Credentials.")
                    return
                jdbc_url = ""
                jar_paths = []

            with self._busy("Reloading database list from Redshift ..."):
                databases = reload_database_cache(args, self._settings, user, password, jdbc_url, jar_paths)
        except SystemExit as exc:
            QMessageBox.warning(self, "Reload Databases", str(exc))
            return
        except Exception as exc:
            QMessageBox.warning(self, "Reload Databases", str(exc))
            return

        self._settings = load_settings()
        self._database_status.setText(self._database_summary())
        QMessageBox.information(
            self,
            "Reload Databases",
            f"Saved {len(databases)} database(s): {', '.join(databases) or '-'}",
        )

    def _browse_import_file(self) -> None:
        chosen, _ = QFileDialog.getOpenFileName(
            self,
            "Load analyzer file",
            str(Path(self._manual_path.text() or ".").parent),
            "Data files (*.csv *.tsv *.txt *.psv);;All files (*.*)",
        )
        if chosen:
            self._manual_path.setText(chosen)

    def _import_file(self) -> None:
        table_name = self._manual_table.currentText().strip()
        file_path = self._manual_path.text().strip()
        if not file_path:
            QMessageBox.information(self, "Manual File Load", "Choose a file first.")
            return
        try:
            with self._busy(f"Loading {table_name} from file ..."):
                result = import_table_file(
                    self._store,
                    table_name,
                    file_path,
                    label="manual file load",
                    source_database=self._manual_source_db.text().strip(),
                    append=self._manual_append.isChecked(),
                )
                self._load_counts()
        except Exception as exc:
            QMessageBox.warning(self, "Manual File Load", str(exc))
            return

        warning = ""
        if result.missing_expected:
            warning = f" Missing expected columns: {', '.join(result.missing_expected[:8])}"
            if len(result.missing_expected) > 8:
                warning += f" and {len(result.missing_expected) - 8} more."
        self._manual_status.setText(
            f"{result.table_name}: {result.row_count:,} rows, {result.column_count:,} columns "
            f"loaded in {result.elapsed_ms:,.0f} ms. Snapshot {result.snapshot_id}. "
            f"{'Appended' if self._manual_append.isChecked() else 'Replaced table rows for this snapshot'}.{warning}"
        )

    def _refresh_counts(self) -> None:
        try:
            with self._busy("Refreshing DuckDB table counts ..."):
                self._load_counts()
        except Exception as exc:
            QMessageBox.warning(self, "Refresh Counts", str(exc))

    def _rebuild_indexes(self) -> None:
        response = QMessageBox.question(
            self,
            "Build Missing Indexes",
            "Create any missing local DuckDB indexes for the existing snapshot file? Existing indexes are left alone. This does not reload Redshift data.",
        )
        if response != QMessageBox.Yes:
            return
        try:
            with self._busy("Checking/building missing local DuckDB indexes ..."):
                with self._store.connect() as con:
                    before = con.execute("SELECT COUNT(*) FROM duckdb_indexes()").fetchone()[0]
                    self._store.rebuild_indexes(con)
                    index_count = con.execute("SELECT COUNT(*) FROM duckdb_indexes()").fetchone()[0]
                self._load_counts()
            created = max(0, int(index_count) - int(before))
            QMessageBox.information(
                self,
                "Build Missing Indexes",
                f"Local DuckDB indexes are ready. Created {created:,} missing index(es). Index count: {index_count}.",
            )
        except Exception as exc:
            QMessageBox.warning(self, "Build Missing Indexes", str(exc))

    def _read_trim_inputs(self) -> tuple[int, int] | None:
        try:
            minutes = int(self._trim_minutes.text().strip())
        except ValueError:
            QMessageBox.warning(self, "Query History Trimmer", "Minimum runtime minutes must be an integer.")
            return None
        try:
            top_n = int(self._trim_top_n.text().strip())
        except ValueError:
            QMessageBox.warning(self, "Query History Trimmer", "Maximum retained queries must be an integer.")
            return None
        if minutes < 0:
            QMessageBox.warning(self, "Query History Trimmer", "Minimum runtime minutes cannot be negative.")
            return None
        if top_n < 0:
            QMessageBox.warning(self, "Query History Trimmer", "Maximum retained queries cannot be negative. Use 0 for no cap.")
            return None
        return minutes, top_n

    def _preview_query_trim(self) -> None:
        values = self._read_trim_inputs()
        if values is None:
            return
        minutes, top_n = values
        try:
            with self._busy("Previewing query-history trim ..."):
                with self._store.connect() as con:
                    preview = self._store.preview_query_trim(
                        con,
                        min_elapsed_minutes=minutes,
                        keep_top_n=top_n,
                    )
            self._trim_preview = preview
            self._trim_status.setText(_format_trim_preview(preview, self._store.path))
        except Exception as exc:
            QMessageBox.warning(self, "Query History Trimmer", str(exc))

    def _trim_query_tables(self) -> None:
        values = self._read_trim_inputs()
        if values is None:
            return
        minutes, top_n = values
        try:
            with self._busy("Previewing query-history trim ..."):
                with self._store.connect() as con:
                    preview = self._store.preview_query_trim(
                        con,
                        min_elapsed_minutes=minutes,
                        keep_top_n=top_n,
                    )
        except Exception as exc:
            QMessageBox.warning(self, "Query History Trimmer", str(exc))
            return

        if int(preview.get("total_rows_removed") or 0) <= 0:
            self._trim_preview = preview
            self._trim_status.setText(_format_trim_preview(preview, self._store.path))
            QMessageBox.information(self, "Query History Trimmer", "No query evidence rows would be removed.")
            return

        response = QMessageBox.question(
            self,
            "Trim Query Tables",
            "Create a restorable DuckDB backup, then delete local query evidence rows outside this retention window?\n\n"
            f"Current DuckDB size: {_fmt_file_size(self._store.path)}\n\n"
            + _format_trim_preview(preview, self._store.path),
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if response != QMessageBox.Yes:
            self._trim_preview = preview
            self._trim_status.setText(_format_trim_preview(preview, self._store.path))
            return

        try:
            with self._busy("Backing up and trimming local DuckDB query evidence ..."):
                backup_path = self._store.backup_database("before-query-trim")
                with self._store.connect() as con:
                    result = self._store.trim_query_evidence(
                        con,
                        min_elapsed_minutes=minutes,
                        keep_top_n=top_n,
                    )
                self._load_counts()
            self._trim_preview = result
            self._trim_status.setText(
                _format_trim_preview(result, self._store.path)
                + f"\nTrim applied. Backup: {backup_path}"
            )
            QMessageBox.information(
                self,
                "Query History Trimmer",
                f"Local query evidence tables were trimmed.\n\nBackup created:\n{backup_path}",
            )
        except Exception as exc:
            QMessageBox.warning(self, "Query History Trimmer", str(exc))

    def _restore_duckdb_backup(self) -> None:
        backup_dir = self._store.path.parent / "backups"
        chosen, _ = QFileDialog.getOpenFileName(
            self,
            "Restore DuckDB backup",
            str(backup_dir if backup_dir.exists() else self._store.path.parent),
            "DuckDB files (*.duckdb *.db);;All files (*.*)",
        )
        if not chosen:
            return
        response = QMessageBox.question(
            self,
            "Restore DuckDB Backup",
            "Replace the current local DuckDB file with this backup?\n\n"
            f"Current DuckDB size: {_fmt_file_size(self._store.path)}\n"
            f"Backup size: {_fmt_file_size(Path(chosen))}\n\n"
            "A backup of the current file will be created first. This does not reload Redshift data.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if response != QMessageBox.Yes:
            return
        try:
            with self._busy("Restoring local DuckDB backup ..."):
                current_backup = self._store.restore_database_backup(chosen)
                self._load_counts()
            suffix = f"\n\nCurrent file was backed up to:\n{current_backup}" if current_backup else ""
            QMessageBox.information(self, "Restore DuckDB Backup", f"Restored local DuckDB backup:\n{chosen}{suffix}")
        except Exception as exc:
            QMessageBox.warning(self, "Restore DuckDB Backup", str(exc))

    def _load_counts(self, selected_table: str = "") -> None:
        selected_table = selected_table or self._selected_table_name()
        if not self._store.path.is_file():
            self._model = _DataFrameModel(
                pd.DataFrame(columns=["dataset", "record_count", "last_capture", "source_info", "indexed_cols"])
            )
            self._table.setModel(self._model)
            self._counts_status.setText(
                f"ACTIVE FILE NOT FOUND: {self._store.path}. Use Browse in Command Center to select the populated DuckDB file."
            )
            self._counts_status.setStyleSheet(f"color:{PALETTE.crit}; font-weight:700;")
            return
        with self._store.connect() as con:
            expected_hashes = {}
            try:
                from ..ingest_redshift import expected_table_sql_hashes

                expected_hashes = expected_table_sql_hashes(self._resolved_args(), self._settings)
            except Exception:
                expected_hashes = {}
            counts = self._store.table_counts(con, expected_hashes)
        total_rows = int(pd.to_numeric(counts.get("record_count"), errors="coerce").fillna(0).sum())
        file_size = self._store.path.stat().st_size if self._store.path.is_file() else 0
        if total_rows:
            self._counts_status.setText(
                f"Active file verified: {self._store.path} | {_fmt_file_size(self._store.path)} | "
                f"{total_rows:,} total rows across tracked datasets."
            )
            self._counts_status.setStyleSheet(f"color:{PALETTE.ok}; font-weight:600;")
        else:
            self._counts_status.setText(
                f"WARNING: every tracked dataset is empty in this specific file: {self._store.path} "
                f"({file_size:,} bytes). The in-memory quadrant may have been loaded from a different DuckDB file. "
                "Close Settings, use Browse, and select the populated redshift.duckdb file."
            )
            self._counts_status.setStyleSheet(f"color:{PALETTE.crit}; font-weight:700;")
        display_counts, sort_counts = self._dataset_metrics_frame(counts)
        self._model = _DataFrameModel(display_counts, sort_sources=sort_counts, row_df=counts)
        self._table.setModel(self._model)
        selection = self._table.selectionModel()
        if selection is not None:
            selection.currentRowChanged.connect(self._on_table_current_row_changed)
        self._table.resizeColumnsToContents()
        target = selected_table or self._last_selected_table or self._source_sql_table.currentText().strip()
        self._select_table_by_name(target)
        self._load_database_overview()

    def _dataset_metrics_frame(self, counts: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        if counts is None or counts.empty:
            columns = ["dataset", "record_count", "last_capture", "source_info", "indexed_cols"]
            return pd.DataFrame(columns=columns), pd.DataFrame(columns=columns)
        df = counts.copy()
        def col(name: str, default: object = "") -> pd.Series:
            return df[name] if name in df.columns else pd.Series([default] * len(df), index=df.index)

        display = pd.DataFrame(
            {
                "dataset": col("table_name").map(lambda value: str(value or "")),
                "record_count": pd.to_numeric(col("record_count", 0), errors="coerce").fillna(0).astype(int),
                "last_capture": col("latest_captured_at").map(_fmt_capture_datetime),
                "source_info": col("source_query", "-").map(lambda value: str(value or "-") or "-"),
                "indexed_cols": col("indexed_columns", "-").map(lambda value: str(value or "-") or "-"),
            }
        )
        sort_sources = pd.DataFrame(
            {
                "dataset": display["dataset"],
                "record_count": display["record_count"],
                "last_capture": pd.to_datetime(col("latest_captured_at"), errors="coerce"),
                "source_info": display["source_info"],
                "indexed_cols": display["indexed_cols"],
            }
        )
        return display, sort_sources

    def _load_database_overview(self) -> None:
        columns = ["database_name", "schema_count", "table_count", "view_count", "procedure_count"]
        try:
            with self._store.connect() as con:
                frame = con.execute(
                    """
WITH objects AS (
  SELECT
    COALESCE(NULLIF(source_db, ''), NULLIF("database", ''), 'unknown') AS database_name,
    COALESCE(NULLIF("schema", ''), 'unknown') AS schema_name,
    'table' AS object_type,
    COALESCE(NULLIF("table", ''), '') AS object_name
  FROM svv_table_info_all
  WHERE COALESCE(NULLIF(source_db, ''), NULLIF("database", '')) IS NOT NULL
    AND COALESCE(NULLIF("table", ''), '') <> ''
  UNION ALL
  SELECT
    COALESCE(NULLIF("database", ''), 'unknown') AS database_name,
    COALESCE(NULLIF("schema", ''), 'unknown') AS schema_name,
    'view' AS object_type,
    COALESCE(NULLIF(view_name, ''), '') AS object_name
  FROM view_definitions
  WHERE COALESCE(NULLIF("database", ''), '') <> ''
    AND COALESCE(NULLIF(view_name, ''), '') <> ''
  UNION ALL
  SELECT
    COALESCE(NULLIF("database", ''), 'unknown') AS database_name,
    COALESCE(NULLIF("schema", ''), 'unknown') AS schema_name,
    'procedure' AS object_type,
    COALESCE(NULLIF(procedure_name, ''), NULLIF(procedure_key, ''), '') AS object_name
  FROM procedure_definitions
  WHERE COALESCE(NULLIF("database", ''), '') <> ''
    AND COALESCE(NULLIF(procedure_name, ''), NULLIF(procedure_key, ''), '') <> ''
)
SELECT
  database_name,
  COUNT(DISTINCT schema_name) AS schema_count,
  COUNT(DISTINCT CASE WHEN object_type = 'table' THEN schema_name || '.' || object_name END) AS table_count,
  COUNT(DISTINCT CASE WHEN object_type = 'view' THEN schema_name || '.' || object_name END) AS view_count,
  COUNT(DISTINCT CASE WHEN object_type = 'procedure' THEN schema_name || '.' || object_name END) AS procedure_count
FROM objects
GROUP BY database_name
ORDER BY database_name
"""
                ).fetchdf()
        except Exception:
            frame = pd.DataFrame(columns=columns)
        for col_name in columns:
            if col_name not in frame.columns:
                frame[col_name] = 0 if col_name.endswith("_count") else ""
        frame = frame[columns]
        self._database_overview.setModel(_DataFrameModel(frame))
        self._database_overview.resizeColumnsToContents()

    def _truncate_selected(self) -> None:
        table_names = self._selected_table_names()
        if not table_names:
            QMessageBox.information(self, "Truncate Table", "Select a DuckDB table first.")
            return
        table_label = ", ".join(table_names)
        response = QMessageBox.question(
            self,
            "Truncate Table",
            f"Delete all rows from {len(table_names)} DuckDB table(s)?\n\n{table_label}",
        )
        if response != QMessageBox.Yes:
            return
        try:
            with self._busy(f"Truncating {len(table_names)} DuckDB table(s) ..."):
                with self._store.connect() as con:
                    for table_name in table_names:
                        self._store.truncate_table(con, table_name)
                self._load_counts(selected_table=table_names[0])
        except Exception as exc:
            QMessageBox.warning(self, "Truncate Table", str(exc))

    def _begin_busy(self, message: str) -> None:
        self._busy_depth += 1
        if self._busy_depth == 1:
            dialog = QProgressDialog(message, "", 0, 0, self)
            dialog.setWindowTitle("Working")
            dialog.setWindowModality(Qt.WindowModal)
            dialog.setMinimumDuration(0)
            dialog.setAutoClose(False)
            dialog.setAutoReset(False)
            dialog.setCancelButton(None)
            dialog.setMinimumWidth(420)
            dialog.show()
            dialog.raise_()
            dialog.activateWindow()
            self._busy_dialog = dialog
            QApplication.setOverrideCursor(Qt.WaitCursor)
        self._set_busy_message(message)

    def _set_busy_message(self, message: str) -> None:
        self._manual_status.setText(message)
        self._source_sql_status.setText(message)
        self._trim_status.setText(message)
        if self._busy_dialog is not None:
            self._busy_dialog.setLabelText(message)
        QApplication.processEvents()

    def _end_busy(self) -> None:
        if self._busy_depth > 0:
            self._busy_depth -= 1
        if self._busy_depth == 0:
            QApplication.restoreOverrideCursor()
            if self._busy_dialog is not None:
                self._busy_dialog.close()
                self._busy_dialog.deleteLater()
                self._busy_dialog = None
        QApplication.processEvents()

    @contextlib.contextmanager
    def _busy(self, message: str):
        self._begin_busy(message)
        try:
            yield
        finally:
            self._end_busy()

    def _refresh_table_by_name(self, table_name: str, *, confirm: bool = True) -> None:
        self._refresh_tables_by_name([table_name], confirm=confirm)

    def _refresh_tables_by_name(self, table_names: list[str], *, confirm: bool = True) -> None:
        from ..ingest_redshift import REFRESH_ORDER

        table_names = [str(name or "").strip() for name in table_names]
        table_names = [name for index, name in enumerate(table_names) if name and name not in table_names[:index]]
        # Refresh in dependency order: history/text anchor the analysis,
        # evidence tables follow, per-database catalog tables run last.
        table_names.sort(key=lambda name: REFRESH_ORDER.get(name, len(REFRESH_ORDER) + 1))
        if not table_names:
            QMessageBox.information(self, "Refresh Table", "Select a DuckDB table first.")
            return
        if confirm:
            table_label = "\n".join(
                f"{index}. {name}" for index, name in enumerate(table_names, start=1)
            )
            response = QMessageBox.question(
                self,
                "Refresh Table",
                f"Reload {len(table_names)} table(s) from Redshift into the latest DuckDB snapshot, "
                f"in this order?\n\n{table_label}",
            )
            if response != QMessageBox.Yes:
                return
        if not self._ensure_redshift_password("Refresh Table"):
            return
        try:
            capture_args = self._capture_threshold_args()
        except Exception as exc:
            QMessageBox.warning(self, "Refresh Table", str(exc))
            return
        jobs = [
            (
                table_name,
                ["--duckdb-path", str(self._store.path), "--refresh-table", table_name, *capture_args],
            )
            for table_name in table_names
        ]

        def on_success(output: str) -> None:
            databases = _databases_accessed_from_output(output)
            suffix = f"\n\nDatabases accessed: {', '.join(databases)}" if databases else ""
            if databases:
                self._source_sql_status.setText(
                    f"{len(table_names)} table(s) refreshed. Databases accessed: {', '.join(databases)}"
                )
            QMessageBox.information(
                self, "Refresh Table", f"{len(table_names)} table(s) refreshed from Redshift.{suffix}"
            )

        self._ingest.start(
            "Refresh Table", jobs, on_success=on_success, select_table=table_names[0]
        )



class _MetricStrip(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        lay = QGridLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setHorizontalSpacing(6)
        lay.setVerticalSpacing(6)
        self._tiles = {
            "queries": _MetricTile("SLOW QUERIES"),
            "critical": _MetricTile("CRITICAL"),
            "warnings": _MetricTile("WARNINGS"),
            "runtime": _MetricTile("TOTAL RUNTIME"),
            "rules": _MetricTile("CHECKS"),
            "repeats": _MetricTile("REPEATS"),
            "actions": _MetricTile("ACTIONS"),
            "rewrites": _MetricTile("REWRITES"),
            "tables": _MetricTile("RISK TABLES"),
        }
        for i, tile in enumerate(self._tiles.values()):
            lay.addWidget(tile, i // 5, i % 5)

    def set_summary(self, summary: dict, rule_count: int) -> None:
        self._tiles["queries"].set_value(_fmt_int(summary.get("slow_query_count")))
        self._tiles["critical"].set_value(_fmt_int(summary.get("critical_count")))
        self._tiles["warnings"].set_value(_fmt_int(summary.get("warning_count")))
        self._tiles["runtime"].set_value(_fmt_seconds(summary.get("total_runtime_s")))
        self._tiles["rules"].set_value(str(rule_count or INSIGHT_RULE_COUNT))
        self._tiles["repeats"].set_value(_fmt_int(summary.get("repeat_group_count")))
        self._tiles["actions"].set_value(_fmt_int(summary.get("action_count")))
        self._tiles["rewrites"].set_value(_fmt_int(summary.get("rewrite_count")))
        self._tiles["tables"].set_value(_fmt_int(summary.get("high_risk_table_count")))


class _MetricTile(QFrame):
    def __init__(self, label: str):
        super().__init__()
        self.setObjectName("Card")
        self.setMinimumHeight(42)
        self.setMaximumHeight(50)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(8, 4, 8, 4)
        lay.setSpacing(0)
        k = QLabel(label)
        k.setObjectName("SectionHeader")
        k.setStyleSheet("font-size:9px;")
        self._value = QLabel("-")
        self._value.setObjectName("Display")
        self._value.setStyleSheet("font-size:14px; font-weight:700;")
        lay.addWidget(k)
        lay.addWidget(self._value)

    def set_value(self, value: str) -> None:
        self._value.setText(value)


class _FamilyBars(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("CardSubtle")
        self._df = pd.DataFrame()
        self.setMinimumHeight(150)

    def set_dataframe(self, df: pd.DataFrame) -> None:
        self._df = df.copy() if df is not None else pd.DataFrame()
        self.update()

    def paintEvent(self, _) -> None:
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        p.fillRect(self.rect(), QColor(PALETTE.bg_1))
        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("Inter", 9, QFont.Bold))
        p.drawText(QRectF(14, 10, self.width() - 28, 18), Qt.AlignLeft, "ISSUE FAMILIES")
        if self._df.empty:
            p.setFont(QFont("Inter", 10))
            p.drawText(self.rect(), Qt.AlignCenter, "No issues found.")
            return
        df = self._df.head(8).reset_index(drop=True)
        max_count = max(float(df["issue_count"].max() or 1), 1.0)
        y = 38
        for _, row in df.iterrows():
            family = str(row.get("family") or "-")
            count = float(row.get("issue_count") or 0)
            crit = int(row.get("critical_count") or 0)
            bar_w = max(3.0, (self.width() - 170) * count / max_count)
            color = QColor(PALETTE.crit if crit else PALETTE.warn)
            p.setPen(QColor(PALETTE.text_1))
            p.setFont(QFont("Inter", 9, QFont.DemiBold))
            p.drawText(QRectF(14, y, 122, 18), Qt.AlignLeft | Qt.AlignVCenter, family[:18])
            p.setPen(Qt.NoPen)
            p.setBrush(QColor(PALETTE.bg_2))
            p.drawRoundedRect(QRectF(140, y + 3, self.width() - 170, 10), 5, 5)
            p.setBrush(color)
            p.drawRoundedRect(QRectF(140, y + 3, bar_w, 10), 5, 5)
            p.setPen(QColor(PALETTE.text_2))
            p.drawText(QRectF(self.width() - 26, y, 22, 18), Qt.AlignRight | Qt.AlignVCenter, str(int(count)))
            y += 20


class _InsightCards(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(8)
        head = QLabel("TOP HIDDEN PERFORMANCE THIEVES")
        head.setObjectName("SectionHeader")
        root.addWidget(head)
        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._scroll.setFrameShape(QFrame.NoFrame)
        self._host = QWidget()
        self._lay = QVBoxLayout(self._host)
        self._lay.setContentsMargins(0, 0, 0, 0)
        self._lay.setSpacing(8)
        self._scroll.setWidget(self._host)
        root.addWidget(self._scroll, 1)

    def set_dataframe(self, df: pd.DataFrame) -> None:
        _clear_layout(self._lay)
        if df is None or df.empty:
            empty = QLabel("No triggered insights yet.")
            empty.setObjectName("Caption")
            self._lay.addWidget(empty)
            self._lay.addStretch(1)
            return
        display_df = _aggregate_insight_cards(df)
        for _, row in display_df.head(14).iterrows():
            self._lay.addWidget(_InsightCard(row))
        self._lay.addStretch(1)


class _InsightCard(QFrame):
    def __init__(self, row: pd.Series):
        super().__init__()
        self.setObjectName("Card")
        lay = QVBoxLayout(self)
        lay.setContentsMargins(14, 12, 14, 12)
        lay.setSpacing(7)
        top = QHBoxLayout()
        sev = str(row.get("severity") or "info")
        chip = QLabel(sev.upper())
        chip.setProperty("chip", True)
        chip.setProperty("severity", sev)
        chip.setAlignment(Qt.AlignCenter)
        top.addWidget(chip, 0)
        fam = QLabel(str(row.get("family") or "-").upper())
        fam.setObjectName("SectionHeader")
        top.addWidget(fam, 0)
        top.addStretch(1)
        hits = QLabel(f"{int(row.get('issue_count') or 1):,} hit(s)")
        hits.setObjectName("Mono")
        hits.setStyleSheet(f"color:{PALETTE.text_2}; font-size:11px;")
        top.addWidget(hits, 0)
        score = QLabel(f"{float(row.get('impact_score') or 0):.0f}")
        score.setObjectName("Mono")
        score.setToolTip("Impact Score is a relative priority score from the triggered rule. Higher means fix sooner.")
        top.addWidget(score, 0)
        lay.addLayout(top)
        title = QLabel(str(row.get("title") or "-"))
        title.setObjectName("H1")
        title.setWordWrap(True)
        lay.addWidget(title)
        target = str(row.get("target_label") or row.get("subject") or "cluster")
        subject = QLabel(f"Top target: {target}")
        subject.setObjectName("Mono")
        subject.setStyleSheet(f"color:{PALETTE.text_2}; font-size:11px;")
        lay.addWidget(subject)
        metric = QLabel(
            f"Max metric: {row.get('metric_label') or 'Observed Value'} "
            f"{row.get('metric_display') or _fmt_value('metric_value', row.get('metric_value'))} | "
            f"Priority: {row.get('impact_band') or 'Unscored'}"
        )
        metric.setObjectName("Caption")
        metric.setWordWrap(True)
        lay.addWidget(metric)
        evidence = QLabel()
        evidence.setObjectName("Caption")
        apply_markdown(evidence, str(row.get("evidence") or ""))
        lay.addWidget(evidence)
        rec = QLabel()
        rec.setWordWrap(True)
        # Route color through PALETTE instead of hardcoded hex, and render as
        # themed markdown prose.
        rec.setStyleSheet(f"color:{PALETTE.text_1}; font-size:11px;")
        apply_markdown(rec, str(row.get("recommendation") or ""))
        lay.addWidget(rec)


def _aggregate_insight_cards(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "insight_id" not in df.columns:
        return df
    group_cols = ["insight_id", "severity", "family", "title", "evidence", "recommendation"]
    existing = [c for c in group_cols if c in df.columns]
    agg = (
        df.groupby(existing, dropna=False)
        .agg(
            issue_count=("insight_id", "size"),
            impact_score=("impact_score", "max"),
            metric_value=("metric_value", "max"),
            metric_label=("metric_label", lambda s: next((str(x) for x in s if str(x).strip()), "Observed Value")),
            metric_display=("metric_display", lambda s: next((str(x) for x in s if str(x).strip()), "-")),
            impact_band=("impact_band", lambda s: next((str(x) for x in s if str(x).strip()), "Unscored")),
            target_label=("target_label", lambda s: next((str(x) for x in s if str(x).strip()), "cluster")),
            subject=("subject", lambda s: next((str(x) for x in s if str(x).strip()), "cluster")),
        )
        .reset_index()
        .sort_values(["impact_score", "issue_count"], ascending=[False, False])
    )
    return agg


class FlowMap(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("CardSubtle")
        self._df = pd.DataFrame()
        self.setMinimumHeight(260)

    def set_dataframe(self, df: pd.DataFrame) -> None:
        self._df = df.copy() if df is not None else pd.DataFrame()
        self.update()

    def paintEvent(self, _) -> None:
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        p.fillRect(self.rect(), QColor(PALETTE.bg_1))
        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("Inter", 9, QFont.Bold))
        p.drawText(QRectF(14, 10, self.width() - 28, 18), Qt.AlignLeft, "ISSUE FLOW")
        if self._df.empty:
            p.setFont(QFont("Inter", 10))
            p.drawText(self.rect(), Qt.AlignCenter, "No issue flow to draw.")
            return

        width = self.width()
        height = self.height()
        start = QRectF(24, height / 2 - 24, 120, 48)
        families = sorted(set(self._df["target"].dropna()) - {"crit", "warn", "info"})
        severities = ["crit", "warn", "info"]
        fam_nodes = _node_positions(families[:8], width * 0.42, 46, height - 32)
        sev_nodes = _node_positions(severities, width - 132, 70, height - 70)

        p.setPen(Qt.NoPen)
        p.setBrush(QColor(PALETTE.bg_2))
        p.drawRoundedRect(start, 8, 8)
        p.setPen(QColor(PALETTE.text_0))
        p.setFont(QFont("Inter", 10, QFont.DemiBold))
        p.drawText(start, Qt.AlignCenter, "Slow queries")

        for _, row in self._df.iterrows():
            source = str(row.get("source") or "")
            target = str(row.get("target") or "")
            weight = float(row.get("weight") or 1)
            impact = float(row.get("max_impact") or 0)
            if source == "Slow queries" and target in fam_nodes:
                a = start.center()
                b = fam_nodes[target].center()
            elif source in fam_nodes and target in sev_nodes:
                a = fam_nodes[source].center()
                b = sev_nodes[target].center()
            else:
                continue
            pen = QPen(_impact_color(impact), max(1.0, min(9.0, 1.5 + math.sqrt(weight))))
            pen.setCapStyle(Qt.RoundCap)
            p.setPen(pen)
            p.drawLine(a, b)

        for name, rect in fam_nodes.items():
            _draw_node(p, rect, name, QColor(PALETTE.bg_2))
        for name, rect in sev_nodes.items():
            color = QColor(PALETTE.crit if name == "crit" else PALETTE.warn if name == "warn" else PALETTE.accent_bright)
            _draw_node(p, rect, name.upper(), color, dark_text=(name != "crit"))


class QueryHeatMap(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("CardSubtle")
        self._df = pd.DataFrame()
        self.setMinimumHeight(250)

    def set_dataframe(self, df: pd.DataFrame) -> None:
        self._df = df.copy() if df is not None else pd.DataFrame()
        self.update()

    def paintEvent(self, _) -> None:
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        p.fillRect(self.rect(), QColor(PALETTE.bg_1))
        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("Inter", 9, QFont.Bold))
        p.drawText(QRectF(14, 10, self.width() - 28, 18), Qt.AlignLeft, "QUERY X ISSUE HEAT MAP")
        if self._df.empty:
            p.setFont(QFont("Inter", 10))
            p.drawText(self.rect(), Qt.AlignCenter, "No query issue matrix yet.")
            return
        df = self._df.copy()
        queries = [str(x) for x in df["query_id"].dropna().drop_duplicates().head(14)]
        families = [str(x) for x in df["family"].dropna().drop_duplicates().head(8)]
        if not queries or not families:
            return
        left = 96
        top = 46
        cell_w = max(28, int((self.width() - left - 18) / max(1, len(families))))
        cell_h = max(14, int((self.height() - top - 18) / max(1, len(queries))))
        p.setFont(QFont("Inter", 8, QFont.DemiBold))
        for j, fam in enumerate(families):
            p.setPen(QColor(PALETTE.text_2))
            p.drawText(QRectF(left + j * cell_w, 28, cell_w, 16), Qt.AlignCenter, fam[:9])
        lookup = {
            (str(r.query_id), str(r.family)): int(r.severity_rank or 0)
            for r in df.itertuples()
        }
        for i, qid in enumerate(queries):
            y = top + i * cell_h
            p.setPen(QColor(PALETTE.text_2))
            p.drawText(QRectF(12, y, left - 18, cell_h), Qt.AlignRight | Qt.AlignVCenter, qid)
            for j, fam in enumerate(families):
                rank = lookup.get((qid, fam), 0)
                color = QColor(PALETTE.bg_2)
                if rank == 3:
                    color = QColor(PALETTE.crit)
                elif rank == 2:
                    color = QColor(PALETTE.warn)
                elif rank == 1:
                    color = QColor(PALETTE.accent_bright)
                p.setPen(QColor(PALETTE.bg_1))
                p.setBrush(color)
                p.drawRoundedRect(QRectF(left + j * cell_w + 2, y + 2, cell_w - 4, cell_h - 4), 3, 3)


class _RewritePage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        header = QHBoxLayout()
        title = QLabel("11 CRITICAL REWRITE OPPORTUNITIES")
        title.setObjectName("SectionHeader")
        header.addWidget(title)
        header.addStretch(1)
        note = QLabel("Ranked by SQL-triggered impact; every card includes a rewrite shape.")
        note.setObjectName("Caption")
        header.addWidget(note)
        root.addLayout(header)

        split = QSplitter(Qt.Horizontal)
        split.setChildrenCollapsible(False)
        self._cards = _RewriteCards()
        self._table = _TablePage("REWRITE LEDGER", REWRITE_COLS)
        split.addWidget(self._cards)
        split.addWidget(self._table)
        split.setSizes([700, 780])
        root.addWidget(split, 1)

    def set_dataframe(self, df: pd.DataFrame) -> None:
        self._cards.set_dataframe(df)
        self._table.set_dataframe(df)


class _ActionPlanPage(QWidget):
    loadRequested = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        header = QHBoxLayout()
        title = QLabel("FIX QUERY — EXECUTIVE DECISION BRIEF")
        title.setObjectName("SectionHeader")
        header.addWidget(title)
        header.addStretch(1)
        note = QLabel("Decisions and workload scope first; query-level proof is available on demand.")
        note.setObjectName("Caption")
        # Yields width: this page is now a top-level tab and must fit a 1280px
        # viewport without forcing a horizontal page scrollbar.
        note.setWordWrap(True)
        note.setMinimumWidth(0)
        note.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Preferred)
        header.addWidget(note, 1)
        load_btn = QPushButton("Load Actions")
        load_btn.setObjectName("Primary")
        header.addWidget(load_btn)
        root.addLayout(header)

        self._brief = _ExecutiveFixQueryView()
        root.addWidget(self._brief, 1)
        load_btn.clicked.connect(lambda: self.loadRequested.emit("action_plan"))

    def set_dataframes(
        self,
        actions: pd.DataFrame,
        rewrites: pd.DataFrame,
        slow_queries: pd.DataFrame | None = None,
    ) -> None:
        self._brief.set_dataframes(actions, rewrites, slow_queries)


class _TableReviewPage(QWidget):
    loadRequested = Signal(str)
    _MIN_ROWS_FILTER_CHOICES = [
        ("No Filter", 0),
        ("1M", 1_000_000),
        ("10M", 10_000_000),
        ("100M", 100_000_000),
        ("1B", 1_000_000_000),
        ("10B", 10_000_000_000),
    ]

    def __init__(self, parent=None):
        super().__init__(parent)
        self._df = pd.DataFrame()
        self._table_status = pd.DataFrame()
        self._load_errors: tuple[str, ...] = ()
        self._snapshot_id = ""
        self._settings = load_settings()
        self._visible_cols = _normalize_table_review_columns(
            self._settings.table_review_visible_cols or TABLE_REVIEW_COLS
        )
        self._model: _DataFrameModel | None = None
        self._status_model: _DataFrameModel | None = None
        self._large_dialog: QDialog | None = None
        self._restoring_column_order = False
        self._intersection_filter_fallback = False

        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(6)

        header = QHBoxLayout()
        filter_label = QLabel("Minimum Rows")
        filter_label.setObjectName("Caption")
        header.addWidget(filter_label)
        self._min_rows_combo = QComboBox()
        for label_text, value in self._MIN_ROWS_FILTER_CHOICES:
            self._min_rows_combo.addItem(label_text, value)
        self._min_rows_combo.setMinimumWidth(96)
        self._min_rows_combo.setMaximumWidth(126)
        header.addWidget(self._min_rows_combo)
        self._source_db_filter = _MultiSelectMenuButton("Source DB")
        self._source_db_filter.setToolTip("Filter Table Review to one or more source databases.")
        header.addWidget(self._source_db_filter)
        self._schema_filter = _MultiSelectMenuButton("Schema")
        self._schema_filter.setToolTip("Filter Table Review to one or more schemas.")
        header.addWidget(self._schema_filter)
        self._table_name_filter = QLineEdit()
        self._table_name_filter.setPlaceholderText("Filter table name...")
        self._table_name_filter.setClearButtonEnabled(True)
        self._table_name_filter.setMinimumWidth(150)
        self._table_name_filter.setMaximumWidth(240)
        self._table_name_filter.setToolTip(
            "Type any part of a table name. Filtering is local and does not reload DuckDB."
        )
        header.addWidget(self._table_name_filter)
        self._hide_without_intersection = QCheckBox("Hide Tables Without Intersection")
        self._hide_without_intersection.setChecked(
            bool(self._settings.table_review_hide_without_intersection)
        )
        self._hide_without_intersection.setToolTip(
            "Checked: show only tables matched to query or scan telemetry. "
            "Tables with no associated workload telemetry remain hidden."
        )
        header.addWidget(self._hide_without_intersection)
        header.addStretch(1)
        self._status = QLabel("Load a DuckDB snapshot to review tables.")
        self._status.setObjectName("Caption")
        self._status.setMaximumWidth(460)
        self._status.setWordWrap(True)
        header.addWidget(self._status)
        self._error_link = QLabel("")
        self._error_link.setObjectName("Caption")
        self._error_link.setTextFormat(Qt.RichText)
        self._error_link.setTextInteractionFlags(Qt.TextBrowserInteraction)
        self._error_link.setOpenExternalLinks(False)
        self._error_link.linkActivated.connect(lambda _href: self._open_error_log())
        self._error_link.hide()
        header.addWidget(self._error_link)
        load_btn = QPushButton("Load")
        load_btn.setObjectName("Primary")
        load_btn.setToolTip("Load Table Review from local DuckDB.")
        load_btn.clicked.connect(lambda: self.loadRequested.emit("table_review"))
        header.addWidget(load_btn)
        columns_btn = QPushButton("Show Columns")
        columns_btn.setObjectName("Ghost")
        columns_btn.setToolTip("Choose Table Review columns.")
        columns_btn.clicked.connect(self._choose_columns)
        header.addWidget(columns_btn)
        large_btn = QPushButton("Wide")
        large_btn.setObjectName("Primary")
        large_btn.setToolTip("Open Table Review in a large grid.")
        large_btn.clicked.connect(self._open_large_view)
        header.addWidget(large_btn)
        coverage_btn = QPushButton("Status")
        coverage_btn.setObjectName("Ghost")
        coverage_btn.setToolTip("Open DuckDB coverage details.")
        coverage_btn.clicked.connect(self._open_coverage_details)
        header.addWidget(coverage_btn)
        root.addLayout(header)

        self._coverage = QLabel("Coverage status unavailable.")
        self._coverage.setObjectName("Caption")
        self._coverage.setWordWrap(True)
        self._coverage.hide()

        self._status_table = QTableView()
        _configure_table_view(self._status_table)

        self._table = QTableView()
        _configure_table_view(self._table)
        self._table.horizontalHeader().setSectionsMovable(True)
        self._table.horizontalHeader().sectionMoved.connect(self._on_column_moved)
        self._table.setMinimumHeight(0)
        self._top_scroll = _add_external_horizontal_scrollbar(root, self._table)
        root.addWidget(self._table, 1)
        self._review_tooltips = _DelayedTableToolTips(self._table, delay_ms=2000)
        self._min_rows_combo.currentIndexChanged.connect(self._apply_columns)
        self._source_db_filter.changed.connect(self._on_source_db_filter_changed)
        self._schema_filter.changed.connect(self._apply_columns)
        self._table_name_filter.textChanged.connect(self._apply_columns)
        self._hide_without_intersection.toggled.connect(self._on_intersection_filter_changed)

    def set_dataframe(self, df: pd.DataFrame) -> None:
        self._df = df.copy() if df is not None else pd.DataFrame()
        self._refresh_filter_options()
        self._apply_columns()

    def show_loading(self) -> None:
        self._status.setText("Loading Table Review from the local DuckDB ...")
        QApplication.processEvents()

    def show_blocked(self, message: str) -> None:
        self._status.setText(message)

    def set_table_status(self, status: pd.DataFrame) -> None:
        self._table_status = status.copy() if status is not None else pd.DataFrame()
        if self._table_status.empty:
            self._status_table.setModel(None)
        else:
            cols = [c for c in TABLE_STATUS_COLS if c in self._table_status.columns]
            cols = cols or list(self._table_status.columns)
            self._status_model = _DataFrameModel(self._table_status[cols])
            self._status_table.setModel(self._status_model)
            self._status_table.resizeColumnsToContents()
        self._update_coverage_label()

    def set_snapshot_info(self, snapshot_id: str | None) -> None:
        self._snapshot_id = str(snapshot_id or "")

    def set_load_errors(self, errors: tuple[str, ...]) -> None:
        self._load_errors = tuple(errors or ())
        self._update_load_error_link()
        self._update_coverage_label()

    def set_load_error(self, message: str) -> None:
        self._load_errors = (str(message),) if message else ()
        self._status.setText("DuckDB load failed. Review the error below and table recap if available.")
        self._update_load_error_link()
        self._update_coverage_label()

    def _apply_columns(self, *_args) -> None:
        if self._df.empty:
            self._table.setModel(None)
            self._model = None
            diagnostic = next(
                (
                    message
                    for message in self._load_errors
                    if str(message).startswith("Table Review returned 0 rows.")
                ),
                "",
            )
            if diagnostic:
                self._status.setText(diagnostic)
                self._status.setToolTip(diagnostic)
            else:
                self._status.setText("No table rows loaded.")
                self._status.setToolTip("")
            self._clear_cards()
            return
        df = self._filtered_df()
        if df.empty:
            self._table.setModel(None)
            self._model = None
            self._status.setText(f"No tables match {self._filter_label()}.")
            self._clear_cards()
            self._update_coverage_label()
            return
        display_df, sort_sources = _table_attribute_display_frame(df, table_review=True)
        cols = [c for c in self._visible_cols if c in display_df.columns]
        if not cols:
            cols = [c for c in TABLE_REVIEW_COLS if c in display_df.columns] or list(display_df.columns)
            self._visible_cols = cols
        self._model = _DataFrameModel(display_df[cols], sort_sources=sort_sources, row_df=df)
        self._table.setModel(self._model)
        self._restore_header_to_model_order(self._table)
        self._table.resizeColumnsToContents()
        self._table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        _sync_external_horizontal_scrollbar(self._top_scroll, self._table)
        filter_label = self._filter_label()
        if filter_label != "No Filter":
            self._status.setText(f"{len(df):,} of {len(self._df):,} tables shown with {filter_label}.")
        else:
            self._status.setText(f"{len(df):,} tables loaded. Click column headers to sort.")
        self._update_coverage_label()
        selection = self._table.selectionModel()
        if selection:
            selection.selectionChanged.connect(self._on_selection_changed)
        if self._model.rowCount() > 0:
            self._table.selectRow(0)
            self._show_row(self._model.row_at(0))

    def _choose_columns(self) -> None:
        if self._df.empty:
            return
        display_df, _sort_sources = _table_attribute_display_frame(self._df, table_review=True)
        dialog = QDialog(self)
        dialog.setWindowTitle("Table Review Columns")
        lay = QVBoxLayout(dialog)
        info = QLabel("Choose visible columns for Table Review. Sorting still uses the raw numeric values behind friendly labels.")
        info.setObjectName("Caption")
        info.setWordWrap(True)
        lay.addWidget(info)

        checks: dict[str, QCheckBox] = {}
        grid_host = QWidget()
        grid = QGridLayout(grid_host)
        grid.setContentsMargins(0, 0, 0, 0)
        columns = list(display_df.columns)
        for i, col in enumerate(columns):
            cb = QCheckBox(DISPLAY_COLUMN_LABELS.get(col, _column_header_label(col)))
            cb.setToolTip(col)
            cb.setChecked(col in self._visible_cols)
            checks[col] = cb
            grid.addWidget(cb, i // 3, i % 3)
        lay.addWidget(grid_host)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        lay.addWidget(buttons)
        if dialog.exec() == QDialog.Accepted:
            selected_set = {col for col, cb in checks.items() if cb.isChecked()}
            if selected_set:
                selected = [col for col in self._visible_cols if col in selected_set]
                selected.extend(col for col in columns if col in selected_set and col not in selected)
                self._visible_cols = _normalize_table_review_columns(selected)
                self._save_column_preferences()
                self._apply_columns()

    def _open_large_view(self) -> None:
        if self._df.empty:
            QMessageBox.information(self, "Table Review", "Load Table Review first.")
            return
        df = self._filtered_df()
        if df.empty:
            QMessageBox.information(self, "Table Review", f"No tables match {self._filter_label()}.")
            return
        if self._large_dialog is not None:
            self._large_dialog.close()

        dialog = QDialog(self)
        dialog.setWindowTitle("Table Review - Large View")
        dialog.setAttribute(Qt.WA_DeleteOnClose, True)
        root = QVBoxLayout(dialog)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(6)

        header = QHBoxLayout()
        title = QLabel(f"{len(df):,} of {len(self._df):,} tables")
        title.setObjectName("SectionHeader")
        header.addWidget(title)
        header.addStretch(1)
        root.addLayout(header)

        display_df, sort_sources = _table_attribute_display_frame(df, table_review=True)
        cols = [c for c in self._visible_cols if c in display_df.columns]
        if not cols:
            cols = [c for c in TABLE_REVIEW_COLS if c in display_df.columns] or list(display_df.columns)
        model = _DataFrameModel(display_df[cols], sort_sources=sort_sources, row_df=df)
        table = QTableView()
        _configure_table_view(table)
        table.horizontalHeader().setSectionsMovable(True)
        table.setModel(model)
        self._restore_header_to_model_order(table)
        table.horizontalHeader().sectionMoved.connect(
            lambda *_args, view=table, source_model=model: self._capture_column_order(
                view, source_model
            )
        )
        dialog._tooltips = _DelayedTableToolTips(table, delay_ms=2000)
        table.resizeColumnsToContents()
        table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        table.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        top_scroll = _add_external_horizontal_scrollbar(root, table)
        _sync_external_horizontal_scrollbar(top_scroll, table)
        root.addWidget(table, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.close)
        root.addWidget(buttons)
        dialog._model = model  # keep model alive while the dialog is open
        dialog.destroyed.connect(lambda *_args: setattr(self, "_large_dialog", None))
        _resize_dialog_to_screen(dialog, 0.97)
        self._large_dialog = dialog
        dialog.show()
        dialog.raise_()
        dialog.activateWindow()

    def _open_error_log(self) -> None:
        _open_load_error_log(self, self._load_errors)

    def _open_coverage_details(self) -> None:
        dialog = QDialog(self)
        dialog.setWindowTitle("DuckDB Coverage Details")
        root = QVBoxLayout(dialog)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)
        detail_text = self._coverage.text() or "Coverage status unavailable."
        if self._snapshot_id:
            detail_text = f"Snapshot: {self._snapshot_id}\n{detail_text}"
        label = QLabel(detail_text)
        label.setObjectName("Caption")
        label.setWordWrap(True)
        root.addWidget(label)
        table = QTableView()
        _configure_table_view(table)
        if not self._table_status.empty:
            cols = [c for c in TABLE_STATUS_COLS if c in self._table_status.columns]
            cols = cols or list(self._table_status.columns)
            model = _DataFrameModel(self._table_status[cols])
            table.setModel(model)
            table.resizeColumnsToContents()
            dialog._model = model
        root.addWidget(table, 1)
        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.close)
        root.addWidget(buttons)
        _resize_dialog_to_screen(dialog, 0.60)
        dialog.exec()

    def _update_coverage_label(self) -> None:
        self._update_load_error_link()
        if self._table_status.empty:
            if self._load_errors:
                self._coverage.setText("DuckDB load error: " + " | ".join(_clip(err, 220) for err in self._load_errors[:3]))
                self._coverage.setStyleSheet(f"color:{PALETTE.crit}; font-weight:600;")
            else:
                self._coverage.setText("Coverage status unavailable.")
                self._coverage.setStyleSheet("")
            return
        total = len(self._table_status)
        empty = int((self._table_status["coverage_status"] == "empty").sum())
        partial = int((self._table_status["coverage_status"] == "partial").sum())
        current = int((self._table_status["coverage_status"] == "current").sum())
        sql_changed = int((self._table_status["coverage_status"] == "sql_changed").sum())
        sql_not_recorded = int((self._table_status["coverage_status"] == "sql_not_recorded").sum())
        scan_row = self._table_status[self._table_status["table_name"] == "table_scan_info"]
        scan_status = str(scan_row.iloc[0].get("coverage_status")) if not scan_row.empty else "empty"
        scan_count = int(scan_row.iloc[0].get("record_count") or 0) if not scan_row.empty else 0
        pieces = [
            f"DuckDB coverage: {total - empty}/{total} analyzer tables populated",
            f"current={current}",
            f"empty={empty}",
            f"partial={partial}",
            f"table_scan_info={scan_status} ({scan_count:,} rows)",
        ]
        if sql_changed:
            pieces.append(f"sql_changed={sql_changed}")
        if sql_not_recorded:
            pieces.append(f"sql_not_recorded={sql_not_recorded}")
        if self._load_errors:
            pieces.append("load_errors=" + str(len(self._load_errors)))
            pieces.extend(_clip(err, 220) for err in self._load_errors[:2])
        if scan_status in {"empty", "partial"}:
            pieces.append("scan metrics are incomplete; use Refresh Source Data -> Refresh Empty Tables")
            self._coverage.setStyleSheet(f"color:{PALETTE.warn}; font-weight:600;")
        elif self._load_errors:
            self._coverage.setStyleSheet(f"color:{PALETTE.crit}; font-weight:600;")
        else:
            self._coverage.setStyleSheet("")
        self._coverage.setText(" | ".join(pieces))

    def _on_selection_changed(self, *_args) -> None:
        if not self._model:
            self._clear_cards()
            return
        indexes = self._table.selectionModel().selectedRows()
        if not indexes:
            return
        self._show_row(self._model.row_at(indexes[0].row()))

    def _show_row(self, row: pd.Series) -> None:
        table_name = f"{row.get('source_db', '')}.{row.get('schema_name', '')}.{row.get('table_name', '')}".strip(".")
        shown = self._model.rowCount() if self._model else len(self._filtered_df())
        prefix = f"{shown:,} of {len(self._df):,} tables shown" if shown != len(self._df) else f"{len(self._df):,} tables loaded"
        if self._intersection_filter_fallback:
            prefix += "; intersection telemetry returned zero matches, so all tables remain visible"
        self._status.setText(f"{prefix}. Selected: {table_name}" if table_name else f"{prefix}.")

    def _clear_cards(self) -> None:
        return

    def _filtered_df(self) -> pd.DataFrame:
        df = self._df.copy()
        self._intersection_filter_fallback = False
        if self._hide_without_intersection.isChecked():
            intersection = _table_review_intersection_mask(df)
            if intersection.any():
                df = df.loc[intersection].copy()
            elif not df.empty:
                # A telemetry load can legitimately be absent or fail while
                # the physical table inventory is healthy. Never turn a
                # successful load into a blank page; show the inventory and
                # tell the operator that the optional filter was bypassed.
                self._intersection_filter_fallback = True
        minimum = self._min_rows_value()
        if minimum > 0 and "tbl_rows" in df.columns:
            rows = pd.to_numeric(df["tbl_rows"], errors="coerce").fillna(0)
            df = df.loc[rows >= minimum].copy()
        source_values = self._source_db_filter.selected_values()
        if source_values and "source_db" in df.columns:
            df = df[df["source_db"].astype(str).str.strip().isin(source_values)].copy()
        schema_values = self._schema_filter.selected_values()
        if schema_values and "schema_name" in df.columns:
            df = df[df["schema_name"].astype(str).str.strip().isin(schema_values)].copy()
        table_text = self._table_name_filter.text().strip()
        if table_text and "table_name" in df.columns:
            df = df[
                df["table_name"].fillna("").astype(str).str.contains(
                    table_text, case=False, regex=False, na=False
                )
            ].copy()
        return df.reset_index(drop=True)

    def _refresh_filter_options(self) -> None:
        if self._df.empty:
            self._source_db_filter.set_options([])
            self._schema_filter.set_options([])
            return
        source_values = self._df["source_db"].tolist() if "source_db" in self._df.columns else []
        self._source_db_filter.set_options(source_values)
        self._refresh_schema_options()

    def _refresh_schema_options(self) -> None:
        """Schema choices depend on the Source DB selection: picking databases
        narrows the schema list to schemas that exist in those databases."""
        if self._df.empty or "schema_name" not in self._df.columns:
            self._schema_filter.set_options([])
            return
        df = self._df
        source_values = self._source_db_filter.selected_values()
        if source_values and "source_db" in df.columns:
            df = df[df["source_db"].astype(str).str.strip().isin(source_values)]
        self._schema_filter.set_options(df["schema_name"].tolist())

    def _on_source_db_filter_changed(self) -> None:
        self._refresh_schema_options()
        self._apply_columns()

    def _filter_label(self) -> str:
        parts: list[str] = []
        minimum = self._min_rows_value()
        if minimum > 0:
            parts.append(_min_rows_filter_label(minimum))
        source_values = self._source_db_filter.selected_values()
        if source_values:
            parts.append(f"source DB={len(source_values)} selected")
        schema_values = self._schema_filter.selected_values()
        if schema_values:
            parts.append(f"schema={len(schema_values)} selected")
        table_text = self._table_name_filter.text().strip()
        if table_text:
            parts.append(f'table name contains "{table_text}"')
        if self._hide_without_intersection.isChecked():
            if self._intersection_filter_fallback:
                parts.append("intersection telemetry unavailable - showing all tables")
            else:
                parts.append("workload intersection only")
        return ", ".join(parts) if parts else "No Filter"

    def _min_rows_value(self) -> int:
        value = self._min_rows_combo.currentData()
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def _update_load_error_link(self) -> None:
        if not self._load_errors:
            self._error_link.hide()
            self._error_link.setText("")
            return
        count = len(self._load_errors)
        label = "error" if count == 1 else "errors"
        self._error_link.setText(f"<span style='color:#FF5E6C;font-weight:600'>DuckDB load {label}: </span><a href='load-errors'>open log ({count})</a>")
        self._error_link.show()

    def _on_intersection_filter_changed(self, checked: bool) -> None:
        self._settings.table_review_hide_without_intersection = bool(checked)
        save_settings(self._settings)
        self._apply_columns()

    def _on_column_moved(self, *_args) -> None:
        if self._restoring_column_order or self._model is None:
            return
        self._capture_column_order(self._table, self._model)

    def _capture_column_order(self, table: QTableView, model: _DataFrameModel) -> None:
        header = table.horizontalHeader()
        ordered: list[str] = []
        for visual_index in range(header.count()):
            logical_index = header.logicalIndex(visual_index)
            column = model.column_name(logical_index)
            if column and column not in ordered:
                ordered.append(column)
        if not ordered:
            return
        hidden = [column for column in self._visible_cols if column not in ordered]
        self._visible_cols = _normalize_table_review_columns(ordered + hidden)
        self._save_column_preferences()

    def _restore_header_to_model_order(self, table: QTableView) -> None:
        header = table.horizontalHeader()
        self._restoring_column_order = True
        try:
            for target_visual in range(header.count()):
                current_visual = header.visualIndex(target_visual)
                if current_visual >= 0 and current_visual != target_visual:
                    header.moveSection(current_visual, target_visual)
        finally:
            self._restoring_column_order = False

    def _save_column_preferences(self) -> None:
        self._visible_cols = _normalize_table_review_columns(self._visible_cols)
        self._settings.table_review_visible_cols = list(self._visible_cols)
        save_settings(self._settings)


class _ReviewMetricCard(QFrame):
    def __init__(self, title: str):
        super().__init__()
        self.setObjectName("Card")
        self.setMaximumHeight(118)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(10, 8, 10, 8)
        lay.setSpacing(4)
        head = QLabel(title)
        head.setObjectName("SectionHeader")
        self._subject = QLabel("-")
        self._subject.setObjectName("Mono")
        self._subject.setWordWrap(True)
        self._body = QLabel("Select a table.")
        self._body.setObjectName("Caption")
        self._body.setWordWrap(True)
        lay.addWidget(head)
        lay.addWidget(self._subject)
        lay.addWidget(self._body, 1)

    def set_metrics(self, subject: str, rows: list[tuple[str, str]]) -> None:
        self._subject.setText(subject or "-")
        if not rows:
            self._body.setText("Select a table.")
            return
        self._body.setText("\n".join(f"{label}: {value}" for label, value in rows))


class _ActionQueuePage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        header = QHBoxLayout()
        title = QLabel("PRIORITIZED DBA ACTION QUEUE")
        title.setObjectName("SectionHeader")
        header.addWidget(title)
        header.addStretch(1)
        note = QLabel("Table blast radius + slow-query evidence + rewrite/maintenance next steps.")
        note.setObjectName("Caption")
        header.addWidget(note)
        root.addLayout(header)

        split = QSplitter(Qt.Horizontal)
        split.setChildrenCollapsible(False)
        self._cards = _ActionCards()
        self._table = _TablePage("ACTION LEDGER", ACTION_COLS)
        split.addWidget(self._cards)
        split.addWidget(self._table)
        split.setSizes([720, 780])
        root.addWidget(split, 1)

    def set_dataframe(self, df: pd.DataFrame) -> None:
        self._cards.set_dataframe(df)
        self._table.set_dataframe(df)


class _MultiSelectMenuButton(QPushButton):
    """Multi-select filter dropdown. Rows are QWidgetAction-embedded checkboxes
    so the menu STAYS OPEN while ticking several values; an All/None master
    checkbox sits at the top; `changed` is debounced so rapid ticking triggers
    one grid refilter instead of one per click."""

    changed = Signal()

    def __init__(self, label: str, parent=None):
        super().__init__(parent)
        self._label = str(label or "Filter")
        self._values: list[str] = []
        self._selected: set[str] = set()
        self._boxes: dict[str, QCheckBox] = {}
        self._all_box: QCheckBox | None = None
        self._menu = QMenu(self)
        self._menu.aboutToHide.connect(self._on_menu_hide)
        self.setObjectName("Ghost")
        self.setMenu(self._menu)
        self.setMinimumWidth(126)
        self.setText(f"{self._label}: All")
        self._emit_timer = QTimer(self)
        self._emit_timer.setSingleShot(True)
        self._emit_timer.setInterval(200)
        self._emit_timer.timeout.connect(self.changed.emit)

    def set_options(self, values: list[object] | tuple[object, ...]) -> None:
        # DuckDB nullable string columns arrive as pandas.NA.  Never use `or`
        # on those scalar values: their boolean value is intentionally
        # ambiguous and used to abort Table Review after the rows had loaded.
        clean = sorted(
            {
                text
                for value in values
                if (text := _clean_compact_text(value))
            }
        )
        previous = {value for value in self._selected if value in clean}
        self._values = clean
        self._selected = previous or set(clean)
        self.setEnabled(bool(clean))
        self._rebuild_menu()
        self._update_text()

    def selected_values(self) -> set[str]:
        if not self._values or not self._selected or len(self._selected) == len(self._values):
            return set()
        return set(self._selected)

    def _widget_action(self, box: QCheckBox) -> QWidgetAction:
        container = QWidget(self._menu)
        lay = QHBoxLayout(container)
        lay.setContentsMargins(10, 4, 10, 4)
        lay.addWidget(box)
        action = QWidgetAction(self._menu)
        action.setDefaultWidget(container)
        return action

    def _rebuild_menu(self) -> None:
        self._menu.clear()
        self._boxes = {}
        self._all_box = None
        if not self._values:
            return
        self._all_box = QCheckBox("All / None")
        self._all_box.setToolTip("Check to select every value; uncheck to clear them all.")
        self._all_box.clicked.connect(self._toggle_all)
        self._menu.addAction(self._widget_action(self._all_box))
        self._menu.addSeparator()
        for value in self._values:
            box = QCheckBox(value if len(value) <= 64 else value[:61] + "...")
            box.setChecked(value in self._selected)
            box.toggled.connect(lambda checked, item=value: self._toggle_value(item, checked))
            self._boxes[value] = box
            self._menu.addAction(self._widget_action(box))
        self._sync_all_box()

    def _toggle_all(self) -> None:
        select_all = len(self._selected) != len(self._values)
        self._selected = set(self._values) if select_all else set()
        for box in self._boxes.values():
            box.blockSignals(True)
            box.setChecked(select_all)
            box.blockSignals(False)
        self._sync_all_box()
        self._update_text()
        self._emit_timer.start()

    def _toggle_value(self, value: str, checked: bool) -> None:
        if checked:
            self._selected.add(value)
        else:
            self._selected.discard(value)
        self._sync_all_box()
        self._update_text()
        self._emit_timer.start()

    def _sync_all_box(self) -> None:
        if self._all_box is None:
            return
        self._all_box.blockSignals(True)
        self._all_box.setTristate(True)
        if not self._selected:
            self._all_box.setCheckState(Qt.Unchecked)
        elif len(self._selected) == len(self._values):
            self._all_box.setCheckState(Qt.Checked)
        else:
            self._all_box.setCheckState(Qt.PartiallyChecked)
        self._all_box.blockSignals(False)

    def _on_menu_hide(self) -> None:
        # Leaving the menu with nothing ticked means "no filter": restore All
        # so the empty grid never looks like lost data.
        if self._values and not self._selected:
            self._selected = set(self._values)
            for box in self._boxes.values():
                box.blockSignals(True)
                box.setChecked(True)
                box.blockSignals(False)
            self._sync_all_box()
            self._update_text()
            self._emit_timer.start()

    def _update_text(self) -> None:
        if not self._values:
            self.setText(f"{self._label}: -")
            return
        if not self._selected:
            self.setText(f"{self._label}: None")
            return
        if len(self._selected) == len(self._values):
            self.setText(f"{self._label}: All")
            return
        if len(self._selected) == 1:
            value = next(iter(self._selected))
            self.setText(f"{self._label}: {value if len(value) <= 24 else value[:21] + '...'}")
            return
        self.setText(f"{self._label}: {len(self._selected)} selected")


def _repeat_grouping_rules_text(settings) -> str:
    """Describe the active deterministic grouping rules from the real settings
    so the UI can never contradict the grouping engine."""
    scope = "per user" if getattr(settings, "repeat_scope_by_user", False) else "across users"
    min_size = max(2, int(getattr(settings, "repeat_min_group_size", 2) or 2))
    merge_pct = float(getattr(settings, "repeat_fuzzy_merge_threshold", 0.95) or 0.95)
    return (
        f"Repeats group by same query type + exact canonical fingerprint ({scope}), "
        f"minimum {min_size} runs; near-identical non-procedure shapes fuzzy-merge at "
        f">= {merge_pct:.0%} text similarity with an identical table set."
    )


class _RepeatQueryPage(QWidget):
    loadRequested = Signal(str)
    queryDiagramRequested = Signal(object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._settings = load_settings()
        self._display_groups = pd.DataFrame()
        self._display_members = pd.DataFrame()
        self._rotation_group_id = ""
        self._rotation_index = 0
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        header = QHBoxLayout()
        title = QLabel("REPEAT QUERY INTELLIGENCE")
        title.setObjectName("SectionHeader")
        header.addWidget(title)
        header.addStretch(1)
        note = QLabel(_repeat_grouping_rules_text(self._settings))
        note.setObjectName("Caption")
        header.addWidget(note)
        load_btn = QPushButton("Load Repeat Analysis")
        load_btn.setObjectName("Primary")
        header.addWidget(load_btn)
        diagram_btn = QPushButton("Diagram Selected")
        diagram_btn.setObjectName("Ghost")
        header.addWidget(diagram_btn)
        root.addLayout(header)

        self._summary = {
            "patterns": _MetricTile("PATTERNS"),
            "queries": _MetricTile("REPEATED RUNS"),
            "runtime": _MetricTile("REPEAT RUNTIME"),
            "priority": _MetricTile("TOP PRIORITY"),
        }
        summary_grid = QGridLayout()
        summary_grid.setContentsMargins(0, 0, 0, 0)
        summary_grid.setHorizontalSpacing(10)
        for i, tile in enumerate(self._summary.values()):
            summary_grid.addWidget(tile, 0, i)
        root.addLayout(summary_grid)

        threshold_card = QFrame()
        threshold_card.setObjectName("CardSubtle")
        threshold_lay = QGridLayout(threshold_card)
        threshold_lay.setContentsMargins(10, 8, 10, 8)
        threshold_lay.setHorizontalSpacing(12)
        threshold_lay.setVerticalSpacing(4)
        self._match_threshold = _SeveritySlider(
            "MATCH %",
            50,
            98,
            int(round(self._settings.repeat_similarity_threshold * 100)),
        )
        self._match_threshold.setToolTip(
            "Legacy display value. Repeat grouping now uses deterministic fingerprints."
        )
        self._prefilter_threshold = _SeveritySlider(
            "PREFILTER %",
            10,
            80,
            int(round(self._settings.repeat_prefilter_threshold * 100)),
        )
        self._prefilter_threshold.setToolTip(
            "Legacy display value. Repeat grouping now uses deterministic fingerprints."
        )
        self._threshold_status = QLabel("")
        self._threshold_status.setObjectName("Caption")
        self._threshold_status.setWordWrap(True)
        threshold_lay.addWidget(self._match_threshold, 0, 0)
        threshold_lay.addWidget(self._prefilter_threshold, 0, 1)
        threshold_lay.addWidget(self._threshold_status, 0, 2)
        threshold_lay.setColumnStretch(2, 1)
        root.addWidget(threshold_card)

        split = QSplitter(Qt.Horizontal)
        split.setChildrenCollapsible(False)
        self._cards = _RepeatGroupCards()
        ledgers = QSplitter(Qt.Vertical)
        ledgers.setChildrenCollapsible(False)
        self._tree = QTreeWidget()
        self._tree.setHeaderLabels(["Group / Query", "Runs", "Elapsed", "User", "Type", "Example / Procedure"])
        self._tree.setAlternatingRowColors(True)
        self._tree.setRootIsDecorated(True)
        self._tree.setUniformRowHeights(False)
        self._tree.setMinimumHeight(170)
        representative_box = QFrame()
        representative_box.setObjectName("CardSubtle")
        representative_lay = QVBoxLayout(representative_box)
        representative_lay.setContentsMargins(10, 8, 10, 8)
        representative_lay.setSpacing(6)
        representative_head = QHBoxLayout()
        self._representative_title = QLabel("REPRESENTATIVE SQL")
        self._representative_title.setObjectName("SectionHeader")
        representative_head.addWidget(self._representative_title)
        representative_head.addStretch(1)
        self._next_representative = QPushButton("Next Example")
        self._next_representative.setObjectName("Ghost")
        self._next_representative.setToolTip("Rotate through the query IDs retained in the selected repeat group.")
        representative_head.addWidget(self._next_representative)
        representative_lay.addLayout(representative_head)
        self._representative_sql = QPlainTextEdit()
        self._representative_sql.setReadOnly(True)
        self._representative_sql.setObjectName("Mono")
        self._representative_sql.setMinimumHeight(110)
        representative_lay.addWidget(self._representative_sql)
        representative_actions = QHBoxLayout()
        representative_actions.addStretch(1)
        format_representative = QPushButton("Format SQL")
        format_representative.setObjectName("Ghost")
        format_representative.clicked.connect(
            lambda: _apply_format_sql(self._representative_sql, self)
        )
        representative_actions.addWidget(format_representative)
        _add_sql_structure_buttons(
            representative_actions,
            self._representative_sql,
            self,
            pd.Series({"query_id": "repeat representative SQL"}),
            pd.DataFrame(),
            pd.DataFrame(),
        )
        representative_lay.addLayout(representative_actions)
        self._groups = _TablePage(
            "REPEAT GROUPS",
            REPEAT_GROUP_COLS,
            empty_message=(
                "No repeat groups are loaded. Click Load Repeat Analysis to parse local SQL shapes "
                "from the top slow-query rows."
            ),
        )
        self._members = _TablePage(
            "MATCHED QUERY MEMBERS",
            REPEAT_MEMBER_COLS,
            empty_message=(
                "No matched repeated queries are loaded. If Load Repeat Analysis returns zero rows, "
                "the local slow-query rows may be missing SQL text or matching patterns."
            ),
        )
        ledgers.addWidget(self._tree)
        ledgers.addWidget(representative_box)
        ledgers.addWidget(self._groups)
        ledgers.addWidget(self._members)
        ledgers.setSizes([210, 180, 300, 340])
        split.addWidget(self._cards)
        split.addWidget(ledgers)
        split.setSizes([620, 920])
        root.addWidget(split, 1)
        load_btn.clicked.connect(lambda: self.loadRequested.emit("repeat_queries"))
        diagram_btn.clicked.connect(self._diagram_selected_member)
        self._members.rowActivated.connect(self.queryDiagramRequested)
        self._tree.itemSelectionChanged.connect(self._on_tree_selection_changed)
        self._tree.itemDoubleClicked.connect(lambda *_args: self._show_next_representative())
        self._next_representative.clicked.connect(self._show_next_representative)
        self._match_threshold.on_change(self._save_repeat_thresholds)
        self._prefilter_threshold.on_change(self._save_repeat_thresholds)
        self._set_empty_summary()
        self._update_threshold_status("Saved thresholds")

    def set_dataframes(
        self,
        groups: pd.DataFrame,
        members: pd.DataFrame,
        summary: dict | None = None,
    ) -> None:
        display_groups = _repeat_groups_for_display(groups)
        display_members = _repeat_members_for_display(members)
        self._display_groups = display_groups.copy()
        self._display_members = display_members.copy()
        self._rotation_group_id = ""
        self._rotation_index = 0
        self._set_summary(display_groups, summary or {})
        self._cards.set_dataframe(display_groups)
        self._set_tree(display_groups, display_members)
        self._groups.set_dataframe(display_groups)
        self._members.set_dataframe(display_members)
        self._show_group_representative(display_groups.iloc[0].get("repeat_group_id") if not display_groups.empty else "")

    def _set_empty_summary(self) -> None:
        for tile in self._summary.values():
            tile.set_value("-")

    def _set_summary(self, groups: pd.DataFrame, summary: dict | None = None) -> None:
        summary = summary or {}
        diagnostic = _repeat_diagnostic_summary(summary)
        if groups is None or groups.empty:
            self._set_empty_summary()
            message = (
                "Loaded result: 0 deterministic repeat patterns. "
                + _repeat_grouping_rules_text(self._settings)
            )
            if diagnostic:
                message = f"{message}\n{diagnostic}"
            self._threshold_status.setText(message)
            return
        self._summary["patterns"].set_value(_fmt_int(len(groups)))
        self._summary["queries"].set_value(_fmt_int(groups.get("query_count", pd.Series(dtype="float64")).sum()))
        self._summary["runtime"].set_value(_fmt_seconds(groups.get("total_runtime_s", pd.Series(dtype="float64")).sum()))
        top = groups.iloc[0].get("repeat_priority_score") if "repeat_priority_score" in groups.columns else 0
        self._summary["priority"].set_value(_fmt_value("repeat_priority_score", top))
        message = "Loaded result used deterministic repeat grouping. " + _repeat_grouping_rules_text(
            self._settings
        )
        if diagnostic:
            message = f"{message}\n{diagnostic}"
        self._threshold_status.setText(message)

    def _set_tree(self, groups: pd.DataFrame, members: pd.DataFrame) -> None:
        self._tree.clear()
        if groups is None or groups.empty:
            return
        member_df = members.copy() if members is not None else pd.DataFrame()
        for _, group in groups.iterrows():
            group_id = str(group.get("repeat_group_id") or "-")
            proc = str(group.get("procedure_key") or "").strip()
            example = str(group.get("example_query_ids") or group.get("query_ids") or "").strip()
            detail = proc or example
            parent = QTreeWidgetItem(
                [
                    group_id,
                    _fmt_int(group.get("query_count")),
                    _fmt_seconds(group.get("total_runtime_s")),
                    str(group.get("users") or ""),
                    str(group.get("repeat_kind") or group.get("query_type") or ""),
                    detail,
                ]
            )
            parent.setData(0, Qt.UserRole, group_id)
            parent.setToolTip(5, str(group.get("repeat_match_basis") or ""))
            self._tree.addTopLevelItem(parent)
            if member_df.empty or "repeat_group_id" not in member_df.columns:
                continue
            group_members = member_df[member_df["repeat_group_id"].astype(str) == group_id].copy()
            if "member_rank" in group_members.columns:
                group_members = group_members.sort_values("member_rank", ascending=True)
            else:
                group_members = group_members.head(10)
            for _, member in group_members.head(10).iterrows():
                child = QTreeWidgetItem(
                    [
                        f"Query {member.get('query_id') or '-'}",
                        "",
                        _fmt_seconds(member.get("elapsed_s")),
                        str(member.get("user_name") or ""),
                        str(member.get("query_type") or ""),
                        _clip(str(member.get("sql_text") or ""), 180),
                    ]
                )
                child.setData(0, Qt.UserRole, group_id)
                child.setData(1, Qt.UserRole, str(member.get("query_id") or ""))
                child.setToolTip(5, str(member.get("sql_text") or ""))
                parent.addChild(child)
            hidden = max(0, len(group_members) - 10)
            if hidden:
                parent.addChild(QTreeWidgetItem([f"{hidden:,} more matching query ID(s) retained in member table", "", "", "", "", ""]))
        for col in range(self._tree.columnCount()):
            self._tree.resizeColumnToContents(col)
        if self._tree.topLevelItemCount():
            self._tree.setCurrentItem(self._tree.topLevelItem(0))

    def _on_tree_selection_changed(self) -> None:
        item = self._tree.currentItem()
        if item is None:
            return
        group_id = str(item.data(0, Qt.UserRole) or "").strip()
        if group_id:
            self._show_group_representative(group_id, reset=False)

    def _show_group_representative(self, group_id: object, *, reset: bool = True) -> None:
        group_id = str(group_id or "").strip()
        if not group_id:
            self._representative_title.setText("REPRESENTATIVE SQL")
            self._representative_sql.clear()
            return
        if reset or group_id != self._rotation_group_id:
            self._rotation_group_id = group_id
            self._rotation_index = 0
        group = self._group_row(group_id)
        if group is None:
            self._representative_sql.clear()
            return
        members = self._members_for_group(group_id)
        if str(group.get("repeat_kind") or "").lower() == "stored_procedure":
            title = str(group.get("procedure_key") or group_id)
            text = str(group.get("procedure_definition") or group.get("sample_sql") or "")
            self._representative_title.setText(f"STORED PROCEDURE BODY - {title}")
            self._representative_sql.setPlainText(text)
            return
        if members.empty:
            text = str(group.get("representative_sql") or group.get("sample_sql") or "")
            self._representative_title.setText(f"REPRESENTATIVE SQL - {group_id}")
            self._representative_sql.setPlainText(text)
            return
        self._rotation_index %= len(members)
        member = members.iloc[self._rotation_index]
        query_id = str(member.get("query_id") or "-")
        self._representative_title.setText(
            f"REPRESENTATIVE SQL - {group_id} / query {query_id} ({self._rotation_index + 1}/{len(members)})"
        )
        self._representative_sql.setPlainText(str(member.get("sql_text") or group.get("representative_sql") or ""))

    def _show_next_representative(self) -> None:
        group_id = self._rotation_group_id
        if not group_id:
            item = self._tree.currentItem()
            group_id = str(item.data(0, Qt.UserRole) or "").strip() if item is not None else ""
        if not group_id:
            return
        members = self._members_for_group(group_id)
        if members.empty:
            return
        self._rotation_index = (self._rotation_index + 1) % len(members)
        self._show_group_representative(group_id, reset=False)

    def _group_row(self, group_id: str) -> pd.Series | None:
        if self._display_groups.empty or "repeat_group_id" not in self._display_groups.columns:
            return None
        rows = self._display_groups[self._display_groups["repeat_group_id"].astype(str) == group_id]
        return rows.iloc[0] if not rows.empty else None

    def _members_for_group(self, group_id: str) -> pd.DataFrame:
        if self._display_members.empty or "repeat_group_id" not in self._display_members.columns:
            return pd.DataFrame()
        rows = self._display_members[self._display_members["repeat_group_id"].astype(str) == group_id].copy()
        if "member_rank" in rows.columns:
            rows["_rank"] = pd.to_numeric(rows["member_rank"], errors="coerce").fillna(999999)
            rows = rows.sort_values("_rank", ascending=True).drop(columns=["_rank"])
        return rows.reset_index(drop=True)

    def _save_repeat_thresholds(self, *_args) -> None:
        self._settings.repeat_similarity_threshold = self._match_threshold.value() / 100.0
        self._settings.repeat_prefilter_threshold = self._prefilter_threshold.value() / 100.0
        save_settings(self._settings)
        self._update_threshold_status("Saved thresholds")

    def _update_threshold_status(self, prefix: str) -> None:
        self._threshold_status.setText(
            f"{prefix}: deterministic grouping is active. "
            + _repeat_grouping_rules_text(self._settings)
            + " Legacy sliders no longer gate groups."
        )

    def _diagram_selected_member(self) -> None:
        row = self._members.selected_row()
        if row is None or row.empty:
            QMessageBox.information(self, "Repeat Query Diagram", "Select a matched query member first.")
            return
        self.queryDiagramRequested.emit(row)


class _XraySqlEdit(QPlainTextEdit):
    """SQL editor with click probes.

    Right-click a token that resolves to a captured table -> metadata popup
    (sortkey/sorted %, distkey/skew, stats staleness). Left-click an `=`, `!=`,
    or `<>` -> popup describing the tables on BOTH sides of the comparison.
    Tokens are cleaned of quotes/parentheses before matching."""

    sqlPasted = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._lookup: dict[str, dict] = {}
        self._view_map: dict[str, str] = {}
        self._explosion_highlight_text = ""
        self.textChanged.connect(self._clear_stale_explosion_highlights)

    def insertFromMimeData(self, source) -> None:
        super().insertFromMimeData(source)
        if source is not None and source.hasText():
            self.sqlPasted.emit()

    def set_probe_context(self, lookup: dict[str, dict], view_map: dict[str, str]) -> None:
        self._lookup = lookup or {}
        self._view_map = view_map or {}

    def set_explosion_highlights(self, spans: list[dict]) -> None:
        colors = ("#FFF59D", "#FFD180", "#FFB74D", "#FFA726", "#FB8C00", "#EF6C00")
        selections: list[QTextEdit.ExtraSelection] = []
        text_length = len(self.toPlainText())
        for span in sorted(spans or [], key=lambda item: (int(item.get("depth", 0)), int(item.get("start", 0)))):
            start = max(0, min(text_length, int(span.get("start", 0))))
            end = max(start, min(text_length, int(span.get("end", start))))
            if end <= start:
                continue
            depth = max(0, int(span.get("depth", 0)))
            cursor = QTextCursor(self.document())
            cursor.setPosition(start)
            cursor.setPosition(end, QTextCursor.KeepAnchor)
            fmt = QTextCharFormat()
            fmt.setBackground(QColor(colors[min(depth, len(colors) - 1)]))
            fmt.setForeground(QColor("#2B2118"))
            selection = QTextEdit.ExtraSelection()
            selection.cursor = cursor
            selection.format = fmt
            selections.append(selection)
        self.setExtraSelections(selections)
        self._explosion_highlight_text = self.toPlainText() if selections else ""

    def clear_explosion_highlights(self) -> None:
        self._explosion_highlight_text = ""
        self.setExtraSelections([])

    def _clear_stale_explosion_highlights(self) -> None:
        if self._explosion_highlight_text and self.toPlainText() != self._explosion_highlight_text:
            self.clear_explosion_highlights()

    def _offset_at(self, pos) -> int:
        return self.cursorForPosition(pos).position()

    def contextMenuEvent(self, event) -> None:
        if self._lookup or self._view_map:
            text = self.toPlainText()
            token = token_at(text, self._offset_at(event.pos()))
            if token:
                resolved = resolve_table(token, alias_map(text), self._lookup)
                if resolved is not None:
                    QToolTip.showText(event.globalPos(), table_popup_text(*resolved), self)
                    return
                if token in self._view_map or token.split(".")[-1] in self._view_map:
                    QToolTip.showText(
                        event.globalPos(),
                        f"{token}  -  VIEW\nUse Explode Views to inline its definition here.",
                        self,
                    )
                    return
        super().contextMenuEvent(event)

    def mousePressEvent(self, event) -> None:
        super().mousePressEvent(event)
        if event.button() != Qt.LeftButton or not self._lookup:
            return
        text = self.toPlainText()
        hit = comparison_at(text, self._offset_at(event.position().toPoint()))
        if hit is None:
            return
        left, right, op = hit
        QToolTip.showText(
            event.globalPosition().toPoint(),
            comparison_popup_text(left, right, op, text, self._lookup, self._view_map),
            self,
        )


class _SqlLensAnalyzeWorker(QObject):
    """Runs analyze_console_sql off the UI thread so a large statement (or a
    large captured-query context) never freezes the window."""

    finished = Signal(object)  # SQLLensAnalysis
    failed = Signal(str)

    def __init__(self, sql: str, table_review, known_queries, view_definitions):
        super().__init__()
        self._sql = sql
        self._table_review = table_review
        self._known_queries = known_queries
        self._view_definitions = view_definitions

    def run(self) -> None:
        try:
            analysis = analyze_console_sql(
                self._sql, self._table_review, self._known_queries, self._view_definitions
            )
        except Exception as exc:
            self.failed.emit(str(exc))
            return
        self.finished.emit(analysis)


class _SqlLensPage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._table_review = pd.DataFrame()
        self._known_queries = pd.DataFrame()
        self._view_definitions = pd.DataFrame()
        self._analysis: SQLLensAnalysis | None = None
        self._analysis_sql = ""
        self._analysis_requested_sql = ""
        self._post_analysis_action = ""
        self._analyze_thread: QThread | None = None
        self._analyze_worker: _SqlLensAnalyzeWorker | None = None
        self._analyze_pending = False
        # Deferred probe-context state; built lazily by _ensure_probe_context.
        self._xray_lookup: dict = {}
        self._view_map: dict = {}
        self._context_built = False

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        header = QHBoxLayout()
        title = QLabel("SQL LENS")
        title.setObjectName("SectionHeader")
        header.addWidget(title)
        header.addStretch(1)
        self._status = QLabel("Paste SQL from the AWS console.")
        self._status.setObjectName("Caption")
        header.addWidget(self._status)
        root.addLayout(header)

        paste_card = QFrame()
        paste_card.setObjectName("CardSubtle")
        paste_lay = QVBoxLayout(paste_card)
        paste_lay.setContentsMargins(12, 10, 12, 10)
        paste_lay.setSpacing(8)
        self._view_alert = QPushButton("")
        self._view_alert.setVisible(False)
        self._view_alert.setCursor(Qt.PointingHandCursor)
        self._view_alert.setToolTip(
            "One or more captured views were detected in the pasted SQL. "
            "Click to inline each view definition as a subquery."
        )
        self._view_alert.clicked.connect(self._explode_detected_views)
        self._view_alert_flash_on = False
        self._view_alert_timer = QTimer(self)
        self._view_alert_timer.setInterval(450)
        self._view_alert_timer.timeout.connect(self._flash_view_alert)
        paste_lay.addWidget(self._view_alert)
        self._sql = _XraySqlEdit()
        self._sql.setPlaceholderText("SELECT ...")
        self._sql.setMinimumHeight(126)
        self._sql.setToolTip(
            "Right-click a table name for sortkey/distkey/stats. "
            "Left-click an = (or != / <>) for both sides of the comparison."
        )
        paste_lay.addWidget(self._sql)
        self._original_sql: str | None = None
        self._xray_lookup: dict[str, dict] = {}
        self._view_map: dict[str, str] = {}
        actions = QHBoxLayout()
        analyze = QPushButton("Analyze SQL")
        analyze.setObjectName("Primary")
        self._analyze_btn = analyze
        clear = QPushButton("Clear")
        clear.setObjectName("Ghost")
        format_btn = QPushButton("Format SQL")
        format_btn.setObjectName("Ghost")
        format_btn.setToolTip("Format the statement; COPY/UNLOAD wrappers are handled by formatting the inner query.")
        lineage_btn = QPushButton("Show Lineage")
        lineage_btn.setObjectName("Ghost")
        lineage_btn.setToolTip("Open full SQL lineage with objects, views, base tables, joins, and table design context.")
        selection_btn = QPushButton("Selection Lineage")
        selection_btn.setObjectName("Ghost")
        selection_btn.setToolTip("Analyze only the selected SQL fragment for lineage, distribution, and sort-key fit.")
        subqueries_btn = QPushButton("Extract Subqueries")
        subqueries_btn.setObjectName("Ghost")
        subqueries_btn.setToolTip("Extract CTEs and subqueries, then analyze a selected block.")
        actions.addStretch(1)
        actions.addWidget(format_btn)
        actions.addWidget(lineage_btn)
        actions.addWidget(selection_btn)
        actions.addWidget(subqueries_btn)
        actions.addWidget(clear)
        actions.addWidget(analyze)
        paste_lay.addLayout(actions)
        xray_actions = QHBoxLayout()
        xray_hint = QLabel("X-RAY:")
        xray_hint.setObjectName("Caption")
        xray_actions.addWidget(xray_hint)
        explode_btn = QPushButton("Explode Views")
        explode_btn.setObjectName("Ghost")
        explode_btn.setToolTip(
            "Recursively inline every captured view as a parenthesized subquery so its "
            "tables, joins, filters, and functions are analyzed as one statement."
        )
        restore_btn = QPushButton("Restore SQL")
        restore_btn.setObjectName("Ghost")
        restore_btn.setToolTip("Return to the original statement as pasted, before any view explosion.")
        markers_btn = QPushButton("Slow Markers")
        markers_btn.setObjectName("Ghost")
        markers_btn.setToolTip(
            "Plain-English list of the markers that most likely make this query slow - "
            "cross joins, functions on sort keys, large tables joined off their dist key, "
            "stale stats - each with the evidence and a fix direction. Not an optimizer."
        )
        optimizer_btn = QPushButton("Fix This Query")
        optimizer_btn.setObjectName("Primary")
        optimizer_btn.setToolTip(
            "Find the safest useful improvement, explain it in plain language, and create a copy-ready Redshift query."
        )
        footprint_btn = QPushButton("Extract All Objects")
        footprint_btn.setObjectName("Ghost")
        footprint_btn.setToolTip(
            "Every table and view this statement touches - including tables reached "
            "only through nested views - with size, sortkey, and stats health."
        )
        fullscreen_btn = QPushButton("Full Screen")
        fullscreen_btn.setObjectName("Ghost")
        fullscreen_btn.setToolTip("Open the SQL editor maximized; your edits return to the Lens when you close it.")
        view_queries_btn = QPushButton("Show Queries with Views")
        view_queries_btn.setObjectName("Ghost")
        view_queries_btn.setToolTip(
            "List captured queries that directly reference one or more captured views, then insert one into SQL Lens."
        )
        for btn in (
            optimizer_btn,
            explode_btn,
            restore_btn,
            markers_btn,
            footprint_btn,
            view_queries_btn,
            fullscreen_btn,
        ):
            xray_actions.addWidget(btn)
        xray_actions.addStretch(1)
        paste_lay.addLayout(xray_actions)
        explode_btn.clicked.connect(self._explode_views)
        optimizer_btn.clicked.connect(self._open_optimizer)
        restore_btn.clicked.connect(self._restore_sql)
        markers_btn.clicked.connect(self._open_slow_markers)
        footprint_btn.clicked.connect(self._open_footprint)
        view_queries_btn.clicked.connect(self._open_queries_with_views)
        fullscreen_btn.clicked.connect(self._open_fullscreen)
        root.addWidget(paste_card)

        self._summary = {
            "parse": _MetricTile("PARSE"),
            "tables": _MetricTile("TABLES"),
            "repeat": _MetricTile("REPEAT"),
            "first": _MetricTile("FIRST STEP"),
        }
        summary_grid = QGridLayout()
        summary_grid.setContentsMargins(0, 0, 0, 0)
        summary_grid.setHorizontalSpacing(10)
        for i, tile in enumerate(self._summary.values()):
            summary_grid.addWidget(tile, 0, i)
        root.addLayout(summary_grid)

        split = QSplitter(Qt.Vertical)
        split.setChildrenCollapsible(False)
        self._flow = _SqlLensFlowCanvas()
        split.addWidget(self._flow)

        lower = QSplitter(Qt.Horizontal)
        lower.setChildrenCollapsible(False)
        self._steps = _FirstStepCards()
        tabs = QTabWidget()
        self._tables = _TablePage("REFERENCED TABLES AND VIEW COMPONENTS", SQL_LENS_TABLE_COLS)
        self._joins = _TablePage("JOINS", SQL_LENS_JOIN_COLS)
        self._predicates = _TablePage("PREDICATES", SQL_LENS_PREDICATE_COLS)
        self._repeats = _TablePage("SIMILAR CAPTURED QUERIES", SQL_LENS_REPEAT_COLS)
        self._findings = _TablePage("FINDINGS", SQL_LENS_STEP_COLS)
        tabs.addTab(self._tables, "Tables")
        tabs.addTab(self._joins, "Joins")
        tabs.addTab(self._predicates, "Predicates")
        tabs.addTab(self._repeats, "Repeat Matches")
        tabs.addTab(self._findings, "First Steps")
        _set_tab_tooltips(tabs)
        analysis_legend = QLabel(
            f"<span style='color:{PALETTE.crit}; font-weight:700;'>RED</span> problem join/filter"
            f" &nbsp; <span style='color:{PALETTE.ok}; font-weight:700;'>GREEN</span> merge candidate or sort-key aligned"
            f" &nbsp; <span style='color:{PALETTE.warn}; font-weight:700;'>AMBER</span> hash candidate or review needed"
            " &nbsp; Join algorithms are inferred unless a captured plan identifies one."
        )
        analysis_legend.setObjectName("Caption")
        analysis_legend.setTextFormat(Qt.RichText)
        analysis_legend.setWordWrap(True)
        lower.addWidget(self._steps)
        result_host = QWidget()
        result_lay = QVBoxLayout(result_host)
        result_lay.setContentsMargins(0, 0, 0, 0)
        result_lay.setSpacing(4)
        result_lay.addWidget(analysis_legend)
        result_lay.addWidget(tabs, 1)
        lower.addWidget(result_host)
        lower.setSizes([460, 980])
        split.addWidget(lower)
        split.setSizes([420, 520])
        root.addWidget(split, 1)

        analyze.clicked.connect(self._analyze)
        clear.clicked.connect(self._clear)
        format_btn.clicked.connect(self._format_sql)
        lineage_btn.clicked.connect(self._open_lineage)
        selection_btn.clicked.connect(self._open_selection_lineage)
        subqueries_btn.clicked.connect(self._open_subquery_extractor)
        self._sql.sqlPasted.connect(self._on_sql_pasted)
        self._set_empty_summary()

    # ------------------------------------------------- paste-time view alert

    def _on_sql_pasted(self) -> None:
        """Pasting analyzes the original SQL; view explosion remains explicit."""
        sql = self._sql.toPlainText()
        if not sql.strip():
            self._hide_view_alert()
            return
        self._original_sql = None
        self._sql.clear_explosion_highlights()
        self._update_view_alert(sql)
        self._analyze()

    def _update_view_alert(self, sql: str) -> None:
        detected: list[str] = []
        if self._view_map and sql.strip():
            _, detected = explode_views(sql, self._view_map)
        if not detected:
            self._hide_view_alert()
            return
        unique = list(dict.fromkeys(detected))
        shown = ", ".join(unique[:4]) + (" ..." if len(unique) > 4 else "")
        self._view_alert.setText(
            f"VIEW DETECTED ({len(unique)}): {shown}  -  click to explode"
        )
        self._view_alert_flash_on = True
        self._flash_view_alert()
        self._view_alert.setVisible(True)
        self._view_alert_timer.start()

    def _flash_view_alert(self) -> None:
        self._view_alert_flash_on = not self._view_alert_flash_on
        bright = (
            "QPushButton { background-color: #D64545; color: #FFFFFF; font-weight: 700; "
            "border: 1px solid #E88; border-radius: 4px; padding: 6px 10px; }"
        )
        dim = (
            "QPushButton { background-color: #6E1F1F; color: #FFD9D9; font-weight: 700; "
            "border: 1px solid #A44; border-radius: 4px; padding: 6px 10px; }"
        )
        self._view_alert.setStyleSheet(bright if self._view_alert_flash_on else dim)

    def _hide_view_alert(self) -> None:
        self._view_alert_timer.stop()
        self._view_alert.setVisible(False)

    def _explode_detected_views(self) -> None:
        self._explode_views()
        if self._sql.toPlainText().strip():
            self._analyze()
        self._update_view_alert(self._sql.toPlainText())

    def set_context(
        self,
        table_review: pd.DataFrame,
        known_queries: pd.DataFrame,
        view_definitions: pd.DataFrame,
    ) -> None:
        self._table_review = table_review if table_review is not None else pd.DataFrame()
        self._known_queries = known_queries if known_queries is not None else pd.DataFrame()
        self._view_definitions = view_definitions if view_definitions is not None else pd.DataFrame()
        # Defer the expensive lookup/view-map builds. On a large multi-cluster
        # capture, building them here froze the "Rendering ..." step for
        # minutes; they are only needed once the user actually analyzes SQL.
        self._xray_lookup = {}
        self._view_map = {}
        self._context_built = False
        self._sql.set_probe_context(self._xray_lookup, self._view_map)
        if self._sql.toPlainText().strip():
            self._ensure_probe_context()
            self._update_view_alert(self._sql.toPlainText())
            self._analyze()
        else:
            self._status.setText(
                f"Context loaded: {len(self._table_review):,} table row(s), "
                f"{len(self._known_queries):,} known query row(s), "
                f"{len(self._view_definitions):,} view definition(s)."
            )

    def has_context(self) -> bool:
        return not self._table_review.empty or not self._known_queries.empty or not self._view_definitions.empty

    def sql_text(self) -> str:
        return self._sql.toPlainText().strip()

    # ------------------------------------------------------------- x-ray

    def _auto_expand_views(self, editor: QPlainTextEdit | None = None) -> list[str]:
        editor = editor or self._sql
        sql = editor.toPlainText()
        if not sql.strip():
            return []
        # The lookup/view maps are deferred out of set_context(); build them
        # on first use so deferral never reads as "no views loaded".
        self._ensure_probe_context()
        if not self._view_map:
            return []
        expanded_sql, exploded, spans = explode_views_recursive_with_spans(sql, self._view_map)
        if not exploded or expanded_sql == sql:
            self._hide_view_alert()
            return []
        if editor is self._sql and self._original_sql is None:
            self._original_sql = sql
        editor.setPlainText(expanded_sql)
        if isinstance(editor, _XraySqlEdit):
            editor.set_explosion_highlights(spans)
        shown = ", ".join(exploded[:6]) + (" ..." if len(exploded) > 6 else "")
        self._status.setText(
            f"Automatically expanded {len(exploded)} captured view(s) inside parentheses: {shown}. "
            "The expanded statement is now analyzed as one query; Restore SQL returns to the paste."
        )
        self._hide_view_alert()
        return exploded

    def _explode_views(self, editor: QPlainTextEdit | None = None) -> None:
        editor = editor or self._sql
        sql = editor.toPlainText()
        if not sql.strip():
            QMessageBox.information(self, "Explode Views", "Paste SQL first.")
            return
        self._ensure_probe_context()
        if not self._view_map:
            QMessageBox.information(
                self,
                "Explode Views",
                "No view definitions are loaded. Load a DuckDB snapshot that captured view definitions.",
            )
            return
        new_sql, exploded, spans = explode_views_recursive_with_spans(sql, self._view_map)
        if not exploded:
            QMessageBox.information(
                self, "Explode Views", "No captured views were found in this statement's FROM/JOIN clauses."
            )
            return
        if self._original_sql is None:
            self._original_sql = sql
        editor.setPlainText(new_sql)
        if isinstance(editor, _XraySqlEdit):
            editor.set_explosion_highlights(spans)
        shown = ", ".join(exploded[:6]) + (" ..." if len(exploded) > 6 else "")
        self._status.setText(
            f"Recursively expanded {len(exploded)} captured view(s): {shown}. "
            "The result is analyzed as one statement; Restore SQL brings back the original."
        )
        if editor is self._sql:
            self._update_view_alert(new_sql)

    def _restore_sql(self, editor: QPlainTextEdit | None = None) -> None:
        editor = editor or self._sql
        if self._original_sql is None:
            QMessageBox.information(self, "Restore SQL", "Nothing to restore - no view has been exploded.")
            return
        editor.setPlainText(self._original_sql)
        if isinstance(editor, _XraySqlEdit):
            editor.clear_explosion_highlights()
        self._original_sql = None
        self._status.setText("Original SQL restored.")

    def _open_slow_markers(self, sql_text: str | None = None) -> None:
        from ..slow_markers import detect_markers, markers_summary

        sql = (sql_text if isinstance(sql_text, str) else "") or self._sql.toPlainText()
        if not sql.strip():
            QMessageBox.information(self, "Slow Markers", "Paste SQL first.")
            return
        markers = detect_markers(sql, self._table_review, self._view_definitions)
        dialog = QDialog(self)
        dialog.setWindowTitle("Slow-Query Markers - what most likely makes this query slow")
        root = QVBoxLayout(dialog)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(10)
        summary = QLabel(markers_summary(markers))
        sfont = summary.font()
        sfont.setBold(True)
        sfont.setPointSizeF(sfont.pointSizeF() + 1.5)
        summary.setFont(sfont)
        root.addWidget(summary)
        disclaimer = QLabel(
            "These are structural markers that correlate with slowness, each proven present in "
            "your SQL and captured table stats. This is not an optimizer and does not rewrite your "
            "query - a DBA reviews each item and decides."
        )
        disclaimer.setObjectName("Caption")
        disclaimer.setWordWrap(True)
        root.addWidget(disclaimer)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        body = QWidget()
        body_lay = QVBoxLayout(body)
        body_lay.setContentsMargins(0, 0, 0, 0)
        body_lay.setSpacing(8)
        severity_color = {"crit": PALETTE.crit, "warn": PALETTE.warn, "info": PALETTE.accent}
        severity_word = {"crit": "CRITICAL", "warn": "WARNING", "info": "NOTE"}
        if not markers:
            ok = QLabel("No slow-query markers detected. The structural signals we check for are absent.")
            ok.setWordWrap(True)
            body_lay.addWidget(ok)
        for marker in markers:
            card = QFrame()
            card.setObjectName("Card")
            cl = QVBoxLayout(card)
            cl.setContentsMargins(12, 10, 12, 10)
            cl.setSpacing(4)
            color = severity_color.get(marker.severity, PALETTE.accent)
            head = QLabel(f"{severity_word.get(marker.severity, 'NOTE')}  -  {marker.title}")
            head.setStyleSheet(f"color:{color}; font-weight:800;")
            head.setWordWrap(True)
            cl.addWidget(head)
            detail = QLabel()
            apply_markdown(detail, marker.detail)
            cl.addWidget(detail)
            fix = QLabel()
            fix.setObjectName("Caption")
            apply_markdown(fix, f"→ {marker.fix}")
            fix.setStyleSheet(f"color:{PALETTE.ok};")
            cl.addWidget(fix)
            body_lay.addWidget(card)
        body_lay.addStretch(1)
        scroll.setWidget(body)
        root.addWidget(scroll, 1)
        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.close)
        root.addWidget(buttons)
        dialog.resize(760, 620)
        dialog.exec()

    def _open_optimizer(self, sql_text: str | None = None) -> None:
        sql = (sql_text if isinstance(sql_text, str) else "") or self._sql.toPlainText()
        if not sql.strip():
            QMessageBox.information(self, "Query Fixer", "Paste a query first, then click Fix This Query.")
            return
        try:
            if self._analysis is None or self._analysis_sql.strip() != sql.strip():
                analysis = analyze_console_sql(
                    sql,
                    self._table_review,
                    self._known_queries,
                    self._view_definitions,
                )
            else:
                analysis = self._analysis
            result = optimize_redshift_sql(
                sql,
                self._table_review,
                self._known_queries,
                self._view_definitions,
                analysis=analysis,
            )
        except Exception as exc:
            QMessageBox.warning(
                self,
                "Query Fixer could not read this query",
                f"The query was not changed. Technical message:\n{exc}",
            )
            return
        if not result.parse_ok:
            QMessageBox.warning(
                self,
                "Query Fixer could not read this query",
                f"Check for a missing comma, quote, or parenthesis, then try again.\n\nTechnical message:\n{result.parse_error}",
            )
            return

        friendly = build_friendly_fix(result)

        dialog = QDialog(self)
        dialog.setWindowTitle("Query Fixer - recommended Redshift improvement")
        root = QVBoxLayout(dialog)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(10)

        tabs = QTabWidget()
        tabs.setAccessibleName("Query Fixer views")
        tabs.setStyleSheet("QTabBar::tab { font-size:13px; padding:10px 16px; }")
        recommended_page = QWidget()
        recommended_layout = QVBoxLayout(recommended_page)
        recommended_layout.setContentsMargins(16, 18, 16, 16)
        recommended_layout.setSpacing(12)

        hero = QFrame()
        hero.setObjectName("QueryFixHero")
        hero.setStyleSheet(
            f"QFrame#QueryFixHero {{ background:{PALETTE.bg_2}; border:1px solid {PALETTE.border_strong}; "
            "border-radius:14px; }"
        )
        hero_layout = QVBoxLayout(hero)
        hero_layout.setContentsMargins(22, 20, 22, 22)
        hero_layout.setSpacing(12)

        status_color = PALETTE.ok if friendly.status.startswith("ready") else PALETTE.warn
        status_text = "FIX READY" if friendly.status.startswith("ready") else "REVIEW NEEDED"
        status_badge = QLabel(status_text)
        status_badge.setAccessibleName(f"Query status: {status_text.lower()}")
        status_badge.setStyleSheet(
            f"color:{status_color}; font-size:12px; font-weight:800; letter-spacing:1px;"
        )
        hero_layout.addWidget(status_badge)

        headline = QLabel(friendly.headline)
        headline.setAccessibleName("Query Fixer result")
        headline.setStyleSheet(f"color:{PALETTE.text_0}; font-size:22px; font-weight:750;")
        headline.setWordWrap(True)
        hero_layout.addWidget(headline)

        explanation = QLabel(friendly.explanation)
        explanation.setAccessibleName("Summary of the recommended fix")
        explanation.setStyleSheet(f"color:{PALETTE.text_1}; font-size:14px;")
        explanation.setWordWrap(True)
        hero_layout.addWidget(explanation)

        initial_copy_label = "Copy the Complete Fix" if friendly.status.startswith("ready") else "No Automatic Fix Available"
        copy_btn = QPushButton(initial_copy_label)
        copy_btn.setObjectName("Primary")
        copy_btn.setMinimumHeight(48)
        copy_btn.setMinimumWidth(260)
        copy_btn.setStyleSheet("font-size:15px; font-weight:750; padding:12px 22px;")
        copy_btn.setEnabled(friendly.status.startswith("ready"))
        copy_btn.setAccessibleName(initial_copy_label)
        copy_btn.setAccessibleDescription("Copies the entire recommended query fix to the clipboard.")
        hero_layout.addWidget(copy_btn, 0, Qt.AlignLeft)

        quick_instruction_text = (
            "Then paste the entire copied block into one Redshift query window and run it together."
            if friendly.is_multistep
            else "Then paste it into Redshift and run it the same way as the original query."
        )
        if not friendly.status.startswith("ready"):
            quick_instruction_text = "The original query has been left unchanged because a safe automatic fix was not available."
        quick_instruction = QLabel(quick_instruction_text)
        quick_instruction.setObjectName("Caption")
        quick_instruction.setStyleSheet(f"color:{PALETTE.text_2}; font-size:12px;")
        quick_instruction.setWordWrap(True)
        hero_layout.addWidget(quick_instruction)
        recommended_layout.addWidget(hero)

        candidate = QPlainTextEdit()
        candidate.setObjectName("Mono")
        candidate.setReadOnly(True)
        candidate.setPlainText(friendly.sql or sql)
        candidate.setMinimumHeight(260)
        candidate.setStyleSheet("font-size:13px; padding:14px;")
        candidate.setAccessibleName("Generated SQL preview")

        disclosure_toggles: list[QToolButton] = []

        def add_disclosure(title: str, content: QWidget) -> QToolButton:
            section = QFrame()
            section.setObjectName("CardSubtle")
            section_layout = QVBoxLayout(section)
            section_layout.setContentsMargins(14, 8, 14, 12)
            section_layout.setSpacing(8)
            toggle = QToolButton()
            toggle.setText(title)
            toggle.setCheckable(True)
            toggle.setChecked(False)
            toggle.setArrowType(Qt.RightArrow)
            toggle.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
            toggle.setAccessibleName(f"Show {title.lower()}")
            toggle.setStyleSheet(
                f"QToolButton {{ color:{PALETTE.text_0}; border:none; font-size:14px; "
                "font-weight:600; padding:6px 2px; text-align:left; }"
            )
            content.setVisible(False)

            def set_open(opened: bool, *, button=toggle, body=content) -> None:
                if opened:
                    for other in disclosure_toggles:
                        if other is not button and other.isChecked():
                            other.setChecked(False)
                button.setArrowType(Qt.DownArrow if opened else Qt.RightArrow)
                button.setAccessibleName(f"{'Hide' if opened else 'Show'} {title.lower()}")
                body.setVisible(opened)

            toggle.toggled.connect(set_open)
            section_layout.addWidget(toggle)
            section_layout.addWidget(content)
            recommended_layout.addWidget(section)
            disclosure_toggles.append(toggle)
            return toggle

        if friendly.why_it_helps:
            why = QLabel("\n".join(f"• {item}" for item in friendly.why_it_helps))
            why.setStyleSheet(f"color:{PALETTE.text_1}; font-size:14px; padding:2px 8px 8px 22px;")
            why.setWordWrap(True)
            why.setTextInteractionFlags(Qt.TextSelectableByMouse)
            add_disclosure("Why this should help", why)

        steps = QLabel("\n".join(f"{index}. {item}" for index, item in enumerate(friendly.next_steps, start=1)))
        steps.setStyleSheet(f"color:{PALETTE.text_1}; font-size:14px; padding:2px 8px 8px 22px;")
        steps.setWordWrap(True)
        steps.setTextInteractionFlags(Qt.TextSelectableByMouse)
        add_disclosure("How to use this fix safely", steps)

        if friendly.things_to_check:
            checks = QLabel("\n".join(f"• {item}" for item in friendly.things_to_check))
            checks.setStyleSheet(f"color:{PALETTE.text_1}; font-size:14px; padding:2px 8px 8px 22px;")
            checks.setWordWrap(True)
            add_disclosure("One-time checks", checks)

        add_disclosure("Preview the generated SQL", candidate)
        recommended_layout.addStretch(1)
        tabs.addTab(recommended_page, "Recommended Fix")

        technical_page = QWidget()
        technical_layout = QVBoxLayout(technical_page)
        technical_layout.setContentsMargins(12, 14, 12, 12)
        technical_layout.setSpacing(8)
        technical_intro = QLabel(
            "Optional details for a database specialist. You do not need this tab to copy the recommended fix."
        )
        technical_intro.setObjectName("Caption")
        technical_intro.setWordWrap(True)
        technical_layout.addWidget(technical_intro)

        candidate_entries: list[tuple[str, str, str]] = [
            (
                friendly.option_label,
                friendly.sql or sql,
                "multistep" if friendly.is_multistep else "single" if friendly.status == "ready_single" else "original",
            )
        ]
        if result.changed and result.rewritten_sql.strip() != (friendly.sql or "").strip():
            candidate_entries.append(("Alternative: simpler one-query rewrite", result.rewritten_sql, "single"))
        for plan in sorted(result.decompositions, key=lambda item: item.score, reverse=True):
            if plan.script.strip() == (friendly.sql or "").strip():
                continue
            candidate_entries.append((f"Alternative: {plan.title}", plan.script, "multistep"))

        candidate_picker = QComboBox()
        for label, candidate_sql, _kind in candidate_entries:
            candidate_picker.addItem(label, candidate_sql)
        candidate_picker.setToolTip("The first option is the fix recommended for this query.")
        technical_layout.addWidget(candidate_picker)
        report = QTextBrowser()
        report.setObjectName("Mono")
        report.setPlainText(optimization_report_text(result))
        technical_layout.addWidget(report, 1)
        tabs.addTab(technical_page, "Technical Details")
        root.addWidget(tabs, 1)

        actions = QHBoxLayout()

        def copy_candidate() -> None:
            QApplication.clipboard().setText(candidate.toPlainText())
            copy_btn.setText("Copied - Ready to Paste")
            quick_instruction.setText("Copied successfully. Paste the entire block into Redshift; nothing was run automatically.")

        copy_btn.clicked.connect(copy_candidate)
        actions.addStretch(1)
        use_btn = QPushButton("Put Fix in Editor")
        use_btn.setObjectName("Ghost")
        use_btn.setEnabled(friendly.can_apply_in_editor)
        use_btn.setVisible(friendly.can_apply_in_editor)
        use_btn.setAccessibleDescription("Places a one-statement fix back into SQL Lens without running it.")
        if friendly.is_multistep:
            use_btn.setToolTip("Multi-step fixes must be copied and run together in one Redshift query window.")

        def select_candidate(_index: int) -> None:
            candidate.setPlainText(str(candidate_picker.currentData() or sql))
            selected_kind = candidate_entries[candidate_picker.currentIndex()][2]
            can_use = selected_kind == "single" and result.changed
            use_btn.setEnabled(can_use)
            use_btn.setVisible(can_use)
            copy_btn.setEnabled(selected_kind in {"single", "multistep"})
            copy_btn.setText(initial_copy_label if candidate_picker.currentIndex() == 0 else "Copy Selected Version")
            quick_instruction.setText(
                "Then paste the entire copied block into one Redshift query window and run it together."
                if selected_kind == "multistep"
                else "Then paste it into Redshift and run it the same way as the original query."
            )

        candidate_picker.currentIndexChanged.connect(select_candidate)

        def use_candidate() -> None:
            if self._original_sql is None:
                self._original_sql = self._sql.toPlainText()
            self._sql.setPlainText(candidate.toPlainText())
            dialog.accept()
            self._analyze()
            self._status.setText(
                "The recommended fix is now in the editor. The original query is still available through Restore SQL."
            )

        use_btn.clicked.connect(use_candidate)
        actions.addWidget(use_btn)
        close_btn = QPushButton("Close")
        close_btn.setObjectName("Ghost")
        close_btn.clicked.connect(dialog.reject)
        actions.addWidget(close_btn)
        root.addLayout(actions)
        dialog.resize(1040, 820)
        dialog.exec()

    def _open_footprint(self, sql_text: str | None = None) -> None:
        sql = (sql_text if isinstance(sql_text, str) else "") or self._sql.toPlainText()
        if not sql.strip():
            QMessageBox.information(self, "Extract All Objects", "Paste SQL first.")
            return
        rows = resolve_footprint(sql, self._view_map, self._xray_lookup)
        if not rows:
            QMessageBox.information(self, "Extract All Objects", "No table or view references were found.")
            return
        dialog = QDialog(self)
        dialog.setWindowTitle("True Object Footprint - every table this statement touches")
        root = QVBoxLayout(dialog)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)
        views = sum(1 for row in rows if row["kind"] == "view")
        tables = sum(1 for row in rows if row["kind"] == "table")
        unknown = sum(1 for row in rows if row["kind"] == "unknown")
        summary = QLabel(
            f"{len(rows)} reference(s): {tables} captured table(s), {views} view(s), "
            f"{unknown} without captured metadata. 'Via' shows the view chain that reaches the object."
        )
        summary.setObjectName("Caption")
        summary.setWordWrap(True)
        root.addWidget(summary)
        headers = [
            "Object", "Type", "Via", "Depth", "Rows", "Size GB",
            "Diststyle", "Sortkey", "Sorted %", "Stats Stale %", "Skew",
        ]
        grid = QTableWidget(len(rows), len(headers))
        grid.setHorizontalHeaderLabels(headers)
        grid.verticalHeader().setVisible(False)
        grid.setEditTriggers(QAbstractItemView.NoEditTriggers)
        grid.setSelectionBehavior(QAbstractItemView.SelectRows)
        grid.setAlternatingRowColors(True)
        grid.setWordWrap(False)

        def _num_item(value, fmt="{:,.1f}"):
            item = QTableWidgetItem()
            if value is None:
                item.setText("-")
                item.setData(Qt.EditRole, -1.0)
            else:
                number = float(value)
                item.setData(Qt.EditRole, number)
                item.setText(fmt.format(number))
            return item

        for r, row in enumerate(rows):
            kind = str(row.get("kind") or "").upper()
            object_item = QTableWidgetItem(str(row.get("object") or ""))
            kind_item = QTableWidgetItem(kind)
            if kind == "VIEW":
                kind_item.setForeground(QColor(PALETTE.warn))
            elif kind == "UNKNOWN":
                kind_item.setForeground(QColor(PALETTE.text_3))
            grid.setItem(r, 0, object_item)
            grid.setItem(r, 1, kind_item)
            grid.setItem(r, 2, QTableWidgetItem(str(row.get("via") or "-")))
            grid.setItem(r, 3, _num_item(row.get("depth"), "{:,.0f}"))
            grid.setItem(r, 4, _num_item(row.get("tbl_rows"), "{:,.0f}"))
            size_mb = row.get("size_mb")
            grid.setItem(r, 5, _num_item(None if size_mb is None else size_mb / 1024.0))
            grid.setItem(r, 6, QTableWidgetItem(str(row.get("diststyle") or "-")))
            grid.setItem(r, 7, QTableWidgetItem(str(row.get("sortkey1") or "-")))
            unsorted = row.get("unsorted")
            grid.setItem(r, 8, _num_item(None if unsorted is None else max(0.0, 100.0 - float(unsorted)), "{:,.0f}"))
            grid.setItem(r, 9, _num_item(row.get("stats_off"), "{:,.0f}"))
            grid.setItem(r, 10, _num_item(row.get("skew_rows")))
        grid.setSortingEnabled(True)
        grid.resizeColumnsToContents()
        root.addWidget(grid, 1)
        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.close)
        root.addWidget(buttons)
        dialog.resize(1150, 560)
        dialog.exec()

    def _open_queries_with_views(self) -> None:
        rows = _queries_with_views_frame(self._known_queries, self._view_map)
        if rows.empty:
            QMessageBox.information(
                self,
                "Queries with Views",
                "No loaded query references a captured view. Load SQL Lens context with both slow queries and View Definitions, then try again.",
            )
            return
        dialog = QDialog(self)
        dialog.setWindowTitle("Show Queries with Views")
        root = QVBoxLayout(dialog)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)
        summary = QLabel(
            f"{len(rows):,} captured quer{'y' if len(rows) == 1 else 'ies'} reference captured views. "
            "Select one and click Insert into Analyzer."
        )
        summary.setWordWrap(True)
        summary.setObjectName("Caption")
        root.addWidget(summary)
        grid = QTableView()
        _configure_table_view(grid)
        grid.setSelectionBehavior(QAbstractItemView.SelectRows)
        grid.setSelectionMode(QAbstractItemView.SingleSelection)
        display_cols = ["query_id", "view_count", "views", "sql_preview"]
        model = _DataFrameModel(rows[display_cols], row_df=rows)
        grid.setModel(model)
        grid.resizeColumnsToContents()
        grid.horizontalHeader().setStretchLastSection(True)
        root.addWidget(grid, 1)
        actions = QHBoxLayout()
        actions.addStretch(1)
        insert_btn = QPushButton("Insert into Analyzer")
        insert_btn.setObjectName("Primary")
        close_btn = QPushButton("Close")
        close_btn.setObjectName("Ghost")
        actions.addWidget(insert_btn)
        actions.addWidget(close_btn)
        root.addLayout(actions)

        def insert_selected() -> None:
            selection = grid.selectionModel()
            selected = selection.selectedRows() if selection is not None else []
            index = selected[0] if selected else grid.currentIndex()
            if not index.isValid():
                QMessageBox.information(dialog, "Queries with Views", "Select a query first.")
                return
            row = model.row_at(index.row())
            sql = str(row.get("sql_text") or "").strip()
            if not sql:
                QMessageBox.information(dialog, "Queries with Views", "The selected query has no SQL text.")
                return
            query_id = str(row.get("query_id") or "-")
            dialog.accept()
            self.analyze_external_sql(sql, f"query {query_id}")

        insert_btn.clicked.connect(insert_selected)
        grid.doubleClicked.connect(lambda _index: insert_selected())
        close_btn.clicked.connect(dialog.reject)
        if model.rowCount() > 0:
            grid.selectRow(0)
        _resize_dialog_to_screen(dialog, 0.86)
        dialog.exec()

    def _open_fullscreen(self) -> None:
        dialog = QDialog(self)
        dialog.setWindowTitle("SQL Lens - Full Screen (edits return to the Lens on close)")
        root = QVBoxLayout(dialog)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)
        editor = _XraySqlEdit()
        editor.setObjectName("Mono")
        editor.set_probe_context(self._xray_lookup, self._view_map)
        editor.setPlainText(self._sql.toPlainText())
        root.addWidget(editor, 1)
        actions = QHBoxLayout()
        format_btn = QPushButton("Format SQL")
        format_btn.setObjectName("Ghost")
        format_btn.clicked.connect(lambda: _apply_format_sql(editor, dialog))
        lineage_btn = QPushButton("Show Lineage")
        lineage_btn.setObjectName("Ghost")
        lineage_btn.clicked.connect(
            lambda: _open_sql_lineage_dialog(
                editor.toPlainText(),
                pd.Series({"query_id": "SQL Lens full screen"}),
                self._table_review,
                self._view_definitions,
                dialog,
            )
        )
        subqueries_btn = QPushButton("Extract Subqueries")
        subqueries_btn.setObjectName("Ghost")
        subqueries_btn.clicked.connect(
            lambda: _open_sql_subqueries_dialog(
                editor.toPlainText(),
                pd.Series({"query_id": "SQL Lens full screen"}),
                self._table_review,
                self._view_definitions,
                dialog,
                source_editor=editor,
            )
        )
        explode_btn = QPushButton("Explode Views")
        explode_btn.setObjectName("Ghost")
        explode_btn.clicked.connect(lambda: self._explode_views(editor))
        restore_btn = QPushButton("Restore SQL")
        restore_btn.setObjectName("Ghost")
        restore_btn.clicked.connect(lambda: self._restore_sql(editor))
        markers_btn = QPushButton("Slow Markers")
        markers_btn.setObjectName("Ghost")
        markers_btn.clicked.connect(lambda: self._open_slow_markers(editor.toPlainText()))
        footprint_btn = QPushButton("Extract All Objects")
        footprint_btn.setObjectName("Ghost")
        footprint_btn.clicked.connect(lambda: self._open_footprint(editor.toPlainText()))
        back_btn = QPushButton("Return to Lens")
        back_btn.setObjectName("Primary")
        back_btn.clicked.connect(dialog.accept)
        actions.addWidget(format_btn)
        actions.addWidget(lineage_btn)
        actions.addWidget(subqueries_btn)
        actions.addWidget(explode_btn)
        actions.addWidget(restore_btn)
        actions.addWidget(markers_btn)
        actions.addWidget(footprint_btn)
        actions.addStretch(1)
        actions.addWidget(back_btn)
        root.addLayout(actions)
        dialog.finished.connect(lambda _result: self._sql.setPlainText(editor.toPlainText()))
        dialog.showMaximized()
        dialog.exec()

    def load_external_sql(
        self,
        sql_text: object,
        label: str = "selected query",
        *,
        analyze: bool = False,
    ) -> None:
        sql = str(sql_text or "").strip()
        if not sql:
            return
        self._sql.setPlainText(sql)
        self._sql.clear_explosion_highlights()
        self._original_sql = None
        self._update_view_alert(sql)
        self._status.setText(
            f"Loaded {label}. Use Format SQL, Extract Subqueries, Explode Views, Show Lineage, or Analyze SQL."
        )
        if analyze:
            self._status.setText(f"Analyzing {label} with SQLGlot.")
            self._analyze()

    def analyze_external_sql(self, sql_text: object, label: str = "selected query") -> None:
        self.load_external_sql(sql_text, label, analyze=True)

    def _ensure_probe_context(self) -> None:
        """Build the table lookup and view map once, on first actual use.

        Kept out of set_context() so opening a large snapshot does not pay the
        cost during the shared "Rendering ..." step. Idempotent.
        """
        if self._context_built:
            return
        self._xray_lookup = build_table_lookup(
            self._table_review.to_dict("records") if not self._table_review.empty else []
        )
        self._view_map = build_view_map(
            self._view_definitions.to_dict("records") if not self._view_definitions.empty else []
        )
        self._sql.set_probe_context(self._xray_lookup, self._view_map)
        self._context_built = True

    def _analyze(self) -> None:
        self._ensure_probe_context()
        sql = self._sql.toPlainText()
        if not sql.strip():
            self._analysis = None
            self._render()
            return
        if self._analyze_thread is not None and self._analyze_thread.isRunning():
            # A run is in flight; remember to re-run with the latest text.
            self._analyze_pending = True
            return
        if self._analysis_sql.strip() != sql.strip():
            self._analysis = None
        self._analyze_btn.setEnabled(False)
        self._analyze_btn.setText("Analyzing ...")
        self._status.setText("Analyzing SQL in the background - the window stays responsive.")
        QApplication.processEvents()
        thread = QThread(self)
        worker = _SqlLensAnalyzeWorker(sql, self._table_review, self._known_queries, self._view_definitions)
        worker.moveToThread(thread)
        thread.started.connect(worker.run, Qt.QueuedConnection)
        worker.finished.connect(self._on_analysis_ready, Qt.QueuedConnection)
        worker.failed.connect(self._on_analysis_failed, Qt.QueuedConnection)
        worker.finished.connect(thread.quit, Qt.DirectConnection)
        worker.failed.connect(thread.quit, Qt.DirectConnection)
        thread.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        self._analyze_thread = thread
        self._analyze_worker = worker
        self._analysis_requested_sql = sql
        thread.start()

    def _finish_analysis_run(self) -> None:
        self._analyze_btn.setEnabled(True)
        self._analyze_btn.setText("Analyze SQL")
        self._analyze_thread = None
        self._analyze_worker = None
        if self._analyze_pending:
            self._analyze_pending = False
            QTimer.singleShot(0, self._analyze)

    def _on_analysis_ready(self, analysis) -> None:
        self._analysis = analysis
        self._analysis_sql = self._analysis_requested_sql
        self._finish_analysis_run()
        self._render()
        self._run_post_analysis_action()

    def _on_analysis_failed(self, message: str) -> None:
        self._finish_analysis_run()
        self._status.setText(f"Analysis failed: {message}")
        self._post_analysis_action = ""

    def _render(self) -> None:
        analysis = self._analysis
        if analysis is None:
            self._set_empty_summary()
            return
        summary = analysis.summary
        self._summary["parse"].set_value(str(summary.get("parse_status", "-")).upper())
        self._summary["tables"].set_value(_fmt_int(summary.get("table_count")))
        best = float(summary.get("best_similarity") or 0)
        repeat_label = f"{best * 100:.0f}%" if best else "0%"
        if summary.get("repeatable_query") == "yes":
            repeat_label = f"YES {repeat_label}"
        self._summary["repeat"].set_value(repeat_label)
        self._summary["first"].set_value(str(summary.get("first_step") or "-").upper())
        self._status.setText(
            f"{summary.get('table_count', 0)} table object(s), "
            f"{summary.get('join_count', 0)} join(s), "
            f"{summary.get('predicate_count', 0)} predicate(s), "
            f"{summary.get('repeat_match_count', 0)} repeat match(es), "
            f"{summary.get('diagram_node_count', 0)} diagram node(s)."
        )
        self._steps.set_dataframe(analysis.first_steps)
        self._tables.set_dataframe(analysis.tables)
        self._joins.set_dataframe(analysis.joins)
        self._predicates.set_dataframe(analysis.predicates)
        self._repeats.set_dataframe(analysis.repeat_matches)
        self._findings.set_dataframe(analysis.first_steps)
        self._flow.set_analysis(analysis)

    def _clear(self) -> None:
        self._sql.clear()
        self._sql.clear_explosion_highlights()
        self._hide_view_alert()
        self._analysis = None
        self._analysis_sql = ""
        self._set_empty_summary()
        self._steps.set_dataframe(pd.DataFrame())
        for page in (self._tables, self._joins, self._predicates, self._repeats, self._findings):
            page.set_dataframe(pd.DataFrame())
        self._flow.set_analysis(None)

    def _set_empty_summary(self) -> None:
        self._summary["parse"].set_value("-")
        self._summary["tables"].set_value("0")
        self._summary["repeat"].set_value("0%")
        self._summary["first"].set_value("-")

    def _format_sql(self) -> None:
        formatted = _apply_format_sql(self._sql, self)
        if formatted and self._analysis is not None:
            self._analyze()

    def _lineage_source_row(self, label: str = "single query") -> pd.Series:
        return pd.Series({"query_id": label, "sql_text": self._sql.toPlainText()})

    def _ensure_analysis(self, action: str = "") -> SQLLensAnalysis | None:
        if not self._sql.toPlainText().strip():
            QMessageBox.information(self, "SQL Lineage", "Paste SQL first.")
            return None
        sql = self._sql.toPlainText().strip()
        if self._analysis is None or self._analysis_sql.strip() != sql:
            self._post_analysis_action = action
            self._analyze()
            self._status.setText(
                "Analyzing SQL now; lineage will open automatically when the results are ready."
                if action == "lineage"
                else "Analyzing SQL now."
            )
            return None
        return self._analysis

    def _open_lineage(self) -> None:
        analysis = self._ensure_analysis("lineage")
        if analysis is None:
            return
        dialog = _SlowQueryLineageDialog(
            self._lineage_source_row(),
            analysis,
            self._table_review,
            self._view_definitions,
            self,
        )
        _resize_dialog_to_screen(dialog, 0.94)
        dialog.exec()

    def _run_post_analysis_action(self) -> None:
        action = self._post_analysis_action
        if not action or self._analysis_sql.strip() != self._sql.toPlainText().strip():
            return
        self._post_analysis_action = ""
        if action == "lineage":
            QTimer.singleShot(0, self._open_lineage)

    def _open_selection_lineage(self) -> None:
        selected_sql = _selected_editor_text(self._sql)
        if not selected_sql:
            QMessageBox.information(self, "Selection Lineage", "Highlight a SELECT/FROM/JOIN section first.")
            return
        analysis_sql = _coerce_sql_fragment_for_analysis(selected_sql)
        try:
            analysis = analyze_console_sql(
                analysis_sql,
                self._table_review,
                pd.DataFrame(),
                self._view_definitions,
            )
        except Exception as exc:
            QMessageBox.warning(self, "Selection Lineage", str(exc))
            return
        row = pd.Series({"query_id": "single query selection", "sql_text": analysis_sql})
        dialog = _SlowQueryLineageDialog(row, analysis, self._table_review, self._view_definitions, self)
        _resize_dialog_to_screen(dialog, 0.94)
        dialog.exec()

    def _open_subquery_extractor(self) -> None:
        sql = self._sql.toPlainText()
        if not sql.strip():
            QMessageBox.information(self, "Extract Subqueries", "Paste SQL first.")
            return
        subqueries = _extract_subquery_rows(sql)
        if subqueries.empty:
            QMessageBox.information(self, "Extract Subqueries", "No CTEs or subqueries were found in this SQL text.")
            return
        dialog = _SubqueryExtractDialog(
            self._lineage_source_row(),
            subqueries,
            self._table_review,
            self._view_definitions,
            self,
            source_editor=self._sql,
        )
        _resize_dialog_to_screen(dialog, 0.84)
        dialog.exec()


class _FirstStepCards(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(8)
        head = QLabel("FIRST STEPS")
        head.setObjectName("SectionHeader")
        root.addWidget(head)
        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._scroll.setFrameShape(QFrame.NoFrame)
        self._host = QWidget()
        self._lay = QVBoxLayout(self._host)
        self._lay.setContentsMargins(0, 0, 0, 0)
        self._lay.setSpacing(8)
        self._scroll.setWidget(self._host)
        root.addWidget(self._scroll, 1)

    def set_dataframe(self, df: pd.DataFrame) -> None:
        _clear_layout(self._lay)
        if df is None or df.empty:
            empty = QLabel("Analyze SQL to get first-step guidance.")
            empty.setObjectName("Caption")
            empty.setWordWrap(True)
            self._lay.addWidget(empty)
            self._lay.addStretch(1)
            return
        for _, row in df.head(5).iterrows():
            self._lay.addWidget(_FirstStepCard(row))
        self._lay.addStretch(1)


class _FirstStepCard(QFrame):
    def __init__(self, row: pd.Series):
        super().__init__()
        self.setObjectName("Card")
        lay = QVBoxLayout(self)
        lay.setContentsMargins(14, 12, 14, 12)
        lay.setSpacing(8)
        top = QHBoxLayout()
        rank = QLabel(f"#{int(row.get('rank') or 0)}")
        rank.setObjectName("Mono")
        rank.setStyleSheet("color:#8BA4FF; font-size:12px; font-weight:700;")
        top.addWidget(rank)
        sev = str(row.get("severity") or "info")
        chip = QLabel(sev.upper())
        chip.setProperty("chip", True)
        chip.setProperty("severity", sev)
        chip.setAlignment(Qt.AlignCenter)
        top.addWidget(chip)
        category = QLabel(str(row.get("category") or "").upper())
        category.setObjectName("SectionHeader")
        top.addWidget(category)
        top.addStretch(1)
        lay.addLayout(top)
        title = QLabel(str(row.get("title") or "-"))
        title.setObjectName("H1")
        title.setWordWrap(True)
        lay.addWidget(title)
        why = QLabel(str(row.get("why") or ""))
        why.setObjectName("Caption")
        why.setWordWrap(True)
        lay.addWidget(why)
        next_step = QLabel(str(row.get("next_step") or ""))
        next_step.setObjectName("Mono")
        next_step.setWordWrap(True)
        next_step.setStyleSheet(
            "background:#0F1420; border:1px solid #232C42; "
            "border-radius:6px; padding:8px; color:#E2E8F7; font-size:10px;"
        )
        lay.addWidget(next_step)


class _SqlLensFlowCanvas(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("CardSubtle")
        self._analysis: SQLLensAnalysis | None = None
        self._node_rects: dict[str, QRectF] = {}
        self._visible_node_rows = pd.DataFrame()
        self._phase = 0.0
        self.setMinimumHeight(340)
        self._timer = QTimer(self)
        self._timer.setInterval(42)
        self._timer.timeout.connect(self._tick)
        self._timer.start()

    def set_analysis(self, analysis: SQLLensAnalysis | None) -> None:
        self._analysis = analysis
        self.update()

    def _tick(self) -> None:
        self._phase = (self._phase + 0.018) % 1.0
        if self._analysis is not None:
            self.update()

    def paintEvent(self, _) -> None:
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        p.fillRect(self.rect(), QColor(PALETTE.bg_1))
        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("Inter", 9, QFont.Bold))
        p.drawText(QRectF(14, 10, self.width() - 28, 18), Qt.AlignLeft, "SQL DIAGRAM")
        self._draw_legend(p)
        if self._analysis is None:
            p.setFont(QFont("Inter", 10))
            p.drawText(self.rect(), Qt.AlignCenter, "Paste SQL and analyze to visualize the statement.")
            return

        analysis = self._analysis
        nodes = analysis.diagram_nodes
        edges = analysis.diagram_edges
        if nodes is None or nodes.empty:
            p.setFont(QFont("Inter", 10))
            p.drawText(self.rect(), Qt.AlignCenter, "No diagram nodes were produced for this statement.")
            return

        visible_nodes, hidden_count = self._visible_nodes(nodes)
        rects = self._layout_graph(visible_nodes)
        self._visible_node_rows = visible_nodes.copy()
        self._node_rects = dict(rects)
        sql_len = len(analysis.normalized_sql or "")
        bar_rect = QRectF(120, 14, min(max(sql_len / 4, 24), max(24, self.width() - 520)), 8)
        p.setPen(Qt.NoPen)
        p.setBrush(QColor(PALETTE.accent))
        p.drawRoundedRect(bar_rect, 4, 4)
        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("Inter", 8))
        hidden = f" | {hidden_count} hidden; see tables" if hidden_count else ""
        p.drawText(
            QRectF(bar_rect.right() + 8, 8, max(120, self.width() - bar_rect.right() - 24), 18),
            Qt.AlignLeft,
            f"{sql_len:,} normalized chars | {len(nodes)} nodes / {0 if edges is None else len(edges)} edges{hidden}",
        )

        if edges is not None and not edges.empty:
            for i, (_, edge) in enumerate(edges.iterrows()):
                source = str(edge.get("source") or "")
                target = str(edge.get("target") or "")
                if source in rects and target in rects:
                    self._draw_graph_edge(p, rects[source], rects[target], edge, i)

        for _, row in visible_nodes.iterrows():
            node_id = str(row.get("node_id") or "")
            rect = rects.get(node_id)
            if rect is not None:
                self._draw_graph_node(p, rect, row)

        self._draw_flow_journey(p, visible_nodes, rects)

    def _draw_legend(self, p: QPainter) -> None:
        items = [
            ("VIEW", QColor(PALETTE.violet)),
            ("TABLE", QColor(PALETTE.cyan)),
            ("JOIN", QColor(PALETTE.pink)),
            ("FILTER", QColor(PALETTE.warn)),
            ("ADVICE", QColor(PALETTE.accent_bright)),
        ]
        x = self.width() - 390
        if x < 220:
            return
        p.setFont(QFont("Inter", 7, QFont.Bold))
        for label, color in items:
            rect = QRectF(x, 9, 62, 18)
            fill = QColor(color)
            fill.setAlpha(34)
            p.setPen(color)
            p.setBrush(fill)
            p.drawRoundedRect(rect, 5, 5)
            p.drawText(rect, Qt.AlignCenter, label)
            x += 70

    def _visible_nodes(self, nodes: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        limits = {
            "statement": 1,
            "relation": 9,
            "operation": 8,
            "intelligence": 2,
            "advice": 3,
        }
        selected = []
        hidden = 0
        for group, limit in limits.items():
            subset = nodes[nodes["group"].astype(str) == group].copy()
            if subset.empty:
                continue
            subset["_score"] = pd.to_numeric(subset.get("score"), errors="coerce").fillna(0)
            if group in {"relation", "operation"}:
                subset = subset.sort_values(["_score", "label"], ascending=[False, True])
            if len(subset) > limit:
                hidden += len(subset) - limit
            selected.append(subset.head(limit).drop(columns=["_score"], errors="ignore"))
        if not selected:
            return nodes.head(24).copy(), max(0, len(nodes) - 24)
        return pd.concat(selected, ignore_index=True), hidden

    def _layout_graph(self, nodes: pd.DataFrame) -> dict[str, QRectF]:
        lanes = {
            "statement": (22.0, max(160.0, self.width() * 0.14)),
            "relation": (self.width() * 0.20, max(210.0, self.width() * 0.22)),
            "operation": (self.width() * 0.49, max(210.0, self.width() * 0.21)),
            "intelligence": (self.width() * 0.76, max(210.0, self.width() * 0.20)),
            "advice": (self.width() * 0.76, max(210.0, self.width() * 0.20)),
        }
        top = 48.0
        bottom = max(top + 120, self.height() - 18.0)
        rects: dict[str, QRectF] = {}
        for group in ("statement", "relation", "operation", "intelligence", "advice"):
            subset = nodes[nodes["group"].astype(str) == group]
            if subset.empty:
                continue
            x, w = lanes[group]
            if group == "advice":
                top_for_group = top + (bottom - top) * 0.42
            else:
                top_for_group = top
            height_for_group = bottom - top_for_group
            gap = height_for_group / max(1, len(subset))
            node_h = max(30.0, min(50.0, gap * 0.76))
            if group == "statement":
                node_h = 58.0
            for i, (_, row) in enumerate(subset.iterrows()):
                y = top_for_group + i * gap + max(0.0, gap - node_h) / 2
                rects[str(row.get("node_id") or "")] = QRectF(x, y, w, node_h)
        return rects

    def _draw_graph_edge(self, p: QPainter, source: QRectF, target: QRectF, edge: pd.Series, offset: int) -> None:
        color = self._edge_color(edge)
        line = QColor(color)
        line.setAlpha(110)
        pen = QPen(line, max(1.2, min(3.8, float(edge.get("weight") or 1.0) * 1.4)))
        pen.setCapStyle(Qt.RoundCap)
        p.setPen(pen)
        path = QPainterPath()
        if target.left() > source.right():
            start = QPointF(source.right(), source.center().y())
            end = QPointF(target.left(), target.center().y())
            dx = max(42.0, (end.x() - start.x()) * 0.48)
            path.moveTo(start)
            path.cubicTo(QPointF(start.x() + dx, start.y()), QPointF(end.x() - dx, end.y()), end)
        else:
            start = QPointF(source.right(), source.center().y())
            end = QPointF(target.right(), target.center().y())
            dx = 32.0 + (offset % 3) * 8.0
            path.moveTo(start)
            path.cubicTo(
                QPointF(start.x() + dx, start.y()),
                QPointF(end.x() + dx, end.y()),
                end,
            )
        p.drawPath(path)

    @staticmethod
    def _operation_sort_key(node_id: str) -> tuple[int, int]:
        kind, _, number = str(node_id).partition(":")
        try:
            order = int(number)
        except (TypeError, ValueError):
            order = 0
        # Joins (FROM clause) come before predicates (WHERE clause) in the
        # statement, and each carries its appearance number within its clause.
        return (0 if kind == "join" else 1, order)

    def _draw_flow_journey(self, p: QPainter, nodes: pd.DataFrame, rects: dict[str, QRectF]) -> None:
        """One continuous animated route: statement -> each join/predicate in
        SQL order, passing through the middle of every operation box."""
        operation_ids = [
            str(row.get("node_id") or "")
            for _, row in nodes.iterrows()
            if str(row.get("group") or "") == "operation"
        ]
        operation_ids = [node_id for node_id in operation_ids if node_id in rects]
        operation_ids.sort(key=self._operation_sort_key)
        start_rect = rects.get("sql:statement")
        if start_rect is None or not operation_ids:
            return
        path = QPainterPath()
        current = QPointF(start_rect.right(), start_rect.center().y())
        path.moveTo(current)
        for node_id in operation_ids:
            rect = rects[node_id]
            entry = QPointF(rect.left(), rect.center().y())
            dx = max(28.0, abs(entry.x() - current.x()) * 0.5)
            path.cubicTo(
                QPointF(current.x() + dx, current.y()),
                QPointF(entry.x() - dx, entry.y()),
                entry,
            )
            exit_point = QPointF(rect.right(), rect.center().y())
            path.lineTo(exit_point)
            current = exit_point
        route = QColor(PALETTE.accent_bright)
        route.setAlpha(120)
        route_pen = QPen(route, 1.6, Qt.DashLine)
        route_pen.setCapStyle(Qt.RoundCap)
        p.setPen(route_pen)
        p.setBrush(Qt.NoBrush)
        p.drawPath(path)
        dot = QColor(PALETTE.accent_bright)
        dot.setAlpha(230)
        p.setPen(Qt.NoPen)
        p.setBrush(dot)
        for k in range(2):
            t = (self._phase + k * 0.5) % 1.0
            point = path.pointAtPercent(t)
            p.drawEllipse(QRectF(point.x() - 4, point.y() - 4, 8, 8))

    def _draw_graph_node(self, p: QPainter, rect: QRectF, row: pd.Series) -> None:
        color = self._node_color(row)
        fill = QColor(PALETTE.bg_2)
        if str(row.get("group") or "") == "statement":
            fill = QColor(PALETTE.bg_3)
        p.setPen(QPen(color, 1.3))
        p.setBrush(fill)
        p.drawRoundedRect(rect, 8, 8)
        stripe = QRectF(rect.left(), rect.top(), 5, rect.height())
        p.setPen(Qt.NoPen)
        p.setBrush(color)
        p.drawRoundedRect(stripe, 3, 3)

        label = str(row.get("label") or "-")
        detail = str(row.get("detail") or "")
        kind = str(row.get("kind") or "").upper().replace("_", " ")
        text_rect = rect.adjusted(12, 5, -8, -4)
        p.setPen(QColor(PALETTE.text_0))
        p.setFont(QFont("Inter", 8, QFont.DemiBold))
        label_width = max(24.0, text_rect.width() - 62.0)
        label_chars = max(12, int(label_width / 6.2))
        p.drawText(text_rect.adjusted(0, 0, -62, 0), Qt.AlignLeft | Qt.AlignTop, _clip(label, label_chars))
        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("Inter", 6, QFont.Bold))
        p.drawText(QRectF(rect.right() - 68, rect.top() + 6, 58, 12), Qt.AlignRight, _clip(kind, 12))
        if rect.height() >= 38 and detail:
            p.setFont(QFont("Inter", 7))
            detail_chars = max(14, int(text_rect.width() / 5.9))
            p.drawText(
                QRectF(text_rect.left(), rect.top() + 24, text_rect.width(), 15),
                Qt.AlignLeft | Qt.AlignTop,
                _clip(detail, detail_chars),
            )

    def _node_color(self, row: pd.Series) -> QColor:
        severity = str(row.get("severity") or "").lower()
        kind = str(row.get("kind") or "").lower()
        if severity == "crit":
            return QColor(PALETTE.crit)
        if severity == "warn":
            return QColor(PALETTE.warn)
        if severity == "ok":
            return QColor(PALETTE.ok)
        if kind in {"view", "view_component_view"}:
            return QColor(PALETTE.violet)
        if kind == "view_component_table":
            return QColor(PALETTE.cyan)
        if kind == "join":
            return QColor(PALETTE.pink)
        if kind == "predicate":
            return QColor(PALETTE.warn)
        if kind == "repeat":
            return QColor(PALETTE.pink)
        if kind == "advice":
            return QColor(PALETTE.accent_bright)
        if kind == "statement":
            return QColor(PALETTE.accent)
        return QColor(PALETTE.ok)

    def _edge_color(self, edge: pd.Series) -> QColor:
        severity = str(edge.get("severity") or "").lower()
        edge_type = str(edge.get("edge_type") or "").lower()
        if severity == "crit":
            return QColor(PALETTE.crit)
        if severity == "warn":
            return QColor(PALETTE.warn)
        if severity == "ok":
            return QColor(PALETTE.ok)
        if edge_type == "view_expansion":
            return QColor(PALETTE.violet)
        if edge_type == "join":
            return QColor(PALETTE.pink)
        if edge_type == "predicate":
            return QColor(PALETTE.warn)
        if edge_type == "similarity":
            return QColor(PALETTE.pink)
        if edge_type == "advice":
            return QColor(PALETTE.accent_bright)
        return QColor(PALETTE.cyan)

    def mouseDoubleClickEvent(self, event) -> None:
        if self._analysis is None or self._visible_node_rows.empty:
            return
        try:
            point = event.position()
        except AttributeError:
            point = event.pos()
        for node_id, rect in self._node_rects.items():
            if rect.contains(point):
                node_rows = self._visible_node_rows[self._visible_node_rows["node_id"].astype(str) == str(node_id)]
                if not node_rows.empty:
                    self._open_node_detail(node_rows.iloc[0])
                return

    def _open_node_detail(self, node: pd.Series) -> None:
        if self._analysis is None:
            return
        dialog = QDialog(self)
        dialog.setWindowTitle(f"SQL Diagram Detail - {node.get('label') or node.get('node_id') or 'node'}")
        root = QVBoxLayout(dialog)
        root.setContentsMargins(12, 12, 12, 12)
        title = QLabel(str(node.get("label") or node.get("node_id") or "SQL node"))
        title.setObjectName("SectionHeader")
        title.setWordWrap(True)
        root.addWidget(title)
        detail = QPlainTextEdit()
        detail.setReadOnly(True)
        detail.setObjectName("Mono")
        detail.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        detail.setPlainText(_sql_flow_node_detail_text(node, self._analysis))
        root.addWidget(detail, 1)
        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.close)
        root.addWidget(buttons)
        _resize_dialog_to_screen(dialog, 0.72)
        dialog.exec()


class _RepeatGroupCards(QWidget):
    def __init__(self, title: str = "", parent=None):
        super().__init__(parent)
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(8)
        if title:
            head = QLabel(title)
            head.setObjectName("SectionHeader")
            root.addWidget(head)
        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._scroll.setFrameShape(QFrame.NoFrame)
        self._host = QWidget()
        self._lay = QVBoxLayout(self._host)
        self._lay.setContentsMargins(0, 0, 0, 0)
        self._lay.setSpacing(8)
        self._scroll.setWidget(self._host)
        root.addWidget(self._scroll, 1)

    def set_dataframe(self, df: pd.DataFrame) -> None:
        _clear_layout(self._lay)
        df = _repeat_groups_for_display(df)
        if df is None or df.empty:
            empty = QLabel("No repeated query shapes loaded yet. Use Load Repeat Analysis to scan the top local slow-query rows.")
            empty.setObjectName("Caption")
            empty.setWordWrap(True)
            self._lay.addWidget(empty)
            self._lay.addStretch(1)
            return
        for _, row in df.head(18).iterrows():
            self._lay.addWidget(_RepeatGroupCard(row))
        self._lay.addStretch(1)


class _RepeatGroupCard(QFrame):
    def __init__(self, row: pd.Series):
        super().__init__()
        self.setObjectName("Card")
        lay = QVBoxLayout(self)
        lay.setContentsMargins(14, 12, 14, 12)
        lay.setSpacing(8)

        top = QHBoxLayout()
        group_id = QLabel(str(row.get("repeat_group_id") or "-"))
        group_id.setObjectName("Mono")
        group_id.setStyleSheet("color:#8BA4FF; font-size:12px; font-weight:700;")
        top.addWidget(group_id, 0)
        verdict = QLabel(str(row.get("repeat_verdict") or _repeat_verdict(row)).upper())
        verdict.setProperty("chip", True)
        verdict.setProperty("severity", _repeat_chip_severity(row))
        verdict.setAlignment(Qt.AlignCenter)
        top.addWidget(verdict, 0)
        top.addStretch(1)
        score = QLabel(f"priority {_fmt_value('repeat_priority_score', row.get('repeat_priority_score'))}")
        score.setObjectName("Mono")
        score.setStyleSheet("color:#3DD68C; font-size:12px; font-weight:700;")
        top.addWidget(score, 0)
        lay.addLayout(top)

        headline = QLabel(f"Fix one pattern, affects {_fmt_int(row.get('query_count'))} captured runs")
        headline.setObjectName("H1")
        headline.setWordWrap(True)
        lay.addWidget(headline)

        impact = QLabel()
        impact.setObjectName("Caption")
        apply_markdown(impact, str(row.get("impact_summary") or _repeat_impact_summary(row)))
        lay.addWidget(impact)

        confidence = QLabel(
            f"Match confidence: {row.get('similarity_label') or _similarity_label(row.get('avg_similarity'))} "
            f"({_fmt_value('avg_similarity', row.get('avg_similarity'))})"
        )
        confidence.setObjectName("Mono")
        confidence.setStyleSheet(f"color:{PALETTE.text_2}; font-size:11px;")
        confidence.setWordWrap(True)
        lay.addWidget(confidence)

        users = str(row.get("users") or "").strip()
        if users:
            owner = QLabel(users)
            owner.setObjectName("Mono")
            owner.setStyleSheet(f"color:{PALETTE.text_2}; font-size:11px;")
            owner.setWordWrap(True)
            lay.addWidget(owner)

        tables = str(row.get("shared_tables") or "").strip()
        if tables:
            table_label = QLabel(tables)
            table_label.setObjectName("Caption")
            table_label.setWordWrap(True)
            lay.addWidget(table_label)

        structure_bits = []
        for label, key in (
            ("tables", "table_count"),
            ("joins", "join_count"),
            ("predicates", "predicate_count"),
            ("CTEs", "cte_count"),
            ("wildcards", "wildcard_count"),
        ):
            try:
                value = int(float(row.get(key) or 0))
            except (TypeError, ValueError):
                value = 0
            if value:
                structure_bits.append(f"{value} {label}")
        if structure_bits:
            structure = QLabel(" / ".join(structure_bits))
            structure.setObjectName("Caption")
            structure.setWordWrap(True)
            lay.addWidget(structure)

        hint = str(row.get("fix_hint") or _repeat_fix_hint(row)).strip()
        if hint:
            hint_label = QLabel()
            hint_label.setObjectName("Caption")
            # Route the box colors through PALETTE instead of hardcoded hex, and
            # render the hint as themed markdown.
            hint_label.setStyleSheet(
                f"background:{PALETTE.bg_1}; border:1px solid {PALETTE.bg_3}; "
                f"border-radius:6px; padding:8px; color:{PALETTE.text_0};"
            )
            apply_markdown(hint_label, hint)
            lay.addWidget(hint_label)

        joins = str(row.get("sql_joins") or "").strip()
        predicates = str(row.get("sql_predicates") or "").strip()
        join_cols = str(row.get("sql_join_columns") or "").strip()
        filter_cols = str(row.get("sql_filter_columns") or "").strip()
        if joins or predicates:
            sql_features = QLabel(" | ".join(part for part in (joins, predicates) if part))
            sql_features.setObjectName("Caption")
            sql_features.setWordWrap(True)
            lay.addWidget(sql_features)
        if join_cols or filter_cols:
            column_features = QLabel(
                " | ".join(
                    part
                    for part in (
                        f"join columns: {join_cols}" if join_cols else "",
                        f"filter columns: {filter_cols}" if filter_cols else "",
                    )
                    if part
                )
            )
            column_features.setObjectName("Caption")
            column_features.setWordWrap(True)
            lay.addWidget(column_features)

        shape_text = str(row.get("sql_shape") or "").strip()
        if shape_text:
            shape_head = QLabel("PATTERN SHAPE")
            shape_head.setObjectName("SectionHeader")
            lay.addWidget(shape_head)

            shape = QLabel(shape_text)
            shape.setObjectName("Mono")
            shape.setTextInteractionFlags(Qt.TextSelectableByMouse)
            shape.setWordWrap(True)
            shape.setStyleSheet(
                "background:#141A2A; border:1px solid #232C42; "
                "border-radius:6px; padding:8px; color:#C4CCE0; font-size:10px;"
            )
            lay.addWidget(shape)

        sample_head = QLabel(
            "STORED PROCEDURE BODY"
            if str(row.get("repeat_kind") or "").lower() == "stored_procedure"
            else "REPRESENTATIVE SQL"
        )
        sample_head.setObjectName("SectionHeader")
        lay.addWidget(sample_head)

        sample_sql = QLabel(str(row.get("sample_sql") or ""))
        sample_sql.setObjectName("Mono")
        sample_sql.setTextInteractionFlags(Qt.TextSelectableByMouse)
        sample_sql.setWordWrap(True)
        sample_sql.setStyleSheet(
            "background:#0F1420; border:1px solid #232C42; "
            "border-radius:6px; padding:8px; color:#C4CCE0; font-size:10px;"
        )
        lay.addWidget(sample_sql)

        shape_head = QLabel("NORMALIZED MATCH SHAPE")
        shape_head.setObjectName("SectionHeader")
        lay.addWidget(shape_head)

        shape = QLabel(str(row.get("sql_shape") or ""))
        shape.setObjectName("Mono")
        shape.setTextInteractionFlags(Qt.TextSelectableByMouse)
        shape.setWordWrap(True)
        shape.setStyleSheet(
            "background:#141A2A; border:1px solid #232C42; "
            "border-radius:6px; padding:8px; color:#8691AD; font-size:10px;"
        )
        lay.addWidget(shape)


class _SeverityQueryPage(QWidget):
    loadRequested = Signal(str)
    queryDiagramRequested = Signal(object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._df = pd.DataFrame()
        self._explain_df = pd.DataFrame()
        self._detail_flow_df = pd.DataFrame()
        self._table_review_df = pd.DataFrame()
        self._view_definitions_df = pd.DataFrame()
        self._model: _DataFrameModel | None = None
        self._formatted_sql_cache: dict[str, str] = {}
        self._slow_filters: dict = {"minimums": {}, "values": {}}

        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(6)

        head = QHBoxLayout()
        title = QLabel("SLOW QUERY SEVERITY")
        title.setObjectName("SectionHeader")
        head.addWidget(title)
        head.addStretch(1)
        self._status = QLabel("Load slow queries from local DuckDB.")
        self._status.setObjectName("Caption")
        self._status.setMaximumWidth(280)
        head.addWidget(self._status)
        score_btn = QPushButton("Scoring")
        score_btn.setObjectName("Ghost")
        score_btn.setToolTip("Show or hide severity scoring controls.")
        head.addWidget(score_btn)
        filters_btn = QPushButton("Filters")
        filters_btn.setObjectName("Ghost")
        filters_btn.setToolTip("Filter slow queries by numeric thresholds or selected values.")
        head.addWidget(filters_btn)
        load_btn = QPushButton("Load Queries")
        load_btn.setObjectName("Primary")
        load_btn.setToolTip("Load threshold-selected slow queries from local DuckDB.")
        head.addWidget(load_btn)
        wide_btn = QPushButton("Wide Grid")
        wide_btn.setObjectName("Ghost")
        wide_btn.setToolTip("Open the slow-query table in a large view with visible horizontal scrolling.")
        head.addWidget(wide_btn)
        diagram_btn = QPushButton("Diagram")
        diagram_btn.setObjectName("Ghost")
        diagram_btn.setToolTip("Diagram the selected query's tables.")
        head.addWidget(diagram_btn)
        lineage_btn = QPushButton("Show Lineage")
        lineage_btn.setObjectName("Ghost")
        lineage_btn.setToolTip("Open tables, views, expanded base tables, and join distribution for the selected query.")
        head.addWidget(lineage_btn)
        open_sql_btn = QPushButton("SQL")
        open_sql_btn.setObjectName("Primary")
        open_sql_btn.setToolTip("Open the selected query SQL.")
        head.addWidget(open_sql_btn)
        plan_btn = QPushButton("Plan")
        plan_btn.setObjectName("Primary")
        plan_btn.setToolTip("Open the selected query explain-plan diagram.")
        head.addWidget(plan_btn)
        root.addLayout(head)

        quick_filters = QHBoxLayout()
        quick_filters.setSpacing(8)
        self._rollup_check = QCheckBox("Group by Repeat Group ID")
        self._rollup_check.setChecked(True)
        self._rollup_check.setToolTip(
            "Checked: show one expandable parent per repeat group and individual query IDs beneath it. "
            "Parent numeric values are averages of their child queries."
        )
        self._rollup_check.stateChanged.connect(self._refresh)
        quick_filters.addWidget(self._rollup_check)
        self._db_filter = _MultiSelectFilter("Database")
        self._db_filter.selectionChanged.connect(self._refresh)
        quick_filters.addWidget(self._db_filter)
        self._user_filter = _MultiSelectFilter("User")
        self._user_filter.selectionChanged.connect(self._refresh)
        quick_filters.addWidget(self._user_filter)
        quick_filters.addStretch(1)
        root.addLayout(quick_filters)

        self._controls = QFrame()
        self._controls.setObjectName("CardSubtle")
        grid = QGridLayout(self._controls)
        grid.setContentsMargins(8, 6, 8, 6)
        grid.setHorizontalSpacing(14)
        grid.setVerticalSpacing(4)
        self._threshold = _SeveritySlider("MIN SCORE", 0, 180, 0)
        self._repeat = _SeveritySlider("REPEAT", 0, 60, 35)
        self._runtime = _SeveritySlider("RUNTIME", 0, 45, 14)
        self._spill = _SeveritySlider("SPILL", 0, 45, 12)
        self._remote = _SeveritySlider("REMOTE I/O", 0, 45, 20)
        self._skew = _SeveritySlider("SKEW", 0, 30, 10)
        self._movement = _SeveritySlider("JOIN MOVE", 0, 45, 18)
        self._external = _SeveritySlider("EXTERNAL/S3", 0, 45, 14)
        self._sliders = (
            self._threshold,
            self._repeat,
            self._runtime,
            self._spill,
            self._remote,
            self._skew,
            self._movement,
            self._external,
        )
        for i, slider in enumerate(self._sliders):
            grid.addWidget(slider, i // 4, i % 4)
            slider.on_change(self._refresh)
        self._controls.hide()
        root.addWidget(self._controls)

        self._detail_title = QLabel("Select a slow query, then use Open SQL or Diagram Selected.")
        self._detail_title.setObjectName("Caption")
        self._detail_title.setWordWrap(True)
        root.addWidget(self._detail_title)
        self._detail_summary = QLabel("")
        self._detail_summary.setObjectName("Caption")
        self._detail_summary.setWordWrap(True)
        root.addWidget(self._detail_summary)

        self._tree = QTreeWidget()
        self._tree.setColumnCount(len(SLOW_QUERY_TREE_COLUMNS))
        self._tree.setHeaderLabels([label for _column, label in SLOW_QUERY_TREE_COLUMNS])
        self._tree.setRootIsDecorated(True)
        self._tree.setAlternatingRowColors(True)
        self._tree.setUniformRowHeights(True)
        self._tree.setSortingEnabled(True)
        self._tree.header().setSortIndicatorShown(True)
        self._tree.header().setSectionsClickable(True)
        self._tree.header().setSectionsMovable(True)
        self._tree.setStyleSheet(
            "QTreeView { border:1px solid #34405A; }"
            "QTreeView::item { border-bottom:1px solid #2B3449; border-right:1px solid #2B3449; padding:3px; }"
            "QHeaderView::section { border-right:1px solid #34405A; border-bottom:1px solid #34405A; padding:5px; }"
        )
        self._tree.setSelectionMode(QAbstractItemView.SingleSelection)
        self._tree.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._tree.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self._tree.setMinimumHeight(120)
        self._tree.itemSelectionChanged.connect(self._on_tree_selection_changed)
        self._tree.itemDoubleClicked.connect(self._on_tree_double_clicked)
        root.addWidget(self._tree, 1)
        score_btn.clicked.connect(self._toggle_scoring_controls)
        filters_btn.clicked.connect(self._open_filter_dialog)
        load_btn.clicked.connect(lambda: self.loadRequested.emit("slow_queries"))
        wide_btn.clicked.connect(self._open_wide_grid)
        diagram_btn.clicked.connect(self._diagram_selected_query)
        lineage_btn.clicked.connect(self._open_selected_query_lineage)
        open_sql_btn.clicked.connect(self._open_selected_query_sql)
        plan_btn.clicked.connect(self._open_selected_query_plan)

    def set_dataframe(self, df: pd.DataFrame, *, loaded: bool = True) -> None:
        self._df = df.copy() if df is not None else pd.DataFrame()
        self._load_completed = bool(loaded)
        if "database_name" in self._df.columns:
            self._db_filter.set_values(self._df["database_name"])
        if "user_name" in self._df.columns:
            self._user_filter.set_values(self._df["user_name"])
        self._refresh()

    def show_loading(self) -> None:
        self._status.setText("Loading Slow Queries from the local DuckDB ...")
        QApplication.processEvents()

    def show_blocked(self, message: str) -> None:
        self._status.setText(message)

    def set_explain_dataframe(self, df: pd.DataFrame) -> None:
        self._explain_df = df.copy() if df is not None else pd.DataFrame()

    def set_detail_flow_dataframe(self, df: pd.DataFrame) -> None:
        self._detail_flow_df = df.copy() if df is not None else pd.DataFrame()

    def set_context(self, table_review: pd.DataFrame, view_definitions: pd.DataFrame) -> None:
        self._table_review_df = table_review.copy() if table_review is not None else pd.DataFrame()
        self._view_definitions_df = view_definitions.copy() if view_definitions is not None else pd.DataFrame()

    def _refresh(self, *_args) -> None:
        if self._df.empty:
            self._tree.clear()
            self._model = None
            self._display_df = pd.DataFrame()
            if getattr(self, "_load_completed", False):
                self._status.setText(
                    "Slow Query load completed: 0 rows returned. Open Error Log if queries should be present."
                )
            else:
                self._status.setText("No slow queries loaded. Click Load Queries to read the local DuckDB.")
            self._clear_query_detail()
            return

        scored = self._df.copy()
        scored["severity_score"] = self._severity_score(scored)
        scored["severity_reason"] = self._severity_reason(scored)
        scored, filter_note = _apply_slow_query_filters(scored, self._slow_filters)
        for column_name, widget, label in (
            ("database_name", self._db_filter, "database"),
            ("user_name", self._user_filter, "user"),
        ):
            selected = widget.selected_values()
            if selected and column_name in scored.columns:
                scored = scored[scored[column_name].astype(str).str.strip().isin(selected)].copy()
                note = f"{label} in {len(selected)} value(s)"
                filter_note = f"{filter_note}; {note}" if filter_note else note
        threshold = self._threshold.value()
        filtered = scored[scored["severity_score"] >= threshold].copy()
        # Empty means empty: never silently restore the full set when the
        # threshold excludes everything — that hides the filter and misleads.
        filtered = filtered.sort_values(
            ["severity_score", "elapsed_s", "risk_score"],
            ascending=[False, False, False],
        )
        if filtered.empty and not scored.empty:
            status = f"0/{len(scored):,} loaded"
            detail = (
                f"No loaded queries meet severity score >= {threshold}. "
                "Lower the severity threshold to see more rows."
            )
        else:
            status = f"{len(filtered):,}/{len(scored):,} loaded"
            detail = f"Showing {len(filtered):,} of {len(scored):,} loaded queries at score >= {threshold}."
        grouped = self._rollup_check.isChecked() and "repeat_group_id" in filtered.columns
        if grouped:
            group_count = _slow_query_group_count(filtered)
            status = f"{group_count:,} groups / {len(filtered):,} queries"
            detail += (
                f" Grouped into {group_count:,} expandable repeat-group branches. "
                "Every numeric value on a parent row is the average of its child queries."
            )
        elif self._rollup_check.isChecked():
            detail += " Grouping unavailable: load Workload Triage so slow-query rows carry repeat group IDs."
        if filter_note:
            status += " | filtered"
            detail += f" Filters: {filter_note}."
        self._status.setText(status)
        self._status.setToolTip(detail)
        if filtered.empty:
            self._tree.clear()
            self._model = None
            self._display_df = pd.DataFrame()
            self._clear_query_detail()
            return
        self._display_df = filtered.reset_index(drop=True)
        cols = [c for c in SLOW_QUERY_LIST_COLS if c in filtered.columns]
        if not cols:
            cols = [c for c in SEVERITY_QUERY_COLS if c in filtered.columns]
        self._model = _DataFrameModel(filtered[cols], row_df=filtered)
        _populate_slow_query_tree(self._tree, self._display_df, grouped=grouped)
        for column in range(self._tree.columnCount()):
            self._tree.resizeColumnToContents(column)
        if self._tree.topLevelItemCount():
            first = self._tree.topLevelItem(0)
            self._tree.setCurrentItem(first)
            payload = first.data(0, Qt.UserRole)
            if isinstance(payload, dict):
                self._show_query_detail(pd.Series(payload))

    def _open_wide_grid(self) -> None:
        _open_model_grid(
            self,
            "Slow Queries Wide Grid",
            self._model,
            "Load slow queries first.",
        )

    def _open_filter_dialog(self) -> None:
        if self._df.empty:
            QMessageBox.information(self, "Slow Query Filters", "Load slow queries first.")
            return
        dialog = _SlowQueryFilterDialog(self._df, self._slow_filters, self)
        _resize_dialog_to_screen(dialog, 0.72)
        if dialog.exec() == QDialog.Accepted:
            self._slow_filters = dialog.filters()
            self._refresh()

    def _diagram_selected_query(self) -> None:
        row = self._selected_individual_query_row("Query Diagram")
        if row is None or row.empty:
            QMessageBox.information(self, "Query Diagram", "Load slow queries first.")
            return
        self.queryDiagramRequested.emit(row)

    def _open_selected_query_lineage(self) -> None:
        row = self._selected_individual_query_row("Query Lineage")
        if row is None or row.empty:
            QMessageBox.information(self, "Query Lineage", "Load slow queries and select a row first.")
            return
        sql_text = self._slow_query_sql_text(row)
        if not sql_text:
            QMessageBox.information(
                self,
                "Query Lineage",
                "The selected row does not include SQL text. Load or refresh query_text so this query can be parsed.",
            )
            return
        try:
            analysis = analyze_console_sql(
                sql_text,
                self._table_review_df,
                pd.DataFrame(),
                self._view_definitions_df,
            )
        except Exception as exc:
            QMessageBox.warning(self, "Query Lineage", str(exc))
            return
        dialog = _SlowQueryLineageDialog(
            row,
            analysis,
            self._table_review_df,
            self._view_definitions_df,
            self,
            explain_rows=self._selected_plan_rows(row),
            detail_rows=_query_evidence_rows(self._detail_flow_df, row),
        )
        _resize_dialog_to_screen(dialog, 0.94)
        dialog.exec()

    def _selected_query_row(self) -> pd.Series | None:
        item = self._tree.currentItem()
        if item is None:
            return None
        payload = item.data(0, Qt.UserRole)
        return pd.Series(payload) if isinstance(payload, dict) else None

    def _selected_individual_query_row(self, action_name: str) -> pd.Series | None:
        row = self._selected_query_row()
        if row is None or row.empty:
            return None
        if bool(row.get("_is_group_parent")):
            QMessageBox.information(
                self,
                action_name,
                "Expand the repeat group and select an individual query ID first.",
            )
            return None
        return row

    def _open_selected_query_sql(self) -> None:
        row = self._selected_individual_query_row("Slow Query SQL")
        if row is None or row.empty:
            QMessageBox.information(self, "Slow Query SQL", "Load slow queries and select a row first.")
            return

        query_id = str(row.get("query_id") or "-")
        dialog = QDialog(self)
        dialog.setWindowTitle(f"Slow Query SQL - {query_id}")
        root = QVBoxLayout(dialog)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)

        title = QLabel(self._query_detail_title(row))
        title.setObjectName("SectionHeader")
        title.setWordWrap(True)
        root.addWidget(title)

        summary = QLabel(self._query_detail_summary(row))
        summary.setObjectName("Caption")
        summary.setWordWrap(True)
        root.addWidget(summary)

        sql_text = self._slow_query_sql_text(row)
        if not sql_text:
            sql_text = (
                "SQL text is not present in this local slow-query row.\n\n"
                "That means repeat-query matching cannot use this row's statement text. "
                "Confirm the local query_text table has rows and that query ids match query_history/query_details."
            )
        editor = QPlainTextEdit()
        editor.setReadOnly(True)
        editor.setObjectName("Mono")
        editor.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        editor.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        cache_key = self._sql_cache_key(row)
        editor.setPlainText(self._formatted_sql_cache.get(cache_key, sql_text))
        _attach_table_info_context_menu(editor, self._table_review_df, dialog)
        viewer_analysis = _analyze_sql_for_viewer(sql_text, self._table_review_df, self._view_definitions_df)
        analysis_holder = {"analysis": viewer_analysis}
        inspector = QPlainTextEdit()
        inspector.setReadOnly(True)
        inspector.setObjectName("Mono")
        inspector.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        inspector.setPlainText(_sql_viewer_summary_text(viewer_analysis, self._table_review_df, self._view_definitions_df))
        viewer_split = QSplitter(Qt.Horizontal)
        viewer_split.setChildrenCollapsible(False)
        viewer_split.addWidget(editor)
        viewer_split.addWidget(inspector)
        viewer_split.setSizes([980, 360])
        root.addWidget(viewer_split, 1)

        actions = QHBoxLayout()
        format_btn = QPushButton("Format SQL")
        format_btn.setObjectName("Primary")
        actions.addWidget(format_btn)
        lineage_btn = QPushButton("Show Lineage")
        lineage_btn.setObjectName("Ghost")
        lineage_btn.setToolTip("Show the tables, views, base tables, and joins used by this SQL.")
        actions.addWidget(lineage_btn)
        identify_btn = QPushButton("Identify Views")
        identify_btn.setObjectName("Ghost")
        identify_btn.setToolTip("Highlight captured views in orange. Double-click a highlighted view to open its stored SQL.")
        actions.addWidget(identify_btn)
        badges_btn = QPushButton("Deficiency Overlay")
        badges_btn.setObjectName("Ghost")
        badges_btn.setToolTip("Highlight views, missing metadata, dist/sort risks, stale stats, and unsorted tables.")
        actions.addWidget(badges_btn)
        selection_btn = QPushButton("Selection Lineage")
        selection_btn.setObjectName("Ghost")
        selection_btn.setToolTip("Analyze only the highlighted SQL text with lineage, distribution, and sort context.")
        actions.addWidget(selection_btn)
        subqueries_btn = QPushButton("Extract Subqueries")
        subqueries_btn.setObjectName("Ghost")
        subqueries_btn.setToolTip("Extract CTEs and subqueries, then analyze one with lineage.")
        actions.addWidget(subqueries_btn)
        actions.addStretch(1)
        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.close)
        actions.addWidget(buttons)
        root.addLayout(actions)

        def format_sql() -> None:
            formatted = _apply_format_sql(editor, dialog)
            if not formatted:
                return
            self._formatted_sql_cache[cache_key] = formatted
            analysis_holder["analysis"] = _analyze_sql_for_viewer(
                formatted,
                self._table_review_df,
                self._view_definitions_df,
            )
            inspector.setPlainText(
                _sql_viewer_summary_text(analysis_holder["analysis"], self._table_review_df, self._view_definitions_df)
            )

        format_btn.clicked.connect(format_sql)
        lineage_btn.clicked.connect(
            lambda: _open_sql_lineage_dialog(
                editor.toPlainText(), row, self._table_review_df, self._view_definitions_df, dialog
            )
        )
        identify_btn.clicked.connect(
            lambda: _identify_sql_views(editor, inspector, analysis_holder["analysis"], dialog)
        )
        badges_btn.clicked.connect(
            lambda: _apply_sql_deficiency_overlay(editor, inspector, analysis_holder["analysis"])
        )
        selection_btn.clicked.connect(lambda: self._open_selection_lineage(editor, row, dialog))
        subqueries_btn.clicked.connect(lambda: self._open_subquery_extractor(editor, row, dialog))
        editor.cursorPositionChanged.connect(
            lambda: _update_sql_object_inspector(editor, inspector, analysis_holder["analysis"], row)
        )
        editor._sql_viewer_filter = _SqlEditorEventFilter(  # type: ignore[attr-defined]
            editor,
            lambda token: _open_view_from_sql_token(token, analysis_holder["analysis"], dialog),
        )
        editor.installEventFilter(editor._sql_viewer_filter)  # type: ignore[attr-defined]
        _resize_dialog_to_screen(dialog, 0.75)
        dialog.exec()

    def _open_selection_lineage(self, editor: QPlainTextEdit, row: pd.Series, parent: QWidget) -> None:
        selected_sql = _selected_editor_text(editor)
        if not selected_sql:
            QMessageBox.information(parent, "Selection Lineage", "Highlight a SELECT/FROM/JOIN section first.")
            return
        analysis_sql = _coerce_sql_fragment_for_analysis(selected_sql)
        try:
            analysis = analyze_console_sql(
                analysis_sql,
                self._table_review_df,
                pd.DataFrame(),
                self._view_definitions_df,
            )
        except Exception as exc:
            QMessageBox.warning(parent, "Selection Lineage", str(exc))
            return
        selection_row = row.copy()
        selection_row["query_id"] = f"{row.get('query_id') or '-'} selection"
        dialog = _SlowQueryLineageDialog(
            selection_row,
            analysis,
            self._table_review_df,
            self._view_definitions_df,
            parent,
        )
        _resize_dialog_to_screen(dialog, 0.94)
        dialog.exec()

    def _open_subquery_extractor(self, editor: QPlainTextEdit, row: pd.Series, parent: QWidget) -> None:
        subqueries = _extract_subquery_rows(editor.toPlainText())
        if subqueries.empty:
            QMessageBox.information(parent, "Extract Subqueries", "No CTEs or subqueries were found in this SQL text.")
            return
        dialog = _SubqueryExtractDialog(
            row,
            subqueries,
            self._table_review_df,
            self._view_definitions_df,
            parent,
            source_editor=editor,
        )
        _resize_dialog_to_screen(dialog, 0.84)
        dialog.exec()

    def _sql_cache_key(self, row: pd.Series) -> str:
        query_id = str(row.get("query_id") or "").strip()
        if query_id:
            return query_id
        return str(abs(hash(self._slow_query_sql_text(row))))

    def _open_selected_query_plan(self) -> None:
        row = self._selected_individual_query_row("Slow Query Plan")
        if row is None or row.empty:
            QMessageBox.information(self, "Explain Plan", "Load slow queries and select a row first.")
            return

        plan_rows = self._selected_plan_rows(row)
        query_id = str(row.get("query_id") or "-")
        if plan_rows.empty:
            QMessageBox.information(
                self,
                "Explain Plan",
                "No full SYS_QUERY_EXPLAIN rows are loaded for this query.\n\n"
                "Reload or refresh the query_explain table from Redshift, then load Slow Queries again.",
            )
            return

        dialog = QDialog(self)
        dialog.setWindowTitle(f"Explain Plan Diagram - {query_id}")
        root = QVBoxLayout(dialog)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)

        title = QLabel(self._query_detail_title(row))
        title.setObjectName("SectionHeader")
        title.setWordWrap(True)
        root.addWidget(title)

        summary = QLabel(_explain_plan_summary(plan_rows))
        summary.setObjectName("Caption")
        summary.setWordWrap(True)
        root.addWidget(summary)

        split = QSplitter(Qt.Vertical)
        split.setChildrenCollapsible(False)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        canvas = _ExplainPlanCanvas()
        canvas.set_dataframe(plan_rows)
        scroll.setWidget(canvas)
        split.addWidget(scroll)

        cols = [
            col for col in (
                "child_query_sequence", "plan_node_id", "plan_parent_id",
                "plan_node", "plan_info",
            )
            if col in plan_rows.columns
        ]
        table = QTableView()
        _configure_table_view(table)
        model = _DataFrameModel(plan_rows[cols] if cols else plan_rows)
        table.setModel(model)
        table.resizeColumnsToContents()
        split.addWidget(table)
        split.setSizes([520, 260])
        root.addWidget(split, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.close)
        root.addWidget(buttons)
        dialog._model = model  # keep model alive while dialog is open
        _resize_dialog_to_screen(dialog, 0.82)
        dialog.exec()

    def _selected_plan_rows(self, row: pd.Series) -> pd.DataFrame:
        if self._explain_df.empty or "query_id" not in self._explain_df.columns:
            return pd.DataFrame()
        query_id = str(row.get("query_id") or "").strip()
        if not query_id:
            return pd.DataFrame()
        query_ids = self._explain_df["query_id"].astype(str).str.strip()
        plan = self._explain_df[query_ids == query_id].copy()
        if plan.empty:
            return plan
        sort_cols = [col for col in ("child_query_sequence", "plan_node_id") if col in plan.columns]
        if sort_cols:
            plan = plan.sort_values(sort_cols, na_position="last")
        return plan.reset_index(drop=True)

    def _on_tree_selection_changed(self) -> None:
        row = self._selected_query_row()
        if row is None or row.empty:
            self._clear_query_detail()
            return
        self._show_query_detail(row)

    def _on_tree_double_clicked(self, item: QTreeWidgetItem, _column: int) -> None:
        payload = item.data(0, Qt.UserRole)
        if isinstance(payload, dict) and payload.get("_is_group_parent"):
            item.setExpanded(not item.isExpanded())
            return
        self._open_selected_query_sql()

    def _show_query_detail(self, row: pd.Series) -> None:
        if row is None or row.empty:
            self._clear_query_detail()
            return
        self._detail_title.setText(self._query_detail_title(row))
        self._detail_summary.setText(self._query_detail_summary(row))
        if bool(row.get("_is_group_parent")):
            self._detail_summary.setText(
                self._detail_summary.text()
                + " | Expand this group and select a query ID for SQL, lineage, diagram, or plan actions."
            )
            return
        sql_text = self._slow_query_sql_text(row)
        if sql_text:
            return
        else:
            self._detail_summary.setText(
                self._detail_summary.text()
                + " | SQL text missing for selected row."
            )

    def _query_detail_title(self, row: pd.Series) -> str:
        if bool(row.get("_is_group_parent")):
            group_id = str(row.get("repeat_group_id") or "UNGROUPED")
            count = int(_safe_float(row.get("_group_query_count")))
            score = _fmt_value("severity_score", row.get("severity_score"))
            elapsed = _fmt_value("elapsed_s", row.get("elapsed_s"))
            risk = _fmt_value("risk_score", row.get("risk_score"))
            return f"Repeat group {group_id} | {count:,} queries | avg score {score} | avg elapsed {elapsed} | avg risk {risk}"
        query_id = str(row.get("query_id") or "-")
        score = _fmt_value("severity_score", row.get("severity_score"))
        elapsed = _fmt_value("elapsed_s", row.get("elapsed_s"))
        risk = _fmt_value("risk_score", row.get("risk_score"))
        return f"Query {query_id} | score {score} | elapsed {elapsed} | risk {risk}"

    def _query_detail_summary(self, row: pd.Series) -> str:
        issue = str(row.get("dominant_issue") or "no issue label")
        reason = str(row.get("severity_reason") or "base risk")
        repeat = str(row.get("repeat_group_id") or "-")
        table_text = str(row.get("sql_tables") or row.get("tables_touched") or "-")
        return f"{issue} | {reason} | repeat group {repeat} | tables {table_text}"

    def _slow_query_sql_text(self, row: pd.Series) -> str:
        for key in ("sql_text_full", "full_sql_text", "query_text_full", "sql_text", "query_text", "query_txt", "text"):
            if key in row.index:
                text = str(row.get(key) or "").strip()
                if text and text.lower() != "nan":
                    return text
        return ""

    def _clear_query_detail(self) -> None:
        self._detail_title.setText("Select a slow query, then use Open SQL or Diagram Selected.")
        self._detail_summary.setText("")

    def _toggle_scoring_controls(self) -> None:
        self._controls.setVisible(not self._controls.isVisible())

    def _severity_score(self, df: pd.DataFrame) -> pd.Series:
        base = _num_series(df, "risk_score").clip(lower=0, upper=180) * 0.55
        repeat_raw = (
            (_num_series(df, "repeat_group_size") / 12.0).clip(lower=0, upper=1) * 0.55
            + _log_scaled(_num_series(df, "repeat_group_runtime_s"), 1.0).clip(lower=0, upper=1) * 0.35
            + _num_series(df, "repeat_similarity_score").clip(lower=0, upper=1) * 0.10
        )
        repeat = repeat_raw.clip(lower=0, upper=1) * self._repeat.value()
        runtime = _log_scaled(_num_series(df, "elapsed_s"), self._runtime.value())
        spill = _log_scaled(_num_series(df, "total_spill"), self._spill.value())
        remote = _num_series(df, "remote_io_ratio").clip(lower=0, upper=1) * self._remote.value()
        skew_raw = pd.concat(
            [
                _num_series(df, "max_data_skewness"),
                _num_series(df, "max_time_skewness"),
            ],
            axis=1,
        ).max(axis=1)
        skew = (skew_raw / 8.0).clip(lower=0, upper=1) * self._skew.value()
        movement_raw = (
            (_num_series(df, "dist_both_cnt") > 0).astype(float)
            + (_num_series(df, "has_nested_loop") > 0).astype(float)
            + (_num_series(df, "dist_total_cnt") / 6.0)
            + (_num_series(df, "bcast_cnt") / 5.0)
        )
        movement = (movement_raw / 2.5).clip(lower=0, upper=1) * self._movement.value()
        external_raw = pd.concat(
            [
                _num_series(df, "external_duration_pct"),
                _num_series(df, "s3_scan_cnt") / 4.0,
            ],
            axis=1,
        ).max(axis=1)
        external = external_raw.clip(lower=0, upper=1) * self._external.value()
        return (base + repeat + runtime + spill + remote + skew + movement + external).round(2)

    def _severity_reason(self, df: pd.DataFrame) -> pd.Series:
        repeat_raw = (
            (_num_series(df, "repeat_group_size") / 12.0).clip(lower=0, upper=1) * 0.55
            + _log_scaled(_num_series(df, "repeat_group_runtime_s"), 1.0).clip(lower=0, upper=1) * 0.35
            + _num_series(df, "repeat_similarity_score").clip(lower=0, upper=1) * 0.10
        )
        repeat = repeat_raw.clip(lower=0, upper=1) * self._repeat.value()
        runtime = _log_scaled(_num_series(df, "elapsed_s"), self._runtime.value())
        spill = _log_scaled(_num_series(df, "total_spill"), self._spill.value())
        remote = _num_series(df, "remote_io_ratio").clip(lower=0, upper=1) * self._remote.value()
        skew = (
            pd.concat(
                [
                    _num_series(df, "max_data_skewness"),
                    _num_series(df, "max_time_skewness"),
                ],
                axis=1,
            ).max(axis=1)
            / 8.0
        ).clip(lower=0, upper=1) * self._skew.value()
        movement = (
            (
                (_num_series(df, "dist_both_cnt") > 0).astype(float)
                + (_num_series(df, "has_nested_loop") > 0).astype(float)
                + (_num_series(df, "dist_total_cnt") / 6.0)
                + (_num_series(df, "bcast_cnt") / 5.0)
            )
            / 2.5
        ).clip(lower=0, upper=1) * self._movement.value()
        external = pd.concat(
            [
                _num_series(df, "external_duration_pct"),
                _num_series(df, "s3_scan_cnt") / 4.0,
            ],
            axis=1,
        ).max(axis=1).clip(lower=0, upper=1) * self._external.value()

        rows = []
        for i in df.index:
            parts = {
                "repeat workload": repeat.loc[i],
                "runtime": runtime.loc[i],
                "spill": spill.loc[i],
                "remote": remote.loc[i],
                "skew": skew.loc[i],
                "join move": movement.loc[i],
                "external": external.loc[i],
            }
            ordered = [name for name, value in sorted(parts.items(), key=lambda x: x[1], reverse=True) if value > 0.1]
            rows.append(", ".join(ordered[:3]) if ordered else "base risk")
        return pd.Series(rows, index=df.index)


class _SlowQueryLineageDialog(QDialog):
    def __init__(
        self,
        row: pd.Series,
        analysis: SQLLensAnalysis,
        table_review: pd.DataFrame,
        view_definitions: pd.DataFrame,
        parent=None,
        *,
        explain_rows: pd.DataFrame | None = None,
        detail_rows: pd.DataFrame | None = None,
    ):
        super().__init__(parent)
        self._row = row
        self._analysis = analysis
        self._table_review = table_review.copy() if table_review is not None else pd.DataFrame()
        self._view_definitions = view_definitions.copy() if view_definitions is not None else pd.DataFrame()
        self._models: list[_DataFrameModel] = []
        query_id = str(row.get("query_id") or "-")
        self.setWindowTitle(f"Slow Query Lineage - {query_id}")

        direct_objects = _lineage_direct_objects(analysis.tables)
        views = _lineage_views(analysis.tables)
        base_tables = _lineage_base_tables(analysis.tables)
        all_joins = _lineage_all_join_rows(analysis, views, self._table_review, self._view_definitions)
        joins = _lineage_join_rows(all_joins, row)
        joins = attach_join_plan_evidence(joins, explain_rows, detail_rows)

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)

        title = QLabel(_lineage_dialog_title(row))
        title.setObjectName("SectionHeader")
        title.setWordWrap(True)
        root.addWidget(title)

        metric_row = QGridLayout()
        metric_row.setHorizontalSpacing(8)
        metrics = [
            ("OBJECTS", len(direct_objects)),
            ("VIEWS", len(views)),
            ("BASE TABLES", len(base_tables)),
            ("JOINS", len(joins)),
        ]
        for col, (label, value) in enumerate(metrics):
            tile = _MetricTile(label)
            tile.set_value(_fmt_int(value))
            metric_row.addWidget(tile, 0, col)
        root.addLayout(metric_row)

        summary_text = _lineage_summary_text(row, analysis, direct_objects, views, base_tables, joins)
        context_notes = []
        if self._view_definitions.empty:
            context_notes.append("view definitions not loaded")
        if self._table_review.empty:
            context_notes.append("table metadata not loaded")
        match_note = _lineage_match_status_note(analysis.tables, self._table_review, self._view_definitions)
        if match_note:
            context_notes.append(match_note)
        if context_notes:
            summary_text += " | " + ", ".join(context_notes)
        summary = QLabel(summary_text)
        summary.setObjectName("Caption")
        summary.setWordWrap(True)
        root.addWidget(summary)

        split = QSplitter(Qt.Vertical)
        split.setChildrenCollapsible(False)
        self._flow = _SqlLensFlowCanvas()
        self._flow.setMinimumHeight(300)
        self._flow.set_analysis(analysis)
        split.addWidget(self._flow)

        tabs = QTabWidget()
        tabs.addTab(
            self._table_tab(direct_objects, SLOW_LINEAGE_OBJECT_COLS, "No direct SQL objects were parsed."),
            "Objects",
        )
        views_table = self._table_tab(
            views,
            SLOW_LINEAGE_VIEW_COLS,
            "No captured view definitions matched this query.",
            on_row_activated=self._open_view_sql,
            tooltip="Double-click a view row to open the captured view SQL.",
        )
        tabs.addTab(views_table, "Views")
        tabs.addTab(
            self._table_tab(
                base_tables,
                SLOW_LINEAGE_BASE_COLS,
                "This query references no views, so every table it touches is "
                "already listed under Objects.",
            ),
            "Behind Views",
        )
        tabs.addTab(
            self._table_tab(
                joins,
                SLOW_LINEAGE_JOIN_COLS,
                "No joins were parsed from this query.",
                on_row_activated=self._open_join_detail,
                tooltip="Double-click a join row for distribution and sort-key detail.",
            ),
            "Joins",
        )
        _set_tab_tooltips(tabs)
        split.addWidget(tabs)
        split.setSizes([360, 420])
        root.addWidget(split, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(self.close)
        root.addWidget(buttons)

    def _table_tab(
        self,
        df: pd.DataFrame,
        preferred_cols: list[str],
        empty_message: str,
        *,
        on_row_activated=None,
        tooltip: str = "",
    ) -> QWidget:
        host = QWidget()
        lay = QVBoxLayout(host)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(6)
        table = QTableView()
        _configure_table_view(table)
        if tooltip:
            table.setToolTip(tooltip)
        top_scroll = _add_external_horizontal_scrollbar(lay, table)
        lay.addWidget(table, 1)
        if df is None or df.empty:
            table.setModel(None)
            label = QLabel(empty_message)
            label.setObjectName("Caption")
            label.setWordWrap(True)
            lay.addWidget(label)
            return host
        display, sort_sources = _table_attribute_display_frame(df)
        cols = [col for col in preferred_cols if col in display.columns]
        cols = cols or list(display.columns)
        model = _DataFrameModel(display[cols], sort_sources=sort_sources, row_df=df)
        table.setModel(model)
        table.resizeColumnsToContents()
        table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        _sync_external_horizontal_scrollbar(top_scroll, table)
        if on_row_activated:
            table.doubleClicked.connect(lambda index, m=model: on_row_activated(m, index))
        self._models.append(model)
        return host

    def _open_join_detail(self, model: _DataFrameModel, index: QModelIndex) -> None:
        if not index.isValid():
            return
        row = model.row_at(index.row())
        dialog = QDialog(self)
        join_no = str(row.get("join_no") or "-")
        dialog.setWindowTitle(f"Join Detail - {join_no}")
        root = QVBoxLayout(dialog)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)

        title = QLabel(f"JOIN {join_no}: {str(row.get('join_type') or 'join').upper()}")
        title.setObjectName("SectionHeader")
        title.setWordWrap(True)
        root.addWidget(title)

        detail = QPlainTextEdit()
        detail.setReadOnly(True)
        detail.setObjectName("Mono")
        detail.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        detail.setPlainText(_join_detail_text(row, self._analysis.tables, self._row))
        root.addWidget(detail, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.close)
        root.addWidget(buttons)
        _resize_dialog_to_screen(dialog, 0.70)
        dialog.exec()

    def _open_view_sql(self, model: _DataFrameModel, index: QModelIndex) -> None:
        if not index.isValid():
            return
        row = model.row_at(index.row())
        object_type = str(row.get("object_type") or "").lower()
        if "view" not in object_type:
            return
        sql_text = str(row.get("source_definition_full") or row.get("source_definition") or "").strip()
        view_name = str(row.get("query_table") or row.get("table_name") or "view")
        if not sql_text:
            QMessageBox.information(self, "View SQL", "No captured SQL definition is available for this view.")
            return
        dialog = QDialog(self)
        dialog.setWindowTitle(f"View SQL - {view_name}")
        root = QVBoxLayout(dialog)
        root.setContentsMargins(12, 12, 12, 12)
        title = QLabel(view_name)
        title.setObjectName("SectionHeader")
        title.setWordWrap(True)
        root.addWidget(title)
        editor = QPlainTextEdit()
        editor.setReadOnly(True)
        editor.setObjectName("Mono")
        editor.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        editor.setPlainText(sql_text)
        root.addWidget(editor, 1)
        actions = QHBoxLayout()
        format_btn = QPushButton("Format SQL")
        format_btn.setObjectName("Primary")
        actions.addWidget(format_btn)
        _add_sql_structure_buttons(
            actions,
            editor,
            dialog,
            row,
            self._table_review,
            self._view_definitions,
        )
        actions.addStretch(1)
        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.close)
        actions.addWidget(buttons)
        root.addLayout(actions)

        def format_sql() -> None:
            _apply_format_sql(editor, dialog)

        format_btn.clicked.connect(format_sql)
        _resize_dialog_to_screen(dialog, 0.82)
        dialog.exec()


class _SubqueryExtractDialog(QDialog):
    def __init__(
        self,
        source_row: pd.Series,
        subqueries: pd.DataFrame,
        table_review: pd.DataFrame,
        view_definitions: pd.DataFrame,
        parent=None,
        *,
        source_editor: QPlainTextEdit | None = None,
    ):
        super().__init__(parent)
        self._source_row = source_row
        self._subqueries = subqueries.copy() if subqueries is not None else pd.DataFrame()
        self._table_review = table_review.copy() if table_review is not None else pd.DataFrame()
        self._view_definitions = view_definitions.copy() if view_definitions is not None else pd.DataFrame()
        self._source_editor = source_editor
        self._model: _DataFrameModel | None = None
        query_id = str(source_row.get("query_id") or "-")
        self.setWindowTitle(f"Extracted Subqueries - {query_id}")

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)

        title = QLabel(f"{len(self._subqueries):,} extracted CTE/subquery block(s)")
        title.setObjectName("SectionHeader")
        root.addWidget(title)

        note = QLabel(
            "Double-click a row to view formatted SQL. Use Analyze Selected to open lineage for that extracted SQL block."
        )
        note.setObjectName("Caption")
        note.setWordWrap(True)
        root.addWidget(note)

        self._table = QTableView()
        _configure_table_view(self._table)
        top_scroll = _add_external_horizontal_scrollbar(root, self._table)
        cols = [col for col in SUBQUERY_EXTRACT_COLS if col in self._subqueries.columns]
        self._model = _DataFrameModel(self._subqueries[cols], row_df=self._subqueries)
        self._table.setModel(self._model)
        selection_model = self._table.selectionModel()
        if selection_model is not None:
            selection_model.currentRowChanged.connect(self._highlight_source_row)
        self._table.resizeColumnsToContents()
        self._table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        _sync_external_horizontal_scrollbar(top_scroll, self._table)
        self._table.doubleClicked.connect(lambda _index: self._open_selected_sql())
        root.addWidget(self._table, 1)

        actions = QHBoxLayout()
        open_sql = QPushButton("Open SQL")
        open_sql.setObjectName("Ghost")
        actions.addWidget(open_sql)
        analyze = QPushButton("Analyze Selected")
        analyze.setObjectName("Primary")
        actions.addWidget(analyze)
        actions.addStretch(1)
        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(self.close)
        actions.addWidget(buttons)
        root.addLayout(actions)
        open_sql.clicked.connect(self._open_selected_sql)
        analyze.clicked.connect(self._analyze_selected)
        if self._table.model() is not None and self._table.model().rowCount() > 0:
            self._table.selectRow(0)

    def _highlight_source_row(self, current, _previous=None) -> None:
        if self._source_editor is None or self._model is None or current is None or not current.isValid():
            return
        row = self._model.row_at(current.row())
        try:
            start = int(row.get("source_start"))
            end = int(row.get("source_end"))
        except (TypeError, ValueError):
            return
        text_length = len(self._source_editor.toPlainText())
        if start < 0 or end <= start or start >= text_length:
            return
        cursor = self._source_editor.textCursor()
        cursor.setPosition(min(start, text_length))
        cursor.setPosition(min(end, text_length), QTextCursor.KeepAnchor)
        self._source_editor.setTextCursor(cursor)
        self._source_editor.ensureCursorVisible()

    def _selected_row(self) -> pd.Series | None:
        if not self._model:
            return None
        selection = self._table.selectionModel()
        indexes = selection.selectedRows() if selection is not None else []
        if indexes:
            return self._model.row_at(indexes[0].row())
        current = self._table.currentIndex()
        if not current.isValid():
            return None
        return self._model.row_at(current.row())

    def _analyze_selected(self) -> None:
        row = self._selected_row()
        if row is None or row.empty:
            QMessageBox.information(self, "Analyze Subquery", "Select a subquery row first.")
            return
        sql_text = str(row.get("sql_text") or "").strip()
        if not sql_text:
            QMessageBox.information(self, "Analyze Subquery", "The selected row has no SQL text.")
            return
        try:
            analysis = analyze_console_sql(sql_text, self._table_review, pd.DataFrame(), self._view_definitions)
        except Exception as exc:
            QMessageBox.warning(self, "Analyze Subquery", str(exc))
            return
        lineage_row = self._source_row.copy()
        lineage_row["query_id"] = f"{self._source_row.get('query_id') or '-'} {row.get('kind')} {row.get('subquery_no')}"
        dialog = _SlowQueryLineageDialog(lineage_row, analysis, self._table_review, self._view_definitions, self)
        _resize_dialog_to_screen(dialog, 0.94)
        dialog.exec()

    def _open_selected_sql(self) -> None:
        row = self._selected_row()
        if row is None or row.empty:
            QMessageBox.information(self, "Subquery SQL", "Select a subquery row first.")
            return
        sql_text = str(row.get("sql_text") or "").strip()
        if not sql_text:
            QMessageBox.information(self, "Subquery SQL", "The selected row has no SQL text.")
            return
        dialog = QDialog(self)
        label = f"{row.get('kind') or 'block'} {row.get('subquery_no') or ''}".strip()
        dialog.setWindowTitle(f"Recursive SQL Analysis - {label}")
        root = QVBoxLayout(dialog)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(6)
        lens = _SqlLensPage(dialog)
        lens.set_context(self._table_review, pd.DataFrame(), self._view_definitions)
        lens.analyze_external_sql(sql_text, f"{label} from query {self._source_row.get('query_id') or '-'}")
        root.addWidget(lens, 1)
        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.close)
        root.addWidget(buttons)
        _resize_dialog_to_screen(dialog, 0.96)
        dialog.exec()


class _SlowQueryFilterDialog(QDialog):
    _MIN_FIELDS = [
        ("elapsed_s", "Minimum Runtime Minutes", "minutes"),
        ("risk_score", "Minimum Risk Score", "number"),
        ("plan_node_count", "Minimum Full Plan Count", "number"),
        ("tables_touched", "Minimum Tables Touched", "number"),
        ("input_rows", "Minimum Input Rows", "number"),
        ("output_rows", "Minimum Output Rows", "number"),
    ]

    def __init__(self, df: pd.DataFrame, filters: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Slow Query Filters")
        self._df = df.copy() if df is not None else pd.DataFrame()
        self._filters = {
            "minimums": dict((filters or {}).get("minimums") or {}),
            "values": {
                str(col): set(str(v) for v in values)
                for col, values in dict((filters or {}).get("values") or {}).items()
            },
        }
        self._current_value_col = ""
        self._minimum_inputs: dict[str, QLineEdit] = {}

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)

        title = QLabel("SLOW QUERY FILTERS")
        title.setObjectName("SectionHeader")
        root.addWidget(title)
        note = QLabel("Numeric fields accept 1000000, 1,000,000, 1M, 1.5B, or 10K. Value filters are local to the loaded rows.")
        note.setObjectName("Caption")
        note.setWordWrap(True)
        root.addWidget(note)

        tabs = QTabWidget()
        numeric = QWidget()
        numeric_grid = QGridLayout(numeric)
        numeric_grid.setContentsMargins(8, 8, 8, 8)
        numeric_grid.setHorizontalSpacing(10)
        numeric_grid.setVerticalSpacing(8)
        for row_no, (col, label, unit) in enumerate(self._MIN_FIELDS):
            numeric_grid.addWidget(QLabel(label), row_no, 0)
            line = QLineEdit()
            line.setPlaceholderText("e.g. 10" if unit == "minutes" else "e.g. 1M")
            value = self._filters["minimums"].get(col)
            if value not in (None, ""):
                if col == "elapsed_s":
                    line.setText(str(float(value) / 60.0).rstrip("0").rstrip("."))
                else:
                    line.setText(str(value))
            numeric_grid.addWidget(line, row_no, 1)
            self._minimum_inputs[col] = line
        numeric_grid.setColumnStretch(2, 1)
        tabs.addTab(numeric, "Minimums")

        values_tab = QWidget()
        values_lay = QVBoxLayout(values_tab)
        values_lay.setContentsMargins(8, 8, 8, 8)
        selector = QHBoxLayout()
        selector.addWidget(QLabel("Field"))
        self._value_field = QComboBox()
        self._value_field.addItems(_slow_filter_value_columns(self._df))
        selector.addWidget(self._value_field, 1)
        clear_field = QPushButton("Clear Field")
        clear_field.setObjectName("Ghost")
        selector.addWidget(clear_field)
        values_lay.addLayout(selector)
        self._value_list = QListWidget()
        self._value_list.setSelectionMode(QAbstractItemView.NoSelection)
        values_lay.addWidget(self._value_list, 1)
        tabs.addTab(values_tab, "Values")
        _set_tab_tooltips(tabs)
        root.addWidget(tabs, 1)

        actions = QHBoxLayout()
        clear_all = QPushButton("Clear All")
        clear_all.setObjectName("Ghost")
        actions.addWidget(clear_all)
        actions.addStretch(1)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)
        actions.addWidget(buttons)
        root.addLayout(actions)

        self._value_field.currentTextChanged.connect(self._on_value_field_changed)
        clear_field.clicked.connect(self._clear_current_value_field)
        clear_all.clicked.connect(self._clear_all)
        if self._value_field.count():
            self._on_value_field_changed(self._value_field.currentText())

    def filters(self) -> dict:
        return {
            "minimums": dict(self._filters.get("minimums") or {}),
            "values": {
                col: set(values)
                for col, values in dict(self._filters.get("values") or {}).items()
                if values
            },
        }

    def _on_value_field_changed(self, col: str) -> None:
        self._save_current_values()
        self._current_value_col = str(col or "")
        self._load_values_for_col(self._current_value_col)

    def _load_values_for_col(self, col: str) -> None:
        self._value_list.clear()
        if not col or col not in self._df.columns:
            return
        selected = set(self._filters["values"].get(col, set()))
        values = _slow_filter_unique_values(self._df[col])
        for value, count in values:
            item = QListWidgetItem(f"{value} ({count:,})")
            item.setData(Qt.UserRole, value)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked if value in selected else Qt.Unchecked)
            self._value_list.addItem(item)

    def _save_current_values(self) -> None:
        col = self._current_value_col
        if not col:
            return
        selected: set[str] = set()
        for i in range(self._value_list.count()):
            item = self._value_list.item(i)
            if item.checkState() == Qt.Checked:
                selected.add(str(item.data(Qt.UserRole)))
        if selected:
            self._filters["values"][col] = selected
        else:
            self._filters["values"].pop(col, None)

    def _clear_current_value_field(self) -> None:
        col = self._current_value_col or self._value_field.currentText()
        if col:
            self._filters["values"].pop(str(col), None)
        for i in range(self._value_list.count()):
            self._value_list.item(i).setCheckState(Qt.Unchecked)

    def _clear_all(self) -> None:
        self._filters = {"minimums": {}, "values": {}}
        for line in self._minimum_inputs.values():
            line.clear()
        self._load_values_for_col(self._current_value_col or self._value_field.currentText())

    def _accept(self) -> None:
        self._save_current_values()
        minimums: dict[str, float] = {}
        for col, _label, unit in self._MIN_FIELDS:
            raw = self._minimum_inputs[col].text().strip()
            if not raw:
                continue
            value = _parse_short_number(raw)
            if value is None:
                QMessageBox.warning(self, "Slow Query Filters", f"Could not parse numeric value for {col}: {raw}")
                return
            minimums[col] = value * 60.0 if unit == "minutes" else value
        self._filters["minimums"] = minimums
        self.accept()


class _ExplainPlanCanvas(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._df = pd.DataFrame()
        self._rects: dict[int, QRectF] = {}
        self.setMinimumHeight(360)

    def set_dataframe(self, df: pd.DataFrame) -> None:
        self._df = df.copy() if df is not None else pd.DataFrame()
        self._layout_nodes()
        self.update()

    def _layout_nodes(self) -> None:
        self._rects = {}
        if self._df.empty:
            self.setMinimumSize(900, 360)
            return

        rows = self._normalized_rows()
        children: dict[int | None, list[dict]] = {}
        ids: set[int] = set()
        for row in rows:
            node_id = row["id"]
            ids.add(node_id)
            parent = row["parent"]
            children.setdefault(parent if parent in ids or parent is not None else parent, []).append(row)

        row_by_id = {row["id"]: row for row in rows}

        def depth_for(node_id: int, seen: set[int] | None = None) -> int:
            seen = set(seen or set())
            if node_id in seen:
                return 0
            seen.add(node_id)
            parent = row_by_id.get(node_id, {}).get("parent")
            if parent is None or parent not in row_by_id:
                return 0
            return depth_for(parent, seen) + 1

        depth_groups: dict[int, list[dict]] = {}
        for row in rows:
            depth_groups.setdefault(depth_for(row["id"]), []).append(row)

        node_w = 310
        node_h = 66
        col_gap = 76
        row_gap = 18
        max_bottom = 0
        max_right = 0
        for depth in sorted(depth_groups):
            group = sorted(depth_groups[depth], key=lambda row: row["order"])
            x = 24 + depth * (node_w + col_gap)
            for idx, row in enumerate(group):
                y = 32 + idx * (node_h + row_gap)
                self._rects[row["id"]] = QRectF(x, y, node_w, node_h)
                max_bottom = max(max_bottom, int(y + node_h + 32))
                max_right = max(max_right, int(x + node_w + 32))

        self.setMinimumSize(max(920, max_right), max(360, max_bottom))

    def _normalized_rows(self) -> list[dict]:
        rows: list[dict] = []
        if self._df.empty:
            return rows
        for idx, row in self._df.reset_index(drop=True).iterrows():
            node_id = _safe_int(row.get("plan_node_id"), idx + 1)
            parent = _safe_int_or_none(row.get("plan_parent_id"))
            rows.append(
                {
                    "id": node_id,
                    "parent": parent,
                    "order": idx,
                    "label": str(row.get("plan_node") or "Plan node").strip(),
                    "info": str(row.get("plan_info") or "").strip(),
                }
            )
        return rows

    def paintEvent(self, _event) -> None:
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        p.fillRect(self.rect(), QColor(PALETTE.bg_1))
        if self._df.empty:
            p.setPen(QColor(PALETTE.text_2))
            p.setFont(QFont("Inter", 11))
            p.drawText(self.rect(), Qt.AlignCenter, "No full explain plan rows loaded.")
            return

        rows = self._normalized_rows()
        for row in rows:
            parent = row["parent"]
            if parent not in self._rects or row["id"] not in self._rects:
                continue
            self._draw_edge(p, self._rects[parent], self._rects[row["id"]], row)

        for row in rows:
            rect = self._rects.get(row["id"])
            if rect is not None:
                self._draw_node(p, rect, row)

    def _draw_edge(self, p: QPainter, src: QRectF, dst: QRectF, row: dict) -> None:
        color = _plan_color(row.get("label", ""), row.get("info", ""))
        start = QPointF(src.right(), src.center().y())
        end = QPointF(dst.left(), dst.center().y())
        path = QPainterPath(start)
        dx = max(48.0, (end.x() - start.x()) * 0.46)
        path.cubicTo(QPointF(start.x() + dx, start.y()), QPointF(end.x() - dx, end.y()), end)
        pen = QPen(QColor(color), 2.0 if _plan_severity(row.get("label", ""), row.get("info", "")) == "crit" else 1.2)
        pen.setCapStyle(Qt.RoundCap)
        p.setPen(pen)
        p.setBrush(Qt.NoBrush)
        p.drawPath(path)

    def _draw_node(self, p: QPainter, rect: QRectF, row: dict) -> None:
        label = str(row.get("label") or "Plan node")
        info = str(row.get("info") or "")
        severity = _plan_severity(label, info)
        color = QColor(_plan_color(label, info))

        fill = QColor(PALETTE.bg_2)
        if severity == "crit":
            fill = QColor("#2B1520")
        elif severity == "warn":
            fill = QColor("#2B2417")

        p.setPen(QPen(color, 1.4))
        p.setBrush(fill)
        p.drawRoundedRect(rect, 8, 8)
        p.setPen(Qt.NoPen)
        p.setBrush(color)
        p.drawRoundedRect(QRectF(rect.left(), rect.top(), 5, rect.height()), 3, 3)

        node_id = str(row.get("id") or "")
        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("Inter", 7, QFont.Bold))
        p.drawText(QRectF(rect.left() + 12, rect.top() + 6, 72, 12), Qt.AlignLeft, f"NODE {node_id}")
        p.drawText(QRectF(rect.right() - 82, rect.top() + 6, 72, 12), Qt.AlignRight, severity.upper())

        p.setPen(QColor(PALETTE.text_0))
        p.setFont(QFont("Inter", 9, QFont.DemiBold))
        p.drawText(QRectF(rect.left() + 12, rect.top() + 22, rect.width() - 22, 17), Qt.AlignLeft, _clip(label, 42))

        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("Inter", 7))
        p.drawText(QRectF(rect.left() + 12, rect.top() + 42, rect.width() - 22, 16), Qt.AlignLeft, _clip(info, 58))


class _SeveritySlider(QWidget):
    def __init__(self, label: str, low: int, high: int, value: int, parent=None):
        super().__init__(parent)
        self.setMaximumHeight(36)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(0)
        top = QHBoxLayout()
        name = QLabel(label)
        name.setObjectName("SectionHeader")
        name.setStyleSheet("font-size:9px;")
        self._value = QLabel(str(value))
        self._value.setObjectName("Mono")
        self._value.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self._value.setMinimumWidth(24)
        self._value.setStyleSheet("font-size:10px;")
        top.addWidget(name)
        top.addStretch(1)
        top.addWidget(self._value)
        lay.addLayout(top)
        self._slider = QSlider(Qt.Horizontal)
        self._slider.setMaximumHeight(16)
        self._slider.setRange(low, high)
        self._slider.setValue(value)
        self._slider.valueChanged.connect(lambda v: self._value.setText(str(v)))
        lay.addWidget(self._slider)

    def value(self) -> int:
        return int(self._slider.value())

    def on_change(self, callback) -> None:
        self._slider.valueChanged.connect(callback)


class _ExecutiveFixQueryView(QWidget):
    """One decision hierarchy; detailed query/table evidence is secondary."""

    _DISPLAY_COLUMNS = [
        "priority",
        "initiative",
        "decision",
        "scope",
        "runtime_in_scope",
        "readiness",
    ]

    def __init__(self, parent=None):
        super().__init__(parent)
        self._initiatives = pd.DataFrame()
        self._actions = pd.DataFrame()
        self._rewrites = pd.DataFrame()
        self._current = pd.Series(dtype=object)
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(10)

        purpose = QLabel(
            "This page consolidates individual findings into decisions. Runtime shown is captured workload in scope, "
            "not a promised savings estimate."
        )
        purpose.setObjectName("Caption")
        purpose.setWordWrap(True)
        root.addWidget(purpose)

        metrics = QHBoxLayout()
        metrics.setSpacing(8)
        self._initiative_tile = _MetricTile("DECISIONS")
        self._query_tile = _MetricTile("QUERIES IN SCOPE")
        self._table_tile = _MetricTile("TABLES IN SCOPE")
        self._runtime_tile = _MetricTile("CAPTURED RUNTIME IN SCOPE")
        for tile in (
            self._initiative_tile,
            self._query_tile,
            self._table_tile,
            self._runtime_tile,
        ):
            metrics.addWidget(tile)
        root.addLayout(metrics)

        section = QHBoxLayout()
        section_title = QLabel("RECOMMENDED INITIATIVES")
        section_title.setObjectName("SectionHeader")
        section.addWidget(section_title)
        section.addStretch(1)
        self._status = QLabel("Load Actions to build the decision brief.")
        self._status.setObjectName("Caption")
        section.addWidget(self._status)
        root.addLayout(section)

        self._table = QTableView()
        _configure_table_view(self._table)
        self._table.setMinimumHeight(190)
        self._table.setMaximumHeight(300)
        self._table.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self._table.clicked.connect(self._select_index)
        root.addWidget(self._table)
        self._model: _DataFrameModel | None = None

        self._detail = QFrame()
        self._detail.setObjectName("Card")
        detail = QVBoxLayout(self._detail)
        detail.setContentsMargins(14, 12, 14, 12)
        detail.setSpacing(7)
        detail_top = QHBoxLayout()
        self._detail_title = QLabel("SELECT AN INITIATIVE")
        self._detail_title.setObjectName("H1")
        self._detail_title.setWordWrap(True)
        detail_top.addWidget(self._detail_title, 1)
        self._readiness = QLabel("")
        self._readiness.setProperty("chip", True)
        self._readiness.setProperty("severity", "info")
        self._readiness.setAlignment(Qt.AlignCenter)
        detail_top.addWidget(self._readiness, 0, Qt.AlignTop)
        detail.addLayout(detail_top)
        self._detail_decision = QLabel("")
        self._detail_decision.setWordWrap(True)
        decision_font = self._detail_decision.font()
        decision_font.setBold(True)
        self._detail_decision.setFont(decision_font)
        detail.addWidget(self._detail_decision)
        self._detail_reason = QLabel("")
        self._detail_reason.setObjectName("Caption")
        self._detail_reason.setWordWrap(True)
        detail.addWidget(self._detail_reason)
        self._detail_scope = QLabel("")
        self._detail_scope.setObjectName("Mono")
        self._detail_scope.setWordWrap(True)
        detail.addWidget(self._detail_scope)
        evidence_row = QHBoxLayout()
        self._detail_evidence = QLabel("")
        self._detail_evidence.setObjectName("Caption")
        self._detail_evidence.setWordWrap(True)
        evidence_row.addWidget(self._detail_evidence, 1)
        self._evidence_btn = QPushButton("Technical Evidence")
        self._evidence_btn.setObjectName("Ghost")
        self._evidence_btn.setEnabled(False)
        self._evidence_btn.clicked.connect(self._open_evidence)
        evidence_row.addWidget(self._evidence_btn, 0, Qt.AlignBottom)
        detail.addLayout(evidence_row)
        root.addWidget(self._detail)
        root.addStretch(1)

    def set_dataframes(
        self,
        actions: pd.DataFrame,
        rewrites: pd.DataFrame,
        slow_queries: pd.DataFrame | None = None,
    ) -> None:
        self._actions = actions.copy() if actions is not None else pd.DataFrame()
        self._rewrites = rewrites.copy() if rewrites is not None else pd.DataFrame()
        self._initiatives = _build_fix_query_initiatives(self._actions, self._rewrites, slow_queries)
        if self._initiatives.empty:
            self._table.setModel(None)
            self._model = None
            self._status.setText("No qualifying actions or rewrites are loaded for this snapshot.")
            for tile in (self._initiative_tile, self._query_tile, self._table_tile, self._runtime_tile):
                tile.set_value("-")
            self._clear_detail()
            return
        display = self._initiatives[self._DISPLAY_COLUMNS]
        self._model = _DataFrameModel(display, row_df=self._initiatives)
        self._table.setModel(self._model)
        self._table.resizeColumnsToContents()
        header = self._table.horizontalHeader()
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self._status.setText(
            f"{len(self._initiatives):,} decision(s) consolidated from "
            f"{len(self._actions) + len(self._rewrites):,} technical finding(s)."
        )
        query_ids = {
            query_id
            for values in self._initiatives["query_ids"]
            for query_id in values
        }
        table_keys = {
            table_key
            for values in self._initiatives["table_keys"]
            for table_key in values
        }
        runtime_by_query: dict[str, float] = {}
        for values in self._initiatives["query_runtime_map"]:
            for query_id, elapsed in values.items():
                runtime_by_query[query_id] = max(runtime_by_query.get(query_id, 0.0), float(elapsed or 0.0))
        runtime = sum(runtime_by_query.get(query_id, 0.0) for query_id in query_ids)
        self._initiative_tile.set_value(f"{len(self._initiatives):,}")
        self._query_tile.set_value(f"{len(query_ids):,}")
        self._table_tile.set_value(f"{len(table_keys):,}")
        self._runtime_tile.set_value(_fmt_seconds(runtime))
        self._table.selectRow(0)
        self._show_row(self._initiatives.iloc[0])

    def _select_index(self, index: QModelIndex) -> None:
        if self._model is None:
            return
        self._show_row(self._model.row_at(index.row()))

    def _show_row(self, row: pd.Series) -> None:
        self._current = row
        self._detail_title.setText(str(row.get("initiative") or "INITIATIVE"))
        self._readiness.setText(str(row.get("readiness") or "REVIEW").upper())
        self._detail_decision.setText(f"Decision: {row.get('decision') or '-'}")
        self._detail_reason.setText(str(row.get("executive_reason") or ""))
        self._detail_scope.setText(
            f"Scope: {row.get('scope') or '-'}    |    Captured runtime represented: "
            f"{row.get('runtime_in_scope') or '-'}"
        )
        self._detail_evidence.setText(
            f"Strongest evidence: {row.get('strongest_evidence') or 'No evidence text was loaded.'}\n"
            f"First next step: {row.get('next_step') or '-'}"
        )
        self._evidence_btn.setEnabled(True)

    def _clear_detail(self) -> None:
        self._current = pd.Series(dtype=object)
        self._detail_title.setText("NO DECISION BRIEF LOADED")
        self._readiness.setText("")
        self._detail_decision.setText("")
        self._detail_reason.setText("")
        self._detail_scope.setText("")
        self._detail_evidence.setText("")
        self._evidence_btn.setEnabled(False)

    def _open_evidence(self) -> None:
        if self._current.empty:
            return
        dialog = QDialog(self)
        dialog.setWindowTitle(f"Technical Evidence — {self._current.get('initiative') or 'Fix Query'}")
        root = QVBoxLayout(dialog)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)
        note = QLabel(
            "This is the supporting proof behind the selected decision. It is intentionally separated "
            "from the executive brief. Candidate SQL and design guidance require DBA review."
        )
        note.setObjectName("Caption")
        note.setWordWrap(True)
        root.addWidget(note)
        evidence = _fix_query_evidence_frame(self._current, self._actions, self._rewrites)
        table = QTableView()
        _configure_table_view(table)
        model = _DataFrameModel(evidence)
        table.setModel(model)
        table.resizeColumnsToContents()
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        root.addWidget(table, 1)
        actions = QHBoxLayout()
        query_ids = [str(value) for value in self._current.get("query_ids", []) if str(value)]
        copy_ids = QPushButton("Copy Query IDs")
        copy_ids.setObjectName("Ghost")
        copy_ids.setEnabled(bool(query_ids))
        copy_ids.clicked.connect(lambda: QApplication.clipboard().setText(",".join(query_ids)))
        actions.addWidget(copy_ids)
        actions.addStretch(1)
        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.close)
        actions.addWidget(buttons)
        root.addLayout(actions)
        _resize_dialog_to_screen(dialog, 0.90)
        dialog.exec()


class _ActionCards(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(8)
        head = QHBoxLayout()
        title = QLabel("ACTION SUMMARY")
        title.setObjectName("SectionHeader")
        head.addWidget(title)
        head.addStretch(1)
        root.addLayout(head)
        self._status = QLabel(
            "Actions are grouped by type so repeated maintenance recommendations do not consume the whole panel."
        )
        self._status.setObjectName("Caption")
        self._status.setWordWrap(True)
        root.addWidget(self._status)
        self._table = QTableView()
        _configure_table_view(self._table)
        self._top_scroll = _add_external_horizontal_scrollbar(root, self._table)
        root.addWidget(self._table, 1)
        self._model: _DataFrameModel | None = None

    def set_dataframe(self, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            self._table.setModel(None)
            self._model = None
            self._status.setText(
                "No prioritized actions are loaded. Click Load Actions to query the local DuckDB action views."
            )
            return
        summary = _action_summary_frame(df)
        cols = [
            "action_type",
            "action_count",
            "critical_count",
            "warning_count",
            "top_score",
            "top_subject",
            "top_reason",
        ]
        self._model = _DataFrameModel(summary[[col for col in cols if col in summary.columns]], row_df=summary)
        self._table.setModel(self._model)
        self._table.resizeColumnsToContents()
        self._table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        _sync_external_horizontal_scrollbar(self._top_scroll, self._table)
        self._status.setText(
            f"{len(df):,} action rows grouped into {len(summary):,} action type(s). "
            "Open the Action Ledger on the right for the sortable row-level detail."
        )


class _ActionCard(QFrame):
    def __init__(self, row: pd.Series):
        super().__init__()
        self.setObjectName("Card")
        lay = QVBoxLayout(self)
        lay.setContentsMargins(14, 12, 14, 12)
        lay.setSpacing(8)

        top = QHBoxLayout()
        rank = QLabel(f"#{int(row.get('priority_rank') or 0):02d}")
        rank.setObjectName("Mono")
        rank.setStyleSheet("color:#8BA4FF; font-size:12px; font-weight:700;")
        top.addWidget(rank, 0)
        sev = str(row.get("severity") or "info")
        chip = QLabel(sev.upper())
        chip.setProperty("chip", True)
        chip.setProperty("severity", sev)
        chip.setAlignment(Qt.AlignCenter)
        top.addWidget(chip, 0)
        action_type = QLabel(str(row.get("action_type") or "").upper())
        action_type.setObjectName("SectionHeader")
        top.addWidget(action_type, 0)
        top.addStretch(1)
        score = QLabel(f"{float(row.get('action_score') or 0):.0f}")
        score.setObjectName("Mono")
        score.setStyleSheet(f"color:{PALETTE.text_2}; font-size:11px;")
        top.addWidget(score, 0)
        lay.addLayout(top)

        title = QLabel(str(row.get("what_to_do") or "-"))
        title.setObjectName("H1")
        title.setWordWrap(True)
        lay.addWidget(title)

        subject = QLabel(str(row.get("subject") or "cluster"))
        subject.setObjectName("Mono")
        subject.setStyleSheet(f"color:{PALETTE.text_2}; font-size:11px;")
        subject.setWordWrap(True)
        lay.addWidget(subject)

        for label, key in (("WHY NOW", "why_now"), ("EVIDENCE", "evidence")):
            head = QLabel(label)
            head.setObjectName("SectionHeader")
            lay.addWidget(head)
            body = QLabel(str(row.get(key) or ""))
            body.setObjectName("Caption")
            body.setWordWrap(True)
            lay.addWidget(body)

        hint = str(row.get("sql_hint") or "").strip()
        if hint:
            sql = QLabel(hint)
            sql.setObjectName("Mono")
            sql.setWordWrap(True)
            sql.setStyleSheet(
                "background:#0F1420; border:1px solid #232C42; "
                "border-radius:6px; padding:8px; color:#C4CCE0; font-size:10px;"
            )
            lay.addWidget(sql)


class _RewriteCards(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(8)
        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._scroll.setFrameShape(QFrame.NoFrame)
        self._host = QWidget()
        self._lay = QVBoxLayout(self._host)
        self._lay.setContentsMargins(0, 0, 0, 0)
        self._lay.setSpacing(8)
        self._scroll.setWidget(self._host)
        root.addWidget(self._scroll, 1)

    def set_dataframe(self, df: pd.DataFrame) -> None:
        _clear_layout(self._lay)
        if df is None or df.empty:
            empty = QLabel(
                "No rewrite opportunities are loaded. Click Load Actions to query the local rewrite view."
            )
            empty.setObjectName("Caption")
            empty.setWordWrap(True)
            self._lay.addWidget(empty)
            self._lay.addStretch(1)
            return
        for _, row in df.head(20).iterrows():
            self._lay.addWidget(_RewriteCard(row))
        self._lay.addStretch(1)


class _RewriteCard(QFrame):
    def __init__(self, row: pd.Series):
        super().__init__()
        self.setObjectName("Card")
        lay = QVBoxLayout(self)
        lay.setContentsMargins(14, 12, 14, 12)
        lay.setSpacing(8)

        top = QHBoxLayout()
        number = QLabel(f"#{int(row.get('opportunity_no') or 0):02d}")
        number.setObjectName("Mono")
        number.setStyleSheet("color:#8BA4FF; font-size:12px; font-weight:700;")
        top.addWidget(number, 0)
        sev = str(row.get("severity") or "info")
        chip = QLabel(sev.upper())
        chip.setProperty("chip", True)
        chip.setProperty("severity", sev)
        chip.setAlignment(Qt.AlignCenter)
        top.addWidget(chip, 0)
        top.addStretch(1)
        score = QLabel(f"impact {float(row.get('impact_score') or 0):.0f}")
        score.setObjectName("Mono")
        score.setStyleSheet(f"color:{PALETTE.text_2}; font-size:11px;")
        top.addWidget(score, 0)
        lay.addLayout(top)

        title = QLabel(str(row.get("title") or "-"))
        title.setObjectName("H1")
        title.setWordWrap(True)
        lay.addWidget(title)

        subject = QLabel(str(row.get("subject") or "cluster"))
        subject.setObjectName("Mono")
        subject.setStyleSheet(f"color:{PALETTE.text_2}; font-size:11px;")
        subject.setWordWrap(True)
        lay.addWidget(subject)

        for label, key in (
            ("TRIGGER", "trigger"),
            ("REWRITE SHAPE", "rewrite_shape"),
            ("WHY IT MATTERS", "why_it_matters"),
        ):
            head = QLabel(label)
            head.setObjectName("SectionHeader")
            lay.addWidget(head)
            text = QLabel(str(row.get(key) or ""))
            text.setObjectName("Caption" if key != "rewrite_shape" else "Mono")
            text.setWordWrap(True)
            if key == "rewrite_shape":
                text.setStyleSheet(f"color:{PALETTE.text_0}; font-size:11px;")
            lay.addWidget(text)

        candidate = str(row.get("candidate_sql") or "").strip()
        if candidate:
            sql = QLabel(candidate)
            sql.setObjectName("Mono")
            sql.setWordWrap(True)
            sql.setStyleSheet(
                "background:#0F1420; border:1px solid #232C42; "
                "border-radius:6px; padding:8px; color:#C4CCE0; font-size:10px;"
            )
            lay.addWidget(sql)


def _lineage_dialog_title(row: pd.Series) -> str:
    query_id = str(row.get("query_id") or "-")
    elapsed = _fmt_value("elapsed_s", row.get("elapsed_s"))
    risk = _fmt_value("risk_score", row.get("risk_score"))
    user = str(row.get("user_name") or "-")
    return f"QUERY {query_id} | USER {user} | ELAPSED {elapsed} | RISK {risk}"


def _lineage_summary_text(
    row: pd.Series,
    analysis: SQLLensAnalysis,
    direct_objects: pd.DataFrame,
    views: pd.DataFrame,
    base_tables: pd.DataFrame,
    joins: pd.DataFrame,
) -> str:
    parse = "parsed" if analysis.parse_ok else f"parse issue: {analysis.parse_error or 'unknown'}"
    movement = []
    bcast = _safe_float(row.get("bcast_cnt"))
    dist_both = _safe_float(row.get("dist_both_cnt"))
    dist_total = _safe_float(row.get("dist_total_cnt"))
    if bcast:
        movement.append(f"broadcast {bcast:.0f}")
    if dist_both:
        movement.append(f"DS_DIST_BOTH {dist_both:.0f}")
    if dist_total:
        movement.append(f"movement {dist_total:.0f}")
    if not movement:
        movement.append("no captured distribution movement")
    return (
        f"{parse} | direct objects {len(direct_objects):,} | views {len(views):,} | "
        f"grand total base tables {len(base_tables):,} | joins {len(joins):,} | "
        f"plan flags: {', '.join(movement)}"
    )


def _lineage_match_status_note(
    tables: pd.DataFrame,
    table_review: pd.DataFrame,
    view_definitions: pd.DataFrame,
) -> str:
    if tables is None or tables.empty or "match_status" not in tables.columns:
        return ""
    statuses = tables["match_status"].astype(str).str.lower()
    not_found = int((statuses == "not found").sum())
    if not not_found:
        return ""
    total = len(tables)
    if table_review is None or table_review.empty:
        return f"{not_found}/{total} objects not found because Table Review metadata is not loaded in this report"
    if view_definitions is None or view_definitions.empty and statuses.isin(["view", "ambiguous view", "not found"]).any():
        return f"{not_found}/{total} objects not found; View Definitions are not loaded and table names may be hidden behind views"
    return f"{not_found}/{total} objects not found; check database/schema qualification and that Table Review is from the same snapshot"


def _lineage_direct_objects(tables: pd.DataFrame) -> pd.DataFrame:
    if tables is None or tables.empty:
        return pd.DataFrame()
    out = tables.copy()
    if "object_type" in out.columns:
        out = out[~out["object_type"].astype(str).str.startswith("view_component", na=False)].copy()
    return _lineage_dedupe(out, ["object_type", "query_table", "alias"])


def _lineage_views(tables: pd.DataFrame) -> pd.DataFrame:
    if tables is None or tables.empty or "object_type" not in tables.columns:
        return pd.DataFrame()
    object_type = tables["object_type"].astype(str)
    out = tables[object_type.isin(["view", "view_component_view"])].copy()
    return _lineage_dedupe(out, ["query_table", "alias", "component_of"])


def _lineage_base_tables(tables: pd.DataFrame) -> pd.DataFrame:
    """Physical tables reached only by expanding a view.

    "Base Tables" exists to answer *what does this query really touch that the
    SQL does not name?* - the tables hidden behind a view. A table written
    directly in the FROM clause is already listed under Objects, so including
    it here too made a one-table query appear twice, as if it had two levels of
    lineage. Only view components qualify.

    When the query references no views at all, this is legitimately empty and
    the dialog says so.
    """
    if tables is None or tables.empty or "object_type" not in tables.columns:
        return pd.DataFrame()
    object_type = tables["object_type"].astype(str)
    out = tables[object_type == "view_component_table"].copy()
    if out.empty:
        return out
    out["_base_key"] = out.apply(_lineage_base_key, axis=1)
    out = _lineage_dedupe(out, ["_base_key"])
    if "table_attention_score" in out.columns:
        out["_sort"] = pd.to_numeric(out["table_attention_score"], errors="coerce").fillna(0)
        out = out.sort_values("_sort", ascending=False).drop(columns="_sort")
    return out.drop(columns=["_base_key"], errors="ignore").reset_index(drop=True)


def _lineage_base_key(row: pd.Series) -> str:
    parts = [
        str(row.get("source_db") or "").strip().lower(),
        str(row.get("schema_name") or "").strip().lower(),
        str(row.get("table_name") or "").strip().lower(),
    ]
    key = ".".join(part for part in parts if part)
    return key or str(row.get("query_table") or "").strip().lower()


def _lineage_dedupe(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    cols = [col for col in columns if col in df.columns]
    if not cols:
        return df.reset_index(drop=True)
    return df.drop_duplicates(subset=cols, keep="first").reset_index(drop=True)


def _lineage_all_join_rows(
    analysis: SQLLensAnalysis,
    views: pd.DataFrame,
    table_review: pd.DataFrame,
    view_definitions: pd.DataFrame,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    if analysis.joins is not None and not analysis.joins.empty:
        query_joins = analysis.joins.copy()
        query_joins.insert(0, "join_scope", "query")
        frames.append(query_joins)
    if views is not None and not views.empty:
        seen_views: set[str] = set()
        for _, view_row in views.iterrows():
            view_name = str(view_row.get("query_table") or view_row.get("table_name") or "").strip()
            sql_text = str(view_row.get("source_definition_full") or view_row.get("source_definition") or "").strip()
            if not view_name or not sql_text or view_name.lower() in seen_views:
                continue
            seen_views.add(view_name.lower())
            try:
                view_analysis = analyze_console_sql(sql_text, table_review, pd.DataFrame(), view_definitions)
            except Exception:
                continue
            if view_analysis.joins is None or view_analysis.joins.empty:
                continue
            view_joins = view_analysis.joins.copy()
            view_joins.insert(0, "join_scope", view_name)
            frames.append(view_joins)
    if not frames:
        return pd.DataFrame()
    joined = pd.concat(frames, ignore_index=True)
    return _lineage_dedupe(joined, ["join_scope", "join_type", "target_table", "condition"])


def _lineage_join_rows(joins: pd.DataFrame, row: pd.Series) -> pd.DataFrame:
    if joins is None or joins.empty:
        return pd.DataFrame()
    out = joins.copy()
    out["distribution_signal"] = out.apply(lambda join_row: _lineage_distribution_signal(join_row, row), axis=1)
    return out.reset_index(drop=True)


def _query_evidence_rows(frame: pd.DataFrame | None, row: pd.Series) -> pd.DataFrame:
    if frame is None or frame.empty or "query_id" not in frame.columns:
        return pd.DataFrame()
    query_id = _normalized_query_id(row.get("query_id"))
    if not query_id:
        return pd.DataFrame()
    ids = frame["query_id"].map(_normalized_query_id)
    return frame.loc[ids == query_id].copy().reset_index(drop=True)


def _lineage_distribution_signal(join_row: pd.Series, query_row: pd.Series) -> str:
    alignment = str(join_row.get("distribution_alignment") or "").lower()
    if "no join condition" in alignment:
        return "Cross join"
    if "co-located" in alignment or "replicated" in alignment:
        return "Co-located"
    if "broadcast" in alignment or "redistribution" in alignment:
        return "Broadcast / redistribute risk"
    if _safe_float(query_row.get("bcast_cnt")) > 0:
        return "Broadcast seen in plan"
    if _safe_float(query_row.get("dist_both_cnt")) > 0:
        return "DS_DIST_BOTH seen in plan"
    if "metadata incomplete" in alignment:
        return "Unknown"
    return "Needs plan check"


def _join_detail_text(join_row: pd.Series, tables: pd.DataFrame, query_row: pd.Series) -> str:
    aliases = _split_csv(join_row.get("aliases"))
    join_columns = _join_columns_by_alias(join_row.get("join_columns"))
    physical_columns: dict[str, list[str]] = {}
    physical_rows: list[pd.Series] = []
    for field in ("left_physical_sources", "right_physical_sources"):
        for source in _split_csv(join_row.get(field)):
            table_identity, column = _physical_source_parts(source)
            if not table_identity:
                continue
            if column:
                physical_columns.setdefault(table_identity, []).append(column)
            row = _table_row_for_physical_source(tables, table_identity)
            if row is not None:
                physical_rows.append(row)
    alias_rows = [_table_row_for_alias(tables, alias) for alias in aliases]
    table_rows: list[pd.Series] = []
    seen_tables: set[str] = set()
    for row in [item for item in alias_rows if item is not None] + physical_rows:
        identity = _table_identity_from_row(row)
        key = identity or str(row.get("alias") or "").strip().lower()
        if key and key not in seen_tables:
            seen_tables.add(key)
            table_rows.append(row)
    lines = [
        "JOIN DETAIL",
        "",
        f"Scope: {join_row.get('join_scope') or 'query'}",
        f"Join: #{join_row.get('join_no') or '-'} {str(join_row.get('join_type') or 'join').upper()}",
        f"Condition: {join_row.get('condition') or '-'}",
        f"Column Pairs: {join_row.get('column_pairs') or '-'}",
        f"Left Physical Source(s): {join_row.get('left_physical_sources') or 'UNRESOLVED'}",
        f"Right Physical Source(s): {join_row.get('right_physical_sources') or 'UNRESOLVED'}",
        f"Physical Column Pairs: {join_row.get('physical_column_pairs') or '-'}",
        f"Physical Lineage: {join_row.get('physical_lineage_status') or 'unresolved'}",
        f"Distribution Signal: {join_row.get('distribution_signal') or '-'}",
        f"Distribution Alignment: {join_row.get('distribution_alignment') or '-'}",
        f"Recommendation: {join_row.get('recommendation') or '-'}",
        "",
        "MAPPED SYS_QUERY_EXPLAIN + SYS_QUERY_DETAIL",
        f"Plan Match: {join_row.get('plan_match_status') or 'No captured plan match'}",
        f"Plan Node: {join_row.get('plan_node_id') or '-'} | Parent: {join_row.get('plan_parent_id') or '-'}",
        f"Plan Operator: {join_row.get('plan_operator') or '-'}",
        f"Plan Condition: {join_row.get('plan_condition') or '-'}",
        f"Actual Steps: {join_row.get('actual_step_names') or '-'}",
        f"Actual Movement: {join_row.get('actual_movement') or '-'}",
        f"Actual Input/Output Rows: {_fmt_value('input_rows', join_row.get('actual_input_rows'))} / {_fmt_value('output_rows', join_row.get('actual_output_rows'))}",
        f"Actual Input/Output Bytes: {_fmt_value('input_bytes', join_row.get('actual_input_bytes'))} / {_fmt_value('output_bytes', join_row.get('actual_output_bytes'))}",
        f"Actual Duration: {_fmt_value('elapsed_s', join_row.get('actual_duration_s'))}",
        f"Actual Spill Local/Remote: {_fmt_value('total_spill', join_row.get('actual_local_spill_blocks'))} / {_fmt_value('total_spill', join_row.get('actual_remote_spill_blocks'))}",
        f"Actual Data/Time Skew: {_fmt_value('max_data_skewness', join_row.get('actual_max_data_skew'))} / {_fmt_value('max_time_skewness', join_row.get('actual_max_time_skew'))}",
        "",
        "PLAN MOVEMENT EVIDENCE",
        f"Broadcast count: {_fmt_int(query_row.get('bcast_cnt'))}",
        f"DS_DIST_BOTH count: {_fmt_int(query_row.get('dist_both_cnt'))}",
        f"Distribution movement count: {_fmt_int(query_row.get('dist_total_cnt'))}",
        "",
        "TABLE FIT",
    ]
    if not table_rows:
        lines.append("No physical table metadata was matched for this join. Load Table Review and qualify schema/database names.")
        return "\n".join(lines)
    for row in table_rows:
        alias = str(row.get("alias") or "").strip()
        identity = _table_identity_from_row(row)
        cols = list(join_columns.get(alias.lower(), []))
        cols.extend(physical_columns.get(identity, []))
        cols = list(dict.fromkeys(col for col in cols if col))
        distkey = str(row.get("distkey") or "")
        sortkey = str(row.get("sortkey1") or "")
        dist_hits = [col for col in cols if _same_sql_column(col, distkey)]
        sort_hits = [col for col in cols if _same_sql_column(col, sortkey)]
        badges = _sql_object_badges(row)
        lines.extend(
            [
                "",
                f"{alias or 'physical'} -> {_sql_object_label(row)}",
                f"Type/Status: {row.get('object_type') or '-'} / {row.get('match_status') or '-'}",
                f"Join columns: {', '.join(cols) or '-'}",
                f"Dist/Sort: {_format_dist_sort_keys(row)}",
                f"Size/Rows: {_format_size_row_count(row)}",
                f"Distkey match: {', '.join(dist_hits) if dist_hits else 'No join column matched the distkey'}",
                f"Sortkey match: {', '.join(sort_hits) if sort_hits else 'No join column matched the leading sort key'}",
                f"Stats Off: {_fmt_value('stats_off', row.get('stats_off'))}",
                f"Sorted Pct: {_fmt_value(TABLE_SORTED_PCT_COL, 100 - _safe_float(row.get('unsorted_pct')))}",
                f"Badges: {', '.join(badges) if badges else 'none'}",
                f"Table recommendation: {row.get('recommendation') or '-'}",
            ]
        )
    return "\n".join(lines)


def _sql_flow_node_detail_text(node: pd.Series, analysis: SQLLensAnalysis) -> str:
    node_id = str(node.get("node_id") or "")
    kind = str(node.get("kind") or "").lower()
    lines = [
        "SQL DIAGRAM NODE",
        "",
        f"Node: {node_id}",
        f"Kind: {kind or '-'}",
        f"Label: {node.get('label') or '-'}",
        f"Severity: {node.get('severity') or '-'}",
        f"Detail: {node.get('detail') or '-'}",
    ]
    if node_id == "sql:statement" or kind == "statement":
        lines.extend(["", "FORMATTED SQL SHAPE", _format_sql_text(analysis.normalized_sql or "")])
        return "\n".join(lines)
    if node_id.startswith("table:"):
        alias = node_id.split(":", 1)[1]
        table_row = _table_row_for_alias(analysis.tables, alias)
        if table_row is None:
            lines.extend(["", "No table row was found for this diagram node."])
            return "\n".join(lines)
        lines.extend(
            [
                "",
                "TABLE / VIEW DETAIL",
                _sql_object_inspector_text(table_row, analysis, pd.Series(dtype=object)),
            ]
        )
        return "\n".join(lines)
    if node_id.startswith("join:"):
        join_no = _safe_int_or_none(node_id.split(":", 1)[1])
        row = _row_by_number(analysis.joins, "join_no", join_no)
        if row is not None:
            lines.extend(["", _join_detail_text(row, analysis.tables, pd.Series(dtype=object))])
        return "\n".join(lines)
    if node_id.startswith("predicate:"):
        predicate_no = _safe_int_or_none(node_id.split(":", 1)[1])
        row = _row_by_number(analysis.predicates, "predicate_no", predicate_no)
        if row is not None:
            lines.extend(
                [
                    "",
                    "PREDICATE DETAIL",
                    f"Clause: {row.get('clause') or '-'}",
                    f"Type: {row.get('predicate_type') or '-'}",
                    f"Condition: {row.get('condition') or '-'}",
                    f"Tables: {row.get('involved_tables') or '-'}",
                    f"Columns: {row.get('columns') or '-'}",
                    f"Sort Alignment: {row.get('sortkey_alignment') or '-'}",
                    f"Recommendation: {row.get('recommendation') or '-'}",
                ]
            )
        return "\n".join(lines)
    if node_id.startswith("advice:") and analysis.first_steps is not None and not analysis.first_steps.empty:
        rank = _safe_int_or_none(node_id.split(":", 1)[1])
        row = _row_by_number(analysis.first_steps, "rank", rank)
        if row is not None:
            lines.extend(
                [
                    "",
                    "ADVICE",
                    f"Category: {row.get('category') or '-'}",
                    f"Why: {row.get('why') or '-'}",
                    f"Next Step: {row.get('next_step') or '-'}",
                ]
            )
    return "\n".join(lines)


def _row_by_number(df: pd.DataFrame, col: str, number: int | None) -> pd.Series | None:
    if df is None or df.empty or col not in df.columns or number is None:
        return None
    values = pd.to_numeric(df[col], errors="coerce")
    matches = df[values == int(number)]
    if matches.empty:
        return None
    return matches.iloc[0]


def _table_row_for_alias(tables: pd.DataFrame, alias: str) -> pd.Series | None:
    if tables is None or tables.empty:
        return None
    wanted = str(alias or "").strip().lower()
    if not wanted or "alias" not in tables.columns:
        return None
    matches = tables[tables["alias"].astype(str).str.lower() == wanted]
    if matches.empty:
        return None
    return matches.iloc[0]


def _physical_source_parts(source: object) -> tuple[str, str]:
    parts = [part.strip().strip('"').lower() for part in str(source or "").split(".") if part.strip()]
    if len(parts) < 2:
        return "", ""
    return ".".join(parts[:-1]), parts[-1]


def _table_identity_from_row(row: pd.Series) -> str:
    return ".".join(
        part for part in (
            str(row.get("source_db") or row.get("database") or row.get("database_name") or "").strip().lower(),
            str(row.get("schema_name") or row.get("schema") or "").strip().lower(),
            str(row.get("table_name") or row.get("view_name") or "").strip().lower(),
        ) if part
    )


def _table_row_for_physical_source(tables: pd.DataFrame, identity: str) -> pd.Series | None:
    if tables is None or tables.empty or not identity:
        return None
    wanted = str(identity).strip().lower()
    identities = tables.apply(_table_identity_from_row, axis=1)
    exact = tables.loc[identities == wanted]
    if not exact.empty:
        return exact.iloc[0]
    # SYS text can omit a database while Table Review carries one. Only use a
    # suffix match when it identifies exactly one physical object.
    suffix = "." + wanted
    matches = tables.loc[identities.map(lambda value: value.endswith(suffix))]
    unique = {_table_identity_from_row(row) for _, row in matches.iterrows()}
    if len(unique) == 1 and not matches.empty:
        return matches.iloc[0]
    return None


def _join_columns_by_alias(value: object) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for part in _split_csv(value):
        if "." not in part:
            continue
        alias, col = part.split(".", 1)
        alias = alias.strip().strip('"').lower()
        col = col.strip().strip('"')
        if alias and col:
            out.setdefault(alias, []).append(col)
    return out


def _split_csv(value: object) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def _same_sql_column(left: object, right: object) -> bool:
    left_leaf = str(left or "").strip().strip('"').split(".")[-1].lower()
    right_text = str(right or "").strip()
    match = re.search(r"\(([^)]+)\)", right_text)
    right_leaf = (match.group(1) if match else right_text).strip().strip('"').split(".")[-1].lower()
    return bool(left_leaf and right_leaf and left_leaf == right_leaf)


def _table_attribute_display_frame(df: pd.DataFrame, *, table_review: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    display = df.copy() if df is not None else pd.DataFrame()
    sort_sources = pd.DataFrame(index=display.index)
    if display.empty:
        return display, sort_sources

    has_size_or_rows = "size_mb" in display.columns or "tbl_rows" in display.columns
    has_dist_or_sort = any(col in display.columns for col in ("diststyle", "distkey", "sortkey1"))

    if has_size_or_rows:
        display[TABLE_SIZE_ROW_COL] = display.apply(_format_size_row_count, axis=1)
        size_sort = pd.to_numeric(display.get("size_mb"), errors="coerce") if "size_mb" in display.columns else pd.Series(index=display.index, dtype="float64")
        row_sort = pd.to_numeric(display.get("tbl_rows"), errors="coerce") if "tbl_rows" in display.columns else pd.Series(index=display.index, dtype="float64")
        sort_sources[TABLE_SIZE_ROW_COL] = size_sort.fillna(row_sort)
    if has_dist_or_sort:
        display[TABLE_DIST_SORT_COL] = display.apply(_format_dist_sort_keys, axis=1)
        sort_sources[TABLE_DIST_SORT_COL] = display[TABLE_DIST_SORT_COL].astype(str)
    if table_review and "unsorted_pct" in display.columns:
        unsorted = pd.to_numeric(display["unsorted_pct"], errors="coerce")
        sorted_pct = (100.0 - unsorted).clip(lower=0, upper=100)
        display[TABLE_SORTED_PCT_COL] = sorted_pct
        sort_sources[TABLE_SORTED_PCT_COL] = sorted_pct
    aux_sort_values: dict[str, pd.Series] = {}
    if table_review:
        aux_cols = [
            "slow_query_count",
            "slow_query_runtime_s",
            "redistribution_query_count",
            "broadcast_query_count",
        ]
        present_aux = [col for col in aux_cols if col in display.columns]
        if len(present_aux) == len(aux_cols):
            aux_numeric = display[present_aux].apply(pd.to_numeric, errors="coerce").fillna(0)
            needs_aux = aux_numeric.sum(axis=1) == 0
            for col in present_aux:
                aux_sort_values[col] = aux_numeric[col]
                display[col] = display[col].astype("object")
                display.loc[needs_aux, col] = "(Needs Addn Qry)"
    if table_review:
        for col in list(display.columns):
            if (
                col.endswith("_score")
                or col in {
                    "avg_scan_duration_s",
                    "scan_input_rows_m",
                    "slow_query_runtime_s",
                    "skew_rows",
                    "stats_off",
                    "vacuum_sort_benefit",
                }
            ):
                sort_sources[col] = pd.to_numeric(display[col], errors="coerce")
        for col, values in aux_sort_values.items():
            sort_sources[col] = values

    raw_compacted = [col for col in ("size_mb", "tbl_rows", "diststyle", "distkey", "sortkey1") if col in display.columns]
    if table_review:
        raw_compacted = ["unsorted_pct"] if "unsorted_pct" in display.columns else []
    if raw_compacted:
        display = display.drop(columns=raw_compacted)
    return display.reset_index(drop=True), sort_sources.reset_index(drop=True)


def _normalize_table_review_columns(columns: list[str] | tuple[str, ...]) -> list[str]:
    normalized: list[str] = []
    for col in columns:
        name = str(col or "").strip()
        if not name:
            continue
        if name == "scan_duration_s":
            name = "avg_scan_duration_s"
        if name == "unsorted_pct":
            name = TABLE_SORTED_PCT_COL
        if name not in normalized:
            normalized.append(name)
    return normalized or list(TABLE_REVIEW_COLS)


_TABLE_REVIEW_INTERSECTION_COLUMNS = (
    "slow_query_count",
    "scan_query_count",
    "redistribution_query_count",
    "broadcast_query_count",
    "skewed_query_count",
    "rrscan_query_count",
    "non_rrscan_query_count",
    "slow_query_runtime_s",
    "scan_duration_s",
    "avg_scan_duration_s",
)


def _table_review_intersection_mask(df: pd.DataFrame) -> pd.Series:
    """True when a table intersects captured query or scan telemetry.

    If no intersection columns exist, fail open rather than hiding the whole
    inventory merely because an older artifact lacks the telemetry fields.
    """
    if df is None or df.empty:
        return pd.Series(False, index=getattr(df, "index", None), dtype=bool)
    available = [column for column in _TABLE_REVIEW_INTERSECTION_COLUMNS if column in df.columns]
    if not available:
        return pd.Series(True, index=df.index, dtype=bool)
    mask = pd.Series(False, index=df.index, dtype=bool)
    for column in available:
        values = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
        mask |= values.abs() > 0
    return mask


def _format_size_row_count(row: pd.Series) -> str:
    size = _fmt_value("size_mb", row.get("size_mb")) if "size_mb" in row.index else "-"
    rows = _fmt_compact_count(row.get("tbl_rows")) if "tbl_rows" in row.index else "-"
    return f"{size} / {rows}"


def _format_dist_sort_keys(row: pd.Series) -> str:
    diststyle = _clean_compact_text(row.get("diststyle") if "diststyle" in row.index else "")
    distkey = _clean_compact_text(row.get("distkey") if "distkey" in row.index else "")
    sortkey = _clean_compact_text(row.get("sortkey1") if "sortkey1" in row.index else "")
    dist_missing = _is_missing_distkey(diststyle, distkey)
    dist_label = "Dist: None" if dist_missing else f"Dist: {diststyle or '-'}"
    pieces = [dist_label]
    if distkey and not dist_missing:
        pieces.append(f"distkey: {distkey}")
    pieces.append(f"Sort: {'Unsorted' if _is_missing_sortkey(sortkey) else sortkey}")
    return " / ".join(pieces)


def _is_auto_even_diststyle(value: object) -> bool:
    text = re.sub(r"\s+", "", _clean_compact_text(value)).upper()
    return text in {"AUTO(EVEN)", "AUTO", "(AUTO)"}


def _is_missing_distkey(diststyle: object, distkey: object = "") -> bool:
    from ..redshift_meta import is_missing_distkey as _shared_missing_distkey

    dist_text = re.sub(r"\s+", "", _clean_compact_text(diststyle)).lower()
    key_text = re.sub(r"\s+", "", _clean_compact_text(distkey)).strip("\"'").lower()
    if not dist_text and not key_text:
        return True
    if _shared_missing_distkey(dist_text) or _shared_missing_distkey(key_text):
        return True
    if key_text in {"", "-", "none", "null", "nan", "auto", "(auto)"}:
        return _is_auto_even_diststyle(diststyle) or "key" not in dist_text
    return False


def _is_missing_sortkey(value: object) -> bool:
    from ..redshift_meta import is_missing_sortkey as _shared_missing_sortkey

    text = _clean_compact_text(value)
    if not text:
        return True
    if _shared_missing_sortkey(text):
        return True
    lowered = text.lower().strip()
    return bool(re.fullmatch(r"\(?\s*sort\s+key\s*\)?", lowered))


def _dist_sort_missing_state(value: object) -> tuple[bool, bool]:
    text = _clean_compact_text(value).lower()
    return "dist: none" in text, "sort: unsorted" in text


def _clean_compact_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return "" if text.lower() in {"", "none", "null", "nan"} else text


def _join_unique_display(series: pd.Series | None, limit: int = 6) -> str:
    if series is None:
        return ""
    seen: list[str] = []
    for value in series:
        text = str(value).strip()
        if text and text.lower() not in ("nan", "none") and text not in seen:
            seen.append(text)
    if len(seen) > limit:
        return ", ".join(seen[:limit]) + f" +{len(seen) - limit} more"
    return ", ".join(seen)


_SLOW_QUERY_NON_AVERAGE_NUMERIC_COLUMNS = {
    "query_id",
    "user_id",
    "transaction_id",
    "session_id",
    "snapshot_id",
    "repeat_group_id",
}


def _slow_query_group_key(value: object) -> str:
    try:
        if value is None or pd.isna(value):
            return "UNGROUPED"
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return "UNGROUPED" if text.lower() in {"", "none", "nan", "<na>"} else text


def _slow_query_group_count(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    if "repeat_group_id" not in df.columns:
        return len(df)
    return int(df["repeat_group_id"].map(_slow_query_group_key).nunique())


def _slow_query_parent_row(group_id: str, members: pd.DataFrame) -> pd.Series:
    if members is None or members.empty:
        return pd.Series(dtype=object)
    ordered = members.copy()
    ordered["__severity"] = _num_series(ordered, "severity_score").fillna(0.0)
    ordered["__elapsed"] = _num_series(ordered, "elapsed_s").fillna(0.0)
    representative = ordered.sort_values(
        ["__severity", "__elapsed"], ascending=[False, False], kind="stable"
    ).iloc[0].drop(labels=["__severity", "__elapsed"]).copy()
    for column in members.columns:
        if column in _SLOW_QUERY_NON_AVERAGE_NUMERIC_COLUMNS:
            continue
        numeric = pd.to_numeric(members[column], errors="coerce")
        if numeric.notna().any():
            representative[column] = float(numeric.mean())
    for column in ("database_name", "user_name"):
        if column in members.columns:
            representative[column] = _join_unique_display(members[column])
    if "dominant_issue" in members.columns:
        issues = members["dominant_issue"].dropna().astype(str).str.strip()
        representative["dominant_issue"] = issues.mode().iloc[0] if not issues.empty else "-"
    representative["repeat_group_id"] = group_id
    representative["_tree_label"] = f"{group_id} ({len(members):,} queries)"
    representative["_group_query_count"] = int(len(members))
    representative["_is_group_parent"] = True
    return representative


def _slow_query_tree_item(row: pd.Series, *, parent: bool) -> QTreeWidgetItem:
    payload = row.to_dict()
    texts: list[str] = []
    for column, _label in SLOW_QUERY_TREE_COLUMNS:
        value = row.get(column)
        if column == "_tree_label":
            label = _clean_compact_text(value)
            texts.append(label or "-")
        else:
            texts.append(_fmt_value(column, value))
    item = _SortableSlowQueryItem(texts)
    item.setData(0, Qt.UserRole, payload)
    for column_index, (column, _label) in enumerate(SLOW_QUERY_TREE_COLUMNS):
        value = row.get(column)
        try:
            numeric = float(value)
            if not math.isnan(numeric):
                item.setData(column_index, Qt.UserRole + 1, numeric)
                item.setTextAlignment(column_index, Qt.AlignRight | Qt.AlignVCenter)
        except (TypeError, ValueError):
            pass
        if parent:
            font = item.font(column_index)
            font.setBold(True)
            item.setFont(column_index, font)
    item.setForeground(0, QBrush(QColor(PALETTE.accent_bright if parent else PALETTE.text_1)))
    return item


class _SortableSlowQueryItem(QTreeWidgetItem):
    """Tree row that sorts numeric spreadsheet columns as numbers, not text."""

    def __lt__(self, other: QTreeWidgetItem) -> bool:
        tree = self.treeWidget()
        column = tree.sortColumn() if tree is not None else 0
        left = self.data(column, Qt.UserRole + 1)
        right = other.data(column, Qt.UserRole + 1)
        if left is not None and right is not None:
            return float(left) < float(right)
        return self.text(column).casefold() < other.text(column).casefold()


def _populate_slow_query_tree(tree: QTreeWidget, df: pd.DataFrame, *, grouped: bool) -> None:
    tree.setSortingEnabled(False)
    tree.clear()
    if df is None or df.empty:
        return
    work = df.copy()
    if grouped and "repeat_group_id" in work.columns:
        work["__group_key"] = work["repeat_group_id"].map(_slow_query_group_key)
        group_rows: list[tuple[float, str, pd.DataFrame, pd.Series]] = []
        for group_id, members in work.groupby("__group_key", sort=False):
            members = members.drop(columns=["__group_key"]).copy()
            parent_row = _slow_query_parent_row(str(group_id), members)
            group_rows.append((_safe_float(parent_row.get("severity_score")), str(group_id), members, parent_row))
        for _score, _group_id, members, parent_row in sorted(
            group_rows, key=lambda value: (-value[0], value[1])
        ):
            parent_item = _slow_query_tree_item(parent_row, parent=True)
            tree.addTopLevelItem(parent_item)
            sort_columns = [
                column for column in ("severity_score", "elapsed_s") if column in members.columns
            ]
            if sort_columns:
                members = members.sort_values(sort_columns, ascending=False, kind="stable")
            for _, member in members.iterrows():
                child = member.copy()
                child["_tree_label"] = f"Query {_normalized_query_id(child.get('query_id')) or '-'}"
                child["_group_query_count"] = 1
                child["_is_group_parent"] = False
                parent_item.addChild(_slow_query_tree_item(child, parent=False))
            parent_item.setExpanded(False)
    else:
        for _, source in work.iterrows():
            row = source.copy()
            row["_tree_label"] = f"Query {_normalized_query_id(row.get('query_id')) or '-'}"
            row["_group_query_count"] = 1
            row["_is_group_parent"] = False
            tree.addTopLevelItem(_slow_query_tree_item(row, parent=False))
    # Turn sorting on only after the hierarchy is built. The custom item keeps
    # numeric columns numeric while Qt sorts parents and their children within
    # their own levels.
    tree.setSortingEnabled(True)
    tree.sortItems(3, Qt.DescendingOrder)


def _roll_up_repeat_patterns(df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    """Collapse rows sharing a repeat_group_id into one representative row.

    The representative (worst severity, then longest runtime) keeps its full
    original columns so SQL/lineage/plan actions still work, plus pattern_*
    aggregates. One-off rows with no repeat group pass through as themselves.
    Returns (frame, False) unchanged when no repeat metadata is present.
    """
    if df is None or df.empty or "repeat_group_id" not in df.columns:
        return df, False
    work = df.copy()
    group_key = work["repeat_group_id"].astype(str).str.strip()
    grouped_mask = ~group_key.str.lower().isin(("", "nan", "none"))
    if not grouped_mask.any():
        return df, False
    work["_elapsed_num"] = pd.to_numeric(work.get("elapsed_s"), errors="coerce").fillna(0.0)
    rolled_rows: list[pd.Series] = []
    for _gid, members in work[grouped_mask].groupby(group_key[grouped_mask], sort=False):
        representative = members.sort_values(
            ["severity_score", "_elapsed_num"], ascending=[False, False]
        ).iloc[0].copy()
        representative["pattern_runs"] = len(members)
        representative["pattern_total_elapsed_s"] = round(float(members["_elapsed_num"].sum()), 1)
        representative["pattern_avg_elapsed_s"] = round(float(members["_elapsed_num"].mean()), 1)
        representative["pattern_databases"] = _join_unique_display(members.get("database_name"))
        representative["pattern_users"] = _join_unique_display(members.get("user_name"))
        rolled_rows.append(representative)
    singles = work[~grouped_mask]
    for _, row in singles.iterrows():
        single = row.copy()
        single["pattern_runs"] = 1
        single["pattern_total_elapsed_s"] = round(float(single["_elapsed_num"]), 1)
        single["pattern_avg_elapsed_s"] = round(float(single["_elapsed_num"]), 1)
        single["pattern_databases"] = str(single.get("database_name") or "").strip()
        single["pattern_users"] = str(single.get("user_name") or "").strip()
        rolled_rows.append(single)
    out = pd.DataFrame(rolled_rows).drop(columns=["_elapsed_num"], errors="ignore")
    out = out.sort_values(
        ["pattern_total_elapsed_s", "severity_score"], ascending=[False, False]
    ).reset_index(drop=True)
    return out, True


def _apply_slow_query_filters(df: pd.DataFrame, filters: dict) -> tuple[pd.DataFrame, str]:
    if df is None or df.empty:
        return pd.DataFrame(), ""
    work = df.copy()
    notes: list[str] = []
    minimums = dict((filters or {}).get("minimums") or {})
    for col, threshold in minimums.items():
        if col not in work.columns:
            continue
        before = len(work)
        values = pd.to_numeric(work[col], errors="coerce").fillna(0)
        work = work[values >= float(threshold)].copy()
        if len(work) != before:
            label = DISPLAY_COLUMN_LABELS.get(col, _column_header_label(col))
            shown = f"{float(threshold) / 60.0:g} min" if col == "elapsed_s" else _fmt_compact_count(threshold)
            notes.append(f"{label} >= {shown}")
    value_filters = dict((filters or {}).get("values") or {})
    for col, selected in value_filters.items():
        if col not in work.columns or not selected:
            continue
        wanted = {str(value) for value in selected}
        work = work[work[col].map(_slow_filter_value_text).isin(wanted)].copy()
        label = DISPLAY_COLUMN_LABELS.get(col, _column_header_label(col))
        notes.append(f"{label} in {len(wanted)} value(s)")
    return work.reset_index(drop=True), "; ".join(notes)


def _slow_filter_value_columns(df: pd.DataFrame) -> list[str]:
    if df is None or df.empty:
        return []
    excluded_fragments = ("sql_text", "sample_sql", "sql_shape", "ast_shape", "source_definition")
    cols = [
        str(col)
        for col in df.columns
        if not any(fragment in str(col).lower() for fragment in excluded_fragments)
    ]
    preferred = [
        "database_name",
        "user_name",
        "dominant_issue",
        "severity_reason",
        "repeat_group_id",
        "full_explain_available",
    ]
    ordered = [col for col in preferred if col in cols]
    ordered.extend(col for col in cols if col not in ordered)
    return ordered


def _slow_filter_unique_values(series: pd.Series, limit: int = 500) -> list[tuple[str, int]]:
    values = series.map(_slow_filter_value_text)
    counts = values.value_counts(dropna=False).head(limit)
    return [(str(index), int(count)) for index, count in counts.items()]


def _slow_filter_value_text(value: object) -> str:
    if value is None:
        return "(blank)"
    try:
        if pd.isna(value):
            return "(blank)"
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return "(blank)"
    return text if len(text) <= 160 else text[:157] + "..."


def _parse_short_number(value: object) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return None
    match = re.fullmatch(r"([-+]?\d+(?:\.\d+)?)\s*([kKmMbBtT]?)", text)
    if not match:
        return None
    number = float(match.group(1))
    suffix = match.group(2).lower()
    scale = {"": 1.0, "k": 1_000.0, "m": 1_000_000.0, "b": 1_000_000_000.0, "t": 1_000_000_000_000.0}[suffix]
    return number * scale


class _SqlEditorEventFilter(QObject):
    def __init__(self, editor: QPlainTextEdit, open_view_callback):
        super().__init__(editor)
        self._editor = editor
        self._open_view_callback = open_view_callback

    def eventFilter(self, obj, event) -> bool:
        if obj is self._editor and event.type() == QEvent.MouseButtonDblClick:
            try:
                pos = event.position().toPoint()
            except AttributeError:
                pos = event.pos()
            cursor = self._editor.cursorForPosition(pos)
            token = _sql_token_at_position(self._editor.toPlainText(), cursor.position())
            if token:
                self._open_view_callback(token)
        return False


def _analyze_sql_for_viewer(
    sql_text: object,
    table_review: pd.DataFrame,
    view_definitions: pd.DataFrame,
) -> SQLLensAnalysis | None:
    sql = _sql_for_lens_analysis(sql_text)
    if not sql:
        return None
    try:
        return analyze_console_sql(sql, table_review, pd.DataFrame(), view_definitions)
    except Exception:
        return None


def _sql_for_lens_analysis(sql_text: object) -> str:
    sql = _decode_sql_display_escapes(str(sql_text or "")).strip()
    inner = _extract_wrapped_command_inner_sql(sql)
    if inner:
        return inner
    return sql


def _sql_viewer_summary_text(
    analysis: SQLLensAnalysis | None,
    table_review: pd.DataFrame,
    view_definitions: pd.DataFrame,
) -> str:
    if analysis is None:
        return "SQL viewer context is not available. Format SQL or verify that the statement can be parsed."
    tables = analysis.tables if analysis.tables is not None else pd.DataFrame()
    views = _sql_view_rows(tables)
    badges = _sql_badge_summary_rows(tables)
    lines = [
        "SQL OBJECT INSPECTOR",
        "",
        f"Parse: {'parsed' if analysis.parse_ok else analysis.parse_error or 'parse issue'}",
        f"Objects: {len(tables):,} | Views: {len(views):,} | Joins: {len(analysis.joins) if analysis.joins is not None else 0:,}",
    ]
    note = _lineage_match_status_note(tables, table_review, view_definitions)
    if note:
        lines.extend(["", "MATCH STATUS", note])
    if not views.empty:
        lines.extend(["", "VIEWS"])
        for _, row in views.head(12).iterrows():
            lines.append(f"- {_sql_object_label(row)}")
    if badges:
        lines.extend(["", "STRUCTURAL BADGES"])
        lines.extend(f"- {label}: {', '.join(items)}" for label, items in badges[:12])
    remedies = _sql_quick_remedies(analysis)
    if remedies:
        lines.extend(["", "QUICK REMEDIES"])
        lines.extend(f"- {item}" for item in remedies[:12])
    lines.extend(
        [
            "",
            "ACTIONS",
            "- Click an object name in the SQL to inspect table/view metadata here.",
            "- Highlight schema.table, right-click, and choose Show Table Info.",
            "- Click Identify Views to highlight views in orange.",
            "- Double-click a highlighted view name to open the stored view SQL.",
            "- Click Deficiency Overlay to highlight dist/sort/stat/match risks.",
        ]
    )
    return "\n".join(lines)


def _identify_sql_views(
    editor: QPlainTextEdit,
    inspector: QPlainTextEdit,
    analysis: SQLLensAnalysis | None,
    parent: QWidget,
) -> None:
    if analysis is None or analysis.tables is None or analysis.tables.empty:
        QMessageBox.information(parent, "Identify Views", "No parsed SQL object metadata is available for this statement.")
        return
    views = _sql_view_rows(analysis.tables)
    selections = _sql_extra_selections(editor, views, QColor("#F59E0B"), QColor("#111827"))
    editor.setExtraSelections(selections)
    if views.empty:
        inspector.setPlainText("No captured views were identified in this SQL. If this is wrong, load View Definitions and Table Review from the same snapshot.")
        return
    lines = ["IDENTIFIED VIEWS", "", "Double-click a highlighted view to open its stored SQL.", ""]
    for _, row in views.iterrows():
        lines.append(f"- {_sql_object_label(row)} | match={row.get('match_status') or '-'}")
    inspector.setPlainText("\n".join(lines))


def _apply_sql_deficiency_overlay(
    editor: QPlainTextEdit,
    inspector: QPlainTextEdit,
    analysis: SQLLensAnalysis | None,
) -> None:
    if analysis is None or analysis.tables is None or analysis.tables.empty:
        inspector.setPlainText("No parsed SQL object metadata is available for the deficiency overlay.")
        return
    selections = []
    for _, row in analysis.tables.iterrows():
        badges = _sql_object_badges(row)
        if not badges:
            continue
        color = _sql_badge_color(badges)
        selections.extend(_sql_extra_selections(editor, pd.DataFrame([row]), color, QColor("#111827")))
    editor.setExtraSelections(selections)
    lines = ["STRUCTURAL DEFICIENCY OVERLAY", ""]
    summary = _sql_badge_summary_rows(analysis.tables)
    if not summary:
        lines.append("No structural badges were found for matched SQL objects.")
    else:
        for label, items in summary:
            lines.append(f"{label}:")
            lines.extend(f"  - {item}" for item in items)
    inspector.setPlainText("\n".join(lines))


def _update_sql_object_inspector(
    editor: QPlainTextEdit,
    inspector: QPlainTextEdit,
    analysis: SQLLensAnalysis | None,
    query_row: pd.Series,
) -> None:
    if analysis is None or analysis.tables is None or analysis.tables.empty:
        return
    token = _sql_token_at_position(editor.toPlainText(), editor.textCursor().position())
    if not token:
        return
    row = _match_sql_object_token(token, analysis.tables)
    if row is None:
        return
    inspector.setPlainText(_sql_object_inspector_text(row, analysis, query_row))


def _open_view_from_sql_token(token: str, analysis: SQLLensAnalysis | None, parent: QWidget) -> None:
    if analysis is None or analysis.tables is None or analysis.tables.empty:
        return
    row = _match_sql_object_token(token, analysis.tables)
    if row is None:
        return
    if "view" not in str(row.get("object_type") or "").lower():
        return
    sql_text = str(row.get("source_definition_full") or row.get("source_definition") or "").strip()
    if not sql_text:
        QMessageBox.information(parent, "View SQL", "No captured SQL definition is available for this view.")
        return
    dialog = QDialog(parent)
    dialog.setWindowTitle(f"View SQL - {_sql_object_label(row)}")
    root = QVBoxLayout(dialog)
    root.setContentsMargins(12, 12, 12, 12)
    title = QLabel(_sql_object_label(row))
    title.setObjectName("SectionHeader")
    title.setWordWrap(True)
    root.addWidget(title)
    editor = QPlainTextEdit()
    editor.setReadOnly(True)
    editor.setObjectName("Mono")
    editor.setLineWrapMode(QPlainTextEdit.WidgetWidth)
    editor.setPlainText(sql_text)
    root.addWidget(editor, 1)
    actions = QHBoxLayout()
    format_btn = QPushButton("Format SQL")
    format_btn.setObjectName("Primary")
    actions.addWidget(format_btn)
    _add_sql_structure_buttons(
        actions,
        editor,
        dialog,
        row,
        pd.DataFrame(),
        pd.DataFrame(),
    )
    actions.addStretch(1)
    buttons = QDialogButtonBox(QDialogButtonBox.Close)
    buttons.rejected.connect(dialog.close)
    actions.addWidget(buttons)
    root.addLayout(actions)
    format_btn.clicked.connect(lambda: _apply_format_sql(editor, dialog))
    _resize_dialog_to_screen(dialog, 0.82)
    dialog.exec()


def _add_sql_structure_buttons(
    actions: QHBoxLayout,
    editor: QPlainTextEdit,
    parent: QWidget,
    source_row: pd.Series,
    table_review: pd.DataFrame | None,
    view_definitions: pd.DataFrame | None,
) -> None:
    """Give every full SQL viewer the same structural-analysis actions."""
    lineage = QPushButton("Show Lineage")
    lineage.setObjectName("Ghost")
    lineage.setToolTip("Show the tables, views, base tables, and joins used by this SQL.")
    lineage.clicked.connect(
        lambda: _open_sql_lineage_dialog(
            editor.toPlainText(), source_row, table_review, view_definitions, parent
        )
    )
    actions.addWidget(lineage)

    subqueries = QPushButton("Extract Subqueries")
    subqueries.setObjectName("Ghost")
    subqueries.setToolTip("List every CTE and nested subquery, then open lineage for any selected block.")
    subqueries.clicked.connect(
        lambda: _open_sql_subqueries_dialog(
            editor.toPlainText(),
            source_row,
            table_review,
            view_definitions,
            parent,
            source_editor=editor,
        )
    )
    actions.addWidget(subqueries)


def _open_sql_lineage_dialog(
    sql_text: object,
    source_row: pd.Series | dict | None,
    table_review: pd.DataFrame | None,
    view_definitions: pd.DataFrame | None,
    parent: QWidget,
) -> None:
    sql = str(sql_text or "").strip()
    if not sql or sql.startswith("SQL text was not captured"):
        QMessageBox.information(parent, "Show Lineage", "No SQL text is available in this viewer.")
        return
    tables = table_review if table_review is not None else pd.DataFrame()
    views = view_definitions if view_definitions is not None else pd.DataFrame()
    try:
        analysis = analyze_console_sql(sql, tables, pd.DataFrame(), views)
    except Exception as exc:
        QMessageBox.warning(parent, "Show Lineage", str(exc))
        return
    row = source_row.copy() if isinstance(source_row, pd.Series) else pd.Series(source_row or {})
    if not str(row.get("query_id") or "").strip():
        row["query_id"] = "SQL viewer"
    row["sql_text"] = sql
    dialog = _SlowQueryLineageDialog(row, analysis, tables, views, parent)
    _resize_dialog_to_screen(dialog, 0.94)
    dialog.exec()


def _open_sql_subqueries_dialog(
    sql_text: object,
    source_row: pd.Series | dict | None,
    table_review: pd.DataFrame | None,
    view_definitions: pd.DataFrame | None,
    parent: QWidget,
    *,
    source_editor: QPlainTextEdit | None = None,
) -> None:
    sql = str(sql_text or "").strip()
    if not sql or sql.startswith("SQL text was not captured"):
        QMessageBox.information(parent, "Extract Subqueries", "No SQL text is available in this viewer.")
        return
    subqueries = _extract_subquery_rows(sql)
    if subqueries.empty:
        QMessageBox.information(parent, "Extract Subqueries", "No CTEs or nested subqueries were found.")
        return
    row = source_row.copy() if isinstance(source_row, pd.Series) else pd.Series(source_row or {})
    if not str(row.get("query_id") or "").strip():
        row["query_id"] = "SQL viewer"
    row["sql_text"] = sql
    dialog = _SubqueryExtractDialog(
        row,
        subqueries,
        table_review if table_review is not None else pd.DataFrame(),
        view_definitions if view_definitions is not None else pd.DataFrame(),
        parent,
        source_editor=source_editor,
    )
    _resize_dialog_to_screen(dialog, 0.84)
    dialog.exec()


def _attach_table_info_context_menu(editor: QPlainTextEdit, table_review: pd.DataFrame, parent: QWidget) -> None:
    editor.setContextMenuPolicy(Qt.CustomContextMenu)

    def open_menu(pos) -> None:
        menu = editor.createStandardContextMenu()
        menu.addSeparator()
        annotation_action = menu.addAction("Add annotation…")
        selected = _selected_editor_text(editor)
        table_row = _find_table_info_for_selection(selected, table_review)
        action = menu.addAction("Show Table Info")
        action.setEnabled(table_row is not None)
        chosen = menu.exec(editor.mapToGlobal(pos))
        if chosen == annotation_action:
            from .sql_annotations import open_sql_annotation
            open_sql_annotation(editor)
            return
        if chosen != action:
            return
        if table_row is None:
            QMessageBox.information(parent, "Show Table Info", "Highlight schema.table or database.schema.table first.")
            return
        _show_table_info_dialog(table_row, parent)

    editor.customContextMenuRequested.connect(open_menu)


def _find_table_info_for_selection(selection: object, table_review: pd.DataFrame) -> pd.Series | None:
    wanted = _normalize_sql_table_selection(selection)
    if not wanted or table_review is None or table_review.empty:
        return None
    matches: list[tuple[int, float, pd.Series]] = []
    for _, row in table_review.iterrows():
        db = _normalize_sql_table_selection(row.get("source_db"))
        schema = _normalize_sql_table_selection(row.get("schema_name"))
        table = _normalize_sql_table_selection(row.get("table_name"))
        if not table:
            continue
        identifiers = {
            table: 1,
            f"{schema}.{table}" if schema else "": 3,
            f"{db}.{schema}.{table}" if db and schema else "": 5,
        }
        query_table = _normalize_sql_table_selection(row.get("query_table"))
        if query_table:
            identifiers[query_table] = max(identifiers.get(query_table, 0), 4 if "." in query_table else 1)
        score = identifiers.get(wanted, 0)
        if score:
            matches.append((score, _safe_float(row.get("tbl_rows")), row))
    if not matches:
        return None
    matches.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return matches[0][2]


def _normalize_sql_table_selection(value: object) -> str:
    text = _decode_sql_display_escapes(str(value or "")).strip()
    if not text:
        return ""
    object_refs = re.findall(
        r'(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$#]*)(?:\s*\.\s*(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$#]*)){0,2}',
        text,
    )
    if object_refs:
        text = max(object_refs, key=len)
    text = text.replace('"."', ".").replace("`", "").replace("[", "").replace("]", "").replace('"', "")
    text = re.sub(r"\s*\.\s*", ".", text)
    text = re.sub(r"^[^A-Za-z0-9_$#]+|[^A-Za-z0-9_$#]+$", "", text)
    text = re.sub(r"\s+", "", text)
    return text.lower()


def _show_table_info_dialog(row: pd.Series, parent: QWidget) -> None:
    dialog = QDialog(parent)
    dialog.setWindowTitle(f"Table Info - {_sql_object_label(row)}")
    root = QVBoxLayout(dialog)
    root.setContentsMargins(12, 12, 12, 12)
    root.setSpacing(8)
    title = QLabel(_sql_object_label(row))
    title.setObjectName("SectionHeader")
    title.setWordWrap(True)
    root.addWidget(title)
    body = QPlainTextEdit()
    body.setReadOnly(True)
    body.setObjectName("Mono")
    body.setLineWrapMode(QPlainTextEdit.WidgetWidth)
    body.setPlainText(_table_info_dialog_text(row))
    root.addWidget(body, 1)
    buttons = QDialogButtonBox(QDialogButtonBox.Close)
    buttons.rejected.connect(dialog.close)
    root.addWidget(buttons)
    _resize_dialog_to_screen(dialog, 0.52)
    dialog.exec()


def _table_info_dialog_text(row: pd.Series) -> str:
    tbl_rows = _safe_float(row.get("tbl_rows"))
    raw_rows = f"{tbl_rows:,.0f}" if tbl_rows else "-"
    sorted_pct = max(0.0, min(100.0, 100.0 - _safe_float(row.get("unsorted_pct"))))
    return "\n".join(
        [
            "TABLE INFO",
            "",
            f"Table: {_sql_object_label(row)}",
            f"Rows: {_fmt_compact_count(row.get('tbl_rows'))} ({raw_rows})",
            f"Size: {_fmt_value('size_mb', row.get('size_mb'))}",
            f"Diststyle: {_clean_compact_text(row.get('diststyle')) or '-'}",
            f"Distkey: {_clean_compact_text(row.get('distkey')) or '-'}",
            f"Sortkey: {'Unsorted' if _is_missing_sortkey(row.get('sortkey1')) else _clean_compact_text(row.get('sortkey1'))}",
            f"Percent Stats Stale: {_fmt_value('stats_off', row.get('stats_off'))}",
            f"Sorted Pct: {_fmt_value(TABLE_SORTED_PCT_COL, sorted_pct)}",
            f"Skew Rows: {_fmt_value('skew_rows', row.get('skew_rows'))}",
            "",
            "SCORES",
            f"Table Attention: {_fmt_value('table_attention_score', row.get('table_attention_score'))}",
            f"Distribution Usage: {_fmt_value('distribution_usage_score', row.get('distribution_usage_score'))}",
            f"Sort Key Usage: {_fmt_value('sort_key_usage_score', row.get('sort_key_usage_score'))}",
            f"Full Scan: {_fmt_value('full_scan_score', row.get('full_scan_score'))}",
            "",
            f"Recommendation: {row.get('recommendation') or '-'}",
        ]
    )


def _sql_extra_selections(
    editor: QPlainTextEdit,
    rows: pd.DataFrame,
    bg: QColor,
    fg: QColor | None = None,
) -> list[QTextEdit.ExtraSelection]:
    if rows is None or rows.empty:
        return []
    text = editor.toPlainText()
    selections: list[QTextEdit.ExtraSelection] = []
    seen_spans: set[tuple[int, int]] = set()
    fmt = QTextCharFormat()
    fmt.setBackground(bg)
    if fg is not None:
        fmt.setForeground(fg)
    for _, row in rows.iterrows():
        for term in _sql_object_search_terms(row):
            pattern = re.compile(rf"(?<![A-Za-z0-9_$]){re.escape(term)}(?![A-Za-z0-9_$])", re.IGNORECASE)
            for match in pattern.finditer(text):
                span = (match.start(), match.end())
                if span in seen_spans:
                    continue
                seen_spans.add(span)
                cursor = QTextCursor(editor.document())
                cursor.setPosition(match.start())
                cursor.setPosition(match.end(), QTextCursor.KeepAnchor)
                selection = QTextEdit.ExtraSelection()
                selection.cursor = cursor
                selection.format = fmt
                selections.append(selection)
    return selections


def _sql_view_rows(tables: pd.DataFrame) -> pd.DataFrame:
    if tables is None or tables.empty or "object_type" not in tables.columns:
        return pd.DataFrame()
    return tables[tables["object_type"].astype(str).str.contains("view", case=False, na=False)].copy()


def _sql_badge_summary_rows(tables: pd.DataFrame) -> list[tuple[str, list[str]]]:
    rows: list[tuple[str, list[str]]] = []
    if tables is None or tables.empty:
        return rows
    for _, row in tables.iterrows():
        badges = _sql_object_badges(row)
        if badges:
            rows.append((_sql_object_label(row), badges))
    return rows


def _sql_quick_remedies(analysis: SQLLensAnalysis | None) -> list[str]:
    if analysis is None:
        return []
    remedies: list[str] = []
    tables = analysis.tables if analysis.tables is not None else pd.DataFrame()
    joins = analysis.joins if analysis.joins is not None else pd.DataFrame()
    predicates = analysis.predicates if analysis.predicates is not None else pd.DataFrame()

    def add(message: str) -> None:
        message = re.sub(r"\s+", " ", str(message or "")).strip()
        if message and message not in remedies:
            remedies.append(message)

    if joins is not None and not joins.empty:
        for _, row in joins.head(12).iterrows():
            condition = str(row.get("condition") or "")
            if _sql_condition_has_function_call(condition):
                add(
                    f"Join #{row.get('join_no') or '-'} calls a function in the join condition. "
                    "Move the function into a staged/key-normalized column so Redshift can use dist/sort alignment."
                )
            alignment = str(row.get("distribution_alignment") or "").lower()
            if "broadcast" in alignment or "redistribution" in alignment or "not co-located" in alignment:
                add(
                    f"Join #{row.get('join_no') or '-'} is not distribution-aligned. "
                    "Check whether the large joined tables share the same DISTKEY on the join column."
                )
            if "no join condition" in alignment:
                add(f"Join #{row.get('join_no') or '-'} has no ON/USING clause. Confirm this is not an accidental cross join.")

    if predicates is not None and not predicates.empty:
        for _, row in predicates.head(12).iterrows():
            condition = str(row.get("condition") or "")
            if _sql_condition_has_function_call(condition):
                add(
                    f"Predicate #{row.get('predicate_no') or '-'} calls a function. "
                    "Rewrite to compare raw/staged columns where possible so sortkey pruning can work."
                )
            alignment = str(row.get("sortkey_alignment") or "").lower()
            if (
                "not aligned" in alignment
                or "no known sort-key" in alignment
                or "misses sort key" in alignment
                or "missing" in alignment
            ):
                add(
                    f"Predicate #{row.get('predicate_no') or '-'} is not aligned with a leading sort key. "
                    "For large filtered tables, test a sortkey on the selective filter/date column."
                )

    if tables is not None and not tables.empty:
        for _, row in tables.iterrows():
            if "table" not in str(row.get("object_type") or "").lower():
                continue
            label = _sql_object_label(row)
            rows = _safe_float(row.get("tbl_rows"))
            role = str(row.get("role") or "").lower()
            badges = set(_sql_object_badges(row))
            if rows >= 10_000_000 and "join" in role and "NO DISTKEY" in badges:
                add(f"{label} is over 10M rows and participates in joins without a usable DISTKEY. Pick the dominant join key.")
            if rows >= 10_000_000 and "filter" in role and ({"UNSORTED", "SORT MISS"} & badges):
                add(f"{label} is over 10M rows and filtered without useful sort-key support. Consider a leading sortkey for the main predicate.")
            if "STATS MISSING" in badges:
                add(f"{label} has missing table stats. Run ANALYZE after load/vacuum before trusting plan choices.")
            elif "STALE STATS" in badges:
                add(f"{label} has stale stats. Run ANALYZE to improve join order, row estimates, and distribution decisions.")
            if "VIEW" in badges:
                add(f"{label} is a view. Use Identify Views or double-click the highlighted view to expose hidden joins and predicates.")

    if re.search(r"(?is)\bselect\s+\*", analysis.normalized_sql or ""):
        add("SELECT * was detected. Project only needed columns to reduce scan, spill, and network movement.")
    return remedies


def _sql_condition_has_function_call(condition: object) -> bool:
    text = str(condition or "")
    if not text:
        return False
    for match in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_$]*)\s*\(", text):
        name = match.group(1).lower()
        if name in {"select", "from", "where", "join", "on", "case", "when", "then", "else", "end", "in", "exists"}:
            continue
        return True
    return False


def _sql_badge_color(badges: list[str]) -> QColor:
    labels = {badge.upper() for badge in badges}
    if labels & {"NOT FOUND", "NO DISTKEY", "UNSORTED", "STATS MISSING", "BCAST RISK", "DIST RISK"}:
        return QColor("#FCA5A5")
    if labels & {"VIEW", "SORT MISS", "AMBIGUOUS"}:
        return QColor("#FCD34D")
    return QColor("#93C5FD")


def _sql_object_badges(row: pd.Series) -> list[str]:
    badges: list[str] = []
    object_type = str(row.get("object_type") or "").lower()
    status = str(row.get("match_status") or "").lower()
    role = str(row.get("role") or "").lower()
    if "view" in object_type:
        badges.append("VIEW")
    if status == "not found":
        badges.append("NOT FOUND")
    if "ambiguous" in status:
        badges.append("AMBIGUOUS")
    diststyle = str(row.get("diststyle") or "")
    distkey = str(row.get("distkey") or "")
    if "table" in object_type and (_is_auto_even_diststyle(diststyle) or not _clean_compact_text(distkey)):
        badges.append("NO DISTKEY")
    sortkey = row.get("sortkey1")
    if "table" in object_type and _is_missing_sortkey(sortkey):
        badges.append("UNSORTED")
    if _safe_float(row.get("stats_off")) >= 100:
        badges.append("STATS MISSING")
    elif _safe_float(row.get("stats_off")) >= 20:
        badges.append("STALE STATS")
    if _safe_float(row.get("distribution_usage_score")) >= 75 and "join" in role:
        badges.append("DIST RISK")
    if _safe_float(row.get("sort_attention_score")) >= 75 and "filter" in role:
        badges.append("SORT MISS")
    return badges


def _sql_object_inspector_text(row: pd.Series, analysis: SQLLensAnalysis, query_row: pd.Series) -> str:
    badges = _sql_object_badges(row)
    lines = [
        "SQL OBJECT INSPECTOR",
        "",
        _sql_object_label(row),
        f"Type: {row.get('object_type') or '-'}",
        f"Alias: {row.get('alias') or '-'}",
        f"Role: {row.get('role') or '-'}",
        f"Match Status: {row.get('match_status') or '-'}",
    ]
    if badges:
        lines.append(f"Badges: {', '.join(badges)}")
    lines.extend(
        [
            "",
            "PHYSICAL DESIGN",
            f"Dist/Sort: {_format_dist_sort_keys(row)}",
            f"Size/Rows: {_format_size_row_count(row)}",
            f"Stats Off: {_fmt_value('stats_off', row.get('stats_off'))}",
            f"Sorted Pct: {_fmt_value(TABLE_SORTED_PCT_COL, 100 - _safe_float(row.get('unsorted_pct')))}",
            f"Table Attention: {_fmt_value('table_attention_score', row.get('table_attention_score'))}",
            f"Full Scan Score: {_fmt_value('full_scan_score', row.get('full_scan_score'))}",
            f"Distribution Score: {_fmt_value('distribution_usage_score', row.get('distribution_usage_score'))}",
            f"Sort Score: {_fmt_value('sort_attention_score', row.get('sort_attention_score'))}",
            "",
            "QUERY CONTEXT",
            f"Plan broadcast count: {_fmt_int(query_row.get('bcast_cnt'))}",
            f"Plan DS_DIST_BOTH count: {_fmt_int(query_row.get('dist_both_cnt'))}",
            f"Recommendation: {row.get('recommendation') or '-'}",
        ]
    )
    if "view" in str(row.get("object_type") or "").lower():
        lines.extend(["", "VIEW", "Double-click this view name in the SQL to open its captured SQL definition."])
    return "\n".join(lines)


def _sql_token_at_position(text: str, pos: int) -> str:
    if not text:
        return ""
    pos = max(0, min(pos, len(text)))
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_$#\".")
    left = pos
    while left > 0 and text[left - 1] in allowed:
        left -= 1
    right = pos
    while right < len(text) and text[right] in allowed:
        right += 1
    return text[left:right].strip().strip('"')


def _match_sql_object_token(token: str, tables: pd.DataFrame) -> pd.Series | None:
    wanted = _clean_sql_object_name(token)
    if not wanted or tables is None or tables.empty:
        return None
    best: tuple[int, pd.Series] | None = None
    for _, row in tables.iterrows():
        terms = {_clean_sql_object_name(term) for term in _sql_object_search_terms(row)}
        terms = {term for term in terms if term}
        if wanted not in terms:
            continue
        score = 3 if "." in wanted else 1
        if "view" in str(row.get("object_type") or "").lower():
            score += 1
        if best is None or score > best[0]:
            best = (score, row)
    return best[1] if best else None


def _sql_object_search_terms(row: pd.Series) -> list[str]:
    terms: list[str] = []
    for key in ("query_table", "table_name", "alias"):
        value = str(row.get(key) or "").strip()
        if value and value.lower() not in {"nan", "none"}:
            terms.append(value)
            if "." in value:
                terms.append(value.split(".")[-1])
    schema = str(row.get("schema_name") or "").strip()
    table = str(row.get("table_name") or "").strip()
    if schema and table:
        terms.append(f"{schema}.{table}")
    out: list[str] = []
    for term in terms:
        cleaned = term.strip().strip('"')
        if cleaned and cleaned not in out and len(cleaned) > 1:
            out.append(cleaned)
    return out


def _clean_sql_object_name(value: object) -> str:
    text = str(value or "").strip().strip('"').lower()
    text = re.sub(r"\s+", "", text)
    return text


def _sql_object_label(row: pd.Series) -> str:
    parts = [
        str(row.get("source_db") or "").strip(),
        str(row.get("schema_name") or "").strip(),
        str(row.get("table_name") or row.get("query_table") or "").strip(),
    ]
    label = ".".join(part for part in parts if part and part.lower() not in {"nan", "none"})
    return label or str(row.get("query_table") or row.get("alias") or "object")


def _selected_editor_text(editor: QPlainTextEdit) -> str:
    text = editor.textCursor().selectedText()
    return text.replace("\u2029", "\n").replace("\u2028", "\n").strip()


def _coerce_sql_fragment_for_analysis(sql_text: object) -> str:
    sql = _decode_sql_display_escapes(str(sql_text or "")).strip().strip(";")
    first = _first_sql_word(sql)
    if first in {"select", "with", "insert", "update", "delete"}:
        return sql
    lowered = f" {sql.lower()} "
    if first == "from":
        return f"SELECT *\n{sql}"
    if " join " in lowered and " from " not in lowered:
        return f"SELECT *\nFROM {sql}"
    return sql


def _extract_subquery_rows(sql_text: object) -> pd.DataFrame:
    source_sql = _decode_sql_display_escapes(str(sql_text or ""))
    sql = (_extract_wrapped_command_inner_sql(source_sql) or source_sql).strip()
    tree = _parse_sql_for_subqueries(sql)
    if tree is None:
        return _fallback_subquery_rows(source_sql)
    rows: list[dict] = []
    used_spans: set[tuple[int, int]] = set()

    def add(kind: str, name: str, node) -> None:
        block_sql = _sqlglot_node_sql(node)
        if not block_sql:
            return
        source_start, source_end = _find_sql_block_span(source_sql, block_sql, used_spans)
        if source_start >= 0:
            used_spans.add((source_start, source_end))
        rows.append(
            {
                "subquery_no": len(rows) + 1,
                "kind": kind,
                "name": name,
                "table_count": _sqlglot_node_count(block_sql, "table"),
                "join_count": _sqlglot_node_count(block_sql, "join"),
                "sql_preview": _clip(re.sub(r"\s+", " ", block_sql).strip(), 220),
                "sql_text": block_sql,
                "source_start": source_start,
                "source_end": source_end,
            }
        )

    try:
        from sqlglot import exp

        for cte in tree.find_all(exp.CTE):
            add("cte", str(cte.alias_or_name or "").strip(), cte.this)
        for subquery in tree.find_all(exp.Subquery):
            add("subquery", str(subquery.alias_or_name or "").strip(), getattr(subquery, "this", subquery))
    except Exception:
        pass
    if not rows:
        return _fallback_subquery_rows(source_sql)
    return pd.DataFrame(rows, columns=SUBQUERY_EXTRACT_DATA_COLS)


def _fallback_subquery_rows(sql_text: object) -> pd.DataFrame:
    """Extract balanced parenthesized SELECT/WITH blocks when full parsing fails."""
    sql = _decode_sql_display_escapes(str(sql_text or ""))
    stack: list[int] = []
    spans: list[tuple[int, int]] = []
    quote = ""
    line_comment = False
    block_comment = False
    index = 0
    while index < len(sql):
        char = sql[index]
        nxt = sql[index + 1] if index + 1 < len(sql) else ""
        if line_comment:
            if char == "\n":
                line_comment = False
            index += 1
            continue
        if block_comment:
            if char == "*" and nxt == "/":
                block_comment = False
                index += 2
            else:
                index += 1
            continue
        if quote:
            if char == quote:
                if nxt == quote:
                    index += 2
                    continue
                quote = ""
            index += 1
            continue
        if char == "-" and nxt == "-":
            line_comment = True
            index += 2
            continue
        if char == "/" and nxt == "*":
            block_comment = True
            index += 2
            continue
        if char in {"'", '"'}:
            quote = char
        elif char == "(":
            stack.append(index)
        elif char == ")" and stack:
            start = stack.pop()
            body = sql[start + 1:index].strip()
            if re.match(r"^(?:select|with)\b", body, flags=re.IGNORECASE):
                spans.append((start + 1, index))
        index += 1

    rows: list[dict] = []
    for start, end in sorted(set(spans), key=lambda item: (item[0], item[1])):
        block_sql = sql[start:end].strip()
        prefix = sql[max(0, start - 180):start - 1]
        cte_match = re.search(r'([A-Za-z_][\w$]*)\s+AS\s*$', prefix, flags=re.IGNORECASE)
        kind = "cte" if cte_match else "subquery"
        name = cte_match.group(1) if cte_match else ""
        rows.append(
            {
                "subquery_no": len(rows) + 1,
                "kind": kind,
                "name": name,
                "table_count": len(re.findall(r"\b(?:from|join)\s+", block_sql, flags=re.IGNORECASE)),
                "join_count": len(re.findall(r"\bjoin\s+", block_sql, flags=re.IGNORECASE)),
                "sql_preview": _clip(re.sub(r"\s+", " ", block_sql), 220),
                "sql_text": block_sql,
                "source_start": start,
                "source_end": end,
            }
        )
    return pd.DataFrame(rows, columns=SUBQUERY_EXTRACT_DATA_COLS)


def _queries_with_views_frame(
    known_queries: pd.DataFrame | None,
    view_map: dict[str, str] | None,
) -> pd.DataFrame:
    columns = ["query_id", "view_count", "views", "sql_preview", "sql_text"]
    if known_queries is None or known_queries.empty or not view_map:
        return pd.DataFrame(columns=columns)
    sql_col = next(
        (name for name in ("sql_text", "query_text", "display_sql", "text") if name in known_queries.columns),
        "",
    )
    if not sql_col:
        return pd.DataFrame(columns=columns)
    query_col = next(
        (name for name in ("query_id", "query", "qid") if name in known_queries.columns),
        "",
    )
    rows: list[dict] = []
    for position, (_, row) in enumerate(known_queries.iterrows(), start=1):
        sql = str(row.get(sql_col) or "").strip()
        if not sql:
            continue
        _expanded, exploded = explode_views(sql, view_map)
        if not exploded:
            continue
        unique_views = list(dict.fromkeys(exploded))
        rows.append(
            {
                "query_id": str(row.get(query_col) if query_col else position),
                "view_count": len(exploded),
                "views": ", ".join(unique_views),
                "sql_preview": _clip(re.sub(r"\s+", " ", sql), 260),
                "sql_text": sql,
            }
        )
    if not rows:
        return pd.DataFrame(columns=columns)
    return (
        pd.DataFrame(rows, columns=columns)
        .drop_duplicates(subset=["query_id", "sql_text"], keep="first")
        .sort_values(["view_count", "query_id"], ascending=[False, True], kind="stable")
        .reset_index(drop=True)
    )


def _find_sql_block_span(
    source_sql: str,
    block_sql: str,
    used_spans: set[tuple[int, int]] | None = None,
) -> tuple[int, int]:
    """Locate SQLGlot's formatted block in the original editor text by tokens."""
    used = used_spans or set()
    try:
        from sqlglot.tokens import Tokenizer

        try:
            tokenizer = Tokenizer(dialect="redshift")
        except TypeError:
            tokenizer = Tokenizer()
        source_tokens = tokenizer.tokenize(source_sql)
        block_tokens = tokenizer.tokenize(block_sql)
    except Exception:
        return (-1, -1)
    if not source_tokens or not block_tokens or len(block_tokens) > len(source_tokens):
        return (-1, -1)

    wanted = [str(token.text).casefold() for token in block_tokens]
    available = [str(token.text).casefold() for token in source_tokens]
    width = len(wanted)
    for index in range(len(available) - width + 1):
        if available[index:index + width] != wanted:
            continue
        start = int(source_tokens[index].start)
        end = int(source_tokens[index + width - 1].end) + 1
        span = (start, end)
        if span not in used:
            return span
    return (-1, -1)


def _parse_sql_for_subqueries(sql: str):
    if not sql:
        return None
    try:
        import sqlglot

        return sqlglot.parse_one(sql, read="redshift")
    except Exception:
        try:
            import sqlglot

            return sqlglot.parse_one(sql)
        except Exception:
            return None


def _sqlglot_node_sql(node) -> str:
    try:
        return str(node.sql(dialect="redshift", pretty=True)).strip()
    except Exception:
        try:
            return str(node.sql(pretty=True)).strip()
        except Exception:
            return str(node or "").strip()


def _sqlglot_node_count(sql: str, kind: str) -> int:
    tree = _parse_sql_for_subqueries(sql)
    if tree is None:
        return 0
    try:
        from sqlglot import exp

        if kind == "join":
            return sum(1 for _ in tree.find_all(exp.Join))
        return sum(1 for _ in tree.find_all(exp.Table))
    except Exception:
        return 0


_FIX_QUERY_INITIATIVES = {
    "statistics": {
        "order": 10,
        "initiative": "Restore optimizer statistics",
        "decision": "Approve targeted ANALYZE for the affected tables.",
        "reason": "Stale statistics can cause the optimizer to choose the wrong join order, distribution strategy, and row estimates across many queries.",
        "readiness": "Runbook-ready",
        "next_step": "Have the DBA validate the listed tables and schedule targeted ANALYZE statements.",
    },
    "scan_sort": {
        "order": 20,
        "initiative": "Restore scan pruning",
        "decision": "Prioritize table sorting and recurring scan-path corrections.",
        "reason": "Unsorted data and weak predicate alignment force Redshift to read more blocks than the business request requires.",
        "readiness": "Maintenance + review",
        "next_step": "Separate immediate VACUUM work from longer-term sort-key or load-order corrections.",
    },
    "distribution": {
        "order": 30,
        "initiative": "Reduce join data movement",
        "decision": "Review distribution design and the highest-movement join shapes together.",
        "reason": "Redistribution, broadcast, and slice imbalance multiply runtime across every affected join rather than harming only one query ID.",
        "readiness": "DBA design review",
        "next_step": "Validate dominant join keys, then choose co-location, EVEN distribution, or controlled staging per workload family.",
    },
    "spill": {
        "order": 40,
        "initiative": "Reduce disk spill",
        "decision": "Rewrite recurring spill-heavy shapes before considering broad capacity changes.",
        "reason": "Disk spill is a symptom of wide or oversized intermediate work and can turn otherwise manageable joins and sorts into long-running operations.",
        "readiness": "Engineering rewrite",
        "next_step": "Start with the highest-runtime shapes; reduce width, filter earlier, and pre-aggregate before joins or sorts.",
    },
    "external": {
        "order": 50,
        "initiative": "Reduce repeated external scans",
        "decision": "Decide which recurring S3 inputs should be materialized, repartitioned, or pruned earlier.",
        "reason": "Repeated remote scans introduce latency and data transfer that local sort and distribution changes cannot correct.",
        "readiness": "Architecture review",
        "next_step": "Identify stable recurring inputs and test a locally staged or partition-pruned path for the highest-runtime group.",
    },
    "fanout": {
        "order": 60,
        "initiative": "Correct join fan-out",
        "decision": "Validate join cardinality and deduplicate one-to-many inputs before tuning infrastructure.",
        "reason": "Unexpected row multiplication increases I/O, memory, spill, and downstream runtime, so treating each resulting slow query separately misses the shared defect.",
        "readiness": "Engineering review",
        "next_step": "Confirm intended join keys and uniqueness rules with the query owner, then test pre-deduplication where necessary.",
    },
    "other": {
        "order": 90,
        "initiative": "Review remaining workload findings",
        "decision": "Assign the remaining findings to a DBA for classification.",
        "reason": "These findings do not yet form a sufficiently specific initiative for executive approval.",
        "readiness": "Needs triage",
        "next_step": "Open Technical Evidence and classify the findings before presenting them as committed work.",
    },
}


def _fix_query_initiative_key(row: pd.Series, source: str) -> str:
    action_id = _clean_compact_text(row.get("action_id")).upper()
    action_type = _clean_compact_text(row.get("action_type")).lower()
    title = _clean_compact_text(row.get("title")).lower()
    if source == "action":
        if "ANALYZE" in action_id:
            return "statistics"
        if "VACUUM" in action_id or "HEAVY_SCAN" in action_id or "scan" in action_type:
            return "scan_sort"
        if "DISTRIBUTION" in action_id or "physical design" in action_type:
            return "distribution"
    if "distributed join" in title:
        return "distribution"
    if "spill" in title:
        return "spill"
    if "external" in title or "spectrum" in title:
        return "external"
    if "fan-out" in title or "fanout" in title:
        return "fanout"
    return "other"


def _normalized_query_id(value: object) -> str:
    text = _clean_compact_text(value)
    if not text or text.lower() in {"none", "nan", "<na>"}:
        return ""
    try:
        number = float(text)
        if number.is_integer():
            return str(int(number))
    except (TypeError, ValueError):
        pass
    return text


def _slow_query_runtime_lookup(slow_queries: pd.DataFrame | None) -> dict[str, float]:
    if slow_queries is None or slow_queries.empty or "query_id" not in slow_queries.columns:
        return {}
    lookup: dict[str, float] = {}
    for _, row in slow_queries.iterrows():
        query_id = _normalized_query_id(row.get("query_id"))
        if query_id:
            lookup[query_id] = max(lookup.get(query_id, 0.0), _safe_float(row.get("elapsed_s")))
    return lookup


def _build_fix_query_initiatives(
    actions: pd.DataFrame,
    rewrites: pd.DataFrame,
    slow_queries: pd.DataFrame | None = None,
) -> pd.DataFrame:
    action_rows = actions.reset_index(drop=True) if actions is not None else pd.DataFrame()
    rewrite_rows = rewrites.reset_index(drop=True) if rewrites is not None else pd.DataFrame()
    runtime_lookup = _slow_query_runtime_lookup(slow_queries)
    buckets: dict[str, list[tuple[str, int, pd.Series]]] = {}
    for source, frame in (("action", action_rows), ("rewrite", rewrite_rows)):
        if frame.empty:
            continue
        for position, row in frame.iterrows():
            buckets.setdefault(_fix_query_initiative_key(row, source), []).append((source, int(position), row))
    rows: list[dict] = []
    for key, findings in buckets.items():
        spec = _FIX_QUERY_INITIATIVES[key]
        query_ids: set[str] = set()
        table_keys: set[str] = set()
        targets: list[str] = []
        critical = 0
        warning = 0
        max_score = 0.0
        strongest_score = -1.0
        strongest_evidence = ""
        query_runtime_map: dict[str, float] = {}
        action_positions: list[int] = []
        rewrite_positions: list[int] = []
        for source, position, finding in findings:
            if source == "action":
                action_positions.append(position)
                query_id = _normalized_query_id(finding.get("query_id"))
                table_key = _clean_compact_text(finding.get("table_key"))
                evidence = _clean_compact_text(finding.get("evidence")) or _clean_compact_text(finding.get("why_now"))
                score = _safe_float(finding.get("action_score"))
            else:
                rewrite_positions.append(position)
                query_id = _normalized_query_id(finding.get("query_id")) or _normalized_query_id(finding.get("subject"))
                table_key = _clean_compact_text(finding.get("table_key"))
                evidence = _clean_compact_text(finding.get("trigger")) or _clean_compact_text(finding.get("why_it_matters"))
                score = _safe_float(finding.get("impact_score"))
            if query_id:
                query_ids.add(query_id)
                query_runtime_map[query_id] = max(
                    query_runtime_map.get(query_id, 0.0),
                    runtime_lookup.get(query_id, _safe_float(finding.get("elapsed_s"))),
                )
            if table_key and table_key.lower() not in {"none", "nan", "<na>"}:
                table_keys.add(table_key)
            target = _clean_compact_text(finding.get("subject"))
            if target and target not in targets:
                targets.append(target)
            severity = _clean_compact_text(finding.get("severity")).lower() or "info"
            critical += int(severity == "crit")
            warning += int(severity == "warn")
            max_score = max(max_score, score)
            if score > strongest_score and evidence:
                strongest_score = score
                strongest_evidence = evidence
        runtime_s = sum(query_runtime_map.get(query_id, 0.0) for query_id in query_ids)
        scope_parts = [f"{len(findings):,} finding(s)"]
        if query_ids:
            scope_parts.append(f"{len(query_ids):,} quer{'y' if len(query_ids) == 1 else 'ies'}")
        if table_keys:
            scope_parts.append(f"{len(table_keys):,} table(s)")
        rows.append(
            {
                "initiative_key": key,
                "initiative_order": spec["order"],
                "initiative": spec["initiative"],
                "decision": spec["decision"],
                "executive_reason": spec["reason"],
                "readiness": spec["readiness"],
                "next_step": spec["next_step"],
                "scope": " • ".join(scope_parts),
                "runtime_s": runtime_s,
                "runtime_in_scope": _fmt_seconds(runtime_s) if runtime_s > 0 else "not linked",
                "critical_findings": critical,
                "warning_findings": warning,
                "max_score": max_score,
                "strongest_evidence": strongest_evidence or "Evidence text was not supplied by the loaded action rows.",
                "sample_targets": targets[:5],
                "query_ids": sorted(query_ids, key=lambda value: (not value.isdigit(), int(value) if value.isdigit() else value)),
                "query_runtime_map": query_runtime_map,
                "table_keys": sorted(table_keys),
                "action_positions": action_positions,
                "rewrite_positions": rewrite_positions,
                "visual_status": "red" if critical else "amber" if warning else "green",
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out = out.sort_values(
        ["critical_findings", "runtime_s", "max_score", "initiative_order"],
        ascending=[False, False, False, True],
        kind="stable",
    ).reset_index(drop=True)
    out.insert(0, "priority", range(1, len(out) + 1))
    return out


def _fix_query_evidence_frame(
    initiative: pd.Series,
    actions: pd.DataFrame,
    rewrites: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict] = []
    action_rows = actions.reset_index(drop=True) if actions is not None else pd.DataFrame()
    rewrite_rows = rewrites.reset_index(drop=True) if rewrites is not None else pd.DataFrame()
    for position in initiative.get("action_positions", []):
        if position < 0 or position >= len(action_rows):
            continue
        row = action_rows.iloc[position]
        rows.append(
            {
                "source": "DBA action",
                "severity": row.get("severity"),
                "target": row.get("subject"),
                "query_id": row.get("query_id"),
                "table_key": row.get("table_key"),
                "evidence": _clean_compact_text(row.get("evidence")) or _clean_compact_text(row.get("why_now")),
                "recommendation": row.get("what_to_do"),
                "candidate_sql": _clean_compact_text(row.get("sql_hint")),
            }
        )
    for position in initiative.get("rewrite_positions", []):
        if position < 0 or position >= len(rewrite_rows):
            continue
        row = rewrite_rows.iloc[position]
        rows.append(
            {
                "source": "Query rewrite",
                "severity": row.get("severity"),
                "target": row.get("subject"),
                "query_id": row.get("query_id"),
                "table_key": row.get("table_key"),
                "evidence": _clean_compact_text(row.get("trigger")) or _clean_compact_text(row.get("why_it_matters")),
                "recommendation": _clean_compact_text(row.get("rewrite_shape")),
                "candidate_sql": _clean_compact_text(row.get("candidate_sql")),
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "source",
            "severity",
            "target",
            "query_id",
            "table_key",
            "evidence",
            "recommendation",
            "candidate_sql",
        ],
    )


def _action_summary_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    if "action_type" not in work.columns:
        work["action_type"] = "action"
    if "severity" not in work.columns:
        work["severity"] = "info"
    if "action_score" not in work.columns:
        work["action_score"] = 0
    work["_action_type"] = (
        work["action_type"].fillna("action").astype(str).str.strip().replace("", "action").str.upper()
    )
    work["_severity"] = work["severity"].fillna("info").astype(str).str.lower()
    work["_score"] = pd.to_numeric(work["action_score"], errors="coerce").fillna(0)
    if "priority_rank" in work.columns:
        work["_rank"] = pd.to_numeric(work["priority_rank"], errors="coerce").fillna(999999)
    else:
        work["_rank"] = 999999

    rows = []
    for action_type, group in work.groupby("_action_type", sort=False):
        top = group.sort_values(["_score", "_rank"], ascending=[False, True]).iloc[0]
        reason = _clean_compact_text(top.get("why_now")) or _clean_compact_text(top.get("evidence"))
        rows.append(
            {
                "action_type": action_type,
                "action_count": int(len(group)),
                "critical_count": int((group["_severity"] == "crit").sum()),
                "warning_count": int((group["_severity"] == "warn").sum()),
                "top_score": round(_safe_float(top.get("_score"))),
                "top_subject": _clean_compact_text(top.get("subject")) or "cluster",
                "top_reason": _clip(reason, 180),
            }
        )
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["top_score", "action_count"], ascending=[False, False], na_position="last")
    return out.reset_index(drop=True)


def _repeat_groups_for_display(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["repeat_priority_score"] = _repeat_priority_scores(out)
    out["similarity_label"] = out.get("avg_similarity", pd.Series(index=out.index, dtype="float64")).map(_similarity_label)
    out["repeat_verdict"] = out.apply(_repeat_verdict, axis=1)
    out["impact_summary"] = out.apply(_repeat_impact_summary, axis=1)
    out["fix_hint"] = out.apply(_repeat_fix_hint, axis=1)
    sort_cols = [col for col in ("repeat_priority_score", "total_runtime_s", "query_count") if col in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, ascending=[False] * len(sort_cols), na_position="last")
    return out.reset_index(drop=True)


def _repeat_members_for_display(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["match_verdict"] = out.get("similarity_score", pd.Series(index=out.index, dtype="float64")).map(_member_match_label)
    sort_cols = [col for col in ("repeat_group_id", "elapsed_s", "risk_score") if col in out.columns]
    if sort_cols:
        ascending = [True] + [False] * (len(sort_cols) - 1)
        out = out.sort_values(sort_cols, ascending=ascending, na_position="last")
    return out.reset_index(drop=True)


def _repeat_diagnostic_summary(summary: dict | None) -> str:
    if not summary:
        return ""
    note = str(summary.get("repeat_diagnostic_note") or "").strip()
    if not note:
        return ""
    prepared = _safe_int(summary.get("repeat_prepared_query_count"))
    same_user = _safe_int(summary.get("repeat_same_user_bucket_count"))
    same_table = _safe_int(summary.get("repeat_same_user_table_bucket_count"))
    strict = _safe_int(summary.get("repeat_strict_family_bucket_count"))
    best = _safe_float(summary.get("repeat_best_strict_similarity"))
    best_table = _safe_float(summary.get("repeat_best_same_user_table_similarity"))
    parts = [
        f"Diagnostics: {note}",
        f"analyzable={prepared:,}",
        f"same-user buckets={same_user:,}",
        f"same-user/query-type buckets={same_table:,}",
        f"strict buckets={strict:,}",
    ]
    if best:
        parts.append(f"best strict={best * 100:.0f}%")
    if best_table:
        parts.append(f"best same-user/table={best_table * 100:.0f}%")
    pair = str(summary.get("repeat_best_same_user_table_query_ids") or "").strip()
    if pair:
        parts.append(f"best near-miss query IDs={pair}")
    return " | ".join(parts)


def _repeat_priority_scores(df: pd.DataFrame) -> pd.Series:
    runtime = _log_scaled(_num_series(df, "total_runtime_s"), 40)
    frequency = _log_scaled(_num_series(df, "query_count"), 30)
    risk = (_num_series(df, "max_risk_score").clip(lower=0, upper=180) / 180.0) * 20
    similarity = _num_series(df, "avg_similarity").clip(lower=0, upper=1)
    confidence = ((similarity - 0.84).clip(lower=0) / 0.16).clip(upper=1) * 10
    return (runtime + frequency + risk + confidence).clip(lower=0, upper=100).round(0)


def _repeat_verdict(row: pd.Series) -> str:
    score = _safe_float(row.get("repeat_priority_score"))
    count = _safe_float(row.get("query_count"))
    runtime = _safe_float(row.get("total_runtime_s"))
    similarity = _safe_float(row.get("avg_similarity"))
    if score >= 75 or (count >= 10 and runtime >= 1800):
        return "Fix once"
    if similarity >= 0.97:
        return "Same template"
    if similarity >= 0.92:
        return "Strong family"
    return "Review pattern"


def _repeat_impact_summary(row: pd.Series) -> str:
    parts = [
        f"{_fmt_int(row.get('query_count'))} runs",
        f"{_fmt_seconds(row.get('total_runtime_s'))} total",
        f"{_fmt_seconds(row.get('worst_runtime_s'))} worst",
    ]
    users = _compact_count_text(row.get("users"), "user")
    dbs = _compact_count_text(row.get("databases"), "db")
    if users:
        parts.append(users)
    if dbs:
        parts.append(dbs)
    return " / ".join(parts)


def _repeat_fix_hint(row: pd.Series) -> str:
    count = _safe_float(row.get("query_count"))
    runtime = _safe_float(row.get("total_runtime_s"))
    joins = _safe_float(row.get("join_count"))
    predicates = _safe_float(row.get("predicate_count"))
    wildcards = _safe_float(row.get("wildcard_count"))
    shared_tables = str(row.get("shared_tables") or row.get("sql_tables") or "").strip()
    if str(row.get("repeat_kind") or "").lower() == "stored_procedure":
        key = str(row.get("procedure_key") or "").strip()
        target = f" `{key}`" if key else ""
        return f"Stored procedure repeat: inspect the captured body for{target}; CALL parameters were stripped from grouping."
    if count >= 25:
        return "Application/report template candidate: one upstream SQL change can remove many repeated slow runs."
    if runtime >= 3600:
        return "High-runtime repeat family: tune the shared access path first, then confirm every member improves."
    if wildcards:
        return "Repeated SELECT * shape: trim projected columns and check whether wide rows drive scan and network cost."
    if joins >= 3:
        return "Repeated join shape: inspect join keys, distribution alignment, and whether a common pre-aggregate can replace repeated joins."
    if predicates:
        return "Repeated filter shape: check sort-key alignment and whether a dashboard/app parameter creates this pattern."
    if shared_tables:
        return f"Start with shared table path: {shared_tables}."
    return "Open representative SQL, confirm the generating report/app, then fix that template once."


def _repeat_chip_severity(row: pd.Series) -> str:
    score = _safe_float(row.get("repeat_priority_score"))
    if score >= 75:
        return "crit"
    if score >= 45:
        return "warn"
    return "info"


def _similarity_label(value: object) -> str:
    similarity = _safe_float(value)
    if similarity >= 0.97:
        return "same template"
    if similarity >= 0.92:
        return "strong template match"
    if similarity >= 0.84:
        return "likely same workload"
    if similarity > 0:
        return "weak match"
    return "unknown"


def _member_match_label(value: object) -> str:
    similarity = _safe_float(value)
    if similarity >= 0.97:
        return "same template"
    if similarity >= 0.92:
        return "close variant"
    if similarity >= 0.84:
        return "related"
    return "manual review"


def _compact_count_text(value: object, noun: str) -> str:
    items = [part.strip() for part in str(value or "").split(",") if part.strip()]
    if not items:
        return ""
    suffix = noun if len(items) == 1 else f"{noun}s"
    return f"{len(items)} {suffix}"


def _safe_float(value: object) -> float:
    try:
        if pd.isna(value):
            return 0.0
    except (TypeError, ValueError):
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: object, fallback: int = 0) -> int:
    try:
        if pd.isna(value):
            return fallback
    except (TypeError, ValueError):
        pass
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return fallback


def _safe_int_or_none(value: object) -> int | None:
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _plan_text(label: object, info: object = "") -> str:
    return f"{label or ''} {info or ''}".lower()


def _plan_severity(label: object, info: object = "") -> str:
    text = _plan_text(label, info)
    if "ds_dist_both" in text or "nested loop" in text or "missing statistics" in text:
        return "crit"
    if "ds_bcast_inner" in text or "s3 seq scan" in text or "partition loop" in text:
        return "warn"
    if "seq scan" in text or "filter:" in text or "sort" in text:
        return "warn"
    return "info"


def _plan_color(label: object, info: object = "") -> str:
    text = _plan_text(label, info)
    severity = _plan_severity(label, info)
    if severity == "crit":
        return PALETTE.crit
    if "join" in text:
        return PALETTE.pink
    if "scan" in text:
        return PALETTE.cyan
    if "sort" in text:
        return PALETTE.accent_bright
    if "aggregate" in text or "agg" in text:
        return PALETTE.warn
    if severity == "warn":
        return PALETTE.warn
    return PALETTE.text_2


def _explain_plan_summary(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "No full explain rows loaded."
    text = " ".join(
        (
            df.get("plan_node", pd.Series(dtype="object")).fillna("").astype(str)
            + " "
            + df.get("plan_info", pd.Series(dtype="object")).fillna("").astype(str)
        )
        .str.lower()
        .tolist()
    )
    checks = [
        ("nodes", len(df)),
        ("redistribute both", text.count("ds_dist_both")),
        ("broadcast", text.count("ds_bcast_inner")),
        ("nested loop", text.count("nested loop")),
        ("s3 scans", text.count("s3 seq scan")),
        ("missing stats", text.count("missing statistics")),
        ("filters", text.count("filter:")),
    ]
    return " | ".join(f"{label}: {value}" for label, value in checks)


def _blend_palette_hex(start_hex: str, end_hex: str, fraction: float) -> QColor:
    fraction = max(0.0, min(1.0, fraction))
    start = QColor(start_hex)
    end = QColor(end_hex)
    return QColor(
        int(start.red() + (end.red() - start.red()) * fraction),
        int(start.green() + (end.green() - start.green()) * fraction),
        int(start.blue() + (end.blue() - start.blue()) * fraction),
    )


def _fmt_contrib_value(value: float, unit: str) -> str:
    if unit == "s":
        if value >= 3600:
            return f"{value / 3600:.1f} h"
        if value >= 60:
            return f"{value / 60:.1f} m"
        return f"{value:.0f} s"
    return _fmt_compact_count(value)


class _ContributorBars(QFrame):
    """Ranked share-of-total bars in a single hue: magnitude is the length,
    identity is the row label, and the percentage is a direct text label."""

    _MAX_ROWS = 12

    def __init__(self, title: str, parent=None):
        super().__init__(parent)
        self.setObjectName("Card")
        self.setMouseTracking(True)
        self._title = title
        self._rows: list[tuple[str, float]] = []
        self._unit = ""
        self._empty_hint = "No data loaded for this panel yet."
        self._bar_rects: list[tuple[QRectF, str]] = []
        self.setMinimumHeight(160)

    def set_rows(self, rows: list[tuple[str, float]], unit: str, empty_hint: str = "") -> None:
        cleaned = [(str(label), float(value)) for label, value in rows if float(value) > 0]
        cleaned.sort(key=lambda item: -item[1])
        if len(cleaned) > self._MAX_ROWS:
            other_total = sum(value for _, value in cleaned[self._MAX_ROWS - 1 :])
            cleaned = cleaned[: self._MAX_ROWS - 1] + [("(other)", other_total)]
        self._rows = cleaned
        self._unit = unit
        if empty_hint:
            self._empty_hint = empty_hint
        self.setMinimumHeight(58 + max(1, len(cleaned)) * 28)
        self.update()

    def paintEvent(self, event) -> None:
        super().paintEvent(event)
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("Inter", 8, QFont.Bold))
        p.drawText(QRectF(14, 8, self.width() - 28, 16), Qt.AlignLeft, self._title)
        self._bar_rects = []
        if not self._rows:
            p.setFont(QFont("Inter", 9))
            p.drawText(self.rect(), Qt.AlignCenter, self._empty_hint)
            return
        total = sum(value for _, value in self._rows) or 1.0
        top = 32.0
        row_h = 28.0
        label_w = max(120.0, self.width() * 0.28)
        value_w = 118.0
        track_left = 14 + label_w + 8
        track_w = max(60.0, self.width() - track_left - value_w - 14)
        max_value = max(value for _, value in self._rows) or 1.0
        for i, (label, value) in enumerate(self._rows):
            y = top + i * row_h
            share = value / total
            p.setPen(QColor(PALETTE.text_1))
            p.setFont(QFont("Inter", 8))
            p.drawText(QRectF(14, y, label_w, 20), Qt.AlignLeft | Qt.AlignVCenter, _clip(label, int(label_w / 5.6)))
            track = QRectF(track_left, y + 4, track_w, 12)
            p.setPen(Qt.NoPen)
            p.setBrush(QColor(PALETTE.bg_2))
            p.drawRoundedRect(track, 3, 3)
            bar_w = max(3.0, track_w * (value / max_value))
            bar = QRectF(track_left, y + 4, bar_w, 12)
            p.setBrush(QColor(PALETTE.accent))
            p.drawRoundedRect(bar, 3, 3)
            self._bar_rects.append((QRectF(14, y, self.width() - 28, row_h), label))
            p.setPen(QColor(PALETTE.text_0))
            p.setFont(QFont("Inter", 8, QFont.DemiBold))
            p.drawText(
                QRectF(track_left + track_w + 6, y, value_w - 8, 20),
                Qt.AlignLeft | Qt.AlignVCenter,
                f"{share * 100:.0f}%  ({_fmt_contrib_value(value, self._unit)})",
            )

    def mouseMoveEvent(self, event) -> None:
        pos = event.position()
        total = sum(value for _, value in self._rows) or 1.0
        for (rect, label), (_, value) in zip(self._bar_rects, self._rows):
            if rect.contains(pos):
                QToolTip.showText(
                    event.globalPosition().toPoint(),
                    f"{label}\n{_fmt_contrib_value(value, self._unit)}  ({value / total * 100:.1f}% of shown total)",
                    self,
                )
                break
        super().mouseMoveEvent(event)


class _ContributorHeatMap(QFrame):
    """Database x issue-family heat map on a single-hue sequential ramp."""

    def __init__(self, title: str, parent=None):
        super().__init__(parent)
        self.setObjectName("Card")
        self.setMouseTracking(True)
        self._title = title
        self._row_labels: list[str] = []
        self._col_labels: list[str] = []
        self._values: dict[tuple[str, str], float] = {}
        self._cell_rects: list[tuple[QRectF, str, str, float]] = []
        self._empty_hint = "Load insights (and slow queries) to build the heat map."
        self.setMinimumHeight(200)

    def set_matrix(self, row_labels: list[str], col_labels: list[str], values: dict[tuple[str, str], float]) -> None:
        self._row_labels = list(row_labels)
        self._col_labels = list(col_labels)
        self._values = dict(values)
        self.setMinimumHeight(74 + max(1, len(self._row_labels)) * 30)
        self.update()

    def paintEvent(self, event) -> None:
        super().paintEvent(event)
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("Inter", 8, QFont.Bold))
        p.drawText(QRectF(14, 8, self.width() - 28, 16), Qt.AlignLeft, self._title)
        self._cell_rects = []
        if not self._row_labels or not self._col_labels:
            p.setFont(QFont("Inter", 9))
            p.drawText(self.rect(), Qt.AlignCenter, self._empty_hint)
            return
        grand_total = sum(self._values.values()) or 1.0
        max_value = max(self._values.values(), default=1.0) or 1.0
        label_w = max(110.0, self.width() * 0.20)
        left = 14 + label_w + 6
        top = 46.0
        cell_gap = 2.0
        cell_w = max(34.0, (self.width() - left - 14 - cell_gap * len(self._col_labels)) / len(self._col_labels))
        cell_h = 28.0
        p.setFont(QFont("Inter", 7, QFont.Bold))
        p.setPen(QColor(PALETTE.text_2))
        for c, col in enumerate(self._col_labels):
            x = left + c * (cell_w + cell_gap)
            p.drawText(QRectF(x, top - 18, cell_w, 16), Qt.AlignCenter, _clip(col, max(6, int(cell_w / 5.4))))
        for r, row_label in enumerate(self._row_labels):
            y = top + r * (cell_h + cell_gap)
            p.setPen(QColor(PALETTE.text_1))
            p.setFont(QFont("Inter", 8))
            p.drawText(QRectF(14, y, label_w, cell_h), Qt.AlignLeft | Qt.AlignVCenter, _clip(row_label, int(label_w / 5.6)))
            for c, col in enumerate(self._col_labels):
                x = left + c * (cell_w + cell_gap)
                value = float(self._values.get((row_label, col), 0.0))
                fraction = (value / max_value) if max_value else 0.0
                cell = QRectF(x, y, cell_w, cell_h)
                p.setPen(Qt.NoPen)
                p.setBrush(_blend_palette_hex(PALETTE.bg_2, PALETTE.accent, fraction))
                p.drawRoundedRect(cell, 3, 3)
                self._cell_rects.append((cell, row_label, col, value))
                if value > 0 and cell_w >= 44:
                    share = value / grand_total * 100
                    p.setPen(QColor(PALETTE.bg_0 if fraction > 0.55 else PALETTE.text_0))
                    p.setFont(QFont("Inter", 7, QFont.DemiBold))
                    p.drawText(cell, Qt.AlignCenter, f"{share:.0f}%")

    def mouseMoveEvent(self, event) -> None:
        pos = event.position()
        grand_total = sum(self._values.values()) or 1.0
        for cell, row_label, col, value in self._cell_rects:
            if cell.contains(pos) and value > 0:
                QToolTip.showText(
                    event.globalPosition().toPoint(),
                    f"{row_label}  x  {col}\nimpact {value:,.0f}  ({value / grand_total * 100:.1f}% of all insight impact)",
                    self,
                )
                break
        super().mouseMoveEvent(event)


class _ExpandableSqlView(QPlainTextEdit):
    """Read-only SQL snippet that requests a full-size viewer on double-click."""

    expandRequested = Signal()

    def mouseDoubleClickEvent(self, event) -> None:
        self.expandRequested.emit()
        event.accept()


def _evidence_text(value: object) -> str:
    try:
        if value is None or pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return "" if text.lower() in ("nan", "none", "<na>") else text


class _ScriptPage(QWidget):
    """Script-first view: the ready-to-run output of the analytics.

    Section 1 (maintenance) is runnable ANALYZE/VACUUM. Section 2 (structural)
    is commented-out SORTKEY/DISTKEY changes, each preceded by its justification.
    Rendered like a code file: numbered lines, monospace, read-only.
    """

    loadRequested = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._report: ClusterReport | None = None
        lay = QVBoxLayout(self)
        lay.setContentsMargins(12, 12, 12, 12)
        lay.setSpacing(8)

        head_row = QHBoxLayout()
        head = QLabel("READY-TO-RUN SCRIPT")
        head.setObjectName("SectionHeader")
        head_row.addWidget(head)
        head_row.addStretch(1)
        self._status = QLabel("Load a report to generate the maintenance + structural script.")
        self._status.setObjectName("Caption")
        self._status.setWordWrap(True)
        head_row.addWidget(self._status)
        self._rendered_btn = QPushButton("Rendered")
        self._rendered_btn.setObjectName("Ghost")
        self._rendered_btn.setCheckable(True)
        self._rendered_btn.setChecked(True)
        self._rendered_btn.setToolTip("Beautiful Markdown view: color, emoji, and the one executable line highlighted.")
        self._raw_btn = QPushButton("Raw .sql")
        self._raw_btn.setObjectName("Ghost")
        self._raw_btn.setCheckable(True)
        self._raw_btn.setToolTip("The exact SQL you copy/save. Prose is /* */ block comments; the one '--' line is the executable ALTER.")
        self._rendered_btn.clicked.connect(lambda: self._set_view(rendered=True))
        self._raw_btn.clicked.connect(lambda: self._set_view(rendered=False))
        head_row.addWidget(self._rendered_btn)
        head_row.addWidget(self._raw_btn)
        copy_btn = QPushButton("Copy Script")
        copy_btn.setObjectName("Ghost")
        copy_btn.clicked.connect(self._copy)
        head_row.addWidget(copy_btn)
        save_btn = QPushButton("Save .sql")
        save_btn.setObjectName("Primary")
        save_btn.clicked.connect(self._save)
        head_row.addWidget(save_btn)
        lay.addLayout(head_row)

        intro = QLabel(
            "Section 1 is safe maintenance and runs as-is. Section 2 lists structural "
            "changes commented out, each with the workload justification above it - "
            "review and uncomment before running."
        )
        intro.setObjectName("Caption")
        intro.setWordWrap(True)
        lay.addWidget(intro)

        self._rendered = QTextBrowser()
        self._rendered.setObjectName("MarkdownView")
        self._rendered.setOpenExternalLinks(False)
        self._editor = QPlainTextEdit()
        self._editor.setObjectName("Mono")
        self._editor.setReadOnly(True)
        self._editor.setLineWrapMode(QPlainTextEdit.NoWrap)
        mono = QFont("Consolas")
        mono.setStyleHint(QFont.Monospace)
        self._editor.setFont(mono)
        self._view_stack = QStackedWidget()
        self._view_stack.addWidget(self._rendered)  # index 0
        self._view_stack.addWidget(self._editor)    # index 1
        lay.addWidget(self._view_stack, 1)
        self._script_text = ""

    def _set_view(self, *, rendered: bool) -> None:
        self._view_stack.setCurrentIndex(0 if rendered else 1)
        self._rendered_btn.setChecked(rendered)
        self._raw_btn.setChecked(not rendered)

    def set_report(self, report: ClusterReport | None) -> None:
        self._report = report
        self._rebuild()

    def _rebuild(self) -> None:
        report = self._report
        if report is None:
            self._editor.setPlainText("")
            self._status.setText("Load a report to generate the script.")
            return
        maintenance = self._maintenance_section(report)
        structural = self._structural_section(report)
        body = maintenance + "\n\n" + structural
        self._script_text = body
        self._editor.setPlainText(_number_script_lines(body))
        self._rendered.setHtml(_script_to_html(body))
        runnable = _count_runnable_statements(maintenance)
        commented = sum(
            1 for line in structural.splitlines() if line.strip().startswith("-- ALTER")
        )
        self._status.setText(
            f"{runnable} runnable maintenance statement(s); "
            f"{commented} structural change(s) staged for review."
        )

    def _maintenance_section(self, report: ClusterReport) -> str:
        try:
            script = build_fix_script(
                getattr(report, "repeat_groups", pd.DataFrame()),
                getattr(report, "repeat_group_tables", pd.DataFrame()),
                action_queue=getattr(report, "action_queue", pd.DataFrame()),
                table_review=getattr(report, "table_review", pd.DataFrame()),
                snapshot_id=getattr(report, "snapshot_id", None),
            )
        except Exception as exc:
            script = f"-- Maintenance script could not be generated: {exc}"
        return script.rstrip()

    def _structural_section(self, report: ClusterReport) -> str:
        try:
            recs = build_structural_recommendations(
                getattr(report, "slow_queries", pd.DataFrame()),
                getattr(report, "table_review", pd.DataFrame()),
            )
            script = build_structural_recommendation_script(
                recs, snapshot_id=getattr(report, "snapshot_id", None)
            )
        except Exception as exc:
            script = f"-- Structural recommendations could not be generated: {exc}"
        return script.rstrip()

    def _copy(self) -> None:
        if not self._script_text:
            QMessageBox.information(self, "Script", "Load a report first.")
            return
        QApplication.clipboard().setText(self._script_text)
        self._status.setText("Script copied to clipboard.")

    def _save(self) -> None:
        if not self._script_text:
            QMessageBox.information(self, "Script", "Load a report first.")
            return
        path, _ = QFileDialog.getSaveFileName(self, "Save Script", "redshift_fix_script.sql", "SQL (*.sql)")
        if not path:
            return
        try:
            Path(path).write_text(self._script_text, encoding="utf-8")
            self._status.setText(f"Saved to {path}")
        except OSError as exc:
            QMessageBox.warning(self, "Save Script", str(exc))


def _number_script_lines(text: str) -> str:
    lines = text.splitlines()
    width = max(3, len(str(len(lines))))
    return "\n".join(f"{i:>{width}} | {line}" for i, line in enumerate(lines, start=1))


def _script_to_html(script: str) -> str:
    """Render the fix script as themed HTML (Qt setHtml, no external library).

    - /* */ block comments become muted 'note' cards.
    - The one '--'-prefixed executable line per recommendation becomes a
      prominent, boxed, monospace code block so 'which row runs' is obvious.
    - Severity words (crit/warn) and section headers get color + emoji.
    Colors route through PALETTE so light and dark themes both read well."""
    import html as _html

    warn, ok = PALETTE.warn, PALETTE.ok
    accent, text_2, text_3 = PALETTE.accent, PALETTE.text_2, PALETTE.text_3
    bg_3 = PALETTE.bg_3

    def _sev_emoji(line: str) -> str:
        low = line.lower()
        if "| crit" in low or "severity crit" in low:
            return "\U0001F534 "  # red
        if "| warn" in low or "severity warn" in low:
            return "\U0001F7E1 "  # yellow
        return ""

    parts: list[str] = [
        f"<div style='font-family:Inter,Segoe UI,sans-serif; color:{text_2}; font-size:13px;'>"
    ]
    lines = script.splitlines()
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        stripped = line.strip()
        if not stripped:
            i += 1
            continue
        if stripped.startswith("/*"):
            # Gather the block body until '*/'.
            block: list[str] = []
            first = stripped[2:].strip()
            if first and first != "*/":
                block.append(first)
            closed = stripped.endswith("*/")
            i += 1
            while not closed and i < n:
                inner = lines[i].strip()
                if inner.endswith("*/"):
                    inner = inner[:-2].strip()
                    closed = True
                if inner:
                    block.append(inner)
                i += 1
            is_header = any("====" in b or "structural recommendations" in b.lower() for b in block)
            emoji = "\U0001F4CB " if is_header else "".join(_sev_emoji(b) for b in block[:1])
            border = accent if is_header else text_3
            rows = []
            for b in block:
                if "====" in b:
                    continue
                safe = _html.escape(b)
                if b.lower().startswith("#") or "recommendation" in b.lower() and is_header:
                    rows.append(f"<div style='font-weight:700; color:{accent};'>{safe}</div>")
                elif b.lower().startswith("evidence:"):
                    rows.append(f"<div style='color:{text_2};'>\U0001F50D {safe}</div>")
                elif b.lower().startswith("table:"):
                    rows.append(f"<div style='color:{text_2}; font-weight:600;'>\U0001F5C4 {safe}</div>")
                else:
                    rows.append(f"<div style='color:{text_3};'>{safe}</div>")
            # Instructional prose as clean styled Markdown - no box, no '/* */'
            # cues. A subtle left accent keeps it grouped; it already reads as a
            # comment because it is prose, not a code line.
            parts.append(
                f"<div style='margin:10px 0 2px 0; padding:2px 0 2px 10px; "
                f"border-left:2px solid {border};'>{emoji}{''.join(rows)}</div>"
            )
            continue
        if stripped.startswith("--"):
            # Executable line - the ONE row that runs when uncommented. Rendered
            # as a distinct, solid-bordered code line directly under its comment
            # box so the comment-vs-execute distinction is unmistakable.
            safe = _html.escape(stripped)
            parts.append(
                f"<div style='margin:0 0 14px 0; padding:8px 12px; background:{bg_3}; "
                f"border-left:4px solid {warn}; border-radius:0 4px 4px 0; "
                f"font-family:Consolas,monospace; font-size:12px;'>"
                f"<span style='color:{warn}; font-weight:700;'>▶ EXECUTE</span> "
                f"<span style='color:{text_3};'>(uncomment to run)</span><br>"
                f"<span style='color:{PALETTE.text_0};'>{safe}</span></div>"
            )
            i += 1
            continue
        # A bare runnable statement (maintenance ANALYZE/VACUUM).
        safe = _html.escape(stripped)
        parts.append(
            f"<div style='margin:2px 0; padding:6px 12px; background:{bg_3}; "
            f"border-radius:4px; font-family:Consolas,monospace; font-size:12px; color:{ok};'>"
            f"✅ {safe}</div>"
        )
        i += 1
    parts.append("</div>")
    return "".join(parts)


def _count_runnable_statements(script: str) -> int:
    """Count executable SQL lines, ignoring /* */ block-comment regions and
    '--' line comments. Under the block-comment convention, prose lines inside
    /* */ do not start with '--', so the old 'not startswith(--)' count would
    wrongly report them as runnable; this walks the block state instead."""
    count = 0
    in_block = False
    for raw in script.splitlines():
        line = raw.strip()
        if not line:
            continue
        if in_block:
            if "*/" in line:
                in_block = False
            continue
        if line.startswith("/*"):
            if "*/" not in line:
                in_block = True
            continue
        if line.startswith("--"):
            continue
        count += 1
    return count


def _group_evidence_kind(group: pd.Series) -> str:
    """Classify a repeat group for the evidence-picker label."""
    sql = " ".join(
        _evidence_text(group.get(column))
        for column in ("sample_sql", "representative_sql", "sql_shape")
    ).strip().lower()
    query_type = _evidence_text(group.get("query_type")).lower()
    if re.search(r"\bunload\s*\(", sql) or query_type == "unload":
        return "UNLOAD"
    if re.match(r"^\s*copy\b", sql) or query_type == "copy":
        return "Load COPY"

    def number(column: str) -> float:
        return _safe_float(group.get(column))

    tables = _evidence_text(group.get("sql_tables_full")) or _evidence_text(group.get("sql_tables"))
    table_text = tables.lower()
    external = (
        number("avg_s3_scan_cnt") > 0
        or number("avg_external_steps") > 0
        or number("avg_external_tables_touched") > 0
        or any(token in table_text for token in ("spectrum", "external", "s3."))
    )
    local = number("triage_tables_matched") > 0 or (
        bool(table_text) and not external
    )
    if external and local:
        return "Mixed Query"
    if external:
        return "Spectrum Query"
    return "Local Query"


def _group_evidence_sql_preview(group: pd.Series, limit: int = 100) -> str:
    raw = ""
    for column in ("sample_sql", "representative_sql", "sql_shape"):
        raw = _evidence_text(group.get(column))
        if raw:
            break
    preview = re.sub(r"\s+", " ", raw).strip()
    if len(preview) > limit:
        preview = preview[: max(1, limit - 3)].rstrip() + "..."
    return preview


def _group_evidence_picker_label(group: pd.Series) -> str:
    gid = _evidence_text(group.get("repeat_group_id")) or "-"
    runs = int(_safe_float(group.get("query_count")))
    preview = _group_evidence_sql_preview(group)
    prefix = f"{gid} — {_group_evidence_kind(group)} — {runs:,} runs"
    return f"{prefix} — {preview}" if preview else prefix


class _GroupEvidencePage(QWidget):
    """Skeptic-friendly proof view: pick a repeat group from a dropdown and
    read every composite SQL statement with its query ID, straight from the
    captured system logs."""

    loadRequested = Signal(str)

    _MAX_MEMBER_CARDS = 60

    def __init__(self, parent=None):
        super().__init__(parent)
        self._groups = pd.DataFrame()
        self._members = pd.DataFrame()
        self._table_review = pd.DataFrame()
        self._view_definitions = pd.DataFrame()
        lay = QVBoxLayout(self)
        lay.setContentsMargins(12, 12, 12, 12)
        lay.setSpacing(8)

        head_row = QHBoxLayout()
        head = QLabel("PATTERN GROUP EVIDENCE")
        head.setObjectName("SectionHeader")
        head_row.addWidget(head)
        head_row.addStretch(1)
        self._status = QLabel("Pick a group and read every captured statement behind it.")
        self._status.setObjectName("Caption")
        self._status.setWordWrap(True)
        head_row.addWidget(self._status)
        load_btn = QPushButton("Load Repeat Analysis")
        load_btn.setObjectName("Primary")
        load_btn.clicked.connect(lambda: self.loadRequested.emit("repeat_queries"))
        head_row.addWidget(load_btn)
        lay.addLayout(head_row)

        picker_row = QHBoxLayout()
        picker_row.setSpacing(8)
        picker_row.addWidget(QLabel("Group"))
        self._group_combo = QComboBox()
        self._group_combo.setMinimumWidth(460)
        self._group_combo.setMaxVisibleItems(18)
        self._group_combo.setToolTip(
            "Group, workload type, run count, and a preview of the representative SQL."
        )
        self._group_combo.setStyleSheet(
            f"QComboBox {{ color:{PALETTE.accent}; }} "
            f"QComboBox QAbstractItemView {{ color:{PALETTE.accent}; }}"
        )
        self._group_combo.currentIndexChanged.connect(self._show_group)
        picker_row.addWidget(self._group_combo, 1)
        copy_btn = QPushButton("Copy All Query IDs")
        copy_btn.setObjectName("Ghost")
        copy_btn.setToolTip(
            "Copy every member query ID so it can be verified directly against SYS_QUERY_HISTORY."
        )
        copy_btn.clicked.connect(self._copy_ids)
        picker_row.addWidget(copy_btn)
        lay.addLayout(picker_row)

        self._summary = QLabel("")
        self._summary.setObjectName("Caption")
        self._summary.setWordWrap(True)
        lay.addWidget(self._summary)

        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._scroll.setFrameShape(QFrame.NoFrame)
        self._scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self._scroll.setMinimumHeight(320)
        host = QWidget()
        self._cards_lay = QVBoxLayout(host)
        self._cards_lay.setContentsMargins(0, 0, 12, 0)
        self._cards_lay.setSpacing(8)
        self._scroll.setWidget(host)
        lay.addWidget(self._scroll, 1)

    def set_report(self, report) -> None:
        groups = getattr(report, "repeat_groups", pd.DataFrame())
        members = getattr(report, "repeat_members", pd.DataFrame())
        self._groups = groups.copy() if groups is not None else pd.DataFrame()
        self._members = members.copy() if members is not None else pd.DataFrame()
        table_review = getattr(report, "table_review", pd.DataFrame())
        view_definitions = getattr(report, "view_definitions", pd.DataFrame())
        self._table_review = table_review.copy() if table_review is not None else pd.DataFrame()
        self._view_definitions = view_definitions.copy() if view_definitions is not None else pd.DataFrame()
        previous = str(self._group_combo.currentData() or "")
        self._group_combo.blockSignals(True)
        self._group_combo.clear()
        if not self._groups.empty:
            for _, row in self._groups.iterrows():
                gid = _evidence_text(row.get("repeat_group_id"))
                if not gid:
                    continue
                label = _group_evidence_picker_label(row)
                self._group_combo.addItem(label, gid)
                item_index = self._group_combo.count() - 1
                self._group_combo.setItemData(item_index, QColor(PALETTE.accent), Qt.ForegroundRole)
                self._group_combo.setItemData(item_index, label, Qt.ToolTipRole)
            if previous:
                index = self._group_combo.findData(previous)
                if index >= 0:
                    self._group_combo.setCurrentIndex(index)
        self._group_combo.blockSignals(False)
        if self._group_combo.count():
            self._status.setText(
                f"{self._group_combo.count():,} repeat groups loaded. "
                "Every statement shown below is verbatim from the captured system logs."
            )
            self._show_group()
        else:
            self._status.setText("Load Repeat Analysis to list the pattern groups.")
            self._summary.setText("")
            _clear_layout(self._cards_lay)

    def _selected_group_row(self) -> pd.Series | None:
        gid = str(self._group_combo.currentData() or "")
        if not gid or self._groups.empty:
            return None
        rows = self._groups[self._groups["repeat_group_id"].astype(str) == gid]
        return rows.iloc[0] if not rows.empty else None

    def _selected_members(self) -> pd.DataFrame:
        gid = str(self._group_combo.currentData() or "")
        if not gid or self._members.empty or "repeat_group_id" not in self._members.columns:
            return pd.DataFrame()
        members = self._members[self._members["repeat_group_id"].astype(str) == gid].copy()
        if "member_rank" in members.columns:
            members["_rank"] = pd.to_numeric(members["member_rank"], errors="coerce").fillna(0)
            members = members.sort_values("_rank").drop(columns=["_rank"])
        return members

    def _show_group(self, *_args) -> None:
        _clear_layout(self._cards_lay)
        group = self._selected_group_row()
        if group is None:
            self._summary.setText("")
            return
        members = self._selected_members()
        parts = [
            f"{int(_safe_float(group.get('query_count'))):,} captured runs",
        ]
        distinct = int(_safe_float(group.get("distinct_sql_count")))
        if distinct:
            parts.append(f"{distinct:,} distinct SQL statements as logged")
        method = _evidence_text(group.get("fingerprint_method"))
        if method:
            parts.append(f"fingerprint: {method}")
        basis = _evidence_text(group.get("repeat_match_basis"))
        if basis:
            parts.append(basis)
        tables = _evidence_text(group.get("sql_tables"))
        if tables:
            parts.append(f"tables: {tables}")
        self._summary.setText("  |  ".join(parts))
        if members.empty:
            note = QLabel(
                "No member rows are available for this group. Reload the Repeat Analysis area."
            )
            note.setObjectName("Caption")
            note.setWordWrap(True)
            self._cards_lay.addWidget(note)
            self._cards_lay.addStretch(1)
            return
        member_texts = {
            text
            for text in (
                _evidence_text(member.get("sql_text_full")) or _evidence_text(member.get("sql_text"))
                for _, member in members.iterrows()
            )
            if text
        }
        if len(member_texts) == 1 and len(members) > 1:
            banner = QLabel(
                f"ALL {len(members):,} CAPTURED RUNS ARE 100% IDENTICAL SQL - "
                "one representative statement is shown below. "
                "Use Copy All Query IDs to verify every run in SYS_QUERY_HISTORY."
            )
            banner.setWordWrap(True)
            banner.setStyleSheet(f"color:{PALETTE.crit}; font-weight:800; font-size:12px;")
            self._cards_lay.addWidget(banner)
            elapsed = pd.to_numeric(members.get("elapsed_s"), errors="coerce").fillna(0.0)
            stats = QLabel(
                f"Total queries: {len(members):,}"
                f"    |    Max runtime: {_fmt_contrib_value(float(elapsed.max()), 's')}"
                f"    |    Min runtime: {_fmt_contrib_value(float(elapsed.min()), 's')}"
                f"    |    Average: {_fmt_contrib_value(float(elapsed.mean()), 's')}"
                f"    |    Combined runtime: {_fmt_contrib_value(float(elapsed.sum()), 's')}"
            )
            stats.setObjectName("Mono")
            stats_font = stats.font()
            stats_font.setBold(True)
            stats.setFont(stats_font)
            stats.setWordWrap(True)
            self._cards_lay.addWidget(stats)
            self._cards_lay.addWidget(self._member_card(members.iloc[0]))
            self._cards_lay.addStretch(1)
            return
        shown = 0
        for _, member in members.iterrows():
            if shown >= self._MAX_MEMBER_CARDS:
                rest = len(members) - shown
                note = QLabel(
                    f"... {rest:,} more member(s) not rendered. Use Copy All Query IDs to verify "
                    "the full set against SYS_QUERY_HISTORY."
                )
                note.setObjectName("Caption")
                note.setWordWrap(True)
                self._cards_lay.addWidget(note)
                break
            self._cards_lay.addWidget(self._member_card(member))
            shown += 1
        self._cards_lay.addStretch(1)

    def _member_card(self, member: pd.Series) -> QFrame:
        card = QFrame()
        card.setObjectName("Card")
        card_lay = QVBoxLayout(card)
        card_lay.setContentsMargins(12, 10, 12, 10)
        card_lay.setSpacing(6)
        header_bits = [f"QUERY {_evidence_text(member.get('query_id')) or '-'}"]
        started = _evidence_text(member.get("start_time"))
        if started:
            header_bits.append(started[:19])
        try:
            elapsed = float(member.get("elapsed_s"))
            header_bits.append(_fmt_contrib_value(elapsed, "s"))
        except (TypeError, ValueError):
            pass
        user = _evidence_text(member.get("user_name"))
        if user:
            header_bits.append(user)
        database = _evidence_text(member.get("database_name"))
        if database:
            header_bits.append(database)
        try:
            similarity = float(member.get("similarity_score"))
            header_bits.append(f"similarity {similarity * 100:.0f}%")
        except (TypeError, ValueError):
            pass
        header = QLabel("   |   ".join(header_bits))
        header.setObjectName("Mono")
        header_font = header.font()
        header_font.setBold(True)
        header.setFont(header_font)
        header.setWordWrap(True)
        sql_text = _evidence_text(member.get("sql_text_full")) or _evidence_text(member.get("sql_text"))
        sql_md5_tail = hashlib.md5(sql_text.encode("utf-8")).hexdigest()[-8:] if sql_text else ""
        header_row = QHBoxLayout()
        header_row.setSpacing(8)
        header_row.addWidget(header, 1)
        if sql_md5_tail:
            md5_label = QLabel(sql_md5_tail)
            md5_label.setObjectName("Mono")
            md5_label.setStyleSheet(f"color:{PALETTE.crit}; font-weight:700;")
            md5_label.setToolTip(
                "Last 8 characters of the MD5 of this run's SQL text. "
                "Two members with the same red tag ran byte-identical SQL."
            )
            header_row.addWidget(md5_label, 0, Qt.AlignRight | Qt.AlignTop)
        card_lay.addLayout(header_row)
        sql_view = _ExpandableSqlView()
        sql_view.setReadOnly(True)
        sql_view.setObjectName("Mono")
        sql_view.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        sql_view.setPlainText(sql_text or "SQL text was not captured for this run.")
        sql_view.setToolTip("Double-click to open this statement full size.")
        line_count = max(1, (sql_text or "").count("\n") + 1)
        sql_view.setFixedHeight(max(64, min(240, 28 + line_count * 17)))
        query_id = _evidence_text(member.get("query_id")) or "-"
        header_text = header.text()
        if sql_md5_tail:
            header_text += f"   |   md5 {sql_md5_tail}"
        sql_view.expandRequested.connect(
            lambda qid=query_id, head=header_text, sql=sql_text: self._open_member_sql(qid, head, sql)
        )
        card_lay.addWidget(sql_view)
        return card

    def _open_member_sql(self, query_id: str, header_text: str, sql_text: str) -> None:
        dialog = QDialog(self)
        dialog.setWindowTitle(f"Group Evidence - Query {query_id}")
        root = QVBoxLayout(dialog)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)
        header = QLabel(header_text)
        header.setObjectName("Mono")
        header_font = header.font()
        header_font.setBold(True)
        header.setFont(header_font)
        header.setWordWrap(True)
        root.addWidget(header)
        editor = QPlainTextEdit()
        editor.setReadOnly(True)
        editor.setObjectName("Mono")
        editor.setLineWrapMode(QPlainTextEdit.NoWrap)
        editor.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        editor.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        editor.setPlainText(sql_text or "SQL text was not captured for this run.")
        root.addWidget(editor, 1)
        actions = QHBoxLayout()
        format_btn = QPushButton("Format SQL")
        format_btn.setObjectName("Primary")
        format_btn.clicked.connect(lambda: _apply_format_sql(editor, dialog))
        actions.addWidget(format_btn)
        _add_sql_structure_buttons(
            actions,
            editor,
            dialog,
            pd.Series({"query_id": query_id, "sql_text": sql_text}),
            self._table_review,
            self._view_definitions,
        )
        actions.addStretch(1)
        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.close)
        actions.addWidget(buttons)
        root.addLayout(actions)
        _resize_dialog_to_screen(dialog, 0.88)
        dialog.exec()

    def _copy_ids(self) -> None:
        members = self._selected_members()
        if members.empty or "query_id" not in members.columns:
            QMessageBox.information(self, "Group Evidence", "Load Repeat Analysis and pick a group first.")
            return
        ids = [
            _evidence_text(value)
            for value in members["query_id"]
            if _evidence_text(value)
        ]
        QApplication.clipboard().setText(",".join(ids))
        self._status.setText(
            f"Copied {len(ids):,} query IDs. Paste into a SYS_QUERY_HISTORY filter to verify every run."
        )


class _FocusContributorsPage(QWidget):
    """BI-style rollup: which databases and schemas contribute the most
    workload pain, as ranked percentage bars plus a heat map by issue family."""

    loadRequested = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._slow_queries = pd.DataFrame()
        self._insights = pd.DataFrame()
        self._table_impact = pd.DataFrame()
        lay = QVBoxLayout(self)
        lay.setContentsMargins(12, 12, 12, 12)
        lay.setSpacing(10)
        head_row = QHBoxLayout()
        head = QLabel("FOCUS CONTRIBUTORS")
        head.setObjectName("SectionHeader")
        head_row.addWidget(head)
        head_row.addStretch(1)
        self._status = QLabel("Which databases and schemas deserve attention first.")
        self._status.setObjectName("Caption")
        self._status.setWordWrap(True)
        head_row.addWidget(self._status)
        head_row.addWidget(QLabel("Measure"))
        self._measure = QComboBox()
        self._measure.addItem("Slow-query runtime", "runtime")
        self._measure.addItem("Slow-query count", "count")
        self._measure.currentIndexChanged.connect(self._recompute)
        head_row.addWidget(self._measure)
        load_btn = QPushButton("Load Insights")
        load_btn.setObjectName("Primary")
        load_btn.setToolTip("Load the insight ledger area; slow queries and table impact feed this page too.")
        load_btn.clicked.connect(lambda: self.loadRequested.emit("insights"))
        head_row.addWidget(load_btn)
        lay.addLayout(head_row)
        bars_row = QHBoxLayout()
        bars_row.setSpacing(10)
        self._db_bars = _ContributorBars("BY DATABASE  -  share of selected measure")
        self._schema_bars = _ContributorBars("BY SCHEMA  -  share of table blast-radius score")
        bars_row.addWidget(self._db_bars, 1)
        bars_row.addWidget(self._schema_bars, 1)
        lay.addLayout(bars_row)
        self._heat = _ContributorHeatMap("DATABASE x ISSUE FAMILY  -  % of total insight impact")
        lay.addWidget(self._heat)
        lay.addStretch(1)

    def set_report_frames(
        self,
        slow_queries: pd.DataFrame,
        insights: pd.DataFrame,
        table_impact: pd.DataFrame,
    ) -> None:
        self._slow_queries = slow_queries.copy() if slow_queries is not None else pd.DataFrame()
        self._insights = insights.copy() if insights is not None else pd.DataFrame()
        self._table_impact = table_impact.copy() if table_impact is not None else pd.DataFrame()
        self._recompute()

    def _recompute(self, *_args) -> None:
        measure = str(self._measure.currentData() or "runtime")
        db_rows: list[tuple[str, float]] = []
        if not self._slow_queries.empty and "database_name" in self._slow_queries.columns:
            work = self._slow_queries.copy()
            work["_db"] = work["database_name"].astype(str).str.strip()
            work = work[~work["_db"].str.lower().isin(("", "nan", "none"))]
            if measure == "count":
                grouped = work.groupby("_db").size()
                db_rows = [(str(k), float(v)) for k, v in grouped.items()]
                self._db_bars.set_rows(db_rows, "", "Load slow queries to rank databases.")
            else:
                work["_elapsed"] = pd.to_numeric(work.get("elapsed_s"), errors="coerce").fillna(0.0)
                grouped = work.groupby("_db")["_elapsed"].sum()
                db_rows = [(str(k), float(v)) for k, v in grouped.items()]
                self._db_bars.set_rows(db_rows, "s", "Load slow queries to rank databases.")
        else:
            self._db_bars.set_rows([], "", "Load slow queries to rank databases.")
        schema_rows: list[tuple[str, float]] = []
        if not self._table_impact.empty and "schema_name" in self._table_impact.columns:
            work = self._table_impact.copy()
            db_part = work.get("source_db")
            db_text = db_part.astype(str).str.strip() if db_part is not None else ""
            work["_schema"] = (db_text + "." if db_part is not None else "") + work["schema_name"].astype(str).str.strip()
            work["_blast"] = pd.to_numeric(work.get("blast_radius_score"), errors="coerce").fillna(0.0)
            grouped = work.groupby("_schema")["_blast"].sum()
            schema_rows = [(str(k), float(v)) for k, v in grouped.items() if str(k).strip(" .")]
        self._schema_bars.set_rows(
            schema_rows,
            "",
            "Load Table Impact rows (Table Impact tab) to rank schemas.",
        )
        self._recompute_heat_map()
        loaded = []
        if db_rows:
            loaded.append(f"{len(db_rows)} database(s)")
        if schema_rows:
            loaded.append(f"{len(schema_rows)} schema(s)")
        if loaded:
            self._status.setText("Ranked by contribution: " + ", ".join(loaded) + ". Hover any bar or cell for detail.")
        else:
            self._status.setText("Load slow queries, insights, and table impact to rank contributors.")

    def _recompute_heat_map(self) -> None:
        if self._insights.empty or "family" not in self._insights.columns:
            self._heat.set_matrix([], [], {})
            return
        db_by_query: dict[str, str] = {}
        if not self._slow_queries.empty and "query_id" in self._slow_queries.columns:
            ids = self._slow_queries["query_id"].astype(str).str.strip()
            dbs = self._slow_queries.get("database_name")
            if dbs is not None:
                db_by_query = dict(zip(ids, dbs.astype(str).str.strip()))
        def _cell_text(value: object) -> str:
            try:
                if value is None or pd.isna(value):
                    return ""
            except (TypeError, ValueError):
                pass
            text = str(value).strip()
            return "" if text.lower() in ("nan", "none", "<na>") else text

        cells: dict[tuple[str, str], float] = {}
        for _, row in self._insights.iterrows():
            family = _cell_text(row.get("family")) or "Other"
            query_id = _cell_text(row.get("query_id"))
            database = db_by_query.get(query_id, "")
            if not database:
                table_key = _cell_text(row.get("table_key"))
                if table_key.count(".") >= 2:
                    database = table_key.split(".", 1)[0]
            if not database or database.lower() in ("nan", "none"):
                database = "(unattributed)"
            impact = float(pd.to_numeric(pd.Series([row.get("impact_score")]), errors="coerce").fillna(0.0).iloc[0])
            if impact <= 0:
                continue
            cells[(database, family)] = cells.get((database, family), 0.0) + impact
        if not cells:
            self._heat.set_matrix([], [], {})
            return
        row_totals: dict[str, float] = {}
        col_totals: dict[str, float] = {}
        for (database, family), value in cells.items():
            row_totals[database] = row_totals.get(database, 0.0) + value
            col_totals[family] = col_totals.get(family, 0.0) + value
        rows = sorted(row_totals, key=lambda k: -row_totals[k])[:10]
        cols = sorted(col_totals, key=lambda k: -col_totals[k])[:8]
        self._heat.set_matrix(rows, cols, cells)


_INSIGHT_SEVERITY_RANK = {"crit": 0, "warn": 1, "info": 2}


def _one_line_sql(sql: object, limit: int = 160) -> str:
    text = " ".join(str(sql or "").split())
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


class _InsightLedgerPage(QWidget):
    """Insight Ledger rollup: insight rule -> repeat group -> query ID."""

    loadRequested = Signal(str)
    rowActivated = Signal(object)

    _COLUMNS = ("Insight", "Severity", "Count / Query", "Metric", "Impact", "Detail")

    def __init__(self, parent=None):
        super().__init__(parent)
        self._df = pd.DataFrame()
        self._sql_lookup: dict[str, str] = {}
        self._query_group_lookup: dict[str, str] = {}
        self._group_representative_query: dict[str, str] = {}
        lay = QVBoxLayout(self)
        lay.setContentsMargins(12, 12, 12, 12)
        lay.setSpacing(8)
        head_row = QHBoxLayout()
        head = QLabel("INSIGHT LEDGER")
        head.setObjectName("SectionHeader")
        head_row.addWidget(head)
        head_row.addStretch(1)
        self._status = QLabel("No insights are loaded.")
        self._status.setObjectName("Caption")
        self._status.setWordWrap(True)
        head_row.addWidget(self._status)
        expand_btn = QPushButton("Expand All")
        expand_btn.setObjectName("Ghost")
        expand_btn.clicked.connect(self._toggle_expand)
        self._expand_btn = expand_btn
        head_row.addWidget(expand_btn)
        load_btn = QPushButton("Load Insights")
        load_btn.setObjectName("Primary")
        load_btn.clicked.connect(lambda: self.loadRequested.emit("insights"))
        head_row.addWidget(load_btn)
        wide_btn = QPushButton("Wide Grid")
        wide_btn.setObjectName("Ghost")
        wide_btn.setToolTip("Open the flat, ungrouped insight rows in a large sortable grid.")
        wide_btn.clicked.connect(self._open_wide_grid)
        head_row.addWidget(wide_btn)
        lay.addLayout(head_row)
        self._tree = QTreeWidget()
        self._tree.setColumnCount(len(self._COLUMNS))
        self._tree.setHeaderLabels(list(self._COLUMNS))
        self._tree.setAlternatingRowColors(True)
        self._tree.setUniformRowHeights(True)
        self._tree.setColumnWidth(0, 320)
        self._tree.setColumnWidth(1, 80)
        self._tree.setColumnWidth(2, 130)
        self._tree.setColumnWidth(3, 150)
        self._tree.setColumnWidth(4, 80)
        self._tree.itemDoubleClicked.connect(self._on_double_click)
        lay.addWidget(self._tree, 1)

    def set_sql_lookup(self, slow_queries: pd.DataFrame) -> None:
        lookup: dict[str, str] = {}
        query_groups: dict[str, str] = {}
        representatives: dict[str, str] = {}
        if slow_queries is not None and not slow_queries.empty and "query_id" in slow_queries.columns:
            for _, row in slow_queries.iterrows():
                qid = _normalized_query_id(row.get("query_id"))
                if not qid:
                    continue
                text = str(row.get("sql_text") or "").strip()
                if text and text.lower() != "nan" and qid not in lookup:
                    lookup[qid] = text
                group_id = str(row.get("repeat_group_id") or "").strip()
                if group_id.lower() in ("nan", "none", "<na>"):
                    group_id = ""
                if group_id:
                    query_groups[qid] = group_id
                    representatives.setdefault(group_id, qid)
        self._sql_lookup = lookup
        self._query_group_lookup = query_groups
        self._group_representative_query = representatives

    def set_dataframe(self, df: pd.DataFrame, *, loaded: bool = True) -> None:
        self._df = df.copy() if df is not None else pd.DataFrame()
        self._load_completed = bool(loaded)
        self._populate()

    def show_loading(self) -> None:
        self._status.setText("Loading Insight Ledger from the local DuckDB ...")
        QApplication.processEvents()

    def show_blocked(self, message: str) -> None:
        self._status.setText(message)

    def _populate(self) -> None:
        self._tree.clear()
        if self._df.empty:
            if getattr(self, "_load_completed", False):
                self._status.setText(
                    "Insight load completed: 0 findings returned. Open Error Log if the source views should contain rows."
                )
            else:
                self._status.setText("No insights are loaded. Click Load Insights to read the local DuckDB.")
            return
        work = self._df.copy()
        work["_impact_num"] = pd.to_numeric(work.get("impact_score"), errors="coerce").fillna(0.0)
        group_col = "insight_id" if "insight_id" in work.columns else "title"
        groups = sorted(
            work.groupby(work[group_col].astype(str), sort=False),
            key=lambda item: -float(item[1]["_impact_num"].max()),
        )
        for _key, members in groups:
            members = members.sort_values("_impact_num", ascending=False)
            top = members.iloc[0]
            severities = {str(s).lower() for s in members.get("severity", pd.Series(dtype=object))}
            severity = min(severities, key=lambda s: _INSIGHT_SEVERITY_RANK.get(s, 9)) if severities else ""
            parent = QTreeWidgetItem(
                [
                    str(top.get("title") or top.get("insight_id") or "Insight"),
                    severity.upper(),
                    f"{len(members):,} finding(s)",
                    str(top.get("metric_label") or ""),
                    f"{float(members['_impact_num'].max()):.0f}",
                    _one_line_sql(top.get("recommendation"), 200),
                ]
            )
            parent.setData(0, Qt.UserRole, top.drop(labels=["_impact_num"], errors="ignore").to_dict())
            parent.setToolTip(0, f"{top.get('family') or ''} - {top.get('evidence') or ''}".strip(" -"))
            parent.setToolTip(5, str(top.get("recommendation") or ""))
            font = parent.font(0)
            font.setBold(True)
            parent.setFont(0, font)
            grouped_members: dict[str, list[pd.Series]] = {}
            for _, row in members.iterrows():
                query_id = _normalized_query_id(row.get("query_id"))
                if query_id:
                    group_id = self._query_group_lookup.get(query_id, "UNGROUPED QUERIES")
                else:
                    group_id = "NON-QUERY FINDINGS"
                grouped_members.setdefault(group_id, []).append(row)
            ordered_groups = sorted(
                grouped_members.items(),
                key=lambda item: -max(float(row.get("_impact_num") or 0.0) for row in item[1]),
            )
            for group_id, group_rows in ordered_groups:
                query_rows = [row for row in group_rows if _normalized_query_id(row.get("query_id"))]
                representative_id = self._group_representative_query.get(group_id, "")
                if not representative_id and query_rows:
                    representative_id = _normalized_query_id(query_rows[0].get("query_id"))
                representative_sql = self._sql_lookup.get(representative_id, "")
                group_severities = {str(row.get("severity") or "").lower() for row in group_rows}
                group_severity = min(
                    group_severities,
                    key=lambda value: _INSIGHT_SEVERITY_RANK.get(value, 9),
                ) if group_severities else ""
                group_impact = max(float(row.get("_impact_num") or 0.0) for row in group_rows)
                group_label = (
                    f"Grouped Query ID {group_id}"
                    if group_id not in ("UNGROUPED QUERIES", "NON-QUERY FINDINGS")
                    else group_id.title()
                )
                group_detail = (
                    _one_line_sql(representative_sql)
                    if representative_sql
                    else _one_line_sql(group_rows[0].get("evidence"))
                )
                group_item = QTreeWidgetItem(
                    [
                        group_label,
                        group_severity.upper(),
                        f"{len(query_rows):,} query ID(s)" if query_rows else f"{len(group_rows):,} finding(s)",
                        str(group_rows[0].get("metric_label") or ""),
                        f"{group_impact:.0f}",
                        group_detail,
                    ]
                )
                representative_row = next(
                    (
                        row for row in group_rows
                        if _normalized_query_id(row.get("query_id")) == representative_id
                    ),
                    group_rows[0],
                )
                group_item.setData(
                    0,
                    Qt.UserRole,
                    representative_row.drop(labels=["_impact_num"], errors="ignore").to_dict(),
                )
                group_item.setToolTip(
                    0,
                    f"Representative query: {representative_id or '-'}; expand to see individual query IDs.",
                )
                if representative_sql:
                    group_item.setToolTip(5, representative_sql[:1500])
                group_font = group_item.font(0)
                group_font.setBold(True)
                group_item.setFont(0, group_font)
                for row in group_rows:
                    query_id = _normalized_query_id(row.get("query_id"))
                    if query_id:
                        label = f"Query ID {query_id}"
                        sql = self._sql_lookup.get(query_id, "")
                        detail = _one_line_sql(sql) if sql else _one_line_sql(row.get("evidence"))
                    else:
                        label = str(row.get("target_label") or row.get("table_key") or row.get("subject") or "-")
                        sql = ""
                        detail = _one_line_sql(row.get("evidence"))
                    child = QTreeWidgetItem(
                        [
                            label,
                            str(row.get("severity") or "").upper(),
                            query_id or "-",
                            str(row.get("metric_display") or row.get("metric_value") or ""),
                            f"{float(row.get('_impact_num') or 0.0):.0f}",
                            detail,
                        ]
                    )
                    child.setData(0, Qt.UserRole, row.drop(labels=["_impact_num"], errors="ignore").to_dict())
                    if sql:
                        child.setToolTip(5, sql[:1500])
                    child.setToolTip(0, "Double-click for the full insight detail.")
                    group_item.addChild(child)
                parent.addChild(group_item)
            self._tree.addTopLevelItem(parent)
        total = len(work)
        self._status.setText(
            f"{len(groups):,} insight rules rolled up from {total:,} findings. "
            "Expand a rule to see grouped query IDs, then expand a group to see individual query IDs."
        )

    def _toggle_expand(self) -> None:
        if self._expand_btn.text() == "Expand All":
            self._tree.expandAll()
            self._expand_btn.setText("Collapse All")
        else:
            self._tree.collapseAll()
            self._expand_btn.setText("Expand All")

    def _on_double_click(self, item: QTreeWidgetItem, _column: int) -> None:
        payload = item.data(0, Qt.UserRole)
        if isinstance(payload, dict) and payload:
            self.rowActivated.emit(pd.Series(payload))

    def _open_wide_grid(self) -> None:
        if self._df.empty:
            QMessageBox.information(self, "Insight Ledger", "Load insights first.")
            return
        cols = [c for c in INSIGHT_COLS if c in self._df.columns] or list(self._df.columns)
        model = _DataFrameModel(self._df[cols], row_df=self._df)
        _open_model_grid(self, "Insight Ledger Wide Grid", model, "Load insights first.")


class _MultiSelectFilter(QToolButton):
    """Dropdown of checkable values; an empty selection means 'show all'.

    Uses QWidgetAction-wrapped checkboxes so the menu stays open while the
    user ticks several values.
    """

    selectionChanged = Signal()

    def __init__(self, label: str, parent=None):
        super().__init__(parent)
        self._label = label
        self._values: list[str] = []
        self._selected: set[str] = set()
        self.setObjectName("Ghost")
        self.setPopupMode(QToolButton.InstantPopup)
        self.setToolButtonStyle(Qt.ToolButtonTextOnly)
        self.setMinimumWidth(140)
        self._menu = QMenu(self)
        self.setMenu(self._menu)
        self._refresh_text()

    def set_values(self, values) -> None:
        cleaned = sorted(
            {
                text
                for text in (str(v).strip() for v in values if v is not None)
                if text and text.lower() not in ("nan", "none")
            }
        )
        if cleaned == self._values:
            return
        self._values = cleaned
        self._selected &= set(cleaned)
        self._rebuild_menu()
        self._refresh_text()

    def selected_values(self) -> set[str]:
        return set(self._selected)

    def _rebuild_menu(self) -> None:
        self._menu.clear()
        show_all = self._menu.addAction("Show All")
        show_all.triggered.connect(self._clear)
        self._menu.addSeparator()
        for value in self._values:
            box = QCheckBox(value)
            box.setChecked(value in self._selected)
            box.setStyleSheet("padding:4px 10px;")
            box.toggled.connect(lambda checked, v=value: self._toggle(v, checked))
            holder = QWidgetAction(self._menu)
            holder.setDefaultWidget(box)
            self._menu.addAction(holder)

    def _toggle(self, value: str, checked: bool) -> None:
        if checked:
            self._selected.add(value)
        else:
            self._selected.discard(value)
        self._refresh_text()
        self.selectionChanged.emit()

    def _clear(self) -> None:
        if not self._selected:
            return
        self._selected.clear()
        self._rebuild_menu()
        self._refresh_text()
        self.selectionChanged.emit()

    def _refresh_text(self) -> None:
        if not self._selected:
            self.setText(f"{self._label}: All")
        elif len(self._selected) == 1:
            self.setText(f"{self._label}: {next(iter(self._selected))}")
        else:
            self.setText(f"{self._label}: {len(self._selected)} selected")


class _ViewDefinitionsPage(QWidget):
    """Sortable view catalog whose definition opens in the full SQL Lens."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._views = pd.DataFrame()
        self._table_review = pd.DataFrame()
        self._known_queries = pd.DataFrame()
        self._model: _DataFrameModel | None = None

        root = QVBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(8)
        header = QHBoxLayout()
        title = QLabel("VIEW DEFINITIONS")
        title.setObjectName("SectionHeader")
        header.addWidget(title)
        self._filter = QLineEdit()
        self._filter.setPlaceholderText("Filter database, schema, or view name...")
        self._filter.setClearButtonEnabled(True)
        self._filter.setMinimumWidth(280)
        self._filter.textChanged.connect(self._refresh)
        header.addWidget(self._filter)
        header.addStretch(1)
        self._status = QLabel("Open this tab to load captured view definitions.")
        self._status.setObjectName("Caption")
        header.addWidget(self._status)
        root.addLayout(header)

        hint = QLabel(
            "Double-click a view to zoom into its complete SQL definition. The examiner includes Format SQL, "
            "Extract Subqueries, Explode Views, Show Lineage, object discovery, and Analyze SQL."
        )
        hint.setObjectName("Caption")
        hint.setWordWrap(True)
        root.addWidget(hint)

        self._table = QTableView()
        _configure_table_view(self._table)
        self._table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._table.doubleClicked.connect(self._open_selected)
        root.addWidget(self._table, 1)

    def set_report(self, report: object) -> None:
        self._views = getattr(report, "view_definitions", pd.DataFrame()).copy()
        self._table_review = getattr(report, "table_review", pd.DataFrame()).copy()
        self._known_queries = getattr(report, "slow_queries", pd.DataFrame()).copy()
        self._refresh()

    def has_data(self) -> bool:
        return not self._views.empty

    def _refresh(self, *_args) -> None:
        frame = self._views.copy()
        text = self._filter.text().strip()
        if text and not frame.empty:
            masks = []
            for column in ("database", "schema", "view_name"):
                if column in frame.columns:
                    masks.append(
                        frame[column].fillna("").astype(str).str.contains(
                            text, case=False, regex=False, na=False
                        )
                    )
            if masks:
                mask = masks[0]
                for candidate in masks[1:]:
                    mask = mask | candidate
                frame = frame.loc[mask].copy()
        if frame.empty:
            self._table.setModel(None)
            self._model = None
            self._status.setText(
                "No views match the filter." if not self._views.empty else "No view definitions are loaded."
            )
            return
        if "source_definition" in frame.columns:
            frame["definition_length"] = frame["source_definition"].fillna("").astype(str).str.len()
            frame["sql_preview"] = (
                frame["source_definition"].fillna("").astype(str).str.replace(r"\s+", " ", regex=True).str.slice(0, 240)
            )
        columns = [
            column for column in
            ("database", "schema", "view_name", "definition_length", "sql_preview", "captured_at")
            if column in frame.columns
        ]
        self._model = _DataFrameModel(frame[columns], row_df=frame)
        self._table.setModel(self._model)
        self._table.resizeColumnsToContents()
        self._table.horizontalHeader().setStretchLastSection(True)
        self._status.setText(f"{len(frame):,} of {len(self._views):,} view definition(s)")
        if self._model.rowCount():
            self._table.selectRow(0)

    def _open_selected(self, index=None) -> None:
        if self._model is None:
            return
        row_index = index.row() if index is not None and index.isValid() else self._table.currentIndex().row()
        row = self._model.row_at(row_index)
        sql = _clean_compact_text(row.get("source_definition"))
        if not sql:
            QMessageBox.information(self, "View Definition", "The selected view has no captured SQL definition.")
            return
        qualified = ".".join(
            part for part in (
                _clean_compact_text(row.get("database")),
                _clean_compact_text(row.get("schema")),
                _clean_compact_text(row.get("view_name")),
            ) if part
        )
        dialog = QDialog(self)
        dialog.setWindowTitle(f"View SQL Examiner - {qualified or 'selected view'}")
        layout = QVBoxLayout(dialog)
        layout.setContentsMargins(0, 0, 0, 0)
        lens = _SqlLensPage(dialog)
        lens.set_context(self._table_review, self._known_queries, self._views)
        lens.load_external_sql(sql, f"view {qualified or 'definition'}", analyze=False)
        layout.addWidget(lens)
        dialog._lens = lens
        dialog.showMaximized()
        dialog.exec()


class _TablePage(QWidget):
    loadRequested = Signal(str)
    rowActivated = Signal(object)
    _MIN_ROWS_FILTER_CHOICES = [
        ("No Filter", 0),
        ("1 Million", 1_000_000),
        ("100 Million", 100_000_000),
        ("1 Billion", 1_000_000_000),
        ("20 Billion", 20_000_000_000),
    ]

    def __init__(
        self,
        title: str,
        preferred_cols: list[str],
        parent=None,
        *,
        load_area: str | None = None,
        load_label: str = "Load Rows",
        empty_message: str | None = None,
        min_rows_filter: bool = False,
        header_labels: dict[str, str] | None = None,
        copy_column: str | None = None,
        filter_columns: list[tuple[str, str]] | None = None,
    ):
        super().__init__(parent)
        self._preferred_cols = preferred_cols
        self._model: _DataFrameModel | None = None
        self._source_df = pd.DataFrame()
        self._load_area = load_area
        self._empty_message = empty_message or "No rows are loaded for this panel."
        self._min_rows_filter_enabled = bool(min_rows_filter)
        self._header_labels = dict(header_labels or {})
        self._copy_column = str(copy_column or "").strip()
        self._filter_columns = list(filter_columns or [])
        self._column_filters: dict[str, _MultiSelectFilter] = {}
        self._copy_flash_serial = 0
        lay = QVBoxLayout(self)
        lay.setContentsMargins(12, 12, 12, 12)
        lay.setSpacing(8)
        head_row = QHBoxLayout()
        head = QLabel(title)
        head.setObjectName("SectionHeader")
        head_row.addWidget(head)
        head_row.addStretch(1)
        self._show_size_rows = QCheckBox("Show Size/Row Cnt")
        self._show_dist_sort = QCheckBox("Show Dist/Sort Keys")
        self._show_size_rows.setChecked(False)
        self._show_dist_sort.setChecked(False)
        has_size_rows = TABLE_SIZE_ROW_COL in preferred_cols
        has_dist_sort = TABLE_DIST_SORT_COL in preferred_cols
        self._show_size_rows.setVisible(has_size_rows)
        self._show_dist_sort.setVisible(has_dist_sort)
        head_row.addWidget(self._show_size_rows)
        head_row.addWidget(self._show_dist_sort)
        if self._load_area:
            load_btn = QPushButton(load_label)
            load_btn.setObjectName("Primary")
            load_btn.clicked.connect(lambda: self.loadRequested.emit(str(self._load_area)))
            head_row.addWidget(load_btn)
        wide_btn = QPushButton("Wide Grid")
        wide_btn.setObjectName("Ghost")
        wide_btn.setToolTip("Open this table in a large view with visible horizontal scrolling.")
        wide_btn.clicked.connect(self._open_wide_grid)
        head_row.addWidget(wide_btn)
        lay.addLayout(head_row)
        if self._filter_columns:
            column_filter_row = QHBoxLayout()
            column_filter_row.setSpacing(8)
            for column_name, label in self._filter_columns:
                widget = _MultiSelectFilter(label)
                widget.selectionChanged.connect(self._refresh_table_model)
                self._column_filters[column_name] = widget
                column_filter_row.addWidget(widget)
            column_filter_row.addStretch(1)
            lay.addLayout(column_filter_row)
        self._min_rows_combo = QComboBox()
        if self._min_rows_filter_enabled:
            filter_row = QHBoxLayout()
            filter_row.setSpacing(8)
            filter_label = QLabel("Minimum Rows")
            filter_label.setObjectName("Caption")
            filter_row.addWidget(filter_label)
            for label_text, value in self._MIN_ROWS_FILTER_CHOICES:
                self._min_rows_combo.addItem(label_text, value)
            self._min_rows_combo.setMinimumWidth(150)
            self._min_rows_combo.setMaximumWidth(190)
            filter_row.addWidget(self._min_rows_combo)
            filter_row.addStretch(1)
            lay.addLayout(filter_row)
        self._status = QLabel(self._empty_message)
        self._status.setObjectName("Caption")
        self._status.setWordWrap(True)
        lay.addWidget(self._status)
        self._table = QTableView()
        _configure_table_view(self._table)
        self._copy_popup = QLabel("", self)
        self._copy_popup.setAlignment(Qt.AlignCenter)
        self._copy_popup.setStyleSheet(
            "background:#DC2626;color:#FFFFFF;font-weight:800;"
            "border-radius:4px;padding:6px 10px;"
        )
        self._copy_popup_effect = QGraphicsOpacityEffect(self._copy_popup)
        self._copy_popup_effect.setOpacity(1.0)
        self._copy_popup.setGraphicsEffect(self._copy_popup_effect)
        self._copy_popup_fade = QPropertyAnimation(self._copy_popup_effect, b"opacity", self)
        self._copy_popup_fade.setDuration(350)
        self._copy_popup_fade.finished.connect(self._copy_popup.hide)
        self._copy_popup.hide()
        self._top_scroll = _add_external_horizontal_scrollbar(lay, self._table)
        lay.addWidget(self._table, 1)
        self._bottom_scroll = _add_external_horizontal_scrollbar(lay, self._table)
        self._delayed_tooltips = _DelayedTableToolTips(self._table, numeric_only=True, delay_ms=2000)
        self._show_size_rows.stateChanged.connect(self._apply_detail_column_visibility)
        self._show_dist_sort.stateChanged.connect(self._apply_detail_column_visibility)
        if self._min_rows_filter_enabled:
            self._min_rows_combo.currentIndexChanged.connect(self._refresh_table_model)
        self._table.doubleClicked.connect(self._emit_row_activated)
        if self._copy_column:
            self._table.horizontalHeader().sectionClicked.connect(self._on_header_section_clicked)

    def set_dataframe(self, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            self._source_df = pd.DataFrame()
            self._table.setModel(None)
            self._model = None
            self._status.setText(self._empty_message)
            return
        self._source_df = df.copy()
        for column_name, widget in self._column_filters.items():
            if column_name in self._source_df.columns:
                widget.set_values(self._source_df[column_name])
        self._refresh_table_model()

    def _refresh_table_model(self, *_args) -> None:
        if self._source_df is None or self._source_df.empty:
            self._table.setModel(None)
            self._model = None
            self._status.setText(self._empty_message)
            return
        df = self._filtered_source_df()
        if df.empty:
            self._table.setModel(None)
            self._model = None
            if self._column_filters_active():
                self._status.setText("No rows match the selected filters. Use Show All to clear a filter.")
            else:
                self._status.setText(f"No rows match {_min_rows_filter_label(self._min_rows_value())}.")
            return
        display_df, sort_sources = _table_attribute_display_frame(df)
        cols = [c for c in self._preferred_cols if c in display_df.columns]
        if not cols:
            cols = list(display_df.columns)
        self._model = _DataFrameModel(
            display_df[cols],
            sort_sources=sort_sources,
            row_df=df,
            header_labels=self._header_labels,
        )
        self._table.setModel(self._model)
        self._table.resizeColumnsToContents()
        self._table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self._sync_scrollbars()
        total = len(self._source_df)
        if self._min_rows_filter_enabled and self._min_rows_value() > 0:
            self._status.setText(
                f"{len(df):,} of {total:,} rows shown with {_min_rows_filter_label(self._min_rows_value())}. "
                "Use Wide Grid if this panel is clipped."
            )
        elif self._column_filters_active():
            self._status.setText(
                f"{len(df):,} of {total:,} rows shown with the selected filters. "
                "Use Wide Grid if this panel is clipped."
            )
        else:
            self._status.setText(f"{len(df):,} rows loaded. Use Wide Grid if this panel is clipped.")
        self._apply_detail_column_visibility()

    def _filtered_source_df(self) -> pd.DataFrame:
        df = self._source_df.copy()
        for column_name, widget in self._column_filters.items():
            selected = widget.selected_values()
            if selected and column_name in df.columns:
                df = df.loc[df[column_name].astype(str).str.strip().isin(selected)]
        minimum = self._min_rows_value()
        if not self._min_rows_filter_enabled or minimum <= 0 or "tbl_rows" not in df.columns:
            return df.reset_index(drop=True)
        rows = pd.to_numeric(df["tbl_rows"], errors="coerce").fillna(0)
        return df.loc[rows >= minimum].reset_index(drop=True)

    def _column_filters_active(self) -> bool:
        return any(widget.selected_values() for widget in self._column_filters.values())

    def _min_rows_value(self) -> int:
        if not self._min_rows_filter_enabled:
            return 0
        value = self._min_rows_combo.currentData()
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def _apply_detail_column_visibility(self, *_args) -> None:
        if not self._model:
            return
        for col, checkbox in (
            (TABLE_SIZE_ROW_COL, self._show_size_rows),
            (TABLE_DIST_SORT_COL, self._show_dist_sort),
        ):
            index = self._model.column_index(col)
            if index is not None:
                self._table.setColumnHidden(index, not checkbox.isChecked())
        self._sync_scrollbars()

    def _sync_scrollbars(self) -> None:
        _sync_external_horizontal_scrollbar(self._top_scroll, self._table)
        _sync_external_horizontal_scrollbar(self._bottom_scroll, self._table)

    def selected_row(self) -> pd.Series | None:
        if not self._model:
            return None
        selection = self._table.selectionModel()
        indexes = selection.selectedRows() if selection is not None else []
        if not indexes:
            return None
        return self._model.row_at(indexes[0].row())

    def _emit_row_activated(self, index: QModelIndex) -> None:
        if not self._model or not index.isValid():
            return
        self.rowActivated.emit(self._model.row_at(index.row()))

    def _on_header_section_clicked(self, section: int) -> None:
        if not self._model or not self._copy_column:
            return
        if self._model.column_name(section) != self._copy_column:
            return
        ids = self._visible_copy_column_ids()
        if not ids:
            self._show_copy_popup("Copied 0 queries", section)
            return
        QApplication.clipboard().setText(", ".join(ids))
        self._show_copy_popup(f"Copied {len(ids):,} queries", section)

    def _visible_copy_column_ids(self) -> list[str]:
        if not self._model or not self._copy_column:
            return []
        ids: list[str] = []
        seen: set[str] = set()
        for row in range(self._model.rowCount()):
            row_data = self._model.row_at(row)
            if self._copy_column not in row_data:
                continue
            for query_id in _split_csv(row_data.get(self._copy_column)):
                if query_id in seen:
                    continue
                seen.add(query_id)
                ids.append(query_id)
        return ids

    def _show_copy_popup(self, message: str, section: int) -> None:
        self._copy_flash_serial += 1
        serial = self._copy_flash_serial
        self._copy_popup_fade.stop()
        self._copy_popup_effect.setOpacity(1.0)
        self._copy_popup.setText(message)
        self._copy_popup.adjustSize()

        header = self._table.horizontalHeader()
        section_x = max(0, header.sectionViewportPosition(section))
        header_pos = header.viewport().mapTo(self, header.viewport().rect().topLeft())
        x = header_pos.x() + section_x + 8
        y = header_pos.y() + header.height() + 6
        max_x = max(8, self.width() - self._copy_popup.width() - 8)
        self._copy_popup.move(min(max(8, x), max_x), max(8, y))
        self._copy_popup.show()
        self._copy_popup.raise_()

        def fade_if_current() -> None:
            if serial == self._copy_flash_serial:
                self._copy_popup_fade.stop()
                self._copy_popup_fade.setStartValue(1.0)
                self._copy_popup_fade.setEndValue(0.0)
                self._copy_popup_fade.start()

        QTimer.singleShot(2000, fade_if_current)

    def _open_wide_grid(self) -> None:
        _open_model_grid(
            self,
            "Wide Grid",
            self._model,
            self._empty_message,
        )


class _DelayedTableToolTips(QObject):
    def __init__(
        self,
        table: QTableView,
        *,
        numeric_only: bool = False,
        delay_ms: int = 2000,
    ):
        super().__init__(table)
        self._table = table
        self._numeric_only = bool(numeric_only)
        self._delay_ms = max(100, int(delay_ms or 2000))
        self._pending_key: tuple[str, int] | None = None
        self._pending_text = ""
        self._pending_pos = None
        self._timer = QTimer(self)
        self._timer.setSingleShot(True)
        self._timer.timeout.connect(self._show_pending)
        self._table.setMouseTracking(True)
        self._table.viewport().setMouseTracking(True)
        self._table.viewport().installEventFilter(self)
        header = self._table.horizontalHeader()
        header.setMouseTracking(True)
        header.viewport().setMouseTracking(True)
        header.viewport().installEventFilter(self)

    def eventFilter(self, watched: QObject, event: QEvent) -> bool:
        event_type = event.type()
        if event_type == QEvent.ToolTip:
            return True
        if event_type == QEvent.MouseMove:
            payload = self._tooltip_payload(watched, event)
            if payload is None:
                self._clear_pending()
                return False
            key, text, global_pos = payload
            if key != self._pending_key or text != self._pending_text:
                QToolTip.hideText()
                self._pending_key = key
                self._pending_text = text
                self._pending_pos = global_pos
                self._timer.start(self._delay_ms)
            else:
                self._pending_pos = global_pos
            return False
        if event_type in {QEvent.Leave, QEvent.Wheel, QEvent.MouseButtonPress}:
            self._clear_pending()
            QToolTip.hideText()
        return False

    def _tooltip_payload(self, watched: QObject, event: QEvent):
        model = self._table.model()
        if model is None:
            return None
        header = self._table.horizontalHeader()
        is_header = watched is header.viewport()
        if is_header:
            section = header.logicalIndexAt(event.pos())
            if section < 0:
                return None
            if self._numeric_only and not _model_column_is_numeric(model, section):
                return None
            text = _model_column_tooltip(model, section)
            key = ("header", int(section))
        else:
            index = self._table.indexAt(event.pos())
            if not index.isValid():
                return None
            if self._numeric_only and not _model_column_is_numeric(model, index.column()):
                return None
            text = _model_column_tooltip(model, index.column())
            key = ("cell", int(index.column()))
        if not text:
            return None
        try:
            global_pos = event.globalPosition().toPoint()
        except AttributeError:
            global_pos = event.globalPos()
        return key, text, global_pos

    def _show_pending(self) -> None:
        if self._pending_text and self._pending_pos is not None:
            QToolTip.showText(self._pending_pos, self._pending_text, self._table)

    def _clear_pending(self) -> None:
        self._timer.stop()
        self._pending_key = None
        self._pending_text = ""
        self._pending_pos = None


def _model_column_tooltip(model: object, section: int) -> str:
    if hasattr(model, "column_tooltip"):
        try:
            return str(model.column_tooltip(section) or "")
        except Exception:
            return ""
    try:
        value = model.headerData(section, Qt.Horizontal, Qt.ToolTipRole)
        return str(value or "")
    except Exception:
        return ""


def _model_column_is_numeric(model: object, section: int) -> bool:
    if hasattr(model, "is_numeric_column"):
        try:
            return bool(model.is_numeric_column(section))
        except Exception:
            return False
    return False


class _DataFrameModel(QAbstractTableModel):
    def __init__(
        self,
        df: pd.DataFrame,
        *,
        sort_sources: pd.DataFrame | None = None,
        row_df: pd.DataFrame | None = None,
        header_labels: dict[str, str] | None = None,
    ):
        super().__init__()
        self._df = df.reset_index(drop=True)
        self._row_df = row_df.reset_index(drop=True) if row_df is not None else self._df.copy()
        self._sort_sources = sort_sources.reset_index(drop=True) if sort_sources is not None else pd.DataFrame(index=self._df.index)
        self._header_labels = dict(header_labels or {})
        self._heat_ranges = self._build_heat_ranges()

    def rowCount(self, parent=QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._df)

    def columnCount(self, parent=QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._df.columns)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.TextAlignmentRole and orientation == Qt.Horizontal:
            return int(Qt.AlignLeft | Qt.AlignVCenter | Qt.TextWordWrap)
        if orientation == Qt.Horizontal:
            col = str(self._df.columns[section])
            if role == Qt.ToolTipRole:
                return self.column_tooltip(section)
            if role != Qt.DisplayRole:
                return None
            return self._header_labels.get(col) or DISPLAY_COLUMN_LABELS.get(col, _column_header_label(col))
        if role != Qt.DisplayRole:
            return None
        return str(section + 1)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        col = self._df.columns[index.column()]
        val = self._df.iat[index.row(), index.column()]
        if role == Qt.DisplayRole:
            return _fmt_value(col, val)
        if role == Qt.TextAlignmentRole and _is_numeric(val):
            return int(Qt.AlignRight | Qt.AlignVCenter)
        if role == Qt.BackgroundRole:
            try:
                visual_status = str(self._row_df.iloc[index.row()].get("visual_status") or "").lower()
            except Exception:
                visual_status = ""
            visual_color = {
                "red": PALETTE.crit,
                "amber": PALETTE.warn,
                "green": PALETTE.ok,
            }.get(visual_status)
            if visual_color:
                color = QColor(visual_color)
                color.setAlpha(58)
                return color
            source_val = self._sort_source_value(col, val, index.row())
            color = self._heat_bg(col, source_val, index.row()) or _cell_bg(col, source_val)
            return QColor(color) if color else None
        if role == Qt.ForegroundRole:
            source_val = self._sort_source_value(col, val, index.row())
            heat_color = self._heat_bg(col, source_val, index.row()) or _cell_bg(col, source_val)
            if _uses_light_heat_text(col) and heat_color:
                return QColor("#111827")
            if col == TABLE_DIST_SORT_COL:
                dist_missing, sort_missing = _dist_sort_missing_state(val)
                if dist_missing and sort_missing:
                    return QColor(PALETTE.crit)
                if dist_missing or sort_missing:
                    return QColor(PALETTE.warn)
            row_object_type = ""
            try:
                row_object_type = str(self._row_df.iloc[index.row()].get("object_type") or "").lower()
            except Exception:
                row_object_type = ""
            if col in {"object_type", "query_table", "table_name", "component_of"}:
                if row_object_type in {"view", "view_component_view"}:
                    return QColor(PALETTE.violet)
                if "table" in row_object_type:
                    return QColor(PALETTE.cyan)
            if col == "distribution_signal":
                signal = str(val or "").lower()
                if "co-located" in signal:
                    return QColor(PALETTE.ok)
                if "broadcast" in signal or "dist_both" in signal or "redistribute" in signal:
                    return QColor(PALETTE.crit)
                if "unknown" in signal or "needs" in signal:
                    return QColor(PALETTE.warn)
            if col == "impact_band":
                band = str(val or "").lower()
                if band == "critical":
                    return QColor(PALETTE.crit)
                if band == "high":
                    return QColor(PALETTE.warn)
                if band == "medium":
                    return QColor(PALETTE.accent_bright)
            if col == "stats_off" and _safe_float(val) >= 100:
                return QColor(PALETTE.crit)
            if col == "severity":
                sev = str(val)
                return QColor(PALETTE.crit if sev == "crit" else PALETTE.warn if sev == "warn" else PALETTE.accent_bright)
            if col in {"join_signal", "predicate_signal"}:
                try:
                    visual_status = str(self._row_df.iloc[index.row()].get("visual_status") or "").lower()
                except Exception:
                    visual_status = ""
                return QColor(
                    PALETTE.crit if visual_status == "red"
                    else PALETTE.ok if visual_status == "green"
                    else PALETTE.warn
                )
        return None

    def sort(self, column: int, order: Qt.SortOrder = Qt.AscendingOrder) -> None:
        if column < 0 or column >= len(self._df.columns):
            return
        self.layoutAboutToBeChanged.emit()
        col = self._df.columns[column]
        ascending = order == Qt.AscendingOrder
        source = self._sort_sources[col] if col in self._sort_sources.columns else self._df[col]
        numeric_values = pd.to_numeric(source, errors="coerce")
        if numeric_values.notna().any():
            sort_values = numeric_values
        else:
            sort_values = source.astype(str)
        order_df = pd.DataFrame({"_row_pos": range(len(self._df)), "_sort_key": sort_values})
        order_df = order_df.sort_values("_sort_key", ascending=ascending, na_position="last", kind="mergesort")
        order_index = order_df["_row_pos"].tolist()
        self._df = self._df.iloc[order_index].reset_index(drop=True)
        self._row_df = self._row_df.iloc[order_index].reset_index(drop=True)
        if not self._sort_sources.empty:
            self._sort_sources = self._sort_sources.iloc[order_index].reset_index(drop=True)
        self._heat_ranges = self._build_heat_ranges()
        self.layoutChanged.emit()

    def row_at(self, row: int) -> pd.Series:
        if row < 0 or row >= len(self._row_df):
            return pd.Series(dtype=object)
        return self._row_df.iloc[row]

    def column_index(self, column: str) -> int | None:
        try:
            return int(self._df.columns.get_loc(column))
        except KeyError:
            return None

    def column_name(self, section: int) -> str:
        if section < 0 or section >= len(self._df.columns):
            return ""
        return str(self._df.columns[section])

    def column_tooltip(self, section: int) -> str:
        col = self.column_name(section)
        if not col:
            return ""
        header = self._header_labels.get(col) or DISPLAY_COLUMN_LABELS.get(col, _column_header_label(col))
        detail = COLUMN_TOOLTIPS.get(col)
        if detail:
            return f"{header}: {detail}"
        return f"{header}: loaded DuckDB analytics column from this panel."

    def is_numeric_column(self, section: int) -> bool:
        col = self.column_name(section)
        if not col:
            return False
        if col in HEAT_RANGE_COLUMNS or col in {
            TABLE_SORTED_PCT_COL,
            "stats_off",
            "skew_rows",
            "vacuum_sort_benefit",
            "sort_key_usage_score",
        }:
            return True
        for frame in (self._sort_sources, self._row_df, self._df):
            if col not in frame.columns:
                continue
            values = pd.to_numeric(frame[col], errors="coerce")
            if values.notna().any():
                return True
        return False

    def row_for_value(self, column: str, value: str) -> int | None:
        if column in self._df.columns:
            source = self._df[column]
        elif column in self._row_df.columns:
            source = self._row_df[column]
        else:
            return None
        wanted = str(value or "").strip()
        if not wanted:
            return None
        matches = source.index[source.astype(str).str.strip() == wanted].tolist()
        return int(matches[0]) if matches else None

    def _build_heat_ranges(self) -> dict[str, tuple[float, float]]:
        ranges: dict[str, tuple[float, float]] = {}
        for col in self._df.columns:
            if str(col) not in HEAT_RANGE_COLUMNS:
                continue
            source = self._sort_sources[col] if col in self._sort_sources.columns else self._df[col]
            values = pd.to_numeric(source, errors="coerce").dropna()
            if values.empty:
                continue
            if str(col) in {"table_attention_score", "table_risk_score", "severity_score", "risk_score"}:
                low = float(values.quantile(0.10))
                high = float(values.quantile(0.90))
            else:
                low = float(values.quantile(0.05))
                high = float(values.quantile(0.95))
            if high <= low:
                high = float(values.max())
                low = float(values.min())
            if high > low:
                ranges[str(col)] = (low, high)
        return ranges

    def _heat_bg(self, col: str, val, row: int | None = None) -> str | None:
        bounds = self._heat_ranges.get(str(col))
        if not bounds:
            return None
        source_val = self._sort_source_value(col, val, row)
        try:
            value = float(source_val)
        except (TypeError, ValueError):
            return None
        low, high = bounds
        if high <= low:
            return None
        ratio = max(0.0, min(1.0, (value - low) / (high - low)))
        return _traffic_heat_color(ratio)

    def _sort_source_value(self, col: str, val, row: int | None):
        if row is None or col not in self._sort_sources.columns:
            return val
        try:
            return self._sort_sources.at[row, col]
        except Exception:
            return val


def _configure_table_view(table: QTableView) -> None:
    table.setAlternatingRowColors(True)
    table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)
    table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
    table.setMinimumSize(0, 0)
    table.setSelectionBehavior(QTableView.SelectRows)
    table.setSelectionMode(QAbstractItemView.SingleSelection)
    table.setSortingEnabled(True)
    table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
    table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
    table.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
    table.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
    table.setWordWrap(False)
    table.verticalHeader().setVisible(False)
    table.horizontalHeader().setHighlightSections(False)
    table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
    table.horizontalHeader().setDefaultAlignment(Qt.AlignLeft | Qt.AlignVCenter | Qt.TextWordWrap)
    table.horizontalHeader().setMinimumHeight(34)
    table.horizontalHeader().setMinimumSectionSize(42)
    table.horizontalHeader().setStretchLastSection(False)


def _add_external_horizontal_scrollbar(layout: QVBoxLayout, table: QTableView) -> QScrollBar:
    bar = QScrollBar(Qt.Horizontal)
    bar.setObjectName("GridTopScroll")
    bar.setMinimumHeight(14)
    bar.setToolTip("Horizontal table scroll")
    source = table.horizontalScrollBar()

    def to_table(value: int) -> None:
        if source.value() != value:
            source.setValue(value)

    def to_bar(value: int) -> None:
        if bar.value() != value:
            bar.setValue(value)

    bar.valueChanged.connect(to_table)
    source.valueChanged.connect(to_bar)
    source.rangeChanged.connect(lambda *_args: _sync_external_horizontal_scrollbar(bar, table))
    layout.addWidget(bar)
    _sync_external_horizontal_scrollbar(bar, table)
    return bar


def _sync_external_horizontal_scrollbar(bar: QScrollBar | None, table: QTableView) -> None:
    if bar is None:
        return
    source = table.horizontalScrollBar()
    bar.setRange(source.minimum(), source.maximum())
    bar.setPageStep(source.pageStep())
    bar.setSingleStep(source.singleStep())
    bar.setEnabled(source.maximum() > source.minimum())
    if bar.value() != source.value():
        bar.setValue(source.value())


def _set_tab_tooltips(tabs: QTabWidget) -> None:
    tabs.tabBar().setUsesScrollButtons(True)
    tabs.tabBar().setElideMode(Qt.ElideRight)
    for index in range(tabs.count()):
        tabs.setTabToolTip(index, tabs.tabText(index))


def _resize_dialog_to_screen(dialog: QDialog, ratio: float) -> None:
    screen = QApplication.primaryScreen()
    if screen is None:
        dialog.resize(1200, 800)
        return
    geometry = screen.availableGeometry()
    if ratio >= 0.92:
        margin = 8
        dialog.setGeometry(geometry.adjusted(margin, margin, -margin, -margin))
        return
    width = max(760, int(geometry.width() * ratio))
    height = max(520, int(geometry.height() * ratio))
    width = min(width, geometry.width())
    height = min(height, geometry.height())
    dialog.resize(width, height)
    dialog.move(
        geometry.x() + max(0, (geometry.width() - width) // 2),
        geometry.y() + max(0, (geometry.height() - height) // 2),
    )


def _open_model_grid(parent: QWidget, title: str, model: _DataFrameModel | None, empty_message: str) -> None:
    if model is None or model.rowCount() == 0:
        QMessageBox.information(parent, "Wide Grid", empty_message)
        return
    dialog = QDialog(parent)
    dialog.setWindowTitle(title)
    root = QVBoxLayout(dialog)
    root.setContentsMargins(10, 10, 10, 10)
    root.setSpacing(6)

    status = QLabel(
        f"{model.rowCount():,} rows x {model.columnCount():,} columns. "
        "Scroll horizontally at the bottom of this window. Sorting here does not "
        "reorder the panel behind this dialog."
    )
    status.setObjectName("Caption")
    status.setWordWrap(True)
    root.addWidget(status)

    table = QTableView()
    _configure_table_view(table)
    # Clone via a sort proxy so dialog sort/filter never mutates the live panel model.
    from PySide6.QtCore import QSortFilterProxyModel

    proxy = QSortFilterProxyModel(dialog)
    proxy.setSourceModel(model)
    proxy.setDynamicSortFilter(True)
    table.setModel(proxy)
    table.setSortingEnabled(True)
    dialog._tooltips = _DelayedTableToolTips(table, delay_ms=2000)
    table.resizeColumnsToContents()
    table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
    table.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
    top_scroll = _add_external_horizontal_scrollbar(root, table)
    _sync_external_horizontal_scrollbar(top_scroll, table)
    root.addWidget(table, 1)

    buttons = QDialogButtonBox(QDialogButtonBox.Close)
    buttons.rejected.connect(dialog.close)
    root.addWidget(buttons)
    dialog._model = model
    dialog._proxy = proxy
    _resize_dialog_to_screen(dialog, 0.97)
    dialog.exec()


def _open_load_error_log(parent: QWidget, errors: tuple[str, ...] | list[str]) -> None:
    if not errors:
        QMessageBox.information(parent, "DuckDB Load Error Log", "No DuckDB load errors are recorded for the last app load.")
        return
    df = _load_error_frame(errors)
    dialog = QDialog(parent)
    dialog.setWindowTitle("DuckDB Load Error Log")
    root = QVBoxLayout(dialog)
    root.setContentsMargins(12, 12, 12, 12)
    root.setSpacing(8)

    summary = QLabel(_load_error_summary(df))
    summary.setObjectName("Caption")
    summary.setWordWrap(True)
    root.addWidget(summary)

    model = _DataFrameModel(df)
    table = QTableView()
    _configure_table_view(table)
    table.setModel(model)
    table.resizeColumnsToContents()
    table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
    table.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
    top_scroll = _add_external_horizontal_scrollbar(root, table)
    _sync_external_horizontal_scrollbar(top_scroll, table)
    root.addWidget(table, 1)

    buttons = QDialogButtonBox(QDialogButtonBox.Close)
    buttons.rejected.connect(dialog.close)
    root.addWidget(buttons)
    dialog._model = model
    _resize_dialog_to_screen(dialog, 0.80)
    dialog.exec()


def _clear_layout(layout) -> None:
    while layout.count():
        item = layout.takeAt(0)
        widget = item.widget()
        if widget:
            widget.deleteLater()


def _area_label(areas: object) -> str:
    if areas is None:
        return REPORT_AREA_LABELS.get("all", "Safe Areas")
    if isinstance(areas, str):
        selected = [areas]
    else:
        try:
            selected = [str(area) for area in areas if str(area).strip()]
        except TypeError:
            selected = [str(areas)]
    if not selected:
        selected = ["all"]
    labels = [REPORT_AREA_LABELS.get(area, area) for area in selected]
    return ", ".join(labels)


def _node_positions(names: list[str], x: float, top: float, bottom: float) -> dict[str, QRectF]:
    if not names:
        return {}
    gap = (bottom - top) / max(1, len(names))
    out = {}
    for i, name in enumerate(names):
        y = top + i * gap + gap / 2 - 18
        out[name] = QRectF(x, y, 118, 36)
    return out


def _draw_node(p: QPainter, rect: QRectF, label: str, color: QColor, dark_text: bool = False) -> None:
    p.setPen(Qt.NoPen)
    p.setBrush(color)
    p.drawRoundedRect(rect, 8, 8)
    p.setPen(QColor(PALETTE.bg_0 if dark_text else PALETTE.text_0))
    p.setFont(QFont("Inter", 9, QFont.DemiBold))
    p.drawText(rect.adjusted(6, 0, -6, 0), Qt.AlignCenter, label[:18])


def _impact_color(score: float) -> QColor:
    if score >= 85:
        return QColor(PALETTE.crit)
    if score >= 55:
        return QColor(PALETTE.warn)
    return QColor(PALETTE.accent_bright)


def _column_header_label(column: str) -> str:
    tokens = str(column or "").replace("_", " ").split()
    acronyms = {"id", "sql", "io", "i/o", "db", "s3", "wlm", "mb"}
    out: list[str] = []
    for token in tokens:
        lowered = token.lower()
        if lowered in acronyms:
            out.append(lowered.upper())
        elif lowered == "pct":
            out.append("Pct")
        elif lowered == "cnt":
            out.append("Cnt")
        elif lowered == "rrscan":
            out.append("RR Scan")
        elif lowered == "userid":
            out.append("User ID")
        elif lowered == "distkey":
            out.append("Dist Key")
        elif lowered == "sortkey":
            out.append("Sort Key")
        elif lowered == "s":
            out.append("Sec")
        elif lowered == "m":
            out.append("M")
        else:
            out.append(lowered.capitalize())
    return " ".join(out)


def _num_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(0.0, index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0)


def _log_scaled(series: pd.Series, weight: int | float) -> pd.Series:
    if not weight:
        return pd.Series(0.0, index=series.index, dtype="float64")
    logged = series.clip(lower=0).map(lambda value: math.log1p(float(value)))
    max_value = logged.max()
    if not max_value or pd.isna(max_value):
        return pd.Series(0.0, index=series.index, dtype="float64")
    return (logged / max_value) * float(weight)


def _fmt_int(value) -> str:
    try:
        return f"{int(float(value)):,}"
    except (TypeError, ValueError):
        return "0"


def _fmt_seconds(value) -> str:
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return "0 s"
    if seconds >= 3600:
        return f"{seconds / 3600:.1f} h"
    if seconds >= 60:
        return f"{seconds / 60:.1f} m"
    return f"{seconds:.0f} s"


def _format_trim_preview(preview: dict | None, db_path: Path | None = None) -> str:
    if not preview:
        return "No trim preview available."
    keep_top_n = int(preview.get("keep_top_n") or 0)
    cap_text = f"at most {keep_top_n:,}" if keep_top_n > 0 else "all qualifying"
    query_before = int(preview.get("query_count_before") or 0)
    query_after = int(preview.get("query_count_after") or 0)
    query_removed = int(preview.get("query_count_removed") or 0)
    row_before = int(preview.get("total_rows_before") or 0)
    row_after = int(preview.get("total_rows_after") or 0)
    row_removed = int(preview.get("total_rows_removed") or 0)
    lines = [
        (
            f"DuckDB: {_fmt_file_size(db_path) if db_path is not None else 'unknown'} | "
            f"Rows: {_fmt_compact_count(row_before)} -> {_fmt_compact_count(row_after)} | "
            f"Queries: {_fmt_compact_count(query_before)} -> {_fmt_compact_count(query_after)}"
        ),
        (
            f"Keep {cap_text} query ids with runtime >= {int(preview.get('min_elapsed_minutes') or 0)} minutes"
            + (", ordered by risk score, spill, then runtime." if keep_top_n > 0 else ".")
        ),
        (
            f"Queries removed: {_fmt_compact_count(query_removed)} "
            f"({float(preview.get('query_reduction_pct') or 0):.1f}%)."
        ),
        (
            f"Raw rows removed: {_fmt_compact_count(row_removed)} "
            f"({float(preview.get('row_reduction_pct') or 0):.1f}%)."
        ),
    ]
    table_rows = list(preview.get("table_rows") or [])
    table_rows = sorted(table_rows, key=lambda row: int(row.get("rows_removed") or 0), reverse=True)
    changed = [row for row in table_rows if int(row.get("rows_removed") or 0) > 0][:5]
    if changed:
        lines.append(
            "Largest table reductions: "
            + "; ".join(
                f"{row.get('table_name')}: "
                f"{_fmt_compact_count(row.get('rows_before'))}->{_fmt_compact_count(row.get('rows_after'))}"
                for row in changed
            )
        )
    return "\n".join(lines)


def _fmt_file_size(path: Path | None) -> str:
    if path is None:
        return "unknown"
    try:
        size = float(path.stat().st_size)
    except OSError:
        return "unknown"
    units = ("B", "KB", "MB", "GB", "TB")
    unit = units[0]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024
    if unit == "B":
        return f"{int(size):,} {unit}"
    return f"{size:,.1f} {unit}"


def _fmt_compact_count(value: object) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "0"
    if pd.isna(number):
        return "0"
    sign = "-" if number < 0 else ""
    number = abs(number)
    if number >= 1_000_000_000:
        return f"{sign}{math.ceil(number / 1_000_000_000):.0f}B"
    if number >= 1_000_000:
        return f"{sign}{math.ceil(number / 1_000_000):.0f}M"
    if number >= 10_000:
        thousands = math.ceil(number / 1_000)
        if thousands >= 1000:
            return f"{sign}{math.ceil(number / 1_000_000):.0f}M"
        return f"{sign}{thousands:.0f}K"
    if number >= 1_000:
        return f"{sign}{math.floor(number / 100) / 10:.1f}K"
    return f"{sign}{int(math.ceil(number)):,}"


def _fmt_value(col: str, val) -> str:
    if col == "full_explain_available":
        if pd.isna(val):
            return "No"
        try:
            return "Yes" if float(val) > 0 else "No"
        except (TypeError, ValueError):
            text = str(val).strip().lower()
            return "Yes" if text in {"yes", "true", "1"} else "No"
    if pd.isna(val):
        return "-"
    if col.endswith("_score"):
        try:
            n = float(val)
            return f"{round(n):,.0f}"
        except (TypeError, ValueError):
            return str(val)
    if col == "avg_scan_duration_s":
        try:
            return f"{round(float(val) / 60.0):,.0f} min"
        except (TypeError, ValueError):
            return str(val)
    if col.endswith("_s") or col in {"elapsed_s", "execution_s", "queue_s"}:
        return _fmt_seconds(val)
    if col in {"avg_similarity", "min_similarity", "max_similarity", "similarity_score"}:
        try:
            return f"{float(val) * 100:.1f}%"
        except (TypeError, ValueError):
            return str(val)
    if col == "stats_off":
        try:
            n = float(val)
            return "Missing" if n >= 100 else f"{n:.0f}%"
        except (TypeError, ValueError):
            return str(val)
    if col in {TABLE_SORTED_PCT_COL, "unsorted_pct", "vacuum_sort_benefit"}:
        try:
            return f"{float(val):.0f}%"
        except (TypeError, ValueError):
            return str(val)
    if col == "skew_rows":
        try:
            return f"{float(val):.3f}"
        except (TypeError, ValueError):
            return str(val)
    if col in {"tbl_rows", "input_rows", "output_rows"}:
        return _fmt_compact_count(val)
    if col.endswith("_pct") or col in {"remote_io_ratio", "external_duration_pct"}:
        try:
            return f"{float(val) * 100:.1f}%"
        except (TypeError, ValueError):
            return str(val)
    if col in {"size_mb"}:
        try:
            return f"{float(val):,.0f} MB"
        except (TypeError, ValueError):
            return str(val)
    if _is_numeric(val):
        try:
            n = float(val)
            if abs(n) >= 1000:
                return f"{n:,.0f}"
            return f"{n:.2f}".rstrip("0").rstrip(".")
        except (TypeError, ValueError):
            return str(val)
    text = str(val)
    return text if len(text) <= 180 else text[:177] + "..."


def _fmt_capture_datetime(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        stamp = pd.to_datetime(value)
        if pd.isna(stamp):
            return "-"
        return stamp.strftime("%Y-%m-%d %I:%M%p")
    except Exception:
        text = str(value).strip()
        return text.split(".")[0] if text else "-"


def _databases_accessed_from_output(output: str) -> list[str]:
    for line in str(output or "").splitlines():
        if not line.startswith("Databases accessed:"):
            continue
        raw = line.split(":", 1)[1]
        return [part.strip() for part in raw.split(",") if part.strip()]
    return []


def _clip(text: object, limit: int) -> str:
    value = str(text or "")
    return value if len(value) <= limit else value[: max(0, limit - 3)] + "..."


def _load_error_frame(errors: tuple[str, ...] | list[str]) -> pd.DataFrame:
    rows = []
    for index, raw in enumerate(errors or (), start=1):
        text = str(raw or "").strip()
        if ":" in text:
            area, message = text.split(":", 1)
        else:
            area, message = "DuckDB Load", text
        rows.append(
            {
                "error_no": index,
                "area": area.strip() or "DuckDB Load",
                "message": message.strip(),
                "raw_error": text,
            }
        )
    return pd.DataFrame(rows, columns=["error_no", "area", "message", "raw_error"])


def _load_error_summary(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "No DuckDB load errors are recorded."
    counts = df["area"].astype(str).value_counts().head(8)
    summary = "; ".join(f"{area}: {count}" for area, count in counts.items())
    return f"Total DuckDB load errors: {len(df):,}. By area: {summary}."


def _min_rows_filter_label(value: object) -> str:
    try:
        minimum = int(value or 0)
    except (TypeError, ValueError):
        minimum = 0
    if minimum <= 0:
        return "No Filter"
    if minimum >= 1_000_000_000:
        return f"minimum rows {_fmt_compact_count(minimum)}"
    return f"minimum rows {_fmt_compact_count(minimum)}"


def _format_sql_text(sql_text: object) -> str:
    sql = _decode_sql_display_escapes(str(sql_text or "")).strip()
    if not sql:
        return ""
    formatted, _reason = _format_sql_with_reason(sql)
    if formatted:
        return formatted
    inner = _extract_wrapped_command_inner_sql(sql)
    return _normalize_crlf(inner or sql)


def _format_sql_with_reason(sql_text: object) -> tuple[str, str]:
    """Return (formatted_sql, failure_reason). formatted_sql is '' when the
    formatter could not produce output; failure_reason then explains why."""
    sql = _decode_sql_display_escapes(str(sql_text or "")).strip()
    if not sql:
        return "", "There is no SQL text to format."
    inner = _extract_wrapped_command_inner_sql(sql)
    target = inner or sql
    formatted, redshift_error = _try_format_sqlglot_with_reason(target, read="redshift")
    generic_error = ""
    if not formatted:
        formatted, generic_error = _try_format_sqlglot_with_reason(target)
    if formatted:
        return _normalize_crlf(formatted), ""
    # The strict parser rejected the statement. Fall back to a parse-free
    # keyword reflow so the Format button still produces readable output.
    from ..sql_soft_format import soft_format_sql

    soft = soft_format_sql(target)
    if soft.strip():
        return _normalize_crlf(soft), ""
    return "", redshift_error or generic_error or "The SQL formatter returned no output."


def _apply_format_sql(editor: QPlainTextEdit, parent: QWidget) -> str | None:
    """Format the editor's SQL in place, or explain in a message box why the
    text did not change. Returns the formatted text only when it changed."""
    original = editor.toPlainText()
    formatted, reason = _format_sql_with_reason(original)
    if not formatted:
        QMessageBox.warning(
            parent,
            "Format SQL",
            f"The SQL could not be reformatted.\n\nReason: {reason}",
        )
        return None
    if formatted.strip() == original.strip():
        QMessageBox.information(
            parent,
            "Format SQL",
            "The SQL is already formatted; nothing needed to change.",
        )
        return None
    editor.setPlainText(formatted)
    return formatted


def _try_format_sqlglot(sql: str, *, read: str | None = None) -> str:
    formatted, _reason = _try_format_sqlglot_with_reason(sql, read=read)
    return formatted


def _try_format_sqlglot_with_reason(sql: str, *, read: str | None = None) -> tuple[str, str]:
    try:
        import sqlglot

        kwargs = {"pretty": True}
        if read:
            kwargs["read"] = read
        formatted = sqlglot.transpile(sql, **kwargs)
        if formatted and str(formatted[0]).strip():
            return str(formatted[0]).strip(), ""
        return "", "The SQL parser produced no formatted output."
    except Exception as exc:
        message = re.sub(r"\x1b\[[0-9;]*m", "", str(exc))
        return "", f"{type(exc).__name__}: {message}"


def _format_unload_sql(sql: str) -> str:
    payload = _extract_unload_sql_payload(sql)
    if payload is None:
        return _normalize_crlf(sql.replace("''", "'"))
    inner_sql, open_quote, close_quote = payload
    inner_sql = _decode_sql_display_escapes(inner_sql.replace("''", "'")).strip()
    formatted_inner = _try_format_sqlglot(inner_sql, read="redshift") or _try_format_sqlglot(inner_sql) or inner_sql
    prefix = sql[: open_quote + 1].rstrip()
    suffix = sql[close_quote:].lstrip()
    display = f"{prefix}\n{formatted_inner}\n{suffix}"
    return _normalize_crlf(display)


def _extract_wrapped_command_inner_sql(sql: str) -> str:
    command = _first_sql_word(sql)
    if command not in {"copy", "unload"}:
        return ""
    if command == "unload":
        payload = _extract_unload_sql_payload(sql)
        if payload is not None:
            inner = _decode_sql_display_escapes(payload[0].replace("''", "'")).strip()
            if _looks_like_inner_query(inner):
                return inner
    for candidate in _iter_sql_string_literals(sql):
        inner = _decode_sql_display_escapes(candidate).strip()
        if _looks_like_inner_query(inner):
            return inner
    parenthesized = _extract_parenthesized_inner_query(sql)
    return parenthesized if _looks_like_inner_query(parenthesized) else ""


def _iter_sql_string_literals(sql: str):
    i = 0
    while i < len(sql):
        if sql[i] != "'":
            i += 1
            continue
        chars: list[str] = []
        i += 1
        while i < len(sql):
            char = sql[i]
            if char == "'":
                if i + 1 < len(sql) and sql[i + 1] == "'":
                    chars.append("'")
                    i += 2
                    continue
                i += 1
                yield "".join(chars)
                break
            chars.append(char)
            i += 1
    return


def _extract_parenthesized_inner_query(sql: str) -> str:
    i = 0
    while i < len(sql):
        char = sql[i]
        if char == "'":
            i = _skip_sql_string_literal(sql, i)
            continue
        if char != "(":
            i += 1
            continue
        close = _matching_sql_paren(sql, i)
        if close > i:
            inner = sql[i + 1 : close].strip()
            if _looks_like_inner_query(inner):
                return inner
            i = close + 1
            continue
        i += 1
    return ""


def _matching_sql_paren(sql: str, open_index: int) -> int:
    depth = 0
    i = open_index
    while i < len(sql):
        char = sql[i]
        if char == "'":
            i = _skip_sql_string_literal(sql, i)
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def _skip_sql_string_literal(sql: str, quote_index: int) -> int:
    i = quote_index + 1
    while i < len(sql):
        if sql[i] == "'":
            if i + 1 < len(sql) and sql[i + 1] == "'":
                i += 2
                continue
            return i + 1
        i += 1
    return len(sql)


def _looks_like_inner_query(sql: object) -> bool:
    text = str(sql or "").lstrip()
    while True:
        block = re.match(r"/\*.*?\*/\s*", text, flags=re.DOTALL)
        if block:
            text = text[block.end() :].lstrip()
            continue
        line = re.match(r"--[^\n]*(?:\n|$)\s*", text)
        if line:
            text = text[line.end() :].lstrip()
            continue
        break
    return bool(re.match(r"(select|with)\b", text, flags=re.IGNORECASE))


def _extract_unload_sql_payload(sql: str) -> tuple[str, int, int] | None:
    open_paren = sql.find("(")
    if open_paren < 0:
        return None
    open_quote = sql.find("'", open_paren)
    if open_quote < 0:
        return None
    chars: list[str] = []
    i = open_quote + 1
    while i < len(sql):
        char = sql[i]
        if char == "'":
            if i + 1 < len(sql) and sql[i + 1] == "'":
                chars.append("''")
                i += 2
                continue
            return "".join(chars), open_quote, i
        chars.append(char)
        i += 1
    return None


def _decode_sql_display_escapes(sql: str) -> str:
    return (
        str(sql or "")
        .replace("\\r\\n", "\n")
        .replace("\\n", "\n")
        .replace("\\r", "\n")
        .replace("\\t", "\t")
    )


def _normalize_crlf(sql: str) -> str:
    text = str(sql or "").replace("\r\n", "\n").replace("\r", "\n")
    return text.replace("\n", "\r\n")


def _first_sql_word(sql: str) -> str:
    match = re.search(r"[A-Za-z_]+", str(sql or ""))
    return match.group(0).lower() if match else ""


def _is_numeric(value) -> bool:
    try:
        if pd.isna(value):
            return False
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def _cell_bg(col: str, val) -> str | None:
    try:
        n = float(val)
    except (TypeError, ValueError):
        return None
    if col == TABLE_SORTED_PCT_COL:
        return _traffic_heat_color(1.0 - max(0.0, min(100.0, n)) / 100.0)
    if col == "stats_off":
        return _traffic_heat_color(max(0.0, min(100.0, n)) / 100.0)
    if col == "skew_rows":
        return _traffic_heat_color(max(0.0, min(3.0, n)) / 3.0)
    if col == "sort_key_usage_score":
        return _traffic_heat_color(1.0 - max(0.0, min(100.0, n)) / 100.0)
    if col in {
        "severity_score",
        "repeat_priority_score",
        "risk_score",
        "table_risk_score",
        "impact_score",
        "statement_table_score",
        "full_scan_score",
        "distribution_usage_score",
        "sort_attention_score",
        "table_attention_score",
    }:
        return _traffic_heat_color(max(0.0, min(100.0, n)) / 100.0)
    if col == "query_count":
        if n >= 25:
            return _traffic_heat_color(1.0)
        if n >= 10:
            return _traffic_heat_color(0.65)
    if col in {"unsorted_pct", "stats_off"}:
        if n >= 50:
            return "#3A1B22"
        if n >= 20:
            return "#3A2B1B"
    return None


def _uses_light_heat_text(col: object) -> bool:
    return str(col) in HEAT_RANGE_COLUMNS or str(col) in {
        TABLE_SORTED_PCT_COL,
        "stats_off",
        "skew_rows",
        "sort_key_usage_score",
    }


def _traffic_heat_color(badness: float) -> str:
    badness = max(0.0, min(1.0, float(badness)))
    stops = (
        (0.0, (220, 252, 231)),
        (0.45, (254, 249, 195)),
        (0.72, (252, 211, 77)),
        (1.0, (252, 165, 165)),
    )
    for idx in range(len(stops) - 1):
        left_pos, left_color = stops[idx]
        right_pos, right_color = stops[idx + 1]
        if badness <= right_pos:
            span = max(right_pos - left_pos, 0.001)
            local = (badness - left_pos) / span
            rgb = tuple(
                int(round(left_color[channel] + (right_color[channel] - left_color[channel]) * local))
                for channel in range(3)
            )
            return "#{:02X}{:02X}{:02X}".format(*rgb)
    return "#FCA5A5"


def _heat_color(ratio: float) -> str | None:
    ratio = max(0.0, min(1.0, float(ratio)))
    stops = (
        (0.0, (19, 33, 55)),
        (0.35, (28, 56, 78)),
        (0.65, (58, 43, 27)),
        (1.0, (72, 25, 33)),
    )
    for idx in range(len(stops) - 1):
        left_pos, left_color = stops[idx]
        right_pos, right_color = stops[idx + 1]
        if ratio <= right_pos:
            span = max(right_pos - left_pos, 0.001)
            local = (ratio - left_pos) / span
            rgb = tuple(
                int(round(left_color[channel] + (right_color[channel] - left_color[channel]) * local))
                for channel in range(3)
            )
            return f"#{rgb[0]:02X}{rgb[1]:02X}{rgb[2]:02X}"
    red = stops[-1][1]
    return f"#{red[0]:02X}{red[1]:02X}{red[2]:02X}"
