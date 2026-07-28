"""Deterministic query decomposition page for Single Query Analysis."""
from __future__ import annotations

import logging

import pandas as pd
import sqlglot
from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QCheckBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from ..cluster_analyze import ClusterReport
from ..query_decomposer import DecompositionResult, decompose_redshift_query


class QueryDecomposerPage(QWidget):
    useOneOffSqlRequested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._report: ClusterReport | None = None
        self._result: DecompositionResult | None = None

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)

        header = QHBoxLayout()
        title = QLabel("QUERY DECOMPOSER")
        title.setObjectName("SectionHeader")
        header.addWidget(title)
        header.addStretch(1)
        self._status = QLabel("Paste a standard Redshift query, then analyze its physical stages.")
        self._status.setObjectName("Caption")
        self._status.setWordWrap(True)
        header.addWidget(self._status)
        root.addLayout(header)

        explanation = QLabel(
            "Creates reviewable temp-table stages by tracing aliases, CTEs, subqueries, and views to physical tables; "
            "retaining downstream-live columns; pushing only provably safe filters; and using captured table, plan, and execution evidence. "
            "It never executes generated SQL."
        )
        explanation.setObjectName("Caption")
        explanation.setWordWrap(True)
        root.addWidget(explanation)

        controls = QHBoxLayout()
        use_one_off = QPushButton("Use One-Off SQL")
        use_one_off.setObjectName("Ghost")
        use_one_off.clicked.connect(self.useOneOffSqlRequested.emit)
        controls.addWidget(use_one_off)
        format_btn = QPushButton("Format SQL")
        format_btn.setObjectName("Ghost")
        format_btn.clicked.connect(self._format_sql)
        controls.addWidget(format_btn)
        controls.addWidget(QLabel("Minimum Rows"))
        self._minimum_rows = QSpinBox()
        self._minimum_rows.setRange(0, 2_000_000_000)
        self._minimum_rows.setSingleStep(1_000_000)
        self._minimum_rows.setValue(1_000_000)
        self._minimum_rows.setGroupSeparatorShown(True)
        controls.addWidget(self._minimum_rows)
        controls.addWidget(QLabel("Minimum Size MB"))
        self._minimum_size = QSpinBox()
        self._minimum_size.setRange(0, 10_000_000)
        self._minimum_size.setSingleStep(1024)
        self._minimum_size.setValue(1024)
        self._minimum_size.setGroupSeparatorShown(True)
        controls.addWidget(self._minimum_size)
        controls.addWidget(QLabel("Captured Query ID (optional)"))
        self._query_id = QLineEdit()
        self._query_id.setPlaceholderText("Auto-match SQL or enter query ID")
        self._query_id.setMaximumWidth(220)
        controls.addWidget(self._query_id)
        controls.addStretch(1)
        analyze = QPushButton("Analyze & Decompose")
        analyze.setObjectName("Primary")
        analyze.clicked.connect(self._analyze)
        controls.addWidget(analyze)
        root.addLayout(controls)

        self._sql = QPlainTextEdit()
        self._sql.setObjectName("Mono")
        self._sql.setPlaceholderText("SELECT ...")
        self._sql.setMinimumHeight(170)
        root.addWidget(self._sql)

        self._tabs = QTabWidget()
        self._stage_table = QTableWidget()
        self._configure_table(self._stage_table)
        self._tabs.addTab(self._stage_table, "Decomposition Stages")
        self._finding_table = QTableWidget()
        self._configure_table(self._finding_table)
        self._tabs.addTab(self._finding_table, "Safety & Review")
        script_host = QWidget()
        script_layout = QVBoxLayout(script_host)
        script_layout.setContentsMargins(0, 0, 0, 0)
        script_actions = QHBoxLayout()
        copy_script = QPushButton("Copy Complete Script")
        copy_script.setObjectName("Primary")
        copy_script.clicked.connect(self._copy_script)
        script_actions.addStretch(1)
        script_actions.addWidget(copy_script)
        script_layout.addLayout(script_actions)
        self._script = QPlainTextEdit()
        self._script.setObjectName("Mono")
        self._script.setReadOnly(True)
        self._script.setPlaceholderText("Generated temp-table script will appear here.")
        script_layout.addWidget(self._script, 1)
        self._tabs.addTab(script_host, "Generated SQL")
        root.addWidget(self._tabs, 1)

    def set_cluster_report(self, report: ClusterReport | None) -> None:
        self._report = report
        if report is None:
            return
        self._status.setText(
            f"Context: {len(report.table_review):,} table rows, {len(report.view_definitions):,} views, "
            f"{len(report.query_explain):,} explain rows, {len(report.query_detail_flow):,} plan-linked execution rows."
        )

    def set_sql(self, sql: object) -> None:
        text = str(sql or "").strip()
        if text:
            self._sql.setPlainText(text)

    def sql_text(self) -> str:
        return self._sql.toPlainText().strip()

    def _analyze(self) -> None:
        sql = self.sql_text()
        if not sql:
            self._status.setText("Paste SQL or click Use One-Off SQL first.")
            return
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self.setEnabled(False)
        QApplication.processEvents()
        try:
            report = self._report
            tables = report.table_review if report is not None else pd.DataFrame()
            views = report.view_definitions if report is not None else pd.DataFrame()
            query_id = self._query_id.text().strip() or self._match_query_id(sql)
            explain = _rows_for_query(report.query_explain, query_id) if report is not None else pd.DataFrame()
            details = _rows_for_query(report.query_detail_flow, query_id) if report is not None else pd.DataFrame()
            self._result = decompose_redshift_query(
                sql,
                tables,
                views,
                explain,
                details,
                minimum_rows=self._minimum_rows.value(),
                minimum_size_mb=self._minimum_size.value(),
            )
            self._render_result(self._result, query_id)
        except Exception as exc:
            # A bare message here hid a pandas dtype crash for weeks: roughly one
            # query in six aborted, the status line said "failed safely", and it
            # read as "this query has no candidates" rather than "the analyzer
            # broke". Name the exception type and log the traceback so the next
            # failure is diagnosable from the error log instead of invisible.
            logging.getLogger(__name__).exception(
                "Decomposition failed for query_id=%s", self._query_id.text().strip() or "?"
            )
            self._status.setText(
                f"Decomposition FAILED ({type(exc).__name__}: {exc}). "
                "This is an analyzer error, not a verdict on the query - see the error log."
            )
        finally:
            self.setEnabled(True)
            QApplication.restoreOverrideCursor()

    def _render_result(self, result: DecompositionResult, query_id: str) -> None:
        _fill_table(self._stage_table, result.stages, [
            "stage_no", "stage_name", "stage_type", "physical_table", "source_rows", "source_size_mb",
            "reference_count", "required_column_count", "required_columns", "pushed_predicates",
            "distkey", "sortkey", "design_reason", "plan_scan_nodes", "plan_estimated_rows",
            "plan_estimated_width", "plan_max_cost", "actual_scan_rows", "actual_output_rows",
            "actual_scan_bytes", "actual_scan_duration_s", "actual_remote_read_blocks", "actual_spill_blocks", "safety",
        ])
        _fill_table(self._finding_table, result.findings, ["level", "title", "detail"])
        self._script.setPlainText(result.generated_sql)
        plan_note = (
            f" Captured query {query_id}: {len(_rows_for_query(self._report.query_explain, query_id)):,} plan row(s), "
            f"{len(_rows_for_query(self._report.query_detail_flow, query_id)):,} execution row(s)."
            if query_id and self._report is not None
            else " No captured query ID matched; decomposition used SQL and table metadata only."
        )
        self._status.setText(
            f"Generated {len(result.stages):,} safe/reviewable temp stage(s).{plan_note} "
            "Nothing was executed. Review Safety & Review before copying the script."
        )
        self._tabs.setCurrentIndex(0 if not result.stages.empty else 1)

    def _match_query_id(self, sql: str) -> str:
        report = self._report
        if report is None or report.slow_queries.empty or "sql_text" not in report.slow_queries.columns:
            return ""
        wanted = _normalized_sql(sql)
        for _, row in report.slow_queries.iterrows():
            if _normalized_sql(row.get("sql_text")) == wanted:
                return str(row.get("query_id") or "").strip()
        return ""

    def _format_sql(self) -> None:
        sql = self.sql_text()
        if not sql:
            return
        try:
            self._sql.setPlainText(sqlglot.parse_one(sql, read="redshift").sql(dialect="redshift", pretty=True))
        except Exception as exc:
            self._status.setText(f"Format failed: {exc}")

    def _copy_script(self) -> None:
        script = self._script.toPlainText()
        if script:
            QApplication.clipboard().setText(script)
            self._status.setText("Complete decomposition script copied. Nothing was executed.")

    @staticmethod
    def _configure_table(table: QTableWidget) -> None:
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setAlternatingRowColors(True)
        table.setSortingEnabled(True)
        table.verticalHeader().setVisible(False)


def _fill_table(table: QTableWidget, frame: pd.DataFrame, preferred: list[str]) -> None:
    table.setSortingEnabled(False)
    table.clear()
    if frame is None or frame.empty:
        table.setRowCount(0)
        table.setColumnCount(0)
        table.setSortingEnabled(True)
        return
    columns = [column for column in preferred if column in frame.columns] or list(frame.columns)
    table.setColumnCount(len(columns))
    table.setHorizontalHeaderLabels([column.replace("_", " ").title() for column in columns])
    table.setRowCount(len(frame))
    for row_no, (_index, row) in enumerate(frame.reset_index(drop=True).iterrows()):
        for col_no, column in enumerate(columns):
            value = row.get(column)
            item = QTableWidgetItem("" if pd.isna(value) else str(value))
            try:
                number = float(value)
                if not pd.isna(number):
                    item.setData(Qt.EditRole, number)
            except (TypeError, ValueError):
                pass
            table.setItem(row_no, col_no, item)
    table.resizeColumnsToContents()
    table.setSortingEnabled(True)


def _rows_for_query(frame: pd.DataFrame | None, query_id: str) -> pd.DataFrame:
    if frame is None or frame.empty or not query_id or "query_id" not in frame.columns:
        return pd.DataFrame()
    wanted = _normalized_id(query_id)
    ids = frame["query_id"].map(_normalized_id)
    return frame.loc[ids == wanted].copy().reset_index(drop=True)


def _normalized_id(value: object) -> str:
    text = str(value or "").strip()
    try:
        number = float(text)
        return str(int(number)) if number.is_integer() else text
    except (TypeError, ValueError):
        return text


def _normalized_sql(value: object) -> str:
    return " ".join(str(value or "").strip().lower().split())
