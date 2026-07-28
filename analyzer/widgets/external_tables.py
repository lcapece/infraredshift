"""Spectrum metrics, health heat map, and engineer-review optimization queue."""
from __future__ import annotations

import math

import pandas as pd
from PySide6.QtCore import QAbstractTableModel, QEvent, QModelIndex, QRect, QSize, Qt, QTimer, Signal
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QPlainTextEdit,
    QScrollArea,
    QSpinBox,
    QTabWidget,
    QTableView,
    QToolTip,
    QVBoxLayout,
    QWidget,
)

from ..spectrum_optimizer import assess_spectrum_tables
from ..theme import PALETTE


GRID_COLUMNS: tuple[tuple[str, str], ...] = (
    ("cluster_name", "Cluster"),
    ("namespace_id", "Namespace ID"),
    ("external_table_key", "External Table"),
    ("s3_location", "S3 Location / Example File"),
    ("partition_key_columns", "Partition Key"),
    ("query_count", "Queries"),
    ("gross_scan_gb", "Scanned GB"),
    ("gross_scan_rows", "Scanned Rows"),
    ("partition_pruning_pct", "Partition Pruning %"),
    ("total_partitions_considered", "Partitions Considered"),
    ("qualified_partitions_scanned", "Partitions Scanned"),
    ("scanned_files", "Files Scanned"),
    ("avg_files_per_segment", "Avg Files / Segment"),
    ("external_duration_s", "Total External Runtime"),
    ("avg_external_duration_s", "Avg External Runtime"),
    ("s3list_time_ms", "S3 List Time (ms)"),
    ("get_partition_time_total_raw", "Partition Lookup (raw)"),
    ("warning_event_count", "Warnings"),
    ("recursive_scan_count", "Recursive Scans"),
    ("nested_scan_count", "Nested Scans"),
    ("observed_file_format", "File Format"),
    ("gross_output_gb", "Output GB"),
    ("row_filter_efficiency_pct", "Row Filtering %"),
    ("filtering_assessment", "Filtering Assessment"),
    ("sampled_error_count", "Sampled Errors"),
    ("external_spill_blocks", "Spill Blocks"),
    ("observation_start_time", "First Observed"),
    ("observation_end_time", "Last Observed"),
)

ACTION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("optimization_priority", "Priority"),
    ("optimization_score", "Score"),
    ("cluster_name", "Cluster"),
    ("namespace_id", "Namespace ID"),
    ("external_table_key", "External Table"),
    ("primary_recommendation", "Primary Recommendation"),
    ("recommendation_count", "Actions"),
    ("gross_scan_gb", "Scanned GB"),
    ("query_count", "Queries"),
    ("partition_pruning_pct", "Partition Pruning %"),
    ("observed_file_format", "File Format"),
    ("partition_key_columns", "Partition Key"),
    ("external_duration_s", "External Runtime"),
    ("recommendation_confidence", "Confidence"),
)

NUMERIC_COLUMNS = {
    key for key, _ in GRID_COLUMNS
    if key not in {
        "cluster_name", "namespace_id", "external_table_key", "table_name", "s3_location", "partition_key_columns", "observed_file_format", "filtering_assessment",
        "observation_start_time", "observation_end_time",
    }
}
NUMERIC_COLUMNS.update({"optimization_score", "recommendation_count"})

HEAT_MODES: tuple[tuple[str, str], ...] = (
    ("optimization", "Optimization Priority"),
    ("composite", "Composite — Pruning + Warnings"),
    ("partition", "Partition Pruning"),
    ("scan", "Scan Volume"),
    ("runtime", "External Runtime"),
    ("files", "File Pressure"),
    ("warnings", "Warnings"),
)


def _number(value: object, default: float = 0.0) -> float:
    try:
        result = float(value)
        return default if math.isnan(result) else result
    except (TypeError, ValueError):
        return default


def _display(value: object, column: str) -> str:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return "—"
    if column in {"gross_scan_gb", "gross_output_gb"}:
        return f"{_number(value):,.3f}"
    if column.endswith("_pct") or column.endswith("_pct_estimate"):
        return f"{_number(value):,.1f}%"
    if column in {"external_duration_s", "avg_external_duration_s"}:
        seconds = _number(value)
        if seconds >= 3600:
            return f"{seconds / 3600:,.2f} h"
        if seconds >= 60:
            return f"{seconds / 60:,.2f} min"
        return f"{seconds:,.2f} s"
    if column in NUMERIC_COLUMNS:
        number = _number(value)
        return f"{number:,.2f}" if not number.is_integer() else f"{number:,.0f}"
    return str(value)


def _partition_severity(row: pd.Series) -> int:
    total = _number(row.get("total_partitions_considered"))
    if total <= 0:
        return 3
    pruning = _number(row.get("partition_pruning_pct"), -1)
    if pruning >= 90:
        return 0
    if pruning >= 50:
        return 1
    return 2


def _warning_severity(row: pd.Series) -> int:
    warnings = _number(row.get("warning_event_count"))
    return 0 if warnings <= 0 else 1 if warnings <= 5 else 2


def _metric_severity(row: pd.Series, mode: str) -> int:
    if mode == "optimization":
        if _number(row.get("query_count")) <= 0:
            return 3
        score = _number(row.get("optimization_score"))
        return 0 if score < 20 else 1 if score < 45 else 2
    if mode == "partition":
        return _partition_severity(row)
    if mode == "warnings":
        return _warning_severity(row)
    if mode == "scan":
        scan_gb = _number(row.get("gross_scan_gb"))
        return 0 if scan_gb < 10 else 1 if scan_gb < 100 else 2
    if mode == "runtime":
        seconds = _number(row.get("external_duration_s"))
        return 0 if seconds < 600 else 1 if seconds < 3600 else 2
    if mode == "files":
        files = _number(row.get("avg_files_per_segment"))
        return 0 if files <= 100 else 1 if files <= 1000 else 2
    return max(_partition_severity(row), _warning_severity(row))


def _severity_color(severity: int) -> QColor:
    return QColor({0: "#2EAD68", 1: "#F5A623", 2: "#D94B4B"}.get(severity, "#7A8494"))


def _filter_external_rows(
    frame: pd.DataFrame,
    *,
    search: str = "",
    min_scan_gb: float = 0.0,
    min_queries: int = 1,
    warnings_only: bool = False,
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=list(frame.columns) if frame is not None else [])
    out = frame.copy()
    for column in NUMERIC_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    out = out[out.get("gross_scan_gb", pd.Series(0, index=out.index)).fillna(0) >= min_scan_gb]
    out = out[out.get("query_count", pd.Series(0, index=out.index)).fillna(0) >= min_queries]
    if warnings_only:
        out = out[out.get("warning_event_count", pd.Series(0, index=out.index)).fillna(0) > 0]
    needle = str(search or "").strip().lower()
    if needle:
        haystack = (
            out.get("external_table_key", pd.Series("", index=out.index)).fillna("").astype(str)
            + " " + out.get("cluster_name", pd.Series("", index=out.index)).fillna("").astype(str)
            + " " + out.get("namespace_id", pd.Series("", index=out.index)).fillna("").astype(str)
            + " " + out.get("table_name", pd.Series("", index=out.index)).fillna("").astype(str)
            + " " + out.get("s3_location", pd.Series("", index=out.index)).fillna("").astype(str)
        ).str.lower()
        out = out[haystack.str.contains(needle, regex=False)]
    sort_cols = [column for column in ("gross_scan_gb", "query_count") if column in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, ascending=[False] * len(sort_cols), na_position="last")
    return out.reset_index(drop=True)


def _filter_optimization_rows(
    frame: pd.DataFrame,
    *,
    focus: str = "all",
    actionable_only: bool = True,
) -> pd.DataFrame:
    """Filter the recommendation queue without discarding the shared grid filters."""
    if frame is None or frame.empty:
        return pd.DataFrame(columns=list(frame.columns) if frame is not None else [])
    out = frame.copy()
    if actionable_only:
        actionable = out.get("optimization_actionable", pd.Series(False, index=out.index))
        out = out[actionable.fillna(False).astype(bool)]
    code = out.get("recommendation_codes", pd.Series("", index=out.index)).fillna("").astype(str)
    priority = out.get("optimization_priority", pd.Series("", index=out.index)).fillna("").astype(str)
    focus = str(focus or "all").lower()
    if focus == "critical_high":
        out = out[priority.isin(("Critical", "High"))]
    elif focus == "partition":
        out = out[code.str.contains("PARTITION", regex=False)]
    elif focus == "format":
        out = out[code.str.contains("COLUMNAR_FORMAT", regex=False)]
    elif focus == "layout":
        out = out[code.str.contains("FILE_LAYOUT", regex=False)]
    elif focus == "statistics":
        out = out[code.str.contains("EXTERNAL_STATISTICS", regex=False)]
    elif focus == "pushdown":
        out = out[code.str.contains("PUSHDOWN", regex=False)]
    elif focus == "materialize":
        out = out[code.str.contains("MATERIALIZE_OR_STAGE|LOCAL_STAGE_DESIGN", regex=True)]
    elif focus == "quality":
        out = out[code.str.contains("DATA_QUALITY", regex=False)]
    sort_columns = [name for name in ("optimization_score", "gross_scan_gb", "query_count") if name in out.columns]
    if sort_columns:
        out = out.sort_values(sort_columns, ascending=[False] * len(sort_columns), na_position="last")
    return out.reset_index(drop=True)


class _ExternalTableModel(QAbstractTableModel):
    def __init__(self, parent=None, columns: tuple[tuple[str, str], ...] | None = None):
        super().__init__(parent)
        self._frame = pd.DataFrame()
        self._columns = list(columns or GRID_COLUMNS)

    def set_frame(self, frame: pd.DataFrame) -> None:
        self.beginResetModel()
        self._frame = frame.copy() if frame is not None else pd.DataFrame()
        self.endResetModel()

    def rowCount(self, parent=QModelIndex()) -> int:  # noqa: N802
        return 0 if parent.isValid() else len(self._frame)

    def columnCount(self, parent=QModelIndex()) -> int:  # noqa: N802
        return 0 if parent.isValid() else len(self._columns)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid() or index.row() >= len(self._frame):
            return None
        column = self._columns[index.column()][0]
        value = self._frame.iloc[index.row()].get(column)
        if role == Qt.DisplayRole:
            return _display(value, column)
        if role == Qt.ToolTipRole:
            if "primary_recommendation" in self._frame.columns:
                row = self._frame.iloc[index.row()]
                return "\n".join((
                    _display(value, column),
                    str(row.get("optimization_priority") or "") + ": "
                    + str(row.get("primary_recommendation") or ""),
                    str(row.get("optimization_evidence") or ""),
                )).strip()
            return _display(value, column)
        if role == Qt.ForegroundRole and column == "optimization_priority":
            value = str(value or "")
            return QColor({"Critical": "#D94B4B", "High": "#C86D15", "Medium": "#B27700", "Healthy": "#2EAD68"}.get(value, PALETTE.text_1))
        if role == Qt.TextAlignmentRole and column in NUMERIC_COLUMNS:
            return int(Qt.AlignRight | Qt.AlignVCenter)
        return None

    def row_at(self, row: int) -> pd.Series | None:
        if not 0 <= row < len(self._frame):
            return None
        return self._frame.iloc[row]

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):  # noqa: N802
        if role == Qt.DisplayRole and orientation == Qt.Horizontal and section < len(self._columns):
            return self._columns[section][1]
        return super().headerData(section, orientation, role)

    def sort(self, column: int, order: Qt.SortOrder = Qt.AscendingOrder) -> None:
        if self._frame.empty or not 0 <= column < len(self._columns):
            return
        name = self._columns[column][0]
        if name not in self._frame.columns:
            return
        self.layoutAboutToBeChanged.emit()
        work = self._frame.copy()
        if name in NUMERIC_COLUMNS:
            work["__sort"] = pd.to_numeric(work[name], errors="coerce")
        else:
            work["__sort"] = work[name].fillna("").astype(str).str.lower()
        self._frame = work.sort_values(
            "__sort", ascending=order == Qt.AscendingOrder, na_position="last", kind="stable"
        ).drop(columns="__sort").reset_index(drop=True)
        self.layoutChanged.emit()


class _ExternalHeatCanvas(QWidget):
    SQUARE = 26
    GAP = 4

    def __init__(self, parent=None):
        super().__init__(parent)
        self._frame = pd.DataFrame()
        self._mode = "composite"
        self._items: list[tuple[QRect, pd.Series]] = []
        self.setMouseTracking(True)

    def set_data(self, frame: pd.DataFrame, mode: str, width: int) -> None:
        self._frame = frame.copy() if frame is not None else pd.DataFrame()
        self._mode = mode
        pitch = self.SQUARE + self.GAP
        columns = max(1, (max(600, width) - 28) // pitch)
        self._items = []
        for offset, (_, row) in enumerate(self._frame.iterrows()):
            x = 12 + (offset % columns) * pitch
            y = 12 + (offset // columns) * pitch
            self._items.append((QRect(x, y, self.SQUARE, self.SQUARE), row))
        rows = max(1, math.ceil(len(self._frame) / columns))
        self.resize(max(600, width), max(120, 24 + rows * pitch))
        self.update()

    def sizeHint(self) -> QSize:
        return QSize(max(600, self.width()), max(120, self.height()))

    def paintEvent(self, event) -> None:
        painter = QPainter(self)
        painter.fillRect(self.rect(), self.palette().base())
        painter.setPen(QPen(QColor(PALETTE.bg_0), 1))
        for rect, row in self._items:
            if self._mode == "composite":
                half = rect.height() // 2
                painter.fillRect(QRect(rect.left(), rect.top(), rect.width(), half), _severity_color(_partition_severity(row)))
                painter.fillRect(QRect(rect.left(), rect.top() + half, rect.width(), rect.height() - half), _severity_color(_warning_severity(row)))
                painter.drawLine(rect.left(), rect.center().y(), rect.right(), rect.center().y())
            else:
                painter.fillRect(rect, _severity_color(_metric_severity(row, self._mode)))
            painter.drawRect(rect)

    def mouseMoveEvent(self, event) -> None:
        pos = event.position().toPoint()
        for rect, row in self._items:
            if rect.contains(pos):
                QToolTip.showText(event.globalPosition().toPoint(), self._tooltip(row), self)
                return
        QToolTip.hideText()

    @staticmethod
    def _tooltip(row: pd.Series) -> str:
        return "\n".join((
            f"Namespace: {row.get('namespace_id') or 'producer'}",
            str(row.get("external_table_key") or row.get("table_name") or "External table"),
            f"Scanned: {_number(row.get('gross_scan_gb')):,.3f} GB / {_number(row.get('gross_scan_rows')):,.0f} rows",
            f"Queries: {_number(row.get('query_count')):,.0f}",
            f"Partition pruning: {_display(row.get('partition_pruning_pct'), 'partition_pruning_pct')}",
            f"Files: {_number(row.get('scanned_files')):,.0f} total; {_number(row.get('avg_files_per_segment')):,.1f} average/segment",
            f"External runtime: {_display(row.get('external_duration_s'), 'external_duration_s')}",
            f"Warnings: {_number(row.get('warning_event_count')):,.0f}",
            f"Optimization: {row.get('optimization_priority') or 'Not assessed'} — {row.get('primary_recommendation') or 'monitor'}",
            f"S3: {row.get('s3_location') or 'not captured'}",
        ))


def _swatch(color: str, text: str) -> QWidget:
    host = QWidget()
    layout = QHBoxLayout(host)
    layout.setContentsMargins(0, 0, 0, 0)
    box = QLabel()
    box.setFixedSize(15, 15)
    box.setStyleSheet(f"background:{color}; border:1px solid {PALETTE.bg_0};")
    layout.addWidget(box)
    layout.addWidget(QLabel(text))
    return host


class ExternalTablesPage(QWidget):
    loadRequested = Signal(str)
    loaderRequested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._frame = pd.DataFrame()
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 10)
        header = QHBoxLayout()
        title = QLabel("EXTERNAL TABLE ACTIVITY")
        title.setObjectName("SectionHeader")
        header.addWidget(title)
        self._status = QLabel("Open this tab to load summarized external-table activity.")
        self._status.setObjectName("Caption")
        header.addStretch(1)
        header.addWidget(self._status)
        loader_btn = QPushButton("Open Data Loader")
        loader_btn.setObjectName("Primary")
        loader_btn.setToolTip(
            "External table metadata from SVV_EXTERNAL_COLUMNS is included in "
            "the normal staged load and promoted with every other dataset."
        )
        loader_btn.clicked.connect(self.loaderRequested.emit)
        header.addWidget(loader_btn)
        root.addLayout(header)

        filters = QFrame()
        filters.setObjectName("CardSubtle")
        filter_bar = QHBoxLayout(filters)
        filter_bar.addWidget(QLabel("Find table or S3 path"))
        self._search = QLineEdit()
        self._search.setPlaceholderText("table, schema, bucket, or prefix")
        self._search.setClearButtonEnabled(True)
        filter_bar.addWidget(self._search, 2)
        filter_bar.addWidget(QLabel("Minimum scanned"))
        self._min_scan = QDoubleSpinBox()
        self._min_scan.setRange(0, 1_000_000_000)
        self._min_scan.setDecimals(2)
        self._min_scan.setSuffix(" GB")
        self._min_scan.setGroupSeparatorShown(True)
        filter_bar.addWidget(self._min_scan)
        filter_bar.addWidget(QLabel("Minimum queries"))
        self._min_queries = QSpinBox()
        self._min_queries.setRange(0, 1_000_000_000)
        self._min_queries.setValue(1)
        self._min_queries.setGroupSeparatorShown(True)
        filter_bar.addWidget(self._min_queries)
        self._warnings_only = QCheckBox("Warnings only")
        filter_bar.addWidget(self._warnings_only)
        root.addWidget(filters)

        self._tabs = QTabWidget()
        self._grid = QTableView()
        self._grid.setAlternatingRowColors(True)
        self._grid.setSortingEnabled(True)
        self._grid.setWordWrap(False)
        self._grid.verticalHeader().setVisible(False)
        self._grid.horizontalHeader().setSectionsMovable(True)
        self._grid.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self._model = _ExternalTableModel(self)
        self._grid.setModel(self._model)
        self._grid.setColumnWidth(0, 240)
        self._grid.setColumnWidth(1, 260)
        self._grid.setColumnWidth(2, 420)
        self._tabs.addTab(self._grid, "Metrics Grid")

        heat_host = QWidget()
        heat_layout = QVBoxLayout(heat_host)
        heat_layout.setContentsMargins(8, 8, 8, 8)
        legend = QFrame()
        legend.setObjectName("CardSubtle")
        legend_layout = QHBoxLayout(legend)
        legend_layout.addWidget(QLabel("Composite: top = partition pruning; bottom = warnings."))
        legend_layout.addWidget(_swatch("#2EAD68", "Healthy / low"))
        legend_layout.addWidget(_swatch("#F5A623", "Review"))
        legend_layout.addWidget(_swatch("#D94B4B", "High concern"))
        legend_layout.addWidget(_swatch("#7A8494", "Not applicable"))
        legend_layout.addStretch(1)
        legend_layout.addWidget(QLabel("Focused view"))
        self._heat_mode = QComboBox()
        for key, label in HEAT_MODES:
            self._heat_mode.addItem(label, key)
        legend_layout.addWidget(self._heat_mode)
        heat_layout.addWidget(legend)
        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(False)
        self._scroll.viewport().installEventFilter(self)
        self._canvas = _ExternalHeatCanvas()
        self._scroll.setWidget(self._canvas)
        heat_layout.addWidget(self._scroll, 1)
        self._tabs.addTab(heat_host, "Heat Map")

        queue_host = QWidget()
        queue_layout = QVBoxLayout(queue_host)
        queue_layout.setContentsMargins(8, 8, 8, 8)
        queue_controls = QFrame()
        queue_controls.setObjectName("CardSubtle")
        queue_control_layout = QHBoxLayout(queue_controls)
        queue_control_layout.addWidget(QLabel("Recommendation focus"))
        self._action_focus = QComboBox()
        for label, key in (
            ("All recommendations", "all"),
            ("Critical and high priority", "critical_high"),
            ("Partition design and pruning", "partition"),
            ("File format", "format"),
            ("File sizing and fan-out", "layout"),
            ("External statistics", "statistics"),
            ("Predicate and column pushdown", "pushdown"),
            ("Materialize or locally stage", "materialize"),
            ("Warnings and data quality", "quality"),
        ):
            self._action_focus.addItem(label, key)
        queue_control_layout.addWidget(self._action_focus)
        self._actionable_only = QCheckBox("Actionable only")
        self._actionable_only.setChecked(True)
        queue_control_layout.addWidget(self._actionable_only)
        queue_control_layout.addStretch(1)
        self._action_count = QLabel()
        self._action_count.setObjectName("Caption")
        queue_control_layout.addWidget(self._action_count)
        queue_layout.addWidget(queue_controls)

        self._action_grid = QTableView()
        self._action_grid.setAlternatingRowColors(True)
        self._action_grid.setSortingEnabled(True)
        self._action_grid.setWordWrap(False)
        self._action_grid.verticalHeader().setVisible(False)
        self._action_grid.horizontalHeader().setSectionsMovable(True)
        self._action_grid.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self._action_model = _ExternalTableModel(self, ACTION_COLUMNS)
        self._action_grid.setModel(self._action_model)
        self._action_grid.setSelectionBehavior(QTableView.SelectRows)
        self._action_grid.setSelectionMode(QTableView.SingleSelection)
        self._action_grid.setColumnWidth(0, 90)
        self._action_grid.setColumnWidth(2, 200)
        self._action_grid.setColumnWidth(4, 320)
        self._action_grid.setColumnWidth(5, 420)
        queue_layout.addWidget(self._action_grid, 3)

        detail_bar = QHBoxLayout()
        detail_bar.addWidget(QLabel("Selected recommendation — evidence, next step, and review-only SQL"))
        detail_bar.addStretch(1)
        self._copy_review_sql = QPushButton("Copy Review SQL")
        self._copy_review_sql.setEnabled(False)
        detail_bar.addWidget(self._copy_review_sql)
        queue_layout.addLayout(detail_bar)
        self._action_detail = QPlainTextEdit()
        self._action_detail.setReadOnly(True)
        self._action_detail.setPlaceholderText("Select an external table to inspect the recommendation evidence.")
        self._action_detail.setMaximumBlockCount(500)
        queue_layout.addWidget(self._action_detail, 2)
        self._tabs.addTab(queue_host, "Optimization Queue")
        root.addWidget(self._tabs, 1)

        self._search.textChanged.connect(self._refresh)
        self._min_scan.valueChanged.connect(self._refresh)
        self._min_queries.valueChanged.connect(self._refresh)
        self._warnings_only.toggled.connect(self._refresh)
        self._heat_mode.currentIndexChanged.connect(self._refresh)
        self._action_focus.currentIndexChanged.connect(self._refresh)
        self._actionable_only.toggled.connect(self._refresh)
        self._action_grid.selectionModel().selectionChanged.connect(self._show_selected_action)
        self._copy_review_sql.clicked.connect(self._copy_selected_review_sql)

    def set_report(self, report) -> None:
        source = report.external_tables.copy() if report.external_tables is not None else pd.DataFrame()
        self._frame = assess_spectrum_tables(source)
        self._refresh()

    def has_data(self) -> bool:
        return not self._frame.empty

    def show_loading(self) -> None:
        self._status.setText("Loading summarized external-table activity from local DuckDB …")

    def _refresh(self) -> None:
        previously_selected = self._selected_action()
        previous_key = str(previously_selected.get("external_table_key") or "") if previously_selected is not None else ""
        filtered = _filter_external_rows(
            self._frame,
            search=self._search.text(),
            min_scan_gb=self._min_scan.value(),
            min_queries=self._min_queries.value(),
            warnings_only=self._warnings_only.isChecked(),
        )
        self._model.set_frame(filtered)
        actions = _filter_optimization_rows(
            filtered,
            focus=str(self._action_focus.currentData() or "all"),
            actionable_only=self._actionable_only.isChecked(),
        )
        self._action_model.set_frame(actions)
        self._action_count.setText(f"{len(actions):,} table recommendations")
        if not actions.empty:
            selected_row = 0
            if previous_key and "external_table_key" in actions.columns:
                matches = actions.index[actions["external_table_key"].fillna("").astype(str).eq(previous_key)].tolist()
                selected_row = int(matches[0]) if matches else 0
            self._action_grid.selectRow(selected_row)
            self._show_selected_action()
        else:
            self._action_detail.clear()
            self._copy_review_sql.setEnabled(False)
        mode = str(self._heat_mode.currentData() or "composite")
        self._canvas.set_data(filtered, mode, max(600, self._scroll.viewport().width() - 2))
        scan_gb = pd.to_numeric(filtered.get("gross_scan_gb", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
        priorities = filtered.get("optimization_priority", pd.Series(dtype=str)).fillna("").astype(str)
        urgent = int(priorities.isin(("Critical", "High")).sum())
        self._status.setText(
            f"{len(filtered):,} of {len(self._frame):,} active external tables; "
            f"{scan_gb:,.2f} GB scanned; {urgent:,} critical/high recommendations."
        )

    def _selected_action(self) -> pd.Series | None:
        rows = self._action_grid.selectionModel().selectedRows()
        return self._action_model.row_at(rows[0].row()) if rows else None

    def _show_selected_action(self, *_args) -> None:
        row = self._selected_action()
        if row is None:
            self._action_detail.clear()
            self._copy_review_sql.setEnabled(False)
            return
        table = row.get("external_table_key") or row.get("table_name") or "External table"
        review_sql = str(row.get("review_sql") or "").strip()
        detail = "\n\n".join(part for part in (
            f"{row.get('optimization_priority') or 'Unrated'} — {table}\n{row.get('primary_recommendation') or ''}",
            "EVIDENCE\n" + str(row.get("optimization_evidence") or "No supporting evidence captured."),
            "RECOMMENDED NEXT STEP\n" + str(row.get("suggested_next_step") or "Review the table with its data owner."),
            "ALL IDENTIFIED ACTIONS\n" + str(row.get("all_recommendations") or "No additional actions."),
            "REVIEW-ONLY SQL (never executed by Infraredshift)\n" + review_sql if review_sql else "",
        ) if part)
        self._action_detail.setPlainText(detail)
        self._copy_review_sql.setEnabled(bool(review_sql))

    def _copy_selected_review_sql(self) -> None:
        row = self._selected_action()
        review_sql = str(row.get("review_sql") or "").strip() if row is not None else ""
        if review_sql:
            QApplication.clipboard().setText(review_sql)

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        QTimer.singleShot(0, self._refresh)

    def eventFilter(self, watched, event) -> bool:
        if watched is self._scroll.viewport() and event.type() == QEvent.Resize:
            QTimer.singleShot(0, self._refresh)
        return super().eventFilter(watched, event)
