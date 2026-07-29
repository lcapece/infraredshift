"""Critical Table Insights: pick a scenario, read the evidence."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt, Signal
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPushButton,
    QSizePolicy,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from ..table_insights import SCENARIOS, InsightResult, run_scenario
from ..theme import PALETTE

# Columns whose value is a verdict rather than a measurement, so they get room
# to breathe instead of being clipped to a number's width.
_WIDE_COLUMNS = {"verdict", "proposed_sortkey", "proposed_distkey", "external_table_key"}


def _pretty(name: str) -> str:
    return str(name).replace("_", " ").title()


class _InsightModel(QAbstractTableModel):
    """Read-only view of a scenario's rows, formatted for reading."""

    def __init__(self, frame: pd.DataFrame, parent=None):
        super().__init__(parent)
        self._frame = frame if frame is not None else pd.DataFrame()

    def rowCount(self, parent=QModelIndex()) -> int:  # noqa: N802
        return 0 if parent.isValid() else len(self._frame)

    def columnCount(self, parent=QModelIndex()) -> int:  # noqa: N802
        return 0 if parent.isValid() else len(self._frame.columns)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid() or index.row() >= len(self._frame):
            return None
        column = self._frame.columns[index.column()]
        value = self._frame.iloc[index.row()][column]

        if role == Qt.DisplayRole:
            if value is None or (not isinstance(value, str) and pd.isna(value)):
                return "—"
            if isinstance(value, (int,)) and not isinstance(value, bool):
                return f"{value:,}"
            if isinstance(value, float):
                return f"{value:,.2f}" if value % 1 else f"{value:,.0f}"
            return str(value)

        if role == Qt.ToolTipRole:
            return str(value)

        # A fit score is the point of the sort-key scenario, so colour it
        # rather than making the reader compare numbers by eye.
        if role == Qt.ForegroundRole and column == "fit_score":
            score = pd.to_numeric(value, errors="coerce")
            if pd.isna(score):
                return None
            if score >= 100:
                return QColor(PALETTE.ok)
            if score >= 70:
                return QColor(PALETTE.warn)
            return QColor(PALETTE.crit)

        if role == Qt.TextAlignmentRole and column not in _WIDE_COLUMNS:
            if pd.api.types.is_numeric_dtype(self._frame[column]):
                return int(Qt.AlignRight | Qt.AlignVCenter)
        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):  # noqa: N802
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return _pretty(self._frame.columns[section])
        return super().headerData(section, orientation, role)


class TableInsightsPage(QWidget):
    """One dropdown, one answer, and the numbers behind it."""

    loadRequested = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._result: InsightResult | None = None
        self._groups = pd.DataFrame()
        self._tables = pd.DataFrame()
        self._external = pd.DataFrame()

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 10)
        root.setSpacing(8)

        header = QHBoxLayout()
        title = QLabel("CRITICAL TABLE INSIGHTS")
        title.setObjectName("SectionHeader")
        header.addWidget(title)
        header.addStretch(1)
        self._export = QPushButton("Export Markdown")
        self._export.setObjectName("Ghost")
        self._export.setToolTip(
            "Write this scenario to a Markdown document, most significant first."
        )
        self._export.clicked.connect(self._export_markdown)
        header.addWidget(self._export)
        reload_btn = QPushButton("Load Analysis")
        reload_btn.setObjectName("Primary")
        reload_btn.clicked.connect(lambda: self.loadRequested.emit("table_heatmap"))
        header.addWidget(reload_btn)
        root.addLayout(header)

        picker = QFrame()
        picker.setObjectName("CardSubtle")
        picker_lay = QHBoxLayout(picker)
        picker_lay.setContentsMargins(12, 8, 12, 8)
        picker_lay.setSpacing(10)
        picker_lay.addWidget(QLabel("Scenario"))
        self._scenario = QComboBox()
        for scenario in SCENARIOS:
            self._scenario.addItem(scenario.title, scenario.key)
        self._scenario.setMinimumWidth(320)
        self._scenario.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self._scenario.currentIndexChanged.connect(self._refresh)
        picker_lay.addWidget(self._scenario, 1)
        root.addWidget(picker)

        self._question = QLabel("")
        self._question.setStyleSheet("font-size:14px; font-weight:700;")
        self._question.setWordWrap(True)
        self._question.setMinimumWidth(0)
        self._question.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Preferred)
        root.addWidget(self._question)

        self._headline = QLabel("Load the analysis to assess table health.")
        self._headline.setWordWrap(True)
        self._headline.setMinimumWidth(0)
        self._headline.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Preferred)
        root.addWidget(self._headline)

        self._table = QTableView()
        self._table.setAlternatingRowColors(True)
        self._table.setSelectionBehavior(QTableView.SelectRows)
        self._table.setSortingEnabled(False)
        self._table.verticalHeader().setVisible(False)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        root.addWidget(self._table, 1)

        self._note = QLabel("")
        self._note.setObjectName("Caption")
        self._note.setWordWrap(True)
        self._note.setMinimumWidth(0)
        self._note.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Preferred)
        root.addWidget(self._note)

        self._refresh()

    # ---- data ------------------------------------------------------------

    def set_report(self, report) -> None:
        def frame(name: str) -> pd.DataFrame:
            value = getattr(report, name, None)
            return value.copy() if isinstance(value, pd.DataFrame) else pd.DataFrame()

        self._groups = frame("repeat_groups")
        self._tables = frame("table_heatmap")
        if self._tables.empty:
            self._tables = frame("table_review")
        # external_tables carries the Spectrum scan telemetry; the metadata
        # table is only the catalog and has no GB scanned.
        self._external = frame("external_tables")
        if self._external.empty:
            self._external = frame("external_table_metadata")
        self._refresh()

    def has_data(self) -> bool:
        return not (self._groups.empty and self._tables.empty and self._external.empty)

    def current_scenario_key(self) -> str:
        return str(self._scenario.currentData() or SCENARIOS[0].key)

    def _refresh(self) -> None:
        key = self.current_scenario_key()
        scenario = next((s for s in SCENARIOS if s.key == key), SCENARIOS[0])
        self._question.setText(scenario.question)
        try:
            result = run_scenario(
                key,
                groups=self._groups,
                table_review=self._tables,
                external_tables=self._external,
            )
        except Exception as exc:  # pragma: no cover - defensive
            self._result = None
            self._headline.setText(f"This scenario could not be run: {exc}")
            self._note.setText("")
            self._table.setModel(_InsightModel(pd.DataFrame()))
            return

        self._result = result
        self._headline.setText(result.headline)
        self._note.setText(result.note or scenario.detail)
        self._table.setModel(_InsightModel(result.rows))
        self._table.resizeColumnsToContents()
        for index, column in enumerate(result.rows.columns):
            if column in _WIDE_COLUMNS:
                self._table.setColumnWidth(index, 320)
        self._export.setEnabled(not result.rows.empty)

    # ---- export ----------------------------------------------------------

    def _export_markdown(self) -> None:
        if self._result is None or self._result.rows.empty:
            QMessageBox.information(
                self, "Export", "This scenario produced no rows to export."
            )
            return
        target, _filter = QFileDialog.getSaveFileName(
            self,
            "Export Table Insights",
            str(Path.home() / f"{self._result.scenario.key}.md"),
            "Markdown (*.md);;All files (*)",
        )
        if not target:
            return
        try:
            Path(target).write_text(self.markdown(), encoding="utf-8")
        except OSError as exc:
            QMessageBox.warning(self, "Export", f"Could not write the file:\n{exc}")
            return
        QMessageBox.information(self, "Export", f"Written to:\n{target}")

    def markdown(self) -> str:
        """The current scenario as a Markdown document."""
        if self._result is None:
            return ""
        result = self._result
        lines = [
            f"# {result.scenario.title}",
            "",
            f"**{result.scenario.question}**",
            "",
            result.headline,
            "",
        ]
        if result.note:
            lines += [f"_{result.note}_", ""]
        if not result.rows.empty:
            columns = list(result.rows.columns)
            lines.append("| " + " | ".join(_pretty(c) for c in columns) + " |")
            lines.append("|" + "|".join("---" for _ in columns) + "|")
            for _index, row in result.rows.iterrows():
                cells = []
                for column in columns:
                    value = row[column]
                    if value is None or (not isinstance(value, str) and pd.isna(value)):
                        cells.append("—")
                    elif isinstance(value, float):
                        cells.append(f"{value:,.2f}" if value % 1 else f"{value:,.0f}")
                    elif isinstance(value, int) and not isinstance(value, bool):
                        cells.append(f"{value:,}")
                    else:
                        cells.append(str(value).replace("|", "\\|"))
                lines.append("| " + " | ".join(cells) + " |")
            lines.append("")
        return "\n".join(lines)
