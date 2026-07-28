"""Table health grid with conditional formatting."""
from __future__ import annotations

import pandas as pd
from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QAbstractItemView, QHBoxLayout, QHeaderView, QLabel, QTableView, QVBoxLayout, QWidget,
)

from ..theme import PALETTE


DISPLAY_COLS = [
    "schema", "table", "diststyle", "sortkey1", "tbl_rows", "size",
    "pct_used", "unsorted", "stats_off", "skew_rows", "risk_event",
]


class _TableModel(QAbstractTableModel):
    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self._df = df.reset_index(drop=True)

    def rowCount(self, parent=QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._df)

    def columnCount(self, parent=QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._df.columns)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return str(self._df.columns[section]).replace("_", " ").upper()
        return str(section + 1)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        col = self._df.columns[index.column()]
        v = self._df.iat[index.row(), index.column()]
        if role == Qt.DisplayRole:
            return _fmt(col, v)
        if role == Qt.TextAlignmentRole:
            if col in ("tbl_rows", "size", "pct_used", "unsorted", "stats_off", "skew_rows"):
                return int(Qt.AlignRight | Qt.AlignVCenter)
            return int(Qt.AlignLeft | Qt.AlignVCenter)
        if role == Qt.BackgroundRole:
            c = _severity_bg(col, v)
            if c is not None:
                return QColor(c)
        if role == Qt.ForegroundRole:
            c = _severity_fg(col, v)
            if c is not None:
                return QColor(c)
        return None

    def sort(self, column: int, order: Qt.SortOrder = Qt.AscendingOrder) -> None:
        if column < 0 or column >= len(self._df.columns):
            return
        self.layoutAboutToBeChanged.emit()
        col = self._df.columns[column]
        ascending = order == Qt.AscendingOrder
        sort_values = pd.to_numeric(self._df[col], errors="coerce")
        if sort_values.notna().any():
            self._df = (
                self._df.assign(_sort_key=sort_values)
                .sort_values("_sort_key", ascending=ascending, na_position="last")
                .drop(columns="_sort_key")
                .reset_index(drop=True)
            )
        else:
            self._df = self._df.sort_values(col, ascending=ascending, na_position="last").reset_index(drop=True)
        self.layoutChanged.emit()


def _fmt(col: str, v) -> str:
    if pd.isna(v):
        return "—"
    if col in ("tbl_rows", "size", "empty", "estimated_visible_rows"):
        try:
            return f"{int(v):,}"
        except (ValueError, TypeError):
            return str(v)
    if col in ("pct_used", "unsorted", "stats_off"):
        try:
            return f"{float(v):.1f}%"
        except (ValueError, TypeError):
            return str(v)
    if col == "skew_rows":
        try:
            return f"×{float(v):.1f}"
        except (ValueError, TypeError):
            return str(v)
    return str(v)


def _severity_bg(col: str, v) -> str | None:
    try:
        if col == "unsorted" and float(v) >= 50:
            return "#3A1B22"
        if col == "stats_off" and float(v) >= 25:
            return "#3A1B22"
        if col == "skew_rows" and float(v) >= 5:
            return "#3A1B22"
        if col == "unsorted" and float(v) >= 20:
            return "#3A2B1B"
        if col == "stats_off" and float(v) >= 10:
            return "#3A2B1B"
        if col == "skew_rows" and float(v) >= 3:
            return "#3A2B1B"
    except (TypeError, ValueError):
        pass
    return None


def _severity_fg(col: str, v) -> str | None:
    try:
        if col == "unsorted" and float(v) >= 50:
            return PALETTE.crit
        if col == "stats_off" and float(v) >= 25:
            return PALETTE.crit
        if col == "skew_rows" and float(v) >= 5:
            return PALETTE.crit
        if col == "unsorted" and float(v) >= 20:
            return PALETTE.warn
        if col == "stats_off" and float(v) >= 10:
            return PALETTE.warn
        if col == "skew_rows" and float(v) >= 3:
            return PALETTE.warn
    except (TypeError, ValueError):
        pass
    return None


class TableDiagnostics(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(16, 16, 16, 16)
        lay.setSpacing(8)

        header = QLabel("TABLE HEALTH")
        header.setObjectName("SectionHeader")
        lay.addWidget(header)

        self._view = QTableView()
        self._view.setAlternatingRowColors(True)
        self._view.setSortingEnabled(True)
        self._view.verticalHeader().setVisible(False)
        self._view.horizontalHeader().setHighlightSections(False)
        self._view.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self._view.horizontalHeader().setStretchLastSection(False)
        self._view.setSelectionBehavior(QTableView.SelectRows)
        self._view.setSelectionMode(QAbstractItemView.SingleSelection)
        self._view.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self._view.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self._view.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        self._view.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        lay.addWidget(self._view, 1)

    def set_dataframe(self, df) -> None:
        if df is None or df.empty:
            self._view.setModel(None)
            return
        cols = [c for c in DISPLAY_COLS if c in df.columns]
        if not cols:
            cols = list(df.columns)
        self._view.setModel(_TableModel(df[cols]))
