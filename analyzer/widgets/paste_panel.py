"""Two-pane paste input wired to PasteProvider."""
from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QFrame, QHBoxLayout, QLabel, QPlainTextEdit, QPushButton, QTabWidget,
    QVBoxLayout, QWidget,
)


class PastePanel(QWidget):
    analyzeRequested = Signal()
    cleared = Signal()

    def __init__(self, provider, parent=None):
        super().__init__(parent)
        self._provider = provider

        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        header = QLabel("DATA SOURCE")
        header.setObjectName("SectionHeader")
        root.addWidget(header)

        tabs = QTabWidget()
        self._qd_editor = self._make_editor(
            "Paste the output of:\n  SELECT * FROM sys_query_details WHERE query_id = <qid>;"
        )
        self._ti_editor = self._make_editor(
            "Paste the output of:\n  SELECT * FROM svv_table_info;"
        )
        tabs.addTab(self._wrap_editor(self._qd_editor, "query details"), "Query Details")
        tabs.addTab(self._wrap_editor(self._ti_editor, "table info"), "Table Info")
        root.addWidget(tabs, 1)

        self._status = QLabel("Waiting for paste…")
        self._status.setObjectName("Caption")
        root.addWidget(self._status)

        btn_row = QHBoxLayout()
        btn_row.setSpacing(8)
        self._clear = QPushButton("Clear")
        self._clear.setObjectName("Ghost")
        self._analyze = QPushButton("Analyze →")
        self._analyze.setObjectName("Primary")
        self._analyze.setEnabled(False)
        btn_row.addWidget(self._clear)
        btn_row.addStretch(1)
        btn_row.addWidget(self._analyze)
        root.addLayout(btn_row)

        self._qd_editor.textChanged.connect(self._on_changed)
        self._ti_editor.textChanged.connect(self._on_changed)
        self._clear.clicked.connect(self._on_clear)
        self._analyze.clicked.connect(self.analyzeRequested.emit)

    def _make_editor(self, placeholder: str) -> QPlainTextEdit:
        ed = QPlainTextEdit()
        ed.setPlaceholderText(placeholder)
        ed.setLineWrapMode(QPlainTextEdit.NoWrap)
        ed.setTabChangesFocus(False)
        return ed

    def _wrap_editor(self, ed: QPlainTextEdit, kind: str) -> QWidget:
        w = QFrame()
        w.setObjectName("CardSubtle")
        lay = QVBoxLayout(w)
        lay.setContentsMargins(10, 10, 10, 10)
        lay.setSpacing(6)
        hint = QLabel(f"Paste {kind} from DBeaver (TSV, CSV, or |-separated)")
        hint.setObjectName("Caption")
        lay.addWidget(hint)
        lay.addWidget(ed, 1)
        return w

    def _on_changed(self) -> None:
        self._provider.set_query_details(self._qd_editor.toPlainText())
        self._provider.set_table_info(self._ti_editor.toPlainText())
        ready = self._provider.is_ready()
        self._analyze.setEnabled(ready)
        if ready:
            qd_rows = self._qd_editor.toPlainText().count("\n")
            ti_rows = self._ti_editor.toPlainText().count("\n")
            self._status.setText(f"Ready · ~{qd_rows} detail rows · ~{ti_rows} table rows")
        else:
            needed = []
            if not self._qd_editor.toPlainText().strip():
                needed.append("query details")
            if not self._ti_editor.toPlainText().strip():
                needed.append("table info")
            self._status.setText("Waiting for " + " & ".join(needed) + "…")

    def _on_clear(self) -> None:
        self._qd_editor.clear()
        self._ti_editor.clear()
        self.cleared.emit()

    def show_error(self, msg: str) -> None:
        self._status.setText(f"Parse error: {msg}")

    def show_parse_notes(self, notes: tuple[str, ...] | list[str]) -> None:
        if not notes:
            return
        self._status.setText(" · ".join(notes))
