"""Findings card list."""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QFrame, QHBoxLayout, QLabel, QScrollArea, QVBoxLayout, QWidget,
)

from ..diagnose import Finding
from ..md_render import apply_markdown, code_block_html
from ..theme import PALETTE

_SQL_LEAD = ("alter ", "vacuum", "analyze", "create ", "select ", "update ", "insert ", "delete ", "set ", "unload", "copy ")


def _looks_like_code(text: str) -> bool:
    """A single-statement SQL/DDL recommendation reads as code; multi-sentence
    guidance reads as prose. Keep code boxed (mono) and prose as markdown."""
    body = str(text or "").strip().lower()
    if not body:
        return False
    if body.startswith(_SQL_LEAD):
        return True
    # Short one-liner ending in ';' with no sentence punctuation -> code.
    return body.endswith(";") and body.count(".") <= 1 and "\n" not in body


class _Chip(QLabel):
    def __init__(self, severity: str, text: str):
        super().__init__(text)
        self.setProperty("chip", True)
        self.setProperty("severity", severity)
        self.setAlignment(Qt.AlignCenter)


class _FindingCard(QFrame):
    def __init__(self, finding: Finding):
        super().__init__()
        self.setObjectName("Card")
        lay = QVBoxLayout(self)
        lay.setContentsMargins(16, 14, 16, 14)
        lay.setSpacing(8)

        head = QHBoxLayout()
        head.setSpacing(10)
        chip = _Chip(finding.severity, finding.severity.upper())
        head.addWidget(chip, 0)
        title = QLabel(finding.title)
        title.setObjectName("H1")
        title.setWordWrap(True)
        head.addWidget(title, 1)
        if finding.table:
            tag = QLabel(f"{finding.table}")
            tag.setObjectName("Mono")
            tag.setStyleSheet("color:#8691AD; font-size:11px;")
            head.addWidget(tag, 0)
        lay.addLayout(head)

        detail = QLabel()
        detail.setObjectName("Caption")
        apply_markdown(detail, finding.detail)
        lay.addWidget(detail)

        if finding.recommendation:
            rec = QFrame()
            rec.setObjectName("CardSubtle")
            rec_lay = QVBoxLayout(rec)
            rec_lay.setContentsMargins(12, 10, 12, 10)
            rec_lay.setSpacing(4)
            rec_head = QLabel("RECOMMENDATION")
            rec_head.setObjectName("SectionHeader")
            rec_lay.addWidget(rec_head)
            rec_body = QLabel()
            rec_body.setWordWrap(True)
            # A recommendation that reads as SQL/DDL is boxed as code; prose is
            # rendered as themed markdown.
            if _looks_like_code(finding.recommendation):
                rec_body.setTextFormat(Qt.RichText)
                rec_body.setText(code_block_html(finding.recommendation, accent=PALETTE.accent))
            else:
                apply_markdown(rec_body, finding.recommendation)
            rec_lay.addWidget(rec_body)
            lay.addWidget(rec)


class Recommendations(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(10)

        head = QLabel("FINDINGS")
        head.setObjectName("SectionHeader")
        root.addWidget(head)

        self._summary = QLabel("—")
        self._summary.setObjectName("Caption")
        root.addWidget(self._summary)

        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._scroll.setFrameShape(QFrame.NoFrame)
        self._host = QWidget()
        self._host_lay = QVBoxLayout(self._host)
        self._host_lay.setContentsMargins(0, 0, 0, 0)
        self._host_lay.setSpacing(10)
        self._host_lay.addStretch(1)
        self._scroll.setWidget(self._host)
        root.addWidget(self._scroll, 1)

    def set_findings(self, findings: list[Finding]) -> None:
        # Clear.
        while self._host_lay.count():
            item = self._host_lay.takeAt(0)
            w = item.widget()
            if w:
                w.deleteLater()

        crit = sum(1 for f in findings if f.severity == "crit")
        warn = sum(1 for f in findings if f.severity == "warn")
        info = sum(1 for f in findings if f.severity == "info")
        if not findings:
            self._summary.setText("No bottlenecks detected.")
        else:
            self._summary.setText(
                f"{crit} critical · {warn} warning · {info} info   —   sorted by impact"
            )

        for f in findings:
            self._host_lay.addWidget(_FindingCard(f))
        self._host_lay.addStretch(1)
