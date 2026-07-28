"""Horizontal bar timeline of step runtimes."""
from __future__ import annotations

from PySide6.QtCore import QRectF, Qt, Signal
from PySide6.QtGui import QBrush, QColor, QFont, QPainter, QPen
from PySide6.QtWidgets import QWidget

from ..diagnose import StepEnrichment
from ..theme import PALETTE, color_for_step


ROW_H = 26
LABEL_W = 160
PAD = 16


class StepTimeline(QWidget):
    stepClicked = Signal(object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._steps: list[StepEnrichment] = []
        self._total_ms: int = 0
        self._hover_idx: int | None = None
        self._selected_id: int | None = None
        self.setMouseTracking(True)
        self.setMinimumHeight(120)

    def set_data(self, steps: list[StepEnrichment], total_ms: int) -> None:
        ordered = sorted(steps, key=lambda s: (s.elapsed_ms or 0), reverse=True)
        self._steps = ordered[:25]
        self._total_ms = total_ms
        self.setMinimumHeight(PAD * 2 + max(1, len(self._steps)) * ROW_H)
        self.updateGeometry()
        self.update()

    def highlight_step(self, step_id: int | None) -> None:
        self._selected_id = step_id
        self.update()

    def _row_rect(self, i: int) -> QRectF:
        return QRectF(PAD, PAD + i * ROW_H, self.width() - 2 * PAD, ROW_H - 6)

    def paintEvent(self, _) -> None:
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        p.fillRect(self.rect(), QColor(PALETTE.bg_0))

        if not self._steps:
            p.setPen(QColor(PALETTE.text_3))
            p.setFont(QFont("Inter", 11))
            p.drawText(self.rect(), Qt.AlignCenter, "No step data yet.")
            return

        max_ms = max((s.elapsed_ms or 0) for s in self._steps) or 1
        for i, s in enumerate(self._steps):
            row = self._row_rect(i)
            label_box = QRectF(row.x(), row.y(), LABEL_W, row.height())
            bar_box = QRectF(row.x() + LABEL_W + 8, row.y(), row.width() - LABEL_W - 8, row.height())

            # Row hover / selected background.
            if self._hover_idx == i or self._selected_id == s.step_id:
                p.fillRect(QRectF(0, row.y() - 2, self.width(), row.height() + 4), QColor(PALETTE.bg_1))

            p.setPen(QColor(PALETTE.text_2))
            p.setFont(QFont("JetBrains Mono", 9))
            step_tag = f"#{s.step_id if s.step_id is not None else '—'}"
            p.drawText(label_box, Qt.AlignLeft | Qt.AlignVCenter, step_tag)

            p.setPen(QColor(PALETTE.text_1))
            p.setFont(QFont("Inter", 9, QFont.DemiBold))
            name = s.step_name or "—"
            if len(name) > 18:
                name = name[:17] + "…"
            p.drawText(label_box.adjusted(36, 0, 0, 0), Qt.AlignLeft | Qt.AlignVCenter, name)

            # Bar background.
            p.setPen(Qt.NoPen)
            p.setBrush(QColor(PALETTE.bg_2))
            p.drawRoundedRect(bar_box, 4, 4)

            # Bar fill.
            pct = (s.elapsed_ms or 0) / max_ms
            if pct > 0:
                fill = QRectF(bar_box.x(), bar_box.y(), bar_box.width() * pct, bar_box.height())
                color = QColor(color_for_step(s.step_name))
                p.setBrush(color)
                p.drawRoundedRect(fill, 4, 4)

            # Elapsed text at right.
            p.setPen(QColor(PALETTE.text_2))
            p.setFont(QFont("JetBrains Mono", 9))
            ms = s.elapsed_ms or 0
            label = f"{ms:,} ms"
            if self._total_ms:
                label += f"  ({ms / self._total_ms * 100:.0f}%)"
            p.drawText(bar_box.adjusted(8, 0, -8, 0), Qt.AlignRight | Qt.AlignVCenter, label)
        p.end()

    def mouseMoveEvent(self, e) -> None:
        y = e.position().y()
        idx = int((y - PAD) / ROW_H)
        if 0 <= idx < len(self._steps):
            self._hover_idx = idx
        else:
            self._hover_idx = None
        self.update()

    def leaveEvent(self, _) -> None:
        self._hover_idx = None
        self.update()

    def mousePressEvent(self, e) -> None:
        if self._hover_idx is None:
            return
        s = self._steps[self._hover_idx]
        self._selected_id = s.step_id
        self.stepClicked.emit(s)
        self.update()
