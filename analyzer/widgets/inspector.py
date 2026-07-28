"""Side-panel step inspector. Slides in when a step is selected."""
from __future__ import annotations

from PySide6.QtCore import QEasingCurve, QPropertyAnimation, Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QFrame, QGridLayout, QLabel, QScrollArea, QVBoxLayout, QWidget,
)

from ..diagnose import StepEnrichment
from ..theme import PALETTE, color_for_step


def _fmt_int(n) -> str:
    try:
        return f"{int(n):,}"
    except (ValueError, TypeError):
        return "—"


def _fmt_ms(n) -> str:
    try:
        n = int(n)
    except (ValueError, TypeError):
        return "—"
    if n >= 60_000:
        return f"{n/60_000:.2f} min"
    if n >= 1_000:
        return f"{n/1_000:.2f} s"
    return f"{n} ms"


class StepInspector(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumWidth(320)
        self.setMaximumWidth(380)
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        self._host = QWidget()
        self._lay = QVBoxLayout(self._host)
        self._lay.setContentsMargins(16, 16, 16, 16)
        self._lay.setSpacing(12)
        scroll.setWidget(self._host)
        outer.addWidget(scroll)

        self._empty_state()

    def _empty_state(self) -> None:
        self._clear()
        head = QLabel("STEP INSPECTOR")
        head.setObjectName("SectionHeader")
        self._lay.addWidget(head)
        hint = QLabel("Select a step in the plan or timeline to see details.")
        hint.setObjectName("Caption")
        hint.setWordWrap(True)
        self._lay.addWidget(hint)
        self._lay.addStretch(1)

    def _clear(self) -> None:
        while self._lay.count():
            item = self._lay.takeAt(0)
            w = item.widget()
            if w:
                w.deleteLater()

    def show_step(self, step: StepEnrichment) -> None:
        self._clear()

        cat_label = QLabel(f"STEP {step.step_id if step.step_id is not None else '—'} · {step.category.upper()}")
        cat_label.setObjectName("SectionHeader")
        cat_label.setStyleSheet(f"color: {color_for_step(step.step_name)};")
        self._lay.addWidget(cat_label)

        name = QLabel(step.step_name or "—")
        name.setObjectName("Display")
        name.setWordWrap(True)
        self._lay.addWidget(name)

        if step.table:
            tbl = QLabel(f"{step.schema or ''}.{step.table}".strip("."))
            tbl.setObjectName("Mono")
            tbl.setStyleSheet("color:#8691AD; font-size:11px;")
            self._lay.addWidget(tbl)

        self._lay.addWidget(self._divider())

        grid_card = QFrame()
        grid_card.setObjectName("CardSubtle")
        grid = QGridLayout(grid_card)
        grid.setContentsMargins(14, 12, 14, 12)
        grid.setHorizontalSpacing(18)
        grid.setVerticalSpacing(8)

        metrics = [
            ("Elapsed", _fmt_ms(step.elapsed_ms)),
            ("% of runtime", f"{step.pct_runtime*100:.1f}%"),
            ("Input rows", _fmt_int(step.input_rows)),
            ("Output rows", _fmt_int(step.output_rows)),
            ("Input bytes", _fmt_int(step.input_bytes)),
            ("Spill (local)", _fmt_int(step.spill_local)),
            ("Spill (remote)", _fmt_int(step.spill_remote)),
            ("Data skew", f"{step.data_skew:.2f}" if step.data_skew is not None else "—"),
            ("Time skew", f"{step.time_skew:.2f}" if step.time_skew is not None else "—"),
            ("Stream · Segment", f"{step.stream_id} · {step.segment_id}"),
        ]
        for i, (k, v) in enumerate(metrics):
            k_lbl = QLabel(k)
            k_lbl.setObjectName("Caption")
            v_lbl = QLabel(v)
            v_lbl.setObjectName("Mono")
            v_lbl.setStyleSheet("color:#F5F7FF; font-size:12px; font-weight:600;")
            grid.addWidget(k_lbl, i, 0)
            grid.addWidget(v_lbl, i, 1)
        self._lay.addWidget(grid_card)

        if step.alert:
            alert_lbl = QLabel(f"Planner alert: {step.alert}")
            alert_lbl.setProperty("chip", True)
            alert_lbl.setProperty("severity", "warn")
            alert_lbl.setAlignment(Qt.AlignCenter)
            self._lay.addWidget(alert_lbl)

        if step.table_info:
            self._lay.addWidget(self._divider())
            head = QLabel("TABLE CONTEXT")
            head.setObjectName("SectionHeader")
            self._lay.addWidget(head)
            self._lay.addWidget(self._table_card(step.table_info))

        self._lay.addStretch(1)

    def _divider(self) -> QFrame:
        d = QFrame()
        d.setObjectName("Divider")
        return d

    def _table_card(self, ti: dict) -> QFrame:
        card = QFrame()
        card.setObjectName("CardSubtle")
        grid = QGridLayout(card)
        grid.setContentsMargins(14, 12, 14, 12)
        grid.setHorizontalSpacing(18)
        grid.setVerticalSpacing(8)

        def _g(key):
            return ti.get(key) if isinstance(ti, dict) else None

        rows = [
            ("Diststyle", _g("diststyle")),
            ("Sortkey", _g("sortkey1")),
            ("Rows", _fmt_int(_g("tbl_rows"))),
            ("Size (MB)", _fmt_int(_g("size"))),
            ("Unsorted %", _g("unsorted")),
            ("Stats off", _g("stats_off")),
            ("Skew rows", _g("skew_rows")),
            ("Risk", _g("risk_event")),
        ]
        for i, (k, v) in enumerate(rows):
            if v is None or (isinstance(v, str) and not v.strip()) or (isinstance(v, float) and v != v):
                v_str = "—"
            else:
                if isinstance(v, float):
                    v_str = f"{v:.1f}"
                else:
                    v_str = str(v)
            k_lbl = QLabel(k)
            k_lbl.setObjectName("Caption")
            v_lbl = QLabel(v_str)
            v_lbl.setObjectName("Mono")
            v_lbl.setStyleSheet("color:#F5F7FF; font-size:12px; font-weight:600;")
            grid.addWidget(k_lbl, i, 0)
            grid.addWidget(v_lbl, i, 1)
        return card
