"""Custom title bar with metric chips."""
from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QPushButton, QSizePolicy, QVBoxLayout, QWidget

from ..assets.logo import mark_pixmap, wordmark_pixmap
from ..brand import PRODUCT_NAME, PRODUCT_NAME_UPPER
from ..theme import PALETTE, normalize_theme_mode


class _Metric(QFrame):
    def __init__(self, label: str, value: str = "—"):
        super().__init__()
        self.setObjectName("CardSubtle")
        self.setMinimumWidth(72)
        self.setMaximumWidth(126)
        self.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(8, 5, 8, 5)
        lay.setSpacing(0)
        l = QLabel(label)
        l.setObjectName("SectionHeader")
        v = QLabel(value)
        v.setObjectName("H1")
        lay.addWidget(l)
        lay.addWidget(v)
        self._label = l
        self._value = v
        self.apply_theme()

    def apply_theme(self) -> None:
        # Use live palette tokens so light mode never shows near-white text.
        # 10px is the floor for readable metric captions (was 8px).
        self._label.setStyleSheet(f"font-size: 10px; color: {PALETTE.text_2};")
        self._value.setStyleSheet(
            f"color: {PALETTE.text_0}; font-size: 13px; font-weight: 600;"
        )

    def set_label(self, label: str) -> None:
        self._label.setText(label)

    def set_value(self, v: str) -> None:
        self._value.setText(v)


class _Logo(QLabel):
    def __init__(self):
        super().__init__()
        self.setObjectName("InfraredshiftLogo")
        self.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.setAccessibleName(f"{PRODUCT_NAME} logo")
        # Prefer compact wordmark; fall back to mark + text handled by title bar.
        pix = wordmark_pixmap(30)
        if pix.isNull():
            pix = mark_pixmap(28)
        if not pix.isNull():
            self.setPixmap(pix)
            self.setFixedHeight(pix.height() + 2)
            self.setMinimumWidth(min(200, pix.width()))
        else:
            self.setText(PRODUCT_NAME_UPPER)
            self.setFixedHeight(28)


class TitleBar(QWidget):
    themeModeChanged = Signal(str)
    exitRequested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("TitleBar")
        self.setFixedHeight(52)
        self.setAccessibleName("Application title bar")
        lay = QHBoxLayout(self)
        lay.setContentsMargins(12, 6, 12, 6)
        lay.setSpacing(10)

        self._logo = _Logo()
        lay.addWidget(self._logo)

        brand_box = QVBoxLayout()
        brand_box.setSpacing(0)
        # Wordmark already carries the name; keep a thin product line for context.
        title = QLabel(PRODUCT_NAME if self._logo.pixmap() is None or self._logo.pixmap().isNull() else "")
        title.setObjectName("AppTitle")
        title.setAccessibleName(PRODUCT_NAME)
        subtitle = QLabel("OFFLINE · AIR-GAPPED · LOCAL DUCKDB")
        subtitle.setObjectName("AppSubtitle")
        if title.text():
            brand_box.addWidget(title)
        brand_box.addWidget(subtitle)
        lay.addLayout(brand_box)
        self._subtitle = subtitle

        lay.addStretch(1)

        # Exit and theme stay available at every width — they are accessibility
        # and safety controls, not optional chrome.
        self._exit_btn = QPushButton("EXIT")
        self._exit_btn.setObjectName("Ghost")
        self._exit_btn.setFixedWidth(76)
        self._exit_btn.setToolTip("Close the application.")
        self._exit_btn.setAccessibleName("Exit application")
        self._exit_btn.setAccessibleDescription(f"Close {PRODUCT_NAME}.")
        self._exit_btn.setShortcut("Ctrl+Q")
        self._exit_btn.clicked.connect(self.exitRequested.emit)
        lay.addWidget(self._exit_btn)

        self._theme_toggle = QPushButton("LIGHT")
        self._theme_toggle.setObjectName("Ghost")
        self._theme_toggle.setCheckable(True)
        self._theme_toggle.setFixedWidth(76)
        self._theme_toggle.setToolTip(
            "Switch between Light and Dark theme for contrast and readability."
        )
        self._theme_toggle.setAccessibleName("Color theme")
        self._theme_toggle.setAccessibleDescription(
            "Toggle light or dark color theme. Always available at any window size."
        )
        self._theme_toggle.setShortcut("Ctrl+Shift+T")
        self._theme_toggle.clicked.connect(self._theme_clicked)
        lay.addWidget(self._theme_toggle)

        lay.addStretch(1)

        self.m_qid = _Metric("QUERY ID")
        self.m_runtime = _Metric("RUNTIME")
        self.m_steps = _Metric("STEPS")
        self.m_tables = _Metric("TABLES")
        self.m_alerts = _Metric("FINDINGS")

        self._metrics = (self.m_qid, self.m_runtime, self.m_steps, self.m_tables, self.m_alerts)

        for m in self._metrics:
            lay.addWidget(m)

    def resizeEvent(self, event) -> None:
        # Collapse metrics and subtitle first. Never hide Exit or Theme.
        width = self.width()
        self._subtitle.setVisible(width >= 820)
        self.m_runtime.setVisible(width >= 900)
        self.m_steps.setVisible(width >= 1040)
        self.m_tables.setVisible(width >= 1160)
        self.m_alerts.setVisible(width >= 980)
        self._theme_toggle.setVisible(True)
        self._exit_btn.setVisible(True)
        super().resizeEvent(event)

    def set_theme_mode(self, mode: object) -> None:
        theme = normalize_theme_mode(mode)
        self._theme_toggle.blockSignals(True)
        self._theme_toggle.setChecked(theme == "dark")
        self._theme_toggle.setText("DARK" if theme == "dark" else "LIGHT")
        self._theme_toggle.blockSignals(False)
        self.apply_theme()

    def apply_theme(self) -> None:
        for metric in self._metrics:
            metric.apply_theme()

    def _theme_clicked(self, checked: bool) -> None:
        mode = "dark" if checked else "light"
        self.set_theme_mode(mode)
        self.themeModeChanged.emit(mode)

    def update_metrics(self, qid, runtime_ms, steps, tables, findings) -> None:
        self.m_qid.set_label("QUERY ID")
        self.m_runtime.set_label("RUNTIME")
        self.m_steps.set_label("STEPS")
        self.m_tables.set_label("TABLES")
        self.m_alerts.set_label("FINDINGS")
        self.m_qid.set_value(str(qid) if qid is not None else "—")
        if runtime_ms and runtime_ms >= 1000:
            self.m_runtime.set_value(f"{runtime_ms/1000:.2f} s")
        elif runtime_ms:
            self.m_runtime.set_value(f"{runtime_ms} ms")
        else:
            self.m_runtime.set_value("—")
        self.m_steps.set_value(str(steps) if steps else "—")
        self.m_tables.set_value(str(tables) if tables else "—")
        self.m_alerts.set_value(str(findings) if findings else "0")

    def update_cluster_metrics(self, summary: dict, rule_count: int) -> None:
        self.m_qid.set_label("SLOW QUERIES")
        self.m_runtime.set_label("TOTAL RUNTIME")
        self.m_steps.set_label("SQL CHECKS")
        self.m_tables.set_label("RISK TABLES")
        self.m_alerts.set_label("CRITICAL")
        self.m_qid.set_value(_fmt_int(summary.get("slow_query_count")))
        self.m_runtime.set_value(_fmt_seconds(summary.get("total_runtime_s")))
        self.m_steps.set_value(str(rule_count))
        self.m_tables.set_value(_fmt_int(summary.get("high_risk_table_count")))
        self.m_alerts.set_value(_fmt_int(summary.get("critical_count")))


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
