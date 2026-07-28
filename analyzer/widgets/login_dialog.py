"""Infraredshift local-access startup screen."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
import os
from pathlib import Path
import sys

from PySide6.QtCore import QPointF, QRectF, Qt, QTimer
from PySide6.QtGui import (
    QColor,
    QFont,
    QLinearGradient,
    QPainter,
    QPainterPath,
    QPen,
    QRadialGradient,
)
from PySide6.QtWidgets import (
    QDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from ..assets.logo import login_logo_pixmap
from ..auth import credentials_exist, save_credentials, validate_new_credentials, verify_credentials
from ..brand import PRODUCT_NAME, WINDOW_TITLE
from .triage_home import _AnalysisProcessFlow

_MAX_ATTEMPTS = 5

# Citizens green system — fixed for the brand splash (not theme-retinted).
_BG = QColor("#F2F7F5")
_PANEL = QColor("#FFFFFF")
_INK = QColor("#163D30")
_MUTED = QColor("#5A7268")
_LINE = QColor("#B7D8CB")
_LINE_STRONG = QColor("#80BFA7")
_ACCENT = QColor("#008555")
_ACCENT_BRIGHT = QColor("#00A86B")
_ACCENT_DIM = QColor("#D8EEE6")
_PACKET = QColor("#00965E")
_WARM = QColor("#E8A838")
_COOL = QColor("#2B8CBE")
# The Infraredshift wordmark is a WIDE banner (1600x420, ~3.81:1), where the
# logo it replaced was near-square at 1.50:1. Carrying over the old 398px width
# would have rendered it only 104px tall - small and adrift in the panel.
#
# A wide mark wants the panel width instead. The dialog is 768px
# (_LOGIN_PANEL_WIDTH + 44) and the panel's own layout eats 12px per side, so
# 664px is the widest the mark can be without touching the panel edge; that
# renders 174px tall and leaves a comfortable margin.
_LOGIN_LOGO_WIDTH_RATIO = 664 / 768

# The process-flow diagram measures 724px at scale 0.67. The logo panel is
# pinned to the same width so the two boxes line up instead of the logo panel
# spanning the full dialog.
_LOGIN_PANEL_WIDTH = 724


def _published_datetime() -> datetime:
    raw = str(os.environ.get("REDSHIFT_ANALYZER_PUBLISHED_AT") or "").strip()
    if raw:
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            pass
    candidates = [
        os.environ.get("REDSHIFT_ANALYZER_LAUNCH_PATH"),
        sys.argv[0] if sys.argv else "",
        __file__,
    ]
    for candidate in candidates:
        try:
            path = Path(str(candidate or "")).resolve()
            if path.is_file():
                return datetime.fromtimestamp(path.stat().st_mtime).astimezone()
        except Exception:
            continue
    return datetime.now().astimezone()


def _published_label(published: datetime | None = None) -> str:
    published = published or _published_datetime()
    zone = published.tzname() or "local time"
    return f"Published: {published:%B} {published.day}, {published:%Y at %I:%M %p} {zone}"


def _release_version(now: datetime | None = None) -> str:
    now = now or _published_datetime()
    release = f"RC-{now.year % 10}{now.month:02d}{now.day}.{now.hour:02d}"
    build_id = str(os.environ.get("INFRAREDSHIFT_BUILD_ID") or "").strip()
    return f"{release} · {build_id[:8]}" if build_id else release


@dataclass(frozen=True)
class _Mode:
    key: str
    title: str
    detail: str
    color: QColor


# Three product modes — the animation cycles emphasis and pumps data through all.
_MODES = (
    _Mode("capture", "CAPTURE", "Producer + consumer SYS/SVV telemetry", _ACCENT),
    _Mode("triage", "TRIAGE", "Repeat patterns ranked by total pain", _COOL),
    _Mode("recommend", "RECOMMEND", "Sort · dist · stats · Spectrum actions", _WARM),
)


class _DataFlowAnimation(QWidget):
    """Live three-mode pipeline: CAPTURE → TRIAGE → RECOMMEND with continuous flow.

    Drawn entirely with QPainter — no video, web, or external assets required.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("DemandSourceAnimation")
        self.setMinimumHeight(220)
        self.setMaximumHeight(280)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self._t = 0.0
        self._timer = QTimer(self)
        self._timer.setInterval(33)  # ~30 fps
        self._timer.timeout.connect(self._tick)

    def showEvent(self, event) -> None:
        super().showEvent(event)
        self._timer.start()

    def hideEvent(self, event) -> None:
        self._timer.stop()
        super().hideEvent(event)

    def _tick(self) -> None:
        self._t = (self._t + 0.018) % (math.tau * 8.0)
        self.update()

    # ---- geometry helpers -------------------------------------------------

    def _layout_rects(self, rect: QRectF) -> tuple[QRectF, QRectF, QRectF, list[QRectF]]:
        """Return mode panels + small cluster sources inside CAPTURE."""
        pad = 10.0
        gap = 18.0
        usable = rect.adjusted(pad, 28, -pad, -22)
        w = (usable.width() - 2 * gap) / 3.0
        h = usable.height()
        capture = QRectF(usable.left(), usable.top(), w, h)
        triage = QRectF(capture.right() + gap, usable.top(), w, h)
        recommend = QRectF(triage.right() + gap, usable.top(), w, h)

        # Three cluster “pumps” stacked in the capture panel.
        sources: list[QRectF] = []
        inner = capture.adjusted(14, 36, -14, -14)
        src_h = 28.0
        spacing = (inner.height() - 3 * src_h) / 2.0 if inner.height() > 3 * src_h else 6.0
        y = inner.top()
        for _ in range(3):
            sources.append(QRectF(inner.left(), y, inner.width(), src_h))
            y += src_h + spacing
        return capture, triage, recommend, sources

    @staticmethod
    def _quad(a: QPointF, b: QPointF, c: QPointF, t: float) -> QPointF:
        u = 1.0 - t
        return QPointF(
            u * u * a.x() + 2 * u * t * b.x() + t * t * c.x(),
            u * u * a.y() + 2 * u * t * b.y() + t * t * c.y(),
        )

    @staticmethod
    def _lerp(a: QPointF, b: QPointF, t: float) -> QPointF:
        return QPointF(a.x() + (b.x() - a.x()) * t, a.y() + (b.y() - a.y()) * t)

    def _active_mode(self) -> int:
        # Cycle emphasis every ~2.2s across the three modes.
        return int((self._t / 2.2) % 3)

    # ---- paint ------------------------------------------------------------

    def paintEvent(self, _event) -> None:
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        rect = QRectF(self.rect()).adjusted(4, 2, -4, -2)

        # Soft stage background
        grad = QLinearGradient(rect.topLeft(), rect.bottomRight())
        grad.setColorAt(0.0, QColor("#F5FAF8"))
        grad.setColorAt(1.0, QColor("#EAF3EF"))
        p.fillRect(rect, grad)
        p.setPen(QPen(_LINE, 1))
        p.setBrush(Qt.NoBrush)
        p.drawRoundedRect(rect.adjusted(0.5, 0.5, -0.5, -0.5), 12, 12)

        capture, triage, recommend, sources = self._layout_rects(rect)
        panels = (capture, triage, recommend)
        active = self._active_mode()

        # Header strip: three mode labels with live activity bars
        self._paint_mode_headers(p, panels, active)

        # Stage panels
        for index, (panel, mode) in enumerate(zip(panels, _MODES)):
            self._paint_stage_panel(p, panel, mode, active=index == active)

        # Cluster pumps inside CAPTURE
        labels = ("PRODUCER", "CONSUMER 1", "CONSUMER 2")
        for i, src in enumerate(sources):
            self._paint_source_node(p, src, labels[i], pump=i)

        # Triage hub + pattern bars
        hub = triage.center()
        self._paint_triage_hub(p, hub, active=active == 1)
        self._paint_pattern_bars(p, triage, active=active == 1)

        # Recommend cards
        self._paint_recommend_cards(p, recommend, active=active == 2)

        # Flow paths + packets (the real motion)
        self._paint_flows(p, sources, hub, recommend, active)

        # Footer caption inside the canvas (keeps chrome thin above the fold)
        p.setPen(_MUTED)
        p.setFont(QFont("Segoe UI", 8, QFont.DemiBold))
        p.drawText(
            QRectF(rect.left(), rect.bottom() - 18, rect.width(), 16),
            Qt.AlignCenter,
            "LIVE FLOW   ·   CAPTURE SYS TELEMETRY   ·   GROUP REPEATS   ·   EMIT PHYSICAL FIXES",
        )

    def _paint_mode_headers(self, p: QPainter, panels: tuple[QRectF, ...], active: int) -> None:
        for index, (panel, mode) in enumerate(zip(panels, _MODES)):
            is_on = index == active
            color = mode.color if is_on else _MUTED
            p.setPen(color)
            p.setFont(QFont("Segoe UI", 9, QFont.Bold))
            title_rect = QRectF(panel.left(), panel.top() - 22, panel.width(), 18)
            p.drawText(title_rect, Qt.AlignHCenter | Qt.AlignVCenter, f"{index + 1}  {mode.title}")

            # Activity bar under the title
            bar = QRectF(panel.left() + panel.width() * 0.18, panel.top() - 4, panel.width() * 0.64, 3)
            p.setPen(Qt.NoPen)
            p.setBrush(QColor(mode.color.red(), mode.color.green(), mode.color.blue(), 40))
            p.drawRoundedRect(bar, 2, 2)
            if is_on:
                fill_w = bar.width() * (0.35 + 0.65 * (0.5 + 0.5 * math.sin(self._t * 3.0 + index)))
                fill = QRectF(bar.left(), bar.top(), fill_w, bar.height())
                p.setBrush(mode.color)
                p.drawRoundedRect(fill, 2, 2)

    def _paint_stage_panel(self, p: QPainter, panel: QRectF, mode: _Mode, *, active: bool) -> None:
        bg = QColor(_PANEL)
        if active:
            bg = QColor(mode.color.red(), mode.color.green(), mode.color.blue(), 18)
        p.setPen(QPen(mode.color if active else _LINE, 1.6 if active else 1.0))
        p.setBrush(bg)
        p.drawRoundedRect(panel, 12, 12)
        # Subtle top accent
        if active:
            p.setPen(Qt.NoPen)
            p.setBrush(mode.color)
            p.drawRoundedRect(QRectF(panel.left() + 16, panel.top() + 1, panel.width() - 32, 3), 2, 2)

    def _paint_source_node(self, p: QPainter, rect: QRectF, label: str, *, pump: int) -> None:
        # Pump scale: each source fires slightly out of phase
        pulse = 0.5 + 0.5 * math.sin(self._t * 4.2 + pump * 1.7)
        inflate = 1.0 + 0.04 * pulse
        cx, cy = rect.center().x(), rect.center().y()
        w, h = rect.width() * inflate, rect.height() * inflate
        r = QRectF(cx - w / 2, cy - h / 2, w, h)

        p.setPen(QPen(_LINE_STRONG, 1.5))
        p.setBrush(_PANEL)
        p.drawRoundedRect(r, 8, 8)

        # Left status LED pumping
        led = QPointF(r.left() + 10, r.center().y())
        led_color = QColor(_ACCENT)
        led_color.setAlphaF(0.45 + 0.55 * pulse)
        p.setPen(Qt.NoPen)
        p.setBrush(led_color)
        p.drawEllipse(led, 3.2, 3.2)

        p.setPen(_INK)
        p.setFont(QFont("Segoe UI", 8, QFont.DemiBold))
        p.drawText(r.adjusted(16, 0, -6, 0), Qt.AlignVCenter | Qt.AlignLeft, label)

        # Tiny egress ticks
        p.setPen(QPen(_ACCENT, 1.2))
        for tick in range(3):
            x = r.right() - 6
            y = r.top() + 7 + tick * 7
            p.drawLine(QPointF(x, y), QPointF(x + 4, y + 2.5))

    def _paint_triage_hub(self, p: QPainter, hub: QPointF, *, active: bool) -> None:
        radius = 30.0 + (3.0 if active else 0.0) * (0.5 + 0.5 * math.sin(self._t * 3.5))
        # Outer glow rings
        for ring in range(3, 0, -1):
            alpha = 28 * ring if active else 12 * ring
            color = QColor(_COOL.red(), _COOL.green(), _COOL.blue(), alpha)
            p.setPen(QPen(color, 1.5))
            p.setBrush(Qt.NoBrush)
            p.drawEllipse(hub, radius + ring * 7, radius + ring * 7)

        rad = QRadialGradient(hub, radius)
        rad.setColorAt(0.0, QColor("#1FA3D4") if active else QColor("#4AA3C0"))
        rad.setColorAt(1.0, _COOL)
        p.setPen(QPen(_ACCENT_DIM, 2))
        p.setBrush(rad)
        p.drawEllipse(hub, radius, radius)

        p.setPen(QColor("#FFFFFF"))
        p.setFont(QFont("Segoe UI", 8, QFont.Bold))
        p.drawText(QRectF(hub.x() - 28, hub.y() - 16, 56, 32), Qt.AlignCenter, "REPEAT\nGROUPS")

    def _paint_pattern_bars(self, p: QPainter, panel: QRectF, *, active: bool) -> None:
        # Ranked pattern bars under the hub — lengths breathe with flow
        base = QPointF(panel.left() + 16, panel.bottom() - 52)
        widths = (0.72, 0.55, 0.40, 0.28)
        for i, frac in enumerate(widths):
            breathe = 0.85 + 0.15 * math.sin(self._t * 2.8 + i * 0.9)
            w = (panel.width() - 32) * frac * breathe
            y = base.y() + i * 10
            bar = QRectF(base.x(), y, w, 6)
            color = QColor(_COOL)
            color.setAlpha(210 if active else 140)
            if i == 0:
                color = _ACCENT if active else color
            p.setPen(Qt.NoPen)
            p.setBrush(color)
            p.drawRoundedRect(bar, 3, 3)

    def _paint_recommend_cards(self, p: QPainter, panel: QRectF, *, active: bool) -> None:
        cards = (
            ("SORTKEY", "order_date"),
            ("DISTKEY", "customer_id"),
            ("VACUUM", "stats + sort"),
        )
        top = panel.top() + 40
        for i, (kind, detail) in enumerate(cards):
            card = QRectF(panel.left() + 14, top + i * 42, panel.width() - 28, 36)
            hit = 0.5 + 0.5 * math.sin(self._t * 3.0 + i * 1.3)
            border = _WARM if active else _LINE_STRONG
            p.setPen(QPen(border, 1.4 if active else 1.0))
            p.setBrush(_PANEL)
            p.drawRoundedRect(card, 8, 8)

            # Arrival flash on the left edge
            flash = QColor(_WARM)
            flash.setAlphaF(0.15 + 0.55 * hit if active else 0.12)
            p.setPen(Qt.NoPen)
            p.setBrush(flash)
            p.drawRoundedRect(QRectF(card.left() + 1, card.top() + 1, 5, card.height() - 2), 2, 2)

            p.setPen(_INK)
            p.setFont(QFont("Segoe UI", 8, QFont.Bold))
            p.drawText(card.adjusted(12, 2, -8, -14), Qt.AlignLeft | Qt.AlignVCenter, kind)
            p.setPen(_MUTED)
            p.setFont(QFont("Segoe UI", 7))
            p.drawText(card.adjusted(12, 14, -8, -2), Qt.AlignLeft | Qt.AlignVCenter, detail)

    def _paint_flows(
        self,
        p: QPainter,
        sources: list[QRectF],
        hub: QPointF,
        recommend: QRectF,
        active: int,
    ) -> None:
        # Paths: each source → hub, then hub → recommend cards
        paths: list[tuple[QPainterPath, QColor, float]] = []
        for i, src in enumerate(sources):
            start = QPointF(src.right(), src.center().y())
            control = QPointF((start.x() + hub.x()) * 0.5, start.y() + (hub.y() - start.y()) * 0.15)
            path = QPainterPath(start)
            path.quadTo(control, hub)
            color = _ACCENT if active == 0 else _LINE_STRONG
            paths.append((path, color, 0.12 + i * 0.08))

        # Hub to three recommend targets
        targets = [
            QPointF(recommend.left() + 8, recommend.top() + 58),
            QPointF(recommend.left() + 8, recommend.top() + 100),
            QPointF(recommend.left() + 8, recommend.top() + 142),
        ]
        for i, end in enumerate(targets):
            control = QPointF((hub.x() + end.x()) * 0.55, hub.y() + (end.y() - hub.y()) * 0.2)
            path = QPainterPath(hub)
            path.quadTo(control, end)
            color = _WARM if active == 2 else (_COOL if active == 1 else _LINE_STRONG)
            paths.append((path, color, 0.05 + i * 0.07))

        # Draw conduit strokes
        for path, color, _phase in paths:
            pen = QPen(QColor(color.red(), color.green(), color.blue(), 90), 2.0)
            pen.setCapStyle(Qt.RoundCap)
            p.setPen(pen)
            p.setBrush(Qt.NoBrush)
            p.drawPath(path)

        # Animated dashed overlay (moving “flow”)
        dash_phase = int(self._t * 40) % 16
        for path, color, _ in paths:
            pen = QPen(QColor(color.red(), color.green(), color.blue(), 160), 1.6, Qt.CustomDashLine)
            pen.setDashPattern([4, 6])
            pen.setDashOffset(-dash_phase)
            pen.setCapStyle(Qt.RoundCap)
            p.setPen(pen)
            p.drawPath(path)

        # Packets racing along each path
        for path_index, (path, color, phase_off) in enumerate(paths):
            length = max(path.length(), 1.0)
            for slot in range(4):
                # Staggered continuous motion
                speed = 0.22 + (path_index % 3) * 0.03
                t = (self._t * speed + phase_off + slot * 0.25) % 1.0
                # Ease slightly so packets bunch near pumps
                te = t * t * (3 - 2 * t)
                point = path.pointAtPercent(te)
                size = 3.0 + 1.4 * math.sin(self._t * 5 + slot + path_index)
                # Trail
                trail_t = max(0.0, te - 0.04)
                trail = path.pointAtPercent(trail_t)
                trail_color = QColor(color)
                trail_color.setAlpha(50)
                p.setPen(Qt.NoPen)
                p.setBrush(trail_color)
                p.drawEllipse(trail, size * 0.7, size * 0.7)
                # Head
                head = QColor(color)
                head.setAlpha(230)
                p.setBrush(head)
                p.drawEllipse(point, size, size)

        # Active-mode callout bar at the bottom of the active panel is enough;
        # packets already encode continuous multi-mode pumping.


class LoginDialog(QDialog):
    """Single above-the-fold startup: brand and local access."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("StartupExperience")
        self.setWindowTitle(WINDOW_TITLE)
        self.setModal(True)
        # Compact: everything visible without scrolling on a laptop.
        # Sized to the process-flow diagram plus margins, so no element is wider
        # than the content it frames.
        self.setMinimumSize(_LOGIN_PANEL_WIDTH + 44, 660)
        self.resize(_LOGIN_PANEL_WIDTH + 44, 720)
        self.setMaximumWidth(_LOGIN_PANEL_WIDTH + 120)
        self.setMaximumHeight(820)
        self._attempts = 0
        self._setup_mode = not credentials_exist()
        self._pending_setup: tuple[str, str] | None = None

        lay = QVBoxLayout(self)
        lay.setContentsMargins(22, 14, 22, 12)
        lay.setSpacing(8)

        # --- header: supplied green logo on a clean white brand panel ---
        self._logo_panel = QFrame()
        self._logo_panel.setObjectName("StartupLogoPanel")
        header = QGridLayout(self._logo_panel)
        header.setContentsMargins(12, 8, 12, 8)
        header.setSpacing(10)
        self._logo = QLabel()
        self._logo.setObjectName("InfraredshiftLogo")
        self._logo.setAlignment(Qt.AlignCenter)
        self._logo.setAccessibleName(f"{PRODUCT_NAME} logo")
        self._logo_source = login_logo_pixmap()
        if not self._logo_source.isNull():
            self._sync_logo_width()
        else:
            self._logo.setText(PRODUCT_NAME)
            self._logo.setObjectName("StartupProduct")
        header.setColumnStretch(0, 1)
        header.setColumnStretch(2, 1)
        header.addWidget(self._logo, 0, 1, Qt.AlignCenter)
        version = QLabel(_release_version())
        version.setObjectName("StartupVersion")
        version.setToolTip(
            "Release stamp and immutable embedded-source build ID. "
            "Use python Infraredshift_APP.txt --self-check to verify this exact file."
        )
        header.addWidget(version, 0, 2, Qt.AlignRight | Qt.AlignTop)
        self._logo_panel.setFixedWidth(_LOGIN_PANEL_WIDTH)
        lay.addWidget(self._logo_panel, 0, Qt.AlignHCenter)

        # --- title (tight) ---
        title = QLabel("Private Redshift intelligence workspace")
        title.setObjectName("StartupTitle")
        title.setAlignment(Qt.AlignCenter)
        lay.addWidget(title)
        subtitle = QLabel(
            "Sign in to inspect captured workloads and physical-design opportunities. "
            "Analysis runs locally on this machine."
        )
        subtitle.setObjectName("StartupSubtitle")
        subtitle.setWordWrap(True)
        subtitle.setAlignment(Qt.AlignCenter)
        lay.addWidget(subtitle)

        # The process overview lives here so the product story is visible
        # before entering the workspace. At 67% of its former dimensions it
        # remains compact enough to keep the access controls above the fold.
        self._animation = _AnalysisProcessFlow(scale=0.67)
        lay.addWidget(self._animation, 0, Qt.AlignHCenter)

        # --- access row (compact, single line) ---
        access = QFrame()
        access.setObjectName("StartupAccess")
        access.setFixedWidth(_LOGIN_PANEL_WIDTH)
        access_lay = QHBoxLayout(access)
        access_lay.setContentsMargins(10, 6, 10, 6)
        access_lay.setSpacing(6)

        access_label = QLabel("Create local access" if self._setup_mode else "Local access")
        access_label.setObjectName("StartupAccessLabel")
        access_lay.addWidget(access_label)

        self._access = QLineEdit()
        self._access.setEchoMode(QLineEdit.Password)
        self._access.setPlaceholderText("Create access code" if self._setup_mode else "Access code")
        self._access.setMaximumWidth(180)
        self._pin = QLineEdit()
        self._pin.setEchoMode(QLineEdit.Password)
        self._pin.setPlaceholderText("Create PIN" if self._setup_mode else "PIN")
        self._pin.setMaxLength(12)
        self._pin.setMaximumWidth(100)
        access_lay.addWidget(self._access)
        access_lay.addWidget(self._pin)
        access_lay.addStretch(1)

        quit_btn = QPushButton("Exit")
        quit_btn.setObjectName("StartupQuietButton")
        quit_btn.clicked.connect(self.reject)
        self._sign_in = QPushButton("Secure workspace" if self._setup_mode else "Open workspace")
        self._sign_in.setObjectName("StartupOpenButton")
        self._sign_in.setDefault(True)
        self._sign_in.clicked.connect(self._attempt)
        access_lay.addWidget(quit_btn)
        access_lay.addWidget(self._sign_in)
        lay.addWidget(access, 0, Qt.AlignHCenter)

        self._status = QLabel("")
        self._status.setObjectName("StartupAccessStatus")
        self._status.setWordWrap(True)
        if self._setup_mode:
            self._status.setText(
                f"First run: create an access code (6+ characters) and a separate 4–12 digit PIN. "
                f"{PRODUCT_NAME} contains no default or backdoor credentials."
            )
        lay.addWidget(self._status)

        # --- footer ---
        footer = QHBoxLayout()
        privacy = QLabel("Local-first · No cloud · Telemetry stays on this device")
        privacy.setObjectName("StartupFooter")
        footer.addWidget(privacy)
        footer.addStretch(1)
        published = QLabel(_published_label())
        published.setObjectName("StartupFooter")
        published.setToolTip("Date and time this application artifact was built for publication.")
        footer.addWidget(published)
        lay.addLayout(footer)

        self._access.returnPressed.connect(self._pin.setFocus)
        self._pin.returnPressed.connect(self._attempt)
        self._access.setFocus()

    def _sync_logo_width(self) -> None:
        if self._logo_source.isNull():
            return
        target_width = max(1, round(self.width() * _LOGIN_LOGO_WIDTH_RATIO))
        # Re-render the vector at the new width rather than resampling a
        # bitmap; rescaling a raster is what makes a resized logo look soft.
        scaled = login_logo_pixmap(target_width)
        if scaled.isNull():
            scaled = self._logo_source.scaledToWidth(
                target_width,
                Qt.SmoothTransformation,
            )
        self._logo.setPixmap(scaled)
        self._logo.setFixedSize(scaled.size())

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        if hasattr(self, "_logo_source"):
            self._sync_logo_width()

    def _attempt(self) -> None:
        access_code = self._access.text().strip()
        pin = self._pin.text().strip()
        if self._setup_mode:
            try:
                validate_new_credentials(access_code, pin)
                if self._pending_setup is None:
                    self._pending_setup = (access_code, pin)
                    self._access.clear()
                    self._pin.clear()
                    self._access.setFocus()
                    self._sign_in.setText("Confirm and secure")
                    self._status.setText("Re-enter the same access code and PIN to confirm them.")
                    return
                if self._pending_setup != (access_code, pin):
                    self._pending_setup = None
                    self._access.clear()
                    self._pin.clear()
                    self._access.setFocus()
                    self._sign_in.setText("Secure workspace")
                    raise ValueError(
                        "The confirmation did not match. Enter the new access code and PIN again."
                    )
                from ..secrets_store import validate_first_run_secrets_state

                validate_first_run_secrets_state()
                save_credentials(access_code, pin)
                from ..secrets_store import unlock_secrets_session

                unlock_secrets_session(access_code, pin)
            except Exception as exc:
                self._status.setText(str(exc))
                self._pin.clear()
                return
            self.accept()
            return
        if verify_credentials(access_code, pin):
            try:
                from ..secrets_store import unlock_secrets_session

                unlock_secrets_session(access_code, pin)
            except Exception as exc:
                self._status.setText(
                    f"Credentials verified, but encrypted cluster credentials could not be opened: {exc}"
                )
                return
            self.accept()
            return
        self._attempts += 1
        remaining = _MAX_ATTEMPTS - self._attempts
        if remaining <= 0:
            self._status.setText("Too many failed attempts. Closing.")
            self._sign_in.setEnabled(False)
            QTimer.singleShot(1200, self.reject)
            return
        self._status.setText(f"Access code or PIN incorrect. {remaining} attempt(s) remaining.")
        self._pin.clear()
        self._access.setFocus()
        self._access.selectAll()
