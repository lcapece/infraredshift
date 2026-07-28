"""Animated query plan DAG. Showcase visualization."""
from __future__ import annotations
import math
from dataclasses import dataclass

from PySide6.QtCore import (
    QEasingCurve, QObject, QPointF, QPropertyAnimation, QRectF, QTimer, Qt, Signal,
    Property,
)
from PySide6.QtGui import (
    QBrush, QColor, QFont, QLinearGradient, QPainter, QPainterPath, QPen,
    QRadialGradient, QWheelEvent,
)
from PySide6.QtWidgets import (
    QGraphicsDropShadowEffect, QGraphicsItem, QGraphicsPathItem, QGraphicsScene,
    QGraphicsView, QGraphicsItemGroup,
)

from ..diagnose import StepEnrichment
from ..theme import PALETTE, color_for_step


NODE_W = 200
NODE_H = 92
COL_GAP = 90
ROW_GAP = 28


def _fmt_int(n: int | None) -> str:
    if n is None:
        return "—"
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return f"{n:,}"


def _fmt_ms(n: int | None) -> str:
    if n is None or n == 0:
        return "0 ms"
    if n >= 60_000:
        return f"{n/60_000:.1f} m"
    if n >= 1_000:
        return f"{n/1_000:.2f} s"
    return f"{n} ms"


@dataclass
class _Node:
    step: StepEnrichment
    x: float
    y: float
    is_worst: bool


class StepNode(QGraphicsItem):
    """Interactive plan-step card."""

    def __init__(self, step: StepEnrichment, is_worst: bool, click_cb):
        super().__init__()
        self.step = step
        self.is_worst = is_worst
        self._click_cb = click_cb
        self._hovered = False
        self._pulse = 0.0
        self.setAcceptHoverEvents(True)
        self.setFlag(QGraphicsItem.ItemIsSelectable, True)
        self.setAcceptedMouseButtons(Qt.LeftButton)

        shadow = QGraphicsDropShadowEffect()
        shadow.setBlurRadius(30 if is_worst else 18)
        shadow.setOffset(0, 6)
        shadow.setColor(QColor(0, 0, 0, 160))
        self.setGraphicsEffect(shadow)

    def boundingRect(self) -> QRectF:
        pad = 12
        return QRectF(-pad, -pad, NODE_W + 2 * pad, NODE_H + 2 * pad)

    def set_pulse(self, v: float) -> None:
        self._pulse = v
        self.update()

    pulse = Property(float, lambda self: self._pulse, set_pulse)

    def hoverEnterEvent(self, e):
        self._hovered = True
        self.update()

    def hoverLeaveEvent(self, e):
        self._hovered = False
        self.update()

    def mousePressEvent(self, e):
        if e.button() == Qt.LeftButton and self._click_cb:
            self._click_cb(self.step)
        super().mousePressEvent(e)

    def paint(self, p: QPainter, option, widget=None) -> None:
        p.setRenderHint(QPainter.Antialiasing)
        accent = QColor(color_for_step(self.step.step_name))
        base = QColor(PALETTE.bg_2)
        base_hi = QColor(PALETTE.bg_3)

        rect = QRectF(0, 0, NODE_W, NODE_H)
        radius = 14

        # Glow for worst step (pulsing).
        if self.is_worst:
            glow_rect = rect.adjusted(-10, -10, 10, 10)
            glow_radius = radius + 10
            rg = QRadialGradient(glow_rect.center(), max(glow_rect.width(), glow_rect.height()) / 2)
            alpha = int(90 + 70 * self._pulse)
            rg.setColorAt(0.0, QColor(accent.red(), accent.green(), accent.blue(), alpha))
            rg.setColorAt(1.0, QColor(accent.red(), accent.green(), accent.blue(), 0))
            p.setPen(Qt.NoPen)
            p.setBrush(rg)
            p.drawRoundedRect(glow_rect, glow_radius, glow_radius)

        # Card background — vertical gradient.
        bg = QLinearGradient(0, 0, 0, NODE_H)
        bg.setColorAt(0, base_hi)
        bg.setColorAt(1, base)
        p.setBrush(bg)
        border_color = accent if (self._hovered or self.isSelected() or self.is_worst) else QColor(PALETTE.border)
        pen = QPen(border_color, 1.2 if not self.is_worst else 1.6)
        p.setPen(pen)
        p.drawRoundedRect(rect, radius, radius)

        # Left accent bar.
        accent_path = QPainterPath()
        accent_path.addRoundedRect(QRectF(0, 0, 4, NODE_H), 2, 2)
        p.fillPath(accent_path, accent)

        # Category dot + step label.
        p.setPen(QColor(PALETTE.text_2))
        f_small = QFont("Inter", 8, QFont.Bold)
        f_small.setLetterSpacing(QFont.PercentageSpacing, 110)
        p.setFont(f_small)
        cat_text = self.step.category.upper()
        if self.step.step_id is not None:
            cat_text = f"STEP {self.step.step_id} · {cat_text}"
        p.drawText(QRectF(14, 8, NODE_W - 20, 14), Qt.AlignLeft | Qt.AlignVCenter, cat_text)

        # Primary line: step_name.
        p.setPen(QColor(PALETTE.text_0))
        f_name = QFont("Inter", 10, QFont.DemiBold)
        p.setFont(f_name)
        name = self.step.step_name or "—"
        if len(name) > 24:
            name = name[:22] + "…"
        p.drawText(QRectF(14, 22, NODE_W - 20, 18), Qt.AlignLeft, name)

        # Secondary: table.
        p.setPen(QColor(PALETTE.text_2))
        p.setFont(QFont("JetBrains Mono", 8))
        tbl = self.step.table or "—"
        if self.step.schema:
            tbl = f"{self.step.schema}.{tbl}"
        if len(tbl) > 28:
            tbl = tbl[:26] + "…"
        p.drawText(QRectF(14, 42, NODE_W - 20, 14), Qt.AlignLeft, tbl)

        # Metrics row: rows · bytes · elapsed.
        p.setPen(QColor(PALETTE.text_1))
        p.setFont(QFont("Inter", 9, QFont.DemiBold))
        rows = _fmt_int(self.step.output_rows if self.step.output_rows is not None else self.step.input_rows)
        ms = _fmt_ms(self.step.elapsed_ms)
        metric = f"{rows} rows  ·  {ms}"
        p.drawText(QRectF(14, 58, NODE_W - 20, 14), Qt.AlignLeft, metric)

        # Runtime bar.
        pct = max(0.0, min(1.0, self.step.pct_runtime))
        bar_y = NODE_H - 14
        p.setPen(Qt.NoPen)
        p.setBrush(QColor(PALETTE.bg_1))
        p.drawRoundedRect(QRectF(14, bar_y, NODE_W - 28, 4), 2, 2)
        if pct > 0:
            p.setBrush(accent)
            p.drawRoundedRect(QRectF(14, bar_y, (NODE_W - 28) * pct, 4), 2, 2)

        # Pct label.
        p.setPen(QColor(PALETTE.text_3))
        p.setFont(QFont("Inter", 8))
        p.drawText(QRectF(14, bar_y - 14, NODE_W - 28, 12), Qt.AlignRight, f"{pct*100:.0f}%")


class _FlowEdge(QGraphicsPathItem):
    def __init__(self, src: StepNode, dst: StepNode, hot: bool):
        super().__init__()
        self._src = src
        self._dst = dst
        self._hot = hot
        self._dash_offset = 0.0
        self.setZValue(-1)
        self._build_path()

    def _build_path(self) -> None:
        s = QPointF(self._src.pos().x() + NODE_W, self._src.pos().y() + NODE_H / 2)
        d = QPointF(self._dst.pos().x(), self._dst.pos().y() + NODE_H / 2)
        dx = (d.x() - s.x()) * 0.5
        path = QPainterPath(s)
        path.cubicTo(QPointF(s.x() + dx, s.y()), QPointF(d.x() - dx, d.y()), d)
        self.setPath(path)

    def set_offset(self, v: float) -> None:
        self._dash_offset = v
        self.update()

    def paint(self, p: QPainter, option, widget=None) -> None:
        p.setRenderHint(QPainter.Antialiasing)
        color = QColor(PALETTE.accent_bright if self._hot else PALETTE.border_strong)
        # Underlay (static, thin).
        under = QPen(QColor(PALETTE.border), 1.2)
        p.setPen(under)
        p.setBrush(Qt.NoBrush)
        p.drawPath(self.path())
        # Animated dash overlay.
        pen = QPen(color, 2.0 if self._hot else 1.3)
        pen.setDashPattern([6, 6])
        pen.setDashOffset(self._dash_offset)
        pen.setCapStyle(Qt.RoundCap)
        p.setPen(pen)
        p.drawPath(self.path())


class _PulseDriver(QObject):
    """Drives the pulse + dash animations on a 16ms tick."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._t = 0.0
        self._edges: list[_FlowEdge] = []
        self._worst: StepNode | None = None
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._tick)
        self._timer.start(16)

    def register(self, edges: list[_FlowEdge], worst: StepNode | None) -> None:
        self._edges = edges
        self._worst = worst

    def _tick(self) -> None:
        self._t += 0.016
        offset = -(self._t * 40.0) % 24
        for e in self._edges:
            e.set_offset(offset)
        if self._worst is not None:
            self._worst.set_pulse(0.5 + 0.5 * math.sin(self._t * 2.4))


class PlanGraphView(QGraphicsView):
    """Zoomable, pannable plan DAG with animated flow."""

    stepClicked = Signal(object)  # emits StepEnrichment

    def __init__(self, parent=None):
        super().__init__(parent)
        self._scene = QGraphicsScene(self)
        self.setScene(self._scene)
        self.setRenderHints(QPainter.Antialiasing | QPainter.SmoothPixmapTransform)
        self.setDragMode(QGraphicsView.ScrollHandDrag)
        self.setTransformationAnchor(QGraphicsView.AnchorUnderMouse)
        self.setResizeAnchor(QGraphicsView.AnchorUnderMouse)
        self.setViewportUpdateMode(QGraphicsView.SmartViewportUpdate)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._driver = _PulseDriver(self)
        self._nodes: list[StepNode] = []

    def clear(self) -> None:
        self._scene.clear()
        self._nodes = []
        self._driver.register([], None)

    def load_steps(self, steps: list[StepEnrichment], worst_step_id: int | None) -> None:
        self.clear()
        if not steps:
            self._empty_state()
            return

        # Layout: group by (stream_id, segment_id) → column. Stream stacks vertically.
        groups: dict[tuple[int, int], list[StepEnrichment]] = {}
        for s in steps:
            key = (s.stream_id or 0, s.segment_id or 0)
            groups.setdefault(key, []).append(s)

        seg_keys = sorted(groups.keys())
        nodes_by_id: dict[int, StepNode] = {}

        col_heights: dict[int, float] = {}
        stream_offsets: dict[int, float] = {}
        stream_max_width: dict[int, int] = {}

        streams = sorted({k[0] for k in seg_keys})
        for i, stream in enumerate(streams):
            stream_offsets[stream] = i * (NODE_H * 5 + 40)

        seg_cols: dict[int, int] = {}
        unique_segs = sorted({k[1] for k in seg_keys})
        for i, seg in enumerate(unique_segs):
            seg_cols[seg] = i

        for (stream, seg), step_list in groups.items():
            step_list.sort(key=lambda s: (s.step_id or 0))
            col = seg_cols[seg]
            base_x = col * (NODE_W + COL_GAP) + 40
            base_y = stream_offsets[stream] + 40
            for row, s in enumerate(step_list):
                x = base_x
                y = base_y + row * (NODE_H + ROW_GAP)
                node = StepNode(s, is_worst=(s.step_id is not None and s.step_id == worst_step_id), click_cb=self._emit_click)
                node.setPos(x, y)
                self._scene.addItem(node)
                self._nodes.append(node)
                if s.step_id is not None:
                    nodes_by_id[s.step_id] = node

        # Edges: within segment, consecutive steps; across segments, connect last → first.
        edges: list[_FlowEdge] = []
        for (stream, seg), step_list in groups.items():
            step_list.sort(key=lambda s: (s.step_id or 0))
            for a, b in zip(step_list, step_list[1:]):
                if a.step_id in nodes_by_id and b.step_id in nodes_by_id:
                    hot = (a.pct_runtime + b.pct_runtime) > 0.3
                    e = _FlowEdge(nodes_by_id[a.step_id], nodes_by_id[b.step_id], hot)
                    self._scene.addItem(e)
                    edges.append(e)

        for stream in streams:
            segs_for_stream = sorted([seg for (st, seg) in seg_keys if st == stream])
            for a_seg, b_seg in zip(segs_for_stream, segs_for_stream[1:]):
                a_steps = groups.get((stream, a_seg), [])
                b_steps = groups.get((stream, b_seg), [])
                if not a_steps or not b_steps:
                    continue
                a_last = max(a_steps, key=lambda s: s.step_id or 0)
                b_first = min(b_steps, key=lambda s: s.step_id or 0)
                if a_last.step_id in nodes_by_id and b_first.step_id in nodes_by_id:
                    hot = (a_last.pct_runtime + b_first.pct_runtime) > 0.3
                    e = _FlowEdge(nodes_by_id[a_last.step_id], nodes_by_id[b_first.step_id], hot)
                    self._scene.addItem(e)
                    edges.append(e)

        worst_node = nodes_by_id.get(worst_step_id) if worst_step_id is not None else None
        self._driver.register(edges, worst_node)

        self._scene.setSceneRect(self._scene.itemsBoundingRect().adjusted(-60, -60, 60, 60))
        self.fitInView(self._scene.sceneRect(), Qt.KeepAspectRatio)
        # Cap zoom after fit so small graphs aren't huge.
        if self.transform().m11() > 1.2:
            self.resetTransform()
            self.scale(1.0, 1.0)

    def _empty_state(self) -> None:
        from PySide6.QtWidgets import QGraphicsTextItem
        t = QGraphicsTextItem("Paste sys_query_details and sys_table_info, then click Analyze.")
        t.setDefaultTextColor(QColor(PALETTE.text_3))
        f = QFont("Inter", 12)
        t.setFont(f)
        self._scene.addItem(t)
        self._scene.setSceneRect(-200, -60, 400, 120)
        t.setPos(-t.boundingRect().width() / 2, -t.boundingRect().height() / 2)

    def _emit_click(self, step: StepEnrichment) -> None:
        self.stepClicked.emit(step)

    def highlight_step(self, step_id: int | None) -> None:
        for n in self._nodes:
            n.setSelected(n.step.step_id == step_id)

    def wheelEvent(self, e: QWheelEvent) -> None:
        factor = 1.15 if e.angleDelta().y() > 0 else 1 / 1.15
        self.scale(factor, factor)
