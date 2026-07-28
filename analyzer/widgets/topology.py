"""Simple cluster load-status page (first tab).

Shows the producer cluster and its three sibling clusters that read from it.
A selector across the top picks the cluster; below it, one row per dataset
with a small progress bar. No canvas, no cards, no pop-up dialogs — the
status check runs on a background thread so the UI never blocks.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from PySide6.QtCore import QObject, QRectF, Qt, QThread, QTimer, Signal
from PySide6.QtGui import QColor, QPainter
from PySide6.QtWidgets import (
    QButtonGroup,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from ..topology import ClusterStatus, REQUIRED_DATASETS, TopologySnapshot, load_topology_snapshot


# Hard-coded display labels: the producer and its three sibling clusters.
# The friendly names in the cluster-profiles JSON are display-only; these
# labels are matched against them so users recognize each cluster.
DISPLAY_LABELS = ("Producer", "Consumer", "Commercial", "FAR")

# The three database-dependent catalog datasets that load cyclically
# per database on every cluster.
_CYCLICAL_TABLES = {"svv_table_info_all", "view_definitions", "procedure_definitions"}

_STATE_COLORS = {
    "ready": ("#23864B", "Loaded"),
    "empty": ("#B77900", "Waiting"),
    "missing": ("#C53A3A", "Missing"),
}

_SEVERITY_TEXT = {
    "complete": "Complete",
    "amber": "Partial",
    "red": "Incomplete",
    "disabled": "Excluded",
}


def _short_count(value: int) -> str:
    number = int(value or 0)
    if number >= 1_000_000_000:
        return f"{number / 1_000_000_000:.1f}B"
    if number >= 1_000_000:
        return f"{number / 1_000_000:.1f}M"
    if number >= 1_000:
        return f"{number / 1_000:.1f}K"
    return f"{number:,}"


def _assign_display_labels(
    clusters: tuple[ClusterStatus, ...],
) -> list[tuple[str, ClusterStatus | None]]:
    """Map the hard-coded labels onto assessed clusters by friendly name.

    The producer maps by role. Siblings map when their configured friendly
    name contains the label; any still-unmatched labels take the remaining
    clusters in order so real data is never hidden by a naming mismatch.
    Clusters beyond the four known labels are appended under their own name.
    """
    remaining = list(clusters)
    assigned: dict[str, ClusterStatus | None] = {}
    producer = next((item for item in remaining if item.role == "producer"), None)
    if producer is not None:
        remaining.remove(producer)
    assigned["Producer"] = producer
    for label in DISPLAY_LABELS[1:]:
        token = label.casefold()
        match = next(
            (item for item in remaining if token in item.friendly_name.casefold()),
            None,
        )
        if match is not None:
            remaining.remove(match)
        assigned[label] = match
    for label in DISPLAY_LABELS[1:]:
        if assigned[label] is None and remaining:
            assigned[label] = remaining.pop(0)
    result = [(label, assigned[label]) for label in DISPLAY_LABELS]
    result.extend((extra.friendly_name, extra) for extra in remaining)
    return result


def _coverage_gaps(buckets: tuple[int, ...]) -> list[tuple[int, int]]:
    """Runs of two or more empty buckets — real capture holes, not quiet hours."""
    gaps: list[tuple[int, int]] = []
    index, count = 0, len(buckets)
    while index < count:
        if buckets[index] == 0:
            end = index
            while end < count and buckets[end] == 0:
                end += 1
            if end - index >= 2:
                gaps.append((index, end))
            index = end
        else:
            index += 1
    return gaps


class _CoverageBar(QWidget):
    """Wide horizontal bar of the capture window: filled where queries exist,
    red where a resume left a hole in the middle."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self._buckets: tuple[int, ...] = ()
        self._gaps: list[tuple[int, int]] = []
        self.setFixedHeight(26)
        self.setMinimumWidth(320)

    def set_coverage(self, buckets: tuple[int, ...]) -> None:
        self._buckets = tuple(buckets)
        self._gaps = _coverage_gaps(self._buckets)
        self.update()

    def paintEvent(self, event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setPen(Qt.NoPen)
        track = QRectF(0, 4, self.width(), 18)
        painter.setBrush(QColor("#DDE3E7"))
        painter.drawRoundedRect(track, 5, 5)
        count = len(self._buckets)
        if not count:
            return
        width = self.width() / count
        gap_buckets = {b for start, end in self._gaps for b in range(start, end)}
        for index, value in enumerate(self._buckets):
            if value > 0:
                painter.setBrush(QColor("#23864B"))
            elif index in gap_buckets:
                painter.setBrush(QColor("#C53A3A"))
            else:
                continue  # single quiet bucket: leave as neutral track
            painter.drawRect(QRectF(index * width, 4, width + 0.5, 18))


class _MiniBar(QWidget):
    """A small horizontal bar showing dataset load progress."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self._fraction = 0.0
        self._color = QColor("#23864B")
        self.setFixedHeight(12)
        self.setMinimumWidth(140)

    def set_value(self, fraction: float, color: str) -> None:
        self._fraction = max(0.0, min(1.0, float(fraction)))
        self._color = QColor(color)
        self.update()

    def paintEvent(self, event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        track = QRectF(0, 2, self.width(), 8)
        painter.setPen(Qt.NoPen)
        painter.setBrush(QColor("#DDE3E7"))
        painter.drawRoundedRect(track, 4, 4)
        if self._fraction > 0:
            fill = QRectF(0, 2, max(8.0, self.width() * self._fraction), 8)
            painter.setBrush(self._color)
            painter.drawRoundedRect(fill, 4, 4)


class _StatusWorker(QObject):
    finished = Signal(object)
    failed = Signal(str)

    def __init__(self, db_path: str):
        super().__init__()
        self._db_path = db_path

    def run(self) -> None:
        try:
            # Load portable JSON/.env values before reading the friendly names.
            try:
                from ..ingest_redshift import load_dotenv

                load_dotenv(None)
            except Exception:
                pass
            self.finished.emit(load_topology_snapshot(self._db_path))
        except Exception as exc:
            self.failed.emit(str(exc))


class TopologyPage(QWidget):
    pathChanged = Signal(str)
    loaderRequested = Signal()

    def __init__(self, db_path: str = "", parent: QWidget | None = None):
        super().__init__(parent)
        self._db_path = str(db_path or "")
        self._snapshot: TopologySnapshot | None = None
        self._labeled: list[tuple[str, ClusterStatus | None]] = []
        self._thread: QThread | None = None
        self._worker: _StatusWorker | None = None

        root = QVBoxLayout(self)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(10)

        heading_row = QHBoxLayout()
        heading = QLabel("CLUSTER LOAD STATUS")
        heading.setObjectName("SectionHeader")
        heading_row.addWidget(heading, 1)
        self._open_loader = QPushButton("Open Data Loader")
        self._open_loader.setObjectName("Primary")
        self._open_loader.setToolTip(
            "Load or resume producer and sibling cluster data, review counts, and promote staging."
        )
        self._open_loader.clicked.connect(self.loaderRequested.emit)
        heading_row.addWidget(self._open_loader)
        self._refresh = QPushButton("Refresh Status")
        self._refresh.clicked.connect(self.refresh)
        heading_row.addWidget(self._refresh)
        root.addLayout(heading_row)

        description = QLabel(
            "The Producer is the primary cluster; Consumer, Commercial, and FAR are its "
            "siblings that read data off of it. All four are captured so their SQL patterns "
            "can inform producer table designs and surface bad queries worth correcting. "
            "Every cluster loads the identical datasets; the three database-dependent catalog "
            "datasets (table information, view definitions, stored procedures) load cyclically "
            "per database."
        )
        description.setWordWrap(True)
        description.setObjectName("Caption")
        root.addWidget(description)

        selector_row = QHBoxLayout()
        selector_row.setSpacing(6)
        self._selector = QButtonGroup(self)
        self._selector.setExclusive(True)
        self._selector_buttons: list[QPushButton] = []
        self._selector_host = QHBoxLayout()
        selector_row.addLayout(self._selector_host)
        selector_row.addStretch(1)
        root.addLayout(selector_row)
        self._selector.idClicked.connect(self._render_selected)

        self._cluster_line = QLabel("")
        self._cluster_line.setWordWrap(True)
        root.addWidget(self._cluster_line)

        # Capture-coverage bar: earliest query on the left edge, most recent
        # on the right, red segments where a late resume left a hole.
        self._coverage_bar = _CoverageBar()
        root.addWidget(self._coverage_bar)
        coverage_row = QHBoxLayout()
        self._coverage_first = QLabel("")
        self._coverage_first.setObjectName("Caption")
        coverage_row.addWidget(self._coverage_first, 0, Qt.AlignLeft)
        self._coverage_summary = QLabel("")
        self._coverage_summary.setObjectName("Caption")
        self._coverage_summary.setAlignment(Qt.AlignCenter)
        coverage_row.addWidget(self._coverage_summary, 1)
        self._coverage_last = QLabel("")
        self._coverage_last.setObjectName("Caption")
        coverage_row.addWidget(self._coverage_last, 0, Qt.AlignRight)
        root.addLayout(coverage_row)

        grid_host = QWidget()
        self._grid = QGridLayout(grid_host)
        self._grid.setContentsMargins(0, 4, 0, 4)
        self._grid.setHorizontalSpacing(12)
        self._grid.setVerticalSpacing(6)
        self._grid.setColumnStretch(1, 1)
        root.addWidget(grid_host)
        root.addStretch(1)

        self._path_label = QLabel()
        self._path_label.setObjectName("Mono")
        self._path_label.setWordWrap(True)
        root.addWidget(self._path_label)
        self._status = QLabel("Load status has not been checked yet.")
        self._status.setObjectName("Caption")
        self._status.setWordWrap(True)
        root.addWidget(self._status)

        QTimer.singleShot(0, self.refresh)

    def set_db_path(self, path: str, *, refresh: bool = True) -> None:
        path = str(path or "")
        changed = path != self._db_path
        self._db_path = path
        if changed:
            self.pathChanged.emit(path)
        if refresh:
            self.refresh()

    def refresh(self) -> None:
        if self._thread is not None and self._thread.isRunning():
            return
        self._refresh.setEnabled(False)
        self._status.setText("Checking local warehouse load status …")
        thread = QThread(self)
        worker = _StatusWorker(self._db_path)
        worker.moveToThread(thread)
        thread.started.connect(worker.run, Qt.QueuedConnection)
        worker.finished.connect(self._on_snapshot, Qt.QueuedConnection)
        worker.failed.connect(self._on_failed, Qt.QueuedConnection)
        # Direct connections: quit the thread from the worker itself so it
        # always exits, even when no GUI event loop is running (tests).
        worker.finished.connect(thread.quit, Qt.DirectConnection)
        worker.failed.connect(thread.quit, Qt.DirectConnection)
        worker.finished.connect(worker.deleteLater)
        worker.failed.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(lambda t=thread: self._clear_worker(t))
        self._thread, self._worker = thread, worker
        thread.start()

    def _clear_worker(self, thread: QThread) -> None:
        if self._thread is thread:
            self._thread = None
            self._worker = None
        self._refresh.setEnabled(True)

    def closeEvent(self, event) -> None:
        # Never let a live status thread outlive the page (test teardown or
        # app shutdown would otherwise destroy a running QThread).
        thread = self._thread
        if thread is not None and thread.isRunning():
            thread.quit()
            thread.wait(3000)
        super().closeEvent(event)

    def _on_failed(self, message: str) -> None:
        self._status.setText(f"Load status check failed: {message}")

    def _on_snapshot(self, snapshot: TopologySnapshot) -> None:
        self._snapshot = snapshot
        self._labeled = _assign_display_labels(snapshot.clusters)
        self._path_label.setText(f"Local DuckDB: {snapshot.db_path or 'not selected'}")

        selected = max(0, self._selector.checkedId())
        for button in self._selector_buttons:
            self._selector.removeButton(button)
            button.deleteLater()
        self._selector_buttons = []
        for index, (label, cluster) in enumerate(self._labeled):
            ready = f" · {cluster.ready_dataset_count}/{len(cluster.datasets)}" if cluster else ""
            button = QPushButton(f"{label}{ready}")
            button.setCheckable(True)
            button.setChecked(index == min(selected, len(self._labeled) - 1))
            self._selector.addButton(button, index)
            self._selector_host.addWidget(button)
            self._selector_buttons.append(button)

        complete = sum(c.severity == "complete" for c in snapshot.clusters)
        text = (
            f"Assessed {len(snapshot.clusters)} cluster(s); {complete} complete. "
            f"Updated {snapshot.assessed_at}."
        )
        if snapshot.error:
            text += f"  Source warning: {snapshot.error}"
        self._status.setText(text)
        self._render_selected(self._selector.checkedId())

    def _render_selected(self, index: int) -> None:
        while self._grid.count():
            item = self._grid.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        if not self._labeled:
            return
        index = min(max(0, index), len(self._labeled) - 1)
        label, cluster = self._labeled[index]
        if cluster is None:
            self._cluster_line.setText(
                f"{label}: no matching cluster profile or captured data was found. "
                "Open the Data Loader to configure and load it."
            )
            self._render_coverage(None)
            return
        role = "Producer (primary)" if cluster.role == "producer" else "Reads from Producer"
        severity = _SEVERITY_TEXT.get(cluster.severity, "Incomplete")
        self._cluster_line.setText(
            f"{label} — {role} — namespace {cluster.namespace_id or 'MISSING'} — "
            f"{cluster.ready_dataset_count}/{len(cluster.datasets)} datasets ready — "
            f"{_short_count(cluster.query_count)} queries in the 7-day window — {severity}. "
            f"{cluster.severity_reason}"
        )
        self._render_coverage(cluster)
        # Bars are scaled against the largest row count for the same dataset
        # across all clusters, so sibling progress reads relative to the
        # producer (the large one).
        peaks: dict[str, int] = {}
        for _label, item in self._labeled:
            if item is None:
                continue
            for status in item.datasets:
                table = status.spec.table_name
                peaks[table] = max(peaks.get(table, 0), status.row_count)
        for row, status in enumerate(cluster.datasets):
            color, state_text = _STATE_COLORS.get(status.state, _STATE_COLORS["missing"])
            cyclical = status.spec.table_name in _CYCLICAL_TABLES
            name = QLabel(
                f"{status.spec.label}" + ("  (cycles per database)" if cyclical else "")
            )
            bar = _MiniBar()
            peak = peaks.get(status.spec.table_name, 0)
            fraction = (status.row_count / peak) if peak > 0 else 0.0
            bar.set_value(fraction, color)
            rows_label = QLabel(_short_count(status.row_count))
            rows_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
            state = QLabel(state_text)
            state.setStyleSheet(f"color:{color}; font-weight:700;")
            tooltip = (
                f"{status.spec.label}\nDuckDB table: {status.spec.table_name}\n"
                f"Namespace rows: {status.row_count:,}\nState: {status.state}"
                + ("\nLoads cyclically per database" if cyclical else "")
            )
            for widget in (name, bar, rows_label, state):
                widget.setToolTip(tooltip)
            self._grid.addWidget(name, row, 0)
            self._grid.addWidget(bar, row, 1)
            self._grid.addWidget(rows_label, row, 2)
            self._grid.addWidget(state, row, 3)

    def _render_coverage(self, cluster: ClusterStatus | None) -> None:
        if cluster is None or not cluster.coverage_buckets:
            self._coverage_bar.set_coverage(())
            self._coverage_first.setText("")
            self._coverage_last.setText("")
            self._coverage_summary.setText(
                "" if cluster is None else "No captured query timestamps yet."
            )
            self._coverage_bar.setToolTip("")
            return
        buckets = cluster.coverage_buckets
        self._coverage_bar.set_coverage(buckets)
        first_text = str(cluster.first_query_at or "")[:16]
        last_text = str(cluster.last_query_at or "")[:16]
        self._coverage_first.setText(f"Earliest: {first_text}")
        self._coverage_last.setText(f"Latest: {last_text}")
        gaps = _coverage_gaps(buckets)
        if not gaps:
            self._coverage_summary.setText("Continuous capture — no gaps detected.")
            self._coverage_bar.setToolTip(
                f"Query capture is continuous from {first_text} to {last_text}."
            )
            return
        gap_lines: list[str] = []
        try:
            start = datetime.fromisoformat(str(cluster.first_query_at))
            end = datetime.fromisoformat(str(cluster.last_query_at))
            span = end - start
            for gap_start, gap_end in gaps:
                gap_from = start + span * (gap_start / len(buckets))
                gap_to = start + span * (gap_end / len(buckets))
                hours = (gap_to - gap_from) / timedelta(hours=1)
                gap_lines.append(
                    f"{gap_from:%Y-%m-%d %H:%M} → {gap_to:%Y-%m-%d %H:%M} (~{hours:.0f}h)"
                )
        except ValueError:
            gap_lines = [f"{len(gaps)} gap(s) detected"]
        self._coverage_summary.setText(
            f"⚠ {len(gaps)} capture gap(s) — load again sooner to fill the window."
        )
        self._coverage_bar.setToolTip("Capture gaps:\n" + "\n".join(gap_lines))

    @property
    def snapshot(self) -> TopologySnapshot | None:
        return self._snapshot
