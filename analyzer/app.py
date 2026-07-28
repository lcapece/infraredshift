"""Main desktop shell for the DuckDB-backed Redshift analyzer."""
from __future__ import annotations
import os
from importlib import resources
from pathlib import Path

from PySide6.QtCore import QEvent, QObject, Qt, QThread, QTimer, Signal, Slot
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import (
    QApplication,
    QAbstractButton,
    QDialog,
    QFrame,
    QMainWindow,
    QProgressDialog,
    QScrollArea,
    QSplitter,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from .cluster_analyze import ClusterReport, REPORT_AREA_LABELS, load_cluster_report, merge_cluster_reports
from .diagnose import DiagnosisReport, StepEnrichment, diagnose
from .providers import PasteProvider, ProviderError
from .settings import load_settings, save_settings
from .theme import PALETTE, get_theme_mode, retint_stylesheet, set_theme_mode
from .widgets.cluster_dashboard import ClusterDashboard, _SqlLensPage
from .widgets.inspector import StepInspector
from .widgets.paste_panel import PastePanel
from .widgets.plan_graph import PlanGraphView
from .widgets.recommendations import Recommendations
from .widgets.step_timeline import StepTimeline
from .widgets.table_diagnostics import TableDiagnostics
from .widgets.query_decomposer import QueryDecomposerPage
from .widgets.title_bar import TitleBar
from .widgets.sql_annotations import SqlAnnotationContextFilter
from .widgets.topology import TopologyPage


def _load_stylesheet() -> str:
    try:
        raw = resources.files(__package__).joinpath("style.qss").read_text(encoding="utf-8")
    except Exception:
        path = Path(__file__).parent / "style.qss"
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError:
            return ""
    return retint_stylesheet(raw)


def _apply_palette(app: QApplication) -> None:
    pal = app.palette()
    for role, color in (
        (QPalette.Window, PALETTE.bg_0),
        (QPalette.WindowText, PALETTE.text_0),
        (QPalette.Base, PALETTE.bg_1),
        (QPalette.AlternateBase, PALETTE.bg_2),
        (QPalette.Text, PALETTE.text_0),
        (QPalette.Button, PALETTE.bg_3),
        (QPalette.ButtonText, PALETTE.text_0),
        (QPalette.Highlight, PALETTE.accent),
        (QPalette.HighlightedText, PALETTE.bg_0 if get_theme_mode() == "dark" else PALETTE.bg_1),
        (QPalette.ToolTipBase, PALETTE.bg_2),
        (QPalette.ToolTipText, PALETTE.text_0),
    ):
        pal.setColor(role, QColor(color))
    for role in (QPalette.WindowText, QPalette.Text, QPalette.ButtonText):
        pal.setColor(QPalette.Disabled, role, QColor(PALETTE.text_3))
    app.setPalette(pal)


def _retint_inline_styles(root: QWidget) -> None:
    widgets = [root]
    widgets.extend(root.findChildren(QWidget))
    for widget in widgets:
        sheet = widget.styleSheet()
        if sheet:
            updated = retint_stylesheet(sheet)
            if updated != sheet:
                widget.setStyleSheet(updated)
        widget.update()
        viewport = getattr(widget, "viewport", None)
        if callable(viewport):
            try:
                viewport().update()
            except Exception:
                pass


def _scroll_guard(widget: QWidget) -> QScrollArea:
    scroll = QScrollArea()
    scroll.setObjectName("PageScrollGuard")
    scroll.setWidgetResizable(True)
    scroll.setFrameShape(QFrame.NoFrame)
    scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
    scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
    scroll.setWidget(widget)
    return scroll


class _ButtonBusyCursorFilter(QObject):
    """Show immediate busy feedback for synchronous button actions.

    The zero-delay restore runs only after the clicked handler returns to the
    event loop. Long-running asynchronous loaders install their own wait cursor
    and therefore remain busy until their worker and UI rendering are complete.
    """

    def __init__(self, parent: QObject | None = None):
        super().__init__(parent)
        self._armed = False

    def eventFilter(self, watched: QObject, event: QEvent) -> bool:
        if (
            isinstance(watched, QAbstractButton)
            and event.type() in (QEvent.MouseButtonRelease, QEvent.KeyRelease)
            and watched.isEnabled()
            and not self._armed
        ):
            self._armed = True
            QApplication.setOverrideCursor(Qt.WaitCursor)
            QTimer.singleShot(0, self._release)
        return False

    def _release(self) -> None:
        if self._armed:
            QApplication.restoreOverrideCursor()
            self._armed = False


class _ClusterLoadWorker(QObject):
    finished = Signal(object)
    failed = Signal(str)
    cancelled = Signal()
    progress = Signal(str, int, int)

    def __init__(self, path: str, areas: object = None):
        super().__init__()
        self._path = path
        self._areas = areas
        self._cancel_requested = False

    @Slot()
    def request_cancel(self) -> None:
        """Cooperative cancel: checked between DuckDB load steps."""
        self._cancel_requested = True

    @Slot()
    def run(self) -> None:
        # Must stay on the worker QThread (moveToThread + started->run).
        try:
            def progress_callback(message: str, current: int, total: int) -> None:
                if self._cancel_requested:
                    raise _ClusterLoadCancelled()
                thread = QThread.currentThread()
                if thread is not None and thread.isInterruptionRequested():
                    self._cancel_requested = True
                    raise _ClusterLoadCancelled()
                self.progress.emit(message, current, total)

            self.finished.emit(
                load_cluster_report(self._path or None, areas=self._areas, progress=progress_callback)
            )
        except _ClusterLoadCancelled:
            self.cancelled.emit()
        except Exception as exc:
            self.failed.emit(str(exc))


class _ClusterLoadCancelled(Exception):
    """Raised when the user cancels a local DuckDB area load mid-flight."""


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        from .brand import WINDOW_TITLE

        self.setWindowTitle(WINDOW_TITLE)
        self.setMinimumSize(720, 520)
        self.resize(1280, 760)

        root = QWidget()
        root.setObjectName("Root")
        root_lay = QVBoxLayout(root)
        root_lay.setContentsMargins(0, 0, 0, 0)
        root_lay.setSpacing(0)

        self._title = TitleBar()
        self._title.set_theme_mode(get_theme_mode())
        self._title.themeModeChanged.connect(self._on_theme_changed)
        self._title.exitRequested.connect(self.close)
        root_lay.addWidget(self._title)

        self._tabs = QTabWidget()
        self._tabs.setAccessibleName("Main workspace tabs")
        self._tabs.setAccessibleDescription(
            "Switch between Load Status, Data Loader, Workload Triage, and "
            "Table Heat Map, and Single Query Analysis."
        )
        self._tabs.tabBar().setUsesScrollButtons(True)
        self._tabs.tabBar().setExpanding(True)
        self._tabs.tabBar().setElideMode(Qt.ElideRight)
        self._tabs.tabBar().setFocusPolicy(Qt.StrongFocus)
        self._cluster = ClusterDashboard()
        self._topology = TopologyPage(self._cluster.active_db_path())
        # One canonical recoverable loader is embedded in the main app. The
        # capture still runs in a separate QProcess, so the tab never freezes.
        from .loader.gui import LoaderWindow

        self._data_loader = LoaderWindow(
            self._cluster.active_db_path(),
            embedded=True,
            credentials_callback=self._cluster._edit_local_credentials,
        )
        self._table_heatmap = self._cluster.table_heatmap_page()
        self._tabs.addTab(_scroll_guard(self._topology), "Load Status")
        self._data_loader_tab = self._tabs.addTab(_scroll_guard(self._data_loader), "Data Loader")
        self._tabs.addTab(_scroll_guard(self._cluster), "Workload Triage")
        self._table_heatmap_tab = self._tabs.addTab(
            _scroll_guard(self._table_heatmap),
            "Table Heat Map",
        )
        self._action_plan = self._cluster.action_plan_page()
        self._action_plan_tab = self._tabs.addTab(
            _scroll_guard(self._action_plan),
            "Fix Queue",
        )
        self._single_query = SingleQueryLab(self._title)
        self._single_query_tab = self._tabs.addTab(
            _scroll_guard(self._single_query),
            "Single Query Analysis",
        )
        for index in range(self._tabs.count()):
            self._tabs.setTabToolTip(index, self._tabs.tabText(index))
        root_lay.addWidget(self._tabs, 1)

        self.setCentralWidget(root)

        self._cluster.reloadRequested.connect(self._start_cluster_reload)
        self._cluster.databasePathChanged.connect(self._topology.set_db_path)
        self._cluster.databasePathChanged.connect(self._data_loader.set_db_path)
        self._cluster.loaderRequested.connect(self._open_data_loader_window)
        self._topology.loaderRequested.connect(self._open_data_loader_window)
        self._tabs.currentChanged.connect(self._on_tab_changed)
        self._last_cluster_report: ClusterReport | None = None
        self._load_thread: QThread | None = None
        self._load_worker: _ClusterLoadWorker | None = None
        self._load_cursor_active = False
        self._load_progress: QProgressDialog | None = None
        self._load_cancel_requested = False
        self._button_busy_filter = _ButtonBusyCursorFilter(self)
        self._sql_annotation_filter = SqlAnnotationContextFilter(self)
        app = QApplication.instance()
        if app is not None:
            app.installEventFilter(self._button_busy_filter)
            app.installEventFilter(self._sql_annotation_filter)

        QTimer.singleShot(0, self._fit_initial_geometry)
        QTimer.singleShot(0, self._cluster.show_idle)
        QTimer.singleShot(0, lambda: _retint_inline_styles(self))

    def _open_data_loader_window(self) -> None:
        # Take the operator to the canonical main-app loader tab.
        self._data_loader.set_db_path(self._cluster.active_db_path())
        self._data_loader.refresh_idle_state()
        self._tabs.setCurrentIndex(self._data_loader_tab)

    def _fit_initial_geometry(self) -> None:
        screen = self.screen() or QApplication.primaryScreen()
        if screen is None:
            return
        available = screen.availableGeometry()
        min_width = min(720, available.width())
        min_height = min(520, available.height())
        self.setMinimumSize(min_width, min_height)
        if available.width() <= 1500 or available.height() <= 900:
            self.setGeometry(available.adjusted(4, 4, -4, -4))
            return
        width = min(1600, max(min_width, int(available.width() * 0.92)))
        height = min(960, max(min_height, int(available.height() * 0.88)))
        width = min(width, available.width())
        height = min(height, available.height())
        self.resize(width, height)
        self.move(
            available.x() + max(0, (available.width() - width) // 2),
            available.y() + max(0, (available.height() - height) // 2),
        )

    def _start_cluster_reload(self, path: str, areas: object = None) -> None:
        if self._load_thread is not None and self._load_thread.isRunning():
            self._cluster.show_area_busy(areas)
            return
        if self._can_reuse_loaded_report(path, areas):
            self._cluster.show_cached(areas)
            return
        self._load_cancel_requested = False
        requested = {
            str(area)
            for area in (areas if isinstance(areas, (list, tuple, set)) else [areas])
            if area
        }
        self._cluster.show_loading(path, areas)
        self._show_load_progress(path, areas)
        if not self._load_cursor_active:
            QApplication.setOverrideCursor(Qt.WaitCursor)
            self._load_cursor_active = True
        thread = QThread(self)
        worker = _ClusterLoadWorker(path, areas)
        worker.moveToThread(thread)
        # Queued: run executes on the worker thread after the event loop starts.
        thread.started.connect(worker.run, Qt.QueuedConnection)
        # Bound methods of this GUI-thread window -> QueuedConnection onto GUI.
        # Do not replace with lambdas that would run on the worker and touch widgets.
        worker.progress.connect(self._on_cluster_load_progress, Qt.QueuedConnection)
        worker.finished.connect(self._on_cluster_loaded, Qt.QueuedConnection)
        worker.failed.connect(self._on_cluster_load_failed, Qt.QueuedConnection)
        worker.cancelled.connect(self._on_cluster_load_cancelled, Qt.QueuedConnection)
        # Quit from the worker thread as soon as work ends (same pattern as topology).
        worker.finished.connect(thread.quit, Qt.DirectConnection)
        worker.failed.connect(thread.quit, Qt.DirectConnection)
        worker.cancelled.connect(thread.quit, Qt.DirectConnection)
        worker.finished.connect(worker.deleteLater)
        worker.failed.connect(worker.deleteLater)
        worker.cancelled.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(lambda t=thread: self._clear_cluster_loader(t))
        self._load_thread = thread
        self._load_worker = worker
        thread.start()

    def _can_reuse_loaded_report(self, path: str, areas: object) -> bool:
        report = self._last_cluster_report
        if report is None or not report.snapshot_id:
            return False
        current_scope = tuple(dict.fromkeys(load_settings().analysis_namespace_filter))
        if current_scope != report.analysis_namespace_scope:
            return False
        requested = {str(area) for area in (areas if isinstance(areas, (list, tuple, set)) else [areas]) if area}
        if not requested or "all" in requested or not requested.issubset(set(report.loaded_areas)):
            return False
        # An empty Table Review must run again so its direct source-table count
        # can distinguish a genuinely empty database from a snapshot/view load
        # mismatch. Do not turn the user's Load click into an instant cache hit.
        if "table_review" in requested and report.table_review.empty:
            return False
        # A successful zero-row insight load must remain retryable. Otherwise
        # the page-level Load Insights button appears to do nothing because the
        # report cache short-circuits the DuckDB query.
        if "insights" in requested and report.insights.empty:
            return False
        if "slow_queries" in requested and report.slow_queries.empty:
            return False
        # This area is lightweight and its explicit button says Reload; always
        # re-read the local table-info view instead of reporting a cache hit.
        if "table_heatmap" in requested:
            return False
        if "external_tables" in requested:
            return False
        try:
            target = Path(path or report.db_path).resolve()
            if target != Path(report.db_path).resolve() or not target.is_file():
                return False
            import duckdb

            con = duckdb.connect(str(target), read_only=True)
            try:
                row = con.execute(
                    "SELECT snapshot_id FROM snapshot_runs "
                    "ORDER BY CASE WHEN LOWER(COALESCE(source, '')) = "
                    "'external-table-loader' THEN 1 ELSE 0 END, captured_at DESC LIMIT 1"
                ).fetchone()
            finally:
                con.close()
        except Exception:
            return False
        return bool(row and str(row[0]) == str(report.snapshot_id))

    def _on_cluster_loaded(self, report: ClusterReport) -> None:
        if self._load_cancel_requested:
            return
        # The background worker is DONE — its rows are loaded. Close the
        # cancellable progress dialog BEFORE rendering: rendering runs on the
        # GUI thread and cannot be cancelled, and a Cancel click landing during
        # a mid-render processEvents() would re-entrantly delete the dialog and
        # crash. Show a plain wait cursor for the render instead.
        self._finish_load_progress("DuckDB rows loaded; rendering ...")
        self._rendering = True
        if not self._load_cursor_active:
            QApplication.setOverrideCursor(Qt.WaitCursor)
            self._load_cursor_active = True
        try:
            import time as _t

            def _step(label, fn):
                # Time each render step and print it, so the step responsible
                # for a long freeze is identified from the real data volume
                # (set REDSHIFT_ANALYZER_RENDER_TIMING=0 to silence).
                start = _t.perf_counter()
                fn()
                elapsed = _t.perf_counter() - start
                if str(os.environ.get("REDSHIFT_ANALYZER_RENDER_TIMING", "1")).strip() != "0":
                    print(f"[render] {label}: {elapsed:.2f}s", flush=True)

            _step("merge_cluster_reports", lambda: setattr(
                self, "_last_cluster_report",
                merge_cluster_reports(self._last_cluster_report, report),
            ))
            report = self._last_cluster_report
            self._topology.set_db_path(str(report.db_path))
            _step("Workload Triage", lambda: self._cluster.set_report(report))
            _step(
                "Single Query Analysis",
                lambda: self._single_query.set_cluster_report(report),
            )
            self._title.update_cluster_metrics(report.summary, report.rule_count)
        except Exception as exc:
            self._cluster.show_error(f"Loaded DuckDB rows, but the screen could not render them: {exc}")
        finally:
            self._rendering = False
            self._restore_load_cursor()

    def _on_cluster_load_failed(self, message: str) -> None:
        if self._load_cancel_requested:
            return
        self._finish_load_progress("Local DuckDB load failed.")
        self._cluster.show_error(message)

    def _clear_cluster_loader(self, thread: QThread) -> None:
        if self._load_thread is thread:
            self._load_thread = None
            self._load_worker = None
            self._load_cancel_requested = False
        if self._load_cursor_active:
            QApplication.restoreOverrideCursor()
            self._load_cursor_active = False

    def _show_load_progress(self, path: str, areas: object = None) -> None:
        self._finish_load_progress()
        target = path or "default DuckDB path"
        area_label = self._format_area_label(areas)
        dialog = QProgressDialog("Preparing local DuckDB load ...", "Cancel", 0, 0, self)
        dialog.setWindowTitle("Loading Local DuckDB")
        dialog.setWindowModality(Qt.WindowModal)
        dialog.setMinimumDuration(0)
        dialog.setAutoClose(False)
        dialog.setAutoReset(False)
        dialog.setLabelText(f"Preparing local DuckDB load\nArea: {area_label}\n{target}")
        dialog.canceled.connect(self._cancel_cluster_load)
        dialog.show()
        dialog.raise_()
        dialog.activateWindow()
        self._load_progress = dialog
        QApplication.processEvents()

    def _cancel_cluster_load(self) -> None:
        self._load_cancel_requested = True
        if self._load_worker is not None:
            self._load_worker.request_cancel()
        if self._load_thread is not None:
            self._load_thread.requestInterruption()
        # Do not tear the UI down here: the worker may still be mid-query.
        # _on_cluster_load_cancelled (or a late finished/failed guard) finishes
        # the dialog once the background thread cooperatively stops.
        if self._load_progress is not None:
            self._load_progress.setLabelText("Cancel requested — stopping after the current DuckDB step ...")

    def _on_cluster_load_cancelled(self) -> None:
        self._finish_load_progress("Local DuckDB load canceled.")
        self._cluster.show_idle()
        self._load_cancel_requested = False

    def _format_area_label(self, areas: object = None) -> str:
        if areas is None:
            return REPORT_AREA_LABELS.get("all", "Safe Areas")
        if isinstance(areas, str):
            keys = [areas]
        else:
            try:
                keys = [str(area) for area in areas]  # type: ignore[arg-type]
            except TypeError:
                keys = [str(areas)]
        labels = [REPORT_AREA_LABELS.get(key, key) for key in keys if key]
        return ", ".join(labels) if labels else REPORT_AREA_LABELS.get("all", "Safe Areas")

    def _on_cluster_load_progress(self, message: str, current: int, total: int) -> None:
        dialog = self._load_progress
        if dialog is None or self._load_cancel_requested:
            return
        total = max(int(total or 0), 1)
        current = max(0, min(int(current or 0), total))
        pct = int(current * 100 / total)
        # QProgressDialog.setValue() can process queued GUI events.  A queued
        # completion may clear self._load_progress re-entrantly, so finish all
        # dialog mutations through this stable local reference and set the
        # label before the potentially re-entrant value update.
        dialog.setRange(0, 100)
        # Sub-indicators (e.g. "150 of 500 query(s)") live in *message*; keep
        # the step line separate so the dialog never looks stuck on a bare step.
        dialog.setLabelText(f"{message}\nStep {current} of {total}")
        dialog.setValue(pct)
        # Do not processEvents here: re-entrancy starts nested loads / tears down
        # the dialog while the worker is still running (looks like "threads died").

    def _finish_load_progress(self, message: str = "") -> None:
        self._restore_load_cursor()
        dialog = self._load_progress
        if dialog is None:
            return
        # Detach first so any event processing inside QProgressDialog cannot
        # re-enter cleanup against the same object or let a late progress
        # signal mutate a dialog that is already closing.
        self._load_progress = None
        if message:
            dialog.setLabelText(message)
            dialog.setValue(100)
            QApplication.processEvents()
        try:
            dialog.canceled.disconnect(self._cancel_cluster_load)
        except (TypeError, RuntimeError):
            pass
        dialog.close()
        dialog.deleteLater()

    def _restore_load_cursor(self) -> None:
        if self._load_cursor_active:
            QApplication.restoreOverrideCursor()
            self._load_cursor_active = False

    def _on_tab_changed(self, index: int) -> None:
        text = self._tabs.tabText(index)
        if text == "Load Status":
            self._topology.refresh()
        elif text == "Workload Triage" and self._last_cluster_report:
            self._title.update_cluster_metrics(
                self._last_cluster_report.summary,
                self._last_cluster_report.rule_count,
            )
        elif text == "Table Heat Map":
            self._cluster.load_table_heatmap_if_needed()

    def _on_theme_changed(self, mode: str) -> None:
        applied = set_theme_mode(mode)
        app = QApplication.instance()
        if app is not None:
            _apply_palette(app)
            app.setStyleSheet(_load_stylesheet())
        _retint_inline_styles(self)
        self._title.set_theme_mode(applied)
        # Force paint-time consumers (charts, title metrics) to redraw with
        # the live palette after a theme toggle.
        self.update()
        try:
            settings = load_settings()
            settings.ui_theme = applied
            save_settings(settings)
        except Exception:
            pass


class SingleQueryLab(QWidget):
    """Featured query decomposition plus one-query execution forensics."""

    def __init__(self, title: TitleBar, parent=None):
        super().__init__(parent)
        self._title = title
        self._provider = PasteProvider()
        self._last_report: DiagnosisReport | None = None

        root_lay = QVBoxLayout(self)
        root_lay.setContentsMargins(0, 0, 0, 0)
        root_lay.setSpacing(0)

        self._mode_tabs = QTabWidget()
        self._mode_tabs.tabBar().setUsesScrollButtons(True)
        self._mode_tabs.tabBar().setElideMode(Qt.ElideRight)

        self._query_decomposer = QueryDecomposerPage()
        self._query_decomposer_tab = self._mode_tabs.addTab(
            _scroll_guard(self._query_decomposer),
            "Query Decomposer",
        )
        self._sql_lens = _SqlLensPage()
        self._one_off_sql_tab = self._mode_tabs.addTab(
            _scroll_guard(self._sql_lens),
            "One-Off SQL",
        )

        body = QSplitter(Qt.Horizontal)
        body.setChildrenCollapsible(False)
        body.setHandleWidth(1)

        sidebar = QFrame()
        sidebar.setObjectName("Sidebar")
        sidebar.setMinimumWidth(360)
        sidebar.setMaximumWidth(480)
        sb_lay = QVBoxLayout(sidebar)
        sb_lay.setContentsMargins(0, 0, 0, 0)
        self._paste = PastePanel(self._provider)
        sb_lay.addWidget(self._paste)
        body.addWidget(sidebar)

        center = QWidget()
        c_lay = QVBoxLayout(center)
        c_lay.setContentsMargins(12, 12, 12, 12)
        c_lay.setSpacing(8)

        self._tabs = QTabWidget()
        self._plan = PlanGraphView()
        self._timeline = StepTimeline()
        self._diagnostics = TableDiagnostics()
        self._recs = Recommendations()

        self._tabs.addTab(self._plan, "Plan Flow")
        self._tabs.addTab(self._wrap_scroll(self._timeline), "Step Timeline")
        self._tabs.addTab(self._diagnostics, "Table Health")
        self._tabs.addTab(self._recs, "Findings")
        self._tabs.tabBar().setUsesScrollButtons(True)
        self._tabs.tabBar().setElideMode(Qt.ElideRight)
        for index in range(self._tabs.count()):
            self._tabs.setTabToolTip(index, self._tabs.tabText(index))
        c_lay.addWidget(self._tabs, 1)
        body.addWidget(center)

        self._inspector = StepInspector()
        inspector_host = QFrame()
        inspector_host.setObjectName("Sidebar")
        ins_lay = QVBoxLayout(inspector_host)
        ins_lay.setContentsMargins(0, 0, 0, 0)
        ins_lay.addWidget(self._inspector)
        body.addWidget(inspector_host)

        body.setStretchFactor(0, 0)
        body.setStretchFactor(1, 1)
        body.setStretchFactor(2, 0)
        body.setSizes([400, 1000, 340])
        self._plan_paste_tab = self._mode_tabs.addTab(
            _scroll_guard(body),
            "Plan Paste",
        )
        for index in range(self._mode_tabs.count()):
            self._mode_tabs.setTabToolTip(index, self._mode_tabs.tabText(index))
        root_lay.addWidget(self._mode_tabs, 1)

        self._paste.analyzeRequested.connect(self._on_analyze)
        self._paste.cleared.connect(self._on_clear)
        self._plan.stepClicked.connect(self._on_step_selected)
        self._timeline.stepClicked.connect(self._on_step_selected)
        self._query_decomposer.useOneOffSqlRequested.connect(self._copy_one_off_to_decomposer)
        self._mode_tabs.currentChanged.connect(self._on_mode_tab_changed)

    def set_cluster_report(self, report: ClusterReport | None) -> None:
        if report is None:
            return
        self._sql_lens.set_context(report.table_review, report.slow_queries, report.view_definitions)
        self._query_decomposer.set_cluster_report(report)

    def has_sql_context(self) -> bool:
        return self._sql_lens.has_context()

    def _copy_one_off_to_decomposer(self) -> None:
        self._query_decomposer.set_sql(self._sql_lens.sql_text())

    def _on_mode_tab_changed(self, index: int) -> None:
        if (
            index == self._query_decomposer_tab
            and not self._query_decomposer.sql_text()
        ):
            self._copy_one_off_to_decomposer()

    def _wrap_scroll(self, w: QWidget) -> QWidget:
        return _scroll_guard(w)

    def _on_analyze(self) -> None:
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self.setEnabled(False)
        QApplication.processEvents()
        try:
            snap = self._provider.load()
            report = diagnose(snap)
            self._last_report = report
            self._paste.show_parse_notes(snap.parse_notes)
            self._plan.load_steps(report.steps, report.worst_step_id)
            self._timeline.set_data(report.steps, report.total_runtime_ms)
            self._diagnostics.set_dataframe(snap.table_info)
            self._recs.set_findings(report.findings)
            self._title.update_metrics(
                qid=snap.query_id,
                runtime_ms=report.total_runtime_ms,
                steps=len(report.steps),
                tables=report.tables_touched,
                findings=len(report.findings),
            )
            if report.worst_step_id is not None:
                worst = next((s for s in report.steps if s.step_id == report.worst_step_id), None)
                if worst:
                    self._on_step_selected(worst)
            self._tabs.setCurrentIndex(0)
        except ProviderError as e:
            self._paste.show_error(str(e))
        finally:
            self.setEnabled(True)
            QApplication.restoreOverrideCursor()

    def _on_clear(self) -> None:
        self._plan.clear()
        self._timeline.set_data([], 0)
        self._diagnostics.set_dataframe(None)
        self._recs.set_findings([])
        self._title.update_metrics(None, 0, 0, 0, 0)
        self._inspector._empty_state()

    def _on_step_selected(self, step: StepEnrichment) -> None:
        self._inspector.show_step(step)
        self._plan.highlight_step(step.step_id)
        self._timeline.highlight_step(step.step_id)


def _warm_sqlglot() -> None:
    # sqlglot's first import can take a minute on locked-down machines
    # (bytecode compilation + antivirus). It is lazy-imported everywhere,
    # so warm it in the background once the window is up, keeping the
    # first SQL analysis from stalling the UI.
    import threading

    def _import() -> None:
        try:
            import sqlglot  # noqa: F401
        except Exception:
            pass

    threading.Thread(target=_import, name="sqlglot-warmup", daemon=True).start()


def run() -> int:
    from .bootstrap import bootstrap_application

    bootstrap_application()
    app = QApplication.instance() or QApplication([])
    app.setStyle("Fusion")
    try:
        set_theme_mode(load_settings().ui_theme)
    except Exception:
        set_theme_mode("light")
    _apply_palette(app)
    app.setStyleSheet(_load_stylesheet())
    from .widgets.login_dialog import LoginDialog

    login = LoginDialog()
    if login.exec() != QDialog.Accepted:
        return 0
    QApplication.setOverrideCursor(Qt.WaitCursor)
    try:
        w = MainWindow()
        w.show()
        QApplication.processEvents()
    finally:
        QApplication.restoreOverrideCursor()
    QTimer.singleShot(0, _warm_sqlglot)
    try:
        return app.exec()
    finally:
        # Decrypted credentials must not outlive the desktop session.
        from .secrets_store import clear_session_secrets

        clear_session_secrets()


if __name__ == "__main__":
    raise SystemExit(run())
