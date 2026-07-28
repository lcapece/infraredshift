"""One universal right-click annotation action for SQL text surfaces."""
from __future__ import annotations

from PySide6.QtCore import QByteArray, QBuffer, QEvent, QIODevice, QObject, Qt
from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QLabel,
    QMessageBox,
    QPlainTextEdit,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from ..sql_annotations import SqlAnnotation, save_annotation


def _string_property(widget: QWidget, name: str) -> str:
    current: QWidget | None = widget
    while current is not None:
        value = current.property(name)
        if value is not None and str(value).strip():
            return str(value).strip()
        current = current.parentWidget()
    return ""


def _window_title(widget: QWidget) -> str:
    window = widget.window()
    return window.windowTitle().strip() if window is not None else ""


class SqlAnnotationDialog(QDialog):
    def __init__(self, source: QPlainTextEdit | QTextEdit, parent=None):
        super().__init__(parent or source.window())
        self.setWindowTitle("Add SQL Annotation")
        self.setMinimumSize(560, 390)
        self._source = source
        self._screenshot: QPixmap | None = None

        root = QVBoxLayout(self)
        root.setContentsMargins(18, 16, 18, 16)
        title = QLabel("Add an analysis note")
        title.setObjectName("H1")
        root.addWidget(title)
        context = source.textCursor().selectedText().replace("\u2029", "\n").strip()
        context_label = QLabel(
            f"Selected SQL: {context[:180]}{'…' if len(context) > 180 else ''}"
            if context else
            "No SQL is selected. The current SQL window will be saved as context."
        )
        context_label.setObjectName("Caption")
        context_label.setWordWrap(True)
        root.addWidget(context_label)

        self._note = QPlainTextEdit()
        self._note.setPlaceholderText("Write your annotation…")
        self._note.setMinimumHeight(170)
        root.addWidget(self._note, 1)

        self._attach = QCheckBox("Attach a screenshot of the current application window")
        self._attach.toggled.connect(self._capture_screenshot)
        root.addWidget(self._attach)
        self._screenshot_status = QLabel("No screenshot attached")
        self._screenshot_status.setObjectName("Caption")
        root.addWidget(self._screenshot_status)

        buttons = QDialogButtonBox(QDialogButtonBox.Cancel | QDialogButtonBox.Save)
        buttons.accepted.connect(self._accept_if_valid)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)
        self._note.setFocus()

    def _capture_screenshot(self, enabled: bool) -> None:
        if not enabled:
            self._screenshot = None
            self._screenshot_status.setText("No screenshot attached")
            return
        window = self._source.window()
        pixmap = window.grab() if window is not None else QPixmap()
        if pixmap.isNull():
            self._attach.setChecked(False)
            self._screenshot_status.setText("Screenshot could not be captured")
            return
        self._screenshot = pixmap
        self._screenshot_status.setText(
            f"Screenshot attached • {pixmap.width()} × {pixmap.height()} PNG"
        )

    def _accept_if_valid(self) -> None:
        if not self._note.toPlainText().strip():
            QMessageBox.information(self, "Annotation", "Enter a note before saving.")
            return
        self.accept()

    def annotation(self) -> SqlAnnotation:
        cursor = self._source.textCursor()
        selected = cursor.selectedText().replace("\u2029", "\n").strip()
        png: bytes | None = None
        width = height = 0
        if self._screenshot is not None:
            raw = QByteArray()
            buffer = QBuffer(raw)
            buffer.open(QIODevice.WriteOnly)
            self._screenshot.save(buffer, "PNG")
            buffer.close()
            png = bytes(raw)
            width, height = self._screenshot.width(), self._screenshot.height()
        return SqlAnnotation(
            note=self._note.toPlainText().strip(),
            selected_sql=selected,
            surrounding_sql=self._source.toPlainText(),
            context_title=_window_title(self._source),
            source_widget=self._source.objectName() or type(self._source).__name__,
            query_id=_string_property(self._source, "query_id"),
            repeat_group_id=_string_property(self._source, "repeat_group_id"),
            namespace_id=_string_property(self._source, "namespace_id"),
            screenshot_png=png,
            screenshot_width=width,
            screenshot_height=height,
        )


class SqlAnnotationContextFilter(QObject):
    """Adds one annotation action while preserving each editor's normal menu."""

    def eventFilter(self, watched: QObject, event: QEvent) -> bool:
        source = watched if isinstance(watched, (QPlainTextEdit, QTextEdit)) else watched.parent()
        if not isinstance(source, (QPlainTextEdit, QTextEdit)):
            return False
        if source.contextMenuPolicy() == Qt.CustomContextMenu:
            return False
        if event.type() != QEvent.ContextMenu or not source.toPlainText().strip():
            return False
        menu = source.createStandardContextMenu()
        menu.addSeparator()
        action = menu.addAction("Add annotation…")
        action.setToolTip("Save a note and optional application screenshot in DuckDB.")
        selected = menu.exec(event.globalPos())
        if selected is action:
            open_sql_annotation(source)
        menu.deleteLater()
        return True



def open_sql_annotation(source: QPlainTextEdit | QTextEdit) -> None:
    dialog = SqlAnnotationDialog(source)
    if dialog.exec() != QDialog.Accepted:
        return
    QApplication.setOverrideCursor(Qt.WaitCursor)
    try:
        annotation_id = save_annotation(dialog.annotation())
    except Exception as exc:
        QMessageBox.critical(
            source.window(),
            "Annotation not saved",
            f"DuckDB could not save this annotation.\n\n{exc}",
        )
    else:
        QMessageBox.information(
            source.window(),
            "Annotation saved",
            f"Saved locally in DuckDB.\nAnnotation ID: {annotation_id}",
        )
    finally:
        QApplication.restoreOverrideCursor()
