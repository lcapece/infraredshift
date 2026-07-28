from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication, QPushButton

from analyzer.app import MainWindow, _ButtonBusyCursorFilter


_APP = QApplication.instance() or QApplication([])


def test_button_action_sees_busy_cursor_and_cursor_is_restored() -> None:
    while QApplication.overrideCursor() is not None:
        QApplication.restoreOverrideCursor()
    button = QPushButton("Run")
    filter_ = _ButtonBusyCursorFilter(button)
    _APP.installEventFilter(filter_)
    seen_during_handler: list[bool] = []
    button.clicked.connect(lambda: seen_during_handler.append(QApplication.overrideCursor() is not None))
    button.show()

    QTest.mouseClick(button, Qt.LeftButton)
    assert seen_during_handler == [True]
    _APP.processEvents()
    assert QApplication.overrideCursor() is None

    _APP.removeEventFilter(filter_)
    button.close()


def test_keyboard_button_action_also_gets_busy_cursor() -> None:
    while QApplication.overrideCursor() is not None:
        QApplication.restoreOverrideCursor()
    button = QPushButton("Run")
    filter_ = _ButtonBusyCursorFilter(button)
    _APP.installEventFilter(filter_)
    seen_during_handler: list[bool] = []
    button.clicked.connect(lambda: seen_during_handler.append(QApplication.overrideCursor() is not None))
    button.show()
    button.setFocus()

    QTest.keyClick(button, Qt.Key_Space)
    assert seen_during_handler == [True]
    _APP.processEvents()
    assert QApplication.overrideCursor() is None

    _APP.removeEventFilter(filter_)
    button.close()


def test_late_progress_update_survives_reentrant_dialog_cleanup() -> None:
    events: list[object] = []

    class ReentrantDialog:
        def setRange(self, low: int, high: int) -> None:
            events.append(("range", low, high))

        def setLabelText(self, label: str) -> None:
            events.append(("label", label))

        def setValue(self, value: int) -> None:
            events.append(("value", value))
            owner._load_progress = None

    owner = type("ProgressOwner", (), {})()
    owner._load_progress = ReentrantDialog()
    owner._load_cancel_requested = False

    MainWindow._on_cluster_load_progress(owner, "Rendering", 5, 5)

    assert events == [
        ("range", 0, 100),
        ("label", "Rendering\nStep 5 of 5"),
        ("value", 100),
    ]
