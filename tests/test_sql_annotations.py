from __future__ import annotations

import duckdb
from PySide6.QtWidgets import QApplication, QPlainTextEdit, QWidget

from analyzer.sql_annotations import SqlAnnotation, save_annotation
from analyzer.widgets.login_dialog import LoginDialog
from analyzer.widgets.sql_annotations import SqlAnnotationDialog


_APP = QApplication.instance() or QApplication([])


def test_annotation_persists_sql_note_and_screenshot_in_duckdb(tmp_path) -> None:
    path = tmp_path / "annotations.duckdb"
    annotation_id = save_annotation(
        SqlAnnotation(
            note="Set sort key on event_date",
            selected_sql="WHERE event_date >= current_date - 7",
            surrounding_sql="SELECT * FROM fact_events WHERE event_date >= current_date - 7",
            context_title="Query RQ024",
            query_id="12345",
            namespace_id="producer-ns",
            screenshot_png=b"\x89PNG\r\n\x1a\nmock",
            screenshot_width=1280,
            screenshot_height=720,
        ),
        path,
    )

    con = duckdb.connect(str(path), read_only=True)
    try:
        row = con.execute(
            "SELECT annotation_id, note, surrounding_sql, query_id, namespace_id, "
            "screenshot_png, screenshot_width, screenshot_height, sync_status "
            "FROM user_sql_annotations"
        ).fetchone()
    finally:
        con.close()

    stored_id, stored_note, stored_sql, query_id, namespace, stored_png, width, height, sync = row
    assert (stored_id, query_id, namespace, width, height, sync) == (
        annotation_id, "12345", "producer-ns", 1280, 720, "local",
    )
    # Payloads are DPAPI-encrypted at rest: the raw table must not leak the
    # note, the SQL, or the screenshot bytes.
    assert stored_note.startswith("enc:v1:")
    assert stored_sql.startswith("enc:v1:")
    assert "fact_events" not in stored_sql
    assert not bytes(stored_png).startswith(b"\x89PNG")

    from analyzer.sql_annotations import read_annotations

    decoded = read_annotations(path)
    assert len(decoded) == 1
    assert decoded[0]["note"] == "Set sort key on event_date"
    assert decoded[0]["surrounding_sql"] == (
        "SELECT * FROM fact_events WHERE event_date >= current_date - 7"
    )
    assert decoded[0]["screenshot_png"] == b"\x89PNG\r\n\x1a\nmock"


def test_annotation_dialog_is_plain_note_and_optional_screenshot() -> None:
    editor = QPlainTextEdit("SELECT * FROM public.fact_sales")
    dialog = SqlAnnotationDialog(editor)

    assert dialog.windowTitle() == "Add SQL Annotation"
    assert dialog._note.placeholderText() == "Write your annotation…"
    assert "screenshot" in dialog._attach.text().lower()
    dialog.close()
    editor.close()


def test_login_is_product_startup_experience() -> None:
    dialog = LoginDialog()

    assert dialog.objectName() == "StartupExperience"
    assert "Infraredshift" in dialog.windowTitle() or "Infraredshift" in dialog.windowTitle()
    assert dialog.findChild(QWidget, "DemandSourceAnimation") is None
    flow = dialog.findChild(QWidget, "AnalysisProcessFlow")
    assert flow is dialog._animation
    assert flow.width() == round(1080 * 0.67)
    assert flow.height() == round(214 * 0.67)
    assert dialog._sign_in.text() == ("Secure workspace" if dialog._setup_mode else "Open workspace")
    assert dialog.minimumWidth() >= 740
    assert dialog.minimumHeight() <= 720
    # Product logo is present on the startup chrome.
    from PySide6.QtWidgets import QFrame, QLabel

    logo_labels = [w for w in dialog.findChildren(QLabel) if w.objectName() == "InfraredshiftLogo"]
    assert logo_labels, "startup must show the Infraredshift logo"
    logo_panel = dialog.findChild(QFrame, "StartupLogoPanel")
    assert logo_panel is not None, "startup logo must sit on its own white panel"
    logo = logo_labels[0]
    assert (logo.pixmap() is not None and not logo.pixmap().isNull()) or bool(logo.text())
    # Assert the rendered logo size, not the ratio that produces it. The form
    # was narrowed so the logo panel matches the process-flow diagram, and the
    # ratio was raised to compensate - the point is that the PNG did NOT shrink.
    from analyzer.widgets.login_dialog import _LOGIN_LOGO_WIDTH_RATIO

    expected_width = round(dialog.width() * _LOGIN_LOGO_WIDTH_RATIO)
    assert abs(logo.pixmap().width() - expected_width) <= 1
    assert logo.pixmap().width() >= 380, "the logo must not be scaled down"

    # The logo panel, the flow diagram and the access row all share one width.
    from analyzer.widgets.login_dialog import _LOGIN_PANEL_WIDTH

    assert logo_panel.width() == _LOGIN_PANEL_WIDTH
    dialog.close()
