"""Heavy external telemetry stays excluded; SVV_EXTERNAL_COLUMNS metadata loads."""
from __future__ import annotations

from types import SimpleNamespace

import duckdb

import runner
from analyzer import ingest_redshift


def test_external_capture_is_globally_disabled_this_version() -> None:
    assert runner.EXTERNAL_CAPTURE_ENABLED is False
    assert ingest_redshift.EXTERNAL_CAPTURE_ENABLED is False


def test_full_refresh_plan_excludes_external_even_when_requested() -> None:
    plan = runner.selected_refresh_tables(SimpleNamespace(include_external=True))
    assert "external_table_info_all" not in plan
    assert "external_table_metadata" in plan
    assert "query_history" in plan

    explicit = runner.selected_refresh_tables(
        SimpleNamespace(
            include_tables=("query_history", "external_table_info_all"),
            include_external=True,
        )
    )
    assert explicit == ("query_history",)


def test_loader_window_has_no_separate_external_load_control(tmp_path) -> None:
    from PySide6.QtWidgets import QApplication, QPushButton

    _app = QApplication.instance() or QApplication([])
    from analyzer.loader.gui import LoaderWindow

    window = LoaderWindow(str(tmp_path / "loader.duckdb"))
    button_text = {button.text() for button in window.findChildren(QPushButton)}
    assert not any("Load External" in text for text in button_text)
    window.deleteLater()


def test_status_names_only_the_unified_external_metadata_dataset(
    tmp_path, capsys,
) -> None:
    path = tmp_path / "status.duckdb"
    duckdb.connect(str(path)).close()

    assert runner.run_status(
        SimpleNamespace(duckdb_path=str(path), lock_wait_seconds=1)
    ) == 0

    output = capsys.readouterr().out
    assert "external_table_metadata_tmp" in output
    assert "external_table_info_all_tmp" not in output
