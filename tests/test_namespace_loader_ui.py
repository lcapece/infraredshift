from __future__ import annotations

from PySide6.QtWidgets import QApplication, QMessageBox
import pytest

import runner
from analyzer.secrets_store import clear_session_secrets, set_session_secret
from analyzer.widgets.cluster_dashboard import RefreshSourceDialog


_APP = QApplication.instance() or QApplication([])


@pytest.fixture(autouse=True)
def _clear_credentials_session():
    clear_session_secrets()
    yield
    clear_session_secrets()


def _only_consumer_one(monkeypatch, tmp_path) -> None:
    legacy_dir = tmp_path / "legacy-environment-only"
    legacy_dir.mkdir()
    (legacy_dir / ".env").write_text("", encoding="utf-8")
    monkeypatch.setenv("REDSHIFT_ANALYZER_LAUNCH_DIR", str(legacy_dir))
    monkeypatch.setenv(
        "REDSHIFT_ANALYZER_PROFILE_PATH",
        str(legacy_dir / "missing-profiles.json"),
    )
    monkeypatch.setenv("REDSHIFT_PRODUCER_ENABLED", "false")
    for number in range(1, 8):
        monkeypatch.setenv(f"REDSHIFT_CONSUMER_{number}_ENABLED", "false")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_ENABLED", "true")
    set_session_secret("REDSHIFT_CONSUMER_1_HOST", "consumer.example")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_NAMESPACE_ID", "namespace-reporting")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_DISPLAY_NAME", "Reporting Consumer")


def test_namespace_loader_uses_friendly_tabs_and_count_columns(monkeypatch, tmp_path) -> None:
    _only_consumer_one(monkeypatch, tmp_path)

    dialog = RefreshSourceDialog(str(tmp_path / "loader.duckdb"))

    assert dialog._namespace_tabs.count() == 1
    assert dialog._namespace_tabs.tabText(0) == "Reporting Consumer"
    tree = dialog._namespace_tabs.widget(0)
    labels = [tree.headerItem().text(index) for index in range(tree.columnCount())]
    assert labels == ["Order", "DuckDB Table", "Redshift Rows", "DuckDB Rows", "Status", "Progress"]
    assert tree.topLevelItemCount() > 10
    assert tree.topLevelItem(tree.topLevelItemCount() - 1).text(1) == "external_table_info_all"
    assert dialog._namespace_progress.maximum() == 100
    dialog.close()


def test_runner_progress_hook_reports_source_and_staged_counts() -> None:
    events = []
    runner.set_progress_hook(lambda *event: events.append(event))
    try:
        runner.emit_progress("namespace-reporting", "query_history", 414, 414, 1, 14, "Staged in DuckDB")
    finally:
        runner.set_progress_hook(None)

    assert events == [
        ("namespace-reporting", "query_history", 414, 414, 1, 14, "Staged in DuckDB")
    ]


def test_namespace_loader_reopens_at_recoverable_checkpoint(monkeypatch, tmp_path) -> None:
    _only_consumer_one(monkeypatch, tmp_path)
    path = tmp_path / "recover.duckdb"
    snapshot_id = "resume-snapshot"
    runner.save_state(
        path,
        1,
        {
            "snapshot_id": snapshot_id,
            "status": "loading",
            "days": "7.0",
            "floor_seconds": "300.0",
            "floor_basis": "execution_time",
            "namespace_ids": "namespace-reporting",
        },
    )
    runner._resolve_multi_run(
        type("Args", (), {
            "resume": True, "lock_wait_seconds": 1, "days": 7.0,
            "floor_seconds": 300.0, "floor_basis": "execution_time",
        })(),
        path,
        [type("Cfg", (), {"namespace_id": "namespace-reporting"})()],
    )
    runner._mark_namespace_table_complete(
        path, 1, snapshot_id, "namespace-reporting", "query_history", 414
    )

    dialog = RefreshSourceDialog(str(path))
    tree = dialog._namespace_tabs.widget(0)
    query_history = next(
        tree.topLevelItem(index)
        for index in range(tree.topLevelItemCount())
        if tree.topLevelItem(index).text(1) == "query_history"
    )

    assert dialog._cycle_load.text() == "Resume Safe Load"
    assert query_history.text(2) == "414"
    assert query_history.text(4) == "Recovered checkpoint"
    dialog.close()


def test_guided_load_launches_the_recoverable_process(monkeypatch, tmp_path) -> None:
    _only_consumer_one(monkeypatch, tmp_path)
    monkeypatch.delenv("REDSHIFT_ANALYZER_LAUNCH_PATH", raising=False)
    dialog = RefreshSourceDialog(str(tmp_path / "loader.duckdb"))
    launched = []
    monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Yes)
    monkeypatch.setattr(
        dialog,
        "_start_loader_process",
        lambda command, *, operation: launched.append((command, operation)),
    )

    dialog._start_namespace_cycle_load()

    command, operation = launched[0]
    assert operation == "guided-stage"
    assert "refresh" in command
    assert "--json-events" in command
    assert "--external-timeout-action" in command
    dialog.close()
