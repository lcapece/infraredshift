from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from analyzer.settings import resolve_source_cluster_config
from analyzer.widgets import cluster_dashboard
from analyzer.widgets.cluster_dashboard import ClusterDashboard, _ConfigDialog, _cluster_identity_key


class _Args:
    connection = "native"
    host = "prod-host"
    port = "5439"
    primary_database = "dev"
    table_databases = ""
    jdbc_url = ""


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_cluster_identity_ignores_database_scope() -> None:
    first = resolve_source_cluster_config(_Args())
    changed_scope = _Args()
    changed_scope.primary_database = "analytics"
    changed_scope.table_databases = "db1,db2"
    second = resolve_source_cluster_config(changed_scope)

    assert _cluster_identity_key(first) == _cluster_identity_key(second)


def test_closing_settings_preserves_manually_loaded_duckdb(monkeypatch, tmp_path) -> None:
    _app()
    dashboard = ClusterDashboard()
    manual_path = tmp_path / "corporate-redshift.duckdb"
    dashboard._path.setText(str(manual_path))
    config = resolve_source_cluster_config(_Args())

    class _FakeConfigDialog:
        def __init__(self, db_path: str, parent=None):
            assert db_path == str(manual_path)

        def exec(self):
            return 0

    switched: list[bool] = []
    monkeypatch.setattr(cluster_dashboard, "_ConfigDialog", _FakeConfigDialog)
    monkeypatch.setattr(cluster_dashboard, "load_settings", lambda: dashboard._settings)
    monkeypatch.setattr(dashboard, "_resolve_active_cluster", lambda: (config, "Native prod-host:5439"))
    monkeypatch.setattr(dashboard, "_sync_active_cluster_file", lambda **kwargs: switched.append(True))

    dashboard._config()

    assert dashboard._path.text() == str(manual_path)
    assert switched == []


def test_settings_counts_use_the_exact_supplied_file_and_do_not_create_missing(tmp_path) -> None:
    _app()
    sample = Path(__file__).resolve().parents[1] / "analyzer" / "samples" / "mock_redshift_3300.duckdb"
    active = tmp_path / "active.duckdb"
    active.write_bytes(sample.read_bytes())
    dialog = _ConfigDialog(str(active))

    assert "total rows across tracked datasets" in dialog._counts_status.text()
    assert str(active) in dialog._counts_status.text()

    missing = tmp_path / "missing.duckdb"
    missing_dialog = _ConfigDialog(str(missing))
    assert not missing.exists()
    assert "ACTIVE FILE NOT FOUND" in missing_dialog._counts_status.text()
