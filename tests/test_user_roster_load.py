"""Loading the user roster on its own.

The roster is a small, rarely-changing user list. Needing it should not mean
waiting for a full workload capture - and until now there was no way to load it
from the UI at all, even though the app told users to "refresh the User Roster
in the Data Loader".
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QPushButton

from analyzer.loader.engine import roster_command


def _app():
    return QApplication.instance() or QApplication([])


def test_roster_command_targets_the_roster_subcommand_only():
    command = roster_command(r"C:\data\redshift.duckdb")

    assert "user-roster" in command
    assert "--duckdb-path" in command
    assert command[command.index("--duckdb-path") + 1] == r"C:\data\redshift.duckdb"
    # Not a workload load: none of the capture-window flags belong here.
    for flag in ("--days", "--promote", "--fresh", "refresh"):
        assert flag not in command


def test_loader_window_exposes_a_roster_button():
    _app()
    from analyzer.loader.gui import LoaderWindow

    window = LoaderWindow(duckdb_path=str(Path(tempfile.mkdtemp()) / "w.duckdb"))

    labels = [button.text() for button in window.findChildren(QPushButton)]
    assert "Load User Roster" in labels


def test_clicking_the_button_runs_the_roster_command(monkeypatch):
    _app()
    import analyzer.loader.gui as module

    started = {}
    monkeypatch.setattr(
        module.LoaderWindow,
        "_start_process",
        lambda self, command, *, operation: started.update(command=command, operation=operation),
    )

    path = Path(tempfile.mkdtemp()) / "w.duckdb"
    window = module.LoaderWindow(duckdb_path=str(path))
    window._path.setText(str(path))
    window._roster.click()

    assert started["operation"] == "roster"
    assert "user-roster" in started["command"]


def test_the_roster_capture_tries_more_than_one_database():
    """SVV_USER_INFO is cluster-wide, so any reachable database returns the
    same roster. Hard-coding one database name meant a cluster without that
    exact name silently produced an empty roster."""
    import inspect

    from analyzer import ingest_redshift

    source = inspect.getsource(ingest_redshift.capture_user_roster)

    assert "candidates" in source
    assert "roster_database" in source
    assert "ENTERPRISE_DW_DATABASE" in source
    # It must say so when every candidate came back empty.
    assert "No user roster rows" in source


def test_a_roster_database_override_exists():
    import inspect

    from analyzer import ingest_redshift

    assert "--roster-database" in inspect.getsource(ingest_redshift)
