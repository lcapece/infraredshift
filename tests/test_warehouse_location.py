"""Where the DuckDB warehouse lives by default.

New installs use ~/Infraredshift/data. An existing corporate install under
~/RQP/data must keep using it - silently reading a new empty location would
look exactly like losing every captured row.
"""
from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest


def _resolve(monkeypatch, home: Path) -> Path:
    for key in (
        "REDSHIFT_DUCKDB_PATH",
        "REDSHIFT_ANALYZER_DATA_DIR",
        "REDSHIFT_ANALYZER_HOME",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    import analyzer.runtime_paths as runtime_paths

    importlib.reload(runtime_paths)
    import analyzer.duckdb_store as store

    importlib.reload(store)
    return store.default_duckdb_path()


def test_a_fresh_install_uses_the_infraredshift_folder(monkeypatch, tmp_path):
    resolved = _resolve(monkeypatch, tmp_path)

    assert resolved == tmp_path / "Infraredshift" / "data" / "redshift.duckdb"


def test_an_existing_rqp_warehouse_is_kept(monkeypatch, tmp_path):
    """The upgrade must not orphan a loaded corporate warehouse."""
    legacy = tmp_path / "RQP" / "data"
    legacy.mkdir(parents=True)
    (legacy / "redshift.duckdb").write_bytes(b"warehouse")

    resolved = _resolve(monkeypatch, tmp_path)

    assert resolved == legacy / "redshift.duckdb"


def test_an_empty_rqp_folder_does_not_win(monkeypatch, tmp_path):
    """Only an actual warehouse counts - an empty leftover folder should not
    pin a new install to the old location."""
    (tmp_path / "RQP" / "data").mkdir(parents=True)

    resolved = _resolve(monkeypatch, tmp_path)

    assert resolved == tmp_path / "Infraredshift" / "data" / "redshift.duckdb"


def test_an_explicit_path_always_wins(monkeypatch, tmp_path):
    legacy = tmp_path / "RQP" / "data"
    legacy.mkdir(parents=True)
    (legacy / "redshift.duckdb").write_bytes(b"warehouse")
    chosen = tmp_path / "elsewhere" / "mine.duckdb"
    monkeypatch.setenv("REDSHIFT_DUCKDB_PATH", str(chosen))
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))

    import analyzer.runtime_paths as runtime_paths

    importlib.reload(runtime_paths)
    import analyzer.duckdb_store as store

    importlib.reload(store)

    assert store.default_duckdb_path() == chosen


def test_per_cluster_files_sit_beside_the_main_warehouse(monkeypatch, tmp_path):
    """Each cluster stages into its own file so one load cannot lock or flush
    another cluster's rows."""
    _resolve(monkeypatch, tmp_path)
    import analyzer.loader.per_cluster as per_cluster

    importlib.reload(per_cluster)
    path = per_cluster._per_cluster_path("REDSHIFT_PRODUCER")

    assert path.parent == tmp_path / "Infraredshift" / "data"
    assert path.name == "redshift.REDSHIFT_PRODUCER.duckdb"
