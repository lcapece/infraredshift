"""Promotion must never carry a leftover *_tmp from a different load."""
from pathlib import Path

import duckdb
import pytest

import runner


def _args(target: Path):
    return type(
        "Args",
        (),
        {"duckdb_path": str(target), "lock_wait_seconds": 2.0, "no_backup": True},
    )()


def _prepare_load(target: Path, snapshot: str = "snap-new") -> None:
    con = duckdb.connect(str(target))
    try:
        con.execute(
            "CREATE TABLE query_history (snapshot_id VARCHAR, namespace_id VARCHAR, query_id BIGINT)"
        )
        con.execute("INSERT INTO query_history VALUES ('snap-old', 'ns-1', 1)")
        con.execute(
            "CREATE TABLE query_history_tmp (snapshot_id VARCHAR, namespace_id VARCHAR, query_id BIGINT)"
        )
        con.execute("INSERT INTO query_history_tmp VALUES (?, 'ns-1', 2)", [snapshot])
    finally:
        con.close()
    runner.save_state(target, 2.0, {"status": "loaded", "snapshot_id": snapshot, "label": "test"})


def _tables(target: Path) -> set[str]:
    con = duckdb.connect(str(target), read_only=True)
    try:
        return {
            str(row[0])
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
            ).fetchall()
        }
    finally:
        con.close()


def test_swap_promotes_current_load_and_skips_foreign_leftover(tmp_path, capsys):
    target = tmp_path / "warehouse.duckdb"
    _prepare_load(target)
    con = duckdb.connect(str(target))
    try:
        # Debris from an older aborted run: same shape, different snapshot.
        con.execute(
            "CREATE TABLE view_definitions_tmp (snapshot_id VARCHAR, namespace_id VARCHAR, view_name VARCHAR)"
        )
        con.execute("INSERT INTO view_definitions_tmp VALUES ('snap-old', 'ns-1', 'v_x')")
    finally:
        con.close()

    assert runner.run_swap(_args(target)) == 0

    tables = _tables(target)
    assert "view_definitions" not in tables, "foreign leftover must not be promoted"
    assert "view_definitions_tmp" in tables, "leftover is kept for inspection, not destroyed"
    con = duckdb.connect(str(target), read_only=True)
    try:
        rows = con.execute("SELECT snapshot_id, query_id FROM query_history").fetchall()
    finally:
        con.close()
    assert rows == [("snap-new", 2)]
    assert "Skipping leftover staged table" in capsys.readouterr().out


def test_swap_aborts_when_planned_table_carries_foreign_snapshot(tmp_path):
    target = tmp_path / "warehouse.duckdb"
    _prepare_load(target)
    runner.save_state(target, 2.0, {"selected_tables": "query_history,view_definitions"})
    con = duckdb.connect(str(target))
    try:
        con.execute(
            "CREATE TABLE view_definitions_tmp (snapshot_id VARCHAR, namespace_id VARCHAR, view_name VARCHAR)"
        )
        con.execute("INSERT INTO view_definitions_tmp VALUES ('snap-old', 'ns-1', 'v_x')")
    finally:
        con.close()

    with pytest.raises(SystemExit, match="carries snapshot"):
        runner.run_swap(_args(target))

    con = duckdb.connect(str(target), read_only=True)
    try:
        rows = con.execute("SELECT snapshot_id, query_id FROM query_history").fetchall()
    finally:
        con.close()
    assert rows == [("snap-old", 1)], "aborted promotion must leave live tables untouched"


def test_swap_skips_unattributable_empty_tmp_on_default_plan(tmp_path):
    target = tmp_path / "warehouse.duckdb"
    _prepare_load(target)
    con = duckdb.connect(str(target))
    try:
        con.execute(
            "CREATE TABLE view_definitions (snapshot_id VARCHAR, namespace_id VARCHAR, view_name VARCHAR)"
        )
        con.execute("INSERT INTO view_definitions VALUES ('snap-old', 'ns-1', 'v_live')")
        con.execute(
            "CREATE TABLE view_definitions_tmp (snapshot_id VARCHAR, namespace_id VARCHAR, view_name VARCHAR)"
        )
    finally:
        con.close()

    assert runner.run_swap(_args(target)) == 0

    con = duckdb.connect(str(target), read_only=True)
    try:
        live = con.execute("SELECT view_name FROM view_definitions").fetchall()
    finally:
        con.close()
    assert live == [("v_live",)], "an empty unattributable tmp must not erase live data"
