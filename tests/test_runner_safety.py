from pathlib import Path

import duckdb

import runner


def _table_names(path: Path) -> list[str]:
    con = duckdb.connect(str(path), read_only=True)
    try:
        return [
            str(row[0])
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_type = 'BASE TABLE' ORDER BY table_name"
            ).fetchall()
        ]
    finally:
        con.close()


def test_runner_command_uses_absolute_self_path_and_target():
    target = Path("C:/data/redshift loaded.duckdb")
    command = runner.runner_command("--swap", "--duckdb-path", target)

    assert f'"{Path(runner.__file__).resolve()}"' in command
    assert '"--swap"' in command
    assert f'"{target}"' in command


def test_backup_only_preserves_all_tables(tmp_path):
    source = Path(__file__).resolve().parents[1] / "analyzer" / "samples" / "mock_redshift_3300.duckdb"
    target = tmp_path / "loaded.duckdb"
    target.write_bytes(source.read_bytes())
    before = _table_names(target)
    args = type("Args", (), {"duckdb_path": str(target), "lock_wait_seconds": 1.0})()

    assert runner.run_backup_only(args) == 0

    backups = list((tmp_path / "backups").glob("*.duckdb"))
    assert len(backups) == 1
    assert _table_names(target) == before
    assert _table_names(backups[0]) == before
