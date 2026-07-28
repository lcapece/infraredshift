"""install_schema recovery must only materialize coherent staged loads."""
import duckdb

from analyzer.duckdb_store import EXPECTED_COLUMNS, DuckDBStore, producer_namespace_id


TABLE = next(iter(EXPECTED_COLUMNS))
TMP = f"{TABLE}_tmp"


def _fresh_tmp(path, rows):
    con = duckdb.connect(str(path))
    try:
        con.execute(
            f'CREATE TABLE "{TMP}" (snapshot_id VARCHAR, namespace_id VARCHAR, query_id BIGINT)'
        )
        for snapshot, namespace, query_id in rows:
            con.execute(f'INSERT INTO "{TMP}" VALUES (?, ?, ?)', [snapshot, namespace, query_id])
    finally:
        con.close()


def _count(path, table) -> int:
    con = duckdb.connect(str(path), read_only=True)
    try:
        return int(con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
    finally:
        con.close()


def test_single_snapshot_tmp_is_recovered(tmp_path):
    path = tmp_path / "warehouse.duckdb"
    _fresh_tmp(path, [("snap-1", "ns-1", 1), ("snap-1", "ns-1", 2)])

    DuckDBStore(path).connect().close()

    assert _count(path, TABLE) == 2
    assert _count(path, TMP) == 2, "staging copy is retained for rollback"


def test_mixed_snapshot_tmp_is_left_alone(tmp_path):
    path = tmp_path / "warehouse.duckdb"
    _fresh_tmp(path, [("snap-1", "ns-1", 1), ("snap-2", "ns-1", 2)])

    DuckDBStore(path).connect().close()

    # ensure_table creates the empty base afterwards; the incoherent debris
    # must not have been copied into it.
    assert _count(path, TABLE) == 0
    assert _count(path, TMP) == 2


def test_tmp_without_snapshot_column_is_left_alone(tmp_path):
    path = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(path))
    try:
        con.execute(f'CREATE TABLE "{TMP}" (anything VARCHAR)')
        con.execute(f'INSERT INTO "{TMP}" VALUES (\'junk\')')
    finally:
        con.close()

    DuckDBStore(path).connect().close()

    assert _count(path, TABLE) == 0


def test_blank_namespace_rows_are_backfilled_on_connect(tmp_path):
    path = tmp_path / "warehouse.duckdb"
    store = DuckDBStore(path)
    store.connect().close()
    con = duckdb.connect(str(path))
    try:
        con.execute(
            f'INSERT INTO "{TABLE}" (snapshot_id, namespace_id) VALUES (\'snap-1\', NULL)'
        )
    finally:
        con.close()

    store.connect().close()

    con = duckdb.connect(str(path), read_only=True)
    try:
        value = con.execute(f'SELECT namespace_id FROM "{TABLE}"').fetchone()[0]
    finally:
        con.close()
    assert value == producer_namespace_id()
