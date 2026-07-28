from __future__ import annotations

import pandas as pd
import pytest

from analyzer import duckdb_store as store_module
from analyzer.duckdb_store import DuckDBStore
from analyzer.ingest_redshift import _replace_capture_preserving_existing


class _FailingInsertConnection:
    def __init__(self, connection, table_name: str):
        self._connection = connection
        self._needle = f"INSERT INTO \"{table_name}\""

    def execute(self, sql, *args, **kwargs):
        if self._needle in str(sql):
            raise RuntimeError("simulated insert interruption")
        return self._connection.execute(sql, *args, **kwargs)

    def register(self, *args, **kwargs):
        return self._connection.register(*args, **kwargs)

    def unregister(self, *args, **kwargs):
        return self._connection.unregister(*args, **kwargs)


def test_replace_rolls_back_delete_when_insert_is_interrupted() -> None:
    store = DuckDBStore(":memory:")
    with store.connect() as con:
        run = store.new_snapshot("atomic replace")
        store.record_snapshot(con, run)
        store.replace_table_from_frame(
            con,
            "query_history",
            pd.DataFrame([{"query_id": 1, "query_text": "old"}]),
            run,
        )

        with pytest.raises(RuntimeError, match="simulated insert interruption"):
            store.replace_table_from_frame(
                _FailingInsertConnection(con, "query_history"),
                "query_history",
                pd.DataFrame([{"query_id": 2, "query_text": "new"}]),
                run,
            )

        assert con.execute(
            "SELECT query_id, query_text FROM query_history"
        ).fetchall() == [("1", "old")]


def test_imported_bookkeeping_columns_are_replaced_by_destination_snapshot() -> None:
    store = DuckDBStore(":memory:")
    with store.connect() as con:
        run = store.new_snapshot("reimport")
        store.record_snapshot(con, run)
        store.replace_table_from_frame(
            con,
            "query_history",
            pd.DataFrame([{
                "snapshot_id": "foreign-snapshot",
                "captured_at": "1999-01-01",
                "query_id": 7,
            }]),
            run,
        )
        snapshot_id, captured_at = con.execute(
            "SELECT snapshot_id, captured_at FROM query_history"
        ).fetchone()

    assert snapshot_id == run.snapshot_id
    assert str(captured_at).startswith("20")


def test_backup_fails_closed_when_checkpoint_cannot_open(monkeypatch, tmp_path) -> None:
    path = tmp_path / "warehouse.duckdb"
    store = DuckDBStore(path)
    with store.connect() as con:
        con.execute("CREATE TABLE evidence(value INTEGER)")
        con.execute("INSERT INTO evidence VALUES (1)")

    monkeypatch.setattr(
        store_module.duckdb,
        "connect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("locked")),
    )
    with pytest.raises(RuntimeError, match="no backup was created"):
        store.backup_database("unsafe")
    assert not list((tmp_path / "backups").glob("*.duckdb"))


def test_zero_row_refresh_preserves_existing_snapshot_slice() -> None:
    store = DuckDBStore(":memory:")
    with store.connect() as con:
        run = store.new_snapshot("zero guard")
        store.record_snapshot(con, run)
        store.replace_table_from_frame(
            con,
            "svv_table_info_all",
            pd.DataFrame([{"source_db": "dev", "schema": "public", "table": "orders"}]),
            run,
        )

        with pytest.raises(RuntimeError, match="Existing data was preserved"):
            _replace_capture_preserving_existing(
                con,
                store,
                "svv_table_info_all",
                pd.DataFrame(),
                run,
            )

        assert con.execute("SELECT COUNT(*) FROM svv_table_info_all").fetchone()[0] == 1


def test_replace_for_one_namespace_preserves_other_namespaces_in_the_snapshot() -> None:
    store = DuckDBStore(":memory:")
    with store.connect() as con:
        run = store.new_snapshot("namespace-scoped replace")
        store.record_snapshot(con, run)
        store.replace_table_from_frame(
            con,
            "query_history",
            pd.DataFrame([{"namespace_id": "producer-ns", "query_id": 1, "query_text": "producer row"}]),
            run,
        )
        store.replace_table_from_frame(
            con,
            "query_history",
            pd.DataFrame([{"namespace_id": "consumer-ns", "query_id": 1, "query_text": "consumer row"}]),
            run,
        )
        # Refresh the consumer again: the producer's rows must survive.
        store.replace_table_from_frame(
            con,
            "query_history",
            pd.DataFrame([{"namespace_id": "consumer-ns", "query_id": 2, "query_text": "consumer refresh"}]),
            run,
        )

        rows = con.execute(
            "SELECT namespace_id, query_text FROM query_history ORDER BY namespace_id, query_text"
        ).fetchall()
        assert rows == [
            ("consumer-ns", "consumer refresh"),
            ("producer-ns", "producer row"),
        ]
