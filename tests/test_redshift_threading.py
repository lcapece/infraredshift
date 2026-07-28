from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from types import SimpleNamespace

import runner


class _Cursor:
    description = (("value",),)

    def __init__(self) -> None:
        self._returned = False
        self.closed = False

    def execute(self, _sql: str) -> None:
        return None

    def fetchmany(self, _size: int):
        if self._returned:
            return []
        self._returned = True
        return [(1,)]

    def close(self) -> None:
        self.closed = True


class _Connection:
    def __init__(self) -> None:
        self.cursor_instance = _Cursor()
        self.closed = False

    def cursor(self) -> _Cursor:
        return self.cursor_instance

    def close(self) -> None:
        self.closed = True


def test_parallel_fetches_own_and_close_separate_redshift_connections(
    monkeypatch,
) -> None:
    created: list[_Connection] = []
    guard = Lock()

    def connect(_cfg, _database: str) -> _Connection:
        connection = _Connection()
        with guard:
            created.append(connection)
        return connection

    monkeypatch.setattr(runner, "connect_redshift", connect)
    cfg = SimpleNamespace(statement_timeout_ms=0)
    with ThreadPoolExecutor(max_workers=2) as pool:
        frames = list(
            pool.map(
                lambda stage: runner.fetch_frame(
                    cfg,
                    "enterprise_datawarehouse",
                    "SELECT 1",
                    stage=stage,
                ),
                ("reader-1", "reader-2"),
            )
        )

    assert len(created) == 2
    assert created[0] is not created[1]
    assert all(connection.closed for connection in created)
    assert all(connection.cursor_instance.closed for connection in created)
    assert [frame.iloc[0, 0] for frame in frames] == [1, 1]
