from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from analyzer.duckdb_store import DuckDBStore


def test_concurrent_connects_serialize_schema_install(tmp_path):
    class ObservedStore(DuckDBStore):
        def __init__(self, path):
            super().__init__(path)
            self.active_installs = 0
            self.max_active_installs = 0
            self.observation_lock = threading.Lock()

        def install_schema(self, con):
            with self.observation_lock:
                self.active_installs += 1
                self.max_active_installs = max(self.max_active_installs, self.active_installs)
            try:
                # Make overlap deterministic if connect() stops serializing the
                # catalog migration in a future change.
                time.sleep(0.02)
                super().install_schema(con)
            finally:
                with self.observation_lock:
                    self.active_installs -= 1

    store = ObservedStore(tmp_path / "concurrent-connect.duckdb")
    barrier = threading.Barrier(4)

    def open_and_read():
        barrier.wait()
        with store.connect() as con:
            return con.execute("SELECT COUNT(*) FROM v_query_details").fetchone()[0]

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: open_and_read(), range(4)))

    assert results == [0, 0, 0, 0]
    assert store.max_active_installs == 1
