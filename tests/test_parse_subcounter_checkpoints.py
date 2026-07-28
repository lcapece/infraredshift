from __future__ import annotations

import pandas as pd

import analyzer.cluster_analyze as ca
from analyzer.duckdb_store import DuckDBStore


def _frame(n: int) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"namespace_id": "ns", "query_id": i,
             "sql_text": f"SELECT c{i} FROM t{i} WHERE k = {i}"}
            for i in range(1, n + 1)
        ]
    )


def test_parse_reports_subcounter_and_checkpoints_each_chunk(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(ca, "_SQL_PARSE_CHUNK_ROWS", 2)
    store = DuckDBStore(tmp_path / "chunks.duckdb")
    messages: list[str] = []

    enriched, hits, misses = ca._enrich_with_incremental_sql_cache(
        store, _frame(5), progress=messages.append
    )

    assert misses == 5 and hits == 0
    assert len(enriched) == 5
    # Intro line naming the external cache + 3 chunk updates (5 stmts / chunk 2).
    assert len(messages) >= 3
    assert "5 of 5" in messages[-1]
    assert all("Parsing SQL shapes" in m for m in messages)
    # Rate + remaining-time estimate for long full-warehouse runs.
    assert any("left" in m or "estimating" in m or "/s" in m for m in messages)
    # Checkpoints live in the external sidecar file (not only inside the app).
    side = ca.sql_feature_cache_path(store)
    assert side.exists(), f"expected external cache at {side}"
    cached = ca._read_sql_feature_cache(store)
    assert len(cached) == 5
    assert any(side.name in m for m in messages)


def test_grouping_reports_subcounter(monkeypatch) -> None:
    """Step-11-class work (grouping) must emit live X-of-Y sub-indicators."""
    from analyzer.query_similarity import build_repeat_query_report

    sqls = [f"SELECT c{i} FROM t WHERE k = {i}" for i in range(12)]
    # Force two multi-member groups via shared shapes with different literals.
    sqls = (
        ["SELECT a FROM fact WHERE d = 1"] * 4
        + ["SELECT b FROM dim WHERE id = 1"] * 4
        + ["SELECT unique_once FROM solo WHERE x = 1"]
    )
    frame = pd.DataFrame(
        [
            {
                "namespace_id": "ns",
                "query_id": i,
                "sql_text": sql,
                "elapsed_s": 10.0 + i,
                "risk_score": 1.0,
                "user_name": "u",
                "database_name": "db",
                "query_type": "SELECT",
            }
            for i, sql in enumerate(sqls, start=1)
        ]
    )
    messages: list[str] = []
    groups, members = build_repeat_query_report(frame, progress=messages.append)
    assert not groups.empty
    assert messages, "grouping must emit sub-indicator progress"
    assert any(" of " in m and "Grouping" in m for m in messages)
    assert any("scanning" in m or "materializing" in m or "clustering" in m for m in messages)
    assert any("left" in m or "estimating" in m or "/s" in m for m in messages)


def test_interrupted_parse_resumes_from_checkpoint(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(ca, "_SQL_PARSE_CHUNK_ROWS", 2)
    store = DuckDBStore(tmp_path / "resume.duckdb")

    class _Kill(Exception):
        pass

    chunks_done = {"n": 0}

    def killing_progress(message: str) -> None:
        # Intro line names the cache; only count real chunk checkpoints.
        if " of " in message and "new statement" in message:
            chunks_done["n"] += 1
            if chunks_done["n"] == 2:  # die after the second chunk is checkpointed
                raise _Kill()

    try:
        ca._enrich_with_incremental_sql_cache(store, _frame(6), progress=killing_progress)
    except _Kill:
        pass
    after_kill = len(ca._read_sql_feature_cache(store))
    assert after_kill == 4, "two chunks of 2 must have been checkpointed before the kill"
    assert ca.sql_feature_cache_path(store).exists()

    # The restarted run only parses what was never checkpointed.
    enriched, hits, misses = ca._enrich_with_incremental_sql_cache(
        store, _frame(6), progress=lambda _m: None
    )
    assert hits == 4
    assert misses == 2
    assert len(enriched) == 6
