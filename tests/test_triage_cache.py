from pathlib import Path

import pandas as pd

from analyzer.cluster_analyze import (
    ClusterReport,
    _enrich_with_incremental_sql_cache,
    _hydrate_loader_repeat_data,
    _read_loader_repeat_data,
    merge_cluster_reports,
)
from analyzer.duckdb_store import DuckDBStore
from analyzer.mock_data import generate_mock_snapshot


def test_incremental_sql_cache_examines_only_new_sql(tmp_path):
    store = DuckDBStore(tmp_path / "triage-cache.duckdb")
    first = pd.DataFrame(
        [
            {"query_id": "1", "sql_text": "SELECT * FROM public.events WHERE id = 1"},
            {"query_id": "2", "sql_text": "SELECT * FROM public.events WHERE id = 1"},
        ]
    )

    enriched, hits, misses = _enrich_with_incremental_sql_cache(store, first)

    assert len(enriched) == 2
    assert hits == 0
    assert misses == 1
    second = pd.DataFrame(
        [
            {"query_id": "3", "sql_text": "SELECT * FROM public.events WHERE id = 1"},
            {"query_id": "4", "sql_text": "SELECT count(*) FROM public.audit"},
        ]
    )

    enriched, hits, misses = _enrich_with_incremental_sql_cache(store, second)

    assert len(enriched) == 2
    assert hits == 1
    assert misses == 1
    assert enriched["sql_parse_status"].eq("parsed").all()


def test_report_merge_drops_old_frames_when_snapshot_changes(tmp_path):
    path = Path(tmp_path / "same.duckdb")
    old = ClusterReport(
        db_path=path,
        snapshot_id="old",
        repeat_groups=pd.DataFrame([{"repeat_group_id": "stale"}]),
        loaded_areas=("repeat_queries",),
    )
    new = ClusterReport(db_path=path, snapshot_id="new", loaded_areas=("status",))

    merged = merge_cluster_reports(old, new)

    assert merged.snapshot_id == "new"
    assert merged.repeat_groups.empty
    assert merged.loaded_areas == ("status",)


def test_report_merge_trusts_empty_frame_for_reloaded_area(tmp_path):
    path = Path(tmp_path / "merge-empty.duckdb")
    old = ClusterReport(
        db_path=path,
        snapshot_id="snap",
        insights=pd.DataFrame([{"insight_id": "stale-row"}]),
        loaded_areas=("insights",),
    )
    new = ClusterReport(
        db_path=path,
        snapshot_id="snap",
        insights=pd.DataFrame(),  # legitimate empty reload
        loaded_areas=("insights",),
    )

    merged = merge_cluster_reports(old, new)

    assert merged.insights.empty, "reloaded empty insights must replace stale rows"
    assert "insights" in merged.loaded_areas


def test_report_merge_keeps_base_when_area_not_reloaded(tmp_path):
    path = Path(tmp_path / "merge-keep.duckdb")
    old = ClusterReport(
        db_path=path,
        snapshot_id="snap",
        insights=pd.DataFrame([{"insight_id": "keep-me"}]),
        loaded_areas=("insights",),
    )
    new = ClusterReport(
        db_path=path,
        snapshot_id="snap",
        slow_queries=pd.DataFrame([{"query_id": "1"}]),
        loaded_areas=("slow_queries",),
    )

    merged = merge_cluster_reports(old, new)

    assert list(merged.insights["insight_id"]) == ["keep-me"]
    assert list(merged.slow_queries["query_id"]) == ["1"]


def test_loader_repeat_data_is_snapshot_bound(tmp_path):
    import duckdb

    path = tmp_path / "loader-groups.duckdb"
    con = duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE loader_repeat_groups(snapshot_id VARCHAR, repeat_group_id VARCHAR)"
        )
        con.execute(
            "CREATE TABLE loader_repeat_members(snapshot_id VARCHAR, repeat_group_id VARCHAR, query_id VARCHAR)"
        )
        con.execute("INSERT INTO loader_repeat_groups VALUES ('new', 'RQ001'), ('old', 'RQ999')")
        con.execute("INSERT INTO loader_repeat_members VALUES ('new', 'RQ001', '101'), ('old', 'RQ999', '999')")

        loaded = _read_loader_repeat_data(con, "new")
    finally:
        con.close()

    assert loaded is not None
    groups, members = loaded
    assert groups["repeat_group_id"].tolist() == ["RQ001"]
    assert members["query_id"].tolist() == ["101"]


def test_loader_repeat_data_recovers_sole_snapshot_when_catalog_anchor_differs(tmp_path):
    import duckdb

    path = tmp_path / "loader-sole-snapshot.duckdb"
    con = duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE loader_repeat_groups(snapshot_id VARCHAR, repeat_group_id VARCHAR)"
        )
        con.execute(
            "CREATE TABLE loader_repeat_members(snapshot_id VARCHAR, repeat_group_id VARCHAR, query_id VARCHAR)"
        )
        con.execute("INSERT INTO loader_repeat_groups VALUES ('loader-anchor', 'RQ001')")
        con.execute("INSERT INTO loader_repeat_members VALUES ('loader-anchor', 'RQ001', '101')")

        loaded = _read_loader_repeat_data(con, "different-catalog-anchor")
    finally:
        con.close()

    assert loaded is not None
    groups, members = loaded
    assert groups["repeat_group_id"].tolist() == ["RQ001"]
    assert members["query_id"].tolist() == ["101"]


def test_loader_groups_are_hydrated_from_current_evidence_without_regrouping():
    groups = pd.DataFrame([
        {"repeat_group_id": "RQ001", "query_count": 2, "total_runtime_s": 0.0}
    ])
    members = pd.DataFrame([
        {"repeat_group_id": "RQ001", "query_id": "101", "elapsed_s": 0.0},
        {"repeat_group_id": "RQ001", "query_id": "102", "elapsed_s": 0.0},
    ])
    slow = pd.DataFrame([
        {"query_id": "101", "elapsed_s": 10.0, "risk_score": 20.0, "total_spill": 100},
        {"query_id": "102", "elapsed_s": 30.0, "risk_score": 40.0, "total_spill": 300},
    ])

    hydrated_groups, hydrated_members = _hydrate_loader_repeat_data(groups, members, slow)

    row = hydrated_groups.iloc[0]
    assert row["total_runtime_s"] == 40.0
    assert row["worst_runtime_s"] == 30.0
    assert row["avg_risk_score"] == 30.0
    assert row["avg_total_spill"] == 200.0
    assert hydrated_members["elapsed_s"].tolist() == [10.0, 30.0]


def test_cluster_report_prefers_loader_groups_without_calling_sqlglot_grouping(
    tmp_path, monkeypatch
):
    import duckdb
    import analyzer.cluster_analyze as cluster_analyze

    path = tmp_path / "precomputed.duckdb"
    generated = generate_mock_snapshot(output=path, query_count=8, table_count=4)
    con = duckdb.connect(str(path))
    try:
        query_ids = [
            str(row[0]) for row in con.execute(
                "SELECT query_id FROM query_history WHERE snapshot_id = ? ORDER BY query_id LIMIT 2",
                [generated.snapshot_id],
            ).fetchall()
        ]
        con.execute(
            "CREATE TABLE loader_repeat_groups AS SELECT ? AS snapshot_id, 'RQ001' AS repeat_group_id, "
            "2 AS query_count, 1.0 AS avg_similarity, 10.0 AS total_runtime_s, "
            "5.0 AS worst_runtime_s, 0.0 AS avg_risk_score, 0.0 AS max_risk_score, "
            "'' AS sql_tables, '' AS sql_tables_full, '' AS sql_ctes, 0 AS join_count, "
            "0 AS predicate_count, 0 AS wildcard_count, 'SELECT' AS query_type",
            [generated.snapshot_id],
        )
        con.execute(
            "CREATE TABLE loader_repeat_members "
            "(snapshot_id VARCHAR, repeat_group_id VARCHAR, query_id VARCHAR, "
            "similarity_score DOUBLE, elapsed_s DOUBLE, risk_score DOUBLE)"
        )
        con.executemany(
            "INSERT INTO loader_repeat_members VALUES (?, 'RQ001', ?, 1.0, 5.0, 0.0)",
            [(generated.snapshot_id, query_id) for query_id in query_ids],
        )
    finally:
        con.close()

    monkeypatch.setattr(
        cluster_analyze,
        "build_repeat_query_report",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("regrouped")),
    )

    report = cluster_analyze.load_cluster_report(path, areas=["repeat_queries"])

    assert report.repeat_groups["repeat_group_id"].tolist() == ["RQ001"]
    assert len(report.repeat_members) == 2
    assert report.summary["repeat_grouping_source"] == "loader"
