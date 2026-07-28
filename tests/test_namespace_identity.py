"""Namespace tie-breaker regressions: identical query ids on two clusters."""
from __future__ import annotations

import pandas as pd

from analyzer.cluster_analyze import _hydrate_loader_repeat_data
from analyzer.sql_annotations import (
    SqlAnnotation,
    save_annotation,
    set_active_annotation_db,
)


def test_hydration_keeps_each_namespace_evidence_separate() -> None:
    groups = pd.DataFrame([{"repeat_group_id": "RQ001", "query_count": 2}])
    members = pd.DataFrame(
        [
            {"repeat_group_id": "RQ001", "query_id": "777", "namespace_id": "producer-ns", "user_name": ""},
            {"repeat_group_id": "RQ001", "query_id": "777", "namespace_id": "consumer-ns", "user_name": ""},
        ]
    )
    slow = pd.DataFrame(
        [
            {"query_id": "777", "namespace_id": "producer-ns", "user_name": "producer-user", "elapsed_s": 10.0, "risk_score": 5},
            {"query_id": "777", "namespace_id": "consumer-ns", "user_name": "consumer-user", "elapsed_s": 99.0, "risk_score": 90},
        ]
    )

    _groups, hydrated = _hydrate_loader_repeat_data(groups, members, slow)

    by_namespace = {
        str(row["namespace_id"]): str(row["user_name"])
        for _, row in hydrated.iterrows()
    }
    assert by_namespace == {
        "producer-ns": "producer-user",
        "consumer-ns": "consumer-user",
    }


def test_hydration_still_works_for_legacy_members_without_namespace() -> None:
    groups = pd.DataFrame([{"repeat_group_id": "RQ001", "query_count": 1}])
    members = pd.DataFrame([{"repeat_group_id": "RQ001", "query_id": "42", "user_name": ""}])
    slow = pd.DataFrame([{"query_id": "42", "user_name": "someone", "elapsed_s": 3.0}])

    _groups, hydrated = _hydrate_loader_repeat_data(groups, members, slow)

    assert str(hydrated.iloc[0]["user_name"]) == "someone"


def test_annotations_follow_the_operator_selected_warehouse(tmp_path) -> None:
    active = tmp_path / "cluster-a.duckdb"
    set_active_annotation_db(active)
    try:
        annotation = SqlAnnotation(
            note="check this join",
            selected_sql="SELECT 1",
            surrounding_sql="SELECT 1",
            context_title="test",
            source_widget="editor",
        )
        save_annotation(annotation)
    finally:
        set_active_annotation_db(None)

    assert active.is_file()
    import duckdb

    con = duckdb.connect(str(active), read_only=True)
    try:
        count = con.execute("SELECT COUNT(*) FROM user_sql_annotations").fetchone()[0]
    finally:
        con.close()
    assert count == 1


def test_repeat_group_key_is_stable_while_display_rank_shifts() -> None:
    from analyzer.query_similarity import build_repeat_query_report

    def workload(fast_pain: float, slow_pain: float) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"query_id": "1", "sql_text": "SELECT a FROM public.orders WHERE id = 1", "elapsed_s": fast_pain, "risk_score": 10, "user_name": "u", "database_name": "db"},
                {"query_id": "2", "sql_text": "SELECT a FROM public.orders WHERE id = 2", "elapsed_s": fast_pain, "risk_score": 10, "user_name": "u", "database_name": "db"},
                {"query_id": "3", "sql_text": "SELECT b FROM public.customers WHERE id = 3", "elapsed_s": slow_pain, "risk_score": 10, "user_name": "u", "database_name": "db"},
                {"query_id": "4", "sql_text": "SELECT b FROM public.customers WHERE id = 4", "elapsed_s": slow_pain, "risk_score": 10, "user_name": "u", "database_name": "db"},
            ]
        )

    groups_a, _members_a = build_repeat_query_report(workload(10.0, 500.0))
    groups_b, _members_b = build_repeat_query_report(workload(500.0, 10.0))

    ids_a = {str(row["repeat_group_key"]): str(row["repeat_group_id"]) for _, row in groups_a.iterrows()}
    ids_b = {str(row["repeat_group_key"]): str(row["repeat_group_id"]) for _, row in groups_b.iterrows()}

    # Same durable keys in both runs…
    assert set(ids_a) == set(ids_b)
    assert all(key.startswith("G") for key in ids_a)
    # …while at least one display rank moved because the pain order flipped.
    assert any(ids_a[key] != ids_b[key] for key in ids_a)
