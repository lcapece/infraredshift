"""Regression tests for repeat-query grouping.

These encode the grouping guarantees: queries that are the same shape with
different literals MUST group; queries on different tables MUST NOT group.
Every real-world mis-grouping found in production captures should be added
here as a new case.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analyzer.query_similarity import (  # noqa: E402
    build_repeat_query_report,
    canonical_sql_fingerprint,
)


def _frame(sqls: list[str], *, query_type: str = "SELECT", users: list[str] | None = None) -> pd.DataFrame:
    rows = []
    for i, sql in enumerate(sqls):
        rows.append(
            {
                "query_id": 1000 + i,
                "snapshot_id": "test",
                "sql_text": sql,
                "user_name": (users[i] if users else "etl_user"),
                "database_name": "dev",
                "query_type": query_type,
                "elapsed_s": 10.0,
                "risk_score": 1.0,
                "start_time": "2026-06-01 00:00:00",
                "dominant_issue": "",
            }
        )
    return pd.DataFrame(rows)


def _group_count(frame: pd.DataFrame) -> tuple[int, pd.DataFrame]:
    groups, members = build_repeat_query_report(frame)
    return len(groups), members


def test_date_literals_group_together():
    sqls = [
        f"SELECT store_id, SUM(net) FROM sales.fact_orders WHERE order_date = '2026-06-{d:02d}' GROUP BY store_id"
        for d in range(1, 8)
    ]
    count, members = _group_count(_frame(sqls))
    assert count == 1
    assert len(members) == 7


def test_in_list_length_does_not_split():
    sqls = [
        "SELECT id, email FROM dim_customer WHERE id IN ({})".format(
            ",".join(str(100 + j) for j in range(n))
        )
        for n in (1, 2, 5, 12, 40)
    ]
    count, members = _group_count(_frame(sqls))
    assert count == 1
    assert len(members) == 5


def test_comments_case_whitespace_do_not_split():
    sqls = [
        "-- morning run\nselect A.x, sum(B.y) FROM  t1 a JOIN t2 b ON a.k=b.k WHERE b.d > 5 group by 1",
        "SELECT a.x, SUM(b.y) FROM t1 A JOIN t2 B ON A.k = B.k WHERE B.d > 99 GROUP BY 1",
        "/* nightly */ SELECT a.x , sum(b.y) FROM t1 a JOIN t2 b ON a.k=b.k WHERE b.d > 12345 GROUP BY 1",
    ]
    count, members = _group_count(_frame(sqls))
    assert count == 1
    assert len(members) == 3


def test_unload_wrappers_group_on_inner_sql():
    sqls = [
        "UNLOAD ('SELECT region, count(*) FROM public.events WHERE ts >= ''2026-0{m}-01'' GROUP BY region') "
        "TO 's3://bkt/out_{m}/' IAM_ROLE 'arn:aws:iam::1:role/r'".format(m=m)
        for m in range(1, 5)
    ]
    count, members = _group_count(_frame(sqls))
    assert count == 1
    assert len(members) == 4


def test_limit_variants_group_together():
    sqls = [
        f"SELECT product_id, revenue FROM mart.product_revenue WHERE snapshot_day = DATE '2026-06-0{d}' "
        f"ORDER BY revenue DESC LIMIT {n}"
        for d, n in [(1, 100), (2, 100), (3, 500), (4, 1000)]
    ]
    count, members = _group_count(_frame(sqls))
    assert count == 1
    assert len(members) == 4


def test_different_tables_never_merge():
    sqls = [
        "SELECT a, b FROM schema1.orders WHERE d = '2026-06-01'",
        "SELECT a, b FROM schema1.orders WHERE d = '2026-06-02'",
        "SELECT a, b FROM schema1.refunds WHERE d = '2026-06-01'",
        "SELECT a, b FROM schema1.refunds WHERE d = '2026-06-02'",
    ]
    groups, members = build_repeat_query_report(_frame(sqls))
    assert len(groups) == 2
    tables = set(groups["shared_tables"])
    assert any("orders" in t for t in tables)
    assert any("refunds" in t for t in tables)


def test_call_statements_group_by_procedure():
    sqls = [
        "CALL etl.load_daily_sales('2026-06-01', 42)",
        "CALL etl.load_daily_sales('2026-06-02', 43)",
        "call ETL.LOAD_DAILY_SALES ( '2026-06-03' , 44 )",
    ]
    count, members = _group_count(_frame(sqls, query_type="CALL"))
    assert count == 1
    assert len(members) == 3


def test_users_do_not_split_groups_by_default():
    sqls = [
        "SELECT x FROM t WHERE d = '2026-06-01'",
        "SELECT x FROM t WHERE d = '2026-06-02'",
    ]
    count, members = _group_count(_frame(sqls, users=["alice", "bob"]))
    assert count == 1
    assert len(members) == 2


def test_scope_by_user_splits_when_enabled():
    sqls = [
        "SELECT x FROM t WHERE d = '2026-06-01'",
        "SELECT x FROM t WHERE d = '2026-06-02'",
        "SELECT x FROM t WHERE d = '2026-06-03'",
        "SELECT x FROM t WHERE d = '2026-06-04'",
    ]
    frame = _frame(sqls, users=["alice", "alice", "bob", "bob"])
    groups, _ = build_repeat_query_report(frame, scope_by_user=True)
    assert len(groups) == 2


def test_unparseable_sql_still_groups_via_regex_fallback():
    sqls = [
        "FETCH FORWARD 500 IN weird_cursor_abc; SELECT bogus syntax %% FROM t WHERE v = 111 AND ts='2026-06-01'",
        "FETCH FORWARD 500 IN weird_cursor_abc; SELECT bogus syntax %% FROM t WHERE v = 999 AND ts='2026-06-30'",
    ]
    groups, members = build_repeat_query_report(_frame(sqls))
    assert len(groups) == 1
    assert groups.iloc[0]["fingerprint_method"] in ("regex", "ast")


def test_fingerprint_is_literal_free():
    a, method_a = canonical_sql_fingerprint(
        "SELECT c1 FROM s.t WHERE dt BETWEEN '2026-01-01' AND '2026-01-31' AND n IN (1,2,3)"
    )
    b, method_b = canonical_sql_fingerprint(
        "select C1 from S.T where DT between '2025-07-04' and '2025-08-08' and N in (9)"
    )
    assert method_a == method_b == "ast"
    assert a == b
    assert "2026" not in a and "'" not in a


def test_short_call_statements_group():
    sqls = ["CALL etl.nightly()", "CALL etl.nightly()", "CALL etl.nightly()"]
    count, members = _group_count(_frame(sqls, query_type="CALL"))
    assert count == 1
    assert len(members) == 3


def test_unqualified_call_matches_captured_procedure():
    proc_defs = pd.DataFrame(
        [
            {
                "procedure_key": "dev.etl.load_daily_sales",
                "source_definition": "BEGIN INSERT INTO sales.fact_orders SELECT 1; END;",
            }
        ]
    )
    sqls = [
        "CALL load_daily_sales('2026-06-01')",
        "CALL etl.load_daily_sales('2026-06-02')",
    ]
    groups, members = build_repeat_query_report(
        _frame(sqls, query_type="CALL"), procedure_definitions=proc_defs
    )
    assert len(groups) == 1
    assert len(members) == 2
    assert "insert into sales.fact_orders" in str(groups.iloc[0]["procedure_definition"]).lower()


def test_procedure_body_sql_is_extracted_and_analyzed():
    body = (
        "BEGIN\n"
        "  DELETE FROM sales.fact_orders WHERE event_date >= p_start_date;\n"
        "  INSERT INTO sales.fact_orders\n"
        "  SELECT * FROM staging.fact_orders WHERE event_date >= p_start_date;\n"
        "  RAISE INFO 'loaded';\n"
        "END"
    )
    proc_defs = pd.DataFrame(
        [{"procedure_key": "dev.etl.refresh_orders", "source_definition": body}]
    )
    sqls = [
        "CALL etl.refresh_orders('2026-06-01')",
        "CALL etl.refresh_orders('2026-06-02')",
    ]
    groups, _ = build_repeat_query_report(
        _frame(sqls, query_type="CALL"), procedure_definitions=proc_defs
    )
    assert len(groups) == 1
    full = str(groups.iloc[0]["sql_tables_full"])
    assert "sales.fact_orders" in full
    assert "staging.fact_orders" in full
    assert float(groups.iloc[0]["parse_success_rate"]) > 0
    assert int(groups.iloc[0]["wildcard_count"]) >= 1


def test_quoted_identifier_tables_do_not_fuzzy_merge():
    sqls = [
        "DELETE FROM \"Sales Schema\".\"orders_a\" WHERE d < '2026-06-01' AND batch_id = 11",
        "DELETE FROM \"Sales Schema\".\"orders_a\" WHERE d < '2026-06-02' AND batch_id = 12",
        "DELETE FROM \"Sales Schema\".\"orders_b\" WHERE d < '2026-06-01' AND batch_id = 13",
        "DELETE FROM \"Sales Schema\".\"orders_b\" WHERE d < '2026-06-02' AND batch_id = 14",
    ]
    groups, _ = build_repeat_query_report(_frame(sqls, query_type="DELETE"))
    assert len(groups) == 2


def test_sql_tables_full_lists_every_table():
    join_tables = [f"warehouse.dim_{chr(97 + i)}{i:02d}" for i in range(16)]
    base = (
        "SELECT t00.x FROM "
        + join_tables[0]
        + " t00 "
        + " ".join(
            f"JOIN {name} t{i:02d} ON t00.k = t{i:02d}.k"
            for i, name in enumerate(join_tables[1:], start=1)
        )
    )
    sqls = [base + f" WHERE t00.d = '2026-06-0{d}'" for d in (1, 2)]
    groups, _ = build_repeat_query_report(_frame(sqls))
    assert len(groups) == 1
    full = str(groups.iloc[0]["sql_tables_full"])
    for name in join_tables:
        assert name in full
    display = [part for part in str(groups.iloc[0]["sql_tables"]).split(", ") if part]
    assert len(display) <= 14


def test_min_group_size_two():
    sqls = [
        "SELECT x FROM t WHERE d = '2026-06-01'",
        "SELECT x FROM t WHERE d = '2026-06-02'",
        "SELECT completely_different_query FROM other_table WHERE z = 5",
    ]
    groups, members = build_repeat_query_report(_frame(sqls))
    assert len(groups) == 1
    assert int(groups.iloc[0]["query_count"]) == 2


def test_repeat_members_preserve_full_sql_for_triage_diagram():
    projection = ", ".join(f"col_{i:03d}" for i in range(260))
    sqls = [
        f"SELECT {projection} FROM public.fact_orders WHERE order_date = '2026-06-{day:02d}'"
        for day in (1, 2)
    ]

    groups, members = build_repeat_query_report(_frame(sqls))

    assert len(groups) == 1
    assert "sql_text_full" in members.columns
    assert str(members.iloc[0]["sql_text"]).endswith("...")
    assert str(members.iloc[0]["sql_text_full"]) == sqls[0]


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as exc:
                failed += 1
                print(f"FAIL {name}: {exc}")
    raise SystemExit(1 if failed else 0)


def test_using_join_does_not_crash_grouping():
    # Regression: JOIN ... USING(col) once crashed the entire workload analysis
    # because sqlglot stores USING as a list and .find_all() was called on it.
    import pandas as pd
    from analyzer.query_similarity import build_repeat_query_report
    sql = "SELECT o.id, c.name FROM orders o JOIN customers c USING(cust_id) WHERE o.dt > 5"
    df = pd.DataFrame({
        "query_id": ["1", "2"],
        "sql_text": [sql, sql.replace("5", "9")],
        "database_name": ["db1", "db1"],
        "user_name": ["a", "b"],
        "elapsed_s": [10.0, 12.0],
        "risk_score": [5, 5],
        "snapshot_id": ["s", "s"],
    })
    groups, members = build_repeat_query_report(df)
    assert len(groups) == 1, "the two USING-join variants should group together"
    assert len(members) == 2


def test_fuzzy_merge_requires_every_shape_pair_to_meet_threshold(monkeypatch):
    from analyzer import query_similarity as similarity

    def item(shape: str) -> dict:
        return {
            "repeat_kind": "sql_text",
            "constraint_key": "owner|shape|select",
            "query_type": "SELECT",
            "group_sql_shape": shape,
            "shape_score": 1.0,
            "match_basis": "exact shape",
        }

    scores = {
        frozenset(("A from public.orders", "B from public.orders")): 0.96,
        frozenset(("B from public.orders", "C from public.orders")): 0.96,
        frozenset(("A from public.orders", "C from public.orders")): 0.92,
    }
    monkeypatch.setattr(
        similarity,
        "_text_ratio",
        lambda left, right: scores[frozenset((left, right))],
    )
    monkeypatch.setattr(
        similarity,
        "_shape_table_set",
        lambda _shape: frozenset(("public.orders",)),
    )

    merged = similarity._fuzzy_merge_shape_buckets(
        [[item("A from public.orders")], [item("B from public.orders")], [item("C from public.orders")]],
        0.95,
    )

    assert sorted(len(group) for group in merged) == [1, 2]


def test_placeholder_in_projection_slot_does_not_break_grouping():
    """Regression: `select.expressions or []` is not a guard.

    After literal canonicalization an arg slot can hold a bare Placeholder.
    A Placeholder is truthy, so `or []` never fires and the loop raised
    "'Placeholder' object is not iterable" - which propagated out of the
    grouping pass and zeroed EVERY repeat group for the whole workload.
    """
    import sqlglot
    from sqlglot import exp

    from analyzer.query_similarity import _projected_columns

    tree = sqlglot.parse_one("SELECT a, b FROM t WHERE id IN (1,2,3)", read="redshift")
    tree.find(exp.Select).set("expressions", exp.Placeholder())
    assert _projected_columns([tree.find(exp.Select)]) == frozenset()


def test_literal_heavy_sql_survives_analysis():
    from analyzer.query_similarity import analyze_sql

    for sql in (
        "SELECT 1, 2, 3 FROM t",
        "SELECT a FROM t WHERE (a, b) IN ((1, 2), (3, 4))",
        "INSERT INTO t VALUES (1, 2), (3, 4)",
        "SELECT DISTINCT 1 FROM t GROUP BY 1 ORDER BY 1",
    ):
        analyze_sql(sql)  # must not raise
