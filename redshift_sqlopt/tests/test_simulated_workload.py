"""End-to-end simulation over 100 queries with synthetic-but-realistic plan rows.

Purpose: exercise the whole pipeline at workload scale without a cluster. Real
Redshift validation would cost money and, on a small sample dataset, would not
even produce the multi-billion-row broadcasts the findings are tuned for.

Scaling: durations and byte counts are scaled down (a "slow" query is 5s here,
not 300s) while the *ratios* that actually drive findings are preserved —
estimate-error multiples, spill relative to the floor, broadcast row counts
relative to BROADCAST_ROW_FLOOR. Thresholds are compared against absolute
values, so each scenario states its own numbers explicitly rather than relying
on a global scale factor.

What this catches that unit tests do not:
* a rule that throws on some shape combination only seen in bulk;
* a rule that fires on far more (or far fewer) queries than intended;
* emitted SQL that does not re-parse;
* findings that never fire because a threshold is unreachable.
"""

from __future__ import annotations

import sqlglot

from redshift_sqlopt import Catalog, Tier, optimize

# ---------------------------------------------------------------------------
# catalog: a small star schema with deliberately mixed physical design
# ---------------------------------------------------------------------------

CATALOG = Catalog.from_rows(
    table_rows=[
        # Large fact, well keyed.
        {
            "database": "dw",
            "schema": "public",
            "table": "fact_sales",
            "distkey": "cust_id",
            "diststyle": "KEY",
            "sortkeys": "sale_date",
            "not_null_columns": "cust_id,sale_date",
            "row_count": 40_000_000,
            "size_mb": 8_600,
        },
        # Large fact, UNKEYED -> should raise DDL findings.
        {
            "database": "dw",
            "schema": "public",
            "table": "fact_events",
            "distkey": "",
            "diststyle": "EVEN",
            "sortkeys": "",
            "row_count": 25_000_000,
            "size_mb": 5_200,
            "skew_ratio": 7.5,
        },
        # Dimensions.
        {
            "database": "dw",
            "schema": "public",
            "table": "dim_customer",
            "distkey": "cust_id",
            "diststyle": "KEY",
            "sortkeys": "cust_id",
            "not_null_columns": "cust_id",
            "row_count": 2_000_000,
            "size_mb": 240,
        },
        {
            "database": "dw",
            "schema": "public",
            "table": "dim_product",
            "distkey": "prod_id",
            "diststyle": "ALL",
            "sortkeys": "prod_id",
            "not_null_columns": "prod_id",
            "row_count": 80_000,
            "size_mb": 12,
        },
    ],
    view_rows=[
        {
            "database": "dw",
            "schema": "reporting",
            "view": "v_sales",
            "sql": (
                "SELECT sale_id, cust_id, prod_id, amount, sale_date "
                "FROM dw.public.fact_sales"
            ),
        }
    ],
)


# ---------------------------------------------------------------------------
# query generators: each returns SQL exhibiting one known anti-pattern
# ---------------------------------------------------------------------------


def _sargable_killers(n: int) -> list[str]:
    """Function-wrapped sort-key column: defeats zone-map pruning."""
    wrappers = ["DATE({c})", "CAST({c} AS DATE)", "DATE_TRUNC('day', {c})"]
    out = []
    for i in range(n):
        wrapper = wrappers[i % len(wrappers)].format(c="sale_date")
        out.append(
            f"SELECT sale_id, amount FROM dw.public.fact_sales "
            f"WHERE {wrapper} = '2024-0{i % 9 + 1}-15'"
        )
    return out


def _redundant_distincts(n: int) -> list[str]:
    return [
        f"SELECT DISTINCT cust_id, COUNT(*) AS n FROM dw.public.fact_sales "
        f"WHERE amount > {i * 10} GROUP BY cust_id"
        for i in range(n)
    ]


def _unpropagated_filters(n: int) -> list[str]:
    return [
        f"SELECT s.sale_id, c.region FROM dw.public.fact_sales s "
        f"JOIN dw.public.dim_customer c ON s.cust_id = c.cust_id "
        f"WHERE s.cust_id = {1000 + i}"
        for i in range(n)
    ]


def _qualified_not_ins(n: int) -> list[str]:
    """Qualified so the correlation can be built; nullability is proven."""
    return [
        f"SELECT s.sale_id FROM dw.public.fact_sales s "
        f"WHERE s.cust_id NOT IN "
        f"(SELECT d.cust_id FROM dw.public.dim_customer d WHERE d.region = 'R{i % 5}')"
        for i in range(n)
    ]


def _unqualified_not_ins(n: int) -> list[str]:
    """Unqualified: must be REFUSED, never rewritten into a tautology."""
    return [
        f"SELECT sale_id FROM dw.public.fact_sales "
        f"WHERE cust_id NOT IN (SELECT cust_id FROM dw.public.dim_customer) "
        f"AND amount > {i}"
        for i in range(n)
    ]


def _view_queries(n: int) -> list[str]:
    return [
        f"SELECT sale_id, amount FROM dw.reporting.v_sales WHERE amount > {i * 100}"
        for i in range(n)
    ]


def _clean_queries(n: int) -> list[str]:
    """Already good: must produce no rewrite at all."""
    return [
        f"SELECT sale_id, amount FROM dw.public.fact_sales "
        f"WHERE sale_date >= '2024-0{i % 9 + 1}-01' AND cust_id = {i}"
        for i in range(n)
    ]


def _outer_join_filters(n: int) -> list[str]:
    """LEFT JOIN: filter propagation must be refused."""
    return [
        f"SELECT s.sale_id, c.region FROM dw.public.fact_sales s "
        f"LEFT JOIN dw.public.dim_customer c ON s.cust_id = c.cust_id "
        f"WHERE s.cust_id = {2000 + i}"
        for i in range(n)
    ]


def _window_queries(n: int) -> list[str]:
    return [
        f"SELECT cust_id, amount, "
        f"ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY sale_date DESC) AS rn "
        f"FROM dw.public.fact_sales WHERE amount > {i * 5}"
        for i in range(n)
    ]


def _unparseable(n: int) -> list[str]:
    """Must degrade to findings-only, never crash the batch."""
    return [f"SELECT !!! FROM /// WHERE {i}" for i in range(n)]


def build_workload() -> list[str]:
    """100 queries across ten shapes, 10 of each."""
    workload: list[str] = []
    for generator in (
        _sargable_killers,
        _redundant_distincts,
        _unpropagated_filters,
        _qualified_not_ins,
        _unqualified_not_ins,
        _view_queries,
        _clean_queries,
        _outer_join_filters,
        _window_queries,
        _unparseable,
    ):
        workload.extend(generator(10))
    return workload


# ---------------------------------------------------------------------------
# plan rows: scaled-down but ratio-faithful
# ---------------------------------------------------------------------------


def plan_for(index: int) -> tuple[list[dict], list[dict]]:
    """Synthesize plan rows for query *index*.

    Durations are in the 0.2-5s range rather than minutes. What is preserved is
    the shape of the problem: every third query broadcasts above the row floor,
    every fifth spills above the byte floor, every seventh has a large planner
    estimate error.
    """
    step = 3 + (index % 4)
    broadcast = index % 3 == 0
    spills = index % 5 == 0
    bad_estimate = index % 7 == 0

    estimated = 500 if bad_estimate else 900_000
    actual = 1_200_000 if broadcast else 800_000

    explain = [
        {
            "step": step,
            "operation": (
                "XN Hash Join DS_BCAST_INNER" if broadcast else "XN Hash Join DS_DIST_NONE"
            ),
            "rows": estimated,
            "table_name": "dw.public.fact_events" if broadcast else "dw.public.fact_sales",
        }
    ]
    detail = [
        {
            "step": step,
            "output_rows": actual,
            "input_bytes": 60_000_000_000 if index % 11 == 0 else 400_000_000,
            "spilled_bytes": 2_000_000_000 if spills else 0,
            # 5s "slow", not 300s — scaled down deliberately.
            "duration_s": round(0.2 + (index % 25) * 0.2, 2),
        }
    ]
    return explain, detail


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


def run_workload() -> list:
    results = []
    workload = build_workload()
    for index, sql in enumerate(workload):
        explain, detail = plan_for(index)
        results.append(
            optimize(sql, catalog=CATALOG, explain_rows=explain, detail_rows=detail)
        )
    return results


def test_workload_is_one_hundred_queries() -> None:
    assert len(build_workload()) == 100


def test_no_query_crashes_the_pipeline() -> None:
    """Ten of the hundred are unparseable; none may raise."""
    results = run_workload()
    assert len(results) == 100


def test_unparseable_queries_degrade_to_findings_only() -> None:
    results = run_workload()
    failed = [r for r in results if not r.parsed]
    assert len(failed) == 10, "expected exactly the 10 injected bad queries"
    for result in failed:
        assert not result.has_rewrite
        assert result.parse_failure is not None


def test_every_emitted_rewrite_reparses() -> None:
    """The single most important invariant: never emit invalid SQL."""
    for result in run_workload():
        if result.has_rewrite:
            assert sqlglot.parse_one(result.rewritten_sql, read="redshift") is not None


def test_clean_queries_are_left_alone() -> None:
    """A well-written query must not be 'optimized'."""
    for index, sql in enumerate(_clean_queries(10)):
        result = optimize(sql, catalog=CATALOG)
        assert not result.has_rewrite, f"clean query {index} was rewritten: {sql}"


def test_unqualified_not_in_is_never_rewritten() -> None:
    """Regression at scale for the tautology bug."""
    for sql in _unqualified_not_ins(10):
        result = optimize(sql, catalog=CATALOG)
        codes = {item.code for item in result.applied}
        assert "NOT_IN_TO_NOT_EXISTS" not in codes
        emitted = (result.rewritten_sql or "").replace("\n", " ")
        assert "cust_id = cust_id" not in emitted


def test_qualified_not_in_is_rewritten_with_a_real_correlation() -> None:
    for sql in _qualified_not_ins(10):
        result = optimize(sql, catalog=CATALOG)
        codes = {item.code for item in result.applied}
        assert "NOT_IN_TO_NOT_EXISTS" in codes
        assert "d.cust_id = s.cust_id" in result.rewritten_sql.replace("\n", " ")


def test_outer_join_filters_are_never_propagated() -> None:
    for sql in _outer_join_filters(10):
        result = optimize(sql, catalog=CATALOG)
        assert "PROPAGATE_JOIN_FILTER" not in {item.code for item in result.applied}


def test_rules_fire_on_the_shapes_they_target() -> None:
    """Coverage check: each targeted anti-pattern must actually be caught."""
    expected = {
        "SARGABLE_SORTKEY": _sargable_killers,
        "REDUNDANT_DISTINCT": _redundant_distincts,
        "PROPAGATE_JOIN_FILTER": _unpropagated_filters,
        "NOT_IN_TO_NOT_EXISTS": _qualified_not_ins,
    }
    for code, generator in expected.items():
        hits = sum(
            1
            for sql in generator(10)
            if code in {item.code for item in optimize(sql, catalog=CATALOG).applied}
        )
        assert hits >= 8, f"{code} fired on only {hits}/10 of its target shape"


def test_unkeyed_table_raises_ddl_findings_across_the_workload() -> None:
    """fact_events has no distkey and no sortkey; broadcasts must cite it."""
    results = run_workload()
    ddl = [
        finding
        for result in results
        for finding in result.findings
        if finding.tier is Tier.DDL
    ]
    assert ddl, "no DDL findings produced across 100 queries"
    assert any("fact_events" in " ".join(f.tables) for f in ddl)


def test_findings_are_always_ranked_cheapest_first() -> None:
    for result in run_workload():
        tiers = [int(f.tier) for f in result.ranked_findings()]
        assert tiers == sorted(tiers)


def test_views_are_inlined_across_the_workload() -> None:
    for sql in _view_queries(10):
        result = optimize(sql, catalog=CATALOG)
        assert "dw.reporting.v_sales" in result.exploded_views


def test_fingerprints_group_same_shape_different_literals() -> None:
    """The 10 clean queries differ only in literals -> few distinct shapes."""
    shapes = {optimize(sql, catalog=CATALOG).fingerprint for sql in _clean_queries(10)}
    assert len(shapes) == 1, f"expected 1 shape, got {len(shapes)}"


def test_distinct_shapes_do_not_collide() -> None:
    generators = (
        _sargable_killers,
        _redundant_distincts,
        _unpropagated_filters,
        _window_queries,
    )
    shapes = {
        optimize(generator(1)[0], catalog=CATALOG).fingerprint for generator in generators
    }
    assert len(shapes) == len(generators)


def test_blocked_rewrites_always_explain_themselves() -> None:
    """A refusal with no reason is useless to the engineer reading it."""
    for result in run_workload():
        for item in result.blocked:
            assert item.reason.strip(), f"{item.code} blocked with no reason"
