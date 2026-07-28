"""View explosion and the rewrite validation gate.

The validation gate is the package's core safety property: nothing is emitted
without passing it. These tests exercise it directly, since a hole here would
let a wrong rewrite reach a production cluster.
"""

from __future__ import annotations

import sqlglot

from redshift_sqlopt import Catalog, optimize, validate_rewrite

VIEW_CATALOG = Catalog.from_rows(
    table_rows=[
        {
            "database": "analytics",
            "schema": "public",
            "table": "fact_orders",
            "distkey": "cust_id",
            "sortkeys": "order_date",
            "row_count": 2_000_000_000,
        }
    ],
    view_rows=[
        {
            "database": "analytics",
            "schema": "reporting",
            "view": "v_orders",
            "sql": "SELECT order_id, cust_id, amount, order_date FROM analytics.public.fact_orders",
        }
    ],
)


def parse(sql: str):
    return sqlglot.parse_one(sql, read="redshift")


# ---------------------------------------------------------------------------
# view explosion
# ---------------------------------------------------------------------------


def test_view_is_inlined() -> None:
    result = optimize(
        "SELECT order_id FROM analytics.reporting.v_orders",
        catalog=VIEW_CATALOG,
    )
    assert "analytics.reporting.v_orders" in result.exploded_views


def test_view_inlining_reveals_the_base_table() -> None:
    """Inlining is what lets catalog and plan reasoning see the real table."""
    result = optimize(
        "SELECT order_id FROM analytics.reporting.v_orders",
        catalog=VIEW_CATALOG,
        detail_rows=[{"step": 1, "spilled_bytes": 5_000_000_000}],
    )
    assert result.exploded_views
    assert result.parsed


def test_view_explosion_can_be_disabled() -> None:
    result = optimize(
        "SELECT order_id FROM analytics.reporting.v_orders",
        catalog=VIEW_CATALOG,
        expand_views=False,
    )
    assert result.exploded_views == ()


def test_unknown_view_is_left_alone() -> None:
    result = optimize("SELECT a FROM some.unknown.thing", catalog=VIEW_CATALOG)
    assert result.exploded_views == ()
    assert result.parsed


def test_unparseable_view_body_does_not_crash() -> None:
    catalog = Catalog.from_rows(
        view_rows=[
            {"database": "d", "schema": "s", "view": "bad_view", "sql": "!!! not sql ///"}
        ]
    )
    result = optimize("SELECT a FROM d.s.bad_view", catalog=catalog)
    assert result.parsed
    assert result.exploded_views == ()


# ---------------------------------------------------------------------------
# validation gate
# ---------------------------------------------------------------------------


def test_identical_tree_passes_validation() -> None:
    tree = parse("SELECT a, b FROM t WHERE x = 1")
    notes = validate_rewrite(tree, tree.copy())
    assert not [note for note in notes if note.startswith("BLOCKED")]


def test_added_table_is_blocked() -> None:
    original = parse("SELECT a FROM t")
    rewritten = parse("SELECT a FROM t JOIN sneaky ON t.k = sneaky.k")
    notes = validate_rewrite(original, rewritten)
    assert any("changed the referenced table set" in note for note in notes)


def test_removed_table_is_blocked() -> None:
    original = parse("SELECT a FROM t JOIN u ON t.k = u.k")
    rewritten = parse("SELECT a FROM t")
    notes = validate_rewrite(original, rewritten)
    assert any(note.startswith("BLOCKED") for note in notes)


def test_invented_column_is_blocked() -> None:
    original = parse("SELECT a FROM t")
    rewritten = parse("SELECT a, phantom_column FROM t")
    notes = validate_rewrite(original, rewritten)
    assert any("introduced column reference" in note for note in notes)


def test_changed_projection_contract_is_blocked() -> None:
    original = parse("SELECT a, b FROM t")
    rewritten = parse("SELECT a FROM t")
    notes = validate_rewrite(original, rewritten)
    assert any("projection contract" in note for note in notes)


def test_reordered_projection_is_blocked() -> None:
    """Safe in a fingerprint hash, unsafe in emitted SQL — this must be caught."""
    original = parse("SELECT a, b FROM t")
    rewritten = parse("SELECT b, a FROM t")
    notes = validate_rewrite(original, rewritten)
    assert any("projection contract" in note for note in notes)


def test_validation_always_demands_human_verification() -> None:
    tree = parse("SELECT a FROM t")
    notes = validate_rewrite(tree, tree.copy())
    assert any("EXPLAIN" in note for note in notes)


def test_applied_rewrites_record_their_validation_notes() -> None:
    result = optimize(
        "SELECT order_id FROM analytics.public.fact_orders "
        "WHERE DATE(order_date) = '2024-01-01'",
        catalog=VIEW_CATALOG,
    )
    assert result.applied
    assert result.applied[0].validation_notes


def test_result_summary_is_renderable() -> None:
    result = optimize(
        "SELECT order_id FROM analytics.public.fact_orders "
        "WHERE DATE(order_date) = '2024-01-01'",
        catalog=VIEW_CATALOG,
        detail_rows=[{"step": 1, "spilled_bytes": 5_000_000_000}],
    )
    text = result.summary()
    assert "Fingerprint" in text
    assert isinstance(text, str) and text


# ---------------------------------------------------------------------------
# regression: the gate must catch an introduced tautology
# ---------------------------------------------------------------------------


def test_introduced_tautology_is_blocked() -> None:
    """A self-referential equality is structurally invisible but semantically fatal."""
    original = parse("SELECT a FROM t WHERE x IN (SELECT y FROM u)")
    rewritten = parse("SELECT a FROM t WHERE NOT EXISTS(SELECT 1 FROM u WHERE y = y)")
    notes = validate_rewrite(original, rewritten)
    assert any("self-referential" in note for note in notes if note.startswith("BLOCKED"))


def test_real_correlation_is_not_flagged_as_tautology() -> None:
    original = parse("SELECT a FROM t o WHERE o.k NOT IN (SELECT i.k FROM u i)")
    rewritten = parse(
        "SELECT a FROM t o WHERE NOT EXISTS(SELECT 1 FROM u i WHERE i.k = o.k)"
    )
    notes = validate_rewrite(original, rewritten)
    assert not [note for note in notes if note.startswith("BLOCKED")]


def test_preexisting_tautology_is_not_our_regression() -> None:
    """A tautology already in the original is the author's business."""
    tree = parse("SELECT a FROM t WHERE k = k")
    notes = validate_rewrite(tree, tree.copy())
    assert not [note for note in notes if note.startswith("BLOCKED")]
