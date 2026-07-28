"""Order-anonymous fingerprinting.

The negative cases matter as much as the positive ones: a canonicalizer that
collapses everything would pass every "same" test and be useless.
"""

from __future__ import annotations

import pytest

from redshift_sqlopt import fingerprint, same_shape


@pytest.mark.parametrize(
    "left,right",
    [
        # commutative predicate order
        ("SELECT a FROM t WHERE x = 1 AND y = 2", "SELECT a FROM t WHERE y = 2 AND x = 1"),
        # projection order
        ("SELECT a, b FROM t", "SELECT b, a FROM t"),
        # literal values
        ("SELECT a FROM t WHERE id = 5", "SELECT a FROM t WHERE id = 7"),
        # IN-list length
        ("SELECT a FROM t WHERE id IN (1, 2, 3)", "SELECT a FROM t WHERE id IN (4, 5, 6, 7)"),
        # GROUP BY key order
        (
            "SELECT a, b, COUNT(*) FROM t GROUP BY a, b",
            "SELECT b, a, COUNT(*) FROM t GROUP BY b, a",
        ),
        # boolean nesting
        ("SELECT a FROM t WHERE (x = 1 AND y = 2)", "SELECT a FROM t WHERE y = 2 AND x = 1"),
        # string literals
        ("SELECT a FROM t WHERE s = 'foo'", "SELECT a FROM t WHERE s = 'bar'"),
    ],
)
def test_same_shape(left: str, right: str) -> None:
    assert same_shape(left, right), f"expected same shape:\n{left}\n{right}"


@pytest.mark.parametrize(
    "left,right",
    [
        # different table
        ("SELECT a FROM t", "SELECT a FROM u"),
        # different column
        ("SELECT a FROM t", "SELECT z FROM t"),
        # ORDER BY direction is semantic, never normalized away
        ("SELECT a FROM t ORDER BY a ASC", "SELECT a FROM t ORDER BY a DESC"),
        # an extra predicate is a different query
        ("SELECT a FROM t WHERE x = 1", "SELECT a FROM t WHERE x = 1 AND y = 2"),
        # different aggregate
        ("SELECT SUM(a) FROM t", "SELECT MAX(a) FROM t"),
        # join vs no join
        ("SELECT a FROM t", "SELECT a FROM t JOIN u ON t.k = u.k"),
    ],
)
def test_different_shape(left: str, right: str) -> None:
    assert not same_shape(left, right), f"expected different shapes:\n{left}\n{right}"


def test_table_aliases_are_anonymous() -> None:
    """The alias is the author's private choice, not part of the query shape."""
    assert same_shape(
        "SELECT o.amount FROM analytics.public.fact_orders o WHERE o.cust_id = 1",
        "SELECT c.amount FROM analytics.public.fact_orders c WHERE c.cust_id = 1",
    )


def test_alias_anonymity_across_a_join() -> None:
    assert same_shape(
        "SELECT o.id FROM orders o JOIN customers c ON o.cid = c.id",
        "SELECT x.id FROM orders x JOIN customers y ON x.cid = y.id",
    )


def test_alias_anonymity_does_not_merge_different_join_orders() -> None:
    """Positional alias renaming must not make genuinely different queries equal."""
    assert not same_shape(
        "SELECT o.id FROM orders o JOIN customers c ON o.cid = c.id",
        "SELECT o.id FROM customers o JOIN orders c ON o.cid = c.id",
    )


def test_output_column_aliases_still_matter() -> None:
    """Column aliases are the output contract, unlike table aliases."""
    assert not same_shape(
        "SELECT amount AS total FROM t",
        "SELECT amount AS grand_total FROM t",
    )


def test_order_by_position_is_preserved() -> None:
    """ORDER BY column order changes results and must not be normalized."""
    assert not same_shape(
        "SELECT a, b FROM t ORDER BY a, b",
        "SELECT a, b FROM t ORDER BY b, a",
    )


def test_unparseable_sql_falls_back_to_text() -> None:
    digest, method = fingerprint("SELECT !!! FROM ///")
    assert method in {"text", "ast"}
    assert digest


def test_empty_sql() -> None:
    assert fingerprint("") == ("", "empty")
    assert fingerprint("   ") == ("", "empty")


def test_fingerprint_is_stable_across_calls() -> None:
    sql = "SELECT a, b FROM t WHERE x = 1"
    assert fingerprint(sql) == fingerprint(sql)


def test_whitespace_and_case_do_not_matter() -> None:
    assert same_shape(
        "select   a\n from  t\twhere x=1",
        "SELECT a FROM t WHERE x = 1",
    )
