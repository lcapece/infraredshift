"""Order-anonymous query fingerprinting.

Two queries that differ only in the order of commutative elements, in literal
values, or in alias naming are the *same query shape* and should collapse to one
identity. That identity is what lets a single fix be credited against every run
of that shape across the workload — turning "this query is slow" into "this
shape ran 3,200 times and cost 18 hours."

**This module never produces runnable SQL.** The canonical form it builds sorts
the SELECT list, which is safe for hashing and unsafe to execute: reordering
projections changes the result contract and silently breaks positional
references like ``GROUP BY 1, 2`` and ``ORDER BY 3``. Emitted SQL comes from
``optimizer.py``, which applies only order changes that preserve semantics.
Keeping the two paths separate is deliberate — sharing a normalizer between them
is how a fingerprinting shortcut becomes a corrupted rewrite.

Verified behaviour of the underlying library (sqlglot 26.x): its ``simplify``
rule already canonicalizes AND/OR operand order and flattens boolean nesting,
so predicate ordering is handled upstream. What it deliberately does not do is
reorder the projection — hence the extra pass here.
"""

from __future__ import annotations

import hashlib
from functools import lru_cache

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.simplify import simplify

DIALECT = "redshift"


def fingerprint(sql: str, *, dialect: str = DIALECT) -> tuple[str, str]:
    """Return ``(hash, method)`` for a SQL statement.

    ``method`` is ``"ast"`` when the statement parsed and was canonicalized, or
    ``"text"`` when it did not parse and a normalized-text fallback was used.
    A text fingerprint still groups identical statements; it just cannot see
    through reordering. Callers that need to know whether the shape is
    trustworthy should check the method.
    """
    if not str(sql or "").strip():
        return "", "empty"
    shape, method = canonical_shape(str(sql))
    if not shape:
        return "", method
    digest = hashlib.sha256(shape.encode("utf-8", errors="replace")).hexdigest()[:16]
    return f"Q{digest}", method


@lru_cache(maxsize=4096)
def canonical_shape(sql: str, dialect: str = DIALECT) -> tuple[str, str]:
    """Build the canonical, order-anonymous string form of a statement."""
    try:
        tree = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return _text_shape(sql), "text"
    if tree is None:
        return _text_shape(sql), "text"

    try:
        canonical = tree.copy()
        canonical = _canonicalize_aliases(canonical)
        canonical = canonical.transform(_strip_literals)
        canonical = simplify(canonical)
        canonical = canonical.transform(_sort_commutative)
        rendered = canonical.sql(dialect=dialect, comments=False, normalize=True)
        return " ".join(rendered.lower().split()), "ast"
    except Exception:
        return _text_shape(sql), "text"


def _canonicalize_aliases(tree: exp.Expression) -> exp.Expression:
    """Rename table aliases to positional names so naming does not split a shape.

    The same query written with ``FROM fact_orders o`` and ``FROM fact_orders c``
    is one shape; the alias is the author's private choice. Aliases are replaced
    with ``_t0``, ``_t1``, ... assigned in the order the sources appear, and
    every column qualifier is rewritten to match.

    Deliberately *not* done via sqlglot's ``qualify`` rule: qualify needs a
    schema to resolve unqualified columns and raises without one, whereas
    fingerprinting must work with no catalog at all. This pass is purely
    syntactic and never fails on an unknown column.

    Column *aliases* are left alone — they are part of the output contract and
    two queries returning differently-named columns are not the same query.
    """
    mapping: dict[str, str] = {}
    for table in tree.find_all(exp.Table):
        alias = str(table.alias or "").strip('"').lower()
        if alias and alias not in mapping:
            mapping[alias] = f"_t{len(mapping)}"

    if not mapping:
        return tree

    for table in tree.find_all(exp.Table):
        alias = str(table.alias or "").strip('"').lower()
        if alias in mapping:
            table.set("alias", exp.TableAlias(this=exp.to_identifier(mapping[alias])))

    for column in tree.find_all(exp.Column):
        qualifier = str(column.table or "").strip('"').lower()
        if qualifier in mapping:
            column.set("table", exp.to_identifier(mapping[qualifier]))

    return tree


def _strip_literals(node: exp.Expression) -> exp.Expression:
    """Collapse literal values so parameter differences do not split a family.

    ``WHERE id = 5`` and ``WHERE id = 7`` are the same shape. Multi-element IN
    lists collapse to a single placeholder so that list *length* does not split
    the family either, and multi-row VALUES clauses collapse to one row.
    """
    if isinstance(node, exp.In):
        expressions = node.args.get("expressions") or []
        if len(expressions) > 1:
            node.set("expressions", [exp.Placeholder()])
    elif isinstance(node, exp.Values):
        rows = node.args.get("expressions") or []
        if len(rows) > 1:
            node.set("expressions", rows[:1])
    if isinstance(node, exp.Literal):
        return exp.Placeholder()
    return node


def _sort_commutative(node: exp.Expression) -> exp.Expression:
    """Sort operands whose order carries no meaning.

    Applied to the SELECT list and GROUP BY keys. NOT applied to ORDER BY,
    whose order is the entire point, nor to window ORDER BY clauses.

    Safe here only because this tree is destined for a hash. See module
    docstring.
    """
    if isinstance(node, exp.Select):
        projections = node.expressions or []
        if len(projections) > 1:
            node.set(
                "expressions",
                sorted(projections, key=lambda item: item.sql(dialect=DIALECT, normalize=True)),
            )
        group = node.args.get("group")
        if group is not None:
            keys = group.expressions or []
            if len(keys) > 1:
                group.set(
                    "expressions",
                    sorted(keys, key=lambda item: item.sql(dialect=DIALECT, normalize=True)),
                )
    return node


def _text_shape(sql: str) -> str:
    """Fallback shape for statements sqlglot cannot parse.

    Strips comments and literals with plain text handling. This cannot see
    through reordering, so it groups less aggressively than the AST path — but
    it never claims two different statements are the same.
    """
    import re

    text = str(sql or "")
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.DOTALL)
    text = re.sub(r"--[^\r\n]*", " ", text)
    text = re.sub(r"'(?:''|[^'])*'", "?", text)
    text = text.lower()
    text = re.sub(r"(?<![a-z0-9_$])[-+]?\d+(?:\.\d+)?(?![a-z0-9_$])", "?", text)
    text = re.sub(r"\(\s*\?(?:\s*,\s*\?)+\s*\)", "(?)", text)
    return " ".join(text.split())


def same_shape(left: str, right: str, *, dialect: str = DIALECT) -> bool:
    """True when two statements share a canonical shape."""
    left_hash, left_method = fingerprint(left, dialect=dialect)
    right_hash, right_method = fingerprint(right, dialect=dialect)
    if not left_hash or not right_hash:
        return False
    return left_hash == right_hash and left_method == right_method
