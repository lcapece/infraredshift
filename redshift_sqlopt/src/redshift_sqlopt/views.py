"""View explosion: replace view references with their SELECT bodies.

A view hides what the query actually touches. ``SELECT * FROM v_orders`` says
nothing about whether the filter can be pushed down, how large the base table
is, or whether the view already contains a DISTINCT that blocks pushdown. Since
the plan rows in SYS_QUERY_EXPLAIN describe base tables, inlining the view body
is also what makes plan evidence line up with the SQL.

Nested views are expanded repeatedly up to ``max_depth``; the depth cap is what
prevents a cycle (``v_a`` selecting from ``v_b`` selecting from ``v_a``) from
looping forever.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from .catalog import Catalog, normalize_ident


def explode_views(
    tree: exp.Expression,
    catalog: Catalog,
    *,
    max_depth: int = 8,
    dialect: str = "redshift",
) -> tuple[exp.Expression, list[str]]:
    """Inline every catalog-known view as an aliased subquery.

    Returns the rewritten tree and the list of view keys that were inlined.
    A view whose body does not parse is left in place rather than guessed at.
    """
    working = tree.copy()
    exploded: list[str] = []

    for _ in range(max_depth):
        targets: list[tuple[exp.Table, exp.Expression, str]] = []
        for table in list(working.find_all(exp.Table)):
            identity = _identity(table)
            if not identity:
                continue
            resolved = catalog.resolve_view(identity)
            if resolved is None:
                continue
            view_key, view_def = resolved
            body_sql = str(view_def.sql or "").strip()
            if not body_sql:
                continue
            try:
                body = sqlglot.parse_one(body_sql, read=dialect)
            except Exception:
                continue  # unparseable view body: leave the reference alone
            if body is None:
                continue
            alias = normalize_ident(table.alias or table.name) or view_key.split(".")[-1]
            subquery = exp.Subquery(
                this=body.this.copy() if isinstance(body, exp.Subquery) else body.copy(),
                alias=exp.TableAlias(this=exp.to_identifier(alias)),
            )
            targets.append((table, subquery, view_key))

        if not targets:
            break

        replacements = {
            (_identity(table), normalize_ident(table.alias or table.name)): (subquery, key)
            for table, subquery, key in targets
        }

        def _swap(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Table):
                return node
            hit = replacements.get(
                (_identity(node), normalize_ident(node.alias or node.name))
            )
            if hit is None:
                return node
            subquery, key = hit
            exploded.append(key)
            return subquery.copy()

        working = working.transform(_swap)

    # Preserve first-seen order while removing duplicates.
    return working, list(dict.fromkeys(exploded))


def _identity(table: exp.Table) -> str:
    parts = [p for p in (table.catalog, table.db, table.name) if p]
    return ".".join(normalize_ident(p) for p in parts)
