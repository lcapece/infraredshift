"""Lightweight AST analysis for decomposition planning."""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlglot import exp

from .catalog import Catalog, normalize_ident
from .identity import table_alias, table_identity


@dataclass
class TableRef:
    identity: str
    catalog_key: str
    alias: str
    is_cte: bool = False
    is_view: bool = False


@dataclass
class JoinEdge:
    left_alias: str
    right_alias: str
    left_column: str
    right_column: str
    side: str  # INNER | LEFT | RIGHT | FULL | CROSS | ""
    kind: str


@dataclass
class Predicate:
    sql: str
    aliases: set[str]
    columns: list[tuple[str, str]]  # (alias, column)
    clause: str  # WHERE | JOIN | HAVING
    is_simple_pushable: bool


@dataclass
class QueryAnalysis:
    tree: exp.Expression
    table_refs: list[TableRef] = field(default_factory=list)
    joins: list[JoinEdge] = field(default_factory=list)
    predicates: list[Predicate] = field(default_factory=list)
    cte_names: set[str] = field(default_factory=set)
    has_star: bool = False
    column_usage: dict[str, set[str]] = field(default_factory=dict)  # catalog_key -> columns
    # alias (including subquery aliases) -> set of physical catalog keys
    alias_lineage: dict[str, set[str]] = field(default_factory=dict)


def analyze_query(tree: exp.Expression, catalog: Catalog) -> QueryAnalysis:
    cte_names = {normalize_ident(cte.alias_or_name) for cte in tree.find_all(exp.CTE)}
    refs: list[TableRef] = []
    for table in tree.find_all(exp.Table):
        identity = table_identity(table)
        alias = table_alias(table)
        name = normalize_ident(table.name)
        if name in cte_names and not table.db and not table.catalog:
            refs.append(TableRef(identity=name, catalog_key=name, alias=alias, is_cte=True))
            continue
        resolved = catalog.resolve_any(identity)
        if resolved is None:
            refs.append(TableRef(identity=identity, catalog_key=identity, alias=alias))
            continue
        key, kind, _obj = resolved
        refs.append(
            TableRef(
                identity=identity,
                catalog_key=key,
                alias=alias,
                is_view=(kind == "view"),
            )
        )

    alias_lineage = _build_alias_lineage(tree, catalog, cte_names)
    joins = _collect_joins(tree)
    predicates = _collect_predicates(tree)
    has_star = _has_star(tree)
    column_usage = _column_usage(tree, refs, alias_lineage, catalog)

    return QueryAnalysis(
        tree=tree,
        table_refs=refs,
        joins=joins,
        predicates=predicates,
        cte_names=cte_names,
        has_star=has_star,
        column_usage=column_usage,
        alias_lineage=alias_lineage,
    )


def _build_alias_lineage(
    tree: exp.Expression,
    catalog: Catalog,
    cte_names: set[str],
) -> dict[str, set[str]]:
    """Map every table/subquery alias to underlying physical catalog keys."""
    lineage: dict[str, set[str]] = {}

    for table in tree.find_all(exp.Table):
        alias = table_alias(table)
        name = normalize_ident(table.name)
        if name in cte_names and not table.db and not table.catalog:
            lineage.setdefault(alias, set())
            continue
        resolved = catalog.resolve_table(table_identity(table))
        if resolved is not None:
            lineage.setdefault(alias, set()).add(resolved[0])

    # Subqueries produced by view explosion: alias -> physical tables inside
    for subq in tree.find_all(exp.Subquery):
        alias = normalize_ident(subq.alias_or_name)
        if not alias:
            continue
        physical: set[str] = set()
        for table in subq.find_all(exp.Table):
            name = normalize_ident(table.name)
            if name in cte_names and not table.db and not table.catalog:
                continue
            resolved = catalog.resolve_table(table_identity(table))
            if resolved is not None:
                physical.add(resolved[0])
        if physical:
            lineage.setdefault(alias, set()).update(physical)

    return lineage


def _collect_joins(tree: exp.Expression) -> list[JoinEdge]:
    edges: list[JoinEdge] = []
    for join in tree.find_all(exp.Join):
        side = str(join.args.get("side") or "").upper()
        kind = str(join.args.get("kind") or "").upper()
        on_expr = join.args.get("on")
        if on_expr is None:
            edges.append(JoinEdge("", "", "", "", side or kind or "CROSS", kind))
            continue
        for equality in on_expr.find_all(exp.EQ):
            left, right = equality.this, equality.expression
            if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
                continue
            edges.append(
                JoinEdge(
                    left_alias=normalize_ident(left.table),
                    right_alias=normalize_ident(right.table),
                    left_column=normalize_ident(left.name),
                    right_column=normalize_ident(right.name),
                    side=side or "INNER",
                    kind=kind,
                )
            )
    return edges


def _collect_predicates(tree: exp.Expression) -> list[Predicate]:
    preds: list[Predicate] = []
    for where in tree.find_all(exp.Where):
        for term in _and_terms(where.this):
            preds.append(_predicate_from_expr(term, "WHERE"))
    for join in tree.find_all(exp.Join):
        on_expr = join.args.get("on")
        if on_expr is None:
            continue
        for term in _and_terms(on_expr):
            preds.append(_predicate_from_expr(term, "JOIN"))
    return preds


def _predicate_from_expr(expr: exp.Expression, clause: str) -> Predicate:
    aliases: set[str] = set()
    columns: list[tuple[str, str]] = []
    for col in expr.find_all(exp.Column):
        alias = normalize_ident(col.table)
        name = normalize_ident(col.name)
        if alias:
            aliases.add(alias)
        if alias and name:
            columns.append((alias, name))
    sql = expr.sql(dialect="redshift")
    pushable = _is_simple_pushable(expr)
    return Predicate(
        sql=sql,
        aliases=aliases,
        columns=columns,
        clause=clause,
        is_simple_pushable=pushable,
    )


def _is_simple_pushable(expr: exp.Expression) -> bool:
    """Conservative: simple column comparisons without OR/subquery/window."""
    if expr.find(exp.Or) or expr.find(exp.Subquery) or expr.find(exp.Exists) or expr.find(exp.Window):
        return False
    # Disallow non-deterministic / session-time functions in pushed predicates.
    # A stage built at T1 with getdate()/current_date can under-include rows
    # relative to final evaluation at T2.
    _volatile = {
        "random",
        "getdate",
        "sysdate",
        "current_timestamp",
        "current_date",
        "current_time",
        "now",
        "timeofday",
    }
    for func in expr.find_all(exp.Func):
        name = normalize_ident(func.sql_name() if hasattr(func, "sql_name") else func.name)
        if name in _volatile:
            return False
    cols = list(expr.find_all(exp.Column))
    if not cols:
        return False
    # single-table predicate only
    tables = {normalize_ident(c.table) for c in cols if normalize_ident(c.table)}
    if len(tables) != 1:
        return False
    return isinstance(
        expr,
        (
            exp.EQ,
            exp.NEQ,
            exp.GT,
            exp.GTE,
            exp.LT,
            exp.LTE,
            exp.Like,
            exp.ILike,
            exp.In,
            exp.Between,
            exp.Is,
            exp.And,
        ),
    ) or (isinstance(expr, exp.Not) and _is_simple_pushable(expr.this))


def _and_terms(expr: exp.Expression | None) -> list[exp.Expression]:
    if expr is None:
        return []
    if isinstance(expr, exp.And):
        return _and_terms(expr.this) + _and_terms(expr.expression)
    return [expr]


def _has_star(tree: exp.Expression) -> bool:
    for select in tree.find_all(exp.Select):
        for projection in select.expressions:
            node = projection.this if isinstance(projection, exp.Alias) else projection
            if isinstance(node, exp.Star):
                return True
            if isinstance(node, exp.Column) and bool(getattr(node, "is_star", False)):
                return True
    return False


def _column_usage(
    tree: exp.Expression,
    refs: list[TableRef],
    alias_lineage: dict[str, set[str]],
    catalog: Catalog,
) -> dict[str, set[str]]:
    usage: dict[str, set[str]] = {}

    def add(key: str, name: str) -> None:
        if key and name and name != "*":
            usage.setdefault(key, set()).add(name)

    physical_by_alias: dict[str, str] = {
        r.alias: r.catalog_key
        for r in refs
        if r.alias and r.catalog_key and not r.is_cte and not r.is_view
    }

    def local_physical_keys(select: exp.Select | None) -> set[str]:
        """Physical tables visible from *select*'s own FROM/JOIN sources.

        An unqualified column belongs to its enclosing scope, not to the whole
        query: a filter-only column inside an exploded view body (WHERE status
        <> '...') must attribute to that view's base table even when the outer
        query joins other tables, or pruning drops a column the script needs.
        """
        if select is None:
            return set()
        sources: list[exp.Expression] = []
        from_ = select.args.get("from")
        if from_ is not None:
            sources.append(from_.this)
        for join in select.args.get("joins") or []:
            sources.append(join.this)
        keys: set[str] = set()
        for source in sources:
            alias = normalize_ident(source.alias_or_name)
            if isinstance(source, exp.Table) and alias in physical_by_alias:
                keys.add(physical_by_alias[alias])
            else:
                keys |= alias_lineage.get(alias, set())
        return keys

    for col in tree.find_all(exp.Column):
        alias = normalize_ident(col.table)
        name = normalize_ident(col.name)
        if not name or name == "*":
            continue
        keys = alias_lineage.get(alias, set()) if alias else set()
        if not keys and not alias:
            local = local_physical_keys(col.find_ancestor(exp.Select))
            if len(local) == 1:
                keys = local
            else:
                physical = {r.catalog_key for r in refs if not r.is_cte and not r.is_view}
                if len(physical) == 1:
                    keys = physical
        for key in keys:
            # Only count columns that exist on the table when schema is known
            stats = catalog.tables.get(key)
            if stats and stats.column_names() and name not in stats.column_names():
                continue
            add(key, name)

    # Direct physical table references also need columns used inside their local scope
    for ref in refs:
        if ref.is_cte or ref.is_view:
            continue
        for col in tree.find_all(exp.Column):
            if normalize_ident(col.table) == ref.alias:
                add(ref.catalog_key, normalize_ident(col.name))

    return usage
