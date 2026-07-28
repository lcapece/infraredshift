"""Discover every table/view reference in SQL (no catalog required).

Success of Redshift decomposition depends on this step: if a referenced
relation is missed here, its attributes and (for views) its body never enter
the catalog, and stage design is incomplete.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import exp, parse, parse_one

from .catalog import normalize_ident, normalize_key


@dataclass(frozen=True)
class ObjectRef:
    """A table or view name mentioned in SQL."""

    identity: str  # as written: db.schema.table | schema.table | table
    database: str
    schema: str
    name: str
    alias: str
    is_cte: bool = False
    source: str = "sql"  # sql | view_body

    @property
    def fetch_key(self) -> str:
        """Stable key for de-duplicating fetch work (schema-aware)."""
        return normalize_key(self.schema, self.name) if self.schema else self.name


def discover_object_refs(
    sql: str | exp.Expression,
    *,
    source: str = "sql",
    include_ctes_as_refs: bool = False,
) -> list[ObjectRef]:
    """Return all relation references (CTEs excluded by default).

    Handles joins, comma-from, subqueries, UNION branches, semi/anti join
    subqueries, and multi-statement scripts (uses sqlglot.parse).
    """
    trees = _as_trees(sql)
    refs: list[ObjectRef] = []
    seen: set[tuple[str, str, str]] = set()  # identity, alias, source

    for tree in trees:
        cte_names = {normalize_ident(cte.alias_or_name) for cte in tree.find_all(exp.CTE)}
        for table in tree.find_all(exp.Table):
            name = normalize_ident(table.name)
            schema = normalize_ident(table.db)
            database = normalize_ident(table.catalog)
            alias = normalize_ident(table.alias_or_name or table.name)
            is_cte = bool(name in cte_names and not schema and not database)
            if is_cte and not include_ctes_as_refs:
                continue
            identity = normalize_key(database, schema, name)
            if not name:
                continue
            key = (identity, alias, source)
            if key in seen:
                continue
            seen.add(key)
            refs.append(
                ObjectRef(
                    identity=identity,
                    database=database,
                    schema=schema,
                    name=name,
                    alias=alias,
                    is_cte=is_cte,
                    source=source,
                )
            )
    return refs


def unique_fetch_targets(refs: list[ObjectRef]) -> list[ObjectRef]:
    """One representative ref per schema.name (prefers longer qualification)."""
    best: dict[str, ObjectRef] = {}
    for ref in refs:
        if ref.is_cte or not ref.name:
            continue
        key = ref.fetch_key
        prev = best.get(key)
        if prev is None or len(ref.identity) > len(prev.identity):
            best[key] = ref
    return list(best.values())


def refs_need_schemas(refs: list[ObjectRef]) -> set[str]:
    return {r.schema for r in refs if r.schema}


def refs_need_names(refs: list[ObjectRef]) -> set[str]:
    return {r.name for r in refs if r.name}


def _as_trees(sql: str | exp.Expression) -> list[exp.Expression]:
    if isinstance(sql, exp.Expression):
        return [sql]
    text = str(sql or "").strip()
    if not text:
        return []
    try:
        trees = parse(text, read="redshift")
        return [t for t in trees if t is not None]
    except Exception:
        # Fall back to single-statement parse for slightly broken scripts
        return [parse_one(text, read="redshift")]
