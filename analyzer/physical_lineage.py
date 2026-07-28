"""Column-level SQL lineage that terminates at physical Redshift tables.

The rest of the application must not treat aliases, CTEs, derived relations,
or views as physical tables.  This module follows projected columns through
those scopes and recursively through captured view definitions.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd
import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import Scope, build_scope


@dataclass(frozen=True)
class PhysicalOrigin:
    database: str
    schema: str
    table: str
    column: str
    path: tuple[str, ...] = ()

    @property
    def table_key(self) -> str:
        return ".".join(part for part in (self.database, self.schema, self.table) if part)

    @property
    def column_key(self) -> str:
        return ".".join(part for part in (self.table_key, self.column) if part)

    def display(self) -> str:
        return self.column_key or self.column


class _ViewDefinitions:
    def __init__(self, rows: object = None):
        self._exact: dict[str, dict] = {}
        self._suffix: dict[str, list[dict]] = {}
        if isinstance(rows, pd.DataFrame):
            records = rows.to_dict("records") if not rows.empty else []
        elif isinstance(rows, dict):
            records = [
                {"view_name": key, "source_definition": value}
                for key, value in rows.items()
            ]
        else:
            records = list(rows or [])
        seen: set[tuple[str, str]] = set()
        for raw in records:
            row = dict(raw or {})
            database = _clean(row.get("database") or row.get("database_name") or row.get("source_db"))
            schema = _clean(row.get("schema") or row.get("schema_name"))
            name = _clean(row.get("view_name") or row.get("table_name"))
            definition = str(
                row.get("source_definition")
                or row.get("view_definition")
                or row.get("definition")
                or ""
            ).strip()
            if not name or not definition:
                continue
            identity = ".".join(part for part in (database, schema, name) if part)
            marker = (identity, definition)
            if marker in seen:
                continue
            seen.add(marker)
            meta = {
                "database": database,
                "schema": schema,
                "name": name,
                "identity": identity,
                "definition": definition,
            }
            if identity:
                self._exact[identity] = meta
            for key in {name, ".".join(part for part in (schema, name) if part)}:
                if key:
                    self._suffix.setdefault(key, []).append(meta)

    def match(self, database: str, schema: str, name: str) -> dict | None:
        database, schema, name = _clean(database), _clean(schema), _clean(name)
        exact = ".".join(part for part in (database, schema, name) if part)
        if exact in self._exact:
            return self._exact[exact]
        for key in (".".join(part for part in (schema, name) if part), name):
            matches = self._suffix.get(key, [])
            if len(matches) == 1:
                return matches[0]
        return None


class PhysicalLineageResolver:
    """Resolve SQLGlot expressions to one or more physical table columns."""

    def __init__(
        self,
        sql_or_tree: str | exp.Expression,
        view_definitions: object = None,
        *,
        default_database: str = "",
        default_schema: str = "",
        _view_index: _ViewDefinitions | None = None,
        _lineage_path: tuple[str, ...] = (),
        _depth: int = 0,
    ):
        tree = (
            sqlglot.parse_one(sql_or_tree, read="redshift")
            if isinstance(sql_or_tree, str)
            else sql_or_tree
        )
        self.tree = _query_expression(tree)
        self.root = build_scope(self.tree)
        self.default_database = _clean(default_database)
        self.default_schema = _clean(default_schema)
        self._views = _view_index or _ViewDefinitions(view_definitions)
        self._lineage_path = tuple(_lineage_path)
        self._depth = int(_depth)
        self._column_scopes: dict[int, Scope] = {}
        self._expression_scopes: list[Scope] = []
        if self.root is not None:
            for scope in self.root.traverse():
                self._expression_scopes.append(scope)
                for column in scope.columns:
                    self._column_scopes[id(column)] = scope

    def origins_for_expression(self, expression: exp.Expression | None) -> list[PhysicalOrigin]:
        if expression is None or self.root is None:
            return []
        origins: list[PhysicalOrigin] = []
        for column in expression.find_all(exp.Column):
            scope = self._column_scopes.get(id(column))
            if scope is None:
                continue
            origins.extend(self._resolve_column(column, scope, set()))
        return _dedupe_origins(origins)

    def origins_for_column(self, column: exp.Column) -> list[PhysicalOrigin]:
        scope = self._column_scopes.get(id(column))
        return self._resolve_column(column, scope, set()) if scope is not None else []

    def output_origins(self, column_name: str) -> list[PhysicalOrigin]:
        if self.root is None:
            return []
        return self._resolve_scope_output(self.root, _clean(column_name), set())

    def _resolve_column(
        self,
        column: exp.Column,
        scope: Scope,
        seen: set[tuple[int, str]],
    ) -> list[PhysicalOrigin]:
        name = _clean(column.name)
        qualifier = _clean(column.table)
        if not name:
            return []
        candidates: list[tuple[str, object]] = []
        if qualifier:
            source = scope.sources.get(qualifier)
            if source is not None:
                candidates.append((qualifier, source))
        else:
            selected = list(scope.selected_sources.items())
            if len(selected) == 1:
                alias, (_node, source) = selected[0]
                candidates.append((_clean(alias), source))
            else:
                for alias, (_node, source) in selected:
                    if isinstance(source, Scope) and self._scope_projects(source, name):
                        candidates.append((_clean(alias), source))
        origins: list[PhysicalOrigin] = []
        for alias, source in candidates:
            marker = (id(source), name)
            if marker in seen:
                continue
            next_seen = set(seen)
            next_seen.add(marker)
            if isinstance(source, Scope):
                origins.extend(self._resolve_scope_output(source, name, next_seen))
            elif isinstance(source, exp.Table):
                origins.extend(self._resolve_table_column(source, name, alias))
        return _dedupe_origins(origins)

    def _resolve_scope_output(
        self,
        scope: Scope,
        column_name: str,
        seen: set[tuple[int, str]],
    ) -> list[PhysicalOrigin]:
        expression = scope.expression
        selects = list(getattr(expression, "selects", []) or [])
        for projection in selects:
            alias = _clean(projection.alias_or_name)
            if alias != column_name:
                continue
            target = projection.this if isinstance(projection, exp.Alias) else projection
            origins: list[PhysicalOrigin] = []
            for column in target.find_all(exp.Column):
                column_scope = self._column_scopes.get(id(column))
                if column_scope is scope:
                    origins.extend(self._resolve_column(column, scope, seen))
            return _dedupe_origins(origins)
        # SELECT * or an unqualified passthrough column.
        has_star = any(isinstance(projection, exp.Star) for projection in selects)
        if has_star or len(scope.selected_sources) == 1:
            synthetic = exp.column(column_name)
            selected = list(scope.selected_sources.items())
            if len(selected) == 1:
                alias = _clean(selected[0][0])
                synthetic.set("table", exp.to_identifier(alias))
                return self._resolve_column(synthetic, scope, seen)
        return []

    def _scope_projects(self, scope: Scope, column_name: str) -> bool:
        selects = list(getattr(scope.expression, "selects", []) or [])
        return any(
            _clean(projection.alias_or_name) == column_name or isinstance(projection, exp.Star)
            for projection in selects
        )

    def _resolve_table_column(
        self,
        table: exp.Table,
        column_name: str,
        alias: str,
    ) -> list[PhysicalOrigin]:
        database = _clean(table.catalog) or self.default_database
        schema = _clean(table.db) or self.default_schema
        name = _clean(table.name)
        view = self._views.match(database, schema, name)
        path_label = ".".join(part for part in (database, schema, name) if part) or alias or name
        if view is not None:
            identity = str(view.get("identity") or path_label)
            if self._depth >= 12 or identity in self._lineage_path:
                return []
            try:
                nested = PhysicalLineageResolver(
                    str(view["definition"]),
                    default_database=str(view.get("database") or database),
                    default_schema=str(view.get("schema") or schema),
                    _view_index=self._views,
                    _lineage_path=self._lineage_path + (identity,),
                    _depth=self._depth + 1,
                )
                origins = nested.output_origins(column_name)
            except Exception:
                origins = []
            return [
                PhysicalOrigin(
                    origin.database,
                    origin.schema,
                    origin.table,
                    origin.column,
                    self._lineage_path + (identity,) + origin.path,
                )
                for origin in origins
            ]
        return [
            PhysicalOrigin(
                database=database,
                schema=schema,
                table=name,
                column=column_name,
                path=self._lineage_path + ((path_label,) if path_label else ()),
            )
        ] if name else []


def expression_physical_origins(
    sql: str,
    expression_sql: str,
    view_definitions: object = None,
) -> list[PhysicalOrigin]:
    """Resolve a standalone operand by matching it inside a parsed statement."""
    resolver = PhysicalLineageResolver(sql, view_definitions)
    wanted = _compact_sql(expression_sql)
    for node in resolver.tree.walk():
        if isinstance(node, exp.Expression) and _compact_sql(node.sql(dialect="redshift")) == wanted:
            origins = resolver.origins_for_expression(node)
            if origins:
                return origins
    return []


def format_origins(origins: Iterable[PhysicalOrigin]) -> str:
    return ", ".join(origin.display() for origin in _dedupe_origins(list(origins)))


def _query_expression(tree: exp.Expression) -> exp.Expression:
    if isinstance(tree, exp.Create) and isinstance(tree.expression, exp.Expression):
        return tree.expression
    return tree


def _dedupe_origins(origins: list[PhysicalOrigin]) -> list[PhysicalOrigin]:
    seen: set[tuple[str, str, str, str]] = set()
    output: list[PhysicalOrigin] = []
    for origin in origins:
        key = (origin.database, origin.schema, origin.table, origin.column)
        if key in seen:
            continue
        seen.add(key)
        output.append(origin)
    return output


def _clean(value: object) -> str:
    return str(value or "").strip().strip('"').lower()


def _compact_sql(value: object) -> str:
    return " ".join(str(value or "").strip().lower().split())
