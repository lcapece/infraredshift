"""Catalog: table attributes and view definitions supplied by the caller."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Mapping


def normalize_ident(value: object) -> str:
    text = str(value or "").strip().strip('"')
    return text.lower()


def normalize_key(*parts: object) -> str:
    return ".".join(normalize_ident(part) for part in parts if normalize_ident(part))


@dataclass(frozen=True)
class TableStats:
    """Physical and logical metadata for one Redshift table (or external table)."""

    columns: Mapping[str, str] = field(default_factory=dict)
    diststyle: str = ""
    distkey: str = ""
    sortkeys: tuple[str, ...] = ()
    rows: float = 0.0
    size_mb: float = 0.0
    is_external: bool = False
    object_type: str = "table"  # table | external_table
    # Spectrum / external: first partition-key column (part_key = 1). Not a full
    # external column list — see svv_external_columns harvest.
    partition_key: str = ""

    def column_names(self) -> tuple[str, ...]:
        return tuple(normalize_ident(name) for name in self.columns.keys())

    def sqlglot_schema(self) -> dict[str, str]:
        return {normalize_ident(name): str(dtype) for name, dtype in self.columns.items()}


@dataclass(frozen=True)
class ViewDef:
    """SQL body for a view (SELECT only; no WITH NO SCHEMA BINDING semantics enforced)."""

    sql: str
    columns: Mapping[str, str] = field(default_factory=dict)
    is_late_binding: bool = False


@dataclass
class Catalog:
    """In-memory catalog assumed complete for all objects referenced by the query.

    Keys may be ``database.schema.table``, ``schema.table``, or bare ``table``.
    Lookups try longer qualifiers first, then fall back to suffix matches when unique.

    ``coverage`` is filled by :func:`fetch_catalog_for_sql` with discovery results
    (resolved tables/views vs missing names).
    """

    tables: dict[str, TableStats] = field(default_factory=dict)
    views: dict[str, ViewDef] = field(default_factory=dict)
    default_database: str = ""
    default_schema: str = "public"
    coverage: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.tables = {normalize_ident(key): stats for key, stats in self.tables.items()}
        self.views = {normalize_ident(key): view for key, view in self.views.items()}
        self.default_database = normalize_ident(self.default_database)
        self.default_schema = normalize_ident(self.default_schema) or "public"
        if self.coverage is None:
            self.coverage = {}

    def is_view(self, identity: str) -> bool:
        return self.resolve_view(identity) is not None

    def is_table(self, identity: str) -> bool:
        return self.resolve_table(identity) is not None

    def resolve_table(self, identity: str) -> tuple[str, TableStats] | None:
        return self._resolve(identity, self.tables)

    def resolve_view(self, identity: str) -> tuple[str, ViewDef] | None:
        return self._resolve(identity, self.views)

    def resolve_any(self, identity: str) -> tuple[str, str, TableStats | ViewDef] | None:
        table = self.resolve_table(identity)
        if table is not None:
            return table[0], "table", table[1]
        view = self.resolve_view(identity)
        if view is not None:
            return view[0], "view", view[1]
        return None

    def sqlglot_schema_mapping(self) -> dict[str, dict[str, str]]:
        """Nested mapping suitable for sqlglot.optimizer.qualify / optimize.

        Uses the longest stored key parts. Also registers short suffixes so
        unqualified table references can resolve when unique.
        """
        schema: dict[str, dict[str, str]] = {}
        for key, stats in self.tables.items():
            cols = stats.sqlglot_schema()
            if not cols:
                continue
            parts = key.split(".")
            # full key as dotted path handled by MappingSchema with nested dicts
            self._set_nested(schema, parts, cols)
            if len(parts) >= 2:
                self._set_nested(schema, parts[-2:], cols)
            self._set_nested(schema, parts[-1:], cols)
        for key, view in self.views.items():
            if not view.columns:
                continue
            cols = {normalize_ident(n): str(t) for n, t in view.columns.items()}
            parts = key.split(".")
            self._set_nested(schema, parts, cols)
            if len(parts) >= 2:
                self._set_nested(schema, parts[-2:], cols)
            self._set_nested(schema, parts[-1:], cols)
        return schema

    def all_table_keys(self) -> Iterable[str]:
        return self.tables.keys()

    def all_view_keys(self) -> Iterable[str]:
        return self.views.keys()

    def _resolve(self, identity: str, store: dict) -> tuple[str, object] | None:
        key = normalize_ident(identity)
        if not key:
            return None
        if key in store:
            return key, store[key]
        # unique suffix match (schema.table or table)
        matches = [stored for stored in store if stored == key or stored.endswith("." + key)]
        if len(matches) == 1:
            return matches[0], store[matches[0]]
        # unique bare-name match
        bare = key.split(".")[-1]
        bare_matches = [stored for stored in store if stored.split(".")[-1] == bare]
        if len(bare_matches) == 1:
            return bare_matches[0], store[bare_matches[0]]
        return None

    @staticmethod
    def _set_nested(root: dict, parts: list[str], leaf: dict[str, str]) -> None:
        node = root
        for part in parts[:-1]:
            node = node.setdefault(part, {})
            if not isinstance(node, dict):
                return
        # Only set if not already a nested dict of further tables
        existing = node.get(parts[-1])
        if existing is None or (isinstance(existing, dict) and all(isinstance(v, str) for v in existing.values())):
            node[parts[-1]] = dict(leaf)
