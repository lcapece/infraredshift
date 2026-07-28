"""Classify repeat-query groups by whether they touch external tables.

A query is "mixed" when its parsed table set contains BOTH a known external
table (from the isolated SVV_EXTERNAL_TABLES catalog) AND a regular Redshift
table. This is a catalog fact — more reliable than the runtime S3-scan
heuristic — so it lets the quadrant flag mixed queries deterministically.
"""
from __future__ import annotations

import re

import pandas as pd

# Classification values written to the group column `mixed_query_class`.
CLASS_MIXED = "mixed"
CLASS_EXTERNAL_ONLY = "external-only"
CLASS_LOCAL_ONLY = "local-only"
CLASS_UNKNOWN = ""  # no parsed tables / no catalog to compare against

_SPLIT = re.compile(r"[,\s]+")


def external_name_index(catalog: pd.DataFrame) -> set[str]:
    """Return the set of external-table identifiers, matched flexibly.

    Includes both the fully-qualified `schema.table` and the bare `table`,
    lowercased, so a parsed table reference matches whether or not the query
    qualified it with the external schema.
    """
    names: set[str] = set()
    if catalog is None or catalog.empty:
        return names
    cols = {c.lower(): c for c in catalog.columns}
    schema_col = cols.get("schema_name")
    table_col = cols.get("table_name")
    if not table_col:
        return names
    for _, row in catalog.iterrows():
        table = str(row.get(table_col) or "").strip().lower()
        if not table:
            continue
        names.add(table)
        if schema_col:
            schema = str(row.get(schema_col) or "").strip().lower()
            if schema:
                names.add(f"{schema}.{table}")
    return names


def _tables_in(value: object) -> list[str]:
    text = str(value or "").strip().lower()
    if not text:
        return []
    return [t for t in (part.strip(" \t\"'`") for part in _SPLIT.split(text)) if t]


def classify_table_set(tables: list[str], external: set[str]) -> str:
    """Classify one query's table list against the external-name index."""
    if not tables:
        return CLASS_UNKNOWN
    has_external = False
    has_local = False
    for table in tables:
        bare = table.rsplit(".", 1)[-1]
        if table in external or bare in external:
            has_external = True
        else:
            has_local = True
    if has_external and has_local:
        return CLASS_MIXED
    if has_external:
        return CLASS_EXTERNAL_ONLY
    return CLASS_LOCAL_ONLY


def view_name_index(view_definitions: pd.DataFrame) -> set[str]:
    """Return the set of view identifiers for fast in-memory membership tests.

    Includes both `schema.view` and the bare `view`, lowercased. Built once
    from the loaded view_definitions; even ~6,000 views is a trivial set and
    membership is O(1), so flagging view usage needs no SQL re-parsing.
    """
    names: set[str] = set()
    if view_definitions is None or view_definitions.empty:
        return names
    cols = {c.lower(): c for c in view_definitions.columns}
    name_col = cols.get("view_name") or cols.get("table_name")
    schema_col = cols.get("schema_name") or cols.get("schema") or cols.get("schemaname")
    db_col = cols.get("database") or cols.get("database_name") or cols.get("source_db")
    if not name_col:
        return names
    for _, row in view_definitions.iterrows():
        view = str(row.get(name_col) or "").strip().lower()
        if not view:
            continue
        names.add(view)
        schema = str(row.get(schema_col) or "").strip().lower() if schema_col else ""
        if schema:
            names.add(f"{schema}.{view}")
            if db_col:
                database = str(row.get(db_col) or "").strip().lower()
                if database:
                    names.add(f"{database}.{schema}.{view}")
    return names


def annotate_view_usage(groups: pd.DataFrame, view_definitions: pd.DataFrame) -> pd.DataFrame:
    """Add a boolean `uses_view` column to repeat groups.

    A group uses a view when any table in its parsed table set matches a known
    view name. Uses the already-parsed `sql_tables_full` — no SQL re-parsing.
    """
    if groups is None or groups.empty:
        return groups
    result = groups.copy()
    views = view_name_index(view_definitions)
    source_col = "sql_tables_full" if "sql_tables_full" in result.columns else (
        "sql_tables" if "sql_tables" in result.columns else None
    )
    if source_col is None or not views:
        result["uses_view"] = False
        return result

    def _row_uses_view(value: object) -> bool:
        for table in _tables_in(value):
            if table in views or table.rsplit(".", 1)[-1] in views:
                return True
        return False

    result["uses_view"] = [bool(_row_uses_view(v)) for v in result[source_col]]
    return result


def annotate_mixed_queries(groups: pd.DataFrame, catalog: pd.DataFrame) -> pd.DataFrame:
    """Add a `mixed_query_class` column to repeat groups.

    Uses `sql_tables_full` (the group's complete parsed table set). When the
    external catalog is empty, every group is left UNKNOWN so nothing is
    falsely flagged mixed.
    """
    if groups is None or groups.empty:
        return groups
    result = groups.copy()
    external = external_name_index(catalog)
    source_col = "sql_tables_full" if "sql_tables_full" in result.columns else (
        "sql_tables" if "sql_tables" in result.columns else None
    )
    if source_col is None or not external:
        result["mixed_query_class"] = CLASS_UNKNOWN
        return result
    result["mixed_query_class"] = [
        classify_table_set(_tables_in(value), external)
        for value in result[source_col]
    ]
    return result
