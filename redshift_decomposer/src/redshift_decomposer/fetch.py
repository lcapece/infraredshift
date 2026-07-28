"""Fetch Redshift table attributes + view definitions into a Catalog.

The library never ships a driver. Pass any callable that can run SQL and return
row mappings (or use a DB-API connection).

Typical Redshift sources (current-database scoped unless noted):

* ``SVV_TABLE_INFO`` — rows, size_mb, diststyle, sortkey1, table_id
* ``information_schema.columns`` — column names + types
* ``pg_views`` — view SQL text for explosion
* ``SVV_EXTERNAL_COLUMNS`` — first partition key only (not full column lists)
* ``pg_table_def`` — compound sort keys / distkey / types (repository path)

**metadata_schema:** when a non-empty schema name is supplied, every catalog
query is retargeted to that schema, using the **same bare table names**
(``svv_table_info``, ``pg_table_def``, ``pg_views``, ``columns``,
``svv_external_columns``). Mirrors need only the columns this package SELECTs;
extra columns are fine and ignored.

Multi-database clusters: call :func:`fetch_catalog_for_sql` once per database
(or pass a connection already ``USE``'d / connected to that database) and merge
with :meth:`Catalog` dict updates.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, Protocol

from sqlglot import exp, parse_one

from .catalog import Catalog, TableStats, ViewDef, normalize_ident, normalize_key
from .discover import ObjectRef, discover_object_refs, unique_fetch_targets

# Bare table names used when metadata_schema is set (lowercase Redshift form).
CATALOG_TABLE_SVV_TABLE_INFO = "svv_table_info"
CATALOG_TABLE_PG_TABLE_DEF = "pg_table_def"
CATALOG_TABLE_PG_VIEWS = "pg_views"
CATALOG_TABLE_COLUMNS = "columns"  # mirrors information_schema.columns
CATALOG_TABLE_SVV_EXTERNAL_COLUMNS = "svv_external_columns"


# ---------------------------------------------------------------------------
# Connection adapters
# ---------------------------------------------------------------------------


class SupportsExecute(Protocol):
    """Minimal DB-API / redshift_connector / psycopg2 shape."""

    def cursor(self) -> Any: ...


RowMapping = Mapping[str, Any]
ExecuteFn = Callable[[str], Sequence[RowMapping]]


def execute_dbapi(connection: SupportsExecute) -> ExecuteFn:
    """Wrap a DB-API connection as ``execute(sql) -> list[dict]``."""

    def _execute(sql: str) -> list[dict[str, Any]]:
        cur = connection.cursor()
        try:
            cur.execute(sql)
            if cur.description is None:
                return []
            columns = [normalize_ident(col[0]) for col in cur.description]
            rows = cur.fetchall()
            out: list[dict[str, Any]] = []
            for row in rows:
                if isinstance(row, Mapping):
                    out.append({normalize_ident(k): v for k, v in row.items()})
                else:
                    out.append(dict(zip(columns, row)))
            return out
        finally:
            try:
                cur.close()
            except Exception:
                pass

    return _execute


# ---------------------------------------------------------------------------
# Public fetch API
# ---------------------------------------------------------------------------


def normalize_metadata_schema(metadata_schema: str | None) -> str | None:
    """Return a normalized schema name, or None when unset/blank (system catalogs)."""
    text = normalize_ident(metadata_schema or "")
    return text or None


def catalog_relation(bare_table: str, metadata_schema: str | None = None) -> str:
    """SQL FROM target for a catalog relation.

    *bare_table* is the canonical name (``svv_table_info``, ``pg_table_def``,
    ``pg_views``, ``columns``, ``svv_external_columns``).

    When *metadata_schema* is None, returns the usual system relation
    (``SVV_TABLE_INFO``, ``information_schema.columns``, …). When set, returns
    ``{metadata_schema}.{bare_table}`` so non-superusers can point at mirrors
    with the same bare names and expected columns.
    """
    bare = normalize_ident(bare_table)
    schema = normalize_metadata_schema(metadata_schema)
    if schema:
        return f"{schema}.{bare}"
    system = {
        CATALOG_TABLE_SVV_TABLE_INFO: "SVV_TABLE_INFO",
        CATALOG_TABLE_PG_TABLE_DEF: "pg_table_def",
        CATALOG_TABLE_PG_VIEWS: "pg_views",
        CATALOG_TABLE_COLUMNS: "information_schema.columns",
        CATALOG_TABLE_SVV_EXTERNAL_COLUMNS: "SVV_EXTERNAL_COLUMNS",
    }
    return system.get(bare, bare)


def fetch_catalog_for_sql(
    sql: str,
    execute: ExecuteFn | SupportsExecute,
    *,
    database: str | None = None,
    fetch_views: bool = True,
    fetch_columns: bool = True,
    include_unreferenced_columns: bool = False,
    max_view_depth: int = 8,
    metadata_schema: str | None = None,
) -> Catalog:
    """Discover objects in *sql*, query Redshift for their attributes, build Catalog.

    Walks the SQL for every relation reference, then recursively expands any
    views found so underlying tables also receive attributes. Coverage notes
    are stored on ``catalog.coverage`` (missing / unresolved names).

    *metadata_schema*: when non-empty, read catalog mirrors from that schema
    (same bare table names as system catalogs). See :func:`catalog_relation`.
    """
    exec_fn = execute if callable(execute) else execute_dbapi(execute)  # type: ignore[arg-type]
    refs = discover_object_refs(sql)
    return fetch_catalog_for_refs(
        refs,
        exec_fn,
        database=database,
        fetch_views=fetch_views,
        fetch_columns=fetch_columns,
        include_unreferenced_columns=include_unreferenced_columns,
        max_view_depth=max_view_depth,
        seed_sql=sql,
        metadata_schema=metadata_schema,
    )


def fetch_catalog_for_refs(
    refs: Sequence[ObjectRef] | Sequence[str],
    execute: ExecuteFn | SupportsExecute,
    *,
    database: str | None = None,
    fetch_views: bool = True,
    fetch_columns: bool = True,
    include_unreferenced_columns: bool = False,
    max_view_depth: int = 8,
    seed_sql: str | None = None,
    metadata_schema: str | None = None,
) -> Catalog:
    """Fetch catalog for *refs*, recursively expanding view bodies to underlying tables."""
    exec_fn = execute if callable(execute) else execute_dbapi(execute)  # type: ignore[arg-type]
    object_refs = _coerce_refs(refs)
    meta = normalize_metadata_schema(metadata_schema)

    if database is None:
        database = _current_database(exec_fn)
    database = normalize_ident(database)

    pending = list(object_refs)
    seen_fetch_keys: set[str] = set()
    all_table_rows: list[dict[str, Any]] = []
    all_column_rows: list[dict[str, Any]] = []
    all_view_rows: list[dict[str, Any]] = []
    all_external_part_rows: list[dict[str, Any]] = []
    seen_table_keys: set[str] = set()
    seen_view_keys: set[str] = set()
    discovered: list[ObjectRef] = list(object_refs)

    for _depth in range(max(1, max_view_depth)):
        batch = unique_fetch_targets(
            [r for r in pending if r.fetch_key and r.fetch_key not in seen_fetch_keys]
        )
        if not batch:
            break
        for r in batch:
            seen_fetch_keys.add(r.fetch_key)

        table_rows = _fetch_table_info(exec_fn, batch, database, metadata_schema=meta)
        for row in table_rows:
            key = normalize_key(
                row.get("database") or database,
                row.get("schema"),
                row.get("table_name") or row.get("table"),
            )
            if key and key not in seen_table_keys:
                seen_table_keys.add(key)
                all_table_rows.append(row)

        if fetch_columns:
            for row in _fetch_columns(
                exec_fn, batch, include_all=include_unreferenced_columns, metadata_schema=meta
            ):
                all_column_rows.append(row)

        # Spectrum / external: first partition key only (summary is enough)
        for row in _fetch_external_partition_keys(exec_fn, batch, database, metadata_schema=meta):
            all_external_part_rows.append(row)

        new_from_views: list[ObjectRef] = []
        if fetch_views:
            view_rows = _fetch_views(exec_fn, batch, database, metadata_schema=meta)
            for row in view_rows:
                key = normalize_key(
                    row.get("database") or database,
                    row.get("schema"),
                    row.get("view_name") or row.get("viewname"),
                )
                if key and key not in seen_view_keys:
                    seen_view_keys.add(key)
                    all_view_rows.append(row)
                definition = str(
                    row.get("source_definition") or row.get("definition") or ""
                )
                if definition.strip():
                    try:
                        nested = discover_object_refs(definition, source="view_body")
                        new_from_views.extend(nested)
                        discovered.extend(nested)
                    except Exception:
                        pass
        pending = new_from_views

    catalog = catalog_from_rows(
        all_table_rows,
        column_rows=all_column_rows,
        view_rows=all_view_rows,
        default_database=database,
    )
    _apply_external_partition_keys(catalog, all_external_part_rows, default_database=database)
    catalog.coverage = assess_catalog_coverage(discovered, catalog, database=database)
    if seed_sql:
        catalog.coverage["seed_sql_ref_count"] = len(discover_object_refs(seed_sql))
    if meta:
        catalog.coverage["metadata_schema"] = meta
    return catalog


def assess_catalog_coverage(
    refs: Sequence[ObjectRef] | Sequence[str],
    catalog: Catalog,
    *,
    database: str = "",
) -> dict[str, Any]:
    """Report which discovered names resolved to tables, views, or nothing."""
    from .discover import unique_fetch_targets

    object_refs = unique_fetch_targets(_coerce_refs(refs))
    resolved_tables: list[str] = []
    resolved_views: list[str] = []
    missing: list[str] = []

    for ref in object_refs:
        candidates = [
            ref.identity,
            normalize_key(database, ref.schema, ref.name),
            normalize_key(ref.schema, ref.name),
            ref.name,
        ]
        hit_table = None
        hit_view = None
        for cand in candidates:
            if not cand:
                continue
            t = catalog.resolve_table(cand)
            if t is not None:
                hit_table = t[0]
                break
            v = catalog.resolve_view(cand)
            if v is not None:
                hit_view = v[0]
                break
        if hit_table:
            resolved_tables.append(hit_table)
        elif hit_view:
            resolved_views.append(hit_view)
        else:
            missing.append(ref.identity)

    return {
        "discovered": sorted({r.identity for r in _coerce_refs(refs)}),
        "resolved_tables": sorted(set(resolved_tables)),
        "resolved_views": sorted(set(resolved_views)),
        "missing": sorted(set(missing)),
        "table_count": len(catalog.tables),
        "view_count": len(catalog.views),
        "complete": len(missing) == 0,
    }


def catalog_from_rows(
    table_rows: Sequence[Mapping[str, Any]],
    *,
    column_rows: Sequence[Mapping[str, Any]] = (),
    view_rows: Sequence[Mapping[str, Any]] = (),
    default_database: str = "",
) -> Catalog:
    """Build a Catalog from already-fetched row mappings (live or DuckDB export).

    Accepts both live Redshift names and DataBasix warehouse aliases
    (``source_db``, ``schema_name``, ``table_name``, ``size`` vs ``size_mb``).
    """
    columns_by_table = _index_columns(column_rows)
    tables: dict[str, TableStats] = {}

    for row in table_rows:
        key, stats = _table_stats_from_row(row, columns_by_table, default_database)
        if key:
            tables[key] = stats

    # Tables that only appear in columns (no SVV_TABLE_INFO hit)
    for table_key, cols in columns_by_table.items():
        if table_key not in tables:
            # try without database
            bare = ".".join(table_key.split(".")[-2:]) if table_key.count(".") >= 2 else table_key
            if any(k.endswith("." + bare) or k == bare for k in tables):
                continue
            tables[table_key] = TableStats(columns=cols)

    views: dict[str, ViewDef] = {}
    for row in view_rows:
        key, view = _view_from_row(row, default_database)
        if key and view.sql.strip():
            views[key] = view

    return Catalog(tables=tables, views=views, default_database=default_database)


def catalog_from_databasix_frames(
    table_info,
    view_definitions=None,
    *,
    columns_frame=None,
    default_database: str = "",
) -> Catalog:
    """Convenience for pandas/DataBasix frames (optional pandas dependency at call site)."""
    table_rows = _frame_to_rows(table_info)
    view_rows = _frame_to_rows(view_definitions) if view_definitions is not None else []
    column_rows = _frame_to_rows(columns_frame) if columns_frame is not None else []
    return catalog_from_rows(
        table_rows,
        column_rows=column_rows,
        view_rows=view_rows,
        default_database=default_database,
    )


# ---------------------------------------------------------------------------
# SQL builders (Redshift)
# ---------------------------------------------------------------------------


def sql_current_database() -> str:
    return "SELECT current_database()::VARCHAR AS database_name"


def sql_table_info_for_names(
    table_names: Iterable[str],
    schemas: Iterable[str] | None = None,
    *,
    metadata_schema: str | None = None,
) -> str:
    """SVV_TABLE_INFO (or ``{metadata_schema}.svv_table_info``) for referenced names."""
    rel = catalog_relation(CATALOG_TABLE_SVV_TABLE_INFO, metadata_schema)
    names = sorted({normalize_ident(n) for n in table_names if normalize_ident(n)})
    if not names:
        return (
            "SELECT database::VARCHAR, schema::VARCHAR, \"table\"::VARCHAR AS table_name, "
            "table_id, diststyle::VARCHAR, sortkey1::VARCHAR, size, tbl_rows, encoded::VARCHAR "
            f"FROM {rel} WHERE 1=0"
        )
    name_list = ", ".join(_sql_string(n) for n in names)
    schema_filter = ""
    schema_set = sorted({normalize_ident(s) for s in (schemas or []) if normalize_ident(s)})
    if schema_set:
        schema_list = ", ".join(_sql_string(s) for s in schema_set)
        schema_filter = f" AND LOWER(schema) IN ({schema_list})"
    return f"""
SELECT
  TRIM(database)::VARCHAR AS database,
  TRIM(schema)::VARCHAR AS schema,
  TRIM("table")::VARCHAR AS table_name,
  table_id,
  TRIM(diststyle)::VARCHAR AS diststyle,
  TRIM(sortkey1)::VARCHAR AS sortkey1,
  size,
  tbl_rows,
  TRIM(encoded)::VARCHAR AS encoded
FROM {rel}
WHERE LOWER("table") IN ({name_list})
{schema_filter}
""".strip()


def sql_columns_for_names(
    table_names: Iterable[str],
    schemas: Iterable[str] | None = None,
    *,
    metadata_schema: str | None = None,
) -> str:
    """``information_schema.columns`` or ``{metadata_schema}.columns``."""
    rel = catalog_relation(CATALOG_TABLE_COLUMNS, metadata_schema)
    names = sorted({normalize_ident(n) for n in table_names if normalize_ident(n)})
    if not names:
        return (
            "SELECT table_schema, table_name, column_name, data_type, ordinal_position "
            f"FROM {rel} WHERE 1=0"
        )
    name_list = ", ".join(_sql_string(n) for n in names)
    schema_filter = ""
    schema_set = sorted({normalize_ident(s) for s in (schemas or []) if normalize_ident(s)})
    if schema_set:
        schema_list = ", ".join(_sql_string(s) for s in schema_set)
        schema_filter = f" AND LOWER(table_schema) IN ({schema_list})"
    return f"""
SELECT
  LOWER(TRIM(table_catalog))::VARCHAR AS database,
  LOWER(TRIM(table_schema))::VARCHAR AS schema,
  LOWER(TRIM(table_name))::VARCHAR AS table_name,
  LOWER(TRIM(column_name))::VARCHAR AS column_name,
  TRIM(data_type)::VARCHAR AS data_type,
  ordinal_position
FROM {rel}
WHERE LOWER(table_schema) NOT IN ('pg_catalog', 'information_schema', 'pg_internal')
  AND LOWER(table_name) IN ({name_list})
{schema_filter}
ORDER BY table_schema, table_name, ordinal_position
""".strip()


def sql_views_for_names(
    view_names: Iterable[str],
    schemas: Iterable[str] | None = None,
    *,
    metadata_schema: str | None = None,
) -> str:
    """``pg_views`` or ``{metadata_schema}.pg_views``."""
    rel = catalog_relation(CATALOG_TABLE_PG_VIEWS, metadata_schema)
    names = sorted({normalize_ident(n) for n in view_names if normalize_ident(n)})
    if not names:
        return (
            "SELECT current_database()::VARCHAR AS database, schemaname AS schema, "
            f"viewname AS view_name, definition AS source_definition FROM {rel} WHERE 1=0"
        )
    name_list = ", ".join(_sql_string(n) for n in names)
    schema_filter = ""
    schema_set = sorted({normalize_ident(s) for s in (schemas or []) if normalize_ident(s)})
    if schema_set:
        schema_list = ", ".join(_sql_string(s) for s in schema_set)
        schema_filter = f" AND LOWER(schemaname) IN ({schema_list})"
    # Chunk definition like DataBasix does for very large views
    chunks = " || ".join(
        f"COALESCE(SUBSTRING(definition, {(i * 65535) + 1}, 65535), '')" for i in range(12)
    )
    return f"""
SELECT
  current_database()::VARCHAR AS database,
  LOWER(TRIM(schemaname))::VARCHAR AS schema,
  LOWER(TRIM(viewname))::VARCHAR AS view_name,
  ({chunks})::VARCHAR AS source_definition
FROM {rel}
WHERE LOWER(schemaname) NOT IN ('pg_catalog', 'information_schema', 'admin')
  AND LOWER(viewname) IN ({name_list})
{schema_filter}
""".strip()


def sql_external_partition_keys(
    table_names: Iterable[str] | None = None,
    schemas: Iterable[str] | None = None,
    *,
    metadata_schema: str | None = None,
) -> str:
    """First partition-key column only from SVV_EXTERNAL_COLUMNS (or mirror).

    Full external column lists are **not** harvested. Only:

    * ``schemaname`` — schema of the external table
    * ``tablename`` — table name
    * ``columnname`` — partition-key column name (for ``part_key = 1``)
    * ``redshift_database_name`` — optional database
    * ``part_key`` — must be 1 for the first partition key (filter)

    A **summary** mirror is fine: one row per external table with the first
    partition key only (still use these exact column names). System relation:
    ``SVV_EXTERNAL_COLUMNS``. Mirror: ``{metadata_schema}.svv_external_columns``.
    """
    rel = catalog_relation(CATALOG_TABLE_SVV_EXTERNAL_COLUMNS, metadata_schema)
    names = sorted({normalize_ident(n) for n in (table_names or []) if normalize_ident(n)})
    schema_set = sorted({normalize_ident(s) for s in (schemas or []) if normalize_ident(s)})
    filters = ["part_key = 1"]
    if names:
        name_list = ", ".join(_sql_string(n) for n in names)
        filters.append(f"LOWER(TRIM(tablename)) IN ({name_list})")
    if schema_set:
        schema_list = ", ".join(_sql_string(s) for s in schema_set)
        filters.append(f"LOWER(TRIM(schemaname)) IN ({schema_list})")
    where = " AND ".join(filters)
    return f"""
SELECT
  TRIM(redshift_database_name)::VARCHAR AS database,
  TRIM(schemaname)::VARCHAR AS schema,
  TRIM(tablename)::VARCHAR AS table_name,
  TRIM(columnname)::VARCHAR AS partition_key
FROM {rel}
WHERE {where}
""".strip()


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _coerce_refs(refs: Sequence[ObjectRef] | Sequence[str]) -> list[ObjectRef]:
    out: list[ObjectRef] = []
    for item in refs:
        if isinstance(item, ObjectRef):
            out.append(item)
            continue
        text = normalize_ident(item)
        parts = [p for p in text.split(".") if p]
        if not parts:
            continue
        if len(parts) == 1:
            out.append(ObjectRef(text, "", "", parts[0], parts[0]))
        elif len(parts) == 2:
            out.append(ObjectRef(text, "", parts[0], parts[1], parts[1]))
        else:
            out.append(ObjectRef(text, parts[0], parts[1], parts[2], parts[2]))
    return out


def _current_database(execute: ExecuteFn) -> str:
    rows = execute(sql_current_database())
    if not rows:
        return ""
    row = rows[0]
    for key in ("database_name", "database", "current_database"):
        if key in row and row[key] is not None:
            return normalize_ident(row[key])
    # first value
    return normalize_ident(next(iter(row.values()), ""))


def _fetch_table_info(
    execute: ExecuteFn,
    refs: list[ObjectRef],
    database: str,
    *,
    metadata_schema: str | None = None,
) -> list[dict[str, Any]]:
    names = {r.name for r in refs if r.name}
    schemas = {r.schema for r in refs if r.schema}
    try:
        rows = list(
            execute(sql_table_info_for_names(names, schemas, metadata_schema=metadata_schema))
        )
    except Exception:
        return []
    out = []
    for row in rows:
        norm = {normalize_ident(k): v for k, v in row.items()}
        if "table" in norm and "table_name" not in norm:
            norm["table_name"] = norm["table"]
        if database and not norm.get("database"):
            norm["database"] = database
        out.append(norm)
    return out


def _fetch_columns(
    execute: ExecuteFn,
    refs: list[ObjectRef],
    *,
    include_all: bool,
    metadata_schema: str | None = None,
) -> list[dict[str, Any]]:
    names = {r.name for r in refs if r.name}
    schemas = {r.schema for r in refs if r.schema} if not include_all else None
    try:
        rows = list(
            execute(sql_columns_for_names(names, schemas, metadata_schema=metadata_schema))
        )
    except Exception:
        return []
    return [{normalize_ident(k): v for k, v in row.items()} for row in rows]


def _fetch_views(
    execute: ExecuteFn,
    refs: list[ObjectRef],
    database: str,
    *,
    metadata_schema: str | None = None,
) -> list[dict[str, Any]]:
    names = {r.name for r in refs if r.name}
    schemas = {r.schema for r in refs if r.schema}
    try:
        rows = list(execute(sql_views_for_names(names, schemas, metadata_schema=metadata_schema)))
    except Exception:
        return []
    out = []
    for row in rows:
        norm = {normalize_ident(k): v for k, v in row.items()}
        if database and not norm.get("database"):
            norm["database"] = database
        out.append(norm)
    return out


def _fetch_external_partition_keys(
    execute: ExecuteFn,
    refs: list[ObjectRef],
    database: str,
    *,
    metadata_schema: str | None = None,
) -> list[dict[str, Any]]:
    """Best-effort; missing SVV_EXTERNAL_COLUMNS / mirror is not fatal."""
    names = {r.name for r in refs if r.name}
    schemas = {r.schema for r in refs if r.schema}
    try:
        rows = list(
            execute(
                sql_external_partition_keys(names, schemas, metadata_schema=metadata_schema)
            )
        )
    except Exception:
        return []
    out = []
    for row in rows:
        norm = {normalize_ident(k): v for k, v in row.items()}
        if database and not norm.get("database"):
            norm["database"] = database
        out.append(norm)
    return out


def _apply_external_partition_keys(
    catalog: Catalog,
    external_rows: Sequence[Mapping[str, Any]],
    *,
    default_database: str = "",
) -> None:
    """Merge first partition-key hits onto catalog tables (marks external)."""
    for row in external_rows:
        r = {normalize_ident(k): v for k, v in row.items()}
        database = normalize_ident(
            r.get("database") or r.get("redshift_database_name") or default_database
        )
        schema = normalize_ident(r.get("schema") or r.get("schemaname") or "")
        table = normalize_ident(
            r.get("table_name") or r.get("tablename") or r.get("table") or ""
        )
        part_col = normalize_ident(
            r.get("partition_key") or r.get("columnname") or r.get("column_name") or ""
        )
        if not table or not part_col:
            continue
        key = normalize_key(database, schema, table)
        short = normalize_key(schema, table)
        existing = catalog.tables.get(key) or catalog.tables.get(short)
        if existing is not None:
            resolved_key = key if key in catalog.tables else short
            catalog.tables[resolved_key] = TableStats(
                columns=existing.columns,
                diststyle=existing.diststyle,
                distkey=existing.distkey,
                sortkeys=existing.sortkeys,
                rows=existing.rows,
                size_mb=existing.size_mb,
                is_external=True,
                object_type="external_table",
                partition_key=part_col,
            )
        else:
            catalog.tables[key] = TableStats(
                is_external=True,
                object_type="external_table",
                partition_key=part_col,
            )


def _index_columns(column_rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, str]]:
    by_table: dict[str, dict[str, str]] = {}
    for row in column_rows:
        r = {normalize_ident(k): v for k, v in row.items()}
        database = normalize_ident(r.get("database") or r.get("table_catalog") or "")
        schema = normalize_ident(r.get("schema") or r.get("table_schema") or r.get("schema_name") or "")
        table = normalize_ident(r.get("table_name") or r.get("table") or "")
        col = normalize_ident(r.get("column_name") or r.get("column") or "")
        dtype = str(r.get("data_type") or r.get("type") or "VARCHAR")
        if not table or not col:
            continue
        key = normalize_key(database, schema, table)
        by_table.setdefault(key, {})[col] = dtype
        # also short key
        short = normalize_key(schema, table)
        if short != key:
            by_table.setdefault(short, {})[col] = dtype
    return by_table


def _table_stats_from_row(
    row: Mapping[str, Any],
    columns_by_table: dict[str, dict[str, str]],
    default_database: str,
) -> tuple[str, TableStats]:
    r = {normalize_ident(k): v for k, v in row.items()}
    database = normalize_ident(
        r.get("database") or r.get("source_db") or r.get("redshift_database_name") or default_database
    )
    schema = normalize_ident(r.get("schema") or r.get("schema_name") or r.get("schemaname") or "")
    table = normalize_ident(r.get("table_name") or r.get("table") or r.get("tablename") or "")
    if not table:
        return "", TableStats()
    key = normalize_key(database, schema, table)
    short = normalize_key(schema, table)
    cols = dict(columns_by_table.get(key) or columns_by_table.get(short) or {})

    diststyle = str(r.get("diststyle") or "")
    distkey = _parse_distkey(diststyle) or normalize_ident(r.get("distkey") or "")
    sortkey1 = str(r.get("sortkey1") or r.get("sortkey") or "")
    sortkeys = _parse_sortkeys(sortkey1, r.get("sortkeys"))

    size_mb = _num(r.get("size_mb") if r.get("size_mb") is not None else r.get("size"))
    rows = _num(r.get("tbl_rows") if r.get("tbl_rows") is not None else r.get("rows"))
    object_type = normalize_ident(r.get("object_type") or "table")
    is_external = object_type in {"external", "external_table"} or bool(r.get("is_external"))

    partition_key = normalize_ident(r.get("partition_key") or "")
    stats = TableStats(
        columns=cols,
        diststyle=diststyle,
        distkey=distkey,
        sortkeys=tuple(sortkeys),
        rows=rows,
        size_mb=size_mb,
        is_external=is_external,
        object_type="external_table" if is_external else "table",
        partition_key=partition_key,
    )
    return key, stats


def _view_from_row(row: Mapping[str, Any], default_database: str) -> tuple[str, ViewDef]:
    r = {normalize_ident(k): v for k, v in row.items()}
    database = normalize_ident(r.get("database") or r.get("source_db") or default_database)
    schema = normalize_ident(r.get("schema") or r.get("schema_name") or r.get("schemaname") or "")
    name = normalize_ident(r.get("view_name") or r.get("viewname") or r.get("table_name") or "")
    # definition may be chunked
    definition = str(r.get("source_definition") or r.get("definition") or r.get("view_definition") or "")
    if not definition:
        parts = []
        for i in range(1, 33):
            key = f"definition_part_{i:02d}"
            if key in r and r[key]:
                parts.append(str(r[key]))
        definition = "".join(parts)
    key = normalize_key(database, schema, name)
    late = bool(r.get("is_late_binding")) or "with no schema binding" in definition.lower()
    return key, ViewDef(sql=definition.strip(), is_late_binding=late)


def _parse_distkey(diststyle: str) -> str:
    text = str(diststyle or "")
    match = re.search(r"key\s*\(\s*([^)]+?)\s*\)", text, re.IGNORECASE)
    if not match:
        return ""
    return normalize_ident(match.group(1))


def _parse_sortkeys(sortkey1: str, sortkeys_value: object) -> list[str]:
    if isinstance(sortkeys_value, (list, tuple)):
        return [normalize_ident(x) for x in sortkeys_value if normalize_ident(x)]
    text = str(sortkey1 or "").strip()
    if not text or text.lower() in {"none", "auto", "auto(sortkey)", "(auto)", "-"}:
        return []
    # forms: "col" or "col1,col2" or compound
    if text.startswith("(") and text.endswith(")"):
        text = text[1:-1]
    return [normalize_ident(part) for part in re.split(r"[,\s]+", text) if normalize_ident(part)]


def _num(value: object) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _frame_to_rows(frame) -> list[dict[str, Any]]:
    if frame is None:
        return []
    if isinstance(frame, list):
        return [dict(row) for row in frame]
    # pandas
    if hasattr(frame, "to_dict"):
        try:
            return frame.to_dict(orient="records")  # type: ignore[no-any-return]
        except Exception:
            pass
    raise TypeError(f"Unsupported frame type: {type(frame)!r}")
