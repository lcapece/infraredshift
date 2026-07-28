"""Cluster-wide table repository cache.

Builds a single on-disk store of table metrics by cycling **local** (physical)
databases — not datashare databases — and reading:

* ``SVV_TABLE_INFO`` — size, rows, diststyle, table_id (slow; cache aggressively)
* ``pg_table_def`` — **full compound sort key order**, distkey column, column types

Lookup policy for decomposition:

1. If the table is in the cache → use cached metrics (no SVV hit)
2. Else → query the live database (SVV_TABLE_INFO + pg_table_def) and optionally
   write through to the cache

Redshift does not support switching databases inside one session, so cache builds
use a ``connect(database)`` factory (new connection per database).
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .catalog import Catalog, TableStats, ViewDef, normalize_ident, normalize_key
from .discover import ObjectRef, discover_object_refs, unique_fetch_targets
from .fetch import (
    CATALOG_TABLE_PG_TABLE_DEF,
    CATALOG_TABLE_SVV_EXTERNAL_COLUMNS,
    CATALOG_TABLE_SVV_TABLE_INFO,
    ExecuteFn,
    SupportsExecute,
    _apply_external_partition_keys,
    _parse_distkey,
    _sql_string,
    assess_catalog_coverage,
    catalog_from_rows,
    catalog_relation,
    execute_dbapi,
    normalize_metadata_schema,
    sql_external_partition_keys,
    sql_views_for_names,
)

ConnectFn = Callable[[str], SupportsExecute | ExecuteFn]
ProgressFn = Callable[[str, dict[str, Any]], None]

# System / non-user databases to skip even if marked local
_SKIP_DATABASES = frozenset(
    {
        "template0",
        "template1",
        "padb_harvest",
        "sys:internal",
        "awsdatacatalog",
    }
)


# ---------------------------------------------------------------------------
# SQL (per-database session)
# ---------------------------------------------------------------------------


def sql_list_local_databases() -> str:
    """Prefer SVV_REDSHIFT_DATABASES; exclude datashares (database_type <> local)."""
    return """
SELECT
  TRIM(database_name)::VARCHAR AS database_name,
  LOWER(TRIM(database_type))::VARCHAR AS database_type
FROM svv_redshift_databases
WHERE LOWER(TRIM(database_type)) = 'local'
ORDER BY 1
""".strip()


def sql_list_databases_fallback() -> str:
    """Older clusters without usable svv_redshift_databases typing."""
    return """
SELECT
  TRIM(datname)::VARCHAR AS database_name,
  'local'::VARCHAR AS database_type
FROM pg_database
WHERE datistemplate = false
  AND datallowconn = true
ORDER BY 1
""".strip()


def sql_svv_table_info_all(*, metadata_schema: str | None = None) -> str:
    """Full SVV_TABLE_INFO (or ``{metadata_schema}.svv_table_info``)."""
    rel = catalog_relation(CATALOG_TABLE_SVV_TABLE_INFO, metadata_schema)
    return f"""
SELECT
  TRIM(database)::VARCHAR AS database,
  TRIM(schema)::VARCHAR AS schema,
  TRIM("table")::VARCHAR AS table_name,
  table_id,
  TRIM(encoded)::VARCHAR AS encoded,
  TRIM(diststyle)::VARCHAR AS diststyle,
  TRIM(sortkey1)::VARCHAR AS sortkey1,
  max_varchar,
  sortkey_num,
  size,
  pct_used,
  empty,
  unsorted,
  stats_off,
  tbl_rows,
  skew_sortkey1,
  skew_rows,
  estimated_visible_rows,
  create_time
FROM {rel}
""".strip()


def sql_pg_table_def_all(*, metadata_schema: str | None = None) -> str:
    """Column-level design: full sort key positions, distkey flag, types.

    ``sortkey`` is 0 when not part of the sort key; non-zero values are the
    1-based position in the compound sort key (what SVV_TABLE_INFO.sortkey1
    alone cannot express for multi-column keys).

    System: ``pg_table_def``. Mirror: ``{metadata_schema}.pg_table_def``.
    """
    rel = catalog_relation(CATALOG_TABLE_PG_TABLE_DEF, metadata_schema)
    return f"""
SELECT
  TRIM(schemaname)::VARCHAR AS schema,
  TRIM(tablename)::VARCHAR AS table_name,
  TRIM("column")::VARCHAR AS column_name,
  TRIM("type")::VARCHAR AS data_type,
  TRIM(encoding)::VARCHAR AS encoding,
  distkey,
  sortkey,
  "notnull" AS is_not_null
FROM {rel}
WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_internal', 'pg_automv')
ORDER BY schemaname, tablename, sortkey, "column"
""".strip()


def sql_pg_table_def_for_tables(
    table_names: Iterable[str],
    schemas: Iterable[str] | None = None,
    *,
    metadata_schema: str | None = None,
) -> str:
    rel = catalog_relation(CATALOG_TABLE_PG_TABLE_DEF, metadata_schema)
    names = sorted({normalize_ident(n) for n in table_names if normalize_ident(n)})
    if not names:
        # Full SELECT with WHERE 1=0 — never append after ORDER BY.
        return f"""
SELECT
  TRIM(schemaname)::VARCHAR AS schema,
  TRIM(tablename)::VARCHAR AS table_name,
  TRIM("column")::VARCHAR AS column_name,
  TRIM("type")::VARCHAR AS data_type,
  TRIM(encoding)::VARCHAR AS encoding,
  distkey,
  sortkey,
  "notnull" AS is_not_null
FROM {rel}
WHERE 1=0
""".strip()
    name_list = ", ".join(_sql_string(n) for n in names)
    schema_filter = ""
    schema_set = sorted({normalize_ident(s) for s in (schemas or []) if normalize_ident(s)})
    if schema_set:
        schema_list = ", ".join(_sql_string(s) for s in schema_set)
        schema_filter = f" AND LOWER(schemaname) IN ({schema_list})"
    return f"""
SELECT
  TRIM(schemaname)::VARCHAR AS schema,
  TRIM(tablename)::VARCHAR AS table_name,
  TRIM("column")::VARCHAR AS column_name,
  TRIM("type")::VARCHAR AS data_type,
  TRIM(encoding)::VARCHAR AS encoding,
  distkey,
  sortkey,
  "notnull" AS is_not_null
FROM {rel}
WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_internal', 'pg_automv')
  AND LOWER(tablename) IN ({name_list})
{schema_filter}
ORDER BY schemaname, tablename, sortkey, "column"
""".strip()


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------


@dataclass
class BuildReport:
    path: str
    databases_planned: list[str] = field(default_factory=list)
    databases_ok: list[str] = field(default_factory=list)
    databases_failed: dict[str, str] = field(default_factory=dict)
    table_count: int = 0
    column_count: int = 0
    elapsed_seconds: float = 0.0


class TableRepository:
    """SQLite-backed cluster table metric cache (single file)."""

    def __init__(self, path: str | Path):
        self.path = str(Path(path).expanduser().resolve())
        self._ensure_schema()

    # -- schema -------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _ensure_schema(self) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS databases (
                  database_name TEXT PRIMARY KEY,
                  database_type TEXT NOT NULL DEFAULT 'local',
                  captured_at TEXT,
                  table_count INTEGER NOT NULL DEFAULT 0,
                  status TEXT NOT NULL DEFAULT 'ok',
                  error TEXT
                );

                CREATE TABLE IF NOT EXISTS table_metrics (
                  database_name TEXT NOT NULL,
                  schema_name TEXT NOT NULL,
                  table_name TEXT NOT NULL,
                  table_id TEXT,
                  diststyle TEXT,
                  distkey TEXT,
                  sortkey1 TEXT,
                  sortkeys_json TEXT NOT NULL DEFAULT '[]',
                  sortkey_num REAL,
                  size_mb REAL,
                  tbl_rows REAL,
                  encoded TEXT,
                  unsorted REAL,
                  stats_off REAL,
                  skew_rows REAL,
                  estimated_visible_rows REAL,
                  create_time TEXT,
                  extras_json TEXT,
                  captured_at TEXT NOT NULL,
                  PRIMARY KEY (database_name, schema_name, table_name)
                );

                CREATE TABLE IF NOT EXISTS table_columns (
                  database_name TEXT NOT NULL,
                  schema_name TEXT NOT NULL,
                  table_name TEXT NOT NULL,
                  column_name TEXT NOT NULL,
                  data_type TEXT,
                  encoding TEXT,
                  is_distkey INTEGER NOT NULL DEFAULT 0,
                  sortkey_pos INTEGER NOT NULL DEFAULT 0,
                  is_not_null INTEGER NOT NULL DEFAULT 0,
                  PRIMARY KEY (database_name, schema_name, table_name, column_name)
                );

                CREATE INDEX IF NOT EXISTS idx_table_metrics_name
                  ON table_metrics(table_name);
                CREATE INDEX IF NOT EXISTS idx_table_columns_name
                  ON table_columns(table_name);
                """
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', '1')"
            )

    # -- build --------------------------------------------------------------

    def build(
        self,
        connect: ConnectFn,
        *,
        databases: Sequence[str] | None = None,
        bootstrap_database: str | None = None,
        progress: ProgressFn | None = None,
        replace: bool = True,
        metadata_schema: str | None = None,
    ) -> BuildReport:
        """Cycle local databases and populate the cache.

        Parameters
        ----------
        connect:
            ``connect(database_name) -> DB-API connection or execute(sql)->rows``.
            Must open a session **to that database** (Redshift cannot USE).
        databases:
            Optional explicit list. Default: discover local DBs via
            ``svv_redshift_databases`` (excludes datashares).
        bootstrap_database:
            Database used only to list cluster databases when *databases* is None.
        replace:
            If True (default), wipe previous table/column rows before rebuild.
        metadata_schema:
            When non-empty, read ``svv_table_info`` / ``pg_table_def`` /
            ``svv_external_columns`` from this schema instead of system catalogs.
        """
        started = time.perf_counter()
        report = BuildReport(path=self.path)
        meta = normalize_metadata_schema(metadata_schema)

        if databases is None:
            boot = bootstrap_database or "dev"
            try:
                exec0 = _as_execute(connect(boot))
            except Exception:
                # last resort: try template1-like names later
                raise
            db_list = list_local_databases(exec0)
        else:
            db_list = [normalize_ident(d) for d in databases if normalize_ident(d)]

        db_list = [d for d in db_list if d and d not in _SKIP_DATABASES]
        report.databases_planned = list(db_list)

        if progress:
            progress("planned", {"databases": db_list, "metadata_schema": meta})

        with self._connect() as conn:
            if replace:
                conn.execute("DELETE FROM table_metrics")
                conn.execute("DELETE FROM table_columns")
                conn.execute("DELETE FROM databases")
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES ('built_at', ?)",
                (_utcnow(),),
            )
            if meta:
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES ('metadata_schema', ?)",
                    (meta,),
                )

        for db in db_list:
            if progress:
                progress("database_start", {"database": db})
            try:
                exec_fn = _as_execute(connect(db))
                n_tables, n_cols = self._ingest_database(db, exec_fn, metadata_schema=meta)
                report.databases_ok.append(db)
                report.table_count += n_tables
                report.column_count += n_cols
                if progress:
                    progress(
                        "database_ok",
                        {"database": db, "tables": n_tables, "columns": n_cols},
                    )
            except Exception as exc:
                msg = str(exc).splitlines()[0][:500]
                report.databases_failed[db] = msg
                with self._connect() as conn:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO databases(
                          database_name, database_type, captured_at, table_count, status, error
                        ) VALUES (?, 'local', ?, 0, 'error', ?)
                        """,
                        (db, _utcnow(), msg),
                    )
                if progress:
                    progress("database_error", {"database": db, "error": msg})

        report.elapsed_seconds = round(time.perf_counter() - started, 3)
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES ('last_build_report', ?)",
                (json.dumps({
                    "databases_ok": report.databases_ok,
                    "databases_failed": report.databases_failed,
                    "table_count": report.table_count,
                    "elapsed_seconds": report.elapsed_seconds,
                }),),
            )
        if progress:
            progress("done", {"report": report})
        return report

    def _ingest_database(
        self,
        database: str,
        execute: ExecuteFn,
        *,
        metadata_schema: str | None = None,
    ) -> tuple[int, int]:
        database = normalize_ident(database)
        captured = _utcnow()
        meta = normalize_metadata_schema(metadata_schema)

        # 1) SVV_TABLE_INFO (slow) — or {metadata_schema}.svv_table_info
        info_rows = [_norm_row(r) for r in execute(sql_svv_table_info_all(metadata_schema=meta))]
        # 2) pg_table_def — full sort keys + columns
        def_rows = [_norm_row(r) for r in execute(sql_pg_table_def_all(metadata_schema=meta))]
        # 3) first partition key for external tables (summary OK)
        try:
            ext_rows = [
                _norm_row(r)
                for r in execute(sql_external_partition_keys(metadata_schema=meta))
            ]
        except Exception:
            ext_rows = []
        ext_by_st: dict[tuple[str, str], str] = {}
        for erow in ext_rows:
            eschema = normalize_ident(erow.get("schema") or erow.get("schemaname"))
            etable = normalize_ident(erow.get("table_name") or erow.get("tablename"))
            pkey = normalize_ident(
                erow.get("partition_key") or erow.get("columnname") or erow.get("column_name")
            )
            if eschema is not None and etable and pkey:
                ext_by_st[(eschema, etable)] = pkey

        cols_by_table: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in def_rows:
            schema = normalize_ident(row.get("schema") or row.get("schemaname"))
            table = normalize_ident(row.get("table_name") or row.get("tablename"))
            if not table:
                continue
            cols_by_table.setdefault((schema, table), []).append(row)

        with self._connect() as conn:
            # clear this database slice for incremental-friendly re-runs
            conn.execute("DELETE FROM table_metrics WHERE database_name = ?", (database,))
            conn.execute("DELETE FROM table_columns WHERE database_name = ?", (database,))

            col_count = 0
            for (schema, table), col_rows in cols_by_table.items():
                for crow in col_rows:
                    col_name = normalize_ident(crow.get("column_name") or crow.get("column"))
                    if not col_name:
                        continue
                    sortkey_pos = _int(crow.get("sortkey"))
                    is_dk = 1 if _truthy(crow.get("distkey")) else 0
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO table_columns(
                          database_name, schema_name, table_name, column_name,
                          data_type, encoding, is_distkey, sortkey_pos, is_not_null
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            database,
                            schema,
                            table,
                            col_name,
                            str(crow.get("data_type") or crow.get("type") or ""),
                            str(crow.get("encoding") or ""),
                            is_dk,
                            sortkey_pos,
                            1 if _truthy(crow.get("is_not_null") or crow.get("notnull")) else 0,
                        ),
                    )
                    col_count += 1

            # index SVV rows by schema.table
            info_by_st: dict[tuple[str, str], dict[str, Any]] = {}
            for row in info_rows:
                schema = normalize_ident(row.get("schema") or row.get("schema_name"))
                table = normalize_ident(row.get("table_name") or row.get("table"))
                if table:
                    info_by_st[(schema, table)] = row

            # union of tables from SVV, pg_table_def, and external partition keys
            all_keys = set(info_by_st) | set(cols_by_table) | set(ext_by_st)
            for schema, table in sorted(all_keys):
                info = info_by_st.get((schema, table), {})
                col_rows = cols_by_table.get((schema, table), [])
                sortkeys = _sortkeys_from_pg_table_def(col_rows)
                distkey = _distkey_from_pg_table_def(col_rows) or _parse_distkey(
                    str(info.get("diststyle") or "")
                )
                # keep SVV sortkey1 as fallback if pg_table_def empty (permissions)
                if not sortkeys:
                    sk1 = normalize_ident(info.get("sortkey1") or "")
                    if sk1 and sk1 not in {"none", "auto", "auto(sortkey)", "-", "0"}:
                        sortkeys = [sk1]

                extras = {
                    k: info.get(k)
                    for k in (
                        "pct_used",
                        "empty",
                        "max_varchar",
                        "skew_sortkey1",
                    )
                    if k in info
                }
                part_key = ext_by_st.get((schema, table), "")
                if part_key:
                    extras["partition_key"] = part_key
                    extras["is_external"] = True
                conn.execute(
                    """
                    INSERT OR REPLACE INTO table_metrics(
                      database_name, schema_name, table_name, table_id,
                      diststyle, distkey, sortkey1, sortkeys_json, sortkey_num,
                      size_mb, tbl_rows, encoded, unsorted, stats_off, skew_rows,
                      estimated_visible_rows, create_time, extras_json, captured_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        database,
                        schema,
                        table,
                        str(info.get("table_id") or ""),
                        str(info.get("diststyle") or ""),
                        distkey,
                        str(info.get("sortkey1") or ""),
                        json.dumps(sortkeys),
                        _float(info.get("sortkey_num")),
                        _float(info.get("size") if info.get("size") is not None else info.get("size_mb")),
                        _float(info.get("tbl_rows")),
                        str(info.get("encoded") or ""),
                        _float(info.get("unsorted")),
                        _float(info.get("stats_off")),
                        _float(info.get("skew_rows")),
                        _float(info.get("estimated_visible_rows")),
                        str(info.get("create_time") or ""),
                        json.dumps(extras, default=str),
                        captured,
                    ),
                )

            conn.execute(
                """
                INSERT OR REPLACE INTO databases(
                  database_name, database_type, captured_at, table_count, status, error
                ) VALUES (?, 'local', ?, ?, 'ok', NULL)
                """,
                (database, captured, len(all_keys)),
            )

        return len(all_keys), col_count

    # -- read API -----------------------------------------------------------

    def databases(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT database_name FROM databases WHERE status = 'ok' ORDER BY 1"
            ).fetchall()
        return [r["database_name"] for r in rows]

    def stats(self) -> dict[str, Any]:
        with self._connect() as conn:
            tables = conn.execute("SELECT COUNT(*) AS c FROM table_metrics").fetchone()["c"]
            cols = conn.execute("SELECT COUNT(*) AS c FROM table_columns").fetchone()["c"]
            dbs = conn.execute("SELECT COUNT(*) AS c FROM databases WHERE status='ok'").fetchone()["c"]
            built = conn.execute(
                "SELECT value FROM meta WHERE key = 'built_at'"
            ).fetchone()
        return {
            "path": self.path,
            "databases": dbs,
            "tables": tables,
            "columns": cols,
            "built_at": built["value"] if built else None,
        }

    def get_table(
        self,
        database: str,
        schema: str,
        table: str,
    ) -> TableStats | None:
        database, schema, table = normalize_ident(database), normalize_ident(schema), normalize_ident(table)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM table_metrics
                WHERE database_name = ? AND schema_name = ? AND table_name = ?
                """,
                (database, schema, table),
            ).fetchone()
            if row is None:
                # try without forcing schema if empty schema lookup
                return None
            cols = conn.execute(
                """
                SELECT column_name, data_type, is_distkey, sortkey_pos
                FROM table_columns
                WHERE database_name = ? AND schema_name = ? AND table_name = ?
                ORDER BY CASE WHEN sortkey_pos > 0 THEN sortkey_pos ELSE 9999 END, column_name
                """,
                (database, schema, table),
            ).fetchall()
        return _row_to_stats(row, cols)

    def find_tables(
        self,
        *,
        database: str | None = None,
        schema: str | None = None,
        table: str | None = None,
    ) -> list[tuple[str, TableStats]]:
        """Return (qualified_key, stats) matches. Unspecified parts are wildcards."""
        clauses = []
        params: list[Any] = []
        if database:
            clauses.append("database_name = ?")
            params.append(normalize_ident(database))
        if schema:
            clauses.append("schema_name = ?")
            params.append(normalize_ident(schema))
        if table:
            clauses.append("table_name = ?")
            params.append(normalize_ident(table))
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM table_metrics{where} ORDER BY database_name, schema_name, table_name",
                params,
            ).fetchall()
            out: list[tuple[str, TableStats]] = []
            for row in rows:
                cols = conn.execute(
                    """
                    SELECT column_name, data_type, is_distkey, sortkey_pos
                    FROM table_columns
                    WHERE database_name = ? AND schema_name = ? AND table_name = ?
                    ORDER BY CASE WHEN sortkey_pos > 0 THEN sortkey_pos ELSE 9999 END, column_name
                    """,
                    (row["database_name"], row["schema_name"], row["table_name"]),
                ).fetchall()
                key = normalize_key(row["database_name"], row["schema_name"], row["table_name"])
                out.append((key, _row_to_stats(row, cols)))
        return out

    def resolve_ref(
        self,
        identity: str,
        *,
        default_database: str = "",
    ) -> tuple[str, TableStats] | None:
        """Resolve ``db.schema.table`` / ``schema.table`` / ``table`` against the cache."""
        identity = normalize_ident(identity)
        parts = [p for p in identity.split(".") if p]
        if not parts:
            return None
        if len(parts) >= 3:
            return self._one(parts[0], parts[1], parts[2])
        if len(parts) == 2:
            schema, table = parts
            hits = self.find_tables(schema=schema, table=table)
            if default_database:
                prefer = [h for h in hits if h[0].startswith(normalize_ident(default_database) + ".")]
                if len(prefer) == 1:
                    return prefer[0]
            if len(hits) == 1:
                return hits[0]
            return None
        # bare table
        hits = self.find_tables(table=parts[0])
        if default_database:
            prefer = [h for h in hits if h[0].startswith(normalize_ident(default_database) + ".")]
            if len(prefer) == 1:
                return prefer[0]
        if len(hits) == 1:
            return hits[0]
        return None

    def _one(self, database: str, schema: str, table: str) -> tuple[str, TableStats] | None:
        stats = self.get_table(database, schema, table)
        if stats is None:
            return None
        return normalize_key(database, schema, table), stats

    def catalog_for_refs(
        self,
        refs: Sequence[ObjectRef] | Sequence[str],
        *,
        default_database: str = "",
    ) -> tuple[Catalog, list[ObjectRef]]:
        """Build a partial Catalog from cache. Returns (catalog, misses)."""
        from .fetch import _coerce_refs

        object_refs = unique_fetch_targets(_coerce_refs(refs))
        tables: dict[str, TableStats] = {}
        misses: list[ObjectRef] = []
        for ref in object_refs:
            hit = self.resolve_ref(ref.identity, default_database=default_database)
            if hit is None and ref.schema and ref.name:
                hit = self.resolve_ref(
                    normalize_key(default_database, ref.schema, ref.name),
                    default_database=default_database,
                )
            if hit is None and ref.name:
                # try schema.table with default db
                if ref.schema:
                    hit = self.resolve_ref(
                        normalize_key(ref.schema, ref.name),
                        default_database=default_database,
                    )
            if hit is None:
                misses.append(ref)
            else:
                tables[hit[0]] = hit[1]
        catalog = Catalog(tables=tables, default_database=default_database)
        return catalog, misses

    def upsert_live_table(
        self,
        database: str,
        schema: str,
        table: str,
        stats: TableStats,
        *,
        table_id: str = "",
        sortkey1: str = "",
    ) -> None:
        """Write-through after a live SVV miss fill."""
        database, schema, table = normalize_ident(database), normalize_ident(schema), normalize_ident(table)
        captured = _utcnow()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO table_metrics(
                  database_name, schema_name, table_name, table_id,
                  diststyle, distkey, sortkey1, sortkeys_json, sortkey_num,
                  size_mb, tbl_rows, encoded, unsorted, stats_off, skew_rows,
                  estimated_visible_rows, create_time, extras_json, captured_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    database,
                    schema,
                    table,
                    table_id,
                    stats.diststyle,
                    stats.distkey,
                    sortkey1 or (stats.sortkeys[0] if stats.sortkeys else ""),
                    json.dumps(list(stats.sortkeys)),
                    float(len(stats.sortkeys)),
                    stats.size_mb,
                    stats.rows,
                    "",
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    "",
                    "{}",
                    captured,
                ),
            )
            conn.execute(
                """
                DELETE FROM table_columns
                WHERE database_name = ? AND schema_name = ? AND table_name = ?
                """,
                (database, schema, table),
            )
            sort_pos = {name: i + 1 for i, name in enumerate(stats.sortkeys)}
            for col, dtype in stats.columns.items():
                conn.execute(
                    """
                    INSERT OR REPLACE INTO table_columns(
                      database_name, schema_name, table_name, column_name,
                      data_type, encoding, is_distkey, sortkey_pos, is_not_null
                    ) VALUES (?, ?, ?, ?, ?, '', ?, ?, 0)
                    """,
                    (
                        database,
                        schema,
                        table,
                        normalize_ident(col),
                        str(dtype),
                        1 if normalize_ident(col) == normalize_ident(stats.distkey) else 0,
                        int(sort_pos.get(normalize_ident(col), 0)),
                    ),
                )


# ---------------------------------------------------------------------------
# Discovery + cache-first catalog fetch
# ---------------------------------------------------------------------------


def list_local_databases(execute: ExecuteFn | SupportsExecute) -> list[str]:
    """Return local (non-datashare) database names."""
    exec_fn = execute if callable(execute) else execute_dbapi(execute)  # type: ignore[arg-type]
    rows: Sequence[Any] = []
    try:
        rows = exec_fn(sql_list_local_databases())
    except Exception:
        rows = exec_fn(sql_list_databases_fallback())
    names: list[str] = []
    for row in rows:
        r = _norm_row(row)
        name = normalize_ident(r.get("database_name") or r.get("datname") or "")
        db_type = normalize_ident(r.get("database_type") or "local")
        if not name or name in _SKIP_DATABASES:
            continue
        if db_type and db_type not in {"local", ""}:
            continue
        names.append(name)
    return sorted(set(names))


def build_table_repository(
    connect: ConnectFn,
    path: str | Path,
    *,
    databases: Sequence[str] | None = None,
    bootstrap_database: str | None = None,
    progress: ProgressFn | None = None,
    replace: bool = True,
    metadata_schema: str | None = None,
) -> BuildReport:
    """Convenience: create/open repository and run a full cluster cache build."""
    repo = TableRepository(path)
    return repo.build(
        connect,
        databases=databases,
        bootstrap_database=bootstrap_database,
        progress=progress,
        replace=replace,
        metadata_schema=metadata_schema,
    )


def fetch_catalog_with_repository(
    sql: str,
    *,
    repository: TableRepository | str | Path | None = None,
    connect: ConnectFn | None = None,
    execute: ExecuteFn | SupportsExecute | None = None,
    database: str | None = None,
    fetch_views: bool = True,
    write_through: bool = True,
    max_view_depth: int = 8,
    metadata_schema: str | None = None,
) -> Catalog:
    """Cache-first catalog load for relations in *sql*.

    1. Discover refs (+ recursive view bodies when views are fetched live)
    2. Fill from repository when present
    3. For misses, live-query the appropriate database (requires *connect* or
       *execute* for the current database only)

    *metadata_schema*: when set, live misses read catalog mirrors from that
    schema (same bare table names as system catalogs).
    """
    from .fetch import fetch_catalog_for_refs, sql_columns_for_names, sql_table_info_for_names

    meta = normalize_metadata_schema(metadata_schema)
    repo = (
        repository
        if isinstance(repository, TableRepository)
        else TableRepository(repository)
        if repository is not None
        else None
    )

    refs = discover_object_refs(sql)
    default_db = normalize_ident(database or "")

    cached = Catalog(default_database=default_db)
    misses: list[ObjectRef] = list(refs)

    if repo is not None:
        cached, misses = repo.catalog_for_refs(refs, default_database=default_db)

    # Live fill for misses
    live_tables: dict[str, TableStats] = dict(cached.tables)
    live_views: dict[str, ViewDef] = dict(cached.views)

    if misses and (connect is not None or execute is not None):
        # Group misses by database when known; otherwise use default / execute
        by_db: dict[str, list[ObjectRef]] = {}
        for ref in misses:
            db = ref.database or default_db or ""
            by_db.setdefault(db, []).append(ref)

        for db, group in by_db.items():
            if connect is not None and db:
                exec_fn = _as_execute(connect(db))
            elif execute is not None:
                exec_fn = execute if callable(execute) else execute_dbapi(execute)  # type: ignore[arg-type]
            elif connect is not None:
                # no db on ref — use bootstrap default
                target = db or default_db or "dev"
                exec_fn = _as_execute(connect(target))
                db = target
            else:
                continue

            if not default_db:
                try:
                    from .fetch import _current_database

                    default_db = _current_database(exec_fn)
                except Exception:
                    default_db = db

            # SVV + pg_table_def for misses only (honors metadata_schema)
            names = {r.name for r in group}
            schemas = {r.schema for r in group if r.schema}
            info_rows = [
                _norm_row(r)
                for r in exec_fn(
                    sql_table_info_for_names(names, schemas, metadata_schema=meta)
                )
            ]
            def_rows = [
                _norm_row(r)
                for r in exec_fn(
                    sql_pg_table_def_for_tables(names, schemas, metadata_schema=meta)
                )
            ]
            col_rows_is = [
                _norm_row(r)
                for r in exec_fn(sql_columns_for_names(names, schemas, metadata_schema=meta))
            ]
            try:
                ext_rows = [
                    _norm_row(r)
                    for r in exec_fn(
                        sql_external_partition_keys(names, schemas, metadata_schema=meta)
                    )
                ]
            except Exception:
                ext_rows = []

            # prefer pg_table_def columns/sortkeys
            def_by_st: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for row in def_rows:
                st = (
                    normalize_ident(row.get("schema")),
                    normalize_ident(row.get("table_name") or row.get("tablename")),
                )
                def_by_st.setdefault(st, []).append(row)

            is_cols: dict[tuple[str, str], dict[str, str]] = {}
            for row in col_rows_is:
                st = (
                    normalize_ident(row.get("schema") or row.get("table_schema")),
                    normalize_ident(row.get("table_name")),
                )
                col = normalize_ident(row.get("column_name"))
                if col:
                    is_cols.setdefault(st, {})[col] = str(row.get("data_type") or "VARCHAR")

            for row in info_rows:
                schema = normalize_ident(row.get("schema"))
                table = normalize_ident(row.get("table_name") or row.get("table"))
                dbname = normalize_ident(row.get("database") or db or default_db)
                col_defs = def_by_st.get((schema, table), [])
                columns = {
                    normalize_ident(c.get("column_name") or c.get("column")): str(
                        c.get("data_type") or c.get("type") or "VARCHAR"
                    )
                    for c in col_defs
                    if normalize_ident(c.get("column_name") or c.get("column"))
                } or is_cols.get((schema, table), {})
                sortkeys = _sortkeys_from_pg_table_def(col_defs)
                if not sortkeys:
                    sk1 = normalize_ident(row.get("sortkey1") or "")
                    if sk1 and sk1 not in {"none", "auto", "-", "0"}:
                        sortkeys = [sk1]
                distkey = _distkey_from_pg_table_def(col_defs) or _parse_distkey(
                    str(row.get("diststyle") or "")
                )
                stats = TableStats(
                    columns=columns,
                    diststyle=str(row.get("diststyle") or ""),
                    distkey=distkey,
                    sortkeys=tuple(sortkeys),
                    rows=_float(row.get("tbl_rows")),
                    size_mb=_float(row.get("size") if row.get("size") is not None else row.get("size_mb")),
                )
                key = normalize_key(dbname, schema, table)
                live_tables[key] = stats
                if repo is not None and write_through:
                    repo.upsert_live_table(
                        dbname,
                        schema,
                        table,
                        stats,
                        table_id=str(row.get("table_id") or ""),
                        sortkey1=str(row.get("sortkey1") or ""),
                    )

            # Merge first partition keys (summary external columns)
            tmp_cat = Catalog(tables=live_tables, views=live_views, default_database=default_db)
            _apply_external_partition_keys(tmp_cat, ext_rows, default_database=db or default_db)
            live_tables = dict(tmp_cat.tables)

            if fetch_views:
                pending_view_sql: list[str] = []
                for row in exec_fn(sql_views_for_names(names, schemas, metadata_schema=meta)):
                    r = _norm_row(row)
                    vdb = normalize_ident(r.get("database") or db or default_db)
                    vschema = normalize_ident(r.get("schema"))
                    vname = normalize_ident(r.get("view_name") or r.get("viewname"))
                    definition = str(r.get("source_definition") or r.get("definition") or "")
                    if vname and definition.strip():
                        live_views[normalize_key(vdb, vschema, vname)] = ViewDef(sql=definition.strip())
                        pending_view_sql.append(definition)
                # Expand view bodies: cache first, then live-fetch remaining tables
                depth = 0
                while pending_view_sql and depth < max_view_depth:
                    depth += 1
                    next_sql: list[str] = []
                    nested_refs: list[ObjectRef] = []
                    for body in pending_view_sql:
                        try:
                            nested_refs.extend(discover_object_refs(body, source="view_body"))
                        except Exception:
                            continue
                    if not nested_refs:
                        break
                    if repo is not None:
                        nested_cat, nested_miss = repo.catalog_for_refs(
                            nested_refs, default_database=db or default_db
                        )
                        live_tables.update(nested_cat.tables)
                    else:
                        nested_miss = nested_refs
                    if nested_miss:
                        more = fetch_catalog_for_refs(
                            nested_miss,
                            exec_fn,
                            database=db or default_db,
                            fetch_views=True,
                            max_view_depth=1,
                            metadata_schema=meta,
                        )
                        live_tables.update(more.tables)
                        live_views.update(more.views)
                        for _vk, vdef in more.views.items():
                            if vdef.sql:
                                next_sql.append(vdef.sql)
                        if repo is not None and write_through:
                            for key, stats in more.tables.items():
                                parts = key.split(".")
                                if len(parts) >= 3:
                                    repo.upsert_live_table(parts[0], parts[1], parts[2], stats)
                    pending_view_sql = next_sql

    catalog = Catalog(
        tables=live_tables,
        views=live_views,
        default_database=default_db,
    )
    catalog.coverage = assess_catalog_coverage(
        discover_object_refs(sql),
        catalog,
        database=default_db,
    )
    if meta:
        catalog.coverage["metadata_schema"] = meta
    if repo is not None:
        catalog.coverage["repository"] = repo.stats()
        catalog.coverage["cache_hits"] = len(cached.tables)
        catalog.coverage["cache_misses"] = len(misses)
    return catalog


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _as_execute(obj: SupportsExecute | ExecuteFn) -> ExecuteFn:
    if callable(obj) and not hasattr(obj, "cursor"):
        return obj  # type: ignore[return-value]
    return execute_dbapi(obj)  # type: ignore[arg-type]


def _norm_row(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return {normalize_ident(k): v for k, v in row.items()}
    if hasattr(row, "keys"):
        return {normalize_ident(k): row[k] for k in row.keys()}
    raise TypeError(f"Expected mapping row, got {type(row)!r}")


def _sortkeys_from_pg_table_def(col_rows: list[dict[str, Any]]) -> list[str]:
    pairs: list[tuple[int, str]] = []
    for row in col_rows:
        pos = _int(row.get("sortkey"))
        name = normalize_ident(row.get("column_name") or row.get("column"))
        if pos > 0 and name:
            pairs.append((pos, name))
    pairs.sort(key=lambda item: item[0])
    return [name for _, name in pairs]


def _distkey_from_pg_table_def(col_rows: list[dict[str, Any]]) -> str:
    for row in col_rows:
        if _truthy(row.get("distkey")):
            return normalize_ident(row.get("column_name") or row.get("column"))
    return ""


def _row_to_stats(row: sqlite3.Row, cols: Sequence[sqlite3.Row]) -> TableStats:
    columns = {normalize_ident(c["column_name"]): str(c["data_type"] or "VARCHAR") for c in cols}
    try:
        sortkeys = tuple(json.loads(row["sortkeys_json"] or "[]"))
    except json.JSONDecodeError:
        sortkeys = ()
    if not sortkeys:
        # rebuild from column positions
        ordered = sorted(
            [(int(c["sortkey_pos"]), normalize_ident(c["column_name"])) for c in cols if int(c["sortkey_pos"] or 0) > 0],
            key=lambda x: x[0],
        )
        sortkeys = tuple(name for _, name in ordered)
    distkey = normalize_ident(row["distkey"] or "")
    if not distkey:
        for c in cols:
            if int(c["is_distkey"] or 0):
                distkey = normalize_ident(c["column_name"])
                break
    extras: dict[str, Any] = {}
    try:
        raw = row["extras_json"] if "extras_json" in row.keys() else None
        if raw:
            extras = json.loads(raw)
    except (json.JSONDecodeError, TypeError, KeyError):
        extras = {}
    partition_key = normalize_ident(extras.get("partition_key") or "")
    is_external = bool(extras.get("is_external")) or bool(partition_key)
    return TableStats(
        columns=columns,
        diststyle=str(row["diststyle"] or ""),
        distkey=distkey,
        sortkeys=sortkeys,
        rows=_float(row["tbl_rows"]),
        size_mb=_float(row["size_mb"]),
        is_external=is_external,
        object_type="external_table" if is_external else "table",
        partition_key=partition_key,
    )


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _float(value: object) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int(value: object) -> int:
    try:
        if value is None:
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "t", "true", "yes", "y"}
