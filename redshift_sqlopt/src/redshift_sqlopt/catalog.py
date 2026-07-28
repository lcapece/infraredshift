"""Table metadata: distribution keys, sort keys, size, and view definitions.

Rules consult this to decide whether a rewrite is worth claiming and whether it
is safe. Without catalog knowledge a rule can only guess — and guessing is what
this package refuses to do.

The catalog is deliberately a plain data structure with no connection of its
own. It can be built from ``SVV_TABLE_INFO`` rows fetched live, from rows the
analyzer already loaded into DuckDB, or hand-built in a test. Nothing here
imports a database driver.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlglot import exp


def normalize_ident(value: object) -> str:
    """Fold an identifier to its comparison form: unquoted, stripped, lowercase."""
    text = str(value or "").strip()
    if text.startswith('"') and text.endswith('"') and len(text) >= 2:
        text = text[1:-1]
    return text.lower()


@dataclass(frozen=True)
class TableStats:
    """What the catalog knows about one physical table.

    ``distkey`` empty with ``diststyle`` of ``EVEN`` is the classic unkeyed
    table — every join against it redistributes. ``sortkeys`` empty means no
    zone-map pruning is possible at all, so every scan is a full scan.
    """

    name: str
    schema: str = ""
    database: str = ""
    distkey: str = ""
    diststyle: str = ""
    sortkeys: tuple[str, ...] = ()
    not_null_columns: frozenset[str] = frozenset()
    """Columns known to be NOT NULL. Empty means *unknown*, never *nullable* —
    rules must treat absence as unproven and refuse accordingly."""
    row_count: int | None = None
    size_mb: float | None = None
    skew_ratio: float | None = None
    unsorted_pct: float | None = None
    stats_off_pct: float | None = None

    @property
    def key(self) -> str:
        parts = [p for p in (self.database, self.schema, self.name) if p]
        return ".".join(normalize_ident(p) for p in parts)

    @property
    def has_distkey(self) -> bool:
        return bool(self.distkey.strip())

    @property
    def has_sortkey(self) -> bool:
        return bool(self.sortkeys)

    @property
    def is_large(self) -> bool:
        """Large enough that distribution and sort choices actually matter."""
        if self.row_count is not None and self.row_count >= 1_000_000:
            return True
        return self.size_mb is not None and self.size_mb >= 1024


@dataclass(frozen=True)
class ViewDef:
    """A view's SELECT body, so it can be inlined before analysis."""

    name: str
    sql: str
    schema: str = ""
    database: str = ""
    is_late_binding: bool = False

    @property
    def key(self) -> str:
        parts = [p for p in (self.database, self.schema, self.name) if p]
        return ".".join(normalize_ident(p) for p in parts)


@dataclass
class Catalog:
    """Lookup over tables and views, tolerant of partial qualification.

    Real queries qualify names inconsistently — ``orders``, ``public.orders``,
    and ``analytics.public.orders`` may all appear for the same table. Lookups
    fall back from fully-qualified to bare name, and a bare name that matches
    more than one table is treated as unresolved rather than guessed at.
    """

    tables: dict[str, TableStats] = field(default_factory=dict)
    views: dict[str, ViewDef] = field(default_factory=dict)

    @classmethod
    def from_rows(
        cls,
        table_rows: list[dict] | None = None,
        view_rows: list[dict] | None = None,
    ) -> Catalog:
        """Build from plain dict rows (e.g. SVV_TABLE_INFO / pg_views output)."""
        catalog = cls()
        for row in table_rows or []:
            # SVV_TABLE_INFO exposes only the FIRST sort column, as sortkey1;
            # the full key is not in that view. A leading "-" marks an
            # interleaved key and is not part of the column name.
            sortkeys = (
                row.get("sortkeys")
                or row.get("sortkey")
                or row.get("sortkey1")
                or ()
            )
            if isinstance(sortkeys, str):
                sortkeys = sortkeys.lstrip("-")
            if isinstance(sortkeys, str):
                sortkeys = tuple(
                    normalize_ident(part) for part in sortkeys.split(",") if part.strip()
                )
            else:
                sortkeys = tuple(normalize_ident(part) for part in sortkeys if str(part).strip())
            not_null = row.get("not_null_columns") or row.get("not_null") or ()
            if isinstance(not_null, str):
                not_null = [part for part in not_null.split(",") if part.strip()]
            stats = TableStats(
                not_null_columns=frozenset(normalize_ident(part) for part in not_null),
                name=normalize_ident(row.get("table") or row.get("table_name") or row.get("name")),
                schema=normalize_ident(row.get("schema") or row.get("schema_name") or ""),
                database=normalize_ident(row.get("database") or row.get("database_name") or ""),
                distkey=normalize_ident(row.get("distkey") or ""),
                diststyle=str(row.get("diststyle") or "").strip().upper(),
                sortkeys=sortkeys,
                # SVV_TABLE_INFO spellings first (tbl_rows, size, skew_rows,
                # unsorted, stats_off) — those are the real column names on a
                # cluster. The friendlier aliases are kept for hand-built
                # catalogs and for the analyzer's DuckDB copy.
                row_count=_int_or_none(
                    row.get("tbl_rows")
                    if row.get("tbl_rows") is not None
                    else row.get("row_count") or row.get("estimated_visible_rows")
                ),
                # SVV_TABLE_INFO.size is in 1 MB blocks, so it is already MB.
                size_mb=_float_or_none(
                    row.get("size") if row.get("size") is not None else row.get("size_mb")
                ),
                skew_ratio=_float_or_none(
                    row.get("skew_rows")
                    if row.get("skew_rows") is not None
                    else row.get("skew_ratio")
                ),
                unsorted_pct=_float_or_none(
                    row.get("unsorted")
                    if row.get("unsorted") is not None
                    else row.get("unsorted_pct")
                ),
                stats_off_pct=_float_or_none(
                    row.get("stats_off")
                    if row.get("stats_off") is not None
                    else row.get("stats_off_pct")
                ),
            )
            if stats.name:
                catalog.tables[stats.key] = stats
        for row in view_rows or []:
            view = ViewDef(
                name=normalize_ident(row.get("view") or row.get("view_name") or row.get("name")),
                sql=str(row.get("sql") or row.get("definition") or ""),
                schema=normalize_ident(row.get("schema") or row.get("schema_name") or ""),
                database=normalize_ident(row.get("database") or row.get("database_name") or ""),
                is_late_binding=bool(row.get("is_late_binding")),
            )
            if view.name:
                catalog.views[view.key] = view
        return catalog

    # -- lookup ------------------------------------------------------------

    def _lookup(self, store: dict, identity: str) -> object | None:
        key = normalize_ident(identity)
        if not key:
            return None
        if key in store:
            return store[key]
        # Fall back to suffix match: "public.orders" should find
        # "analytics.public.orders" when unambiguous.
        matches = [value for stored, value in store.items() if stored.endswith("." + key)]
        if len(matches) == 1:
            return matches[0]
        # Bare name against the last segment.
        bare = key.split(".")[-1]
        matches = [
            value for stored, value in store.items() if stored.split(".")[-1] == bare
        ]
        return matches[0] if len(matches) == 1 else None

    def resolve_table(self, identity: str) -> TableStats | None:
        found = self._lookup(self.tables, identity)
        return found if isinstance(found, TableStats) else None

    def resolve_view(self, identity: str) -> tuple[str, ViewDef] | None:
        found = self._lookup(self.views, identity)
        if isinstance(found, ViewDef):
            return found.key, found
        return None

    def is_sortkey(self, table_identity: str, column: str) -> bool:
        stats = self.resolve_table(table_identity)
        if stats is None:
            return False
        return normalize_ident(column) in stats.sortkeys

    def is_not_null(self, table_identity: str, column: str) -> bool:
        """True only when the catalog *proves* the column is NOT NULL.

        Returns False for an unknown table or an uncatalogued column: absence of
        evidence is not evidence of non-nullability, and a rule that treats it
        as such would emit an unsound rewrite.
        """
        stats = self.resolve_table(table_identity)
        if stats is None:
            return False
        return normalize_ident(column) in stats.not_null_columns

    def is_distkey(self, table_identity: str, column: str) -> bool:
        stats = self.resolve_table(table_identity)
        if stats is None:
            return False
        return normalize_ident(column) == stats.distkey

    def table_for_column(self, column: exp.Column, tree: exp.Expression) -> str | None:
        """Resolve which table a column reference belongs to.

        Uses the column's own qualifier when present, otherwise falls back to
        the single FROM/JOIN source when the query has exactly one — with more
        than one source and no qualifier, the answer is genuinely ambiguous and
        ``None`` is the correct response.
        """
        qualifier = normalize_ident(column.table)
        sources = _table_sources(tree)
        if qualifier:
            for alias, identity in sources.items():
                if alias == qualifier or identity.split(".")[-1] == qualifier:
                    return identity
            return qualifier or None
        distinct = {identity for identity in sources.values()}
        return next(iter(distinct)) if len(distinct) == 1 else None

    def unkeyed_tables(self, identities: list[str]) -> list[TableStats]:
        """Large referenced tables with no distkey or no sortkey.

        These are DDL-tier findings: fixing the table helps every query that
        touches it, which is strictly better than rewriting queries around it.
        """
        out: list[TableStats] = []
        for identity in identities:
            stats = self.resolve_table(identity)
            if stats is None or not stats.is_large:
                continue
            if not stats.has_distkey or not stats.has_sortkey:
                out.append(stats)
        return out


def _table_sources(tree: exp.Expression) -> dict[str, str]:
    """Map alias (or bare name) -> fully qualified table identity."""
    sources: dict[str, str] = {}
    for table in tree.find_all(exp.Table):
        parts = [p for p in (table.catalog, table.db, table.name) if p]
        identity = ".".join(normalize_ident(p) for p in parts)
        alias = normalize_ident(table.alias or table.name)
        if alias:
            sources[alias] = identity
    return sources


def table_identities(tree: exp.Expression) -> list[str]:
    """Every distinct table identity referenced in the tree."""
    seen: list[str] = []
    for table in tree.find_all(exp.Table):
        parts = [p for p in (table.catalog, table.db, table.name) if p]
        identity = ".".join(normalize_ident(p) for p in parts)
        if identity and identity not in seen:
            seen.append(identity)
    return seen


def _int_or_none(value: object) -> int | None:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _float_or_none(value: object) -> float | None:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None
