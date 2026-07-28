"""Paste-in SQL analysis against captured Redshift table metadata."""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from functools import lru_cache

import pandas as pd

from ._lazy_sqlglot import exp, sqlglot
from .physical_lineage import PhysicalLineageResolver, PhysicalOrigin, format_origins
from .query_similarity import DEFAULT_SIMILARITY_THRESHOLD, analyze_sql, score_sql_similarity
from .redshift_meta import MISSING_DISTKEY_VALUES, MISSING_SORTKEY_VALUES


@lru_cache(maxsize=1)
def _predicate_types() -> tuple:
    return (
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
    )


_DISTKEY_RE = re.compile(r"\bkey\s*\(\s*([^)]+?)\s*\)", re.IGNORECASE)
_MISSING_KEY_VALUES = set(MISSING_SORTKEY_VALUES) | set(MISSING_DISTKEY_VALUES)


@dataclass
class SQLLensAnalysis:
    parse_ok: bool
    parse_error: str = ""
    normalized_sql: str = ""
    summary: dict = field(default_factory=dict)
    tables: pd.DataFrame = field(default_factory=pd.DataFrame)
    joins: pd.DataFrame = field(default_factory=pd.DataFrame)
    predicates: pd.DataFrame = field(default_factory=pd.DataFrame)
    diagram_nodes: pd.DataFrame = field(default_factory=pd.DataFrame)
    diagram_edges: pd.DataFrame = field(default_factory=pd.DataFrame)
    findings: pd.DataFrame = field(default_factory=pd.DataFrame)
    repeat_matches: pd.DataFrame = field(default_factory=pd.DataFrame)
    first_steps: pd.DataFrame = field(default_factory=pd.DataFrame)


def analyze_console_sql(
    sql_text: object,
    table_review: pd.DataFrame | None,
    known_queries: pd.DataFrame | None = None,
    view_definitions: pd.DataFrame | None = None,
) -> SQLLensAnalysis:
    """Analyze pasted Redshift SQL against the known table-review snapshot."""
    sql = "" if sql_text is None else str(sql_text).strip()
    if not sql:
        first_steps = pd.DataFrame(
            [
                _first_step(
                    1,
                    "query",
                    "info",
                    "Paste SQL",
                    "No statement has been analyzed yet.",
                    "Paste the SQL from the AWS console and click Analyze SQL.",
                )
            ]
        )
        return SQLLensAnalysis(
            parse_ok=False,
            parse_error="Paste a SQL statement first.",
            summary=_summary(False, 0, 0, 0, 0, 0),
            first_steps=first_steps,
        )

    table_index = _TableIndex(table_review)
    view_index = _ViewIndex(view_definitions)
    intel = analyze_sql(sql)
    try:
        tree = sqlglot.parse_one(sql, read="redshift")
    except Exception as exc:
        findings = pd.DataFrame(
            [
                _finding(
                    "crit",
                    "SQL did not parse",
                    "statement",
                    str(exc).splitlines()[0][:240],
                )
            ]
        )
        first_steps = _build_first_steps(
            sql=sql,
            intel=intel,
            table_rows=pd.DataFrame(),
            join_rows=pd.DataFrame(),
            predicate_rows=pd.DataFrame(),
            findings=findings,
            repeat_matches=pd.DataFrame(),
        )
        return SQLLensAnalysis(
            parse_ok=False,
            parse_error=str(exc).splitlines()[0][:240],
            normalized_sql=intel.normalized_sql,
            summary=_summary(False, 0, 0, 0, 1, 0),
            findings=findings,
            first_steps=first_steps,
        )

    findings: list[dict] = []
    cte_names = {_clean(cte.alias_or_name) for cte in tree.find_all(exp.CTE) if cte.alias_or_name}
    table_refs = _extract_table_refs(tree, cte_names, table_index, view_index)
    table_refs.extend(_expand_view_components(table_refs, table_index, view_index, findings))
    alias_lookup = _alias_lookup([ref for ref in table_refs if not ref.get("is_view_component")])
    physical_lineage = PhysicalLineageResolver(tree, view_definitions)
    view_component_refs = [ref for ref in table_refs if ref.get("is_view_component")]
    for ref in table_refs:
        if ref.get("match_status") == "view":
            components = [c.get("query_table") for c in view_component_refs if c.get("component_of") == ref.get("alias")]
            findings.append(
                _finding(
                    "info",
                    "View reference expanded",
                    ref.get("query_table") or ref.get("alias") or "view",
                    (
                        f"View definition resolves to {len(components)} component table(s): "
                        + ", ".join(str(c) for c in components[:8])
                    ),
                )
            )
    join_rows, join_aliases = _extract_joins(
        tree, alias_lookup, findings, physical_lineage, table_index
    )
    predicate_rows, predicate_aliases, implicit_join_aliases = _extract_predicates(
        tree, alias_lookup, findings, physical_lineage, table_index
    )
    table_rows = _table_rows(table_refs, join_aliases | implicit_join_aliases, predicate_aliases, findings)
    finding_rows = _dedupe_findings(findings)
    repeat_matches = _repeat_matches(sql, known_queries)
    first_steps = _build_first_steps(sql, intel, table_rows, join_rows, predicate_rows, finding_rows, repeat_matches)
    diagram_nodes, diagram_edges = _build_diagram(table_rows, join_rows, predicate_rows, first_steps, repeat_matches)

    parsed_summary = _summary(
        True,
        len(table_rows),
        len(join_rows),
        len(predicate_rows),
        int((finding_rows["severity"] == "crit").sum()) if not finding_rows.empty else 0,
        int((finding_rows["severity"] == "warn").sum()) if not finding_rows.empty else 0,
    )
    parsed_summary.update(
        {
            "sql_parse_status": "parsed" if intel.parse_ok else "fallback",
            "sql_table_count": len(intel.tables),
            "sql_join_count": intel.join_count,
            "sql_predicate_count": intel.predicate_count,
            "sql_filter_columns": ", ".join(sorted(intel.filter_columns)),
            "sql_join_columns": ", ".join(sorted(intel.join_columns)),
            "repeat_match_count": len(repeat_matches),
            "best_similarity": (
                float(repeat_matches["similarity_score"].max()) if not repeat_matches.empty else 0.0
            ),
            "repeatable_query": (
                "yes"
                if not repeat_matches.empty
                and float(repeat_matches["similarity_score"].max()) >= DEFAULT_SIMILARITY_THRESHOLD
                else "no"
            ),
            "repeat_runtime_s": (
                float(repeat_matches["elapsed_s"].fillna(0).sum())
                if not repeat_matches.empty and "elapsed_s" in repeat_matches.columns
                else 0.0
            ),
            "first_step": (
                str(first_steps.iloc[0].get("category"))
                if not first_steps.empty
                else "review"
            ),
            "diagram_node_count": len(diagram_nodes),
            "diagram_edge_count": len(diagram_edges),
        }
    )
    return SQLLensAnalysis(
        parse_ok=True,
        normalized_sql=intel.normalized_sql,
        summary=parsed_summary,
        tables=table_rows,
        joins=join_rows,
        predicates=predicate_rows,
        diagram_nodes=diagram_nodes,
        diagram_edges=diagram_edges,
        findings=finding_rows,
        repeat_matches=repeat_matches,
        first_steps=first_steps,
    )


class _TableIndex:
    def __init__(self, table_review: pd.DataFrame | None):
        self._rows: list[dict] = []
        self._exact: dict[str, dict] = {}
        self._schema_table: dict[str, list[dict]] = {}
        self._table_only: dict[str, list[dict]] = {}
        if table_review is None or table_review.empty:
            return
        for _, row in table_review.iterrows():
            item = row.to_dict()
            source_db = _clean(item.get("source_db"))
            schema = _clean(item.get("schema_name"))
            table = _clean(item.get("table_name"))
            key = _clean(item.get("table_key")) or ".".join(part for part in (source_db, schema, table) if part)
            item["_table_key"] = key
            item["_schema_table_key"] = ".".join(part for part in (schema, table) if part)
            item["_table_only_key"] = table
            item["_distkey"] = _distkey(item.get("diststyle"))
            item["_sortkey"] = _sortkey(item.get("sortkey1"))
            self._rows.append(item)
            if key:
                self._exact[key] = item
            if item["_schema_table_key"]:
                self._schema_table.setdefault(item["_schema_table_key"], []).append(item)
            if table:
                self._table_only.setdefault(table, []).append(item)

    def match(self, catalog: str, schema: str, table: str) -> tuple[str, dict | None]:
        catalog = _clean(catalog)
        schema = _clean(schema)
        table = _clean(table)
        exact_key = ".".join(part for part in (catalog, schema, table) if part)
        if exact_key and exact_key in self._exact:
            return "matched", self._exact[exact_key]

        schema_key = ".".join(part for part in (schema, table) if part)
        if schema_key:
            matches = self._schema_table.get(schema_key, [])
            if len(matches) == 1:
                return "matched", matches[0]
            if len(matches) > 1:
                return "ambiguous", max(matches, key=lambda row: _num(row.get("table_attention_score")))

        matches = self._table_only.get(table, [])
        if len(matches) == 1:
            return "matched", matches[0]
        if len(matches) > 1:
            return "ambiguous", max(matches, key=lambda row: _num(row.get("table_attention_score")))
        return "not found", None


class _ViewIndex:
    def __init__(self, view_definitions: pd.DataFrame | None):
        self._exact: dict[str, dict] = {}
        self._schema_view: dict[str, list[dict]] = {}
        self._view_only: dict[str, list[dict]] = {}
        if view_definitions is None or view_definitions.empty:
            return
        for _, row in view_definitions.iterrows():
            item = row.to_dict()
            database = _clean(item.get("database") or item.get("database_name") or item.get("source_db"))
            schema = _clean(item.get("schema") or item.get("schema_name"))
            view_name = _clean(item.get("view_name") or item.get("table_name"))
            definition = item.get("source_definition") or item.get("view_definition") or ""
            item["_database"] = database
            item["_schema"] = schema
            item["_view_name"] = view_name
            item["_definition"] = str(definition or "")
            exact_key = ".".join(part for part in (database, schema, view_name) if part)
            schema_key = ".".join(part for part in (schema, view_name) if part)
            if exact_key:
                self._exact[exact_key] = item
            if schema_key:
                self._schema_view.setdefault(schema_key, []).append(item)
            if view_name:
                self._view_only.setdefault(view_name, []).append(item)

    def match(self, catalog: str, schema: str, view_name: str) -> tuple[str, dict | None]:
        catalog = _clean(catalog)
        schema = _clean(schema)
        view_name = _clean(view_name)
        exact_key = ".".join(part for part in (catalog, schema, view_name) if part)
        if exact_key and exact_key in self._exact:
            return "view", self._exact[exact_key]
        schema_key = ".".join(part for part in (schema, view_name) if part)
        if schema_key:
            matches = self._schema_view.get(schema_key, [])
            if len(matches) == 1:
                return "view", matches[0]
            if len(matches) > 1:
                return "ambiguous view", matches[0]
        matches = self._view_only.get(view_name, [])
        if len(matches) == 1:
            return "view", matches[0]
        if len(matches) > 1:
            return "ambiguous view", matches[0]
        return "not found", None


def _extract_table_refs(
    tree: exp.Expression,
    cte_names: set[str],
    table_index: _TableIndex,
    view_index: _ViewIndex,
) -> list[dict]:
    refs: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for table_expr in tree.find_all(exp.Table):
        name = _clean(table_expr.name)
        schema = _clean(table_expr.db)
        catalog = _clean(table_expr.catalog)
        alias = _clean(table_expr.alias_or_name or name)
        full_name = ".".join(part for part in (catalog, schema, name) if part)
        key = (alias, full_name)
        if key in seen:
            continue
        seen.add(key)
        is_cte = name in cte_names and not schema and not catalog
        if is_cte:
            status, meta = "cte", None
        else:
            status, meta = view_index.match(catalog, schema, name)
            if status == "not found":
                status, meta = table_index.match(catalog, schema, name)
        refs.append(
            {
                "alias": alias,
                "query_table": full_name or name,
                "catalog": catalog,
                "schema": schema,
                "name": name,
                "match_status": status,
                "meta": meta,
                "is_cte": is_cte,
                "is_view_component": False,
                "component_of": "",
            }
        )
    return refs


def _expand_view_components(
    table_refs: list[dict],
    table_index: _TableIndex,
    view_index: _ViewIndex,
    findings: list[dict],
) -> list[dict]:
    components: list[dict] = []
    seen: set[tuple[str, str, str]] = set()

    def expand_one(ref: dict, parent_alias: str, depth: int, lineage: tuple[str, ...]) -> None:
        if ref.get("match_status") not in {"view", "ambiguous view"}:
            return
        view_meta = ref.get("meta") or {}
        definition = str(view_meta.get("_definition") or "")
        if not definition.strip():
            findings.append(
                _finding(
                    "warn",
                    "View definition missing",
                    ref.get("query_table") or ref.get("alias") or "view",
                    "Refresh view definitions; this view cannot be expanded into component tables.",
                )
            )
            return
        if depth > 5:
            findings.append(
                _finding(
                    "warn",
                    "View expansion depth limit reached",
                    ref.get("query_table") or ref.get("alias") or "view",
                    "Nested views exceeded five expansion levels; inspect this view chain manually.",
                )
            )
            return
        try:
            view_tree = sqlglot.parse_one(definition, read="redshift")
        except Exception as exc:
            findings.append(
                _finding(
                    "warn",
                    "View definition did not parse",
                    ref.get("query_table") or ref.get("alias") or "view",
                    str(exc).splitlines()[0][:240],
                )
            )
            return
        cte_names = {_clean(cte.alias_or_name) for cte in view_tree.find_all(exp.CTE) if cte.alias_or_name}
        view_identity = ".".join(
            part
            for part in (
                _clean(view_meta.get("_database")),
                _clean(view_meta.get("_schema")),
                _clean(view_meta.get("_view_name")),
            )
            if part
        )
        for table_expr in view_tree.find_all(exp.Table):
            name = _clean(table_expr.name)
            raw_schema = _clean(table_expr.db)
            schema = raw_schema or _clean(view_meta.get("_schema"))
            catalog = _clean(table_expr.catalog) or _clean(view_meta.get("_database"))
            alias = _clean(table_expr.alias_or_name or name)
            full_name = ".".join(part for part in (catalog, schema, name) if part)
            if name in cte_names and not raw_schema:
                continue
            if full_name == view_identity or ".".join(part for part in (schema, name) if part) == ".".join(view_identity.split(".")[-2:]):
                continue
            lineage_key = full_name or name
            if lineage_key in lineage:
                findings.append(
                    _finding(
                        "warn",
                        "View expansion cycle skipped",
                        ref.get("query_table") or ref.get("alias") or "view",
                        f"Skipped recursive reference to {lineage_key}.",
                    )
                )
                continue
            key = (parent_alias, alias, lineage_key)
            if key in seen:
                continue
            seen.add(key)
            status, meta = view_index.match(catalog, schema, name)
            if status == "not found":
                status, meta = table_index.match(catalog, schema, name)
            component_alias = f"{parent_alias}.{alias}" if alias else parent_alias
            component = {
                "alias": component_alias,
                "query_table": full_name or name,
                "catalog": catalog,
                "schema": schema,
                "name": name,
                "match_status": status,
                "meta": meta,
                "is_cte": False,
                "is_view_component": True,
                "component_of": parent_alias,
                "view_depth": depth,
            }
            components.append(component)
            if status in {"view", "ambiguous view"}:
                expand_one(component, component_alias, depth + 1, lineage + (lineage_key,))

    for ref in table_refs:
        parent_alias = _clean(ref.get("alias")) or _clean(ref.get("name")) or "view"
        expand_one(ref, parent_alias, 1, tuple())
    return components


def _alias_lookup(table_refs: list[dict]) -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    for ref in table_refs:
        for key in (ref.get("alias"), ref.get("name"), ref.get("query_table")):
            clean_key = _clean(key)
            if clean_key and clean_key not in lookup:
                lookup[clean_key] = ref
    return lookup


def _extract_joins(
    tree: exp.Expression,
    alias_lookup: dict[str, dict],
    findings: list[dict],
    physical_lineage: PhysicalLineageResolver | None = None,
    table_index: _TableIndex | None = None,
) -> tuple[pd.DataFrame, set[str]]:
    rows: list[dict] = []
    aliases_in_joins: set[str] = set()
    for join_no, join in enumerate(tree.find_all(exp.Join), start=1):
        target = _join_target(join)
        target_alias = _clean(target.get("alias"))
        on_expr = join.args.get("on")
        using_expr = join.args.get("using")
        columns = _columns_in(on_expr) + _columns_in(using_expr)
        resolved = [_resolve_column(col, alias_lookup) for col in columns]
        resolved = [item for item in resolved if item[0]]
        aliases = {alias for alias, _ in resolved}
        if target_alias:
            aliases.add(target_alias)
        aliases_in_joins.update(aliases)
        pairs = _join_column_pairs(on_expr)
        alignment, severity, recommendation = _join_alignment(aliases, resolved, alias_lookup)
        physical_pairs: list[str] = []
        left_origins: list[PhysicalOrigin] = []
        right_origins: list[PhysicalOrigin] = []
        if physical_lineage is not None and on_expr is not None:
            equalities = list(on_expr.find_all(exp.EQ))
            if isinstance(on_expr, exp.EQ) and on_expr not in equalities:
                equalities.insert(0, on_expr)
            for equality in equalities:
                pair_left = physical_lineage.origins_for_expression(equality.left)
                pair_right = physical_lineage.origins_for_expression(equality.right)
                left_origins.extend(pair_left)
                right_origins.extend(pair_right)
                if pair_left or pair_right:
                    physical_pairs.append(
                        f"{format_origins(pair_left) or 'UNRESOLVED'} = "
                        f"{format_origins(pair_right) or 'UNRESOLVED'}"
                    )
        left_origins = _dedupe_physical_origins(left_origins)
        right_origins = _dedupe_physical_origins(right_origins)
        if left_origins or right_origins:
            alignment, severity, recommendation = _physical_join_alignment(
                left_origins, right_origins, table_index
            )
        if on_expr is None and using_expr is None:
            severity = "crit"
            alignment = "no join condition"
            recommendation = "Add an ON/USING clause or verify this is an intentional cross join."
        join_type = _join_type(join)
        join_signal, visual_status = _join_visual_signal(
            join_type, severity, aliases, resolved, alias_lookup
        )
        if severity in {"crit", "warn"}:
            findings.append(
                _finding(
                    severity,
                    f"Join {join_no}: {alignment}",
                    ", ".join(_alias_subjects(aliases, alias_lookup)) or target.get("label", "join"),
                    recommendation,
                )
            )
        rows.append(
            {
                "join_no": join_no,
                "join_type": join_type,
                "join_signal": join_signal,
                "visual_status": visual_status,
                "target_table": target.get("label", ""),
                "target_alias": target_alias,
                "involved_tables": ", ".join(_alias_subjects(aliases, alias_lookup)),
                "aliases": ", ".join(sorted(aliases)),
                "join_columns": ", ".join(f"{alias}.{col}" for alias, col in resolved),
                "column_pairs": "; ".join(pairs),
                "left_physical_sources": format_origins(left_origins),
                "right_physical_sources": format_origins(right_origins),
                "physical_column_pairs": "; ".join(physical_pairs),
                "physical_lineage_status": (
                    "resolved"
                    if left_origins and right_origins
                    else "partially resolved"
                    if left_origins or right_origins
                    else "unresolved"
                ),
                "condition": _sql_snippet(on_expr or using_expr),
                "distribution_alignment": alignment,
                "severity": severity,
                "recommendation": recommendation,
            }
        )
    return pd.DataFrame(rows), aliases_in_joins


def _extract_predicates(
    tree: exp.Expression,
    alias_lookup: dict[str, dict],
    findings: list[dict],
    physical_lineage: PhysicalLineageResolver | None = None,
    table_index: _TableIndex | None = None,
) -> tuple[pd.DataFrame, set[str], set[str]]:
    rows: list[dict] = []
    aliases_in_predicates: set[str] = set()
    aliases_in_implicit_joins: set[str] = set()
    clauses: list[tuple[str, exp.Expression]] = []
    for clause_name, clause_type in (
        ("WHERE", exp.Where),
        ("HAVING", getattr(exp, "Having", exp.Where)),
        ("QUALIFY", getattr(exp, "Qualify", exp.Where)),
    ):
        for clause in tree.find_all(clause_type):
            if clause_name == "WHERE" and not isinstance(clause, exp.Where):
                continue
            clauses.append((clause_name, clause))

    predicate_no = 0
    for clause_name, clause in clauses:
        predicates = list(clause.find_all(*_predicate_types()))
        if not predicates:
            predicates = [clause]
        for predicate in predicates:
            columns = _columns_in(predicate)
            resolved = [_resolve_column(col, alias_lookup) for col in columns]
            resolved = [item for item in resolved if item[0]]
            aliases = {alias for alias, _ in resolved}
            aliases_in_predicates.update(aliases)
            implicit_join = _is_implicit_join_predicate(predicate, resolved)
            if implicit_join:
                aliases_in_implicit_joins.update(aliases)
                alignment, severity, recommendation = _join_alignment(aliases, resolved, alias_lookup)
                alignment = f"implicit join equality: {alignment}"
            else:
                alignment, severity, recommendation = _predicate_alignment(aliases, resolved, alias_lookup)
            physical_origins = (
                physical_lineage.origins_for_expression(predicate)
                if physical_lineage is not None
                else []
            )
            if physical_origins and not implicit_join:
                alignment, severity, recommendation = _physical_predicate_alignment(
                    physical_origins, table_index
                )
            predicate_signal, visual_status = _predicate_visual_signal(severity, alignment, implicit_join)
            predicate_no += 1
            if severity in {"crit", "warn"}:
                findings.append(
                    _finding(
                        severity,
                        f"{clause_name} {'equality join' if implicit_join else 'predicate'}: {alignment}",
                        ", ".join(_alias_subjects(aliases, alias_lookup)) or "statement",
                        recommendation,
                    )
                )
            rows.append(
                {
                    "predicate_no": predicate_no,
                    "clause": clause_name,
                    "predicate_type": type(predicate).__name__,
                    "predicate_role": "implicit join" if implicit_join else "filter",
                    "predicate_signal": predicate_signal,
                    "visual_status": visual_status,
                    "involved_tables": ", ".join(_alias_subjects(aliases, alias_lookup)),
                    "aliases": ", ".join(sorted(aliases)),
                    "columns": ", ".join(f"{alias}.{col}" for alias, col in resolved),
                    "physical_sources": format_origins(physical_origins),
                    "physical_lineage_status": "resolved" if physical_origins else "unresolved",
                    "sortkey_alignment": alignment,
                    "severity": severity,
                    "condition": _sql_snippet(predicate),
                    "recommendation": recommendation,
                }
            )
    return pd.DataFrame(rows), aliases_in_predicates, aliases_in_implicit_joins


def _dedupe_physical_origins(origins: list[PhysicalOrigin]) -> list[PhysicalOrigin]:
    seen: set[tuple[str, str, str, str]] = set()
    output: list[PhysicalOrigin] = []
    for origin in origins:
        key = (origin.database, origin.schema, origin.table, origin.column)
        if key not in seen:
            seen.add(key)
            output.append(origin)
    return output


def _origin_meta(origin: PhysicalOrigin, table_index: _TableIndex | None) -> dict | None:
    if table_index is None:
        return None
    _status, meta = table_index.match(origin.database, origin.schema, origin.table)
    return meta


def _physical_join_alignment(
    left: list[PhysicalOrigin],
    right: list[PhysicalOrigin],
    table_index: _TableIndex | None,
) -> tuple[str, str, str]:
    if not left or not right:
        return (
            "physical lineage partially resolved",
            "info",
            "One join side could not be traced to a physical table column; no table-design judgment was made.",
        )
    left_pairs = [(origin, _origin_meta(origin, table_index)) for origin in left]
    right_pairs = [(origin, _origin_meta(origin, table_index)) for origin in right]
    metas = [meta for _origin, meta in left_pairs + right_pairs if meta]
    if len(metas) < 2:
        return (
            "physical tables resolved; catalog metadata incomplete",
            "info",
            "Physical sources are known, but Table Review lacks metadata for one or both tables.",
        )
    if any(_is_diststyle_all(meta.get("diststyle")) for meta in metas):
        return (
            "co-located by physical DISTSTYLE ALL input",
            "ok",
            "A resolved physical input is replicated; confirm the plan does not show an unexpected movement node.",
        )
    left_hits = [
        origin for origin, meta in left_pairs
        if meta and _same_column(origin.column, meta.get("_distkey"))
    ]
    right_hits = [
        origin for origin, meta in right_pairs
        if meta and _same_column(origin.column, meta.get("_distkey"))
    ]
    if left_hits and right_hits:
        return (
            "physical join columns align with both DISTKEYs",
            "ok",
            "Both physical join sides use their captured distribution keys; verify the actual plan node is co-located.",
        )
    large_misses = sum(
        1 for origin, meta in left_pairs + right_pairs
        if meta and _is_large_table(meta) and not _same_column(origin.column, meta.get("_distkey"))
    )
    if large_misses >= 2:
        return (
            "physical large-table join columns miss DISTKEYs",
            "crit",
            "Use captured plan/detail evidence to identify redistribution, then consider aligned staging or pre-aggregation.",
        )
    if large_misses == 1:
        return (
            "one physical large-table join column misses its DISTKEY",
            "warn",
            "Check the mapped plan node for broadcast or redistribution before changing physical design.",
        )
    return (
        "physical sources resolved; distribution benefit undetermined",
        "info",
        "The physical tables are known, but their captured keys do not prove a co-located join.",
    )


def _physical_predicate_alignment(
    origins: list[PhysicalOrigin],
    table_index: _TableIndex | None,
) -> tuple[str, str, str]:
    matches = [(origin, _origin_meta(origin, table_index)) for origin in origins]
    known = [(origin, meta) for origin, meta in matches if meta]
    if not known:
        return (
            "physical column resolved; catalog metadata incomplete",
            "info",
            "The source column is known, but its table is missing from Table Review.",
        )
    if any(_same_column(origin.column, meta.get("_sortkey")) for origin, meta in known):
        return (
            "physical predicate column uses a leading sort key",
            "ok",
            "The physical source column aligns with a captured leading sort key; confirm RRSCAN in execution detail.",
        )
    if any(_num(meta.get("stats_off")) >= 20 for _origin, meta in known):
        return (
            "physical predicate touches a stale-stat table",
            "warn",
            "Run ANALYZE before trusting plan selectivity for this physical source.",
        )
    return (
        "physical predicate does not match a captured leading sort key",
        "info",
        "Use the mapped scan plan/detail node to judge actual pruning and rows removed.",
    )


def _table_rows(
    table_refs: list[dict],
    join_aliases: set[str],
    predicate_aliases: set[str],
    findings: list[dict],
) -> pd.DataFrame:
    rows: list[dict] = []
    for ref in table_refs:
        alias = _clean(ref.get("alias"))
        status = str(ref.get("match_status") or "")
        meta = ref.get("meta") or {}
        if status in {"not found", "ambiguous", "ambiguous view"}:
            findings.append(
                _finding(
                    "warn",
                    f"Table metadata {status}",
                    ref.get("query_table") or alias,
                    "Load or refresh SVV_TABLE_INFO for this database, or qualify the table name if it is ambiguous.",
                )
            )
        roles = []
        if ref.get("is_view_component"):
            roles.append("view_component")
        elif status in {"view", "ambiguous view"}:
            roles.append("view")
        if alias in join_aliases:
            roles.append("join")
        if alias in predicate_aliases:
            roles.append("filter")
        if not roles:
            roles.append("scan")
        recommendation = _table_recommendation(ref, meta, roles, findings)
        rows.append(
            {
                "query_table": ref.get("query_table") or "",
                "alias": alias,
                "object_type": _object_type(ref),
                "component_of": ref.get("component_of") or "",
                "role": "/".join(roles),
                "match_status": status,
                "view_depth": ref.get("view_depth", 0),
                "statement_table_score": _statement_table_score(meta, status),
                "source_db": meta.get("source_db", meta.get("_database", "")),
                "schema_name": meta.get("schema_name", meta.get("_schema", ref.get("schema") or "")),
                "table_name": meta.get("table_name", meta.get("_view_name", ref.get("name") or "")),
                "diststyle": meta.get("diststyle", ""),
                "distkey": meta.get("_distkey", ""),
                "sortkey1": meta.get("sortkey1", ""),
                "size_mb": meta.get("size_mb", 0),
                "tbl_rows": meta.get("tbl_rows", 0),
                "stats_off": meta.get("stats_off", 0),
                "unsorted_pct": meta.get("unsorted_pct", 0),
                "table_attention_score": meta.get("table_attention_score", 0),
                "full_scan_score": meta.get("full_scan_score", 0),
                "distribution_usage_score": meta.get("distribution_usage_score", 0),
                "sort_key_usage_score": meta.get("sort_key_usage_score", 0),
                "sort_attention_score": meta.get("sort_attention_score", 0),
                "scan_query_count": meta.get("scan_query_count", 0),
                "full_scan_query_pct": meta.get("full_scan_query_pct", 0),
                "rrscan_query_pct": meta.get("rrscan_query_pct", 0),
                "source_definition": _definition_snippet(meta.get("_definition", "")),
                "source_definition_full": str(meta.get("_definition", "") or ""),
                "recommendation": recommendation,
            }
        )
    return pd.DataFrame(rows)


def _build_diagram(
    table_rows: pd.DataFrame,
    join_rows: pd.DataFrame,
    predicate_rows: pd.DataFrame,
    first_steps: pd.DataFrame,
    repeat_matches: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    nodes: list[dict] = []
    edges: list[dict] = []
    node_ids: set[str] = set()
    edge_ids: set[tuple[str, str, str]] = set()
    table_ids: dict[str, str] = {}

    def add_node(
        node_id: str,
        group: str,
        kind: str,
        label: str,
        detail: str = "",
        severity: str = "info",
        score: float = 0.0,
        alias: str = "",
    ) -> None:
        if not node_id or node_id in node_ids:
            return
        node_ids.add(node_id)
        nodes.append(
            {
                "node_id": node_id,
                "group": group,
                "kind": kind,
                "label": label,
                "detail": detail,
                "severity": severity,
                "score": round(float(score or 0), 2),
                "alias": alias,
            }
        )

    def add_edge(
        source: str,
        target: str,
        label: str,
        edge_type: str,
        severity: str = "info",
        weight: float = 1.0,
    ) -> None:
        if not source or not target or source == target:
            return
        key = (source, target, edge_type)
        if key in edge_ids:
            return
        edge_ids.add(key)
        edges.append(
            {
                "source": source,
                "target": target,
                "label": label,
                "edge_type": edge_type,
                "severity": severity,
                "weight": round(float(weight or 1), 2),
            }
        )

    relation_count = 0 if table_rows is None or table_rows.empty else len(table_rows)
    join_count = 0 if join_rows is None or join_rows.empty else len(join_rows)
    predicate_count = 0 if predicate_rows is None or predicate_rows.empty else len(predicate_rows)
    add_node(
        "sql:statement",
        "statement",
        "statement",
        "Pasted SQL",
        f"{relation_count} object(s), {join_count} join(s), {predicate_count} predicate(s)",
        "info",
        0,
    )

    if table_rows is not None and not table_rows.empty:
        for idx, row in table_rows.iterrows():
            alias = _clean(row.get("alias")) or _clean(row.get("query_table")) or f"table_{idx}"
            node_id = f"table:{alias}"
            table_ids[alias] = node_id
            query_table = str(row.get("query_table") or row.get("table_name") or alias)
            object_type = str(row.get("object_type") or "table")
            score = _num(row.get("statement_table_score"))
            detail_parts = [
                str(row.get("role") or "").strip(),
                str(row.get("match_status") or "").strip(),
            ]
            if row.get("component_of"):
                detail_parts.append(f"inside view {row.get('component_of')}")
            if score:
                detail_parts.append(f"score {score:.0f}")
            detail = " | ".join(part for part in detail_parts if part)
            label = _diagram_table_label(row, alias, query_table)
            add_node(
                node_id,
                "relation",
                object_type,
                label,
                detail,
                _diagram_severity(score, object_type),
                score,
                alias,
            )

        for _, row in table_rows.iterrows():
            alias = _clean(row.get("alias")) or _clean(row.get("query_table"))
            node_id = table_ids.get(alias)
            if not node_id:
                continue
            component_of = _clean(row.get("component_of"))
            if component_of and component_of in table_ids:
                add_edge(table_ids[component_of], node_id, "expands to", "view_expansion", "info", 1.4)
            else:
                add_edge("sql:statement", node_id, "reads", "read", "info", 1.0)

    operation_ids: list[str] = []
    if join_rows is not None and not join_rows.empty:
        for _, row in join_rows.iterrows():
            join_no = int(_num(row.get("join_no")) or len(operation_ids) + 1)
            node_id = f"join:{join_no}"
            operation_ids.append(node_id)
            severity = str(row.get("severity") or "info")
            label = f"{str(row.get('join_type') or 'join').upper()} #{join_no}"
            detail = str(row.get("distribution_alignment") or row.get("condition") or "")
            add_node(node_id, "operation", "join", label, detail, severity, _severity_score(severity))
            aliases = _split_aliases(row.get("aliases")) or _aliases_from_columns(row.get("join_columns"))
            for alias in aliases:
                source = table_ids.get(alias)
                if source:
                    add_edge(source, node_id, "joins", "join", severity, 1.2)

    if predicate_rows is not None and not predicate_rows.empty:
        for _, row in predicate_rows.iterrows():
            predicate_no = int(_num(row.get("predicate_no")) or len(operation_ids) + 1)
            node_id = f"predicate:{predicate_no}"
            operation_ids.append(node_id)
            severity = str(row.get("severity") or "info")
            label = _predicate_diagram_label(row.get("condition"), row.get("clause"), predicate_no)
            detail = str(row.get("sortkey_alignment") or row.get("condition") or "")
            add_node(node_id, "operation", "predicate", label, detail, severity, _severity_score(severity))
            aliases = _split_aliases(row.get("aliases")) or _aliases_from_columns(row.get("columns"))
            for alias in aliases:
                source = table_ids.get(alias)
                if source:
                    add_edge(source, node_id, "filters", "predicate", severity, 1.1)

    if repeat_matches is not None and not repeat_matches.empty:
        scores = (
            pd.to_numeric(repeat_matches["similarity_score"], errors="coerce").fillna(0.0)
            if "similarity_score" in repeat_matches.columns
            else pd.Series(dtype="float64")
        )
        best = float(scores.max()) if not scores.empty else 0.0
        repeat_node = "intel:repeat"
        add_node(
            repeat_node,
            "intelligence",
            "repeat",
            "Repeat workload",
            f"{len(repeat_matches)} match(es), best {best * 100:.0f}%",
            "warn" if best >= DEFAULT_SIMILARITY_THRESHOLD else "info",
            best * 100,
        )
        add_edge("sql:statement", repeat_node, "similar to", "similarity", "warn", 1.6)

    if first_steps is not None and not first_steps.empty:
        for _, row in first_steps.head(3).iterrows():
            rank = int(_num(row.get("rank")) or 0)
            node_id = f"advice:{rank}"
            severity = str(row.get("severity") or "info")
            add_node(
                node_id,
                "advice",
                "advice",
                str(row.get("title") or "Next step"),
                str(row.get("next_step") or row.get("why") or ""),
                severity,
                _severity_score(severity),
            )
            for source in _advice_sources(str(row.get("category") or ""), table_rows, table_ids, operation_ids):
                add_edge(source, node_id, "drives", "advice", severity, 1.0)

    node_columns = ["node_id", "group", "kind", "label", "detail", "severity", "score", "alias"]
    edge_columns = ["source", "target", "label", "edge_type", "severity", "weight"]
    return pd.DataFrame(nodes, columns=node_columns), pd.DataFrame(edges, columns=edge_columns)


def _diagram_table_label(row: pd.Series, alias: str, query_table: str) -> str:
    object_type = str(row.get("object_type") or "table")
    if object_type in {"view", "view_component_view"}:
        return f"VIEW {alias}" if alias and alias != query_table else f"VIEW {query_table}"
    if object_type == "view_component_table":
        return str(row.get("table_name") or query_table or alias)
    if object_type == "cte":
        return f"CTE {alias or query_table}"
    return str(row.get("table_name") or query_table or alias)


def _predicate_diagram_label(condition: object, clause: object, predicate_no: object) -> str:
    text = re.sub(r"\s+", " ", str(condition or "")).strip()
    if text:
        return text
    return f"{str(clause or 'filter').upper()} #{int(_num(predicate_no) or 0)}"


def _diagram_severity(score: float, object_type: str) -> str:
    if score >= 85:
        return "crit"
    if score >= 55:
        return "warn"
    if object_type in {"view", "view_component_table", "cte"}:
        return "info"
    return "ok"


def _severity_score(severity: object) -> float:
    return {"crit": 95.0, "warn": 70.0, "info": 35.0, "ok": 12.0}.get(str(severity or "").lower(), 25.0)


def _split_aliases(value: object) -> list[str]:
    return [_clean(part) for part in str(value or "").split(",") if _clean(part)]


def _aliases_from_columns(value: object) -> list[str]:
    aliases: list[str] = []
    for part in str(value or "").split(","):
        token = part.strip()
        if "." not in token:
            continue
        alias = _clean(token.split(".", 1)[0])
        if alias and alias not in aliases:
            aliases.append(alias)
    return aliases


def _advice_sources(
    category: str,
    table_rows: pd.DataFrame,
    table_ids: dict[str, str],
    operation_ids: list[str],
) -> list[str]:
    category = _clean(category)
    if category == "table" and table_rows is not None and not table_rows.empty:
        scored = table_rows.copy()
        if "statement_table_score" in scored.columns:
            scored["_score"] = pd.to_numeric(scored["statement_table_score"], errors="coerce").fillna(0)
        else:
            scored["_score"] = pd.Series(0.0, index=scored.index)
        sources = []
        for _, row in scored.sort_values("_score", ascending=False).head(3).iterrows():
            alias = _clean(row.get("alias")) or _clean(row.get("query_table"))
            if alias in table_ids:
                sources.append(table_ids[alias])
        return sources or ["sql:statement"]
    if category == "query" and operation_ids:
        return operation_ids[:3]
    return ["sql:statement"]


def _table_recommendation(ref: dict, meta: dict, roles: list[str], findings: list[dict]) -> str:
    status = str(ref.get("match_status") or "")
    if status == "cte":
        return "CTE or derived relation; inspect the underlying tables listed separately."
    if status in {"view", "ambiguous view"}:
        return "View reference; inspect the component table rows expanded from the stored view definition."
    if status != "matched":
        return "Metadata unavailable; refresh table info before making physical design decisions."

    subject = _subject_from_meta(meta)
    issues = []
    if _num(meta.get("stats_off")) >= 20:
        issues.append("stats are stale")
        findings.append(_finding("warn", "Stale table statistics", subject, "Run ANALYZE before judging the plan."))
    if _num(meta.get("unsorted_pct")) >= 20:
        issues.append("table is unsorted")
        findings.append(_finding("warn", "Sort key maintenance needed", subject, "VACUUM or rebuild if this table is important to the pasted query."))
    if _num(meta.get("full_scan_score")) >= 75 and "filter" in roles:
        issues.append("known full-scan pressure")
        findings.append(_finding("crit", "Filtered table has full-scan history", subject, "Check whether predicates align to sort keys and whether RR scans are being used."))
    if _num(meta.get("distribution_usage_score")) >= 75 and "join" in roles:
        issues.append("known redistribution pressure")
        findings.append(_finding("warn", "Joined table has redistribution history", subject, "Check join columns against DISTKEY choices."))
    if not issues:
        return "No captured table-health red flags for this statement."
    return "Review: " + ", ".join(issues) + "."


def _join_alignment(
    aliases: set[str],
    columns: list[tuple[str, str]],
    alias_lookup: dict[str, dict],
) -> tuple[str, str, str]:
    matched = [alias for alias in aliases if (alias_lookup.get(alias, {}).get("meta") or {})]
    if len(matched) < 2:
        return "metadata incomplete", "info", "Qualify table names or refresh table info to check distribution alignment."
    replicated = 0
    dist_hits = 0
    big_misses = 0
    for alias, col in columns:
        meta = alias_lookup.get(alias, {}).get("meta") or {}
        if not meta:
            continue
        if _is_diststyle_all(meta.get("diststyle")):
            replicated += 1
        if _same_column(col, meta.get("_distkey")):
            dist_hits += 1
        elif _is_large_table(meta):
            big_misses += 1
    for alias in matched:
        meta = alias_lookup.get(alias, {}).get("meta") or {}
        if _is_diststyle_all(meta.get("diststyle")) and not any(alias == col_alias for col_alias, _ in columns):
            replicated += 1
    if replicated:
        return "co-located by replicated DISTSTYLE ALL", "ok", "One joined side is replicated, so Redshift should not need to broadcast that table at runtime."
    if dist_hits >= 2:
        return "co-located on matching DISTKEY join columns", "ok", "Distribution keys appear compatible for this join."
    if big_misses >= 2:
        return "broadcast/redistribution risk: large join columns miss DISTKEYs", "crit", "Consider aligning DISTKEYs, changing join order, or pre-aggregating/staging."
    if big_misses:
        return "broadcast/redistribution risk: one large side misses DISTKEY", "warn", "Check whether this join causes redistribution or broadcast."
    return "distribution unknown: join columns are not known DISTKEYs", "info", "No immediate table-size red flag, but verify EXPLAIN for DS_DIST/DS_BCAST movement."


def _predicate_alignment(
    aliases: set[str],
    columns: list[tuple[str, str]],
    alias_lookup: dict[str, dict],
) -> tuple[str, str, str]:
    if not aliases:
        return "no table column detected", "info", "Predicate may be constant or derived."
    sort_hit = False
    miss_on_scan_table = False
    stale_stats = False
    for alias, col in columns:
        meta = alias_lookup.get(alias, {}).get("meta") or {}
        if not meta:
            continue
        if _same_column(col, meta.get("_sortkey")):
            sort_hit = True
        elif meta.get("_sortkey") and _num(meta.get("full_scan_score")) >= 60:
            miss_on_scan_table = True
        if _num(meta.get("stats_off")) >= 20:
            stale_stats = True
    if sort_hit:
        return "predicate uses sort key", "ok", "This predicate can support range-restricted scans if the plan cooperates."
    if miss_on_scan_table:
        return "predicate misses sort key on scan-heavy table", "warn", "Consider sort-key fit, predicate rewrite, or table maintenance."
    if stale_stats:
        return "predicate touches stale-stat table", "warn", "Run ANALYZE before trusting selectivity estimates."
    return "no known sort-key alignment", "info", "Check EXPLAIN for RRSCAN and filter selectivity."


def _join_visual_signal(
    join_type: str,
    severity: str,
    aliases: set[str],
    columns: list[tuple[str, str]],
    alias_lookup: dict[str, dict],
) -> tuple[str, str]:
    """Describe actual/inferred join strategy without claiming an unseen plan."""
    lowered = str(join_type or "").lower()
    if severity == "crit":
        return "PROBLEM JOIN", "red"
    if "merge" in lowered:
        return "MERGE JOIN", "green"
    if "hash" in lowered:
        return "HASH JOIN", "amber"
    sort_aligned_aliases = {
        alias
        for alias, column in columns
        if alias in aliases
        and _same_column(column, (alias_lookup.get(alias, {}).get("meta") or {}).get("_sortkey"))
    }
    if len(sort_aligned_aliases) >= 2 and severity == "ok":
        return "MERGE JOIN CANDIDATE", "green"
    if severity == "warn":
        return "HASH JOIN / DATA MOVEMENT RISK", "amber"
    return "HASH JOIN CANDIDATE - VERIFY PLAN", "amber"


def _predicate_visual_signal(
    severity: str,
    alignment: str,
    implicit_join: bool,
) -> tuple[str, str]:
    if severity == "crit":
        return ("PROBLEM IMPLICIT JOIN" if implicit_join else "PROBLEM FILTER"), "red"
    if severity == "ok" or "uses sort key" in str(alignment or "").lower():
        return ("ALIGNED IMPLICIT JOIN" if implicit_join else "SORT-KEY ALIGNED FILTER"), "green"
    return ("IMPLICIT JOIN - REVIEW" if implicit_join else "FILTER - REVIEW SELECTIVITY"), "amber"


def _statement_table_score(meta: dict, status: str) -> float:
    if status in {"view", "ambiguous view"}:
        return 10.0
    if status == "not found":
        return 35.0
    if status == "ambiguous":
        return 25.0
    if not meta:
        return 0.0
    return round(
        min(
            _num(meta.get("table_attention_score")) * 0.45
            + _num(meta.get("full_scan_score")) * 0.22
            + _num(meta.get("distribution_usage_score")) * 0.18
            + _num(meta.get("sort_attention_score")) * 0.15,
            180.0,
        ),
        2,
    )


def _columns_in(node: object) -> list[exp.Column]:
    # An ON expression is a single node; a USING clause is a LIST of Identifier
    # or Column nodes. find_all() exists only on expressions, so lists must be
    # walked directly — and Identifier nodes need promotion to Column so join
    # evidence is not silently dropped.
    if node is None:
        return []
    if isinstance(node, (list, tuple)):
        found: list[exp.Column] = []
        for item in node:
            found.extend(_columns_in(item))
        return found
    if isinstance(node, exp.Column):
        return [node]
    if isinstance(node, exp.Identifier):
        name = str(getattr(node, "this", "") or node).strip()
        return [exp.column(name)] if name else []
    finder = getattr(node, "find_all", None)
    if callable(finder):
        return list(finder(exp.Column))
    return []


def _resolve_column(column: exp.Column, alias_lookup: dict[str, dict]) -> tuple[str, str]:
    alias = _clean(column.table)
    name = _column_leaf(column.name)
    if alias:
        return alias, name
    matched_aliases = [key for key, ref in alias_lookup.items() if ref.get("match_status") in {"matched", "ambiguous"}]
    unique_aliases = sorted(set(matched_aliases))
    if len(unique_aliases) == 1:
        return unique_aliases[0], name
    return "", name


def _is_implicit_join_predicate(predicate: exp.Expression, resolved: list[tuple[str, str]]) -> bool:
    if not isinstance(predicate, exp.EQ):
        return False
    aliases = {alias for alias, _ in resolved if alias}
    if len(aliases) < 2:
        return False
    try:
        left_cols = list(predicate.left.find_all(exp.Column)) if predicate.left is not None else []
        right_cols = list(predicate.right.find_all(exp.Column)) if predicate.right is not None else []
    except Exception:
        return len(resolved) >= 2
    return bool(left_cols and right_cols)


def _join_column_pairs(on_expr: exp.Expression | None) -> list[str]:
    if on_expr is None:
        return []
    pairs = []
    for eq in on_expr.find_all(exp.EQ):
        cols = list(eq.find_all(exp.Column))
        if len(cols) >= 2:
            pairs.append(f"{_sql_snippet(cols[0])} = {_sql_snippet(cols[1])}")
    return pairs


def _join_target(join: exp.Join) -> dict:
    node = join.this
    if isinstance(node, exp.Table):
        alias = _clean(node.alias_or_name or node.name)
        label = ".".join(part for part in (_clean(node.catalog), _clean(node.db), _clean(node.name)) if part)
        return {"alias": alias, "label": label or _clean(node.name)}
    return {"alias": "", "label": type(node).__name__.lower() if node is not None else "join target"}


def _join_type(join: exp.Join) -> str:
    pieces = [
        str(join.args.get("side") or "").lower(),
        str(join.args.get("kind") or "").lower(),
        str(join.args.get("method") or "").lower(),
    ]
    return " ".join(piece for piece in pieces if piece) or "join"


def _alias_subjects(aliases: set[str], alias_lookup: dict[str, dict]) -> list[str]:
    out = []
    for alias in sorted(aliases):
        ref = alias_lookup.get(alias) or {}
        meta = ref.get("meta") or {}
        if meta:
            out.append(_subject_from_meta(meta))
        else:
            out.append(str(ref.get("query_table") or alias))
    return out


def _subject_from_meta(meta: dict) -> str:
    return ".".join(
        str(meta.get(part) or "").strip()
        for part in ("source_db", "schema_name", "table_name")
        if str(meta.get(part) or "").strip()
    )


def _dedupe_findings(findings: list[dict]) -> pd.DataFrame:
    seen = set()
    rows = []
    severity_rank = {"crit": 0, "warn": 1, "info": 2, "ok": 3}
    for finding in findings:
        key = (
            finding.get("severity"),
            finding.get("title"),
            finding.get("subject"),
            finding.get("recommendation"),
        )
        if key in seen:
            continue
        seen.add(key)
        rows.append(finding)
    if not rows:
        return pd.DataFrame(columns=["severity", "title", "subject", "recommendation"])
    return (
        pd.DataFrame(rows)
        .assign(_rank=lambda df: df["severity"].map(severity_rank).fillna(9))
        .sort_values(["_rank", "title", "subject"])
        .drop(columns="_rank")
        .reset_index(drop=True)
    )


def _finding(severity: str, title: str, subject: str, recommendation: str) -> dict:
    return {
        "severity": severity,
        "title": title,
        "subject": subject,
        "recommendation": recommendation,
    }


def _summary(parse_ok: bool, tables: int, joins: int, predicates: int, crit: int, warn: int) -> dict:
    return {
        "parse_status": "parsed" if parse_ok else "error",
        "table_count": tables,
        "join_count": joins,
        "predicate_count": predicates,
        "critical_findings": crit,
        "warning_findings": warn,
    }


def _repeat_matches(sql: str, known_queries: pd.DataFrame | None) -> pd.DataFrame:
    if known_queries is None or known_queries.empty or "sql_text" not in known_queries.columns:
        return pd.DataFrame(
            columns=[
                "similarity_score",
                "repeat_verdict",
                "query_id",
                "elapsed_s",
                "risk_score",
                "repeat_group_id",
                "user_name",
                "database_name",
                "dominant_issue",
                "sql_text",
            ]
        )
    rows = []
    for _, row in known_queries.iterrows():
        candidate = row.get("sql_text")
        if not str(candidate or "").strip():
            continue
        score = score_sql_similarity(sql, candidate)
        if score < 0.68:
            continue
        rows.append(
            {
                "similarity_score": score,
                "repeat_verdict": "repeatable" if score >= DEFAULT_SIMILARITY_THRESHOLD else "similar",
                "query_id": row.get("query_id"),
                "elapsed_s": row.get("elapsed_s"),
                "risk_score": row.get("risk_score"),
                "repeat_group_id": row.get("repeat_group_id", ""),
                "user_name": row.get("user_name", ""),
                "database_name": row.get("database_name", ""),
                "dominant_issue": row.get("dominant_issue", ""),
                "sql_text": _definition_snippet(candidate, limit=700),
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "similarity_score",
                "repeat_verdict",
                "query_id",
                "elapsed_s",
                "risk_score",
                "repeat_group_id",
                "user_name",
                "database_name",
                "dominant_issue",
                "sql_text",
            ]
        )
    return (
        pd.DataFrame(rows)
        .sort_values(["similarity_score", "elapsed_s", "risk_score"], ascending=[False, False, False])
        .head(40)
        .reset_index(drop=True)
    )


def _build_first_steps(
    sql: str,
    intel,
    table_rows: pd.DataFrame,
    join_rows: pd.DataFrame,
    predicate_rows: pd.DataFrame,
    findings: pd.DataFrame,
    repeat_matches: pd.DataFrame,
) -> pd.DataFrame:
    steps: list[dict] = []
    crit_count = int((findings["severity"] == "crit").sum()) if not findings.empty and "severity" in findings.columns else 0
    warn_count = int((findings["severity"] == "warn").sum()) if not findings.empty and "severity" in findings.columns else 0
    repeatable = (
        not repeat_matches.empty
        and "similarity_score" in repeat_matches.columns
        and float(repeat_matches["similarity_score"].max()) >= DEFAULT_SIMILARITY_THRESHOLD
    )
    table_pressure = 0.0
    if not table_rows.empty:
        for col in ("full_scan_score", "distribution_usage_score", "sort_attention_score", "statement_table_score"):
            if col in table_rows.columns:
                table_pressure = max(table_pressure, float(pd.to_numeric(table_rows[col], errors="coerce").fillna(0).max()))
    bad_join = (
        not join_rows.empty
        and "severity" in join_rows.columns
        and join_rows["severity"].isin(["crit", "warn"]).any()
    )
    bad_predicate = (
        not predicate_rows.empty
        and "severity" in predicate_rows.columns
        and predicate_rows["severity"].isin(["crit", "warn"]).any()
    )
    has_view = (
        not table_rows.empty
        and "object_type" in table_rows.columns
        and table_rows["object_type"].astype(str).str.contains("view", case=False, na=False).any()
    )

    if has_view:
        steps.append(
            _first_step(
                len(steps) + 1,
                "query",
                "warn" if warn_count else "info",
                "Drill into the view first",
                "The pasted SQL touches at least one view, so the visible SQL can hide joins and filters.",
                "Review the expanded component table rows, then inspect the view definition before changing base table design.",
            )
        )
    if crit_count or table_pressure >= 85:
        steps.append(
            _first_step(
                len(steps) + 1,
                "table",
                "crit" if crit_count else "warn",
                "Fix table health/design first",
                "Known table metadata shows full-scan, distribution, stale-stats, or sort-key pressure on tables used by this SQL.",
                "Start with ANALYZE/VACUUM where indicated, then review DISTKEY/SORTKEY fit for the joined and filtered tables.",
            )
        )
    if bad_join or bad_predicate or int(getattr(intel, "wildcard_count", 0) or 0) > 0:
        steps.append(
            _first_step(
                len(steps) + 1,
                "query",
                "warn",
                "Fix query shape next",
                "Join/predicate analysis found shape-level risks, or the SQL projects wider data than needed.",
                "Check join predicates, remove unnecessary SELECT *, push filters earlier, and verify EXPLAIN for redistribution.",
            )
        )
    if repeatable:
        best = float(repeat_matches["similarity_score"].max())
        runtime = float(pd.to_numeric(repeat_matches.get("elapsed_s"), errors="coerce").fillna(0).sum())
        steps.append(
            _first_step(
                len(steps) + 1,
                "query",
                "warn",
                "Treat this as a workload pattern",
                f"Similar captured queries already exist; best similarity is {best:.0%} across {len(repeat_matches)} match(es).",
                f"Prioritize this over one-off tuning; matched runtime totals about {runtime:.0f} seconds in the loaded snapshot.",
            )
        )
    if not steps:
        steps.append(
            _first_step(
                1,
                "cluster",
                "info",
                "Check cluster contention",
                "The SQL parses cleanly and table/query alignment did not surface a stronger local fault.",
                "Look next at WLM queue time, concurrency, locks, workload bursts, and whether this ran during cluster pressure.",
            )
        )
    return pd.DataFrame(steps)


def _first_step(rank: int, category: str, severity: str, title: str, why: str, next_step: str) -> dict:
    return {
        "rank": rank,
        "category": category,
        "severity": severity,
        "title": title,
        "why": why,
        "next_step": next_step,
    }


def _object_type(ref: dict) -> str:
    if ref.get("is_cte"):
        return "cte"
    if ref.get("is_view_component"):
        if ref.get("match_status") in {"view", "ambiguous view"}:
            return "view_component_view"
        return "view_component_table"
    if ref.get("match_status") in {"view", "ambiguous view"}:
        return "view"
    return "table"


def _definition_snippet(value: object, limit: int = 900) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")
    text = text.strip()
    return text if len(text) <= limit else text[: limit - 3].rstrip() + "..."


def _distkey(value: object) -> str:
    text = str(value or "").strip()
    normalized = re.sub(r"\s+", "", text).strip("\"'").lower()
    if normalized in _MISSING_KEY_VALUES:
        return ""
    match = _DISTKEY_RE.search(text)
    if not match:
        return ""
    key = _column_leaf(match.group(1))
    return "" if re.sub(r"\s+", "", key).lower() in _MISSING_KEY_VALUES else key


def _sortkey(value: object) -> str:
    key = _column_leaf(value)
    return "" if re.sub(r"\s+", "", key).lower() in _MISSING_KEY_VALUES else key


def _column_leaf(value: object) -> str:
    text = _clean(value)
    if not text or text in {"nan", "none"}:
        return ""
    return text.split(".")[-1].strip('"')


def _same_column(left: object, right: object) -> bool:
    return bool(_column_leaf(left) and _column_leaf(left) == _column_leaf(right))


def _is_large_table(meta: dict) -> bool:
    return _num(meta.get("size_mb")) >= 1024 or _num(meta.get("tbl_rows")) >= 10_000_000


def _is_diststyle_all(value: object) -> bool:
    text = re.sub(r"\s+", "", str(value or "").upper())
    return text in {"ALL", "AUTO(ALL)"}


def _sql_snippet(node: exp.Expression | None, limit: int = 220) -> str:
    if node is None:
        return ""
    try:
        text = node.sql(dialect="redshift", pretty=False)
    except Exception:
        text = str(node)
    text = re.sub(r"\s+", " ", text).strip()
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _clean(value: object) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return str(value).strip().strip('"').lower()


def _num(value: object) -> float:
    try:
        if value is None or pd.isna(value):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0
