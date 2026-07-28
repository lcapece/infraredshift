"""Deterministic Redshift query decomposition into narrow temporary stages."""
from __future__ import annotations

from dataclasses import dataclass, field
import re

import pandas as pd
import sqlglot
from sqlglot import exp

from .physical_lineage import PhysicalLineageResolver
from .sql_lens import analyze_console_sql


@dataclass
class DecompositionResult:
    parse_ok: bool
    original_sql: str
    generated_sql: str = ""
    stages: pd.DataFrame = field(default_factory=pd.DataFrame)
    findings: pd.DataFrame = field(default_factory=pd.DataFrame)
    summary: dict = field(default_factory=dict)


def decompose_redshift_query(
    sql: str,
    table_review: pd.DataFrame | None = None,
    view_definitions: pd.DataFrame | None = None,
    explain_rows: pd.DataFrame | None = None,
    detail_rows: pd.DataFrame | None = None,
    *,
    minimum_rows: int = 1_000_000,
    minimum_size_mb: float = 1024.0,
) -> DecompositionResult:
    original = str(sql or "").strip()
    if not original:
        return DecompositionResult(False, original, findings=_findings_frame([
            _finding("blocked", "No SQL supplied", "Paste a query before decomposing it.")
        ]))
    try:
        tree = sqlglot.parse_one(original, read="redshift")
    except Exception as exc:
        return DecompositionResult(False, original, findings=_findings_frame([
            _finding("blocked", "SQL did not parse", str(exc).splitlines()[0][:500])
        ]))

    tables = table_review.copy() if table_review is not None else pd.DataFrame()
    views = view_definitions.copy() if view_definitions is not None else pd.DataFrame()
    explain = explain_rows.copy() if explain_rows is not None else pd.DataFrame()
    details = detail_rows.copy() if detail_rows is not None else pd.DataFrame()
    analysis = analyze_console_sql(original, tables, pd.DataFrame(), views)
    resolver = PhysicalLineageResolver(tree, views)
    findings: list[dict] = []

    origins_by_table: dict[str, set[str]] = {}
    for column in tree.find_all(exp.Column):
        for origin in resolver.origins_for_column(column):
            if origin.table_key and origin.column:
                origins_by_table.setdefault(origin.table_key, set()).add(origin.column)

    direct_refs = _direct_physical_references(tree, views)
    has_star = _has_projection_star(tree)
    if has_star:
        findings.append(_finding(
            "review",
            "Wildcard projection prevents minimum-width proof",
            "At least one SELECT * is present. Affected stages retain all columns until the wildcard is expanded.",
        ))

    candidate_rows = _candidate_table_rows(analysis.tables, tables)
    stage_rows: list[dict] = []
    stage_sql: list[str] = []
    replacements: dict[str, str] = {}
    stage_no = 0
    for _, table in candidate_rows.iterrows():
        identity = _table_identity(table)
        if not identity:
            continue
        rows = _num(table.get("tbl_rows"))
        size_mb = _num(table.get("size_mb"))
        refs = _references_for_identity(direct_refs, identity)
        if not refs:
            # The table may be underneath a captured view. It is valid lineage,
            # but rewriting the unopened view body here would change statement
            # structure. Report it without emitting unsafe SQL.
            if rows >= minimum_rows or size_mb >= minimum_size_mb:
                findings.append(_finding(
                    "review",
                    f"Large physical table is underneath a view: {identity}",
                    "Explode the view before generating a physical temp stage for this table.",
                ))
            continue
        if rows < minimum_rows and size_mb < minimum_size_mb and len(refs) < 2:
            continue
        stage_no += 1
        temp_name = _temp_name(stage_no, table.get("table_name"))
        required_columns = sorted(origins_by_table.get(identity, set()))
        projection = "*" if has_star or not required_columns else ",\n    ".join(
            _quote_identifier(column) for column in required_columns
        )
        aliases = sorted({alias for alias in refs if alias})
        filters, unsafe_filters = _pushable_filters(analysis.predicates, identity, aliases)
        where_sql = _stage_where(filters, aliases)
        join_columns = _join_columns_for_table(analysis.joins, identity)
        filter_columns = _predicate_columns_for_table(analysis.predicates, identity) if filters else []
        distkey, dist_reason = _choose_distkey(table, join_columns)
        sortkey, sort_reason = _choose_sortkey(table, filter_columns)
        physical_sql = _qualified_table_sql(identity)
        ddl = [f"DROP TABLE IF EXISTS {temp_name};", f"CREATE TEMP TABLE {temp_name}"]
        if distkey:
            ddl.append(f"DISTKEY({_quote_identifier(distkey)})")
        if sortkey:
            ddl.append(f"SORTKEY({_quote_identifier(sortkey)})")
        ddl.extend(
            [
                "AS",
                "SELECT",
                f"    {projection}",
                f"FROM {physical_sql} AS src",
            ]
        )
        if where_sql:
            ddl.append(f"WHERE {where_sql}")
        ddl[-1] = ddl[-1] + ";"
        ddl.append(f"ANALYZE {temp_name};")
        sql_block = "\n".join(ddl)
        stage_sql.append(sql_block)
        replacements[identity] = temp_name
        evidence = _table_execution_evidence(table, explain, details)
        stage_rows.append(
            {
                "stage_no": stage_no,
                "stage_name": temp_name,
                "stage_type": "physical fact/input",
                "physical_table": identity,
                "source_rows": rows,
                "source_size_mb": size_mb,
                "reference_count": len(refs),
                "required_column_count": len(required_columns) if projection != "*" else None,
                "required_columns": "*" if projection == "*" else ", ".join(required_columns),
                "pushed_predicates": " AND ".join(filters),
                "review_predicates": "; ".join(unsafe_filters),
                "distkey": distkey,
                "sortkey": sortkey,
                "design_reason": "; ".join(part for part in (dist_reason, sort_reason) if part),
                **evidence,
                "generated_sql": sql_block,
                "safety": "review" if unsafe_filters or projection == "*" else "safe",
            }
        )

    intermediate = _intermediate_candidates(tree, candidate_rows)
    for item in intermediate:
        findings.append(_finding("review", item["title"], item["detail"]))

    if not stage_rows:
        findings.append(_finding(
            "info",
            "No automatic temp stage crossed the threshold",
            f"No directly referenced physical table had at least {minimum_rows:,} rows, {minimum_size_mb:,.0f} MB, or repeated scans.",
        ))
        return DecompositionResult(
            True,
            original,
            generated_sql="-- No safe automatic decomposition stage was identified.\n" + original,
            stages=pd.DataFrame(),
            findings=_findings_frame(findings),
            summary={"stage_count": 0, "physical_table_count": len(candidate_rows), "parse_status": "parsed"},
        )

    rewritten = _replace_physical_tables(tree, replacements)
    generated = (
        "-- EXPERIMENTAL REDSHIFT DECOMPOSITION\n"
        "-- Review the safety ledger and compare results plus EXPLAIN before use.\n\n"
        + "\n\n".join(stage_sql)
        + "\n\n-- Final query over reduced stages\n"
        + rewritten.sql(dialect="redshift", pretty=True)
        + ";\n"
    )
    findings.append(_finding(
        "required",
        "Validate equivalence before use",
        "Compare row counts, duplicates, nulls, data types, representative results, and EXPLAIN plans between the original and decomposed SQL.",
    ))
    return DecompositionResult(
        True,
        original,
        generated_sql=generated,
        stages=pd.DataFrame(stage_rows),
        findings=_findings_frame(findings),
        summary={
            "stage_count": len(stage_rows),
            "physical_table_count": len(candidate_rows),
            "minimum_rows": int(minimum_rows),
            "minimum_size_mb": float(minimum_size_mb),
            "parse_status": "parsed",
        },
    )


def _direct_physical_references(tree: exp.Expression, view_definitions: pd.DataFrame) -> dict[str, list[str]]:
    cte_names = {_clean(cte.alias_or_name) for cte in tree.find_all(exp.CTE)}
    view_keys = set()
    if view_definitions is not None and not view_definitions.empty:
        for _, row in view_definitions.iterrows():
            database = _clean(row.get("database") or row.get("database_name") or row.get("source_db"))
            schema = _clean(row.get("schema") or row.get("schema_name"))
            name = _clean(row.get("view_name") or row.get("table_name"))
            view_keys.update(
                key for key in (
                    ".".join(part for part in (database, schema, name) if part),
                    ".".join(part for part in (schema, name) if part),
                    name,
                ) if key
            )
    refs: dict[str, list[str]] = {}
    for table in tree.find_all(exp.Table):
        name = _clean(table.name)
        schema = _clean(table.db)
        database = _clean(table.catalog)
        identity = ".".join(part for part in (database, schema, name) if part)
        if (name in cte_names and not schema and not database) or any(
            key in view_keys for key in (identity, ".".join(part for part in (schema, name) if part), name)
        ):
            continue
        refs.setdefault(identity, []).append(_clean(table.alias_or_name or name))
    return refs


def _references_for_identity(references: dict[str, list[str]], physical_identity: str) -> list[str]:
    """Bind schema.table SQL to one resolved database.schema.table identity.

    SQL Lens has already resolved the catalog row. Suffix matching is only
    accepted when one direct SQL identity matches, so an ambiguous unqualified
    table name cannot silently produce a physical rewrite.
    """
    exact = references.get(physical_identity)
    if exact:
        return exact
    matches = [
        aliases
        for sql_identity, aliases in references.items()
        if physical_identity.endswith("." + sql_identity)
        or sql_identity.endswith("." + physical_identity)
    ]
    return matches[0] if len(matches) == 1 else []


def _candidate_table_rows(analysis_tables: pd.DataFrame, table_review: pd.DataFrame) -> pd.DataFrame:
    if analysis_tables is None or analysis_tables.empty:
        return pd.DataFrame()
    rows = analysis_tables.copy()
    if "object_type" in rows.columns:
        rows = rows[rows["object_type"].astype(str).str.contains("table", case=False, na=False)]
    rows["__identity"] = rows.apply(_table_identity, axis=1)
    rows = rows[rows["__identity"].astype(bool)].copy()
    # SQL Lens intentionally publishes a compact table view. Restore catalog
    # identifiers and any other source-only telemetry needed to bind
    # SYS_QUERY_DETAIL scans back to their physical tables.
    if table_review is not None and not table_review.empty:
        catalog = table_review.copy()
        catalog["__identity"] = catalog.apply(_table_identity, axis=1)
        catalog = catalog[catalog["__identity"].astype(bool)].drop_duplicates("__identity")
        for column in catalog.columns:
            if column == "__identity":
                continue
            values = catalog.set_index("__identity")[column]
            mapped = rows["__identity"].map(values)
            if column not in rows.columns:
                rows[column] = mapped
                continue
            missing = rows[column].isna() | rows[column].astype(str).str.strip().isin(("", "nan", "None"))
            if not missing.any():
                continue
            # Pandas 2.x refuses a cross-dtype .loc assignment: writing a
            # string-backed NA (or any object value) into a float64 column raises
            # TypeError and, before this guard, aborted decomposition for ~16% of
            # real queries. The app caught that at widgets/query_decomposer.py and
            # reported "Decomposition failed safely", so the failures were
            # invisible and looked like "no candidates found".
            #
            # Align dtypes before assigning: fill in place when the kinds already
            # match, otherwise widen the column to object so mixed values are legal.
            fill = mapped.loc[missing]
            try:
                if rows[column].dtype == fill.dtype:
                    rows.loc[missing, column] = fill
                else:
                    coerced = fill.astype(rows[column].dtype)
                    rows.loc[missing, column] = coerced
            except (TypeError, ValueError):
                rows[column] = rows[column].astype(object)
                rows.loc[missing, column] = fill.astype(object)
    sort_columns = [column for column in ("tbl_rows", "size_mb") if column in rows.columns]
    if sort_columns:
        rows = rows.sort_values(sort_columns, ascending=False)
    return rows.drop_duplicates("__identity", keep="first").reset_index(drop=True)


def _pushable_filters(predicates: pd.DataFrame, identity: str, aliases: list[str]) -> tuple[list[str], list[str]]:
    safe: list[str] = []
    review: list[str] = []
    if predicates is None or predicates.empty:
        return safe, review
    candidates: list[tuple[str, set[str]]] = []
    for _, row in predicates.iterrows():
        if str(row.get("clause") or "").upper() != "WHERE" or str(row.get("predicate_role") or "") != "filter":
            continue
        sources = _source_tables(row.get("physical_sources"))
        if sources != {identity}:
            continue
        condition = str(row.get("condition") or "").strip()
        predicate_aliases = {_clean(value) for value in str(row.get("aliases") or "").split(",") if _clean(value)}
        if condition:
            candidates.append((condition, predicate_aliases))
    # A table scanned through multiple aliases has independent row domains.
    # Applying one alias's predicate to a shared temp table could remove rows
    # needed by the other scan, so leave all such predicates in the final SQL.
    if len(set(aliases)) > 1:
        review.extend(condition for condition, _ in candidates)
        return safe, list(dict.fromkeys(review))
    for condition, predicate_aliases in candidates:
        if not predicate_aliases or predicate_aliases.issubset(set(aliases)):
            safe.append(condition)
        else:
            review.append(condition)
    return list(dict.fromkeys(safe)), list(dict.fromkeys(review))


def _stage_where(filters: list[str], aliases: list[str]) -> str:
    if not filters:
        return ""
    normalized = []
    for condition in filters:
        text = condition
        for alias in sorted(aliases, key=len, reverse=True):
            text = re.sub(rf'(?i)(?<![\w$])"?{re.escape(alias)}"?\s*\.', "src.", text)
        normalized.append(f"({text})")
    return "\n  AND ".join(normalized)


def _join_columns_for_table(joins: pd.DataFrame, identity: str) -> list[str]:
    columns: list[str] = []
    if joins is None or joins.empty:
        return columns
    for _, row in joins.iterrows():
        for field in ("left_physical_sources", "right_physical_sources"):
            for source in _split_csv(row.get(field)):
                table, column = _source_table_column(source)
                if table == identity and column:
                    columns.append(column)
    return columns


def _predicate_columns_for_table(predicates: pd.DataFrame, identity: str) -> list[str]:
    columns: list[str] = []
    if predicates is None or predicates.empty:
        return columns
    for _, row in predicates.iterrows():
        if str(row.get("clause") or "").upper() != "WHERE":
            continue
        for source in _split_csv(row.get("physical_sources")):
            table, column = _source_table_column(source)
            if table == identity and column:
                columns.append(column)
    return columns


def _choose_distkey(table: pd.Series, join_columns: list[str]) -> tuple[str, str]:
    existing = _distkey(table.get("diststyle"))
    if existing and existing in join_columns:
        return existing, f"DISTKEY preserves the source key used by {join_columns.count(existing)} downstream join reference(s)"
    if join_columns:
        candidate = max(set(join_columns), key=join_columns.count)
        return candidate, f"DISTKEY candidate is the most-used downstream physical join column ({join_columns.count(candidate)} reference(s))"
    return "", "DISTSTYLE defaults to EVEN because no physical downstream join key was proven"


def _choose_sortkey(table: pd.Series, filter_columns: list[str]) -> tuple[str, str]:
    existing = _clean(table.get("sortkey1"))
    if existing and existing in filter_columns:
        return existing, "SORTKEY preserves the source leading key used by a pushed predicate"
    if filter_columns:
        date_candidates = [column for column in filter_columns if any(token in column for token in ("date", "time", "dt"))]
        candidate = date_candidates[0] if date_candidates else max(set(filter_columns), key=filter_columns.count)
        return candidate, "SORTKEY candidate supports the strongest safely pushed filter column"
    return "", "No SORTKEY emitted because no safely pushed physical filter column was proven"


def _table_execution_evidence(table: pd.Series, explain: pd.DataFrame, details: pd.DataFrame) -> dict:
    table_id = str(table.get("table_id") or "").strip()
    matched = pd.DataFrame()
    if table_id and not details.empty and "table_id" in details.columns:
        ids = details["table_id"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        matched = details.loc[ids == table_id.replace(".0", "")].copy()
    plan_matches = pd.DataFrame()
    identity = _table_identity(table)
    table_name = identity.split(".")[-1] if identity else ""
    if table_name and explain is not None and not explain.empty:
        plan_text = explain.apply(
            lambda row: " ".join(str(row.get(column) or "") for column in ("plan_node", "plan_info")),
            axis=1,
        )
        pattern = rf"(?i)(?<![\w$]){re.escape(table_name)}(?![\w$])"
        plan_matches = explain.loc[plan_text.str.contains(pattern, regex=True, na=False)].copy()
    estimated_rows: list[float] = []
    estimated_widths: list[float] = []
    max_costs: list[float] = []
    plan_nodes: list[str] = []
    for _, row in plan_matches.iterrows():
        text = " ".join(str(row.get(column) or "") for column in ("plan_node", "plan_info"))
        plan_nodes.append(str(row.get("plan_node_id") or "").strip())
        for target, regex in (
            (estimated_rows, r"(?i)rows\s*=\s*([0-9.]+)"),
            (estimated_widths, r"(?i)width\s*=\s*([0-9.]+)"),
            (max_costs, r"(?i)cost\s*=\s*[0-9.]+\.\.([0-9.]+)"),
        ):
            match = re.search(regex, text)
            if match:
                target.append(_num(match.group(1)))
    return {
        "plan_scan_nodes": ", ".join(dict.fromkeys(node for node in plan_nodes if node)),
        "plan_estimated_rows": max(estimated_rows, default=0),
        "plan_estimated_width": max(estimated_widths, default=0),
        "plan_max_cost": max(max_costs, default=0),
        "actual_scan_rows": _sum(matched, "input_rows"),
        "actual_output_rows": _sum(matched, "output_rows"),
        "actual_scan_bytes": _sum(matched, "input_bytes"),
        "actual_scan_duration_s": _sum(matched, "duration_s"),
        "actual_remote_read_blocks": _sum(matched, "remote_read_io"),
        "actual_spill_blocks": _sum(matched, "spilled_block_local_disk") + _sum(matched, "spilled_block_remote_disk"),
    }


def _has_projection_star(tree: exp.Expression) -> bool:
    for select in tree.find_all(exp.Select):
        for projection in select.expressions:
            expression = projection.this if isinstance(projection, exp.Alias) else projection
            if isinstance(expression, exp.Star):
                return True
            if isinstance(expression, exp.Column) and bool(getattr(expression, "is_star", False)):
                return True
    return False


def _intermediate_candidates(tree: exp.Expression, tables: pd.DataFrame) -> list[dict]:
    candidates: list[dict] = []
    for cte in tree.find_all(exp.CTE):
        query = cte.this
        reasons = []
        if next(query.find_all(exp.Join), None) is not None:
            reasons.append("contains joins")
        if next(query.find_all(exp.Window), None) is not None:
            reasons.append("contains window functions")
        if next(query.find_all(exp.AggFunc), None) is not None:
            reasons.append("contains aggregation")
        if query.args.get("distinct"):
            reasons.append("deduplicates rows")
        name = str(cte.alias_or_name or "CTE")
        references = sum(1 for table in tree.find_all(exp.Table) if _clean(table.name) == _clean(name))
        if references > 1:
            reasons.append(f"is referenced {references} times")
        if reasons:
            candidates.append(
                {
                    "title": f"Intermediate materialization candidate: {name}",
                    "detail": ", ".join(reasons) + ". Review this boundary after the physical input stages are validated.",
                }
            )
    return candidates


def _replace_physical_tables(tree: exp.Expression, replacements: dict[str, str]) -> exp.Expression:
    rewritten = tree.copy()

    def replace(node):
        if not isinstance(node, exp.Table):
            return node
        identity = ".".join(
            part for part in (_clean(node.catalog), _clean(node.db), _clean(node.name)) if part
        )
        temp = replacements.get(identity)
        if not temp:
            suffix_matches = {
                replacement
                for physical_identity, replacement in replacements.items()
                if physical_identity.endswith("." + identity)
                or identity.endswith("." + physical_identity)
            }
            temp = next(iter(suffix_matches)) if len(suffix_matches) == 1 else None
        if not temp:
            return node
        alias_name = str(node.alias_or_name or node.name)
        replacement = exp.Table(this=exp.to_identifier(temp))
        replacement.set("alias", exp.TableAlias(this=exp.to_identifier(alias_name)))
        return replacement

    return rewritten.transform(replace)


def _table_identity(row: pd.Series) -> str:
    return ".".join(
        part for part in (
            _clean(row.get("source_db")),
            _clean(row.get("schema_name")),
            _clean(row.get("table_name")),
        ) if part
    )


def _source_tables(value: object) -> set[str]:
    return {_source_table_column(source)[0] for source in _split_csv(value) if _source_table_column(source)[0]}


def _source_table_column(source: str) -> tuple[str, str]:
    parts = [_clean(part) for part in str(source or "").split(".") if _clean(part)]
    if len(parts) < 2:
        return "", ""
    return ".".join(parts[:-1]), parts[-1]


def _split_csv(value: object) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def _distkey(value: object) -> str:
    match = re.search(r"key\s*\(\s*([^)]+?)\s*\)", str(value or ""), re.IGNORECASE)
    return _clean(match.group(1)) if match else ""


def _temp_name(number: int, table: object) -> str:
    name = re.sub(r"[^a-z0-9_]+", "_", _clean(table)).strip("_") or "stage"
    return f"tmp_decomp_{number:02d}_{name[:36]}"


def _qualified_table_sql(identity: str) -> str:
    return ".".join(_quote_identifier(part) for part in identity.split(".") if part)


def _quote_identifier(value: object) -> str:
    text = str(value or "")
    return '"' + text.replace('"', '""') + '"'


def _sum(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    return float(pd.to_numeric(frame[column], errors="coerce").fillna(0).sum())


def _num(value: object) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _clean(value: object) -> str:
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value or "").strip().strip('"').lower()


def _finding(level: str, title: str, detail: str) -> dict:
    return {"level": level, "title": title, "detail": detail}


def _findings_frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["level", "title", "detail"])
