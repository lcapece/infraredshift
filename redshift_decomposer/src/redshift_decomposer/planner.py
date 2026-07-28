"""Stage planning: which temps to create and how to design them."""

from __future__ import annotations

from dataclasses import dataclass, field
from collections import Counter

from sqlglot import exp

from .analyze import QueryAnalysis, Predicate
from .catalog import Catalog, TableStats, normalize_ident
from .identity import qualified_sql, quote_ident, safe_temp_name, table_alias, table_identity
from .models import Finding, Stage


@dataclass
class PlannerConfig:
    minimum_rows: float = 1_000_000
    minimum_size_mb: float = 1024.0
    materialize_repeated_ctes: bool = True
    materialize_cte_min_complexity: int = 2
    push_predicates: bool = True
    prune_columns: bool = True
    analyze_after_ctas: bool = True


@dataclass
class PlannedStage:
    temp_name: str
    stage_type: str
    source_key: str
    select_sql: str
    columns: tuple[str, ...]
    diststyle: str
    distkey: str
    sortkeys: tuple[str, ...]
    estimated_rows: float
    estimated_mb: float
    pushed_predicates: tuple[str, ...]
    rationale: str
    safety: str
    replace_identities: tuple[str, ...]  # SQL identities/aliases to rewrite in final query
    replace_cte_name: str = ""


@dataclass
class PlanBuild:
    stages: list[PlannedStage] = field(default_factory=list)
    findings: list[Finding] = field(default_factory=list)
    replacements: dict[str, str] = field(default_factory=dict)  # physical identity -> temp
    cte_replacements: dict[str, str] = field(default_factory=dict)  # cte name -> temp


def plan_stages(
    analysis: QueryAnalysis,
    catalog: Catalog,
    config: PlannerConfig | None = None,
) -> PlanBuild:
    config = config or PlannerConfig()
    build = PlanBuild()
    used_names: set[str] = set()
    index = 0

    # --- physical input stages ---
    physical_groups: dict[str, list] = {}
    for ref in analysis.table_refs:
        if ref.is_cte or ref.is_view:
            continue
        resolved = catalog.resolve_table(ref.catalog_key or ref.identity)
        if resolved is None:
            continue
        key, stats = resolved
        physical_groups.setdefault(key, []).append(ref)

    for catalog_key, refs in physical_groups.items():
        stats = catalog.tables[catalog_key]
        aliases = [r.alias for r in refs]
        repeated = len(refs) >= 2
        large = stats.rows >= config.minimum_rows or stats.size_mb >= config.minimum_size_mb
        if not large and not repeated:
            continue

        # Independent multi-scan domains (different aliases OR same alias used
        # twice, e.g. CTE body + outer) must not share one over-filtered temp.
        multi_alias = len(set(aliases)) > 1 or repeated
        # Aliases that line up to this physical table (direct + exploded view subqueries)
        related_aliases = {
            alias
            for alias, keys in analysis.alias_lineage.items()
            if catalog_key in keys
        } | set(aliases)

        pushable, review = _pushable_for_table(
            analysis, list(related_aliases), catalog_key, multi_alias, config
        )
        if review:
            build.findings.append(
                Finding(
                    "review",
                    f"Predicates left on final query for {catalog_key}",
                    "; ".join(review),
                )
            )

        columns = _required_columns(analysis, catalog_key, stats, config)
        if analysis.has_star and not columns:
            build.findings.append(
                Finding(
                    "review",
                    f"SELECT * prevents proven column pruning for {catalog_key}",
                    "Stage projects all known columns from the catalog when available.",
                )
            )
            columns = list(stats.column_names()) if stats.column_names() else []
        # If lineage still left us too thin vs catalog, keep all known columns
        if stats.column_names() and columns and set(columns) < set(stats.column_names()):
            used = analysis.column_usage.get(catalog_key, set())
            if not used or not set(columns).issuperset(used):
                columns = list(stats.column_names())

        join_cols = _join_columns_for_aliases(analysis, list(related_aliases))
        # also pull join columns attributed via lineage to this physical table
        for edge in analysis.joins:
            for edge_alias, edge_col in (
                (edge.left_alias, edge.left_column),
                (edge.right_alias, edge.right_column),
            ):
                if edge_col and catalog_key in analysis.alias_lineage.get(edge_alias, set()):
                    join_cols.append(edge_col)
        filter_cols = [col for _alias, col in _predicate_columns(analysis, list(related_aliases), pushable)]
        distkey, sortkeys, diststyle, key_reason = _choose_keys(stats, join_cols, filter_cols)

        index += 1
        temp = safe_temp_name(index, catalog_key.split(".")[-1], used_names)
        projection = ", ".join(quote_ident(c) for c in columns) if columns else "*"
        where_sql = _rewrite_predicates_to_src(pushable, list(related_aliases | set(aliases)))
        select_lines = [
            "SELECT",
            f"    {projection}",
            f"FROM {qualified_sql(catalog_key)} AS src",
        ]
        if where_sql:
            select_lines.append(f"WHERE {where_sql}")
        select_sql = "\n".join(select_lines)

        rationale_bits = []
        if large:
            rationale_bits.append(
                f"large source (~{stats.rows:,.0f} rows, {stats.size_mb:,.0f} MB)"
            )
        if repeated:
            rationale_bits.append(f"referenced {len(refs)} times")
        if pushable:
            rationale_bits.append(f"pushed {len(pushable)} predicate(s)")
        if key_reason:
            rationale_bits.append(key_reason)
        if stats.is_external:
            rationale_bits.append("external/Spectrum source staged into local temp")

        safety = "review" if (multi_alias or not columns or analysis.has_star) else "safe"
        stage = PlannedStage(
            temp_name=temp,
            stage_type="physical_input",
            source_key=catalog_key,
            select_sql=select_sql,
            columns=tuple(columns) if columns else ("*",),
            diststyle=diststyle,
            distkey=distkey,
            sortkeys=tuple(sortkeys),
            estimated_rows=stats.rows,
            estimated_mb=stats.size_mb,
            pushed_predicates=tuple(pushable),
            rationale="; ".join(rationale_bits),
            safety=safety,
            replace_identities=tuple({r.identity for r in refs} | {catalog_key}),
        )
        build.stages.append(stage)
        for ident in stage.replace_identities:
            build.replacements[ident] = temp
        build.replacements[catalog_key] = temp

    # --- CTE stages ---
    if config.materialize_repeated_ctes:
        index = _plan_cte_stages(analysis, catalog, config, build, used_names, index)

    if not build.stages:
        build.findings.append(
            Finding(
                "info",
                "No automatic stage crossed planning thresholds",
                (
                    f"Need ≥ {config.minimum_rows:,.0f} rows, ≥ {config.minimum_size_mb:,.0f} MB, "
                    "repeated scans, or expensive multi-use CTEs."
                ),
            )
        )
    return build


def stages_to_models(planned: list[PlannedStage], config: PlannerConfig) -> list[Stage]:
    out: list[Stage] = []
    for i, p in enumerate(planned, start=1):
        ddl = _emit_stage_ddl(p, config)
        out.append(
            Stage(
                index=i,
                name=p.temp_name,
                stage_type=p.stage_type,
                source=p.source_key,
                sql=ddl,
                columns=p.columns,
                diststyle=p.diststyle,
                distkey=p.distkey,
                sortkeys=p.sortkeys,
                estimated_source_rows=p.estimated_rows,
                estimated_source_mb=p.estimated_mb,
                pushed_predicates=p.pushed_predicates,
                rationale=p.rationale,
                safety=p.safety,
            )
        )
    return out


def _emit_stage_ddl(stage: PlannedStage, config: PlannerConfig) -> str:
    lines = [f"DROP TABLE IF EXISTS {stage.temp_name};", f"CREATE TEMP TABLE {stage.temp_name}"]
    if stage.distkey:
        lines.append(f"DISTKEY({quote_ident(stage.distkey)})")
    elif stage.diststyle.upper() == "ALL":
        lines.append("DISTSTYLE ALL")
    if stage.sortkeys:
        sk = ", ".join(quote_ident(c) for c in stage.sortkeys)
        lines.append(f"SORTKEY({sk})")
    lines.append("AS")
    lines.append(stage.select_sql)
    lines[-1] = lines[-1].rstrip() + ";"
    if config.analyze_after_ctas:
        lines.append(f"ANALYZE {stage.temp_name};")
    return "\n".join(lines)


def _plan_cte_stages(
    analysis: QueryAnalysis,
    catalog: Catalog,
    config: PlannerConfig,
    build: PlanBuild,
    used_names: set[str],
    index: int,
) -> int:
    tree = analysis.tree
    with_clause = tree.args.get("with") if isinstance(tree, exp.Expression) else None
    # also on Select
    if with_clause is None:
        select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
        if select is not None:
            with_clause = select.args.get("with")
    if with_clause is None:
        return index

    for cte in with_clause.expressions:
        name = normalize_ident(cte.alias_or_name)
        body = cte.this
        if not name or not isinstance(body, exp.Select):
            continue
        refs = sum(1 for t in tree.find_all(exp.Table) if normalize_ident(t.name) == name and not t.db)
        complexity = (
            2 * sum(1 for _ in body.find_all(exp.Join))
            + 2 * sum(1 for _ in body.find_all(exp.Group))
            + sum(1 for _ in body.find_all(exp.Window))
            + sum(1 for _ in body.find_all(exp.Subquery))
            + int(body.args.get("distinct") is not None)
        )
        if refs < 2 and complexity < config.materialize_cte_min_complexity:
            continue
        index += 1
        temp = safe_temp_name(index, name, used_names)
        # rewrite body physical tables that already have stages
        body_sql = _replace_tables_in_sql_tree(body.copy(), build.replacements).sql(
            dialect="redshift", pretty=True
        )
        join_cols = _join_columns_for_aliases(analysis, [name])
        distkey = join_cols[0] if join_cols else ""
        diststyle = "KEY" if distkey else "EVEN"
        stage = PlannedStage(
            temp_name=temp,
            stage_type="cte",
            source_key=name,
            select_sql=body_sql,
            columns=tuple(_select_output_names(body)),
            diststyle=diststyle,
            distkey=distkey,
            sortkeys=(),
            estimated_rows=0,
            estimated_mb=0,
            pushed_predicates=(),
            rationale=(
                f"CTE materialization; refs={refs}; complexity={complexity}"
                + (f"; DISTKEY from downstream join on {distkey}" if distkey else "")
            ),
            safety="review",
            replace_identities=(),
            replace_cte_name=name,
        )
        build.stages.append(stage)
        build.cte_replacements[name] = temp
    return index


def _replace_tables_in_sql_tree(tree: exp.Expression, replacements: dict[str, str]) -> exp.Expression:
    def transform(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Table):
            return node
        identity = table_identity(node)
        temp = replacements.get(identity)
        if temp is None:
            matches = [
                t
                for ident, t in replacements.items()
                if ident.endswith("." + identity) or identity.endswith("." + ident)
            ]
            temp = matches[0] if len(matches) == 1 else None
        if not temp:
            return node
        alias = table_alias(node)
        replacement = exp.table_(temp, alias=alias)
        return replacement

    return tree.transform(transform)


def rewrite_final_query(
    tree: exp.Expression,
    replacements: dict[str, str],
    cte_replacements: dict[str, str],
) -> exp.Expression:
    rewritten = tree.copy()

    def transform(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Table):
            return node
        name = normalize_ident(node.name)
        if name in cte_replacements and not node.db and not node.catalog:
            temp = cte_replacements[name]
            return exp.table_(temp, alias=table_alias(node))
        identity = table_identity(node)
        temp = replacements.get(identity)
        if temp is None:
            matches = [
                t
                for ident, t in replacements.items()
                if ident.endswith("." + identity) or identity.endswith("." + ident)
            ]
            temp = matches[0] if len(set(matches)) == 1 else None
        if not temp:
            return node
        return exp.table_(temp, alias=table_alias(node))

    rewritten = rewritten.transform(transform)

    # Drop CTEs that were fully staged
    select = rewritten if isinstance(rewritten, exp.Select) else rewritten.find(exp.Select)
    if select is not None and cte_replacements:
        with_clause = select.args.get("with")
        if with_clause is not None:
            remaining = [
                cte
                for cte in with_clause.expressions
                if normalize_ident(cte.alias_or_name) not in cte_replacements
            ]
            if remaining:
                with_clause.set("expressions", remaining)
            else:
                select.set("with", None)
    return rewritten


def _pushable_for_table(
    analysis: QueryAnalysis,
    aliases: list[str],
    catalog_key: str,
    multi_alias: bool,
    config: PlannerConfig,
) -> tuple[list[str], list[str]]:
    if not config.push_predicates:
        return [], []
    alias_set = set(aliases)
    # Also accept predicates whose aliases lineage-resolve only to this table
    lineage_aliases = {
        alias
        for alias, keys in analysis.alias_lineage.items()
        if keys == {catalog_key}
    }
    alias_set |= lineage_aliases
    safe: list[str] = []
    review: list[str] = []
    for pred in analysis.predicates:
        if pred.clause != "WHERE":
            continue
        if not pred.aliases:
            continue
        # every alias in the predicate must resolve only to this physical table
        ok = True
        for alias in pred.aliases:
            keys = analysis.alias_lineage.get(alias, set())
            if alias in alias_set:
                continue
            if keys and keys <= {catalog_key}:
                continue
            ok = False
            break
        if not ok:
            continue
        if multi_alias and len(pred.aliases) == 1 and pred.aliases <= set(aliases):
            # independent multi-scan domains — do not push shared filter
            review.append(pred.sql)
            continue
        if multi_alias:
            # predicates on lineage aliases from exploded single-source views are ok
            if not pred.aliases <= lineage_aliases | set(aliases):
                review.append(pred.sql)
                continue
        # Require the conservative pushability analysis — column lineage alone
        # must not bypass _is_simple_pushable (OR groups, volatiles, etc.).
        if not pred.is_simple_pushable:
            review.append(pred.sql)
            continue
        if not _predicate_columns_on_table(pred, catalog_key, analysis):
            review.append(pred.sql)
            continue
        safe.append(pred.sql)
    return list(dict.fromkeys(safe)), list(dict.fromkeys(review))


def _predicate_columns_on_table(pred: Predicate, catalog_key: str, analysis: QueryAnalysis) -> bool:
    if not pred.aliases:
        return False
    for alias in pred.aliases:
        keys = analysis.alias_lineage.get(alias, set())
        if keys and keys != {catalog_key}:
            return False
        if not keys and alias not in {r.alias for r in analysis.table_refs if r.catalog_key == catalog_key}:
            return False
    return True


def _required_columns(
    analysis: QueryAnalysis,
    catalog_key: str,
    stats: TableStats,
    config: PlannerConfig,
) -> list[str]:
    if not config.prune_columns:
        return list(stats.column_names())
    used = set(analysis.column_usage.get(catalog_key, set()))
    # always include dist/sort/join keys from catalog design for correctness of later stages
    if stats.distkey:
        used.add(normalize_ident(stats.distkey))
    for sk in stats.sortkeys:
        used.add(normalize_ident(sk))
    # join columns from analysis
    for edge in analysis.joins:
        for alias, col in ((edge.left_alias, edge.left_column), (edge.right_alias, edge.right_column)):
            for ref in analysis.table_refs:
                if ref.alias == alias and ref.catalog_key == catalog_key and col:
                    used.add(col)
    known = set(stats.column_names())
    if known and used:
        # keep stable order: catalog order for known used cols
        ordered = [c for c in stats.column_names() if c in used]
        extras = sorted(used - known)
        return ordered + extras
    if used:
        return sorted(used)
    return list(stats.column_names())


def _join_columns_for_aliases(analysis: QueryAnalysis, aliases: list[str]) -> list[str]:
    alias_set = set(aliases)
    cols: list[str] = []
    for edge in analysis.joins:
        if edge.left_alias in alias_set and edge.left_column:
            cols.append(edge.left_column)
        if edge.right_alias in alias_set and edge.right_column:
            cols.append(edge.right_column)
    return cols


def _predicate_columns(
    analysis: QueryAnalysis,
    aliases: list[str],
    pushable_sql: list[str],
) -> list[tuple[str, str]]:
    push_set = set(pushable_sql)
    out: list[tuple[str, str]] = []
    alias_set = set(aliases)
    for pred in analysis.predicates:
        if pred.sql not in push_set:
            continue
        for alias, col in pred.columns:
            if alias in alias_set:
                out.append((alias, col))
    return out


def _choose_keys(
    stats: TableStats,
    join_cols: list[str],
    filter_cols: list[str],
) -> tuple[str, list[str], str, str]:
    reasons: list[str] = []
    distkey = ""
    existing_dk = normalize_ident(stats.distkey)
    if existing_dk and existing_dk in join_cols:
        distkey = existing_dk
        reasons.append(f"DISTKEY preserves source key {distkey}")
    elif join_cols:
        distkey = Counter(join_cols).most_common(1)[0][0]
        reasons.append(f"DISTKEY from join column {distkey}")
    diststyle = "KEY" if distkey else ("ALL" if stats.diststyle.upper() == "ALL" and not join_cols else "EVEN")

    sortkeys: list[str] = []
    existing_sk = [normalize_ident(s) for s in stats.sortkeys if normalize_ident(s)]
    if existing_sk and any(s in filter_cols for s in existing_sk):
        sortkeys = [s for s in existing_sk if s in filter_cols][:1] or existing_sk[:1]
        reasons.append(f"SORTKEY preserves source leading key {sortkeys[0]}")
    elif filter_cols:
        dateish = [c for c in filter_cols if any(tok in c for tok in ("date", "time", "dt", "day"))]
        candidate = dateish[0] if dateish else Counter(filter_cols).most_common(1)[0][0]
        sortkeys = [candidate]
        reasons.append(f"SORTKEY supports pushed filter on {candidate}")

    return distkey, sortkeys, diststyle, "; ".join(reasons)


def _select_output_names(select: exp.Select) -> list[str]:
    names: list[str] = []
    for proj in select.expressions:
        if isinstance(proj, exp.Alias):
            names.append(normalize_ident(proj.alias))
        elif isinstance(proj, exp.Column):
            names.append(normalize_ident(proj.alias_or_name))
        elif isinstance(proj, exp.Star):
            names.append("*")
        else:
            names.append(normalize_ident(proj.alias_or_name or proj.sql(dialect="redshift")[:40]))
    return names


def _rewrite_predicates_to_src(predicates: list[str], aliases: list[str]) -> str:
    if not predicates:
        return ""
    import re

    parts = []
    for condition in predicates:
        text = condition
        for alias in sorted(aliases, key=len, reverse=True):
            text = re.sub(rf'(?i)(?<![\w$])"?{re.escape(alias)}"?\s*\.', "src.", text)
        parts.append(f"({text})")
    return "\n  AND ".join(parts)
