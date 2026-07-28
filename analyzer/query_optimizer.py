"""Deterministic, telemetry-grounded Amazon Redshift query rewrites.

This module deliberately does not use an LLM.  It parses SQL into an AST,
applies only conservative transformations with explicit semantic guards, and
uses SQL Lens table telemetry to decide when a rewrite is relevant.  Changes
that need business knowledge remain advisories instead of executable SQL.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Iterable

import pandas as pd

from ._lazy_sqlglot import exp, sqlglot
from .sql_lens import SQLLensAnalysis, analyze_console_sql


from .redshift_meta import MISSING_SORTKEY_VALUES, is_missing_sortkey

_DATE_LITERAL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_AUTO_KEY_VALUES = set(MISSING_SORTKEY_VALUES)


@dataclass(frozen=True)
class OptimizationChange:
    rule_id: str
    title: str
    evidence: str
    expected_effect: str
    confidence: str = "high"
    score: float = 0.0


@dataclass(frozen=True)
class OptimizationAdvisory:
    advisory_id: str
    severity: str
    title: str
    evidence: str
    next_step: str


@dataclass(frozen=True)
class DecompositionStage:
    stage_name: str
    source: str
    diststyle: str
    distkey: str
    sortkeys: tuple[str, ...]
    rationale: str
    estimated_source_gb: float


@dataclass(frozen=True)
class DecompositionPlan:
    plan_id: str
    title: str
    script: str
    stages: tuple[DecompositionStage, ...]
    rationale: str
    score: float
    warnings: tuple[str, ...] = ()


@dataclass
class RedshiftOptimization:
    parse_ok: bool
    original_sql: str
    rewritten_sql: str = ""
    parse_error: str = ""
    changes: list[OptimizationChange] = field(default_factory=list)
    advisories: list[OptimizationAdvisory] = field(default_factory=list)
    decompositions: list[DecompositionPlan] = field(default_factory=list)
    validation_notes: list[str] = field(default_factory=list)
    score: float = 0.0

    @property
    def changed(self) -> bool:
        return bool(self.changes and self.rewritten_sql.strip())


@dataclass(frozen=True)
class FriendlyFix:
    """Plain-language recommendation for people who do not tune SQL for a living."""

    status: str
    headline: str
    explanation: str
    safety_label: str
    option_label: str
    sql: str
    is_multistep: bool
    can_apply_in_editor: bool
    why_it_helps: tuple[str, ...]
    next_steps: tuple[str, ...]
    things_to_check: tuple[str, ...] = ()


def optimize_redshift_sql(
    sql_text: object,
    table_review: pd.DataFrame | None,
    known_queries: pd.DataFrame | None = None,
    view_definitions: pd.DataFrame | None = None,
    *,
    analysis: SQLLensAnalysis | None = None,
) -> RedshiftOptimization:
    """Return a conservative Redshift rewrite plus evidence-only advisories.

    The returned SQL is never executed.  A caller must still compare EXPLAIN
    plans and validate result equivalence on representative data.
    """
    original = str(sql_text or "").strip()
    if not original:
        return RedshiftOptimization(False, original, parse_error="Paste a SQL statement first.")
    try:
        original_tree = sqlglot.parse_one(original, read="redshift")
    except Exception as exc:
        return RedshiftOptimization(False, original, parse_error=str(exc).splitlines()[0][:400])

    analysis = analysis or analyze_console_sql(original, table_review, known_queries, view_definitions)
    if not analysis.parse_ok:
        return RedshiftOptimization(False, original, parse_error=analysis.parse_error)

    result = RedshiftOptimization(True, original)
    result.advisories.extend(_build_advisories(original_tree, analysis))

    if not isinstance(original_tree, exp.Select):
        result.rewritten_sql = original_tree.sql(dialect="redshift", pretty=True)
        result.advisories.insert(
            0,
            OptimizationAdvisory(
                "statement-type",
                "info",
                "Automatic rewrites are limited to SELECT statements",
                f"Parsed statement type: {type(original_tree).__name__}.",
                "Use the findings as review guidance; no DML or DDL is rewritten automatically.",
            ),
        )
        return result

    working = original_tree.copy()
    alias_meta = _alias_metadata(analysis.tables)
    changes: list[OptimizationChange] = []
    changes.extend(_rewrite_sortkey_date_filters(working, alias_meta))
    changes.extend(_rewrite_sortkey_year_filters(working, alias_meta))
    changes.extend(_propagate_inner_join_filters(working, alias_meta))
    changes.extend(_remove_redundant_distinct(working))

    rewritten = working.sql(dialect="redshift", pretty=True)
    validation = _validate_rewrite(original_tree, working)
    result.validation_notes.extend(validation)
    if any(note.startswith("BLOCKED:") for note in validation):
        result.advisories.insert(
            0,
            OptimizationAdvisory(
                "rewrite-guard",
                "crit",
                "Candidate rewrite failed a structural safety guard",
                "; ".join(validation),
                "Keep the original SQL and review the detected opportunities manually.",
            ),
        )
        result.rewritten_sql = original_tree.sql(dialect="redshift", pretty=True)
        return result

    result.changes = changes
    result.rewritten_sql = rewritten
    result.score = round(min(100.0, sum(change.score for change in changes)), 1)
    result.decompositions = _build_decomposition_plans(working, analysis, alias_meta)
    return result


def optimization_report_text(result: RedshiftOptimization) -> str:
    lines = [
        "DETERMINISTIC REDSHIFT OPTIMIZER",
        "No AI service or model was used. No SQL was executed.",
        "",
    ]
    if not result.parse_ok:
        return "\n".join(lines + [f"Parse failed: {result.parse_error}"])
    lines.append(f"Safe rewrite changes: {len(result.changes)} | opportunity score: {result.score:.1f}/100")
    for index, change in enumerate(result.changes, start=1):
        lines.extend(
            [
                "",
                f"{index}. {change.title} [{change.confidence.upper()}]",
                f"   Evidence: {change.evidence}",
                f"   Expected effect: {change.expected_effect}",
            ]
        )
    if result.advisories:
        lines.extend(["", "REVIEW-ONLY ADVISORIES"])
        for index, item in enumerate(result.advisories, start=1):
            lines.extend(
                [
                    f"{index}. [{item.severity.upper()}] {item.title}",
                    f"   Evidence: {item.evidence}",
                    f"   Next: {item.next_step}",
                ]
            )
    if result.decompositions:
        lines.extend(["", "MULTI-STEP DECOMPOSITION PLANS"])
        for index, plan in enumerate(result.decompositions, start=1):
            lines.append(f"{index}. {plan.title} | {len(plan.stages)} stage(s) | score {plan.score:.1f}/100")
            lines.append(f"   {plan.rationale}")
            for stage in plan.stages:
                design = stage.diststyle
                if stage.distkey:
                    design += f" DISTKEY({stage.distkey})"
                if stage.sortkeys:
                    design += f" SORTKEY({', '.join(stage.sortkeys)})"
                lines.append(
                    f"   - {stage.stage_name}: {design}; source ~{stage.estimated_source_gb:,.1f} GB; {stage.rationale}"
                )
    if result.validation_notes:
        lines.extend(["", "STRUCTURAL VALIDATION", *[f"- {note}" for note in result.validation_notes]])
    lines.extend(
        [
            "",
            "Required before use: compare original and rewritten EXPLAIN plans, then compare results on representative data.",
        ]
    )
    return "\n".join(lines)


def build_friendly_fix(result: RedshiftOptimization) -> FriendlyFix:
    """Choose one recommendation and explain it without optimizer terminology."""
    if not result.parse_ok:
        return FriendlyFix(
            "error",
            "I could not read this query yet",
            "The query may be incomplete or contain syntax this fixer could not understand.",
            "Nothing was changed",
            "Original query",
            result.original_sql,
            False,
            False,
            (),
            ("Check the highlighted query for a missing comma, quote, or parenthesis, then try again.",),
            (result.parse_error,) if result.parse_error else (),
        )

    plans = sorted(result.decompositions, key=lambda item: item.score, reverse=True)
    best_plan = plans[0] if plans else None
    use_plan = best_plan is not None and (
        not result.changed or best_plan.score >= max(55.0, result.score + 20.0)
    )
    checks = tuple(_friendly_advisory(item) for item in result.advisories[:4])

    if use_plan and best_plan is not None:
        if best_plan.plan_id == "filtered-fact-heart":
            why = (
                "It pulls only the needed rows and columns from the largest table before doing the joins.",
                "The temporary result is arranged around the columns used by the rest of the query.",
                "This can sharply reduce how much data Redshift has to move and scan.",
            )
        else:
            why = (
                "It saves the expensive middle part of the query once instead of rebuilding it repeatedly.",
                "Each temporary result is arranged for the joins and filters that follow.",
                "The work is split into smaller steps that are easier to inspect if something is slow.",
            )
        return FriendlyFix(
            "ready_multistep",
            "A stronger staged version is ready",
            "This query will likely benefit more from being run in a few well-designed steps than as one large statement.",
            "Strong recommendation - verify the result once",
            "Recommended: staged fix for this large query",
            best_plan.script,
            True,
            False,
            why,
            (
                "Click Copy Recommended Fix below.",
                "Paste the entire script into one Redshift query window.",
                "Run the entire script together; its temporary tables last only for that connection.",
                "The first time, compare its row count or result with the original query.",
            ),
            checks,
        )

    if result.changed:
        return FriendlyFix(
            "ready_single",
            "A simpler, faster version is ready",
            "I found a conservative change that keeps the same tables and output columns while making the query easier for Redshift to process.",
            "Low-risk rewrite - verify the result once",
            "Recommended: simpler one-query fix",
            result.rewritten_sql,
            False,
            True,
            tuple(_friendly_change(change) for change in result.changes),
            (
                "Click Put Fix in Editor to review it in SQL Lens, or copy it.",
                "Run it the same way you run the original query.",
                "The first time, compare its row count or result with the original query.",
            ),
            checks,
        )

    if result.advisories:
        return FriendlyFix(
            "review",
            "I found likely problems, but no change I can make safely",
            "The fixer will not guess when a change could alter the answer. The items below explain what needs attention.",
            "Review needed - original query kept",
            "Original query (no automatic rewrite)",
            result.original_sql,
            False,
            False,
            checks,
            ("Open Technical Details if someone familiar with the data can review these items.",),
            (),
        )

    return FriendlyFix(
        "no_change",
        "No clear automatic improvement was found",
        "The query may already be reasonable, or the loaded table information is not enough to justify a safe rewrite.",
        "Original query kept",
        "Original query",
        result.original_sql,
        False,
        False,
        (),
        ("Keep the original query. Use Technical Details if you want to inspect the analysis.",),
        (),
    )


def _friendly_change(change: OptimizationChange) -> str:
    messages = {
        "bare-sortkey-date-range": "It changes the date check so Redshift can skip unrelated blocks instead of checking every row.",
        "bare-sortkey-year-range": "It changes the year check into a date range so Redshift can skip unrelated blocks.",
        "propagate-inner-join-sort-filter": "It gives both sides of the join the same filter so each table can discard unneeded data sooner.",
        "remove-redundant-distinct": "It removes a duplicate-removal step that the GROUP BY had already performed.",
    }
    return messages.get(change.rule_id, change.expected_effect)


def _friendly_advisory(item: OptimizationAdvisory) -> str:
    advisory_id = item.advisory_id.lower()
    if advisory_id == "select-star":
        return "The query asks for every column. Listing only the columns actually needed may make it faster."
    if advisory_id == "union-distinct":
        return "The query removes duplicates between combined results. Keep this unless duplicates are acceptable."
    if advisory_id.startswith("stats-"):
        return "Redshift's information about a table is old; ask an administrator to refresh its statistics."
    if advisory_id.startswith("unsorted-"):
        return "A table has lost much of its useful sort order; table maintenance may be needed."
    if advisory_id.startswith("metadata-"):
        return "Some table details are missing or ambiguous, so the fixer refused to guess."
    if advisory_id.startswith("join-"):
        return "A join may be moving more data between Redshift nodes than necessary."
    return item.title.rstrip(".") + "."


def _rewrite_sortkey_date_filters(
    tree: exp.Expression,
    alias_meta: dict[str, dict],
) -> list[OptimizationChange]:
    changes: list[OptimizationChange] = []
    for equality in list(tree.find_all(exp.EQ)):
        if equality.find_ancestor(exp.Where) is None:
            continue
        matched = _date_wrapped_equality(equality)
        if matched is None:
            continue
        column, date_text, wrapper = matched
        meta = _metadata_for_sort_column(column, alias_meta)
        if meta is None:
            continue
        column_sql = column.sql(dialect="redshift")
        replacement = _parse_condition(
            f"{column_sql} >= DATE '{date_text}' "
            f"AND {column_sql} < DATEADD(day, 1, DATE '{date_text}')"
        )
        equality.replace(replacement)
        changes.append(
            OptimizationChange(
                "bare-sortkey-date-range",
                "Replace a function-wrapped sort-key date equality with a half-open range",
                _table_evidence(meta, f"{wrapper} wraps leading sort key {column_sql}"),
                "Allows Redshift zone-map pruning on the bare leading sort key while preserving the selected calendar day.",
                score=_benefit_score(meta, 28),
            )
        )
    return changes


def _rewrite_sortkey_year_filters(
    tree: exp.Expression,
    alias_meta: dict[str, dict],
) -> list[OptimizationChange]:
    changes: list[OptimizationChange] = []
    for equality in list(tree.find_all(exp.EQ)):
        if equality.find_ancestor(exp.Where) is None:
            continue
        matched = _year_wrapped_equality(equality)
        if matched is None:
            continue
        column, year = matched
        meta = _metadata_for_sort_column(column, alias_meta)
        if meta is None:
            continue
        column_sql = column.sql(dialect="redshift")
        replacement = _parse_condition(
            f"{column_sql} >= DATE '{year:04d}-01-01' "
            f"AND {column_sql} < DATE '{year + 1:04d}-01-01'"
        )
        equality.replace(replacement)
        changes.append(
            OptimizationChange(
                "bare-sortkey-year-range",
                "Replace YEAR/EXTRACT on a sort key with a bounded range",
                _table_evidence(meta, f"year extraction wraps leading sort key {column_sql}"),
                "Makes the predicate eligible for block pruning instead of evaluating a function for every scanned row.",
                score=_benefit_score(meta, 30),
            )
        )
    return changes


def _propagate_inner_join_filters(
    tree: exp.Expression,
    alias_meta: dict[str, dict],
) -> list[OptimizationChange]:
    changes: list[OptimizationChange] = []
    select = tree if isinstance(tree, exp.Select) else None
    if select is None:
        return changes
    where = select.args.get("where")
    if where is None:
        return changes
    terms = _and_terms(where.this)
    simple_filters: list[tuple[exp.Expression, exp.Column]] = []
    existing_filter_columns: set[tuple[str, str]] = set()
    for term in terms:
        item = _simple_column_filter(term)
        if item is None:
            continue
        filter_expr, filter_column = item
        simple_filters.append((filter_expr, filter_column))
        existing_filter_columns.add(_column_key(filter_column))

    additions: list[exp.Expression] = []
    for join in select.args.get("joins") or []:
        side = str(join.args.get("side") or "").upper()
        kind = str(join.args.get("kind") or "").upper()
        if side not in {"", "INNER"} or kind == "CROSS":
            continue
        on_expr = join.args.get("on")
        if on_expr is None:
            continue
        for equality in on_expr.find_all(exp.EQ):
            left, right = equality.this, equality.expression
            if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
                continue
            for source, target in ((left, right), (right, left)):
                target_key = _column_key(target)
                if target_key in existing_filter_columns:
                    continue
                target_meta = _metadata_for_sort_column(target, alias_meta)
                if target_meta is None:
                    continue
                for filter_expr, filter_column in simple_filters:
                    if _column_key(filter_column) != _column_key(source):
                        continue
                    propagated = filter_expr.copy().transform(
                        lambda node: target.copy()
                        if isinstance(node, exp.Column) and _column_key(node) == _column_key(source)
                        else node
                    )
                    additions.append(propagated)
                    existing_filter_columns.add(target_key)
                    changes.append(
                        OptimizationChange(
                            "propagate-inner-join-sort-filter",
                            "Propagate an inner-join filter to the other table's leading sort key",
                            _table_evidence(
                                target_meta,
                                f"{source.sql(dialect='redshift')} = {target.sql(dialect='redshift')} and the source side is already filtered",
                            ),
                            "Gives Redshift an explicit predicate on both collocated values so both scans can prune blocks.",
                            score=_benefit_score(target_meta, 22),
                        )
                    )
                    break

    combined = where.this
    for addition in additions:
        combined = exp.and_(combined, addition)
    if additions:
        where.set("this", combined)
    return changes


def _remove_redundant_distinct(tree: exp.Expression) -> list[OptimizationChange]:
    changes: list[OptimizationChange] = []
    for select in tree.find_all(exp.Select):
        if select.args.get("distinct") is None:
            continue
        group = select.args.get("group")
        if group is None or not group.expressions:
            continue
        if any(isinstance(item, exp.Literal) and not item.is_string for item in group.expressions):
            continue
        projected = {
            _canonical_expression(item.this if isinstance(item, exp.Alias) else item)
            for item in select.expressions
        }
        grouped = {_canonical_expression(item) for item in group.expressions}
        if not grouped or not grouped.issubset(projected):
            continue
        select.set("distinct", None)
        changes.append(
            OptimizationChange(
                "remove-redundant-distinct",
                "Remove DISTINCT when every GROUP BY key is already projected",
                "GROUP BY already guarantees uniqueness for the complete projected grouping key.",
                "Avoids an unnecessary duplicate-elimination operation above the aggregation.",
                score=8,
            )
        )
    return changes


def _build_decomposition_plans(
    tree: exp.Select,
    analysis: SQLLensAnalysis,
    alias_meta: dict[str, dict],
) -> list[DecompositionPlan]:
    plans: list[DecompositionPlan] = []
    cte_plan = _build_cte_decomposition(tree, analysis, alias_meta)
    if cte_plan is not None:
        plans.append(cte_plan)
    heart_plan = _build_heart_decomposition(tree, analysis, alias_meta)
    if heart_plan is not None:
        plans.append(heart_plan)
    return sorted(plans, key=lambda item: (-item.score, item.plan_id))


def _build_cte_decomposition(
    tree: exp.Select,
    analysis: SQLLensAnalysis,
    alias_meta: dict[str, dict],
) -> DecompositionPlan | None:
    with_clause = tree.args.get("with")
    if with_clause is None or not with_clause.expressions:
        return None
    metadata_index = _table_metadata_index(analysis.tables)
    cte_names = {_clean(cte.alias_or_name) for cte in with_clause.expressions}
    reference_counts = {
        name: sum(1 for table in tree.find_all(exp.Table) if _clean(table.name) == name)
        for name in cte_names
    }
    outer = tree.copy()
    outer.set("with", None)
    stage_map: dict[str, str] = {}
    stages: list[DecompositionStage] = []
    statements: list[str] = []
    staged_names: set[str] = set()

    for cte in with_clause.expressions:
        name = _clean(cte.alias_or_name)
        if not name or not isinstance(cte.this, exp.Select):
            continue
        body = cte.this.copy()
        source_gb = _expression_source_gb(body, metadata_index)
        complexity = (
            2 * sum(1 for _ in body.find_all(exp.Join))
            + 2 * sum(1 for _ in body.find_all(exp.Group))
            + sum(1 for _ in body.find_all(exp.Window))
            + sum(1 for _ in body.find_all(exp.Subquery))
            + int(body.args.get("distinct") is not None)
        )
        repeated = reference_counts.get(name, 0) >= 2
        if not repeated and complexity < 2:
            continue
        if source_gb and source_gb < 0.1 and not repeated:
            continue
        body = _replace_named_tables(body, stage_map)
        output_columns, has_star = _select_output_columns(body)
        aliases = _reference_aliases(outer, name)
        distkey, sortkeys, key_reason = _choose_stage_keys(
            outer,
            aliases,
            output_columns,
            has_star,
            alias_meta,
        )
        stage_name = _safe_temp_name(f"tmp_opt_{name}", set(stage_map.values()))
        stage_map[name] = stage_name
        staged_names.add(name)
        diststyle = "KEY" if distkey else "EVEN"
        reason_bits = []
        if repeated:
            reason_bits.append(f"referenced {reference_counts.get(name, 0)} times")
        if complexity:
            reason_bits.append(f"complexity score {complexity}")
        if key_reason:
            reason_bits.append(key_reason)
        rationale = "; ".join(reason_bits) or "materialize a reusable intermediate"
        stages.append(
            DecompositionStage(
                stage_name,
                name,
                diststyle,
                distkey,
                tuple(sortkeys),
                rationale,
                source_gb,
            )
        )
        statements.append(_temp_stage_sql(stage_name, body.sql(dialect="redshift", pretty=True), distkey, sortkeys))

    if not stages:
        return None
    final_tree = _replace_named_tables(tree.copy(), stage_map)
    final_with = final_tree.args.get("with")
    if final_with is not None:
        remaining = [cte for cte in final_with.expressions if _clean(cte.alias_or_name) not in staged_names]
        if remaining:
            final_with.set("expressions", remaining)
        else:
            final_tree.set("with", None)
    script = _decomposition_script(statements, final_tree.sql(dialect="redshift", pretty=True))
    score = min(100.0, 35.0 + len(stages) * 14.0 + sum(min(stage.estimated_source_gb, 20) for stage in stages))
    return DecompositionPlan(
        "cte-heart-staging",
        "Materialize the query's expensive CTE heart into designed temp tables",
        script,
        tuple(stages),
        "Cuts repeated or complex intermediate work into inspectable stages and designs each stage for its downstream joins and filters.",
        round(score, 1),
        (
            "Run the script in one Redshift session because temporary tables are session-scoped.",
            "CTAS normally creates initial statistics; rerun ANALYZE only if a stage is modified afterward.",
        ),
    )


def _build_heart_decomposition(
    tree: exp.Select,
    analysis: SQLLensAnalysis,
    alias_meta: dict[str, dict],
) -> DecompositionPlan | None:
    tables = _top_level_tables(tree)
    if len(tables) < 2:
        return None
    where = tree.args.get("where")
    if where is None:
        return None
    terms = _and_terms(where.this)
    # With multiple tables, unqualified columns make projection ownership
    # ambiguous. Refuse to guess which columns the staged table must expose.
    if any(not _clean(column.table) for column in tree.find_all(exp.Column)):
        return None
    candidates: list[tuple[float, exp.Table, dict, list[exp.Expression]]] = []
    for table in tables:
        alias = _clean(table.alias_or_name)
        meta = alias_meta.get(alias)
        if not alias or not meta:
            continue
        local_terms = [term for term in terms if _term_belongs_to_alias(term, alias)]
        if not local_terms:
            continue
        size_gb = _num(meta.get("size_mb")) / 1024.0
        if size_gb < 1.0:
            continue
        score = size_gb * (1.0 + _num(meta.get("full_scan_score")) / 100.0) + len(local_terms) * 8.0
        candidates.append((score, table, meta, local_terms))
    if not candidates:
        return None
    _, heart, meta, local_terms = max(candidates, key=lambda item: item[0])
    alias = _clean(heart.alias_or_name)
    required_columns = sorted({_clean(column.name) for column in tree.find_all(exp.Column) if _clean(column.table) == alias})
    if not required_columns:
        return None
    projection = ",\n  ".join(f"{_identifier(alias)}.{_identifier(column)}" for column in required_columns)
    local_filter = "\n  AND ".join(term.sql(dialect="redshift", pretty=False) for term in local_terms)
    source = heart.copy()
    source.set("alias", heart.args.get("alias").copy() if heart.args.get("alias") is not None else None)
    source_sql = source.sql(dialect="redshift", pretty=False)
    stage_select = f"SELECT\n  {projection}\nFROM {source_sql}\nWHERE {local_filter}"
    distkey, sortkeys, key_reason = _choose_stage_keys(
        tree,
        {alias},
        set(required_columns),
        False,
        alias_meta,
    )
    stage_name = _safe_temp_name(f"tmp_opt_{_clean(heart.name)}_heart", set())
    final_tree = tree.copy()

    def replace_heart(node):
        if isinstance(node, exp.Table) and _clean(node.alias_or_name) == alias and _clean(node.name) == _clean(heart.name):
            replacement = exp.Table(this=exp.Identifier(this=stage_name, quoted=False))
            if node.args.get("alias") is not None:
                replacement.set("alias", node.args["alias"].copy())
            return replacement
        return node

    final_tree = final_tree.transform(replace_heart)
    size_gb = _num(meta.get("size_mb")) / 1024.0
    diststyle = "KEY" if distkey else "EVEN"
    rationale = (
        f"{meta.get('query_table') or heart.sql(dialect='redshift')} is the largest safely prefiltered input "
        f"(~{size_gb:,.1f} GB) with {len(local_terms)} alias-local predicate(s); {key_reason or 'even distribution is safer than guessing a key'}"
    )
    stage = DecompositionStage(
        stage_name,
        heart.sql(dialect="redshift", pretty=False),
        diststyle,
        distkey,
        tuple(sortkeys),
        rationale,
        size_gb,
    )
    script = _decomposition_script(
        [_temp_stage_sql(stage_name, stage_select, distkey, sortkeys)],
        final_tree.sql(dialect="redshift", pretty=True),
    )
    score = min(100.0, 42.0 + min(30.0, size_gb) + len(local_terms) * 6.0)
    return DecompositionPlan(
        "filtered-fact-heart",
        "Extract the filtered fact-side heart before the remaining joins",
        script,
        (stage,),
        "Projects only referenced columns, applies safe single-alias predicates before the large join graph, and physically lays out the intermediate for downstream work.",
        round(score, 1),
        (
            "The original filter remains in the final query as a redundant semantic guard.",
            "Compare stage row count and the final result with the original before operational use.",
        ),
    )


def _choose_stage_keys(
    expression: exp.Expression,
    aliases: set[str],
    output_columns: set[str],
    has_star: bool,
    alias_meta: dict[str, dict],
) -> tuple[str, list[str], str]:
    aliases = {_clean(alias) for alias in aliases if _clean(alias)}
    join_scores: dict[str, float] = {}
    filter_scores: dict[str, float] = {}
    for join in expression.find_all(exp.Join):
        on_expr = join.args.get("on")
        if on_expr is None:
            continue
        for equality in on_expr.find_all(exp.EQ):
            left, right = equality.this, equality.expression
            if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
                continue
            for own, other in ((left, right), (right, left)):
                if _clean(own.table) not in aliases:
                    continue
                column = _clean(own.name)
                if not has_star and column not in output_columns:
                    continue
                score = 10.0
                other_meta = alias_meta.get(_clean(other.table))
                if other_meta and _clean(str(other_meta.get("distkey") or "")) == _clean(other.name):
                    score += 30.0
                if other_meta:
                    score += min(20.0, math.log10(max(1.0, _num(other_meta.get("size_mb")))) * 4.0)
                join_scores[column] = join_scores.get(column, 0.0) + score
    for where in expression.find_all(exp.Where):
        for term in _and_terms(where.this):
            item = _simple_column_filter(term)
            if item is None:
                continue
            filter_expr, column_expr = item
            if _clean(column_expr.table) not in aliases:
                continue
            column = _clean(column_expr.name)
            if not has_star and column not in output_columns:
                continue
            weight = 18.0 if isinstance(filter_expr, (exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Between)) else 12.0
            filter_scores[column] = filter_scores.get(column, 0.0) + weight
    distkey = max(join_scores, key=join_scores.get) if join_scores else ""
    sort_order = sorted(filter_scores, key=lambda col: (-filter_scores[col], col))
    if distkey and distkey not in sort_order:
        sort_order.append(distkey)
    sortkeys = sort_order[:2]
    reasons = []
    if distkey:
        reasons.append(f"DISTKEY {distkey} selected from downstream equality joins")
    if sortkeys:
        reasons.append(f"SORTKEY {', '.join(sortkeys)} selected from downstream filters/join order")
    return distkey, sortkeys, "; ".join(reasons)


def _temp_stage_sql(stage_name: str, select_sql: str, distkey: str, sortkeys: list[str]) -> str:
    lines = [f"DROP TABLE IF EXISTS {_identifier(stage_name)};", f"CREATE TEMP TABLE {_identifier(stage_name)}"]
    if distkey:
        lines.extend(["DISTSTYLE KEY", f"DISTKEY ({_identifier(distkey)})"])
    else:
        lines.append("DISTSTYLE EVEN")
    if sortkeys:
        lines.append("COMPOUND SORTKEY (" + ", ".join(_identifier(column) for column in sortkeys) + ")")
    lines.extend(["AS", select_sql.rstrip(";"), ";", "-- CTAS creates initial statistics automatically in Redshift."])
    return "\n".join(lines)


def _decomposition_script(stage_statements: list[str], final_sql: str) -> str:
    return "\n".join(
        [
            "-- REVIEW BEFORE RUNNING: deterministic Redshift decomposition",
            "-- Run the complete script in one session; temp tables disappear when the session ends.",
            "BEGIN;",
            "",
            "\n\n".join(stage_statements),
            "",
            "-- Final query over the staged heart",
            final_sql.rstrip(";") + ";",
            "",
            "COMMIT;",
        ]
    )


def _replace_named_tables(expression: exp.Expression, mapping: dict[str, str]) -> exp.Expression:
    if not mapping:
        return expression

    def replace(node):
        if isinstance(node, exp.Table) and _clean(node.name) in mapping:
            replacement = exp.Table(this=exp.Identifier(this=mapping[_clean(node.name)], quoted=False))
            if node.args.get("alias") is not None:
                replacement.set("alias", node.args["alias"].copy())
            return replacement
        return node

    return expression.transform(replace)


def _reference_aliases(expression: exp.Expression, table_name: str) -> set[str]:
    return {
        _clean(table.alias_or_name)
        for table in expression.find_all(exp.Table)
        if _clean(table.name) == _clean(table_name)
    }


def _select_output_columns(select: exp.Select) -> tuple[set[str], bool]:
    columns: set[str] = set()
    has_star = False
    for item in select.expressions:
        target = item.this if isinstance(item, exp.Alias) else item
        if isinstance(target, exp.Star) or any(True for _ in target.find_all(exp.Star)):
            has_star = True
        name = _clean(item.alias_or_name)
        if name and name != "*":
            columns.add(name)
    return columns, has_star


def _top_level_tables(select: exp.Select) -> list[exp.Table]:
    result: list[exp.Table] = []
    from_expr = select.args.get("from")
    if from_expr is not None and isinstance(from_expr.this, exp.Table):
        result.append(from_expr.this)
    for join in select.args.get("joins") or []:
        if isinstance(join.this, exp.Table):
            result.append(join.this)
    return result


def _term_belongs_to_alias(term: exp.Expression, alias: str) -> bool:
    columns = list(term.find_all(exp.Column))
    return bool(columns) and all(_clean(column.table) == _clean(alias) for column in columns)


def _table_metadata_index(table_rows: pd.DataFrame | None) -> dict[str, dict]:
    index: dict[str, dict] = {}
    if table_rows is None or table_rows.empty:
        return index
    for row in table_rows.to_dict("records"):
        for value in (row.get("query_table"), row.get("table_name")):
            key = _clean(value)
            if key:
                index.setdefault(key, row)
                index.setdefault(key.split(".")[-1], row)
    return index


def _expression_source_gb(expression: exp.Expression, metadata_index: dict[str, dict]) -> float:
    seen: set[str] = set()
    total_mb = 0.0
    for table in expression.find_all(exp.Table):
        key = _clean(table.name)
        if not key or key in seen:
            continue
        seen.add(key)
        meta = metadata_index.get(_clean(table.sql(dialect="redshift", pretty=False))) or metadata_index.get(key)
        if meta:
            total_mb += _num(meta.get("size_mb"))
    return total_mb / 1024.0


def _safe_temp_name(base: str, used: set[str]) -> str:
    cleaned = re.sub(r"[^a-z0-9_]+", "_", _clean(base)).strip("_")[:110] or "tmp_opt_stage"
    candidate = cleaned
    suffix = 2
    while candidate in used:
        candidate = f"{cleaned[:105]}_{suffix}"
        suffix += 1
    return candidate


def _identifier(value: str) -> str:
    return exp.Identifier(this=str(value), quoted=False).sql(dialect="redshift")


def _build_advisories(tree: exp.Expression, analysis: SQLLensAnalysis) -> list[OptimizationAdvisory]:
    items: list[OptimizationAdvisory] = []
    if any(True for _ in tree.find_all(exp.Star)):
        items.append(
            OptimizationAdvisory(
                "select-star",
                "warn",
                "SELECT * prevents safe projection pruning",
                "The output contract does not identify which columns consumers actually need.",
                "Replace * with the required columns; the optimizer will not guess and risk changing the result contract.",
            )
        )
    for union in tree.find_all(exp.Union):
        if union.args.get("distinct", True):
            items.append(
                OptimizationAdvisory(
                    "union-distinct",
                    "info",
                    "UNION includes duplicate elimination",
                    "Redshift must perform a distinct set operation for UNION.",
                    "Use UNION ALL only after confirming duplicate preservation is acceptable.",
                )
            )
            break
    if analysis.joins is not None and not analysis.joins.empty:
        for _, row in analysis.joins.iterrows():
            severity = str(row.get("severity") or "info")
            if severity not in {"crit", "warn"}:
                continue
            items.append(
                OptimizationAdvisory(
                    "join-" + str(row.get("join_no") or "review"),
                    severity,
                    str(row.get("distribution_alignment") or "Join distribution review"),
                    str(row.get("condition") or row.get("involved_tables") or "Captured join telemetry"),
                    str(row.get("recommendation") or "Compare the join with the captured DISTKEY and SORTKEY telemetry."),
                )
            )
    if analysis.tables is not None and not analysis.tables.empty:
        for _, row in analysis.tables.iterrows():
            table = str(row.get("query_table") or row.get("table_name") or "table")
            if _num(row.get("stats_off")) >= 20:
                items.append(
                    OptimizationAdvisory(
                        "stats-" + table.lower(),
                        "warn",
                        f"Statistics are stale for {table}",
                        f"Captured stats_off={_num(row.get('stats_off')):.0f}%.",
                        "ANALYZE the table before judging query rewrites; stale statistics can produce a misleading plan.",
                    )
                )
            if _num(row.get("unsorted_pct")) >= 20:
                items.append(
                    OptimizationAdvisory(
                        "unsorted-" + table.lower(),
                        "warn",
                        f"Sort order is degraded for {table}",
                        f"Captured unsorted percentage={_num(row.get('unsorted_pct')):.0f}%.",
                        "Address table maintenance before attributing the full scan solely to SQL shape.",
                    )
                )
            if str(row.get("match_status") or "") in {"not found", "ambiguous", "ambiguous view"}:
                items.append(
                    OptimizationAdvisory(
                        "metadata-" + table.lower(),
                        "crit",
                        f"Metadata is incomplete for {table}",
                        f"SQL Lens match status: {row.get('match_status')}.",
                        "Qualify the table or refresh SVV_TABLE_INFO before accepting a physical-design recommendation.",
                    )
                )
    return _dedupe_advisories(items)


def _validate_rewrite(original: exp.Expression, rewritten: exp.Expression) -> list[str]:
    notes: list[str] = []
    try:
        reparsed = sqlglot.parse_one(rewritten.sql(dialect="redshift"), read="redshift")
    except Exception as exc:
        return [f"BLOCKED: rewritten SQL does not parse as Redshift SQL: {exc}"]
    original_tables = sorted(_table_identity(table) for table in original.find_all(exp.Table))
    rewritten_tables = sorted(_table_identity(table) for table in reparsed.find_all(exp.Table))
    if original_tables != rewritten_tables:
        notes.append("BLOCKED: rewrite changed the referenced table set")
    else:
        notes.append("Referenced table multiset is unchanged")
    original_columns = {_column_key(col) for col in original.find_all(exp.Column)}
    rewritten_columns = {_column_key(col) for col in reparsed.find_all(exp.Column)}
    invented = sorted(rewritten_columns - original_columns)
    if invented:
        notes.append("BLOCKED: rewrite introduced unknown column reference(s): " + ", ".join(".".join(item) for item in invented))
    else:
        notes.append("No new column identifiers were introduced")
    if isinstance(original, exp.Select) and isinstance(reparsed, exp.Select):
        original_projection = [_canonical_expression(item) for item in original.expressions]
        rewritten_projection = [_canonical_expression(item) for item in reparsed.expressions]
        if original_projection != rewritten_projection:
            notes.append("BLOCKED: rewrite changed the top-level projection contract")
        else:
            notes.append("Top-level projection contract is unchanged")
    notes.append("EXPLAIN and representative result comparison are still required")
    return notes


def _date_wrapped_equality(equality: exp.EQ) -> tuple[exp.Column, str, str] | None:
    for wrapped, literal in ((equality.this, equality.expression), (equality.expression, equality.this)):
        date_text = _date_literal(literal)
        if not date_text:
            continue
        if isinstance(wrapped, exp.Date) and isinstance(wrapped.this, exp.Column):
            return wrapped.this.copy(), date_text, "DATE()"
        if isinstance(wrapped, exp.Cast) and isinstance(wrapped.this, exp.Column) and _cast_is_date(wrapped):
            return wrapped.this.copy(), date_text, "CAST(... AS DATE)"
        if isinstance(wrapped, exp.TimestampTrunc) and isinstance(wrapped.this, exp.Column):
            if str(wrapped.args.get("unit") or "").strip().upper() == "DAY":
                return wrapped.this.copy(), date_text, "DATE_TRUNC('day', ...)"
    return None


def _year_wrapped_equality(equality: exp.EQ) -> tuple[exp.Column, int] | None:
    for wrapped, literal in ((equality.this, equality.expression), (equality.expression, equality.this)):
        if not isinstance(literal, exp.Literal) or literal.is_string:
            continue
        try:
            year = int(str(literal.this))
        except (TypeError, ValueError):
            continue
        if year < 1 or year >= 9999:
            continue
        if isinstance(wrapped, exp.Year) and isinstance(wrapped.this, exp.Column):
            return wrapped.this.copy(), year
        if isinstance(wrapped, exp.Extract):
            unit = str(wrapped.this or "").strip().upper()
            column = wrapped.expression
            if unit == "YEAR" and isinstance(column, exp.Column):
                return column.copy(), year
    return None


def _date_literal(node: exp.Expression) -> str:
    if isinstance(node, exp.Literal) and node.is_string and _DATE_LITERAL_RE.fullmatch(str(node.this)):
        return str(node.this)
    if isinstance(node, exp.Cast) and _cast_is_date(node):
        inner = node.this
        if isinstance(inner, exp.Literal) and inner.is_string and _DATE_LITERAL_RE.fullmatch(str(inner.this)):
            return str(inner.this)
    return ""


def _cast_is_date(node: exp.Cast) -> bool:
    return str(node.args.get("to") or "").strip().upper() == "DATE"


def _alias_metadata(table_rows: pd.DataFrame | None) -> dict[str, dict]:
    result: dict[str, dict] = {}
    if table_rows is None or table_rows.empty:
        return result
    for row in table_rows.to_dict("records"):
        if str(row.get("component_of") or "").strip():
            continue
        alias = _clean(row.get("alias"))
        if alias and alias not in result:
            result[alias] = row
    return result


def _metadata_for_sort_column(column: exp.Column, alias_meta: dict[str, dict]) -> dict | None:
    name = _clean(column.name)
    alias = _clean(column.table)
    if alias:
        meta = alias_meta.get(alias)
        return meta if meta and _clean_sortkey(meta.get("sortkey1")) == name else None
    matches = [meta for meta in alias_meta.values() if _clean_sortkey(meta.get("sortkey1")) == name]
    return matches[0] if len(matches) == 1 else None


def _clean_sortkey(value: object) -> str:
    text = _clean(str(value or "").split(",", 1)[0].strip().strip("()"))
    return "" if text in _AUTO_KEY_VALUES or "auto" in text else text


def _table_evidence(meta: dict, detail: str) -> str:
    table = str(meta.get("query_table") or meta.get("table_name") or "table")
    size_gb = _num(meta.get("size_mb")) / 1024.0
    return (
        f"{detail}; {table} is {size_gb:,.1f} GB with sortkey={meta.get('sortkey1') or '-'}, "
        f"full-scan score={_num(meta.get('full_scan_score')):.0f}, "
        f"RR-scan usage={_num(meta.get('rrscan_query_pct')):.0f}%"
    )


def _benefit_score(meta: dict, base: float) -> float:
    size_bonus = min(14.0, math.log10(max(1.0, _num(meta.get("size_mb")))) * 3.0)
    scan_bonus = min(8.0, _num(meta.get("full_scan_score")) / 12.5)
    return round(base + size_bonus + scan_bonus, 1)


def _simple_column_filter(term: exp.Expression) -> tuple[exp.Expression, exp.Column] | None:
    if isinstance(term, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
        left, right = term.this, term.expression
        if isinstance(left, exp.Column) and not any(True for _ in right.find_all(exp.Column)):
            return term, left
        if isinstance(right, exp.Column) and not any(True for _ in left.find_all(exp.Column)):
            return term, right
    if isinstance(term, exp.Between) and isinstance(term.this, exp.Column):
        if not any(True for _ in term.args["low"].find_all(exp.Column)) and not any(
            True for _ in term.args["high"].find_all(exp.Column)
        ):
            return term, term.this
    return None


def _and_terms(node: exp.Expression) -> list[exp.Expression]:
    if isinstance(node, exp.And):
        return _and_terms(node.this) + _and_terms(node.expression)
    return [node]


def _parse_condition(sql: str) -> exp.Expression:
    parsed = sqlglot.parse_one(f"SELECT 1 WHERE {sql}", read="redshift")
    return parsed.args["where"].this


def _column_key(column: exp.Column) -> tuple[str, str]:
    return _clean(column.table), _clean(column.name)


def _table_identity(table: exp.Table) -> str:
    return ".".join(_clean(part) for part in (table.catalog, table.db, table.name) if _clean(part))


def _canonical_expression(node: exp.Expression) -> str:
    return re.sub(r"\s+", " ", node.sql(dialect="redshift", pretty=False).strip().lower())


def _clean(value: object) -> str:
    return str(value or "").strip().strip('"').lower()


def _num(value: object) -> float:
    try:
        number = float(value)
        return number if math.isfinite(number) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _dedupe_advisories(items: Iterable[OptimizationAdvisory]) -> list[OptimizationAdvisory]:
    result: list[OptimizationAdvisory] = []
    seen: set[tuple[str, str]] = set()
    for item in items:
        key = (item.advisory_id, item.title)
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result
