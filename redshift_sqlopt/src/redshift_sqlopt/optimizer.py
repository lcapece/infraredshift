"""The optimizer entry point.

Pipeline, in order:

1. Parse. Failure here is terminal for rewriting but not for analysis — plan and
   catalog findings never needed the AST.
2. Explode views, so reasoning happens against real base tables rather than a
   name that hides a two-billion-row scan.
3. Apply Redshift-specific rules, each of which refuses unless its precondition
   is proven.
4. Validate the result. A rewrite that changes the table set, invents a column,
   or alters the projection contract is discarded, not shipped.
5. Attach plan evidence and rank everything cheapest-fix-first.

The validation step in (4) is the reason this package can be trusted: no rewrite
leaves here without passing it, whether it came from a rule today or from a
model later.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from .catalog import Catalog, table_identities
from .fingerprint import fingerprint
from .models import (
    AppliedRewrite,
    BlockedRewrite,
    Finding,
    OptimizationResult,
    ParseFailure,
    PlanEvidence,
)
from .plan import evidence_from_rows, findings_from_evidence
from .rules import ALL_RULES
from .views import explode_views

DIALECT = "redshift"


def optimize(
    sql: str,
    *,
    catalog: Catalog | None = None,
    explain_rows: list[dict] | None = None,
    detail_rows: list[dict] | None = None,
    expand_views: bool = True,
    dialect: str = DIALECT,
) -> OptimizationResult:
    """Analyze and rewrite one already-executed Redshift query.

    ``catalog`` supplies distribution/sort keys and view bodies. Without it the
    rules that require proof will refuse rather than guess, so results degrade
    to findings-only — which is the intended behaviour, not a failure.
    """
    catalog = catalog or Catalog()
    result = OptimizationResult(original_sql=sql)

    result.fingerprint, result.fingerprint_method = fingerprint(sql, dialect=dialect)

    evidence: list[PlanEvidence] = evidence_from_rows(explain_rows, detail_rows)

    try:
        tree = sqlglot.parse_one(sql, read=dialect)
        if tree is None:
            raise ValueError("statement parsed to nothing")
    except Exception as exc:
        # Terminal for rewriting only. Plan-derived findings still stand, so the
        # caller gets DDL recommendations even for SQL we could not parse.
        result.parse_failure = ParseFailure(
            reason=str(exc).splitlines()[0][:300],
            sql=sql,
        )
        result.findings = findings_from_evidence(evidence, catalog, [])
        return result

    working = tree
    if expand_views and catalog.views:
        working, exploded = explode_views(working, catalog)
        result.exploded_views = tuple(exploded)

    referenced = table_identities(working)

    applied: list[AppliedRewrite] = []
    blocked: list[BlockedRewrite] = []
    for rule in ALL_RULES:
        try:
            candidate, rule_applied, rule_blocked = rule(working, catalog)
        except Exception as exc:  # a rule bug must not take down the analysis
            blocked.append(
                BlockedRewrite(
                    code=getattr(rule, "__name__", "rule"),
                    title="Rule raised an error",
                    reason=f"{type(exc).__name__}: {str(exc)[:200]}",
                )
            )
            continue
        blocked.extend(rule_blocked)
        if not rule_applied:
            continue
        notes = validate_rewrite(working, candidate, dialect=dialect)
        rejected = [note for note in notes if note.startswith("BLOCKED")]
        if rejected:
            for item in rule_applied:
                blocked.append(
                    BlockedRewrite(
                        code=item.code,
                        title=item.title,
                        reason="; ".join(rejected),
                        precondition=item.precondition,
                        would_have_done=item.rationale,
                    )
                )
            continue
        working = candidate
        applied.extend(
            AppliedRewrite(
                code=item.code,
                title=item.title,
                rationale=item.rationale,
                precondition=item.precondition,
                validation_notes=tuple(notes),
            )
            for item in rule_applied
        )

    result.applied = applied
    result.blocked = blocked
    if applied:
        result.rewritten_sql = working.sql(dialect=dialect, pretty=True)

    result.findings = findings_from_evidence(evidence, catalog, referenced)
    return result


def validate_rewrite(
    original: exp.Expression,
    rewritten: exp.Expression,
    *,
    dialect: str = DIALECT,
) -> list[str]:
    """Structural safety gate for a candidate rewrite.

    Returns human-readable notes; any note beginning with ``BLOCKED`` means the
    candidate must be discarded. These checks are necessary but not sufficient
    for equivalence — nothing short of running both queries proves that — so the
    final note always says validation is still required. Their job is to catch
    the rewrite that quietly drops a table or invents a column.
    """
    notes: list[str] = []
    try:
        rendered = rewritten.sql(dialect=dialect)
        reparsed = sqlglot.parse_one(rendered, read=dialect)
    except Exception as exc:
        return [f"BLOCKED: rewritten SQL does not parse as {dialect} SQL: {exc}"]
    if reparsed is None:
        return ["BLOCKED: rewritten SQL parsed to nothing"]

    original_tables = sorted(_identity(t) for t in original.find_all(exp.Table))
    rewritten_tables = sorted(_identity(t) for t in reparsed.find_all(exp.Table))
    if original_tables != rewritten_tables:
        added = sorted(set(rewritten_tables) - set(original_tables))
        removed = sorted(set(original_tables) - set(rewritten_tables))
        detail = []
        if added:
            detail.append(f"added {', '.join(added)}")
        if removed:
            detail.append(f"removed {', '.join(removed)}")
        notes.append("BLOCKED: rewrite changed the referenced table set: " + "; ".join(detail))
    else:
        notes.append("Referenced table set is unchanged")

    original_columns = {_column_key(c) for c in original.find_all(exp.Column)}
    rewritten_columns = {_column_key(c) for c in reparsed.find_all(exp.Column)}
    invented = sorted(rewritten_columns - original_columns)
    if invented:
        notes.append(
            "BLOCKED: rewrite introduced column reference(s) absent from the original: "
            + ", ".join(".".join(part for part in item if part) for item in invented)
        )
    else:
        notes.append("No new column identifiers were introduced")

    original_select = original if isinstance(original, exp.Select) else original.find(exp.Select)
    rewritten_select = reparsed if isinstance(reparsed, exp.Select) else reparsed.find(exp.Select)
    if original_select is not None and rewritten_select is not None:
        before = [_projection_key(item) for item in original_select.expressions or []]
        after = [_projection_key(item) for item in rewritten_select.expressions or []]
        if before != after:
            notes.append(
                f"BLOCKED: rewrite changed the projection contract "
                f"({len(before)} -> {len(after)} column(s), or order/aliases differ)"
            )
        else:
            notes.append("Top-level projection contract is unchanged")

    tautology = _introduced_tautology(original, reparsed)
    if tautology:
        notes.append(
            f"BLOCKED: rewrite introduced a self-referential predicate ({tautology}), "
            "which is always true and would change the result set"
        )
    else:
        notes.append("No self-referential predicate was introduced")

    notes.append("EXPLAIN and a representative row-count comparison are still required")
    return notes


def _introduced_tautology(
    original: exp.Expression,
    rewritten: exp.Expression,
) -> str:
    """Return a description of any always-true equality the rewrite added.

    Catches the class of error where a rewrite builds a correlation predicate
    whose two sides resolve identically — ``WHERE k = k``. Structurally such a
    rewrite is invisible: same tables, same columns, same projection. Semantically
    it destroys the query, because it turns a correlated anti-join into an
    unconditional existence test.

    Only *introduced* predicates are reported. A tautology already present in the
    original is the author's business, not a regression this gate should block.
    """

    def self_equalities(tree: exp.Expression) -> set[str]:
        found: set[str] = set()
        for node in tree.find_all(exp.EQ):
            left = node.this
            right = node.expression
            if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
                continue
            if left.sql(dialect=DIALECT, normalize=True) == right.sql(
                dialect=DIALECT, normalize=True
            ):
                found.add(node.sql(dialect=DIALECT, normalize=True))
        return found

    introduced = self_equalities(rewritten) - self_equalities(original)
    return ", ".join(sorted(introduced))


def _identity(table: exp.Table) -> str:
    parts = [p for p in (table.catalog, table.db, table.name) if p]
    return ".".join(str(p).strip('"').lower() for p in parts)


def _column_key(column: exp.Column) -> tuple[str, str]:
    return (
        str(column.table or "").strip('"').lower(),
        str(column.name or "").strip('"').lower(),
    )


def _projection_key(node: exp.Expression) -> str:
    """Identify a projection by output name where possible, else by expression."""
    if isinstance(node, exp.Alias):
        return str(node.alias or "").strip('"').lower()
    if isinstance(node, exp.Column):
        return str(node.name or "").strip('"').lower()
    return node.sql(dialect=DIALECT, normalize=True).lower()
