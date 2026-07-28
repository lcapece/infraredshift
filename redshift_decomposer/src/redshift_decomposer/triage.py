"""Decomposability likelihood: a fast 0.0-1.0 pre-flight for one query.

Parse-only (no catalog, no cluster) so it answers in milliseconds. The score
is the estimated **likelihood that ``decompose()`` will produce a usable,
semantically-safe staged plan** for this SQL — not a measure of how much
runtime the rewrite would save, and not a person-hours estimate.

    1.0            clean multi-table / filtered shape the planner is built for
    0.7  - 1.0     HIGH       - strong chance of a usable plan
    0.4  - 0.7     MODERATE   - plan likely, but expect review findings
    0.05 - 0.4     LOW        - plan may be empty, unsafe, or heavily qualified
    0.0  - 0.05    UNLIKELY   - unparseable, not a SELECT, or blocked shape

Deductions map to real planner behavior and known limitations (CTE filter
gaps, multi-alias self-joins, set-op branch isolation, SUPER/JSON, etc.).
Every deduction is a named signal so a demo audience can see *why* a query
scores the way it does. This file is deliberately self-contained (sqlglot
is its only import) so it can be copied anywhere and run directly:

    python triage.py            # interactive: paste a query, blank line ends it
    python triage.py file.sql   # score a file
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

_JSON_FUNCTIONS = {
    "JSON_PARSE",
    "JSON_SERIALIZE",
    "JSON_EXTRACT_PATH_TEXT",
    "JSON_EXTRACT_ARRAY_ELEMENT_TEXT",
    "IS_VALID_JSON",
    "IS_VALID_JSON_ARRAY",
    "JSON_TYPEOF",
    "CAN_JSON_PARSE",
    "GET_ARRAY_LENGTH",
    "SPLIT_TO_ARRAY",
    "JSON_ARRAY_LENGTH",
    "PARSEJSON",
    "JSONEXTRACTSCALAR",
    "JSON_EXTRACT_SCALAR",
}
_NONDETERMINISTIC = {
    "GETDATE",
    "SYSDATE",
    "RANDOM",
    "CURRENT_TIMESTAMP",
    "NOW",
    "CURRENT_TIME",
    "TIMEOFDAY",
}
# Schemas commonly used for Redshift Spectrum / external catalogs. Staging
# still parses, but DISTKEY/SORTKEY and local-scan economics do not apply.
_EXTERNAL_SCHEMA_HINTS = {
    "SPECTRUM",
    "EXTERNAL",
    "S3",
    "GLUE",
    "ATHENA",
    "CATALOG",
}


@dataclass(frozen=True)
class TriageSignal:
    impact: float
    title: str
    detail: str


@dataclass
class TriageReport:
    """Likelihood report for one SQL string.

    ``score`` is in ``[0.0, 1.0]`` and is the estimated probability that
    ``decompose()`` yields a usable staged plan (aliases: ``likelihood``).
    """

    score: float
    verdict: str
    parse_ok: bool
    signals: list[TriageSignal] = field(default_factory=list)

    @property
    def likelihood(self) -> float:
        """Alias for ``score`` — estimated P(usable decomposition plan)."""
        return self.score

    @property
    def blocking(self) -> bool:
        """True when the shape is essentially not a decomposition candidate."""
        return self.score <= 0.05 or not self.parse_ok

    def summary(self) -> str:
        """Engineer-facing brief: imperfect tool, conversion likelihood, skeleton."""
        bar_units = int(round(self.score * 10))
        bar = "[" + "#" * bar_units + "-" * (10 - bar_units) + "]"
        lines = [
            "Redshift Query Decomposer is not perfect - treat output as a skeleton,",
            "not a guaranteed production rewrite. One query at a time; engineer review required.",
            f"{bar} {self.score:.2f}  estimated conversion-success likelihood",
            f"  {self.verdict}",
        ]
        if self.signals:
            lines.append("  likelihood reductions:")
            for signal in self.signals:
                lines.append(
                    f"  -{signal.impact:.2f}  {signal.title}: {signal.detail}"
                )
        elif self.parse_ok:
            lines.append(
                "  no structural risk signals - shape matches the planner happy path;"
            )
            lines.append(
                "  still validate row counts, nulls, and EXPLAIN vs the original."
            )
        return "\n".join(lines)


def _verdict(score: float) -> str:
    if score >= 0.7:
        return (
            "HIGH estimated conversion success - still a skeleton; engineer must validate"
        )
    if score >= 0.4:
        return (
            "MODERATE estimated conversion success - expect review findings; not drop-in"
        )
    if score > 0.05:
        return (
            "LOW estimated conversion success - use only as a cautious starting sketch"
        )
    return "UNLIKELY conversion success - not a reliable decomposition candidate"


def _norm(name: str | None) -> str:
    return (name or "").strip().strip('"').lower()


def _function_names(tree: exp.Expression) -> set[str]:
    names: set[str] = set()
    for node in tree.find_all(exp.Func):
        if isinstance(node, exp.Anonymous):
            names.add(str(node.this or "").upper())
        else:
            names.add(type(node).__name__.upper())
            try:
                names.add(node.sql_name().upper())
            except Exception:
                pass
    return names


def _has_super_cast(tree: exp.Expression) -> bool:
    for cast in tree.find_all(exp.Cast):
        try:
            if "SUPER" in cast.to.sql().upper():
                return True
        except Exception:
            continue
    return False


def _subquery_roots(tree: exp.Expression) -> list[exp.Expression]:
    """Expression nodes that introduce an independent nested SELECT scope."""
    roots: list[exp.Expression] = []
    for node in tree.find_all(exp.Subquery, exp.Exists, exp.Any, exp.All):
        roots.append(node)
    # Scalar / IN subqueries sometimes appear as bare Select under In/EQ/etc.
    for node in tree.find_all(exp.In, exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE):
        for side in (node.this, node.expression):
            if isinstance(side, exp.Select):
                roots.append(side)
            elif isinstance(side, exp.Subquery):
                roots.append(side)
    # de-dupe by id while preserving order
    seen: set[int] = set()
    unique: list[exp.Expression] = []
    for root in roots:
        key = id(root)
        if key in seen:
            continue
        seen.add(key)
        unique.append(root)
    return unique


def _local_aliases(scope_root: exp.Expression) -> set[str]:
    """Relation aliases defined inside *scope_root* (inclusive)."""
    aliases: set[str] = set()
    for table in scope_root.find_all(exp.Table):
        aliases.add(_norm(table.alias_or_name))
        aliases.add(_norm(table.name))
    for subq in scope_root.find_all(exp.Subquery):
        if subq is scope_root:
            continue
        alias = _norm(subq.alias_or_name)
        if alias:
            aliases.add(alias)
    for cte in scope_root.find_all(exp.CTE):
        aliases.add(_norm(cte.alias_or_name))
    return {a for a in aliases if a}


def _inner_select(node: exp.Expression) -> exp.Expression | None:
    if isinstance(node, exp.Select):
        return node
    if isinstance(node, (exp.Subquery, exp.Exists, exp.Any, exp.All)):
        inner = node.this
        if isinstance(inner, exp.Subquery):
            return _inner_select(inner)
        return inner
    return node.find(exp.Select)


def _is_truly_correlated(scope_root: exp.Expression) -> bool:
    """True when the nested scope references a relation alias defined outside it.

    sqlglot's ``is_correlated_subquery`` over-flags uncorrelated IN/EXISTS and
    UNION branches (they carry ``external_columns`` without outer aliases).
    Outer-alias column checks match the decomposer's real constraint: stages
    cannot be extracted independently when a subquery closes over outer rows.
    """
    inner = _inner_select(scope_root)
    if inner is None:
        return False
    local = _local_aliases(inner)
    # Also treat the subquery's own alias as local when present
    own_alias = _norm(getattr(scope_root, "alias_or_name", None))
    if own_alias:
        local.add(own_alias)
    for col in inner.find_all(exp.Column):
        table = _norm(col.table)
        if table and table not in local:
            return True
    return False


def _correlated_subquery_count(tree: exp.Expression) -> int:
    count = 0
    for root in _subquery_roots(tree):
        # Derived tables in FROM are normal staging fodder; only penalize when
        # they close over outer aliases.
        if _is_truly_correlated(root):
            count += 1
    return count


def _select_depth(tree: exp.Expression) -> int:
    deepest = 0
    for select in tree.find_all(exp.Select):
        depth = 0
        node = select
        while node.parent is not None:
            if isinstance(node.parent, exp.Select):
                depth += 1
            node = node.parent
        deepest = max(deepest, depth)
    return deepest


def _table_identity(table: exp.Table) -> str:
    parts = [_norm(p) for p in (table.catalog, table.db, table.name) if _norm(p)]
    return ".".join(parts)


def _multi_alias_self_joins(tree: exp.Expression) -> list[str]:
    """Physical tables scanned under 2+ distinct aliases (self-join domains).

    The planner refuses to push a single shared filter onto a multi-alias temp
    (independent domains) and labels the stage ``safety=review``.
    """
    cte_names = {
        _norm(cte.alias_or_name) for cte in tree.find_all(exp.CTE)
    }
    aliases_by_table: dict[str, set[str]] = {}
    for table in tree.find_all(exp.Table):
        name = _norm(table.name)
        # bare CTE references are not physical self-joins
        if name in cte_names and not table.db and not table.catalog:
            continue
        identity = _table_identity(table)
        if not identity:
            continue
        alias = _norm(table.alias_or_name) or identity
        aliases_by_table.setdefault(identity, set()).add(alias)
    return sorted(
        identity
        for identity, aliases in aliases_by_table.items()
        if len(aliases) >= 2
    )


def _cte_names(tree: exp.Expression) -> set[str]:
    return {_norm(cte.alias_or_name) for cte in tree.find_all(exp.CTE) if _norm(cte.alias_or_name)}


def _tables_only_inside_ctes(tree: exp.Expression) -> list[str]:
    """Physical tables that appear inside CTE bodies but never in the outer query.

    Known planner gap: outer WHERE on a CTE alias does not push into a stage
    built from a physical table that only lives inside the CTE definition, so
    the staged copy can be unfiltered.
    """
    ctes = list(tree.find_all(exp.CTE))
    if not ctes:
        return []
    inside: set[str] = set()
    cte_name_set = _cte_names(tree)
    for cte in ctes:
        for table in cte.find_all(exp.Table):
            name = _norm(table.name)
            if name in cte_name_set and not table.db and not table.catalog:
                continue
            identity = _table_identity(table)
            if identity:
                inside.add(identity)
    outside: set[str] = set()
    # Walk the main statement with CTE bodies skipped: remove WITH, scan rest
    main = tree.copy()
    if isinstance(main, exp.Expression) and main.args.get("with") is not None:
        main.set("with", None)
    for table in main.find_all(exp.Table):
        name = _norm(table.name)
        if name in cte_name_set and not table.db and not table.catalog:
            continue
        identity = _table_identity(table)
        if identity:
            outside.add(identity)
    return sorted(inside - outside)


def _outer_filters_reference_ctes(tree: exp.Expression) -> bool:
    """True when the outer (non-CTE-body) WHERE references a CTE alias."""
    cte_name_set = _cte_names(tree)
    if not cte_name_set:
        return False
    main = tree.copy()
    if main.args.get("with") is not None:
        main.set("with", None)
    for where in main.find_all(exp.Where):
        for col in where.find_all(exp.Column):
            if _norm(col.table) in cte_name_set:
                return True
            # unqualified columns on a FROM that is only CTEs still count
        # also: FROM cte WHERE col = ... with unqualified col
        from_ = main.args.get("from") if isinstance(main, exp.Select) else None
        if from_ is not None:
            src = from_.this
            if isinstance(src, exp.Table) and _norm(src.name) in cte_name_set:
                if where.find(exp.Column) is not None:
                    return True
    return False


def _set_op_with_branch_tables(tree: exp.Expression) -> tuple[int, bool]:
    """Return (set_op_count, branches_have_physical_tables)."""
    ops = list(tree.find_all(exp.Union, exp.Except, exp.Intersect))
    if not ops:
        return 0, False
    # tables under any set-op branch
    has_tables = False
    for op in ops:
        if op.find(exp.Table) is not None:
            has_tables = True
            break
    return len(ops), has_tables


def _has_pushable_where(tree: exp.Expression) -> bool:
    return tree.find(exp.Where) is not None


def _has_having_only_filters(tree: exp.Expression) -> bool:
    return tree.find(exp.Where) is None and tree.find(exp.Having) is not None


def _cross_join_without_on(tree: exp.Expression) -> int:
    count = 0
    for join in tree.find_all(exp.Join):
        side = str(join.args.get("side") or "").upper()
        kind = str(join.args.get("kind") or "").upper()
        on_expr = join.args.get("on")
        using = join.args.get("using")
        if on_expr is not None or using:
            continue
        if kind == "CROSS" or side == "CROSS" or (not kind and not side and on_expr is None):
            # bare JOIN without ON is also cross-ish in some dialects; sqlglot
            # usually sets kind. Count explicit CROSS and ON-less joins.
            count += 1
    return count


def _equijoin_count(tree: exp.Expression) -> int:
    count = 0
    for join in tree.find_all(exp.Join):
        on_expr = join.args.get("on")
        if on_expr is not None and on_expr.find(exp.EQ) is not None:
            count += 1
        elif join.args.get("using"):
            count += 1
    return count


def _external_schema_tables(tree: exp.Expression) -> list[str]:
    hits: list[str] = []
    for table in tree.find_all(exp.Table):
        schema = _norm(table.db).upper()
        catalog = _norm(table.catalog).upper()
        if schema in _EXTERNAL_SCHEMA_HINTS or catalog in _EXTERNAL_SCHEMA_HINTS:
            hits.append(_table_identity(table))
    return sorted(set(hits))


def _qualification_depth_mix(tree: exp.Expression) -> bool:
    """True when table refs mix 1-part and multi-part names (qualify skip risk)."""
    depths: set[int] = set()
    cte_name_set = _cte_names(tree)
    for table in tree.find_all(exp.Table):
        name = _norm(table.name)
        if name in cte_name_set and not table.db and not table.catalog:
            continue
        depth = sum(1 for p in (table.catalog, table.db, table.name) if _norm(p))
        if depth:
            depths.add(depth)
    return len(depths) >= 2


def _or_in_where(tree: exp.Expression) -> bool:
    for where in tree.find_all(exp.Where):
        if where.find(exp.Or) is not None:
            return True
    return False


def _distinct_physical_tables(tree: exp.Expression) -> set[str]:
    cte_name_set = _cte_names(tree)
    tables: set[str] = set()
    for table in tree.find_all(exp.Table):
        name = _norm(table.name)
        if name in cte_name_set and not table.db and not table.catalog:
            continue
        identity = _table_identity(table)
        if identity:
            tables.add(identity)
    return tables


def assess_decomposability(sql: str) -> TriageReport:
    """Estimate how likely ``decompose()`` is to succeed on *sql* (0.0 .. 1.0).

    The score is a parse-only prior over planner success: hard blockers collapse
    toward zero; shapes the planner handles cleanly stay near 1.0; known review
    cases land in the middle with named reasons.
    """
    try:
        tree = sqlglot.parse_one(sql, read="redshift")
    except Exception as error:
        return TriageReport(
            0.0,
            _verdict(0.0),
            False,
            [
                TriageSignal(
                    1.0,
                    "Does not parse as Redshift SQL",
                    str(error).splitlines()[0][:160],
                )
            ],
        )
    if tree is None:
        return TriageReport(
            0.0,
            _verdict(0.0),
            False,
            [TriageSignal(1.0, "Empty statement", "")],
        )

    signals: list[TriageSignal] = []
    score = 1.0

    def deduct(amount: float, title: str, detail: str) -> None:
        nonlocal score
        amount = round(float(amount), 2)
        if amount <= 0:
            return
        score = max(0.0, score - amount)
        signals.append(TriageSignal(amount, title, detail))

    is_query = isinstance(
        tree, (exp.Select, exp.Union, exp.Except, exp.Intersect, exp.Subquery)
    )
    if not is_query:
        # CREATE TABLE AS / INSERT ... SELECT still have a SELECT body, but the
        # public decompose() entry expects a read query to rewrite.
        select_body = tree.find(exp.Select)
        if select_body is not None and isinstance(
            tree, (exp.Create, exp.Insert, exp.Command)
        ):
            deduct(
                0.70,
                "Not a bare SELECT",
                f"{type(tree).__name__} wraps a query - extract the SELECT and "
                "re-score; the decomposer rewrites read queries, not DDL/DML wrappers.",
            )
        else:
            deduct(
                0.95,
                "Not a SELECT statement",
                f"{type(tree).__name__} - the decomposer only stages read queries.",
            )
        score = round(score, 2)
        signals.sort(key=lambda s: (-s.impact, s.title))
        return TriageReport(score, _verdict(score), True, signals)

    with_clause = tree.args.get("with")
    if with_clause is not None and with_clause.args.get("recursive"):
        deduct(
            0.90,
            "Recursive CTE",
            "WITH RECURSIVE cannot be materialized as independent staged temps.",
        )
        score = round(score, 2)
        signals.sort(key=lambda s: (-s.impact, s.title))
        return TriageReport(score, _verdict(score), True, signals)

    # --- structural risks aligned with planner / known limitations -----------

    function_names = _function_names(tree)
    json_candidates = {
        name for name in function_names if "JSON" in name
    } | (function_names & _JSON_FUNCTIONS)
    json_hits = sorted(
        name
        for name in json_candidates
        if "_" in name
        or not any(
            other != name and other.replace("_", "") == name
            for other in json_candidates
        )
    )
    super_cast = _has_super_cast(tree)
    bracket_navigation = tree.find(exp.Bracket) is not None
    if json_hits or super_cast or bracket_navigation:
        evidence = ", ".join(
            json_hits
            + (["CAST to SUPER"] if super_cast else [])
            + (["array/bracket navigation"] if bracket_navigation else [])
        )
        deduct(
            0.50,
            "SUPER / JSON manipulation",
            f"{evidence}. Semi-structured semantics are not preserved by "
            "column pruning and predicate pushdown; likelihood of a safe "
            "staged plan is low.",
        )

    if tree.find(exp.Lateral) is not None:
        deduct(
            0.35,
            "LATERAL construct",
            "Lateral references break independent stage extraction.",
        )

    correlated = _correlated_subquery_count(tree)
    if correlated:
        # Stronger than before: true correlation is the main structural block
        # short of SUPER/JSON and recursive CTEs.
        impact = min(0.45, 0.25 + 0.10 * (correlated - 1))
        deduct(
            impact,
            "Correlated subquery",
            f"{correlated} nested scope(s) reference outer aliases; those "
            "scopes cannot be staged as independent temps without changing "
            "results.",
        )

    set_ops, set_ops_have_tables = _set_op_with_branch_tables(tree)
    if set_ops:
        deduct(
            0.20 if set_ops_have_tables else 0.12,
            "Set operation (UNION/INTERSECT/EXCEPT)",
            "Branch filters are not shared across stages; a table referenced "
            "only inside a branch can be staged unfiltered (known limitation).",
        )

    cte_only = _tables_only_inside_ctes(tree)
    if cte_only and _outer_filters_reference_ctes(tree):
        sample = ", ".join(cte_only[:4])
        more = f" (+{len(cte_only) - 4} more)" if len(cte_only) > 4 else ""
        deduct(
            0.22,
            "CTE filter gap",
            f"Physical table(s) only inside CTE bodies ({sample}{more}) while "
            "outer WHERE filters the CTE alias - pushdown does not currently "
            "rewrite those filters into the staged scan.",
        )
    elif cte_only and not _has_pushable_where(tree):
        # CTE hides physical tables and nothing filters them at all
        deduct(
            0.08,
            "Physical tables only inside CTEs",
            "Staging may still work, but there is no outer filter evidence to "
            "push; confirm CTE bodies already filter heavily.",
        )

    self_joins = _multi_alias_self_joins(tree)
    if self_joins:
        sample = ", ".join(self_joins[:3])
        deduct(
            0.15,
            "Multi-alias self-join",
            f"{sample}: the planner will not push a shared filter onto one "
            "temp when aliases are independent domains (stage safety=review).",
        )

    external = _external_schema_tables(tree)
    if external:
        deduct(
            0.15,
            "External / Spectrum relation",
            f"{', '.join(external[:4])}: external scans do not take local "
            "DISTKEY/SORTKEY the same way; staged CTAS is still possible but "
            "economics and keys need manual review.",
        )

    if not _has_pushable_where(tree):
        if _has_having_only_filters(tree):
            deduct(
                0.12,
                "No WHERE clause (HAVING only)",
                "HAVING filters apply after aggregation and cannot be pushed "
                "into base-table stages; staging only pays via column pruning "
                "or shared scans.",
            )
        else:
            deduct(
                0.15,
                "No WHERE clause anywhere",
                "Nothing to push into stages; a successful plan is still "
                "possible, but stages are unfiltered copies plus pruning.",
            )

    depth = _select_depth(tree)
    if depth >= 5:
        deduct(0.20, "Very deep nesting", f"SELECT nesting depth {depth}.")
    elif depth >= 3:
        deduct(0.10, "Deep nesting", f"SELECT nesting depth {depth}.")

    tables = _distinct_physical_tables(tree)
    if len(tables) > 8:
        deduct(
            0.12,
            "Many relations",
            f"{len(tables)} distinct table references - stage count and "
            "lineage ambiguity both rise.",
        )
    elif len(tables) == 0:
        # Pure CTE / values / function — nothing physical to stage
        deduct(
            0.25,
            "No physical base tables visible",
            "Parse-only view cannot see view bodies; without a catalog the "
            "planner may emit an empty stage list.",
        )

    windows = sum(1 for _ in tree.find_all(exp.Window))
    if windows:
        deduct(
            0.10,
            "Window functions",
            f"{windows} window expression(s); filters cannot be pushed through "
            "them, and stages must preserve partitioning columns.",
        )

    if tree.find(exp.Star) is not None:
        deduct(
            0.05,
            "SELECT *",
            "Column pruning cannot be proven; stages project all known columns "
            "when a catalog is available.",
        )

    nondeterministic = sorted(function_names & _NONDETERMINISTIC)
    if nondeterministic:
        deduct(
            0.05,
            "Non-deterministic functions",
            ", ".join(nondeterministic)
            + " - equivalence validation after staging needs care.",
        )

    cross_joins = _cross_join_without_on(tree)
    if cross_joins and _equijoin_count(tree) == 0 and len(tables) >= 2:
        deduct(
            0.12,
            "Cross join without equijoin keys",
            f"{cross_joins} join(s) lack ON/USING equality - DISTKEY selection "
            "has no join key to preserve co-location.",
        )

    if _or_in_where(tree):
        deduct(
            0.05,
            "OR in WHERE",
            "Disjunctive predicates are not treated as simple pushable filters; "
            "they often remain on the final query (review).",
        )

    if _qualification_depth_mix(tree):
        deduct(
            0.08,
            "Mixed name qualification depth",
            "Catalog keys and query refs disagree on db.schema.table depth - "
            "SQLGlot qualify may be skipped (known limitation); stages fall "
            "back to catalog-order projection.",
        )

    # Soft floor: many mild issues should not claim "impossible"
    if score < 0.05 and any(
        s.title
        not in {
            "Does not parse as Redshift SQL",
            "Empty statement",
            "Not a SELECT statement",
            "Recursive CTE",
            "SUPER / JSON manipulation",
        }
        for s in signals
    ):
        # keep as-is; only hard blockers should pin to UNLIKELY via their own weights
        pass

    score = round(max(0.0, min(1.0, score)), 2)
    signals.sort(key=lambda s: (-s.impact, s.title))
    return TriageReport(score, _verdict(score), True, signals)


def _read_interactive() -> str:
    print(
        "Paste a Redshift query. Finish with an empty line "
        "(Ctrl+Z/Ctrl+D to quit)."
    )
    lines: list[str] = []
    while True:
        try:
            line = input()
        except EOFError:
            break
        if not line.strip() and lines:
            break
        lines.append(line)
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    import sys

    args = list(sys.argv[1:] if argv is None else argv)
    if args:
        sql = open(args[0], encoding="utf-8-sig").read()
        print(assess_decomposability(sql).summary())
        return 0
    while True:
        sql = _read_interactive()
        if not sql.strip():
            return 0
        print()
        print(assess_decomposability(sql).summary())
        print()


if __name__ == "__main__":
    raise SystemExit(main())
