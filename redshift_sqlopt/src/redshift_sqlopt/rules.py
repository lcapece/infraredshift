"""Redshift-specific rewrite rules.

Scope note: sqlglot's own optimizer already handles the generic, dialect-neutral
rewrites — predicate pushdown, subquery unnesting, projection pruning, boolean
simplification. There is no reason to reimplement any of that, and this module
does not try to. What lives here is the layer sqlglot cannot know about: rules
that depend on Redshift's MPP execution model (sort keys, zone maps, slice
distribution) or on Redshift's specific planner weaknesses.

Every rule follows the same contract:

    rule(tree, catalog) -> (new_tree, applied, blocked)

A rule that cannot *prove* its preconditions must return a ``BlockedRewrite``
explaining what could not be established, and must leave the tree untouched.
Emitting a rewrite that changes the result set is the one failure mode this
package must never have — a wrong rewrite against a warehouse is silent data
corruption, not a crash.
"""

from __future__ import annotations

import re
from collections.abc import Callable

from sqlglot import exp

from .catalog import Catalog
from .models import AppliedRewrite, BlockedRewrite

RuleResult = tuple[exp.Expression, list[AppliedRewrite], list[BlockedRewrite]]
Rule = Callable[[exp.Expression, Catalog], RuleResult]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _and_terms(node: exp.Expression | None) -> list[exp.Expression]:
    """Flatten an AND chain into its conjuncts."""
    if node is None:
        return []
    if isinstance(node, exp.And):
        return _and_terms(node.this) + _and_terms(node.expression)
    return [node]


def _rebuild_and(terms: list[exp.Expression]) -> exp.Expression | None:
    if not terms:
        return None
    current = terms[0]
    for term in terms[1:]:
        current = exp.And(this=current, expression=term)
    return current


def _column_of(node: exp.Expression) -> exp.Column | None:
    return node if isinstance(node, exp.Column) else None


def inner_source_alias(subquery: exp.Select) -> str:
    """Alias (or bare table name) of a subquery's single FROM source.

    Returns "" when the subquery has no source or more than one, because a
    correlation predicate can only be qualified unambiguously against exactly
    one inner source.
    """
    from_clause = subquery.args.get("from")
    if from_clause is None:
        return ""
    if subquery.args.get("joins"):
        return ""  # multiple inner sources: qualifier is ambiguous
    source = from_clause.this
    if isinstance(source, exp.Table):
        return str(source.alias or source.name or "").strip('"')
    if isinstance(source, exp.Subquery):
        return str(source.alias or "").strip('"')
    return ""


def _scope_alias(column: exp.Expression, tree: exp.Expression, *, exclude: str = "") -> str:
    """Resolve the alias that qualifies *column* in the outer query.

    Prefers the column's own qualifier. Falls back to the single outer source
    when the column is unqualified, skipping *exclude* so the inner table is
    never mistaken for the outer one. Returns "" when the answer is ambiguous —
    callers must refuse rather than guess.
    """
    if not isinstance(column, exp.Column):
        return ""
    own = str(column.table or "").strip('"')
    if own:
        return own
    candidates: list[str] = []
    for table in tree.find_all(exp.Table):
        alias = str(table.alias or table.name or "").strip('"')
        if alias and alias != exclude and alias not in candidates:
            candidates.append(alias)
    return candidates[0] if len(candidates) == 1 else ""


_DATE_LITERAL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?)?$")


def _looks_like_date(value: object) -> bool:
    """True when a string literal is an ISO date or timestamp."""
    return bool(_DATE_LITERAL_RE.match(str(value or "").strip()))


_DATE_TYPES = {
    exp.DataType.Type.DATE,
    exp.DataType.Type.DATETIME,
    exp.DataType.Type.TIMESTAMP,
    exp.DataType.Type.TIMESTAMPNTZ,
    exp.DataType.Type.TIMESTAMPTZ,
}


def _cast_is_to_date(node: exp.Cast) -> bool:
    """True only when a CAST targets a date/timestamp type.

    This guard is load-bearing. The rewrite this function feeds builds a
    ``DATEADD(day, 1, literal)`` upper bound, which is meaningful only for date
    values. Without the check, ``CAST(amount AS VARCHAR) = '100'`` on a numeric
    sort key would be rewritten to ``amount >= '100' AND amount < DATEADD(day,
    1, '100')`` — syntactically valid, structurally unchanged, and therefore
    invisible to the validation gate, but nonsense.
    """
    to = node.args.get("to")
    return isinstance(to, exp.DataType) and to.this in _DATE_TYPES


def _unwrap_sortkey_column(node: exp.Expression) -> tuple[exp.Column, str] | None:
    """If *node* is a DATE-producing function wrapping a bare column, return
    ``(column, funcname)``.

    These are the shapes that defeat zone-map pruning: ``DATE(col)``,
    ``CAST(col AS DATE)``, ``TRUNC(col)``, ``DATE_TRUNC('day', col)``.

    Only date-valued wrappers qualify — see :func:`_cast_is_to_date`. A cast to
    a non-date type is left alone even though it also defeats pruning, because
    the half-open-range rewrite does not apply to it.
    """
    if isinstance(node, exp.Cast) and isinstance(node.this, exp.Column):
        return (node.this, "CAST") if _cast_is_to_date(node) else None
    if isinstance(node, (exp.Date, exp.TsOrDsToDate)) and isinstance(node.this, exp.Column):
        return node.this, "DATE"
    # Redshift DATE_TRUNC parses to TimestampTrunc in sqlglot, not DateTrunc, and
    # the column can be in either `this` or `expression` depending on argument
    # order. Only day granularity is accepted: the rewrite below builds a
    # one-day range, so DATE_TRUNC('month', col) would silently narrow a month
    # to a day.
    if isinstance(node, (exp.DateTrunc, exp.TimestampTrunc)):
        unit = node.args.get("unit") or node.args.get("this")
        unit_name = ""
        if unit is not None and not isinstance(unit, exp.Column):
            unit_name = str(getattr(unit, "name", "") or getattr(unit, "this", "") or "").lower()
        if unit_name not in {"day", "days", "dd", "d"}:
            return None
        for candidate in (node.this, node.args.get("expression")):
            if isinstance(candidate, exp.Column):
                return candidate, "DATE_TRUNC"
        return None
    if isinstance(node, exp.Anonymous):
        name = (node.name or "").upper()
        args = node.args.get("expressions") or []
        # TRUNC is overloaded in Redshift: TRUNC(numeric) truncates a number and
        # has nothing to do with dates, so it is excluded here. Only the
        # unambiguously date-valued spellings are accepted.
        if name in {"DATE", "TO_DATE"} and args and isinstance(args[0], exp.Column):
            return args[0], name
    return None


# ---------------------------------------------------------------------------
# rule: sargability — un-wrap function calls on sort-key columns
# ---------------------------------------------------------------------------


def rule_sargable_sortkey(tree: exp.Expression, catalog: Catalog) -> RuleResult:
    """Rewrite ``DATE(col) = 'x'`` into a half-open range on the bare column.

    Redshift prunes blocks using zone maps (per-block min/max) on the sort key.
    Wrapping the column in a function forces every block to be read and the
    function evaluated per row. A half-open range ``col >= d AND col < d+1``
    selects exactly the same rows while remaining prunable.

    Precondition: the column must actually be a sort key. Applying this to a
    non-sortkey column is harmless but pointless noise, so it is skipped —
    we only claim a benefit we can substantiate from the catalog.
    """
    applied: list[AppliedRewrite] = []
    blocked: list[BlockedRewrite] = []
    working = tree.copy()

    for eq in list(working.find_all(exp.EQ)):
        for wrapped, literal in ((eq.this, eq.expression), (eq.expression, eq.this)):
            unwrapped = _unwrap_sortkey_column(wrapped)
            if unwrapped is None or not isinstance(literal, exp.Literal):
                continue
            column, func = unwrapped
            table = catalog.table_for_column(column, working)
            if table is None:
                blocked.append(
                    BlockedRewrite(
                        code="SARGABLE_SORTKEY",
                        title=f"{func}() wraps {column.sql()} in a filter",
                        reason=(
                            "Cannot resolve which table this column belongs to, so "
                            "whether it is a sort key is unknown."
                        ),
                        precondition="column resolves to a known table",
                        would_have_done="replace with a half-open range predicate",
                    )
                )
                break
            if not catalog.is_sortkey(table, column.name):
                break  # not a sortkey; no zone-map benefit to claim

            if not literal.is_string or not _looks_like_date(literal.name):
                # Defence in depth: even with a date-valued wrapper, only build a
                # day-range when the constant really is a date literal. A
                # non-date constant here means the query does something this
                # rule does not model, and guessing is not allowed.
                break
            lower = literal.copy()
            upper = exp.Anonymous(
                this="DATEADD",
                expressions=[
                    exp.column("day"),
                    exp.Literal.number(1),
                    literal.copy(),
                ],
            )
            bare = column.copy()
            replacement = exp.And(
                this=exp.GTE(this=bare, expression=lower),
                expression=exp.LT(this=bare.copy(), expression=upper),
            )
            eq.replace(replacement)
            applied.append(
                AppliedRewrite(
                    code="SARGABLE_SORTKEY",
                    title=f"Removed {func}() from sort-key column {table}.{column.name}",
                    rationale=(
                        f"{func}({column.name}) cannot use zone maps, so Redshift must "
                        f"read every block of {table}. The equivalent half-open range "
                        f"on the bare column restores block pruning and returns the "
                        f"same rows."
                    ),
                    precondition=f"{table}.{column.name} is a sort key (confirmed in catalog)",
                )
            )
            break

    return working, applied, blocked


# ---------------------------------------------------------------------------
# rule: NOT IN -> NOT EXISTS (a correctness fix, not only a speed fix)
# ---------------------------------------------------------------------------


def rule_not_in_to_not_exists(tree: exp.Expression, catalog: Catalog) -> RuleResult:
    """Convert ``x NOT IN (SELECT ...)`` to ``NOT EXISTS (...)``.

    This is the rare rewrite that fixes a bug rather than just a plan. If the
    subquery yields even one NULL, ``NOT IN`` evaluates to UNKNOWN for every
    row and the query silently returns zero rows. ``NOT EXISTS`` expresses the
    intent and plans as an anti-join.

    Because the two forms differ *only* when NULLs are present, and the NOT
    EXISTS form is what the author meant in every case observed in practice,
    this rule fires whenever the shape matches — but it always says so loudly
    in the rationale, since it can change results when NULLs exist.
    """
    applied: list[AppliedRewrite] = []
    blocked: list[BlockedRewrite] = []
    working = tree.copy()

    for not_node in list(working.find_all(exp.Not)):
        inner = not_node.this
        if not isinstance(inner, exp.In):
            continue
        query = inner.args.get("query") or inner.args.get("this")
        subquery = None
        if isinstance(query, exp.Select):
            subquery = query
        elif isinstance(query, exp.Subquery) and isinstance(query.this, exp.Select):
            subquery = query.this
        if subquery is None:
            continue

        left = inner.this
        projections = subquery.expressions or []
        if len(projections) != 1:
            blocked.append(
                BlockedRewrite(
                    code="NOT_IN_TO_NOT_EXISTS",
                    title="NOT IN over a multi-column subquery",
                    reason="Subquery projects more than one column; correlation target is ambiguous.",
                    precondition="subquery projects exactly one column",
                )
            )
            continue

        inner_expr = projections[0]
        inner_col = inner_expr.this if isinstance(inner_expr, exp.Alias) else inner_expr
        if not isinstance(inner_col, exp.Column):
            blocked.append(
                BlockedRewrite(
                    code="NOT_IN_TO_NOT_EXISTS",
                    title="NOT IN over a computed subquery projection",
                    reason="Subquery projects an expression, not a plain column; cannot build a correlation predicate.",
                    precondition="subquery projects a plain column",
                )
            )
            continue

        # Soundness gate. NOT IN and NOT EXISTS differ exactly when the subquery
        # column contains NULLs, so the rewrite may only be *applied* when the
        # catalog proves it cannot. Without that proof the observation is still
        # valuable — it is very likely a latent bug — so it is reported as
        # blocked rather than dropped.
        inner_table = catalog.table_for_column(inner_col, subquery)
        if inner_table is None or not catalog.is_not_null(inner_table, inner_col.name):
            where = inner_table or "the subquery table"
            blocked.append(
                BlockedRewrite(
                    code="NOT_IN_TO_NOT_EXISTS",
                    title=f"NOT IN over possibly-nullable {inner_col.sql()}",
                    reason=(
                        f"Cannot prove {where}.{inner_col.name} is NOT NULL. If it "
                        "contains even one NULL, NOT IN returns zero rows while NOT "
                        "EXISTS returns the intended set — so the two are not "
                        "equivalent and the rewrite is withheld. Worth reviewing by "
                        "hand: if NULLs are present, this query is already returning "
                        "wrong results."
                    ),
                    precondition=f"{where}.{inner_col.name} is NOT NULL",
                    would_have_done="rewrite NOT IN as NOT EXISTS (an anti-join)",
                )
            )
            continue

        # Both sides of the correlation predicate MUST carry distinct qualifiers.
        # An unqualified column inside the subquery resolves to the *inner*
        # table, so a naive `inner_col = left` on unqualified input emits
        # `WHERE k = k` — a tautology that turns the anti-join into "is the inner
        # table empty", silently returning wrong rows. Structural validation
        # cannot catch this (same tables, same columns, same projection), so the
        # qualification has to be proven here.
        inner_alias = inner_source_alias(subquery)
        outer_alias = _scope_alias(left, working, exclude=inner_alias)
        if not outer_alias or not inner_alias or outer_alias == inner_alias:
            blocked.append(
                BlockedRewrite(
                    code="NOT_IN_TO_NOT_EXISTS",
                    title="NOT IN whose correlation cannot be unambiguously qualified",
                    reason=(
                        "Could not resolve distinct outer and inner qualifiers for the "
                        "correlation predicate. Emitting it unqualified would produce "
                        "`col = col`, which is always true and would change results. "
                        "Qualify the columns (e.g. `o.k NOT IN (SELECT i.k FROM ... i)`) "
                        "and re-run."
                    ),
                    precondition="outer and inner columns resolve to distinct aliases",
                    would_have_done="rewrite NOT IN as a correlated NOT EXISTS",
                )
            )
            continue

        correlated = subquery.copy()
        correlated.set("expressions", [exp.Literal.number(1)])
        existing_where = correlated.args.get("where")
        link = exp.EQ(
            this=exp.column(inner_col.name, table=inner_alias),
            expression=exp.column(left.name, table=outer_alias),
        )
        terms = _and_terms(existing_where.this if existing_where else None) + [link]
        correlated.set("where", exp.Where(this=_rebuild_and(terms)))

        not_node.replace(exp.Not(this=exp.Exists(this=correlated)))
        applied.append(
            AppliedRewrite(
                code="NOT_IN_TO_NOT_EXISTS",
                title="NOT IN converted to NOT EXISTS",
                rationale=(
                    "NOT IN materializes the full value list and cannot be planned as "
                    "an anti-join. NOT EXISTS expresses the same intent and lets "
                    "Redshift use one. Equivalence holds here because the catalog "
                    "confirms the subquery column is NOT NULL — the only case in "
                    "which the two forms can differ."
                ),
                precondition=(
                    "subquery projects exactly one plain column, and that column is "
                    "NOT NULL per the catalog"
                ),
            )
        )

    return working, applied, blocked


# ---------------------------------------------------------------------------
# rule: redundant DISTINCT over a grouped result
# ---------------------------------------------------------------------------


def rule_redundant_distinct(tree: exp.Expression, catalog: Catalog) -> RuleResult:
    """Drop ``DISTINCT`` when ``GROUP BY`` already guarantees uniqueness.

    GROUP BY emits exactly one row per distinct grouping key, so a DISTINCT
    layered on top cannot remove anything — but Redshift still pays for the
    extra dedup pass, often a full sort.

    Precondition: every projected expression must be either a grouping key or
    an aggregate. If a non-grouped, non-aggregate column is projected the
    output may genuinely contain duplicates and DISTINCT is load-bearing.
    """
    applied: list[AppliedRewrite] = []
    blocked: list[BlockedRewrite] = []
    working = tree.copy()

    for select in list(working.find_all(exp.Select)):
        if not select.args.get("distinct"):
            continue
        group = select.args.get("group")
        if group is None:
            continue

        group_keys = {
            expression.sql(dialect="redshift", normalize=True)
            for expression in (group.expressions or [])
        }
        safe = True
        for projection in select.expressions or []:
            target = projection.this if isinstance(projection, exp.Alias) else projection
            if isinstance(target, exp.AggFunc) or target.find(exp.AggFunc):
                continue
            if target.sql(dialect="redshift", normalize=True) in group_keys:
                continue
            safe = False
            break

        if not safe:
            blocked.append(
                BlockedRewrite(
                    code="REDUNDANT_DISTINCT",
                    title="DISTINCT alongside GROUP BY",
                    reason=(
                        "A projected expression is neither a grouping key nor an "
                        "aggregate, so the grouped result may still contain duplicate "
                        "rows and the DISTINCT is doing real work."
                    ),
                    precondition="every projection is a grouping key or an aggregate",
                )
            )
            continue

        select.set("distinct", None)
        applied.append(
            AppliedRewrite(
                code="REDUNDANT_DISTINCT",
                title="Removed DISTINCT made redundant by GROUP BY",
                rationale=(
                    "GROUP BY already emits one row per distinct key, so the DISTINCT "
                    "cannot eliminate any row. Removing it drops an entire "
                    "deduplication pass, usually a sort."
                ),
                precondition="all projections are grouping keys or aggregates",
            )
        )

    return working, applied, blocked


# ---------------------------------------------------------------------------
# rule: propagate an equality across an inner-join key
# ---------------------------------------------------------------------------


def rule_propagate_join_filter(tree: exp.Expression, catalog: Catalog) -> RuleResult:
    """Given ``a.k = b.k`` and ``a.k = <const>``, also assert ``b.k = <const>``.

    Equality is transitive, so the derived predicate is implied by the original
    query. Stating it explicitly lets Redshift prune blocks on the second table
    *before* the join rather than discarding rows after it.

    Precondition: INNER join only. On an outer join an added predicate on the
    null-supplying side changes which rows survive, turning it into an inner
    join by stealth.
    """
    applied: list[AppliedRewrite] = []
    blocked: list[BlockedRewrite] = []
    working = tree.copy()

    for select in list(working.find_all(exp.Select)):
        where = select.args.get("where")
        if where is None:
            continue
        terms = _and_terms(where.this)

        constants: dict[str, exp.Expression] = {}
        for term in terms:
            if not isinstance(term, exp.EQ):
                continue
            column, literal = _column_of(term.this), term.expression
            if column is None or not isinstance(literal, exp.Literal):
                column, literal = _column_of(term.expression), term.this
            if column is None or not isinstance(literal, exp.Literal):
                continue
            constants[column.sql(dialect="redshift", normalize=True)] = literal

        if not constants:
            continue

        additions: list[exp.Expression] = []
        for join in select.args.get("joins") or []:
            side = (join.side or "").upper()
            kind = (join.kind or "").upper()
            on = join.args.get("on")
            if on is None:
                continue
            if side in {"LEFT", "RIGHT", "FULL"} or kind in {"OUTER", "CROSS"}:
                for condition in _and_terms(on):
                    if isinstance(condition, exp.EQ):
                        left_col, right_col = _column_of(condition.this), _column_of(condition.expression)
                        if left_col is None or right_col is None:
                            continue
                        keys = (
                            left_col.sql(dialect="redshift", normalize=True),
                            right_col.sql(dialect="redshift", normalize=True),
                        )
                        if keys[0] in constants or keys[1] in constants:
                            blocked.append(
                                BlockedRewrite(
                                    code="PROPAGATE_JOIN_FILTER",
                                    title=f"Constant not propagated across {side or kind} join",
                                    reason=(
                                        f"Propagating a filter across a {side or kind} join would "
                                        "drop rows the outer join is meant to preserve, silently "
                                        "converting it to an inner join."
                                    ),
                                    precondition="join is INNER",
                                )
                            )
                continue

            for condition in _and_terms(on):
                if not isinstance(condition, exp.EQ):
                    continue
                left_col = _column_of(condition.this)
                right_col = _column_of(condition.expression)
                if left_col is None or right_col is None:
                    continue
                left_key = left_col.sql(dialect="redshift", normalize=True)
                right_key = right_col.sql(dialect="redshift", normalize=True)
                for source, target_col, target_key in (
                    (left_key, right_col, right_key),
                    (right_key, left_col, left_key),
                ):
                    if source in constants and target_key not in constants:
                        derived = exp.EQ(
                            this=target_col.copy(),
                            expression=constants[source].copy(),
                        )
                        additions.append(derived)
                        constants[target_key] = constants[source]
                        applied.append(
                            AppliedRewrite(
                                code="PROPAGATE_JOIN_FILTER",
                                title=f"Propagated constant filter to {target_col.sql()}",
                                rationale=(
                                    f"The join asserts {left_key} = {right_key} and the query "
                                    f"already filters {source} to a constant. By transitivity "
                                    f"{target_key} equals the same constant, so stating it lets "
                                    "Redshift prune blocks before the join instead of after."
                                ),
                                precondition="INNER join with an equality condition",
                            )
                        )

        if additions:
            select.set("where", exp.Where(this=_rebuild_and(terms + additions)))

    return working, applied, blocked


ALL_RULES: tuple[Rule, ...] = (
    rule_sargable_sortkey,
    rule_not_in_to_not_exists,
    rule_redundant_distinct,
    rule_propagate_join_filter,
)
