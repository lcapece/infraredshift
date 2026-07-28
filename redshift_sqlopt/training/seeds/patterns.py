"""Seed anti-pattern corpus for Redshift SQL rewrite training.

Each entry is one *pattern*, not one example. A pattern carries a ``bad`` and a
``good`` SQL template plus the reasoning that connects them; the generator in
``generate.py`` multiplies each pattern into hundreds of concrete pairs by
substituting schema names, columns, literals and join arity.

Why templates instead of hand-written pairs: a fine-tune of a 1.5B base wants
thousands of examples. Hand-writing thousands invites transcription errors, and
a training corpus with wrong labels is worse than no corpus at all. Writing the
*transformation* once and generating instances from it keeps every pair correct
by construction — and every generated pair is still machine-verified (both sides
parse, identical table multiset, identical projection) before it is emitted.

Soundness note: patterns marked ``requires_not_null`` change row counts when the
join/predicate column contains NULLs. The generator only emits those against
columns it declares NOT NULL, mirroring the ``refuse unless provable`` rule the
deterministic engine follows. The model must never learn a rewrite that is only
conditionally correct without the condition being true in the example.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Pattern:
    """One bad->good rewrite transformation.

    Templates use ``{placeholder}`` fields filled by the generator. Every
    placeholder that appears in ``bad`` must appear in ``good`` (or be a literal
    the rewrite legitimately drops), or the pair will fail verification.
    """

    code: str
    title: str
    category: str
    bad: str
    good: str
    rationale: str
    plan_signature: str
    severity: str = "medium"
    requires_not_null: tuple[str, ...] = ()
    tags: tuple[str, ...] = field(default_factory=tuple)


# --------------------------------------------------------------------------
# Category 1: correlated subqueries — the classic Redshift nested-loop killer
# --------------------------------------------------------------------------

CORRELATED = [
    Pattern(
        code="CORR_SCALAR_TO_JOIN",
        title="Correlated scalar subquery in SELECT list -> LEFT JOIN aggregate",
        category="correlated_subquery",
        bad=(
            "SELECT {a}.{k}, {a}.{c1}, "
            "(SELECT MAX({b}.{m}) FROM {schema}.{tb} {b} "
            "WHERE {b}.{k} = {a}.{k}) AS {m}_max "
            "FROM {schema}.{ta} {a}"
        ),
        good=(
            "SELECT {a}.{k}, {a}.{c1}, {agg}.{m}_max "
            "FROM {schema}.{ta} {a} "
            "LEFT JOIN (SELECT {k}, MAX({m}) AS {m}_max FROM {schema}.{tb} GROUP BY {k}) {agg} "
            "ON {agg}.{k} = {a}.{k}"
        ),
        rationale=(
            "A correlated scalar subquery is re-evaluated per outer row. Redshift "
            "cannot always decorrelate it, so the plan degrades to a nested loop. "
            "Pre-aggregating once and joining turns O(rows) subquery executions "
            "into a single hash join."
        ),
        plan_signature="nested loop join; inner scan repeated per outer row",
        severity="high",
        tags=("subquery", "nested_loop"),
    ),
    Pattern(
        code="CORR_EXISTS_TO_SEMIJOIN",
        title="Correlated EXISTS -> semi-join via IN on distinct keys",
        category="correlated_subquery",
        bad=(
            "SELECT {a}.{k}, {a}.{c1} FROM {schema}.{ta} {a} "
            "WHERE EXISTS (SELECT 1 FROM {schema}.{tb} {b} "
            "WHERE {b}.{k} = {a}.{k} AND {b}.{c2} = {lit_str})"
        ),
        good=(
            "SELECT {a}.{k}, {a}.{c1} FROM {schema}.{ta} {a} "
            "JOIN (SELECT DISTINCT {k} FROM {schema}.{tb} WHERE {c2} = {lit_str}) {b} "
            "ON {b}.{k} = {a}.{k}"
        ),
        rationale=(
            "Materializing the distinct key set once lets Redshift hash-join it, "
            "instead of probing the inner table per outer row. DISTINCT preserves "
            "EXISTS semantics: at most one output row per outer row."
        ),
        plan_signature="correlated subplan; repeated inner scan",
        severity="high",
        requires_not_null=("k",),
        tags=("subquery", "exists", "semijoin"),
    ),
    Pattern(
        code="NOT_IN_TO_NOT_EXISTS",
        title="NOT IN over a nullable column -> NOT EXISTS",
        category="correlated_subquery",
        bad=(
            "SELECT {a}.{k}, {a}.{c1} FROM {schema}.{ta} {a} "
            "WHERE {a}.{k} NOT IN (SELECT {b}.{k} FROM {schema}.{tb} {b})"
        ),
        good=(
            "SELECT {a}.{k}, {a}.{c1} FROM {schema}.{ta} {a} "
            "WHERE NOT EXISTS (SELECT 1 FROM {schema}.{tb} {b} WHERE {b}.{k} = {a}.{k})"
        ),
        rationale=(
            "NOT IN returns zero rows when the subquery yields a single NULL, "
            "because NULL comparison is unknown. NOT EXISTS has the intended "
            "semantics and plans as an anti-join. This rewrite also FIXES a "
            "latent correctness bug, not just performance."
        ),
        plan_signature="anti-join absent; full materialization of IN list",
        severity="critical",
        tags=("subquery", "null_semantics", "correctness"),
    ),
]


# --------------------------------------------------------------------------
# Category 2: sortkey / distkey defeat — predicates Redshift cannot prune on
# --------------------------------------------------------------------------

KEY_DEFEAT = [
    Pattern(
        code="SORTKEY_FUNC_WRAP",
        title="Function-wrapped sortkey column -> sargable range predicate",
        category="key_defeat",
        bad=(
            "SELECT {a}.{k}, {a}.{c1} FROM {schema}.{ta} {a} "
            "WHERE DATE({a}.{dt}) = {lit_date}"
        ),
        good=(
            "SELECT {a}.{k}, {a}.{c1} FROM {schema}.{ta} {a} "
            "WHERE {a}.{dt} >= {lit_date} AND {a}.{dt} < DATEADD(day, 1, {lit_date})"
        ),
        rationale=(
            "Wrapping a sortkey column in a function makes the predicate "
            "non-sargable: Redshift cannot use zone maps and must scan every "
            "block. A half-open range on the bare column restores block pruning "
            "and returns identical rows."
        ),
        plan_signature="seq scan, zone-map pruning not applied, high blocks_read",
        severity="high",
        tags=("sortkey", "sargable", "zone_map"),
    ),
    Pattern(
        code="SORTKEY_YEAR_EXTRACT",
        title="EXTRACT(YEAR ...) on sortkey -> year boundary range",
        category="key_defeat",
        bad=(
            "SELECT {a}.{k}, {a}.{c1} FROM {schema}.{ta} {a} "
            "WHERE EXTRACT(YEAR FROM {a}.{dt}) = {lit_year}"
        ),
        good=(
            "SELECT {a}.{k}, {a}.{c1} FROM {schema}.{ta} {a} "
            "WHERE {a}.{dt} >= {lit_year_start} AND {a}.{dt} < {lit_year_end}"
        ),
        rationale=(
            "EXTRACT hides the column from zone-map pruning. An explicit "
            "half-open year range is sargable and selects the same rows."
        ),
        plan_signature="seq scan over all partitions; zone map unused",
        severity="high",
        tags=("sortkey", "sargable", "date"),
    ),
    Pattern(
        code="IMPLICIT_CAST_JOIN",
        title="Implicit cast on join key -> cast the literal side instead",
        category="key_defeat",
        bad=(
            "SELECT {a}.{k}, {a}.{c1} FROM {schema}.{ta} {a} "
            "WHERE {a}.{k}::VARCHAR = {lit_str}"
        ),
        good=(
            "SELECT {a}.{k}, {a}.{c1} FROM {schema}.{ta} {a} "
            "WHERE {a}.{k} = CAST({lit_str} AS INTEGER)"
        ),
        rationale=(
            "Casting the column forces a per-row computation and defeats both "
            "zone maps and distribution-key matching. Casting the constant "
            "instead is evaluated once at plan time."
        ),
        plan_signature="seq scan; DS_BCAST or DS_DIST_BOTH on a distkey join",
        severity="medium",
        tags=("distkey", "cast", "sargable"),
    ),
]


# --------------------------------------------------------------------------
# Category 3: window functions — the case the user called out explicitly
# --------------------------------------------------------------------------

WINDOW = [
    Pattern(
        code="WINDOW_FILTER_AFTER",
        title="Filter applied after windowing -> filter pushed before the window",
        category="window_function",
        bad=(
            "SELECT * FROM ("
            "SELECT {a}.{k}, {a}.{c1}, {a}.{dt}, "
            "ROW_NUMBER() OVER (PARTITION BY {a}.{k} ORDER BY {a}.{dt} DESC) AS rn "
            "FROM {schema}.{ta} {a}) {sub} "
            "WHERE {sub}.rn = 1 AND {sub}.{dt} >= {lit_date}"
        ),
        good=(
            "SELECT * FROM ("
            "SELECT {a}.{k}, {a}.{c1}, {a}.{dt}, "
            "ROW_NUMBER() OVER (PARTITION BY {a}.{k} ORDER BY {a}.{dt} DESC) AS rn "
            "FROM {schema}.{ta} {a} WHERE {a}.{dt} >= {lit_date}) {sub} "
            "WHERE {sub}.rn = 1"
        ),
        rationale=(
            "The date filter does not reference the window result, so it can be "
            "evaluated before the window is computed. Sorting and ranking a "
            "smaller set is dramatically cheaper. Note this is only sound because "
            "the predicate column is also the ORDER BY column and the filter is "
            "monotonic — filtering before a ROW_NUMBER can otherwise change which "
            "row ranks first."
        ),
        plan_signature="window agg over full table; large sort spill",
        severity="high",
        tags=("window", "predicate_pushdown", "sort"),
    ),
    Pattern(
        code="WINDOW_DEDUP_TO_AGG",
        title="ROW_NUMBER dedup for a single max -> plain GROUP BY aggregate",
        category="window_function",
        bad=(
            "SELECT {sub}.{k}, {sub}.{m} FROM ("
            "SELECT {a}.{k}, {a}.{m}, "
            "ROW_NUMBER() OVER (PARTITION BY {a}.{k} ORDER BY {a}.{m} DESC) AS rn "
            "FROM {schema}.{ta} {a}) {sub} WHERE {sub}.rn = 1"
        ),
        good=(
            "SELECT {a}.{k}, MAX({a}.{m}) AS {m} "
            "FROM {schema}.{ta} {a} GROUP BY {a}.{k}"
        ),
        rationale=(
            "When only the top-ranked value of the ORDER BY column is projected, "
            "a window sort is unnecessary. GROUP BY with MAX is a hash aggregate "
            "— no global sort, no spill. Equivalent only because no other "
            "non-key column is projected from the ranked row."
        ),
        plan_signature="window agg + sort; spill to disk",
        severity="high",
        tags=("window", "aggregate", "spill"),
    ),
    Pattern(
        code="WINDOW_MISMATCHED_PARTITIONS",
        title="Windows partitioned on a non-distkey column -> pre-aggregate then join",
        category="window_function",
        bad=(
            "SELECT {a}.{k}, {a}.{c1}, "
            "SUM({a}.{m}) OVER (PARTITION BY {a}.{c1}) AS {c1}_total "
            "FROM {schema}.{ta} {a} WHERE {a}.{dt} >= {lit_date}"
        ),
        good=(
            "SELECT {a}.{k}, {a}.{c1}, {agg}.{c1}_total "
            "FROM {schema}.{ta} {a} "
            "JOIN (SELECT {c1}, SUM({m}) AS {c1}_total FROM {schema}.{ta} "
            "WHERE {dt} >= {lit_date} GROUP BY {c1}) {agg} ON {agg}.{c1} = {a}.{c1} "
            "WHERE {a}.{dt} >= {lit_date}"
        ),
        rationale=(
            "A window PARTITION BY on a column that is not the distribution key "
            "forces Redshift to redistribute the entire table across slices before "
            "it can compute the window. Pre-aggregating to one row per partition "
            "value moves far less data, and the join broadcasts a small result."
        ),
        plan_signature="DS_DIST_BOTH or DS_BCAST feeding a window agg step",
        severity="high",
        tags=("window", "distkey", "redistribute"),
    ),
    Pattern(
        code="WINDOW_COUNT_OVER_TO_JOIN",
        title="COUNT(*) OVER (PARTITION BY ...) used only for filtering -> HAVING",
        category="window_function",
        bad=(
            "SELECT {sub}.{k}, {sub}.{c1} FROM ("
            "SELECT {a}.{k}, {a}.{c1}, COUNT(*) OVER (PARTITION BY {a}.{k}) AS grp_cnt "
            "FROM {schema}.{ta} {a}) {sub} WHERE {sub}.grp_cnt > {lit_int}"
        ),
        good=(
            "SELECT {a}.{k}, MIN({a}.{c1}) AS {c1} FROM {schema}.{ta} {a} "
            "GROUP BY {a}.{k} HAVING COUNT(*) > {lit_int}"
        ),
        rationale=(
            "When a window count is used only as a filter and no per-row detail "
            "survives, a GROUP BY with HAVING computes the same set with a hash "
            "aggregate instead of a full partition sort. Equivalent only because "
            "the projection collapses to one row per key."
        ),
        plan_signature="window agg followed by filter; large sort",
        severity="medium",
        tags=("window", "aggregate", "having"),
    ),
]


# --------------------------------------------------------------------------
# Category 4: general sloppiness — cheap, high-frequency wins
# --------------------------------------------------------------------------

SLOPPY = [
    Pattern(
        code="REDUNDANT_DISTINCT",
        title="DISTINCT over an already-unique grouped result",
        category="sloppy_sql",
        bad=(
            "SELECT DISTINCT {a}.{k}, COUNT(*) AS cnt "
            "FROM {schema}.{ta} {a} GROUP BY {a}.{k}"
        ),
        good=(
            "SELECT {a}.{k}, COUNT(*) AS cnt "
            "FROM {schema}.{ta} {a} GROUP BY {a}.{k}"
        ),
        rationale=(
            "GROUP BY already guarantees one row per key. The DISTINCT adds a "
            "second deduplication pass — often a full sort — that cannot remove "
            "anything."
        ),
        plan_signature="extra unique/sort step after hash aggregate",
        severity="medium",
        tags=("distinct", "redundant"),
    ),
    Pattern(
        code="SELECT_STAR_NARROW",
        title="SELECT * when few columns are used -> explicit projection",
        category="sloppy_sql",
        bad=(
            "SELECT * FROM {schema}.{ta} {a} "
            "JOIN {schema}.{tb} {b} ON {b}.{k} = {a}.{k} "
            "WHERE {a}.{dt} >= {lit_date}"
        ),
        good=(
            "SELECT {a}.{k}, {a}.{c1}, {b}.{c2} FROM {schema}.{ta} {a} "
            "JOIN {schema}.{tb} {b} ON {b}.{k} = {a}.{k} "
            "WHERE {a}.{dt} >= {lit_date}"
        ),
        rationale=(
            "Redshift is columnar: unreferenced columns still cost I/O when "
            "selected. Naming only the needed columns cuts scan bytes and the "
            "width of every intermediate result."
        ),
        plan_signature="high bytes_scanned relative to output width",
        severity="medium",
        tags=("projection", "columnar", "io"),
    ),
    Pattern(
        code="OR_TO_UNION_ALL",
        title="OR across two sargable ranges -> UNION ALL of prunable scans",
        category="sloppy_sql",
        bad=(
            "SELECT {a}.{k}, {a}.{c1} FROM {schema}.{ta} {a} "
            "WHERE {a}.{dt} = {lit_date} OR {a}.{dt} = {lit_date2}"
        ),
        good=(
            "SELECT {a}.{k}, {a}.{c1} FROM {schema}.{ta} {a} WHERE {a}.{dt} = {lit_date} "
            "UNION ALL "
            "SELECT {a}.{k}, {a}.{c1} FROM {schema}.{ta} {a} WHERE {a}.{dt} = {lit_date2}"
        ),
        rationale=(
            "An OR over a sortkey column can prevent zone-map pruning entirely. "
            "Two separately-pruned scans combined with UNION ALL each keep their "
            "block pruning. UNION ALL (not UNION) preserves row multiplicity, and "
            "the two ranges are disjoint so no duplicates are introduced."
        ),
        plan_signature="seq scan with OR predicate; zone map unused",
        severity="medium",
        tags=("or", "sortkey", "union"),
    ),
    Pattern(
        code="HAVING_TO_WHERE",
        title="HAVING on a non-aggregate -> WHERE before grouping",
        category="sloppy_sql",
        bad=(
            "SELECT {a}.{k}, COUNT(*) AS cnt FROM {schema}.{ta} {a} "
            "GROUP BY {a}.{k} HAVING {a}.{k} > {lit_int}"
        ),
        good=(
            "SELECT {a}.{k}, COUNT(*) AS cnt FROM {schema}.{ta} {a} "
            "WHERE {a}.{k} > {lit_int} GROUP BY {a}.{k}"
        ),
        rationale=(
            "HAVING filters after aggregation. A predicate on a grouping column "
            "does not depend on the aggregate, so moving it to WHERE shrinks the "
            "input to the aggregate rather than discarding its output."
        ),
        plan_signature="filter applied above hash aggregate",
        severity="medium",
        tags=("having", "predicate_pushdown"),
    ),
    Pattern(
        code="COUNT_DISTINCT_TO_GROUP",
        title="COUNT(DISTINCT) -> pre-grouped subquery",
        category="sloppy_sql",
        bad=(
            "SELECT COUNT(DISTINCT {a}.{k}) AS uniq FROM {schema}.{ta} {a} "
            "WHERE {a}.{dt} >= {lit_date}"
        ),
        good=(
            "SELECT COUNT(*) AS uniq FROM "
            "(SELECT {a}.{k} FROM {schema}.{ta} {a} WHERE {a}.{dt} >= {lit_date} "
            "GROUP BY {a}.{k}) {sub}"
        ),
        rationale=(
            "COUNT(DISTINCT) is computed on a single node after redistributing "
            "every value. A GROUP BY deduplicates in parallel across slices "
            "first, so only the distinct set is gathered."
        ),
        plan_signature="DS_DIST_ALL_INNER or single-slice aggregate step",
        severity="high",
        tags=("count_distinct", "skew", "aggregate"),
    ),
]


# --------------------------------------------------------------------------
# Category 5: join structure — distribution and order problems
# --------------------------------------------------------------------------

JOINS = [
    Pattern(
        code="CROSS_JOIN_FILTER",
        title="Implicit cross join with a WHERE predicate -> explicit JOIN ON",
        category="join_structure",
        bad=(
            "SELECT {a}.{k}, {b}.{c2} FROM {schema}.{ta} {a}, {schema}.{tb} {b} "
            "WHERE {a}.{k} = {b}.{k} AND {a}.{dt} >= {lit_date}"
        ),
        good=(
            "SELECT {a}.{k}, {b}.{c2} FROM {schema}.{ta} {a} "
            "JOIN {schema}.{tb} {b} ON {a}.{k} = {b}.{k} "
            "WHERE {a}.{dt} >= {lit_date}"
        ),
        rationale=(
            "Comma joins hide the join condition among filters. Explicit JOIN ON "
            "separates join keys from predicates, which makes the intended "
            "distribution obvious and prevents an accidental cartesian product "
            "if a condition is later edited away."
        ),
        plan_signature="nested loop join with join filter applied late",
        severity="medium",
        tags=("join", "cross_join", "clarity"),
    ),
    Pattern(
        code="FILTER_NOT_PROPAGATED",
        title="Filter on one side of an equi-join not propagated to the other",
        category="join_structure",
        bad=(
            "SELECT {a}.{k}, {b}.{c2} FROM {schema}.{ta} {a} "
            "JOIN {schema}.{tb} {b} ON {a}.{k} = {b}.{k} "
            "WHERE {a}.{k} = {lit_int}"
        ),
        good=(
            "SELECT {a}.{k}, {b}.{c2} FROM {schema}.{ta} {a} "
            "JOIN {schema}.{tb} {b} ON {a}.{k} = {b}.{k} "
            "WHERE {a}.{k} = {lit_int} AND {b}.{k} = {lit_int}"
        ),
        rationale=(
            "Equality is transitive: if a.k = b.k and a.k = 42 then b.k = 42. "
            "Stating it explicitly lets Redshift prune blocks on the second table "
            "before the join instead of after. Sound for INNER joins only — on an "
            "OUTER join this would change which rows are preserved."
        ),
        plan_signature="full scan of inner table before join filter",
        severity="high",
        tags=("join", "predicate_propagation", "inner_only"),
    ),
]


ALL_PATTERNS: tuple[Pattern, ...] = tuple(
    CORRELATED + KEY_DEFEAT + WINDOW + SLOPPY + JOINS
)


def patterns_by_category() -> dict[str, list[Pattern]]:
    out: dict[str, list[Pattern]] = {}
    for pattern in ALL_PATTERNS:
        out.setdefault(pattern.category, []).append(pattern)
    return out


if __name__ == "__main__":  # pragma: no cover - manual inspection helper
    grouped = patterns_by_category()
    total = 0
    for category, items in sorted(grouped.items()):
        print(f"{category}: {len(items)}")
        for pattern in items:
            print(f"  {pattern.code:<28} [{pattern.severity}] {pattern.title}")
        total += len(items)
    print(f"\n{total} seed patterns across {len(grouped)} categories")
