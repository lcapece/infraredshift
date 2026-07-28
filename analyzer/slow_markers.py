"""Slow-query markers: provably-present structural signals that correlate with
slowness, in plain English.

This is NOT an optimizer. It never claims a rewrite is faster. It identifies
markers that are *demonstrably present* in the SQL and the captured table
metadata - a cross join, a function wrapping a sortkey, a large table probed off
its distribution key, stale stats - and reports each one with the evidence that
fired it and a human-readable fix direction. A DBA reads the list and decides.

Pure logic, no Qt. Every marker is unit-testable.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from ._lazy_sqlglot import exp, sqlglot
from .join_size_highlight import alias_map
from .sql_xray import build_table_lookup, build_view_map, clean_token

LARGE_TABLE_MB = 5120.0        # 5 GB: "large" for join/dist decisions
STALE_STATS_PCT = 20.0         # stats_off at/above this is flagged
HEAVY_UNSORTED_PCT = 20.0      # unsorted at/above this is flagged
HIGH_SKEW = 4.0                # skew_rows at/above this is flagged
MANY_JOINS = 8                 # join count at/above this is flagged

# Severity ranks for ordering (higher = worse).
_SEVERITY_RANK = {"crit": 3, "warn": 2, "info": 1}


@dataclass(frozen=True)
class SlowMarker:
    key: str            # stable slug, e.g. "func-on-sortkey"
    severity: str       # "crit" | "warn" | "info"
    title: str          # short plain-English headline
    detail: str         # the concrete evidence that fired this marker
    fix: str            # human-readable direction (never a promise of speed)
    objects: tuple[str, ...] = field(default_factory=tuple)  # tables/columns cited

    @property
    def rank(self) -> int:
        return _SEVERITY_RANK.get(self.severity, 0)


def _try_parse(sql: str):
    try:
        return sqlglot.parse_one(str(sql or ""), read="redshift")
    except Exception:
        return None


_FUNC_ON_COL_RE = re.compile(r"\b([a-z_][\w$]*)\s*\(\s*((?:[a-z_][\w$]*\.)?[a-z_][\w$]*)\s*[,)]", re.IGNORECASE)


def _resolve(col_token: str, aliases: dict, lookup: dict) -> dict | None:
    token = clean_token(col_token)
    if "." not in token:
        # Bare column: only resolvable when the statement has a single table.
        tables = set(aliases.values())
        if len(tables) == 1:
            table = next(iter(tables))
            return lookup.get(table) or lookup.get(table.split(".")[-1])
        return None
    qualifier = token.rsplit(".", 1)[0].split(".")[-1]
    table = aliases.get(qualifier, qualifier)
    return lookup.get(table) or lookup.get(table.split(".")[-1])


def _col_name(col_token: str) -> str:
    return clean_token(col_token).rsplit(".", 1)[-1]


def detect_markers(
    sql: str,
    table_rows=None,
    view_rows=None,
) -> list[SlowMarker]:
    """Return the slow-markers present in `sql`, most-severe first.

    table_rows: Table Review records (size, sortkey, diststyle, unsorted,
    stats_off, skew). view_rows: captured view definitions, so a reference to a
    view is itself flagged (hidden joins). Both optional - structural markers
    (cross join, wildcard, window, function-on-column) fire from the SQL alone;
    metadata markers only fire when the table is captured."""
    markers: list[SlowMarker] = []
    text = str(sql or "")
    if not text.strip():
        return markers
    aliases = alias_map(text)
    lookup = build_table_lookup(table_rows.to_dict("records") if hasattr(table_rows, "to_dict") else (table_rows or []))
    view_map = build_view_map(view_rows.to_dict("records") if hasattr(view_rows, "to_dict") else (view_rows or []))
    tree = _try_parse(text)

    _structural_markers(text, tree, markers)
    _view_markers(text, aliases, view_map, markers)
    _table_metadata_markers(text, tree, aliases, lookup, markers)

    markers.sort(key=lambda m: (-m.rank, m.key))
    # De-dup by (key, objects) so the same finding on the same object appears once.
    seen: set[tuple] = set()
    unique: list[SlowMarker] = []
    for marker in markers:
        sig = (marker.key, marker.objects)
        if sig in seen:
            continue
        seen.add(sig)
        unique.append(marker)
    return unique


def _structural_markers(text: str, tree, markers: list[SlowMarker]) -> None:
    lowered = text.lower()

    # SELECT * — reads every column, defeats column pruning.
    if tree is not None:
        stars = list(tree.find_all(exp.Star))
    else:
        stars = re.findall(r"select\s+\*", lowered)
    if stars:
        markers.append(SlowMarker(
            "select-star", "warn",
            "SELECT * reads every column",
            f"{len(stars)} wildcard projection(s) found. Redshift is columnar, so unused columns are still scanned.",
            "Project only the columns you use so the scan touches fewer column blocks.",
        ))

    if tree is not None:
        # Cross join / cartesian product.
        cross = [j for j in tree.find_all(exp.Join) if (j.kind or "").upper() == "CROSS" or (not j.args.get("on") and not j.args.get("using") and j.kind != "" and (j.side or j.kind))]
        explicit_cross = [j for j in tree.find_all(exp.Join) if (j.kind or "").upper() == "CROSS"]
        if explicit_cross:
            markers.append(SlowMarker(
                "cross-join", "crit",
                "Cross join (cartesian product)",
                f"{len(explicit_cross)} CROSS JOIN(s): every left row pairs with every right row.",
                "Confirm the cartesian is intended; otherwise add the missing join predicate.",
            ))

        # A join with no ON/USING (comma join without a WHERE-side link is a common cartesian).
        unlinked = [
            j for j in tree.find_all(exp.Join)
            if not j.args.get("on") and not j.args.get("using") and (j.kind or "").upper() != "CROSS"
        ]
        if unlinked:
            markers.append(SlowMarker(
                "join-no-condition", "crit",
                "Join with no ON/USING condition",
                f"{len(unlinked)} join(s) have no join key - this multiplies rows like a cross join.",
                "Add the join predicate that links the two tables.",
            ))

        # Window functions — sort + partition, expensive at scale.
        windows = list(tree.find_all(exp.Window))
        if windows:
            markers.append(SlowMarker(
                "window-functions", "info",
                "Window functions present",
                f"{len(windows)} window function(s) (OVER ...). Each partitions and sorts the intermediate result.",
                "Fine in moderation; if slow, check the PARTITION BY / ORDER BY columns against the sort key.",
            ))

        # DISTINCT over a wide/aggregated set.
        if list(tree.find_all(exp.Distinct)):
            markers.append(SlowMarker(
                "distinct", "info",
                "DISTINCT forces a full de-duplication",
                "DISTINCT sorts or hashes the entire result to remove duplicates.",
                "Confirm duplicates are actually possible; a GROUP BY or better join key may avoid it.",
            ))

        # Many joins — planner search space and intermediate blow-up.
        join_count = sum(1 for _ in tree.find_all(exp.Join))
        if join_count >= MANY_JOINS:
            markers.append(SlowMarker(
                "many-joins", "warn",
                f"High join count ({join_count} joins)",
                f"{join_count} joins in one statement enlarge intermediate results and the planner's search.",
                "Consider staging intermediate results, or confirm every join is necessary.",
            ))

        # Correlated / deep subqueries.
        subqueries = list(tree.find_all(exp.Subquery))
        if len(subqueries) >= 3:
            markers.append(SlowMarker(
                "many-subqueries", "info",
                f"Nested subqueries ({len(subqueries)})",
                f"{len(subqueries)} subquery blocks. Deep nesting can hide repeated scans of the same table.",
                "Check whether a CTE or a single join would read each base table once.",
            ))

    # LIKE with a leading wildcard — cannot use any index/zone map.
    if re.search(r"like\s+'%%|like\s+'%", lowered) or re.search(r"like\s+'\s*%", lowered):
        markers.append(SlowMarker(
            "leading-wildcard-like", "warn",
            "LIKE with a leading wildcard",
            "A pattern like '%value' cannot prune by zone map - it scans every row.",
            "Anchor the pattern ('value%') or use a different predicate if possible.",
        ))


def _view_markers(text: str, aliases: dict, view_map: dict, markers: list[SlowMarker]) -> None:
    if not view_map:
        return
    referenced = set()
    for token in set(aliases.values()) | set(re.findall(r"(?:from|join)\s+([a-z_][\w$.\"]*)", text, re.IGNORECASE)):
        name = clean_token(token)
        if name in view_map or name.split(".")[-1] in view_map:
            referenced.add(name)
    if referenced:
        markers.append(SlowMarker(
            "hidden-view-joins", "warn",
            "Query reads through view(s) - joins are hidden",
            f"References {len(referenced)} captured view(s): {', '.join(sorted(referenced)[:5])}. "
            "Their internal joins and functions are not visible in this statement.",
            "Use Explode Views to inline them, then re-check markers on the real joins.",
            tuple(sorted(referenced)),
        ))


def _table_metadata_markers(text: str, tree, aliases: dict, lookup: dict, markers: list[SlowMarker]) -> None:
    if not lookup:
        return

    # 1. Large table joined off its distribution key / sort key.
    join_pairs = _equality_column_pairs(text)
    for left, right in join_pairs:
        for side in (left, right):
            entry = _resolve(side, aliases, lookup)
            if entry is None:
                continue
            size = entry.get("size_mb") or 0.0
            if size < LARGE_TABLE_MB:
                continue
            col = _col_name(side)
            sortkey = (entry.get("sortkey1") or "").lower()
            diststyle = (entry.get("diststyle") or "").lower()
            distkey_match = re.search(r"key\s*\(\s*([^)]+?)\s*\)", diststyle)
            distkey = (distkey_match.group(1).strip().lower() if distkey_match else "")
            table = entry.get("table") or side
            if distkey and col != distkey:
                markers.append(SlowMarker(
                    "join-off-distkey", "crit",
                    f"Large table joined off its DISTKEY: {table}",
                    f"{table} is {size / 1024:.0f} GB, distributed on '{distkey}', but joined on '{col}'. "
                    "Rows are redistributed across nodes for this join.",
                    f"Align the join key with the DISTKEY, or set DISTKEY({col}) if this join dominates.",
                    (table,),
                ))
            if sortkey and col != sortkey:
                markers.append(SlowMarker(
                    "join-off-sortkey", "warn",
                    f"Large table probed off its SORTKEY: {table}",
                    f"{table} is {size / 1024:.0f} GB, sorted on '{sortkey}', but joined/filtered on '{col}'. "
                    "The sort key gives no scan pruning here.",
                    f"Consider a SORTKEY that includes '{col}', or confirm the current key serves other queries.",
                    (table,),
                ))

    # 2. Function wrapping a sort/dist/filter column — non-sargable.
    for match in _FUNC_ON_COL_RE.finditer(text):
        func, col_token = match.group(1), match.group(2)
        if func.lower() in {"count", "sum", "avg", "min", "max", "coalesce", "cast", "as"}:
            continue
        entry = _resolve(col_token, aliases, lookup)
        if entry is None:
            continue
        col = _col_name(col_token)
        sortkey = (entry.get("sortkey1") or "").lower()
        if sortkey and col == sortkey:
            table = entry.get("table") or ""
            markers.append(SlowMarker(
                "func-on-sortkey", "crit",
                f"Function wraps the SORTKEY: {func}({col})",
                f"'{col}' is the sort key of {table}, but it is wrapped in {func}(). "
                "Wrapping a column defeats zone-map pruning - the scan cannot skip blocks.",
                f"Rewrite so {col} is compared bare (move the function to the constant side).",
                (table,) if table else (),
            ))

    # 3. Stale stats / heavy unsorted / high skew on any referenced table.
    referenced = set(aliases.values())
    if tree is not None:
        referenced |= {clean_token(_table_name(t)) for t in tree.find_all(exp.Table)}
    for name in sorted(x for x in referenced if x):
        entry = lookup.get(name) or lookup.get(name.split(".")[-1])
        if entry is None:
            continue
        table = entry.get("table") or name
        stats_off = entry.get("stats_off")
        if stats_off is not None and stats_off >= STALE_STATS_PCT:
            markers.append(SlowMarker(
                "stale-stats", "warn",
                f"Stale statistics: {table}",
                f"{table} has {stats_off:.0f}% stale stats. The planner may misestimate rows and pick a bad plan.",
                f"Run ANALYZE {table}; (safe, non-destructive).",
                (table,),
            ))
        unsorted = entry.get("unsorted")
        if unsorted is not None and unsorted >= HEAVY_UNSORTED_PCT:
            markers.append(SlowMarker(
                "heavily-unsorted", "warn",
                f"Heavily unsorted: {table}",
                f"{table} is {unsorted:.0f}% unsorted. Sort-key scan pruning is degraded until it is vacuumed.",
                f"Run VACUUM SORT ONLY {table}; during a maintenance window.",
                (table,),
            ))
        skew = entry.get("skew_rows")
        if skew is not None and skew >= HIGH_SKEW:
            markers.append(SlowMarker(
                "data-skew", "warn",
                f"Data skew across nodes: {table}",
                f"{table} has a row skew of {skew:.1f}x. One slice holds far more data, so it becomes the bottleneck.",
                "Review the DISTKEY - a skewed key concentrates rows on one node.",
                (table,),
            ))


def _table_name(table_node) -> str:
    try:
        parts = [p.name for p in (table_node.args.get("catalog"), table_node.args.get("db"), table_node.this) if p]
        return ".".join(part for part in parts if part)
    except Exception:
        return str(getattr(table_node, "name", "") or "")


_EQ_PAIR_RE = re.compile(
    r"((?:[a-z_][\w$]*\.)?[a-z_][\w$]*)\s*=\s*((?:[a-z_][\w$]*\.)?[a-z_][\w$]*)",
    re.IGNORECASE,
)


def _equality_column_pairs(text: str) -> list[tuple[str, str]]:
    """column = column pairs (both sides look like identifiers, not literals)."""
    pairs = []
    for match in _EQ_PAIR_RE.finditer(text):
        left, right = match.group(1), match.group(2)
        if left[0].isdigit() or right[0].isdigit():
            continue
        pairs.append((left, right))
    return pairs


def markers_summary(markers: list[SlowMarker]) -> str:
    """One-line headline: 'X critical, Y warnings, Z notes'."""
    crit = sum(1 for m in markers if m.severity == "crit")
    warn = sum(1 for m in markers if m.severity == "warn")
    info = sum(1 for m in markers if m.severity == "info")
    if not markers:
        return "No slow-query markers detected."
    bits = []
    if crit:
        bits.append(f"{crit} critical")
    if warn:
        bits.append(f"{warn} warning{'s' if warn != 1 else ''}")
    if info:
        bits.append(f"{info} note{'s' if info != 1 else ''}")
    return ", ".join(bits) + " detected."
