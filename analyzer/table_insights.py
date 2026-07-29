"""Critical table insights: physical design measured against observed workload.

Every scenario answers one question with evidence already captured, and each
row carries the numbers behind its verdict so a DBA can check it rather than
trust it. Nothing here predicts; it counts.

Qt-free so the scenarios can be unit tested and scripted.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import re

import pandas as pd


# A column has to be used by more than one pattern before it is worth
# proposing as a sort key: a single query is an anecdote.
MIN_PATTERNS_FOR_RECOMMENDATION = 2

# Redshift compound sort keys stop earning much past three or four columns.
COMPOUND_SORT_KEY_WIDTH = 3

LARGE_TABLE_ROW_THRESHOLD = 10_000_000

# A filter is what a sort key actually accelerates (zone-map pruning). Join
# columns matter too, but they are the distribution key's job, so they count
# for less when ranking sort-key candidates.
FILTER_WEIGHT = 1.0
JOIN_WEIGHT = 0.4


@dataclass
class Scenario:
    key: str
    title: str
    question: str
    detail: str


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        "spectrum_scanned",
        "Spectrum tables by gigabytes scanned",
        "Which external tables move the most data?",
        "Ranked by gross GB scanned in the captured window. Scan volume is "
        "what Spectrum bills and what dominates external query time.",
    ),
    Scenario(
        "spectrum_unpartitioned",
        "Spectrum tables in active use with no partition key",
        "Which external tables are scanned in full every time?",
        "An external table with no partition key cannot be pruned - every "
        "query reads everything. Only tables with observed activity appear.",
    ),
    Scenario(
        "sort_key_fit",
        "Large tables: declared sort key vs. observed filters",
        "Does the sort key match how the table is actually queried?",
        "For tables over 10M rows, ranks the predicate columns seen across "
        "grouped query patterns and scores how well the declared SORTKEY "
        "matches. Proposes a compound key from the top columns.",
    ),
    Scenario(
        "distribution_fit",
        "Large EVEN-distributed tables that are joined",
        "Which tables are paying to redistribute on every join?",
        "A table with DISTSTYLE EVEN has no co-location. When patterns "
        "consistently join it on one column, that column is a distribution "
        "key candidate.",
    ),
    Scenario(
        "maintenance_debt",
        "Tables where maintenance is costing the most",
        "Where would VACUUM and ANALYZE buy back the most?",
        "Unsorted fraction and stale statistics, weighted by the table's "
        "size and by how much captured query time actually touches it.",
    ),
)


@dataclass
class InsightResult:
    scenario: Scenario
    rows: pd.DataFrame = field(default_factory=pd.DataFrame)
    headline: str = ""
    note: str = ""


def _text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _number(value: object, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return default if pd.isna(result) else result


def _split(value: object) -> list[str]:
    text = _text(value)
    if not text:
        return []
    return [item.strip().lower() for item in re.split(r"[,;|]\s*", text) if item.strip()]


def _bare_column(value: str) -> str:
    """Strip a table alias: "f.event_date" -> "event_date".

    The parser records columns as written, so the same physical column arrives
    as ``f.event_date`` from one pattern and ``event_date`` from another.
    Counting those separately splits the evidence for a sort key exactly where
    it needs to accumulate, and puts an alias no DDL can use into the proposed
    SORTKEY.
    """
    text = _text(value).lower()
    return text.rsplit(".", 1)[-1] if "." in text else text


def _split_columns(value: object) -> list[str]:
    """Column names with aliases removed, de-duplicated, order preserved."""
    seen: list[str] = []
    for item in _split(value):
        column = _bare_column(item)
        if column and column not in seen:
            seen.append(column)
    return seen


def _table_leaf(name: str) -> str:
    """Last component of a qualified name, for matching across notations."""
    return _text(name).lower().rsplit(".", 1)[-1]


def _missing_sortkey(value: object) -> bool:
    text = "".join(_text(value).upper().split())
    return text in {"", "-", "NONE", "NULL", "NAN", "AUTO", "AUTO(SORTKEY)", "SORTKEY"}


def _is_even_distribution(value: object) -> bool:
    text = "".join(_text(value).upper().split())
    return text.startswith("EVEN") or text in {"AUTO(EVEN)", "AUTO"}


def column_demand(groups: pd.DataFrame) -> dict[str, dict[str, float]]:
    """Per table, how often each column is filtered or joined on.

    Weighted by the pattern's captured runtime: a column used by a pattern
    that burns ten hours matters more than one used by a pattern that burns
    ten seconds. Returns {table_leaf: {column: score}}.
    """
    demand: dict[str, dict[str, float]] = {}
    if groups is None or groups.empty:
        return demand

    for _index, group in groups.iterrows():
        tables = _split(group.get("sql_tables")) or _split(group.get("sql_tables_full"))
        if not tables:
            continue
        runtime = _number(group.get("total_runtime_s"))
        runs = max(1.0, _number(group.get("query_count"), 1.0))
        # Weight by cost, with a floor so a cheap-but-frequent pattern still
        # registers rather than being rounded away.
        weight = max(1.0, runtime / 3600.0) * runs

        for column in _split_columns(group.get("sql_filter_columns")):
            for table in tables:
                bucket = demand.setdefault(_table_leaf(table), {})
                bucket[column] = bucket.get(column, 0.0) + weight * FILTER_WEIGHT
        for column in _split_columns(group.get("sql_join_columns")):
            for table in tables:
                bucket = demand.setdefault(_table_leaf(table), {})
                bucket[column] = bucket.get(column, 0.0) + weight * JOIN_WEIGHT
    return demand


def pattern_counts(groups: pd.DataFrame) -> dict[str, dict[str, int]]:
    """Per table, how many distinct patterns use each column."""
    counts: dict[str, dict[str, int]] = {}
    if groups is None or groups.empty:
        return counts
    for _index, group in groups.iterrows():
        tables = _split(group.get("sql_tables")) or _split(group.get("sql_tables_full"))
        columns = set(_split_columns(group.get("sql_filter_columns"))) | set(
            _split_columns(group.get("sql_join_columns"))
        )
        for table in tables:
            bucket = counts.setdefault(_table_leaf(table), {})
            for column in columns:
                bucket[column] = bucket.get(column, 0) + 1
    return counts


def sort_key_fit_score(declared: object, ranked_columns: list[str]) -> tuple[int, str]:
    """Score 0-100 for how well the declared sort key matches demand.

    100 - the declared key is the most-demanded column.
    70  - it is in the top three, but not first.
    30  - it is used, but outside the top three.
    0   - no sort key, or the declared key is never filtered on.

    Deliberately coarse. A finer scale would imply a precision this evidence
    does not have, and the verdict text carries the actual reasoning.
    """
    if not ranked_columns:
        return 0, "No filter or join columns were observed for this table."
    if _missing_sortkey(declared):
        return 0, f"No sort key declared; workload filters on {ranked_columns[0]}."

    # Normalise the declared key the same way as the observed columns, so
    # "f.event_date" and "event_date" compare equal.
    key = _bare_column(_text(declared).lower().split("(")[0].strip())
    if key == ranked_columns[0]:
        return 100, f"Declared sort key {key} is the most-filtered column."
    if key in ranked_columns[:COMPOUND_SORT_KEY_WIDTH]:
        position = ranked_columns.index(key) + 1
        return 70, (
            f"Declared sort key {key} is used, but ranks #{position} behind "
            f"{ranked_columns[0]}."
        )
    if key in ranked_columns:
        return 30, (
            f"Declared sort key {key} is rarely filtered on; "
            f"{ranked_columns[0]} dominates."
        )
    return 0, (
        f"Declared sort key {key} is never filtered or joined on in the "
        f"captured workload; {ranked_columns[0]} is."
    )


# The same physical facts arrive under two column vocabularies: the raw
# svv_table_info_all capture ("table", "schema", "size") and the prepared
# heat-map frame ("table_name", "schema_name", "size_mb"). Reading only one
# silently produced zero rows from a frame that was fully populated.
_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "table": ("table", "table_name"),
    "schema": ("schema", "schema_name"),
    "rows": ("tbl_rows", "rows"),
    "size_mb": ("size", "size_mb"),
    "sortkey": ("sortkey1", "sortkey"),
    "diststyle": ("diststyle",),
    "unsorted": ("unsorted", "unsorted_pct"),
    "stats_off": ("stats_off", "stats_off_pct"),
}


def _field(row, logical: str, default=None):
    """Read a logical field regardless of which vocabulary the frame uses."""
    for candidate in _COLUMN_ALIASES.get(logical, (logical,)):
        if candidate in row.index:
            value = row[candidate]
            if value is not None and not (
                not isinstance(value, str) and pd.isna(value)
            ):
                return value
    return default


def _row_column(frame: pd.DataFrame, logical: str) -> str:
    for candidate in _COLUMN_ALIASES.get(logical, (logical,)):
        if candidate in frame.columns:
            return candidate
    return ""


def _large_tables(table_review: pd.DataFrame) -> pd.DataFrame:
    if table_review is None or table_review.empty:
        return pd.DataFrame()
    frame = table_review.copy()
    column = _row_column(frame, "rows")
    if not column:
        return pd.DataFrame()
    rows = pd.to_numeric(frame[column], errors="coerce").fillna(0)
    return frame[rows >= LARGE_TABLE_ROW_THRESHOLD]


def sort_key_fit(groups: pd.DataFrame, table_review: pd.DataFrame) -> InsightResult:
    scenario = SCENARIOS[2]
    large = _large_tables(table_review)
    if large.empty:
        return InsightResult(
            scenario,
            headline="No tables over 10 million rows were captured.",
            note="Load Table Review to populate physical table metadata.",
        )

    demand = column_demand(groups)
    counts = pattern_counts(groups)
    if not demand:
        return InsightResult(
            scenario,
            headline=f"{len(large):,} large table(s), but no query patterns to compare against.",
            note="Load the repeat-query analysis so filters and joins can be measured.",
        )

    rows: list[dict[str, object]] = []
    for _index, table in large.iterrows():
        name = _text(_field(table, "table"))
        leaf = _table_leaf(name)
        scores = demand.get(leaf, {})
        if not scores:
            continue
        seen = counts.get(leaf, {})
        ranked = [
            column
            for column, _score in sorted(scores.items(), key=lambda item: -item[1])
            if seen.get(column, 0) >= MIN_PATTERNS_FOR_RECOMMENDATION
        ]
        if not ranked:
            continue
        declared = _field(table, "sortkey")
        score, verdict = sort_key_fit_score(declared, ranked)
        top = ranked[:COMPOUND_SORT_KEY_WIDTH]
        rows.append(
            {
                "schema": _text(_field(table, "schema")),
                "table": name,
                "rows": int(_number(_field(table, "rows"))),
                "size_mb": int(_number(_field(table, "size_mb"))),
                "declared_sortkey": _text(declared) or "(none)",
                "fit_score": score,
                "verdict": verdict,
                "predicate_1": top[0] if len(top) > 0 else "",
                "predicate_2": top[1] if len(top) > 1 else "",
                "predicate_3": top[2] if len(top) > 2 else "",
                "proposed_sortkey": f"SORTKEY({', '.join(top)})",
                "patterns_using": max(seen.get(column, 0) for column in top),
            }
        )

    if not rows:
        return InsightResult(
            scenario,
            headline="No large table had a column used by two or more patterns.",
            note=(
                "A single pattern is an anecdote; recommendations need a column "
                "that more than one pattern relies on."
            ),
        )

    frame = pd.DataFrame(rows).sort_values(
        ["fit_score", "rows"], ascending=[True, False]
    ).reset_index(drop=True)
    poor = int((frame["fit_score"] < 70).sum())
    return InsightResult(
        scenario,
        rows=frame,
        headline=(
            f"{len(frame):,} large table(s) assessed; {poor:,} have a sort key "
            f"that does not match the observed workload."
        ),
        note=(
            "Worst fit first. proposed_sortkey is a compound key built from the "
            "top predicate columns, ranked by captured runtime. Verify against "
            "your own EXPLAIN before applying."
        ),
    )


def distribution_fit(groups: pd.DataFrame, table_review: pd.DataFrame) -> InsightResult:
    scenario = SCENARIOS[3]
    large = _large_tables(table_review)
    if large.empty:
        return InsightResult(
            scenario, headline="No tables over 10 million rows were captured."
        )

    join_demand: dict[str, dict[str, float]] = {}
    join_patterns: dict[str, dict[str, int]] = {}
    if groups is not None and not groups.empty:
        for _index, group in groups.iterrows():
            columns = _split_columns(group.get("sql_join_columns"))
            if not columns:
                continue
            runtime = _number(group.get("total_runtime_s"))
            for table in _split(group.get("sql_tables")):
                leaf = _table_leaf(table)
                bucket = join_demand.setdefault(leaf, {})
                seen = join_patterns.setdefault(leaf, {})
                for column in columns:
                    bucket[column] = bucket.get(column, 0.0) + max(1.0, runtime / 3600.0)
                    seen[column] = seen.get(column, 0) + 1

    rows: list[dict[str, object]] = []
    for _index, table in large.iterrows():
        if not _is_even_distribution(_field(table, "diststyle")):
            continue
        leaf = _table_leaf(_field(table, "table"))
        scores = join_demand.get(leaf, {})
        seen = join_patterns.get(leaf, {})
        ranked = [
            column
            for column, _score in sorted(scores.items(), key=lambda item: -item[1])
            if seen.get(column, 0) >= MIN_PATTERNS_FOR_RECOMMENDATION
        ]
        if not ranked:
            continue
        rows.append(
            {
                "schema": _text(_field(table, "schema")),
                "table": _text(_field(table, "table")),
                "rows": int(_number(_field(table, "rows"))),
                "size_mb": int(_number(_field(table, "size_mb"))),
                "diststyle": _text(_field(table, "diststyle")),
                "join_column": ranked[0],
                "patterns_joining": seen.get(ranked[0], 0),
                "proposed_distkey": f"DISTKEY({ranked[0]})",
                "verdict": (
                    f"EVEN distribution, but {seen.get(ranked[0], 0)} pattern(s) "
                    f"join on {ranked[0]} - every one pays to redistribute."
                ),
            }
        )

    if not rows:
        return InsightResult(
            scenario,
            headline="No large EVEN-distributed table is consistently joined.",
            note="EVEN is the right choice for a table nothing joins on.",
        )

    frame = pd.DataFrame(rows).sort_values(
        ["patterns_joining", "rows"], ascending=[False, False]
    ).reset_index(drop=True)
    return InsightResult(
        scenario,
        rows=frame,
        headline=f"{len(frame):,} large EVEN table(s) are joined on a consistent column.",
        note=(
            "A DISTKEY on that column co-locates the join. Check the column's "
            "cardinality first - a low-cardinality key causes skew."
        ),
    )


def maintenance_debt(groups: pd.DataFrame, table_review: pd.DataFrame) -> InsightResult:
    scenario = SCENARIOS[4]
    if table_review is None or table_review.empty:
        return InsightResult(scenario, headline="No table metadata was captured.")

    touched: dict[str, float] = {}
    if groups is not None and not groups.empty:
        for _index, group in groups.iterrows():
            runtime = _number(group.get("total_runtime_s"))
            for table in _split(group.get("sql_tables")):
                leaf = _table_leaf(table)
                touched[leaf] = touched.get(leaf, 0.0) + runtime

    rows: list[dict[str, object]] = []
    for _index, table in table_review.iterrows():
        unsorted_pct = _number(_field(table, "unsorted"))
        stats_off = _number(_field(table, "stats_off"))
        size_mb = _number(_field(table, "size_mb"))
        if unsorted_pct <= 0 and stats_off <= 0:
            continue
        leaf = _table_leaf(_field(table, "table"))
        runtime = touched.get(leaf, 0.0)
        # Debt only matters where queries actually land, so workload time is a
        # multiplier rather than a tiebreak.
        impact = (unsorted_pct + stats_off) * (size_mb / 1024.0) * (1.0 + runtime / 3600.0)
        if impact <= 0:
            continue
        actions = []
        if unsorted_pct >= 10:
            actions.append("VACUUM")
        if stats_off >= 10:
            actions.append("ANALYZE")
        rows.append(
            {
                "schema": _text(_field(table, "schema")),
                "table": _text(_field(table, "table")),
                "size_gb": round(size_mb / 1024.0, 2),
                "unsorted_pct": round(unsorted_pct, 1),
                "stats_off_pct": round(stats_off, 1),
                "captured_runtime_h": round(runtime / 3600.0, 2),
                "impact_score": round(impact, 1),
                "action": " + ".join(actions) or "monitor",
            }
        )

    if not rows:
        return InsightResult(
            scenario,
            headline="No table shows meaningful sort or statistics debt.",
            note="Every captured table is sorted and has current statistics.",
        )

    frame = pd.DataFrame(rows).sort_values("impact_score", ascending=False).reset_index(drop=True)
    urgent = int((frame["action"] != "monitor").sum())
    return InsightResult(
        scenario,
        rows=frame,
        headline=(
            f"{urgent:,} table(s) would benefit from VACUUM or ANALYZE now, "
            f"of {len(frame):,} carrying some debt."
        ),
        note=(
            "impact_score combines unsorted fraction, stale statistics and size, "
            "multiplied by captured query time - debt on a table nothing queries "
            "is not urgent."
        ),
    )


def spectrum_scanned(external_tables: pd.DataFrame) -> InsightResult:
    scenario = SCENARIOS[0]
    if external_tables is None or external_tables.empty:
        return InsightResult(
            scenario,
            headline="No external (Spectrum) telemetry was captured.",
            note=(
                "External capture is opt-in. Run with --external-tables to "
                "populate it."
            ),
        )
    frame = external_tables.copy()
    frame["gross_scan_gb"] = pd.to_numeric(
        frame.get("gross_scan_gb"), errors="coerce"
    ).fillna(0.0)
    frame = frame[frame["gross_scan_gb"] > 0]
    if frame.empty:
        return InsightResult(
            scenario, headline="No external table recorded any scanned bytes."
        )

    columns = [
        c
        for c in (
            "external_table_key", "gross_scan_gb", "gross_output_gb",
            "query_count", "partition_key_columns", "external_duration_s",
        )
        if c in frame.columns
    ]
    out = frame.sort_values("gross_scan_gb", ascending=False)[columns].reset_index(drop=True)
    total = float(out["gross_scan_gb"].sum())
    return InsightResult(
        scenario,
        rows=out,
        headline=f"{len(out):,} external table(s) scanned {total:,.1f} GB in the captured window.",
        note="Scan volume is what Spectrum bills, and what dominates external query time.",
    )


def spectrum_unpartitioned(external_tables: pd.DataFrame) -> InsightResult:
    scenario = SCENARIOS[1]
    if external_tables is None or external_tables.empty:
        return InsightResult(
            scenario,
            headline="No external (Spectrum) telemetry was captured.",
            note="External capture is opt-in. Run with --external-tables.",
        )
    frame = external_tables.copy()
    for column in ("gross_scan_gb", "query_count", "partition_key_count"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0)

    active = frame[frame.get("query_count", pd.Series(0, index=frame.index)) > 0]
    if "partition_key_count" in active.columns:
        unpartitioned = active[active["partition_key_count"] <= 0]
    else:
        keys = active.get("partition_key_columns", pd.Series("", index=active.index))
        unpartitioned = active[keys.fillna("").astype(str).str.strip() == ""]

    if unpartitioned.empty:
        return InsightResult(
            scenario,
            headline="Every actively-used external table has a partition key.",
            note="Nothing here is being scanned in full unnecessarily.",
        )

    columns = [
        c
        for c in (
            "external_table_key", "query_count", "gross_scan_gb",
            "external_duration_s", "s3_location",
        )
        if c in unpartitioned.columns
    ]
    out = unpartitioned.sort_values(
        "gross_scan_gb" if "gross_scan_gb" in columns else columns[0], ascending=False
    )[columns].reset_index(drop=True)
    wasted = float(out.get("gross_scan_gb", pd.Series(dtype=float)).sum())
    return InsightResult(
        scenario,
        rows=out,
        headline=(
            f"{len(out):,} actively-used external table(s) have NO partition key "
            f"- {wasted:,.1f} GB scanned with no pruning possible."
        ),
        note=(
            "Without a partition key every query reads the whole table. "
            "Partitioning on the column these queries filter on is the fix."
        ),
    )


def run_scenario(
    key: str,
    *,
    groups: pd.DataFrame | None = None,
    table_review: pd.DataFrame | None = None,
    external_tables: pd.DataFrame | None = None,
) -> InsightResult:
    """Run one scenario by key."""
    groups = groups if groups is not None else pd.DataFrame()
    table_review = table_review if table_review is not None else pd.DataFrame()
    external_tables = external_tables if external_tables is not None else pd.DataFrame()

    if key == "spectrum_scanned":
        return spectrum_scanned(external_tables)
    if key == "spectrum_unpartitioned":
        return spectrum_unpartitioned(external_tables)
    if key == "sort_key_fit":
        return sort_key_fit(groups, table_review)
    if key == "distribution_fit":
        return distribution_fit(groups, table_review)
    if key == "maintenance_debt":
        return maintenance_debt(groups, table_review)
    raise KeyError(f"unknown scenario: {key}")
