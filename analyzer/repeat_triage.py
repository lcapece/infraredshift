"""Triage enrichment for repeat-query groups.

Joins each repeat-query family to the physical tables it touches and produces the
operator-facing verdict: is this recurring workload slow because the SQL is badly
written, because the tables it hits are badly designed, or both. Every verdict
carries evidence strings and concrete recommended fixes so the DBA team can act
without re-deriving the analysis.
"""
from __future__ import annotations

import math

import pandas as pd

from .redshift_meta import is_missing_sortkey

VERDICT_QUERY = "FIX QUERY"
VERDICT_TABLES = "FIX TABLES"
VERDICT_BOTH = "FIX BOTH"
VERDICT_MONITOR = "MONITOR"

COVERAGE_COMPLETE = "complete"
COVERAGE_PARTIAL = "partial"
COVERAGE_NONE = "none"

_SYSTEM_TABLE_PREFIXES = ("pg_", "svv_", "sys_", "stl_", "stv_", "svl_", "information_schema")

_GROUP_TABLE_COLUMNS = [
    "repeat_group_id",
    "table_key",
    "schema_name",
    "table_name",
    "diststyle",
    "sortkey1",
    "size_mb",
    "tbl_rows",
    "unsorted_pct",
    "stats_off",
    "skew_rows",
    "scan_query_count",
    "scan_duration_s",
    "scan_input_rows_m",
    "rrscan_query_pct",
    "full_scan_query_pct",
    "table_flags",
    "table_recommendation",
]


def build_repeat_triage(
    repeat_groups: pd.DataFrame,
    repeat_members: pd.DataFrame,
    table_review: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (enriched repeat_groups, per-group table detail frame)."""
    if repeat_groups is None or repeat_groups.empty:
        return repeat_groups, pd.DataFrame(columns=_GROUP_TABLE_COLUMNS)
    groups = repeat_groups.copy()
    table_index = _build_table_index(table_review)

    verdicts: list[str] = []
    query_flag_texts: list[str] = []
    table_flag_texts: list[str] = []
    recommendations: list[str] = []
    matched_counts: list[int] = []
    flagged_counts: list[int] = []
    group_table_rows: list[dict] = []

    coverage_levels: list[str] = []
    missing_lists: list[str] = []
    coverage_notes: list[str] = []

    for _, group in groups.iterrows():
        query_flags = _query_side_flags(group)
        tables = _group_table_names(group, repeat_members)
        checkable = _coverage_checkable_names(tables, group)
        matched, unmatched = _match_tables(checkable, table_index, group)
        table_flags_all: list[str] = []
        flagged_tables = 0
        for table_row in matched:
            flags, recommendation = _table_side_flags(table_row, group)
            if flags:
                flagged_tables += 1
            table_flags_all.extend(f"{table_row.get('table_name')}: {flag}" for flag in flags)
            group_table_rows.append(
                {
                    "repeat_group_id": group.get("repeat_group_id"),
                    "table_key": table_row.get("table_key", ""),
                    "schema_name": table_row.get("schema_name", ""),
                    "table_name": table_row.get("table_name", ""),
                    "diststyle": table_row.get("diststyle", ""),
                    "sortkey1": table_row.get("sortkey1", ""),
                    "size_mb": _to_float(table_row.get("size_mb")),
                    "tbl_rows": _to_float(table_row.get("tbl_rows")),
                    "unsorted_pct": _to_float(table_row.get("unsorted_pct")),
                    "stats_off": _to_float(table_row.get("stats_off")),
                    "skew_rows": _to_float(table_row.get("skew_rows")),
                    "scan_query_count": _to_float(table_row.get("scan_query_count")),
                    "scan_duration_s": _to_float(table_row.get("scan_duration_s")),
                    "scan_input_rows_m": _to_float(table_row.get("scan_input_rows_m")),
                    "rrscan_query_pct": _to_float(table_row.get("rrscan_query_pct")),
                    "full_scan_query_pct": _to_float(table_row.get("full_scan_query_pct")),
                    "table_flags": "; ".join(flags),
                    "table_recommendation": recommendation,
                }
            )

        has_query_problem = bool(query_flags)
        has_table_problem = flagged_tables > 0
        if has_query_problem and has_table_problem:
            verdict = VERDICT_BOTH
        elif has_query_problem:
            verdict = VERDICT_QUERY
        elif has_table_problem:
            verdict = VERDICT_TABLES
        else:
            verdict = VERDICT_MONITOR

        if not checkable:
            coverage = COVERAGE_COMPLETE
        elif not matched:
            coverage = COVERAGE_NONE
        elif unmatched:
            coverage = COVERAGE_PARTIAL
        else:
            coverage = COVERAGE_COMPLETE
        coverage_levels.append(coverage)
        missing_lists.append(", ".join(unmatched[:8]))
        coverage_notes.append(_coverage_note(coverage, unmatched, group))

        verdicts.append(verdict)
        query_flag_texts.append("; ".join(query_flags))
        table_flag_texts.append("; ".join(table_flags_all))
        recommendation = _group_recommendation(group, query_flags, table_flags_all, verdict)
        if coverage != COVERAGE_COMPLETE:
            recommendation += (
                "; capture SVV_TABLE_INFO for the missing database(s) to complete the table-design evidence"
            )
        recommendations.append(recommendation)
        matched_counts.append(len(matched))
        flagged_counts.append(flagged_tables)

    groups["triage_verdict"] = verdicts
    groups["triage_query_flags"] = query_flag_texts
    groups["triage_table_flags"] = table_flag_texts
    groups["triage_recommendation"] = recommendations
    groups["triage_tables_matched"] = matched_counts
    groups["triage_tables_flagged"] = flagged_counts
    groups["triage_stats_coverage"] = coverage_levels
    groups["triage_missing_tables"] = missing_lists
    groups["triage_coverage_note"] = coverage_notes
    groups["triage_priority_score"] = [
        _priority_score(row) for _, row in groups.iterrows()
    ]
    group_tables = pd.DataFrame(group_table_rows, columns=_GROUP_TABLE_COLUMNS)
    return groups, group_tables


def _build_table_index(table_review: pd.DataFrame) -> dict[str, dict]:
    """Index table_review rows by full key, schema.table, and (unambiguous) bare name."""
    index: dict[str, dict] = {}
    ambiguous: set[str] = set()
    if table_review is None or table_review.empty:
        return index
    for _, row in table_review.iterrows():
        record = row.to_dict()
        schema = str(row.get("schema_name") or "").strip().lower()
        name = str(row.get("table_name") or "").strip().lower()
        database = str(row.get("source_db") or row.get("database_name") or "").strip().lower()
        key = str(row.get("table_key") or "").strip().lower()
        if not name:
            continue
        for candidate in {
            key,
            f"{database}.{schema}.{name}" if database and schema else "",
            f"{schema}.{name}" if schema else "",
            name,
        }:
            if not candidate:
                continue
            if candidate in index and index[candidate].get("table_key") != record.get("table_key"):
                ambiguous.add(candidate)
                continue
            index[candidate] = record
    for candidate in ambiguous:
        # Bare names AND schema.table keys can collide across the cluster's
        # databases; dropping them forces an honest "no captured stats" result
        # instead of silently using another database's table stats.
        if candidate.count(".") <= 1:
            index.pop(candidate, None)
    return index


def _group_table_names(group: pd.Series, repeat_members: pd.DataFrame) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()

    def add(raw: object) -> None:
        for part in str(raw or "").split(","):
            text = part.strip().strip("`\"").lower()
            if text and text not in seen:
                seen.add(text)
                names.append(text)

    group_full = group.get("sql_tables_full")
    if isinstance(group_full, str) and group_full.strip():
        add(group_full)
    else:
        add(group.get("sql_tables"))
    group_id = group.get("repeat_group_id")
    if repeat_members is not None and not repeat_members.empty:
        member_col = "sql_tables_full" if "sql_tables_full" in repeat_members.columns else "sql_tables"
        if member_col in repeat_members.columns:
            members = repeat_members[repeat_members["repeat_group_id"] == group_id]
            for value in members[member_col].head(20):
                add(value)
    return names


def _coverage_checkable_names(names: list[str], group: pd.Series) -> list[str]:
    """Filter to names that should have SVV_TABLE_INFO stats on this producer
    cluster: drop CTE aliases, #temp tables, and system catalogs."""
    ctes = {
        part.strip().lower()
        for part in str(group.get("sql_ctes") or "").split(",")
        if part.strip()
    }
    checkable: list[str] = []
    for name in names:
        bare = name.split(".")[-1]
        if name.startswith("#") or bare.startswith("#"):
            continue
        if bare in ctes or name in ctes:
            continue
        if bare.startswith(_SYSTEM_TABLE_PREFIXES) or name.startswith(_SYSTEM_TABLE_PREFIXES):
            continue
        checkable.append(name)
    return checkable


def _match_tables(
    names: list[str], table_index: dict[str, dict], group: pd.Series | None = None
) -> tuple[list[dict], list[str]]:
    matched: list[dict] = []
    unmatched: list[str] = []
    seen_keys: set[str] = set()
    databases = [
        part.strip().lower()
        for part in str(group.get("databases") if group is not None else "").split(",")
        if part.strip()
    ]
    for name in names:
        record = table_index.get(name)
        if record is None and len(databases) == 1 and name.count(".") == 1:
            record = table_index.get(f"{databases[0]}.{name}")
        if record is None and name.count(".") >= 2:
            record = table_index.get(".".join(name.split(".")[-2:]))
        if record is None and name.count(".") >= 1:
            record = table_index.get(name.split(".")[-1])
        if record is None:
            unmatched.append(name)
            continue
        key = str(record.get("table_key") or record.get("table_name"))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        matched.append(record)
    return matched, unmatched


def _coverage_note(coverage: str, unmatched: list[str], group: pd.Series) -> str:
    if coverage == COVERAGE_COMPLETE:
        return ""
    databases = str(group.get("databases") or "").strip()
    shown = ", ".join(unmatched[:8])
    if len(unmatched) > 8:
        shown += f" (+{len(unmatched) - 8} more)"
    note = f"Table metadata unavailable for: {shown}."
    if databases:
        note += f" This pattern ran in database(s): {databases}; extract SVV_TABLE_INFO there for local tables."
    else:
        note += " Extract SVV_TABLE_INFO from the database(s) this pattern runs in for local tables."
    note += " Spectrum/external objects and datashares may not expose local sort/dist stats."
    return note


def _query_side_flags(group: pd.Series) -> list[str]:
    flags: list[str] = []
    if _to_float(group.get("wildcard_count")) > 0:
        flags.append("SELECT * projection pulls every column")
    if _to_float(group.get("predicate_count")) == 0 and _to_float(group.get("join_count")) > 0:
        flags.append("joins run with no row-restricting predicate")
    if _to_float(group.get("avg_total_spill")) > 0:
        flags.append("spills intermediate results to disk")
    if _to_float(group.get("avg_has_nested_loop")) >= 0.3:
        flags.append("nested-loop join in the plan")
    if _to_float(group.get("avg_dist_both_cnt")) >= 0.5:
        flags.append("DS_DIST_BOTH redistributes both join sides")
    if _to_float(group.get("avg_bcast_cnt")) >= 0.5:
        flags.append("broadcasts a large intermediate set")
    if _to_float(group.get("avg_remote_io_ratio")) >= 0.3:
        flags.append("heavy remote/S3 I/O share")
    if _to_float(group.get("avg_max_data_skewness")) >= 4:
        flags.append("skewed execution across slices")
    if _to_float(group.get("avg_selectivity_ratio")) >= 0.9 and _to_float(group.get("predicate_count")) == 0:
        flags.append("reads nearly every input row (no selective filter)")
    return flags


def _table_side_flags(table_row: dict, group: pd.Series) -> tuple[list[str], str]:
    flags: list[str] = []
    recs: list[str] = []
    table_name = str(table_row.get("table_name") or "table")
    sortkey = str(table_row.get("sortkey1") or "").strip().lower()
    diststyle = str(table_row.get("diststyle") or "").strip().lower()
    size_mb = _to_float(table_row.get("size_mb"))
    unsorted_pct = _to_float(table_row.get("unsorted_pct"))
    stats_off = _to_float(table_row.get("stats_off"))
    skew_rows = _to_float(table_row.get("skew_rows"))
    rrscan_pct = _to_float(table_row.get("rrscan_query_pct"))
    full_scan_pct = _to_float(table_row.get("full_scan_query_pct"))
    scan_queries = _to_float(table_row.get("scan_query_count"))
    filter_columns = str(group.get("sql_filter_columns") or "").strip()
    join_columns = str(group.get("sql_join_columns") or "").strip()

    if is_missing_sortkey(sortkey) and size_mb >= 100:
        flags.append("no sort key")
        target = filter_columns or "the columns this workload filters by"
        recs.append(f"add a SORTKEY to {table_name} (workload filters on: {target})")
    if unsorted_pct >= 20:
        flags.append(f"{unsorted_pct:.0f}% unsorted rows")
        recs.append(f"VACUUM SORT {table_name} and schedule regular vacuums")
    if stats_off >= 10:
        flags.append(f"statistics {stats_off:.0f}% stale")
        recs.append(f"ANALYZE {table_name}")
    # AUTO tables are "missing" a durable sort key, not "rarely pruning" with one.
    if scan_queries >= 3 and rrscan_pct <= 0.5 and not is_missing_sortkey(sortkey):
        flags.append(f"sort key rarely prunes scans ({rrscan_pct * 100:.0f}% range-restricted)")
        target = filter_columns or "the workload's filter columns"
        recs.append(f"re-evaluate {table_name} sort key against actual filters ({target})")
    if full_scan_pct >= 0.5 and scan_queries >= 3:
        flags.append(f"{full_scan_pct * 100:.0f}% of scans are full-table")
    if diststyle in {"even", "all"} and size_mb >= 1000:
        flags.append(f"DISTSTYLE {diststyle.upper()} on a {size_mb / 1024:.1f} GB table")
        target = join_columns or "the workload's join key"
        recs.append(f"consider DISTKEY on {table_name} using {target}")
    if skew_rows >= 4:
        flags.append(f"row distribution skew {skew_rows:.1f}x")
        recs.append(f"re-check {table_name} DISTKEY cardinality (skew {skew_rows:.1f}x)")
    return flags, "; ".join(recs)


def _group_recommendation(
    group: pd.Series,
    query_flags: list[str],
    table_flags: list[str],
    verdict: str,
) -> str:
    recs: list[str] = []
    if _to_float(group.get("wildcard_count")) > 0:
        recs.append("replace SELECT * with the needed columns")
    if _to_float(group.get("predicate_count")) == 0 and _to_float(group.get("join_count")) > 0:
        recs.append("push a row-restricting filter before the join")
    if _to_float(group.get("avg_total_spill")) > 0:
        recs.append("cut intermediate width or pre-aggregate before joining to stop disk spill")
    if _to_float(group.get("avg_dist_both_cnt")) >= 0.5 or _to_float(group.get("avg_bcast_cnt")) >= 0.5:
        join_cols = str(group.get("sql_join_columns") or "").strip()
        detail = f" (join columns: {join_cols})" if join_cols else ""
        recs.append(f"align join and distribution keys to stop data movement{detail}")
    if _to_float(group.get("avg_remote_io_ratio")) >= 0.3:
        recs.append("materialize the recurring external/S3 scan into a local staged table")
    if table_flags:
        recs.append("apply the per-table design fixes listed under Tables")
    if not recs:
        if verdict == VERDICT_MONITOR:
            runs = int(_to_float(group.get("query_count")))
            recs.append(
                f"no structural defect detected; {runs} runs of one shape — "
                "confirm the schedule is still needed and consider result caching"
            )
        else:
            recs.append("inspect the representative SQL; flagged evidence is listed above")
    return "; ".join(recs)


def _priority_score(group: pd.Series) -> float:
    runtime = _to_float(group.get("total_runtime_s"))
    runs = max(_to_float(group.get("query_count")), 1.0)
    spill = _to_float(group.get("avg_total_spill"))
    rows = _to_float(group.get("total_input_rows"))
    score = runtime + math.sqrt(runs) * 10.0
    if spill > 0:
        score *= 1.25
    if rows > 0:
        score += min(rows / 1e7, 50.0)
    return round(score, 1)


def _to_float(value: object) -> float:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(number) or math.isinf(number):
        return 0.0
    return number
