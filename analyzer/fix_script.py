"""Generate a DBA-approvable fix script from triage findings.

Safe maintenance (ANALYZE, VACUUM SORT ONLY) is emitted as runnable SQL.
Design changes (ALTER SORTKEY / ALTER DISTSTYLE) are emitted COMMENTED OUT with
their evidence, so a DBA must review and consciously uncomment them. Every
statement carries the finding that produced it.

CLI:
    python -m analyzer.fix_script --output fix_script.sql
"""
from __future__ import annotations

import argparse
import math
import re
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd

from .redshift_meta import MISSING_SORTKEY_VALUES, is_missing_sortkey

_MISSING_SORTKEY_VALUES = set(MISSING_SORTKEY_VALUES)
_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_$]*$")


def _to_float(value: object) -> float:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if (math.isnan(number) or math.isinf(number)) else number


def _clean_columns(raw: object) -> list[str]:
    """Strip alias prefixes from 'a.col, b.col2' style lists; keep plain idents."""
    out: list[str] = []
    for part in str(raw or "").split(","):
        name = part.strip().lower()
        if "." in name:
            name = name.split(".")[-1]
        if name and _IDENT_RE.match(name):
            out.append(name)
    return out


def _sql_ident(name: str) -> str:
    name = str(name or "").strip()
    if _IDENT_RE.match(name.lower()):
        return name
    return '"' + name.replace('"', '""') + '"'


def _qualified(schema: str, table: str) -> str:
    schema = str(schema or "").strip()
    table = str(table or "").strip()
    if schema:
        return f"{_sql_ident(schema)}.{_sql_ident(table)}"
    return _sql_ident(table)


def build_fix_script(
    repeat_groups: pd.DataFrame,
    repeat_group_tables: pd.DataFrame,
    *,
    action_queue: pd.DataFrame | None = None,
    table_review: pd.DataFrame | None = None,
    snapshot_id: str | None = None,
    generated_at: datetime | None = None,
) -> str:
    stamp = (generated_at or datetime.now()).strftime("%Y-%m-%d %H:%M")
    lines: list[str] = [
        "-- ============================================================================",
        "-- Infraredshift - workload fix script (REVIEW BEFORE RUNNING)",
        f"-- Generated: {stamp}" + (f" | snapshot: {snapshot_id}" if snapshot_id else ""),
        "--",
        "-- Section 1 is safe maintenance (ANALYZE / VACUUM SORT ONLY) and is runnable.",
        "-- Section 2 contains DESIGN CHANGES; they are commented out on purpose.",
        "--   * Verify each candidate column exists on the table and fits the workload.",
        "--   * ALTER SORTKEY / DISTSTYLE rewrites data; run in a maintenance window.",
        "-- Evidence below each item comes from the captured workload triage.",
        "-- ============================================================================",
        "",
    ]

    if repeat_group_tables is None or repeat_group_tables.empty:
        if action_queue is not None and not action_queue.empty:
            lines.extend(_action_queue_sections(action_queue))
            return "\n".join(lines)
        if table_review is not None and not table_review.empty:
            repeat_group_tables = _table_review_fallback_findings(table_review)
            repeat_groups = pd.DataFrame()
        if repeat_group_tables is None or repeat_group_tables.empty:
            lines.append("-- No table findings in the current triage analysis.")
            return "\n".join(lines)

    group_lookup: dict[str, pd.Series] = {}
    if repeat_groups is not None and not repeat_groups.empty:
        for _, group in repeat_groups.iterrows():
            group_lookup[str(group.get("repeat_group_id"))] = group

    # Deduplicate per table across groups; aggregate evidence.
    tables: dict[str, dict] = {}
    for _, row in repeat_group_tables.iterrows():
        key = str(row.get("table_key") or f"{row.get('schema_name')}.{row.get('table_name')}").lower()
        entry = tables.setdefault(
            key,
            {
                "schema_name": row.get("schema_name"),
                "table_name": row.get("table_name"),
                "database": str(row.get("table_key") or "").split(".")[0] if str(row.get("table_key") or "").count(".") >= 2 else "",
                "diststyle": row.get("diststyle"),
                "sortkey1": row.get("sortkey1"),
                "size_mb": _to_float(row.get("size_mb")),
                "unsorted_pct": _to_float(row.get("unsorted_pct")),
                "stats_off": _to_float(row.get("stats_off")),
                "skew_rows": _to_float(row.get("skew_rows")),
                "rrscan_query_pct": _to_float(row.get("rrscan_query_pct")),
                "full_scan_query_pct": _to_float(row.get("full_scan_query_pct")),
                "scan_duration_s": 0.0,
                "scan_query_count": 0.0,
                "groups": [],
                "filter_columns": Counter(),
                "join_columns": Counter(),
            },
        )
        entry["scan_duration_s"] = _to_float(row.get("scan_duration_s"))
        entry["scan_query_count"] = _to_float(row.get("scan_query_count"))
        gid = str(row.get("repeat_group_id"))
        if gid not in entry["groups"]:
            entry["groups"].append(gid)
        group = group_lookup.get(gid)
        if group is not None:
            weight = max(int(_to_float(group.get("query_count"))), 1)
            for col in _clean_columns(group.get("sql_filter_columns")):
                entry["filter_columns"][col] += weight
            for col in _clean_columns(group.get("sql_join_columns")):
                entry["join_columns"][col] += weight

    ordered = sorted(tables.values(), key=lambda t: -t["scan_duration_s"])

    maintenance: list[str] = []
    design: list[str] = []

    for t in ordered:
        qualified = _qualified(t["schema_name"], t["table_name"])
        where = f" (database: {t['database']})" if t["database"] else ""
        head = (
            f"-- {qualified}{where} | {t['size_mb'] / 1024:.1f} GB | DISTSTYLE {str(t['diststyle'] or '-').upper()}"
            f" | SORTKEY {t['sortkey1'] or 'none'} | touched by pattern(s) {', '.join(t['groups'][:6])}"
        )

        if t["stats_off"] >= 10:
            maintenance.append(head)
            maintenance.append(f"--   finding: statistics {t['stats_off']:.0f}% stale")
            if t["database"]:
                maintenance.append(
                    f"-- IMPORTANT: connect to database {t['database']} before running "
                    f"(Redshift ANALYZE is session-database scoped)."
                )
            maintenance.append(f"ANALYZE {qualified};")
            maintenance.append("")
        if t["unsorted_pct"] >= 20:
            maintenance.append(head)
            maintenance.append(f"--   finding: {t['unsorted_pct']:.0f}% of rows unsorted")
            if t["database"]:
                maintenance.append(
                    f"-- IMPORTANT: connect to database {t['database']} before running "
                    f"(Redshift VACUUM is session-database scoped)."
                )
            # By construction: SORT ONLY only — never bare VACUUM SORT.
            maintenance.append(f"VACUUM SORT ONLY {qualified};")
            maintenance.append("")

        sortkey = str(t["sortkey1"] or "").strip().lower()
        missing_sortkey = is_missing_sortkey(sortkey) and t["size_mb"] >= 100
        ineffective_sortkey = (
            not is_missing_sortkey(sortkey)
            and t["scan_query_count"] >= 3
            and t["rrscan_query_pct"] <= 0.5
        )
        if missing_sortkey or ineffective_sortkey:
            candidates = [col for col, _ in t["filter_columns"].most_common(3)]
            design.append(head)
            if missing_sortkey:
                design.append("--   finding: no sort key on a table this workload scans")
            else:
                design.append(
                    f"--   finding: sort key '{t['sortkey1']}' rarely prunes scans "
                    f"({t['rrscan_query_pct'] * 100:.0f}% range-restricted, "
                    f"{t['full_scan_query_pct'] * 100:.0f}% full scans)"
                )
            if candidates:
                design.append(
                    f"--   workload filters most on: {', '.join(candidates)} "
                    "(verify these columns belong to THIS table before uncommenting)"
                )
                design.append(f"-- ALTER TABLE {qualified} ALTER SORTKEY ({', '.join(candidates)});")
            else:
                design.append("--   no filter-column candidates extracted; choose the sort key manually")
                design.append(f"-- ALTER TABLE {qualified} ALTER SORTKEY (<column>);")
            design.append("")

        diststyle = str(t["diststyle"] or "").strip().lower()
        if diststyle.startswith(("even", "all")) and t["size_mb"] >= 1000:
            join_candidates = [col for col, _ in t["join_columns"].most_common(2)]
            design.append(head)
            design.append(
                f"--   finding: DISTSTYLE {diststyle.upper()} on a {t['size_mb'] / 1024:.1f} GB table forces "
                "redistribution on joins"
            )
            if join_candidates:
                design.append(
                    f"--   workload joins most on: {', '.join(join_candidates)} "
                    "(verify column and cardinality before uncommenting)"
                )
                design.append(
                    f"-- ALTER TABLE {qualified} ALTER DISTSTYLE KEY DISTKEY ({join_candidates[0]});"
                )
            else:
                design.append("--   no join-column candidates extracted; choose the DISTKEY manually")
                design.append(f"-- ALTER TABLE {qualified} ALTER DISTSTYLE KEY DISTKEY (<column>);")
            design.append("")
        if t["skew_rows"] >= 4 and diststyle.startswith("key"):
            design.append(head)
            design.append(
                f"--   finding: row distribution skew {t['skew_rows']:.1f}x - current DISTKEY has low/uneven "
                "cardinality; re-evaluate the key or use DISTSTYLE EVEN"
            )
            design.append("")

    lines.append("-- ============================ SECTION 1: SAFE MAINTENANCE ============================")
    lines.append("")
    if maintenance:
        lines.extend(maintenance)
    else:
        lines.append("-- No maintenance actions needed (statistics and sort order look healthy).")
        lines.append("")
    lines.append("-- ==================== SECTION 2: DESIGN CHANGES (REVIEW + UNCOMMENT) =================")
    lines.append("")
    if design:
        lines.extend(design)
    else:
        lines.append("-- No design-change candidates from the current findings.")
        lines.append("")
    return "\n".join(lines)


def _action_queue_sections(action_queue: pd.DataFrame) -> list[str]:
    maintenance: list[str] = []
    design: list[str] = []
    seen_runnable: set[str] = set()

    ordered = action_queue.copy()
    if "action_score" in ordered.columns:
        ordered["_score"] = pd.to_numeric(ordered["action_score"], errors="coerce").fillna(0.0)
        ordered = ordered.sort_values("_score", ascending=False)
    for _, row in ordered.iterrows():
        subject = str(row.get("subject") or row.get("table_key") or "unknown").strip()
        action_id = str(row.get("action_id") or "").strip()
        action_type = str(row.get("action_type") or "").strip()
        severity = str(row.get("severity") or "").upper()
        score = _to_float(row.get("action_score"))
        what = str(row.get("what_to_do") or "").strip()
        why = str(row.get("why_now") or "").strip()
        evidence = str(row.get("evidence") or "").strip()
        sql_hint = str(row.get("sql_hint") or "").strip()
        head = f"-- {subject} | {severity or 'INFO'} | score {score:.1f} | {action_id or action_type}"
        detail = [head]
        if what:
            detail.append(f"--   do: {what}")
        if why:
            detail.append(f"--   why: {why}")
        if evidence:
            detail.append(f"--   evidence: {evidence}")
        runnable = _runnable_maintenance_sql(sql_hint)
        if runnable:
            if runnable not in seen_runnable:
                maintenance.extend(detail)
                maintenance.append(runnable)
                maintenance.append("")
                seen_runnable.add(runnable)
        else:
            design.extend(detail)
            if sql_hint:
                for line in sql_hint.splitlines():
                    line = line.strip()
                    if line:
                        design.append(f"--   next: {line}")
            design.append("")

    lines = ["-- ============================ SECTION 1: SAFE MAINTENANCE ============================", ""]
    if maintenance:
        lines.extend(maintenance)
    else:
        lines.append("-- No runnable maintenance actions in the current action queue.")
        lines.append("")
    lines.append("-- ==================== SECTION 2: DESIGN / REWRITE WORK (REVIEW) =====================")
    lines.append("")
    if design:
        lines.extend(design)
    else:
        lines.append("-- No design or rewrite review actions in the current action queue.")
        lines.append("")
    return lines


def _runnable_maintenance_sql(sql_hint: object) -> str:
    """Only emit ANALYZE / VACUUM SORT ONLY — never bare VACUUM or design DDL."""
    text = str(sql_hint or "").strip()
    if not text:
        return ""
    normalized = re.sub(r"\s+", " ", text).strip()
    if not normalized.endswith(";"):
        normalized += ";"
    lowered = normalized.lower()
    # Force SORT ONLY by construction whenever a VACUUM SORT form appears.
    if re.match(r"^vacuum\s+sort\s+(?!only\s+)", lowered):
        normalized = re.sub(r"(?i)^vacuum\s+sort\s+", "VACUUM SORT ONLY ", normalized)
        lowered = normalized.lower()
    # Reject bare VACUUM (full rewrite) — too dangerous for auto-emission.
    if re.match(r"^vacuum\s+(?!sort\s+only\s+)", lowered):
        return ""
    if re.match(r"^analyze\s+[^;]+;$", lowered) or re.match(r"^vacuum\s+sort\s+only\s+[^;]+;$", lowered):
        return normalized
    return ""


def _table_review_fallback_findings(table_review: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for _, row in table_review.iterrows():
        stats = _to_float(row.get("stats_off"))
        unsorted = _to_float(row.get("unsorted_pct"))
        vacuum = _to_float(row.get("vacuum_sort_benefit"))
        skew = _to_float(row.get("skew_rows"))
        attention = _to_float(row.get("table_attention_score"))
        size_mb = _to_float(row.get("size_mb"))
        if size_mb <= 0:
            size_mb = _to_float(row.get("table_size_mb"))
        missing_sort = is_missing_sortkey(row.get("sortkey1") or row.get("sort_key") or "")
        if not (stats >= 10 or unsorted >= 20 or vacuum >= 10 or skew >= 3 or attention >= 60 or (missing_sort and size_mb >= 100)):
            continue
        rows.append(
            {
                "repeat_group_id": "TABLE_REVIEW",
                "table_key": row.get("table_key") or ".".join(
                    str(row.get(col) or "").strip()
                    for col in ("source_db", "schema_name", "table_name")
                    if str(row.get(col) or "").strip()
                ),
                "schema_name": row.get("schema_name"),
                "table_name": row.get("table_name"),
                "diststyle": row.get("diststyle"),
                "sortkey1": row.get("sortkey1"),
                "size_mb": size_mb,
                "unsorted_pct": unsorted,
                "stats_off": stats,
                "skew_rows": skew,
                "scan_query_count": _to_float(row.get("scan_query_count")),
                "scan_duration_s": _to_float(row.get("avg_scan_duration_s")) * max(_to_float(row.get("scan_query_count")), 1),
                "scan_input_rows_m": _to_float(row.get("scan_input_rows_m")),
                "rrscan_query_pct": _to_float(row.get("rrscan_query_pct")),
                "full_scan_query_pct": _to_float(row.get("full_scan_query_pct")),
                "table_flags": row.get("recommendation") or "",
                "table_recommendation": row.get("recommendation") or "",
            }
        )
    return pd.DataFrame(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate a DBA-approvable fix script from triage findings.")
    parser.add_argument("--duckdb-path", default=None, help="Captured DuckDB file (default: app data path)")
    parser.add_argument("--output", default="fix_script.sql", help="Output SQL file")
    args = parser.parse_args(argv)

    from .cluster_analyze import load_cluster_report

    report = load_cluster_report(args.duckdb_path or None, areas=["action_plan", "table_review"])
    script = build_fix_script(
        report.repeat_groups,
        report.repeat_group_tables,
        action_queue=report.action_queue,
        table_review=report.table_review,
        snapshot_id=report.snapshot_id,
    )
    out = Path(args.output)
    out.write_text(script, encoding="utf-8")
    print(f"Wrote {out.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
