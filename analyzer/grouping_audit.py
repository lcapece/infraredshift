"""Grouping Audit: human-verifiable report of repeat-query grouping.

Run against a captured DuckDB file and open the HTML output in any browser:

    python -m analyzer.grouping_audit
    python -m analyzer.grouping_audit --duckdb-path C:\\path\\redshift.duckdb --output audit.html

The report shows every parent group with the full SQL of its members side by
side (verify nothing was wrongly merged), and a "possible missed groups"
section listing ungrouped queries that touch the same tables (verify nothing
was wrongly split). Any error found here should become a regression test in
tests/test_grouping.py.
"""
from __future__ import annotations

import argparse
import html
import re
from pathlib import Path

import duckdb
import pandas as pd

from .duckdb_store import default_duckdb_path
from .query_similarity import (
    _TABLE_REF_RE,
    _deterministic_repeat_candidates,
    build_repeat_query_report,
)
from .settings import load_settings

_MAX_SQL_CHARS = 6000
_MAX_MEMBERS_SHOWN = 25

_CSS = """
body { font-family: Segoe UI, Arial, sans-serif; background: #101420; color: #e8ecf8; margin: 24px; }
h1 { font-size: 20px; } h2 { font-size: 16px; margin-top: 28px; }
.summary { background: #1a2133; border: 1px solid #2a3350; border-radius: 8px; padding: 12px 16px; }
.group { background: #151b2b; border: 1px solid #2a3350; border-radius: 8px; padding: 10px 14px; margin: 12px 0; }
.group h3 { margin: 4px 0; font-size: 14px; }
.meta { color: #9aa7c7; font-size: 12px; }
.shape { color: #7fd7a8; font-family: Consolas, monospace; font-size: 12px; white-space: pre-wrap; }
details { margin: 6px 0; }
summary { cursor: pointer; color: #8fb0ff; font-size: 13px; }
pre { background: #0d1120; border: 1px solid #232c42; border-radius: 6px; padding: 8px;
      font-family: Consolas, monospace; font-size: 12px; white-space: pre-wrap; word-break: break-word; }
.warn { color: #ffb454; } .bad { color: #ff6b6b; }
table.t { border-collapse: collapse; font-size: 12px; }
table.t td, table.t th { border: 1px solid #2a3350; padding: 3px 8px; }
"""


def _esc(value: object, limit: int = _MAX_SQL_CHARS) -> str:
    text = "" if value is None else str(value)
    if len(text) > limit:
        text = text[:limit] + f"\n... [{len(text) - limit:,} more chars]"
    return html.escape(text)


def _load_queries(db_path: str | None, limit: int) -> pd.DataFrame:
    path = str(db_path or default_duckdb_path())
    con = duckdb.connect(path, read_only=True)
    try:
        return con.execute(
            f"""
            SELECT snapshot_id, query_id, user_name, database_name, query_type,
                   start_time, elapsed_s, risk_score, sql_text, '' AS dominant_issue
            FROM v_slow_queries
            WHERE sql_text IS NOT NULL AND LENGTH(sql_text) > 0
            ORDER BY elapsed_s DESC NULLS LAST
            LIMIT {int(limit)}
            """
        ).fetchdf()
    finally:
        con.close()


def _shape_tables(shape: str) -> tuple[str, ...]:
    return tuple(sorted(set(_TABLE_REF_RE.findall(shape or ""))))


def _missed_group_candidates(queries: pd.DataFrame, members: pd.DataFrame) -> list[dict]:
    """Ungrouped queries bucketed by (query_type, table set). Buckets with 2+
    distinct shapes are candidates for under-grouping review."""
    grouped_ids = set(members["query_id"]) if not members.empty else set()
    singles = queries[~queries["query_id"].isin(grouped_ids)]
    if singles.empty:
        return []
    candidates = _deterministic_repeat_candidates(singles, {})
    buckets: dict[tuple, list[dict]] = {}
    for item in candidates:
        row = singles.loc[item["frame_index"]]
        key = (item["query_type"], _shape_tables(item["group_sql_shape"]))
        if not key[1]:
            continue
        buckets.setdefault(key, []).append(
            {
                "query_id": row.get("query_id"),
                "user": row.get("user_name"),
                "elapsed_s": row.get("elapsed_s"),
                "sql": row.get("sql_text"),
                "shape": item["group_sql_shape"],
            }
        )
    out = []
    for (qtype, tables), items in buckets.items():
        if len(items) < 2:
            continue
        if len({entry["shape"] for entry in items}) < 2:
            continue
        out.append({"query_type": qtype, "tables": ", ".join(tables), "items": items[:8]})
    out.sort(key=lambda bucket: -len(bucket["items"]))
    return out[:40]


def render_report(db_path: str | None = None, limit: int = 1000) -> str:
    queries = _load_queries(db_path, limit)
    settings = load_settings()
    groups, members = build_repeat_query_report(
        queries,
        scope_by_user=settings.repeat_scope_by_user,
        min_group_size=settings.repeat_min_group_size,
        fuzzy_merge_threshold=settings.repeat_fuzzy_merge_threshold,
    )
    missed = _missed_group_candidates(queries, members)
    grouped_ids = set(members["query_id"]) if not members.empty else set()

    parts: list[str] = [
        f"<style>{_CSS}</style>",
        "<h1>Repeat-Query Grouping Audit</h1>",
        "<div class='summary'>",
        f"<div>Queries analyzed: <b>{len(queries):,}</b> &nbsp;|&nbsp; "
        f"Parent groups: <b>{len(groups):,}</b> &nbsp;|&nbsp; "
        f"Grouped queries: <b>{len(grouped_ids):,}</b> &nbsp;|&nbsp; "
        f"Ungrouped (one-off) queries: <b>{len(queries) - len(grouped_ids):,}</b></div>",
        "<div class='meta'>Verify each group top-to-bottom: members under one parent must be, for all practical "
        "purposes, the same query with different values. Then check 'possible missed groups' for anything that "
        "should have grouped but did not. Report any error so it becomes a permanent regression test.</div>",
        "</div>",
        "<h2>Parent groups (largest first)</h2>",
    ]

    if groups.empty:
        parts.append("<p class='warn'>No repeat groups found in this capture.</p>")
    else:
        ordered = groups.sort_values(["query_count", "total_runtime_s"], ascending=False)
        member_lookup = (
            dict(tuple(members.groupby("repeat_group_id", sort=False))) if not members.empty else {}
        )
        for _, group in ordered.iterrows():
            gid = group.get("repeat_group_id")
            method = group.get("fingerprint_method") or "-"
            method_note = "" if method == "ast" else " <span class='warn'>(regex fallback)</span>"
            parts.append("<div class='group'>")
            parts.append(
                f"<h3>{_esc(gid)} — {int(group.get('query_count') or 0)} runs, "
                f"{float(group.get('total_runtime_s') or 0.0):,.0f}s total{method_note}</h3>"
            )
            parts.append(
                f"<div class='meta'>match basis: {_esc(group.get('repeat_match_basis'))} | "
                f"users: {_esc(group.get('users'))} | databases: {_esc(group.get('databases'))}</div>"
            )
            parts.append(
                f"<details><summary>normalized shape</summary>"
                f"<div class='shape'>{_esc(group.get('sql_shape'), 2000)}</div></details>"
            )
            group_members = member_lookup.get(gid)
            if group_members is not None:
                shown = 0
                for _, member in group_members.iterrows():
                    if shown >= _MAX_MEMBERS_SHOWN:
                        parts.append(
                            f"<div class='meta'>... {len(group_members) - shown:,} more member(s) not shown</div>"
                        )
                        break
                    parts.append(
                        f"<details><summary>query {_esc(member.get('query_id'))} — "
                        f"{float(member.get('elapsed_s') or 0):,.1f}s — {_esc(member.get('user_name'))}"
                        f"</summary><pre>{_esc(member.get('sql_text'))}</pre></details>"
                    )
                    shown += 1
            parts.append("</div>")

    parts.append("<h2>Possible missed groups (same tables, did not group)</h2>")
    if not missed:
        parts.append("<p>None found — every set of same-table queries either grouped or is structurally distinct.</p>")
    else:
        parts.append(
            "<p class='meta'>Each bucket below contains ungrouped queries that reference the same tables. "
            "If two entries in a bucket are really the same query, that is an under-grouping bug — report it.</p>"
        )
        for bucket in missed:
            parts.append("<div class='group'>")
            parts.append(
                f"<h3 class='warn'>{len(bucket['items'])} ungrouped {_esc(bucket['query_type'])} "
                f"queries on: {_esc(bucket['tables'], 300)}</h3>"
            )
            for entry in bucket["items"]:
                parts.append(
                    f"<details><summary>query {_esc(entry['query_id'])} — "
                    f"{float(entry['elapsed_s'] or 0):,.1f}s — {_esc(entry['user'])}"
                    f"</summary><pre>{_esc(entry['sql'])}</pre></details>"
                )
            parts.append("</div>")

    return "\n".join(parts)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Emit an HTML audit of repeat-query grouping.")
    parser.add_argument("--duckdb-path", default=None, help="Captured DuckDB file (default: app data path)")
    parser.add_argument("--output", default="grouping_audit.html", help="Output HTML file")
    parser.add_argument("--limit", type=int, default=1000, help="Max queries to analyze")
    args = parser.parse_args(argv)
    report = render_report(args.duckdb_path, args.limit)
    out = Path(args.output)
    out.write_text(report, encoding="utf-8")
    print(f"Wrote {out.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
