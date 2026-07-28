"""Generate sort-key and dist-key recommendations from captured workload SQL."""
from __future__ import annotations

import argparse
import math
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

from ._lazy_sqlglot import exp, sqlglot
from .redshift_meta import MISSING_SORTKEY_VALUES, is_missing_sortkey

_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_$]*$")
_DISTKEY_RE = re.compile(r"\bKEY\s*\(?\s*([a-z_][a-z0-9_$]*)\s*\)?", re.IGNORECASE)
_MISSING_SORTKEY_VALUES = set(MISSING_SORTKEY_VALUES)


def build_structural_recommendations(
    slow_queries: pd.DataFrame,
    table_review: pd.DataFrame,
    *,
    limit: int = 0,
) -> pd.DataFrame:
    """Return ranked physical-design recommendations focused on SORTKEY/DISTKEY.

    This intentionally does not emit ANALYZE/VACUUM maintenance. It answers:
    which tables have workload evidence that their sort or distribution design
    should change, and which columns appear to be the candidate keys.
    """
    if slow_queries is None or slow_queries.empty or table_review is None or table_review.empty:
        return _empty_recommendations()

    table_index, table_meta = _build_table_index(table_review)
    stats: dict[str, dict] = {}
    parse_failures = 0
    considered = 0

    for _, query in slow_queries.iterrows():
        sql = str(query.get("sql_text") or "").strip()
        if len(sql) < 24:
            continue
        considered += 1
        try:
            tree = sqlglot.parse_one(sql, read="redshift")
        except Exception:
            parse_failures += 1
            continue

        query_database = str(query.get("database_name") or "").strip()
        alias_to_key = _table_aliases(tree, table_index, query_database)
        table_keys = {key for key in alias_to_key.values() if key}
        if not table_keys:
            continue

        filter_cols = _filter_columns(tree, alias_to_key, table_keys)
        join_cols = _join_columns(tree, alias_to_key, table_keys)
        query_runtime = _to_float(query.get("elapsed_s"))
        query_id = str(query.get("query_id") or "").strip()

        join_table_keys = {key for key, _col in join_cols}
        for key in table_keys:
            entry = stats.setdefault(key, _new_stats(table_meta.get(key, {})))
            entry["query_count"] += 1
            entry["runtime_s"] += query_runtime
            # Redistribution/broadcast blame belongs to tables that participate
            # in joins — not every table the SELECT happens to touch.
            if key in join_table_keys or not join_table_keys:
                entry["dist_total_cnt"] += _to_float(query.get("dist_total_cnt"))
                entry["dist_both_cnt"] += _to_float(query.get("dist_both_cnt"))
                entry["bcast_cnt"] += _to_float(query.get("bcast_cnt"))
            entry["spill_blocks"] += _to_float(query.get("total_spill"))
            if query_id and len(entry["query_ids"]) < 12:
                entry["query_ids"].append(query_id)
        for key, col in filter_cols:
            entry = stats.setdefault(key, _new_stats(table_meta.get(key, {})))
            entry["filter_columns"][col] += 1
            entry["filter_runtime"][col] += query_runtime
        for key, col in join_cols:
            entry = stats.setdefault(key, _new_stats(table_meta.get(key, {})))
            entry["join_columns"][col] += 1
            entry["join_runtime"][col] += query_runtime

    rows: list[dict] = []
    for key, entry in stats.items():
        rows.extend(_sortkey_recommendations(key, entry))
        rows.extend(_distkey_recommendations(key, entry))

    if not rows:
        frame = _empty_recommendations()
    else:
        frame = pd.DataFrame(rows)
        frame = frame.sort_values("priority_score", ascending=False).reset_index(drop=True)
        frame.insert(0, "priority_rank", range(1, len(frame) + 1))
        if limit > 0:
            frame = frame.head(limit).copy()
    # Surface parse coverage so callers can detect biased recommendations built
    # from only the sqlglot-parseable subset of the workload.
    frame.attrs["parse_failures"] = parse_failures
    frame.attrs["queries_considered"] = considered
    frame.attrs["parse_ok_rate"] = (
        0.0 if considered == 0 else max(0.0, (considered - parse_failures) / considered)
    )
    return frame


def _safe_block_comment_text(value: object) -> str:
    """Neutralize any '*/' or '/*' inside prose so it cannot prematurely close
    (or nest) a /* ... */ block comment and corrupt the surrounding SQL."""
    text = str(value if value is not None else "")
    return text.replace("*/", "* /").replace("/*", "/ *")


def _block_comment(lines: list[str]) -> list[str]:
    """Wrap prose lines in a /* ... */ block. Every line is escaped so the SQL
    stays valid even when evidence text contains comment delimiters."""
    out = ["/*"]
    for line in lines:
        out.append(f"   {_safe_block_comment_text(line)}")
    out.append("*/")
    return out


def build_structural_recommendation_script(
    recommendations: pd.DataFrame,
    *,
    snapshot_id: str | None = None,
    generated_at: datetime | None = None,
) -> str:
    """Emit the structural script with a strict comment convention:

    - All prose (headers, justification, evidence) lives inside /* ... */ block
      comments.
    - The ONLY line beginning with '--' is the executable ALTER statement, left
      commented so nothing runs by default; the DBA uncomments the one line to
      run it. This makes "which row gets executed" unmistakable.
    """
    stamp = (generated_at or datetime.now()).strftime("%Y-%m-%d %H:%M")
    parse_failures = int(getattr(recommendations, "attrs", {}).get("parse_failures") or 0)
    considered = int(getattr(recommendations, "attrs", {}).get("queries_considered") or 0)
    parse_rate = float(getattr(recommendations, "attrs", {}).get("parse_ok_rate") or 0.0)
    coverage_line = (
        f"Workload parse coverage: {considered - parse_failures}/{considered} "
        f"queries parsed ({parse_rate * 100:.0f}%). "
        f"{parse_failures} unparseable SQL statement(s) were skipped."
        if considered
        else "Workload parse coverage: not reported for this recommendation set."
    )
    header = [
        "============================================================================",
        "Infraredshift - structural recommendations (REVIEW BEFORE RUNNING)",
        f"Generated: {stamp}" + (f" | snapshot: {snapshot_id}" if snapshot_id else ""),
        coverage_line,
        "",
        "This file is intentionally focused on SORTKEY and DISTKEY candidates.",
        "No statement is runnable by default. DBA review is required.",
        "Each recommendation ends with ONE '--'-prefixed ALTER line: uncomment",
        "that single line to run it. Everything else is a /* */ block comment.",
        "Syntax follows Amazon Redshift ALTER TABLE forms:",
        "  ALTER SORTKEY (column_name [, ...])",
        "  ALTER DISTKEY column_name",
        "============================================================================",
    ]
    lines = _block_comment(header) + [""]
    if recommendations is None or recommendations.empty:
        lines += _block_comment(["No structural recommendations were generated from the loaded workload."])
        return "\n".join(lines).rstrip() + "\n"

    for _, row in recommendations.iterrows():
        lines.extend(
            _block_comment(
                [
                    f"#{int(row.get('priority_rank') or 0)} {row.get('recommendation_type')} | {row.get('severity')} | score {_to_float(row.get('priority_score')):.1f}",
                    f"table: {row.get('qualified_table')}",
                    f"current: DISTSTYLE {row.get('current_diststyle') or '-'} | DISTKEY {row.get('current_distkey') or 'none'} | SORTKEY {row.get('current_sortkey') or 'none'}",
                    f"recommend: {row.get('recommended_columns')}",
                    f"evidence: {row.get('evidence')}",
                    f"query_ids: {row.get('query_ids') or '-'}",
                ]
            )
        )
        lines.append(_commented_executable(row.get("candidate_sql")))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _commented_executable(candidate_sql: object) -> str:
    """The one executable line per recommendation, prefixed '-- ' so it is
    commented by default. Any inline '--' already in the candidate is preserved;
    the leading '-- ' is what the DBA removes to run it."""
    sql = str(candidate_sql or "").strip()
    if not sql:
        return "-- (no candidate statement)"
    # If the generator already produced a commented line, keep it single-prefixed.
    return sql if sql.startswith("--") else f"-- {sql}"


def _sortkey_recommendations(key: str, entry: dict) -> list[dict]:
    filters: Counter = entry["filter_columns"]
    if not filters:
        return []
    current_sort = _clean_identifier(entry.get("sortkey1"))
    candidates = [col for col, _ in filters.most_common(3)]
    top = candidates[0]
    missing_sort = is_missing_sortkey(current_sort)
    rrscan = _to_float(entry.get("rrscan_query_pct"))
    full_scan_pct = _to_float(entry.get("full_scan_query_pct"))
    scan_count = _to_float(entry.get("scan_query_count"))
    size_mb = _to_float(entry.get("size_mb"))
    # Never hand back the table's current leading sort key as a "fix" — it is a
    # no-op. If it already leads on the top filter column, the real remedy is
    # VACUUM / a non-leading predicate, not an ALTER SORTKEY.
    if not missing_sort and _same_column(top, current_sort):
        return []
    if scan_count < 1 and entry["query_count"] < 2:
        return []
    # Size floor (matches fix_script's 100 MB gate): a sort key on a tiny table
    # does not pay for the rewrite, so do not flag it even when it is missing.
    if size_mb < 100:
        return []
    pressure = (
        _to_float(entry.get("sort_attention_score"))
        + _to_float(entry.get("full_scan_score")) * 0.5
        + min(entry["filter_runtime"][top] / 3600.0, 35)
        + min(filters[top] * 5, 35)
        + max(0.0, 1.0 - rrscan) * 25
        + full_scan_pct * 20
    )
    if pressure < 45 and not missing_sort:
        return []
    qualified = _qualified(entry.get("schema_name"), entry.get("table_name"))
    return [
        {
            **_base_row(key, entry),
            "recommendation_type": "SORTKEY",
            "severity": "crit" if pressure >= 115 or missing_sort else "warn",
            "priority_score": round(pressure, 2),
            "recommended_columns": ", ".join(candidates),
            "evidence": (
                f"{filters[top]} captured query(s) filter on {top}; "
                f"filter_runtime={entry['filter_runtime'][top] / 60.0:.1f} min; "
                f"rrscan={rrscan * 100:.0f}%; full_scan={full_scan_pct * 100:.0f}%; "
                f"scan_count={scan_count:.0f}"
            ),
            "candidate_sql": f"-- ALTER TABLE {qualified} ALTER SORTKEY ({', '.join(_sql_ident(c) for c in candidates)});",
        }
    ]


def _distkey_recommendations(key: str, entry: dict) -> list[dict]:
    joins: Counter = entry["join_columns"]
    current_distkey = _clean_identifier(entry.get("current_distkey"))
    diststyle = str(entry.get("diststyle") or "").strip()
    diststyle_norm = _clean_identifier(diststyle)
    is_all = diststyle_norm.startswith("all") or diststyle_norm == "auto(all)"
    size_mb = _to_float(entry.get("size_mb"))
    redistribution = _to_float(entry.get("redistribution_query_count")) + entry["dist_total_cnt"]
    broadcast = _to_float(entry.get("broadcast_query_count")) + entry["bcast_cnt"]
    skew = _to_float(entry.get("skew_rows"))
    rows: list[dict] = []
    if joins:
        top = joins.most_common(1)[0][0]
        pressure = (
            _to_float(entry.get("distribution_usage_score"))
            + min(entry["join_runtime"][top] / 3600.0, 40)
            + min(joins[top] * 6, 36)
            + min(redistribution * 4, 35)
            + min(broadcast * 3, 25)
            + (20 if size_mb >= 1024 else 0)
        )
        if is_all:
            # A DISTSTYLE ALL table is replicated on every node, so joins never
            # redistribute it. Emitting "ALTER DISTKEY" would REMOVE that
            # replication and introduce the very movement ALL prevents. The only
            # honest recommendation is a size-driven review of whether ALL still
            # fits — never a blind DISTKEY change.
            if size_mb >= 2048:
                qualified = _qualified(entry.get("schema_name"), entry.get("table_name"))
                rows.append(
                    {
                        **_base_row(key, entry),
                        "recommendation_type": "DISTSTYLE_ALL_REVIEW",
                        "severity": "warn",
                        "priority_score": round(min(pressure, 80) + min(size_mb / 1024.0, 20), 2),
                        "recommended_columns": f"review ALL vs KEY({top})",
                        "evidence": (
                            f"table is DISTSTYLE ALL at {size_mb / 1024.0:.1f} GB (replicated on every node); "
                            f"{joins[top]} join(s) use {top}. ALL avoids redistribution but costs storage on "
                            "every node. Review whether this table is still small enough to justify ALL."
                        ),
                        "candidate_sql": (
                            f"-- Review only: {qualified} is DISTSTYLE ALL. Do NOT blindly set a DISTKEY —\n"
                            f"-- that removes replication. If it is too large for ALL, consider:\n"
                            f"-- ALTER TABLE {qualified} ALTER DISTSTYLE KEY DISTKEY {_sql_ident(top)};"
                        ),
                    }
                )
        elif not _same_column(top, current_distkey) and pressure >= 45:
            qualified = _qualified(entry.get("schema_name"), entry.get("table_name"))
            rows.append(
                {
                    **_base_row(key, entry),
                    "recommendation_type": "DISTKEY",
                    "severity": "crit" if pressure >= 120 or redistribution >= 5 else "warn",
                    "priority_score": round(pressure, 2),
                    "recommended_columns": top,
                    "evidence": (
                        f"{joins[top]} captured query join(s) use {top}; "
                        f"join_runtime={entry['join_runtime'][top] / 60.0:.1f} min; "
                        f"redistribution_signals={redistribution:.0f}; broadcast_signals={broadcast:.0f}; "
                        f"current DISTSTYLE {diststyle or 'unknown'}; table_size_gb={size_mb / 1024.0:.1f}"
                    ),
                    "candidate_sql": f"-- ALTER TABLE {qualified} ALTER DISTKEY {_sql_ident(top)};",
                }
            )
    if skew >= 4 and current_distkey and (not joins or not _same_column(joins.most_common(1)[0][0], current_distkey)):
        rows.append(
            {
                **_base_row(key, entry),
                "recommendation_type": "DISTKEY_SKEW_REVIEW",
                "severity": "crit",
                "priority_score": round(85 + min(skew * 6, 45), 2),
                "recommended_columns": joins.most_common(1)[0][0] if joins else "EVEN or a higher-cardinality key",
                "evidence": (
                    f"current distkey={current_distkey}; skew_rows={skew:.1f}x; "
                    f"current DISTSTYLE {diststyle}; table_size_gb={size_mb / 1024.0:.1f}"
                ),
                "candidate_sql": (
                    f"-- ALTER TABLE {_qualified(entry.get('schema_name'), entry.get('table_name'))} "
                    f"ALTER DISTKEY {_sql_ident(joins.most_common(1)[0][0])};"
                    if joins
                    else f"-- ALTER TABLE {_qualified(entry.get('schema_name'), entry.get('table_name'))} ALTER DISTSTYLE EVEN;"
                ),
            }
        )
    return rows


def _base_row(key: str, entry: dict) -> dict:
    return {
        "table_key": key,
        "source_db": entry.get("source_db"),
        "schema_name": entry.get("schema_name"),
        "table_name": entry.get("table_name"),
        "qualified_table": _qualified(entry.get("schema_name"), entry.get("table_name")),
        "current_diststyle": entry.get("diststyle"),
        "current_distkey": entry.get("current_distkey"),
        "current_sortkey": entry.get("sortkey1"),
        "query_count": int(entry.get("query_count") or 0),
        "total_runtime_s": round(_to_float(entry.get("runtime_s")), 3),
        "query_ids": ", ".join(entry.get("query_ids") or ()),
    }


def _new_stats(meta: dict) -> dict:
    diststyle = str(meta.get("diststyle") or "")
    return {
        **meta,
        "current_distkey": _distkey_from_diststyle(diststyle),
        "query_count": 0,
        "runtime_s": 0.0,
        "dist_total_cnt": 0.0,
        "dist_both_cnt": 0.0,
        "bcast_cnt": 0.0,
        "spill_blocks": 0.0,
        "query_ids": [],
        "filter_columns": Counter(),
        "filter_runtime": defaultdict(float),
        "join_columns": Counter(),
        "join_runtime": defaultdict(float),
    }


def _table_aliases(tree, table_index: dict, query_database: str = "") -> dict[str, str]:
    out: dict[str, str] = {}
    for table in tree.find_all(exp.Table):
        key = _resolve_table(_table_names(table), table_index, query_database)
        if not key:
            continue
        for alias in {_clean_identifier(getattr(table, "alias_or_name", "")), _clean_identifier(getattr(table, "name", ""))}:
            if alias:
                out[alias] = key
    return out


def _filter_columns(tree, alias_to_key: dict[str, str], table_keys: set[str]) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    clause_types = [exp.Where, getattr(exp, "Having", exp.Where), getattr(exp, "Qualify", exp.Where)]
    for clause_type in clause_types:
        for clause in tree.find_all(clause_type):
            for col in clause.find_all(exp.Column):
                resolved = _resolve_column_key(col, alias_to_key, table_keys)
                if resolved:
                    out.add(resolved)
    return out


def _join_columns(tree, alias_to_key: dict[str, str], table_keys: set[str]) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for join in tree.find_all(exp.Join):
        using_expr = join.args.get("using")
        on_expr = join.args.get("on")
        # USING (col) is an equality on both sides of the join. Attribute the
        # column only to the tables actually participating in this join, never
        # to every table_keys entry in the whole query.
        if using_expr is not None:
            join_keys = _join_participant_keys(join, alias_to_key)
            for name in _using_column_names(using_expr):
                for key in join_keys or ():
                    out.add((key, name))
            continue
        for col in _iter_columns(on_expr):
            resolved = _resolve_column_key(col, alias_to_key, table_keys)
            if resolved:
                out.add(resolved)
            # Unqualified ON columns are intentionally NOT credited to every
            # table in a two-table query — that invents candidate DISTKEYs on
            # tables that may not even own the column.
    return out


def _using_column_names(value) -> list[str]:
    """sqlglot stores USING columns as Identifier nodes (or lists of them)."""
    names: list[str] = []
    if value is None:
        return names
    if isinstance(value, (list, tuple)):
        for item in value:
            names.extend(_using_column_names(item))
        return names
    name = _clean_identifier(
        getattr(value, "name", None)
        or getattr(value, "this", None)
        or value
    )
    if name:
        names.append(name)
    else:
        for col in _iter_columns(value):
            leaf = _clean_identifier(getattr(col, "name", ""))
            if leaf:
                names.append(leaf)
    return names


def _join_participant_keys(join, alias_to_key: dict[str, str]) -> set[str]:
    """Tables participating in one JOIN node (right side + parent FROM/join left)."""
    keys: set[str] = set()
    target = join.this
    if isinstance(target, exp.Table):
        for alias in {
            _clean_identifier(getattr(target, "alias_or_name", "")),
            _clean_identifier(getattr(target, "name", "")),
        }:
            if alias and alias in alias_to_key:
                keys.add(alias_to_key[alias])
    parent = join.parent
    while parent is not None:
        if isinstance(parent, exp.Table):
            for alias in {
                _clean_identifier(getattr(parent, "alias_or_name", "")),
                _clean_identifier(getattr(parent, "name", "")),
            }:
                if alias and alias in alias_to_key:
                    keys.add(alias_to_key[alias])
            break
        if isinstance(parent, exp.Join):
            left = parent.this
            if isinstance(left, exp.Table):
                for alias in {
                    _clean_identifier(getattr(left, "alias_or_name", "")),
                    _clean_identifier(getattr(left, "name", "")),
                }:
                    if alias and alias in alias_to_key:
                        keys.add(alias_to_key[alias])
            break
        if isinstance(parent, exp.From):
            this = parent.this
            if isinstance(this, exp.Table):
                for alias in {
                    _clean_identifier(getattr(this, "alias_or_name", "")),
                    _clean_identifier(getattr(this, "name", "")),
                }:
                    if alias and alias in alias_to_key:
                        keys.add(alias_to_key[alias])
            break
        parent = parent.parent
    return keys


def _iter_columns(value) -> list:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        out = []
        for item in value:
            out.extend(_iter_columns(item))
        return out
    try:
        return list(value.find_all(exp.Column))
    except Exception:
        return []


def _resolve_column_key(col, alias_to_key: dict[str, str], table_keys: set[str]) -> tuple[str, str] | None:
    name = _clean_identifier(getattr(col, "name", ""))
    if not name:
        return None
    alias = _clean_identifier(getattr(col, "table", ""))
    if alias and alias in alias_to_key:
        return alias_to_key[alias], name
    if not alias and len(table_keys) == 1:
        return next(iter(table_keys)), name
    return None


def _build_table_index(table_review: pd.DataFrame) -> tuple[dict, dict[str, dict]]:
    """Build a database-aware table index.

    On a cluster where many databases share identical schemas (e.g. 35 copies
    of public.orders), a bare schema.table key is ambiguous. The index keeps
    fully-qualified db.schema.table keys exact, records per-database lookups so
    a query's own database_name can disambiguate, and only falls back to a bare
    schema.table when exactly one database defines it.

    Returns (index, meta) where index has:
      index["qualified"][db.schema.table] -> table_key   (always safe)
      index["by_db"][(db, schema.table)]  -> table_key   (safe when db known)
      index["bare"][schema.table]         -> table_key   (only if unambiguous)
      index["bare_name"][table]           -> table_key   (only if unambiguous)
    """
    qualified: dict[str, str] = {}
    by_db: dict[tuple[str, str], str] = {}
    bare_owners: dict[str, set[str]] = defaultdict(set)
    name_owners: dict[str, set[str]] = defaultdict(set)
    bare_map: dict[str, str] = {}
    name_map: dict[str, str] = {}
    meta: dict[str, dict] = {}

    for _, row in table_review.iterrows():
        record = row.to_dict()
        source = _clean_identifier(record.get("source_db"))
        schema = _clean_identifier(record.get("schema_name"))
        table = _clean_identifier(record.get("table_name"))
        key = _clean_identifier(record.get("table_key")) or ".".join(
            part for part in (source, schema, table) if part
        )
        if not key:
            continue
        meta[key] = record
        schema_table = f"{schema}.{table}".strip(".")
        if source and schema_table:
            qualified[f"{source}.{schema_table}"] = key
        if source and schema_table:
            by_db[(source, schema_table)] = key
        if schema_table:
            bare_owners[schema_table].add(key)
            bare_map[schema_table] = key
        if table:
            name_owners[table].add(key)
            name_map[table] = key

    bare = {k: v for k, v in bare_map.items() if len(bare_owners[k]) == 1}
    bare_name = {k: v for k, v in name_map.items() if len(name_owners[k]) == 1}
    index = {
        "qualified": qualified,
        "by_db": by_db,
        "bare": bare,
        "bare_name": bare_name,
    }
    return index, meta


def _table_names(table) -> list[str]:
    name = _clean_identifier(getattr(table, "name", ""))
    db = _clean_identifier(getattr(table, "db", ""))
    catalog = _clean_identifier(getattr(table, "catalog", ""))
    candidates = []
    if catalog and db and name:
        candidates.append(f"{catalog}.{db}.{name}")
    if db and name:
        candidates.append(f"{db}.{name}")
    if name:
        candidates.append(name)
    return candidates


def _resolve_table(candidates: list[str], table_index: dict, query_database: str = "") -> str:
    qualified = table_index.get("qualified", {})
    by_db = table_index.get("by_db", {})
    bare = table_index.get("bare", {})
    bare_name = table_index.get("bare_name", {})
    db = _clean_identifier(query_database)
    cleaned = [_clean_identifier(c) for c in candidates if _clean_identifier(c)]

    # 1. Fully-qualified db.schema.table written in the SQL: always exact.
    for candidate in cleaned:
        if candidate in qualified:
            return qualified[candidate]
    # 2. schema.table (or bare table) disambiguated by the query's own database.
    if db:
        for candidate in cleaned:
            if (db, candidate) in by_db:
                return by_db[(db, candidate)]
            parts = candidate.split(".")
            if len(parts) >= 2 and (db, ".".join(parts[-2:])) in by_db:
                return by_db[(db, ".".join(parts[-2:]))]
    # 3. Unambiguous fallback: schema.table or bare table owned by exactly one DB.
    for candidate in cleaned:
        if candidate in bare:
            return bare[candidate]
        if candidate in bare_name:
            return bare_name[candidate]
    return ""


def _qualified(schema: object, table: object) -> str:
    schema_text = str(schema or "").strip()
    table_text = str(table or "").strip()
    if schema_text:
        return f"{_sql_ident(schema_text)}.{_sql_ident(table_text)}"
    return _sql_ident(table_text)


def _sql_ident(name: object) -> str:
    text = str(name or "").strip()
    if _IDENT_RE.match(text.lower()):
        return text
    return '"' + text.replace('"', '""') + '"'


def _clean_identifier(value: object) -> str:
    text = str(value or "").strip().strip('"').strip("'").lower()
    return re.sub(r"\s+", "", text)


def _same_column(left: object, right: object) -> bool:
    return _clean_identifier(left).split(".")[-1] == _clean_identifier(right).split(".")[-1]


def _distkey_from_diststyle(value: object) -> str:
    text = _clean_identifier(value)
    if text in {"auto", "(auto)", "auto(even)", "auto(key)", "distkey(auto)", "key(auto)"}:
        return ""
    match = _DISTKEY_RE.search(str(value or ""))
    if not match:
        return ""
    key = _clean_identifier(match.group(1))
    return "" if key in {"auto", "(auto)", "none", "null", "nan"} else key


def _to_float(value: object) -> float:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if math.isnan(number) or math.isinf(number) else number


def _empty_recommendations() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "priority_rank",
            "recommendation_type",
            "severity",
            "priority_score",
            "source_db",
            "schema_name",
            "table_name",
            "qualified_table",
            "current_diststyle",
            "current_distkey",
            "current_sortkey",
            "recommended_columns",
            "evidence",
            "candidate_sql",
            "query_count",
            "total_runtime_s",
            "query_ids",
        ]
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate Redshift SORTKEY/DISTKEY recommendations.")
    parser.add_argument("--duckdb-path", default=None, help="Captured DuckDB file (default: app data path)")
    parser.add_argument("--output", default="structural_recommendations.sql", help="Output SQL review file")
    parser.add_argument("--csv-output", default="", help="Optional CSV recommendation ledger")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum recommendations to emit; 0 means all threshold-qualified recommendations.",
    )
    args = parser.parse_args(argv)

    from .cluster_analyze import load_cluster_report

    report = load_cluster_report(args.duckdb_path or None, areas=["slow_queries", "table_review"])
    recs = build_structural_recommendations(report.slow_queries, report.table_review, limit=args.limit)
    script = build_structural_recommendation_script(recs, snapshot_id=report.snapshot_id)
    out = Path(args.output)
    out.write_text(script, encoding="utf-8")
    print(f"Wrote {out.resolve()} ({len(recs)} recommendation(s))")
    if args.csv_output:
        csv_out = Path(args.csv_output)
        recs.to_csv(csv_out, index=False)
        print(f"Wrote {csv_out.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
