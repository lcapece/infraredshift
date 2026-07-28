#!/usr/bin/env python3
"""Fast, rollback-safe repair of the analyzer's repeat-query core.

Keep this file beside runner.py and its existing .env file. The runner reloads
only seven tables:

  query_history, query_text, query_details, query_health, query_explain,
  query_detail_flow, table_scan_info

It captures executions over five minutes, removes owner + normalized-SQL-prefix
groups that occur only once, and gathers detailed evidence only for one
representative per repeated parent. It never loads or promotes child query
text, table information, views, procedures, or other catalog tables.

A verified full-file backup is made before the first Redshift query. All slow
work uses private staging tables. Live tables change only in one short atomic
transaction after every staged table passes validation.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import uuid
from datetime import datetime
from pathlib import Path

try:
    import runner
except ImportError as exc:
    raise SystemExit(
        "Could not import runner.py. Put repair_short_tables.py beside the existing runner.py."
    ) from exc


ROOT_TABLES = ("query_history", "query_text")
EVIDENCE_TABLES = (
    "query_details", "query_health", "query_explain", "query_detail_flow", "table_scan_info",
)
REFRESH_TABLES = ROOT_TABLES + EVIDENCE_TABLES
GROUP_TABLES = ("loader_repeat_groups", "loader_repeat_members")
PROMOTE_TABLES = REFRESH_TABLES + GROUP_TABLES
PROTECTED_TABLES = (
    "svv_table_info_all", "view_definitions", "procedure_definitions",
    "child_query_text", "query_history_all", "user_info",
)
STAGE_PREFIX = "__repeat_repair_"


def _table_exists(con, table_name: str) -> bool:
    return bool(con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_type = 'BASE TABLE' AND lower(table_name) = lower(?) LIMIT 1",
        [table_name],
    ).fetchone())


def _columns(con, table_name: str) -> set[str]:
    return {
        str(row[0]).lower()
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE lower(table_name) = lower(?)",
            [table_name],
        ).fetchall()
    }


def _row_count(con, table_name: str) -> int:
    if not _table_exists(con, table_name):
        return 0
    return int(con.execute(
        f"SELECT COUNT(*) FROM {runner.quote_ident(table_name)}"
    ).fetchone()[0] or 0)


def _scan_coverage(con, table_name: str) -> int:
    if not _table_exists(con, table_name) or "queries" not in _columns(con, table_name):
        return 0
    return int(con.execute(
        f"SELECT SUM(COALESCE(TRY_CAST(queries AS BIGINT), 0)) "
        f"FROM {runner.quote_ident(table_name)}"
    ).fetchone()[0] or 0)


def _catalog_anchor_snapshot(con) -> str:
    """Reuse the catalog snapshot so a query-only refresh cannot hide Table Review."""
    if _table_exists(con, "svv_table_info_all") and "snapshot_id" in _columns(
        con, "svv_table_info_all"
    ):
        if _table_exists(con, "snapshot_runs"):
            row = con.execute(
                """
SELECT t.snapshot_id
FROM svv_table_info_all t
LEFT JOIN snapshot_runs s ON s.snapshot_id = t.snapshot_id
WHERE NULLIF(TRIM(t.snapshot_id), '') IS NOT NULL
GROUP BY t.snapshot_id
ORDER BY MAX(s.captured_at) DESC NULLS LAST, COUNT(*) DESC
LIMIT 1
"""
            ).fetchone()
        else:
            row = con.execute(
                "SELECT snapshot_id FROM svv_table_info_all "
                "WHERE NULLIF(TRIM(snapshot_id), '') IS NOT NULL "
                "GROUP BY snapshot_id ORDER BY COUNT(*) DESC LIMIT 1"
            ).fetchone()
        if row and row[0]:
            return str(row[0])
    if _table_exists(con, "snapshot_runs"):
        row = con.execute(
            "SELECT snapshot_id FROM snapshot_runs ORDER BY captured_at DESC NULLS LAST LIMIT 1"
        ).fetchone()
        if row and row[0]:
            return str(row[0])
    return str(uuid.uuid4())


def _write_stage(con, table_name: str, frame, snapshot_id: str) -> int:
    stage_name = STAGE_PREFIX + table_name
    df = frame.copy()
    df.columns = [str(column).strip() for column in df.columns]
    df = df.loc[:, ~df.columns.duplicated()].copy()
    for column in df.columns:
        df[column] = df[column].map(runner._raw_store_value)
    df.insert(0, "captured_at", datetime.now())
    df.insert(0, "snapshot_id", snapshot_id)
    columns = list(dict.fromkeys(df.columns))

    con.execute(f"DROP TABLE IF EXISTS {runner.quote_ident(stage_name)}")
    expected = tuple(runner.EXPECTED_COLUMNS.get(table_name, ())) + tuple(
        column for column in columns if column not in runner.COMMON_COLUMNS
    )
    runner.ensure_table(con, stage_name, expected)
    registered = f"repair_incoming_{uuid.uuid4().hex}"
    con.register(registered, df)
    try:
        select_list = [
            f"CAST({runner.quote_ident(column)} AS TIMESTAMP) AS {runner.quote_ident(column)}"
            if column == "captured_at"
            else f"CAST({runner.quote_ident(column)} AS VARCHAR) AS {runner.quote_ident(column)}"
            for column in columns
        ]
        con.execute(
            f"INSERT INTO {runner.quote_ident(stage_name)} "
            f"({', '.join(runner.quote_ident(column) for column in columns)}) "
            f"SELECT {', '.join(select_list)} FROM {runner.quote_ident(registered)}"
        )
    finally:
        con.unregister(registered)
    return _row_count(con, stage_name)


def preselect_history_ids(
    frame, minimum_seconds: float = 300.0, prefix_chars: int = 80,
    floor_basis: str = "execution_time",
) -> list[str]:
    """Cheap SYS_QUERY_HISTORY-only cut before full SYS_QUERY_TEXT is fetched."""
    if frame is None or frame.empty:
        return []
    df = frame.copy()
    df.columns = [str(column).strip().lower() for column in df.columns]
    if "query_id" not in df.columns:
        return []
    basis = floor_basis if floor_basis in df.columns else (
        "elapsed_time" if "elapsed_time" in df.columns else ""
    )
    text_column = "query_text" if "query_text" in df.columns else ""
    floor_us = max(0, int(float(minimum_seconds) * 1_000_000))
    prefix_length = max(8, int(prefix_chars or 80))
    eligible: list[tuple[str, str, str]] = []
    counts: dict[tuple[str, str], int] = {}
    for row in df.to_dict("records"):
        try:
            runtime = int(float(str(row.get(basis) or 0))) if basis else floor_us
        except (TypeError, ValueError):
            runtime = 0
        status = str(row.get("status") or "success").strip().lower()
        cache_hit = str(row.get("result_cache_hit") or "false").strip().lower() in {
            "1", "t", "true", "yes"
        }
        query_type = str(row.get("query_type") or "").strip().upper()
        if (
            runtime < floor_us
            or status not in {"success", "completed", "complete"}
            or cache_hit
            or query_type in {"PROCEDURE", "UTILITY"}
        ):
            continue
        owner = str(row.get("user_name") or row.get("user_id") or "UNKNOWN").strip().lower()
        text = str(row.get(text_column) or "").strip() if text_column else ""
        # Older SYS_QUERY_HISTORY variants may omit query_text. In that case
        # retain eligible IDs for the full-text phase rather than guessing.
        prefix = (runner.simple_fingerprint(text) or text.lower())[:prefix_length] if text else ""
        query_id = str(row.get("query_id"))
        key = (owner, prefix)
        counts[key] = counts.get(key, 0) + 1
        eligible.append((query_id, owner, prefix))
    if not text_column or not any(item[2] for item in eligible):
        return list(dict.fromkeys(item[0] for item in eligible))
    return list(dict.fromkeys(
        query_id for query_id, owner, prefix in eligible if counts.get((owner, prefix), 0) > 1
    ))


def _filter_frame_to_ids(frame, query_ids: list[str]):
    keep = {str(value) for value in query_ids}
    query_id_column = next(
        (column for column in frame.columns if str(column).strip().lower() == "query_id"), None
    )
    if query_id_column is None:
        return frame.iloc[0:0].copy()
    return frame[frame[query_id_column].map(lambda value: str(value) in keep)].copy()


def _query_text_sql(query_ids: list[str]) -> str:
    ids = ", ".join(str(int(value)) for value in query_ids)
    return f"SELECT qt.* FROM sys_query_text qt WHERE qt.query_id IN ({ids})"


def _canonical_sqlglot_fingerprint(sql: object) -> tuple[str, str]:
    text = str(sql or "").strip()
    if not text:
        return "", "empty"
    try:
        import sqlglot
        from sqlglot import exp

        def canonicalize(node):
            if isinstance(node, exp.In) and len(node.args.get("expressions") or []) > 1:
                node.set("expressions", [exp.Placeholder()])
            elif isinstance(node, exp.Values) and len(node.args.get("expressions") or []) > 1:
                node.set("expressions", (node.args.get("expressions") or [])[:1])
            if isinstance(node, exp.Literal):
                return exp.Placeholder()
            return node

        statements = sqlglot.parse(text, dialect="redshift")
        rendered = []
        for tree in statements:
            if tree is not None:
                rendered.append(
                    tree.transform(canonicalize).sql(
                        dialect="redshift", comments=False, normalize=True
                    )
                )
        if not rendered:
            raise ValueError("empty SQLGlot parse")
        return re.sub(r"\s+", " ", " ; ".join(rendered).lower()).strip(), "ast"
    except Exception:
        return runner.simple_fingerprint(text), "regex"


def _sqlglot_structure(sql: object) -> dict[str, object]:
    try:
        import sqlglot
        from sqlglot import exp

        statements = sqlglot.parse(str(sql or ""), dialect="redshift")
        ctes = {
            str(cte.alias_or_name).strip().lower()
            for tree in statements if tree is not None
            for cte in tree.find_all(exp.CTE) if cte.alias_or_name
        }
        tables = sorted({
            str(table.name).strip().lower()
            for tree in statements if tree is not None
            for table in tree.find_all(exp.Table)
            if table.name and str(table.name).strip().lower() not in ctes
        })
        return {
            "tables": tables,
            "ctes": sorted(ctes),
            "join_count": sum(
                1 for tree in statements if tree is not None for _ in tree.find_all(exp.Join)
            ),
            "cte_count": len(ctes),
            "wildcard_count": sum(
                1 for tree in statements if tree is not None for _ in tree.find_all(exp.Star)
            ),
        }
    except Exception:
        return {"tables": [], "ctes": [], "join_count": 0, "cte_count": 0, "wildcard_count": 0}


def write_candidate_sidecar(sidecar_path: Path, candidate_frame) -> None:
    """Persist the cheap-cut history candidates before SQLGlot work starts."""
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    con = runner.duckdb.connect(str(sidecar_path))
    try:
        con.execute("DROP TABLE IF EXISTS candidate_patterns")
        con.execute("DROP TABLE IF EXISTS candidate_query_text")
        con.execute("DROP TABLE IF EXISTS candidate_query_history")
        registered = f"candidate_history_{uuid.uuid4().hex}"
        con.register(registered, candidate_frame)
        try:
            con.execute(
                f"CREATE TABLE candidate_query_history AS "
                f"SELECT * FROM {runner.quote_ident(registered)}"
            )
        finally:
            con.unregister(registered)
        con.execute("CHECKPOINT")
    finally:
        con.close()


def write_candidate_text_sidecar(sidecar_path: Path, text_frame) -> None:
    con = runner.duckdb.connect(str(sidecar_path))
    try:
        con.execute("DROP TABLE IF EXISTS candidate_query_text")
        registered = f"candidate_text_{uuid.uuid4().hex}"
        con.register(registered, text_frame)
        try:
            con.execute(
                f"CREATE TABLE candidate_query_text AS "
                f"SELECT * FROM {runner.quote_ident(registered)}"
            )
        finally:
            con.unregister(registered)
        con.execute("CHECKPOINT")
    finally:
        con.close()


def sqlglot_representatives_from_sidecar(
    sidecar_path: Path, floor_basis: str = "execution_time"
) -> tuple[list[str], list[int]]:
    """Canonicalize candidates and persist the SQLGlot decision in the sidecar."""
    con = runner.duckdb.connect(str(sidecar_path))
    try:
        columns = _columns(con, "candidate_query_history")
        text_columns = _columns(con, "candidate_query_text")
        if "query_id" not in columns or "query_id" not in text_columns:
            raise SystemExit(
                "Candidate history/text is incomplete, so SQLGlot matching cannot run. "
                "Live tables remain unchanged."
            )
        basis = floor_basis if floor_basis in columns else "elapsed_time"
        if basis not in columns:
            basis = "query_id"
        owner_parts = [
            runner.quote_ident(name) for name in ("user_name", "user_id") if name in columns
        ]
        owner_sql = "COALESCE(" + ", ".join(
            [f"NULLIF(CAST({name} AS VARCHAR), '')" for name in owner_parts] + ["'UNKNOWN'"]
        ) + ")"
        type_sql = (
            f"COALESCE(CAST({runner.quote_ident('query_type')} AS VARCHAR), '')"
            if "query_type" in columns else "''"
        )
        value_columns = [
            name for name in ("text", "sql_text", "query_text") if name in text_columns
        ]
        if not value_columns:
            raise SystemExit("SYS_QUERY_TEXT supplied no usable text column; live tables are unchanged.")
        text_value = "COALESCE(" + ", ".join(
            f"NULLIF({runner.quote_ident(name)}, '')" for name in value_columns
        ) + ", '')"
        order_columns = [
            name for name in ("sequence", "sequence_num") if name in text_columns
        ]
        order_expr = (
            "COALESCE(" + ", ".join(
                f"TRY_CAST({runner.quote_ident(name)} AS BIGINT)" for name in order_columns
            ) + ", 0)" if order_columns else "0"
        )
        database_sql = (
            f"COALESCE(CAST(h.{runner.quote_ident('database_name')} AS VARCHAR), '')"
            if "database_name" in columns else "''"
        )
        start_sql = (
            f"CAST(h.{runner.quote_ident('start_time')} AS VARCHAR)"
            if "start_time" in columns else "NULL"
        )
        rows = con.execute(
            f"SELECT query_id, {owner_sql}, {type_sql}, "
            f"COALESCE(TRY_CAST({runner.quote_ident(basis)} AS BIGINT), 0), "
            f"COALESCE(t.full_sql, ''), {database_sql}, {start_sql} "
            "FROM candidate_query_history h LEFT JOIN ("
            f"SELECT query_id, STRING_AGG({text_value}, '' ORDER BY {order_expr}) AS full_sql "
            "FROM candidate_query_text GROUP BY query_id"
            ") t USING (query_id)"
        ).fetchall()
        cache: dict[str, tuple[str, str]] = {}
        analyzed: list[dict[str, object]] = []
        for query_id, owner, query_type, runtime, sql_text, database_name, start_time in rows:
            raw = str(sql_text or "")
            fingerprint, method = cache.setdefault(raw, _canonical_sqlglot_fingerprint(raw))
            if fingerprint:
                analyzed.append({
                    "query_id": str(query_id),
                    "owner": str(owner).lower(),
                    "query_type": str(query_type).upper(),
                    "runtime": int(runtime or 0),
                    "fingerprint": fingerprint,
                    "method": method,
                    "sql_text": raw,
                    "database_name": str(database_name or ""),
                    "start_time": start_time,
                })
        groups: dict[tuple[str, str, str], list[dict[str, object]]] = {}
        for item in analyzed:
            groups.setdefault(
                (str(item["owner"]), str(item["query_type"]), str(item["fingerprint"])), []
            ).append(item)
        repeated_groups = [items for items in groups.values() if len(items) > 1]
        repeated_groups.sort(key=lambda items: -sum(int(item["runtime"]) for item in items))
        repeated_ids: list[str] = []
        representatives: list[int] = []
        pattern_rows = []
        group_rows = []
        member_rows = []
        for group_number, items in enumerate(repeated_groups, start=1):
            group_id = f"RQ{group_number:03d}"
            representative = max(items, key=lambda item: int(item["runtime"]))
            try:
                representative_id = int(str(representative["query_id"]))
            except ValueError:
                continue
            representatives.append(representative_id)
            members_sorted = sorted(
                items, key=lambda item: (-int(item["runtime"]), str(item["query_id"]))
            )
            query_ids = [str(item["query_id"]) for item in members_sorted]
            total_runtime_s = sum(int(item["runtime"]) for item in members_sorted) / 1_000_000.0
            worst_runtime_s = max(int(item["runtime"]) for item in members_sorted) / 1_000_000.0
            distinct_sql = {str(item["sql_text"]) for item in members_sorted}
            users = sorted({str(item["owner"]) for item in members_sorted if str(item["owner"])})
            databases = sorted({
                str(item["database_name"]) for item in members_sorted if str(item["database_name"])
            })
            representative_sql = str(representative["sql_text"])
            structure = _sqlglot_structure(representative_sql)
            sql_tables = list(structure["tables"])
            sql_ctes = list(structure["ctes"])
            group_rows.append({
                "repeat_group_id": group_id,
                "query_count": len(members_sorted),
                "distinct_sql_count": len(distinct_sql),
                "avg_similarity": 1.0,
                "min_similarity": 1.0,
                "max_similarity": 1.0,
                "fingerprint_method": str(representative["method"]),
                "parse_success_rate": sum(
                    1 for item in members_sorted if item["method"] == "ast"
                ) / len(members_sorted),
                "total_runtime_s": total_runtime_s,
                "worst_runtime_s": worst_runtime_s,
                "avg_risk_score": 0.0,
                "max_risk_score": 0.0,
                "users": ", ".join(users),
                "databases": ", ".join(databases),
                "query_type": str(representative["query_type"]),
                "repeat_kind": "statement",
                "repeat_match_basis": "loader SQLGlot canonical fingerprint",
                "repeat_constraint_key": str(representative["fingerprint"]),
                "query_ids": ", ".join(query_ids),
                "bridge_query_ids": ", ".join(query_ids),
                "bridge_query_count": len(query_ids),
                "example_query_ids": ", ".join(query_ids[:3]),
                "example_query_id_1": query_ids[0] if query_ids else "",
                "example_query_id_2": query_ids[1] if len(query_ids) > 1 else "",
                "example_query_id_3": query_ids[2] if len(query_ids) > 2 else "",
                "representative_query_id": str(representative["query_id"]),
                "representative_sql": representative_sql[:1200],
                "sql_shape": str(representative["fingerprint"])[:520],
                "sample_sql": representative_sql[:4000],
                "sql_tables": ", ".join(sql_tables[:14]),
                "sql_tables_full": ", ".join(sql_tables),
                "sql_ctes": ", ".join(sql_ctes[:14]),
                "table_count": len(sql_tables),
                "join_count": int(structure["join_count"]),
                "predicate_count": 0,
                "cte_count": int(structure["cte_count"]),
                "wildcard_count": int(structure["wildcard_count"]),
            })
            for rank, item in enumerate(members_sorted, start=1):
                query_id = str(item["query_id"])
                repeated_ids.append(query_id)
                pattern_rows.append((
                    query_id, item["fingerprint"], item["method"], True,
                    query_id == str(representative["query_id"]),
                ))
                member_rows.append({
                    "repeat_group_id": group_id,
                    "member_rank": rank,
                    "shown_in_tree": rank <= 10,
                    "query_id": query_id,
                    "similarity_score": 1.0,
                    "elapsed_s": int(item["runtime"]) / 1_000_000.0,
                    "risk_score": 0.0,
                    "user_name": str(item["owner"]),
                    "database_name": str(item["database_name"]),
                    "query_type": str(item["query_type"]),
                    "start_time": item["start_time"],
                    "dominant_issue": "",
                    "repeat_kind": "statement",
                    "constraint_key": str(item["fingerprint"]),
                    "sql_length": len(str(item["sql_text"])),
                    "sql_parse_status": "parsed" if item["method"] == "ast" else "fallback",
                    "sql_parse_error": "",
                    "sql_tables": ", ".join(sql_tables[:14]),
                    "sql_tables_full": ", ".join(sql_tables),
                })
        con.execute(
            "CREATE TABLE candidate_patterns "
            "(query_id VARCHAR, canonical_fingerprint VARCHAR, method VARCHAR, "
            "is_repeated BOOLEAN, is_representative BOOLEAN)"
        )
        if pattern_rows:
            con.executemany("INSERT INTO candidate_patterns VALUES (?, ?, ?, ?, ?)", pattern_rows)
        import pandas as pd

        for table_name, records in (
            ("precomputed_repeat_groups", group_rows),
            ("precomputed_repeat_members", member_rows),
        ):
            con.execute(f"DROP TABLE IF EXISTS {runner.quote_ident(table_name)}")
            frame = pd.DataFrame(records)
            registered = f"precomputed_{uuid.uuid4().hex}"
            con.register(registered, frame)
            try:
                con.execute(
                    f"CREATE TABLE {runner.quote_ident(table_name)} AS "
                    f"SELECT * FROM {runner.quote_ident(registered)}"
                )
            finally:
                con.unregister(registered)
        con.execute("CHECKPOINT")
        return list(dict.fromkeys(repeated_ids)), representatives
    finally:
        con.close()


def select_repeated_query_ids(
    con,
    snapshot_id: str,
    floor_basis: str = "execution_time",
    minimum_seconds: float = 300.0,
    prefix_chars: int = 80,
    history_table: str | None = None,
    text_table: str | None = None,
) -> tuple[list[str], list[int]]:
    """Return all repeated execution IDs and one representative per parent."""
    history_table = history_table or STAGE_PREFIX + "query_history"
    text_table = text_table or STAGE_PREFIX + "query_text"
    history_columns = _columns(con, history_table)
    text_columns = _columns(con, text_table)
    if "query_id" not in history_columns or "query_id" not in text_columns:
        return [], []

    basis = str(floor_basis or "execution_time").lower()
    if basis not in {"execution_time", "elapsed_time"} or basis not in history_columns:
        basis = "elapsed_time" if "elapsed_time" in history_columns else "execution_time"
    if basis not in history_columns:
        return [], []
    owner_parts = [
        f"NULLIF(CAST(h.{runner.quote_ident(name)} AS VARCHAR), '')"
        for name in ("user_name", "user_id") if name in history_columns
    ]
    owner_expr = "COALESCE(" + ", ".join((*owner_parts, "'UNKNOWN'")) + ")"
    status_expr = (
        f"COALESCE(CAST(h.{runner.quote_ident('status')} AS VARCHAR), '')"
        if "status" in history_columns else "'success'"
    )
    cache_expr = (
        f"COALESCE(CAST(h.{runner.quote_ident('result_cache_hit')} AS VARCHAR), 'false')"
        if "result_cache_hit" in history_columns else "'false'"
    )
    type_expr = (
        f"COALESCE(CAST(h.{runner.quote_ident('query_type')} AS VARCHAR), '')"
        if "query_type" in history_columns else "''"
    )
    value_columns = [name for name in ("text", "sql_text", "query_text") if name in text_columns]
    if not value_columns:
        return [], []
    text_value = "COALESCE(" + ", ".join(
        f"NULLIF({runner.quote_ident(name)}, '')" for name in value_columns
    ) + ", '')"
    order_columns = [name for name in ("sequence", "sequence_num") if name in text_columns]
    order_expr = (
        "COALESCE(" + ", ".join(
            f"TRY_CAST({runner.quote_ident(name)} AS BIGINT)" for name in order_columns
        ) + ", 0)" if order_columns else "0"
    )
    history_snapshot = "h.snapshot_id = ?" if "snapshot_id" in history_columns else "TRUE"
    text_snapshot = "snapshot_id = ?" if "snapshot_id" in text_columns else "TRUE"
    params = ([snapshot_id] if "snapshot_id" in text_columns else []) + (
        [snapshot_id] if "snapshot_id" in history_columns else []
    )
    frame = con.execute(
        f"""
SELECT h.query_id,
       {owner_expr} AS query_owner,
       {status_expr} AS query_status,
       {cache_expr} AS result_cache_hit,
       {type_expr} AS query_type,
       COALESCE(TRY_CAST(h.{runner.quote_ident(basis)} AS BIGINT), 0) AS execution_time,
       COALESCE(t.sql_text, '') AS sql_text
FROM {runner.quote_ident(history_table)} h
LEFT JOIN (
  SELECT query_id, STRING_AGG({text_value}, '' ORDER BY {order_expr}) AS sql_text
  FROM {runner.quote_ident(text_table)}
  WHERE {text_snapshot}
  GROUP BY query_id
) t ON t.query_id = h.query_id
WHERE {history_snapshot}
""",
        params,
    ).fetchdf()
    floor_us = max(0, int(float(minimum_seconds) * 1_000_000))
    prefix_length = max(8, int(prefix_chars or 80))
    candidates: list[tuple[str, str, int, str]] = []
    prefix_counts: dict[tuple[str, str], int] = {}
    for row in frame.itertuples(index=False):
        runtime = int(row.execution_time or 0)
        text = str(row.sql_text or "").strip()
        status = str(row.query_status or "").strip().lower()
        cache_hit = str(row.result_cache_hit or "").strip().lower() in {"1", "t", "true", "yes"}
        query_type = str(row.query_type or "").strip().upper()
        if (
            runtime < floor_us
            or not text
            or status not in {"success", "completed", "complete"}
            or cache_hit
            or query_type in {"PROCEDURE", "UTILITY"}
        ):
            continue
        owner = str(row.query_owner or "UNKNOWN").strip().lower() or "unknown"
        normalized = runner.simple_fingerprint(text) or text.lower()
        query_id = str(row.query_id)
        key = (owner, normalized[:prefix_length])
        prefix_counts[key] = prefix_counts.get(key, 0) + 1
        candidates.append((owner, normalized, runtime, query_id))

    repeated = [
        item for item in candidates
        if prefix_counts.get((item[0], item[1][:prefix_length]), 0) > 1
    ]
    groups: dict[tuple[str, str], dict[str, object]] = {}
    for owner, fingerprint, runtime, query_id in repeated:
        group = groups.setdefault(
            (owner, fingerprint), {"total": 0, "best_runtime": -1, "best_id": None}
        )
        group["total"] = int(group["total"]) + runtime
        if runtime > int(group["best_runtime"]):
            group["best_runtime"] = runtime
            group["best_id"] = query_id
    ranked = sorted(groups.values(), key=lambda group: -int(group["total"]))
    representatives: list[int] = []
    for group in ranked:
        try:
            representatives.append(int(str(group["best_id"])))
        except (TypeError, ValueError):
            continue
    repeated_ids = list(dict.fromkeys(item[3] for item in repeated))
    return repeated_ids, representatives


def _prune_staged_roots(con, repeated_ids: list[str]) -> None:
    import pandas as pd

    keep = pd.DataFrame({"query_id": [str(value) for value in repeated_ids]})
    registered = f"repair_keep_{uuid.uuid4().hex}"
    con.register(registered, keep)
    try:
        for table_name in ROOT_TABLES:
            stage_name = STAGE_PREFIX + table_name
            con.execute(
                f"DELETE FROM {runner.quote_ident(stage_name)} "
                f"WHERE CAST(query_id AS VARCHAR) NOT IN "
                f"(SELECT query_id FROM {runner.quote_ident(registered)})"
            )
    finally:
        con.unregister(registered)


def _backup_file(duckdb_path: Path, lock_wait: float, reason: str = "before-repeat-repair") -> Path:
    con = runner.open_duck(duckdb_path, lock_wait)
    try:
        con.execute("CHECKPOINT")
    finally:
        con.close()
    backup_dir = duckdb_path.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = backup_dir / f"{duckdb_path.stem}.{reason}.{stamp}{duckdb_path.suffix}"
    shutil.copy2(duckdb_path, backup_path)
    if backup_path.stat().st_size != duckdb_path.stat().st_size:
        raise SystemExit(f"Backup verification failed: {backup_path}. Nothing was changed.")
    return backup_path


def restore_backup(duckdb_path: Path, backup_path: Path, lock_wait: float) -> int:
    if not backup_path.is_file():
        raise SystemExit(f"Backup file not found: {backup_path}")
    if backup_path.resolve() == duckdb_path.resolve():
        raise SystemExit("The backup and live DuckDB paths are the same; nothing was changed.")
    check = runner.duckdb.connect(str(backup_path), read_only=True)
    try:
        tables = int(check.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
        ).fetchone()[0] or 0)
    finally:
        check.close()
    if tables <= 0:
        raise SystemExit("The selected backup contains no base tables; restore was refused.")
    safety_copy = _backup_file(duckdb_path, lock_wait, reason="before-manual-restore")
    shutil.copy2(backup_path, duckdb_path)
    if duckdb_path.stat().st_size != backup_path.stat().st_size:
        shutil.copy2(safety_copy, duckdb_path)
        raise SystemExit("Restore verification failed; the pre-restore safety copy was put back.")
    print(f"Restore complete: {backup_path}")
    print(f"Pre-restore safety copy: {safety_copy}")
    return 0


def _config(args):
    cfg = runner.build_config(argparse.Namespace(
        table_databases=None,
        days=args.days,
        floor_seconds=args.minimum_seconds,
        floor_basis=args.floor_basis,
    ))
    cfg.evidence_parent_limit = 0
    return cfg


def _promote(con, snapshot_id: str, sql_by_table: dict[str, str]) -> None:
    con.execute("BEGIN TRANSACTION")
    try:
        for table_name in PROMOTE_TABLES:
            con.execute(f"DROP TABLE IF EXISTS {runner.quote_ident(table_name)}")
            con.execute(
                f"ALTER TABLE {runner.quote_ident(STAGE_PREFIX + table_name)} "
                f"RENAME TO {runner.quote_ident(table_name)}"
            )
            runner.ensure_table(con, table_name, runner.EXPECTED_COLUMNS.get(table_name, ()))
        con.execute(
            "CREATE TABLE IF NOT EXISTS snapshot_runs "
            "(snapshot_id VARCHAR PRIMARY KEY, captured_at TIMESTAMP, label VARCHAR, source VARCHAR)"
        )
        con.execute(
            "INSERT OR REPLACE INTO snapshot_runs VALUES (?, ?, ?, ?)",
            [snapshot_id, datetime.now(), "five-minute repeat repair", "repeat-repair-runner"],
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS analyzer_source_sql "
            "(table_name VARCHAR, snapshot_id VARCHAR, sql_hash VARCHAR, sql_text VARCHAR, "
            "recorded_at TIMESTAMP, source VARCHAR, PRIMARY KEY (table_name, snapshot_id))"
        )
        for table_name in PROMOTE_TABLES:
            sql_text = sql_by_table.get(
                table_name, "loader-side SQLGlot materialization from sysquery_history.db"
            )
            con.execute(
                "INSERT OR REPLACE INTO analyzer_source_sql VALUES (?, ?, ?, ?, ?, ?)",
                [table_name, snapshot_id, runner.sql_hash(sql_text), sql_text,
                 datetime.now(), "repeat-repair-runner"],
            )
        runner.ensure_indexes(con)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def run(args) -> int:
    duckdb_path = Path(args.duckdb_path).expanduser().resolve()
    if not duckdb_path.is_file():
        raise SystemExit(f"DuckDB file not found: {duckdb_path}")

    print("== Five-minute repeated-query repair ==")
    print(f"DuckDB: {duckdb_path}")
    print("Protected and never touched: " + ", ".join(PROTECTED_TABLES))
    print("\nCreating the rollback point BEFORE any Redshift work ...")
    backup_path = _backup_file(duckdb_path, args.lock_wait_seconds)
    print(f"ROLLBACK FILE: {backup_path}")
    print(f"Verified bytes: {backup_path.stat().st_size:,}\n")

    con = runner.open_duck(duckdb_path, args.lock_wait_seconds)
    try:
        old_counts = {name: _row_count(con, name) for name in REFRESH_TABLES}
        protected_counts = {name: _row_count(con, name) for name in PROTECTED_TABLES}
        snapshot_id = _catalog_anchor_snapshot(con)
    finally:
        con.close()
    if args.backup_only:
        print("Backup-only complete. No tables were loaded, staged, or changed.")
        return 0

    cfg = _config(args)
    sidecar_path = duckdb_path.parent / "sysquery_history.db"
    print(f"Snapshot anchor: {snapshot_id} (keeps Table Review catalog visible)")
    sql_by_table: dict[str, str] = {}
    print(
        f"Loading SYS_QUERY_HISTORY gate for {args.days:g} day(s), "
        f">= {args.minimum_seconds:g}s {args.floor_basis} ..."
    )
    print("Cheap cuts: successful, non-cache, non-PROCEDURE/UTILITY, repeating owner + SQL prefix.")
    history_sql = runner.table_sql(cfg, "query_history")
    sql_by_table["query_history"] = history_sql
    history_frame = runner.fetch_frame(
        cfg, cfg.primary_database, history_sql, stage="query_history gate"
    )
    preliminary_ids = preselect_history_ids(
        history_frame, args.minimum_seconds, args.repeat_prefix_chars, args.floor_basis
    )
    if not preliminary_ids:
        raise SystemExit(
            "The SYS_QUERY_HISTORY gate found no repeated candidates. Live tables remain unchanged. "
            f"Rollback file: {backup_path}"
        )
    history_frame = _filter_frame_to_ids(history_frame, preliminary_ids)
    print(
        f"  query_history: {len(history_frame.index):,} repeated-prefix candidate execution(s) "
        "retained before full text"
    )
    write_candidate_sidecar(sidecar_path, history_frame)
    print(f"  candidate sidecar: {sidecar_path}")
    text_sql = _query_text_sql(preliminary_ids)
    sql_by_table["query_text"] = text_sql
    text_frame = runner.fetch_frame(
        cfg, cfg.primary_database, text_sql, stage="query_text for cheap-cut candidates"
    )
    write_candidate_text_sidecar(sidecar_path, text_frame)
    print(f"  candidate query text: {len(text_frame.index):,} fragment row(s) cached in sidecar")
    print("Running SQLGlot canonical matching against full candidate text ...")
    repeated_ids, target_ids = sqlglot_representatives_from_sidecar(
        sidecar_path, args.floor_basis
    )
    if not repeated_ids or not target_ids:
        raise SystemExit(
            "SQLGlot found no genuinely repeated families after the cheap cut. "
            f"Live tables remain unchanged. Rollback file: {backup_path}"
        )
    sidecar_con = runner.duckdb.connect(str(sidecar_path), read_only=True)
    try:
        group_frames = {
            "loader_repeat_groups": sidecar_con.execute(
                "SELECT * FROM precomputed_repeat_groups"
            ).fetchdf(),
            "loader_repeat_members": sidecar_con.execute(
                "SELECT * FROM precomputed_repeat_members"
            ).fetchdf(),
        }
    finally:
        sidecar_con.close()
    history_frame = _filter_frame_to_ids(history_frame, repeated_ids)
    text_frame = _filter_frame_to_ids(text_frame, repeated_ids)
    print(f"  query_text: {len(text_frame.index):,} fragment row(s) retained for confirmed repeats")

    con = runner.open_duck(duckdb_path, args.lock_wait_seconds)
    try:
        _write_stage(con, "query_history", history_frame, snapshot_id)
        _write_stage(con, "query_text", text_frame, snapshot_id)
        _prune_staged_roots(con, repeated_ids)
        staged_root_counts = {
            name: _row_count(con, STAGE_PREFIX + name) for name in ROOT_TABLES
        }
    finally:
        con.close()
    del history_frame, text_frame
    print(
        f"SQLGlot repetition cut: {len(repeated_ids):,} executions retained; "
        f"{len(target_ids):,} parent representatives."
    )

    print("\nLoading all five query-ID auxiliary datasets for those representatives ...")
    evidence_frames = {}
    for table_name in EVIDENCE_TABLES:
        sql_text = runner.table_sql(cfg, table_name, target_ids=target_ids)
        sql_by_table[table_name] = sql_text
        evidence_frames[table_name] = runner.fetch_frame(
            cfg, cfg.primary_database, sql_text, stage=table_name
        )
        print(f"  {table_name}: {len(evidence_frames[table_name].index):,} source row(s)")

    con = runner.open_duck(duckdb_path, args.lock_wait_seconds)
    try:
        for table_name in EVIDENCE_TABLES:
            _write_stage(con, table_name, evidence_frames[table_name], snapshot_id)
        new_counts = {
            **staged_root_counts,
            **{name: _row_count(con, STAGE_PREFIX + name) for name in EVIDENCE_TABLES},
        }
        scan_coverage = _scan_coverage(con, STAGE_PREFIX + "table_scan_info")
        for table_name in GROUP_TABLES:
            _write_stage(con, table_name, group_frames[table_name], snapshot_id)
        group_counts = {
            name: _row_count(con, STAGE_PREFIX + name) for name in GROUP_TABLES
        }
    finally:
        con.close()
    del evidence_frames

    print("\nPre-promotion validation:")
    invalid = []
    for table_name in REFRESH_TABLES:
        print(f"  {table_name}: {old_counts[table_name]:,} -> {new_counts[table_name]:,}")
        if new_counts[table_name] <= 0:
            invalid.append(table_name)
    for table_name in GROUP_TABLES:
        print(f"  {table_name}: {group_counts[table_name]:,}")
        if group_counts[table_name] <= 0:
            invalid.append(table_name)
    print(f"  table_scan_info query/table intersections: {scan_coverage:,}")
    if invalid:
        raise SystemExit(
            "Promotion refused because these staged tables are empty: " + ", ".join(invalid)
            + f". Live tables remain unchanged. Rollback file: {backup_path}"
        )
    if args.stage_only:
        print("Stage-only complete. Live tables were not changed.")
        print(f"Rollback file: {backup_path}")
        return 0

    con = runner.open_duck(duckdb_path, args.lock_wait_seconds)
    try:
        _promote(con, snapshot_id, sql_by_table)
        con.execute("CHECKPOINT")
        final_counts = {name: _row_count(con, name) for name in REFRESH_TABLES}
        protected_after = {name: _row_count(con, name) for name in PROTECTED_TABLES}
    finally:
        con.close()
    if protected_after != protected_counts:
        raise SystemExit(
            "Protected-table verification changed unexpectedly. Restore immediately with:\n"
            f'  python repair_short_tables.py --duckdb-path "{duckdb_path}" '
            f'--restore-backup "{backup_path}"'
        )

    print("\nRepair complete. Query data plus precomputed grouping promoted:")
    for table_name in REFRESH_TABLES:
        print(f"  {table_name}: {final_counts[table_name]:,}")
    for table_name in GROUP_TABLES:
        print(f"  {table_name}: {group_counts[table_name]:,}")
    print("Protected table counts verified unchanged, including table information.")
    print(f"Rollback file retained at: {backup_path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rollback-safe SYS_QUERY_HISTORY-gated repeat repair; never touches table information."
    )
    parser.add_argument("--duckdb-path", default=None, help="Analyzer DuckDB file.")
    parser.add_argument("--days", type=float, default=runner.DEFAULT_DAYS)
    parser.add_argument("--minimum-seconds", type=float, default=300.0)
    parser.add_argument("--repeat-prefix-chars", type=int, default=80)
    parser.add_argument(
        "--floor-basis", choices=("execution_time", "elapsed_time"), default="execution_time"
    )
    parser.add_argument(
        "--lock-wait-seconds", type=float, default=runner.DEFAULT_LOCK_WAIT_SECONDS
    )
    parser.add_argument("--stage-only", action="store_true")
    parser.add_argument("--backup-only", action="store_true")
    parser.add_argument(
        "--restore-backup", default=None,
        help="Restore a selected full DuckDB backup. Close the analyzer first.",
    )
    args = parser.parse_args()
    runner._load_dotenv_if_present()
    args.duckdb_path = (
        args.duckdb_path or os.environ.get("REDSHIFT_DUCKDB_PATH")
        or str(runner.resolve_default_duckdb_path())
    )
    duckdb_path = Path(args.duckdb_path).expanduser().resolve()
    if args.restore_backup:
        return restore_backup(
            duckdb_path, Path(args.restore_backup).expanduser().resolve(), args.lock_wait_seconds
        )
    if runner._IMPORT_ERROR is not None:
        print(f"Missing package: {runner._IMPORT_ERROR}")
        return 2
    if not args.backup_only:
        try:
            __import__("redshift_connector")
        except ImportError:
            print("Use the same Python environment that successfully ran runner.py yesterday.")
            return 2
        for key in ("REDSHIFT_HOST", "REDSHIFT_USER"):
            if not os.environ.get(key):
                print(f"{key} is not set. Keep the existing .env beside this script.")
                return 2
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
