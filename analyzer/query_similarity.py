"""SQL intelligence and repeat-query detection for captured Redshift SQL text."""
from __future__ import annotations

import hashlib
import math
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from difflib import SequenceMatcher
from functools import lru_cache

import pandas as pd


def format_progress_eta(done: int, total: int, started_at: float) -> str:
    """Human ETA suffix for load sub-indicators (rate + time remaining).

    Used on long steps (SQL parse, grouping / step ~11) so the dialog can
    estimate wall time left, not only "X of Y".
    """
    done = max(0, int(done))
    total = max(0, int(total))
    if total <= 0:
        return ""
    if done <= 0:
        return f" (0 of {total:,}; estimating...)"
    elapsed = max(0.001, time.monotonic() - float(started_at))
    rate = done / elapsed
    remaining = max(0, total - done)
    if rate < 1e-6:
        return f" ({done:,} of {total:,}; rate unknown)"
    secs_left = remaining / rate
    if secs_left < 90:
        left = f"~{max(1, int(round(secs_left)))}s left"
    elif secs_left < 3600:
        left = f"~{int(round(secs_left / 60))} min left"
    else:
        hours = secs_left / 3600.0
        left = f"~{hours:.1f} h left"
    per_sec = f"{rate:.1f}/s" if rate >= 0.1 else f"{rate:.2f}/s"
    return f" ({done:,} of {total:,} @ {per_sec}, {left})"

from ._lazy_sqlglot import exp, sqlglot

try:
    from rapidfuzz import fuzz as _rapidfuzz

    def _text_ratio(left: str, right: str) -> float:
        return float(_rapidfuzz.ratio(left, right)) / 100.0
except ImportError:
    def _text_ratio(left: str, right: str) -> float:
        return SequenceMatcher(None, left, right).ratio()


DEFAULT_SIMILARITY_THRESHOLD = 0.78
DEFAULT_PREFILTER_THRESHOLD = 0.30
DEFAULT_CROSS_TABLE_PREFILTER_THRESHOLD = 0.72
DEFAULT_FUZZY_MERGE_THRESHOLD = 0.95
MIN_SQL_CHARS = 24
MIN_REPEAT_GROUP_SIZE = 2
_FUZZY_LENGTH_WINDOW = 0.25
_FUZZY_MAX_PAIRS_PER_BLOCK = 4000

_REPEAT_TOTAL_COLUMNS = (
    ("elapsed_s", "total_elapsed_s"),
    ("execution_s", "total_execution_s"),
    ("queue_s", "total_queue_s"),
    ("input_rows", "total_input_rows"),
    ("output_rows", "total_output_rows"),
    ("input_bytes", "total_input_bytes"),
    ("output_bytes", "total_output_bytes"),
    ("total_spill", "total_spill_blocks"),
    ("remote_read_io", "total_remote_read_io"),
)

_REPEAT_AVERAGE_COLUMNS = (
    "elapsed_s",
    "execution_s",
    "queue_s",
    "planning_s",
    "compile_s",
    "total_spill",
    "input_bytes",
    "output_bytes",
    "blocks_read",
    "blocks_write",
    "local_read_io",
    "remote_read_io",
    "input_rows",
    "output_rows",
    "selectivity_ratio",
    "total_step_duration_s",
    "max_step_duration_s",
    "avg_step_duration_s",
    "total_steps",
    "max_data_skewness",
    "max_time_skewness",
    "segments_used",
    "streams_used",
    "scan_steps",
    "join_steps",
    "sort_steps",
    "agg_steps",
    "tables_touched",
    "alert_count",
    "external_steps",
    "s3_steps",
    "external_input_bytes",
    "external_input_rows",
    "external_duration_s",
    "external_duration_pct",
    "remote_io_ratio",
    "external_selectivity",
    "external_spill_blocks",
    "external_tables_touched",
    "external_data_skew",
    "seq_scan_cnt",
    "s3_scan_cnt",
    "partition_loop_cnt",
    "dist_both_cnt",
    "bcast_cnt",
    "dist_total_cnt",
    "has_nested_loop",
    "hash_join_cnt",
    "subquery_cnt",
    "network_cnt",
    "missing_stats_flag",
    "max_est_rows",
    "max_cost",
    "cost_score",
    "plan_node_count",
    "sort_node_cnt",
    "agg_node_cnt",
    "filter_node_cnt",
    "risk_score",
)

_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT_RE = re.compile(r"--[^\r\n]*")
_SINGLE_QUOTED_RE = re.compile(r"'(?:''|[^'])*'")
_DOLLAR_QUOTED_RE = re.compile(r"\$[A-Za-z_]*\$.*?\$[A-Za-z_]*\$", re.DOTALL)
_NUMERIC_LITERAL_RE = re.compile(
    r"(?<![A-Za-z0-9_$])[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?(?![A-Za-z0-9_$])"
)
_QUESTION_LIST_RE = re.compile(r"\(\s*\?(?:\s*,\s*\?){1,}\s*\)")

# Run-specific suffixes inside IDENTIFIERS. _NUMERIC_LITERAL_RE deliberately
# skips digits attached to a word, so staging tables like stg_orders_20240727,
# tmp_load_12345 and etl_run_998877 each fingerprinted as a distinct shape -
# every nightly run of the same job looked unique and nothing ever grouped.
# Requires a leading letter and separator so genuine names such as
# fact_orders_2024 (a real partitioned table) are not collapsed away: the
# pattern targets a trailing _<digits> only.
_RUN_SUFFIX_RE = re.compile("([a-z][a-z0-9_$]*?_)[0-9]{3,}(?![a-z0-9_$])")


def _run_suffix_replacement(match) -> str:
    """Collapse a run-specific numeric suffix while keeping the stem.

    stg_tmp_12345 and stg_tmp_98765 both become stg_tmp_#, so nightly runs
    of the same job share one fingerprint instead of each looking unique.
    A named function avoids the backreference-escaping mistakes that a
    "\1" template invites when this file is edited programmatically.
    """
    return match.group(1) + "#"
_WHITESPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-z_][a-z0-9_$]*|\?|<=|>=|<>|!=|[(),.=+*/%-]")
_TABLE_REF_RE = re.compile(
    r"\b(?:from|join|update|into)\s+"
    r"([a-z_][a-z0-9_$]*(?:\.[a-z_][a-z0-9_$]*){0,2})"
)
@lru_cache(maxsize=1)
def _predicate_types() -> tuple:
    return (
        exp.EQ,
        exp.NEQ,
        exp.GT,
        exp.GTE,
        exp.LT,
        exp.LTE,
        exp.Like,
        exp.ILike,
        exp.In,
        exp.Between,
        exp.Is,
    )


@dataclass(frozen=True)
class SQLIntelligence:
    normalized_sql: str
    ast_node_shape: tuple[str, ...]
    parse_ok: bool
    parse_error: str
    tables: frozenset[str]
    columns: frozenset[str]
    joins: frozenset[str]
    predicates: frozenset[str]
    predicate_operators: frozenset[str]
    ctes: frozenset[str]
    join_columns: frozenset[str]
    filter_columns: frozenset[str]
    projected_columns: frozenset[str]
    order_columns: frozenset[str]
    group_columns: frozenset[str]
    projection_count: int
    wildcard_count: int
    join_count: int
    predicate_count: int
    cte_count: int
    subquery_count: int
    aggregate_count: int
    function_count: int


@dataclass(frozen=True)
class _PreparedQuery:
    frame_index: int
    query_id: object
    user_key: str
    repeat_structure_key: str
    intelligence: SQLIntelligence
    tokens: tuple[str, ...]
    token_set: frozenset[str]
    token_bigrams: frozenset[tuple[str, str]]
    elapsed_s: float
    risk_score: float

    @property
    def normalized_sql(self) -> str:
        return self.intelligence.normalized_sql

    @property
    def tables(self) -> frozenset[str]:
        return self.intelligence.tables

    @property
    def columns(self) -> frozenset[str]:
        return self.intelligence.columns

    @property
    def joins(self) -> frozenset[str]:
        return self.intelligence.joins

    @property
    def predicates(self) -> frozenset[str]:
        return self.intelligence.predicates

    @property
    def parse_ok(self) -> bool:
        return self.intelligence.parse_ok


def normalize_sql_shape(sql: object) -> str:
    """Return a stable SQL shape where literals and line breaks do not hide repeats."""
    return analyze_sql(sql).normalized_sql


def analyze_sql(sql: object) -> SQLIntelligence:
    """Parse SQL with sqlglot and extract structural features for analysis.

    Outermost guard. Individual internal paths are guarded too, but this is the
    boundary the loader calls, so nothing raised anywhere beneath it may escape:
    a single unanalyzable statement must never zero the whole workload's
    grouping. Degrading to the regex-derived fallback loses descriptive detail
    for that one query and keeps the other thousands.
    """
    text = _sql_to_text(sql)
    if not text:
        return _fallback_intelligence("")
    try:
        return _analyze_sql_text(text)
    except Exception:
        return _fallback_intelligence(text)


def enrich_slow_queries_with_sql_features(
    slow_queries: pd.DataFrame,
    progress: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Add parse status and sqlglot-derived feature columns to slow-query rows.

    *progress*, when set, receives live "X of Y" messages with a rate-based ETA.
    This loop parses every captured statement, so on a large capture it can run
    for minutes; without a callback the UI shows only a spinner and is
    indistinguishable from a hang.
    """
    if slow_queries is None or slow_queries.empty or "sql_text" not in slow_queries.columns:
        return slow_queries
    enriched = slow_queries.copy()
    feature_rows = []
    total_rows = len(enriched)
    report_every = max(1, min(200, total_rows // 25 or 1))
    parse_started = time.monotonic()
    for position, (_, row) in enumerate(enriched.iterrows()):
        if progress is not None and (
            position == 0
            or (position + 1) % report_every == 0
            or position + 1 == total_rows
        ):
            done = position + 1
            progress(
                f"Parsing SQL shapes - extracting features from {done:,} of {total_rows:,} query(s)"
                f"{format_progress_eta(done, total_rows, parse_started)}"
            )
        intel = analyze_sql(row.get("sql_text"))
        feature_rows.append(
            {
                "sql_parse_status": "parsed" if intel.parse_ok else "fallback",
                "sql_parse_error": intel.parse_error,
                "sql_table_count": len(intel.tables),
                "sql_join_count": intel.join_count,
                "sql_predicate_count": intel.predicate_count,
                "sql_cte_count": intel.cte_count,
                "sql_subquery_count": intel.subquery_count,
                "sql_wildcard_count": intel.wildcard_count,
                "sql_aggregate_count": intel.aggregate_count,
                "sql_function_count": intel.function_count,
                "sql_projection_count": intel.projection_count,
                "sql_tables": _join_sorted(intel.tables, limit=12),
                "sql_columns": _join_sorted(intel.columns, limit=18),
                "sql_join_columns": _join_sorted(intel.join_columns, limit=14),
                "sql_filter_columns": _join_sorted(intel.filter_columns, limit=14),
                "sql_projected_columns": _join_sorted(intel.projected_columns, limit=18),
                "sql_order_columns": _join_sorted(intel.order_columns, limit=10),
                "sql_group_columns": _join_sorted(intel.group_columns, limit=10),
                "sql_joins": _join_sorted(intel.joins, limit=8),
                "sql_predicates": _join_sorted(intel.predicates, limit=8),
                "sql_predicate_operators": _join_sorted(intel.predicate_operators, limit=12),
                "sql_repeat_signature": _repeat_structure_key(intel),
                "sql_shape": _truncate(intel.normalized_sql, 520),
            }
        )
    features = pd.DataFrame(feature_rows, index=enriched.index)
    for col in features.columns:
        enriched[col] = features[col]
    return enriched


def score_sql_similarity(left_sql: object, right_sql: object) -> float:
    """Score two SQL statements from 0.0 to 1.0 using AST shape when available."""
    left = _prepare_sql(left_sql)
    right = _prepare_sql(right_sql)
    if not left.normalized_sql or not right.normalized_sql:
        return 0.0
    return _score_prepared(left, right)


def build_repeat_query_report(
    slow_queries: pd.DataFrame,
    *,
    threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    prefilter_threshold: float = DEFAULT_PREFILTER_THRESHOLD,
    cross_table_prefilter_threshold: float = DEFAULT_CROSS_TABLE_PREFILTER_THRESHOLD,
    procedure_definitions: pd.DataFrame | None = None,
    scope_by_user: bool = False,
    min_group_size: int = MIN_REPEAT_GROUP_SIZE,
    fuzzy_merge_threshold: float = DEFAULT_FUZZY_MERGE_THRESHOLD,
    progress: Callable[[str], None] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Cluster repeated query families by canonical SQL fingerprint.

    Primary grouping is the literal-free canonical fingerprint (sqlglot AST with a
    regex fallback), so the same query run with different predicate values, dates,
    or IN-list lengths lands in one family. A guarded fuzzy pass then merges
    near-identical shapes that share the same table set.

    *progress*, when set, receives live "X of Y" sub-indicator messages with a
    rate-based time estimate so the loading dialog can show how long this step
    (typically step ~11 after parse) will take.
    """
    _ = threshold
    _ = prefilter_threshold
    _ = cross_table_prefilter_threshold
    if slow_queries is None or slow_queries.empty or "sql_text" not in slow_queries.columns:
        return _empty_groups(), _empty_members()
    procedure_map = _procedure_definition_map(procedure_definitions)
    candidates = _deterministic_repeat_candidates(
        slow_queries,
        procedure_map,
        scope_by_user=scope_by_user,
        progress=progress,
    )
    if progress is not None:
        progress(
            f"Grouping repeated query patterns - clustering {len(candidates):,} "
            f"fingerprint candidate(s)"
        )
    raw_groups = _deterministic_repeat_groups(
        candidates,
        min_group_size=min_group_size,
        fuzzy_merge_threshold=fuzzy_merge_threshold,
    )
    if not raw_groups:
        if progress is not None:
            progress("Grouping repeated query patterns - no repeat groups found")
        return _empty_groups(), _empty_members()

    raw_groups.sort(
        key=lambda members: (
            -sum(item["elapsed_s"] for item in members),
            -max(item["risk_score"] for item in members),
            -len(members),
        )
    )

    group_rows: list[dict] = []
    member_rows: list[dict] = []
    seen_group_keys: dict[str, int] = {}
    total_groups = len(raw_groups)
    materialize_started = time.monotonic()
    for group_number, members in enumerate(raw_groups, start=1):
        if progress is not None and (
            group_number == 1
            or group_number == total_groups
            or group_number % 5 == 0
        ):
            eta = format_progress_eta(group_number, total_groups, materialize_started)
            progress(
                f"Grouping repeated query patterns - materializing group "
                f"{group_number:,} of {total_groups:,}{eta}"
            )
        group_id = f"RQ{group_number:03d}"
        members_sorted = sorted(members, key=lambda item: (-item["elapsed_s"], -item["risk_score"], item["row_order"]))
        members_by_first_seen = sorted(members_sorted, key=lambda item: item["row_order"])
        rows = slow_queries.loc[[item["frame_index"] for item in members_sorted]]
        rows_by_first_seen = slow_queries.loc[[item["frame_index"] for item in members_by_first_seen]]
        representative = min(members_sorted, key=lambda item: item["row_order"])
        # RQnnn is only a display rank and shifts whenever the pain ranking
        # changes. The durable identity is a hash of what the group IS —
        # its kind, constraints, and SQL shape — so saved work (annotations,
        # reviews) can survive a refresh.
        key_basis = "|".join((
            str(representative.get("repeat_kind") or ""),
            str(representative.get("constraint_key") or ""),
            str(representative.get("group_sql_shape") or ""),
        ))
        repeat_group_key = "G" + hashlib.sha256(
            key_basis.encode("utf-8", errors="replace")
        ).hexdigest()[:12]
        collision = seen_group_keys.get(repeat_group_key, 0)
        seen_group_keys[repeat_group_key] = collision + 1
        if collision:
            repeat_group_key = f"{repeat_group_key}-{collision + 1}"
        representative_row = slow_queries.loc[representative["frame_index"]]
        query_ids = _join_values(rows_by_first_seen.get("query_id"), limit=999999)
        snapshot_ids = _join_values(rows_by_first_seen.get("snapshot_id"), limit=8)
        example_ids = _ordered_unique_text(rows_by_first_seen.get("query_id"), limit=3)
        # When the user is NOT part of the pattern identity, a group can span
        # many users and listing eight of them implies a precision the grouping
        # deliberately does not have. Name the situation instead.
        users = _grouped_user_label(
            rows.get("user_name"), scope_by_user=scope_by_user
        ) or representative["user_key"]
        databases = _join_values(rows.get("database_name"), limit=8)
        total_runtime = sum(item["elapsed_s"] for item in members_sorted)
        worst_runtime = max(item["elapsed_s"] for item in members_sorted)
        avg_metrics = _average_metric_values(rows)
        # The sproc body is static, so it is resolved once per group here
        # instead of being copied onto every captured run of the CALL.
        procedure_definition = procedure_map.get(representative.get("procedure_key") or "", "")
        member_intels = []
        for item in members_sorted:
            if item["repeat_kind"] == "stored_procedure":
                member_intels.append(_fallback_intelligence(item["group_sql_shape"]))
            else:
                member_intels.append(analyze_sql(slow_queries.loc[item["frame_index"]].get("sql_text")))
        if representative["repeat_kind"] == "stored_procedure":
            statement_intels = _procedure_statement_intels(procedure_definition) if procedure_definition else []
            if statement_intels:
                intel = statement_intels[0]
                agg_intels = statement_intels
            else:
                intel = _fallback_intelligence(procedure_definition or representative["group_sql_shape"])
                agg_intels = [intel]
        else:
            intel = analyze_sql(representative_row.get("sql_text"))
            agg_intels = member_intels
        parse_success_rate = sum(1 for mi in agg_intels if mi.parse_ok) / len(agg_intels)

        def _union_of(attr: str) -> list[str]:
            return sorted({str(v).strip() for mi in agg_intels for v in getattr(mi, attr) if str(v).strip()})

        union_tables = _union_of("tables")
        shared_tables = sorted(
            set.intersection(*({str(v).strip() for v in mi.tables if str(v).strip()} for mi in agg_intels))
        )
        sample_sql = procedure_definition if procedure_definition else representative_row.get("sql_text")
        shape_scores = [float(item.get("shape_score", 1.0)) for item in members_sorted]
        distinct_sql_texts: set[str] = set()
        sql_values = rows.get("sql_text")
        if sql_values is not None:
            for value in sql_values:
                if value is None or (isinstance(value, float) and math.isnan(value)):
                    continue
                text = str(value).strip()
                if text:
                    distinct_sql_texts.add(text)
        group_row = {
            "repeat_group_id": group_id,
            "repeat_group_key": repeat_group_key,
            "query_count": len(members_sorted),
            "distinct_sql_count": len(distinct_sql_texts),
            "avg_similarity": round(sum(shape_scores) / len(shape_scores), 4),
            "min_similarity": round(min(shape_scores), 4),
            "max_similarity": round(max(shape_scores), 4),
            "fingerprint_method": representative.get("fingerprint_method", ""),
            "parse_success_rate": round(parse_success_rate, 4),
            "total_runtime_s": round(total_runtime, 3),
            "worst_runtime_s": round(worst_runtime, 3),
            "avg_risk_score": round(avg_metrics.get("risk_score", 0.0), 3),
            "max_risk_score": round(max(item["risk_score"] for item in members_sorted), 3),
            "table_count": len(union_tables),
            "join_count": max(mi.join_count for mi in agg_intels),
            "predicate_count": max(mi.predicate_count for mi in agg_intels),
            "cte_count": max(mi.cte_count for mi in agg_intels),
            "wildcard_count": max(mi.wildcard_count for mi in agg_intels),
            "users": users,
            "databases": databases,
            "query_type": representative["query_type"],
            "repeat_kind": representative["repeat_kind"],
            "repeat_match_basis": representative["match_basis"],
            "repeat_constraint_key": representative["constraint_key"],
            "sql_length_min": min(item["sql_length"] for item in members_sorted),
            "sql_length_max": max(item["sql_length"] for item in members_sorted),
            "sql_length_avg": round(sum(item["sql_length"] for item in members_sorted) / len(members_sorted), 1),
            "query_ids": query_ids,
            "bridge_query_ids": query_ids,
            "bridge_query_count": len(members_sorted),
            "bridge_snapshot_ids": snapshot_ids,
            "example_query_ids": ", ".join(example_ids),
            "example_query_id_1": example_ids[0] if len(example_ids) > 0 else "",
            "example_query_id_2": example_ids[1] if len(example_ids) > 1 else "",
            "example_query_id_3": example_ids[2] if len(example_ids) > 2 else "",
            "representative_query_id": representative.get("query_id"),
            "representative_sql": _display_sql(representative["representative_sql"], 1200),
            "procedure_key": representative.get("procedure_key", ""),
            "procedure_definition": _display_sql(procedure_definition, 4000),
            "sql_shape": _truncate(representative["group_sql_shape"], 520),
            "ast_shape": _truncate(" ".join(intel.ast_node_shape), 520),
            "sample_sql": _display_sql(sample_sql, 4000),
            "sql_tables": _join_sorted(union_tables, limit=14),
            "sql_tables_full": ", ".join(union_tables),
            "sql_ctes": _join_sorted(_union_of("ctes"), limit=14),
            "sql_columns": _join_sorted(_union_of("columns"), limit=20),
            "sql_join_columns": _join_sorted(_union_of("join_columns"), limit=16),
            "sql_filter_columns": _join_sorted(_union_of("filter_columns"), limit=16),
            "sql_projected_columns": _join_sorted(_union_of("projected_columns"), limit=20),
            "sql_order_columns": _join_sorted(_union_of("order_columns"), limit=12),
            "sql_group_columns": _join_sorted(_union_of("group_columns"), limit=12),
            "sql_joins": _join_sorted(_union_of("joins"), limit=10),
            "sql_predicates": _join_sorted(_union_of("predicates"), limit=10),
            "sql_predicate_operators": _join_sorted(_union_of("predicate_operators"), limit=12),
            "predicate_operator_signature": _join_sorted(_union_of("predicate_operators"), limit=16),
            "shared_tables": _join_sorted(shared_tables, limit=10),
        }
        for col, value in avg_metrics.items():
            group_row[col] = round(value, 3)
            group_row[f"avg_{col}"] = round(value, 3)
        for source_col, total_col in _REPEAT_TOTAL_COLUMNS:
            if source_col in rows.columns:
                values = pd.to_numeric(rows[source_col], errors="coerce").dropna()
                group_row[total_col] = round(float(values.sum()), 3) if not values.empty else 0.0
        group_rows.append(group_row)

        for rank, item in enumerate(members_sorted, start=1):
            source = slow_queries.loc[item["frame_index"]]
            member_intel = member_intels[rank - 1]
            member_rows.append(
                {
                    "repeat_group_id": group_id,
                    "repeat_group_key": repeat_group_key,
                    "member_rank": rank,
                    "shown_in_tree": rank <= 10,
                    "query_id": item.get("query_id"),
                    "snapshot_id": source.get("snapshot_id"),
                    # Identical query ids exist on different clusters; the
                    # namespace keeps member identity unambiguous.
                    "namespace_id": source.get("namespace_id"),
                    "bridge_key": (
                        f"{source.get('snapshot_id') or ''}:"
                        f"{source.get('namespace_id') or ''}:{item.get('query_id') or ''}"
                    ),
                    "similarity_score": round(float(item.get("shape_score", 1.0)), 4),
                    "elapsed_s": item["elapsed_s"],
                    "risk_score": item["risk_score"],
                    "user_name": source.get("user_name"),
                    "database_name": source.get("database_name"),
                    "query_type": source.get("query_type"),
                    "start_time": source.get("start_time"),
                    "dominant_issue": source.get("dominant_issue"),
                    "repeat_kind": item["repeat_kind"],
                    "procedure_key": item.get("procedure_key", ""),
                    "constraint_key": item["constraint_key"],
                    "sql_length": item["sql_length"],
                    "sql_parse_status": "parsed" if member_intel.parse_ok else "fallback",
                    "sql_parse_error": member_intel.parse_error,
                    "sql_table_count": len(member_intel.tables),
                    "sql_join_count": member_intel.join_count,
                    "sql_predicate_count": member_intel.predicate_count,
                    "sql_cte_count": member_intel.cte_count,
                    "sql_wildcard_count": member_intel.wildcard_count,
                    "sql_tables": _join_sorted(member_intel.tables, limit=12),
                    "sql_tables_full": ", ".join(
                        sorted({str(t).strip() for t in member_intel.tables if str(t).strip()})
                    ),
                    "sql_columns": _join_sorted(member_intel.columns, limit=18),
                    "sql_join_columns": _join_sorted(member_intel.join_columns, limit=14),
                    "sql_filter_columns": _join_sorted(member_intel.filter_columns, limit=14),
                    "sql_projected_columns": _join_sorted(member_intel.projected_columns, limit=18),
                    "sql_order_columns": _join_sorted(member_intel.order_columns, limit=10),
                    "sql_group_columns": _join_sorted(member_intel.group_columns, limit=10),
                    "sql_joins": _join_sorted(member_intel.joins, limit=8),
                    "sql_predicates": _join_sorted(member_intel.predicates, limit=8),
                    "sql_predicate_operators": _join_sorted(member_intel.predicate_operators, limit=12),
                    "sql_shape": _truncate(item["group_sql_shape"], 520),
                    "sql_text": _display_sql(source.get("sql_text"), 1200),
                    "sql_text_full": _display_sql(source.get("sql_text"), 1000000),
                }
            )

    return pd.DataFrame(group_rows), pd.DataFrame(member_rows)


def _procedure_definition_map(procedure_definitions: pd.DataFrame | None) -> dict[str, str]:
    if procedure_definitions is None or procedure_definitions.empty:
        return {}
    key_col = "procedure_key" if "procedure_key" in procedure_definitions.columns else ""
    if not key_col and {"database", "schema", "procedure_name"}.issubset(procedure_definitions.columns):
        df = procedure_definitions.copy()
        df["procedure_key"] = df.apply(
            lambda row: ".".join(
                str(row.get(col) or "").strip().lower()
                for col in ("database", "schema", "procedure_name")
            ),
            axis=1,
        )
        key_col = "procedure_key"
    if not key_col or "source_definition" not in procedure_definitions.columns:
        return {}
    out: dict[str, str] = {}
    for _, row in procedure_definitions.iterrows():
        key = _collapse_sql(str(row.get(key_col) or "").strip().lower())
        source = _procedure_body_begin_to_end(row.get("source_definition"))
        if key and source and key not in out:
            out[key] = source
    return out


def _procedure_body_begin_to_end(value: object) -> str:
    text = _display_sql(value, 100000)
    if not text:
        return ""
    begin = re.search(r"\bbegin\b", text, flags=re.IGNORECASE)
    if not begin:
        return text.strip()
    end_matches = list(re.finditer(r"\bend\b\s*;?", text, flags=re.IGNORECASE))
    end_after_begin = [match for match in end_matches if match.start() >= begin.start()]
    if not end_after_begin:
        return text[begin.start():].strip()
    end = end_after_begin[-1]
    return text[begin.start():end.end()].strip()


_PROC_SQL_KEYWORDS = (
    "select",
    "insert",
    "update",
    "delete",
    "merge",
    "with",
    "create",
    "drop",
    "truncate",
    "unload",
    "copy",
    "analyze",
    "vacuum",
    "call",
)


def _split_procedure_statements(body: str) -> list[str]:
    """Split a BEGIN...END procedure body into top-level statements, honoring
    quotes, dollar-quoting, and comments, keeping only fragments that start
    with a plain-SQL keyword the analyzer can parse (procedural constructs
    like RAISE/IF/LOOP are skipped)."""
    text = _decode_escaped_whitespace(str(body or ""))
    text = re.sub(r"^\s*begin\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bend\s*;?\s*$", "", text, flags=re.IGNORECASE)
    statements: list[str] = []
    current: list[str] = []
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch == "'":
            j = i + 1
            while j < n:
                if text[j] == "'" and j + 1 < n and text[j + 1] == "'":
                    j += 2
                    continue
                if text[j] == "'":
                    break
                j += 1
            current.append(text[i : j + 1])
            i = j + 1
            continue
        if ch == '"':
            j = text.find('"', i + 1)
            j = n - 1 if j < 0 else j
            current.append(text[i : j + 1])
            i = j + 1
            continue
        if ch == "$":
            match = re.match(r"\$[A-Za-z_]*\$", text[i:])
            if match:
                tag = match.group(0)
                j = text.find(tag, i + len(tag))
                j = n - len(tag) if j < 0 else j
                current.append(text[i : j + len(tag)])
                i = j + len(tag)
                continue
        if text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j + 1
            continue
        if text.startswith("/*", i):
            j = text.find("*/", i)
            i = n if j < 0 else j + 2
            continue
        if ch == ";":
            statements.append("".join(current).strip())
            current = []
            i += 1
            continue
        current.append(ch)
        i += 1
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    kept: list[str] = []
    for statement in statements:
        first = re.match(r"[a-z_]+", statement.strip().lower())
        if first and first.group(0) in _PROC_SQL_KEYWORDS:
            kept.append(statement)
    return kept


def _procedure_statement_intels(body: str, *, limit: int = 100) -> list[SQLIntelligence]:
    """Analyze the extractable SQL statements inside a static procedure body."""
    return [analyze_sql(statement) for statement in _split_procedure_statements(body)[:limit]]


def _deterministic_repeat_candidates(
    slow_queries: pd.DataFrame,
    procedure_map: dict[str, str],
    *,
    scope_by_user: bool = False,
    progress: Callable[[str], None] | None = None,
) -> list[dict]:
    candidates: list[dict] = []
    # Queries dropped by per-query isolation below. Surfaced via progress so a
    # swallowed failure names the offending query_id instead of vanishing.
    skipped: list[str] = []
    name_index = _procedure_name_index(procedure_map)
    total_rows = len(slow_queries)
    # Sub-indicator cadence: frequent enough to feel live, not every row.
    report_every = max(1, min(50, total_rows // 20 or 1))
    scan_started = time.monotonic()
    for row_order, (idx, row) in enumerate(slow_queries.iterrows()):
        if progress is not None and (
            row_order == 0
            or (row_order + 1) % report_every == 0
            or row_order + 1 == total_rows
        ):
            done = row_order + 1
            eta = format_progress_eta(done, total_rows, scan_started)
            progress(
                f"Grouping repeated query patterns - scanning "
                f"{done:,} of {total_rows:,} query(s){eta}"
            )
        # Per-query isolation. Without this, one statement whose AST trips a
        # sqlglot edge case aborts _deterministic_repeat_candidates entirely, so
        # the caller reports "Repeat Grouping: <error>" and the user sees zero
        # groups for the whole workload instead of losing a single query.
        try:
            raw_sql = _sql_to_text(row.get("sql_text"))
            normalized = _deterministic_sql_text(raw_sql)
            call_key = _canonical_call_key(raw_sql, row.get("database_name"))
        except Exception as exc:
            skipped.append(f"{row.get('query_id')}: {type(exc).__name__}: {str(exc)[:120]}")
            continue
        if call_key:
            call_key = _resolve_call_key(call_key, procedure_map, name_index)
        elif len(normalized) < MIN_SQL_CHARS:
            continue
        user_key = _repeat_user_key(row)
        scope_key = user_key if scope_by_user else "*"
        scope_basis = "same user + " if scope_by_user else ""
        query_type = _normalize_query_type(row.get("query_type"))
        if call_key:
            constraint_key = f"proc|{scope_key}|{query_type}|{call_key}"
            group_sql_shape = f"call {call_key}"
            representative_sql = group_sql_shape
            repeat_kind = "stored_procedure"
            fingerprint_method = "call"
            match_basis = f"{scope_basis}same query_type + same canonical db.schema.procedure; CALL parameters stripped"
            sql_length = len(group_sql_shape)
        else:
            try:
                group_sql_shape, fingerprint_method = canonical_sql_fingerprint(raw_sql)
            except Exception as exc:
                # canonical_sql_fingerprint already falls back to regex on parse
                # failure; reaching here means something deeper went wrong. Lose
                # the one query, keep the workload.
                skipped.append(f"{row.get('query_id')}: {type(exc).__name__}: {str(exc)[:120]}")
                continue
            if len(group_sql_shape) < MIN_SQL_CHARS:
                continue
            sql_length = len(normalized)
            shape_hash = hashlib.sha1(group_sql_shape.encode("utf-8")).hexdigest()[:20]
            constraint_key = f"sql|{scope_key}|{query_type}|{shape_hash}"
            representative_sql = raw_sql
            repeat_kind = "sql_text"
            if fingerprint_method == "ast":
                match_basis = (
                    f"{scope_basis}same query_type + identical canonical SQL structure "
                    "(sqlglot AST; literals, IN-lists, and VALUES rows normalized to placeholders)"
                )
            else:
                match_basis = (
                    f"{scope_basis}same query_type + identical normalized SQL text "
                    "(regex fallback; string/numeric literals and IN-lists stripped)"
                )
        candidates.append(
            {
                "frame_index": idx,
                "row_order": row_order,
                "query_id": row.get("query_id"),
                "user_key": user_key,
                "query_type": query_type,
                "constraint_key": constraint_key,
                "repeat_kind": repeat_kind,
                "fingerprint_method": fingerprint_method,
                "match_basis": match_basis,
                "shape_score": 1.0,
                "sql_length": sql_length,
                "elapsed_s": _to_float(row.get("elapsed_s")),
                "risk_score": _to_float(row.get("risk_score")),
                "group_sql_shape": group_sql_shape,
                "representative_sql": representative_sql,
                "procedure_key": call_key,
            }
        )
    if skipped and progress is not None:
        progress(
            f"Grouping repeated query patterns - skipped {len(skipped):,} "
            f"unanalyzable query(s): {'; '.join(skipped[:3])}"
            + (f" (+{len(skipped) - 3:,} more)" if len(skipped) > 3 else "")
        )
    return candidates


def _procedure_name_index(procedure_map: dict[str, str]) -> dict[tuple[str, str], list[str]]:
    """Index captured procedure keys by (database, procedure_name)."""
    index: dict[tuple[str, str], list[str]] = {}
    for full_key in procedure_map:
        parts = full_key.split(".")
        if len(parts) != 3:
            continue
        index.setdefault((parts[0], parts[2]), []).append(full_key)
    return index


def _resolve_call_key(
    call_key: str,
    procedure_map: dict[str, str],
    name_index: dict[tuple[str, str], list[str]],
) -> str:
    """Map an unqualified CALL key ('db..proc') onto its captured
    'db.schema.proc' key when exactly one captured procedure has that name in
    that database, so search_path-resolved calls share a group with qualified
    calls and pick up the captured body."""
    if call_key in procedure_map:
        return call_key
    parts = call_key.split(".")
    if len(parts) != 3 or parts[1]:
        return call_key
    matches = name_index.get((parts[0], parts[2]), [])
    if len(matches) == 1:
        return matches[0]
    return call_key


def _deterministic_repeat_groups(
    candidates: list[dict],
    *,
    min_group_size: int = MIN_REPEAT_GROUP_SIZE,
    fuzzy_merge_threshold: float = DEFAULT_FUZZY_MERGE_THRESHOLD,
) -> list[list[dict]]:
    buckets: dict[str, list[dict]] = {}
    for item in candidates:
        buckets.setdefault(item["constraint_key"], []).append(item)
    merged = _fuzzy_merge_shape_buckets(list(buckets.values()), fuzzy_merge_threshold)
    return [bucket for bucket in merged if len(bucket) >= max(2, int(min_group_size))]


def _fuzzy_merge_shape_buckets(
    buckets: list[list[dict]],
    threshold: float,
) -> list[list[dict]]:
    """Union near-identical SQL shapes (e.g. one extra projected column) into one
    family. Only sql_text buckets with the same query_type and same referenced
    table set are compared, so unrelated statements can never merge."""
    if threshold >= 1.0 or len(buckets) < 2:
        return buckets
    blocks: dict[tuple, list[int]] = {}
    for index, bucket in enumerate(buckets):
        seed = bucket[0]
        if seed["repeat_kind"] != "sql_text":
            continue
        tables = _shape_table_set(seed["group_sql_shape"])
        if not tables:
            # Unknown table set: never fuzzy-merge, or unrelated statements
            # whose tables the extractor cannot read would collapse together.
            continue
        block_key = (seed["constraint_key"].split("|")[1], seed["query_type"], tables)
        blocks.setdefault(block_key, []).append(index)

    merged_groups: list[tuple[list[int], float]] = []
    for indices in blocks.values():
        if len(indices) < 2:
            continue
        ordered = sorted(indices, key=lambda i: len(buckets[i][0]["group_sql_shape"]))
        clusters: list[list[int]] = []
        pair_scores: dict[tuple[int, int], float] = {}
        pairs = 0
        for index in ordered:
            placed = False
            for cluster in clusters:
                scores: list[float] = []
                compatible = True
                for member in cluster:
                    key = tuple(sorted((index, member)))
                    score = pair_scores.get(key)
                    if score is None:
                        left = buckets[member][0]["group_sql_shape"]
                        right = buckets[index][0]["group_sql_shape"]
                        shorter, longer = sorted((left, right), key=len)
                        if len(longer) > len(shorter) * (1.0 + _FUZZY_LENGTH_WINDOW):
                            score = 0.0
                        else:
                            pairs += 1
                            if pairs > _FUZZY_MAX_PAIRS_PER_BLOCK:
                                compatible = False
                                break
                            score = _text_ratio(left, right)
                        pair_scores[key] = score
                    scores.append(score)
                    if score < threshold:
                        compatible = False
                        break
                if compatible and scores:
                    cluster.append(index)
                    placed = True
                    break
            if not placed:
                clusters.append([index])
        for cluster in clusters:
            if len(cluster) < 2:
                continue
            scores = [
                pair_scores[tuple(sorted((left, right)))]
                for position, left in enumerate(cluster)
                for right in cluster[position + 1:]
            ]
            merged_groups.append((cluster, min(scores) if scores else 1.0))

    if not merged_groups:
        return buckets
    merged_indices = {index for group, _score in merged_groups for index in group}
    result = [bucket for index, bucket in enumerate(buckets) if index not in merged_indices]
    for group, score in merged_groups:
        bucket: list[dict] = []
        for index in group:
            bucket.extend(buckets[index])
        for item in bucket:
            item["shape_score"] = min(item.get("shape_score", 1.0), score)
            item["match_basis"] = (
                f"{item['match_basis']}; near-identical shapes fuzzy-merged at >= {threshold:.0%} "
                "complete-link similarity with identical table set"
            )
        result.append(bucket)
    return result


def _shape_table_set(shape_sql: str) -> frozenset[str]:
    """Referenced-table set used to gate fuzzy merging. Prefers the sqlglot AST
    so comma joins, USING clauses, and quoted identifiers are read correctly;
    the regex is only a fallback for unparseable shapes."""
    intel = analyze_sql(shape_sql)
    if intel.parse_ok:
        return frozenset(str(t).strip().lower() for t in intel.tables if str(t).strip())
    return frozenset(_TABLE_REF_RE.findall(shape_sql))


def _deterministic_sql_text(sql: object) -> str:
    text = _decode_escaped_whitespace(_sql_to_text(sql)).lower()
    return _collapse_sql(text)


_UNLOAD_INNER_RE = re.compile(
    r"^\s*unload\s*\(\s*'((?:''|[^'])*)'\s*\)", re.IGNORECASE | re.DOTALL
)
_INSERT_VALUES_TAIL_RE = re.compile(r"(values\s*\(\?\))(\s*,\s*\(\?\))+", re.IGNORECASE)


def canonical_sql_fingerprint(sql: object) -> tuple[str, str]:
    """Return (shape, method) for grouping: SQL with every literal, IN-list, and
    VALUES row collapsed to placeholders so predicate/filter values never split a
    repeat family. method is "ast" (sqlglot parse) or "regex" (fallback)."""
    text = _strip_leading_comments_and_space(_decode_escaped_whitespace(_sql_to_text(sql)))
    if not text:
        return "", "empty"
    prefix = ""
    unload = _UNLOAD_INNER_RE.match(text)
    if unload:
        prefix = "unload: "
        text = unload.group(1).replace("''", "'").strip()
    try:
        shape, method = _canonical_shape_cached(text)
    except Exception:
        # Same contract as analyze_sql: a statement that cannot be canonicalized
        # still gets a usable regex shape rather than aborting the pass.
        shape = _regex_normalize_sql(text)
        shape = _INSERT_VALUES_TAIL_RE.sub(r"", shape)
        method = "regex-error"
    return (prefix + shape if shape else ""), method


# Above this size the sqlglot parse + transform + re-render round trip costs
# more than the extra grouping precision is worth. Measured on the real corp
# workload: individual queries were taking ~1.5s each, and at 15,000 queries
# that is over six hours - the pass never finished, so the triage table stayed
# empty and no bubbles rendered.
#
# The regex fallback below already exists for unparseable SQL and produces a
# usable shape in microseconds. Routing oversized statements to it trades a
# little fidelity on a handful of monster queries for a pass that completes.
_MAX_AST_FINGERPRINT_CHARS = 8_000

# Size alone is not a reliable proxy. Field measurements showed real 16,000-char
# statements costing 0.62s while synthetic ones of the same length cost 0.08s -
# an 8x gap driven by structural complexity, not length. So once a statement has
# proven expensive, remember it and take the regex path on every later
# encounter. The lru_cache above only helps for byte-identical text; this
# catches the near-duplicates a workload is full of.
_SLOW_SHAPE_BUDGET = 0.30
_SLOW_SHAPE_KEYS: set[tuple[int, str]] = set()


def _shape_budget_key(text: str) -> tuple[int, str]:
    """Cheap fingerprint of a statement's cost profile: length plus a prefix.

    Two runs of the same report differ only in literals buried deep in the text,
    so length and opening clause identify them without hashing 268,000
    characters on every call.
    """
    return (len(text) // 1024, text[:160])

# A statement taking longer than this to canonicalize is logged. At 15,000
# queries a 1.5s average is over six hours, so the count and total below are
# what turn "the load hangs" into a number the user can act on.
_SLOW_PARSE_SECONDS = 0.5
_SLOW_PARSE_COUNTER: dict[str, float] = {"count": 0, "seconds": 0.0}


def slow_parse_summary() -> str:
    """One line describing expensive parses seen so far, or empty if none."""
    count = int(_SLOW_PARSE_COUNTER["count"])
    if not count:
        return ""
    seconds = float(_SLOW_PARSE_COUNTER["seconds"])
    return (
        f"{count:,} query(s) took over {_SLOW_PARSE_SECONDS:g}s to canonicalize "
        f"({seconds:,.0f}s total). Statements over "
        f"{_MAX_AST_FINGERPRINT_CHARS:,} characters use the faster regex shape."
    )


@lru_cache(maxsize=8192)
def _canonical_shape_cached(text: str) -> tuple[str, str]:
    budget_key = _shape_budget_key(text)
    if len(text) > _MAX_AST_FINGERPRINT_CHARS or budget_key in _SLOW_SHAPE_KEYS:
        shape = _regex_normalize_sql(text)
        shape = _INSERT_VALUES_TAIL_RE.sub(r"", shape)
        return shape, "regex-oversize"
    started = time.perf_counter()
    try:
        statements = sqlglot.parse(text, dialect="redshift")
        rendered_parts: list[str] = []
        for tree in statements:
            if tree is None:
                continue
            canon = tree.transform(_canonicalize_shape_node)
            rendered_parts.append(canon.sql(dialect="redshift", comments=False, normalize=True))
        if not rendered_parts:
            raise ValueError("empty parse result")
        rendered = " ; ".join(rendered_parts)
        elapsed = time.perf_counter() - started
        if elapsed > _SLOW_SHAPE_BUDGET:
            # Pay the full cost once; every similar statement afterwards takes
            # the regex path instead.
            _SLOW_SHAPE_KEYS.add(budget_key)
        if elapsed > _SLOW_PARSE_SECONDS:
            _SLOW_PARSE_COUNTER["count"] += 1
            _SLOW_PARSE_COUNTER["seconds"] += elapsed
        return _collapse_sql(rendered.lower()), "ast"
    except Exception:
        shape = _regex_normalize_sql(text)
        shape = _INSERT_VALUES_TAIL_RE.sub(r"\1", shape)
        return shape, "regex"


def _canonicalize_shape_node(node: exp.Expression) -> exp.Expression:
    # transform() visits children before parents, so by the time an In/Values
    # node is seen its literal children may already be Placeholder nodes. A
    # Placeholder is a bare expression: it has no len(), cannot be sliced, and
    # cannot be iterated. Guard the container shape before touching it, or a
    # single query raises TypeError/ValueError and kills the whole grouping pass.
    if isinstance(node, exp.In):
        exprs = _as_node_list(node.args.get("expressions"))
        if len(exprs) > 1:
            node.set("expressions", [exp.Placeholder()])
    elif isinstance(node, exp.Values):
        rows = _as_node_list(node.args.get("expressions"))
        if len(rows) > 1:
            node.set("expressions", rows[:1])
    if isinstance(node, exp.Literal):
        return exp.Placeholder()
    return node


def _as_node_list(value: object) -> list:
    """Coerce a sqlglot arg slot to a real list.

    An arg that should hold a list of expressions can legitimately hold a single
    bare expression, or (after literal canonicalization) a Placeholder. Callers
    need len()/slicing to be safe, so normalize to a list here rather than at
    every use site.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _canonical_call_key(sql: object, database_name: object) -> str:
    text = _strip_leading_comments_and_space(_decode_escaped_whitespace(_sql_to_text(sql))).lower()
    match = re.match(
        r"call\s+((?:\"[^\"]+\"|[a-z_][a-z0-9_$]*)(?:\s*\.\s*(?:\"[^\"]+\"|[a-z_][a-z0-9_$]*)){0,2})\s*\(",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ""
    parts = [
        _clean_call_identifier(part)
        for part in re.split(r"\s*\.\s*", match.group(1))
        if _clean_call_identifier(part)
    ]
    if not parts:
        return ""
    database = _normalize_user_identity(database_name)
    if len(parts) == 1:
        parts = [database, "", parts[0]]
    elif len(parts) == 2:
        parts = [database, parts[0], parts[1]]
    else:
        parts = parts[-3:]
    return ".".join(parts).lower()


def _strip_leading_comments_and_space(text: str) -> str:
    pos = 0
    length = len(text)
    while pos < length:
        whitespace = re.match(r"\s+", text[pos:])
        if whitespace:
            pos += whitespace.end()
            continue
        if text.startswith("--", pos):
            end = text.find("\n", pos + 2)
            pos = length if end < 0 else end + 1
            continue
        if text.startswith("/*", pos):
            end = text.find("*/", pos + 2)
            pos = length if end < 0 else end + 2
            continue
        break
    return text[pos:]


def _clean_call_identifier(value: object) -> str:
    text = str(value or "").strip().strip('"').strip()
    return re.sub(r"\s+", "", text).lower()


def _normalize_query_type(value: object) -> str:
    text = _normalize_user_identity(value)
    return text or "unknown"


def _average_metric_values(rows: pd.DataFrame) -> dict[str, float]:
    out: dict[str, float] = {}
    for col in _REPEAT_AVERAGE_COLUMNS:
        if col not in rows.columns:
            continue
        values = pd.to_numeric(rows[col], errors="coerce").dropna()
        if values.empty:
            continue
        out[col] = float(values.mean())
    return out


def _ordered_unique_text(values: pd.Series | None, *, limit: int) -> list[str]:
    if values is None:
        return []
    out: list[str] = []
    for value in values:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            continue
        text = str(value).strip()
        if not text or text in out:
            continue
        out.append(text)
        if len(out) >= limit:
            break
    return out


def diagnose_repeat_query_candidates(
    slow_queries: pd.DataFrame,
    *,
    threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    prefilter_threshold: float = DEFAULT_PREFILTER_THRESHOLD,
    procedure_definitions: pd.DataFrame | None = None,
    scope_by_user: bool = False,
    min_group_size: int = MIN_REPEAT_GROUP_SIZE,
    fuzzy_merge_threshold: float = DEFAULT_FUZZY_MERGE_THRESHOLD,
    progress: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """Return counters that explain deterministic repeat-query grouping.

    The grouping parameters must match the ones passed to
    build_repeat_query_report, or the diagnostic note will describe a
    different grouping than the one shown. Always pass the same
    procedure_definitions frame used for the displayed groups.
    """
    _ = threshold
    _ = prefilter_threshold
    raw_rows = 0 if slow_queries is None else len(slow_queries)
    if slow_queries is None or slow_queries.empty or "sql_text" not in slow_queries.columns:
        return {
            "repeat_raw_query_rows": raw_rows,
            "repeat_sql_text_rows": 0,
            "repeat_prepared_query_count": 0,
            "repeat_diagnostic_note": "Repeat analysis could not run because the loaded slow-query rows do not include SQL text.",
        }

    sql_text_rows = int(
        slow_queries["sql_text"].map(lambda value: len(_deterministic_sql_text(value)) >= MIN_SQL_CHARS).sum()
    )
    procedure_map = _procedure_definition_map(procedure_definitions)
    if progress is not None:
        progress("Diagnosing repeat grouping coverage - re-scanning candidates")
    candidates = _deterministic_repeat_candidates(
        slow_queries, procedure_map, scope_by_user=scope_by_user, progress=progress
    )
    # Parse success is derived from the fingerprint method already recorded per
    # candidate rather than by calling analyze_sql on every row again. The old
    # form re-parsed the entire workload a SECOND time - on a 13,000-query
    # capture that repeated a 25-minute pass with no progress on screen, so the
    # app looked frozen right after grouping had succeeded.
    parsed_rows = sum(1 for item in candidates if item.get("fingerprint_method") == "ast")
    user_buckets = _bucket_dict(candidates, lambda item: item["user_key"])
    same_user_buckets = [bucket for bucket in user_buckets.values() if len(bucket) >= 2]
    same_user_type_buckets = [
        bucket
        for bucket in _bucket_dict(candidates, lambda item: (item["user_key"], item["query_type"])).values()
        if len(bucket) >= 2
    ]
    strict_buckets = [bucket for bucket in _bucket_dict(candidates, lambda item: item["constraint_key"]).values() if len(bucket) >= 2]
    repeat_groups = _deterministic_repeat_groups(
        candidates,
        min_group_size=min_group_size,
        fuzzy_merge_threshold=fuzzy_merge_threshold,
    )
    best_size = max((len(group) for group in strict_buckets), default=0)
    best_ids = ""
    if strict_buckets:
        best_bucket = max(strict_buckets, key=len)
        best_rows = slow_queries.loc[[item["frame_index"] for item in best_bucket]]
        best_ids = _join_values(best_rows.get("query_id"), limit=3)
    diagnostics: dict[str, object] = {
        "repeat_raw_query_rows": raw_rows,
        "repeat_sql_text_rows": sql_text_rows,
        "repeat_prepared_query_count": len(candidates),
        "repeat_parse_success_count": parsed_rows,
        "repeat_same_user_bucket_count": len(same_user_buckets),
        "repeat_same_user_table_bucket_count": len(same_user_type_buckets),
        "repeat_strict_family_bucket_count": len(strict_buckets),
        "repeat_strict_candidate_pairs": sum(_candidate_pair_count_for_size(len(bucket)) for bucket in strict_buckets),
        "repeat_pairs_scored": 0,
        "repeat_best_strict_similarity": 1.0 if repeat_groups else 0.0,
        "repeat_best_same_user_table_similarity": 1.0 if same_user_type_buckets else 0.0,
        "repeat_best_same_user_table_query_ids": best_ids,
        "repeat_diagnostic_capped": False,
        "repeat_deterministic_group_count": len(repeat_groups),
        "repeat_largest_bucket_size": best_size,
        "repeat_min_group_size": max(2, int(min_group_size)),
    }
    diagnostics["repeat_diagnostic_note"] = _deterministic_repeat_diagnostic_note(diagnostics)
    return diagnostics


def _bucket_dict(items: list[dict], key_fn) -> dict[object, list[dict]]:
    buckets: dict[object, list[dict]] = {}
    for item in items:
        key = key_fn(item)
        if key in {"", None}:
            continue
        buckets.setdefault(key, []).append(item)
    return buckets


def _candidate_pair_count_for_size(size: int) -> int:
    return (size * (size - 1)) // 2 if size >= 2 else 0


def _deterministic_repeat_diagnostic_note(diagnostics: dict[str, object]) -> str:
    raw_rows = int(diagnostics.get("repeat_raw_query_rows") or 0)
    sql_rows = int(diagnostics.get("repeat_sql_text_rows") or 0)
    prepared = int(diagnostics.get("repeat_prepared_query_count") or 0)
    same_user = int(diagnostics.get("repeat_same_user_bucket_count") or 0)
    same_type = int(diagnostics.get("repeat_same_user_table_bucket_count") or 0)
    strict = int(diagnostics.get("repeat_strict_family_bucket_count") or 0)
    groups = int(diagnostics.get("repeat_deterministic_group_count") or 0)
    largest = int(diagnostics.get("repeat_largest_bucket_size") or 0)
    best_ids = str(diagnostics.get("repeat_best_same_user_table_query_ids") or "").strip()
    min_size = int(diagnostics.get("repeat_min_group_size") or MIN_REPEAT_GROUP_SIZE)
    if raw_rows < min_size:
        return f"Repeat analysis needs at least {min_size} loaded slow-query rows."
    if sql_rows < min_size:
        return f"Only {sql_rows:,} loaded row(s) have usable SQL text; repeat detection needs captured query_text rows."
    if prepared < min_size:
        return f"{sql_rows:,} rows have SQL text, but fewer than {min_size} passed deterministic parser gates."
    if same_user == 0 and same_type == 0:
        return "No two analyzable SQL statements share the same query_type; repeat grouping needs at least one recurring statement type."
    if same_type == 0:
        return "Candidates exist, but none share the same query_type."
    if strict == 0:
        return "Same-user/query_type candidates exist, but none share the exact repeat fingerprint."
    if groups == 0:
        suffix = f" Example query IDs: {best_ids}." if best_ids else ""
        return (
            f"{strict:,} deterministic fingerprint bucket(s) exist, but the largest has {largest:,} run(s); "
            f"minimum repeat group size is {min_size}.{suffix}"
        )
    return (
        f"{groups:,} repeat group(s) found using same query_type + canonical literal-free SQL fingerprint "
        "(sqlglot AST with regex fallback), plus guarded fuzzy merge of near-identical shapes."
    )


@lru_cache(maxsize=4096)
def _analyze_sql_text(text: str) -> SQLIntelligence:
    clean = _decode_escaped_whitespace(text).strip()
    if not clean:
        return _fallback_intelligence("")
    if len(clean) > _MAX_AST_FINGERPRINT_CHARS:
        # Same reasoning as _canonical_shape_cached: a full parse plus feature
        # extraction on a very large statement dominates the whole pass. The
        # regex fallback yields the same descriptive fields at a fraction of
        # the cost, and these outsized statements are a small minority.
        return _fallback_intelligence(clean)
    try:
        tree = sqlglot.parse_one(clean, read="redshift")
    except Exception as exc:
        fallback = _fallback_intelligence(clean)
        return SQLIntelligence(
            normalized_sql=fallback.normalized_sql,
            ast_node_shape=fallback.ast_node_shape,
            parse_ok=False,
            parse_error=str(exc).splitlines()[0][:240],
            tables=fallback.tables,
            columns=fallback.columns,
            joins=fallback.joins,
            predicates=fallback.predicates,
            predicate_operators=fallback.predicate_operators,
            ctes=fallback.ctes,
            join_columns=fallback.join_columns,
            filter_columns=fallback.filter_columns,
            projected_columns=fallback.projected_columns,
            order_columns=fallback.order_columns,
            group_columns=fallback.group_columns,
            projection_count=fallback.projection_count,
            wildcard_count=fallback.wildcard_count,
            join_count=fallback.join_count,
            predicate_count=fallback.predicate_count,
            cte_count=fallback.cte_count,
            subquery_count=fallback.subquery_count,
            aggregate_count=fallback.aggregate_count,
            function_count=fallback.function_count,
        )

    canonical = tree.copy().transform(_replace_literals)
    normalized = _collapse_sql(canonical.sql(dialect="redshift", pretty=False).lower())
    tables = frozenset(_table_name(table) for table in canonical.find_all(exp.Table))
    columns = frozenset(_collapse_sql(col.sql(dialect="redshift").lower()) for col in canonical.find_all(exp.Column))
    joins = frozenset(_join_signature(join) for join in canonical.find_all(exp.Join))
    predicates = frozenset(_predicate_signature(node) for node in canonical.find_all(*_predicate_types()))
    predicate_operators = frozenset(
        _predicate_operator_signature(node)
        for node in canonical.find_all(*_predicate_types())
    )
    ctes = frozenset(_clean_identifier(cte.alias_or_name) for cte in canonical.find_all(exp.CTE) if cte.alias_or_name)
    selects = list(canonical.find_all(exp.Select))
    join_columns = _join_columns(canonical)
    filter_columns = _filter_columns(canonical)
    projected_columns = _projected_columns(selects)
    order_columns = _clause_columns(canonical, exp.Order)
    group_columns = _clause_columns(canonical, exp.Group)
    projection_count = sum(
        len(_as_node_list(select.args.get("expressions"))) for select in selects
    )
    wildcard_count = sum(1 for _ in canonical.find_all(exp.Star))
    functions = list(canonical.find_all(exp.Func))
    aggregates = list(canonical.find_all(exp.AggFunc))
    ast_node_shape = tuple(type(node).__name__ for node in canonical.walk())

    return SQLIntelligence(
        normalized_sql=normalized,
        ast_node_shape=ast_node_shape,
        parse_ok=True,
        parse_error="",
        tables=frozenset(item for item in tables if item),
        columns=frozenset(item for item in columns if item),
        joins=frozenset(item for item in joins if item),
        predicates=frozenset(item for item in predicates if item),
        predicate_operators=frozenset(item for item in predicate_operators if item),
        ctes=ctes,
        join_columns=join_columns,
        filter_columns=filter_columns,
        projected_columns=projected_columns,
        order_columns=order_columns,
        group_columns=group_columns,
        projection_count=projection_count,
        wildcard_count=wildcard_count,
        join_count=sum(1 for _ in canonical.find_all(exp.Join)),
        predicate_count=sum(1 for _ in canonical.find_all(*_predicate_types())),
        cte_count=len(ctes),
        subquery_count=sum(1 for _ in canonical.find_all(exp.Subquery)),
        aggregate_count=len(aggregates),
        function_count=len(functions),
    )


def _fallback_intelligence(text: str) -> SQLIntelligence:
    normalized = _regex_normalize_sql(text)
    tokens = tuple(_TOKEN_RE.findall(normalized))
    tables = frozenset(_TABLE_REF_RE.findall(normalized))
    return SQLIntelligence(
        normalized_sql=normalized,
        ast_node_shape=tokens,
        parse_ok=False,
        parse_error="",
        tables=tables,
        columns=frozenset(),
        joins=frozenset(),
        predicates=frozenset(),
        predicate_operators=_regex_predicate_operators(normalized),
        ctes=frozenset(),
        join_columns=frozenset(),
        filter_columns=frozenset(),
        projected_columns=frozenset(),
        order_columns=frozenset(),
        group_columns=frozenset(),
        projection_count=0,
        wildcard_count=normalized.count("*"),
        join_count=normalized.count(" join "),
        predicate_count=1 if " where " in f" {normalized} " else 0,
        cte_count=1 if normalized.startswith("with ") else 0,
        subquery_count=normalized.count("(select "),
        aggregate_count=sum(normalized.count(f"{name}(") for name in ("sum", "count", "avg", "min", "max")),
        function_count=normalized.count("("),
    )


def _replace_literals(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Literal):
        return exp.Placeholder()
    return node


def _regex_normalize_sql(sql: object) -> str:
    if sql is None or (isinstance(sql, float) and math.isnan(sql)):
        return ""
    text = _decode_escaped_whitespace(str(sql))
    text = _BLOCK_COMMENT_RE.sub(" ", text)
    text = _LINE_COMMENT_RE.sub(" ", text)
    text = _DOLLAR_QUOTED_RE.sub("?", text)
    text = _SINGLE_QUOTED_RE.sub("?", text)
    text = text.lower()
    # Run-suffix normalization must precede literal stripping: once
    # _NUMERIC_LITERAL_RE has rewritten digits, the identifier pattern no
    # longer matches.
    text = _RUN_SUFFIX_RE.sub(_run_suffix_replacement, text)
    text = _NUMERIC_LITERAL_RE.sub("?", text)
    text = _QUESTION_LIST_RE.sub("(?)", text)
    return _collapse_sql(text)


def _prepare_sql(sql: object) -> _PreparedQuery:
    intelligence = analyze_sql(sql)
    tokens = tuple(_TOKEN_RE.findall(intelligence.normalized_sql))
    return _PreparedQuery(
        frame_index=-1,
        query_id=None,
        user_key="",
        repeat_structure_key=_repeat_structure_key(intelligence),
        intelligence=intelligence,
        tokens=tokens,
        token_set=frozenset(tokens),
        token_bigrams=frozenset(zip(tokens, tokens[1:])),
        elapsed_s=0.0,
        risk_score=0.0,
    )


def _prepare_queries(slow_queries: pd.DataFrame) -> list[_PreparedQuery]:
    prepared: list[_PreparedQuery] = []
    for idx, row in slow_queries.iterrows():
        intelligence = analyze_sql(row.get("sql_text"))
        if len(intelligence.normalized_sql) < MIN_SQL_CHARS:
            continue
        tokens = tuple(_TOKEN_RE.findall(intelligence.normalized_sql))
        if len(tokens) < 6:
            continue
        prepared.append(
            _PreparedQuery(
                frame_index=idx,
                query_id=row.get("query_id"),
                user_key=_repeat_user_key(row),
                repeat_structure_key=_repeat_structure_key(intelligence),
                intelligence=intelligence,
                tokens=tokens,
                token_set=frozenset(tokens),
                token_bigrams=frozenset(zip(tokens, tokens[1:])),
                elapsed_s=_to_float(row.get("elapsed_s")),
                risk_score=_to_float(row.get("risk_score")),
            )
        )
    return prepared


def _queries_by_repeat_family(prepared: list[_PreparedQuery]) -> dict[tuple[str, str], list[_PreparedQuery]]:
    buckets: dict[tuple[str, str], list[_PreparedQuery]] = {}
    for item in prepared:
        if not item.repeat_structure_key:
            continue
        buckets.setdefault((item.user_key, item.repeat_structure_key), []).append(item)
    return buckets


def _bucket_prepared(prepared: list[_PreparedQuery], key_fn) -> dict[object, list[_PreparedQuery]]:
    buckets: dict[object, list[_PreparedQuery]] = {}
    for item in prepared:
        key = key_fn(item)
        if key in {"", None}:
            continue
        buckets.setdefault(key, []).append(item)
    return buckets


def _table_key(item: _PreparedQuery) -> str:
    return "|".join(sorted(item.tables)) if item.tables else ""


def _candidate_pair_count(bucket: list[_PreparedQuery]) -> int:
    size = len(bucket)
    return (size * (size - 1)) // 2 if size >= 2 else 0


def _best_similarity_for_buckets(
    buckets: list[list[_PreparedQuery]],
    *,
    prefilter_threshold: float,
    max_pairs: int = 25_000,
) -> dict[str, object]:
    best_similarity = 0.0
    best_pair: tuple[_PreparedQuery, _PreparedQuery] | None = None
    pair_count = 0
    pairs_scored = 0
    capped = False
    for bucket in sorted(buckets, key=len, reverse=True):
        for left_pos, left in enumerate(bucket):
            for right in bucket[left_pos + 1 :]:
                pair_count += 1
                if pair_count > max_pairs:
                    capped = True
                    return {
                        "best_similarity": round(best_similarity, 4),
                        "best_query_ids": _diagnostic_pair_ids(best_pair),
                        "pairs_scored": pairs_scored,
                        "capped": capped,
                    }
                cheap_score = _cheap_overlap_score(left, right)
                if cheap_score < prefilter_threshold:
                    continue
                pairs_scored += 1
                score = _score_prepared(left, right)
                if score > best_similarity:
                    best_similarity = score
                    best_pair = (left, right)
    return {
        "best_similarity": round(best_similarity, 4),
        "best_query_ids": _diagnostic_pair_ids(best_pair),
        "pairs_scored": pairs_scored,
        "capped": capped,
    }


def _diagnostic_pair_ids(pair: tuple[_PreparedQuery, _PreparedQuery] | None) -> str:
    if pair is None:
        return ""
    left, right = pair
    return f"{left.query_id}, {right.query_id}"


def _repeat_diagnostic_note(
    diagnostics: dict[str, object],
    *,
    threshold: float,
    prefilter_threshold: float,
) -> str:
    raw_rows = int(diagnostics.get("repeat_raw_query_rows") or 0)
    sql_rows = int(diagnostics.get("repeat_sql_text_rows") or 0)
    prepared = int(diagnostics.get("repeat_prepared_query_count") or 0)
    same_user = int(diagnostics.get("repeat_same_user_bucket_count") or 0)
    same_user_table = int(diagnostics.get("repeat_same_user_table_bucket_count") or 0)
    strict = int(diagnostics.get("repeat_strict_family_bucket_count") or 0)
    best_strict = float(diagnostics.get("repeat_best_strict_similarity") or 0.0)
    best_table = float(diagnostics.get("repeat_best_same_user_table_similarity") or 0.0)
    best_pair = str(diagnostics.get("repeat_best_same_user_table_query_ids") or "").strip()
    if raw_rows < 2:
        return "Repeat analysis needs at least two loaded slow-query rows."
    if sql_rows < 2:
        return f"Only {sql_rows:,} loaded row(s) have usable SQL text; repeat detection needs captured query_text rows."
    if prepared < 2:
        return f"{sql_rows:,} rows have SQL text, but fewer than two passed the minimum SQL-shape parser gates."
    if same_user == 0:
        return "No loaded user has two or more analyzable SQL statements; repeats are intentionally same-user only."
    if same_user_table == 0:
        return "Same-user candidates exist, but none touch the exact same parsed table set."
    if strict == 0:
        note = (
            f"Same-user/table candidates exist, but none share the same predicate/operator shape. "
            f"Best same-user/table near miss scored {best_table * 100:.0f}%"
        )
        return f"{note} for query IDs {best_pair}." if best_pair else f"{note}."
    if best_strict < threshold:
        return (
            f"{strict:,} strict same-user/table/operator bucket(s) exist, but best similarity "
            f"is {best_strict * 100:.0f}% below the match threshold {threshold * 100:.0f}%. "
            f"Prefilter is {prefilter_threshold * 100:.0f}%."
        )
    return (
        f"Repeat candidates exist with best strict similarity {best_strict * 100:.0f}%; "
        "if the grid is still empty, inspect the matched-member merge and loaded query IDs."
    )


def _repeat_user_key(row: pd.Series) -> str:
    for column in ("user_name", "usename", "user_id", "userid", "usesysid"):
        if column in row:
            key = _normalize_user_identity(row.get(column))
            if key:
                return key
    query_id = _normalize_user_identity(row.get("query_id"))
    if query_id:
        return f"unknown-query:{query_id}"
    return f"unknown-row:{_normalize_user_identity(getattr(row, 'name', '')) or id(row)}"


def _normalize_user_identity(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = _WHITESPACE_RE.sub(" ", str(value).strip()).lower()
    if text in {"", "-", "none", "null", "nan", "<na>"}:
        return ""
    return text


def _score_prepared(left: _PreparedQuery, right: _PreparedQuery) -> float:
    if left.normalized_sql == right.normalized_sql:
        return 1.0
    sql_sequence = SequenceMatcher(None, left.normalized_sql, right.normalized_sql, autojunk=False).ratio()
    token_overlap = _jaccard(left.token_set, right.token_set)
    bigram_overlap = _jaccard(left.token_bigrams, right.token_bigrams)

    if left.parse_ok and right.parse_ok:
        ast_sequence = SequenceMatcher(
            None,
            left.intelligence.ast_node_shape,
            right.intelligence.ast_node_shape,
            autojunk=False,
        ).ratio()
        table_overlap = _jaccard(left.tables, right.tables)
        column_overlap = _jaccard(left.columns, right.columns)
        join_overlap = _jaccard(left.joins, right.joins)
        predicate_overlap = _jaccard(left.predicates, right.predicates)
        join_column_overlap = _jaccard(left.intelligence.join_columns, right.intelligence.join_columns)
        filter_column_overlap = _jaccard(left.intelligence.filter_columns, right.intelligence.filter_columns)
        projected_column_overlap = _jaccard(left.intelligence.projected_columns, right.intelligence.projected_columns)
        score = (
            sql_sequence * 0.25
            + ast_sequence * 0.22
            + token_overlap * 0.10
            + bigram_overlap * 0.05
            + table_overlap * 0.09
            + column_overlap * 0.07
            + join_overlap * 0.04
            + predicate_overlap * 0.03
            + join_column_overlap * 0.06
            + filter_column_overlap * 0.05
            + projected_column_overlap * 0.04
        )
        return round(score, 4)

    score = (sql_sequence * 0.55) + (token_overlap * 0.30) + (bigram_overlap * 0.15)
    return round(score, 4)


def _cheap_overlap_score(left: _PreparedQuery, right: _PreparedQuery) -> float:
    if left.normalized_sql == right.normalized_sql:
        return 1.0
    token_score = (0.65 * _jaccard(left.token_set, right.token_set)) + (
        0.35 * _jaccard(left.token_bigrams, right.token_bigrams)
    )
    if left.parse_ok and right.parse_ok:
        feature_score = (
            _jaccard(left.tables, right.tables) * 0.28
            + _jaccard(left.columns, right.columns) * 0.16
            + _jaccard(left.intelligence.join_columns, right.intelligence.join_columns) * 0.18
            + _jaccard(left.intelligence.filter_columns, right.intelligence.filter_columns) * 0.14
            + _jaccard(left.joins, right.joins) * 0.14
            + _jaccard(frozenset(left.intelligence.ast_node_shape), frozenset(right.intelligence.ast_node_shape))
            * 0.10
        )
        return max(token_score, feature_score)
    return token_score


def _jaccard(left: frozenset, right: frozenset) -> float:
    if not left or not right:
        return 0.0
    union = left | right
    return len(left & right) / len(union) if union else 0.0


def _find(parent: dict[int, int], item: int) -> int:
    while parent[item] != item:
        parent[item] = parent[parent[item]]
        item = parent[item]
    return item


def _union(parent: dict[int, int], left: int, right: int) -> None:
    left_root = _find(parent, left)
    right_root = _find(parent, right)
    if left_root != right_root:
        parent[right_root] = left_root


def _pair_key(left: int, right: int) -> tuple[int, int]:
    return (left, right) if left < right else (right, left)


def _scores_for_group(
    members: list[_PreparedQuery],
    pair_scores: dict[tuple[int, int], float],
) -> list[float]:
    scores = []
    for left_pos, left in enumerate(members):
        for right in members[left_pos + 1 :]:
            key = _pair_key(left.frame_index, right.frame_index)
            score = pair_scores.get(key)
            if score is not None:
                scores.append(score)
    return scores


def _best_member_score(
    member: _PreparedQuery,
    group: list[_PreparedQuery],
    pair_scores: dict[tuple[int, int], float],
) -> float:
    if len(group) <= 1:
        return 1.0
    scores = [
        pair_scores.get(_pair_key(member.frame_index, other.frame_index), 0.0)
        for other in group
        if other.frame_index != member.frame_index
    ]
    return max(scores) if scores else 1.0


def _shared_feature(members: list[_PreparedQuery], attr: str) -> set[str]:
    feature_sets = [set(getattr(item, attr)) for item in members if getattr(item, attr)]
    if not feature_sets:
        return set()
    return set.intersection(*feature_sets)


def _table_name(table: exp.Table) -> str:
    parts = [table.catalog, table.db, table.name]
    return ".".join(_clean_identifier(part) for part in parts if part)


def _join_signature(join: exp.Join) -> str:
    side = str(join.args.get("side") or "").lower()
    kind = str(join.args.get("kind") or "").lower()
    method = str(join.args.get("method") or "").lower()
    join_type = " ".join(part for part in (side, kind, method) if part) or "join"
    target = _join_target(join.this)
    on_expr = join.args.get("on")
    using_expr = join.args.get("using")
    if on_expr is not None:
        on_cols = sorted(
            _collapse_sql(col.sql(dialect="redshift").lower())
            for col in _iter_column_nodes(on_expr)
        )
    elif using_expr is not None:
        # sqlglot stores USING as a list of Identifier nodes, not Columns.
        on_cols = sorted(_using_identifier_names(using_expr))
        join_type = f"{join_type} using" if "using" not in join_type else join_type
    else:
        on_cols = []
    return _collapse_sql(f"{join_type}:{target}:{','.join(on_cols[:8])}")


def _using_identifier_names(value: object) -> list[str]:
    names: list[str] = []
    if value is None:
        return names
    if isinstance(value, (list, tuple)):
        for item in value:
            names.extend(_using_identifier_names(item))
        return names
    text = str(
        getattr(value, "name", None)
        or getattr(value, "this", None)
        or value
        or ""
    ).strip().strip('"').lower()
    if text and text not in {"identifier", "column"}:
        names.append(_clean_identifier(text))
    else:
        for col in _iter_column_nodes(value):
            leaf = _clean_identifier(getattr(col, "name", "") or col)
            if leaf:
                names.append(leaf)
    return names


def _join_target(node: exp.Expression | None) -> str:
    if isinstance(node, exp.Table):
        return _table_name(node)
    if node is None:
        return ""
    return type(node).__name__.lower()


def _predicate_signature(node: exp.Expression) -> str:
    return _collapse_sql(node.sql(dialect="redshift", pretty=False).lower())


def _predicate_operator_signature(node: exp.Expression) -> str:
    operator = type(node).__name__.lower()
    column_count = sum(1 for _ in node.find_all(exp.Column))
    function_flag = "func" if any(True for _ in node.find_all(exp.Func)) else "plain"
    return _collapse_sql(f"{operator}:{column_count}col:{function_flag}")


def _column_leaf_signature(column: exp.Column) -> str:
    name = str(getattr(column, "name", "") or "").strip().lower()
    return _clean_identifier(name) if name else _column_signature(column)


def _column_signature(column: exp.Column) -> str:
    return _collapse_sql(column.sql(dialect="redshift", pretty=False).lower())


def _iter_column_nodes(node: object) -> list[exp.Column]:
    # sqlglot stores an ON expression as a single node, but a USING clause as a
    # LIST of Identifier (or Column) expressions. find_all() exists only on
    # expression nodes, so a list must be walked directly or it raises
    # AttributeError and kills whole-workload analysis.
    if node is None:
        return []
    if isinstance(node, (list, tuple)):
        found: list[exp.Column] = []
        for item in node:
            found.extend(_iter_column_nodes(item))
        return found
    if isinstance(node, exp.Column):
        return [node]
    if isinstance(node, exp.Identifier):
        name = str(getattr(node, "this", "") or node).strip()
        return [exp.column(name)] if name else []
    finder = getattr(node, "find_all", None)
    if callable(finder):
        # find_all walks children and can raise on a malformed subtree (e.g. an
        # arg slot holding a bare Placeholder where a list belongs). A column
        # list is descriptive metadata only, so degrading to "no columns found"
        # is always preferable to failing the whole workload analysis.
        try:
            return list(finder(exp.Column))
        except (TypeError, ValueError, AttributeError):
            return []
    return []


def _columns_in(node: object) -> set[str]:
    return {_column_signature(col) for col in _iter_column_nodes(node)}


def _join_columns(tree: exp.Expression) -> frozenset[str]:
    out: set[str] = set()
    for join in tree.find_all(exp.Join):
        out.update(_columns_in(join.args.get("on")))
        out.update(_columns_in(join.args.get("using")))
    return frozenset(out)


def _filter_columns(tree: exp.Expression) -> frozenset[str]:
    out: set[str] = set()
    for where in tree.find_all(exp.Where):
        out.update(_columns_in(where))
    having_cls = getattr(exp, "Having", None)
    if having_cls is not None:
        for having in tree.find_all(having_cls):
            out.update(_columns_in(having))
    return frozenset(out)


def _repeat_structure_key(intel: SQLIntelligence) -> str:
    if not intel.parse_ok or not intel.tables:
        return ""
    table_key = "|".join(sorted(intel.tables))
    predicate_key = "|".join(sorted(intel.predicate_operators)) or "no_predicates"
    return f"tables={table_key};predicate_count={intel.predicate_count};predicate_ops={predicate_key}"


def _regex_predicate_operators(normalized_sql: str) -> frozenset[str]:
    if not normalized_sql:
        return frozenset()
    operators: set[str] = set()
    for token, name in (
        (" between ", "between"),
        (" like ", "like"),
        (" ilike ", "ilike"),
        (" in ", "in"),
        (" is ", "is"),
        ("<>", "neq"),
        ("!=", "neq"),
        (">=", "gte"),
        ("<=", "lte"),
        (">", "gt"),
        ("<", "lt"),
        ("=", "eq"),
    ):
        if token in normalized_sql:
            operators.add(f"{name}:*:fallback")
    return frozenset(operators)


def _projected_columns(selects: list[exp.Select]) -> frozenset[str]:
    out: set[str] = set()
    for select in selects:
        # `select.expressions or []` is NOT a guard: after literal
        # canonicalization this slot can hold a bare Placeholder, which is
        # truthy, so the `or` never fires and the loop raises
        # "'Placeholder' object is not iterable" - killing the whole workload.
        for expression in _as_node_list(select.args.get("expressions")):
            out.update(_columns_in(expression))
    return frozenset(out)


def _clause_columns(tree: exp.Expression, clause_type: type[exp.Expression]) -> frozenset[str]:
    out: set[str] = set()
    for clause in tree.find_all(clause_type):
        out.update(_columns_in(clause))
    return frozenset(out)


def _clean_identifier(value: object) -> str:
    return str(value).strip('"').strip().lower()


def _sql_to_text(sql: object) -> str:
    """Coerce any SQL-ish input to text, never raising.

    This is the single entry point every analysis function funnels through, so
    a failure here takes down the whole workload rather than one query.

    ``str()`` on a sqlglot Expression calls ``.sql()``, which walks the tree.
    If any arg slot that should hold a list holds a bare node instead - a
    Placeholder left by literal canonicalization is the case seen in the field -
    that walk raises ``TypeError: 'Placeholder' object is not iterable``. The
    exception escaped grouping and zeroed every repeat group, so the triage
    chart rendered nothing.

    Falling back to ``repr`` keeps the pipeline alive: downstream code gets a
    stable string, that one query fingerprints via the regex path instead of
    the AST path, and the other thousands are unaffected.
    """
    if sql is None or (isinstance(sql, float) and math.isnan(sql)):
        return ""
    try:
        return str(sql)
    except (TypeError, ValueError, AttributeError, RecursionError):
        try:
            return repr(sql)
        except Exception:
            return ""


def _join_values(values: pd.Series | None, *, limit: int) -> str:
    if values is None:
        return ""
    output: list[str] = []
    for value in values:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            continue
        text = str(value)
        if not text:
            continue
        if text in output:
            continue
        output.append(text)
        if len(output) >= limit:
            break
    return ", ".join(output)


MULTIPLE_USERS_LABEL = "Multiple Users"


def _distinct_user_count(values: pd.Series | None) -> int:
    if values is None:
        return 0
    return len({
        key
        for key in (_normalize_user_identity(value) for value in values)
        if key
    })


def _grouped_user_label(
    values: pd.Series | None, *, scope_by_user: bool = False
) -> str:
    """User column for a repeat group.

    With per-user scoping ON a group is one user by construction, so the name
    is exact. With it OFF the group intentionally spans users, and the honest
    label is that it is many - listing the first eight would read as the
    complete set.
    """
    if not scope_by_user and _distinct_user_count(values) > 1:
        return MULTIPLE_USERS_LABEL
    return _join_user_values(values, limit=8)


def _join_user_values(values: pd.Series | None, *, limit: int) -> str:
    if values is None:
        return ""
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        key = _normalize_user_identity(value)
        if not key or key in seen:
            continue
        display = _WHITESPACE_RE.sub(" ", str(value).strip())
        if not display:
            continue
        seen.add(key)
        output.append(display)
        if len(output) >= limit:
            break
    return ", ".join(output)


def _join_sorted(values, *, limit: int) -> str:
    return ", ".join(sorted(str(value) for value in values if str(value).strip())[:limit])


def _to_float(value: object) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _clamp_float(value: object, default: float, low: float, high: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    if number < low:
        return float(low)
    if number > high:
        return float(high)
    return float(number)


def _truncate(value: str, limit: int) -> str:
    text = _collapse_sql(value)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _display_sql(value: object, limit: int) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = _decode_escaped_whitespace(str(value))
    text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    return text if len(text) <= limit else text[: limit - 3].rstrip() + "..."


def _decode_escaped_whitespace(text: str) -> str:
    return (
        text.replace("\\r\\n", "\n")
        .replace("\\n", "\n")
        .replace("\\r", "\n")
        .replace("\\t", "\t")
    )


def _collapse_sql(text: str) -> str:
    return _WHITESPACE_RE.sub(" ", text).strip()


def _empty_groups() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "repeat_group_id",
            "repeat_group_key",
            "query_count",
            "distinct_sql_count",
            "avg_similarity",
            "min_similarity",
            "max_similarity",
            "fingerprint_method",
            "parse_success_rate",
            "total_runtime_s",
            "worst_runtime_s",
            "total_elapsed_s",
            "total_execution_s",
            "total_queue_s",
            "total_input_rows",
            "total_output_rows",
            "total_input_bytes",
            "total_output_bytes",
            "total_spill_blocks",
            "total_remote_read_io",
            "avg_risk_score",
            "max_risk_score",
            "table_count",
            "join_count",
            "predicate_count",
            "cte_count",
            "wildcard_count",
            "users",
            "databases",
            "query_type",
            "repeat_kind",
            "repeat_match_basis",
            "repeat_constraint_key",
            "sql_length_min",
            "sql_length_max",
            "sql_length_avg",
            "predicate_operator_signature",
            "shared_tables",
            "sql_tables",
            "sql_tables_full",
            "sql_ctes",
            "sql_columns",
            "sql_join_columns",
            "sql_filter_columns",
            "sql_projected_columns",
            "sql_order_columns",
            "sql_group_columns",
            "sql_joins",
            "sql_predicates",
            "sql_predicate_operators",
            "query_ids",
            "bridge_query_ids",
            "bridge_query_count",
            "bridge_snapshot_ids",
            "example_query_ids",
            "example_query_id_1",
            "example_query_id_2",
            "example_query_id_3",
            "representative_query_id",
            "representative_sql",
            "procedure_key",
            "procedure_definition",
            "sql_shape",
            "ast_shape",
            "sample_sql",
        ]
    )


def _empty_members() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "repeat_group_id",
            "repeat_group_key",
            "member_rank",
            "shown_in_tree",
            "query_id",
            "snapshot_id",
            "namespace_id",
            "bridge_key",
            "similarity_score",
            "elapsed_s",
            "risk_score",
            "user_name",
            "database_name",
            "query_type",
            "start_time",
            "dominant_issue",
            "repeat_kind",
            "procedure_key",
            "constraint_key",
            "sql_length",
            "sql_parse_status",
            "sql_parse_error",
            "sql_table_count",
            "sql_join_count",
            "sql_predicate_count",
            "sql_cte_count",
            "sql_wildcard_count",
            "sql_tables",
            "sql_tables_full",
            "sql_columns",
            "sql_join_columns",
            "sql_filter_columns",
            "sql_projected_columns",
            "sql_order_columns",
            "sql_group_columns",
            "sql_joins",
            "sql_predicates",
            "sql_predicate_operators",
            "sql_shape",
            "sql_text",
            "sql_text_full",
        ]
    )
