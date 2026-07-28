"""Load cluster-wide DuckDB analytics into UI-friendly data frames."""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd

from .duckdb_store import DuckDBStore, INSIGHT_RULE_COUNT, producer_namespace_id
from .query_similarity import (
    build_repeat_query_report,
    diagnose_repeat_query_candidates,
    enrich_slow_queries_with_sql_features,
    format_progress_eta,
)
from .repeat_triage import build_repeat_triage
from .settings import load_settings
from . import sql_feature_cache
from .sql_feature_cache import (
    cache_path as sql_feature_cache_path,
    read_cache as _read_sql_feature_cache,
    sql_feature_key as _sql_feature_key,
    unify_feature_dtypes as _unify_feature_dtypes,
    write_cache as _write_sql_feature_cache,
)


ProgressCallback = Callable[[str, int, int], None]

# Repeat-query analysis (SQL parsing + similarity grouping) dominates triage
# load time, so its results are cached inside the DuckDB file and reused until
# the snapshot, the grouping settings, or the underlying rows change.
_REPEAT_CACHE_META = "analysis_cache_meta"
_REPEAT_CACHE_FRAMES = {
    "slow_features": "analysis_cache_slow_features",
    "repeat_groups": "analysis_cache_repeat_groups",
    "repeat_members": "analysis_cache_repeat_members",
    "repeat_group_tables": "analysis_cache_repeat_group_tables",
}
# SQL parse cache lives in analyzer.sql_feature_cache (external sidecar file).
_SQL_FEATURE_CACHE = sql_feature_cache.LEGACY_WAREHOUSE_TABLE
_SQL_FEATURE_CACHE_VERSION = sql_feature_cache.CACHE_VERSION
# Bumped to v4 so the Placeholder-crash fix actually re-runs. The cache key
# hashes this constant plus the data identities - never the source code - so a
# crashed run that cached an EMPTY result would otherwise be read straight back
# and the fixed code would never execute.
_REPEAT_CACHE_VERSION = "v4-sql-to-text-guarded"
_LOADER_REPEAT_GROUPS = "loader_repeat_groups"
_LOADER_REPEAT_MEMBERS = "loader_repeat_members"


def _table_exists(con, table_name: str) -> bool:
    try:
        row = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _table_is_empty(con, table_name: str) -> bool:
    """True when the table is absent or holds no rows.

    External capture is opt-in, so external_table_metadata usually exists but is
    empty. Querying it anyway made the Table Heat Map spend a whole step
    fetching nothing, which on screen was indistinguishable from a stall.
    """
    if not _table_exists(con, table_name):
        return True
    try:
        row = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
        return not (row and row[0])
    except Exception:
        return True


def _read_loader_repeat_data(con, snapshot_id: str | None) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """Read grouping materialized by the selective loader for this snapshot.

    The repair loader atomically replaces both tables, so a single common
    loader snapshot is authoritative even when older catalog metadata causes
    ``latest_snapshot()`` to choose a different id.  Never guess when multiple
    loader snapshots are present.
    """
    try:
        existing = {
            str(row[0]).lower()
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
            ).fetchall()
        }
        if _LOADER_REPEAT_GROUPS not in existing or _LOADER_REPEAT_MEMBERS not in existing:
            return None
        group_columns = {
            str(row[0]).lower()
            for row in con.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{_LOADER_REPEAT_GROUPS}'"
            ).fetchall()
        }
        member_columns = {
            str(row[0]).lower()
            for row in con.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{_LOADER_REPEAT_MEMBERS}'"
            ).fetchall()
        }
        snapshot_bound = "snapshot_id" in group_columns and "snapshot_id" in member_columns
        if snapshot_id and snapshot_bound:
            groups = con.execute(
                f'SELECT * FROM "{_LOADER_REPEAT_GROUPS}" WHERE snapshot_id = ?',
                [snapshot_id],
            ).fetchdf()
            members = con.execute(
                f'SELECT * FROM "{_LOADER_REPEAT_MEMBERS}" WHERE snapshot_id = ?',
                [snapshot_id],
            ).fetchdf()
            if groups.empty or members.empty:
                common_snapshots = con.execute(
                    f'''
SELECT DISTINCT CAST(g.snapshot_id AS VARCHAR) AS snapshot_id
FROM "{_LOADER_REPEAT_GROUPS}" g
INNER JOIN "{_LOADER_REPEAT_MEMBERS}" m
  ON CAST(m.snapshot_id AS VARCHAR) = CAST(g.snapshot_id AS VARCHAR)
WHERE NULLIF(TRIM(CAST(g.snapshot_id AS VARCHAR)), '') IS NOT NULL
ORDER BY snapshot_id
'''
                ).fetchall()
                if len(common_snapshots) == 1:
                    loader_snapshot = str(common_snapshots[0][0])
                    groups = con.execute(
                        f'SELECT * FROM "{_LOADER_REPEAT_GROUPS}" WHERE CAST(snapshot_id AS VARCHAR) = ?',
                        [loader_snapshot],
                    ).fetchdf()
                    members = con.execute(
                        f'SELECT * FROM "{_LOADER_REPEAT_MEMBERS}" WHERE CAST(snapshot_id AS VARCHAR) = ?',
                        [loader_snapshot],
                    ).fetchdf()
        else:
            groups = con.execute(f'SELECT * FROM "{_LOADER_REPEAT_GROUPS}"').fetchdf()
            members = con.execute(f'SELECT * FROM "{_LOADER_REPEAT_MEMBERS}"').fetchdf()
    except Exception:
        return None
    if groups is None or groups.empty or members is None or members.empty:
        return None
    return groups, members


def _hydrate_loader_repeat_data(
    groups: pd.DataFrame,
    members: pd.DataFrame,
    slow_queries: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Add current evidence metrics without repeating SQL parsing or grouping."""
    if groups is None or groups.empty or members is None or members.empty:
        return groups, members
    hydrated_groups = groups.copy()
    hydrated_members = members.copy()
    if slow_queries is None or slow_queries.empty or "query_id" not in slow_queries.columns:
        return hydrated_groups, hydrated_members
    slow = slow_queries.copy()
    # Identical query ids exist on different clusters. When both frames carry
    # the namespace, it participates in member identity so hydration can never
    # apply one cluster's evidence to another's member row.
    namespace_aware = "namespace_id" in slow.columns and "namespace_id" in hydrated_members.columns

    def _member_key(frame: pd.DataFrame) -> pd.Series:
        qid = frame["query_id"].astype(str)
        if not namespace_aware:
            return qid
        namespace = frame["namespace_id"].fillna("").astype(str).str.strip().str.lower()
        return namespace + "|" + qid

    slow["__qid"] = _member_key(slow)
    hydrated_members["__qid"] = _member_key(hydrated_members)
    update_columns = [
        column for column in (
            "elapsed_s", "risk_score", "user_name", "database_name", "query_type",
            "start_time", "dominant_issue",
        ) if column in slow.columns
    ]
    if update_columns:
        updates = slow[["__qid", *update_columns]].drop_duplicates("__qid", keep="first")
        hydrated_members = hydrated_members.merge(
            updates, on="__qid", how="left", suffixes=("", "__current")
        )
        for column in update_columns:
            current = f"{column}__current"
            if current in hydrated_members.columns:
                hydrated_members[column] = hydrated_members[current].combine_first(
                    hydrated_members[column] if column in hydrated_members.columns else pd.Series(
                        index=hydrated_members.index, dtype="object"
                    )
                )
                hydrated_members.drop(columns=[current], inplace=True)

    evidence_columns = [
        column for column in (
            "total_spill", "has_nested_loop", "dist_both_cnt", "bcast_cnt",
            "remote_io_ratio", "max_data_skewness", "selectivity_ratio",
            "input_rows", "output_rows", "input_bytes", "output_bytes",
            "remote_read_io", "elapsed_s", "execution_s", "queue_s", "risk_score",
        ) if column in slow.columns
    ]
    slow_index = slow.set_index("__qid", drop=False)
    for index, group in hydrated_groups.iterrows():
        group_id = str(group.get("repeat_group_id") or "")
        ids = hydrated_members.loc[
            hydrated_members["repeat_group_id"].astype(str) == group_id, "__qid"
        ].astype(str)
        rows = slow_index.loc[slow_index.index.intersection(ids)]
        if rows.empty:
            continue
        hydrated_groups.at[index, "query_count"] = int(len(ids))
        if "elapsed_s" in rows.columns:
            elapsed = pd.to_numeric(rows["elapsed_s"], errors="coerce").fillna(0.0)
            hydrated_groups.at[index, "total_runtime_s"] = float(elapsed.sum())
            hydrated_groups.at[index, "worst_runtime_s"] = float(elapsed.max())
        if "risk_score" in rows.columns:
            risks = pd.to_numeric(rows["risk_score"], errors="coerce").fillna(0.0)
            hydrated_groups.at[index, "avg_risk_score"] = float(risks.mean())
            hydrated_groups.at[index, "max_risk_score"] = float(risks.max())
        for column in evidence_columns:
            values = pd.to_numeric(rows[column], errors="coerce").dropna()
            if values.empty:
                continue
            hydrated_groups.at[index, f"avg_{column}"] = float(values.mean())
            if column in {"input_rows", "output_rows", "input_bytes", "output_bytes", "remote_read_io"}:
                hydrated_groups.at[index, f"total_{column}"] = float(values.sum())
    return hydrated_groups, hydrated_members.drop(columns=["__qid"], errors="ignore")


# Columns that must be numeric for merging, summing, and the quadrant chart.
_REPEAT_NUMERIC_COLUMNS = {
    "query_count", "total_runtime_s", "avg_runtime_s", "total_input_rows",
    "triage_priority_score", "similarity_score", "uses_view",
    "distinct_sql_count", "repeat_group_size", "repeat_group_runtime_s",
    "total_spill", "plan_evidence_count",
}


def _coerce_repeat_numeric_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Force known-numeric repeat columns to numbers, whatever their source.

    Loader-materialized and cached frames round-trip through DuckDB and can
    come back as VARCHAR (or as object columns mixing int and str after
    hydration). Mixed columns crash pandas sums with int+str TypeErrors and
    silently break the quadrant's numeric axes.
    """
    if frame is None or frame.empty:
        return frame
    result = frame.copy()
    for col in result.columns:
        if col in _REPEAT_NUMERIC_COLUMNS or (
            result[col].dtype == object
            and str(col).endswith(("_s", "_count", "_score", "_rows", "_us"))
        ):
            result[col] = pd.to_numeric(result[col], errors="coerce")
    return result


# Statements parsed between cache checkpoints. Each chunk is written to the
# external sql_feature_cache sidecar so an interrupted parse resumes where it
# stopped and the loading dialog can show a live "X of Y" sub-counter.
_SQL_PARSE_CHUNK_ROWS = 150


def _enrich_with_incremental_sql_cache(
    store: DuckDBStore,
    slow_queries: pd.DataFrame,
    progress: Callable[[str], None] | None = None,
    *,
    warehouse_con: object | None = None,
) -> tuple[pd.DataFrame, int, int]:
    """Reuse parsed SQL features by SQL text; examine only unseen/changed rows.

    *warehouse_con* is the load's open DuckDB handle (optional). Cache reads must
    not open a nested warehouse connection while that handle is live.
    """
    if slow_queries is None or slow_queries.empty or "sql_text" not in slow_queries.columns:
        return slow_queries, 0, 0
    base = slow_queries.copy()
    base["__sql_feature_key"] = base["sql_text"].map(_sql_feature_key)
    try:
        cached = _read_sql_feature_cache(store, warehouse_con=warehouse_con)
    except Exception:
        cached = pd.DataFrame()

    cache_path = sql_feature_cache_path(store)
    cached_keys = set(cached.get("sql_feature_key", pd.Series(dtype="object")).astype(str))
    missing = base[~base["__sql_feature_key"].astype(str).isin(cached_keys)]
    unique_missing = missing.drop_duplicates("__sql_feature_key", keep="first")
    new_feature_frames: list[pd.DataFrame] = []
    total_missing = len(unique_missing)
    parse_started = time.monotonic() if total_missing else 0.0
    if progress is not None and total_missing == 0:
        progress(
            f"Parsing SQL shapes - all {len(cached_keys):,} statement(s) already cached "
            f"in {cache_path.name} (nothing new to parse)"
        )
    elif progress is not None and total_missing > 0:
        progress(
            f"Parsing SQL shapes - external cache {cache_path.name} "
            f"({len(cached_keys):,} cached; {total_missing:,} new to parse)"
        )
    for start in range(0, total_missing, _SQL_PARSE_CHUNK_ROWS):
        chunk = unique_missing.iloc[start:start + _SQL_PARSE_CHUNK_ROWS]
        raw_chunk = chunk.drop(columns=["__sql_feature_key"])
        enriched_chunk = enrich_slow_queries_with_sql_features(raw_chunk, progress=progress)
        feature_cols = [col for col in enriched_chunk.columns if col not in raw_chunk.columns]
        if feature_cols:
            features = enriched_chunk[feature_cols].copy()
            features.insert(
                0,
                "sql_feature_key",
                chunk.loc[enriched_chunk.index, "__sql_feature_key"].astype(str).values,
            )
            new_feature_frames.append(features)
            # Checkpoint every chunk into the external file (survives app kill).
            try:
                _write_sql_feature_cache(store, features)
            except Exception:
                pass
        if progress is not None:
            done = min(total_missing, start + len(chunk))
            eta = format_progress_eta(done, total_missing, parse_started)
            progress(
                f"Parsing SQL shapes - {done:,} of {total_missing:,} new statement(s)"
                f"{eta}; {len(cached_keys):,} already in {cache_path.name}; checkpointed"
            )
    new_features = (
        pd.concat(new_feature_frames, ignore_index=True, sort=False)
        if new_feature_frames
        else pd.DataFrame()
    )

    feature_cache = pd.concat([cached, new_features], ignore_index=True, sort=False)
    if feature_cache.empty:
        enriched = enrich_slow_queries_with_sql_features(slow_queries, progress=progress)
        return enriched, 0, len(unique_missing)
    feature_cache = feature_cache.drop_duplicates("sql_feature_key", keep="last")
    # DuckDB round-trips can degrade numeric feature columns to VARCHAR.
    # Unify each column so cached (str) and fresh (numeric) rows never mix
    # types — mixed object columns crash downstream sums with int+str.
    feature_cache = _unify_feature_dtypes(feature_cache)
    feature_cols = [col for col in feature_cache.columns if col != "sql_feature_key"]
    enriched = base.merge(
        feature_cache[["sql_feature_key", *feature_cols]],
        left_on="__sql_feature_key",
        right_on="sql_feature_key",
        how="left",
        sort=False,
    ).drop(columns=["__sql_feature_key", "sql_feature_key"])
    reused_rows = int(len(base) - len(missing))
    return enriched, reused_rows, int(len(unique_missing))


def _repeat_cache_key(
    snapshot_id: str | None,
    settings,
    slow_queries: pd.DataFrame,
    procedure_definitions: pd.DataFrame,
    table_review: pd.DataFrame,
) -> str:
    digest = hashlib.sha256()
    digest.update(_REPEAT_CACHE_VERSION.encode("utf-8"))
    digest.update(str(snapshot_id or "").encode("utf-8"))
    digest.update(
        (
            f"|{settings.repeat_similarity_threshold}|{settings.repeat_prefilter_threshold}"
            f"|{settings.repeat_scope_by_user}|{settings.repeat_min_group_size}"
            f"|{settings.repeat_fuzzy_merge_threshold}"
            f"|{','.join(sorted(str(value).lower() for value in settings.analysis_namespace_filter))}"
        ).encode("utf-8")
    )
    for frame, key_col in (
        (slow_queries, "query_id"),
        (procedure_definitions, "procedure_key"),
        (table_review, "table_key"),
    ):
        digest.update(f"|{len(frame)}".encode("utf-8"))
        if frame is not None and not frame.empty and key_col in frame.columns:
            namespace = (
                frame["namespace_id"].fillna("").astype(str)
                if "namespace_id" in frame.columns
                else pd.Series([""] * len(frame), index=frame.index)
            )
            identities = namespace + "|" + frame[key_col].fillna("").astype(str)
            for value in sorted(identities):
                digest.update(value.encode("utf-8", errors="replace"))
    return digest.hexdigest()[:24]


def _read_repeat_cache(con, cache_key: str) -> dict | None:
    try:
        meta = con.execute(
            f"SELECT cache_key, diagnostics_json FROM {_REPEAT_CACHE_META}"
        ).fetchone()
    except Exception:
        return None
    if not meta or str(meta[0]) != cache_key:
        return None
    result: dict = {}
    try:
        result["diagnostics"] = json.loads(str(meta[1] or "{}"))
        for name, table in _REPEAT_CACHE_FRAMES.items():
            result[name] = con.execute(f'SELECT * FROM "{table}"').fetchdf()
    except Exception:
        return None
    return result


def _write_repeat_cache(con, cache_key: str, frames: dict, diagnostics: dict) -> None:
    for name, table in _REPEAT_CACHE_FRAMES.items():
        frame = frames.get(name)
        frame = frame if frame is not None else pd.DataFrame()
        registered = f"cache_{uuid.uuid4().hex}"
        con.register(registered, frame)
        try:
            con.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM {registered}')
        finally:
            con.unregister(registered)
    meta = pd.DataFrame(
        [
            {
                "cache_key": cache_key,
                "diagnostics_json": json.dumps(
                    diagnostics, default=lambda o: o.item() if hasattr(o, "item") else str(o)
                ),
                "created_at": str(datetime.now()),
            }
        ]
    )
    registered = f"cache_{uuid.uuid4().hex}"
    con.register(registered, meta)
    try:
        con.execute(f"CREATE OR REPLACE TABLE {_REPEAT_CACHE_META} AS SELECT * FROM {registered}")
    finally:
        con.unregister(registered)


@dataclass
class ClusterReport:
    db_path: Path
    snapshot_id: str | None = None
    summary: dict = field(default_factory=dict)
    slow_queries: pd.DataFrame = field(default_factory=pd.DataFrame)
    query_explain: pd.DataFrame = field(default_factory=pd.DataFrame)
    query_detail_flow: pd.DataFrame = field(default_factory=pd.DataFrame)
    insights: pd.DataFrame = field(default_factory=pd.DataFrame)
    family_summary: pd.DataFrame = field(default_factory=pd.DataFrame)
    table_risk: pd.DataFrame = field(default_factory=pd.DataFrame)
    table_impact: pd.DataFrame = field(default_factory=pd.DataFrame)
    table_review: pd.DataFrame = field(default_factory=pd.DataFrame)
    table_heatmap: pd.DataFrame = field(default_factory=pd.DataFrame)
    external_table_metadata: pd.DataFrame = field(default_factory=pd.DataFrame)
    external_tables: pd.DataFrame = field(default_factory=pd.DataFrame)
    view_definitions: pd.DataFrame = field(default_factory=pd.DataFrame)
    procedure_definitions: pd.DataFrame = field(default_factory=pd.DataFrame)
    table_status: pd.DataFrame = field(default_factory=pd.DataFrame)
    action_queue: pd.DataFrame = field(default_factory=pd.DataFrame)
    rewrites: pd.DataFrame = field(default_factory=pd.DataFrame)
    repeat_groups: pd.DataFrame = field(default_factory=pd.DataFrame)
    repeat_members: pd.DataFrame = field(default_factory=pd.DataFrame)
    repeat_group_tables: pd.DataFrame = field(default_factory=pd.DataFrame)
    flow_edges: pd.DataFrame = field(default_factory=pd.DataFrame)
    query_heatmap: pd.DataFrame = field(default_factory=pd.DataFrame)
    rule_count: int = INSIGHT_RULE_COUNT
    analysis_namespace_scope: tuple[str, ...] = ()
    loaded_areas: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()
    load_errors: tuple[str, ...] = ()

    @property
    def is_empty(self) -> bool:
        return self.slow_queries.empty and self.table_risk.empty and self.insights.empty


def _build_query_issue_heatmap(slow_queries: pd.DataFrame) -> pd.DataFrame:
    """Build a compact query×issue matrix for the Overview heat map widget.

    Uses dominant_issue / risk signals already present on slow-query rows so
    Overview is never left with an empty matrix after a successful load.
    """
    if slow_queries is None or slow_queries.empty:
        return pd.DataFrame(columns=["query_id", "family", "severity_rank"])
    rows: list[dict] = []
    ordered = slow_queries.copy()
    if "risk_score" in ordered.columns:
        ordered = ordered.sort_values("risk_score", ascending=False)
    elif "elapsed_s" in ordered.columns:
        ordered = ordered.sort_values("elapsed_s", ascending=False)
    for _, query in ordered.head(40).iterrows():
        qid = str(query.get("query_id") or "").strip()
        if not qid:
            continue
        family = str(query.get("dominant_issue") or query.get("severity_reason") or "GENERAL").strip() or "GENERAL"
        risk = 0.0
        try:
            risk = float(query.get("risk_score") or 0)
        except (TypeError, ValueError):
            risk = 0.0
        if risk >= 80:
            rank = 3
        elif risk >= 50:
            rank = 2
        elif risk > 0:
            rank = 1
        else:
            rank = 1
        rows.append({"query_id": qid, "family": family[:24], "severity_rank": rank})
    return pd.DataFrame(rows)


def merge_cluster_reports(base: ClusterReport | None, update: ClusterReport) -> ClusterReport:
    if base is None or base.db_path != update.db_path:
        return update
    if base.snapshot_id and update.snapshot_id and base.snapshot_id != update.snapshot_id:
        # A refresh/swap produced a new dataset. Never merge cached frames from
        # the previous snapshot into the new report.
        return update
    if base.analysis_namespace_scope != update.analysis_namespace_scope:
        # A scope change must never merge frames from excluded clusters back
        # into the newly focused report.
        return update

    # Areas present in update.loaded_areas were intentionally re-read. An empty
    # result is authoritative (e.g. zero insights after a refresh) — do not keep
    # the prior cached frame, which would silently show stale data as current.
    trusted_areas = set(update.loaded_areas or ())
    area_to_frame = {
        "slow_queries": {"slow_queries", "repeat_queries", "sql_lens"},
        "query_explain": {"slow_queries", "repeat_queries", "sql_lens"},
        "query_detail_flow": {"slow_queries", "repeat_queries", "sql_lens"},
        "insights": {"insights"},
        "family_summary": {"insights"},
        "table_risk": {"table_risk", "sql_lens"},
        "table_impact": {"table_impact"},
        "table_review": {"table_review", "sql_lens", "repeat_queries"},
        "table_heatmap": {"table_heatmap"},
        "external_table_metadata": {"table_heatmap"},
        "external_tables": {"external_tables"},
        # View definitions ride with the triage load: they are the fuel for
        # per-group inline view explosion in the drill-in dialog.
        "view_definitions": {"sql_lens", "repeat_queries"},
        "procedure_definitions": {"sql_lens", "repeat_queries"},
        "table_status": set(),  # always refreshed with every load
        "action_queue": {"action_plan"},
        "rewrites": {"action_plan"},
        "repeat_groups": {"repeat_queries"},
        "repeat_members": {"repeat_queries"},
        "repeat_group_tables": {"repeat_queries"},
        "flow_edges": {"slow_queries", "repeat_queries", "sql_lens"},
        "query_heatmap": {"slow_queries", "repeat_queries", "sql_lens"},
    }

    def merged_frame(name: str) -> pd.DataFrame:
        update_frame = getattr(update, name)
        if update_frame is None:
            return getattr(base, name)
        owners = area_to_frame.get(name)
        if owners is None:
            # Unknown frame: prefer non-empty update, else base.
            return update_frame if not update_frame.empty else getattr(base, name)
        if not owners or owners & trusted_areas:
            # Explicitly reloaded (or always-refreshed status): empty wins.
            return update_frame
        return update_frame if not update_frame.empty else getattr(base, name)

    return ClusterReport(
        db_path=update.db_path,
        snapshot_id=update.snapshot_id or base.snapshot_id,
        summary={**base.summary, **update.summary},
        slow_queries=merged_frame("slow_queries"),
        query_explain=merged_frame("query_explain"),
        query_detail_flow=merged_frame("query_detail_flow"),
        insights=merged_frame("insights"),
        family_summary=merged_frame("family_summary"),
        table_risk=merged_frame("table_risk"),
        table_impact=merged_frame("table_impact"),
        table_review=merged_frame("table_review"),
        table_heatmap=merged_frame("table_heatmap"),
        external_table_metadata=merged_frame("external_table_metadata"),
        external_tables=merged_frame("external_tables"),
        view_definitions=merged_frame("view_definitions"),
        procedure_definitions=merged_frame("procedure_definitions"),
        table_status=merged_frame("table_status"),
        action_queue=merged_frame("action_queue"),
        rewrites=merged_frame("rewrites"),
        repeat_groups=merged_frame("repeat_groups"),
        repeat_members=merged_frame("repeat_members"),
        repeat_group_tables=merged_frame("repeat_group_tables"),
        flow_edges=merged_frame("flow_edges"),
        query_heatmap=merged_frame("query_heatmap"),
        rule_count=update.rule_count,
        analysis_namespace_scope=update.analysis_namespace_scope,
        loaded_areas=tuple(sorted(set(base.loaded_areas) | set(update.loaded_areas))),
        notes=tuple(dict.fromkeys(base.notes + update.notes)),
        load_errors=tuple(dict.fromkeys(base.load_errors + update.load_errors)),
    )


REPORT_AREA_CHOICES: tuple[tuple[str, str], ...] = (
    ("status", "DuckDB Table Status"),
    ("slow_queries", "Slow Queries"),
    ("table_review", "Table Review"),
    ("table_heatmap", "Table Heat Map"),
    ("external_tables", "External Tables"),
    ("insights", "Insight Ledger"),
    ("sql_lens", "SQL Lens Context"),
    ("repeat_queries", "Workload Triage Data"),
    ("table_impact", "Table Impact - expensive"),
    ("action_plan", "Action Plan - expensive"),
    ("all", "Safe Areas"),
)

REPORT_AREA_LABELS = {**dict(REPORT_AREA_CHOICES), "table_risk": "Table Risk Data"}
_SAFE_REPORT_AREAS = (
    "status", "slow_queries", "table_risk", "table_review", "table_heatmap",
    "external_tables", "sql_lens",
)
_ALL_REPORT_AREAS = tuple(key for key, _ in REPORT_AREA_CHOICES if key not in {"status", "all"})
_VALID_REPORT_AREAS = set(_ALL_REPORT_AREAS) | {"status", "table_risk"}


def load_cluster_report(
    db_path: str | Path | None = None,
    snapshot_id: str | None = None,
    areas: Iterable[str] | None = None,
    progress: ProgressCallback | None = None,
) -> ClusterReport:
    store = DuckDBStore(db_path)
    settings = load_settings()
    notes: list[str] = []
    selected_areas = _normalize_report_areas(areas)
    needs_slow_queries = bool(selected_areas & {"slow_queries", "repeat_queries", "sql_lens"})
    needs_insights = "insights" in selected_areas
    # Table Review now computes its physical-design score in one direct query.
    # Do not run v_table_risk separately first: that duplicated a large local
    # scan and made the button appear stalled before the review query began.
    needs_table_risk = bool(selected_areas & {"table_risk", "sql_lens"})
    needs_table_review = bool(selected_areas & {"table_review", "sql_lens", "repeat_queries"})
    needs_table_heatmap = "table_heatmap" in selected_areas
    # The heat map still consumes external metadata WHEN IT EXISTS - that is
    # what makes Spectrum tables visible alongside local ones. But external
    # capture is opt-in (see ingest_redshift.EXTERNAL_CAPTURE_ENABLED), so on
    # most installs the table is empty, and the fetch below skips it entirely
    # rather than spending step 5 querying a table with nothing in it.
    needs_external_metadata = needs_table_heatmap or "external_tables" in selected_areas
    needs_external_tables = "external_tables" in selected_areas
    needs_table_impact = "table_impact" in selected_areas
    needs_action_plan = "action_plan" in selected_areas
    # The triage drill-in explodes views inline per group, so view definitions
    # must ride with every repeat-queries load (there is no Views tab anymore).
    needs_view_definitions = bool(selected_areas & {"sql_lens", "repeat_queries"})
    needs_procedure_definitions = bool(selected_areas & {"sql_lens", "repeat_queries"})
    needs_repeat = "repeat_queries" in selected_areas
    total_steps = (
        3
        + (3 if needs_slow_queries else 0)  # slow queries + explain plans + linked execution steps
        + (2 if needs_insights else 0)
        + int(needs_table_risk)
        + int(needs_table_impact)
        + int(needs_table_review)
        + int(needs_table_heatmap)
        + int(needs_external_metadata)
        + int(needs_external_tables)
        + int(needs_view_definitions)
        + int(needs_procedure_definitions)
        + (2 if needs_action_plan else 0)
        # Repeat-query analysis dominates wall time, so it reports as five
        # separate steps: parse, group, table join, diagnose, merge.
        + (5 if (needs_slow_queries and needs_repeat) else 0)
    )
    completed_steps = 0

    def announce(message: str) -> None:
        if progress:
            # In-progress work reports as the next step (1-based), so a long
            # phase like grouping shows as "Step 11 of N" with its sub-indicator
            # rather than the last completed step number.
            current = min(completed_steps + 1, total_steps) if total_steps else 0
            progress(message, current, total_steps)

    def complete(message: str) -> None:
        nonlocal completed_steps
        completed_steps += 1
        if progress:
            progress(message, completed_steps, total_steps)

    announce(f"Opening local DuckDB snapshot: {store.path}")
    with store.connect() as con:
        complete("Opened local DuckDB snapshot")
        announce("Reading DuckDB table counts and index status")
        table_status = store.table_counts(con)
        complete(f"Read table/index status for {len(table_status):,} analyzer tables")
        announce("Finding latest local snapshot")
        if snapshot_id is None:
            row = con.execute(
                """
SELECT snapshot_id
FROM snapshot_runs
ORDER BY
  CASE WHEN LOWER(COALESCE(source, '')) = 'external-table-loader' THEN 1 ELSE 0 END,
  captured_at DESC
LIMIT 1
"""
            ).fetchone()
            snapshot_id = row[0] if row else None
        complete(f"Selected snapshot {snapshot_id or 'all rows'}")
        namespace_scope = tuple(dict.fromkeys(settings.analysis_namespace_filter))
        where, params = _snapshot_filter(snapshot_id, namespace_scope)
        table_where, table_params = _snapshot_filter(snapshot_id, namespace_scope, alias="t")
        if namespace_scope:
            notes.append(f"Analysis cluster scope: {', '.join(namespace_scope)}")
        else:
            notes.append("Analysis cluster scope: all loaded clusters")
        # Query-only repairs can carry a newer snapshot anchor than the
        # physical catalog. Resolve table metadata independently, and allow
        # legacy promoted rows with blank snapshot ids instead of filtering
        # 38K valid catalog rows down to zero.
        catalog_snapshot_id = _resolve_catalog_snapshot(con, snapshot_id)
        table_review_where, table_review_params = _snapshot_filter(
            catalog_snapshot_id, namespace_scope, alias="t"
        )
        view_snapshot_id = _resolve_table_snapshot(con, "view_definitions", snapshot_id)
        view_where, view_params = _snapshot_filter(view_snapshot_id, namespace_scope)
        external_snapshot_id = _resolve_table_snapshot(con, "external_table_info_all", snapshot_id)
        external_where, external_params = _snapshot_filter(external_snapshot_id, namespace_scope)
        detail_where, detail_params = _snapshot_filter(snapshot_id, namespace_scope, alias="f")
        load_errors: list[str] = []

        def fetch_df(label: str, sql: str, query_params: list[str]) -> pd.DataFrame:
            announce(f"Running local DuckDB query: {label}")
            try:
                df = con.execute(sql, query_params).fetchdf()
                complete(f"{label}: {len(df):,} row(s)")
                return df
            except Exception as exc:
                load_errors.append(f"{label}: {exc}")
                complete(f"{label}: failed")
                return pd.DataFrame()

        def fetch_capped(label: str, sql: str, query_params: list[str], limit: int) -> pd.DataFrame:
            # Fetch one row past the cap so truncation is reported, not silent.
            df = fetch_df(label, sql.replace(f"LIMIT {limit}", f"LIMIT {limit + 1}"), query_params)
            if len(df) > limit:
                load_errors.append(
                    f"{label}: showing the top {limit:,} rows by rank; more matched — "
                    "narrow the snapshot or filters to see the rest."
                )
                return df.head(limit).copy()
            return df

        slow_queries = pd.DataFrame()
        query_explain = pd.DataFrame()
        query_detail_flow = pd.DataFrame()
        insights = pd.DataFrame()
        family_summary = pd.DataFrame()
        table_risk = pd.DataFrame()
        table_impact = pd.DataFrame()
        table_review = pd.DataFrame()
        table_heatmap = pd.DataFrame()
        external_table_metadata = pd.DataFrame()
        external_tables = pd.DataFrame()
        table_review_zero_counts: dict[str, int] = {}
        view_definitions = pd.DataFrame()
        procedure_definitions = pd.DataFrame()
        action_queue = pd.DataFrame()
        rewrites = pd.DataFrame()
        query_heatmap = pd.DataFrame()
        flow_edges = pd.DataFrame()
        summary_dict: dict = {}

        if needs_slow_queries:
            # One-off queries are excluded inside DuckDB (window-function
            # repeat counts) so they never reach Python or sqlglot, and each
            # configured cluster's minimum-seconds cutoff applies first.
            floor_clause, floor_params = _namespace_floor_filter()
            slow_queries = fetch_df(
                "Repeating Queries",
                _repeating_slow_queries_sql(where, floor_clause, _slow_query_view_columns(con)),
                params + floor_params,
            )
            if floor_params:
                notes.append(
                    "Per-cluster minimum query cutoff applied (FLOOR_SECONDS in the "
                    "cluster profiles JSON; defaults producer 300s, consumers 30s)."
                )
            notes.append(
                "Workload scope: repeating query patterns only — one-off queries are "
                "excluded inside DuckDB before analysis."
            )
            if slow_queries.empty:
                # SAFETY NET: the repeat pre-filter must never blank the whole
                # analysis (e.g. hash columns absent and SQL text not captured
                # the way the filter expects). Fall back to the unfiltered set
                # and say so, rather than presenting an empty workload.
                slow_queries = fetch_df(
                    "All Queries (repeat pre-filter matched nothing)",
                    f"""
SELECT *
FROM v_slow_queries
WHERE {where}
ORDER BY risk_score DESC NULLS LAST, elapsed_s DESC NULLS LAST
""",
                    params,
                )
                notes.append(
                    "Repeat pre-filter matched no rows in this warehouse; showing "
                    "ALL captured queries instead so analysis is never empty. "
                    "Repeat grouping still runs on what is shown."
                )
            if not slow_queries.empty:
                query_heatmap = _build_query_issue_heatmap(slow_queries)
                # Downstream evidence joins reuse the surviving repeat set, so
                # explain plans and steps are never fetched for one-offs.
                con.register(
                    "_repeating_query_scope",
                    slow_queries[["namespace_id", "query_id"]].drop_duplicates(),
                )
                query_explain = fetch_df(
                    "Full Explain Plans",
                    f"""
SELECT e.*
FROM v_query_explain e
JOIN _repeating_query_scope s
  ON s.namespace_id IS NOT DISTINCT FROM e.namespace_id
 AND s.query_id = e.query_id
WHERE {"e.snapshot_id = ?" if snapshot_id else "TRUE"}
ORDER BY e.query_id, e.child_query_sequence, e.plan_node_id
""",
                    [snapshot_id] if snapshot_id else [],
                )
                query_detail_flow = fetch_df(
                    "Plan-linked Execution Steps",
                    f"""
SELECT d.*
FROM v_query_detail_flow d
JOIN _repeating_query_scope s
  ON s.namespace_id IS NOT DISTINCT FROM d.namespace_id
 AND s.query_id = d.query_id
WHERE {"d.snapshot_id = ?" if snapshot_id else "TRUE"}
  AND d.plan_node_id IS NOT NULL
ORDER BY d.query_id, d.child_query_sequence, d.plan_node_id, d.stream_id, d.segment_id, d.step_id
""",
                    [snapshot_id] if snapshot_id else [],
                )
            else:
                complete("Full Explain Plans: skipped (no repeating queries)")
                complete("Plan-linked Execution Steps: skipped (no repeating queries)")
        if needs_insights:
            insights = fetch_capped(
                "Insight Ledger",
                f"""
SELECT *
FROM v_insights
WHERE {where}
ORDER BY impact_score DESC, severity
LIMIT 1000
""",
                params,
                1000,
            )
            insights = _enrich_insights(insights)
            family_summary = fetch_df(
                "Issue Families",
                f"""
SELECT *
FROM v_insight_family_summary
WHERE {where}
ORDER BY max_impact DESC NULLS LAST, issue_count DESC
""",
                params,
            )
        if needs_table_risk:
            table_risk = fetch_capped(
                "Table Risk Data",
                f"""
SELECT *
FROM v_table_risk
WHERE {where}
ORDER BY table_risk_score DESC NULLS LAST, size_mb DESC NULLS LAST
LIMIT 500
""",
                params,
                500,
            )
        if needs_table_impact:
            table_impact = fetch_df(
                "Table Impact",
                _fast_table_impact_sql(where, table_review_where),
                params + table_review_params,
            )
        if needs_table_review:
            table_review = fetch_df(
                "Table Review",
                _fast_table_review_sql(where, table_review_where, detail_where),
                params + detail_params + table_review_params,
            )
            if table_review.empty:
                diagnostic, table_review_zero_counts = _table_review_zero_row_diagnostic(
                    con, snapshot_id
                )
                load_errors.append(diagnostic)
        if needs_table_heatmap:
            heatmap_where, heatmap_params = _snapshot_filter(catalog_snapshot_id, namespace_scope)
            table_heatmap = fetch_df(
                "Table Heat Map",
                f"""
SELECT
  snapshot_id,
  namespace_id,
  source_db,
  database_name,
  schema_name,
  table_name,
  table_key,
  size_mb,
  tbl_rows,
  sortkey1,
  unsorted_pct,
  100.0 - LEAST(100.0, GREATEST(0.0, COALESCE(unsorted_pct, 100.0))) AS sorted_pct,
  diststyle,
  skew_rows,
  stats_off
FROM v_table_info
WHERE {heatmap_where}
ORDER BY size_mb DESC NULLS LAST, tbl_rows DESC NULLS LAST
""",
                heatmap_params,
            )
        if needs_external_metadata and _table_is_empty(con, "external_table_metadata"):
            # External capture is opt-in, so this table is usually empty. Skip
            # the fetch AND retire its step, or the dialog parks at "4 of 5"
            # waiting for a step that never reports - indistinguishable from a
            # freeze.
            total_steps = max(1, total_steps - 1)
            needs_external_metadata = False
        if needs_external_metadata:
            producer_namespace = producer_namespace_id()
            external_table_metadata = fetch_df(
                "External Table Metadata (Producer)",
                """
SELECT *
FROM external_table_metadata
WHERE LOWER(
  COALESCE(
    NULLIF(TRIM(CAST(namespace_id AS VARCHAR)), ''),
    ?
  )
) = LOWER(?)
ORDER BY
  redshift_database_name,
  schema_name,
  table_name,
  partition_key_ordinal,
  column_number
""",
                [producer_namespace, producer_namespace],
            )
            notes.append(
                "External table heat map source: Producer-only "
                "SVV_EXTERNAL_COLUMNS metadata."
            )
        if needs_external_tables:
            external_tables = fetch_df(
                "External Tables",
                f"""
SELECT *
FROM v_external_table_info
WHERE {external_where}
ORDER BY gross_scan_bytes DESC NULLS LAST, external_table_key
""",
                external_params,
            )
        if needs_view_definitions:
            view_definitions = fetch_df(
                "View Definitions",
                f"""
SELECT *
FROM v_view_definitions
WHERE {view_where}
ORDER BY database, schema, view_name
""",
                view_params,
            )
        if needs_procedure_definitions:
            procedure_definitions = fetch_df(
                "Procedure Definitions",
                f"""
SELECT *
FROM v_procedure_definitions
WHERE {where}
ORDER BY database, schema, procedure_name
""",
                params,
            )
        if needs_action_plan:
            action_queue = fetch_df(
                "Fast Action Queue",
                _fast_action_queue_sql(where, table_where),
                params + table_params,
            )
            rewrites = fetch_df(
                "Fast Rewrite Opportunities",
                _fast_rewrite_opportunities_sql(where),
                params,
            )
        cluster_names = _cluster_display_names(con)
        (
            slow_queries, query_explain, query_detail_flow, insights, family_summary,
            table_risk, table_impact, table_review, table_heatmap,
            external_table_metadata, external_tables,
            view_definitions, procedure_definitions, action_queue, rewrites,
        ) = tuple(
            _attach_cluster_display_name(frame, cluster_names)
            for frame in (
                slow_queries, query_explain, query_detail_flow, insights, family_summary,
                table_risk, table_impact, table_review, table_heatmap,
                external_table_metadata, external_tables,
                view_definitions, procedure_definitions, action_queue, rewrites,
            )
        )
        summary_dict = _summary_from_loaded_frames(
            slow_queries=slow_queries,
            insights=insights,
            table_risk=table_risk,
            action_queue=action_queue,
            rewrites=rewrites,
        )
        summary_dict.update(table_review_zero_counts)
        if not table_status.empty:
            summary_dict["empty_duckdb_table_count"] = int((table_status["coverage_status"] == "empty").sum())
            summary_dict["partial_duckdb_table_count"] = int((table_status["coverage_status"] == "partial").sum())
            summary_dict["stale_duckdb_table_count"] = int((table_status["coverage_status"] == "stale").sum())
        if not procedure_definitions.empty:
            summary_dict["stored_procedure_count"] = int(len(procedure_definitions))

    repeat_groups = pd.DataFrame()
    repeat_members = pd.DataFrame()
    repeat_group_tables = pd.DataFrame()
    if needs_slow_queries and needs_repeat:
        repeat_threshold = settings.repeat_similarity_threshold
        repeat_prefilter = settings.repeat_prefilter_threshold
        cache_key = _repeat_cache_key(
            snapshot_id, settings, slow_queries, procedure_definitions, table_review
        )
        cached = None
        cache_source = "cache"
        if not slow_queries.empty:
            try:
                # Warehouse connection from the fetch phase is already closed
                # (released so long Python parse/group work does not hold the
                # file lock and freeze other threads / UI status checks).
                with store.connect() as cache_con:
                    loader_data = _read_loader_repeat_data(cache_con, snapshot_id)
                    if loader_data is not None:
                        loader_groups, loader_members = loader_data
                        loader_groups, loader_members = _hydrate_loader_repeat_data(
                            loader_groups, loader_members, slow_queries
                        )
                        loader_groups, loader_group_tables = build_repeat_triage(
                            loader_groups, loader_members, table_review
                        )
                        cached = {
                            "slow_features": pd.DataFrame(),
                            "repeat_groups": loader_groups,
                            "repeat_members": loader_members,
                            "repeat_group_tables": loader_group_tables,
                            "diagnostics": {
                                "repeat_candidates_total": int(len(slow_queries)),
                                "repeat_deterministic_group_count": int(len(loader_groups)),
                                "repeat_grouping_source": "loader",
                            },
                        }
                        cache_source = "loader"
                    else:
                        cached = _read_repeat_cache(cache_con, cache_key)
            except Exception:
                cached = None
        if cached is not None:
            # A prepared/cached result with ZERO groups while queries exist is
            # untrustworthy (stale or broken loader materialization): discard
            # it and run fresh grouping instead of presenting an empty triage.
            cached_groups = cached.get("repeat_groups")
            if (cached_groups is None or cached_groups.empty) and not slow_queries.empty:
                notes.append(
                    "Prepared repeat grouping was empty; rebuilt fresh from the "
                    "captured queries instead."
                )
                cached = None
        if cached is not None:
            announce(
                "Loading repeat-query analysis prepared by the loader"
                if cache_source == "loader"
                else "Loading repeat-query analysis from cache"
            )
            summary_dict["triage_sql_feature_cache_hits"] = int(len(slow_queries))
            summary_dict["triage_sql_feature_cache_misses"] = 0
            features = cached.get("slow_features")
            if features is not None and not features.empty and "query_id" in features.columns:
                features = features.copy()
                features["__qid"] = features["query_id"].astype(str)
                merge_cols = [
                    c for c in features.columns
                    if c not in {"query_id", "__qid"} and c not in slow_queries.columns
                ]
                if merge_cols:
                    slow_queries = slow_queries.copy()
                    slow_queries["__qid"] = slow_queries["query_id"].astype(str)
                    slow_queries = slow_queries.merge(
                        features[merge_cols + ["__qid"]].drop_duplicates("__qid"),
                        on="__qid",
                        how="left",
                    ).drop(columns=["__qid"])
            complete(
                "Parsed SQL shapes: prepared during data loading"
                if cache_source == "loader"
                else "Parsed SQL shapes: reused cached analysis"
            )
            repeat_groups = cached.get("repeat_groups", pd.DataFrame())
            repeat_members = cached.get("repeat_members", pd.DataFrame())
            complete(
                f"Grouped repeated patterns: {len(repeat_groups):,} group(s) "
                f"({'loader' if cache_source == 'loader' else 'cached'})"
            )
            repeat_group_tables = cached.get("repeat_group_tables", pd.DataFrame())
            complete(
                f"Joined table health for {len(repeat_group_tables):,} group-table link(s) "
                f"({'loader' if cache_source == 'loader' else 'cached'})"
            )
            summary_dict.update(cached.get("diagnostics") or {})
            complete(
                "Diagnosed repeat grouping coverage (loader)"
                if cache_source == "loader"
                else "Diagnosed repeat grouping coverage (cached)"
            )
        else:
          # DISASTER GUARD: a failure anywhere in repeat analysis must degrade
          # to "queries without groups" — never abort the whole load again.
          try:
            announce("Parsing SQL shapes for repeat-query analysis")
            feature_base_cols = set(slow_queries.columns)
            # warehouse_con=None: parse checkpoints only the external sidecar so
            # this multi-hour phase does not hold redshift.duckdb open.
            slow_queries, feature_cache_hits, feature_cache_misses = _enrich_with_incremental_sql_cache(
                store, slow_queries, progress=announce, warehouse_con=None
            )
            summary_dict["triage_sql_feature_cache_hits"] = feature_cache_hits
            summary_dict["triage_sql_feature_cache_misses"] = feature_cache_misses
            complete(
                f"Parsed SQL shapes: reused {feature_cache_hits:,} cached row(s); "
                f"examined {feature_cache_misses:,} new or changed SQL shape(s)"
            )
            announce("Grouping repeated query patterns")
            repeat_groups, repeat_members = build_repeat_query_report(
                slow_queries,
                threshold=repeat_threshold,
                prefilter_threshold=repeat_prefilter,
                procedure_definitions=procedure_definitions,
                scope_by_user=settings.repeat_scope_by_user,
                min_group_size=settings.repeat_min_group_size,
                fuzzy_merge_threshold=settings.repeat_fuzzy_merge_threshold,
                progress=announce,
            )
            complete(f"Grouped repeated patterns: {len(repeat_groups):,} group(s)")
            announce("Joining repeat groups to table health")
            repeat_groups, repeat_group_tables = build_repeat_triage(
                repeat_groups, repeat_members, table_review
            )
            complete(f"Joined table health for {len(repeat_group_tables):,} group-table link(s)")
            announce("Diagnosing repeat grouping coverage")
            repeat_diagnostics = diagnose_repeat_query_candidates(
                slow_queries,
                threshold=repeat_threshold,
                prefilter_threshold=repeat_prefilter,
                procedure_definitions=procedure_definitions,
                scope_by_user=settings.repeat_scope_by_user,
                min_group_size=settings.repeat_min_group_size,
                fuzzy_merge_threshold=settings.repeat_fuzzy_merge_threshold,
                progress=announce,
            )
            summary_dict.update(repeat_diagnostics)
            complete("Diagnosed repeat grouping coverage")
            if not slow_queries.empty and "query_id" in slow_queries.columns:
                feature_cols = [c for c in slow_queries.columns if c not in feature_base_cols]
                features_frame = (
                    slow_queries[["query_id"] + feature_cols].copy() if feature_cols else pd.DataFrame()
                )
                try:
                    with store.connect() as cache_con:
                        _write_repeat_cache(
                            cache_con,
                            cache_key,
                            {
                                "slow_features": features_frame,
                                "repeat_groups": repeat_groups,
                                "repeat_members": repeat_members,
                                "repeat_group_tables": repeat_group_tables,
                            },
                            repeat_diagnostics,
                        )
                except Exception:
                    pass
          except Exception as exc:
            repeat_groups = pd.DataFrame()
            repeat_members = pd.DataFrame()
            repeat_group_tables = pd.DataFrame()
            load_errors.append(f"Repeat Grouping: {exc}")
            notes.append(
                "Repeat grouping failed and was skipped; captured queries are "
                "shown without pattern groups. See the error log."
            )
            complete("Repeat grouping failed; continuing without groups")
        announce("Merging repeat metadata into slow queries")
        # Loader/cache round-trips can deliver numeric columns as VARCHAR (or
        # mixed). Unify BEFORE any merge or sum — mixed int/str columns crash
        # with "unsupported operand type(s) for +: 'int' and 'str'".
        repeat_groups = _coerce_repeat_numeric_columns(repeat_groups)
        repeat_members = _coerce_repeat_numeric_columns(repeat_members)
        repeat_group_tables = _coerce_repeat_numeric_columns(repeat_group_tables)
        summary_dict["repeat_similarity_threshold"] = repeat_threshold
        summary_dict["repeat_prefilter_threshold"] = repeat_prefilter
        if not repeat_members.empty and "query_id" in slow_queries.columns:
            repeat_meta = repeat_members[
                ["query_id", "repeat_group_id", "similarity_score"]
            ].rename(columns={"similarity_score": "repeat_similarity_score"})
            group_meta = repeat_groups[
                ["repeat_group_id", "query_count", "total_runtime_s"]
            ].rename(
                columns={
                    "query_count": "repeat_group_size",
                    "total_runtime_s": "repeat_group_runtime_s",
                }
            )
            repeat_meta = repeat_meta.merge(group_meta, on="repeat_group_id", how="left")
            # DuckDB may infer query_id as numeric in v_slow_queries while the
            # loader deliberately stores portable VARCHAR ids. Join on an
            # explicit string bridge so precomputed groups work with either.
            slow_queries = slow_queries.copy()
            slow_queries["__repeat_qid"] = slow_queries["query_id"].astype(str)
            repeat_meta = repeat_meta.copy()
            repeat_meta["__repeat_qid"] = repeat_meta["query_id"].astype(str)
            repeat_meta.drop(columns=["query_id"], inplace=True)
            slow_queries = slow_queries.merge(
                repeat_meta, on="__repeat_qid", how="left"
            ).drop(columns=["__repeat_qid"])
        else:
            slow_queries["repeat_group_id"] = ""
            slow_queries["repeat_similarity_score"] = 0.0
            slow_queries["repeat_group_size"] = 0
            slow_queries["repeat_group_runtime_s"] = 0.0
        complete(f"Repeat-query analysis complete: {len(repeat_groups):,} group(s)")
    # Tag each group as mixed / external-only / local-only using the unified
    # SVV_EXTERNAL_COLUMNS metadata. Legacy catalog files remain readable.
    if not repeat_groups.empty:
        if _table_exists(con, "external_table_metadata"):
            external_catalog = fetch_df(
                "External Table Metadata",
                'SELECT * FROM external_table_metadata',
                [],
            )
        elif _table_exists(con, "external_tables_catalog"):
            external_catalog = fetch_df(
                "Legacy External Tables Catalog",
                'SELECT * FROM external_tables_catalog',
                [],
            )
        else:
            external_catalog = pd.DataFrame()
        from .mixed_query import annotate_mixed_queries, annotate_view_usage

        repeat_groups = annotate_mixed_queries(repeat_groups, external_catalog)
        summary_dict["mixed_query_group_count"] = int(
            (repeat_groups.get("mixed_query_class") == "mixed").sum()
        ) if "mixed_query_class" in repeat_groups.columns else 0
        # Flag groups that reference a view, using the loaded view names as an
        # in-memory set (no SQL re-parsing). Read only the identity columns of
        # view_definitions (not the huge SQL bodies) so ~6,000 views cost
        # almost nothing; the set + membership check is instant.
        view_names = fetch_df(
            "View Names",
            'SELECT DISTINCT database, schema, view_name FROM view_definitions',
            [],
        ) if _table_exists(con, "view_definitions") else pd.DataFrame()
        repeat_groups = annotate_view_usage(repeat_groups, view_names)
        summary_dict["view_using_group_count"] = int(
            pd.to_numeric(repeat_groups.get("uses_view"), errors="coerce").fillna(0).sum()
        ) if "uses_view" in repeat_groups.columns else 0
    summary_dict["repeat_group_count"] = len(repeat_groups)
    summary_dict["repeat_query_count"] = (
        int(pd.to_numeric(repeat_groups["query_count"], errors="coerce").fillna(0).sum())
        if not repeat_groups.empty else 0
    )
    summary_dict["repeat_runtime_s"] = (
        float(pd.to_numeric(repeat_groups["total_runtime_s"], errors="coerce").fillna(0).sum())
        if not repeat_groups.empty else 0.0
    )

    if snapshot_id is None:
        notes.append("No snapshot metadata found; showing any rows present in DuckDB.")
    if load_errors:
        notes.append(f"{len(load_errors)} analytics panel(s) failed to load; open the Table Review error log.")
    return ClusterReport(
        db_path=store.path,
        snapshot_id=snapshot_id,
        summary=summary_dict,
        slow_queries=slow_queries,
        query_explain=query_explain,
        query_detail_flow=query_detail_flow,
        insights=insights,
        family_summary=family_summary,
        table_risk=table_risk,
        table_impact=table_impact,
        table_review=table_review,
        table_heatmap=table_heatmap,
        external_table_metadata=external_table_metadata,
        external_tables=external_tables,
        view_definitions=view_definitions,
        procedure_definitions=procedure_definitions,
        table_status=table_status,
        action_queue=action_queue,
        rewrites=rewrites,
        repeat_groups=repeat_groups,
        repeat_members=repeat_members,
        repeat_group_tables=repeat_group_tables,
        flow_edges=flow_edges,
        query_heatmap=query_heatmap,
        analysis_namespace_scope=namespace_scope,
        loaded_areas=tuple(sorted(selected_areas)),
        notes=tuple(notes),
        load_errors=tuple(load_errors),
    )


# Same-user prefix/suffix width for the repeat-shape match: two queries from
# one user whose first and last characters agree are treated as the same
# repeating pattern (parameterized variants of one template).
_REPEAT_AFFIX_CHARS = 64

# Administrator-adjustable per cluster via FLOOR_SECONDS in the portable
# cluster-profiles JSON; fallbacks by role when it is not set.
_ANALYSIS_FLOOR_PRODUCER_SECONDS = 300.0
_ANALYSIS_FLOOR_CONSUMER_SECONDS = 30.0


def _configured_namespace_floors() -> dict[str, float]:
    """Per-cluster minimum query cutoff, keyed by lowercase namespace id."""
    floors: dict[str, float] = {}
    try:
        from .topology import configured_profiles

        for profile in configured_profiles():
            namespace = str(profile.get("namespace_id") or "").strip().lower()
            if not namespace:
                continue
            role = str(profile.get("role") or "consumer").strip().lower()
            default = (
                _ANALYSIS_FLOOR_PRODUCER_SECONDS
                if role == "producer"
                else _ANALYSIS_FLOOR_CONSUMER_SECONDS
            )
            prefix = str(profile.get("prefix") or "").strip()
            raw = str(os.environ.get(f"{prefix}_FLOOR_SECONDS") or "").strip() if prefix else ""
            if not raw and role == "producer":
                raw = str(os.environ.get("REDSHIFT_FLOOR_SECONDS") or "").strip()
            try:
                floors[namespace] = float(raw) if raw else default
            except ValueError:
                floors[namespace] = default
    except Exception:
        return {}
    return floors


def _namespace_floor_filter() -> tuple[str, list[object]]:
    """SQL predicate applying each configured cluster's minimum-seconds cutoff.

    Unconfigured namespaces (mock files, tests, ad-hoc loads) are not cut.
    """
    floors = _configured_namespace_floors()
    if not floors:
        return "TRUE", []
    cases = " ".join("WHEN ? THEN ?" for _ in floors)
    clause = (
        "COALESCE(elapsed_s, execution_s, 0) >= "
        f"CASE LOWER(COALESCE(NULLIF(TRIM(CAST(namespace_id AS VARCHAR)), ''), 'producer')) "
        f"{cases} ELSE 0 END"
    )
    params: list[object] = []
    for namespace, seconds in floors.items():
        params.extend([namespace, float(seconds)])
    return clause, params


def _slow_query_view_columns(con) -> set[str]:
    try:
        return {str(row[0]).strip().lower() for row in con.execute("DESCRIBE v_slow_queries").fetchall()}
    except Exception:
        return set()


def _repeating_slow_queries_sql(where: str, floor_clause: str, columns: set[str]) -> str:
    """Repeat-only workload selection, computed entirely inside DuckDB.

    A query survives only when it repeats: identical normalized SQL from any
    user, the same user with matching leading/trailing characters, or the
    same SYS query hash when a hash column is captured. One-off queries are
    dropped here so they never reach the Python/sqlglot analysis stages.

    generic_query_hash is preferred: SYS_QUERY_HISTORY computes it over the
    normalized statement with literals stripped, so parameterized repeats
    group across users even when their SQL text differs.
    """
    hash_key = next(
        (k for k in ("generic_query_hash", "user_query_hash", "query_hash") if k in columns),
        "",
    )
    hash_window = ""
    hash_predicate = ""
    hash_exclude = ""
    if hash_key:
        hash_window = (
            ",\n    COUNT(*) OVER (PARTITION BY NULLIF(TRIM(CAST("
            f"{hash_key} AS VARCHAR)), '')) AS _hash_repeats"
        )
        hash_predicate = (
            f" OR (NULLIF(TRIM(CAST({hash_key} AS VARCHAR)), '') IS NOT NULL "
            "AND _hash_repeats > 1)"
        )
        hash_exclude = ", _hash_repeats"
    return f"""
WITH scoped AS (
  SELECT *
  FROM v_slow_queries
  WHERE {where}
    AND {floor_clause}
),
keyed AS (
  SELECT
    *,
    UPPER(REGEXP_REPLACE(TRIM(COALESCE(CAST(sql_text AS VARCHAR), '')), '\\s+', ' ', 'g')) AS _norm_sql
  FROM scoped
),
counted AS (
  SELECT
    *,
    COUNT(*) OVER (PARTITION BY _norm_sql) AS _exact_repeats,
    COUNT(*) OVER (
      PARTITION BY
        LOWER(COALESCE(CAST(user_name AS VARCHAR), '')),
        LEFT(_norm_sql, {_REPEAT_AFFIX_CHARS}),
        RIGHT(_norm_sql, {_REPEAT_AFFIX_CHARS})
    ) AS _shape_repeats{hash_window}
  FROM keyed
)
SELECT * EXCLUDE (_norm_sql, _exact_repeats, _shape_repeats{hash_exclude})
FROM counted
WHERE _norm_sql <> ''
  AND (_exact_repeats > 1
       -- The affix-shape rule means "the SAME user repeating a pattern";
       -- blank/NULL users all share one pseudo-user, so for them the loose
       -- first/last-64-char match must not count as a repeat on its own.
       OR (_shape_repeats > 1
           AND NULLIF(TRIM(CAST(user_name AS VARCHAR)), '') IS NOT NULL){hash_predicate})
ORDER BY risk_score DESC NULLS LAST, elapsed_s DESC NULLS LAST
"""


def _snapshot_filter(
    snapshot_id: str | None,
    namespace_scope: Iterable[str] = (),
    *,
    alias: str = "",
) -> tuple[str, list[str]]:
    prefix = f"{alias}." if alias else ""
    clauses: list[str] = []
    params: list[str] = []
    if snapshot_id:
        clauses.append(f"{prefix}snapshot_id = ?")
        params.append(snapshot_id)
    namespaces = [str(value).strip() for value in namespace_scope if str(value).strip()]
    if namespaces:
        placeholders = ", ".join("?" for _ in namespaces)
        clauses.append(f"{prefix}namespace_id IN ({placeholders})")
        params.extend(namespaces)
    return " AND ".join(clauses) if clauses else "TRUE", params


def _cluster_display_names(con) -> dict[str, str]:
    result: dict[str, str] = {}
    try:
        for namespace_id, cluster_name in con.execute(
            """
SELECT namespace_id, ANY_VALUE(cluster_name)
FROM snapshot_cluster_runs
WHERE NULLIF(TRIM(namespace_id), '') IS NOT NULL
GROUP BY namespace_id
"""
        ).fetchall():
            if namespace_id and cluster_name and str(cluster_name).strip():
                result[str(namespace_id).strip().lower()] = str(cluster_name).strip()
    except Exception:
        pass
    consumer_ordinals = sorted({
        int(match.group(1))
        for key in os.environ
        if (match := re.match(r"^REDSHIFT_CONSUMER_(\d+)_", str(key).upper()))
    })
    for ordinal in (0, *consumer_ordinals):
        if ordinal == 0:
            prefix = "REDSHIFT_PRODUCER"
            namespace_id = (
                os.environ.get("REDSHIFT_NAMESPACE")
                or os.environ.get(f"{prefix}_NAMESPACE_ID")
                or os.environ.get("REDSHIFT_NAMESPACE_ID")
            )
        else:
            prefix = f"REDSHIFT_CONSUMER_{ordinal}"
            namespace_id = os.environ.get(f"{prefix}_NAMESPACE_ID")
        display_name = (
            (os.environ.get("REDSHIFT_FRIENDLY") if ordinal == 0 else os.environ.get(f"{prefix}_FRIENDLY"))
            or os.environ.get(f"{prefix}_DISPLAY_NAME")
            or (os.environ.get("REDSHIFT_ENV") if ordinal == 0 else "")
        )
        if namespace_id and display_name and str(display_name).strip():
            result[str(namespace_id).strip().lower()] = str(display_name).strip()
    return result


def _attach_cluster_display_name(frame: pd.DataFrame, names: dict[str, str]) -> pd.DataFrame:
    if frame is None or frame.empty or "namespace_id" not in frame.columns:
        return frame
    result = frame.copy()
    namespace = result["namespace_id"].fillna("").astype(str).str.strip()
    result["cluster_name"] = namespace.str.lower().map(names).fillna(namespace)
    columns = list(result.columns)
    columns.remove("cluster_name")
    insert_at = columns.index("namespace_id") + 1
    columns.insert(insert_at, "cluster_name")
    return result[columns]


def _resolve_catalog_snapshot(con, requested_snapshot_id: str | None) -> str | None:
    """Return the usable table-catalog snapshot, or None to use all rows.

    This is intentionally independent of the query snapshot. A selective
    query repair must not make an older physical catalog invisible.
    """
    try:
        total = int(con.execute("SELECT COUNT(*) FROM svv_table_info_all").fetchone()[0] or 0)
        if total <= 0:
            return requested_snapshot_id
        if requested_snapshot_id:
            exact = int(
                con.execute(
                    "SELECT COUNT(*) FROM svv_table_info_all WHERE CAST(snapshot_id AS VARCHAR) = ?",
                    [requested_snapshot_id],
                ).fetchone()[0]
                or 0
            )
            if exact > 0:
                return str(requested_snapshot_id)
        row = con.execute(
            """
SELECT CAST(snapshot_id AS VARCHAR) AS snapshot_id, COUNT(*) AS row_count
FROM svv_table_info_all
WHERE NULLIF(TRIM(CAST(snapshot_id AS VARCHAR)), '') IS NOT NULL
GROUP BY CAST(snapshot_id AS VARCHAR)
ORDER BY row_count DESC, snapshot_id
LIMIT 1
"""
        ).fetchone()
        return str(row[0]) if row and row[0] is not None else None
    except Exception:
        # The installed views can still expose the rows even when a legacy raw
        # table lacks snapshot metadata; fail open for Table Review.
        return None


def _resolve_table_snapshot(con, table_name: str, requested_snapshot_id: str | None) -> str | None:
    """Resolve a catalog table independently from a newer query-only repair snapshot."""
    if table_name not in {"view_definitions", "procedure_definitions", "external_table_info_all"}:
        raise ValueError(f"Unsupported catalog table: {table_name}")
    try:
        total = int(con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0] or 0)
        if total <= 0:
            return requested_snapshot_id
        if requested_snapshot_id:
            exact = int(
                con.execute(
                    f'SELECT COUNT(*) FROM "{table_name}" WHERE CAST(snapshot_id AS VARCHAR) = ?',
                    [requested_snapshot_id],
                ).fetchone()[0] or 0
            )
            if exact > 0:
                return str(requested_snapshot_id)
        row = con.execute(
            f'''SELECT CAST(snapshot_id AS VARCHAR), COUNT(*) AS row_count
                FROM "{table_name}"
                WHERE NULLIF(TRIM(CAST(snapshot_id AS VARCHAR)), '') IS NOT NULL
                GROUP BY CAST(snapshot_id AS VARCHAR)
                ORDER BY row_count DESC, 1
                LIMIT 1'''
        ).fetchone()
        return str(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def _table_review_zero_row_diagnostic(
    con,
    snapshot_id: str | None,
) -> tuple[str, dict[str, int]]:
    """Count Table Review's physical sources after its derived query returns no rows."""

    counts: dict[str, int] = {}
    failures: list[str] = []

    def count_rows(
        table_name: str,
        *,
        selected_snapshot: bool = False,
        optional: bool = False,
    ) -> int | None:
        try:
            if optional:
                exists = con.execute(
                    """
SELECT 1
FROM information_schema.tables
WHERE table_schema = current_schema()
  AND table_name = ?
LIMIT 1
""",
                    [table_name],
                ).fetchone()
                if not exists:
                    return None
            if selected_snapshot and snapshot_id:
                row = con.execute(
                    f'SELECT COUNT(*) FROM "{table_name}" WHERE snapshot_id = ?',
                    [snapshot_id],
                ).fetchone()
            else:
                row = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
            return int((row or [0])[0] or 0)
        except Exception as exc:
            failures.append(f"{table_name}: {exc}")
            return None

    source_rows = count_rows("svv_table_info_all")
    if source_rows is not None:
        counts["table_review_direct_source_rows"] = source_rows

    selected_rows = None
    if snapshot_id:
        selected_rows = count_rows("svv_table_info_all", selected_snapshot=True)
        if selected_rows is not None:
            counts["table_review_direct_selected_snapshot_rows"] = selected_rows

    staging_rows = count_rows("svv_table_info_all_tmp", optional=True)
    if staging_rows is not None:
        counts["table_review_direct_staging_rows"] = staging_rows

    parts: list[str] = []
    if source_rows is not None:
        parts.append(f"svv_table_info_all has {source_rows:,} total row(s)")
    if snapshot_id and selected_rows is not None:
        parts.append(
            f"{selected_rows:,} row(s) match selected snapshot {snapshot_id}"
        )
    if staging_rows is not None:
        parts.append(f"svv_table_info_all_tmp has {staging_rows:,} staged row(s)")
    if failures:
        parts.append("count failure(s): " + " | ".join(failures))

    if source_rows and selected_rows == 0:
        conclusion = (
            "Source data exists, but the selected snapshot is not present in the promoted table; "
            "check for an unswapped *_tmp load."
        )
    elif source_rows:
        conclusion = (
            "Source data exists for the direct table check, so the derived Table Review query/load "
            "path needs attention."
        )
    elif source_rows == 0:
        conclusion = "The promoted source table also contains zero rows."
    else:
        conclusion = "The direct source count could not be completed."
    details = "; ".join(parts) or "no source counts were available"
    return (
        f"Table Review returned 0 rows. Direct DuckDB source check: {details}. {conclusion}",
        counts,
    )


def _normalize_report_areas(areas: Iterable[str] | None) -> set[str]:
    if areas is None:
        return set(_SAFE_REPORT_AREAS)
    if isinstance(areas, str):
        areas = (areas,)
    selected = {str(area).strip() for area in areas if str(area).strip()}
    if not selected or "all" in selected:
        return set(_SAFE_REPORT_AREAS)
    valid = selected & _VALID_REPORT_AREAS
    return valid or {"status"}


def _missing_sortkey_sql(expr: str) -> str:
    normalized = (
        f"REGEXP_REPLACE(UPPER(TRIM(COALESCE(CAST({expr} AS VARCHAR), ''))), "
        "'\\s+', '', 'g')"
    )
    return (
        f"{normalized} IN ("
        "'', '-', 'NONE', 'NULL', 'NAN', 'SORTKEY', '(SORTKEY)', "
        "'AUTO(SORTKEY)', '(AUTO(SORTKEY))'"
        ")"
    )


_INSIGHT_METRIC_LABELS = {
    "Q01_REMOTE_SPILL": "Spill Blocks",
    "Q02_EXTERNAL_SPILL": "External Spill Blocks",
    "Q03_REMOTE_IO_RATIO": "Remote I/O Ratio",
    "Q04_DATA_SKEW_STEP": "Data Skew",
    "Q05_TIME_SKEW_STEP": "Time Skew",
    "Q06_BROADCAST_JOIN": "Broadcast Count",
    "Q07_DS_DIST_BOTH": "DS_DIST_BOTH Count",
    "Q08_DIST_HEAVY": "Redistribution Count",
    "Q09_NESTED_LOOP": "Nested Loop Flag",
    "Q10_MISSING_STATS_PLAN": "Missing Stats Flag",
    "Q11_S3_SCAN": "S3 Scan Count",
    "Q12_EXTERNAL_SHARE": "External Runtime Share",
    "Q13_EXTERNAL_LOW_SELECTIVITY": "External Selectivity",
    "Q14_SEQ_SCAN_HEAVY": "Sequential Scan Count",
    "Q15_PARTITION_LOOP": "Partition Loop Count",
    "Q16_NETWORK_NODE": "Network Node Count",
    "Q17_HIGH_COST_SCORE": "Plan Cost Score",
    "Q18_ALERT_COUNT": "Planner Alert Count",
    "Q19_ROW_EXPANSION": "Row Expansion Ratio",
    "Q20_INPUT_WASTE": "Input Rows",
    "Q21_SELECT_STAR": "SQL Shape Flag",
    "Q22_LEADING_WILDCARD": "SQL Shape Flag",
    "Q23_UNION_DISTINCT": "SQL Shape Flag",
    "Q24_CROSS_JOIN": "SQL Shape Flag",
    "Q25_ORDER_BY_NO_LIMIT": "SQL Shape Flag",
    "T26_TABLE_SKEW": "Skew Rows",
    "T27_UNSORTED": "Unsorted Percent",
    "T28_STATS_OFF": "Stats Off Percent",
    "T29_VACUUM_BENEFIT": "Vacuum Sort Benefit",
    "T30_LARGE_ALL_TABLE": "Table Size MB",
    "T31_LARGE_EVEN_TABLE": "Table Rows",
    "T32_NO_SORTKEY_LARGE": "Table Size MB",
}


_PERCENT_METRIC_IDS = {
    "Q03_REMOTE_IO_RATIO",
    "Q12_EXTERNAL_SHARE",
    "Q13_EXTERNAL_LOW_SELECTIVITY",
    "T27_UNSORTED",
    "T28_STATS_OFF",
    "T29_VACUUM_BENEFIT",
}


def _enrich_insights(insights: pd.DataFrame) -> pd.DataFrame:
    if insights is None or insights.empty:
        return insights if insights is not None else pd.DataFrame()
    out = insights.copy()
    if "scope" not in out.columns:
        out["scope"] = ""
    if "insight_id" not in out.columns:
        out["insight_id"] = ""
    out["target_type"] = out.apply(_insight_target_type, axis=1)
    out["target_label"] = out.apply(_insight_target_label, axis=1)
    out["metric_label"] = out["insight_id"].map(_INSIGHT_METRIC_LABELS).fillna("Observed Value")
    out["metric_display"] = out.apply(_insight_metric_display, axis=1)
    out["impact_band"] = out["impact_score"].map(_impact_band)
    return out


def _insight_target_type(row: pd.Series) -> str:
    scope = str(row.get("scope") or "").lower()
    if scope == "query" or pd.notna(row.get("query_id")):
        return "Query ID"
    if scope == "table" or str(row.get("table_key") or "").strip():
        return "Table / Object"
    return "Cluster"


def _insight_target_label(row: pd.Series) -> str:
    target_type = _insight_target_type(row)
    if target_type == "Query ID":
        query_id = row.get("query_id")
        try:
            if pd.notna(query_id):
                return f"Query ID {int(float(query_id))}"
        except (TypeError, ValueError):
            pass
    subject = str(row.get("subject") or "").strip()
    return subject or target_type


def _insight_metric_display(row: pd.Series) -> str:
    value = row.get("metric_value")
    insight_id = str(row.get("insight_id") or "")
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value or "-")
    if pd.isna(number):
        return "-"
    if insight_id in _PERCENT_METRIC_IDS:
        pct = number * 100 if abs(number) <= 1 else number
        return f"{pct:.0f}%"
    if "FLAG" in str(row.get("metric_label") or "").upper():
        return "Yes" if number else "No"
    if abs(number) >= 1_000_000_000:
        return f"{number / 1_000_000_000:.1f}B"
    if abs(number) >= 1_000_000:
        return f"{number / 1_000_000:.1f}M"
    if abs(number) >= 10_000:
        return f"{number:,.0f}"
    if number.is_integer():
        return f"{number:,.0f}"
    return f"{number:,.2f}".rstrip("0").rstrip(".")


def _impact_band(value: object) -> str:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return "Unscored"
    if pd.isna(score):
        return "Unscored"
    if score >= 95:
        return "Critical"
    if score >= 80:
        return "High"
    if score >= 60:
        return "Medium"
    return "Low"


def _fast_table_impact_sql(where: str, table_where: str) -> str:
    return f"""
WITH {_fast_table_impact_ctes(where, table_where)}
SELECT *
FROM impact
ORDER BY blast_radius_score DESC NULLS LAST, slow_query_count DESC
LIMIT 500
"""


def _fast_table_review_sql(where: str, table_where: str, detail_where: str | None = None) -> str:
    """Fast Table Review path: physical inventory plus captured scan telemetry.

    The older path performed an all-pairs SQL-text/table-name match before the
    grid could open. On large snapshots that took many minutes and was also
    already available separately in the opt-in Table Impact area.
    """
    risk_score = f"""
      CASE WHEN COALESCE(t.skew_rows, 0) >= 3 THEN LEAST(COALESCE(t.skew_rows, 0) * 8, 40) ELSE 0 END
      + CASE WHEN COALESCE(t.unsorted_pct, 0) >= 20 THEN LEAST(COALESCE(t.unsorted_pct, 0) / 2, 30) ELSE 0 END
      + CASE WHEN COALESCE(t.stats_off, 0) >= 10 THEN LEAST(COALESCE(t.stats_off, 0) / 2, 30) ELSE 0 END
      + CASE WHEN COALESCE(t.vacuum_sort_benefit, 0) >= 10 THEN LEAST(COALESCE(t.vacuum_sort_benefit, 0), 20) ELSE 0 END
      + CASE WHEN UPPER(COALESCE(t.diststyle, '')) LIKE 'ALL%' AND COALESCE(t.size_mb, 0) >= 1024 THEN 25 ELSE 0 END
      + CASE WHEN {_missing_sortkey_sql("t.sortkey1")} AND COALESCE(t.size_mb, 0) >= 1024 THEN 20 ELSE 0 END
    """
    detail_where = detail_where or where
    return f"""
WITH scan AS (
  SELECT
    snapshot_id,
    namespace_id,
    table_key,
    SUM(COALESCE(scan_query_count, 0)) AS scan_query_count,
    SUM(COALESCE(scan_duration_s, 0)) AS scan_duration_s,
    SUM(COALESCE(scan_input_rows_m, 0)) AS scan_input_rows_m,
    SUM(COALESCE(scan_output_rows_m, 0)) AS scan_output_rows_m,
    SUM(COALESCE(rrscan_query_count, 0)) AS rrscan_query_count,
    SUM(COALESCE(non_rrscan_query_count, 0)) AS non_rrscan_query_count
  FROM v_table_scan_info
  WHERE {where}
  GROUP BY snapshot_id, namespace_id, table_key
), detail_tables AS (
  SELECT DISTINCT
    f.snapshot_id,
    f.namespace_id,
    f.query_id,
    f.table_id
  FROM v_query_detail_flow f
  WHERE {detail_where}
    AND f.query_id IS NOT NULL
    AND f.table_id IS NOT NULL
), telemetry AS (
  SELECT
    d.namespace_id,
    d.table_id,
    COUNT(DISTINCT d.query_id) AS slow_query_count,
    SUM(COALESCE(q.elapsed_s, 0)) AS slow_query_runtime_s,
    COUNT(DISTINCT CASE
      WHEN COALESCE(q.dist_both_cnt, 0) > 0 OR COALESCE(q.dist_total_cnt, 0) >= 2
      THEN d.query_id END) AS redistribution_query_count,
    COUNT(DISTINCT CASE
      WHEN COALESCE(q.bcast_cnt, 0) > 0
      THEN d.query_id END) AS broadcast_query_count,
    COUNT(DISTINCT CASE
      WHEN COALESCE(q.max_data_skewness, 0) >= 4 OR COALESCE(q.max_time_skewness, 0) >= 4
      THEN d.query_id END) AS skewed_query_count
  FROM detail_tables d
  LEFT JOIN v_slow_queries q
    ON q.snapshot_id IS NOT DISTINCT FROM d.snapshot_id
   AND q.namespace_id IS NOT DISTINCT FROM d.namespace_id
   AND q.query_id = d.query_id
  GROUP BY d.namespace_id, d.table_id
), base AS (
  SELECT t.*, ({risk_score}) AS table_risk_score
  FROM v_table_info t
  WHERE {table_where}
)
SELECT
  t.snapshot_id,
  t.namespace_id,
  t.source_db,
  t.schema_name,
  t.table_name,
  t.table_key,
  t.table_id,
  t.diststyle,
  t.sortkey1,
  t.size_mb,
  t.tbl_rows,
  t.unsorted_pct,
  t.stats_off,
  t.skew_rows,
  t.vacuum_sort_benefit,
  t.risk_event,
  CAST(GREATEST(COALESCE(x.slow_query_count, 0), COALESCE(s.scan_query_count, 0)) AS BIGINT) AS slow_query_count,
  COALESCE(x.slow_query_runtime_s, 0) AS slow_query_runtime_s,
  CAST(COALESCE(x.redistribution_query_count, 0) AS BIGINT) AS redistribution_query_count,
  CAST(COALESCE(x.broadcast_query_count, 0) AS BIGINT) AS broadcast_query_count,
  CAST(COALESCE(x.skewed_query_count, 0) AS BIGINT) AS skewed_query_count,
  COALESCE(s.scan_query_count, 0) AS scan_query_count,
  COALESCE(s.scan_duration_s, 0) AS scan_duration_s,
  CASE WHEN COALESCE(s.scan_query_count, 0) > 0
    THEN COALESCE(s.scan_duration_s, 0) / s.scan_query_count ELSE 0 END AS avg_scan_duration_s,
  COALESCE(s.scan_input_rows_m, 0) AS scan_input_rows_m,
  COALESCE(s.scan_output_rows_m, 0) AS scan_output_rows_m,
  COALESCE(s.rrscan_query_count, 0) AS rrscan_query_count,
  COALESCE(s.non_rrscan_query_count, 0) AS non_rrscan_query_count,
  CASE WHEN COALESCE(s.scan_query_count, 0) > 0
    THEN s.rrscan_query_count / s.scan_query_count ELSE 0 END AS rrscan_query_pct,
  CASE WHEN COALESCE(s.scan_query_count, 0) > 0
    THEN s.non_rrscan_query_count / s.scan_query_count ELSE 0 END AS full_scan_query_pct,
  CASE WHEN {_missing_sortkey_sql("t.sortkey1")} OR COALESCE(s.scan_query_count, 0) = 0 THEN 0
    ELSE ROUND(100.0 * s.rrscan_query_count / s.scan_query_count, 1) END AS sort_key_usage_score,
  LEAST(
    CASE WHEN COALESCE(s.scan_query_count, 0) > 0 THEN s.non_rrscan_query_count / s.scan_query_count ELSE 0 END * 60
      + LEAST(COALESCE(s.non_rrscan_query_count, 0) * 6, 30)
      + LEAST(COALESCE(s.scan_input_rows_m, 0) / 100.0, 30), 120
  ) AS full_scan_score,
  LEAST(LEAST(COALESCE(t.skew_rows, 0) * 6, 35), 120) AS distribution_usage_score,
  LEAST(
    CASE WHEN {_missing_sortkey_sql("t.sortkey1")} AND COALESCE(s.scan_query_count, 0) > 0 THEN 30 ELSE 0 END
      + COALESCE(t.unsorted_pct, 0) * 0.7
      + COALESCE(t.vacuum_sort_benefit, 0) * 0.7
      + CASE WHEN COALESCE(s.scan_query_count, 0) > 0
          THEN (1 - s.rrscan_query_count / s.scan_query_count) * 35 ELSE 0 END, 120
  ) AS sort_attention_score,
  LEAST(
    t.table_risk_score
      + LEAST(COALESCE(s.scan_duration_s, 0) / 3600.0, 30)
      + LEAST(COALESCE(s.non_rrscan_query_count, 0) * 4, 30)
      + LEAST(COALESCE(s.scan_input_rows_m, 0) / 120.0, 25), 180
  ) AS table_attention_score
FROM base t
LEFT JOIN scan s
  ON s.namespace_id IS NOT DISTINCT FROM t.namespace_id
 AND t.table_key = s.table_key
LEFT JOIN telemetry x
  ON x.namespace_id IS NOT DISTINCT FROM t.namespace_id
 AND CAST(x.table_id AS VARCHAR) = CAST(t.table_id AS VARCHAR)
ORDER BY table_attention_score DESC NULLS LAST, full_scan_score DESC NULLS LAST, size_mb DESC NULLS LAST
"""


def _fast_table_impact_ctes(where: str, table_where: str, *, table_limit: int = 2000) -> str:
    return f"""
workload_queries AS (
  SELECT
    snapshot_id,
    namespace_id,
    query_id,
    elapsed_s,
    risk_score,
    total_spill,
    dist_total_cnt,
    dist_both_cnt,
    bcast_cnt,
    max_data_skewness,
    max_time_skewness,
    dominant_issue,
    sql_text,
    LOWER(COALESCE(sql_text, '')) AS sql_text_lc,
    ' ' || REGEXP_REPLACE(LOWER(COALESCE(sql_text, '')), '[^a-z0-9_]+', ' ', 'g') || ' ' AS sql_tokens
  FROM v_slow_queries
  WHERE {where}
    AND sql_text IS NOT NULL
  ORDER BY risk_score DESC NULLS LAST, elapsed_s DESC NULLS LAST
),
top_tables AS (
  SELECT
    t.snapshot_id,
    t.namespace_id,
    t.table_key,
    t.source_db,
    t.schema_name,
    t.table_name,
    t.diststyle,
    t.sortkey1,
    t.size_mb,
    t.tbl_rows,
    t.unsorted_pct,
    t.stats_off,
    t.skew_rows,
    t.vacuum_sort_benefit,
    t.table_risk_score,
    LOWER(COALESCE(t.schema_name, '') || '.' || COALESCE(t.table_name, '')) AS schema_table_lc,
    TRIM(REGEXP_REPLACE(LOWER(COALESCE(t.table_name, '')), '[^a-z0-9_]+', ' ', 'g')) AS table_tokens
  FROM v_table_risk t
  WHERE {table_where}
  ORDER BY table_risk_score DESC NULLS LAST, size_mb DESC NULLS LAST
  LIMIT {int(table_limit)}
),
candidate_refs AS (
  SELECT
    q.snapshot_id,
    q.namespace_id,
    q.query_id,
    q.elapsed_s,
    q.risk_score,
    q.total_spill,
    q.dist_total_cnt,
    q.dist_both_cnt,
    q.bcast_cnt,
    q.max_data_skewness,
    q.max_time_skewness,
    q.dominant_issue,
    t.table_key,
    t.source_db,
    t.schema_name,
    t.table_name,
    t.diststyle,
    t.sortkey1,
    t.size_mb,
    t.tbl_rows,
    t.unsorted_pct,
    t.stats_off,
    t.skew_rows,
    t.vacuum_sort_benefit,
    t.table_risk_score
  FROM workload_queries q
  JOIN top_tables t
    ON t.namespace_id IS NOT DISTINCT FROM q.namespace_id
  WHERE (
      LENGTH(t.schema_table_lc) > 1
      AND POSITION(t.schema_table_lc IN q.sql_text_lc) > 0
    )
    OR (
      LENGTH(t.table_tokens) > 0
      AND POSITION(' ' || t.table_tokens || ' ' IN q.sql_tokens) > 0
    )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY q.snapshot_id, q.namespace_id, q.query_id, t.table_key
    ORDER BY t.table_risk_score DESC
  ) = 1
),
impact AS (
SELECT
  r.snapshot_id,
  r.namespace_id,
  r.table_key,
  r.source_db,
  r.schema_name,
  r.table_name,
  ANY_VALUE(r.diststyle) AS diststyle,
  ANY_VALUE(r.sortkey1) AS sortkey1,
  MAX(r.size_mb) AS size_mb,
  MAX(r.tbl_rows) AS tbl_rows,
  MAX(r.unsorted_pct) AS unsorted_pct,
  MAX(r.stats_off) AS stats_off,
  MAX(r.skew_rows) AS skew_rows,
  MAX(r.vacuum_sort_benefit) AS vacuum_sort_benefit,
  MAX(r.table_risk_score) AS table_risk_score,
  COUNT(DISTINCT r.query_id) AS slow_query_count,
  SUM(COALESCE(r.elapsed_s, 0)) AS total_runtime_s,
  AVG(COALESCE(r.elapsed_s, 0)) AS avg_runtime_s,
  MAX(COALESCE(r.elapsed_s, 0)) AS worst_runtime_s,
  AVG(COALESCE(r.risk_score, 0)) AS avg_query_risk,
  MAX(COALESCE(r.risk_score, 0)) AS max_query_risk,
  SUM(COALESCE(r.total_spill, 0)) AS total_spill_blocks,
  SUM(CASE WHEN COALESCE(r.dist_both_cnt, 0) > 0 OR COALESCE(r.dist_total_cnt, 0) >= 2 THEN 1 ELSE 0 END) AS redistribution_query_count,
  SUM(CASE WHEN COALESCE(r.bcast_cnt, 0) > 0 THEN 1 ELSE 0 END) AS broadcast_query_count,
  SUM(CASE WHEN COALESCE(r.max_data_skewness, 0) >= 4 OR COALESCE(r.max_time_skewness, 0) >= 4 THEN 1 ELSE 0 END) AS skewed_query_count,
  STRING_AGG(CAST(r.query_id AS VARCHAR), ', ' ORDER BY COALESCE(r.elapsed_s, 0) DESC) AS query_ids,
  (
    MAX(r.table_risk_score)
    + LEAST(COUNT(DISTINCT r.query_id) * 2.5, 35)
    + LEAST(AVG(COALESCE(r.elapsed_s, 0)) / 3600.0, 35)
    + LEAST(SUM(COALESCE(r.total_spill, 0)) / 100000.0, 20)
    + LEAST(SUM(CASE WHEN COALESCE(r.dist_both_cnt, 0) > 0 OR COALESCE(r.dist_total_cnt, 0) >= 2 THEN 1 ELSE 0 END) * 3, 25)
  ) AS blast_radius_score
FROM candidate_refs r
GROUP BY r.snapshot_id, r.namespace_id, r.table_key, r.source_db, r.schema_name, r.table_name
)
"""


def _fast_action_queue_sql(where: str, table_where: str) -> str:
    return f"""
WITH scan AS (
  SELECT
    snapshot_id,
    namespace_id,
    table_key,
    SUM(COALESCE(scan_query_count, 0)) AS scan_query_count,
    SUM(COALESCE(scan_duration_s, 0)) AS scan_duration_s,
    SUM(COALESCE(scan_input_rows_m, 0)) AS scan_input_rows_m,
    SUM(COALESCE(non_rrscan_query_count, 0)) AS non_rrscan_query_count,
    CASE
      WHEN SUM(COALESCE(scan_query_count, 0)) > 0
      THEN SUM(COALESCE(scan_duration_s, 0)) / SUM(COALESCE(scan_query_count, 0))
      ELSE 0
    END AS avg_scan_duration_s
  FROM v_table_scan_info
  WHERE {where}
  GROUP BY snapshot_id, namespace_id, table_key
),
base AS (
  SELECT
    t.snapshot_id,
    t.namespace_id,
    t.source_db,
    t.schema_name,
    t.table_name,
    t.table_key,
    t.diststyle,
    t.sortkey1,
    t.table_risk_score,
    t.stats_off,
    t.unsorted_pct,
    t.skew_rows,
    t.vacuum_sort_benefit,
    t.size_mb,
    COALESCE(s.scan_query_count, 0) AS scan_query_count,
    COALESCE(s.scan_duration_s, 0) AS scan_duration_s,
    COALESCE(s.avg_scan_duration_s, 0) AS avg_scan_duration_s,
    COALESCE(s.scan_input_rows_m, 0) AS scan_input_rows_m,
    COALESCE(s.non_rrscan_query_count, 0) AS non_rrscan_query_count
  FROM v_table_risk t
  LEFT JOIN scan s
    ON s.snapshot_id IS NOT DISTINCT FROM t.snapshot_id
   AND s.namespace_id IS NOT DISTINCT FROM t.namespace_id
   AND s.table_key = t.table_key
  WHERE {table_where}
),
raw_actions AS (
  SELECT
    snapshot_id,
    namespace_id,
    'A01_ANALYZE_STALE_STATS' AS action_id,
    'Maintenance' AS action_type,
    CASE WHEN COALESCE(stats_off, 0) >= 35 THEN 'crit' ELSE 'warn' END AS severity,
    source_db || '.' || schema_name || '.' || table_name AS subject,
    CAST(NULL AS BIGINT) AS query_id,
    table_key,
    72 + LEAST(COALESCE(stats_off, 0), 45) + LEAST(COALESCE(scan_query_count, 0) * 2, 25) AS action_score,
    'Run ANALYZE on this table.' AS what_to_do,
    'Statistics are stale enough to risk bad row estimates.' AS why_now,
    'stats_off=' || CAST(ROUND(COALESCE(stats_off, 0), 0) AS VARCHAR) || '%'
      || ', scans=' || CAST(COALESCE(scan_query_count, 0) AS VARCHAR)
      || ', avg_scan=' || CAST(ROUND(COALESCE(avg_scan_duration_s, 0) / 60.0, 1) AS VARCHAR) || ' min' AS evidence,
    'ANALYZE ' || schema_name || '.' || table_name || ';' AS sql_hint
  FROM base
  WHERE COALESCE(stats_off, 0) >= 10
  UNION ALL
  SELECT
    snapshot_id,
    namespace_id,
    'A02_VACUUM_SORT_TABLE',
    'Maintenance',
    CASE WHEN COALESCE(unsorted_pct, 0) >= 50 OR COALESCE(vacuum_sort_benefit, 0) >= 35 THEN 'crit' ELSE 'warn' END,
    source_db || '.' || schema_name || '.' || table_name,
    NULL,
    table_key,
    70 + LEAST(COALESCE(unsorted_pct, 0) / 1.2, 45) + LEAST(COALESCE(non_rrscan_query_count, 0) * 4, 25),
    'Run VACUUM SORT or correct the load/sort pattern.',
    'Unsorted rows and non-range-restricted scans weaken zone-map pruning.',
    'unsorted=' || CAST(ROUND(COALESCE(unsorted_pct, 0), 0) AS VARCHAR) || '%'
      || ', vacuum_benefit=' || CAST(ROUND(COALESCE(vacuum_sort_benefit, 0), 0) AS VARCHAR)
      || ', non_rr_scans=' || CAST(COALESCE(non_rrscan_query_count, 0) AS VARCHAR),
    'VACUUM SORT ' || schema_name || '.' || table_name || ';'
  FROM base
  WHERE COALESCE(unsorted_pct, 0) >= 20 OR COALESCE(vacuum_sort_benefit, 0) >= 10
  UNION ALL
  SELECT
    snapshot_id,
    namespace_id,
    'A03_REVIEW_DISTRIBUTION',
    'Physical Design',
    CASE WHEN COALESCE(skew_rows, 0) >= 5 THEN 'crit' ELSE 'warn' END,
    source_db || '.' || schema_name || '.' || table_name,
    NULL,
    table_key,
    70 + LEAST(COALESCE(skew_rows, 0) * 8, 40) + LEAST(COALESCE(table_risk_score, 0) / 3, 25),
    'Review DISTSTYLE and DISTKEY for this table.',
    'Distribution skew can make joins and scans run unevenly across slices.',
    'diststyle=' || COALESCE(diststyle, '')
      || ', skew_rows=' || CAST(ROUND(COALESCE(skew_rows, 0), 2) AS VARCHAR)
      || ', table_risk=' || CAST(ROUND(COALESCE(table_risk_score, 0), 0) AS VARCHAR),
    'Check dominant join columns; use KEY for co-location, EVEN for standalone scans, ALL only for small dimensions.'
  FROM base
  WHERE COALESCE(skew_rows, 0) >= 3
  UNION ALL
  SELECT
    snapshot_id,
    namespace_id,
    'A04_REVIEW_HEAVY_SCAN',
    'Scan Path',
    CASE WHEN COALESCE(avg_scan_duration_s, 0) >= 600 THEN 'crit' ELSE 'warn' END,
    source_db || '.' || schema_name || '.' || table_name,
    NULL,
    table_key,
    62 + LEAST(COALESCE(avg_scan_duration_s, 0) / 60.0, 35) + LEAST(COALESCE(scan_input_rows_m, 0) / 250.0, 25),
    'Review this table scan path.',
    'Average scan time is high enough to matter per query, not just in total.',
    'avg_scan=' || CAST(ROUND(COALESCE(avg_scan_duration_s, 0) / 60.0, 1) AS VARCHAR) || ' min'
      || ', scans=' || CAST(COALESCE(scan_query_count, 0) AS VARCHAR)
      || ', input_rows_m=' || CAST(ROUND(COALESCE(scan_input_rows_m, 0), 0) AS VARCHAR),
    'Check predicates, sort key alignment, projected columns, and whether a summary table would avoid repeated scans.'
  FROM base
  WHERE COALESCE(avg_scan_duration_s, 0) >= 120 OR COALESCE(scan_input_rows_m, 0) >= 500
)
SELECT
  ROW_NUMBER() OVER (ORDER BY action_score DESC NULLS LAST) AS priority_rank,
  *
FROM raw_actions
ORDER BY action_score DESC NULLS LAST, action_id
LIMIT 200
"""


def _fast_rewrite_opportunities_sql(where: str) -> str:
    return f"""
WITH q AS (
  SELECT *
  FROM v_slow_queries
  WHERE {where}
  ORDER BY risk_score DESC NULLS LAST, elapsed_s DESC NULLS LAST
),
raw AS (
  SELECT
    snapshot_id,
    namespace_id,
    query_id,
    NULL AS table_key,
    CASE WHEN COALESCE(dist_both_cnt, 0) > 0 OR COALESCE(dist_total_cnt, 0) >= 4 THEN 'crit' ELSE 'warn' END AS severity,
    'Distributed Join Rewrite' AS title,
    CAST(query_id AS VARCHAR) AS subject,
    COALESCE(elapsed_s, 0) AS elapsed_s,
    86 + LEAST(COALESCE(dist_total_cnt, 0) * 4, 30) AS impact_score,
    'dist_total=' || CAST(COALESCE(dist_total_cnt, 0) AS VARCHAR)
      || ', bcast=' || CAST(COALESCE(bcast_cnt, 0) AS VARCHAR)
      || ', dist_both=' || CAST(COALESCE(dist_both_cnt, 0) AS VARCHAR) AS trigger,
    'Stage distributed join inputs into analyzed temp tables with aligned DISTKEY/SORTKEY.' AS rewrite_shape,
    'Data movement before joins is a direct runtime and spill multiplier.' AS why_it_matters,
    'CREATE TEMP TABLE stage_x DISTKEY(join_key) SORTKEY(date_key) AS SELECT ...; ANALYZE stage_x;' AS candidate_sql
  FROM q
  WHERE COALESCE(dist_both_cnt, 0) > 0 OR COALESCE(dist_total_cnt, 0) >= 4
  UNION ALL
  SELECT
    snapshot_id,
    namespace_id,
    query_id,
    NULL,
    CASE WHEN COALESCE(total_spill, 0) >= 100000 THEN 'crit' ELSE 'warn' END,
    'Spill Reduction Rewrite',
    CAST(query_id AS VARCHAR),
    COALESCE(elapsed_s, 0),
    82 + LEAST(COALESCE(total_spill, 0) / 10000.0, 35),
    'spill_blocks=' || CAST(COALESCE(total_spill, 0) AS VARCHAR)
      || ', input_rows=' || CAST(COALESCE(input_rows, 0) AS VARCHAR),
    'Reduce row width before joins/sorts and push filters earlier.',
    'Spill means hash/sort work exceeded memory and likely dominates elapsed time.',
    'Project only needed columns, pre-aggregate, and filter before large joins/sorts.'
  FROM q
  WHERE COALESCE(total_spill, 0) > 0
  UNION ALL
  SELECT
    snapshot_id,
    namespace_id,
    query_id,
    NULL,
    CASE WHEN COALESCE(external_duration_pct, 0) >= 0.5 THEN 'crit' ELSE 'warn' END,
    'External Scan Materialization',
    CAST(query_id AS VARCHAR),
    COALESCE(elapsed_s, 0),
    80 + COALESCE(external_duration_pct, 0) * 35,
    'external_duration_pct=' || CAST(ROUND(COALESCE(external_duration_pct, 0) * 100, 1) AS VARCHAR) || '%'
      || ', s3_scans=' || CAST(COALESCE(s3_scan_cnt, 0) AS VARCHAR),
    'Materialize or repartition external/S3 scan inputs before the expensive join path.',
    'External scan time is a material share of slow-query runtime.',
    'CREATE TABLE optimized_local DISTKEY(join_key) SORTKEY(date_key) AS SELECT ... FROM external_table WHERE ...;'
  FROM q
  WHERE COALESCE(external_duration_pct, 0) >= 0.25 OR COALESCE(s3_scan_cnt, 0) > 0
  UNION ALL
  SELECT
    snapshot_id,
    namespace_id,
    query_id,
    NULL,
    'crit',
    'Fan-Out Join Review',
    CAST(query_id AS VARCHAR),
    COALESCE(elapsed_s, 0),
    88 + LEAST(COALESCE(selectivity_ratio, 0), 25),
    'selectivity_ratio=' || CAST(ROUND(COALESCE(selectivity_ratio, 0), 4) AS VARCHAR)
      || ', input_rows=' || CAST(COALESCE(input_rows, 0) AS VARCHAR)
      || ', output_rows=' || CAST(COALESCE(output_rows, 0) AS VARCHAR),
    'Verify join predicates and pre-deduplicate one-to-many dimensions.',
    'The query expands rows or contains cross-join/fan-out risk.',
    'Pre-deduplicate dimensions and verify every join has the intended key.'
  FROM q
  WHERE COALESCE(selectivity_ratio, 0) >= 5 OR POSITION('cross join' IN LOWER(COALESCE(sql_text, ''))) > 0
)
SELECT
  ROW_NUMBER() OVER (ORDER BY impact_score DESC NULLS LAST) AS opportunity_no,
  *
FROM raw
ORDER BY impact_score DESC NULLS LAST, opportunity_no
LIMIT 200
"""


def _summary_from_loaded_frames(
    *,
    slow_queries: pd.DataFrame,
    insights: pd.DataFrame,
    table_risk: pd.DataFrame,
    action_queue: pd.DataFrame,
    rewrites: pd.DataFrame,
) -> dict:
    summary: dict = {}
    if not slow_queries.empty:
        summary["slow_query_count"] = len(slow_queries)
        if "elapsed_s" in slow_queries.columns:
            elapsed = pd.to_numeric(slow_queries["elapsed_s"], errors="coerce").fillna(0)
            summary["total_runtime_s"] = float(elapsed.sum())
            summary["worst_runtime_s"] = float(elapsed.max())
        if "risk_score" in slow_queries.columns:
            risk = pd.to_numeric(slow_queries["risk_score"], errors="coerce").dropna()
            summary["avg_query_risk"] = float(risk.mean()) if not risk.empty else 0
    if not insights.empty:
        summary["insight_count"] = len(insights)
        if "severity" in insights.columns:
            severity = insights["severity"].astype(str)
            summary["critical_count"] = int((severity == "crit").sum())
            summary["warning_count"] = int((severity == "warn").sum())
            summary["info_count"] = int((severity == "info").sum())
    if not table_risk.empty:
        summary["table_count"] = len(table_risk)
        if "table_risk_score" in table_risk.columns:
            scores = pd.to_numeric(table_risk["table_risk_score"], errors="coerce").fillna(0)
            summary["high_risk_table_count"] = int((scores >= 50).sum())
            summary["worst_table_risk"] = float(scores.max()) if len(scores) else 0
    summary["action_count"] = len(action_queue)
    summary["rewrite_count"] = len(rewrites)
    return summary
