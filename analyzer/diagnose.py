"""Rules engine. Consumes Snapshot, emits Findings + per-step enrichment."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

from .providers.base import Snapshot
from .schema import categorize_step


Severity = Literal["crit", "warn", "info", "ok"]


@dataclass(frozen=True)
class Finding:
    severity: Severity
    title: str
    detail: str
    table: str | None = None
    step_ids: tuple[int, ...] = ()
    recommendation: str | None = None
    impact_score: float = 0.0  # 0..1, drives sort order


@dataclass
class StepEnrichment:
    step_id: int | None
    stream_id: int | None
    segment_id: int | None
    step_name: str
    category: str
    table: str | None
    schema: str | None
    input_rows: int | None
    output_rows: int | None
    input_bytes: int | None
    elapsed_ms: int | None
    spill_local: int | None
    spill_remote: int | None
    data_skew: float | None
    time_skew: float | None
    alert: str | None
    is_rrscan: bool | None = None
    blocks_read: int | None = None
    pct_runtime: float = 0.0
    table_info: dict | None = None


@dataclass
class DiagnosisReport:
    findings: list[Finding] = field(default_factory=list)
    steps: list[StepEnrichment] = field(default_factory=list)
    total_runtime_ms: int = 0
    worst_step_id: int | None = None
    tables_touched: int = 0


def _to_int(v) -> int | None:
    try:
        if pd.isna(v):
            return None
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_float(v) -> float | None:
    try:
        if pd.isna(v):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_bool(v) -> bool | None:
    """Cast Redshift-style boolean (t/f, true/false, 1/0) to Python bool."""
    try:
        if v is None or pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, bool):
        return v
    t = str(v).strip().lower()
    if t in ("t", "true", "1", "y", "yes"):
        return True
    if t in ("f", "false", "0", "n", "no"):
        return False
    return None


def _to_str(v) -> str:
    """Safe string cast that treats pandas NA / NaN as empty string."""
    try:
        if v is None or pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    return str(v)


def _enrich_steps(snap: Snapshot) -> tuple[list[StepEnrichment], int]:
    qd = snap.query_details
    ti = snap.table_info

    ti_indexed: dict[str, dict] = {}
    if not ti.empty and "table" in ti.columns:
        for _, row in ti.iterrows():
            key_full = ".".join(
                [_to_str(row.get("schema")), _to_str(row.get("table"))]
            ).lower().strip(".")
            key_simple = _to_str(row.get("table")).lower()
            payload = {k: row.get(k) for k in ti.columns}
            if key_full:
                ti_indexed[key_full] = payload
            if key_simple:
                ti_indexed.setdefault(key_simple, payload)

    steps: list[StepEnrichment] = []
    total_ms = 0
    for _, r in qd.iterrows():
        elapsed = _to_int(r.get("elapsed_time"))
        if elapsed:
            total_ms += elapsed
        t_name = r.get("table_name")
        t_schema = r.get("schema_name")
        ti_row = None
        if isinstance(t_name, str) and t_name:
            full = f"{t_schema or ''}.{t_name}".lower().strip(".")
            ti_row = ti_indexed.get(full) or ti_indexed.get(t_name.lower())
        steps.append(
            StepEnrichment(
                step_id=_to_int(r.get("step_id")),
                stream_id=_to_int(r.get("stream_id")),
                segment_id=_to_int(r.get("segment_id")),
                step_name=_to_str(r.get("step_name")),
                category=categorize_step(r.get("step_name")),
                table=str(t_name) if isinstance(t_name, str) and t_name else None,
                schema=str(t_schema) if isinstance(t_schema, str) and t_schema else None,
                input_rows=_to_int(r.get("input_rows")),
                output_rows=_to_int(r.get("output_rows")),
                input_bytes=_to_int(r.get("input_bytes")),
                elapsed_ms=elapsed,
                spill_local=_to_int(r.get("spilled_block_local_disk")),
                spill_remote=_to_int(r.get("spilled_block_remote_disk")),
                data_skew=_to_float(r.get("data_skewness")),
                time_skew=_to_float(r.get("time_skewness")),
                alert=(lambda v: str(v) if isinstance(v, str) and v else None)(r.get("alert_type")),
                is_rrscan=_to_bool(r.get("is_rrscan")),
                blocks_read=_to_int(r.get("query_blocks_read")),
                table_info=ti_row,
            )
        )

    if total_ms > 0:
        for s in steps:
            if s.elapsed_ms:
                s.pct_runtime = s.elapsed_ms / total_ms
    return steps, total_ms


def _rule_redist_dominant(steps: list[StepEnrichment]) -> list[Finding]:
    redist = [s for s in steps if s.category == "redistribute" and s.elapsed_ms]
    if not redist:
        return []
    total_redist = sum(s.elapsed_ms or 0 for s in redist)
    total = sum(s.elapsed_ms or 0 for s in steps) or 1
    pct = total_redist / total
    if pct < 0.15:
        return []
    sev: Severity = "crit" if pct > 0.4 else "warn"
    worst = max(redist, key=lambda s: s.elapsed_ms or 0)
    return [
        Finding(
            severity=sev,
            title=f"Redistribution is {pct:.0%} of runtime",
            detail=(
                f"{len(redist)} redistribute/broadcast step(s) burned "
                f"{total_redist:,} ms. Worst: step {worst.step_id} "
                f"on `{worst.table or 'unknown'}` ({(worst.elapsed_ms or 0):,} ms)."
            ),
            table=worst.table,
            step_ids=tuple(s.step_id for s in redist if s.step_id is not None),
            recommendation=(
                "Align DISTKEY on join columns or switch to DISTSTYLE ALL for small "
                "dimension tables to eliminate inter-slice traffic."
            ),
            impact_score=pct,
        )
    ]


def _rule_distkey_mismatch(steps: list[StepEnrichment]) -> list[Finding]:
    out: list[Finding] = []
    for s in steps:
        if s.category != "join" or not s.table_info:
            continue
        diststyle = _to_str(s.table_info.get("diststyle")).upper()
        if "KEY" not in diststyle:
            continue
        # A join step on a KEY-distributed table that is preceded by redistribution
        # of the same relation is the smoking gun. Heuristic: the previous step in
        # the same segment is a redistribute on the same table.
        prev = _previous_step(steps, s)
        if prev and prev.category == "redistribute" and (prev.table or "") == (s.table or ""):
            out.append(
                Finding(
                    severity="crit",
                    title=f"DISTKEY mismatch on `{s.table}`",
                    detail=(
                        f"`{s.table}` is {diststyle} but the join at step {s.step_id} "
                        f"forced redistribution (step {prev.step_id}, "
                        f"{(prev.elapsed_ms or 0):,} ms)."
                    ),
                    table=s.table,
                    step_ids=(s.step_id, prev.step_id) if s.step_id and prev.step_id else (),
                    recommendation=(
                        f"Change `{s.table}`'s DISTKEY to the join column used here, "
                        "or co-locate via DISTSTYLE ALL if the table is small."
                    ),
                    impact_score=(prev.pct_runtime or 0) + (s.pct_runtime or 0),
                )
            )
    return out


def _previous_step(steps: list[StepEnrichment], s: StepEnrichment) -> StepEnrichment | None:
    candidates = [
        x for x in steps
        if x.stream_id == s.stream_id and x.segment_id == s.segment_id
        and (x.step_id or -1) < (s.step_id or 0)
    ]
    return max(candidates, key=lambda x: x.step_id or 0, default=None)


def _rule_sortkey_ineffective(steps: list[StepEnrichment]) -> list[Finding]:
    out: list[Finding] = []
    for s in steps:
        if s.category != "scan" or not s.table_info:
            continue
        unsorted = _to_float(s.table_info.get("unsorted"))
        sortkey = _to_str(s.table_info.get("sortkey1")).strip()
        tbl_rows = _to_int(s.table_info.get("tbl_rows")) or 0
        if not sortkey or unsorted is None:
            continue
        if unsorted >= 20 and tbl_rows > 1_000_000:
            out.append(
                Finding(
                    severity="warn" if unsorted < 50 else "crit",
                    title=f"`{s.table}` scan reads {unsorted:.0f}% unsorted",
                    detail=(
                        f"SORTKEY `{sortkey}` exists but {unsorted:.0f}% of "
                        f"{tbl_rows:,} rows are unsorted. Zone maps will not "
                        "prune effectively."
                    ),
                    table=s.table,
                    step_ids=(s.step_id,) if s.step_id else (),
                    recommendation=f"Run VACUUM SORT on `{s.table}`.",
                    impact_score=(unsorted / 100) * (s.pct_runtime or 0.1),
                )
            )
    return out


def _rule_sortkey_not_used(steps: list[StepEnrichment]) -> list[Finding]:
    """Scan on a sortkey-defined table where the query did not range-restrict.

    Signal: `is_rrscan = false` on a large table that has a sortkey1. Zone-map
    pruning did not happen because the WHERE clause does not filter on the
    sortkey column (or filters on a non-leading one).
    """
    out: list[Finding] = []
    for s in steps:
        if s.category != "scan" or not s.table_info:
            continue
        if s.is_rrscan is not False:
            continue  # True or unknown → skip
        sortkey = _to_str(s.table_info.get("sortkey1")).strip()
        tbl_rows = _to_int(s.table_info.get("tbl_rows")) or 0
        if not sortkey or tbl_rows < 1_000_000:
            continue
        input_rows = s.input_rows or 0
        if input_rows < tbl_rows * 0.25:  # scan read <25% despite no rrscan → probably pre-filtered
            continue
        severity: Severity = "crit" if (s.pct_runtime or 0) > 0.25 else "warn"
        out.append(
            Finding(
                severity=severity,
                title=f"`{s.table}` scan did not use SORTKEY",
                detail=(
                    f"SORTKEY `{sortkey}` is defined but this query read "
                    f"{input_rows:,} / {tbl_rows:,} rows with is_rrscan=false "
                    f"(blocks read: {s.blocks_read or 0:,}). The WHERE clause "
                    "does not filter on the leading sortkey column, so zone "
                    "maps could not prune blocks."
                ),
                table=s.table,
                step_ids=(s.step_id,) if s.step_id else (),
                recommendation=(
                    f"Add a filter on `{sortkey}` to this query, or change "
                    f"`{s.table}`'s SORTKEY to the columns this query actually "
                    "filters on."
                ),
                impact_score=0.4 + (s.pct_runtime or 0),
            )
        )
    return out


def _rule_stale_stats(snap: Snapshot) -> list[Finding]:
    ti = snap.table_info
    if ti.empty or "stats_off" not in ti.columns:
        return []
    out: list[Finding] = []
    for _, r in ti.iterrows():
        stats_off = _to_float(r.get("stats_off"))
        tbl = r.get("table")
        if stats_off is None or not isinstance(tbl, str):
            continue
        if stats_off >= 10:
            out.append(
                Finding(
                    severity="warn" if stats_off < 25 else "crit",
                    title=f"Stale stats on `{tbl}`",
                    detail=f"stats_off = {stats_off:.1f}. Planner row estimates will drift.",
                    table=tbl,
                    recommendation=f"ANALYZE {tbl};",
                    impact_score=min(stats_off / 100, 1.0) * 0.3,
                )
            )
    return out


def _rule_skew(snap: Snapshot) -> list[Finding]:
    ti = snap.table_info
    if ti.empty or "skew_rows" not in ti.columns:
        return []
    out: list[Finding] = []
    for _, r in ti.iterrows():
        skew = _to_float(r.get("skew_rows"))
        tbl = r.get("table")
        if skew is None or not isinstance(tbl, str):
            continue
        if skew >= 3:
            out.append(
                Finding(
                    severity="warn" if skew < 5 else "crit",
                    title=f"Data skew on `{tbl}` (×{skew:.1f})",
                    detail=(
                        f"Heaviest slice holds {skew:.1f}× the median. "
                        "One slice becomes the runtime bottleneck."
                    ),
                    table=tbl,
                    recommendation="Pick a higher-cardinality DISTKEY or switch to EVEN.",
                    impact_score=min(skew / 10, 1.0) * 0.5,
                )
            )
    return out


def _rule_spill(steps: list[StepEnrichment]) -> list[Finding]:
    out: list[Finding] = []
    for s in steps:
        local = s.spill_local or 0
        remote = s.spill_remote or 0
        if remote > 0:
            out.append(
                Finding(
                    severity="crit",
                    title=f"Step {s.step_id} spilled to remote disk",
                    detail=f"{remote:,} blocks spilled remotely. This step is memory-starved.",
                    step_ids=(s.step_id,) if s.step_id else (),
                    recommendation="Increase WLM memory or reduce working-set via filters earlier in plan.",
                    impact_score=0.9,
                )
            )
        elif local > 1000:
            out.append(
                Finding(
                    severity="warn",
                    title=f"Step {s.step_id} spilled to local disk ({local:,} blocks)",
                    detail="Hash/sort operation exceeded memory budget.",
                    step_ids=(s.step_id,) if s.step_id else (),
                    recommendation="Consider reducing join fan-out or adjusting WLM memory.",
                    impact_score=0.4,
                )
            )
    return out


def _rule_alerts(steps: list[StepEnrichment]) -> list[Finding]:
    return [
        Finding(
            severity="warn",
            title=f"Planner alert at step {s.step_id}: {s.alert}",
            detail=f"Redshift emitted `{s.alert}` for this step.",
            step_ids=(s.step_id,) if s.step_id else (),
            recommendation="Inspect the step's join type and input distribution.",
            impact_score=0.3,
        )
        for s in steps
        if s.alert and s.alert.lower() not in ("none", "nan", "")
    ]


def diagnose(snap: Snapshot) -> DiagnosisReport:
    steps, total = _enrich_steps(snap)
    findings: list[Finding] = []
    findings += _rule_redist_dominant(steps)
    findings += _rule_distkey_mismatch(steps)
    findings += _rule_sortkey_ineffective(steps)
    findings += _rule_stale_stats(snap)
    findings += _rule_skew(snap)
    findings += _rule_spill(steps)
    findings += _rule_alerts(steps)
    findings.sort(key=lambda f: f.impact_score, reverse=True)

    worst = max(steps, key=lambda s: s.elapsed_ms or 0, default=None)
    tables = {s.table for s in steps if s.table}

    return DiagnosisReport(
        findings=findings,
        steps=steps,
        total_runtime_ms=total,
        worst_step_id=worst.step_id if worst else None,
        tables_touched=len(tables),
    )
