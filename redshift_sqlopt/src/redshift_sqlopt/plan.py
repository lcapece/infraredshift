"""Plan-evidence analysis: the predictive half of the optimizer.

The query already ran, so its cost is measured, not estimated. This module reads
``SYS_QUERY_EXPLAIN`` (what the planner decided) and ``SYS_QUERY_DETAIL`` (what
actually happened) and turns them into findings that carry real numbers.

That is what makes a recommendation predictive rather than stylistic. "Add a
DISTKEY" is advice. "Step 7 broadcast 2.1B rows and spilled 84 GB; this table
has DISTSTYLE EVEN and no sort key" is a prediction with its evidence attached.

Nothing here parses SQL, so every finding in this module survives a query whose
text sqlglot could not parse.
"""

from __future__ import annotations

import re

from .catalog import Catalog, TableStats, normalize_ident
from .models import Finding, PlanEvidence, Severity, Tier

# A step that redistributes or broadcasts is where MPP queries actually die.
BROADCAST_TOKENS = ("DS_BCAST", "BCAST", "BROADCAST")
REDIST_TOKENS = ("DS_DIST_BOTH", "DS_DIST_INNER", "DS_DIST_OUTER", "DS_DIST_ALL", "REDISTRIBUTE")

# Thresholds. Deliberately conservative: a finding that fires on every query is
# noise, and noise is what makes engineers stop reading the report.
SPILL_BYTES_FLOOR = 1_000_000_000          # 1 GB spilled is worth mentioning
BROADCAST_ROW_FLOOR = 1_000_000            # broadcasting under 1M rows is fine
ESTIMATE_ERROR_FLOOR = 100.0               # 100x off means stats are stale
LARGE_SCAN_BYTES = 50_000_000_000          # 50 GB scanned
SKEW_FLOOR = 4.0                           # 4x slice imbalance


# Real Redshift column names, verified against the published pg_catalog schema
# (SYS_QUERY_EXPLAIN / SYS_QUERY_DETAIL). Earlier releases of this module guessed
# at these and guessed wrong on almost every one, which would have made the
# plan-derived findings silently produce nothing on a real cluster. Alternative
# spellings are still accepted so rows that arrived via the analyzer's DuckDB
# copy (which renames some columns) keep working.
_NODE_ID_KEYS = ("plan_node_id", "step_id", "step", "nodeid")
_NODE_NAME_KEYS = ("plan_node", "step_name", "operation", "node", "plannode")
_NODE_INFO_KEYS = ("plan_info", "step_attribute", "info", "detail")
_ACTUAL_ROW_KEYS = ("output_rows", "rows", "actual_rows")
_INPUT_BYTE_KEYS = ("input_bytes", "bytes", "bytes_scanned")
_TABLE_KEYS = ("table_name", "tables")

# SYS_QUERY_DETAIL reports spill in BLOCKS, not bytes. A Redshift block is 1 MB.
_SPILL_BLOCK_KEYS = ("spilled_block_local_disk", "spilled_block_remote_disk", "total_spill")
_SPILL_BYTE_KEYS = ("spilled_bytes", "spill_bytes")
REDSHIFT_BLOCK_BYTES = 1_048_576

# SYS_QUERY_DETAIL.duration is MICROSECONDS.
_DURATION_US_KEYS = ("duration",)
_DURATION_S_KEYS = ("duration_s", "elapsed_s", "step_duration_s")


def _first(row: dict, keys: tuple[str, ...]) -> object:
    """First present, non-empty value among *keys*."""
    for key in keys:
        if key in row:
            value = row[key]
            if value is not None and str(value).strip() != "":
                return value
    return None


def evidence_from_rows(
    explain_rows: list[dict] | None = None,
    detail_rows: list[dict] | None = None,
) -> list[PlanEvidence]:
    """Build evidence records from SYS view rows.

    Keyed on ``plan_node_id``, which is what actually joins SYS_QUERY_EXPLAIN to
    SYS_QUERY_DETAIL. Note that ``step_id`` is a *different* identifier in
    SYS_QUERY_DETAIL (a step within a segment) and does not correspond to an
    explain node, so it is only a fallback for renamed feeds.
    """
    by_step: dict[int, dict] = {}

    for row in explain_rows or []:
        step = _int(_first(row, _NODE_ID_KEYS))
        if step is None:
            continue
        entry = by_step.setdefault(step, {})
        entry["node"] = str(_first(row, _NODE_NAME_KEYS) or "").strip()
        info = str(_first(row, _NODE_INFO_KEYS) or "").strip()
        entry["detail"] = info
        # SYS_QUERY_EXPLAIN has no numeric row estimate: the planner's guess is
        # embedded in plan_info text such as "rows=1234 width=56".
        entry["estimated_rows"] = _int(
            _first(row, ("rows", "estimated_rows", "plan_rows"))
        ) or _rows_from_plan_info(info)
        tables = _first(row, _TABLE_KEYS) or ""
        entry["tables"] = tuple(
            normalize_ident(part) for part in str(tables).split(",") if str(part).strip()
        )

    for row in detail_rows or []:
        step = _int(_first(row, _NODE_ID_KEYS))
        if step is None:
            continue
        entry = by_step.setdefault(step, {})
        entry["actual_rows"] = _int(_first(row, _ACTUAL_ROW_KEYS))
        entry["bytes_scanned"] = _int(_first(row, _INPUT_BYTE_KEYS))

        spill_bytes = _int(_first(row, _SPILL_BYTE_KEYS))
        if spill_bytes is None:
            blocks = sum(
                _int(row.get(key)) or 0
                for key in ("spilled_block_local_disk", "spilled_block_remote_disk")
                if key in row
            )
            if not blocks:
                blocks = _int(_first(row, _SPILL_BLOCK_KEYS)) or 0
            spill_bytes = blocks * REDSHIFT_BLOCK_BYTES if blocks else None
        entry["spill_bytes"] = spill_bytes

        duration = _float(_first(row, _DURATION_S_KEYS))
        if duration is None:
            micros = _float(_first(row, _DURATION_US_KEYS))
            duration = micros / 1_000_000.0 if micros is not None else None
        entry["duration_s"] = duration

        if not entry.get("node"):
            entry["node"] = str(_first(row, _NODE_NAME_KEYS) or "").strip()
        if not entry.get("detail"):
            entry["detail"] = str(_first(row, ("step_attribute", "alert")) or "").strip()
        if not entry.get("tables"):
            tables = _first(row, _TABLE_KEYS) or ""
            entry["tables"] = tuple(
                normalize_ident(part) for part in str(tables).split(",") if str(part).strip()
            )

    evidence: list[PlanEvidence] = []
    for step in sorted(by_step):
        entry = by_step[step]
        blob = f"{entry.get('node', '')} {entry.get('detail', '')}".upper()
        evidence.append(
            PlanEvidence(
                step=step,
                node=entry.get("node", ""),
                detail=entry.get("detail", ""),
                actual_rows=entry.get("actual_rows"),
                estimated_rows=entry.get("estimated_rows"),
                bytes_scanned=entry.get("bytes_scanned"),
                spill_bytes=entry.get("spill_bytes"),
                duration_s=entry.get("duration_s"),
                is_broadcast=any(token in blob for token in BROADCAST_TOKENS),
                is_redistribute=any(token in blob for token in REDIST_TOKENS),
                tables=entry.get("tables", ()),
            )
        )
    return evidence


def findings_from_evidence(
    evidence: list[PlanEvidence],
    catalog: Catalog,
    referenced_tables: list[str] | None = None,
) -> list[Finding]:
    """Turn measured plan facts into ranked, tiered findings."""
    findings: list[Finding] = []
    findings.extend(_broadcast_findings(evidence, catalog))
    findings.extend(_spill_findings(evidence))
    findings.extend(_estimate_error_findings(evidence))
    findings.extend(_scan_findings(evidence))
    findings.extend(_unkeyed_table_findings(catalog, referenced_tables or []))
    return findings


def _broadcast_findings(evidence: list[PlanEvidence], catalog: Catalog) -> list[Finding]:
    """Broadcast/redistribution of a large row set is the classic MPP killer."""
    out: list[Finding] = []
    for item in evidence:
        if not (item.is_broadcast or item.is_redistribute):
            continue
        rows = item.actual_rows or 0
        if rows < BROADCAST_ROW_FLOOR:
            continue
        kind = "broadcast" if item.is_broadcast else "redistribution"
        severity = Severity.CRITICAL if rows >= 100_000_000 else Severity.HIGH
        table_note = ""
        suggested = ""
        tier = Tier.DDL
        for table in item.tables:
            stats = catalog.resolve_table(table)
            if stats is None:
                continue
            if not stats.has_distkey:
                table_note = (
                    f" {stats.key} has DISTSTYLE {stats.diststyle or 'EVEN'} and no "
                    "distribution key, so every join against it must move data."
                )
                suggested = (
                    f"ALTER TABLE {stats.key} ALTER DISTSTYLE KEY DISTKEY (<join_column>);"
                )
                break
        out.append(
            Finding(
                tier=tier,
                severity=severity,
                code="PLAN_BROADCAST",
                title=f"Step {item.step}: {kind} of {rows:,} rows",
                explanation=(
                    f"This step moved {rows:,} rows across the cluster network before it "
                    f"could join.{table_note} Matching the distribution key to the join "
                    "column removes this movement entirely."
                ),
                evidence=(item,),
                tables=item.tables,
                suggested_ddl=suggested,
                estimated_benefit=(
                    f"Eliminates network movement of {rows:,} rows on every run of this shape."
                ),
            )
        )
    return out


def _spill_findings(evidence: list[PlanEvidence]) -> list[Finding]:
    """Spill means the working set exceeded memory and went to disk."""
    out: list[Finding] = []
    for item in evidence:
        spill = item.spill_bytes or 0
        if spill < SPILL_BYTES_FLOOR:
            continue
        out.append(
            Finding(
                tier=Tier.REWRITE,
                severity=Severity.HIGH if spill >= 10_000_000_000 else Severity.MEDIUM,
                code="PLAN_SPILL",
                title=f"Step {item.step}: {spill / 1e9:.1f} GB spilled to disk",
                explanation=(
                    "The working set for this step did not fit in memory, so Redshift "
                    "wrote it to disk and read it back. Reducing the row or column "
                    "volume reaching this step — an earlier filter, a narrower "
                    "projection, or pre-aggregation — avoids the spill."
                ),
                evidence=(item,),
                tables=item.tables,
                estimated_benefit=f"Avoids {spill / 1e9:.1f} GB of disk I/O per run.",
            )
        )
    return out


def _estimate_error_findings(evidence: list[PlanEvidence]) -> list[Finding]:
    """A large estimate error means the planner chose its plan on bad information."""
    out: list[Finding] = []
    for item in evidence:
        ratio = item.estimate_error_ratio
        if ratio is None or ratio == float("inf") or ratio < ESTIMATE_ERROR_FLOOR:
            continue
        out.append(
            Finding(
                tier=Tier.DDL,
                severity=Severity.HIGH,
                code="PLAN_STATS_STALE",
                title=f"Step {item.step}: actual rows {ratio:,.0f}x the planner estimate",
                explanation=(
                    f"The planner expected {item.estimated_rows:,} rows and got "
                    f"{item.actual_rows:,}. Join order and join strategy are chosen from "
                    "these estimates, so a gap this large means the plan itself was "
                    "selected on bad information. Refreshing statistics often fixes the "
                    "plan without touching the query."
                ),
                evidence=(item,),
                tables=item.tables,
                suggested_ddl=(
                    f"ANALYZE {item.tables[0]};" if item.tables else "ANALYZE <table>;"
                ),
                estimated_benefit="May change the planner's join strategy for this query shape.",
            )
        )
    return out


def _scan_findings(evidence: list[PlanEvidence]) -> list[Finding]:
    """A large scan with a small output suggests pruning that did not happen."""
    out: list[Finding] = []
    for item in evidence:
        scanned = item.bytes_scanned or 0
        if scanned < LARGE_SCAN_BYTES:
            continue
        rows_out = item.actual_rows
        selective = rows_out is not None and rows_out > 0 and scanned / max(rows_out, 1) > 10_000
        if not selective:
            continue
        out.append(
            Finding(
                tier=Tier.REWRITE,
                severity=Severity.HIGH,
                code="PLAN_WIDE_SCAN",
                title=f"Step {item.step}: scanned {scanned / 1e9:.0f} GB to emit {rows_out:,} rows",
                explanation=(
                    "The volume read is far larger than the volume returned, which means "
                    "block pruning did not apply. Common causes are a function wrapped "
                    "around a sort-key column in the WHERE clause, a predicate on a "
                    "non-sort-key column, or a missing sort key on the table."
                ),
                evidence=(item,),
                tables=item.tables,
                estimated_benefit=(
                    f"Sargable predicates on the sort key could avoid most of "
                    f"{scanned / 1e9:.0f} GB per run."
                ),
            )
        )
    return out


def _unkeyed_table_findings(catalog: Catalog, referenced: list[str]) -> list[Finding]:
    """Large referenced tables missing a distribution or sort key.

    This is deliberately Tier.DDL and never Tier.DECOMPOSE. Decomposing queries
    around an unkeyed table leaves every other query on that table paying the
    same cost; one DDL change fixes the whole workload.
    """
    out: list[Finding] = []
    for stats in catalog.unkeyed_tables(referenced):
        missing: list[str] = []
        if not stats.has_distkey:
            missing.append("distribution key")
        if not stats.has_sortkey:
            missing.append("sort key")
        size_note = ""
        if stats.row_count:
            size_note = f" ({stats.row_count:,} rows)"
        elif stats.size_mb:
            size_note = f" ({stats.size_mb / 1024:.1f} GB)"

        ddl_parts: list[str] = []
        if not stats.has_distkey:
            ddl_parts.append(
                f"ALTER TABLE {stats.key} ALTER DISTSTYLE KEY DISTKEY (<most_common_join_column>);"
            )
        if not stats.has_sortkey:
            ddl_parts.append(
                f"ALTER TABLE {stats.key} ALTER SORTKEY (<most_common_filter_column>);"
            )

        out.append(
            Finding(
                tier=Tier.DDL,
                severity=Severity.HIGH if not stats.has_distkey else Severity.MEDIUM,
                code="TABLE_UNKEYED",
                title=f"{stats.key} has no {' and no '.join(missing)}{size_note}",
                explanation=(
                    f"Without a distribution key Redshift spreads rows evenly and must "
                    f"move them for every join. Without a sort key it cannot prune blocks, "
                    f"so every filter reads the whole table. Fixing this helps every query "
                    f"touching {stats.key}, not only this one — which is why it ranks above "
                    "rewriting individual queries."
                ),
                tables=(stats.key,),
                suggested_ddl=" ".join(ddl_parts),
                estimated_benefit="Applies to the entire workload against this table.",
            )
        )

        if stats.skew_ratio is not None and stats.skew_ratio >= SKEW_FLOOR:
            out.append(
                Finding(
                    tier=Tier.DDL,
                    severity=Severity.HIGH,
                    code="TABLE_SKEW",
                    title=f"{stats.key} is skewed {stats.skew_ratio:.1f}x across slices",
                    explanation=(
                        "Rows are distributed unevenly, so the busiest slice does far more "
                        "work than the rest and the whole query waits for it. The current "
                        "distribution key has too few distinct values or a dominant value."
                    ),
                    tables=(stats.key,),
                    suggested_ddl=(
                        f"-- consider a higher-cardinality DISTKEY, or DISTSTYLE ALL if "
                        f"{stats.key} is small enough to replicate"
                    ),
                    estimated_benefit=f"Rebalances work currently concentrated on one slice.",
                )
            )
    return out


_PLAN_INFO_ROWS_RE = re.compile(r"\brows=(\d+)", re.IGNORECASE)


def _rows_from_plan_info(info: str) -> int | None:
    """Extract the planner's row estimate from SYS_QUERY_EXPLAIN.plan_info.

    Redshift stores the estimate inside free text rather than a numeric column,
    e.g. ``(cost=0.00..1234.56 rows=98765 width=42)``. Without this the
    estimate-error finding can never fire against real cluster rows.
    """
    match = _PLAN_INFO_ROWS_RE.search(str(info or ""))
    return int(match.group(1)) if match else None


def _int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None
