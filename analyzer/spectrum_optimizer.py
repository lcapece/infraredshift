"""Deterministic, engineer-reviewable Redshift Spectrum recommendations.

The rules use only table-grain catalog and runtime evidence already captured in
``external_table_info_all``.  They never enumerate external partitions, inspect
S3 objects directly, or execute DDL.  Recommendations are deliberately phrased
as review actions because file layout, partition design, and Glue statistics
usually belong to an upstream data-owner workflow.
"""
from __future__ import annotations

import math
import re

import pandas as pd


ASSESSMENT_COLUMNS: tuple[str, ...] = (
    "optimization_priority",
    "optimization_score",
    "optimization_actionable",
    "primary_action_code",
    "recommendation_codes",
    "primary_recommendation",
    "recommendation_count",
    "recommendation_summary",
    "optimization_evidence",
    "suggested_next_step",
    "review_sql",
    "recommendation_confidence",
    "all_recommendations",
)


def _number(value: object, default: float = 0.0) -> float:
    try:
        number = float(value)
        return default if math.isnan(number) else number
    except (TypeError, ValueError):
        return default


def _text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _quoted_identifier(value: object, fallback: str) -> str:
    text = _text(value) or fallback
    return '"' + text.replace('"', '""') + '"'


def _qualified_table(row: pd.Series) -> str:
    schema = _quoted_identifier(row.get("schema_name"), "external_schema")
    table = _quoted_identifier(row.get("table_name"), "external_table")
    return f"{schema}.{table}"


def _observed_format(row: pd.Series) -> str:
    return (_text(row.get("observed_file_format")) or _text(row.get("input_format"))).upper()


def _is_columnar(file_format: str) -> bool:
    normalized = re.sub(r"[^A-Z0-9]", "", str(file_format or "").upper())
    return any(name in normalized for name in ("PARQUET", "ORC", "RCFILE"))


def _has_numrows_statistics(parameters: object) -> bool:
    text = _text(parameters)
    return bool(re.search(r"(?i)(?:^|[^a-z])numrows(?:[^a-z]|$)", text))


def _priority(score: int) -> str:
    if score >= 70:
        return "Critical"
    if score >= 45:
        return "High"
    if score >= 20:
        return "Medium"
    if score > 0:
        return "Low"
    return "Healthy"


def _confidence(row: pd.Series) -> str:
    queries = _number(row.get("query_count"))
    scan_gb = _number(row.get("gross_scan_gb"))
    matches = _number(row.get("output_metric_match_count"))
    if queries >= 5 and scan_gb > 0 and matches > 0:
        return "High"
    if queries > 0 and scan_gb > 0:
        return "Medium"
    return "Low"


def _issue(
    weight: int,
    code: str,
    title: str,
    reason: str,
    next_step: str,
    review_sql: str = "",
) -> dict[str, object]:
    return {
        "weight": int(weight),
        "code": code,
        "title": title,
        "reason": reason,
        "next_step": next_step,
        "review_sql": review_sql,
    }


def _assess_row(row: pd.Series) -> dict[str, object]:
    queries = _number(row.get("query_count"))
    scan_gb = _number(row.get("gross_scan_gb"))
    output_gb = _number(row.get("gross_output_gb"))
    runtime_s = _number(row.get("external_duration_s"))
    total_partitions = _number(row.get("total_partitions_considered"))
    pruning = _number(row.get("partition_pruning_pct"), -1.0)
    avg_files = _number(row.get("avg_files_per_segment"))
    scanned_files = _number(row.get("scanned_files"))
    list_ms = _number(row.get("s3list_time_ms"))
    filter_pct = _number(row.get("row_filter_efficiency_pct"), -1.0)
    output_matches = _number(row.get("output_metric_match_count"))
    warnings = _number(row.get("warning_event_count"))
    errors = _number(row.get("sampled_error_count"))
    spill = _number(row.get("external_spill_blocks"))
    partition_count = _number(row.get("partition_key_count"))
    partition_columns = _text(row.get("partition_key_columns"))
    has_partition_key = partition_count > 0 or bool(partition_columns)
    file_format = _observed_format(row)
    qualified = _qualified_table(row)

    if queries <= 0 or scan_gb <= 0:
        return {
            "optimization_priority": "No Activity",
            "optimization_score": 0,
            "optimization_actionable": False,
            "primary_action_code": "NO_ACTIVITY",
            "recommendation_codes": "NO_ACTIVITY",
            "primary_recommendation": "No recent Spectrum workload",
            "recommendation_count": 0,
            "recommendation_summary": "Catalog metadata exists, but the captured window contains no usable S3 scan activity.",
            "optimization_evidence": "No recent scanned bytes were captured.",
            "suggested_next_step": "Keep the table visible and reassess after a representative workload window.",
            "review_sql": "",
            "recommendation_confidence": "Low",
            "all_recommendations": "",
        }

    issues: list[dict[str, object]] = []

    if file_format and not _is_columnar(file_format):
        issues.append(_issue(
            30,
            "COLUMNAR_FORMAT",
            "Convert the S3 dataset to a columnar format",
            f"Observed format is {file_format}; {scan_gb:,.1f} GB was scanned across {queries:,.0f} queries.",
            "Have the data owner rewrite the underlying objects as Parquet (preferred) or ORC, validate types, then update or recreate the external-table metadata.",
        ))

    if total_partitions > 1:
        if pruning < 50:
            if has_partition_key:
                title = "Make queries prune the existing partitions"
                next_step = (
                    f"Review predicates on {partition_columns or 'the captured partition key'} and ensure each recurring query "
                    "uses direct, type-compatible partition filters without wrapping the key in a function."
                )
                code = "PARTITION_PREDICATE"
            else:
                title = "Redesign or verify the partition strategy"
                next_step = (
                    "Partition the S3 layout around the most common selective predicates (usually date plus a stable source/domain key), "
                    "or repair missing partition-key metadata before judging the design."
                )
                code = "PARTITION_DESIGN"
            issues.append(_issue(
                30 if scan_gb >= 10 else 20,
                code,
                title,
                f"Only {max(0.0, pruning):,.1f}% of {total_partitions:,.0f} considered partitions were pruned.",
                next_step,
                f"-- Review recurring SQL against {qualified}\n-- Require direct predicates on: {partition_columns or '<candidate_partition_columns>'}",
            ))
        elif pruning < 90 and scan_gb >= 10:
            issues.append(_issue(
                15,
                "PARTITION_PREDICATE",
                "Tighten partition predicates",
                f"Partition pruning is {pruning:,.1f}% across {total_partitions:,.0f} considered partitions.",
                f"Compare recurring predicates with {partition_columns or 'the partition-key columns'} and test a narrower partition range.",
            ))
    elif not has_partition_key and scan_gb >= 25:
        issues.append(_issue(
            20,
            "PARTITION_DESIGN",
            "Evaluate a workload-aligned partition key",
            f"The table scanned {scan_gb:,.1f} GB with no captured partition-key metadata.",
            "Use recurring selective predicates to propose a low-to-moderate cardinality partition hierarchy; verify that the metadata capture completed before changing the lake layout.",
        ))

    list_ms_per_query = list_ms / max(queries, 1.0)
    files_per_query = scanned_files / max(queries, 1.0)
    if avg_files > 1000 or files_per_query > 5000 or list_ms_per_query > 2000:
        issues.append(_issue(
            25,
            "FILE_LAYOUT",
            "Compact and rebalance the S3 files",
            f"Observed file fan-out is {avg_files:,.0f} files/segment, {files_per_query:,.0f} files/query, and {list_ms_per_query:,.0f} ms listing/query.",
            "Inspect actual S3 object sizes, merge small files, and target roughly 64 MB–1 GB files of similar size; retest listing time and scan parallelism.",
        ))
    elif avg_files > 100 or list_ms_per_query > 500:
        issues.append(_issue(
            15,
            "FILE_LAYOUT",
            "Review S3 file sizing and fan-out",
            f"Observed file fan-out is {avg_files:,.0f} files/segment with {list_ms_per_query:,.0f} ms listing/query.",
            "Sample actual S3 object sizes and size skew. Consolidate small files or split oversized unsplittable files while keeping files similarly sized.",
        ))

    if output_matches > 0 and filter_pct >= 90 and scan_gb >= 25:
        issues.append(_issue(
            20,
            "PUSHDOWN",
            "Push selective work earlier",
            f"Matched scan steps discarded approximately {filter_pct:,.1f}% of scanned rows after {scan_gb:,.1f} GB of S3 reads.",
            "Select only required columns and rewrite eligible filters and GROUP BY work so EXPLAIN shows S3 predicate/aggregation pushdown; remove avoidable DISTINCT or pre-sort dependencies.",
            f"-- EXPLAIN the representative query and verify S3 Seq Scan / S3 HashAggregate work for {qualified}",
        ))
    elif output_matches > 0 and filter_pct >= 50 and scan_gb >= 100:
        issues.append(_issue(
            12,
            "PUSHDOWN",
            "Review predicate and column pushdown",
            f"Matched scan steps reduced rows by {filter_pct:,.1f}% while scanning {scan_gb:,.1f} GB.",
            "Inspect EXPLAIN for work performed above XN S3 Query Scan and reduce projected columns before joining to local tables.",
        ))

    if queries >= 10 and (scan_gb >= 100 or runtime_s >= 3600):
        issues.append(_issue(
            25 if queries >= 25 or scan_gb >= 500 else 18,
            "MATERIALIZE_OR_STAGE",
            "Materialize or stage the repeated external working set",
            f"{queries:,.0f} queries repeatedly scanned {scan_gb:,.1f} GB and consumed {runtime_s / 3600.0:,.2f} external runtime hours.",
            "For a stable repeated shape, test a materialized view over the external table; otherwise stage the minimum filtered columns into a local table with workload-aligned DISTKEY and SORTKEY.",
            (
                f"CREATE MATERIALIZED VIEW <review_schema>.<review_name> AS\n"
                f"SELECT <required_columns>\nFROM {qualified}\nWHERE <stable_repeated_predicate>;\n"
                "-- Review refresh behavior, ownership, and data-latency requirements before deployment."
            ),
        ))

    if not _has_numrows_statistics(row.get("table_parameters")) and queries >= 5 and scan_gb >= 10:
        issues.append(_issue(
            12,
            "EXTERNAL_STATISTICS",
            "Verify external-table statistics",
            "The captured table properties do not show a numRows statistic for an active external table.",
            "Obtain a verified current row count and set the case-sensitive numRows table property, or confirm that current AWS Glue column statistics cover the table.",
            f"ALTER TABLE {qualified} SET TABLE PROPERTIES ('numRows'='<verified_row_count>');",
        ))

    if warnings > 5 or errors > 0:
        issues.append(_issue(
            20 if errors > 0 else 12,
            "DATA_QUALITY",
            "Resolve recurring Spectrum warnings or data errors",
            f"The window contains {warnings:,.0f} warning events and {errors:,.0f} sampled errors.",
            "Review the captured warning category and sample, validate schema/file compatibility, and correct the producer pipeline before masking or dropping bad rows.",
        ))

    if spill > 0 and queries >= 5:
        issues.append(_issue(
            12,
            "LOCAL_STAGE_DESIGN",
            "Review a local distributed stage for downstream work",
            f"Matched external steps are associated with {spill:,.0f} spill blocks.",
            "After applying early filters, test a narrow local stage distributed on the dominant join key and sorted on the dominant range predicate.",
        ))

    if not issues:
        return {
            "optimization_priority": "Healthy",
            "optimization_score": 0,
            "optimization_actionable": False,
            "primary_action_code": "MONITOR",
            "recommendation_codes": "MONITOR",
            "primary_recommendation": "Monitor the current Spectrum design",
            "recommendation_count": 0,
            "recommendation_summary": "No material optimization trigger crossed the current evidence thresholds.",
            "optimization_evidence": (
                f"{queries:,.0f} queries; {scan_gb:,.1f} GB scanned; "
                f"{max(0.0, pruning):,.1f}% partition pruning; format {file_format or 'unknown'}."
            ),
            "suggested_next_step": "Retain the design and reassess after a larger or more representative workload window.",
            "review_sql": "",
            "recommendation_confidence": _confidence(row),
            "all_recommendations": "",
        }

    issues.sort(key=lambda item: (-int(item["weight"]), str(item["title"])))
    score = min(100, sum(int(item["weight"]) for item in issues))
    primary = issues[0]
    evidence = [str(item["reason"]) for item in issues[:4]]
    review_sql = "\n\n".join(
        str(item["review_sql"]).strip() for item in issues if str(item.get("review_sql") or "").strip()
    )
    if output_gb > 0 and scan_gb > 0:
        evidence.append(f"Observed output was {output_gb:,.1f} GB from {scan_gb:,.1f} GB scanned.")
    return {
        "optimization_priority": _priority(score),
        "optimization_score": score,
        "optimization_actionable": True,
        "primary_action_code": primary["code"],
        "recommendation_codes": "|".join(str(item["code"]) for item in issues),
        "primary_recommendation": primary["title"],
        "recommendation_count": len(issues),
        "recommendation_summary": " ".join(f"{item['title']}." for item in issues[:3]),
        "optimization_evidence": " | ".join(evidence),
        "suggested_next_step": primary["next_step"],
        "review_sql": review_sql,
        "recommendation_confidence": _confidence(row),
        "all_recommendations": "\n".join(
            f"{index}. {item['title']}: {item['reason']} Next: {item['next_step']}"
            for index, item in enumerate(issues, 1)
        ),
    }


def assess_spectrum_tables(frame: pd.DataFrame | None) -> pd.DataFrame:
    """Return the input rows enriched with deterministic Spectrum assessments."""
    if frame is None:
        return pd.DataFrame(columns=list(ASSESSMENT_COLUMNS))
    out = frame.copy()
    if out.empty:
        for column in ASSESSMENT_COLUMNS:
            if column not in out.columns:
                out[column] = pd.Series(dtype="object")
        return out
    assessments = pd.DataFrame(
        [_assess_row(row) for _, row in out.iterrows()],
        index=out.index,
    )
    for column in ASSESSMENT_COLUMNS:
        out[column] = assessments[column]
    return out
