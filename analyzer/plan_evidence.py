"""Bind parsed SQL joins to captured Redshift plan and execution-step rows."""
from __future__ import annotations

import re

import pandas as pd


_JOIN_NODE_RE = re.compile(r"\b(?:join|nestloop|nested loop)\b", re.IGNORECASE)
_TOKEN_RE = re.compile(r"[a-z_][a-z0-9_$]*", re.IGNORECASE)


def attach_join_plan_evidence(
    joins: pd.DataFrame,
    explain_rows: pd.DataFrame | None,
    detail_rows: pd.DataFrame | None,
) -> pd.DataFrame:
    if joins is None or joins.empty:
        return joins if joins is not None else pd.DataFrame()
    out = joins.copy()
    explain = explain_rows.copy() if explain_rows is not None else pd.DataFrame()
    details = detail_rows.copy() if detail_rows is not None else pd.DataFrame()
    evidence = [_match_join(row, out, explain, details) for _, row in out.iterrows()]
    evidence_frame = pd.DataFrame(evidence, index=out.index)
    for column in evidence_frame.columns:
        out[column] = evidence_frame[column]
    return out


def _match_join(
    join_row: pd.Series,
    all_joins: pd.DataFrame,
    explain: pd.DataFrame,
    details: pd.DataFrame,
) -> dict:
    empty = _empty_evidence()
    if explain.empty:
        empty["plan_match_status"] = "No SYS_QUERY_EXPLAIN rows loaded"
        return empty
    candidates = explain.copy()
    plan_text = (
        candidates.get("plan_node", pd.Series("", index=candidates.index)).fillna("").astype(str)
        + " "
        + candidates.get("plan_info", pd.Series("", index=candidates.index)).fillna("").astype(str)
    )
    join_mask = plan_text.str.contains(_JOIN_NODE_RE, regex=True, na=False)
    if join_mask.any():
        candidates = candidates.loc[join_mask].copy()
    if candidates.empty:
        empty["plan_match_status"] = "No join plan nodes found"
        return empty

    wanted_tokens = _join_tokens(join_row)
    wanted_condition = _normalized_condition(join_row.get("condition"))
    scores: list[tuple[float, int]] = []
    for index, candidate in candidates.iterrows():
        text = " ".join(
            (
                str(candidate.get("plan_node") or ""),
                str(candidate.get("plan_info") or ""),
            )
        ).lower()
        score = sum(1.0 for token in wanted_tokens if token in text)
        if wanted_condition and wanted_condition in _normalized_condition(text):
            score += 4.0
        for physical_column in _split_csv(join_row.get("left_physical_sources")) + _split_csv(
            join_row.get("right_physical_sources")
        ):
            parts = physical_column.lower().split(".")
            if len(parts) >= 2 and parts[-2] in text:
                score += 2.0
        scores.append((score, index))
    scores.sort(key=lambda item: item[0], reverse=True)
    best_score, best_index = scores[0]
    unique_join_count = len(all_joins)
    if best_score <= 0 and not (unique_join_count == 1 and len(candidates) == 1):
        empty["plan_match_status"] = "Plan join present; SQL-to-plan match unresolved"
        return empty
    candidate = candidates.loc[best_index]
    confidence = (
        "high" if best_score >= 4
        else "medium" if best_score >= 2
        else "single-join inference"
    )
    node_id = _int_or_none(candidate.get("plan_node_id"))
    child_sequence = _int_or_none(candidate.get("child_query_sequence"))
    matched_details = details.copy()
    if not matched_details.empty and node_id is not None and "plan_node_id" in matched_details.columns:
        ids = pd.to_numeric(matched_details["plan_node_id"], errors="coerce")
        matched_details = matched_details.loc[ids == node_id].copy()
        if child_sequence is not None and "child_query_sequence" in matched_details.columns:
            sequences = pd.to_numeric(matched_details["child_query_sequence"], errors="coerce")
            matched_details = matched_details.loc[sequences == child_sequence].copy()
    else:
        matched_details = pd.DataFrame()
    return {
        "plan_match_status": f"Matched ({confidence})",
        "plan_match_confidence": confidence,
        "plan_node_id": node_id,
        "plan_parent_id": _int_or_none(candidate.get("plan_parent_id")),
        "plan_child_query_sequence": child_sequence,
        "plan_operator": str(candidate.get("plan_node") or "").strip(),
        "plan_condition": str(candidate.get("plan_info") or "").strip(),
        "actual_step_names": _join_unique(matched_details.get("step_name")),
        "actual_duration_s": _sum_numeric(matched_details, "duration_s"),
        "actual_input_rows": _sum_numeric(matched_details, "input_rows"),
        "actual_output_rows": _sum_numeric(matched_details, "output_rows"),
        "actual_input_bytes": _sum_numeric(matched_details, "input_bytes"),
        "actual_output_bytes": _sum_numeric(matched_details, "output_bytes"),
        "actual_local_spill_blocks": _sum_numeric(matched_details, "spilled_block_local_disk"),
        "actual_remote_spill_blocks": _sum_numeric(matched_details, "spilled_block_remote_disk"),
        "actual_remote_read_blocks": _sum_numeric(matched_details, "remote_read_io"),
        "actual_max_data_skew": _max_numeric(matched_details, "data_skewness"),
        "actual_max_time_skew": _max_numeric(matched_details, "time_skewness"),
        "actual_movement": _movement_summary(candidate, matched_details),
    }


def _empty_evidence() -> dict:
    return {
        "plan_match_status": "Unresolved",
        "plan_match_confidence": "none",
        "plan_node_id": None,
        "plan_parent_id": None,
        "plan_child_query_sequence": None,
        "plan_operator": "",
        "plan_condition": "",
        "actual_step_names": "",
        "actual_duration_s": 0.0,
        "actual_input_rows": 0.0,
        "actual_output_rows": 0.0,
        "actual_input_bytes": 0.0,
        "actual_output_bytes": 0.0,
        "actual_local_spill_blocks": 0.0,
        "actual_remote_spill_blocks": 0.0,
        "actual_remote_read_blocks": 0.0,
        "actual_max_data_skew": 0.0,
        "actual_max_time_skew": 0.0,
        "actual_movement": "",
    }


def _join_tokens(row: pd.Series) -> set[str]:
    text = " ".join(
        str(row.get(column) or "")
        for column in (
            "condition",
            "column_pairs",
            "physical_column_pairs",
            "left_physical_sources",
            "right_physical_sources",
        )
    ).lower()
    ignored = {"on", "and", "or", "cast", "varchar", "char", "as", "true", "false"}
    return {token for token in _TOKEN_RE.findall(text) if len(token) > 1 and token not in ignored}


def _normalized_condition(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"^\s*(?:hash\s+cond|merge\s+cond|join\s+filter|filter)\s*:\s*", "", text)
    text = text.strip("() ")
    return re.sub(r"\s+", " ", text)


def _movement_summary(plan_row: pd.Series, details: pd.DataFrame) -> str:
    text = " ".join(
        (
            str(plan_row.get("plan_node") or ""),
            str(plan_row.get("plan_info") or ""),
            _join_unique(details.get("step_name")),
        )
    ).upper()
    signals = []
    for token in ("DS_DIST_BOTH", "DS_BCAST_INNER", "DS_DIST_INNER", "DS_DIST_OUTER"):
        if token in text:
            signals.append(token)
    if "BROADCAST" in text and "DS_BCAST_INNER" not in signals:
        signals.append("BROADCAST")
    if "DISTRIBUTE" in text and not any("DIST" in signal for signal in signals):
        signals.append("DISTRIBUTE")
    return ", ".join(signals) or "No mapped movement signal"


def _sum_numeric(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    return float(pd.to_numeric(frame[column], errors="coerce").fillna(0).sum())


def _max_numeric(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    values = pd.to_numeric(frame[column], errors="coerce")
    return float(values.max()) if values.notna().any() else 0.0


def _join_unique(series: pd.Series | None) -> str:
    if series is None:
        return ""
    values = [str(value).strip() for value in series.dropna() if str(value).strip()]
    return ", ".join(dict.fromkeys(values))


def _split_csv(value: object) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def _int_or_none(value: object) -> int | None:
    try:
        if pd.isna(value):
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None
