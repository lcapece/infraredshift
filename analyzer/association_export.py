"""Export assigned engineers and associated users as a Markdown handoff.

The triage screen shows ownership one bubble at a time. This produces the
document you actually send: who owns what, the SQL shape they own, and enough
query ids per cluster to look them up in Redshift.

Qt-free so it can be unit tested and scripted.
"""
from __future__ import annotations

from datetime import datetime
import re

import pandas as pd


# Enough ids to find the query in SYS_QUERY_HISTORY without turning the
# document into a list of numbers.
MAX_QUERY_IDS_PER_CLUSTER = 20

MULTI_CLUSTER = "Multi-Cluster"
UNASSIGNED = "_Unassigned_"


def _text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _cluster_label(row: pd.Series, names: dict[str, str]) -> str:
    """Prefer a human cluster name, fall back to the namespace id."""
    namespace = _text(row.get("namespace_id"))
    friendly = _text(names.get(namespace)) if namespace else ""
    return friendly or namespace or "(unknown cluster)"


def _split_ids(value: object) -> list[str]:
    text = _text(value)
    if not text:
        return []
    return [part.strip() for part in re.split(r"[,\s]+", text) if part.strip()]


def _fence(sql: str) -> str:
    """Fence SQL safely even when it contains a ``` sequence of its own."""
    body = sql.rstrip()
    fence = "```"
    while fence in body:
        fence += "`"
    return f"{fence}sql\n{body}\n{fence}"


def collect_associations(
    groups: pd.DataFrame,
    members: pd.DataFrame,
    cluster_names: dict[str, str] | None = None,
) -> list[dict]:
    """Build one record per assigned/associated group, newest cost first.

    Only groups carrying an engineer or an associated user are included - the
    document is a handoff of owned work, not a dump of every pattern.
    """
    if groups is None or groups.empty:
        return []
    names = cluster_names or {}
    members = members if members is not None else pd.DataFrame()

    records: list[dict] = []
    for _index, group in groups.iterrows():
        engineer = _text(group.get("assigned_engineer"))
        associated = _text(group.get("associated_user"))
        if not engineer and not associated:
            continue

        key = _text(group.get("repeat_group_key"))
        group_id = _text(group.get("repeat_group_id"))

        # Per-cluster query ids come from the members frame, which carries the
        # namespace each individual query actually ran on. The group's own
        # query_ids column is cluster-blind, so it cannot answer "multi-cluster".
        by_cluster: dict[str, list[str]] = {}
        observed_users: set[str] = set()
        if not members.empty and "repeat_group_key" in members.columns:
            mine = members[members["repeat_group_key"].astype(str) == key]
            if mine.empty and group_id and "repeat_group_id" in members.columns:
                mine = members[members["repeat_group_id"].astype(str) == group_id]
            for _pos, member in mine.iterrows():
                label = _cluster_label(member, names)
                query_id = _text(member.get("query_id"))
                if query_id:
                    by_cluster.setdefault(label, []).append(query_id)
                user = _text(member.get("user_name"))
                if user:
                    observed_users.add(user)

        if not by_cluster:
            # No member rows (e.g. members not loaded): fall back to the group's
            # own id list so the export still names something actionable, and
            # say the cluster is unknown rather than inventing one.
            fallback = _split_ids(group.get("query_ids")) or _split_ids(
                group.get("example_query_ids")
            )
            if fallback:
                by_cluster["(cluster not recorded)"] = fallback

        clusters = sorted(by_cluster)
        records.append(
            {
                "repeat_group_id": group_id,
                "repeat_group_key": key,
                "assigned_engineer": engineer,
                "associated_user": associated,
                "scope": MULTI_CLUSTER if len(clusters) > 1 else (
                    clusters[0] if clusters else "(cluster not recorded)"
                ),
                "is_multi_cluster": len(clusters) > 1,
                "clusters": clusters,
                "query_ids_by_cluster": {
                    name: by_cluster[name][:MAX_QUERY_IDS_PER_CLUSTER]
                    for name in clusters
                },
                "query_id_totals": {name: len(by_cluster[name]) for name in clusters},
                "query_count": int(pd.to_numeric(group.get("query_count"), errors="coerce") or 0),
                "total_runtime_s": float(
                    pd.to_numeric(group.get("total_runtime_s"), errors="coerce") or 0.0
                ),
                "verdict": _text(group.get("triage_verdict")),
                "observed_users": sorted(observed_users),
                "sql": _text(group.get("sample_sql"))
                or _text(group.get("representative_sql"))
                or _text(group.get("sql_shape")),
                "tables": _text(group.get("sql_tables")),
            }
        )

    records.sort(key=lambda item: item["total_runtime_s"], reverse=True)
    return records


def render_markdown(
    records: list[dict],
    *,
    generated_at: datetime | None = None,
    source: str = "",
) -> str:
    """Render the association records as a Markdown document."""
    stamp = (generated_at or datetime.now()).strftime("%Y-%m-%d %H:%M")
    lines: list[str] = [
        "# Query Ownership — Engineer and User Associations",
        "",
        f"Generated {stamp}"
        + (f" from `{source}`" if source else ""),
        "",
    ]

    if not records:
        lines += [
            "No query groups have an assigned engineer or an associated user yet.",
            "",
            "Right-click a bubble on the Workload Triage screen and choose "
            "**Assign to Engineer…** or **Associate Query to User…** to record "
            "ownership, then export again.",
            "",
        ]
        return "\n".join(lines)

    multi = sum(1 for item in records if item["is_multi_cluster"])
    total_runtime = sum(item["total_runtime_s"] for item in records)
    lines += [
        f"**{len(records)}** owned query pattern(s) · "
        f"**{multi}** running on more than one cluster · "
        f"**{total_runtime / 3600:,.1f} hours** of captured runtime.",
        "",
        "## Summary",
        "",
        "| Pattern | Engineer | Associated User | Scope | Runs | Runtime |",
        "|---|---|---|---|---:|---:|",
    ]
    for item in records:
        lines.append(
            f"| {item['repeat_group_id'] or '—'} "
            f"| {item['assigned_engineer'] or UNASSIGNED} "
            f"| {item['associated_user'] or UNASSIGNED} "
            f"| {item['scope']} "
            f"| {item['query_count']:,} "
            f"| {item['total_runtime_s'] / 3600:,.2f} h |"
        )
    lines.append("")

    lines += ["---", "", "## Detail", ""]
    for item in records:
        heading = item["repeat_group_id"] or item["repeat_group_key"][:12] or "Pattern"
        lines += [f"### {heading}", ""]

        lines += [
            f"- **Assigned engineer:** {item['assigned_engineer'] or UNASSIGNED}",
            f"- **Associated user:** {item['associated_user'] or UNASSIGNED}",
            f"- **Scope:** {item['scope']}",
        ]
        if item["is_multi_cluster"]:
            lines.append(
                f"  - Runs on {len(item['clusters'])} clusters: "
                + ", ".join(item["clusters"])
            )
        lines += [
            f"- **Captured runs:** {item['query_count']:,}",
            f"- **Total runtime:** {item['total_runtime_s'] / 3600:,.2f} h",
        ]
        if item["verdict"]:
            lines.append(f"- **Verdict:** {item['verdict']}")
        if item["tables"]:
            lines.append(f"- **Tables:** {item['tables']}")
        if item["observed_users"]:
            lines.append(
                "- **Ran as:** " + ", ".join(item["observed_users"][:10])
                + (" …" if len(item["observed_users"]) > 10 else "")
            )
        lines.append("")

        if item["sql"]:
            lines += ["**Grouped SQL**", "", _fence(item["sql"]), ""]

        lines += ["**Query IDs by cluster**", ""]
        for cluster in item["clusters"]:
            ids = item["query_ids_by_cluster"][cluster]
            total = item["query_id_totals"][cluster]
            shown = f"showing {len(ids)} of {total:,}" if total > len(ids) else f"{total:,}"
            lines += [f"- **{cluster}** ({shown})", f"  - `{'`, `'.join(ids)}`"]
        if not item["clusters"]:
            lines.append("- (no query ids recorded)")
        lines += ["", "---", ""]

    return "\n".join(lines)


def export_markdown(
    groups: pd.DataFrame,
    members: pd.DataFrame,
    *,
    cluster_names: dict[str, str] | None = None,
    generated_at: datetime | None = None,
    source: str = "",
) -> str:
    """Collect and render in one call."""
    return render_markdown(
        collect_associations(groups, members, cluster_names),
        generated_at=generated_at,
        source=source,
    )
