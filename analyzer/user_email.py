"""Compose an email to the user behind a slow query pattern.

Redshift usernames are frequently service accounts (``svc_ingestion``,
``etl_airflow``) rather than people, so "no address available" is the normal
case and not an edge case. The address is resolved from the username when it is
already an email, otherwise from the captured user roster.

Qt-free so the composition can be unit tested and reused.
"""
from __future__ import annotations

import re
from urllib.parse import quote

import pandas as pd


# Deliberately permissive: this only needs to recognise an address that is
# already there, never to validate one for delivery.
# ':' is excluded from the local part on purpose: Redshift usernames arrive
# prefixed as "IAM:jane.doe@bank.com", and carrying the prefix through would
# hand Outlook an address that silently fails to send.
_EMAIL_RE = re.compile(r"[^@\s;,<>:]+@[^@\s;,<>:]+\.[A-Za-z]{2,}")

MAX_SUBJECT_TABLES = 2


def _text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def extract_email(value: object) -> str:
    """Return the email address inside *value*, or "" if there is not one.

    Handles a bare address, a ``Name <addr>`` form, and a Redshift username
    that happens to be an address. A username with no ``@`` yields "".
    """
    text = _text(value)
    if not text:
        return ""
    match = _EMAIL_RE.search(text)
    return match.group(0) if match else ""


def resolve_recipient(
    user_value: object,
    roster: pd.DataFrame | None = None,
) -> tuple[str, str]:
    """Resolve (email, display_name) for a captured username.

    Prefers an address embedded in the username itself; falls back to the
    ``email`` column of the captured user roster. Returns ("", name) when no
    address is known - the caller must handle that rather than guessing a
    domain, because a wrong address sends a real email to a real stranger.
    """
    name = _text(user_value)
    if not name:
        return "", ""

    direct = extract_email(name)
    if direct:
        return direct, name

    if roster is not None and not roster.empty and "email" in roster.columns:
        column = "user_name" if "user_name" in roster.columns else None
        if column:
            match = roster[roster[column].astype(str).str.strip().str.lower() == name.lower()]
            if not match.empty:
                found = extract_email(match.iloc[0].get("email"))
                if found:
                    return found, name
    return "", name


def _tables(group: dict) -> list[str]:
    raw = _text(group.get("sql_tables")) or _text(group.get("sql_tables_full"))
    if not raw:
        return []
    return [item.strip() for item in re.split(r"[,;]\s*", raw) if item.strip()]


def _fmt_hours(seconds: float) -> str:
    if seconds >= 3600:
        return f"{seconds / 3600:,.1f} hours"
    if seconds >= 60:
        return f"{seconds / 60:,.0f} minutes"
    return f"{seconds:,.0f} seconds"


def build_subject(group: dict) -> str:
    """A subject line that says which query, without jargon."""
    tables = _tables(group)
    if tables:
        shown = ", ".join(item.split(".")[-1] for item in tables[:MAX_SUBJECT_TABLES])
        if len(tables) > MAX_SUBJECT_TABLES:
            shown += f" +{len(tables) - MAX_SUBJECT_TABLES}"
        return f"Help identifying a recurring Redshift query on {shown}"
    identifier = _text(group.get("repeat_group_id")) or "a recurring query"
    return f"Help identifying a recurring Redshift query ({identifier})"


def build_body(
    group: dict,
    *,
    display_name: str = "",
    sender: str = "",
    sql: str = "",
    max_sql_chars: int = 1200,
) -> str:
    """A plain-English request for help identifying a query.

    Written for someone who may not read SQL and did not ask to be emailed:
    it says what was observed, why it matters, what is being asked, and it does
    not accuse anyone of anything.
    """
    greeting = f"Hi {display_name.split('@')[0].replace('.', ' ').title()}," if display_name else "Hi,"

    runs = int(pd.to_numeric(group.get("query_count"), errors="coerce") or 0)
    runtime = float(pd.to_numeric(group.get("total_runtime_s"), errors="coerce") or 0.0)
    avg = runtime / runs if runs else 0.0
    tables = _tables(group)

    lines: list[str] = [greeting, ""]
    lines.append(
        "We are reviewing the queries that take up the most time on our "
        "Redshift cluster, and one of them appears to be running under your "
        "user account. Before we change anything, we would like to understand "
        "what it is for."
    )
    lines.append("")
    lines.append("Here is what we can see:")
    lines.append("")

    if runs:
        lines.append(f"  - It ran {runs:,} times in the period we looked at.")
    if runtime:
        lines.append(f"  - Together those runs used about {_fmt_hours(runtime)} of cluster time.")
    if avg:
        lines.append(f"  - Each run takes roughly {_fmt_hours(avg)}.")
    if tables:
        listed = ", ".join(tables[:6]) + (" and others" if len(tables) > 6 else "")
        lines.append(f"  - It reads from: {listed}")

    lines.append("")
    lines.append("What would help us most:")
    lines.append("")
    lines.append("  1. Do you recognise this query, and what is it used for?")
    lines.append("  2. Is it something you run yourself, or does a report, "
                 "dashboard or scheduled job run it for you?")
    lines.append("  3. Does it still need to run as often as it does?")
    lines.append("")
    lines.append(
        "To be clear, nothing is wrong on your side - a query running often is "
        "usually a sign that it is useful. We are looking for the ones where a "
        "small change would save a lot of cluster time for everyone, and knowing "
        "what this one is for tells us whether that is worth doing."
    )

    if sql:
        body_sql = sql.strip()
        truncated = len(body_sql) > max_sql_chars
        if truncated:
            body_sql = body_sql[:max_sql_chars].rstrip()
        lines.append("")
        lines.append("For reference, this is the query (you do not need to read it):")
        lines.append("")
        lines.append(body_sql)
        if truncated:
            lines.append("... (shortened)")

    lines.append("")
    lines.append("Thanks very much,")
    if sender:
        lines.append(sender)
    return "\n".join(lines)


def build_mailto(
    email: str,
    subject: str,
    body: str,
    *,
    max_length: int = 1800,
) -> str:
    """Build a mailto: URL, trimming the body if the URL would be too long.

    Windows caps the command line a mailto handler receives, and Outlook
    silently opens a blank message when that cap is exceeded rather than
    reporting an error - so the body is trimmed to keep the URL usable.
    """
    trimmed = body
    while True:
        url = (
            f"mailto:{quote(email)}"
            f"?subject={quote(subject, safe='')}"
            f"&body={quote(trimmed, safe='')}"
        )
        if len(url) <= max_length or len(trimmed) <= 200:
            return url
        cut = max(200, int(len(trimmed) * 0.8))
        trimmed = trimmed[:cut].rstrip() + "\n... (shortened)"
