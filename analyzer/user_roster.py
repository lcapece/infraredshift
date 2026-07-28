"""Parse a user roster from PG_USER usernames.

Usernames arrive in these shapes (producer cluster, enterprise_data_warehouse):
    firstname.lastname@citizensbank.com
    firstname.lastname@citizensprivatebank.com
    IAM:firstname.lastname@citizensbank.com
    louis.n.capece@citizensbank.com          (middle name / initial)

Rule: drop an optional ``IAM:`` prefix, take the local part before ``@``, split
on ``.`` -> first token = first name, last token = last name, anything between
= middle name / initial. Usernames that do not fit this shape are still kept in
the roster with best-effort fields so nothing is silently dropped.
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class ParsedUser:
    user_name: str
    email: str
    first_name: str
    middle_name: str
    middle_initial: str
    last_name: str
    domain: str
    parsed: bool


def parse_username(user_name: object) -> ParsedUser:
    raw = str(user_name or "").strip()
    work = raw
    # Optional IAM: (or similar) prefix before the email.
    if ":" in work:
        prefix, _, rest = work.partition(":")
        if prefix.strip().lower() in {"iam", "iamrole", "iam_role", "role"}:
            work = rest.strip()
    email = work
    local, _, domain = work.partition("@")
    local = local.strip()
    domain = domain.strip().lower()
    if not local or "." not in local:
        # Not the expected shape: keep the row, leave name fields blank.
        return ParsedUser(
            user_name=raw, email=email, first_name=local, middle_name="",
            middle_initial="", last_name="", domain=domain, parsed=False,
        )
    tokens = [t for t in local.split(".") if t]
    first = tokens[0] if tokens else ""
    last = tokens[-1] if len(tokens) >= 2 else ""
    middle_tokens = tokens[1:-1] if len(tokens) > 2 else []
    middle_name = " ".join(middle_tokens)
    # A single-character middle token is an initial; otherwise take the first
    # letter of the first middle token as the initial.
    middle_initial = ""
    if middle_tokens:
        first_mid = middle_tokens[0]
        middle_initial = first_mid[0].upper() if first_mid else ""
    return ParsedUser(
        user_name=raw,
        email=email,
        first_name=first,
        middle_name=middle_name,
        middle_initial=middle_initial,
        last_name=last,
        domain=domain,
        parsed=bool(first and last),
    )


def build_user_roster(user_rows: pd.DataFrame) -> pd.DataFrame:
    """Turn a PG_USER frame (user_id, user_name) into a parsed roster frame."""
    columns = [
        "user_id", "user_name", "email", "first_name", "middle_name",
        "middle_initial", "last_name", "domain", "parsed",
    ]
    if user_rows is None or user_rows.empty:
        return pd.DataFrame(columns=columns)
    cols = {c.lower(): c for c in user_rows.columns}
    name_col = cols.get("user_name") or cols.get("usename")
    id_col = cols.get("user_id") or cols.get("usesysid")
    if not name_col:
        return pd.DataFrame(columns=columns)
    records = []
    seen: set[str] = set()
    for _, row in user_rows.iterrows():
        parsed = parse_username(row.get(name_col))
        key = parsed.user_name.lower()
        if key in seen:
            continue
        seen.add(key)
        records.append(
            {
                "user_id": row.get(id_col) if id_col else None,
                "user_name": parsed.user_name,
                "email": parsed.email,
                "first_name": parsed.first_name,
                "middle_name": parsed.middle_name,
                "middle_initial": parsed.middle_initial,
                "last_name": parsed.last_name,
                "domain": parsed.domain,
                "parsed": parsed.parsed,
            }
        )
    return pd.DataFrame(records, columns=columns)
