"""Shared Redshift physical-design metadata constants.

Keep AUTO / missing sortkey and distkey vocabularies in one place so triage,
recommenders, fix scripts, and SQL Lens agree on what "no real key" means.
"""
from __future__ import annotations

# Values that mean "this table does not have a durable, DBA-chosen sort key."
# Modern Redshift AUTO tables report forms like auto(sortkey); treating those as
# real sort keys produces false "sortkey rarely prunes" findings.
MISSING_SORTKEY_VALUES: frozenset[str] = frozenset(
    {
        "",
        "-",
        "none",
        "null",
        "nan",
        "0",
        "auto",
        "(auto)",
        "auto(none)",
        "auto(sortkey)",
        "(auto(sortkey))",
        "sortkey(auto)",
        "(sortkey(auto))",
        "sortkey",
        "(sortkey)",
    }
)

# Values that mean "no durable DBA-chosen distkey" (includes AUTO forms).
MISSING_DISTKEY_VALUES: frozenset[str] = frozenset(
    {
        "",
        "-",
        "none",
        "null",
        "nan",
        "0",
        "auto",
        "(auto)",
        "auto(even)",
        "auto(key)",
        "auto(none)",
        "auto(all)",
        "distkey(auto)",
        "key(auto)",
        "even",
        "all",
    }
)


def clean_key_token(value: object) -> str:
    """Normalize a diststyle/sortkey token for membership checks."""
    text = str(value if value is not None else "").strip().strip('"').strip("'").lower()
    return " ".join(text.split())


def is_missing_sortkey(value: object) -> bool:
    return clean_key_token(value) in MISSING_SORTKEY_VALUES


def is_missing_distkey(value: object) -> bool:
    token = clean_key_token(value)
    if token in MISSING_DISTKEY_VALUES:
        return True
    # DISTSTYLE fragments like "KEY(AUTO)" still count as missing a chosen key.
    return "auto" in token and "key" in token
