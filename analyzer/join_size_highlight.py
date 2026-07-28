"""Size-aware coloring of `=` conditions and table references in displayed SQL.

Pure logic (no Qt) so it is unit-testable: given SQL text and table metadata
(size_mb, sortkey1, diststyle), annotate each line with colored spans:

- ``red``    - an `=` join/filter where a LARGE table is compared off its sort
               key (the matching key does not correlate with the table's
               physical order - the expensive case).
- ``yellow`` - an `=` involving a large table that is otherwise acceptable
               (key matches sortkey/distkey) or has a mixed/unknown attribute.
- ``green``  - an `=` where every resolvable table is below the large
               threshold: inconsequential.
- ``large``  - any reference to a table at/above the threshold (predicate
               visibility highlight).
- ``small``  - override: any reference to a table below the threshold is
               de-emphasized (gray) so it reads as present but inconsequential.
"""
from __future__ import annotations

import math
import re

LARGE_TABLE_MB_DEFAULT = 5120.0  # 5 GB

_RESERVED_ALIAS = {
    "where", "on", "join", "inner", "left", "right", "full", "cross", "outer",
    "group", "order", "having", "limit", "union", "select", "using", "set",
    "when", "then", "and", "or", "not", "as", "lateral", "natural",
}

_FROM_JOIN_RE = re.compile(
    r"\b(?:from|join|update|into)\s+"
    r"((?:\"[^\"]+\"|[a-z_][\w$]*)(?:\.(?:\"[^\"]+\"|[a-z_][\w$]*)){0,2})"
    r"(?:\s+(?:as\s+)?([a-z_][\w$]*))?",
    re.IGNORECASE,
)

_EQ_RE = re.compile(
    r"((?:[a-z_][\w$]*\.){0,2}[a-z_][\w$]*)"
    r"\s*=\s*"
    r"((?:[a-z_][\w$]*\.){0,2}[a-z_][\w$]*|'(?:''|[^'])*'|\d+(?:\.\d+)?)",
    re.IGNORECASE,
)


def _to_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if math.isnan(number) else number


def build_table_meta(rows) -> dict[str, dict]:
    """Metadata lookup keyed by every suffix of the table name (db.schema.table,
    schema.table, table) so unqualified SQL references still resolve."""
    meta: dict[str, dict] = {}
    if rows is None:
        return meta
    for row in rows:
        name = str(row.get("table_name") or "").strip().lower().strip('"')
        if not name:
            continue
        entry = {
            "size_mb": _to_float(row.get("size_mb")),
            "sortkey1": str(row.get("sortkey1") or "").strip().lower(),
            "diststyle": str(row.get("diststyle") or "").strip().lower(),
        }
        parts = name.split(".")
        for start in range(len(parts)):
            meta.setdefault(".".join(parts[start:]), entry)
    return meta


def alias_map(sql: str) -> dict[str, str]:
    """alias (or bare table name) -> table name, regex-based so it works on any
    displayable text even when a strict parser would fail."""
    mapping: dict[str, str] = {}
    for match in _FROM_JOIN_RE.finditer(str(sql or "")):
        table = match.group(1).replace('"', "").strip().lower()
        alias = (match.group(2) or "").strip().lower()
        if alias in _RESERVED_ALIAS:
            alias = ""
        short = table.split(".")[-1]
        mapping.setdefault(short, table)
        mapping.setdefault(table, table)
        if alias:
            mapping[alias] = table
    return mapping


def _side_table(side: str, aliases: dict[str, str]) -> str:
    text = str(side or "").strip().lower()
    if text.startswith("'") or (text and text[0].isdigit()):
        return ""  # literal
    if "." in text:
        qualifier = text.rsplit(".", 1)[0].split(".")[-1]
        return aliases.get(qualifier, "")
    if len(set(aliases.values())) == 1:
        return next(iter(aliases.values()))  # single-table statement
    return ""


def _side_column(side: str) -> str:
    text = str(side or "").strip().lower()
    if text.startswith("'") or (text and text[0].isdigit()):
        return ""
    return text.rsplit(".", 1)[-1]


def classify_equality(
    left: str,
    right: str,
    aliases: dict[str, str],
    meta: dict[str, dict],
    large_mb: float = LARGE_TABLE_MB_DEFAULT,
) -> str:
    """'' (unknown) | 'red' | 'yellow' | 'green' for one `left = right`."""
    sides = []
    for side in (left, right):
        table = _side_table(side, aliases)
        if not table:
            continue
        entry = meta.get(table) or meta.get(table.split(".")[-1])
        if entry is None:
            continue
        sides.append((entry, _side_column(side)))
    if not sides:
        return ""
    large_sides = [(entry, column) for entry, column in sides if entry["size_mb"] >= large_mb]
    if not large_sides:
        return "green"
    for entry, column in large_sides:
        sortkey = entry.get("sortkey1") or ""
        if not sortkey or column != sortkey:
            return "red"
    return "yellow"


def annotate_line(
    line: str,
    aliases: dict[str, str],
    meta: dict[str, dict],
    large_mb: float = LARGE_TABLE_MB_DEFAULT,
) -> list[tuple[int, int, str]]:
    """Spans for one display line: (start, end, kind) where kind is one of
    'red' / 'yellow' / 'green' (equality conditions), 'large' / 'small'
    (table references). Table spans first so equality colors paint on top."""
    spans: list[tuple[int, int, str]] = []
    text = str(line or "")
    lowered = text.lower()
    for name in sorted(meta, key=len, reverse=True):
        entry = meta[name]
        kind = "large" if entry["size_mb"] >= large_mb else "small"
        for match in re.finditer(rf"(?<![\w$.]){re.escape(name)}(?![\w$])", lowered):
            spans.append((match.start(), match.end(), kind))
    # De-emphasize aliases of small tables too, so predicates read gray.
    for alias, table in aliases.items():
        entry = meta.get(table) or meta.get(table.split(".")[-1])
        if entry is None or entry["size_mb"] >= large_mb or alias in meta:
            continue
        for match in re.finditer(rf"(?<![\w$.]){re.escape(alias)}(?=\.)", lowered):
            spans.append((match.start(), match.end(), "small"))
    for match in _EQ_RE.finditer(text):
        severity = classify_equality(match.group(1), match.group(2), aliases, meta, large_mb)
        if severity:
            spans.append((match.start(), match.end(), severity))
    return spans
