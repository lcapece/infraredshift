"""Last-resort SQL reflow for statements the strict parser rejects.

sqlglot refuses to format anything it cannot fully parse, so one unknown token
makes the Format button fail on an otherwise simple query. This formatter never
parses: it protects string literals, collapses whitespace, uppercases major
keywords, and breaks lines before them. Output is always produced.
"""
from __future__ import annotations

import re

_KEYWORD_RE = re.compile(
    r"(?i)\b("
    r"select|from|where|group\s+by|order\s+by|having|union\s+all|union|limit|"
    r"(?:left|right|full|inner|cross)(?:\s+outer)?\s+join|join|"
    r"on|and|or|when|else|case|end"
    r")\b"
)
_INDENTED = {"on", "and", "or", "when", "else"}


def soft_format_sql(sql: object) -> str:
    text = str(sql or "").strip()
    if not text:
        return ""
    literals: list[str] = []

    def _stash(match: re.Match) -> str:
        literals.append(match.group(0))
        return f"\x00{len(literals) - 1}\x00"

    protected = re.sub(r"'(?:''|[^'])*'", _stash, text)
    protected = re.sub(r"\s+", " ", protected).strip()

    def _break(match: re.Match) -> str:
        word = re.sub(r"\s+", " ", match.group(1))
        indent = "  " if word.lower() in _INDENTED else ""
        return "\n" + indent + word.upper()

    formatted = _KEYWORD_RE.sub(_break, protected).strip()
    formatted = re.sub(r"\n{2,}", "\n", formatted)

    def _restore(match: re.Match) -> str:
        return literals[int(match.group(1))]

    return re.sub(r"\x00(\d+)\x00", _restore, formatted)
