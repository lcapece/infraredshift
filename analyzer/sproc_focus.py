"""Locate executable statement spans inside a stored procedure body.

The UI uses these character spans to keep DML/DDL statements in full color
while dimming declarations, control flow, comments, and other scaffolding.
The scanner is heuristic by design: when it finds nothing, the caller shows
the text undimmed, so a miss never hides information.
"""
from __future__ import annotations

import re

# Statements a DBA reads for workload insight.
_EXEC_START = {
    "select",
    "with",
    "insert",
    "update",
    "delete",
    "merge",
    "truncate",
    "copy",
    "unload",
    "call",
    "execute",
    "analyze",
    "vacuum",
    "refresh",
    "create",
    "drop",
    "alter",
    "grant",
    "revoke",
}

# CREATE [OR REPLACE] PROCEDURE/FUNCTION is the wrapper, not workload SQL.
_HEADER_GUARD = {"procedure", "function"}

# Words that both re-open "expecting a statement" and are themselves scaffolding.
_BLOCK_WORDS = {"begin", "declare", "exception", "then", "else", "loop", "end"}

# Control-flow heads whose condition runs to THEN/LOOP instead of a semicolon.
_CONDITION_HEADS = {"if", "elsif", "elseif", "while", "when", "case"}

# Keywords that mark embedded queries inside otherwise-scaffolding statements
# (variable assignments, RETURN QUERY, OPEN cursor FOR ...).
_EMBEDDED_QUERY_WORDS = {"select", "with", "insert", "update", "delete"}

_WORD_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
_DOLLAR_TAG_RE = re.compile(r"\$[A-Za-z_]*\$")
_BODY_HINT_RE = re.compile(r"\b(begin|select|insert|update|delete|call)\b", re.IGNORECASE)


def _tokens(sql: str) -> list[tuple[str, int, int]]:
    """Yield (kind, start, end) tokens: kind 'w' for words, ';' for terminators.

    Comments, quoted strings, and dollar-quoted literals are skipped, except
    that a dollar-quoted section that looks like the procedure body itself
    (contains BEGIN/DML) is scanned transparently.
    """
    out: list[tuple[str, int, int]] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch == "-" and sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j + 1
            continue
        if ch == "/" and sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
            continue
        if ch == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    break
                j += 1
            i = min(j + 1, n)
            continue
        if ch == '"':
            j = sql.find('"', i + 1)
            i = n if j < 0 else j + 1
            continue
        if ch == "$":
            match = _DOLLAR_TAG_RE.match(sql, i)
            if match:
                tag = match.group(0)
                close = sql.find(tag, match.end())
                inner = sql[match.end() : close] if close >= 0 else sql[match.end() :]
                if len(inner) > 40 and _BODY_HINT_RE.search(inner):
                    i = match.end()  # transparent: scan the body inside
                    continue
                i = n if close < 0 else close + len(tag)
                continue
        if ch == ";":
            out.append((";", i, i + 1))
            i += 1
            continue
        match = _WORD_RE.match(sql, i)
        if match:
            out.append(("w", match.start(), match.end()))
            i = match.end()
            continue
        i += 1
    return out


def _merge(spans: list[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for start, end in sorted(spans):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def executable_statement_spans(sql: str) -> list[tuple[int, int]]:
    """Return merged (start, end) character spans of executable statements."""
    text = sql or ""
    lower = text.lower()
    toks = _tokens(text)
    n = len(toks)
    spans: list[tuple[int, int]] = []
    i = 0
    awaiting = True
    while i < n:
        kind, start, end = toks[i]
        if kind == ";":
            awaiting = True
            i += 1
            continue
        word = lower[start:end]
        if not awaiting:
            if word in _BLOCK_WORDS:
                awaiting = True
            i += 1
            continue
        if word in _BLOCK_WORDS:
            i += 1
            continue
        if word == "create":
            lookahead = 0
            is_header = False
            j = i + 1
            while j < n and lookahead < 3 and toks[j][0] == "w":
                next_word = lower[toks[j][1] : toks[j][2]]
                if next_word in _HEADER_GUARD:
                    is_header = True
                    break
                lookahead += 1
                j += 1
            if is_header:
                i += 1
                continue
        if word == "for":
            # FOR record IN SELECT ... LOOP — highlight the embedded query.
            j = i + 1
            query_start = None
            loop_boundary = None
            while j < n:
                tok_kind, tok_start, tok_end = toks[j]
                if tok_kind == ";":
                    break
                tok_word = lower[tok_start:tok_end]
                if query_start is None and tok_word in ("select", "with"):
                    query_start = tok_start
                if tok_word == "loop":
                    loop_boundary = tok_start
                    break
                j += 1
            if query_start is not None and loop_boundary is not None:
                spans.append((query_start, loop_boundary))
            i = j + 1
            awaiting = True
            continue
        if word in _CONDITION_HEADS:
            j = i + 1
            while j < n:
                tok_kind, tok_start, tok_end = toks[j]
                if tok_kind == ";":
                    break
                if lower[tok_start:tok_end] in ("then", "loop"):
                    break
                j += 1
            i = j + 1
            awaiting = True
            continue
        if word in _EXEC_START:
            j = i + 1
            while j < n and toks[j][0] != ";":
                j += 1
            spans.append((start, toks[j][2] if j < n else len(text)))
            i = j + 1
            awaiting = True
            continue
        # Scaffolding statement (assignment, RAISE, RETURN, OPEN ... FOR, ...):
        # dim it, but surface any query embedded inside it.
        j = i + 1
        embedded_start = None
        while j < n and toks[j][0] != ";":
            tok_word = lower[toks[j][1] : toks[j][2]]
            if embedded_start is None and tok_word in _EMBEDDED_QUERY_WORDS:
                embedded_start = toks[j][1]
            j += 1
        if embedded_start is not None:
            spans.append((embedded_start, toks[j][2] if j < n else len(text)))
        i = j + 1
        awaiting = True
    return _merge(spans)
