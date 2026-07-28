"""Table identity helpers for Redshift AST nodes."""

from __future__ import annotations

from sqlglot import exp

from .catalog import normalize_ident, normalize_key


def table_identity(table: exp.Table) -> str:
    return normalize_key(table.catalog, table.db, table.name)


def table_alias(table: exp.Table) -> str:
    return normalize_ident(table.alias_or_name or table.name)


def quote_ident(name: str) -> str:
    text = str(name or "")
    return '"' + text.replace('"', '""') + '"'


def qualified_sql(identity: str) -> str:
    return ".".join(quote_ident(part) for part in identity.split(".") if part)


def safe_temp_name(index: int, label: str, used: set[str]) -> str:
    base = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in normalize_ident(label))
    base = base.strip("_") or "stage"
    base = base[:36]
    candidate = f"tmp_rsd_{index:02d}_{base}"
    n = 1
    while candidate in used:
        n += 1
        candidate = f"tmp_rsd_{index:02d}_{base}_{n}"
    used.add(candidate)
    return candidate
