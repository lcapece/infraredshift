"""Interactive SQL X-ray helpers for the SQL Lens.

Pure logic (no Qt) so every piece is unit-testable:

- token/operator hit-testing for click probes in a SQL editor
- table metadata popups (sortkey/sorted %, distkey/skew, stats staleness)
- both-sides resolution for `=` / `!=` / `<>` comparisons
- one-level view explosion (view reference -> parenthesized definition)
- recursive object footprint: every table and view a statement touches,
  including tables reached only through nested views.
"""
from __future__ import annotations

import math
import re

from .join_size_highlight import alias_map
from .physical_lineage import expression_physical_origins
from .query_similarity import analyze_sql

_IDENT_CHARS = re.compile(r'[\w$."]')
_STRIP_CHARS = '"\'()[],;`'
_VIEW_MAX_DEPTH = 5


def clean_token(text: object) -> str:
    """Normalize a clicked token: strip quotes, parentheses, commas, and other
    punctuation so `("public"."fact_sales")` resolves like public.fact_sales."""
    token = str(text or "").strip()
    token = token.strip(_STRIP_CHARS)
    token = ".".join(part.strip(_STRIP_CHARS) for part in token.split("."))
    return token.strip(".").lower()


def token_at(text: str, offset: int) -> str:
    """The identifier-ish token spanning character `offset` in `text`."""
    body = str(text or "")
    if not body or offset < 0 or offset >= len(body):
        return ""
    if not _IDENT_CHARS.match(body[offset]):
        return ""
    start = offset
    while start > 0 and _IDENT_CHARS.match(body[start - 1]):
        start -= 1
    end = offset
    while end < len(body) and _IDENT_CHARS.match(body[end]):
        end += 1
    return clean_token(body[start:end])


def comparison_at(text: str, offset: int) -> tuple[str, str, str] | None:
    """(left_token, right_token, operator) when `offset` sits on or beside an
    `=`, `!=`, or `<>` comparison; None otherwise. `>=`/`<=` are not matches."""
    body = str(text or "")
    if not body:
        return None
    for pos in range(max(0, offset - 1), min(len(body), offset + 2)):
        op = ""
        if body[pos] == "=":
            prev = body[pos - 1] if pos > 0 else ""
            if prev in {"<", ">"}:
                continue
            op, start, end = ("!=", pos - 1, pos + 1) if prev == "!" else ("=", pos, pos + 1)
        elif body[pos] == "<" and pos + 1 < len(body) and body[pos + 1] == ">":
            op, start, end = "<>", pos, pos + 2
        else:
            continue
        left_end = start
        while left_end > 0 and body[left_end - 1].isspace():
            left_end -= 1
        left_start = left_end
        while left_start > 0 and _IDENT_CHARS.match(body[left_start - 1]):
            left_start -= 1
        right_start = end
        while right_start < len(body) and body[right_start].isspace():
            right_start += 1
        right_end = right_start
        while right_end < len(body) and _IDENT_CHARS.match(body[right_end]):
            right_end += 1
        left = clean_token(body[left_start:left_end])
        right = clean_token(body[right_start:right_end])
        if left or right:
            return left, right, op
    return None


def _to_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(number) else number


def _first(row: dict, *names: str) -> object:
    for name in names:
        if name in row and row.get(name) is not None:
            value = row.get(name)
            if isinstance(value, float) and math.isnan(value):
                continue
            text = str(value).strip()
            if text and text.lower() not in {"none", "nan", "<na>"}:
                return value
    return None


def build_table_lookup(rows) -> dict[str, dict]:
    """Suffix-keyed metadata map (db.schema.table / schema.table / table) from
    Table Review rows. Tolerates both raw SVV and typed-view column names."""
    lookup: dict[str, dict] = {}
    if rows is None:
        return lookup
    for row in rows:
        table = str(_first(row, "table_name", "table") or "").strip().lower()
        if not table:
            continue
        schema = str(_first(row, "schema_name", "schema") or "").strip().lower()
        database = str(_first(row, "source_db", "database") or "").strip().lower()
        entry = {
            "table": ".".join(part for part in (database, schema, table) if part),
            "sortkey1": str(_first(row, "sortkey1") or "").strip(),
            "diststyle": str(_first(row, "diststyle") or "").strip(),
            "unsorted": _to_float(_first(row, "unsorted_pct", "unsorted")),
            "stats_off": _to_float(_first(row, "stats_off")),
            "skew_rows": _to_float(_first(row, "skew_rows")),
            "tbl_rows": _to_float(_first(row, "tbl_rows")),
            "size_mb": _to_float(_first(row, "size_mb", "size")),
        }
        parts = [part for part in (database, schema, table) if part]
        for start in range(len(parts)):
            lookup.setdefault(".".join(parts[start:]), entry)
    return lookup


def _distkey(diststyle: str) -> str:
    match = re.search(r"key\s*\(\s*([^)]+?)\s*\)", str(diststyle or ""), re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return str(diststyle or "").strip() or "-"


def _fmt_rows(value: float | None) -> str:
    if value is None:
        return "?"
    for unit, size in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if value >= size:
            return f"{value / size:.1f}{unit}"
    return f"{value:,.0f}"


def table_popup_text(name: str, entry: dict) -> str:
    """The probe popup block:
    Sortkey: xxx  Sorted: xx%
    Distkey: xxx  Skew: xx.x
    Stats: xx% stale
    """
    unsorted = entry.get("unsorted")
    sorted_pct = f"{max(0.0, 100.0 - unsorted):.0f}%" if unsorted is not None else "n/a"
    skew = entry.get("skew_rows")
    skew_text = f"{skew:.1f}" if skew is not None else "n/a"
    stats_off = entry.get("stats_off")
    stats_text = f"{stats_off:.0f}% stale" if stats_off is not None else "n/a"
    size_mb = entry.get("size_mb")
    size_text = f", {size_mb / 1024:.1f} GB" if size_mb is not None else ""
    header = f"{entry.get('table') or name}  -  {_fmt_rows(entry.get('tbl_rows'))} rows{size_text}"
    return (
        f"{header}\n"
        f"Sortkey: {entry.get('sortkey1') or '-'}  Sorted: {sorted_pct}\n"
        f"Distkey: {_distkey(entry.get('diststyle'))}  Skew: {skew_text}\n"
        f"Stats: {stats_text}"
    )


def resolve_table(token: str, aliases: dict[str, str], lookup: dict[str, dict]) -> tuple[str, dict] | None:
    """Resolve a clicked token (table name, alias, or alias.column) to a
    metadata entry."""
    token = clean_token(token)
    if not token:
        return None
    if token in lookup:
        return token, lookup[token]
    if token in aliases and aliases[token] in lookup:
        return aliases[token], lookup[aliases[token]]
    if "." in token:
        qualifier = token.rsplit(".", 1)[0]
        base = qualifier.split(".")[-1]
        for candidate in (qualifier, aliases.get(base, ""), base):
            if candidate and candidate in lookup:
                return candidate, lookup[candidate]
    return None


def comparison_popup_text(
    left: str,
    right: str,
    op: str,
    sql: str,
    lookup: dict[str, dict],
    view_definitions: object = None,
) -> str:
    """Both-sides popup for a comparison: resolve each operand's table via the
    statement's FROM/JOIN aliases and stack the two metadata blocks."""
    aliases = alias_map(sql)
    blocks: list[str] = [f"{left or '?'} {op} {right or '?'}"]
    for label, side in (("LEFT", left), ("RIGHT", right)):
        if not side:
            blocks.append(f"{label}: (nothing clickable)")
            continue
        if side[0].isdigit():
            blocks.append(f"{label}: {side}  -  literal value")
            continue
        physical = expression_physical_origins(sql, side, view_definitions)
        if physical:
            blocks.append(f"{label} PHYSICAL ORIGIN(S):")
            for origin in physical:
                entry = _lookup_physical_origin(origin.table_key, lookup)
                if entry is None:
                    blocks.append(
                        f"{origin.display()}\nPhysical source resolved; captured table metadata is unavailable."
                    )
                else:
                    blocks.append(
                        f"{origin.display()}\n{table_popup_text(origin.display(), entry)}"
                    )
            continue
        resolved = resolve_table(side, aliases, lookup)
        if resolved is None:
            blocks.append(f"{label}: {side}  -  no captured table metadata")
        else:
            _, entry = resolved
            blocks.append(f"{label}: {side}\n{table_popup_text(side, entry)}")
    return "\n\n".join(blocks)


def _lookup_physical_origin(table_key: str, lookup: dict[str, dict]) -> dict | None:
    key = clean_token(table_key)
    if key in lookup:
        return lookup[key]
    parts = key.split(".")
    for start in range(1, len(parts)):
        candidate = ".".join(parts[start:])
        if candidate in lookup:
            return lookup[candidate]
    return None


# ------------------------------------------------------------------- views


def build_view_map(rows) -> dict[str, str]:
    """Suffix-keyed view-name -> definition map from captured view rows.
    Accepts source_definition / view_definition / definition_part_* columns."""
    views: dict[str, str] = {}
    if rows is None:
        return views
    for row in rows:
        name = str(_first(row, "view_name", "table_name") or "").strip().lower()
        if not name:
            continue
        definition = str(_first(row, "source_definition", "view_definition", "definition") or "")
        if not definition:
            parts = []
            for index in range(1, 13):
                part = row.get(f"definition_part_{index:02d}")
                if part is None or (isinstance(part, float) and math.isnan(part)):
                    continue
                parts.append(str(part))
            definition = "".join(parts)
        definition = _normalize_view_definition_sql(definition)
        if not definition:
            continue
        schema = str(_first(row, "schema_name", "schema") or "").strip().lower()
        database = str(_first(row, "source_db", "database") or "").strip().lower()
        parts = [part for part in (database, schema, name) if part]
        for start in range(len(parts)):
            views.setdefault(".".join(parts[start:]), definition)
    return views


def _normalize_view_definition_sql(definition: object) -> str:
    """Return only the SELECT/WITH body so it is legal inside parentheses."""
    text = str(definition or "").strip().rstrip(";").strip()
    if not text:
        return ""
    create_view = re.match(
        r"^\s*create\s+(?:or\s+replace\s+)?(?:late\s+binding\s+)?view\b[\s\S]*?\bas\b\s*",
        text,
        flags=re.IGNORECASE,
    )
    if create_view:
        text = text[create_view.end():].strip()
    text = re.sub(
        r"\s+with\s+no\s+schema\s+binding\s*$",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip().rstrip(";").strip()
    return text


_FROM_JOIN_REF_RE = re.compile(
    r"(\b(?:from|join)\s+)"
    r"((?:\"[^\"]+\"|[a-z_][\w$]*)(?:\.(?:\"[^\"]+\"|[a-z_][\w$]*)){0,2})"
    r"(\s+(?:as\s+)?(?!on\b|where\b|inner\b|left\b|right\b|full\b|cross\b|join\b|group\b|order\b|using\b|union\b)"
    r"([a-z_][\w$]*))?",
    re.IGNORECASE,
)


def explode_views(sql: str, view_map: dict[str, str]) -> tuple[str, list[str]]:
    """Replace each FROM/JOIN reference to a captured view with the view's
    definition as a parenthesized subquery, keeping (or minting) the alias so
    outer column references keep working. One level per call: run again to
    open nested views. Returns (new_sql, exploded_view_names)."""
    exploded: list[str] = []

    def _replace(match: re.Match) -> str:
        raw_name = match.group(2)
        name = clean_token(raw_name)
        definition = view_map.get(name) or view_map.get(name.split(".")[-1])
        if not definition:
            return match.group(0)
        alias = match.group(4) or name.split(".")[-1]
        exploded.append(name)
        indented = "\n".join("    " + line for line in definition.splitlines())
        return f"{match.group(1)}(\n{indented}\n) AS {alias}"

    return _FROM_JOIN_REF_RE.sub(_replace, str(sql or "")), exploded


def explode_views_recursive(
    sql: str,
    view_map: dict[str, str],
    max_depth: int = _VIEW_MAX_DEPTH,
) -> tuple[str, list[str]]:
    """Inline all reachable captured views while preventing recursive cycles."""
    expanded, exploded, _spans = explode_views_recursive_with_spans(sql, view_map, max_depth)
    return expanded, exploded


def explode_views_recursive_with_spans(
    sql: str,
    view_map: dict[str, str],
    max_depth: int = _VIEW_MAX_DEPTH,
) -> tuple[str, list[str], list[dict]]:
    """Inline views and return exact highlighted ranges for each nesting depth.

    Each span covers the complete parenthesized SQL that replaced a view.
    Nested spans are returned after their parents so UI shading can override
    the parent yellow with progressively deeper amber shades.
    """
    current = str(sql or "")
    remaining = dict(view_map or {})
    all_exploded: list[str] = []
    marker_meta: dict[str, dict] = {}
    marker_counter = 0
    for depth in range(max(0, int(max_depth))):
        if not remaining:
            break
        exploded: list[str] = []

        def replace(match: re.Match) -> str:
            nonlocal marker_counter
            raw_name = match.group(2)
            name = clean_token(raw_name)
            definition = remaining.get(name) or remaining.get(name.split(".")[-1])
            if not definition:
                return match.group(0)
            alias = match.group(4) or name.split(".")[-1]
            marker_id = f"{marker_counter:08d}"
            marker_counter += 1
            marker_meta[marker_id] = {"view": name, "depth": depth}
            exploded.append(name)
            indented = "\n".join("    " + line for line in definition.splitlines())
            start = f"/*__RQA_VIEW_START_{marker_id}__*/"
            end = f"/*__RQA_VIEW_END_{marker_id}__*/"
            return f"{match.group(1)}{start}(\n{indented}\n){end} AS {alias}"

        current = _FROM_JOIN_REF_RE.sub(replace, current)
        if not exploded:
            break
        all_exploded.extend(exploded)
        exploded_names = {clean_token(name) for name in exploded}
        exploded_leaf_names = {name.split(".")[-1] for name in exploded_names}
        remaining = {
            key: definition
            for key, definition in remaining.items()
            if clean_token(key) not in exploded_names
            and clean_token(key).split(".")[-1] not in exploded_leaf_names
        }

    marker_pattern = re.compile(r"/\*__RQA_VIEW_(START|END)_([0-9]{8})__\*/")
    clean_parts: list[str] = []
    clean_length = 0
    starts: dict[str, int] = {}
    spans: list[dict] = []
    cursor = 0
    for marker in marker_pattern.finditer(current):
        chunk = current[cursor:marker.start()]
        clean_parts.append(chunk)
        clean_length += len(chunk)
        kind, marker_id = marker.group(1), marker.group(2)
        if kind == "START":
            starts[marker_id] = clean_length
        elif marker_id in starts:
            meta = marker_meta.get(marker_id, {})
            spans.append(
                {
                    "start": starts[marker_id],
                    "end": clean_length,
                    "depth": int(meta.get("depth", 0)),
                    "view": str(meta.get("view") or ""),
                }
            )
        cursor = marker.end()
    tail = current[cursor:]
    clean_parts.append(tail)
    clean_sql = "".join(clean_parts)
    spans.sort(key=lambda item: (int(item["depth"]), int(item["start"])))
    return clean_sql, list(dict.fromkeys(all_exploded)), spans


def resolve_footprint(
    sql: str,
    view_map: dict[str, str],
    lookup: dict[str, dict],
    max_depth: int = _VIEW_MAX_DEPTH,
) -> list[dict]:
    """Every table and view the statement touches, recursing through captured
    view definitions. Each row: object, kind (view/table/unknown), via (view
    chain), depth, plus table metadata when captured."""
    rows: list[dict] = []
    seen: set[tuple[str, str]] = set()

    def _walk(statement: str, via: str, depth: int) -> None:
        if depth > max_depth:
            return
        intel = analyze_sql(statement)
        for raw in sorted(str(t).strip().lower() for t in intel.tables if str(t).strip()):
            name = clean_token(raw)
            key = (name, via)
            if not name or key in seen:
                continue
            seen.add(key)
            definition = view_map.get(name) or view_map.get(name.split(".")[-1])
            entry = lookup.get(name) or lookup.get(name.split(".")[-1])
            if definition is not None:
                rows.append({"object": name, "kind": "view", "via": via or "-", "depth": depth})
                _walk(definition, f"{via} > {name}" if via else name, depth + 1)
            else:
                row = {"object": name, "kind": "table" if entry else "unknown", "via": via or "-", "depth": depth}
                if entry:
                    row.update(
                        {
                            "tbl_rows": entry.get("tbl_rows"),
                            "size_mb": entry.get("size_mb"),
                            "diststyle": entry.get("diststyle"),
                            "sortkey1": entry.get("sortkey1"),
                            "unsorted": entry.get("unsorted"),
                            "stats_off": entry.get("stats_off"),
                            "skew_rows": entry.get("skew_rows"),
                        }
                    )
                rows.append(row)

    _walk(str(sql or ""), "", 0)
    return rows
