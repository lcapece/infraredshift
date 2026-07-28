"""Paste text → canonical DataFrame.

Handles DBeaver's common formats and messy paste realities:
  - Headers present (canonical or vendor names, any case)
  - Headers absent (DBeaver's default copy) → positional template match,
    then value-shape inference as fallback
  - TSV, CSV (quoted), pipe-wrapped "| a | b |" rows, semicolon, pretty-
    print box-drawing separators
  - DBeaver row-number prefixes ("1  scan  store_sales  ...")

Every parse result carries a `.attrs["parse_notes"]` list explaining what
was detected so the UI can surface "Detected headerless paste; matched
sys_query_details 38-column layout" instead of hiding the magic.
"""
from __future__ import annotations
import io
import re
from typing import Iterable

import pandas as pd

from .schema import (
    QUERY_DETAIL_ALIASES,
    QUERY_DETAIL_COLUMNS,
    TABLE_INFO_ALIASES,
    TABLE_INFO_COLUMNS,
)


class ParseError(ValueError):
    pass


# Canonical positional layouts for Redshift's system views. If a headerless
# paste has exactly this column count, trust the layout.
SYS_QUERY_DETAIL_POSITIONAL: tuple[str, ...] = (
    "user_id", "query_id", "transaction_id", "session_id", "database_name",
    "parent_query_id", "child_query_sequence", "start_time", "end_time",
    "elapsed_time", "aborted", "step_name", "table_id", "table_name",
    "schema_name", "input_bytes", "input_rows", "output_bytes", "output_rows",
    "query_text", "source_query_id", "redshift_version", "result_cache_hit",
    "step_id", "stream_id", "is_rrscan", "segment_id", "is_child",
    "source_type", "query_blocks_read", "spilled_block_local_disk",
    "spilled_block_remote_disk", "data_skewness", "time_skewness",
    "is_delayed_scan", "delayed_scan_rows", "join_type", "alert_type",
)

SVV_TABLE_INFO_POSITIONAL: tuple[str, ...] = (
    "database", "schema", "table_id", "table", "encoded", "diststyle",
    "sortkey1", "max_varchar", "sortkey1_enc", "sortkey_num", "size",
    "pct_used", "empty", "unsorted", "stats_off", "tbl_rows",
    "skew_sortkey1", "skew_rows", "estimated_visible_rows", "risk_event",
    "vacuum_sort_benefit", "create_time",
)


# ---------- Pre-clean ---------------------------------------------------------

_BOX_DRAWING = re.compile(r"^[\s\u2500-\u257F\-=+_|`~*]+$")
_PIPE_WRAP = re.compile(r"^\s*\|(.*)\|\s*$")
_ROW_NUM_PREFIX = re.compile(r"^\s*\d+\s*[:|]\s+")


def _preclean(text: str) -> str:
    """Strip noise that DBeaver / terminal copies add."""
    lines: list[str] = []
    for raw in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = raw.rstrip()
        if not line.strip():
            continue
        # Drop pure separator lines (├───┼───┤ etc).
        if _BOX_DRAWING.match(line):
            continue
        # Strip "| ... |" pipe wrapping, then use | as the delimiter.
        m = _PIPE_WRAP.match(line)
        if m:
            line = m.group(1)
        # Strip leading row number prefix ("1  foo bar", "1| foo", "1: foo").
        line = _ROW_NUM_PREFIX.sub("", line)
        lines.append(line)
    return "\n".join(lines)


# ---------- Delimiter sniffing -----------------------------------------------

def _sniff_delimiter(sample: str) -> str:
    candidates = ["\t", "|", ";", ","]
    lines = [ln for ln in sample.splitlines()[:40] if ln.strip()]
    if not lines:
        return "\t"
    best = "\t"
    best_score = -1
    for d in candidates:
        counts = [ln.count(d) for ln in lines]
        if not counts or counts[0] == 0:
            continue
        consistent = sum(1 for c in counts if c == counts[0])
        score = counts[0] * consistent
        if score > best_score:
            best_score = score
            best = d
    # If no candidate found a hit, fall back to whitespace.
    if best_score < 0:
        return r"\s{2,}"
    return best


# ---------- Raw read ----------------------------------------------------------

def _read_rows(text: str, delim: str) -> pd.DataFrame:
    kwargs: dict = dict(
        sep=delim,
        engine="python",
        dtype=str,
        keep_default_na=False,
        na_values=["", "NULL", "null", "None", "NaN", "<null>"],
        header=None,
        skipinitialspace=True,
        quotechar='"',
        on_bad_lines="skip",
    )
    try:
        return pd.read_csv(io.StringIO(text), **kwargs)
    except Exception as e:
        raise ParseError(f"could not tokenize paste: {e}") from e


# ---------- Header detection --------------------------------------------------

_NUMERIC_RE = re.compile(r"^-?\d[\d,\.eE+\-]*$")
_TIMESTAMP_HINT = re.compile(r"\d{4}-\d{2}-\d{2}")


def _looks_like_data(cell: str | None) -> bool:
    if cell is None:
        return True
    t = str(cell).strip()
    if not t or t.upper() in {"NULL", "NONE", "NAN"}:
        return True
    if _NUMERIC_RE.match(t):
        return True
    if _TIMESTAMP_HINT.search(t):
        return True
    return False


def _header_confidence(row: Iterable[str], known: set[str]) -> float:
    """Fraction of row cells that match known column names (canonical or alias)."""
    cells = [_normalize_header(str(c or "")) for c in row]
    if not cells:
        return 0.0
    hits = sum(1 for c in cells if c and c in known)
    return hits / len(cells)


def _normalize_header(name: str) -> str:
    n = name.strip().strip('"').strip("'").lower()
    n = re.sub(r"[^\w]+", "_", n)
    n = re.sub(r"_+", "_", n).strip("_")
    return n


def _has_header(first_row: list[str], known_names: set[str]) -> tuple[bool, float]:
    conf = _header_confidence(first_row, known_names)
    if conf >= 0.4:
        return True, conf
    data_cells = sum(1 for c in first_row if _looks_like_data(c))
    if data_cells >= max(2, len(first_row) // 2):
        return False, conf
    # Ambiguous: if ≥1 cell matches a known name and none look numeric, treat as header.
    if conf > 0 and data_cells == 0:
        return True, conf
    return False, conf


# ---------- Positional + shape inference --------------------------------------

def _positional_layout(
    ncols: int,
    template: tuple[str, ...],
) -> list[str] | None:
    if ncols == len(template):
        return list(template)
    return None


def _infer_by_shape(df: pd.DataFrame, spec: dict[str, str]) -> list[str]:
    """Fallback when no positional template fits. Returns best-guess column names."""
    names: list[str] = []
    used: set[str] = set()
    # Pre-compute column statistics.
    for i, col in enumerate(df.columns):
        s = df[col].dropna().astype(str).head(80)
        pct_num = (s.map(lambda v: bool(_NUMERIC_RE.match(v.replace(",", "")))).sum() / len(s)) if len(s) else 0
        pct_ts = (s.map(lambda v: bool(_TIMESTAMP_HINT.search(v))).sum() / len(s)) if len(s) else 0
        uniq = s.nunique()
        sample = " ".join(s.head(10))
        guess = _shape_guess(pct_num, pct_ts, uniq, len(s), sample, used, spec)
        names.append(guess or f"col_{i}")
        used.add(guess or "")
    return names


_STEP_WORDS = re.compile(r"\b(scan|hash|join|aggregate|sort|dist|bcast|broadcast|merge|limit|project|filter)\b", re.I)


def _shape_guess(pct_num, pct_ts, uniq, n, sample, used, spec) -> str | None:
    # Timestamps → first unused datetime column.
    if pct_ts > 0.5:
        for k, v in spec.items():
            if k not in used and v.startswith("datetime"):
                return k
    # Step-name-shaped strings.
    if _STEP_WORDS.search(sample) and "step_name" in spec and "step_name" not in used:
        return "step_name"
    # Mostly numeric, few distinct → small-int positional columns.
    if pct_num > 0.8:
        if uniq <= max(5, n // 10):
            for k in ("step_id", "stream_id", "segment_id", "aborted"):
                if k in spec and k not in used:
                    return k
        # Otherwise big-int-like columns.
        for k in ("elapsed_time", "input_rows", "output_rows", "input_bytes",
                  "output_bytes", "query_id", "transaction_id"):
            if k in spec and k not in used:
                return k
    # Alphanumeric-looking short strings → table/schema names.
    if pct_num < 0.2:
        for k in ("table_name", "schema_name", "database_name", "join_type",
                  "alert_type", "source_type"):
            if k in spec and k not in used:
                return k
    return None


# ---------- Post-processing ---------------------------------------------------

def _apply_aliases(df: pd.DataFrame, aliases: dict[str, str]) -> pd.DataFrame:
    rename = {col: aliases[col] for col in df.columns if col in aliases}
    if rename:
        df = df.rename(columns=rename)
    return df


def _coerce_types(df: pd.DataFrame, spec: dict[str, str]) -> pd.DataFrame:
    for col, dtype in spec.items():
        if col not in df.columns:
            continue
        try:
            if dtype.startswith("Int"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype == "boolean":
                df[col] = _to_bool(df[col])
            elif dtype.startswith("datetime"):
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=False)
            elif dtype == "string":
                df[col] = df[col].astype("string")
        except Exception:
            pass
    return df


def _to_bool(s: pd.Series) -> pd.Series:
    truthy = {"t", "true", "1", "y", "yes"}
    falsy = {"f", "false", "0", "n", "no", ""}

    def cast(v):
        if pd.isna(v):
            return pd.NA
        t = str(v).strip().lower()
        if t in truthy:
            return True
        if t in falsy:
            return False
        return pd.NA

    return s.map(cast).astype("boolean")


# ---------- Top-level parse ---------------------------------------------------

def _parse(
    text: str,
    *,
    positional: tuple[str, ...],
    aliases: dict[str, str],
    spec: dict[str, str],
    required: set[str],
    kind: str,
) -> pd.DataFrame:
    cleaned = _preclean(text)
    if not cleaned.strip():
        raise ParseError("empty input")
    delim = _sniff_delimiter(cleaned)
    raw = _read_rows(cleaned, delim)
    if raw.empty:
        raise ParseError("no rows parsed")

    notes: list[str] = []

    # Decide if first row is a header.
    known_names = set(spec.keys()) | set(aliases.keys()) | set(positional)
    first_row = list(raw.iloc[0].fillna("").astype(str))
    has_header, confidence = _has_header(first_row, known_names)

    if has_header:
        header = [_normalize_header(c) for c in first_row]
        df = raw.iloc[1:].copy()
        df.columns = header
        notes.append(f"Header detected (confidence {confidence:.0%})")
    else:
        df = raw.copy()
        positional_names = _positional_layout(df.shape[1], positional)
        if positional_names is not None:
            df.columns = positional_names
            notes.append(
                f"Headerless paste — matched {kind} {len(positional_names)}-column layout"
            )
        else:
            inferred = _infer_by_shape(df, spec)
            df.columns = inferred
            recognized = sum(1 for c in inferred if c in spec)
            notes.append(
                f"Headerless paste — inferred {recognized}/{len(inferred)} columns by value shape"
            )

    # De-dup columns (DBeaver can emit duplicates) and drop all-empty.
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.dropna(axis=1, how="all")

    df = _apply_aliases(df, aliases)
    df = _coerce_types(df, spec)

    missing = required - set(df.columns)
    if missing:
        raise ParseError(
            f"{kind}: could not identify required column(s): {sorted(missing)}. "
            f"Hint: in DBeaver use Copy Advanced → Copy With Column Names, "
            f"or ensure the selected columns match sys_query_details / svv_table_info ordering."
        )

    df = df.reset_index(drop=True)
    df.attrs["parse_notes"] = notes
    return df


def parse_query_details(text: str) -> pd.DataFrame:
    df = _parse(
        text,
        positional=SYS_QUERY_DETAIL_POSITIONAL,
        aliases=QUERY_DETAIL_ALIASES,
        spec=QUERY_DETAIL_COLUMNS,
        required={"step_name"},
        kind="sys_query_details",
    )
    # Opportunistic sort for deterministic rendering.
    sort_cols = [c for c in ("stream_id", "segment_id", "step_id") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, na_position="last").reset_index(drop=True)
    return df


def parse_table_info(text: str) -> pd.DataFrame:
    return _parse(
        text,
        positional=SVV_TABLE_INFO_POSITIONAL,
        aliases=TABLE_INFO_ALIASES,
        spec=TABLE_INFO_COLUMNS,
        required={"table"},
        kind="svv_table_info",
    )


def join_keys(df: pd.DataFrame, keys: Iterable[str]) -> pd.Series:
    parts = []
    for k in keys:
        if k in df.columns:
            parts.append(df[k].astype("string").fillna(""))
    if not parts:
        return pd.Series([""] * len(df), index=df.index, dtype="string")
    out = parts[0]
    for p in parts[1:]:
        out = out.str.cat(p, sep=".")
    return out.str.lower()
