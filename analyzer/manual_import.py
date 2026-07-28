"""Manual file import into analyzer DuckDB tables."""
from __future__ import annotations

import io
import re
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .duckdb_store import DuckDBStore, EXPECTED_COLUMNS
from .schema import TABLE_INFO_ALIASES


@dataclass(frozen=True)
class ImportResult:
    table_name: str
    file_path: Path
    row_count: int
    column_count: int
    elapsed_ms: float
    snapshot_id: str
    missing_expected: tuple[str, ...]


def import_table_file(
    store: DuckDBStore,
    table_name: str,
    file_path: str | Path,
    *,
    label: str = "manual file import",
    source_database: str = "",
    append: bool = True,
) -> ImportResult:
    # Default append=True so multi-file / multi-database imports do not silently
    # keep only the last file (REPLACE). Callers that need replace must pass
    # append=False explicitly.
    if table_name not in EXPECTED_COLUMNS:
        raise ValueError(f"Unknown analyzer table: {table_name}")

    path = Path(file_path)
    started = time.perf_counter()
    frame = read_delimited_file(path)
    frame = normalize_import_frame(table_name, frame, source_database=source_database)

    with store.connect() as con:
        run = store.latest_snapshot(con)
        if run is None:
            run = store.new_snapshot(label)
            store.record_snapshot(con, run, source="manual-file")
        if append:
            store.append_table_from_frame(con, table_name, frame, run)
        else:
            store.replace_table_from_frame(con, table_name, frame, run)

    elapsed_ms = (time.perf_counter() - started) * 1000.0
    expected = set(EXPECTED_COLUMNS.get(table_name, ()))
    missing = tuple(sorted(expected - set(frame.columns)))
    return ImportResult(
        table_name=table_name,
        file_path=path,
        row_count=len(frame),
        column_count=len(frame.columns),
        elapsed_ms=elapsed_ms,
        snapshot_id=run.snapshot_id,
        missing_expected=missing,
    )


def read_delimited_file(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    text = read_text_file(path)
    delimiter = sniff_delimiter(text)
    frame = pd.read_csv(
        io.StringIO(text),
        sep=delimiter,
        engine="python",
        dtype=str,
        keep_default_na=False,
        na_values=["", "NULL", "null", "None", "NaN", "<null>"],
        skipinitialspace=True,
        quotechar='"',
        on_bad_lines="skip",
    )
    frame.columns = [normalize_header(str(col)) for col in frame.columns]
    frame = frame.loc[:, ~frame.columns.duplicated()]
    frame = frame.dropna(axis=1, how="all")
    return frame.reset_index(drop=True)


def read_text_file(path: Path) -> str:
    raw = path.read_bytes()
    for encoding in ("utf-8-sig", "utf-16", "cp1252"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def sniff_delimiter(text: str) -> str:
    candidates = ["\t", "|", ";", ","]
    lines = [line for line in text.splitlines()[:50] if line.strip()]
    best = ","
    best_score = -1
    for delimiter in candidates:
        counts = [line.count(delimiter) for line in lines]
        if not counts or max(counts) == 0:
            continue
        common_count = max(set(counts), key=counts.count)
        if common_count <= 0:
            continue
        score = common_count * counts.count(common_count)
        if score > best_score:
            best = delimiter
            best_score = score
    return best


def normalize_import_frame(table_name: str, frame: pd.DataFrame, *, source_database: str = "") -> pd.DataFrame:
    df = frame.copy()
    if table_name == "svv_table_info_all":
        rename = {col: TABLE_INFO_ALIASES[col] for col in df.columns if col in TABLE_INFO_ALIASES}
        if rename:
            df = df.rename(columns=rename)
        if source_database:
            df["source_db"] = source_database
            if "database" not in df.columns:
                df["database"] = source_database
    elif table_name == "view_definitions":
        df = normalize_view_definition_frame(df, source_database=source_database)
    elif table_name == "procedure_definitions":
        df = normalize_procedure_definition_frame(df, source_database=source_database)
    return df


def normalize_view_definition_frame(frame: pd.DataFrame, *, source_database: str = "") -> pd.DataFrame:
    df = frame.copy()
    rename: dict[str, str] = {}
    for col in df.columns:
        if col == "schemaname":
            rename[col] = "schema"
        elif col == "viewname":
            rename[col] = "view_name"
        elif col in {"definition", "view_definition"}:
            rename[col] = "source_definition"
    if rename:
        df = df.rename(columns=rename)

    if "database" not in df.columns:
        df["database"] = source_database

    part_cols = sorted(
        [col for col in df.columns if re.match(r"definition_part_\d+$", str(col))],
        key=lambda col: int(re.search(r"(\d+)$", str(col)).group(1)),
    )
    if part_cols:
        df["source_definition"] = df[part_cols].apply(
            lambda row: "".join("" if pd.isna(value) else str(value) for value in row),
            axis=1,
        )
    elif "source_definition" not in df.columns:
        df["source_definition"] = ""

    for col in ("schema", "view_name"):
        if col not in df.columns:
            df[col] = ""
    return df


def normalize_procedure_definition_frame(frame: pd.DataFrame, *, source_database: str = "") -> pd.DataFrame:
    df = frame.copy()
    rename: dict[str, str] = {}
    for col in df.columns:
        normalized = str(col).strip().lower()
        if normalized in {"nspname", "schemaname", "schema_name"}:
            rename[col] = "schema"
        elif normalized in {"proname", "procname", "stored_procedure_name"}:
            rename[col] = "procedure_name"
        elif normalized in {"prooid", "oid"}:
            rename[col] = "procedure_oid"
        elif normalized in {"prosrc", "procedure_source", "definition", "procedure_definition"}:
            rename[col] = "source_definition"
    if rename:
        df = df.rename(columns=rename)

    if "database" not in df.columns:
        df["database"] = source_database

    part_cols = sorted(
        [col for col in df.columns if re.match(r"definition_part_\d+$", str(col).strip().lower())],
        key=lambda col: int(re.search(r"(\d+)$", str(col)).group(1)),
    )
    if part_cols:
        df["source_definition"] = df[part_cols].apply(
            lambda row: "".join("" if pd.isna(value) else str(value) for value in row),
            axis=1,
        )
    elif "source_definition" not in df.columns:
        df["source_definition"] = ""
    df["source_definition"] = df["source_definition"].map(procedure_body_begin_to_end)
    df["source_length"] = df["source_definition"].map(lambda value: len(str(value or "")))

    for col in ("schema", "procedure_name"):
        if col not in df.columns:
            df[col] = ""
    if "procedure_key" not in df.columns:
        df["procedure_key"] = df.apply(
            lambda row: ".".join(
                str(row.get(key) or "").strip().lower()
                for key in ("database", "schema", "procedure_name")
            ),
            axis=1,
        )
    return df


def procedure_body_begin_to_end(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value)
    begin = re.search(r"\bbegin\b", text, flags=re.IGNORECASE)
    if not begin:
        return text.strip()
    end_matches = list(re.finditer(r"\bend\b\s*;?", text, flags=re.IGNORECASE))
    end_after_begin = [match for match in end_matches if match.start() >= begin.start()]
    if not end_after_begin:
        return text[begin.start():].strip()
    end = end_after_begin[-1]
    return text[begin.start():end.end()].strip()


def normalize_header(name: str) -> str:
    normalized = name.strip().strip('"').strip("'").lower()
    normalized = re.sub(r"[^\w]+", "_", normalized)
    return re.sub(r"_+", "_", normalized).strip("_")
