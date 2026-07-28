"""Optional cached detection of views that may resolve to Spectrum tables.

The scan is deliberately outside the normal load and triage paths. It opens
the warehouse read-only and writes only to a sibling sidecar cache, so a slow
or failed scan cannot change staged/live analyzer tables.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import re
from typing import Callable
import uuid

import duckdb
import pandas as pd

from .query_similarity import analyze_sql


CACHE_VERSION = "v1-spectrum-view-definition-and-catalog"
CACHE_TABLE = "spectrum_view_candidates"


@dataclass(frozen=True)
class SpectrumViewScanResult:
    snapshot_id: str
    candidates: pd.DataFrame
    view_count: int
    external_table_count: int
    cache_hits: int
    analyzed_count: int
    cache_file: Path


def spectrum_view_cache_path(warehouse_path: str | Path) -> Path:
    warehouse = Path(warehouse_path).expanduser().resolve(strict=False)
    return warehouse.with_name(f"{warehouse.stem}.spectrum_views.duckdb")


def _clean_identifier(value: object) -> str:
    text = str(value or "").strip().lower()
    return ".".join(
        part.strip().strip('"`[]')
        for part in text.split(".")
        if part.strip().strip('"`[]')
    )


def _first(row: pd.Series, *names: str) -> str:
    for name in names:
        if name not in row.index:
            continue
        value = row.get(name)
        if value is None or (isinstance(value, float) and pd.isna(value)):
            continue
        text = str(value).strip()
        if text and text.lower() not in {"none", "nan", "<na>"}:
            return text
    return ""


def _view_definition(row: pd.Series) -> str:
    direct = _first(
        row,
        "source_definition",
        "view_definition",
        "definition",
    )
    if direct:
        return direct
    return "".join(
        _first(row, f"definition_part_{index:02d}")
        for index in range(1, 13)
    )


def _view_records(
    view_definitions: pd.DataFrame,
    snapshot_id: str,
    catalog_hash: str,
) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    if view_definitions is None or view_definitions.empty:
        return records
    for _, row in view_definitions.iterrows():
        view_name = _first(row, "view_name", "table_name")
        definition = _view_definition(row)
        if not view_name or not definition:
            continue
        database = _first(row, "source_db", "database", "database_name")
        schema = _first(row, "schema_name", "schema", "schemaname")
        view_key = _clean_identifier(".".join(
            part for part in (database, schema, view_name) if part
        ))
        digest = hashlib.sha256(
            (
                f"{CACHE_VERSION}|{snapshot_id}|{catalog_hash}|"
                f"{view_key}|{definition}"
            ).encode("utf-8", errors="replace")
        ).hexdigest()
        records.append({
            "analysis_key": digest,
            "snapshot_id": snapshot_id,
            "view_key": view_key,
            "database_name": database,
            "schema_name": schema,
            "view_name": view_name,
            "view_definition": definition,
        })
    return records


def _read_external_metadata(warehouse_path: Path) -> pd.DataFrame:
    if not warehouse_path.is_file():
        return pd.DataFrame()
    try:
        con = duckdb.connect(str(warehouse_path), read_only=True)
    except Exception:
        return pd.DataFrame()
    try:
        exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE LOWER(table_name) = 'external_table_metadata'"
        ).fetchone()[0]
        if not exists:
            return pd.DataFrame()
        return con.execute("SELECT * FROM external_table_metadata").fetchdf()
    except Exception:
        return pd.DataFrame()
    finally:
        con.close()


def _external_lookup(frame: pd.DataFrame) -> tuple[dict[str, set[str]], str]:
    lookup: dict[str, set[str]] = {}
    canonical: set[str] = set()
    if frame is None or frame.empty:
        return lookup, hashlib.sha256(b"empty").hexdigest()
    for _, row in frame.iterrows():
        database = _first(
            row,
            "source_db",
            "redshift_database_name",
            "database_name",
            "database",
        )
        schema = _first(row, "schema_name", "schemaname", "schema")
        table = _first(row, "table_name", "tablename")
        full = _clean_identifier(".".join(
            part for part in (database, schema, table) if part
        ))
        if not table or not full:
            continue
        canonical.add(full)
        parts = full.split(".")
        for start in range(len(parts)):
            lookup.setdefault(".".join(parts[start:]), set()).add(full)
    fingerprint = hashlib.sha256(
        "\n".join(sorted(canonical)).encode("utf-8")
    ).hexdigest()
    return lookup, fingerprint


def _cached_rows(path: Path, keys: set[str]) -> pd.DataFrame:
    if not path.is_file() or not keys:
        return pd.DataFrame()
    try:
        con = duckdb.connect(str(path), read_only=True)
    except Exception:
        return pd.DataFrame()
    try:
        frame = con.execute(f'SELECT * FROM "{CACHE_TABLE}"').fetchdf()
    except Exception:
        frame = pd.DataFrame()
    finally:
        con.close()
    if frame.empty or "analysis_key" not in frame.columns:
        return pd.DataFrame()
    return frame[frame["analysis_key"].astype(str).isin(keys)].copy()


def _write_cache(path: Path, rows: pd.DataFrame) -> None:
    if rows is None or rows.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _cached_rows(path, set())
    if path.is_file():
        try:
            con = duckdb.connect(str(path), read_only=True)
            try:
                existing = con.execute(f'SELECT * FROM "{CACHE_TABLE}"').fetchdf()
            finally:
                con.close()
        except Exception:
            existing = pd.DataFrame()
    combined = pd.concat([existing, rows], ignore_index=True, sort=False)
    combined = combined.drop_duplicates("analysis_key", keep="last")
    con = duckdb.connect(str(path))
    registered = f"spectrum_views_{uuid.uuid4().hex}"
    try:
        con.register(registered, combined)
        con.execute(
            f'CREATE OR REPLACE TABLE "{CACHE_TABLE}" AS '
            f"SELECT * FROM {registered}"
        )
    finally:
        try:
            con.unregister(registered)
        except Exception:
            pass
        con.close()


def identify_possible_spectrum_views(
    warehouse_path: str | Path,
    snapshot_id: str | None,
    view_definitions: pd.DataFrame,
    *,
    progress: Callable[[int, int], None] | None = None,
    cancelled: Callable[[], bool] | None = None,
) -> SpectrumViewScanResult:
    """Analyze view bodies against Producer SVV_EXTERNAL_COLUMNS metadata."""
    warehouse = Path(warehouse_path).expanduser().resolve(strict=False)
    snapshot = str(snapshot_id or "unversioned")
    cache_file = spectrum_view_cache_path(warehouse)
    external_frame = _read_external_metadata(warehouse)
    external_lookup, catalog_hash = _external_lookup(external_frame)
    records = _view_records(view_definitions, snapshot, catalog_hash)
    keys = {record["analysis_key"] for record in records}
    cached = _cached_rows(cache_file, keys)
    cached_keys = (
        set(cached["analysis_key"].astype(str))
        if not cached.empty and "analysis_key" in cached.columns
        else set()
    )
    fresh_rows: list[dict[str, object]] = []
    total = len(records)
    for completed, record in enumerate(records, start=1):
        if record["analysis_key"] in cached_keys:
            if progress:
                progress(completed, total)
            continue
        if cancelled and cancelled():
            break
        intelligence = analyze_sql(record["view_definition"])
        matched: set[str] = set()
        for table in intelligence.tables:
            key = _clean_identifier(table)
            parts = key.split(".")
            for start in range(len(parts)):
                matched.update(external_lookup.get(".".join(parts[start:]), set()))
        fresh_rows.append({
            **{key: value for key, value in record.items() if key != "view_definition"},
            "is_possible_spectrum": bool(matched),
            "matched_external_objects": ", ".join(sorted(matched)),
            "referenced_tables": ", ".join(sorted(intelligence.tables)),
            "parse_ok": bool(intelligence.parse_ok),
            "analyzed_at": datetime.now(timezone.utc).isoformat(),
        })
        if progress:
            progress(completed, total)
    fresh = pd.DataFrame(fresh_rows)
    _write_cache(cache_file, fresh)
    all_rows = pd.concat([cached, fresh], ignore_index=True, sort=False)
    if not all_rows.empty:
        all_rows = all_rows.drop_duplicates("analysis_key", keep="last")
        candidates = all_rows[
            all_rows["is_possible_spectrum"].fillna(False).astype(bool)
        ].copy()
    else:
        candidates = pd.DataFrame()
    return SpectrumViewScanResult(
        snapshot_id=snapshot,
        candidates=candidates,
        view_count=total,
        external_table_count=len({
            value
            for values in external_lookup.values()
            for value in values
        }),
        cache_hits=len(cached_keys),
        analyzed_count=len(fresh_rows),
        cache_file=cache_file,
    )
