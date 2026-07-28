"""External SQL-feature parse cache (file only — not the main app warehouse).

Checkpoints live in a sibling DuckDB file next to the capture warehouse, e.g.::

    %USERPROFILE%\\RQP\\data\\redshift.duckdb
    %USERPROFILE%\\RQP\\data\\redshift.sql_features.duckdb

This module is the single owner of that file: path resolution, read, write,
keying, and dtype cleanup. The triage loader calls it; nothing else stores
parse features.
"""
from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path

import duckdb
import pandas as pd

from .duckdb_store import DuckDBStore

# Table name inside the sidecar file only.
CACHE_TABLE = "sql_features"
# Legacy table name when older builds wrote into the main warehouse.
LEGACY_WAREHOUSE_TABLE = "analysis_cache_sql_features"
# Bumped when feature column typing changes (orphans old keys on purpose).
CACHE_VERSION = "v2-typed-feature-columns"
ENV_CACHE_PATH = "INFRAREDSHIFT_SQL_FEATURE_CACHE"


def sql_feature_key(sql_text: object) -> str:
    """Stable hash for one SQL text at the current cache version."""
    payload = f"{CACHE_VERSION}|{str(sql_text or '')}"
    return hashlib.sha256(payload.encode("utf-8", errors="replace")).hexdigest()


def cache_path(store: DuckDBStore | Path | str) -> Path:
    """Sidecar path for the parse cache.

    Default: ``{warehouse_stem}.sql_features.duckdb`` beside the warehouse.
    Override with env ``INFRAREDSHIFT_SQL_FEATURE_CACHE``.
    """
    override = str(os.environ.get(ENV_CACHE_PATH) or "").strip()
    if override:
        return Path(os.path.expandvars(os.path.expanduser(override))).resolve(strict=False)
    root = Path(store.path if isinstance(store, DuckDBStore) else store).resolve(strict=False)
    return root.with_name(f"{root.stem}.sql_features.duckdb")


def unify_feature_dtypes(frame: pd.DataFrame) -> pd.DataFrame:
    """One dtype per feature column so cached + fresh rows can merge safely."""
    if frame is None or frame.empty:
        return frame if frame is not None else pd.DataFrame()
    result = frame.copy()
    for col in result.columns:
        if col == "sql_feature_key":
            continue
        series = result[col]
        if series.dtype != object:
            continue
        non_blank = series.notna() & (series.astype(str).str.strip() != "")
        numeric = pd.to_numeric(series, errors="coerce")
        if bool(numeric.notna()[non_blank].all()):
            result[col] = numeric
        else:
            result[col] = series.mask(non_blank, series.astype(str))
    return result


def _read_table(path: Path, table: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        con = duckdb.connect(str(path), read_only=True)
    except Exception:
        return pd.DataFrame()
    try:
        try:
            frame = con.execute(f'SELECT * FROM "{table}"').fetchdf()
        except Exception:
            return pd.DataFrame()
    finally:
        con.close()
    if frame is None or frame.empty or "sql_feature_key" not in frame.columns:
        return pd.DataFrame()
    return frame.drop_duplicates("sql_feature_key", keep="last")


def read_cache(
    store: DuckDBStore,
    *,
    warehouse_con: object | None = None,
) -> pd.DataFrame:
    """Load features from the external sidecar, then optional legacy warehouse rows.

    *warehouse_con*: already-open warehouse connection to read legacy in-table
    cache. When omitted, legacy is read **read_only** only if the sidecar is
    empty (one-shot migration). Never opens a second RW handle on the warehouse
    while another connection is live — that freezes multi-threaded UI loads.
    """
    frames: list[pd.DataFrame] = []
    external = _read_table(cache_path(store), CACHE_TABLE)
    if not external.empty:
        frames.append(external)
    legacy = pd.DataFrame()
    if warehouse_con is not None:
        try:
            legacy = warehouse_con.execute(
                f'SELECT * FROM "{LEGACY_WAREHOUSE_TABLE}"'
            ).fetchdf()
        except Exception:
            legacy = pd.DataFrame()
    elif external.empty and Path(store.path).exists():
        # Migrate once from old in-warehouse table without RW / install_schema.
        try:
            con = duckdb.connect(str(store.path), read_only=True)
            try:
                legacy = con.execute(
                    f'SELECT * FROM "{LEGACY_WAREHOUSE_TABLE}"'
                ).fetchdf()
            except Exception:
                legacy = pd.DataFrame()
            finally:
                con.close()
        except Exception:
            legacy = pd.DataFrame()
    if legacy is not None and not legacy.empty and "sql_feature_key" in legacy.columns:
        frames.append(legacy.drop_duplicates("sql_feature_key", keep="last"))
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True, sort=False)
    return combined.drop_duplicates("sql_feature_key", keep="last")


def write_cache(store: DuckDBStore, features: pd.DataFrame) -> Path:
    """Append-merge *features* into the external sidecar; return its path.

    Each successful write is a durable checkpoint (safe after app kill).
    """
    if features is None or features.empty or "sql_feature_key" not in features.columns:
        return cache_path(store)
    path = cache_path(store)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _read_table(path, CACHE_TABLE)
    combined = pd.concat([existing, features], ignore_index=True, sort=False)
    combined = combined.drop_duplicates("sql_feature_key", keep="last")
    combined = unify_feature_dtypes(combined)
    con = duckdb.connect(str(path))
    registered = f"cache_{uuid.uuid4().hex}"
    try:
        con.register(registered, combined)
        con.execute(
            f'CREATE OR REPLACE TABLE "{CACHE_TABLE}" AS SELECT * FROM {registered}'
        )
    finally:
        try:
            con.unregister(registered)
        except Exception:
            pass
        con.close()
    return path
