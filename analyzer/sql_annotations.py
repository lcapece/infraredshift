"""Local DuckDB persistence for user-authored SQL exploration annotations.

Annotation payloads (the note, captured SQL, and screenshot) are encrypted
with Windows DPAPI for the current user before they touch disk — a shared or
copied DuckDB file must not leak query text or screen contents. Metadata
columns stay plain so annotations can still be listed and filtered.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timezone
import getpass
import os
from pathlib import Path
import uuid

import duckdb

from .duckdb_store import default_duckdb_path

_ENC_TEXT_PREFIX = "enc:v1:"
_ENC_BLOB_PREFIX = b"DPAPIv1\x00"
_ANNOTATION_ENTROPY = b"Infraredshift-annotations-v1"


def _protect_text(value: str) -> str:
    if not value or os.name != "nt":
        return value
    from .secrets_store import _dpapi_protect

    payload = _dpapi_protect(value.encode("utf-8"), _ANNOTATION_ENTROPY)
    return _ENC_TEXT_PREFIX + base64.b64encode(payload).decode("ascii")


def _unprotect_text(value: object) -> str:
    text = str(value or "")
    if not text.startswith(_ENC_TEXT_PREFIX):
        return text
    from .secrets_store import _dpapi_unprotect

    payload = base64.b64decode(text[len(_ENC_TEXT_PREFIX):].encode("ascii"))
    return _dpapi_unprotect(payload, _ANNOTATION_ENTROPY).decode("utf-8")


def _protect_blob(data: bytes | None) -> bytes | None:
    if not data or os.name != "nt":
        return data
    from .secrets_store import _dpapi_protect

    return _ENC_BLOB_PREFIX + _dpapi_protect(bytes(data), _ANNOTATION_ENTROPY)


def _unprotect_blob(data: object) -> bytes | None:
    if data is None:
        return None
    raw = bytes(data)
    if not raw.startswith(_ENC_BLOB_PREFIX):
        return raw
    from .secrets_store import _dpapi_unprotect

    return _dpapi_unprotect(raw[len(_ENC_BLOB_PREFIX):], _ANNOTATION_ENTROPY)


@dataclass(frozen=True)
class SqlAnnotation:
    note: str
    selected_sql: str = ""
    surrounding_sql: str = ""
    context_title: str = ""
    source_widget: str = ""
    query_id: str = ""
    repeat_group_id: str = ""
    namespace_id: str = ""
    screenshot_png: bytes | None = None
    screenshot_width: int = 0
    screenshot_height: int = 0


def ensure_annotation_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS user_sql_annotations (
            annotation_id VARCHAR PRIMARY KEY,
            created_at TIMESTAMP,
            created_by VARCHAR,
            note VARCHAR,
            selected_sql VARCHAR,
            surrounding_sql VARCHAR,
            context_title VARCHAR,
            source_widget VARCHAR,
            query_id VARCHAR,
            repeat_group_id VARCHAR,
            namespace_id VARCHAR,
            screenshot_png BLOB,
            screenshot_width INTEGER,
            screenshot_height INTEGER,
            sync_status VARCHAR DEFAULT 'local'
        )
        """
    )


_ACTIVE_DB_PATH: str | None = None


def set_active_annotation_db(path: str | Path | None) -> None:
    """Route annotations to the warehouse the operator is currently viewing.

    The dashboard calls this whenever its DuckDB selection changes; without
    it, annotations from context menus would land in the default file even
    while a different per-cluster warehouse is open.
    """
    global _ACTIVE_DB_PATH
    _ACTIVE_DB_PATH = str(path).strip() if path and str(path).strip() else None


def save_annotation(annotation: SqlAnnotation, db_path: str | Path | None = None) -> str:
    """Persist an annotation in the active local DuckDB and return its stable id."""
    target = db_path or _ACTIVE_DB_PATH
    path = Path(target) if target else default_duckdb_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    annotation_id = str(uuid.uuid4())
    con = duckdb.connect(str(path))
    try:
        ensure_annotation_table(con)
        con.execute(
            """
            INSERT INTO user_sql_annotations (
                annotation_id, created_at, created_by, note, selected_sql,
                surrounding_sql, context_title, source_widget, query_id,
                repeat_group_id, namespace_id, screenshot_png,
                screenshot_width, screenshot_height, sync_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'local')
            """,
            [
                annotation_id,
                datetime.now(timezone.utc).replace(tzinfo=None),
                getpass.getuser(),
                _protect_text(annotation.note.strip()),
                _protect_text(annotation.selected_sql),
                _protect_text(annotation.surrounding_sql),
                annotation.context_title,
                annotation.source_widget,
                annotation.query_id,
                annotation.repeat_group_id,
                annotation.namespace_id,
                _protect_blob(annotation.screenshot_png),
                int(annotation.screenshot_width or 0),
                int(annotation.screenshot_height or 0),
            ],
        )
    finally:
        con.close()
    return annotation_id


def read_annotations(db_path: str | Path | None = None) -> list[dict]:
    """Return saved annotations with their payloads decrypted for this user."""
    target = db_path or _ACTIVE_DB_PATH
    path = Path(target) if target else default_duckdb_path()
    if not path.is_file():
        return []
    con = duckdb.connect(str(path), read_only=True)
    try:
        try:
            cursor = con.execute(
                "SELECT * FROM user_sql_annotations ORDER BY created_at DESC"
            )
        except Exception:
            return []
        columns = [description[0] for description in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        con.close()
    for row in rows:
        for field in ("note", "selected_sql", "surrounding_sql"):
            row[field] = _unprotect_text(row.get(field))
        row["screenshot_png"] = _unprotect_blob(row.get("screenshot_png"))
    return rows
