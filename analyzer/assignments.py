"""Query-group ownership: assign a roster engineer to a repeat query group.

Assignments are keyed on the DURABLE ``repeat_group_key`` (a hash of the query
shape) so an engineer stays attached to the same query pattern even after a
reload reshuffles the RQ display ids. The table is persistent (in
duckdb_store.PRESERVED_TABLES) and offline: everything reads/writes the local
DuckDB, no network.
"""
from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pandas as pd

_ASSIGN_TABLE = "query_group_assignments"
_ROSTER_TABLE = "user_roster"

_ASSIGN_DDL = (
    f"CREATE TABLE IF NOT EXISTS {_ASSIGN_TABLE} ("
    "repeat_group_key VARCHAR PRIMARY KEY, user_name VARCHAR, "
    "engineer_display VARCHAR, assigned_at TIMESTAMP, "
    "associated_user_name VARCHAR, associated_user_display VARCHAR, "
    "associated_at TIMESTAMP)"
)

# Columns added after the table shipped. A warehouse created by an older build
# has the table but not these, and the assignment table is PRESERVED across
# loads - so it is never recreated and must be migrated in place.
_ASSIGN_ADDED_COLUMNS = (
    ("associated_user_name", "VARCHAR"),
    ("associated_user_display", "VARCHAR"),
    ("associated_at", "TIMESTAMP"),
)


def _ensure_assignment_table(con) -> None:
    con.execute(_ASSIGN_DDL)
    existing = {
        str(row[0]).lower()
        for row in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE lower(table_name) = '{_ASSIGN_TABLE}'"
        ).fetchall()
    }
    for column, dtype in _ASSIGN_ADDED_COLUMNS:
        if column not in existing:
            con.execute(f"ALTER TABLE {_ASSIGN_TABLE} ADD COLUMN {column} {dtype}")


def engineer_display(row: dict) -> str:
    """'Lastname, Firstname M' for the dropdown, sortable by last then first."""
    last = str(row.get("last_name") or "").strip().title()
    first = str(row.get("first_name") or "").strip().title()
    mid_init = str(row.get("middle_initial") or "").strip().upper()
    if not last and not first:
        return str(row.get("user_name") or "").strip()
    name = f"{last}, {first}" if last else first
    if mid_init:
        name += f" {mid_init}"
    return name.strip().strip(",").strip()


def load_roster_choices(db_path) -> list[dict]:
    """Return roster engineers as dicts, sorted by last name then first name.

    Each dict has: user_name, display (Lastname, Firstname M), last_name,
    first_name. Drives the searchable assignment dropdown.
    """
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception:
        return []
    try:
        rows = con.execute(
            f"SELECT user_name, first_name, middle_initial, last_name "
            f"FROM {_ROSTER_TABLE} ORDER BY LOWER(last_name), LOWER(first_name)"
        ).fetchall()
        cols = [d[0] for d in con.execute(
            f"SELECT user_name, first_name, middle_initial, last_name FROM {_ROSTER_TABLE} LIMIT 0"
        ).description]
    except Exception:
        return []
    finally:
        con.close()
    choices = []
    for r in rows:
        record = dict(zip(cols, r))
        choices.append(
            {
                "user_name": str(record.get("user_name") or ""),
                "display": engineer_display(record),
                "last_name": str(record.get("last_name") or ""),
                "first_name": str(record.get("first_name") or ""),
            }
        )
    return choices


def load_assignments(db_path) -> dict[str, dict]:
    """Return {repeat_group_key: {...}} covering engineer AND associated user.

    Reads the associated-user columns defensively: a warehouse written by an
    older build has the table without them, and this is a read-only path that
    cannot migrate.
    """
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception:
        return {}
    try:
        available = {
            str(row[0]).lower()
            for row in con.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE lower(table_name) = '{_ASSIGN_TABLE}'"
            ).fetchall()
        }
        has_assoc = "associated_user_name" in available
        columns = "repeat_group_key, user_name, engineer_display"
        if has_assoc:
            columns += ", associated_user_name, associated_user_display"
        rows = con.execute(f"SELECT {columns} FROM {_ASSIGN_TABLE}").fetchall()
    except Exception:
        return {}
    finally:
        con.close()
    out: dict[str, dict] = {}
    for row in rows:
        key = str(row[0] or "")
        if not key:
            continue
        out[key] = {
            "user_name": str(row[1] or ""),
            "engineer_display": str(row[2] or ""),
            "associated_user_name": str(row[3] or "") if has_assoc else "",
            "associated_user_display": str(row[4] or "") if has_assoc else "",
        }
    return out


def set_association(
    db_path,
    repeat_group_key: str,
    user_name: str,
    user_display_text: str,
    *,
    now: datetime | None = None,
) -> None:
    """Associate (or clear) the business user a query group belongs to.

    Separate from set_assignment: the engineer who owns the FIX and the user
    whose workload the query IS are different people. Upserts without
    disturbing an existing engineer assignment on the same key.
    """
    key = str(repeat_group_key or "").strip()
    if not key:
        return
    stamp = (now or datetime.now(timezone.utc)).replace(tzinfo=None)
    con = duckdb.connect(str(db_path))
    try:
        _ensure_assignment_table(con)
        exists = con.execute(
            f"SELECT COUNT(*) FROM {_ASSIGN_TABLE} WHERE repeat_group_key = ?", [key]
        ).fetchone()[0]
        name = str(user_name or "").strip()
        display = str(user_display_text or "").strip()
        if exists:
            con.execute(
                f"UPDATE {_ASSIGN_TABLE} SET associated_user_name = ?, "
                "associated_user_display = ?, associated_at = ? "
                "WHERE repeat_group_key = ?",
                [name, display, stamp if name else None, key],
            )
            # Drop the row once neither an engineer nor an association remains,
            # so cleared entries do not accumulate.
            con.execute(
                f"DELETE FROM {_ASSIGN_TABLE} WHERE repeat_group_key = ? "
                "AND COALESCE(user_name,'') = '' AND COALESCE(associated_user_name,'') = ''",
                [key],
            )
        elif name:
            con.execute(
                f"INSERT INTO {_ASSIGN_TABLE} (repeat_group_key, user_name, "
                "engineer_display, assigned_at, associated_user_name, "
                "associated_user_display, associated_at) VALUES (?, '', '', NULL, ?, ?, ?)",
                [key, name, display, stamp],
            )
    finally:
        con.close()


def set_assignment(
    db_path,
    repeat_group_key: str,
    user_name: str,
    engineer_display_text: str,
    *,
    now: datetime | None = None,
) -> None:
    """Assign (or clear, when user_name is empty) an engineer to a group key."""
    key = str(repeat_group_key or "").strip()
    if not key:
        return
    stamp = (now or datetime.now(timezone.utc)).replace(tzinfo=None)
    con = duckdb.connect(str(db_path))
    try:
        _ensure_assignment_table(con)
        name = str(user_name or "").strip()
        exists = con.execute(
            f"SELECT COUNT(*) FROM {_ASSIGN_TABLE} WHERE repeat_group_key = ?", [key]
        ).fetchone()[0]
        # UPDATE rather than INSERT OR REPLACE: replacing the row would silently
        # discard any associated user recorded against the same group.
        if exists:
            con.execute(
                f"UPDATE {_ASSIGN_TABLE} SET user_name = ?, engineer_display = ?, "
                "assigned_at = ? WHERE repeat_group_key = ?",
                [name, str(engineer_display_text or "").strip() if name else "",
                 stamp if name else None, key],
            )
            con.execute(
                f"DELETE FROM {_ASSIGN_TABLE} WHERE repeat_group_key = ? "
                "AND COALESCE(user_name,'') = '' AND COALESCE(associated_user_name,'') = ''",
                [key],
            )
        elif name:
            con.execute(
                f"INSERT INTO {_ASSIGN_TABLE} (repeat_group_key, user_name, "
                "engineer_display, assigned_at) VALUES (?, ?, ?, ?)",
                [key, name, str(engineer_display_text or "").strip(), stamp],
            )
    finally:
        con.close()


_ATTACHED_COLUMNS = (
    ("assigned_engineer", "engineer_display"),
    ("assigned_user_name", "user_name"),
    ("associated_user", "associated_user_display"),
    ("associated_user_name", "associated_user_name"),
)


def attach_assignments(groups: pd.DataFrame, assignments: dict[str, dict]) -> pd.DataFrame:
    """Add engineer and associated-user columns to the groups frame."""
    if groups is None or groups.empty:
        return groups
    result = groups.copy()
    key_col = "repeat_group_key" if "repeat_group_key" in result.columns else None
    if key_col is None or not assignments:
        for column, _source in _ATTACHED_COLUMNS:
            result[column] = ""
        return result
    for column, source in _ATTACHED_COLUMNS:
        result[column] = [
            assignments.get(str(key), {}).get(source, "") for key in result[key_col]
        ]
    return result
