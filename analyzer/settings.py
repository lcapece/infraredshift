"""Persistent local analyzer settings."""
from __future__ import annotations

import json
import math
import os
import re
import hashlib
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from .runtime_paths import resolve_runtime_paths


DEFAULT_DATABASE_MIN_QUERY_COUNT = 250
DEFAULT_REPEAT_SIMILARITY_THRESHOLD = 0.78
DEFAULT_REPEAT_PREFILTER_THRESHOLD = 0.30
DEFAULT_REPEAT_FUZZY_MERGE_THRESHOLD = 0.95
DEFAULT_REPEAT_MIN_GROUP_SIZE = 2
DEFAULT_CAPTURE_QUERY_LIMIT = 0
DEFAULT_CAPTURE_RANK_BY = "elapsed_time"
DEFAULT_ROOT_MIN_EXECUTION_SECONDS = 30
CAPTURE_SELECTION_VERSION = 3
LEGACY_DEFAULT_CAPTURE_QUERY_LIMIT = 1000

DEFAULT_DATABASE_DISCOVERY_SQL = """\
SELECT
  TRIM(database_name)::VARCHAR AS database_name,
  LOWER(TRIM(database_type))::VARCHAR AS database_type,
  0::BIGINT AS query_count
FROM svv_redshift_databases
WHERE LOWER(TRIM(database_type)) = 'local'
ORDER BY database_name
"""

UNSAFE_PG_DATABASE_DISCOVERY_SQL = """\
SELECT
  datname::VARCHAR AS database_name,
  0::BIGINT AS query_count
FROM pg_database
WHERE datname NOT IN ('template0', 'template1', 'padb_harvest')
ORDER BY database_name
"""

LEGACY_DATABASE_DISCOVERY_SQL = """\
SELECT
  database_name::VARCHAR AS database_name,
  COUNT(*) AS query_count
FROM sys_query_history
WHERE NULLIF(TRIM(database_name::VARCHAR), '') IS NOT NULL
GROUP BY 1
HAVING COUNT(*) > {min_query_count}
ORDER BY query_count DESC, database_name
"""


@dataclass
class AnalyzerSettings:
    database_discovery_sql: str = DEFAULT_DATABASE_DISCOVERY_SQL
    database_min_query_count: int = DEFAULT_DATABASE_MIN_QUERY_COUNT
    repeat_similarity_threshold: float = DEFAULT_REPEAT_SIMILARITY_THRESHOLD
    repeat_prefilter_threshold: float = DEFAULT_REPEAT_PREFILTER_THRESHOLD
    repeat_fuzzy_merge_threshold: float = DEFAULT_REPEAT_FUZZY_MERGE_THRESHOLD
    repeat_min_group_size: int = DEFAULT_REPEAT_MIN_GROUP_SIZE
    # Default ON: including the user is the safer starting point - it keeps
    # attribution exact and never merges two teams' work into one pattern.
    # Turning it off merges harder and is the deliberate second step.
    repeat_scope_by_user: bool = True
    discovered_databases: list[str] = field(default_factory=list)
    discovered_at: str = ""
    discovery_source: str = ""
    table_sql_overrides: dict[str, str] = field(default_factory=dict)
    table_review_visible_cols: list[str] = field(default_factory=list)
    table_review_show_size_rows: bool = False
    table_review_show_dist_sort: bool = False
    table_review_hide_without_intersection: bool = True
    capture_query_limit: int = DEFAULT_CAPTURE_QUERY_LIMIT
    capture_rank_by: str = DEFAULT_CAPTURE_RANK_BY
    capture_selection_version: int = CAPTURE_SELECTION_VERSION
    capture_include_tables: list[str] = field(default_factory=list)
    # Empty means aggregate every namespace present in the loaded snapshot.
    analysis_namespace_filter: list[str] = field(default_factory=list)
    root_min_execution_seconds: int = DEFAULT_ROOT_MIN_EXECUTION_SECONDS
    root_floor_basis: str = "execution_time"
    last_source_cluster_fingerprint: str = ""
    last_source_cluster_summary: str = ""
    ui_theme: str = "light"
    # Topology page zoom preference (0.4–2.0; 1.0 = 100%).
    topology_zoom: float = 1.0


def settings_path() -> Path:
    return resolve_runtime_paths().settings_file


def load_settings(path: str | os.PathLike | None = None) -> AnalyzerSettings:
    target = Path(path) if path else settings_path()
    if not target.exists():
        return AnalyzerSettings()
    try:
        data = json.loads(target.read_text(encoding="utf-8-sig"))
        if not isinstance(data, dict):
            raise ValueError("settings root must be a JSON object")
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError, TypeError):
        # A truncated settings file must never prevent Infraredshift from opening.
        # Preserve it for support review and continue with safe defaults.
        _preserve_corrupt_settings(target)
        return AnalyzerSettings()
    discovery_sql = str(data.get("database_discovery_sql") or DEFAULT_DATABASE_DISCOVERY_SQL)
    migrated_legacy_discovery = normalize_sql_for_hash(discovery_sql) in {
        normalize_sql_for_hash(LEGACY_DATABASE_DISCOVERY_SQL),
        normalize_sql_for_hash(UNSAFE_PG_DATABASE_DISCOVERY_SQL),
    }
    if migrated_legacy_discovery:
        discovery_sql = DEFAULT_DATABASE_DISCOVERY_SQL
    return AnalyzerSettings(
        database_discovery_sql=discovery_sql,
        database_min_query_count=_bounded_int(
            data.get("database_min_query_count"), DEFAULT_DATABASE_MIN_QUERY_COUNT, minimum=0
        ),
        repeat_similarity_threshold=_bounded_float(
            data.get("repeat_similarity_threshold"),
            DEFAULT_REPEAT_SIMILARITY_THRESHOLD,
            0.50,
            0.98,
        ),
        repeat_prefilter_threshold=_bounded_float(
            data.get("repeat_prefilter_threshold"),
            DEFAULT_REPEAT_PREFILTER_THRESHOLD,
            0.10,
            0.80,
        ),
        repeat_fuzzy_merge_threshold=_bounded_float(
            data.get("repeat_fuzzy_merge_threshold"),
            DEFAULT_REPEAT_FUZZY_MERGE_THRESHOLD,
            0.80,
            1.00,
        ),
        repeat_min_group_size=_bounded_int(
            data.get("repeat_min_group_size"), DEFAULT_REPEAT_MIN_GROUP_SIZE, minimum=2
        ),
        repeat_scope_by_user=_stored_bool(data.get("repeat_scope_by_user"), True),
        discovered_databases=(
            []
            if migrated_legacy_discovery
            else _string_list(data.get("discovered_databases"))
        ),
        discovered_at=str(data.get("discovered_at") or ""),
        discovery_source=str(data.get("discovery_source") or ""),
        table_sql_overrides=_string_dict(data.get("table_sql_overrides")),
        table_review_visible_cols=_string_list(data.get("table_review_visible_cols")),
        table_review_show_size_rows=_stored_bool(data.get("table_review_show_size_rows"), False),
        table_review_show_dist_sort=_stored_bool(data.get("table_review_show_dist_sort"), False),
        table_review_hide_without_intersection=_stored_bool(
            data.get("table_review_hide_without_intersection"), True
        ),
        capture_query_limit=_capture_query_limit(data),
        capture_rank_by=_capture_rank_by(data.get("capture_rank_by")),
        capture_selection_version=CAPTURE_SELECTION_VERSION,
        root_min_execution_seconds=_bounded_int(
            data.get("root_min_execution_seconds"), DEFAULT_ROOT_MIN_EXECUTION_SECONDS, minimum=1
        ),
        root_floor_basis=_root_floor_basis(data.get("root_floor_basis")),
        capture_include_tables=_capture_include_tables(data),
        analysis_namespace_filter=_string_list(data.get("analysis_namespace_filter")),
        last_source_cluster_fingerprint=str(data.get("last_source_cluster_fingerprint") or ""),
        last_source_cluster_summary=str(data.get("last_source_cluster_summary") or ""),
        ui_theme=_ui_theme(data.get("ui_theme")),
        topology_zoom=_bounded_float(data.get("topology_zoom"), 1.0, 0.4, 2.0),
    )


def _bounded_float(value: object, default: float, low: float, high: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    if not math.isfinite(number):
        return float(default)
    if number < low:
        return float(low)
    if number > high:
        return float(high)
    return float(number)


def _bounded_int(value: object, default: int, *, minimum: int = 0, maximum: int = 2_147_483_647) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return int(default)
    return max(int(minimum), min(int(maximum), number))


def _stored_bool(value: object, default: bool) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "on"}:
        return True
    if text in {"false", "0", "no", "off", ""}:
        return False
    return bool(default)


def _string_list(value: object) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _string_dict(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key).strip(): str(item).strip()
        for key, item in value.items()
        if str(key).strip() and str(item).strip()
    }


def _capture_rank_by(value: object) -> str:
    text = str(value or DEFAULT_CAPTURE_RANK_BY).strip().lower()
    return text if text in {"elapsed_time", "execution_time"} else DEFAULT_CAPTURE_RANK_BY


def _capture_query_limit(data: dict) -> int:
    try:
        version = int(data.get("capture_selection_version") or 0)
    except (TypeError, ValueError):
        version = 0
    try:
        value = int(data.get("capture_query_limit") or DEFAULT_CAPTURE_QUERY_LIMIT)
    except (TypeError, ValueError):
        return DEFAULT_CAPTURE_QUERY_LIMIT
    if version < CAPTURE_SELECTION_VERSION and value == LEGACY_DEFAULT_CAPTURE_QUERY_LIMIT:
        return DEFAULT_CAPTURE_QUERY_LIMIT
    return max(0, value)


def _capture_include_tables(data: dict) -> list[str]:
    selected = _string_list(data.get("capture_include_tables"))
    try:
        version = int(data.get("capture_selection_version") or 0)
    except (TypeError, ValueError):
        version = 0
    # Version 3 introduces SYS_CHILD_QUERY_TEXT. Existing explicit selections
    # could not have opted out of a dataset that did not exist, so add it while
    # preserving the user's prior ordering and all other choices.
    if selected and version < 3 and "child_query_text" not in selected:
        insert_after = selected.index("query_text") + 1 if "query_text" in selected else len(selected)
        selected.insert(insert_after, "child_query_text")
    return selected


def _root_floor_basis(value: object) -> str:
    text = str(value or "execution_time").strip().lower()
    return text if text in {"execution_time", "elapsed_time"} else "execution_time"


def _ui_theme(value: object) -> str:
    text = str(value or "light").strip().lower()
    return "dark" if text == "dark" else "light"


def save_settings(settings: AnalyzerSettings, path: str | os.PathLike | None = None) -> Path:
    target = Path(path) if path else settings_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(asdict(settings), indent=2), encoding="utf-8")
    os.replace(temporary, target)
    return target


def _preserve_corrupt_settings(target: Path) -> Path | None:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    preserved = target.with_name(f"{target.stem}.corrupt-{stamp}{target.suffix}")
    try:
        if not preserved.exists():
            preserved.write_bytes(target.read_bytes())
        return preserved
    except OSError:
        return None


def render_database_discovery_sql(sql_template: str, min_query_count: int) -> str:
    threshold = str(int(min_query_count))
    return (
        sql_template
        .replace("{min_query_count}", threshold)
        .replace("{threshold}", threshold)
        .strip()
    )


def is_safe_local_database_discovery_sql(sql: str) -> bool:
    """Require an explicit Redshift-local classification before cycling DBs."""
    normalized = normalize_sql_for_hash(str(sql or "")).lower()
    return (
        "svv_redshift_databases" in normalized
        and "database_type" in normalized
        and bool(re.search(r"database_type.{0,100}=\s*'local'", normalized, flags=re.DOTALL))
    )


def render_sql_template(sql_template: str, *, minutes: int) -> str:
    return (
        sql_template
        .replace("{minutes}", str(int(minutes)))
        .replace("{threshold_seconds}", str(int(minutes) * 60))
        .strip()
    )


def normalize_sql_for_hash(sql: str) -> str:
    return "\n".join(line.rstrip() for line in str(sql or "").strip().splitlines()).strip()


def sql_hash(sql: str) -> str:
    return hashlib.sha256(normalize_sql_for_hash(sql).encode("utf-8")).hexdigest()[:16]


def extract_database_names(frame) -> list[str]:
    if frame is None or frame.empty:
        return []
    preferred = ("database_name", "datname", "database", "db_name", "name")
    normalized = {str(col).strip().lower(): col for col in frame.columns}
    source_col: Any = None
    for name in preferred:
        if name in normalized:
            source_col = normalized[name]
            break
    if source_col is None:
        source_col = frame.columns[0]
    type_col = normalized.get("database_type")

    databases: list[str] = []
    seen: set[str] = set()
    for row_index, value in frame[source_col].items():
        if type_col is not None:
            database_type = str(frame.at[row_index, type_col] or "").strip().lower()
            if database_type != "local":
                continue
        name = str(value or "").strip()
        key = name.lower()
        if not name or key in seen:
            continue
        databases.append(name)
        seen.add(key)
    return databases


def update_discovered_databases(
    settings: AnalyzerSettings,
    databases: list[str],
    *,
    source: str,
) -> AnalyzerSettings:
    settings.discovered_databases = databases
    settings.discovered_at = datetime.now().isoformat(timespec="seconds")
    settings.discovery_source = source
    return settings


def resolve_source_cluster_config(args: Any) -> dict[str, str]:
    connection = str(getattr(args, "connection", "") or "native").strip().lower()
    primary_database = str(getattr(args, "primary_database", "") or "").strip()
    table_databases = _normalize_csv(str(getattr(args, "table_databases", "") or ""))
    config = {
        "connection": connection,
        "primary_database": primary_database,
        "table_databases": table_databases,
    }
    if connection == "jdbc":
        config["jdbc_url"] = str(getattr(args, "jdbc_url", "") or "").strip()
    else:
        config["host"] = str(getattr(args, "host", "") or "").strip().lower()
        config["port"] = str(getattr(args, "port", "") or "").strip()
    return config


def source_cluster_configured(config: dict[str, str]) -> bool:
    connection = str(config.get("connection") or "native").lower()
    if connection == "jdbc":
        return bool(str(config.get("jdbc_url") or "").strip())
    return bool(str(config.get("host") or "").strip())


def source_cluster_fingerprint(config: dict[str, str]) -> str:
    normalized = {
        key: str(value).strip()
        for key, value in config.items()
        if str(value or "").strip()
    }
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def source_cluster_summary(config: dict[str, str]) -> str:
    connection = str(config.get("connection") or "native").lower()
    primary = config.get("primary_database") or "-"
    table_databases = config.get("table_databases") or "auto-discovered"
    if connection == "jdbc":
        endpoint = config.get("jdbc_url") or "-"
        return f"JDBC {endpoint}; primary database {primary}; table databases {table_databases}"
    host = config.get("host") or "-"
    port = config.get("port") or "5439"
    return f"Native {host}:{port}; primary database {primary}; table databases {table_databases}"


def update_last_source_cluster(settings: AnalyzerSettings, args: Any) -> bool:
    config = resolve_source_cluster_config(args)
    if not source_cluster_configured(config):
        return False
    settings.last_source_cluster_fingerprint = source_cluster_fingerprint(config)
    settings.last_source_cluster_summary = source_cluster_summary(config)
    return True


# ---------------------------------------------------------------------------
# Per-cluster DuckDB file identity.
#
# Each physical cluster gets its own DuckDB file so switching between (e.g.)
# production and a dev cluster never flushes the other's rows. File identity is
# keyed on the ENDPOINT ONLY (host:port, or jdbc_url) - deliberately NOT on
# primary_database / table_databases. Re-scoping which databases you analyze on
# the same physical cluster must stay in the same file; only a different cluster
# endpoint gets a new file. (The full source_cluster_fingerprint, which folds in
# the database scope, is still used for change-detection messaging - not here.)
# ---------------------------------------------------------------------------


def source_cluster_endpoint(config: dict[str, str]) -> str:
    """Stable, human-readable endpoint token for the cluster, or "" if unset."""
    connection = str(config.get("connection") or "native").lower()
    if connection == "jdbc":
        return str(config.get("jdbc_url") or "").strip().lower()
    host = str(config.get("host") or "").strip().lower()
    if not host:
        return ""
    port = str(config.get("port") or "5439").strip() or "5439"
    return f"{host}:{port}"


def source_cluster_endpoint_key(config: dict[str, str]) -> str:
    """Short hash of the endpoint, safe for a filename. "" if not configured."""
    endpoint = source_cluster_endpoint(config)
    if not endpoint:
        return ""
    return hashlib.sha256(endpoint.encode("utf-8")).hexdigest()[:12]


def _endpoint_slug(config: dict[str, str]) -> str:
    """A short, filesystem-safe label from the host, so the per-cluster file is
    recognizable on disk (e.g. redshift.prod-cluster-abc123.a1b2c3d4e5f6.duckdb)."""
    connection = str(config.get("connection") or "native").lower()
    raw = str(config.get("jdbc_url") if connection == "jdbc" else config.get("host") or "").strip().lower()
    # Keep the leading label of a hostname (before the first dot), sanitized.
    leading = re.split(r"[/@:]", raw)[-1] if raw else ""
    leading = leading.split(".")[0]
    slug = re.sub(r"[^a-z0-9_-]+", "-", leading).strip("-")
    return slug[:40] or "cluster"


def per_cluster_duckdb_path(base_path: Path, config: dict[str, str]) -> Path:
    """Resolve the DuckDB file for a specific cluster, derived from `base_path`.

    Returns `base_path` unchanged when the cluster is not configured (so demo /
    mock / manual flows keep working). Otherwise returns a sibling file named
    <stem>.<slug>.<endpoint-key><suffix> in the same directory."""
    if not source_cluster_configured(config):
        return base_path
    key = source_cluster_endpoint_key(config)
    if not key:
        return base_path
    base_path = Path(base_path)
    stem = base_path.stem or "redshift"
    suffix = base_path.suffix or ".duckdb"
    return base_path.with_name(f"{stem}.{_endpoint_slug(config)}.{key}{suffix}")


def _normalize_csv(value: str) -> str:
    parts = [part.strip() for part in value.split(",") if part.strip()]
    return ",".join(dict.fromkeys(parts))
