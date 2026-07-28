"""Read-only cluster topology and local DuckDB completeness assessment."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import os
from pathlib import Path
import re
from typing import Mapping

import duckdb


@dataclass(frozen=True)
class DatasetSpec:
    table_name: str
    label: str
    category: str
    producer_only: bool = False


REQUIRED_DATASETS: tuple[DatasetSpec, ...] = (
    DatasetSpec("query_history", "Query history", "Workload"),
    DatasetSpec("query_history_all", "History safety copy", "Workload"),
    DatasetSpec("query_text", "SQL text", "Workload"),
    DatasetSpec("query_details", "Workload details", "Execution"),
    DatasetSpec("query_health", "Query health", "Execution"),
    DatasetSpec("query_explain", "Explain plans", "Execution"),
    DatasetSpec("query_detail_flow", "Execution flow", "Execution"),
    DatasetSpec("table_scan_info", "Table scan telemetry", "Execution"),
    DatasetSpec("user_info", "User identities", "Identity"),
    DatasetSpec("svv_table_info_all", "Table information", "Catalog"),
    DatasetSpec("view_definitions", "View definitions", "Catalog"),
    DatasetSpec("procedure_definitions", "Stored procedures", "Catalog"),
    DatasetSpec(
        "external_table_metadata",
        "External table metadata",
        "Catalog",
        producer_only=True,
    ),
)


@dataclass(frozen=True)
class DatasetStatus:
    spec: DatasetSpec
    row_count: int
    state: str  # ready | empty | missing


@dataclass(frozen=True)
class ClusterStatus:
    friendly_name: str
    namespace_id: str
    role: str
    enabled: bool
    host: str
    datasets: tuple[DatasetStatus, ...]
    min_query_id: int | None
    max_query_id: int | None
    query_count: int
    first_query_at: str
    last_query_at: str
    severity: str  # complete | amber | red | disabled
    severity_reason: str
    # Query-time histogram between first_query_at and last_query_at: one count
    # per equal time bucket, so the UI can draw capture coverage and expose
    # gaps left by a late resume.
    coverage_buckets: tuple[int, ...] = ()

    @property
    def ready_dataset_count(self) -> int:
        return sum(item.state == "ready" for item in self.datasets)


@dataclass(frozen=True)
class TopologySnapshot:
    db_path: str
    clusters: tuple[ClusterStatus, ...]
    assessed_at: str
    error: str = ""


def _enabled(value: object, default: bool) -> bool:
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().lower() not in {"0", "false", "no", "off", "disabled"}


def configured_profiles(environment: Mapping[str, str] | None = None) -> list[dict[str, object]]:
    """Return all specified profiles, including intentionally disabled ones."""
    from .portable_config import active_profile_prefixes

    env = environment if environment is not None else os.environ
    result: list[dict[str, object]] = []
    active = active_profile_prefixes(env)
    if active is None:
        configured_consumers = {
            int(match.group(1))
            for key in env
            if (
                match := re.match(
                    r"^REDSHIFT_CONSUMER_(\d+)_", str(key).upper()
                )
            )
        }
        prefixes = (
            "REDSHIFT_PRODUCER",
            *(
                f"REDSHIFT_CONSUMER_{ordinal}"
                for ordinal in sorted(configured_consumers)
            ),
        )
    else:
        prefixes = active
    for prefix in prefixes:
        ordinal_match = re.fullmatch(r"REDSHIFT_CONSUMER_(\d+)", prefix)
        ordinal = int(ordinal_match.group(1)) if ordinal_match else 0
        role = "producer" if ordinal == 0 else "consumer"
        fallback = "Producer" if ordinal == 0 else f"Consumer {ordinal}"
        host = str(env.get(f"{prefix}_HOST") or (env.get("REDSHIFT_HOST") if ordinal == 0 else "") or "").strip()
        namespace = str(
            env.get(f"{prefix}_NAMESPACE_ID")
            or (env.get("REDSHIFT_NAMESPACE") if ordinal == 0 else "")
            or (env.get("REDSHIFT_NAMESPACE_ID") if ordinal == 0 else "")
            or ""
        ).strip()
        friendly = str(
            env.get(f"{prefix}_DISPLAY_NAME")
            or (
                env.get("REDSHIFT_FRIENDLY")
                if ordinal == 0
                else env.get(f"{prefix}_FRIENDLY")
            )
            or (env.get("REDSHIFT_ENV") if ordinal == 0 else "")
            or fallback
        ).strip()
        specified = any((host, namespace, friendly if friendly != fallback else ""))
        if not specified:
            continue
        result.append(
            {
                "friendly_name": friendly,
                "namespace_id": namespace,
                "role": role,
                "enabled": _enabled(
                    (env.get("REDSHIFT_ENABLED") if ordinal == 0 else None)
                    or env.get(f"{prefix}_ENABLED"),
                    bool(host),
                ),
                "host": host,
                "prefix": prefix,
            }
        )
    return result


def _registry_profiles(con) -> list[dict[str, object]]:
    try:
        rows = con.execute(
            """
SELECT namespace_id, cluster_name, cluster_role, cluster_host, captured_at
FROM snapshot_cluster_runs
WHERE NULLIF(TRIM(CAST(namespace_id AS VARCHAR)), '') IS NOT NULL
ORDER BY captured_at DESC NULLS LAST
"""
        ).fetchall()
    except Exception:
        return []
    result: list[dict[str, object]] = []
    seen: set[str] = set()
    for namespace, name, role, host, _captured in rows:
        key = str(namespace or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        role_text = str(role or "consumer").strip().lower()
        result.append(
            {
                "friendly_name": str(name or ("Producer" if role_text == "producer" else namespace)).strip(),
                "namespace_id": str(namespace).strip(),
                "role": role_text,
                "enabled": True,
                "host": str(host or "").strip(),
            }
        )
    return result


def _table_inventory(con) -> tuple[set[str], dict[str, set[str]]]:
    rows = con.execute(
        """
SELECT LOWER(table_name), LOWER(column_name)
FROM information_schema.columns
WHERE LOWER(table_schema) = 'main'
"""
    ).fetchall()
    tables: set[str] = set()
    columns: dict[str, set[str]] = {}
    for table_name, column_name in rows:
        tables.add(table_name)
        columns.setdefault(table_name, set()).add(column_name)
    return tables, columns


def _dataset_counts(con, tables: set[str], columns: dict[str, set[str]]) -> tuple[dict[tuple[str, str], int], set[str]]:
    counts: dict[tuple[str, str], int] = {}
    namespaces: set[str] = set()
    for spec in REQUIRED_DATASETS:
        table = spec.table_name
        if table not in tables:
            continue
        try:
            if "namespace_id" in columns.get(table, set()):
                rows = con.execute(
                    f'''SELECT LOWER(COALESCE(NULLIF(TRIM(CAST(namespace_id AS VARCHAR)), ''), 'producer')), COUNT(*)
                        FROM "{table}" GROUP BY 1'''
                ).fetchall()
                for namespace, row_count in rows:
                    key = str(namespace or "producer").strip().lower()
                    namespaces.add(key)
                    counts[(table, key)] = int(row_count or 0)
            else:
                row_count = int(con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] or 0)
                namespaces.add("producer")
                counts[(table, "producer")] = row_count
        except Exception:
            # A corrupt/unreadable object remains distinguishable from an
            # absent table because it is present in ``tables`` and has 0 rows.
            continue
    return counts, namespaces


# Bucket count for the capture-coverage histogram. 64 equal buckets across
# the observed window keeps the SQL cheap while making multi-day resume gaps
# obvious in the UI.
COVERAGE_BUCKETS = 64


def _coverage_histogram(
    con,
    table: str,
    namespace_clause: str,
    namespace_params: list[object],
    first: str,
    last: str,
) -> tuple[int, ...]:
    try:
        start = datetime.fromisoformat(str(first))
        end = datetime.fromisoformat(str(last))
    except ValueError:
        return ()
    span_seconds = (end - start).total_seconds()
    if span_seconds <= 0:
        return (1,)
    predicate = (
        f"{namespace_clause} AND " if namespace_clause else "WHERE "
    ) + "TRY_CAST(start_time AS TIMESTAMP) IS NOT NULL"
    try:
        rows = con.execute(
            f"""
SELECT
  CAST(LEAST({COVERAGE_BUCKETS - 1}, GREATEST(0, FLOOR(
    (EPOCH(TRY_CAST(start_time AS TIMESTAMP)) - EPOCH(TRY_CAST(? AS TIMESTAMP)))
    / ? * {COVERAGE_BUCKETS}
  ))) AS INTEGER) AS bucket,
  COUNT(*)
FROM "{table}" {predicate}
GROUP BY 1
""",
            [str(first), span_seconds, *namespace_params],
        ).fetchall()
    except Exception:
        return ()
    counts = [0] * COVERAGE_BUCKETS
    for bucket, count in rows:
        if bucket is not None and 0 <= int(bucket) < COVERAGE_BUCKETS:
            counts[int(bucket)] = int(count or 0)
    return tuple(counts)


def _query_coverage(
    con, namespace: str, tables: set[str], columns: dict[str, set[str]]
) -> tuple[int | None, int | None, int, str, str, tuple[int, ...]]:
    for table in ("query_history", "query_history_all"):
        if table not in tables or "query_id" not in columns.get(table, set()):
            continue
        namespace_clause = ""
        params: list[object] = []
        if "namespace_id" in columns.get(table, set()):
            namespace_clause = "WHERE LOWER(COALESCE(NULLIF(TRIM(CAST(namespace_id AS VARCHAR)), ''), 'producer')) = ?"
            params.append(namespace.lower())
        has_start = "start_time" in columns.get(table, set())
        start_expr = "MIN(TRY_CAST(start_time AS TIMESTAMP))" if has_start else "NULL"
        end_expr = "MAX(TRY_CAST(start_time AS TIMESTAMP))" if has_start else "NULL"
        try:
            row = con.execute(
                f"""
SELECT MIN(TRY_CAST(query_id AS BIGINT)), MAX(TRY_CAST(query_id AS BIGINT)),
       COUNT(DISTINCT TRY_CAST(query_id AS BIGINT)), {start_expr}, {end_expr}
FROM "{table}" {namespace_clause}
""",
                params,
            ).fetchone()
        except Exception:
            continue
        if row and int(row[2] or 0) > 0:
            first = str(row[3] or "")
            last = str(row[4] or "")
            buckets: tuple[int, ...] = ()
            if has_start and first and last:
                buckets = _coverage_histogram(con, table, namespace_clause, params, first, last)
            return (
                int(row[0]) if row[0] is not None else None,
                int(row[1]) if row[1] is not None else None,
                int(row[2] or 0),
                first,
                last,
                buckets,
            )
    return None, None, 0, "", "", ()


def load_topology_snapshot(
    db_path: str | os.PathLike,
    environment: Mapping[str, str] | None = None,
) -> TopologySnapshot:
    """Assess configured and observed namespaces without changing DuckDB."""
    from .portable_config import active_profile_prefixes

    path = Path(db_path)
    profiles = configured_profiles(environment)
    profile_environment = environment if environment is not None else os.environ
    active = active_profile_prefixes(profile_environment)
    manifest_is_authoritative = active is not None
    producer_expected = active is None or "REDSHIFT_PRODUCER" in active
    assessed_at = datetime.now().astimezone().isoformat(timespec="seconds")
    if not path.is_file():
        if producer_expected and not any(
            str(profile.get("role")) == "producer" for profile in profiles
        ):
            profiles.insert(0, {"friendly_name": "Producer", "namespace_id": "producer", "role": "producer", "enabled": True, "host": ""})
        clusters = tuple(_empty_cluster(profile, tables=set()) for profile in profiles)
        return TopologySnapshot(str(path), clusters, assessed_at, f"DuckDB file not found: {path}")

    try:
        con = duckdb.connect(str(path), read_only=True)
    except Exception as exc:
        clusters = tuple(_empty_cluster(profile, tables=set()) for profile in profiles)
        return TopologySnapshot(str(path), clusters, assessed_at, str(exc))

    try:
        tables, columns = _table_inventory(con)
        counts, observed_namespaces = _dataset_counts(con, tables, columns)
        registry = _registry_profiles(con)
        merged: dict[str, dict[str, object]] = {}
        configured_namespaces = {
            str(profile.get("namespace_id") or "").strip().lower()
            for profile in profiles
            if str(profile.get("namespace_id") or "").strip()
        }
        for profile in registry:
            key = str(profile.get("namespace_id") or "producer").lower()
            if manifest_is_authoritative and key not in configured_namespaces:
                continue
            merged[key] = dict(profile)
        # Environment configuration is authoritative for friendly names and
        # enabled state; the capture registry fills in historical clusters.
        for profile in profiles:
            key = str(profile.get("namespace_id") or ("producer" if profile.get("role") == "producer" else profile.get("prefix"))).lower()
            merged[key] = {**merged.get(key, {}), **profile}
        for namespace in observed_namespaces:
            if (
                manifest_is_authoritative
                and namespace not in configured_namespaces
            ):
                continue
            merged.setdefault(
                namespace,
                {
                    "friendly_name": "Producer" if namespace == "producer" else namespace,
                    "namespace_id": namespace,
                    "role": "producer" if namespace == "producer" else "consumer",
                    "enabled": True,
                    "host": "",
                },
            )
        if producer_expected and not any(
            str(profile.get("role") or "").lower() == "producer"
            for profile in merged.values()
        ):
            merged["producer"] = {
                "friendly_name": "Producer",
                "namespace_id": "producer",
                "role": "producer",
                "enabled": True,
                "host": "",
            }

        clusters: list[ClusterStatus] = []
        for key, profile in merged.items():
            namespace = str(profile.get("namespace_id") or key or "producer").strip()
            role = str(profile.get("role") or "consumer").lower()
            dataset_states = tuple(
                DatasetStatus(
                    spec,
                    counts.get((spec.table_name, namespace.lower()), 0),
                    (
                        "missing"
                        if spec.table_name not in tables
                        else "ready"
                        if counts.get((spec.table_name, namespace.lower()), 0) > 0
                        else "empty"
                    ),
                )
                for spec in REQUIRED_DATASETS
                if not spec.producer_only or role == "producer"
            )
            coverage = _query_coverage(con, namespace, tables, columns)
            clusters.append(_cluster_from_profile(profile, namespace, dataset_states, coverage))
        clusters.sort(key=lambda item: (0 if item.role == "producer" else 1, item.friendly_name.lower()))
        return TopologySnapshot(str(path), tuple(clusters), assessed_at)
    except Exception as exc:
        clusters = tuple(_empty_cluster(profile, tables=set()) for profile in profiles)
        return TopologySnapshot(str(path), clusters, assessed_at, str(exc))
    finally:
        con.close()


def _cluster_from_profile(
    profile: Mapping[str, object],
    namespace: str,
    datasets: tuple[DatasetStatus, ...],
    coverage: tuple[int | None, int | None, int, str, str, tuple[int, ...]],
) -> ClusterStatus:
    enabled = bool(profile.get("enabled", True))
    ready = sum(item.state == "ready" for item in datasets)
    history_ready = any(item.spec.table_name in {"query_history", "query_history_all"} and item.state == "ready" for item in datasets)
    if not enabled:
        severity, reason = "disabled", "Excluded from the current load configuration"
    elif ready == len(datasets):
        severity, reason = "complete", "All required local datasets contain rows"
    elif ready == 0 or not history_ready:
        severity, reason = "red", "Core seven-day query history is unavailable"
    else:
        severity, reason = "amber", f"{len(datasets) - ready} required dataset(s) are missing or empty"
    return ClusterStatus(
        friendly_name=str(profile.get("friendly_name") or namespace or "Cluster"),
        namespace_id=namespace,
        role=str(profile.get("role") or "consumer").lower(),
        enabled=enabled,
        host=str(profile.get("host") or ""),
        datasets=datasets,
        min_query_id=coverage[0],
        max_query_id=coverage[1],
        query_count=coverage[2],
        first_query_at=coverage[3],
        last_query_at=coverage[4],
        severity=severity,
        severity_reason=reason,
        coverage_buckets=coverage[5] if len(coverage) > 5 else (),
    )


def _empty_cluster(profile: Mapping[str, object], tables: set[str]) -> ClusterStatus:
    role = str(profile.get("role") or "consumer").lower()
    datasets = tuple(
        DatasetStatus(
            spec,
            0,
            "empty" if spec.table_name in tables else "missing",
        )
        for spec in REQUIRED_DATASETS
        if not spec.producer_only or role == "producer"
    )
    namespace = str(profile.get("namespace_id") or ("producer" if profile.get("role") == "producer" else profile.get("prefix") or "unknown"))
    return _cluster_from_profile(profile, namespace, datasets, (None, None, 0, "", "", ()))
