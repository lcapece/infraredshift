"""Capture Redshift system-view snapshots into the local DuckDB warehouse."""
from __future__ import annotations

import argparse
import getpass
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from .duckdb_store import DuckDBStore, quote_ident
from .manual_import import import_table_file
from .redshift_queries import (
    DEFAULT_DETAIL_FLOW_ROWS_PER_QUERY,
    DEFAULT_EXPLAIN_QUERY_LIMIT,
    DEFAULT_ROOT_MIN_EXECUTION_SECONDS,
    DEFAULT_SLOW_QUERY_MINUTES,
    EXTERNAL_CATALOG_STAGE_SQL,
    EXTERNAL_COLUMN_STATS_STAGE_SQL,
    EXTERNAL_TABLE_INFO_SQL,
    EXTERNAL_TABLES_CATALOG_SQL,
    EXTRACTIONS,
    PROCEDURE_DEFINITIONS_SQL,
    SOURCE_REQUIREMENTS,
    TABLE_INFO_SQL,
    VIEW_DEFINITIONS_SQL,
    assemble_external_table_info,
    external_catalog_sql,
    external_metadata_sql,
    external_query_ids,
    external_segments_stage_sql,
    external_steps_stage_sql,
    minimal_external_catalog_from_segments,
)
from .settings import (
    AnalyzerSettings,
    extract_database_names,
    load_settings,
    render_database_discovery_sql,
    render_sql_template,
    save_settings,
    sql_hash,
    update_last_source_cluster,
    update_discovered_databases,
)


DEFAULT_DATABASES = ("dev", "enterprise_datawarehouse", "businesslayer")
# Emergency operating policy shared with Runner. All per-database catalog
# captures are fixed to this database until the operator restores discovery.
CATALOG_DATABASE_SCOPE = (
    "datalake_cl",
    "enterprise_datawarehouse",
    "businesslayer",
    "datalake_wealth",
    "datalake_cl1",
    "datalake_cl2",
    "datalake",
    "byod",
    "investors_bank",
    "smart_leads",
)
DEFAULT_REDSHIFT_PORT = 5439
PER_DATABASE_TABLES = {
    "svv_table_info_all", "external_table_info_all", "view_definitions", "procedure_definitions"
}
# Phase-1 roots: every execution above the execution_time floor, no row-count cap.
ROOT_TABLES = {"query_history", "query_text", "child_query_text", "query_history_all"}
# Tuple order is the dependency-aware refresh order (see EXTRACTIONS comment).
LIVE_REFRESH_TABLES = tuple(EXTRACTIONS) + (
    "svv_table_info_all", "view_definitions", "procedure_definitions", "external_table_info_all"
)
REFRESH_ORDER = {name: index for index, name in enumerate(LIVE_REFRESH_TABLES, start=1)}
# External-table (Spectrum) capture is OPT-IN rather than compiled out.
#
# It stays off by default because it hits SVV_EXTERNAL_* views that not every
# cluster grants, it is the slowest stage in the load, and the Table Heat Map is
# the only consumer. It is enabled per run with --external-tables (or by setting
# REDSHIFT_CAPTURE_EXTERNAL=1), so it can be run on its own instead of being
# bolted onto every refresh.
#
# runner.py keeps its own copy of this constant so both files stay
# standalone-importable.
EXTERNAL_CAPTURE_ENABLED = str(
    os.environ.get("REDSHIFT_CAPTURE_EXTERNAL", "")
).strip().lower() in {"1", "true", "yes", "y", "on"}


def _split_list(value: str) -> tuple[str, ...]:
    """Split a comma/semicolon/whitespace separated config value."""
    return tuple(
        item.strip()
        for item in re.split(r"[,;\s]+", str(value or ""))
        if item.strip()
    )


def external_capture_scope(prefix: str = "REDSHIFT_PRODUCER") -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Read the configured external-capture scope for a cluster profile.

    Set in redshift_cluster_profiles.json beside floor_seconds::

        "external_schemas": "spectrum, raw",
        "external_table_patterns": "fact_*, dim_*"

    Returns (schemas, table_patterns), either of which may be empty. An empty
    scope means "no restriction" for that dimension - callers that care about
    the unbounded case check it explicitly, because on a catalog with millions
    of external tables an accidental full capture is the failure being fixed.
    """
    schemas = _split_list(os.environ.get(f"{prefix}_EXTERNAL_SCHEMAS", ""))
    patterns = _split_list(os.environ.get(f"{prefix}_EXTERNAL_TABLE_PATTERNS", ""))
    return schemas, patterns
INCREMENTAL_APPEND_TABLES = set(EXTRACTIONS) - {"user_info"}
INCREMENTAL_DEDUPE_KEYS: dict[str, tuple[str, ...]] = {
    "query_history": ("query_id",),
    "query_history_all": ("query_id",),
    "query_details": ("query_id",),
    "query_health": ("query_id",),
    "query_explain": ("query_id",),
    "query_detail_flow": ("query_id",),
    "query_text": ("query_id",),
    "child_query_text": ("query_id", "child_query_sequence", "sequence"),
    "table_scan_info": ("query_id", "full_table_name"),
}


def refresh_step_label(table_name: str) -> str:
    position = REFRESH_ORDER.get(table_name)
    if position is None:
        return table_name
    return f"[{position:02d}/{len(LIVE_REFRESH_TABLES)}] {table_name}"


class IngestCancelled(RuntimeError):
    """Raised from the progress hook to abort an in-flight capture. Safe:
    nothing is written to DuckDB until a table's fetch fully completes."""


_FETCH_BATCH_ROWS = 25000
_PROGRESS_HOOK = None
_EXTERNAL_TIMEOUT_DECISION_HOOK = None


def set_progress_hook(hook) -> None:
    """Install a callable(stage: str, rows_fetched: int) invoked as capture
    rows stream in from Redshift. Runs on the capturing thread. Raising
    IngestCancelled from the hook aborts the current fetch."""
    global _PROGRESS_HOOK
    _PROGRESS_HOOK = hook


def set_external_timeout_decision_hook(hook) -> None:
    """Install a callable(stage, error) returning 'continue' or 'next'."""
    global _EXTERNAL_TIMEOUT_DECISION_HOOK
    _EXTERNAL_TIMEOUT_DECISION_HOOK = hook


def _is_external_timeout(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(marker in text for marker in (
        "statement timeout", "canceling statement", "system requested abort",
        "abort query", "query timeout", "timed out",
    ))


def _report_progress(stage: str, rows: int) -> None:
    hook = _PROGRESS_HOOK
    if hook is not None:
        hook(stage, rows)


def _frame_from_cursor(cur, stage: str) -> pd.DataFrame:
    columns = [str(d[0]) for d in cur.description]
    rows: list = []
    while True:
        batch = cur.fetchmany(_FETCH_BATCH_ROWS)
        if not batch:
            break
        rows.extend(batch)
        _report_progress(stage, len(rows))
    return pd.DataFrame.from_records(rows, columns=columns)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(args.env_file)
    settings = load_settings()
    resolve_args_from_env(args, settings)
    if args.import_file:
        if not args.import_table:
            raise SystemExit("Provide --import-table with --import-file.")
        result = import_table_file(
            DuckDBStore(args.duckdb_path),
            args.import_table,
            args.import_file,
            label=args.label,
            source_database=args.import_source_database or "",
            append=args.import_append,
        )
        print(
            f"Imported {result.row_count:,} row(s), {result.column_count:,} column(s) "
            f"into {result.table_name} from {result.file_path} in {result.elapsed_ms:,.0f} ms. "
            f"Snapshot: {result.snapshot_id}"
        )
        if result.missing_expected:
            print(f"Warning: missing expected column(s): {', '.join(result.missing_expected)}")
        return 0

    password = resolve_password(args)
    user = args.user
    if not user:
        if sys.stdin.isatty():
            user = input("Redshift username: ").strip()
            args.user = user
        if not user:
            raise SystemExit(
                "No unlocked Redshift username is available. Open Infraredshift and "
                "configure Local Credentials first."
            )
    if args.connection == "jdbc":
        jdbc_url = args.jdbc_url
        jdbc_jar = args.jdbc_jar
        if not jdbc_url:
            raise SystemExit("Provide --jdbc-url, set REDSHIFT_JDBC_URL, or add it to .env.")
        if not jdbc_jar:
            raise SystemExit("Provide --jdbc-jar, set REDSHIFT_JDBC_JAR, or add it to .env.")
        jar_paths = [str(Path(p).expanduser()) for p in split_csv(jdbc_jar)]
        missing_jars = [p for p in jar_paths if not Path(p).exists()]
        if missing_jars:
            raise SystemExit(f"JDBC jar path not found: {missing_jars[0]}")
    else:
        jdbc_url = ""
        jar_paths = []
        if not args.host:
            raise SystemExit("Provide --host, set REDSHIFT_HOST, or add REDSHIFT_HOST to .env.")

    if args.reload_databases and not args.reload_view_definitions and not args.refresh_empty_only:
        reload_database_cache(args, settings, user, password, jdbc_url, jar_paths)
        return 0

    store = DuckDBStore(args.duckdb_path)

    with store.connect() as duck:
        use_latest_snapshot = (
            args.refresh_empty_only
            or args.reload_view_definitions
            or args.refresh_external_catalog
            or args.refresh_user_roster
            or args.refresh_table
            or (args.incremental and not args.truncate_all_first)
        )
        run = store.latest_snapshot(duck) if use_latest_snapshot else None
        created_snapshot = run is None
        if run is None:
            run = store.new_snapshot(args.label)
        configure_incremental_high_water(args, duck, store)
        if args.refresh_table:
            accessed_databases = refresh_one_table(
                args, settings, duck, store, run, args.refresh_table, user, password, jdbc_url, jar_paths
            )
            print(f"Table {args.refresh_table} refreshed in snapshot {run.snapshot_id} at {store.path}")
            if accessed_databases:
                print(f"Databases accessed: {', '.join(accessed_databases)}")
            if created_snapshot:
                store.record_snapshot(duck, run, source=f"redshift-{args.connection}")
            return 0
        if args.reload_view_definitions:
            table_databases = resolve_table_databases(args, settings, user, password, jdbc_url, jar_paths)
            for database in table_databases:
                validate_source_view(args, database, user, password, jdbc_url, jar_paths, "pg_views")
            capture_view_definitions(args, duck, store, run, table_databases, user, password, jdbc_url, jar_paths)
            if created_snapshot:
                store.record_snapshot(duck, run, source=f"redshift-{args.connection}")
            print(f"View definitions reloaded in snapshot {run.snapshot_id} at {store.path}")
            return 0

        if args.refresh_user_roster:
            # Producer-only, enterprise_data_warehouse. Persistent table.
            capture_user_roster(args, duck, store, run, user, password, jdbc_url, jar_paths)
            if created_snapshot:
                store.record_snapshot(duck, run, source=f"redshift-{args.connection}")
            print(f"User roster reloaded in snapshot {run.snapshot_id} at {store.path}")
            return 0

        if args.refresh_external_catalog:
            # Isolated, producer-only external-table catalog. Cycles per
            # database with a bounded timeout so a slow Glue/Hive catalog
            # cannot hang the app.
            table_databases = resolve_table_databases(args, settings, user, password, jdbc_url, jar_paths)
            capture_external_tables_catalog(
                args, duck, store, run, table_databases, user, password, jdbc_url, jar_paths,
                statement_timeout_seconds=int(getattr(args, "external_catalog_timeout", 120) or 120),
            )
            if created_snapshot:
                store.record_snapshot(duck, run, source=f"redshift-{args.connection}")
            print(f"External-table catalog reloaded in snapshot {run.snapshot_id} at {store.path}")
            return 0

        included_tables = included_refresh_tables(args.include_tables)
        print("Validating Redshift source views and columns")
        validate_sources(
            args,
            args.primary_database,
            user,
            password,
            jdbc_url,
            jar_paths,
            include_svv=False,
            included_tables=included_tables,
        )
        table_databases = resolve_table_databases(args, settings, user, password, jdbc_url, jar_paths)
        for database in table_databases:
            validate_sources(
                args,
                database,
                user,
                password,
                jdbc_url,
                jar_paths,
                include_sys=False,
                included_tables=included_tables,
            )
        if args.truncate_all_first:
            # Full refresh still writes a NEW snapshot first and only publishes
            # it after capture. Take a local backup so operators can roll back
            # if the new capture is empty or wrong.
            try:
                if store.path.exists():
                    backup_path = store.backup_database("before-full-refresh")
                    print(f"Backup before full refresh: {backup_path}")
            except Exception as exc:
                print(f"WARNING: could not backup DuckDB before full refresh: {exc}")
            print(
                "Validation passed; safe full-refresh mode is preserving the current "
                "snapshot until the new snapshot completes"
            )
        print(f"Capturing slow-query views from primary database {args.primary_database!r}")
        step = refresh_step_label
        for table_name, sql_factory in EXTRACTIONS.items():
            if table_name not in included_tables:
                print(f"  {step(table_name)}: skipped, not selected for this run")
                continue
            if args.refresh_empty_only and not store.table_is_empty(duck, table_name):
                print(f"  {step(table_name)}: skipped, DuckDB table already has rows")
                continue
            if table_name not in ROOT_TABLES and table_name != "user_info":
                resolve_evidence_targets(args, duck, run)
            print(f"  {step(table_name)}: capturing")
            capture_extraction_table(args, duck, store, run, table_name, user, password, jdbc_url, jar_paths)

        if "svv_table_info_all" not in included_tables:
            print(f"  {step('svv_table_info_all')}: skipped, not selected for this run")
        elif args.refresh_empty_only and not store.table_is_empty(duck, "svv_table_info_all"):
            print(f"  {step('svv_table_info_all')}: skipped, DuckDB table already has rows")
        else:
            print(f"  {step('svv_table_info_all')}: capturing")
            capture_table_info(args, duck, store, run, table_databases, user, password, jdbc_url, jar_paths)

        if "view_definitions" not in included_tables:
            print(f"  {step('view_definitions')}: skipped, not selected for this run")
        elif args.refresh_empty_only and not store.table_is_empty(duck, "view_definitions"):
            print(f"  {step('view_definitions')}: skipped, DuckDB table already has rows")
        else:
            print(f"  {step('view_definitions')}: capturing")
            capture_view_definitions(args, duck, store, run, table_databases, user, password, jdbc_url, jar_paths)

        if "procedure_definitions" not in included_tables:
            print(f"  {step('procedure_definitions')}: skipped, not selected for this run")
        elif args.refresh_empty_only and not store.table_is_empty(duck, "procedure_definitions"):
            print(f"  {step('procedure_definitions')}: skipped, DuckDB table already has rows")
        else:
            print(f"  {step('procedure_definitions')}: capturing")
            capture_procedure_definitions(args, duck, store, run, table_databases, user, password, jdbc_url, jar_paths)

        # Keep the potentially slow external catalog as the final capture so
        # all other analyzer functionality is already safely loaded.
        capture_external = EXTERNAL_CAPTURE_ENABLED or bool(
            getattr(args, "external_tables", False)
        )
        if not capture_external:
            print(
                f"  {step('external_table_info_all')}: skipped "
                "(pass --external-tables to capture Spectrum metadata)"
            )
        elif "external_table_info_all" not in included_tables:
            print(f"  {step('external_table_info_all')}: skipped, not selected for this run")
        elif args.refresh_empty_only and not store.table_is_empty(duck, "external_table_info_all"):
            print(f"  {step('external_table_info_all')}: skipped, DuckDB table already has rows")
        else:
            print(f"  {step('external_table_info_all')}: capturing last")
            capture_external_table_info(
                args, duck, store, run, table_databases, user, password, jdbc_url, jar_paths
            )

        if created_snapshot:
            # Publishing the snapshot is the promotion point. If any capture
            # above fails, the prior snapshot remains the latest visible run.
            store.record_snapshot(duck, run, source=f"redshift-{args.connection}")

    print(f"Snapshot {run.snapshot_id} stored in {store.path}")
    if update_last_source_cluster(settings, args):
        save_settings(settings)
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture Redshift slow-query diagnostics into a local DuckDB file."
    )
    parser.add_argument("--duckdb-path", default=None, help="Output DuckDB path.")
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional .env path. Defaults to .env in the working directory, launcher directory, or repo root.",
    )
    parser.add_argument(
        "--connection",
        choices=("native", "jdbc"),
        default=None,
        help="Capture transport. Native uses redshift-connector; JDBC uses JayDeBeApi if installed.",
    )
    parser.add_argument("--host", default=None, help="Redshift host for native connection.")
    parser.add_argument("--port", type=int, default=None, help="Redshift port for native connection.")
    parser.add_argument("--jdbc-url", default=None, help="Redshift JDBC URL. May contain {database}.")
    parser.add_argument("--jdbc-jar", default=None, help="Path to Redshift JDBC driver jar.")
    parser.add_argument("--driver", default="com.amazon.redshift.jdbc.Driver", help="JDBC driver class.")
    parser.add_argument("--user", default=None, help=argparse.SUPPRESS)
    parser.add_argument(
        "--password-env",
        default="REDSHIFT_PASSWORD",
        help=(
            "Environment variable containing the Redshift password. If it is not set "
            "and the command is running in an interactive terminal, you will be prompted."
        ),
    )
    parser.add_argument("--primary-database", default=None, help="Database used for cluster-wide SYS views.")
    parser.add_argument(
        "--table-databases",
        default=None,
        help=(
            "Optional comma-separated database override for SVV_TABLE_INFO and view definitions. "
            "Procedure definitions use the same database list. By default, databases are discovered from sys_query_history."
        ),
    )
    parser.add_argument(
        "--database-min-query-count",
        type=int,
        default=None,
        help="Discover databases with more than this many sys_query_history rows.",
    )
    parser.add_argument(
        "--database-discovery-sql",
        default=None,
        help="SQL used to discover database names. May contain {min_query_count}.",
    )
    parser.add_argument(
        "--reload-databases",
        action="store_true",
        help="Force the database discovery SQL to run and save the local database list.",
    )
    parser.add_argument("--minutes", type=int, default=DEFAULT_SLOW_QUERY_MINUTES)
    parser.add_argument(
        "--min-execution-seconds",
        type=float,
        default=None,
        help=(
            "Phase-1 characteristic floor: capture every sys_query_history execution "
            "whose time on the chosen basis is at least this many seconds."
        ),
    )
    parser.add_argument(
        "--floor-basis",
        choices=("execution_time", "elapsed_time"),
        default=None,
        help=(
            "Time basis for the phase-1 floor: execution_time (default, nets out "
            "queue/wait) or elapsed_time (wall clock including queue/wait)."
        ),
    )
    parser.add_argument(
        "--evidence-parent-limit",
        dest="explain_query_limit",
        type=int,
        default=None,
        help=(
            "Optional cap on representative parent-pattern query IDs used for bounded SYS "
            "evidence captures. Defaults to 0, meaning every threshold-qualified parent pattern."
        ),
    )
    parser.add_argument(
        "--explain-query-limit",
        dest="explain_query_limit",
        type=int,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--top-query-rank-by",
        choices=("elapsed_time", "execution_time"),
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--detail-flow-rows-per-query",
        type=int,
        default=None,
        help=(
            "Maximum aggregated SYS_QUERY_DETAIL flow rows kept per representative parent query."
        ),
    )
    parser.add_argument("--label", default="manual capture")
    parser.add_argument("--import-file", default=None, help="Import one CSV/TSV file into a DuckDB analyzer table and exit.")
    parser.add_argument("--import-table", default=None, help="Target analyzer table for --import-file.")
    parser.add_argument(
        "--import-source-database",
        default="",
        help="Optional source database for per-database SVV_TABLE_INFO or pg_views files.",
    )
    parser.add_argument(
        "--import-append",
        action="store_true",
        help="Append imported rows to the latest snapshot instead of replacing that table in the snapshot.",
    )
    parser.add_argument(
        "--refresh-empty-only",
        action="store_true",
        help="Only run Redshift extractions for analyzer DuckDB tables that currently have zero rows.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help=(
            "Append query/evidence rows newer than the local query high-water mark. "
            "Catalog tables still refresh as point-in-time replacements."
        ),
    )
    parser.add_argument("--incremental-after-time", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--incremental-after-query-id", default=None, help=argparse.SUPPRESS)
    parser.add_argument(
        "--truncate-all-first",
        action="store_true",
        help=(
            "Truncate all analyzer DuckDB tables before capturing, but only after "
            "source validation succeeds. Used for full reloads."
        ),
    )
    parser.add_argument(
        "--include-tables",
        default=None,
        help=(
            "Comma-separated analyzer tables to include in a full or refresh-empty run. "
            "Defaults to all Redshift extraction tables."
        ),
    )
    parser.add_argument(
        "--refresh-table",
        choices=LIVE_REFRESH_TABLES,
        default=None,
        help="Refresh one analyzer DuckDB table from Redshift and exit.",
    )
    parser.add_argument(
        "--reload-view-definitions",
        action="store_true",
        help="Reload only pg_views into the latest DuckDB snapshot.",
    )
    parser.add_argument(
        "--refresh-external-catalog",
        action="store_true",
        help="Reload only the SVV_EXTERNAL_TABLES catalog (producer, per database) and exit.",
    )
    parser.add_argument(
        "--refresh-user-roster",
        action="store_true",
        help="Reload only the parsed SVV_USER_INFO roster (producer, enterprise_data_warehouse) and exit.",
    )
    parser.add_argument(
        "--external-catalog-timeout",
        type=int,
        default=120,
        help="Per-database statement timeout (seconds) for the external catalog fetch.",
    )
    parser.add_argument(
        "--external-tables",
        action="store_true",
        help=(
            "Capture Spectrum/external table metadata. Off by default: it needs "
            "SVV_EXTERNAL_* privileges, is the slowest stage of the load, and only "
            "the Table Heat Map consumes it. Run it on its own rather than adding "
            "it to every refresh."
        ),
    )
    return parser.parse_args(argv)


def load_dotenv(explicit_path: str | None = None) -> None:
    from .portable_config import (
        apply_portable_environment,
        portable_config_candidates,
        read_portable_environment,
    )
    from .secrets_store import is_secret_key

    loaded = False
    for path in dotenv_candidates(explicit_path):
        if not path.exists():
            continue
        values = parse_dotenv(path)
        for key, value in values.items():
            # Usernames/passwords and legacy protected endpoints are unlocked
            # into an in-process credential session by the login gate.  Never
            # copy them into the inheritable process environment here.
            if not is_secret_key(key):
                os.environ.setdefault(key, value)
        if values:
            print(f"Loaded .env settings from {path}")
        loaded = True
        break
    if explicit_path and not loaded:
        raise SystemExit(f"Environment configuration not found: {explicit_path}")
    if not explicit_path:
        for path in portable_config_candidates(include_legacy_cwd=False):
            if not path.is_file():
                continue
            values = read_portable_environment(path)
            apply_portable_environment(values)
            if values:
                print(f"Loaded portable non-secret cluster profiles from {path}")
            break


def dotenv_candidates(explicit_path: str | None = None) -> tuple[Path, ...]:
    if explicit_path:
        return (Path(explicit_path).expanduser(),)

    candidates: list[Path] = []
    launch_dir = os.environ.get("REDSHIFT_ANALYZER_LAUNCH_DIR")
    if launch_dir:
        candidates.append(Path(launch_dir) / ".env")
    else:
        # Source-tree CLI compatibility only. A packaged launcher always sets
        # LAUNCH_DIR, so an arbitrary shortcut/scheduler CWD is not trusted.
        candidates.append(Path.cwd() / ".env")
    candidates.append(Path(__file__).resolve().parents[1] / ".env")

    unique: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path.resolve()) if path.exists() else str(path.absolute())
        if key not in seen:
            unique.append(path)
            seen.add(key)
    return tuple(unique)


def parse_dotenv(path: Path) -> dict[str, str]:
    return parse_dotenv_text(path.read_text(encoding="utf-8-sig"), source=str(path))


def parse_dotenv_text(text: str, *, source: str = ".env configuration") -> dict[str, str]:
    values: dict[str, str] = {}
    for line_no, raw_line in enumerate(str(text).splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            raise SystemExit(f"Invalid .env line {line_no} in {source}: expected KEY=value")
        key, value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise SystemExit(f"Invalid .env key {key!r} on line {line_no} in {source}")
        values[key] = parse_dotenv_value(value.strip())
    return values


def parse_dotenv_value(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        inner = value[1:-1]
        if value[0] == '"':
            return (
                inner
                .replace(r"\\", "\\")
                .replace(r"\"", '"')
                .replace(r"\n", "\n")
                .replace(r"\r", "\r")
                .replace(r"\t", "\t")
            )
        return inner

    chars: list[str] = []
    previous = ""
    for char in value:
        if char == "#":
            if not previous or previous.isspace():
                break
        chars.append(char)
        previous = char
    return "".join(chars).strip()


def resolve_args_from_env(args: argparse.Namespace, settings: AnalyzerSettings) -> None:
    from .secrets_store import session_secret

    if args.user:
        raise SystemExit(
            "--user is not accepted because command-line arguments are visible to other processes. "
            "Store the Redshift username in Infraredshift Local Credentials."
        )
    args.connection = args.connection or os.environ.get("REDSHIFT_CONNECTION") or "native"
    args.host = args.host or session_secret("REDSHIFT_HOST") or os.environ.get("REDSHIFT_HOST")
    args.user = args.user or session_secret("REDSHIFT_USER")
    args.jdbc_url = args.jdbc_url or os.environ.get("REDSHIFT_JDBC_URL")
    if args.jdbc_url and re.search(
        r"(?i)(?:[?;&](?:user|username|password|pwd)=|://[^/@\s]+:[^/@\s]+@)",
        str(args.jdbc_url),
    ):
        raise SystemExit(
            "JDBC URL must not contain a username or password. Store credentials in "
            "Infraredshift Local Credentials instead."
        )
    args.jdbc_jar = args.jdbc_jar or os.environ.get("REDSHIFT_JDBC_JAR")
    args.duckdb_path = args.duckdb_path or os.environ.get("REDSHIFT_DUCKDB_PATH")
    args.primary_database = (
        args.primary_database
        or os.environ.get("REDSHIFT_PRIMARY_DATABASE")
        or os.environ.get("REDSHIFT_DATABASE")
        or "dev"
    )
    # Database scope is restricted to physically local Redshift databases.
    args.table_databases = args.table_databases if args.table_databases is not None else ""
    args.database_discovery_sql = (
        args.database_discovery_sql
        or os.environ.get("REDSHIFT_DATABASE_DISCOVERY_SQL")
        or settings.database_discovery_sql
    )
    from .settings import DEFAULT_DATABASE_DISCOVERY_SQL, is_safe_local_database_discovery_sql
    if not is_safe_local_database_discovery_sql(args.database_discovery_sql):
        print("Ignoring unsafe database discovery SQL; using local physical Redshift databases only.")
        args.database_discovery_sql = DEFAULT_DATABASE_DISCOVERY_SQL
    if args.database_min_query_count is None:
        threshold = os.environ.get("REDSHIFT_DATABASE_MIN_QUERY_COUNT")
        if threshold is None:
            args.database_min_query_count = settings.database_min_query_count
        else:
            try:
                args.database_min_query_count = int(threshold)
            except ValueError as exc:
                raise SystemExit(
                    f"REDSHIFT_DATABASE_MIN_QUERY_COUNT must be an integer, got {threshold!r}"
                ) from exc
    if args.port is None:
        port = os.environ.get("REDSHIFT_PORT", str(DEFAULT_REDSHIFT_PORT))
        try:
            args.port = int(port)
        except ValueError as exc:
            raise SystemExit(f"REDSHIFT_PORT must be an integer, got {port!r}") from exc
    if args.explain_query_limit is None:
        limit = os.environ.get("REDSHIFT_EVIDENCE_PARENT_LIMIT") or os.environ.get("REDSHIFT_EXPLAIN_QUERY_LIMIT")
        if limit:
            try:
                args.explain_query_limit = max(0, int(limit))
            except ValueError as exc:
                raise SystemExit(
                    f"REDSHIFT_EVIDENCE_PARENT_LIMIT must be an integer, got {limit!r}"
                ) from exc
        else:
            args.explain_query_limit = max(
                0, int(settings.capture_query_limit or DEFAULT_EXPLAIN_QUERY_LIMIT)
            )
    if args.top_query_rank_by is None:
        rank_by = os.environ.get("REDSHIFT_TOP_QUERY_RANK_BY", settings.capture_rank_by or "elapsed_time").strip().lower()
        if rank_by not in {"elapsed_time", "execution_time"}:
            raise SystemExit(
                "REDSHIFT_TOP_QUERY_RANK_BY must be elapsed_time or execution_time, "
                f"got {rank_by!r}"
            )
        args.top_query_rank_by = rank_by
    if args.include_tables is None:
        args.include_tables = os.environ.get("REDSHIFT_INCLUDE_TABLES")
        if args.include_tables is None and settings.capture_include_tables:
            args.include_tables = ",".join(settings.capture_include_tables)
    if args.min_execution_seconds is None:
        floor_value = os.environ.get("REDSHIFT_MIN_EXECUTION_SECONDS")
        if floor_value:
            try:
                args.min_execution_seconds = float(floor_value)
            except ValueError as exc:
                raise SystemExit(
                    f"REDSHIFT_MIN_EXECUTION_SECONDS must be a number, got {floor_value!r}"
                ) from exc
        else:
            args.min_execution_seconds = float(
                getattr(settings, "root_min_execution_seconds", DEFAULT_ROOT_MIN_EXECUTION_SECONDS)
            )
    if args.floor_basis is None:
        basis = (
            os.environ.get("REDSHIFT_FLOOR_BASIS")
            or getattr(settings, "root_floor_basis", "execution_time")
            or "execution_time"
        ).strip().lower()
        if basis not in {"execution_time", "elapsed_time"}:
            raise SystemExit(
                f"REDSHIFT_FLOOR_BASIS must be execution_time or elapsed_time, got {basis!r}"
            )
        args.floor_basis = basis
    args.target_ids = None
    if args.detail_flow_rows_per_query is None:
        limit = os.environ.get("REDSHIFT_DETAIL_FLOW_ROWS_PER_QUERY")
        if limit:
            try:
                args.detail_flow_rows_per_query = int(limit)
            except ValueError as exc:
                raise SystemExit(
                    f"REDSHIFT_DETAIL_FLOW_ROWS_PER_QUERY must be an integer, got {limit!r}"
                ) from exc
        else:
            args.detail_flow_rows_per_query = DEFAULT_DETAIL_FLOW_ROWS_PER_QUERY


def resolve_password(args: argparse.Namespace) -> str:
    from .secrets_store import session_secret

    password = session_secret(args.password_env)
    if password:
        return password

    if sys.stdin.isatty():
        password = getpass.getpass("Redshift password: ")
        if password:
            return password
        raise SystemExit("Redshift password cannot be blank.")

    raise SystemExit(
        "No unlocked Redshift password is available and no interactive terminal "
        "is available for a secure prompt. Open Infraredshift and unlock Local Credentials first."
    )


def configure_incremental_high_water(args: argparse.Namespace, duck, store: DuckDBStore) -> None:
    if not getattr(args, "incremental", False) or getattr(args, "truncate_all_first", False):
        args.incremental = False
        return
    if args.incremental_after_time or args.incremental_after_query_id:
        print(
            "Incremental refresh fixed at high-water mark "
            f"QUERY: {args.incremental_after_query_id or '-'} Date: {args.incremental_after_time or '-'}"
        )
        return
    mark = store.query_high_water_mark(duck)
    if mark is None:
        print("Incremental refresh requested; no local query high-water mark exists, so this run captures all qualifying roots.")
        return
    query_id, start_time = mark
    args.incremental_after_query_id = query_id
    args.incremental_after_time = _incremental_time_arg(start_time)
    print(
        "Incremental refresh fixed at high-water mark "
        f"QUERY: {args.incremental_after_query_id or '-'} Date: {args.incremental_after_time or '-'}"
    )


def _incremental_time_arg(value: object) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    try:
        parsed = pd.to_datetime(value, errors="coerce")
        if parsed is not None and not pd.isna(parsed):
            return parsed.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    return str(value).replace("T", " ").replace("Z", "").strip()


def default_table_sql(
    args: argparse.Namespace,
    table_name: str,
    target_ids=None,
    *,
    include_incremental: bool = True,
) -> str:
    incremental_after_time = args.incremental_after_time if include_incremental and getattr(args, "incremental", False) else None
    incremental_after_query_id = (
        args.incremental_after_query_id if include_incremental and getattr(args, "incremental", False) else None
    )
    min_execution_seconds = getattr(args, "min_execution_seconds", None)
    if table_name in ROOT_TABLES:
        return EXTRACTIONS[table_name](
            args.minutes,
            args.explain_query_limit,
            args.top_query_rank_by,
            min_execution_seconds=min_execution_seconds,
            floor_basis=getattr(args, "floor_basis", "execution_time"),
            incremental_after_time=incremental_after_time,
            incremental_after_query_id=incremental_after_query_id,
            window_days=getattr(args, "root_window_days", None),
        )
    if table_name == "query_detail_flow":
        return EXTRACTIONS[table_name](
            args.minutes,
            args.explain_query_limit,
            args.detail_flow_rows_per_query,
            args.top_query_rank_by,
            target_ids=target_ids,
            incremental_after_time=incremental_after_time,
            incremental_after_query_id=incremental_after_query_id,
        )
    if table_name == "user_info":
        return EXTRACTIONS[table_name](args.minutes, args.explain_query_limit, args.top_query_rank_by)
    if table_name in EXTRACTIONS:
        return EXTRACTIONS[table_name](
            args.minutes,
            args.explain_query_limit,
            args.top_query_rank_by,
            target_ids=target_ids,
            incremental_after_time=incremental_after_time,
            incremental_after_query_id=incremental_after_query_id,
        )
    if table_name == "svv_table_info_all":
        return TABLE_INFO_SQL
    if table_name == "external_table_info_all":
        return EXTERNAL_TABLE_INFO_SQL
    if table_name == "view_definitions":
        return VIEW_DEFINITIONS_SQL
    if table_name == "procedure_definitions":
        return PROCEDURE_DEFINITIONS_SQL
    raise ValueError(f"Unknown refreshable table: {table_name}")


def table_sql(
    args: argparse.Namespace,
    settings: AnalyzerSettings,
    table_name: str,
    target_ids=None,
    *,
    include_incremental: bool = True,
) -> str:
    override = settings.table_sql_overrides.get(table_name, "").strip()
    if override:
        return render_sql_template(override, minutes=args.minutes)
    return default_table_sql(args, table_name, target_ids=target_ids, include_incremental=include_incremental)


def compute_parent_target_ids(
    duck,
    snapshot_id: str,
    limit: int,
    floor_basis: str = "execution_time",
    *,
    incremental_after_time: object = None,
    incremental_after_query_id: object = None,
    history_table: str = "query_history",
    text_table: str = "query_text",
) -> list[int]:
    """Phase 2: group the captured roots into parents by canonical,
    literal-agnostic SQL text and return one representative query_id (the
    heaviest execution) per parent, ranked by the parent's TOTAL time on the
    chosen basis. Runs entirely against the local DuckDB file."""
    basis = str(floor_basis or "execution_time").strip().lower()
    if basis not in {"execution_time", "elapsed_time"}:
        basis = "execution_time"
    history_ident = quote_ident(history_table)
    text_ident = quote_ident(text_table)
    try:
        history_cols = {
            str(row[0]).lower()
            for row in duck.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
                [history_table],
            ).fetchall()
        }
        history_text = "NULLIF(h.query_text, '')" if "query_text" in history_cols else "NULL"
        incremental_where, incremental_params = _local_incremental_where(
            "h",
            incremental_after_time,
            incremental_after_query_id,
        )
        # Source views differ in column naming (sys_query_text uses text/sequence;
        # imports may carry sql_text/sequence_num), so coalesce across variants.
        frame = duck.execute(
            f"""
SELECT
  h.query_id,
  COALESCE(TRY_CAST(h.{basis} AS BIGINT), 0) AS execution_time,
  COALESCE(NULLIF(t.sql_text, ''), {history_text}, '') AS sql_text
FROM {history_ident} h
LEFT JOIN (
  SELECT
    query_id,
    STRING_AGG(
      COALESCE(NULLIF(text, ''), NULLIF(sql_text, ''), NULLIF(query_text, ''), ''),
      ''
      ORDER BY COALESCE(TRY_CAST(sequence AS BIGINT), TRY_CAST(sequence_num AS BIGINT), 0)
    ) AS sql_text
  FROM {text_ident}
  WHERE snapshot_id = ?
  GROUP BY query_id
) t ON t.query_id = h.query_id
WHERE h.snapshot_id = ?
  AND {incremental_where}
""",
            [snapshot_id, snapshot_id, *incremental_params],
        ).fetchdf()
    except Exception:
        return []
    if frame is None or frame.empty:
        return []

    from .query_similarity import canonical_sql_fingerprint

    fingerprint_cache: dict[str, str] = {}
    groups: dict[str, dict] = {}
    for row in frame.itertuples(index=False):
        text = str(row.sql_text or "").strip()
        if not text:
            continue
        fingerprint = fingerprint_cache.get(text)
        if fingerprint is None:
            try:
                fingerprint = canonical_sql_fingerprint(text)[0] or text
            except Exception:
                fingerprint = text
            fingerprint_cache[text] = fingerprint
        execution = int(row.execution_time or 0)
        group = groups.setdefault(
            fingerprint, {"total": 0, "best_execution": -1, "best_id": None}
        )
        group["total"] += execution
        if execution > group["best_execution"]:
            group["best_execution"] = execution
            group["best_id"] = row.query_id
    ranked = sorted(groups.values(), key=lambda group: -group["total"])
    cap = max(0, int(limit or 0))
    selected_groups = ranked if cap <= 0 else ranked[:cap]
    target_ids: list[int] = []
    for group in selected_groups:
        try:
            target_ids.append(int(str(group["best_id"])))
        except (TypeError, ValueError):
            continue
    return target_ids


def _local_incremental_where(alias: str, after_time: object = None, after_query_id: object = None) -> tuple[str, list[object]]:
    query_id = 0
    try:
        query_id = max(0, int(float(str(after_query_id).strip())))
    except (TypeError, ValueError):
        query_id = 0
    timestamp = _incremental_time_arg(after_time)
    alias = alias.strip() or "h"
    if timestamp and query_id > 0:
        return (
            f"(TRY_CAST({alias}.start_time AS TIMESTAMP) > TRY_CAST(? AS TIMESTAMP) "
            f"OR (TRY_CAST({alias}.start_time AS TIMESTAMP) = TRY_CAST(? AS TIMESTAMP) "
            f"AND TRY_CAST({alias}.query_id AS BIGINT) > ?) "
            f"OR (TRY_CAST({alias}.start_time AS TIMESTAMP) IS NULL AND TRY_CAST({alias}.query_id AS BIGINT) > ?))",
            [timestamp, timestamp, query_id, query_id],
        )
    if timestamp:
        return f"TRY_CAST({alias}.start_time AS TIMESTAMP) > TRY_CAST(? AS TIMESTAMP)", [timestamp]
    if query_id > 0:
        return f"TRY_CAST({alias}.query_id AS BIGINT) > ?", [query_id]
    return "TRUE", []


def resolve_evidence_targets(args: argparse.Namespace, duck, run) -> None:
    """Resolve phase-2 parent targets once per run (idempotent)."""
    if getattr(args, "target_ids", None) is not None:
        return
    ids = compute_parent_target_ids(
        duck,
        run.snapshot_id,
        args.explain_query_limit,
        floor_basis=getattr(args, "floor_basis", "execution_time"),
        incremental_after_time=args.incremental_after_time if getattr(args, "incremental", False) else None,
        incremental_after_query_id=args.incremental_after_query_id if getattr(args, "incremental", False) else None,
    )
    args.target_ids = ids or None
    if ids:
        print(f"  Parent targets resolved: {len(ids)} representative query id(s) from captured roots")
    else:
        print("  No captured roots available; evidence tables fall back to live threshold selection")


def expected_table_sql_hashes(args: argparse.Namespace, settings: AnalyzerSettings) -> dict[str, str]:
    return {
        table_name: sql_hash(table_sql(args, settings, table_name, include_incremental=False))
        for table_name in LIVE_REFRESH_TABLES
    }


def _replace_capture_preserving_existing(
    duck,
    store: DuckDBStore,
    table_name: str,
    frame: pd.DataFrame,
    run,
) -> None:
    """Never replace a populated snapshot slice with an unexplained zero rows."""
    existing = store.snapshot_row_count(duck, table_name, run.snapshot_id)
    if frame.empty and existing > 0:
        raise RuntimeError(
            f"{table_name} returned zero rows while the current snapshot contains "
            f"{existing:,}. Existing data was preserved; verify permissions and source availability."
        )
    store.replace_table_from_frame(duck, table_name, frame, run)


def resolve_table_databases(
    args: argparse.Namespace,
    settings: AnalyzerSettings,
    user: str,
    password: str,
    jdbc_url: str,
    jars: Iterable[str],
) -> tuple[str, ...]:
    print(
        "Using fixed catalog database scope: "
        f"{', '.join(CATALOG_DATABASE_SCOPE)} "
        "(automatic database discovery disabled by emergency policy)"
    )
    return CATALOG_DATABASE_SCOPE


def reload_database_cache(
    args: argparse.Namespace,
    settings: AnalyzerSettings,
    user: str,
    password: str,
    jdbc_url: str,
    jars: Iterable[str],
) -> list[str]:
    sql = render_database_discovery_sql(args.database_discovery_sql, args.database_min_query_count)
    frame = fetch_frame(args, args.primary_database, user, password, sql, jdbc_url, jars, stage="database discovery")
    discovered = extract_database_names(frame)
    if discovered:
        settings.database_discovery_sql = args.database_discovery_sql
        settings.database_min_query_count = int(args.database_min_query_count)
        update_discovered_databases(
            settings,
            discovered,
            source=f"{args.connection}:{args.primary_database}",
        )
        path = save_settings(settings)
        print(f"Saved table/view databases to {path}: {', '.join(discovered)}")
    else:
        print("Database discovery query returned no database names.")
    return discovered


def capture_view_definitions(
    args: argparse.Namespace,
    duck,
    store: DuckDBStore,
    run,
    table_databases: Iterable[str],
    user: str,
    password: str,
    jdbc_url: str,
    jars: Iterable[str],
) -> None:
    all_view_defs: list[pd.DataFrame] = []
    print("Capturing view definitions across databases")
    for database in table_databases:
        sql = table_sql(args, load_settings(), "view_definitions", include_incremental=False)
        frame = fetch_frame(args, database, user, password, sql, jdbc_url, jars, stage=f"view_definitions [{database}]")
        frame = normalize_view_definitions(frame, database)
        all_view_defs.append(frame)
        print(f"  {database}: {len(frame):,} rows")
    view_defs = pd.concat(all_view_defs, ignore_index=True) if all_view_defs else pd.DataFrame()
    _replace_capture_preserving_existing(duck, store, "view_definitions", view_defs, run)
    store.record_source_sql(
        duck,
        "view_definitions",
        run,
        table_sql(args, load_settings(), "view_definitions", include_incremental=False),
    )


def capture_procedure_definitions(
    args: argparse.Namespace,
    duck,
    store: DuckDBStore,
    run,
    table_databases: Iterable[str],
    user: str,
    password: str,
    jdbc_url: str,
    jars: Iterable[str],
) -> None:
    all_procedure_defs: list[pd.DataFrame] = []
    print("Capturing stored procedure definitions across databases")
    for database in table_databases:
        sql = table_sql(args, load_settings(), "procedure_definitions", include_incremental=False)
        frame = fetch_frame(args, database, user, password, sql, jdbc_url, jars, stage=f"procedure_definitions [{database}]")
        frame = normalize_procedure_definitions(frame, database)
        all_procedure_defs.append(frame)
        print(f"  {database}: {len(frame):,} rows")
    procedure_defs = pd.concat(all_procedure_defs, ignore_index=True) if all_procedure_defs else pd.DataFrame()
    _replace_capture_preserving_existing(duck, store, "procedure_definitions", procedure_defs, run)
    store.record_source_sql(
        duck,
        "procedure_definitions",
        run,
        table_sql(args, load_settings(), "procedure_definitions", include_incremental=False),
    )


def capture_extraction_table(
    args: argparse.Namespace,
    duck,
    store: DuckDBStore,
    run,
    table_name: str,
    user: str,
    password: str,
    jdbc_url: str,
    jars: Iterable[str],
) -> None:
    if table_name not in EXTRACTIONS:
        raise ValueError(f"{table_name} is not a SYS extraction table.")
    settings = load_settings()
    # Execute with the resolved parent ids, but record the id-free template so
    # the saved SQL hash stays stable across runs.
    sql_template = table_sql(args, settings, table_name, include_incremental=False)
    sql_exec = table_sql(args, settings, table_name, target_ids=getattr(args, "target_ids", None))
    frame = fetch_frame(args, args.primary_database, user, password, sql_exec, jdbc_url, jars, stage=table_name)
    if _incremental_append_enabled(args, table_name):
        store.append_table_from_frame(
            duck,
            table_name,
            frame,
            run,
            dedupe_keys=INCREMENTAL_DEDUPE_KEYS.get(table_name),
        )
    else:
        _replace_capture_preserving_existing(duck, store, table_name, frame, run)
    store.record_source_sql(duck, table_name, run, sql_template)
    print(f"  {table_name}: {len(frame):,} rows")


def capture_table_info(
    args: argparse.Namespace,
    duck,
    store: DuckDBStore,
    run,
    table_databases: Iterable[str],
    user: str,
    password: str,
    jdbc_url: str,
    jars: Iterable[str],
) -> None:
    all_table_info: list[pd.DataFrame] = []
    sql = table_sql(args, load_settings(), "svv_table_info_all", include_incremental=False)
    print("Capturing SVV_TABLE_INFO across databases")
    for database in table_databases:
        frame = fetch_frame(args, database, user, password, sql, jdbc_url, jars, stage=f"svv_table_info_all [{database}]")
        frame["source_db"] = database
        all_table_info.append(frame)
        print(f"  {database}: {len(frame):,} rows")
    table_info = pd.concat(all_table_info, ignore_index=True) if all_table_info else pd.DataFrame()
    _replace_capture_preserving_existing(duck, store, "svv_table_info_all", table_info, run)
    store.record_source_sql(duck, "svv_table_info_all", run, sql)


ENTERPRISE_DW_DATABASE = "enterprise_data_warehouse"


def capture_user_roster(
    args: argparse.Namespace,
    duck,
    store: DuckDBStore,
    run,
    user: str,
    password: str,
    jdbc_url: str,
    jars: Iterable[str],
) -> None:
    """Parse the SVV_USER_INFO roster from the producer's enterprise_data_warehouse.

    Persistent: written with replace semantics but the table is in
    duckdb_store.PRESERVED_TABLES, so a normal reload never clears it — only
    an explicit manual truncate does. A zero-row fetch preserves the existing
    roster rather than blanking it.
    """
    from .redshift_queries import USER_ROSTER_SQL
    from .user_roster import build_user_roster

    print(f"Capturing user roster from SVV_USER_INFO on {ENTERPRISE_DW_DATABASE} (producer)")
    frame = fetch_frame(
        args, ENTERPRISE_DW_DATABASE, user, password, USER_ROSTER_SQL, jdbc_url, jars,
        stage=f"user_roster [{ENTERPRISE_DW_DATABASE}]",
    )
    roster = build_user_roster(frame)
    print(f"  parsed {len(roster):,} user(s)")
    _replace_capture_preserving_existing(duck, store, "user_roster", roster, run)
    store.record_source_sql(duck, "user_roster", run, USER_ROSTER_SQL)


def capture_external_tables_catalog(
    args: argparse.Namespace,
    duck,
    store: DuckDBStore,
    run,
    table_databases: Iterable[str],
    user: str,
    password: str,
    jdbc_url: str,
    jars: Iterable[str],
    *,
    statement_timeout_seconds: int = 120,
) -> None:
    """Isolated producer-only SVV_EXTERNAL_TABLES catalog, cycled per database.

    Deliberately separate from the heavy Spectrum-metrics stage. Each database
    is fetched under a bounded statement timeout so an unreachable Glue/Hive
    catalog degrades to an empty slice for that database instead of hanging
    the whole load. A per-database failure is logged and skipped, not fatal.
    """
    frames: list[pd.DataFrame] = []
    schemas, patterns = external_capture_scope()
    catalog_sql = external_catalog_sql(schemas, patterns)
    print("Capturing SVV_EXTERNAL_TABLES catalog across databases (producer only)")
    if schemas or patterns:
        print(f"  scope: schemas={list(schemas) or 'all'} patterns={list(patterns) or 'all'}")
    for database in table_databases:
        try:
            frame = fetch_frame(
                args, database, user, password, catalog_sql, jdbc_url, jars,
                stage=f"external_tables_catalog [{database}]",
                statement_timeout_seconds=statement_timeout_seconds,
            )
        except Exception as exc:
            print(f"  {database}: external catalog unavailable ({exc}); skipped")
            continue
        if frame is None or frame.empty:
            print(f"  {database}: 0 external tables")
            continue
        frame["source_db"] = database
        frames.append(frame)
        print(f"  {database}: {len(frame):,} external table(s)")
    catalog = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    # Zero-row guard preserves an existing catalog if every database came back
    # empty (e.g. a transient Glue outage) rather than blanking it.
    _replace_capture_preserving_existing(duck, store, "external_tables_catalog", catalog, run)
    store.record_source_sql(duck, "external_tables_catalog", run, catalog_sql)


def capture_external_table_info(
    args: argparse.Namespace,
    duck,
    store: DuckDBStore,
    run,
    table_databases: Iterable[str],
    user: str,
    password: str,
    jdbc_url: str,
    jars: Iterable[str],
) -> None:
    frames: list[pd.DataFrame] = []
    timeout_seconds = max(1, int(os.environ.get("REDSHIFT_EXTERNAL_STAGE_TIMEOUT_SECONDS", "600")))
    batch_size = max(1, int(os.environ.get("REDSHIFT_EXTERNAL_QUERY_BATCH_SIZE", "100")))
    source_sql = external_segments_stage_sql(args.days)
    print(
        "Capturing external-table sources independently across databases; "
        f"local DuckDB joins; {timeout_seconds}s per-stage timeout"
    )

    def optional(database: str, sql: str, stage: str) -> pd.DataFrame:
        while True:
            try:
                return fetch_frame(
                    args, database, user, password, sql, jdbc_url, jars,
                    stage=stage, statement_timeout_seconds=timeout_seconds,
                )
            except Exception as exc:
                if _is_external_timeout(exc) and callable(_EXTERNAL_TIMEOUT_DECISION_HOOK):
                    decision = str(_EXTERNAL_TIMEOUT_DECISION_HOOK(stage, str(exc)) or "next").lower()
                    if decision == "continue":
                        print(f"  Retrying {stage} with another 10-minute window")
                        continue
                print(f"  OPTIONAL STAGE SKIPPED: {stage}: {exc}")
                return pd.DataFrame()

    for database in table_databases:
        print(f"  {database}: staging minimal external-table catalog")
        catalog = optional(database, EXTERNAL_CATALOG_STAGE_SQL, f"external catalog [{database}]")
        segments = optional(database, source_sql, f"external scan segments [{database}]")
        if catalog.empty:
            catalog = minimal_external_catalog_from_segments(segments, database)
        query_ids = external_query_ids(segments)
        history = pd.DataFrame({
            "query_id": query_ids,
            "database_name": [database] * len(query_ids),
        })
        step_frames = []
        for offset in range(0, len(query_ids), batch_size):
            batch = query_ids[offset:offset + batch_size]
            step_frames.append(optional(
                database,
                external_steps_stage_sql(batch),
                f"external output metrics [{database}; {offset + 1}-{offset + len(batch)}]",
            ))
        # Partition-key discovery is deliberately the last optional stage.
        columns = optional(
            database,
            EXTERNAL_COLUMN_STATS_STAGE_SQL,
            f"external partition keys [{database}]",
        )
        stages = {
            "svv_external_tables": catalog,
            "external_column_stats": columns,
            "sys_query_history": history,
            "sys_external_query_detail": segments,
            "sys_query_detail": pd.concat(step_frames, ignore_index=True) if step_frames else pd.DataFrame(),
            "sys_external_query_error": pd.DataFrame(),
        }
        frame = assemble_external_table_info(stages)
        frames.append(frame)
        print(f"  {database}: {len(frame):,} external table row(s) assembled locally")
    external_info = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not external_info.empty and "external_table_key" in external_info.columns:
        external_info = external_info.drop_duplicates("external_table_key", keep="first")
    _replace_capture_preserving_existing(duck, store, "external_table_info_all", external_info, run)
    store.record_source_sql(duck, "external_table_info_all", run, source_sql)


def _incremental_append_enabled(args: argparse.Namespace, table_name: str) -> bool:
    return (
        bool(getattr(args, "incremental", False))
        and not bool(getattr(args, "refresh_empty_only", False))
        and table_name in INCREMENTAL_APPEND_TABLES
    )


def refresh_one_table(
    args: argparse.Namespace,
    settings: AnalyzerSettings,
    duck,
    store: DuckDBStore,
    run,
    table_name: str,
    user: str,
    password: str,
    jdbc_url: str,
    jars: Iterable[str],
) -> tuple[str, ...]:
    if table_name in EXTRACTIONS:
        validate_sources(args, args.primary_database, user, password, jdbc_url, jars, include_svv=False)
        if table_name not in ROOT_TABLES and table_name != "user_info":
            resolve_evidence_targets(args, duck, run)
        capture_extraction_table(args, duck, store, run, table_name, user, password, jdbc_url, jars)
        return ()

    table_databases = resolve_table_databases(args, settings, user, password, jdbc_url, jars)
    if table_name == "svv_table_info_all":
        for database in table_databases:
            validate_source_view(args, database, user, password, jdbc_url, jars, "SVV_TABLE_INFO")
        capture_table_info(args, duck, store, run, table_databases, user, password, jdbc_url, jars)
        return tuple(table_databases)
    if table_name == "external_table_info_all":
        # Every external source is optional and independently staged.  Do not
        # hard-fail this focused refresh during preflight; capture reports and
        # skips an unavailable view while preserving whatever stages succeed.
        capture_external_table_info(
            args, duck, store, run, table_databases, user, password, jdbc_url, jars
        )
        return tuple(table_databases)
    if table_name == "view_definitions":
        for database in table_databases:
            validate_source_view(args, database, user, password, jdbc_url, jars, "pg_views")
        capture_view_definitions(args, duck, store, run, table_databases, user, password, jdbc_url, jars)
        return tuple(table_databases)
    if table_name == "procedure_definitions":
        for database in table_databases:
            validate_source_view(args, database, user, password, jdbc_url, jars, "pg_catalog.pg_proc_info")
            validate_source_view(args, database, user, password, jdbc_url, jars, "pg_catalog.pg_namespace")
            validate_source_view(args, database, user, password, jdbc_url, jars, "svv_user_info")
        capture_procedure_definitions(args, duck, store, run, table_databases, user, password, jdbc_url, jars)
        return tuple(table_databases)
    raise ValueError(f"Unsupported refresh table: {table_name}")


def normalize_view_definitions(frame: pd.DataFrame, database: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["database", "schema", "view_name", "source_definition"])

    df = frame.copy()
    rename: dict[str, str] = {}
    for col in df.columns:
        normalized = str(col).strip().lower()
        if normalized == "schemaname":
            rename[col] = "schema"
        elif normalized == "viewname":
            rename[col] = "view_name"
    if rename:
        df = df.rename(columns=rename)

    if "database" not in df.columns:
        df["database"] = database

    part_cols = sorted(
        [col for col in df.columns if re.match(r"definition_part_\d+$", str(col).strip().lower())],
        key=lambda col: int(re.search(r"(\d+)$", str(col)).group(1)),
    )
    if part_cols:
        df["source_definition"] = df[part_cols].apply(
            lambda row: "".join(_view_definition_part(value) for value in row),
            axis=1,
        )
    elif "source_definition" not in df.columns:
        df["source_definition"] = ""

    for col in ("schema", "view_name"):
        if col not in df.columns:
            df[col] = ""

    return df[["database", "schema", "view_name", "source_definition"]]


def normalize_procedure_definitions(frame: pd.DataFrame, database: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(
            columns=[
                "database",
                "schema",
                "procedure_name",
                "procedure_oid",
                "procedure_key",
                "owner_id",
                "owner_name",
                "argument_names",
                "argument_modes",
                "argument_type_oids",
                "all_argument_type_oids",
                "source_length",
                "source_definition",
            ]
        )

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
        df["database"] = database

    part_cols = sorted(
        [col for col in df.columns if re.match(r"definition_part_\d+$", str(col).strip().lower())],
        key=lambda col: int(re.search(r"(\d+)$", str(col)).group(1)),
    )
    if part_cols:
        df["source_definition"] = df[part_cols].apply(
            lambda row: "".join(_view_definition_part(value) for value in row),
            axis=1,
        )
    elif "source_definition" not in df.columns:
        df["source_definition"] = ""
    df["source_definition"] = df["source_definition"].map(_procedure_body_begin_to_end)
    df["source_length"] = df["source_definition"].map(lambda value: len(str(value or "")))

    for col in (
        "schema",
        "procedure_name",
        "procedure_oid",
        "owner_id",
        "owner_name",
        "argument_names",
        "argument_modes",
        "argument_type_oids",
        "all_argument_type_oids",
        "source_length",
    ):
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

    return df[
        [
            "database",
            "schema",
            "procedure_name",
            "procedure_oid",
            "procedure_key",
            "owner_id",
            "owner_name",
            "argument_names",
            "argument_modes",
            "argument_type_oids",
            "all_argument_type_oids",
            "source_length",
            "source_definition",
        ]
    ]


def _view_definition_part(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value)
    return "" if text in {"None", "NULL", "nan"} else text


def _procedure_body_begin_to_end(value: object) -> str:
    text = _view_definition_part(value)
    if not text:
        return ""
    begin = re.search(r"\bbegin\b", text, flags=re.IGNORECASE)
    if not begin:
        return text.strip()
    end_matches = list(re.finditer(r"\bend\b\s*;?", text, flags=re.IGNORECASE))
    end_after_begin = [match for match in end_matches if match.start() >= begin.start()]
    if not end_after_begin:
        return text[begin.start():].strip()
    end = end_after_begin[-1]
    return text[begin.start():end.end()].strip()


def fetch_frame(
    args: argparse.Namespace,
    database: str,
    user: str,
    password: str,
    sql: str,
    jdbc_url: str,
    jars: Iterable[str],
    stage: str = "",
    statement_timeout_seconds: int = 0,
) -> pd.DataFrame:
    if args.connection == "jdbc":
        return fetch_frame_jdbc(
            jdbc_url=format_jdbc_url(jdbc_url, database),
            driver=args.driver,
            jars=jars,
            user=user,
            password=password,
            sql=sql,
            stage=stage,
            statement_timeout_seconds=statement_timeout_seconds,
        )
    return fetch_frame_native(
        host=args.host,
        port=args.port,
        database=database,
        user=user,
        password=password,
        sql=sql,
        stage=stage,
        statement_timeout_seconds=statement_timeout_seconds,
    )


def validate_sources(
    args: argparse.Namespace,
    database: str,
    user: str,
    password: str,
    jdbc_url: str,
    jars: Iterable[str],
    *,
    include_sys: bool = True,
    include_svv: bool = True,
    included_tables: set[str] | None = None,
) -> None:
    def wanted(table_name: str) -> bool:
        return included_tables is None or table_name in included_tables

    view_names = []
    if include_sys and (included_tables is None or any(name in included_tables for name in EXTRACTIONS)):
        view_names.extend(
            [
                "sys_query_detail",
                "sys_query_history",
                "sys_query_explain",
                "sys_query_text",
                "sys_child_query_text",
                "svv_user_info",
            ]
        )
    if include_svv:
        if wanted("svv_table_info_all"):
            view_names.append("SVV_TABLE_INFO")
        # External metadata is an optional final phase.  Its independent stage
        # queries handle unsupported/unavailable sources without allowing a
        # preflight failure to block normal workload and catalog capture.
        if wanted("view_definitions"):
            view_names.append("pg_views")
        if wanted("procedure_definitions"):
            view_names.append("pg_catalog.pg_proc_info")
            view_names.append("pg_catalog.pg_namespace")
            view_names.append("svv_user_info")
    for view_name in dict.fromkeys(view_names):
        validate_source_view(args, database, user, password, jdbc_url, jars, view_name)


def validate_source_view(
    args: argparse.Namespace,
    database: str,
    user: str,
    password: str,
    jdbc_url: str,
    jars: Iterable[str],
    view_name: str,
) -> None:
    actual_cols = fetch_columns(args, database, user, password, jdbc_url, jars, view_name)
    normalized = {c.lower() for c in actual_cols}
    required = {c.lower() for c in SOURCE_REQUIREMENTS[view_name]}
    missing = sorted(required - normalized)
    if missing:
        available = ", ".join(actual_cols[:80])
        raise RuntimeError(
            f"{database}.{view_name} is missing required column(s): {missing}. "
            f"Available columns from live metadata: {available}"
        )
    print(f"  {database}.{view_name}: {len(actual_cols)} columns ok")


def fetch_columns(
    args: argparse.Namespace,
    database: str,
    user: str,
    password: str,
    jdbc_url: str,
    jars: Iterable[str],
    view_name: str,
) -> list[str]:
    sql = f"SELECT * FROM {view_name} WHERE 1 = 0"
    if args.connection == "jdbc":
        return fetch_columns_jdbc(
            jdbc_url=format_jdbc_url(jdbc_url, database),
            driver=args.driver,
            jars=jars,
            user=user,
            password=password,
            sql=sql,
        )
    return fetch_columns_native(
        host=args.host,
        port=args.port,
        database=database,
        user=user,
        password=password,
        sql=sql,
    )


def fetch_columns_jdbc(
    *,
    jdbc_url: str,
    driver: str,
    jars: Iterable[str],
    user: str,
    password: str,
    sql: str,
) -> list[str]:
    try:
        import jaydebeapi
    except ImportError as exc:
        raise RuntimeError(
            "JDBC mode requires JayDeBeApi and JPype1. Use --connection native "
            "when direct Redshift socket access is allowed."
        ) from exc
    conn = jaydebeapi.connect(driver, jdbc_url, [user, password], list(jars))
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            return [str(d[0]) for d in cur.description]
        finally:
            cur.close()
    finally:
        conn.close()


def fetch_columns_native(
    *,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    sql: str,
) -> list[str]:
    try:
        import redshift_connector
    except ImportError as exc:
        raise RuntimeError("Native mode requires redshift-connector from requirements.txt.") from exc
    conn = redshift_connector.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            return [str(d[0]) for d in cur.description]
        finally:
            cur.close()
    finally:
        conn.close()


def fetch_frame_jdbc(
    *,
    jdbc_url: str,
    driver: str,
    jars: Iterable[str],
    user: str,
    password: str,
    sql: str,
    stage: str = "",
    statement_timeout_seconds: int = 0,
) -> pd.DataFrame:
    try:
        import jaydebeapi
    except ImportError as exc:
        raise RuntimeError(
            "JDBC mode requires JayDeBeApi and JPype1. On Windows ARM/Python 3.13, "
            "JPype may require a compatible wheel or local build tools. Use "
            "--connection native when direct Redshift socket access is allowed."
        ) from exc
    conn = jaydebeapi.connect(driver, jdbc_url, [user, password], list(jars))
    try:
        cur = conn.cursor()
        try:
            if statement_timeout_seconds > 0:
                cur.execute(f"SET statement_timeout TO {int(statement_timeout_seconds) * 1000}")
            cur.execute(sql)
            return _frame_from_cursor(cur, stage)
        finally:
            cur.close()
    finally:
        conn.close()


def fetch_frame_native(
    *,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    sql: str,
    stage: str = "",
    statement_timeout_seconds: int = 0,
) -> pd.DataFrame:
    try:
        import redshift_connector
    except ImportError as exc:
        raise RuntimeError("Native mode requires redshift-connector from requirements.txt.") from exc
    conn = redshift_connector.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )
    try:
        cur = conn.cursor()
        try:
            if statement_timeout_seconds > 0:
                cur.execute(f"SET statement_timeout TO {int(statement_timeout_seconds) * 1000}")
            cur.execute(sql)
            return _frame_from_cursor(cur, stage)
        finally:
            cur.close()
    finally:
        conn.close()


def split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def included_refresh_tables(value: str | None) -> set[str]:
    selected = set(split_csv(value))
    if not selected:
        return set(LIVE_REFRESH_TABLES)
    valid = set(LIVE_REFRESH_TABLES)
    invalid = sorted(selected - valid)
    if invalid:
        raise SystemExit(
            f"--include-tables has unknown table(s): {', '.join(invalid)}. "
            f"Valid tables: {', '.join(LIVE_REFRESH_TABLES)}"
        )
    return selected


def format_jdbc_url(url: str, database: str) -> str:
    if "{database}" in url:
        return url.format(database=database)
    # Handles jdbc:redshift://host:5439/dev?ssl=true by replacing only the path
    # database. If the URL is non-standard, leave it unchanged.
    match = re.match(r"^(jdbc:redshift://[^/]+/)([^?;]+)(.*)$", url)
    if match:
        return f"{match.group(1)}{database}{match.group(3)}"
    return url


if __name__ == "__main__":
    raise SystemExit(main())
