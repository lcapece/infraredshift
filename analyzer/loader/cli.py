"""Command-line interface for the one Infraredshift loader engine."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from .engine import LoaderAlreadyRunningError, LoaderEngine, LoaderRequest


def _redact(value: object) -> str:
    try:
        from analyzer.secrets_store import redact_sensitive_text

        return redact_sensitive_text(value)
    except ImportError:
        return str(value)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="infraredshift_loader",
        description=(
            "Recoverable Infraredshift Redshift-to-DuckDB loader. The same command "
            "is used by the GUI and Windows Task Scheduler."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    refresh = subparsers.add_parser("refresh", help="Resume or start a staged refresh")
    refresh.add_argument("--duckdb-path", default=None)
    refresh.add_argument("--days", type=float, default=7.0)
    # None = per-cluster floors (JSON FLOOR_SECONDS, else producer 300s /
    # consumers 30s). An explicit value applies load-wide beneath the JSON.
    refresh.add_argument("--floor-seconds", type=float, default=None)
    refresh.add_argument("--floor-basis", choices=("execution_time", "elapsed_time"), default="execution_time")
    refresh.add_argument("--lock-wait-seconds", type=float, default=600.0)
    refresh.add_argument("--fresh", action="store_true", help="Discard resumable checkpoints and start a fresh staging snapshot")
    refresh.add_argument("--promote", action="store_true", help="Promote only after the full selected capture succeeds")
    refresh.add_argument(
        "--skip-external",
        action="store_true",
        help=(
            "Skip legacy external performance telemetry. Producer "
            "SVV_EXTERNAL_COLUMNS metadata remains part of the normal load."
        ),
    )
    refresh.add_argument("--no-backup", action="store_true", help="Do not back up before promotion (not recommended)")
    refresh.add_argument(
        "--external-timeout-action", choices=("skip", "retry", "ask"), default="skip",
        help="After an optional external-stage timeout: skip, retry, or ask over stdin",
    )
    refresh.add_argument("--json-events", action="store_true", help="Emit parseable INFRAREDSHIFT_EVENT progress lines")
    refresh.add_argument(
        "--table", dest="include_tables", action="append", default=None,
        help="Refresh only this DuckDB table (repeat for multiple tables)",
    )

    promote = subparsers.add_parser("promote", help="Validate and promote an already-staged snapshot")
    promote.add_argument("--duckdb-path", default=None)
    promote.add_argument("--lock-wait-seconds", type=float, default=600.0)
    promote.add_argument("--no-backup", action="store_true", help="Do not back up before promotion (not recommended)")
    promote.add_argument("--json-events", action="store_true", help="Emit parseable INFRAREDSHIFT_EVENT progress lines")

    status = subparsers.add_parser("status", help="Show durable staging/checkpoint status")
    status.add_argument("--duckdb-path", default=None)
    status.add_argument("--lock-wait-seconds", type=float, default=30.0)

    external = subparsers.add_parser(
        "external-catalog", help="Refresh only the producer's SVV_EXTERNAL_TABLES catalog"
    )
    external.add_argument("--duckdb-path", default=None)
    external.add_argument("--timeout-seconds", type=int, default=120)
    external.add_argument("--json-events", action="store_true")

    roster = subparsers.add_parser(
        "user-roster", help="Refresh the persistent parsed PG_USER roster (producer)"
    )
    roster.add_argument("--duckdb-path", default=None)
    roster.add_argument("--json-events", action="store_true")

    roster_clear = subparsers.add_parser(
        "clear-user-roster", help="Manually clear the persistent user roster"
    )
    roster_clear.add_argument("--duckdb-path", default=None)
    roster_clear.add_argument("--json-events", action="store_true")
    return parser


def _resolve_path(value: str | None) -> Path:
    if value:
        return Path(value)
    configured = os.environ.get("REDSHIFT_DUCKDB_PATH")
    if configured:
        return Path(configured)
    import runner

    return runner.resolve_default_duckdb_path()


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    path = _resolve_path(args.duckdb_path)
    if args.command == "status":
        try:
            return LoaderEngine().status(path, args.lock_wait_seconds)
        except BaseException as exc:
            print(f"Infraredshift loader status failed: {_redact(exc)}", file=sys.stderr)
            return int(exc.code) if isinstance(exc, SystemExit) and isinstance(exc.code, int) else 1

    def sink(event) -> None:
        if args.json_events:
            print("INFRAREDSHIFT_EVENT " + event.to_json(), flush=True)

    if args.command == "external-catalog":
        try:
            return LoaderEngine(sink).refresh_external_catalog(
                path, timeout_seconds=args.timeout_seconds
            )
        except LoaderAlreadyRunningError as exc:
            print(f"Infraredshift loader is already running: {_redact(exc)}", file=sys.stderr)
            return 3
        except BaseException as exc:
            print(f"Infraredshift external catalog refresh failed: {_redact(exc)}", file=sys.stderr)
            return int(exc.code) if isinstance(exc, SystemExit) and isinstance(exc.code, int) else 1

    if args.command == "user-roster":
        try:
            return LoaderEngine(sink).refresh_user_roster(path)
        except LoaderAlreadyRunningError as exc:
            print(f"Infraredshift loader is already running: {_redact(exc)}", file=sys.stderr)
            return 3
        except BaseException as exc:
            print(f"Infraredshift user roster refresh failed: {_redact(exc)}", file=sys.stderr)
            return int(exc.code) if isinstance(exc, SystemExit) and isinstance(exc.code, int) else 1

    if args.command == "clear-user-roster":
        try:
            return LoaderEngine(sink).clear_user_roster(path)
        except BaseException as exc:
            print(f"Infraredshift user roster clear failed: {_redact(exc)}", file=sys.stderr)
            return int(exc.code) if isinstance(exc, SystemExit) and isinstance(exc.code, int) else 1

    if args.command == "promote":
        try:
            return LoaderEngine(sink).promote(
                path,
                backup_before_promote=not args.no_backup,
                lock_wait_seconds=args.lock_wait_seconds,
            )
        except LoaderAlreadyRunningError as exc:
            print(f"Infraredshift loader is already running: {_redact(exc)}", file=sys.stderr)
            return 3
        except BaseException as exc:
            print(f"Infraredshift promotion failed: {_redact(exc)}", file=sys.stderr)
            return int(exc.code) if isinstance(exc, SystemExit) and isinstance(exc.code, int) else 1

    request = LoaderRequest(
        duckdb_path=path,
        days=args.days,
        floor_seconds=args.floor_seconds,
        floor_basis=args.floor_basis,
        resume=not args.fresh,
        promote=args.promote,
        include_external=not args.skip_external,
        backup_before_promote=not args.no_backup,
        lock_wait_seconds=args.lock_wait_seconds,
        external_timeout_action=args.external_timeout_action,
        include_tables=tuple(args.include_tables) if args.include_tables else None,
    )

    def timeout_decider(stage: str, _error: str) -> str:
        print(
            f"External metadata timed out at {stage}. Reply RETRY or SKIP:",
            flush=True,
        )
        try:
            return input().strip()
        except EOFError:
            return "skip"
    try:
        decider = timeout_decider if args.external_timeout_action == "ask" else None
        return LoaderEngine(sink, timeout_decider=decider).refresh(request)
    except LoaderAlreadyRunningError as exc:
        print(f"Infraredshift loader is already running: {_redact(exc)}", file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        print("Infraredshift loader interrupted. Completed checkpoints are preserved; rerun to resume.", file=sys.stderr)
        return 130
    except BaseException as exc:
        print(f"Infraredshift loader failed: {_redact(exc)}", file=sys.stderr)
        return int(exc.code) if isinstance(exc, SystemExit) and isinstance(exc.code, int) else 1
