"""Headless, recoverable orchestration for the Infraredshift Redshift loader.

This module deliberately contains no capture SQL and no credentials.  It
provides the stable process boundary around ``runner`` so a GUI or scheduler
can start exactly the same loader without importing Redshift work onto its UI
thread.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, TextIO


EventSink = Callable[["LoaderEvent"], None]
TimeoutDecider = Callable[[str, str], str]


@dataclass(frozen=True)
class LoaderRequest:
    """Non-secret settings for one loader execution."""

    duckdb_path: Path
    days: float = 7.0
    # None = per-cluster floors (JSON FLOOR_SECONDS, else producer 300s /
    # consumers 30s). An explicit value applies load-wide beneath the JSON.
    floor_seconds: float | None = None
    floor_basis: str = "execution_time"
    resume: bool = True
    promote: bool = False
    # Controls only the legacy external performance-telemetry aggregate
    # (external_table_info_all). Producer SVV_EXTERNAL_COLUMNS metadata is a
    # required normal dataset and is not disabled by this compatibility flag.
    include_external: bool = True
    backup_before_promote: bool = True
    lock_wait_seconds: float = 600.0
    external_timeout_action: str = "skip"
    include_tables: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "duckdb_path", Path(self.duckdb_path).expanduser().resolve())
        if self.days <= 0:
            raise ValueError("days must be greater than zero")
        if self.floor_seconds is not None and self.floor_seconds < 0:
            raise ValueError("floor_seconds cannot be negative")
        if self.floor_basis not in {"execution_time", "elapsed_time"}:
            raise ValueError("floor_basis must be execution_time or elapsed_time")
        if self.lock_wait_seconds < 0:
            raise ValueError("lock_wait_seconds cannot be negative")
        if self.external_timeout_action not in {"skip", "retry", "ask"}:
            raise ValueError("external_timeout_action must be skip, retry, or ask")
        if self.include_tables is not None:
            normalized = tuple(dict.fromkeys(
                str(table).strip() for table in self.include_tables if str(table).strip()
            ))
            if not normalized:
                raise ValueError("include_tables cannot be empty")
            object.__setattr__(self, "include_tables", normalized)


@dataclass(frozen=True)
class LoaderEvent:
    event: str
    message: str
    timestamp: str
    namespace_id: str = ""
    table_name: str = ""
    source_rows: int = 0
    duckdb_rows: int = 0
    completed: int = 0
    total: int = 0

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)


class LoaderAlreadyRunningError(RuntimeError):
    """Raised when another loader owns the target warehouse lock."""


class _ProcessLock:
    """OS-managed, crash-safe single-instance lock for one DuckDB target."""

    def __init__(self, duckdb_path: Path) -> None:
        self.duckdb_path = duckdb_path
        self.path = duckdb_path.parent / f".{duckdb_path.name}.infraredshift-loader.lock"
        self._handle: TextIO | None = None

    def __enter__(self) -> "_ProcessLock":
        # Escape hatch: INFRAREDSHIFT_NO_LOCK=1 disables the single-instance guard.
        # Safe for the per-cluster loaders (each writes its own file), and a
        # release valve if a crashed run ever leaves a stuck lock.
        if str(os.environ.get("INFRAREDSHIFT_NO_LOCK", "")).strip().lower() in {"1", "true", "yes", "on"}:
            self._handle = None
            return self
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            _lock_one_byte(handle)
        except Exception:
            handle.close()
            raise
        self._handle = handle
        metadata = {
            "pid": os.getpid(),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "duckdb_path": str(self.duckdb_path),
        }
        handle.seek(1)
        handle.truncate()
        handle.write(json.dumps(metadata, sort_keys=True).encode("utf-8"))
        handle.flush()
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        if self._handle is None:
            return
        try:
            self._handle.seek(0)
            _unlock_one_byte(self._handle)
        finally:
            self._handle.close()
            self._handle = None


def _lock_one_byte(handle: TextIO) -> None:
    try:
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        raise LoaderAlreadyRunningError(
            "Another Infraredshift loader is already running for this DuckDB file. "
            "Use loader status or wait for that load to finish."
        ) from exc


def _unlock_one_byte(handle: TextIO) -> None:
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _redact(value: object) -> str:
    try:
        from analyzer.secrets_store import redact_sensitive_text

        return redact_sensitive_text(value)
    except ImportError:
        return str(value)


def _unlock_scheduled_credentials() -> None:
    """Load same-Windows-user DPAPI credentials into the in-process session."""
    try:
        from analyzer.secrets_store import unlock_scheduled_secrets_session
    except ImportError:
        return
    try:
        unlock_scheduled_secrets_session()
    except FileNotFoundError:
        # Legacy standalone deployments may intentionally supply credentials
        # through their already-established process environment.
        return


class LoaderEngine:
    """Run the existing staged loader under a recoverable process contract."""

    def __init__(
        self,
        event_sink: EventSink | None = None,
        timeout_decider: TimeoutDecider | None = None,
    ) -> None:
        self._event_sink = event_sink
        self._timeout_decider = timeout_decider

    def _emit(self, event: str, message: str, **values: object) -> None:
        payload = LoaderEvent(
            event=event,
            message=_redact(message),
            timestamp=_utc_now(),
            namespace_id=str(values.get("namespace_id") or ""),
            table_name=str(values.get("table_name") or ""),
            source_rows=int(values.get("source_rows") or 0),
            duckdb_rows=int(values.get("duckdb_rows") or 0),
            completed=int(values.get("completed") or 0),
            total=int(values.get("total") or 0),
        )
        if self._event_sink is not None:
            self._event_sink(payload)

    @staticmethod
    def _is_fully_loaded(duckdb_path: Path, lock_wait_seconds: float) -> bool:
        """True only when the durable state says the last load fully completed.

        The one-button loader records-and-continues on failures and only flips
        status to "loaded" on a fully clean run; a partial load stays "loading".
        Promotion must gate on this so a partial load is never swapped live.
        """
        import runner

        con = runner.open_duck(duckdb_path, lock_wait_seconds)
        try:
            return runner.read_state(con).get("status") == "loaded"
        finally:
            con.close()

    def refresh(self, request: LoaderRequest) -> int:
        """Stage a refresh and optionally promote it after successful capture."""
        import runner

        args = argparse.Namespace(
            duckdb_path=str(request.duckdb_path),
            days=request.days,
            floor_seconds=request.floor_seconds,
            floor_basis=request.floor_basis,
            resume=request.resume,
            lock_wait_seconds=request.lock_wait_seconds,
            no_backup=not request.backup_before_promote,
            include_external=request.include_external,
            swap=False,
            status=False,
            backup_only=False,
            include_tables=request.include_tables,
        )
        with _ProcessLock(request.duckdb_path):
            self._emit("started", "Infraredshift loader started")
            runner._load_dotenv_if_present()
            _unlock_scheduled_credentials()
            problems = runner.sense_environment(args)
            if problems:
                message = "Environment is not ready: " + "; ".join(_redact(p) for p in problems)
                self._emit("failed", message)
                raise RuntimeError(message)
            configs = runner.build_configs(args)

            def timeout_decision(stage: str, error: str) -> str:
                self._emit(
                    "external_timeout", str(error), table_name=str(stage),
                )
                action = request.external_timeout_action
                if action == "retry":
                    return "continue"
                if action == "ask" and self._timeout_decider is not None:
                    answer = str(self._timeout_decider(str(stage), str(error)) or "next")
                    return "continue" if answer.strip().lower() in {"continue", "retry", "r", "c"} else "next"
                return "next"

            for config in configs:
                config.timeout_decision_callback = timeout_decision

            def progress(namespace_id, table_name, source_rows, duckdb_rows, completed, total, status):
                self._emit(
                    "progress", str(status), namespace_id=namespace_id,
                    table_name=table_name, source_rows=source_rows,
                    duckdb_rows=duckdb_rows, completed=completed, total=total,
                )

            runner.set_progress_hook(progress)
            try:
                # Always use the namespaced implementation, even for one cluster.
                # This keeps GUI, scheduled, producer-only and multi-cluster loads
                # on one checkpoint format and one execution path.
                result = int(runner.run_multi_load(args, configs) or 0)
            except BaseException as exc:
                self._emit("failed", str(exc) or exc.__class__.__name__)
                raise
            finally:
                runner.set_progress_hook(None)
            if result:
                self._emit("failed", f"Loader returned exit code {result}")
                return result
            fully_loaded = self._is_fully_loaded(
                request.duckdb_path, args.lock_wait_seconds,
            )
            if not fully_loaded:
                self._emit(
                    "partial",
                    "The load finished with skipped or incomplete items. "
                    "Live data is untouched; resume the safe load to finish staging.",
                )
                return 0
            self._emit("staged", "All selected datasets are safely staged")
            if request.promote:
                # A partial (non-halting) load leaves durable status "loading";
                # only a fully clean load flips it to "loaded". Never let the
                # swap be the thing that refuses — that would surface as a crash
                # at the end of an otherwise-graceful run. Check first and skip
                # promotion cleanly so a partial load exits 0 with a clear note.
                self._emit("promoting", "Valid staged snapshot is being promoted")
                try:
                    result = int(runner.run_swap(args) or 0)
                except BaseException as exc:
                    self._emit("failed", str(exc) or exc.__class__.__name__)
                    raise
                if result:
                    self._emit("failed", f"Promotion returned exit code {result}")
                    return result
                self._emit("completed", "Refresh and promotion completed")
            else:
                self._emit("completed", "Refresh completed; staged data awaits promotion")
            return 0

    def promote(
        self,
        duckdb_path: Path,
        *,
        backup_before_promote: bool = True,
        lock_wait_seconds: float = 600.0,
    ) -> int:
        """Validate and promote a previously completed staged snapshot."""
        import runner

        target = Path(duckdb_path).expanduser().resolve()
        args = argparse.Namespace(
            duckdb_path=str(target),
            lock_wait_seconds=lock_wait_seconds,
            no_backup=not backup_before_promote,
        )
        with _ProcessLock(target):
            self._emit("promoting", "Validating the staged snapshot before promotion")
            con = runner.open_duck(target, lock_wait_seconds)
            try:
                state = runner.read_state(con)
                if state.get("status") not in {"loaded", "loading"}:
                    raise RuntimeError(
                        "No completed staged load is ready to promote. Resume the loader first."
                    )
                try:
                    failure_count = int(state.get("failure_count", "0") or 0)
                except ValueError:
                    failure_count = 0
                if failure_count:
                    raise RuntimeError(
                        f"The staged load has {failure_count} incomplete item(s). "
                        "Resume the safe load before promotion."
                    )
                selected = tuple(
                    value.strip()
                    for value in str(state.get("selected_tables") or "").split(",")
                    if value.strip()
                )
                existing = {
                    str(row[0]).lower()
                    for row in con.execute(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_type = 'BASE TABLE'"
                    ).fetchall()
                }
                ready = selected or tuple(
                    table for table in runner.LIVE_REFRESH_TABLES
                    if runner.tmp_name(table).lower() in existing
                )
                missing = [
                    table for table in ready
                    if runner.tmp_name(table).lower() not in existing
                ]
                if not ready or missing:
                    detail = ", ".join(f"{table}_tmp" for table in missing) or "no staging tables"
                    raise RuntimeError(f"Staged load validation failed: {detail}")
            except BaseException as exc:
                self._emit("failed", str(exc) or exc.__class__.__name__)
                raise
            finally:
                con.close()
            try:
                result = int(runner.run_swap(args) or 0)
            except BaseException as exc:
                self._emit("failed", str(exc) or exc.__class__.__name__)
                raise
            if result:
                self._emit("failed", f"Promotion returned exit code {result}")
                return result
            self._emit("completed", "Staged snapshot promoted successfully")
            return 0

    def status(self, duckdb_path: Path, lock_wait_seconds: float = 30.0) -> int:
        """Print the runner's durable checkpoint status without taking the load lock."""
        import runner

        args = argparse.Namespace(
            duckdb_path=str(Path(duckdb_path).expanduser().resolve()),
            lock_wait_seconds=lock_wait_seconds,
        )
        return int(runner.run_status(args) or 0)

    def refresh_user_roster(self, duckdb_path: Path) -> int:
        """Producer-only PG_USER roster (enterprise_data_warehouse). Persistent."""
        from analyzer import ingest_redshift

        target = Path(duckdb_path).expanduser().resolve()
        with _ProcessLock(target):
            self._emit("started", "User roster refresh started (producer only)")
            argv = ["--duckdb-path", str(target), "--refresh-user-roster"]
            try:
                result = int(ingest_redshift.main(argv) or 0)
            except BaseException as exc:
                self._emit("failed", str(exc) or exc.__class__.__name__)
                raise
            if result:
                self._emit("failed", f"User roster refresh returned exit code {result}")
                return result
            self._emit("completed", "User roster refreshed")
            return 0

    def clear_user_roster(self, duckdb_path: Path, lock_wait_seconds: float = 60.0) -> int:
        """Manually clear the persistent user roster (the only thing that wipes it)."""
        import runner

        from ..duckdb_store import quote_ident

        target = Path(duckdb_path).expanduser().resolve()
        with _ProcessLock(target):
            self._emit("started", "Clearing user roster")
            con = runner.open_duck(target, lock_wait_seconds)
            try:
                con.execute(
                    "CREATE TABLE IF NOT EXISTS user_roster (user_id BIGINT, user_name VARCHAR)"
                )
                con.execute(f"DELETE FROM {quote_ident('user_roster')}")
            finally:
                con.close()
            self._emit("completed", "User roster cleared")
            return 0

    def refresh_external_catalog(
        self,
        duckdb_path: Path,
        *,
        timeout_seconds: int = 120,
    ) -> int:
        """Isolated, producer-only SVV_EXTERNAL_TABLES catalog refresh.

        Runs directly through analyzer.ingest_redshift (NOT the multi-cluster
        staging path), so it only touches the producer and cannot disturb the
        main workload snapshot. Each database is fetched under a bounded
        statement timeout.
        """
        from analyzer import ingest_redshift

        target = Path(duckdb_path).expanduser().resolve()
        with _ProcessLock(target):
            self._emit("started", "External-table catalog refresh started (producer only)")
            argv = [
                "--duckdb-path", str(target),
                "--refresh-external-catalog",
                "--external-catalog-timeout", str(int(timeout_seconds)),
            ]
            try:
                result = int(ingest_redshift.main(argv) or 0)
            except BaseException as exc:
                self._emit("failed", str(exc) or exc.__class__.__name__)
                raise
            if result:
                self._emit("failed", f"External catalog refresh returned exit code {result}")
                return result
            self._emit("completed", "External-table catalog refreshed")
            return 0


def loader_script_path() -> Path:
    return Path(__file__).resolve().parents[2] / "infraredshift_loader.py"


def loader_gui_script_path() -> Path:
    """Return the standalone desktop-loader launcher for source installs."""
    return Path(__file__).resolve().parents[2] / "infraredshift_loader_gui.py"


def build_loader_gui_command(
    duckdb_path: Path | str | None = None,
    *,
    python_executable: str | None = None,
) -> list[str]:
    """Return a detached-safe command that opens the standalone loader window."""
    executable = python_executable or sys.executable
    launch_path = str(os.environ.get("REDSHIFT_ANALYZER_LAUNCH_PATH") or "").strip()
    command: list[str] = []
    if launch_path:
        launcher = Path(launch_path).expanduser()
        if launcher.is_file():
            command = [executable, str(launcher.resolve()), "--loader-gui"]
    if not command:
        script = loader_gui_script_path()
        command = [executable, str(script)] if script.is_file() else [executable, "-m", "analyzer.loader.gui"]
    if duckdb_path:
        command.extend(("--duckdb-path", str(Path(duckdb_path).expanduser().resolve())))
    return command


def _loader_entry_command(python_executable: str | None = None) -> list[str]:
    """Return a durable loader entry point for source and single-file installs."""
    executable = python_executable or sys.executable
    launch_path = str(os.environ.get("REDSHIFT_ANALYZER_LAUNCH_PATH") or "").strip()
    if launch_path:
        launcher = Path(launch_path).expanduser()
        if launcher.is_file():
            return [executable, str(launcher.resolve()), "--loader"]
    script = loader_script_path()
    if script.is_file():
        return [executable, str(script)]
    # Development/package fallback. The caller's Python installation must be
    # able to import analyzer, which is true for installed packages.
    return [executable, "-m", "analyzer.loader.cli"]


def build_loader_command(
    request: LoaderRequest,
    *,
    python_executable: str | None = None,
    json_events: bool = False,
) -> list[str]:
    """Return an argument-list command safe for QProcess and Task Scheduler."""
    command = [*_loader_entry_command(python_executable), "refresh"]
    command.extend(("--duckdb-path", str(request.duckdb_path)))
    command.extend(("--days", str(request.days)))
    if request.floor_seconds is not None:
        command.extend(("--floor-seconds", str(request.floor_seconds)))
    command.extend(("--floor-basis", request.floor_basis))
    command.extend(("--lock-wait-seconds", str(request.lock_wait_seconds)))
    if not request.resume:
        command.append("--fresh")
    if request.promote:
        command.append("--promote")
    if not request.include_external:
        command.append("--skip-external")
    if not request.backup_before_promote:
        command.append("--no-backup")
    command.extend(("--external-timeout-action", request.external_timeout_action))
    for table_name in request.include_tables or ():
        command.extend(("--table", table_name))
    if json_events:
        command.append("--json-events")
    return command


def build_promote_command(
    duckdb_path: Path,
    *,
    python_executable: str | None = None,
    lock_wait_seconds: float = 600.0,
    backup_before_promote: bool = True,
    json_events: bool = False,
) -> list[str]:
    """Return the out-of-process command for promoting existing staging."""
    command = [
        *_loader_entry_command(python_executable),
        "promote",
        "--duckdb-path", str(Path(duckdb_path).expanduser().resolve()),
        "--lock-wait-seconds", str(lock_wait_seconds),
    ]
    if not backup_before_promote:
        command.append("--no-backup")
    if json_events:
        command.append("--json-events")
    return command


def build_external_catalog_command(
    duckdb_path: Path,
    *,
    python_executable: str | None = None,
    timeout_seconds: int = 120,
    json_events: bool = False,
) -> list[str]:
    """Return the out-of-process command for the isolated external-catalog refresh."""
    command = [
        *_loader_entry_command(python_executable),
        "external-catalog",
        "--duckdb-path", str(Path(duckdb_path).expanduser().resolve()),
        "--timeout-seconds", str(int(timeout_seconds)),
    ]
    if json_events:
        command.append("--json-events")
    return command


def printable_loader_command(request: LoaderRequest) -> str:
    command = build_loader_command(request)
    return subprocess_list2cmdline(command) if os.name == "nt" else shlex.join(command)


def subprocess_list2cmdline(command: list[str]) -> str:
    # Import locally to keep the loader engine's import surface minimal.
    import subprocess

    return subprocess.list2cmdline(command)
