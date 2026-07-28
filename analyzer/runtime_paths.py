"""Deterministic runtime locations for installed and single-file Infraredshift.

No application path may depend on the process working directory.  Corporate
launchers are frequently started from shortcuts, schedulers, or file-association
handlers whose working directory is unrelated to the application.  This module
keeps install/configuration locations separate from per-user state and data,
while retaining the legacy locations so existing installations do not appear
to lose their settings or DuckDB warehouse after an upgrade.
"""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import sys
from typing import Iterable

from .brand import (
    APP_STATE_FOLDER,
    LEGACY_APP_STATE_FOLDERS,
    LEGACY_DATA_PARTS,
    PORTABLE_PROFILE_FILENAME,
    PRODUCT_NAME,
)


def _expanded_path(value: str | os.PathLike, *, relative_to: Path) -> Path:
    """Return an absolute normalized path without requiring it to exist."""
    raw = os.path.expandvars(os.path.expanduser(str(value).strip()))
    path = Path(raw)
    if not path.is_absolute():
        path = relative_to / path
    return path.resolve(strict=False)


def installation_dir(environment: os._Environ[str] | dict[str, str] | None = None) -> Path:
    """Directory containing the user-visible launcher/application package."""
    env = environment if environment is not None else os.environ
    launch_dir = str(env.get("REDSHIFT_ANALYZER_LAUNCH_DIR") or "").strip()
    if launch_dir:
        # A relative launcher override is anchored to the source installation,
        # never to whichever directory a shortcut happened to start in.
        source_root = Path(__file__).resolve().parents[1]
        return _expanded_path(launch_dir, relative_to=source_root)

    launch_path = str(env.get("REDSHIFT_ANALYZER_LAUNCH_PATH") or "").strip()
    if launch_path:
        source_root = Path(__file__).resolve().parents[1]
        return _expanded_path(launch_path, relative_to=source_root).parent

    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def _environment_path(
    name: str,
    environment: os._Environ[str] | dict[str, str],
    *,
    relative_to: Path,
) -> Path | None:
    value = str(environment.get(name) or "").strip()
    return _expanded_path(value, relative_to=relative_to) if value else None


@dataclass(frozen=True)
class RuntimePaths:
    install_dir: Path
    state_dir: Path
    config_dir: Path
    data_dir: Path
    cache_dir: Path
    logs_dir: Path
    support_dir: Path
    settings_file: Path
    auth_file: Path
    duckdb_file: Path
    portable_profile_file: Path

    def writable_directories(self) -> tuple[Path, ...]:
        """Per-user directories safe to create during bootstrap."""
        return tuple(
            dict.fromkeys(
                (
                    self.state_dir,
                    self.config_dir,
                    self.data_dir,
                    self.cache_dir,
                    self.logs_dir,
                    self.support_dir,
                    self.duckdb_file.parent,
                )
            )
        )


def resolve_runtime_paths(
    environment: os._Environ[str] | dict[str, str] | None = None,
) -> RuntimePaths:
    """Resolve all runtime paths from one stable precedence model.

    Explicit paths may be relative for a portable team package, but relative
    values are always anchored to the installation directory.  They therefore
    behave identically from Explorer, a shortcut, cmd.exe, or Task Scheduler.
    """
    env = environment if environment is not None else os.environ
    install = installation_dir(env)

    analyzer_home = _environment_path("REDSHIFT_ANALYZER_HOME", env, relative_to=install)
    if analyzer_home is not None:
        state = analyzer_home
    else:
        local_app = _environment_path("LOCALAPPDATA", env, relative_to=install)
        if local_app is not None:
            # Prefer the Infraredshift folder; fall back to prior product folders so
            # an upgrade does not orphan settings.json / auth.json / .secrets.
            preferred = local_app / APP_STATE_FOLDER
            if preferred.exists():
                state = preferred
            else:
                legacy_hit = next(
                    (local_app / name for name in LEGACY_APP_STATE_FOLDERS if (local_app / name).exists()),
                    None,
                )
                state = legacy_hit or preferred
        else:
            home_state = Path.home().resolve() / f".{PRODUCT_NAME.lower()}"
            legacy_home = Path.home().resolve() / ".redshift-query-anatomy"
            state = home_state if home_state.exists() or not legacy_home.exists() else legacy_home

    config = _environment_path("REDSHIFT_ANALYZER_CONFIG_DIR", env, relative_to=install) or state
    cache = _environment_path("REDSHIFT_ANALYZER_CACHE_DIR", env, relative_to=install) or state / "cache"
    logs = _environment_path("REDSHIFT_ANALYZER_LOG_DIR", env, relative_to=install) or state / "logs"
    support = _environment_path("REDSHIFT_ANALYZER_SUPPORT_DIR", env, relative_to=install) or state / "support"

    configured_data = _environment_path("REDSHIFT_ANALYZER_DATA_DIR", env, relative_to=install)
    if configured_data is not None:
        data = configured_data
    elif analyzer_home is not None:
        # Preserve the historical REDSHIFT_ANALYZER_HOME contract.
        data = analyzer_home
    else:
        data = Path.home().resolve().joinpath(*LEGACY_DATA_PARTS)

    configured_duckdb = _environment_path("REDSHIFT_DUCKDB_PATH", env, relative_to=install)
    duckdb_file = configured_duckdb or data / "redshift.duckdb"
    profile_path = _environment_path("REDSHIFT_ANALYZER_PROFILE_PATH", env, relative_to=install)

    return RuntimePaths(
        install_dir=install,
        state_dir=state,
        config_dir=config,
        data_dir=data,
        cache_dir=cache,
        logs_dir=logs,
        support_dir=support,
        settings_file=state / "settings.json",
        auth_file=state / "auth.json",
        duckdb_file=duckdb_file,
        portable_profile_file=profile_path or install / PORTABLE_PROFILE_FILENAME,
    )


def _unique_paths(paths: Iterable[Path]) -> tuple[Path, ...]:
    result: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        normalized = path.resolve(strict=False)
        key = os.path.normcase(str(normalized))
        if key not in seen:
            seen.add(key)
            result.append(normalized)
    return tuple(result)


def configuration_candidates(
    filename: str,
    *,
    environment: os._Environ[str] | dict[str, str] | None = None,
    include_legacy_cwd: bool = True,
) -> tuple[Path, ...]:
    """Candidate files in deterministic trust/portability order.

    The launcher directory wins over the current working directory.  The CWD
    remains a final compatibility fallback for older command-line workflows,
    but can no longer unexpectedly override a profile shipped with Infraredshift.
    """
    env = environment if environment is not None else os.environ
    paths = resolve_runtime_paths(env)
    candidates: list[Path] = []
    if filename == PORTABLE_PROFILE_FILENAME:
        explicit = _environment_path("REDSHIFT_ANALYZER_PROFILE_PATH", env, relative_to=paths.install_dir)
        if explicit is not None:
            candidates.append(explicit)
    candidates.extend(
        (
            paths.install_dir / filename,
            paths.config_dir / filename,
            Path(__file__).resolve().parents[1] / filename,
        )
    )
    if include_legacy_cwd:
        candidates.append(Path.cwd() / filename)
    return _unique_paths(candidates)


def initialize_runtime_environment(
    environment: os._Environ[str] | dict[str, str] | None = None,
    *,
    create_directories: bool = True,
) -> RuntimePaths:
    """Publish stable launcher metadata and prepare per-user directories."""
    env = environment if environment is not None else os.environ
    paths = resolve_runtime_paths(env)
    env.setdefault("REDSHIFT_ANALYZER_LAUNCH_DIR", str(paths.install_dir))
    env.setdefault("REDSHIFT_ANALYZER_SUPPORT_DIR", str(paths.support_dir))
    if create_directories:
        for directory in paths.writable_directories():
            directory.mkdir(parents=True, exist_ok=True)
    return paths

