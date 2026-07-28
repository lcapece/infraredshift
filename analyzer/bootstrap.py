"""Small, dependency-free application bootstrap for Infraredshift."""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from .portable_config import portable_config_candidates, read_portable_environment
from .runtime_paths import RuntimePaths, initialize_runtime_environment


@dataclass(frozen=True)
class BootstrapResult:
    paths: RuntimePaths
    profile_path: Path | None = None
    warning: str = ""


def bootstrap_application(
    environment: os._Environ[str] | dict[str, str] | None = None,
    *,
    create_directories: bool = True,
) -> BootstrapResult:
    """Initialize paths and import the first valid non-secret team profile.

    Environment variables supplied by a launcher or enterprise scheduler are
    authoritative.  Portable values only fill missing keys.  A damaged profile
    is skipped with a diagnostic rather than preventing the desktop application
    from opening; subsequent candidates remain eligible.
    """
    env = environment if environment is not None else os.environ
    paths = initialize_runtime_environment(env, create_directories=create_directories)
    warnings: list[str] = []
    for candidate in portable_config_candidates(environment=env, include_legacy_cwd=False):
        if not candidate.is_file():
            continue
        try:
            values = read_portable_environment(candidate)
        except (OSError, ValueError, TypeError) as exc:
            warnings.append(f"Could not read {candidate.name}: {exc}")
            continue
        for key, value in values.items():
            env.setdefault(key, value)
        return BootstrapResult(paths=paths, profile_path=candidate, warning="; ".join(warnings))
    return BootstrapResult(paths=paths, warning="; ".join(warnings))
