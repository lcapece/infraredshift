"""Recoverable Infraredshift loader process boundary.

The capture SQL and DuckDB promotion implementation remain in :mod:`runner`.
This package is the single public orchestration API used by the GUI, command
line, and Windows Task Scheduler.
"""

from .engine import (
    LoaderAlreadyRunningError,
    LoaderEngine,
    LoaderEvent,
    LoaderRequest,
    build_loader_command,
    build_promote_command,
)

__all__ = [
    "LoaderAlreadyRunningError",
    "LoaderEngine",
    "LoaderEvent",
    "LoaderRequest",
    "build_loader_command",
    "build_promote_command",
]
