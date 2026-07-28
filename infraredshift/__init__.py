"""Infraredshift - Amazon Redshift workload triage.

The implementation lives in the ``analyzer`` package, which predates the
DataBa6ix -> Infraredshift rename. This module exists so the import name
matches the distribution name: ``pip install infraredshift`` should be
followed by ``python -m infraredshift``, not by a package name the user has
no way to guess.

Renaming ``analyzer`` itself would break every existing install, the
single-file launcher, and the DPAPI-scoped credential path, for a cosmetic
gain. An alias costs nothing and removes the surprise.
"""
from __future__ import annotations

from analyzer.brand import PRODUCT_NAME, PRODUCT_TAGLINE  # noqa: F401

__all__ = ["PRODUCT_NAME", "PRODUCT_TAGLINE", "run"]


def run() -> int:
    """Launch the desktop application."""
    from analyzer.app import run as _run

    return _run()
