"""Deferred sqlglot import.

sqlglot's first import can take a minute or more on locked-down machines
(bytecode compilation of a very large package, antivirus scanning, roaming
profiles), and it sits on the app-startup import chain via query_similarity
and sql_lens. These proxies keep module import instant; the real sqlglot
loads on first attribute access, i.e. the first actual SQL analysis.
"""
from __future__ import annotations

import importlib
import logging


def _quiet_sqlglot_logger() -> None:
    """Silence sqlglot's own WARNING chatter.

    Real captured SQL (UNLOAD wrappers, REFRESH MATERIALIZED VIEW, INITCAP with
    custom delimiters) makes sqlglot log 'contains unsupported syntax. Falling
    back to parsing as a Command' and similar per statement. The analyzer
    already handles those fallbacks; the logging only floods the console. Errors
    still propagate as exceptions, so raising the threshold to ERROR loses no
    diagnostics."""
    logging.getLogger("sqlglot").setLevel(logging.ERROR)


class _LazyModule:
    def __init__(self, name: str) -> None:
        self._name = name
        self._module = None

    def __getattr__(self, attr: str):
        if self._module is None:
            self._module = importlib.import_module(self._name)
            _quiet_sqlglot_logger()
        return getattr(self._module, attr)


sqlglot = _LazyModule("sqlglot")
exp = _LazyModule("sqlglot.expressions")
