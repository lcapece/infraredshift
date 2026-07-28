"""Pin these tests to THIS subproject's source.

A sibling checkout of redshift_decomposer is pip-installed editable on some
machines. When pytest runs from the parent repo root, this pyproject's
``pythonpath = ["src"]`` does not apply (wrong rootdir) and ``import
redshift_decomposer`` silently resolves to that older sibling — making these
tests pass or fail based on a directory outside this repository.
"""
import sys
from pathlib import Path

_SRC = str(Path(__file__).resolve().parents[1] / "src")

if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# Evict any copy already imported from somewhere else (editable install,
# site-packages) so the insertion above actually takes effect.
for _name in [
    n for n in list(sys.modules)
    if n == "redshift_decomposer" or n.startswith("redshift_decomposer.")
]:
    _file = getattr(sys.modules[_name], "__file__", "") or ""
    if not _file.lower().startswith(_SRC.lower()):
        del sys.modules[_name]
