"""The single-file launchers must render cleanly from the current source.

The launchers are generated BUILD OUTPUT and are no longer committed to the
repo (they were previously committed in ~10 copies, ~800k lines of churn).
CI rebuilds them from analyzer/ source on every run, so a stale/committed
launcher can no longer exist. What still matters, and is what these tests
guard, is:

  1. the text builder renders deterministically from source without error, and
  2. when launchers HAVE been built on disk, the .py/.txt twins are identical.

Because the launchers are build output, tests that touch them tolerate their
absence (a fresh checkout that has not run tools/build_text_py.py yet).
"""
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

_STAMP = re.compile(r"REDSHIFT_ANALYZER_PUBLISHED_AT\", '[^']*'")


def _normalized(text: str) -> str:
    return _STAMP.sub("REDSHIFT_ANALYZER_PUBLISHED_AT\", '<stamp>'", text)


def test_text_launcher_twins_are_byte_identical():
    pairs = [
        ("Infraredshift.py", "Infraredshift.txt"),
        ("redshift_analyzer_text.py", "redshift_analyzer_text.txt"),
        ("runner.py", "runner.txt"),
        ("Databas6ix_fat.py", "Databas6ix_fat.txt"),
        ("redshift_analyzer_fat.py", "redshift_analyzer_fat.txt"),
    ]
    for py_name, txt_name in pairs:
        py_path, txt_path = ROOT / py_name, ROOT / txt_name
        if not py_path.is_file() or not txt_path.is_file():
            continue
        if py_path.read_bytes() != txt_path.read_bytes():
            pytest.fail(f"{py_name} and {txt_name} have drifted apart; rerun the launcher build.")


def test_text_launcher_renders_from_source():
    """The text builder must render deterministically from analyzer/ source.

    Replaces the old "committed launcher matches source" check: the launcher
    is no longer committed, so the mechanism that prevents drift is now
    "build fresh from source", and this asserts that build succeeds and is
    stable across two renders (modulo the publish timestamp).
    """
    sys.path.insert(0, str(ROOT / "tools"))
    try:
        import build_text_py
    finally:
        sys.path.pop(0)

    files, binary_files = build_text_py.collect_files()
    first = build_text_py.render_launcher(files, binary_files)
    second = build_text_py.render_launcher(files, binary_files)
    assert _normalized(first) == _normalized(second), (
        "build_text_py render is non-deterministic across two calls on the "
        "same source; the launcher build is not reproducible."
    )
    assert "DataBasix" in first and "def main(" in first, (
        "rendered launcher is missing expected structure; build_text_py may "
        "be broken."
    )
