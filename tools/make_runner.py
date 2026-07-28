#!/usr/bin/env python3
"""Build the standalone runner.py from the analyzer's own source of truth.

    python tools/make_runner.py

Bundles, in order:
  1. tools/runner_header.part                      - docstring, imports, env loading
  2. analyzer/redshift_queries.py (verbatim body)  - every capture SQL factory
  3. tools/runner_logic.part                       - store/write/swap logic, with
     EXPECTED_COLUMNS and PERFORMANCE_INDEXES injected from analyzer.duckdb_store

Rerun after changing the analyzer's extraction SQL, table schemas, or indexes.

When a reviewed hotfix was first made directly in ``runner.py``, adopt its
logic section once before rebuilding:

    python tools/make_runner.py --adopt-runner-logic

The command preserves the generated SQL/schema sections and refreshes only the
store/load/swap implementation in ``tools/runner_logic.part``.
"""
from __future__ import annotations

import pprint
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from analyzer.duckdb_store import EXPECTED_COLUMNS, PERFORMANCE_INDEXES  # noqa: E402


LOGIC_MARKER = 'COMMON_COLUMNS = ("snapshot_id", "captured_at", "namespace_id")'
STORE_PLACEHOLDER = "#<<STORE_LITERALS>>"


def queries_body() -> str:
    source = (REPO / "analyzer" / "redshift_queries.py").read_text(encoding="utf-8")
    # Drop the module docstring and the __future__ import; keep everything else.
    parts = source.split('"""', 2)
    body = parts[2] if len(parts) == 3 else source
    return body.replace("from __future__ import annotations", "", 1).strip("\n")


def store_literals() -> str:
    return (
        f"EXPECTED_COLUMNS = {pprint.pformat(EXPECTED_COLUMNS, width=100, sort_dicts=False)}\n\n"
        f"PERFORMANCE_INDEXES = {pprint.pformat(PERFORMANCE_INDEXES, width=100)}"
    )


def adopt_runner_logic() -> None:
    """Mechanically sync the inspectable logic part from reviewed runner.py."""
    runner_source = (REPO / "runner.py").read_text(encoding="utf-8")
    marker_at = runner_source.find(LOGIC_MARKER)
    if marker_at < 0:
        raise SystemExit(f"Could not find runner logic marker: {LOGIC_MARKER}")
    part_path = REPO / "tools" / "runner_logic.part"
    prior = part_path.read_text(encoding="utf-8")
    placeholder_at = prior.find(STORE_PLACEHOLDER)
    if placeholder_at < 0:
        raise SystemExit(f"Could not find {STORE_PLACEHOLDER} in {part_path}")
    prefix = prior[: placeholder_at + len(STORE_PLACEHOLDER)].rstrip("\n")
    logic = runner_source[marker_at:].lstrip("\n")
    part_path.write_text(prefix + "\n\n" + logic, encoding="utf-8", newline="\n")
    print(f"Adopted reviewed runner logic into {part_path} ({len(logic.splitlines()):,} lines)")


def main() -> int:
    allowed = {"--adopt-runner-logic", "--check"}
    unknown = set(sys.argv[1:]) - allowed
    if unknown:
        raise SystemExit("Unknown option(s): " + ", ".join(sorted(unknown)))
    if "--adopt-runner-logic" in sys.argv[1:]:
        adopt_runner_logic()
    header = (REPO / "tools" / "runner_header.part").read_text(encoding="utf-8")
    logic = (REPO / "tools" / "runner_logic.part").read_text(encoding="utf-8")
    logic = logic.replace("#<<STORE_LITERALS>>", store_literals())
    output = header.rstrip("\n") + "\n\n" + queries_body() + "\n" + logic
    # Exactly the two tracked spellings. A capital-R Runner.py is invisible on
    # Windows but becomes a divergent duplicate on any case-sensitive clone.
    targets = (REPO / "runner.py", REPO / "runner.txt")
    if "--check" in sys.argv[1:]:
        mismatched = [
            target
            for target in targets
            if not target.is_file() or target.read_text(encoding="utf-8") != output
        ]
        if mismatched:
            print("Runner generation check failed: " + ", ".join(str(path) for path in mismatched))
            return 1
        print("Runner generation check passed: runner.py and runner.txt are synchronized.")
        return 0
    for target in targets:
        target.write_text(output, encoding="utf-8")
        print(f"Wrote {target} ({len(output.splitlines()):,} lines)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
