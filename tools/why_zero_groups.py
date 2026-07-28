"""Explain exactly why repeat grouping produced zero groups.

    python WHY-ZERO-GROUPS.py

Read-only. The Infraredshift app can stay open.

Run this from the folder that holds Infraredshift.py, or pass --app <path> if it
lives elsewhere - the analyzer code is imported from that file.

The grouping pipeline already computes a full diagnostic explaining any zero
result. It is normally buried in the analysis output; this prints it, together
with the row counts at each stage, so the failure has a named cause instead of
an empty screen.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

try:
    import duckdb
    import pandas as pd
except ImportError as exc:
    sys.exit(f"missing dependency: {exc}.  Fix:  pip install duckdb pandas")


def load_analyzer(app_dir: Path):
    """Import the analyzer package from a source tree OR the single-file app.

    The client install is Infraredshift.py - a single concatenated file, not an
    analyzer/ folder. It extracts its sources to a temp directory at startup
    and puts that on sys.path. Reuse the same mechanism rather than requiring a
    source checkout that the target machine does not have.
    """
    # 1. A plain source tree, if one happens to be present.
    for candidate in (app_dir, app_dir / "RQP", Path.cwd(), Path.cwd() / "RQP"):
        if (candidate / "analyzer" / "query_similarity.py").exists():
            sys.path.insert(0, str(candidate))
            from analyzer import query_similarity  # type: ignore

            return query_similarity

    # 2. Sources already extracted by a previous app run.
    import tempfile

    extracted = Path(tempfile.gettempdir()) / "redshift_query_anatomy_text"
    if extracted.exists():
        for child in sorted(
            extracted.glob("analyzer_sources_*"), key=lambda p: p.stat().st_mtime, reverse=True
        ):
            if (child / "analyzer" / "query_similarity.py").exists():
                sys.path.insert(0, str(child))
                try:
                    from analyzer import query_similarity  # type: ignore

                    return query_similarity
                except Exception:
                    sys.path.pop(0)

    # 3. Ask the launcher itself to extract, without starting the GUI.
    for name in ("Infraredshift.py", "Infraredshift.txt", "Databas6ix.py"):
        launcher = app_dir / name
        if not launcher.exists():
            continue
        try:
            import importlib.util

            spec = importlib.util.spec_from_file_location("_dbx_launcher", launcher)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)  # defines _install_sources, no GUI
            if hasattr(module, "_install_sources"):
                module._install_sources()
                from analyzer import query_similarity  # type: ignore

                return query_similarity
        except Exception:
            continue

    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db",
        default=os.path.expandvars(r"%USERPROFILE%\RQP\data\redshift.duckdb"),
    )
    parser.add_argument(
        "--app",
        default=os.path.expandvars(r"%USERPROFILE%\RQP"),
        help="folder containing the analyzer package",
    )
    args = parser.parse_args()

    db = Path(args.db)
    print("=" * 70)
    print(" WHY DID REPEAT GROUPING RETURN ZERO GROUPS?")
    print("=" * 70)
    print(f" Database: {db}")

    if not db.exists():
        print("\nSTOP: that database does not exist.")
        return 1

    con = duckdb.connect(str(db), read_only=True)
    tables = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables"
    ).fetchall()}

    source = next(
        (n for n in ("v_slow_queries", "slow_queries", "v_query_history", "query_history")
         if n in tables),
        None,
    )
    if source is None:
        print("\nSTOP: no slow-query table found. Nothing has been loaded.")
        return 1

    frame = con.execute(f'SELECT * FROM "{source}"').df()
    print(f" Source:   {source} ({len(frame):,} rows)\n")

    # ------------------------------------------------------------------ stage
    print("-" * 70)
    print(" STAGE COUNTS")
    print("-" * 70)
    if "sql_text" not in frame.columns:
        print("   sql_text column: MISSING")
        print("\nSTOP: repeat grouping needs captured SQL text. Nothing else matters.")
        return 1

    with_sql = int(frame["sql_text"].notna().sum())
    long_enough = int(
        frame["sql_text"].fillna("").astype(str).str.strip().str.len().ge(24).sum()
    )
    print(f"   rows total            {len(frame):>8,}")
    print(f"   rows with sql_text    {with_sql:>8,}")
    print(f"   sql_text >= 24 chars  {long_enough:>8,}")

    if "query_type" in frame.columns:
        print("\n   query_type spread (grouping requires a SHARED type):")
        for value, count in frame["query_type"].fillna("(null)").value_counts().head(8).items():
            print(f"     {str(value):<24} {count:>8,}")
    else:
        print("\n   query_type column: MISSING - every row lands in one bucket")

    if "namespace_id" in frame.columns:
        print("\n   namespace spread:")
        for value, count in frame["namespace_id"].fillna("(null)").value_counts().head(8).items():
            flag = "  <-- PLACEHOLDER" if str(value).startswith("REPLACE-") else ""
            print(f"     {str(value):<40} {count:>8,}{flag}")

    con.close()

    # ------------------------------------------------------------ diagnostic
    print("\n" + "-" * 70)
    print(" GROUPING DIAGNOSTIC")
    print("-" * 70)

    module = load_analyzer(Path(args.app))
    if module is None:
        print("   Could not import the analyzer package.")
        print(f"   Looked under: {args.app}")
        print("   Pass --app <folder containing analyzer/> to run the full diagnostic.")
        print("\n   The stage counts above still narrow it down:")
        print("     * 0 rows with sql_text  -> query text was never captured")
        print("     * only 1 query_type row -> nothing can repeat")
        print("     * all rows one-of-a-kind -> no shape occurs twice")
        return 0

    # Time each query individually with live output. If grouping hangs rather
    # than returning zero, this prints the exact SQL it stalled on instead of
    # freezing on a blank line - which is what a silent diagnostic did.
    import time

    print("   Timing per-query parse (slowest are printed)...")
    slow: list[tuple[float, int, str]] = []
    started = time.time()
    texts = frame["sql_text"].fillna("").astype(str).tolist()
    ids = (
        frame["query_id"].tolist()
        if "query_id" in frame.columns
        else list(range(len(texts)))
    )
    for position, (qid, text) in enumerate(zip(ids, texts)):
        if not text.strip():
            continue
        mark = time.time()
        try:
            module.canonical_sql_fingerprint(text)
        except Exception as exc:
            print(f"     query {qid}: RAISED {type(exc).__name__}: {str(exc)[:70]}")
            continue
        cost = time.time() - mark
        if cost > 1.0:
            print(f"     query {qid}: {cost:.1f}s  ({len(text):,} chars)  <-- SLOW")
            slow.append((cost, qid, text[:120]))
        if position and position % 2000 == 0:
            print(f"     ...{position:,} of {len(texts):,} parsed, {time.time()-started:.0f}s elapsed")
    total = time.time() - started
    print(f"   parsed {len(texts):,} queries in {total:.1f}s")
    if slow:
        slow.sort(reverse=True)
        print(f"
   {len(slow)} query(s) took over 1s. Worst:")
        for cost, qid, snippet in slow[:5]:
            print(f"     {cost:>7.1f}s  query {qid}: {snippet}")
        print("
   These are what stall the grouping pass.")

    print("
   Running the full grouping diagnostic...")
    try:
        diagnostics = module.diagnose_repeat_query_candidates(frame)
    except Exception as exc:
        print(f"   Diagnostic itself failed: {type(exc).__name__}: {exc}")
        return 1

    for key in (
        "repeat_raw_query_rows",
        "repeat_sql_text_rows",
        "repeat_prepared_query_count",
        "repeat_parse_success_count",
        "repeat_strict_family_bucket_count",
        "repeat_deterministic_group_count",
        "repeat_largest_bucket_size",
        "repeat_min_group_size",
    ):
        if key in diagnostics:
            label = key.replace("repeat_", "").replace("_", " ")
            print(f"   {label:<34} {diagnostics[key]:>10}")

    note = str(diagnostics.get("repeat_diagnostic_note") or "").strip()
    print("\n" + "=" * 70)
    print(" ANSWER")
    print("=" * 70)
    print(f"   {note}" if note else "   (no diagnostic note produced)")

    largest = int(diagnostics.get("repeat_largest_bucket_size") or 0)
    if largest <= 1:
        print("\n   Every query shape is unique - nothing runs twice in this capture.")
        print("   Repeat grouping has nothing to group. Widen the capture window,")
        print("   or lower the slow-query threshold to include recurring queries.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
