"""Find which query stalls repeat grouping. Read-only; the app can stay open.

    cd %USERPROFILE%\\RQP
    python FIND-THE-HANG.py

Grouping appears to freeze rather than fail, and a frozen terminal says
nothing. This parses every query one at a time and writes the query it is
about to parse to CURRENT-QUERY.txt, flushed each time. If it stalls, that
file names the culprit.

Synthetic testing could not reproduce the stall: 15,000 rows group in under
4 seconds, a 10,000-element IN list parses in 0.19s, and 4,000 distinct shapes
on one table fuzzy-merge in 9.6s. So the cause is specific to the real
workload, and only the real workload can reveal it.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

try:
    import duckdb
except ImportError as exc:
    sys.exit(f"missing dependency: {exc}.  Fix:  pip install duckdb")


def load_analyzer(app_dir: Path):
    """Import the analyzer from a source tree, extracted sources, or the app.

    The client install is the single-file Infraredshift.py, which extracts its
    sources to a temp folder at startup - so there is no analyzer/ directory to
    import from directly.
    """
    for candidate in (app_dir, app_dir / "RQP", Path.cwd(), Path.cwd() / "RQP"):
        if (candidate / "analyzer" / "query_similarity.py").exists():
            sys.path.insert(0, str(candidate))
            from analyzer import query_similarity  # type: ignore

            return query_similarity

    import tempfile

    root = Path(tempfile.gettempdir()) / "redshift_query_anatomy_text"
    if root.exists():
        for child in sorted(
            root.glob("analyzer_sources_*"),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        ):
            if (child / "analyzer" / "query_similarity.py").exists():
                sys.path.insert(0, str(child))
                try:
                    from analyzer import query_similarity  # type: ignore

                    return query_similarity
                except Exception:
                    sys.path.pop(0)

    for name in ("Infraredshift.py", "Infraredshift.txt", "Databas6ix.py"):
        launcher = app_dir / name
        if not launcher.exists():
            continue
        try:
            import importlib.util

            spec = importlib.util.spec_from_file_location("_dbx_launcher", launcher)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
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
        "--db", default=os.path.expandvars(r"%USERPROFILE%\RQP\data\redshift.duckdb")
    )
    parser.add_argument("--app", default=os.path.expandvars(r"%USERPROFILE%\RQP"))
    parser.add_argument(
        "--slow-ms",
        type=int,
        default=500,
        help="report any query slower than this (default 500)",
    )
    args = parser.parse_args()

    db = Path(args.db)
    print("=" * 70)
    print(" FIND THE HANG - per-query parse timing")
    print("=" * 70)
    print(f" Database: {db}\n")
    if not db.exists():
        print("STOP: database not found.")
        return 1

    module = load_analyzer(Path(args.app))
    if module is None:
        print("STOP: could not import the analyzer.")
        print(f"      Looked under {args.app}. Run this from the RQP folder,")
        print("      or start the app once so it extracts its sources.")
        return 1

    con = duckdb.connect(str(db), read_only=True)
    tables = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
    }
    source = next(
        (
            name
            for name in ("v_slow_queries", "slow_queries", "v_query_history", "query_history")
            if name in tables
        ),
        None,
    )
    if source is None:
        print("STOP: no slow-query table found.")
        return 1

    rows = con.execute(
        f'SELECT query_id, sql_text FROM "{source}" WHERE sql_text IS NOT NULL'
    ).fetchall()
    con.close()

    marker = Path("CURRENT-QUERY.txt")
    handle = marker.open("w", encoding="utf-8")

    print(f" {len(rows):,} queries to parse, reporting anything over {args.slow_ms}ms.")
    print(f" If this stalls, open:  {marker.resolve()}")
    print(" That file names the query being parsed at that moment.\n")
    print("-" * 70)

    threshold = args.slow_ms / 1000.0
    slow: list[tuple[float, object, str]] = []
    failures = 0
    started = time.time()

    for position, (query_id, sql) in enumerate(rows, 1):
        text = str(sql or "")
        if not text.strip():
            continue

        # Record what is about to be parsed BEFORE parsing it, flushed, so a
        # hang leaves the culprit named on disk. The terminal stays quiet
        # because \r does not reliably overwrite in every Windows console.
        handle.seek(0)
        handle.truncate()
        handle.write(
            f"position {position:,} of {len(rows):,}\n"
            f"query_id {query_id}\n"
            f"chars    {len(text):,}\n\n{text[:4000]}\n"
        )
        handle.flush()

        mark = time.time()
        try:
            module.canonical_sql_fingerprint(text)
            module.analyze_sql(text)
        except Exception as exc:
            failures += 1
            print(f"  query {query_id}: RAISED {type(exc).__name__}: {str(exc)[:70]}")
            continue
        cost = time.time() - mark

        if cost >= threshold:
            slow.append((cost, query_id, text[:150].replace("\n", " ")))
            print(f"  SLOW  {cost:>7.2f}s  query {query_id}  ({len(text):,} chars)")

        if position % 1000 == 0:
            elapsed = time.time() - started
            rate = position / max(elapsed, 0.001)
            left = (len(rows) - position) / max(rate, 0.001)
            print(
                f"  ...{position:,} of {len(rows):,} in {elapsed:.0f}s "
                f"({rate:.0f}/s, ~{left:.0f}s left)"
            )

    handle.close()
    try:
        marker.unlink()
    except OSError:
        pass

    elapsed = time.time() - started
    print("\n" + "=" * 70)
    print(" RESULT")
    print("=" * 70)
    print(f"   parsed  : {len(rows):,} queries in {elapsed:.1f}s")
    print(f"   failed  : {failures:,}")
    print(f"   slow    : {len(slow):,} over {args.slow_ms}ms")

    if slow:
        slow.sort(reverse=True)
        total = sum(item[0] for item in slow)
        print(f"\n   Slowest ({total:.0f}s of the {elapsed:.0f}s total):")
        for cost, query_id, snippet in slow[:10]:
            print(f"     {cost:>7.2f}s  query {query_id}")
            print(f"              {snippet}")
        print("\n   These are what stall the grouping pass.")
    elif failures:
        print("\n   Nothing was slow, but some queries RAISED. Those are the problem.")
    else:
        print("\n   Every query parsed quickly and none failed.")
        print("   Parsing is NOT the hang - it is later in the pass")
        print("   (grouping, fuzzy merge, or rendering). Send this output.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
