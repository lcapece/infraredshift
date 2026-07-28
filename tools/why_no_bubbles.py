"""Find out why the triage bubble chart is empty. Read-only, safe with the app open.

    python WHY-NO-BUBBLES.py

Checks the whole chain in order and stops at the first broken link, so the
answer is a specific cause rather than a guess:

    warehouse exists -> rows loaded -> namespaces sane -> repeat groups built
    -> groups have chart metric columns -> rows survive the chart filters

Nothing is written. The Infraredshift app can stay open.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    sys.exit("duckdb missing.  Fix:  pip install duckdb")

DB = Path(os.path.expandvars(r"%USERPROFILE%\RQP\data\redshift.duckdb"))

# Columns the bubble chart plots (analyzer/widgets/triage_home.py CHART_METRICS)
CHART_COLUMNS = (
    "total_input_rows",
    "total_input_bytes",
    "total_spill_blocks",
    "total_queue_s",
)


def rule(title: str) -> None:
    print("\n" + "=" * 68)
    print(f" {title}")
    print("=" * 68)


def main() -> int:
    print("=" * 68)
    print(" WHY ARE THERE NO BUBBLES?")
    print("=" * 68)
    print(f" Database: {DB}")

    if not DB.exists():
        print("\nSTOP: that file does not exist.")
        print("The app is reading a different warehouse than this script.")
        return 1

    size_mb = DB.stat().st_size / 1e6
    print(f" Size:     {size_mb:,.1f} MB")
    if size_mb < 1:
        print("\nSTOP: the warehouse is essentially empty. Nothing has been promoted.")
        return 1

    con = duckdb.connect(str(DB), read_only=True)
    tables = {row[0] for row in con.execute(
        "SELECT table_name FROM information_schema.tables"
    ).fetchall()}

    # ---------------------------------------------------------------- step 1
    rule("1. Is any query data loaded?")
    total = 0
    for name in ("query_history", "v_query_history", "v_slow_queries"):
        if name in tables:
            count = con.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            print(f"   {name:<20} {count:>10,} rows")
            total = max(total, count)
    if total == 0:
        print("\nSTOP: no query rows at all. The load never promoted to live.")
        print("      Fix the load first; the chart cannot show what is not there.")
        return 1

    # ---------------------------------------------------------------- step 2
    rule("2. What namespaces do those rows carry?")
    if "query_history" in tables:
        for value, count in con.execute(
            "SELECT COALESCE(NULLIF(TRIM(namespace_id),''),'(null/empty)'), COUNT(*) "
            'FROM "query_history" GROUP BY 1 ORDER BY 2 DESC'
        ).fetchall():
            flag = "  <-- PLACEHOLDER, needs FIX-NAMESPACES" if str(value).startswith("REPLACE-") else ""
            print(f"   {str(value):<40} {count:>9,} rows{flag}")

    # ---------------------------------------------------------------- step 3
    rule("3. Were repeat groups actually built?")
    group_table = next(
        (n for n in ("repeat_groups", "v_repeat_groups", "analysis_cache_repeat_groups")
         if n in tables),
        None,
    )
    if group_table is None:
        print("   No repeat-group table exists in this warehouse.")
        print("\nSTOP: grouping has never completed on this database.")
        print("      Run the analysis; if it errors, send the exact message.")
        con.close()
        return 1

    groups = con.execute(f'SELECT COUNT(*) FROM "{group_table}"').fetchone()[0]
    print(f"   {group_table}: {groups:,} groups")
    if groups == 0:
        print("\nSTOP: the table exists but is EMPTY.")
        print("      Grouping ran and produced nothing - this is the bubble problem.")
        print("      Check the error log for the message logged during analysis.")
        con.close()
        return 1

    # ---------------------------------------------------------------- step 4
    rule("4. Do the groups carry the columns the chart plots?")
    columns = {
        row[0] for row in con.execute(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{group_table}'"
        ).fetchall()
    }
    missing = [column for column in CHART_COLUMNS if column not in columns]
    for column in CHART_COLUMNS:
        if column in columns:
            nonzero = con.execute(
                f'SELECT COUNT(*) FROM "{group_table}" '
                f'WHERE COALESCE(TRY_CAST({column} AS DOUBLE), 0) > 0'
            ).fetchone()[0]
            print(f"   {column:<22} present, {nonzero:>6,} group(s) with a value > 0")
        else:
            print(f"   {column:<22} MISSING")
    if missing:
        print(f"\nSTOP: the chart cannot plot without {', '.join(missing)}.")
        print("      The groups were built by an older build that did not emit them.")
        print("      Re-run the analysis with the current build.")
        con.close()
        return 1

    # ---------------------------------------------------------------- step 5
    rule("5. Do any groups survive the chart's own filters?")
    if "avg_elapsed_s" in columns:
        for label, threshold in (
            ("Any average runtime", 0.0),
            ("At least 1 second/run", 1.0),
            ("At least 5 seconds/run", 5.0),
            ("At least 30 seconds/run", 30.0),
            ("At least 5 minutes/run", 300.0),
        ):
            passes = con.execute(
                f'SELECT COUNT(*) FROM "{group_table}" '
                f"WHERE COALESCE(TRY_CAST(avg_elapsed_s AS DOUBLE), 0) >= {threshold}"
            ).fetchone()[0]
            note = "   <-- empty chart at this setting" if passes == 0 else ""
            print(f"   {label:<26} {passes:>5} group(s){note}")
        print("\n   If your selected filter shows 0, that alone empties the chart")
        print("   with NO error. Set the dropdown to 'Any average runtime'.")
    else:
        print("   avg_elapsed_s missing - cannot evaluate the runtime filter.")

    # ---------------------------------------------------------------- verdict
    rule("VERDICT")
    print(f"   {groups:,} groups exist, with the columns the chart needs.")
    print("   The data is fine, so the chart should render.")
    print("\n   If it does not, the cause is in the UI, not the data:")
    print("     - the runtime filter dropdown (see step 5)")
    print("     - Settings -> Analysis cluster scope excluding your namespaces")
    print("     - the app running an older build than the one just deployed")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
