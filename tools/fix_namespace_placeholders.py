"""Repair placeholder namespace IDs in an existing DuckDB warehouse.

Why this exists: ``redshift_cluster_profiles.json`` ships with template values
like ``REPLACE-PRODUCER-NAMESPACE-ID``. Nothing validates them, so a load run
against an unedited profile stores every row under the literal placeholder
string. The data is completely correct — only its namespace label is wrong — but
the analysis scope filters on the real namespace GUID, so the UI shows zero rows
for a cluster whose data is sitting right there.

This rewrites the label in place instead of reloading, which matters when the
cluster has a multi-hour queue and a reload is not an option.

Safety
------
* Dry run by default. Nothing is written without ``--apply``.
* Takes a file-copy backup before the first write.
* Discovers target tables from ``information_schema`` at runtime rather than
  trusting a hard-coded list, so a schema change cannot cause a silent miss.
* Updates BASE TABLES only. Views deriving from them follow automatically;
  updating a view would fail or, worse, write through unpredictably.
* Refuses to write a new value that is itself a placeholder.
* Every statement runs inside one transaction — all tables move together or
  none do.

Usage
-----
    # See what would change
    python fix_namespace_placeholders.py --db "%USERPROFILE%\\RQP\\data\\redshift.duckdb"

    # Apply one mapping
    python fix_namespace_placeholders.py --db ... \\
        --map REPLACE-PRODUCER-NAMESPACE-ID=a1b2c3d4-... --apply

    # Apply several at once
    python fix_namespace_placeholders.py --db ... \\
        --map REPLACE-PRODUCER-NAMESPACE-ID=<guid> \\
        --map REPLACE-CONSUMER-1-NAMESPACE-ID=<guid> --apply
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

try:
    import duckdb
except ImportError:  # pragma: no cover
    print("duckdb is required: pip install duckdb", file=sys.stderr)
    raise SystemExit(2)

PLACEHOLDER_RE = re.compile(r"^REPLACE[-_]", re.IGNORECASE)
# A Redshift namespace id is a UUID. Warn (do not block) on anything else, since
# single-cluster installs legitimately use the literal "producer".
UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)


def looks_like_placeholder(value: str) -> bool:
    return bool(PLACEHOLDER_RE.match(str(value or "").strip()))


def target_tables(con) -> list[str]:
    """Base tables carrying a namespace_id column.

    Views are excluded deliberately: they derive from these tables, so updating
    the base rows updates them too.
    """
    rows = con.execute(
        """
        SELECT c.table_name
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON t.table_name = c.table_name
         AND t.table_schema = c.table_schema
        WHERE c.column_name = 'namespace_id'
          AND c.table_schema = 'main'
          AND t.table_type = 'BASE TABLE'
        ORDER BY c.table_name
        """
    ).fetchall()
    return [row[0] for row in rows]


def survey(con, tables: list[str]) -> dict[str, dict[str, int]]:
    """namespace_id -> {table: row_count} for every distinct value present."""
    found: dict[str, dict[str, int]] = {}
    for table in tables:
        try:
            rows = con.execute(
                f'SELECT namespace_id, COUNT(*) FROM "{table}" GROUP BY 1'
            ).fetchall()
        except Exception as exc:  # table may be empty or mid-migration
            print(f"  ! could not read {table}: {exc}", file=sys.stderr)
            continue
        for namespace, count in rows:
            key = "" if namespace is None else str(namespace)
            found.setdefault(key, {})[table] = int(count)
    return found


def backup(path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    target = path.with_name(f"{path.stem}.pre-nsfix-{stamp}{path.suffix}")
    shutil.copy2(path, target)
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", required=True, help="path to redshift.duckdb")
    parser.add_argument(
        "--map",
        action="append",
        default=[],
        metavar="OLD=NEW",
        help="namespace remap, repeatable (e.g. REPLACE-PRODUCER-NAMESPACE-ID=<guid>)",
    )
    parser.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    parser.add_argument("--no-backup", action="store_true", help="skip the backup copy")
    args = parser.parse_args(argv)

    db_path = Path(os.path.expandvars(args.db)).expanduser()
    if not db_path.exists():
        print(f"ERROR: database not found: {db_path}", file=sys.stderr)
        return 2

    mapping: dict[str, str] = {}
    for item in args.map:
        if "=" not in item:
            print(f"ERROR: --map needs OLD=NEW, got: {item}", file=sys.stderr)
            return 2
        old, new = item.split("=", 1)
        old, new = old.strip(), new.strip()
        if not old or not new:
            print(f"ERROR: empty side in --map {item}", file=sys.stderr)
            return 2
        if looks_like_placeholder(new):
            print(
                f"ERROR: refusing to write another placeholder as the NEW value: {new}",
                file=sys.stderr,
            )
            return 2
        if not UUID_RE.match(new) and new.lower() != "producer":
            print(f"  warning: {new!r} does not look like a namespace UUID")
        mapping[old] = new

    con = duckdb.connect(str(db_path), read_only=not args.apply)
    tables = target_tables(con)
    print(f"database : {db_path}")
    print(f"tables   : {len(tables)} base table(s) carry namespace_id\n")

    present = survey(con, tables)
    if not present:
        print("No namespace_id values found. Nothing to do.")
        return 0

    print("Current namespace_id values:")
    placeholders_found: list[str] = []
    for namespace in sorted(present, key=lambda k: -sum(present[k].values())):
        total = sum(present[namespace].values())
        flag = ""
        if looks_like_placeholder(namespace):
            flag = "   <-- PLACEHOLDER"
            placeholders_found.append(namespace)
        elif namespace == "":
            flag = "   <-- blank"
        label = namespace or "(empty)"
        print(f"  {label:<42} {total:>10,} rows across "
              f"{len(present[namespace])} table(s){flag}")

    if not mapping:
        print()
        if placeholders_found:
            print("Placeholder namespace(s) detected. Re-run with a mapping, e.g.:")
            for namespace in placeholders_found:
                print(f'  --map "{namespace}=<real-namespace-guid>"')
            print("\nFind the real value on each cluster with:  SELECT current_namespace;")
        else:
            print("No placeholder namespaces found; nothing to repair.")
        return 0

    # Report the planned work before touching anything.
    print("\nPlanned changes:")
    planned: list[tuple[str, str, str, int]] = []
    for old, new in mapping.items():
        if old not in present:
            print(f"  ! {old} is not present in this database - skipping")
            continue
        if new in present:
            print(
                f"  ! {new} ALREADY EXISTS with "
                f"{sum(present[new].values()):,} rows - merging into it"
            )
        for table, count in sorted(present[old].items()):
            planned.append((table, old, new, count))
            print(f"  {table:<32} {count:>10,} rows   {old} -> {new}")

    if not planned:
        print("\nNothing to change.")
        return 0

    total_rows = sum(item[3] for item in planned)
    print(f"\nTotal: {total_rows:,} rows across {len({p[0] for p in planned})} table(s)")

    if not args.apply:
        print("\nDRY RUN - nothing written. Re-run with --apply to make these changes.")
        return 0

    con.close()
    if not args.no_backup:
        copy = backup(db_path)
        print(f"\nBackup written: {copy}")

    con = duckdb.connect(str(db_path), read_only=False)
    changed = 0
    try:
        con.execute("BEGIN TRANSACTION")
        for table, old, new, _count in planned:
            con.execute(
                f'UPDATE "{table}" SET namespace_id = ? WHERE namespace_id = ?',
                [new, old],
            )
            changed += 1
        con.execute("COMMIT")
    except Exception as exc:
        con.execute("ROLLBACK")
        print(f"\nFAILED - rolled back, database unchanged: {exc}", file=sys.stderr)
        return 1

    print(f"\nApplied to {changed} table(s).")
    print("\nResulting namespace_id values:")
    for namespace, tables_map in sorted(
        survey(con, tables).items(), key=lambda kv: -sum(kv[1].values())
    ):
        print(f"  {namespace or '(empty)':<42} {sum(tables_map.values()):>10,} rows")

    print(
        "\nNEXT: fix redshift_cluster_profiles.json so the next load stores the "
        "correct namespace, and check Settings -> Analysis cluster scope."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
