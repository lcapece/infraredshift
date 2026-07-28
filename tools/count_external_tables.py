"""Count external (Spectrum) tables per database and schema, before capturing them.

    cd %USERPROFILE%\\RQP
    python COUNT-EXTERNAL-TABLES.py

Read-only against Redshift. Touches only SVV_EXTERNAL_COLUMNS on the producer.
Writes EXTERNAL-TABLE-COUNTS.csv next to itself.

Why this exists
---------------
SVV_EXTERNAL_COLUMNS has ONE ROW PER COLUMN, not per table. A catalog with
14 million tables is not 14 million rows to fetch - at even ten columns per
table it is 140 million. The capture had no WHERE clause, so it tried to pull
all of them, which is what killed the app.

This script answers the question the filter needs: which databases and schemas
actually hold the tables, and how many column-rows each one costs to capture.
Point the include-filter at the handful of schemas that matter and the capture
becomes affordable.

Nothing here writes to Redshift or to the local warehouse.
"""

from __future__ import annotations

import argparse
import csv
import getpass
import os
import sys
from pathlib import Path


# One row per database+schema. COUNT(DISTINCT tablename) is the real table
# count; COUNT(*) is the column-row count, which is what the capture actually
# has to transfer and therefore what determines whether it is affordable.
COUNT_SQL = """
SELECT
  TRIM(redshift_database_name) AS database_name,
  TRIM(schemaname)             AS schema_name,
  COUNT(DISTINCT TRIM(tablename)) AS table_count,
  COUNT(*)                        AS column_rows
FROM svv_external_columns
GROUP BY 1, 2
ORDER BY column_rows DESC
"""


def _load_profiles(app_dir: Path):
    """Read the producer connection details from the cluster profiles JSON."""
    import json

    for name in (
        "redshift_cluster_profiles.json",
        "kit/redshift_cluster_profiles.json",
    ):
        candidate = app_dir / name
        if candidate.exists():
            data = json.loads(candidate.read_text(encoding="utf-8"))
            for profile in data.get("profiles", []):
                if str(profile.get("profile", "")).upper().endswith("PRODUCER"):
                    return profile, candidate
    return None, None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--app", default=os.path.expandvars(r"%USERPROFILE%\RQP"))
    parser.add_argument("--host", default="", help="override the producer host")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--database", default="", help="database to connect to")
    parser.add_argument("--user", default="")
    parser.add_argument(
        "--out",
        default="EXTERNAL-TABLE-COUNTS.csv",
        help="where to write the per-schema counts",
    )
    args = parser.parse_args()

    print("=" * 72)
    print(" EXTERNAL TABLE COUNTS - per database and schema")
    print("=" * 72)
    print(" Read-only. Touches only SVV_EXTERNAL_COLUMNS on the producer.\n")

    app_dir = Path(args.app)
    profile, source = _load_profiles(app_dir)
    if profile is not None:
        print(f" Producer profile from: {source}")

    host = args.host or os.environ.get("REDSHIFT_PRODUCER_HOST", "")
    port = args.port or int(
        (profile or {}).get("port") or os.environ.get("REDSHIFT_PRODUCER_PORT") or 5439
    )
    database = (
        args.database
        or (profile or {}).get("primary_database")
        or os.environ.get("REDSHIFT_PRODUCER_DATABASE")
        or "dev"
    )
    user = args.user or os.environ.get("REDSHIFT_PRODUCER_USER", "")

    if not host:
        host = input(" Producer host: ").strip()
    if not user:
        user = input(" Username: ").strip()
    if not host or not user:
        print("\nSTOP: a host and username are required.")
        return 1

    password = os.environ.get("REDSHIFT_PRODUCER_PASSWORD", "")
    if not password:
        password = getpass.getpass(" Password (not echoed): ")
    if not password:
        print("\nSTOP: no password supplied.")
        return 1

    try:
        import redshift_connector
    except ImportError:
        print("\nSTOP: missing redshift_connector.")
        print("      Fix:  pip install redshift-connector")
        return 1

    print(f"\n Connecting to {host}:{port}/{database} as {user} ...")
    try:
        conn = redshift_connector.connect(
            host=host, port=port, database=database, user=user, password=password
        )
    except Exception as exc:
        print(f"\nSTOP: could not connect: {type(exc).__name__}: {exc}")
        return 1

    print(" Counting external tables. On a large catalog this can take a few")
    print(" minutes - it is one aggregate scan, not a row-by-row fetch.\n")

    try:
        cursor = conn.cursor()
        try:
            cursor.execute(COUNT_SQL)
            rows = cursor.fetchall()
        finally:
            cursor.close()
    except Exception as exc:
        print(f"STOP: the count query failed: {type(exc).__name__}: {exc}")
        print("      Most likely cause: no SELECT grant on SVV_EXTERNAL_COLUMNS.")
        return 1
    finally:
        conn.close()

    if not rows:
        print(" No external tables are visible to this user. Nothing to filter.")
        return 0

    records = [
        {
            "database_name": str(row[0] or ""),
            "schema_name": str(row[1] or ""),
            "table_count": int(row[2] or 0),
            "column_rows": int(row[3] or 0),
        }
        for row in rows
    ]
    total_tables = sum(item["table_count"] for item in records)
    total_columns = sum(item["column_rows"] for item in records)

    print("-" * 72)
    print(f" {'DATABASE':<18} {'SCHEMA':<24} {'TABLES':>10} {'COLUMN ROWS':>14}")
    print("-" * 72)
    for item in records[:40]:
        print(
            f" {item['database_name'][:18]:<18} {item['schema_name'][:24]:<24} "
            f"{item['table_count']:>10,} {item['column_rows']:>14,}"
        )
    if len(records) > 40:
        print(f" ... and {len(records) - 40:,} more schema(s); see the CSV.")

    print("-" * 72)
    print(f" {'TOTAL':<43} {total_tables:>10,} {total_columns:>14,}")
    print()

    out_path = Path(args.out)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["database_name", "schema_name", "table_count", "column_rows"],
        )
        writer.writeheader()
        writer.writerows(records)
    print(f" Full list written to: {out_path.resolve()}")

    # The whole point of the count is to choose a filter, so end by naming the
    # schemas worth including rather than leaving that inference to the reader.
    print()
    print("=" * 72)
    print(" WHAT TO DO WITH THIS")
    print("=" * 72)
    print(f"   {total_tables:,} external table(s) across {len(records):,} schema(s),")
    print(f"   costing {total_columns:,} column rows to capture in full.")
    print()
    if total_columns > 5_000_000:
        print("   That is far too many to capture unfiltered - it is what killed")
        print("   the app. Choose the schemas you actually analyze and list them")
        print("   in the cluster profiles JSON:")
    else:
        print("   That is small enough to capture unfiltered, but you can still")
        print("   narrow it in the cluster profiles JSON:")
    print()
    biggest = records[0]
    sample = [item["schema_name"] for item in records[:3] if item["schema_name"]]
    print('       "external_schemas": "' + ", ".join(sample or ["your_schema"]) + '",')
    print('       "external_table_patterns": "fact_*, dim_*"')
    print()
    print("   Both are optional. external_schemas limits which schemas are read;")
    print("   external_table_patterns further limits table names within them.")
    print(f"   Largest single schema: {biggest['database_name']}.{biggest['schema_name']}")
    print(f"   at {biggest['column_rows']:,} column rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
