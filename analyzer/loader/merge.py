"""Merge per-cluster DuckDB files into the unified analyzer warehouse.

The per-cluster loader writes one file per cluster (redshift.<role>.duckdb).
This step copies every namespace's rows out of those files into one shared
warehouse under a single new snapshot, then the analyzer sees all clusters
together for the cross-cluster topology. Live warehouse tables are replaced
only at the end, and only for the tables actually present in the sources.

Safety contract (mirrors the loader's refresh/promote):
* the whole merge runs under the loader process lock for the target file, so
  it cannot interleave with a scheduled refresh or promote;
* the target is backed up before any live table is replaced;
* a missing or unpromoted per-cluster file aborts the merge (--allow-partial
  overrides), so a failed LOAD window cannot silently shrink the warehouse;
* per-cluster files whose latest snapshots are far apart abort the merge
  (--allow-stale overrides), so yesterday's consumer file cannot silently be
  presented as part of today's picture;
* each namespace's rows are taken from exactly one source file per table, so
  double-captured namespaces cannot double-count.
"""
from __future__ import annotations

import argparse
import re
import uuid
from datetime import datetime
from pathlib import Path

EXPECTED_ROLES = ("producer", "consumer_1", "consumer_2", "consumer_3")
DEFAULT_MAX_SKEW_HOURS = 18.0
_ROLE_FROM_NAME = re.compile(r"^redshift\.(?P<role>[a-z0-9_]+)\.duckdb$", re.IGNORECASE)


def _analyzer_tables(con) -> list[str]:
    import runner

    present = {
        str(row[0]).lower()
        for row in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE NOT internal"
        ).fetchall()
    }
    return [t for t in runner.LIVE_REFRESH_TABLES if t.lower() in present]


def _path_literal(path: Path | str) -> str:
    """A filesystem path as a single-quoted SQL literal (O'Brien-safe)."""
    return str(path).replace("'", "''")


def _string_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _role_from_filename(path: Path) -> str:
    match = _ROLE_FROM_NAME.match(path.name)
    return match.group("role").lower() if match else ""


def _source_inventory(src: Path) -> dict:
    """Latest promoted snapshot and cluster registry of one per-cluster file."""
    import duckdb

    info: dict = {"path": src, "snapshot_id": "", "captured_at": None, "registry": []}
    con = duckdb.connect(str(src), read_only=True)
    try:
        try:
            row = con.execute(
                "SELECT snapshot_id, captured_at FROM snapshot_runs "
                "ORDER BY captured_at DESC LIMIT 1"
            ).fetchone()
        except Exception:
            row = None
        if row and row[0]:
            info["snapshot_id"] = str(row[0])
            info["captured_at"] = row[1]
        if info["snapshot_id"]:
            try:
                info["registry"] = [
                    tuple(r)
                    for r in con.execute(
                        "SELECT namespace_id, cluster_role, cluster_name, cluster_host, "
                        "primary_database FROM snapshot_cluster_runs WHERE snapshot_id = ?",
                        [info["snapshot_id"]],
                    ).fetchall()
                ]
            except Exception:
                info["registry"] = []
    finally:
        con.close()
    return info


def _gate_sources(
    source_paths: list[Path],
    *,
    allow_partial: bool,
    allow_stale: bool,
    max_skew_hours: float,
) -> tuple[list[dict], list[Path]]:
    """Fail closed on missing, unpromoted, or mutually stale source files."""
    present = [p for p in source_paths if p.is_file()]
    missing = [p for p in source_paths if not p.is_file()]
    if not present:
        raise SystemExit("No per-cluster DuckDB files were found to merge.")
    if missing and not allow_partial:
        raise SystemExit(
            "Refusing to merge: missing per-cluster file(s):\n  "
            + "\n  ".join(str(p) for p in missing)
            + "\nRun the matching LOAD_*.cmd first, or pass --allow-partial to "
            "merge without them (the merged warehouse will omit those clusters)."
        )

    inventories = [_source_inventory(p) for p in present]
    unpromoted = [i for i in inventories if i["captured_at"] is None]
    if unpromoted and not allow_stale:
        raise SystemExit(
            "Refusing to merge: these per-cluster file(s) have no promoted snapshot "
            "(their load may not have finished):\n  "
            + "\n  ".join(str(i["path"]) for i in unpromoted)
            + "\nFinish (or re-run) the matching LOAD_*.cmd, or pass --allow-stale to override."
        )
    stamped = [i for i in inventories if i["captured_at"] is not None]
    if len(stamped) >= 2 and not allow_stale:
        newest = max(i["captured_at"] for i in stamped)
        oldest = min(i["captured_at"] for i in stamped)
        skew_hours = (newest - oldest).total_seconds() / 3600.0
        if skew_hours > max_skew_hours:
            detail = "\n  ".join(
                f"{i['path']}  (latest snapshot {i['captured_at']})" for i in stamped
            )
            raise SystemExit(
                f"Refusing to merge: per-cluster snapshots are {skew_hours:.1f}h apart "
                f"(limit {max_skew_hours:g}h) — one file looks stale:\n  {detail}\n"
                "Re-run the stale LOAD_*.cmd, or pass --allow-stale to merge anyway."
            )
    return inventories, missing


def merge_cluster_files(
    sources: list[str | Path],
    target: str | Path,
    *,
    lock_wait: float = 600.0,
    allow_partial: bool = False,
    allow_stale: bool = False,
    max_skew_hours: float = DEFAULT_MAX_SKEW_HOURS,
    backup: bool = True,
) -> dict:
    """Combine per-cluster warehouses into one unified snapshot in `target`.

    `sources` is the EXPECTED list of per-cluster files; missing entries abort
    the merge unless `allow_partial` is set.
    """
    import duckdb

    import runner

    from .engine import _ProcessLock

    # Normalize and dedupe: the same file listed twice would double every row.
    source_paths: list[Path] = []
    seen: set[str] = set()
    for entry in sources:
        path = Path(entry)
        try:
            key = str(path.resolve()).lower()
        except OSError:
            key = str(path).lower()
        if key in seen:
            continue
        seen.add(key)
        source_paths.append(path)

    inventories, missing = _gate_sources(
        source_paths,
        allow_partial=allow_partial,
        allow_stale=allow_stale,
        max_skew_hours=max_skew_hours,
    )
    present_paths = [i["path"] for i in inventories]

    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)

    snapshot_id = str(uuid.uuid4())
    merged: dict[str, int] = {}
    namespaces: set[str] = set()
    namespace_source: dict[str, Path] = {}
    backup_path: Path | None = None

    # Same single-instance lock as loader refresh/promote: a merge must never
    # interleave DROP/RENAME sequences with a scheduled load on this file.
    with _ProcessLock(target):
        if backup and target.is_file():
            print("Backing up the analyzer warehouse before the merge ...")
            backup_path = runner._backup_file(target, lock_wait)
            print(f"  backup written: {backup_path}")

        con = runner.open_duck(target, lock_wait)
        try:
            con.execute(
                "CREATE TABLE IF NOT EXISTS snapshot_runs "
                "(snapshot_id VARCHAR PRIMARY KEY, captured_at TIMESTAMP, label VARCHAR, source VARCHAR)"
            )
            con.execute(
                "CREATE TABLE IF NOT EXISTS snapshot_cluster_runs "
                "(snapshot_id VARCHAR, namespace_id VARCHAR, cluster_role VARCHAR, cluster_name VARCHAR, "
                "cluster_host VARCHAR, primary_database VARCHAR, captured_at TIMESTAMP, "
                "PRIMARY KEY (snapshot_id, namespace_id))"
            )
            registry_columns = {
                str(row[1]).lower()
                for row in con.execute("PRAGMA table_info('snapshot_cluster_runs')").fetchall()
            }
            if "cluster_name" not in registry_columns:
                con.execute("ALTER TABLE snapshot_cluster_runs ADD COLUMN cluster_name VARCHAR")

            # Discover the union of analyzer tables across all source files.
            tables: list[str] = []
            for src in present_paths:
                sc = duckdb.connect(str(src), read_only=True)
                try:
                    for t in _analyzer_tables(sc):
                        if t not in tables:
                            tables.append(t)
                finally:
                    sc.close()

            # ATTACH/DETACH cannot run inside a transaction, so attach every source
            # once up front and copy into fresh *_tmp tables (no live table touched).
            aliases: list[tuple[str, Path]] = []
            for index, src in enumerate(present_paths):
                alias = f"src_{index}"
                con.execute(f"ATTACH '{_path_literal(src)}' AS {alias} (READ_ONLY)")
                aliases.append((alias, src))
            try:
                for table in tables:
                    first = True
                    claimed: set[str] = set()
                    tmp_ident = runner.quote_ident(runner.tmp_name(table))
                    for alias, src in aliases:
                        cols = [
                            str(row[0]) for row in con.execute(
                                "SELECT column_name FROM duckdb_columns() "
                                "WHERE database_name = ? AND table_name = ?",
                                [alias, table],
                            ).fetchall()
                        ]
                        if not cols or "snapshot_id" not in cols:
                            continue
                        select_cols = ", ".join(
                            f"'{snapshot_id}' AS snapshot_id" if c == "snapshot_id"
                            else runner.quote_ident(c)
                            for c in cols
                        )
                        qualified = f"{alias}.{runner.quote_ident(table)}"
                        # A namespace is copied from exactly one source file per
                        # table; later files repeating it cannot double-count.
                        where = ""
                        if "namespace_id" in cols and claimed:
                            skip = ", ".join(_string_literal(ns) for ns in sorted(claimed))
                            where = f" WHERE namespace_id IS NULL OR namespace_id NOT IN ({skip})"
                        if first:
                            con.execute(f"DROP TABLE IF EXISTS {tmp_ident}")
                            con.execute(
                                f"CREATE TABLE {tmp_ident} AS "
                                f"SELECT {select_cols} FROM {qualified}{where}"
                            )
                            first = False
                        else:
                            con.execute(
                                f"INSERT INTO {tmp_ident} "
                                f"SELECT {select_cols} FROM {qualified}{where}"
                            )
                        if "namespace_id" in cols:
                            for row in con.execute(
                                f"SELECT DISTINCT namespace_id FROM {qualified}"
                            ).fetchall():
                                if row[0]:
                                    ns = str(row[0])
                                    namespaces.add(ns)
                                    claimed.add(ns)
                                    namespace_source.setdefault(ns, src)
                    if not first:
                        merged[table] = int(
                            con.execute(f"SELECT COUNT(*) FROM {tmp_ident}").fetchone()[0] or 0
                        )
            finally:
                for alias, _src in aliases:
                    con.execute(f"DETACH {alias}")

            # Cluster registry for the merged snapshot: the topology page and the
            # dashboard resolve roles/friendly names from snapshot_cluster_runs,
            # so the merge must publish it exactly like a direct promotion does.
            registry_rows: list[list] = []
            registered: set[str] = set()
            for info in inventories:
                role_hint = _role_from_filename(info["path"])
                for namespace_id, role, name, host, primary_db in info["registry"]:
                    ns = str(namespace_id or "")
                    if not ns or ns in registered:
                        continue
                    registered.add(ns)
                    registry_rows.append(
                        [snapshot_id, ns, str(role or role_hint), str(name or ""),
                         str(host or ""), str(primary_db or "")]
                    )
            for ns in sorted(namespaces - registered):
                # Data from a file loaded before the loader recorded a registry:
                # keep the namespace visible, with the role read off the filename.
                src = namespace_source.get(ns)
                registry_rows.append(
                    [snapshot_id, ns, _role_from_filename(src) if src else "", "", "", ""]
                )

            # Promote all merged *_tmp tables and publish the snapshot in one
            # transaction; a failure leaves the previous warehouse untouched.
            con.execute("BEGIN TRANSACTION")
            try:
                for table in merged:
                    con.execute(f"DROP TABLE IF EXISTS {runner.quote_ident(table)}")
                    con.execute(
                        f"ALTER TABLE {runner.quote_ident(runner.tmp_name(table))} "
                        f"RENAME TO {runner.quote_ident(table)}"
                    )
                con.execute(
                    "INSERT OR REPLACE INTO snapshot_runs (snapshot_id, captured_at, label, source) "
                    "VALUES (?, ?, ?, ?)",
                    [snapshot_id, datetime.now(), "per-cluster merge", "per-cluster-merge"],
                )
                for row in registry_rows:
                    con.execute(
                        "INSERT OR REPLACE INTO snapshot_cluster_runs "
                        "(snapshot_id, namespace_id, cluster_role, cluster_name, cluster_host, "
                        "primary_database, captured_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        row + [datetime.now()],
                    )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

            for table in merged:
                runner.ensure_table(con, table, runner.EXPECTED_COLUMNS.get(table, ()))
            runner.ensure_indexes(con)
            try:
                con.execute("CHECKPOINT")
            except Exception:
                pass
        finally:
            con.close()
    return {
        "snapshot_id": snapshot_id,
        "tables": merged,
        "namespaces": sorted(namespaces),
        "sources": [
            {
                "path": str(i["path"]),
                "snapshot_id": i["snapshot_id"],
                "captured_at": str(i["captured_at"] or ""),
            }
            for i in inventories
        ],
        "missing_sources": [str(p) for p in missing],
        "backup": str(backup_path) if backup_path else "",
    }


def _default_sources_and_target() -> tuple[list[Path], Path]:
    from ..duckdb_store import default_duckdb_path

    base = default_duckdb_path()
    # The full expected set — missing files are the completeness gate's job,
    # not silently dropped here.
    return [base.with_name(f"redshift.{r}.duckdb") for r in EXPECTED_ROLES], base


def main(argv: list[str] | None = None) -> int:
    from .engine import LoaderAlreadyRunningError

    parser = argparse.ArgumentParser(description="Merge per-cluster DuckDB files into the analyzer warehouse.")
    parser.add_argument("--source", action="append", default=None, help="A per-cluster DuckDB file (repeat).")
    parser.add_argument("--target", default=None, help="Unified analyzer warehouse (default: standard path).")
    parser.add_argument(
        "--allow-partial", action="store_true",
        help="Merge even when an expected per-cluster file is missing.",
    )
    parser.add_argument(
        "--allow-stale", action="store_true",
        help="Merge even when the per-cluster snapshots are unpromoted or far apart in time.",
    )
    parser.add_argument(
        "--max-skew-hours", type=float, default=DEFAULT_MAX_SKEW_HOURS,
        help="Largest allowed age spread between per-cluster snapshots (default %(default)s).",
    )
    parser.add_argument("--no-backup", action="store_true", help="Do not back up the warehouse before the merge (not recommended).")
    args = parser.parse_args(argv)

    if args.source:
        sources = [Path(s) for s in args.source]
        from ..duckdb_store import default_duckdb_path

        target = Path(args.target) if args.target else default_duckdb_path()
    else:
        sources, target = _default_sources_and_target()
        if args.target:
            target = Path(args.target)
    try:
        result = merge_cluster_files(
            sources,
            target,
            allow_partial=args.allow_partial,
            allow_stale=args.allow_stale,
            max_skew_hours=args.max_skew_hours,
            backup=not args.no_backup,
        )
    except LoaderAlreadyRunningError as exc:
        print(f"Infraredshift loader is already running: {exc}")
        return 3
    for source in result["sources"]:
        print(f"Source {source['path']}: snapshot {source['snapshot_id']} at {source['captured_at']}")
    if result["missing_sources"]:
        print("Merged WITHOUT missing cluster file(s): " + ", ".join(result["missing_sources"]))
    print(f"Merged {len(result['tables'])} table(s) from {len(result['sources'])} cluster file(s).")
    print(f"Namespaces: {', '.join(result['namespaces']) or 'none'}")
    print(f"Unified warehouse: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
