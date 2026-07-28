"""Load ONE cluster into its OWN DuckDB file — safe to run 4× in parallel.

Each cluster writes a separate file (redshift.<role>.duckdb), so four of
these can run at once with zero shared state: no cross-process clobbering,
and one cluster hanging can never touch the others or the live warehouse.
A separate merge step (analyzer.loader.merge) combines the per-cluster files
into the unified analyzer warehouse when the user is ready.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


# Every cluster env prefix the runner understands.
_PREFIXES = ("REDSHIFT_PRODUCER",) + tuple(f"REDSHIFT_CONSUMER_{n}" for n in range(1, 8))


def _per_cluster_path(role_key: str) -> Path:
    from ..duckdb_store import default_duckdb_path

    base = default_duckdb_path()
    return base.with_name(f"redshift.{role_key}.duckdb")


def _isolate_single_cluster(prefix: str) -> None:
    """Enable only this cluster; disable the rest so build_configs returns one.

    Credentials for every cluster are already in the unlocked DPAPI session;
    this only flips the ENABLED flags the runner reads from the environment.
    """
    prefix = prefix.upper()
    if prefix == "REDSHIFT_PRODUCER":
        os.environ["REDSHIFT_ENABLED"] = "true"
        os.environ["REDSHIFT_PRODUCER_ENABLED"] = "true"
    else:
        # The producer is the legacy default-on profile; force it off.
        os.environ["REDSHIFT_ENABLED"] = "false"
        os.environ["REDSHIFT_PRODUCER_ENABLED"] = "false"
    for other in _PREFIXES:
        if other == prefix:
            os.environ[f"{other}_ENABLED"] = "true"
        elif other != "REDSHIFT_PRODUCER":
            os.environ[f"{other}_ENABLED"] = "false"


def load_one(prefix: str, *, days: float, floor_seconds: float | None = None, duckdb_path: Path | None = None) -> int:
    import runner

    role_key = prefix.lower().replace("redshift_", "")
    target = Path(duckdb_path) if duckdb_path else _per_cluster_path(role_key)
    target.parent.mkdir(parents=True, exist_ok=True)

    runner._load_dotenv_if_present()
    try:
        from .engine import _unlock_scheduled_credentials

        _unlock_scheduled_credentials()
    except Exception:
        pass

    _isolate_single_cluster(prefix)

    args = argparse.Namespace(
        duckdb_path=str(target),
        days=days,
        floor_seconds=floor_seconds,
        floor_basis="execution_time",
        resume=True,
        lock_wait_seconds=600.0,
        no_backup=False,
        include_external=False,   # excluded this version
        include_tables=None,
        swap=False,
        status=False,
        backup_only=False,
    )
    configs = runner.build_configs(args)
    if len(configs) != 1:
        raise SystemExit(
            f"Expected exactly one enabled cluster for {prefix}, got {len(configs)}. "
            "Check that only this cluster is configured for this script."
        )
    cfg = configs[0]
    print(f"== Loading {cfg.cluster_name} (namespace {cfg.namespace_id}) ==", flush=True)
    print(f"== Own file: {target} — safe to run other clusters at the same time ==\n", flush=True)

    def hook(namespace_id, table_name, source_rows, duckdb_rows, completed, total, status):
        pct = int(100 * completed / total) if total else 0
        print(f"  [{pct:3d}%] {table_name}: {status} ({duckdb_rows:,} rows)", flush=True)

    runner.set_progress_hook(hook)
    try:
        rc = int(runner.run_multi_load(args, configs) or 0)
    finally:
        runner.set_progress_hook(None)
    if rc:
        return rc
    # Single-file cluster: promote its own staging immediately. Nothing else
    # writes this file, so the swap is safe and self-contained.
    print(f"\n== Promoting {cfg.cluster_name} into {target.name} ==", flush=True)
    return int(runner.run_swap(args) or 0)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load one Redshift cluster into its own DuckDB file.")
    parser.add_argument("prefix", help="Cluster env prefix, e.g. REDSHIFT_PRODUCER or REDSHIFT_CONSUMER_1")
    parser.add_argument("--days", type=float, default=2.0)
    # None = per-cluster floors (JSON FLOOR_SECONDS, else producer 300s /
    # consumers 30s).
    parser.add_argument("--floor-seconds", type=float, default=None)
    parser.add_argument("--duckdb-path", default=None)
    args = parser.parse_args(argv)
    try:
        return load_one(
            args.prefix, days=args.days, floor_seconds=args.floor_seconds,
            duckdb_path=args.duckdb_path,
        )
    except BaseException as exc:  # noqa: BLE001 — one cluster's failure is isolated
        print(f"\n!! {args.prefix} load failed: {exc}", file=sys.stderr, flush=True)
        return int(exc.code) if isinstance(exc, SystemExit) and isinstance(exc.code, int) else 1


if __name__ == "__main__":
    raise SystemExit(main())
