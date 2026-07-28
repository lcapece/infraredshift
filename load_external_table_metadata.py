#!/usr/bin/env python
"""Producer-only parallel SVV_EXTERNAL_COLUMNS loader with console progress.

Run this in a separate terminal while Databa6ix remains open:

    python load_external_table_metadata.py

The long Redshift work is written to a recovery sidecar DuckDB first. The
script then waits for a brief safe write window and transactionally replaces
only ``external_table_metadata`` in the main DuckDB. It never connects to a
consumer and never touches the workload loader's staging/checkpoint tables.
"""
from __future__ import annotations

import argparse
import concurrent.futures
from datetime import datetime
import importlib.util
import os
from pathlib import Path
import runpy
import sys
import time
import uuid


_REQUIRED_RUNNER_API = (
    "_load_dotenv_if_present",
    "_profile_config",
    "resolve_default_duckdb_path",
    "fetch_frame",
    "stamp_cluster_namespace",
    "write_tmp_table",
    "open_duck",
    "EXTERNAL_TABLE_METADATA_SQL",
)

# Emergency iteration policy: use only this explicit Producer list. Some names
# may resolve to data shares rather than connectable databases; those entries
# are logged and skipped while the other databases continue.
EXTERNAL_METADATA_DATABASES = (
    "datalake_cl",
    "enterprise_datawarehouse",
    "businesslayer",
    "datalake_wealth",
    "datalake_cl1",
    "datalake_cl2",
    "datalake",
    "byod",
    "investors_bank",
    "smart_leads",
)


def _runner_has_required_api(module) -> bool:
    return all(hasattr(module, name) for name in _REQUIRED_RUNNER_API)


def _load_runner_from_path(path: Path):
    spec = importlib.util.spec_from_file_location("runner", path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"Could not load the exact Runner source at {path}.")
    module = importlib.util.module_from_spec(spec)
    # Runner is deliberately installed under its canonical name only after its
    # exact file has been resolved. This prevents an unrelated PyPI module
    # named "runner" from winning normal import resolution.
    sys.modules["runner"] = module
    spec.loader.exec_module(module)
    if not _runner_has_required_api(module):
        missing = [
            name for name in _REQUIRED_RUNNER_API
            if not hasattr(module, name)
        ]
        raise SystemExit(
            f"The resolved Runner at {path} is incompatible; missing: "
            + ", ".join(missing)
        )
    return module


def _load_runner_module():
    """Load only this checkout's or the sibling launcher's embedded Runner."""
    script_dir = Path(__file__).resolve().parent
    local_runner = script_dir / "runner.py"
    if local_runner.is_file():
        return _load_runner_from_path(local_runner)

    launcher = next(
        (
            candidate
            for candidate in (
                script_dir / "Infraredshift.py",
                script_dir / "Infraredshift_APP.txt",
                script_dir / "Infraredshift.txt",
                # Legacy kit names stay accepted so an existing install that
                # has not been re-deployed still resolves its runner.
                script_dir / "DataBa6ix.py",
                script_dir / "DataBa6ix_APP.txt",
                script_dir / "DataBa6ix.txt",
            )
            if candidate.is_file()
        ),
        None,
    )
    if launcher is None:
        raise SystemExit(
            "The matching runner.py is unavailable. Put this script beside "
            "Infraredshift.py, Infraredshift_APP.txt, or Infraredshift.txt, or "
            "run it from the Redshift source folder."
        )
    namespace = runpy.run_path(
        str(launcher),
        run_name="external_metadata_bootstrap",
    )
    materialize = namespace.get("_materialize_sources")
    if not callable(materialize):
        raise SystemExit(f"{launcher} does not expose its packaged sources.")
    source_root = Path(materialize()).resolve()
    embedded_runner = source_root / "runner.py"
    if not embedded_runner.is_file():
        raise SystemExit(
            f"{launcher} did not contain its matching Runner source."
        )
    if str(source_root) not in sys.path:
        sys.path.insert(0, str(source_root))
    return _load_runner_from_path(embedded_runner)


def _redact(value: object) -> str:
    try:
        from analyzer.secrets_store import redact_sensitive_text

        return redact_sensitive_text(value)
    except Exception:
        return str(value)


def _progress(message: str) -> None:
    stamp = datetime.now().astimezone().strftime("%H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Load Producer SVV_EXTERNAL_COLUMNS in parallel into a sidecar "
            "DuckDB, then publish only external_table_metadata."
        )
    )
    parser.add_argument(
        "--duckdb-path",
        default=None,
        help="Main Databa6ix DuckDB. Defaults to the configured application path.",
    )
    parser.add_argument(
        "--sidecar-path",
        default=None,
        help=(
            "Recovery/cache DuckDB. Defaults beside the main database as "
            "<name>.external-metadata.duckdb."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Maximum Producer databases fetched concurrently (default: 4).",
    )
    parser.add_argument(
        "--lock-wait-seconds",
        type=float,
        default=1800.0,
        help="How long to wait for a safe final main-DuckDB write window.",
    )
    parser.add_argument(
        "--no-publish",
        action="store_true",
        help="Build and validate the sidecar only; do not update the main DuckDB.",
    )
    return parser


def _producer_config(runner):
    runner._load_dotenv_if_present()
    args = argparse.Namespace(
        days=1.0,
        floor_seconds=None,
        floor_basis="execution_time",
    )
    active = None
    active_key = getattr(
        runner,
        "ACTIVE_PROFILE_PREFIXES_ENV",
        "DATABA6IX_ACTIVE_PROFILE_PREFIXES",
    )
    if active_key in os.environ:
        active = tuple(
            value.strip().upper()
            for value in str(os.environ.get(active_key) or "").split(",")
            if value.strip()
        )
    if active is not None and "REDSHIFT_PRODUCER" not in active:
        raise SystemExit(
            "REDSHIFT_PRODUCER is not present in redshift_cluster_profiles.json."
        )
    cfg = runner._profile_config(
        args,
        "REDSHIFT_PRODUCER",
        "producer",
    )
    if cfg is None:
        raise SystemExit(
            "REDSHIFT_PRODUCER is disabled. Enable it before loading external metadata."
        )
    return cfg


def _fetch_database(runner, cfg, database: str):
    started = time.monotonic()
    _progress(f"FETCH START  {database}")
    frame = runner.fetch_frame(
        cfg,
        database,
        runner.EXTERNAL_TABLE_METADATA_SQL,
        stage=f"SVV_EXTERNAL_COLUMNS [{database}]",
    )
    frame["source_db"] = database
    frame = runner.stamp_cluster_namespace(frame, cfg)
    elapsed = time.monotonic() - started
    _progress(
        f"FETCH DONE   {database}: {len(frame):,} column row(s) in {elapsed:,.1f}s"
    )
    return database, frame


def fetch_external_metadata(runner, cfg, databases, workers: int):
    """Fetch Producer databases concurrently and return one de-duplicated frame."""
    import pandas as pd

    names = tuple(dict.fromkeys(str(name).strip() for name in databases if str(name).strip()))
    if not names:
        raise RuntimeError("Producer database discovery returned no local databases.")
    worker_count = max(1, min(int(workers), len(names)))
    _progress(
        f"Parallel fetch: {len(names)} Producer database(s), {worker_count} worker(s)"
    )
    frames = []
    skipped = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="external-metadata",
    ) as pool:
        futures = {
            pool.submit(_fetch_database, runner, cfg, database): database
            for database in names
        }
        complete = 0
        for future in concurrent.futures.as_completed(futures):
            database = futures[future]
            try:
                _database, frame = future.result()
            except BaseException as exc:
                skipped.append(database)
                _progress(
                    f"SKIP {database}: unavailable or data share; "
                    f"continuing. Detail: {_redact(exc)}"
                )
            else:
                frames.append(frame)
            complete += 1
            _progress(f"DATABASE PROGRESS {complete}/{len(names)} processed")
    if not frames and skipped:
        raise RuntimeError(
            "No explicit Producer database could be read; skipped: "
            + ", ".join(skipped)
        )
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    identity = [
        column
        for column in (
            "namespace_id",
            "redshift_database_name",
            "schema_name",
            "table_name",
            "column_name",
        )
        if column in combined.columns
    ]
    if identity:
        before = len(combined)
        combined = combined.drop_duplicates(subset=identity, keep="last").reset_index(drop=True)
        removed = before - len(combined)
        if removed:
            _progress(f"Deduplicated {removed:,} repeated catalog row(s)")
    return combined


def _write_sidecar(runner, sidecar: Path, frame, snapshot_id: str) -> int:
    working = sidecar.with_name(sidecar.name + ".partial")
    working.parent.mkdir(parents=True, exist_ok=True)
    if working.is_file():
        working.unlink()
    _progress(f"SIDECAR WRITE {working}")
    runner.write_tmp_table(
        working,
        "external_table_metadata",
        frame,
        snapshot_id,
        runner.EXTERNAL_TABLE_METADATA_SQL,
        0.0,
    )
    con = runner.open_duck(working, 0.0)
    try:
        con.execute("BEGIN TRANSACTION")
        con.execute("DROP TABLE IF EXISTS external_table_metadata")
        con.execute(
            "ALTER TABLE external_table_metadata_tmp "
            "RENAME TO external_table_metadata"
        )
        con.execute("COMMIT")
        rows = int(
            con.execute(
                "SELECT COUNT(*) FROM external_table_metadata"
            ).fetchone()[0]
            or 0
        )
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    os.replace(working, sidecar)
    _progress(f"SIDECAR READY {sidecar}: {rows:,} validated row(s)")
    return rows


def _publish_sidecar(
    runner,
    sidecar: Path,
    target: Path,
    lock_wait_seconds: float,
) -> int:
    if sidecar.resolve() == target.resolve():
        raise ValueError("Sidecar and main DuckDB paths must be different.")
    _progress(
        "PUBLISH WAIT Looking for a safe main-DuckDB write window; "
        "the application may remain open"
    )
    con = runner.open_duck(target, lock_wait_seconds)
    attached = False
    try:
        escaped = str(sidecar.resolve()).replace("'", "''")
        con.execute(f"ATTACH '{escaped}' AS external_metadata_sidecar (READ_ONLY)")
        attached = True
        con.execute("BEGIN TRANSACTION")
        con.execute(
            "CREATE OR REPLACE TABLE external_table_metadata AS "
            "SELECT * FROM external_metadata_sidecar.external_table_metadata"
        )
        rows = int(
            con.execute(
                "SELECT COUNT(*) FROM external_table_metadata"
            ).fetchone()[0]
            or 0
        )
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        if attached:
            try:
                con.execute("DETACH external_metadata_sidecar")
            except Exception:
                pass
        con.close()
    _progress(
        f"PUBLISHED external_table_metadata: {rows:,} row(s) in {target}"
    )
    return rows


def main(argv: list[str] | None = None) -> int:
    options = _parser().parse_args(argv)
    runner = _load_runner_module()

    target = (
        Path(options.duckdb_path).expanduser()
        if options.duckdb_path
        else Path(runner.resolve_default_duckdb_path())
    ).resolve()
    sidecar = (
        Path(options.sidecar_path).expanduser()
        if options.sidecar_path
        else target.with_name(
            f"{target.stem}.external-metadata{target.suffix or '.duckdb'}"
        )
    ).resolve()
    snapshot_id = f"external-metadata-{uuid.uuid4()}"

    _progress("START Producer-only SVV_EXTERNAL_COLUMNS load")
    _progress(f"Main DuckDB: {target}")
    _progress(f"Recovery sidecar: {sidecar}")
    cfg = _producer_config(runner)
    _progress(
        f"Producer profile: {cfg.cluster_name or 'Producer'} "
        f"(namespace {cfg.namespace_id})"
    )
    databases = EXTERNAL_METADATA_DATABASES
    _progress(
        "Emergency Producer database scope: "
        f"{', '.join(databases)} (database discovery disabled; "
        "unavailable/data-share entries are skipped)"
    )
    frame = fetch_external_metadata(
        runner,
        cfg,
        databases,
        options.workers,
    )
    _progress(
        f"FETCH COMPLETE {len(frame):,} unique external-column row(s)"
    )
    sidecar_rows = _write_sidecar(runner, sidecar, frame, snapshot_id)
    if options.no_publish:
        _progress("DONE Sidecar validated; main DuckDB was not changed")
        return 0
    published_rows = _publish_sidecar(
        runner,
        sidecar,
        target,
        options.lock_wait_seconds,
    )
    if published_rows != sidecar_rows:
        raise RuntimeError(
            f"Published row count {published_rows:,} does not match "
            f"sidecar count {sidecar_rows:,}."
        )
    _progress("DONE External table metadata is live; sidecar retained for recovery")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nCanceled; the main DuckDB was not partially updated.", file=sys.stderr)
        raise SystemExit(130)
    except BaseException as exc:
        if isinstance(exc, SystemExit):
            raise
        print(
            f"External metadata load failed safely: {_redact(exc)}",
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(1)
