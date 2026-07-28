"""Build a plain-text single-file Redshift analyzer launcher.

Unlike redshift_analyzer_fat.py, this launcher does not embed a base64 zip.
It stores each analyzer source/resource file as quoted text lines so security
reviewers can inspect the payload directly.

Email-safe delivery: the canonical transport artifact is
``redshift_analyzer_text.txt`` (not .py), because corporate mail filters often
block or quarantine .py attachments. Python runs the .txt file the same way:

    python redshift_analyzer_text.txt

A .py twin is still written for local convenience when filters are not an issue.
"""
from __future__ import annotations

import base64
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "analyzer"
# Primary product launchers (email-safe .txt preferred for corporate mail).
# Canonical name is Infraredshift (matches the product name).
OUT_PRIMARY_PY = ROOT / "Infraredshift.py"
OUT_PRIMARY_TXT = ROOT / "Infraredshift.txt"
KIT_PRIMARY_TXT = ROOT / "kit" / "Infraredshift.txt"
KIT_PRIMARY_PY = ROOT / "kit" / "Infraredshift.py"
# Legacy names kept as identical twins so existing docs/scripts still work.
OUT = ROOT / "redshift_analyzer_text.py"
OUT_TXT = ROOT / "redshift_analyzer_text.txt"
KIT_TXT = ROOT / "kit" / "redshift_analyzer_text.txt"
KIT_PY = ROOT / "kit" / "redshift_analyzer_text.py"
INCLUDE_SUFFIXES = {".py", ".qss", ".tsv"}
BINARY_SUFFIXES = {".png"}


def main() -> int:
    files, binary_files = collect_files()
    rendered = render_launcher(files, binary_files)
    targets = (
        OUT_PRIMARY_PY,
        OUT_PRIMARY_TXT,
        KIT_PRIMARY_TXT,
        KIT_PRIMARY_PY,
        OUT,
        OUT_TXT,
        KIT_TXT,
        KIT_PY,
    )
    for target in targets:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(rendered, encoding="utf-8", newline="\n")
        print(f"Wrote {target} ({target.stat().st_size:,} bytes)")
    # Build the one operator-facing ZIP from the freshly rendered launcher.
    # Developer/compatibility twins above never appear in that ZIP.
    from build_email_mailsafe import build as build_email_mailsafe

    build_email_mailsafe()
    return 0


def collect_files() -> tuple[dict[str, str], dict[str, bytes]]:
    files: dict[str, str] = {}
    binary_files: dict[str, bytes] = {}
    for path in sorted(PACKAGE.rglob("*")):
        if path.is_dir():
            continue
        if "__pycache__" in path.parts:
            continue
        suffix = path.suffix.lower()
        if suffix not in INCLUDE_SUFFIXES and suffix not in BINARY_SUFFIXES:
            continue
        rel = path.relative_to(ROOT).as_posix()
        if suffix in BINARY_SUFFIXES:
            binary_files[rel] = path.read_bytes()
        else:
            files[rel] = path.read_text(encoding="utf-8")
    # The recoverable loader is launched out-of-process and imports the
    # capture implementation as ``runner``. Embed it beside the analyzer
    # package so a single-file deployment needs no companion script.
    files["runner.py"] = (ROOT / "runner.py").read_text(encoding="utf-8")
    # Path-independent loader entry points (new brand + legacy alias).
    for name in ("infraredshift_loader.py", "databasix_loader.py"):
        path = ROOT / name
        if path.is_file():
            files[name] = path.read_text(encoding="utf-8")
    return files, binary_files


def render_launcher(files: dict[str, str], binary_files: dict[str, bytes]) -> str:
    published_at = datetime.now().astimezone().isoformat(timespec="seconds")
    return f'''#!/usr/bin/env python
"""Plain-text single-file DataBasix launcher.

This file embeds the analyzer package as inspectable Python string literals.
It still requires the pip dependencies: PySide6, pandas, numpy, duckdb,
sqlglot, and redshift-connector.

Email-safe delivery uses the .txt twin (same contents). Corporate filters
often block .py; either form is valid Python:

  python Databas6ix.txt
  python Databas6ix.txt --demo
  python Databas6ix.txt --make-mock --output mock.duckdb
  python Databas6ix.txt --index-duckdb --duckdb-path redshift.duckdb
  python Databas6ix.txt --ingest -- --help

Legacy aliases redshift_analyzer_text.txt / .py are identical twins.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import os
from pathlib import Path
import shutil
import sys
import tempfile


_FILES = {{
{render_files_dict(files)}
}}

_BINARY_FILES = {{
{render_binary_files_dict(binary_files)}
}}


def _source_digest() -> str:
    hasher = hashlib.sha256()
    for name in sorted(_FILES):
        hasher.update(name.encode("utf-8"))
        hasher.update(b"\\0")
        hasher.update(_FILES[name].encode("utf-8"))
        hasher.update(b"\\0")
    for name in sorted(_BINARY_FILES):
        hasher.update(name.encode("utf-8"))
        hasher.update(b"\\0")
        hasher.update(_BINARY_FILES[name].encode("ascii"))
        hasher.update(b"\\0")
    return hasher.hexdigest()[:16]


def _materialized_sources_valid(root: Path) -> bool:
    marker = root / ".complete"
    try:
        if marker.is_symlink() or marker.read_text(encoding="ascii").strip() != _source_digest():
            return False
        for name, expected in _FILES.items():
            path = root / name
            if path.is_symlink() or not path.is_file() or path.read_text(encoding="utf-8") != expected:
                return False
        for name, encoded in _BINARY_FILES.items():
            path = root / name
            if path.is_symlink() or not path.is_file() or path.read_bytes() != base64.b64decode(encoded.encode("ascii")):
                return False
        return True
    except (OSError, UnicodeError):
        return False


def _clear_materialized_root(root: Path) -> None:
    if root.is_symlink():
        root.unlink()
        root.mkdir(parents=True, exist_ok=True)
        return
    root.mkdir(parents=True, exist_ok=True)
    for child in root.iterdir():
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()


def _remove_bytecode(root: Path) -> None:
    # Never trust cached bytecode from a previous process.  The inspectable
    # source embedded in the launcher remains the sole executable authority.
    for path in root.rglob("__pycache__"):
        if path.is_symlink():
            try:
                path.unlink()
            except OSError:
                pass
        elif path.is_dir():
            shutil.rmtree(path, ignore_errors=True)


def _materialize_sources() -> Path:
    root = Path(tempfile.gettempdir()) / "redshift_query_anatomy_text" / f"analyzer_sources_{{_source_digest()}}"
    marker = root / ".complete"
    if _materialized_sources_valid(root):
        _remove_bytecode(root)
        return root
    _clear_materialized_root(root)
    for name, text in _FILES.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8", newline="")
    for name, encoded in _BINARY_FILES.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(base64.b64decode(encoded.encode("ascii")))
    marker.write_text(_source_digest(), encoding="ascii")
    if not _materialized_sources_valid(root):
        raise RuntimeError("DataBasix could not verify its extracted application files.")
    return root


def _install_sources() -> None:
    root = _materialize_sources()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def main(argv: list[str] | None = None) -> int:
    if sys.argv and sys.argv[0]:
        os.environ.setdefault("REDSHIFT_ANALYZER_LAUNCH_DIR", str(Path(sys.argv[0]).resolve().parent))
        os.environ.setdefault("REDSHIFT_ANALYZER_LAUNCH_PATH", str(Path(sys.argv[0]).resolve()))
    os.environ.setdefault("REDSHIFT_ANALYZER_PUBLISHED_AT", {published_at!r})
    parser = argparse.ArgumentParser(description="Plain-text single-file DataBasix launcher.")
    parser.add_argument("--demo", action="store_true", help="Generate demo DuckDB at the default app path, then launch UI.")
    parser.add_argument("--make-mock", action="store_true", help="Generate a mock DuckDB file and exit.")
    parser.add_argument("--output", default=None, help="Mock DuckDB output path for --make-mock.")
    parser.add_argument("--index-duckdb", action="store_true", help="Create local DuckDB performance indexes and exit.")
    parser.add_argument("--duckdb-path", default=None, help="DuckDB path for --index-duckdb / --verify.")
    parser.add_argument("--verify", action="store_true", help="Health-check the DuckDB warehouse and exit (read-only).")
    parser.add_argument("--ingest", action="store_true", help="Delegate remaining args after -- to analyzer.ingest_redshift.")
    parser.add_argument("--loader", action="store_true", help="Run the recoverable DataBasix loader process.")
    parser.add_argument("--loader-gui", action="store_true", help="Open the standalone Infraredshift Loader window.")
    parser.add_argument("--self-check", action="store_true", help="Verify this exact application and its embedded loader, then exit.")
    parser.add_argument("--loader-run-cluster", default=None, metavar="PREFIX", help="Load ONE cluster into its own file (e.g. REDSHIFT_PRODUCER).")
    parser.add_argument("--loader-merge", action="store_true", help="Merge per-cluster DuckDB files into the analyzer warehouse.")
    parser.add_argument("--days", type=float, default=2.0, help="Capture window for --loader-run-cluster.")
    parser.add_argument("remaining", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    _install_sources()
    os.environ.setdefault("INFRAREDSHIFT_BUILD_ID", _source_digest())

    if args.self_check:
        root = _materialize_sources()
        required = (
            root / "runner.py",
            root / "analyzer" / "app.py",
            root / "analyzer" / "loader" / "engine.py",
            root / "analyzer" / "loader" / "gui.py",
        )
        missing = [str(path.relative_to(root)) for path in required if not path.is_file()]
        if missing or not _materialized_sources_valid(root):
            detail = ", ".join(missing) if missing else "embedded source verification failed"
            print(f"Infraredshift self-check: FAIL ({{detail}})", file=sys.stderr)
            return 2
        import runner  # noqa: F401
        from analyzer.loader.engine import LoaderEngine, LoaderRequest  # noqa: F401

        print("Infraredshift self-check: PASS")
        print(f"Build ID: {{_source_digest()}}")
        print(
            "Published: "
            + str(os.environ.get("REDSHIFT_ANALYZER_PUBLISHED_AT") or "unknown")
        )
        print("Application entry point: Infraredshift_APP.txt")
        print("Recoverable loader: embedded")
        print("Redshift reads: separate direct connections; no S3 required")
        print(
            "External metadata: Producer SVV_EXTERNAL_COLUMNS / "
            "explicit 10-database scope"
        )
        return 0

    if args.loader:
        from analyzer.loader.cli import main as loader_main
        return loader_main(args.remaining)

    if args.loader_gui:
        from analyzer.loader.gui import main as loader_gui_main
        remaining = args.remaining
        if remaining and remaining[0] == "--":
            remaining = remaining[1:]
        return loader_gui_main(remaining)

    if args.loader_run_cluster:
        from analyzer.loader.per_cluster import load_one
        return load_one(args.loader_run_cluster, days=args.days, floor_seconds=600.0)

    if args.loader_merge:
        from analyzer.loader.merge import main as merge_main
        return merge_main([])

    if args.ingest:
        from analyzer.ingest_redshift import main as ingest_main
        remaining = args.remaining
        if remaining and remaining[0] == "--":
            remaining = remaining[1:]
        return ingest_main(remaining)

    if args.make_mock:
        from analyzer.mock_data import main as mock_main
        mock_args = []
        if args.output:
            mock_args.extend(["--output", args.output])
        return mock_main(mock_args)

    if args.index_duckdb:
        from analyzer.duckdb_store import DuckDBStore
        store = DuckDBStore(args.duckdb_path)
        with store.connect() as con:
            store.rebuild_indexes(con)
            try:
                count = con.execute("SELECT COUNT(*) FROM duckdb_indexes()").fetchone()[0]
            except Exception:
                count = "unknown"
        print(f"Indexed DuckDB at {{store.path}}. Index count: {{count}}")
        return 0

    if args.verify:
        # Read-only post-install verification: proves the schema is in place
        # and says what is still missing, without opening the GUI. Exit code
        # is non-zero only on a real fault, so it can gate a setup script.
        from analyzer.duckdb_health import FAIL, check_warehouse, format_report
        from analyzer.duckdb_store import default_duckdb_path
        target = args.duckdb_path or default_duckdb_path()
        report = check_warehouse(target)
        print(format_report(report))
        return 1 if report.status == FAIL else 0

    if args.demo:
        from analyzer.duckdb_store import default_duckdb_path
        from analyzer.mock_data import generate_mock_snapshot
        output = default_duckdb_path()
        generate_mock_snapshot(output=output, label="single-file demo")
        print(f"Demo snapshot written to {{output}}")

    from analyzer.app import run
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
'''


def render_files_dict(files: dict[str, str]) -> str:
    chunks: list[str] = []
    for name, text in files.items():
        chunks.append(f"    {name!r}: (")
        if text:
            lines = text.splitlines(keepends=True)
            for line in lines:
                chunks.append(f"        {line!r}")
        else:
            chunks.append("        ''")
        chunks.append("    ),")
    return "\n".join(chunks)


def render_binary_files_dict(files: dict[str, bytes]) -> str:
    chunks: list[str] = []
    for name, payload in files.items():
        chunks.append(f"    {name!r}: {base64.b64encode(payload).decode('ascii')!r},")
    return "\n".join(chunks)


if __name__ == "__main__":
    raise SystemExit(main())
