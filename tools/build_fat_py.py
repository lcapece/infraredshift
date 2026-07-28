"""Build the single-file Redshift analyzer launcher.

The generated file embeds the analyzer package as a base64 zip. This keeps the
locked-down delivery artifact to one Python file while preserving the package
structure internally.
"""
from __future__ import annotations

import base64
from datetime import datetime
import io
from pathlib import Path
import textwrap
import zipfile


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "analyzer"
OUT_PRIMARY_PY = ROOT / "Databas6ix_fat.py"
OUT_PRIMARY_TXT = ROOT / "Databas6ix_fat.txt"
KIT_PRIMARY_TXT = ROOT / "kit" / "Databas6ix_fat.txt"
KIT_PRIMARY_PY = ROOT / "kit" / "Databas6ix_fat.py"
OUT = ROOT / "redshift_analyzer_fat.py"
OUT_TXT = ROOT / "redshift_analyzer_fat.txt"
KIT_TXT = ROOT / "kit" / "redshift_analyzer_fat.txt"
KIT_PY = ROOT / "kit" / "redshift_analyzer_fat.py"


def main() -> int:
    payload = build_zip_payload()
    encoded = base64.b64encode(payload).decode("ascii")
    rendered = render_launcher(encoded)
    for target in (
        OUT_PRIMARY_PY,
        OUT_PRIMARY_TXT,
        KIT_PRIMARY_TXT,
        KIT_PRIMARY_PY,
        OUT,
        OUT_TXT,
        KIT_TXT,
        KIT_PY,
    ):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(rendered, encoding="utf-8", newline="\n")
        print(f"Wrote {target} ({target.stat().st_size:,} bytes)")
    return 0


def build_zip_payload() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for path in PACKAGE.rglob("*"):
            if path.is_dir():
                continue
            if "__pycache__" in path.parts:
                continue
            if path.suffix.lower() not in {".py", ".qss", ".md", ".tsv", ".png"}:
                continue
            zf.write(path, path.relative_to(ROOT).as_posix())
        zf.write(ROOT / "runner.py", "runner.py")
    return buf.getvalue()


def render_launcher(encoded: str) -> str:
    wrapped = "\n".join(textwrap.wrap(encoded, 76))
    published_at = datetime.now().astimezone().isoformat(timespec="seconds")
    return f'''#!/usr/bin/env python
"""Single-file DataBasix launcher.

This file embeds the analyzer package. It still requires the pip dependencies:
PySide6, pandas, numpy, duckdb, sqlglot, and redshift-connector.

Usage:
  python redshift_analyzer_fat.py
  python redshift_analyzer_fat.py --demo
  python redshift_analyzer_fat.py --make-mock --output mock.duckdb
  python redshift_analyzer_fat.py --index-duckdb --duckdb-path redshift.duckdb
  python redshift_analyzer_fat.py --ingest -- --connection native --host ...
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import os
from pathlib import Path
import sys
import tempfile


_PAYLOAD_B64 = """
{wrapped}
"""


def _bundle_zip_path() -> Path:
    root = Path(tempfile.gettempdir()) / "redshift_query_anatomy_fat"
    root.mkdir(parents=True, exist_ok=True)
    data = base64.b64decode(_PAYLOAD_B64.encode("ascii"))
    digest = hashlib.sha256(data).hexdigest()[:16]
    path = root / f"analyzer_bundle_{{digest}}.zip"
    valid = False
    if path.is_file() and not path.is_symlink():
        try:
            valid = hashlib.sha256(path.read_bytes()).digest() == hashlib.sha256(data).digest()
        except OSError:
            valid = False
    if not valid:
        temporary = root / f".{{path.name}}.{{os.getpid()}}.tmp"
        temporary.write_bytes(data)
        os.replace(temporary, path)
    if hashlib.sha256(path.read_bytes()).digest() != hashlib.sha256(data).digest():
        raise RuntimeError("DataBasix could not verify its embedded application bundle.")
    return path


def _install_bundle() -> None:
    path = _bundle_zip_path()
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def main(argv: list[str] | None = None) -> int:
    if sys.argv and sys.argv[0]:
        os.environ.setdefault("REDSHIFT_ANALYZER_LAUNCH_DIR", str(Path(sys.argv[0]).resolve().parent))
        os.environ.setdefault("REDSHIFT_ANALYZER_LAUNCH_PATH", str(Path(sys.argv[0]).resolve()))
    os.environ.setdefault("REDSHIFT_ANALYZER_PUBLISHED_AT", {published_at!r})
    parser = argparse.ArgumentParser(description="Single-file DataBasix launcher.")
    parser.add_argument("--demo", action="store_true", help="Generate demo DuckDB at the default app path, then launch UI.")
    parser.add_argument("--make-mock", action="store_true", help="Generate a mock DuckDB file and exit.")
    parser.add_argument("--output", default=None, help="Mock DuckDB output path for --make-mock.")
    parser.add_argument("--index-duckdb", action="store_true", help="Create local DuckDB performance indexes and exit.")
    parser.add_argument("--duckdb-path", default=None, help="DuckDB path for --index-duckdb.")
    parser.add_argument("--ingest", action="store_true", help="Delegate remaining args after -- to analyzer.ingest_redshift.")
    parser.add_argument("--loader", action="store_true", help="Run the recoverable DataBasix loader process.")
    parser.add_argument("--loader-gui", action="store_true", help="Open the standalone Infraredshift Loader window.")
    parser.add_argument("--loader-run-cluster", default=None, metavar="PREFIX", help="Load ONE cluster into its own file (e.g. REDSHIFT_PRODUCER).")
    parser.add_argument("--loader-merge", action="store_true", help="Merge per-cluster DuckDB files into the analyzer warehouse.")
    parser.add_argument("--days", type=float, default=2.0, help="Capture window for --loader-run-cluster.")
    parser.add_argument("remaining", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    _install_bundle()

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


if __name__ == "__main__":
    raise SystemExit(main())
