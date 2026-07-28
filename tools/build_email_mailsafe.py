"""Build the one canonical Infraredshift work-laptop handoff.

The repository contains source, compatibility launchers, and developer tools.
None of those choices belong in the operator handoff.  This builder creates
exactly one user-visible deliverable:

    SEND-THIS-ONE/Infraredshift-WORK-LAPTOP-ONLY.zip

The ZIP has one application entry point.  The recoverable loader and its
Redshift capture implementation are embedded inside ``Infraredshift_APP.txt``.
There is deliberately no standalone runner, external-metadata loader, legacy
launcher, profile template, or command file in the release.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
from pathlib import Path
import tempfile
import zipfile


ROOT = Path(__file__).resolve().parents[1]
SEND_DIR = ROOT / "SEND-THIS-ONE"
ZIP_PATH = SEND_DIR / "Infraredshift-WORK-LAPTOP-ONLY.zip"
APP_NAME = "Infraredshift_APP.txt"
REQUIREMENTS_NAME = "requirements.txt"
README_NAME = "README-FIRST.txt"
MANIFEST_NAME = "RELEASE-MANIFEST.txt"
RELEASE_NAMES = (
    APP_NAME,
    REQUIREMENTS_NAME,
    README_NAME,
    MANIFEST_NAME,
)


README = """Infraredshift — one recoverable application
=========================================

THE ONE FILE TO RUN
-------------------
Run only:

    python Infraredshift_APP.txt

Do not run runner.py or a separate external-metadata loader. Both capabilities
are embedded in this application.

SAFE UPGRADE ON THE WORK LAPTOP
-------------------------------
1. Close the old Infraredshift application. If a load is active, use Cancel Load
   first and wait for "checkpoints preserved".
2. Put Infraredshift_APP.txt in the same folder as the prior application and
   replace only the old application file.
3. Do NOT delete or replace:
      redshift_cluster_profiles.json
      redshift.duckdb
      the backups folder
      Windows AppData Infraredshift settings or encrypted credentials
4. Verify the downloaded application:

      python Infraredshift_APP.txt --self-check

5. Launch it:

      python Infraredshift_APP.txt

If packages are missing, install them once:

    python -m pip install -r requirements.txt

ONE LOADING WORKFLOW
--------------------
Use the Data Loader tab inside the main application.

* Start Safe Load starts a new staged load when no recovery exists.
* Resume Safe Load reuses every completed dataset checkpoint.
* Cancel Load stops the child loader; completed checkpoints remain recoverable.
* Promote to Live is enabled only after the required staged checkpoints pass.
* Promotion writes a verified DuckDB backup first and swaps staged tables in
  one transaction.
* A consumer with no qualifying queries is a successful yellow result.
* External table metadata means SVV_EXTERNAL_COLUMNS. It is loaded only from
  the enabled Producer across the explicit 10-database Producer scope, as part
  of the same safe load. Unavailable/data-share entries are logged and skipped;
  the remaining Producer databases continue.
* Consumer catalog datasets remain limited to enterprise_datawarehouse. A
  consumer without that database is a successful yellow zero-row result.

NO S3 ACCESS IS REQUIRED
------------------------
The loader reads Redshift directly. Enabled clusters are read concurrently
using separate Redshift connections. DuckDB writes are serialized so worker
threads never share a Redshift connection or write the warehouse concurrently.

RECOVERY GUARANTEE
------------------
A failed or canceled refresh does not replace the current live snapshot.
Resume the same staged snapshot from the Data Loader tab. A partial snapshot
cannot be promoted. Verified pre-promotion backups are stored beside the
DuckDB warehouse in the backups folder.
"""


def _sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def _write_manifest(release_dir: Path, generated_at: datetime) -> None:
    lines = [
        "Infraredshift canonical work-laptop release",
        f"Built: {generated_at.astimezone().isoformat(timespec='seconds')}",
        "Entry point: python Infraredshift_APP.txt",
        "Loader: embedded in the application",
        "External metadata: Producer SVV_EXTERNAL_COLUMNS / explicit 10-database scope",
        "S3 required: no",
        "",
        "SHA-256:",
    ]
    for name in (APP_NAME, REQUIREMENTS_NAME, README_NAME):
        path = release_dir / name
        lines.append(f"{_sha256(path)}  {name}")
    (release_dir / MANIFEST_NAME).write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
        newline="\r\n",
    )


def build_release(
    app_source: Path,
    requirements_source: Path,
    zip_path: Path,
    *,
    generated_at: datetime | None = None,
) -> Path:
    """Create one ZIP with the exact canonical release contents."""
    app_source = Path(app_source).resolve()
    requirements_source = Path(requirements_source).resolve()
    zip_path = Path(zip_path).resolve()
    if not app_source.is_file():
        raise FileNotFoundError(f"Missing canonical application: {app_source}")
    if not requirements_source.is_file():
        raise FileNotFoundError(f"Missing requirements file: {requirements_source}")
    zip_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="infraredshift-release-") as temporary:
        release_dir = Path(temporary)
        (release_dir / APP_NAME).write_bytes(app_source.read_bytes())
        (release_dir / REQUIREMENTS_NAME).write_bytes(
            requirements_source.read_bytes()
        )
        (release_dir / README_NAME).write_text(
            README,
            encoding="utf-8",
            newline="\r\n",
        )
        _write_manifest(release_dir, generated_at or datetime.now().astimezone())

        actual_names = tuple(sorted(path.name for path in release_dir.iterdir()))
        expected_names = tuple(sorted(RELEASE_NAMES))
        if actual_names != expected_names:
            raise RuntimeError(
                "Canonical release contents changed unexpectedly: "
                f"expected {expected_names}, found {actual_names}"
            )

        temporary_zip = zip_path.with_suffix(zip_path.suffix + ".building")
        if temporary_zip.exists():
            temporary_zip.unlink()
        with zipfile.ZipFile(
            temporary_zip,
            "w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=9,
        ) as archive:
            for name in RELEASE_NAMES:
                archive.write(release_dir / name, arcname=name)

        with zipfile.ZipFile(temporary_zip, "r") as archive:
            archived_names = tuple(sorted(archive.namelist()))
            if archived_names != expected_names:
                raise RuntimeError(
                    "Canonical ZIP verification failed: "
                    f"expected {expected_names}, found {archived_names}"
                )
            bad_member = archive.testzip()
            if bad_member:
                raise RuntimeError(f"Canonical ZIP CRC verification failed: {bad_member}")
        temporary_zip.replace(zip_path)
    return zip_path


def _canonical_app_source() -> Path:
    for candidate in (ROOT / "Infraredshift.txt", ROOT / "Infraredshift.py"):
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        "Missing Infraredshift.txt. Run tools/build_text_py.py to build the application."
    )


def _requirements_source() -> Path:
    for candidate in (
        ROOT / "Infraredshift_requirements.txt",
        ROOT / "redshift_analyzer_requirements.txt",
        ROOT / "analyzer" / "requirements.txt",
    ):
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("Missing Infraredshift requirements file.")


def build() -> Path:
    """Build and report the single operator-facing release ZIP."""
    SEND_DIR.mkdir(parents=True, exist_ok=True)
    # Only this exact known deliverable is replaced. Unexpected files are left
    # untouched and cause an explicit failure instead of being deleted.
    unexpected = [
        path for path in SEND_DIR.iterdir()
        if path.name != ZIP_PATH.name
    ]
    if unexpected:
        raise RuntimeError(
            f"{SEND_DIR} contains unexpected files. Move them elsewhere first: "
            + ", ".join(path.name for path in unexpected)
        )
    release = build_release(
        _canonical_app_source(),
        _requirements_source(),
        ZIP_PATH,
    )
    print(f"Wrote ONE deliverable: {release}")
    print(f"SHA-256: {_sha256(release)}")
    with zipfile.ZipFile(release, "r") as archive:
        for info in archive.infolist():
            print(f"  {info.filename:24} {info.file_size:10,} bytes")
    return release


if __name__ == "__main__":
    build()
