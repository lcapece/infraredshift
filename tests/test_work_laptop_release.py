from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from pathlib import Path
import zipfile

from tools.build_email_mailsafe import (
    APP_NAME,
    MANIFEST_NAME,
    README_NAME,
    RELEASE_NAMES,
    REQUIREMENTS_NAME,
    build_release,
)


def test_work_laptop_release_has_one_entry_point_and_no_standalone_loaders(
    tmp_path: Path,
) -> None:
    app = tmp_path / "built-app.txt"
    requirements = tmp_path / "built-requirements.txt"
    release = tmp_path / "Infraredshift-WORK-LAPTOP-ONLY.zip"
    app.write_text("# canonical app\n", encoding="utf-8")
    requirements.write_text("duckdb\nredshift-connector\n", encoding="utf-8")

    built = build_release(
        app,
        requirements,
        release,
        generated_at=datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc),
    )

    assert built == release.resolve()
    with zipfile.ZipFile(built, "r") as archive:
        assert tuple(archive.namelist()) == RELEASE_NAMES
        assert archive.testzip() is None
        names = {name.casefold() for name in archive.namelist()}
        assert names == {
            APP_NAME.casefold(),
            REQUIREMENTS_NAME.casefold(),
            README_NAME.casefold(),
            MANIFEST_NAME.casefold(),
        }
        assert not any(
            token in name
            for name in names
            for token in ("runner", "external_metadata_loader", "legacy", ".cmd")
        )
        readme = archive.read(README_NAME).decode("utf-8")
        assert "python Infraredshift_APP.txt" in readme
        assert "--self-check" in readme
        assert "SVV_EXTERNAL_COLUMNS" in readme
        assert "NO S3 ACCESS IS REQUIRED" in readme
        manifest = archive.read(MANIFEST_NAME).decode("utf-8")
        assert hashlib.sha256(app.read_bytes()).hexdigest() in manifest
        assert "Entry point: python Infraredshift_APP.txt" in manifest


def test_release_contract_has_only_one_application_entry_point() -> None:
    entry_points = [
        name for name in RELEASE_NAMES
        if name.casefold().startswith("infraredshift") and name.casefold().endswith(".txt")
    ]
    assert entry_points == [APP_NAME]
