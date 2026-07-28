from __future__ import annotations

import json

from analyzer.bootstrap import bootstrap_application
from analyzer.portable_config import PORTABLE_FILENAME, portable_config_candidates
from analyzer.runtime_paths import resolve_runtime_paths


def test_runtime_paths_do_not_depend_on_working_directory(tmp_path, monkeypatch) -> None:
    install = tmp_path / "team-package"
    unrelated = tmp_path / "random-shortcut-working-directory"
    local_app = tmp_path / "user-appdata"
    install.mkdir()
    unrelated.mkdir()
    monkeypatch.chdir(unrelated)

    paths = resolve_runtime_paths(
        {
            "REDSHIFT_ANALYZER_LAUNCH_DIR": str(install),
            "LOCALAPPDATA": str(local_app),
        }
    )

    assert paths.install_dir == install.resolve()
    assert paths.settings_file == local_app / "Infraredshift" / "settings.json"
    assert paths.auth_file == local_app / "Infraredshift" / "auth.json"
    assert paths.portable_profile_file == install / PORTABLE_FILENAME
    assert unrelated not in paths.settings_file.parents


def test_relative_overrides_are_install_relative_not_cwd_relative(tmp_path, monkeypatch) -> None:
    install = tmp_path / "Infraredshift"
    cwd = tmp_path / "elsewhere"
    install.mkdir()
    cwd.mkdir()
    monkeypatch.chdir(cwd)

    paths = resolve_runtime_paths(
        {
            "REDSHIFT_ANALYZER_LAUNCH_DIR": str(install),
            "REDSHIFT_ANALYZER_HOME": "user-state",
            "REDSHIFT_DUCKDB_PATH": "warehouse\\team.duckdb",
            "REDSHIFT_ANALYZER_SUPPORT_DIR": "support-bundles",
        }
    )

    assert paths.state_dir == install / "user-state"
    assert paths.duckdb_file == install / "warehouse" / "team.duckdb"
    assert paths.support_dir == install / "support-bundles"


def test_portable_profile_next_to_launcher_wins_over_legacy_cwd(tmp_path, monkeypatch) -> None:
    install = tmp_path / "install"
    cwd = tmp_path / "cwd"
    state = tmp_path / "state"
    install.mkdir()
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    environment = {
        "REDSHIFT_ANALYZER_LAUNCH_DIR": str(install),
        "REDSHIFT_ANALYZER_HOME": str(state),
    }

    candidates = portable_config_candidates(environment)

    assert candidates[0] == install / PORTABLE_FILENAME
    assert candidates[-1] == cwd / PORTABLE_FILENAME


def test_bootstrap_skips_damaged_profile_and_uses_valid_user_profile(tmp_path) -> None:
    install = tmp_path / "install"
    state = tmp_path / "state"
    install.mkdir()
    state.mkdir()
    (install / PORTABLE_FILENAME).write_text("{not-json", encoding="utf-8")
    (state / PORTABLE_FILENAME).write_text(
        json.dumps(
            {
                "format": "redshift-query-anatomy-cluster-profiles",
                "version": 1,
                "contains_credentials": False,
                "profiles": [
                    {
                        "profile": "REDSHIFT_PRODUCER",
                        "display_name": "Business Producer",
                        "namespace_id": "producer-ns",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    environment = {
        "REDSHIFT_ANALYZER_LAUNCH_DIR": str(install),
        "REDSHIFT_ANALYZER_HOME": str(state),
    }

    result = bootstrap_application(environment)

    assert result.profile_path == state / PORTABLE_FILENAME
    assert "Could not read" in result.warning
    assert environment["REDSHIFT_PRODUCER_DISPLAY_NAME"] == "Business Producer"
    assert environment["REDSHIFT_PRODUCER_NAMESPACE_ID"] == "producer-ns"
    assert result.paths.support_dir.is_dir()


def test_bootstrap_never_imports_profile_from_unrelated_working_directory(tmp_path, monkeypatch) -> None:
    install = tmp_path / "install"
    state = tmp_path / "state"
    unrelated = tmp_path / "unrelated"
    install.mkdir()
    unrelated.mkdir()
    (unrelated / PORTABLE_FILENAME).write_text(
        json.dumps(
            {
                "format": "redshift-query-anatomy-cluster-profiles",
                "version": 1,
                "contains_credentials": False,
                "profiles": [
                    {
                        "profile": "REDSHIFT_PRODUCER",
                        "display_name": "Untrusted CWD profile",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(unrelated)
    environment = {
        "REDSHIFT_ANALYZER_LAUNCH_DIR": str(install),
        "REDSHIFT_ANALYZER_HOME": str(state),
    }

    result = bootstrap_application(environment)

    assert result.profile_path is None
    assert "REDSHIFT_PRODUCER_DISPLAY_NAME" not in environment
