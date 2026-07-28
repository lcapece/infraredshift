"""Shared test isolation: keep developer-machine state out of the suite.

The suite must behave identically on a developer machine (real .env, cluster
profiles, unlocked credential session) and on a clean CI checkout. Two leaks
previously made results order-dependent: config discovery falling back to the
repo-root .env / profiles JSON, and os.environ mutations from one test
surviving into the next.
"""
from __future__ import annotations

import os

import pytest


def _clear_credential_session() -> None:
    try:
        from analyzer.secrets_store import clear_session_secrets
    except Exception:
        return
    clear_session_secrets()


@pytest.fixture(autouse=True)
def _isolated_environment(tmp_path_factory):
    saved = os.environ.copy()
    # An empty .env in a per-test launch dir satisfies the dotenv search at
    # its first candidate, so the repo-root fallback never loads the
    # developer's real configuration. Tests that need a specific launch dir
    # override this with monkeypatch.setenv as before.
    launch_dir = tmp_path_factory.mktemp("launch")
    (launch_dir / ".env").write_text("", encoding="utf-8")
    os.environ["REDSHIFT_ANALYZER_LAUNCH_DIR"] = str(launch_dir)
    # Pin profile discovery to a clean empty file so a developer's real
    # redshift_cluster_profiles.json (untracked, sits at the repo root for
    # local runs) can never leak cluster identity into the suite.
    empty_profile = launch_dir / "redshift_cluster_profiles.json"
    empty_profile.write_text(
        '{"format": "redshift-query-anatomy-cluster-profiles", "version": 1, '
        '"contains_credentials": false, "profiles": []}',
        encoding="utf-8",
    )
    os.environ["REDSHIFT_ANALYZER_PROFILE_PATH"] = str(empty_profile)
    _clear_credential_session()
    yield
    os.environ.clear()
    os.environ.update(saved)
    _clear_credential_session()
