from __future__ import annotations

import json
import os

import pytest

import analyzer.secrets_store as secrets_store
from analyzer.secrets_store import (
    FORMAT,
    clear_session_secrets,
    parse_editor_text,
    redact_sensitive_text,
    read_encrypted_secrets,
    session_secret,
    set_session_secret,
    unlock_secrets_session,
    unlock_scheduled_secrets_session,
    validate_first_run_secrets_state,
    write_encrypted_secrets,
)


pytestmark = pytest.mark.skipif(os.name != "nt", reason="DataBasix uses Windows DPAPI")


def test_encrypted_secrets_are_scrambled_and_round_trip(tmp_path) -> None:
    path = tmp_path / ".secrets"
    values = {
        "REDSHIFT_HOST": "producer.example.redshift.amazonaws.com",
        "REDSHIFT_USER": "demo-user",
        "REDSHIFT_PASSWORD": "demo-password",
    }

    write_encrypted_secrets(path, values, "access-code", "2468")

    text = path.read_text(encoding="utf-8")
    payload = json.loads(text)
    assert payload["format"] == FORMAT
    assert payload["ciphertext"]
    assert "producer.example" not in text
    assert "demo-user" not in text
    assert "demo-password" not in text
    assert read_encrypted_secrets(path, "access-code", "2468") == values


def test_encrypted_secrets_reject_wrong_databasix_credentials(tmp_path) -> None:
    path = tmp_path / ".secrets"
    write_encrypted_secrets(path, {"REDSHIFT_PASSWORD": "hidden"}, "right", "1234")

    with pytest.raises(ValueError):
        read_encrypted_secrets(path, "wrong", "9999")


def test_secrets_editor_rejects_non_secret_configuration() -> None:
    with pytest.raises(ValueError, match="Only server address"):
        parse_editor_text("REDSHIFT_NAMESPACE=not-a-secret\n")


def test_unlocked_secrets_are_not_copied_to_process_environment(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(tmp_path))
    monkeypatch.setenv("REDSHIFT_ANALYZER_LAUNCH_DIR", str(tmp_path))
    monkeypatch.delenv("REDSHIFT_USER", raising=False)
    monkeypatch.delenv("REDSHIFT_PASSWORD", raising=False)
    path = tmp_path / ".secrets"
    write_encrypted_secrets(
        path,
        {"REDSHIFT_USER": "private-user", "REDSHIFT_PASSWORD": "private-password"},
        "access-code",
        "2468",
    )

    clear_session_secrets()
    unlock_secrets_session("access-code", "2468")

    assert session_secret("REDSHIFT_USER") == "private-user"
    assert session_secret("REDSHIFT_PASSWORD") == "private-password"
    assert "REDSHIFT_USER" not in os.environ
    assert "REDSHIFT_PASSWORD" not in os.environ
    clear_session_secrets()


def test_failed_migration_preserves_plaintext_source(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(tmp_path))
    monkeypatch.setenv("REDSHIFT_ANALYZER_LAUNCH_DIR", str(tmp_path))
    env_path = tmp_path / ".env"
    env_path.write_text("REDSHIFT_PASSWORD=only-copy\nREDSHIFT_PORT=5439\n", encoding="utf-8")

    def fail_write(*_args, **_kwargs):
        raise OSError("simulated storage failure")

    monkeypatch.setattr(secrets_store, "write_encrypted_secrets", fail_write)
    with pytest.raises(OSError, match="simulated storage failure"):
        unlock_secrets_session("access-code", "2468")

    assert "REDSHIFT_PASSWORD=only-copy" in env_path.read_text(encoding="utf-8")


def test_secret_payload_with_extra_fields_fails_closed(tmp_path) -> None:
    path = tmp_path / ".secrets"
    write_encrypted_secrets(path, {"REDSHIFT_PASSWORD": "hidden"}, "access-code", "2468")
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["unexpected"] = "value"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="unsupported or missing"):
        read_encrypted_secrets(path, "access-code", "2468")


def test_sensitive_error_text_is_redacted(monkeypatch) -> None:
    monkeypatch.setattr(
        secrets_store,
        "_SESSION_VALUES",
        {"REDSHIFT_USER": "private-user", "REDSHIFT_PASSWORD": "private-password"},
    )

    rendered = redact_sensitive_text(
        "connection user=private-user password=private-password at postgresql://private-user:private-password@host/db"
    )

    assert "private-user" not in rendered
    assert "private-password" not in rendered
    assert "<redacted>" in rendered


def test_session_setter_accepts_only_nonblank_credential_keys() -> None:
    clear_session_secrets()
    set_session_secret("redshift_password", "memory-only")
    assert session_secret("REDSHIFT_PASSWORD") == "memory-only"
    with pytest.raises(ValueError, match="Unsupported"):
        set_session_secret("REDSHIFT_NAMESPACE", "not-a-credential")
    with pytest.raises(ValueError, match="cannot be blank"):
        set_session_secret("REDSHIFT_PASSWORD", "")
    clear_session_secrets()


def test_scheduled_unlock_uses_same_user_dpapi_without_environment(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("REDSHIFT_USER", raising=False)
    monkeypatch.delenv("REDSHIFT_PASSWORD", raising=False)
    path = tmp_path / ".secrets"
    write_encrypted_secrets(
        path,
        {"REDSHIFT_USER": "scheduled-user", "REDSHIFT_PASSWORD": "scheduled-password"},
        "access-code",
        "2468",
    )

    clear_session_secrets()
    values = unlock_scheduled_secrets_session(path)

    assert values["REDSHIFT_USER"] == "scheduled-user"
    assert session_secret("REDSHIFT_PASSWORD") == "scheduled-password"
    assert "REDSHIFT_USER" not in os.environ
    assert "REDSHIFT_PASSWORD" not in os.environ
    clear_session_secrets()


def test_scheduled_unlock_rejects_legacy_v2_until_gui_migration(tmp_path) -> None:
    path = tmp_path / ".secrets"
    clear = json.dumps({"REDSHIFT_PASSWORD": "legacy-password"}).encode("utf-8")
    cipher = secrets_store._dpapi_protect(
        clear,
        secrets_store._credential_entropy("old-access", "2468"),
    )
    path.write_text(
        json.dumps(
            {
                "format": FORMAT,
                "version": secrets_store.LEGACY_VERSION,
                "algorithm": secrets_store.LEGACY_ALGORITHM,
                "ciphertext": secrets_store.base64.b64encode(cipher).decode("ascii"),
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="sign in once"):
        unlock_scheduled_secrets_session(path)


def test_gui_unlock_migrates_legacy_v2_for_scheduling(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(tmp_path))
    monkeypatch.setenv("REDSHIFT_ANALYZER_LAUNCH_DIR", str(tmp_path))
    (tmp_path / ".env").write_text("REDSHIFT_PORT=5439\n", encoding="utf-8")
    path = tmp_path / ".secrets"
    clear = json.dumps({"REDSHIFT_PASSWORD": "legacy-password"}).encode("utf-8")
    cipher = secrets_store._dpapi_protect(
        clear,
        secrets_store._credential_entropy("old-access", "2468"),
    )
    path.write_text(
        json.dumps(
            {
                "format": FORMAT,
                "version": secrets_store.LEGACY_VERSION,
                "algorithm": secrets_store.LEGACY_ALGORITHM,
                "ciphertext": secrets_store.base64.b64encode(cipher).decode("ascii"),
            }
        ),
        encoding="utf-8",
    )

    unlocked = unlock_secrets_session("old-access", "2468")

    assert unlocked["REDSHIFT_PASSWORD"] == "legacy-password"
    assert json.loads(path.read_text(encoding="utf-8"))["version"] == secrets_store.VERSION
    assert unlock_scheduled_secrets_session(path)["REDSHIFT_PASSWORD"] == "legacy-password"
    clear_session_secrets()


def test_gui_unlock_relocates_credentials_out_of_install_directory(tmp_path, monkeypatch) -> None:
    install = tmp_path / "installed-app"
    state = tmp_path / "user-state"
    install.mkdir()
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(state))
    monkeypatch.setenv("REDSHIFT_ANALYZER_LAUNCH_DIR", str(install))
    (install / ".env").write_text("REDSHIFT_PORT=5439\n", encoding="utf-8")
    legacy_path = install / ".secrets"
    write_encrypted_secrets(
        legacy_path,
        {"REDSHIFT_USER": "relocate-user", "REDSHIFT_PASSWORD": "relocate-password"},
        "access-code",
        "2468",
    )

    unlocked = unlock_secrets_session("access-code", "2468")

    destination = state / ".secrets"
    assert unlocked["REDSHIFT_USER"] == "relocate-user"
    assert destination.is_file()
    assert not legacy_path.exists()
    assert unlock_scheduled_secrets_session(destination)["REDSHIFT_PASSWORD"] == "relocate-password"
    clear_session_secrets()


def test_dpapi_boundary_can_be_tested_without_exposing_values(monkeypatch) -> None:
    protected: list[bytes] = []

    def protect(clear: bytes, entropy=None) -> bytes:
        assert entropy is None
        protected.append(clear)
        return b"opaque-ciphertext"

    monkeypatch.setattr(secrets_store, "_dpapi_protect", protect)

    rendered = secrets_store.encrypt_values(
        {"REDSHIFT_PASSWORD": "not-printed"}, "access-code", "2468"
    )

    assert protected
    assert "not-printed" not in rendered


def test_first_run_refuses_to_orphan_existing_encrypted_credentials(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(tmp_path))
    monkeypatch.setenv("REDSHIFT_ANALYZER_LAUNCH_DIR", str(tmp_path))
    write_encrypted_secrets(
        tmp_path / ".secrets",
        {"REDSHIFT_PASSWORD": "preserve-me"},
        "existing-access",
        "2468",
    )

    with pytest.raises(ValueError, match="will not overwrite"):
        validate_first_run_secrets_state()


def test_first_run_rejects_malformed_legacy_credentials_before_auth_creation(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("REDSHIFT_ANALYZER_HOME", str(tmp_path))
    monkeypatch.setenv("REDSHIFT_ANALYZER_LAUNCH_DIR", str(tmp_path))
    (tmp_path / ".secrets").write_text("this is not dotenv\n", encoding="utf-8")

    with pytest.raises(ValueError, match="expected KEY=value"):
        validate_first_run_secrets_state()
