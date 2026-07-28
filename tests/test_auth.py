from __future__ import annotations

import json

import pytest

from analyzer.auth import (
    credentials_exist,
    save_credentials,
    validate_new_credentials,
    verify_credentials,
)


def test_no_credentials_are_created_implicitly(tmp_path):
    auth_file = tmp_path / "auth.json"

    assert not credentials_exist(auth_file)
    assert not verify_credentials("any-code", "1234", auth_file)
    assert not auth_file.exists()


def test_custom_credentials_roundtrip(tmp_path):
    auth_file = tmp_path / "auth.json"
    save_credentials("my-code", "9999", auth_file)
    assert verify_credentials("my-code", "9999", auth_file)
    assert not verify_credentials("my-code", "1111", auth_file)


def test_no_plaintext_in_auth_file(tmp_path):
    auth_file = tmp_path / "auth.json"
    save_credentials("unique-access", "7391", auth_file)
    raw = auth_file.read_text(encoding="utf-8")
    assert "unique-access" not in raw
    assert "7391" not in raw


@pytest.mark.parametrize(
    ("access_code", "pin"),
    (("short", "1234"), ("long-enough", "abc4"), ("123456", "123456")),
)
def test_weak_first_run_credentials_are_rejected(access_code, pin):
    with pytest.raises(ValueError):
        validate_new_credentials(access_code, pin)


def test_tampered_auth_policy_fails_closed(tmp_path):
    auth_file = tmp_path / "auth.json"
    save_credentials("my-code", "9999", auth_file)
    payload = json.loads(auth_file.read_text(encoding="utf-8"))
    payload["iterations"] = 1
    auth_file.write_text(json.dumps(payload), encoding="utf-8")

    assert not verify_credentials("my-code", "9999", auth_file)


def test_existing_verifier_without_descriptive_kdf_remains_usable(tmp_path):
    auth_file = tmp_path / "auth.json"
    save_credentials("my-code", "9999", auth_file)
    payload = json.loads(auth_file.read_text(encoding="utf-8"))
    payload.pop("kdf")
    auth_file.write_text(json.dumps(payload), encoding="utf-8")

    assert verify_credentials("my-code", "9999", auth_file)


def test_corrupt_auth_file_fails_closed(tmp_path):
    auth_file = tmp_path / "auth.json"
    auth_file.write_text("not-json", encoding="utf-8")

    assert not verify_credentials("anything", "1234", auth_file)
