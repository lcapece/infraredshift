"""Password-encrypted cluster-profile export for sharing with teammates.

The security property that matters is negative: credentials must never appear
in the exported file, whatever the source document contains.
"""
from __future__ import annotations

import base64
import json

import pytest

from analyzer.config_export import (
    ConfigExportError,
    EXPORTED_PROFILE_FIELDS,
    MIN_PASSWORD_LENGTH,
    build_payload,
    decrypt_document,
    encrypt_document,
    to_profiles_document,
)

_PASSWORD = "correct horse battery"


def _document() -> dict:
    return {
        "format": "redshift-query-anatomy-cluster-profiles",
        "version": 1,
        "environment": "PROD",
        "profiles": [
            {
                "profile": "REDSHIFT_PRODUCER",
                "display_name": "Producer",
                "namespace_id": "abc-123",
                "port": "5439",
                "primary_database": "dev",
                "floor_seconds": "300",
                "external_schemas": "curated",
                "external_table_patterns": "fact_*",
                # Must not survive the export.
                "user": "admin",
                "password": "hunter2",
                "host": "prod.redshift.amazonaws.com",
            },
            {
                "profile": "REDSHIFT_CONSUMER_1",
                "display_name": "FAR",
                "namespace_id": "def-456",
                "floor_seconds": "30",
            },
        ],
    }


def test_credentials_never_reach_the_exported_file():
    blob = encrypt_document(_document(), _PASSWORD)

    for secret in ("hunter2", "admin", "prod.redshift.amazonaws.com"):
        assert secret not in blob

    profile = decrypt_document(blob, _PASSWORD)["profiles"][0]
    for field in ("password", "user", "host"):
        assert field not in profile


def test_floor_seconds_and_capture_scope_are_carried():
    payload = decrypt_document(encrypt_document(_document(), _PASSWORD), _PASSWORD)
    producer, consumer = payload["profiles"]

    assert producer["floor_seconds"] == "300"
    assert consumer["floor_seconds"] == "30"
    assert producer["external_schemas"] == "curated"
    assert producer["external_table_patterns"] == "fact_*"


def test_only_allowlisted_fields_are_exported():
    profile = build_payload(_document())["profiles"][0]

    assert set(profile).issubset(set(EXPORTED_PROFILE_FIELDS))


def test_wrong_password_is_rejected():
    blob = encrypt_document(_document(), _PASSWORD)

    with pytest.raises(ConfigExportError):
        decrypt_document(blob, "not the password")


def test_tampering_is_detected():
    """Encrypt-then-MAC: an altered file must fail rather than decrypt to junk."""
    envelope = json.loads(encrypt_document(_document(), _PASSWORD))
    raw = bytearray(base64.b64decode(envelope["ciphertext"]))
    raw[5] ^= 1
    envelope["ciphertext"] = base64.b64encode(bytes(raw)).decode("ascii")

    with pytest.raises(ConfigExportError):
        decrypt_document(json.dumps(envelope), _PASSWORD)


def test_tampering_with_the_salt_is_also_detected():
    envelope = json.loads(encrypt_document(_document(), _PASSWORD))
    raw = bytearray(base64.b64decode(envelope["salt"]))
    raw[0] ^= 1
    envelope["salt"] = base64.b64encode(bytes(raw)).decode("ascii")

    with pytest.raises(ConfigExportError):
        decrypt_document(json.dumps(envelope), _PASSWORD)


def test_wrong_password_and_tampering_are_indistinguishable():
    """The holder should not learn which of the two happened."""
    blob = encrypt_document(_document(), _PASSWORD)
    envelope = json.loads(blob)
    raw = bytearray(base64.b64decode(envelope["ciphertext"]))
    raw[0] ^= 1
    envelope["ciphertext"] = base64.b64encode(bytes(raw)).decode("ascii")

    with pytest.raises(ConfigExportError) as wrong_password:
        decrypt_document(blob, "not the password")
    with pytest.raises(ConfigExportError) as tampered:
        decrypt_document(json.dumps(envelope), _PASSWORD)

    assert str(wrong_password.value) == str(tampered.value)


def test_short_passwords_are_refused():
    with pytest.raises(ConfigExportError):
        encrypt_document(_document(), "a" * (MIN_PASSWORD_LENGTH - 1))


def test_each_export_differs_even_with_the_same_password():
    """Random salt and nonce per export; no key/nonce reuse."""
    first = encrypt_document(_document(), _PASSWORD)
    second = encrypt_document(_document(), _PASSWORD)

    assert first != second
    assert decrypt_document(first, _PASSWORD) == decrypt_document(second, _PASSWORD)


def test_a_document_with_no_profiles_is_refused():
    with pytest.raises(ConfigExportError):
        encrypt_document({"profiles": []}, _PASSWORD)


def test_import_produces_a_loadable_profiles_document():
    payload = decrypt_document(encrypt_document(_document(), _PASSWORD), _PASSWORD)

    document = to_profiles_document(payload)

    assert document["format"] == "redshift-query-anatomy-cluster-profiles"
    assert document["contains_credentials"] is False
    assert len(document["profiles"]) == 2


def test_a_foreign_file_is_rejected():
    with pytest.raises(ConfigExportError):
        decrypt_document(json.dumps({"format": "something-else"}), _PASSWORD)
    with pytest.raises(ConfigExportError):
        decrypt_document("not json at all", _PASSWORD)
