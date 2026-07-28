from __future__ import annotations

import json

from analyzer.portable_config import (
    active_profile_prefixes,
    apply_portable_environment,
    export_portable_config,
    read_portable_environment,
)


def test_portable_cluster_profiles_exclude_credentials(tmp_path) -> None:
    target = tmp_path / "redshift_cluster_profiles.json"
    environment = {
        "REDSHIFT_PRODUCER_ENABLED": "true",
        "REDSHIFT_PRODUCER_DISPLAY_NAME": "Business Producer",
        "REDSHIFT_PRODUCER_NAMESPACE_ID": "namespace-producer",
        "REDSHIFT_PRODUCER_HOST": "producer.example.redshift.amazonaws.com",
        "REDSHIFT_PRODUCER_PORT": "5439",
        "REDSHIFT_PRODUCER_PRIMARY_DATABASE": "dev",
        "REDSHIFT_PRODUCER_TABLE_DATABASES": "must-not-be-transported",
        "REDSHIFT_PRODUCER_USER": "must-not-travel",
        "REDSHIFT_PRODUCER_PASSWORD": "super-secret-password",
        "REDSHIFT_CONSUMER_1_ENABLED": "false",
        "REDSHIFT_CONSUMER_1_DISPLAY_NAME": "Reporting Consumer",
        "REDSHIFT_CONSUMER_1_NAMESPACE_ID": "namespace-consumer",
        "REDSHIFT_CONSUMER_1_HOST": "consumer.example.redshift.amazonaws.com",
        "REDSHIFT_CONSUMER_1_USER": "also-private",
        "REDSHIFT_CONSUMER_1_PASSWORD": "also-secret",
    }

    export_portable_config(target, environment)

    text = target.read_text(encoding="utf-8")
    payload = json.loads(text)
    assert payload["contains_credentials"] is False
    assert "must-not-travel" not in text
    assert "super-secret-password" not in text
    assert "also-private" not in text
    assert "also-secret" not in text
    assert '"user"' not in text.lower()
    assert '"password"' not in text.lower()

    loaded = read_portable_environment(target)
    assert loaded["REDSHIFT_PRODUCER_DISPLAY_NAME"] == "Business Producer"
    assert "REDSHIFT_PRODUCER_HOST" not in loaded
    assert "REDSHIFT_PRODUCER_TABLE_DATABASES" not in loaded
    assert "must-not-be-transported" not in text
    assert loaded["REDSHIFT_CONSUMER_1_DISPLAY_NAME"] == "Reporting Consumer"
    assert loaded["REDSHIFT_CONSUMER_1_ENABLED"] == "false"
    assert not any(key.endswith(("_USER", "_PASSWORD")) for key in loaded)


def test_portable_environment_sets_authoritative_profile_membership() -> None:
    environment = {
        "REDSHIFT_CONSUMER_1_DISPLAY_NAME": "Old FAR Label",
        "REDSHIFT_CONSUMER_4_ENABLED": "true",
    }
    values = {
        "REDSHIFT_PRODUCER_ENABLED": "true",
        "REDSHIFT_PRODUCER_DISPLAY_NAME": "Main Warehouse",
        "REDSHIFT_CONSUMER_1_ENABLED": "true",
        "REDSHIFT_CONSUMER_1_DISPLAY_NAME": "FAR",
        "REDSHIFT_CONSUMER_2_ENABLED": "true",
        "REDSHIFT_CONSUMER_2_DISPLAY_NAME": "Commercial",
        "REDSHIFT_CONSUMER_3_ENABLED": "true",
        "REDSHIFT_CONSUMER_3_DISPLAY_NAME": "Consumer",
    }

    applied = apply_portable_environment(values, environment)

    assert applied == (
        "REDSHIFT_PRODUCER",
        "REDSHIFT_CONSUMER_1",
        "REDSHIFT_CONSUMER_2",
        "REDSHIFT_CONSUMER_3",
    )
    assert active_profile_prefixes(environment) == applied
    assert environment["REDSHIFT_CONSUMER_1_DISPLAY_NAME"] == "FAR"


def test_portable_reader_rejects_credentials_and_unrecognized_profiles(tmp_path) -> None:
    target = tmp_path / "redshift_cluster_profiles.json"
    target.write_text(
        json.dumps(
            {
                "format": "redshift-query-anatomy-cluster-profiles",
                "version": 1,
                "profiles": [
                    {
                        "profile": "REDSHIFT_PRODUCER",
                        "host": "producer.example",
                        "user": "injected-user",
                        "password": "injected-password",
                    },
                    {"profile": "UNAPPROVED_CLUSTER", "host": "bad.example"},
                ],
            }
        ),
        encoding="utf-8",
    )

    import pytest

    with pytest.raises(ValueError, match="Credential field"):
        read_portable_environment(target)


def test_portable_export_preserves_legacy_producer_identity_without_credentials(tmp_path) -> None:
    target = tmp_path / "redshift_cluster_profiles.json"

    export_portable_config(
        target,
        {
            "REDSHIFT_HOST": "legacy-producer.example",
            "REDSHIFT_NAMESPACE_ID": "legacy-namespace",
            "REDSHIFT_DATABASE": "legacy_database",
            "REDSHIFT_USER": "private-user",
            "REDSHIFT_PASSWORD": "private-password",
        },
    )

    loaded = read_portable_environment(target)
    text = target.read_text(encoding="utf-8")
    assert "REDSHIFT_PRODUCER_HOST" not in loaded
    assert "legacy-producer.example" not in text
    assert loaded["REDSHIFT_PRODUCER_NAMESPACE_ID"] == "legacy-namespace"
    assert loaded["REDSHIFT_PRODUCER_PRIMARY_DATABASE"] == "legacy_database"
    assert "private-user" not in text
    assert "private-password" not in text


def test_portable_export_fills_enabled_when_gui_defaults_checked(tmp_path) -> None:
    """GUI treats missing ENABLED as checked when host/namespace is set; JSON must not stay blank."""
    target = tmp_path / "redshift_cluster_profiles.json"

    export_portable_config(
        target,
        {
            "REDSHIFT_HOST": "producer.example",
            "REDSHIFT_NAMESPACE_ID": "ns-producer",
            "REDSHIFT_FRIENDLY": "Producer",
            "REDSHIFT_PORT": "5439",
            "REDSHIFT_DATABASE": "dev",
            # No REDSHIFT_ENABLED / *_ENABLED keys — matches the user's bug.
            "REDSHIFT_CONSUMER_1_HOST": "c1.example",
            "REDSHIFT_CONSUMER_1_NAMESPACE_ID": "ns-c1",
            "REDSHIFT_CONSUMER_1_DISPLAY_NAME": "Consumer 1",
            "REDSHIFT_CONSUMER_2_HOST": "c2.example",
            "REDSHIFT_CONSUMER_2_NAMESPACE_ID": "ns-c2",
            "REDSHIFT_CONSUMER_2_DISPLAY_NAME": "Consumer 2",
            "REDSHIFT_CONSUMER_3_HOST": "c3.example",
            "REDSHIFT_CONSUMER_3_NAMESPACE_ID": "ns-c3",
            "REDSHIFT_CONSUMER_3_DISPLAY_NAME": "Consumer 3",
        },
    )

    payload = json.loads(target.read_text(encoding="utf-8"))
    by_profile = {row["profile"]: row for row in payload["profiles"]}
    assert by_profile["REDSHIFT_PRODUCER"]["enabled"] == "true"
    assert by_profile["REDSHIFT_CONSUMER_1"]["enabled"] == "true"
    assert by_profile["REDSHIFT_CONSUMER_2"]["enabled"] == "true"
    assert by_profile["REDSHIFT_CONSUMER_3"]["enabled"] == "true"
    # No blank/null enabled values.
    assert all(row.get("enabled") in {"true", "false"} for row in payload["profiles"])


def test_portable_export_respects_explicit_disabled(tmp_path) -> None:
    target = tmp_path / "redshift_cluster_profiles.json"
    export_portable_config(
        target,
        {
            "REDSHIFT_ENABLED": "false",
            "REDSHIFT_NAMESPACE_ID": "ns-producer",
            "REDSHIFT_HOST": "producer.example",
            "REDSHIFT_CONSUMER_1_ENABLED": "false",
            "REDSHIFT_CONSUMER_1_NAMESPACE_ID": "ns-c1",
            "REDSHIFT_CONSUMER_1_HOST": "c1.example",
        },
    )
    payload = json.loads(target.read_text(encoding="utf-8"))
    by_profile = {row["profile"]: row for row in payload["profiles"]}
    assert by_profile["REDSHIFT_PRODUCER"]["enabled"] == "false"
    assert by_profile["REDSHIFT_CONSUMER_1"]["enabled"] == "false"


def test_portable_export_accepts_canonical_producer_keys(tmp_path) -> None:
    target = tmp_path / "redshift_cluster_profiles.json"

    export_portable_config(
        target,
        {
            "REDSHIFT_ENV": "PROD",
            "REDSHIFT_ENABLED": "true",
            "REDSHIFT_FRIENDLY": "Business Producer",
            "REDSHIFT_NAMESPACE": "producer-namespace",
            "REDSHIFT_PORT": "5439",
            "REDSHIFT_DATABASE": "dev",
        },
    )

    loaded = read_portable_environment(target)
    assert loaded["REDSHIFT_ENV"] == "PROD"
    assert loaded["REDSHIFT_PRODUCER_ENABLED"] == "true"
    assert loaded["REDSHIFT_PRODUCER_DISPLAY_NAME"] == "Business Producer"
    assert loaded["REDSHIFT_PRODUCER_NAMESPACE_ID"] == "producer-namespace"


def test_portable_reader_accepts_safe_legacy_file_without_declaration(tmp_path) -> None:
    target = tmp_path / "redshift_cluster_profiles.json"
    target.write_text(
        json.dumps(
            {
                "format": "redshift-query-anatomy-cluster-profiles",
                "version": 1,
                "profiles": [],
            }
        ),
        encoding="utf-8",
    )

    assert read_portable_environment(target) == {}
