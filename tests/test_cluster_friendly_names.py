from __future__ import annotations

from analyzer.widgets.cluster_dashboard import _ConfigDialog
from analyzer.secrets_store import clear_session_secrets, set_session_secret


def test_friendly_name_replaces_numbered_consumer_label(monkeypatch) -> None:
    clear_session_secrets()
    set_session_secret("REDSHIFT_CONSUMER_1_HOST", "consumer.example")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_NAMESPACE_ID", "ns-consumer")
    monkeypatch.setenv("REDSHIFT_CONSUMER_1_DISPLAY_NAME", "Reporting Consumer")

    try:
        profiles = {prefix: values for prefix, *values in _ConfigDialog._cluster_profiles_from_env()}
    finally:
        clear_session_secrets()

    label, configured, namespace_id, host, enabled = profiles["REDSHIFT_CONSUMER_1"]
    assert label == "Reporting Consumer"
    assert configured is True
    assert namespace_id == "ns-consumer"
    assert host == "consumer.example"
