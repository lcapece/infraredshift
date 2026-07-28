from __future__ import annotations

from analyzer.widgets.cluster_dashboard import _updated_dotenv_text


def test_cluster_flag_update_scrubs_migrated_plaintext_credentials() -> None:
    before = (
        "REDSHIFT_PRODUCER_PASSWORD=do-not-change\n"
        "REDSHIFT_PRODUCER_ENABLED=true\n"
        "REDSHIFT_PRODUCER_DISPLAY_NAME=Old Name\n"
    )

    after = _updated_dotenv_text(
        before,
        {
            "REDSHIFT_PRODUCER_ENABLED": "false",
            "REDSHIFT_PRODUCER_DISPLAY_NAME": "Business Producer",
        },
    )

    assert "do-not-change" not in after
    assert "REDSHIFT_PRODUCER_PASSWORD" not in after
    assert "REDSHIFT_PRODUCER_ENABLED=false" in after
    assert "REDSHIFT_PRODUCER_DISPLAY_NAME=Business Producer" in after


def test_cluster_flag_update_rejects_secret_updates() -> None:
    import pytest

    with pytest.raises(ValueError, match="cannot be written"):
        _updated_dotenv_text("", {"REDSHIFT_PASSWORD": "never-write-this"})
