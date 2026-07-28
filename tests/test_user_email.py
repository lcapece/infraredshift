"""Email User - reach the person behind a slow query pattern.

Redshift usernames are frequently service accounts, so "no address available"
is the normal case. Guessing a domain would email a real stranger, so the
feature explains instead.
"""
from __future__ import annotations

import os
from urllib.parse import unquote

import pandas as pd

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from analyzer.user_email import (
    build_body,
    build_mailto,
    build_subject,
    extract_email,
    resolve_recipient,
)

_GROUP = {
    "repeat_group_id": "RQ001",
    "repeat_group_key": "K1",
    "query_count": 9042,
    "total_runtime_s": 36_000.0,
    "sql_tables": "dev.mart.fact_policies, dev.mart.dim_customer",
    "sample_sql": "SELECT 1",
}


def test_an_email_username_is_extracted():
    assert extract_email("jane.doe@bank.com") == "jane.doe@bank.com"
    assert extract_email("Jane Doe <jane.doe@bank.com>") == "jane.doe@bank.com"


def test_an_iam_prefix_is_stripped():
    """Redshift usernames arrive as "IAM:jane.doe@bank.com". Carrying the
    prefix through hands the mail client an address that silently fails."""
    assert extract_email("IAM:jane.doe@bank.com") == "jane.doe@bank.com"
    assert extract_email("IAMR:x@y.com") == "x@y.com"


def test_a_service_account_yields_no_address():
    for name in ("svc_ingestion", "etl_airflow", "bi_looker", "", None):
        assert extract_email(name) == ""


def test_roster_supplies_the_address_when_the_username_does_not():
    roster = pd.DataFrame([{"user_name": "svc_ingestion", "email": "data-eng@bank.com"}])

    assert resolve_recipient("svc_ingestion", roster)[0] == "data-eng@bank.com"


def test_no_domain_is_ever_guessed():
    """A guessed address sends a real email to a real stranger."""
    roster = pd.DataFrame([{"user_name": "someone_else", "email": "x@y.com"}])

    email, name = resolve_recipient("etl_airflow", roster)

    assert email == ""
    assert name == "etl_airflow"


def test_subject_names_the_tables():
    subject = build_subject(_GROUP)

    assert "fact_policies" in subject
    assert "dim_customer" in subject
    assert "Redshift" in subject


def test_subject_falls_back_to_the_pattern_id():
    subject = build_subject({"repeat_group_id": "RQ007"})

    assert "RQ007" in subject


def test_body_explains_the_evidence_in_plain_terms():
    body = build_body(_GROUP, display_name="jane.doe@bank.com")

    assert "9,042 times" in body
    assert "10.0 hours" in body
    assert "dev.mart.fact_policies" in body
    # Three concrete questions, not an open-ended complaint.
    assert "1." in body and "2." in body and "3." in body


def test_body_does_not_accuse_the_recipient():
    """They did not ask to be emailed; a query running often is normal."""
    body = build_body(_GROUP).lower()

    assert "nothing is wrong on your side" in body
    for word in ("bad query", "your fault", "problem you", "misuse"):
        assert word not in body


def test_body_greets_a_person_by_name():
    body = build_body(_GROUP, display_name="jane.doe@bank.com")

    assert body.startswith("Hi Jane Doe,")


def test_mailto_is_addressed_and_encoded():
    url = build_mailto("jane.doe@bank.com", "Subject here", "Body here")

    assert url.startswith("mailto:jane.doe%40bank.com")
    assert "subject=Subject%20here" in url
    assert unquote(url).endswith("Body here")


def test_mailto_is_trimmed_rather_than_left_too_long():
    """Outlook opens a blank message when the URL exceeds the command-line
    cap, rather than reporting an error - so trim instead."""
    url = build_mailto("a@b.co", "S", "x" * 20_000)

    assert len(url) <= 1800
    assert "shortened" in unquote(url)


def test_email_action_is_on_the_bubble_context_menu():
    from PySide6.QtCore import QPoint
    from PySide6.QtWidgets import QApplication, QMenu

    app = QApplication.instance() or QApplication([])
    _ = app
    import analyzer.widgets.triage_home as module

    seen = {}

    class _Menu(QMenu):
        def exec(self, *args, **kwargs):
            seen["items"] = [a.text() for a in self.actions() if a.text()]
            return None

    original = module.QMenu
    module.QMenu = _Menu
    try:
        page = module.TriagePage()
        page.set_dataframes(
            pd.DataFrame([_GROUP | {"total_input_rows": 10, "query_ids": "1"}]),
            pd.DataFrame(), pd.DataFrame(), {"total_runtime_s": 36_000.0},
        )
        page._show_group_context_menu("RQ001", QPoint(0, 0))
    finally:
        module.QMenu = original

    assert any("Email User" in text for text in seen.get("items", []))


def test_service_account_explains_instead_of_opening_a_client(monkeypatch):
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])
    _ = app
    import analyzer.widgets.triage_home as module

    opened, shown = [], []
    monkeypatch.setattr(
        module.QDesktopServices, "openUrl", staticmethod(lambda url: opened.append(url) or True)
    )
    monkeypatch.setattr(
        module.QMessageBox, "information",
        staticmethod(lambda parent, title, message: shown.append(message)),
    )

    page = module.TriagePage()
    members = pd.DataFrame(
        [{"repeat_group_id": "RQ001", "repeat_group_key": "K1",
          "query_id": "1", "user_name": "svc_ingestion", "member_rank": 1}]
    )
    page.set_dataframes(
        pd.DataFrame([_GROUP | {"total_input_rows": 10, "query_ids": "1"}]),
        members, pd.DataFrame(), {"total_runtime_s": 36_000.0},
    )
    page._email_group_user(_GROUP)

    assert not opened, "must not mail a guessed address"
    assert shown and "svc_ingestion" in shown[0]
