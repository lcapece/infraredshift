"""The optional decomposer bridge must never break the app.

Infraredshift ships as a single concatenated file to a locked-down machine. A hard
dependency on an optional package would be fatal, so every path here degrades
to a labelled "unavailable" result rather than raising.
"""

from __future__ import annotations

import builtins

import pytest

from analyzer import decomposer_bridge as bridge


@pytest.fixture(autouse=True)
def _clear_cache():
    bridge.availability.cache_clear()
    yield
    bridge.availability.cache_clear()


def test_missing_package_is_reported_not_raised(monkeypatch):
    real_import = builtins.__import__

    def fake(name, *args, **kwargs):
        if name == bridge.IMPORT_NAME:
            raise ImportError("No module named 'redshift_decomposer'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake)
    state = bridge.availability()
    assert not state.installed
    assert not state.usable
    assert "not installed" in state.status_text
    assert "pip install" in state.install_hint


def test_assess_without_the_package_returns_a_result(monkeypatch):
    real_import = builtins.__import__

    def fake(name, *args, **kwargs):
        if name == bridge.IMPORT_NAME:
            raise ImportError("nope")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake)
    result = bridge.assess("SELECT a FROM t")
    assert not result.available
    assert not result.likely_worth_decomposing
    assert result.error


def test_too_old_a_version_is_named(monkeypatch):
    class Stub:
        __version__ = "0.1.0"

    monkeypatch.setattr(builtins, "__import__", lambda name, *a, **k: Stub())
    state = bridge.availability()
    assert state.installed
    assert not state.usable
    assert "older" in state.reason


def test_missing_api_is_named(monkeypatch):
    """A build without assess_decomposability must be reported, not crash later."""

    class Stub:
        __version__ = "0.3.0"  # new enough, but lacks the function

    monkeypatch.setattr(builtins, "__import__", lambda name, *a, **k: Stub())
    state = bridge.availability()
    assert state.installed
    assert not state.usable
    assert "assess_decomposability" in state.reason


def test_status_line_always_renders():
    text = bridge.status_line()
    assert isinstance(text, str) and text.strip()
    assert "Decomposability triage" in text


def test_empty_sql_is_handled():
    result = bridge.assess("")
    assert not result.likely_worth_decomposing


def test_low_score_is_not_recommended():
    assessment = bridge.DecomposabilityAssessment(
        available=True, score=0.2, parse_ok=True
    )
    assert not assessment.likely_worth_decomposing


def test_high_score_is_recommended():
    assessment = bridge.DecomposabilityAssessment(
        available=True, score=0.85, parse_ok=True
    )
    assert assessment.likely_worth_decomposing


def test_unparseable_sql_is_never_recommended():
    assessment = bridge.DecomposabilityAssessment(
        available=True, score=0.9, parse_ok=False
    )
    assert not assessment.likely_worth_decomposing
