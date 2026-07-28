"""Regression tests for persisted analyzer settings."""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analyzer.settings import CAPTURE_SELECTION_VERSION, AnalyzerSettings, load_settings, save_settings  # noqa: E402


def test_legacy_default_capture_limit_migrates_to_all_patterns(tmp_path):
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(json.dumps({"capture_query_limit": 1000}), encoding="utf-8")

    settings = load_settings(settings_path)

    assert settings.capture_query_limit == 0
    assert settings.capture_selection_version == CAPTURE_SELECTION_VERSION


def test_versioned_capture_limit_preserves_explicit_cap(tmp_path):
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(
        json.dumps(
            {
                "capture_query_limit": 1000,
                "capture_selection_version": CAPTURE_SELECTION_VERSION,
            }
        ),
        encoding="utf-8",
    )

    settings = load_settings(settings_path)

    assert settings.capture_query_limit == 1000


def test_legacy_capture_selection_adds_child_query_text(tmp_path):
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(
        json.dumps(
            {
                "capture_selection_version": 2,
                "capture_include_tables": ["query_history", "query_text", "query_details"],
            }
        ),
        encoding="utf-8",
    )

    settings = load_settings(settings_path)

    assert settings.capture_include_tables == [
        "query_history",
        "query_text",
        "child_query_text",
        "query_details",
    ]
    assert settings.capture_selection_version == CAPTURE_SELECTION_VERSION


def test_corrupt_settings_are_preserved_and_do_not_block_startup(tmp_path):
    settings_path = tmp_path / "settings.json"
    settings_path.write_text('{"ui_theme": "dark"', encoding="utf-8")

    settings = load_settings(settings_path)

    assert settings == AnalyzerSettings()
    preserved = list(tmp_path.glob("settings.corrupt-*.json"))
    assert len(preserved) == 1
    assert preserved[0].read_text(encoding="utf-8") == '{"ui_theme": "dark"'


def test_settings_save_replaces_file_without_leaving_temporary_file(tmp_path):
    settings_path = tmp_path / "settings.json"
    settings_path.write_text('{"ui_theme": "dark"}', encoding="utf-8")
    settings = AnalyzerSettings(ui_theme="light")

    save_settings(settings, settings_path)

    assert load_settings(settings_path).ui_theme == "light"
    assert not list(tmp_path.glob(".*.tmp"))


def test_valid_json_with_invalid_setting_types_falls_back_field_by_field(tmp_path):
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(
        json.dumps(
            {
                "database_min_query_count": "not-a-number",
                "repeat_min_group_size": {"bad": "type"},
                "root_min_execution_seconds": "invalid",
                "table_sql_overrides": ["not", "a", "mapping"],
                "discovered_databases": "not-a-list",
                "table_review_hide_without_intersection": "false",
            }
        ),
        encoding="utf-8",
    )

    settings = load_settings(settings_path)

    assert settings.database_min_query_count == AnalyzerSettings().database_min_query_count
    assert settings.repeat_min_group_size == AnalyzerSettings().repeat_min_group_size
    assert settings.root_min_execution_seconds == AnalyzerSettings().root_min_execution_seconds
    assert settings.table_sql_overrides == {}
    assert settings.discovered_databases == []
    assert settings.table_review_hide_without_intersection is False
