import json
import pandas as pd

from analyzer.settings import (
    DEFAULT_DATABASE_DISCOVERY_SQL,
    LEGACY_DATABASE_DISCOVERY_SQL,
    UNSAFE_PG_DATABASE_DISCOVERY_SQL,
    is_safe_local_database_discovery_sql,
    extract_database_names,
    load_settings,
)


def test_legacy_workload_database_filter_migrates_to_all_database_catalog_discovery(tmp_path) -> None:
    path = tmp_path / "settings.json"
    path.write_text(
        json.dumps(
            {
                "database_discovery_sql": LEGACY_DATABASE_DISCOVERY_SQL,
                "discovered_databases": ["only_busy_database"],
            }
        ),
        encoding="utf-8",
    )

    settings = load_settings(path)

    assert settings.database_discovery_sql == DEFAULT_DATABASE_DISCOVERY_SQL
    assert settings.discovered_databases == []
    assert "svv_redshift_databases" in settings.database_discovery_sql
    assert "database_type" in settings.database_discovery_sql
    assert "local" in settings.database_discovery_sql


def test_pg_database_cache_is_invalidated_because_it_includes_shared_catalogs(tmp_path) -> None:
    path = tmp_path / "settings.json"
    path.write_text(
        json.dumps(
            {
                "database_discovery_sql": UNSAFE_PG_DATABASE_DISCOVERY_SQL,
                "discovered_databases": ["dev", "awsdatacatalog"],
            }
        ),
        encoding="utf-8",
    )

    settings = load_settings(path)

    assert settings.database_discovery_sql == DEFAULT_DATABASE_DISCOVERY_SQL
    assert settings.discovered_databases == []


def test_only_explicit_local_database_discovery_is_accepted() -> None:
    assert is_safe_local_database_discovery_sql(DEFAULT_DATABASE_DISCOVERY_SQL)
    assert not is_safe_local_database_discovery_sql(UNSAFE_PG_DATABASE_DISCOVERY_SQL)
    assert not is_safe_local_database_discovery_sql("SELECT database_name FROM svv_redshift_databases")


def test_database_name_extraction_drops_shared_and_catalog_rows() -> None:
    frame = pd.DataFrame(
        {
            "database_name": ["dev", "consumer_share", "awsdatacatalog"],
            "database_type": ["local", "shared", "auto mounted catalog"],
        }
    )

    assert extract_database_names(frame) == ["dev"]
