from __future__ import annotations

from importlib import util
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest

import runner


SCRIPT = Path(__file__).resolve().parents[1] / "load_external_table_metadata.py"
SPEC = util.spec_from_file_location("external_metadata_oneoff", SCRIPT)
assert SPEC and SPEC.loader
oneoff = util.module_from_spec(SPEC)
SPEC.loader.exec_module(oneoff)


def test_emergency_external_metadata_scope_is_explicit_producer_list() -> None:
    assert oneoff.EXTERNAL_METADATA_DATABASES == (
        "datalake_cl",
        "enterprise_datawarehouse",
        "businesslayer",
        "datalake_wealth",
        "datalake_cl1",
        "datalake_cl2",
        "datalake",
        "byod",
        "investors_bank",
        "smart_leads",
    )
    assert "resolve_table_databases" not in oneoff._REQUIRED_RUNNER_API


def test_runner_resolution_prefers_the_exact_checkout_runner(
    monkeypatch,
) -> None:
    calls = []
    sentinel = object()
    monkeypatch.setattr(
        oneoff,
        "_load_runner_from_path",
        lambda path: calls.append(Path(path).resolve()) or sentinel,
    )

    assert oneoff._load_runner_module() is sentinel
    assert calls == [Path(runner.__file__).resolve()]
    assert oneoff._runner_has_required_api(runner)


def test_runner_resolution_supports_email_safe_launcher_name(
    monkeypatch,
    tmp_path,
) -> None:
    helper = tmp_path / "external_metadata_loader.txt"
    helper.write_text("", encoding="utf-8")
    launcher = tmp_path / "Infraredshift_APP.txt"
    launcher.write_text("", encoding="utf-8")
    source_root = tmp_path / "materialized"
    source_root.mkdir()
    embedded_runner = source_root / "runner.py"
    embedded_runner.write_text("", encoding="utf-8")
    sentinel = object()
    run_paths = []
    loaded_paths = []

    monkeypatch.setattr(oneoff, "__file__", str(helper))
    monkeypatch.setattr(
        oneoff.runpy,
        "run_path",
        lambda path, run_name: (
            run_paths.append((Path(path), run_name))
            or {"_materialize_sources": lambda: source_root}
        ),
    )
    monkeypatch.setattr(
        oneoff,
        "_load_runner_from_path",
        lambda path: loaded_paths.append(Path(path)) or sentinel,
    )

    assert oneoff._load_runner_module() is sentinel
    assert run_paths == [(launcher, "external_metadata_bootstrap")]
    assert loaded_paths == [embedded_runner]


def test_producer_config_does_not_require_private_active_profile_helper(
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "INFRAREDSHIFT_ACTIVE_PROFILE_PREFIXES",
        "REDSHIFT_PRODUCER,REDSHIFT_CONSUMER_1",
    )
    expected = SimpleNamespace(
        namespace_id="producer-ns",
        cluster_name="Producer",
    )
    minimal_runner = SimpleNamespace(
        ACTIVE_PROFILE_PREFIXES_ENV="INFRAREDSHIFT_ACTIVE_PROFILE_PREFIXES",
        _load_dotenv_if_present=lambda: None,
        _profile_config=lambda *_args, **_kwargs: expected,
    )

    assert oneoff._producer_config(minimal_runner) is expected


def _frame(database: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "external_table_key": f"{database}.spectrum.sales",
                "redshift_database_name": database,
                "schema_name": "spectrum",
                "table_name": "sales",
                "column_name": "sale_date",
                "data_type": "date",
                "column_number": 1,
                "partition_key_ordinal": 1,
                "is_nullable": "true",
            }
        ]
    )


def test_parallel_fetch_uses_only_supplied_producer_databases(monkeypatch) -> None:
    cfg = SimpleNamespace(namespace_id="producer-ns")
    calls = []

    def fetch_frame(_cfg, database, sql, stage=""):
        calls.append((database, sql, stage))
        return _frame(database)

    monkeypatch.setattr(runner, "fetch_frame", fetch_frame)

    frame = oneoff.fetch_external_metadata(
        runner,
        cfg,
        ("dev", "warehouse"),
        workers=2,
    )

    assert {call[0] for call in calls} == {"dev", "warehouse"}
    assert all("SVV_EXTERNAL_COLUMNS" in call[2] for call in calls)
    assert set(frame["namespace_id"]) == {"producer-ns"}
    assert len(frame) == 2


def test_parallel_fetch_skips_unavailable_data_share_and_keeps_going(
    monkeypatch,
) -> None:
    cfg = SimpleNamespace(namespace_id="producer-ns")
    calls = []

    def fetch_frame(_cfg, database, sql, stage=""):
        calls.append(database)
        if database == "datalake_cl":
            raise RuntimeError("database cannot be opened")
        return _frame(database)

    monkeypatch.setattr(runner, "fetch_frame", fetch_frame)

    frame = oneoff.fetch_external_metadata(
        runner,
        cfg,
        ("datalake_cl", "enterprise_datawarehouse"),
        workers=2,
    )

    assert set(calls) == {"datalake_cl", "enterprise_datawarehouse"}
    assert frame["redshift_database_name"].tolist() == [
        "enterprise_datawarehouse"
    ]


def test_parallel_fetch_fails_if_no_explicit_producer_entry_is_readable(
    monkeypatch,
) -> None:
    cfg = SimpleNamespace(namespace_id="producer-ns")
    monkeypatch.setattr(
        runner,
        "fetch_frame",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("database cannot be opened")
        ),
    )

    with pytest.raises(
        RuntimeError, match="No explicit Producer database could be read"
    ):
        oneoff.fetch_external_metadata(
            runner,
            cfg,
            ("datalake_cl", "enterprise_datawarehouse"),
            workers=2,
        )


def test_sidecar_publish_replaces_only_external_metadata(tmp_path) -> None:
    sidecar = tmp_path / "metadata.duckdb"
    target = tmp_path / "main.duckdb"
    con = duckdb.connect(str(target))
    try:
        con.execute("CREATE TABLE query_history(marker VARCHAR)")
        con.execute("INSERT INTO query_history VALUES ('keep me')")
        con.execute("CREATE TABLE external_table_metadata(marker VARCHAR)")
        con.execute("INSERT INTO external_table_metadata VALUES ('old')")
    finally:
        con.close()
    frame = _frame("dev")
    frame.insert(0, "namespace_id", "producer-ns")

    rows = oneoff._write_sidecar(runner, sidecar, frame, "snapshot-test")
    published = oneoff._publish_sidecar(runner, sidecar, target, 1.0)

    assert rows == published == 1
    con = duckdb.connect(str(target), read_only=True)
    try:
        assert con.execute(
            "SELECT marker FROM query_history"
        ).fetchone()[0] == "keep me"
        result = con.execute(
            "SELECT namespace_id, redshift_database_name, column_name "
            "FROM external_table_metadata"
        ).fetchone()
        assert result == ("producer-ns", "dev", "sale_date")
    finally:
        con.close()
