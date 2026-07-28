"""External (Spectrum) capture scope - the filter that keeps huge catalogs loadable.

SVV_EXTERNAL_COLUMNS is ONE ROW PER COLUMN, so a catalog with millions of
external tables is tens of millions of rows. The restriction has to run on
Redshift; anything filtered after the fetch has already been paid for.
"""
from __future__ import annotations

import duckdb
import pytest

import runner
from analyzer.redshift_queries import (
    EXTERNAL_TABLE_METADATA_SQL,
    EXTERNAL_TABLES_CATALOG_SQL,
    _glob_to_like,
    external_capture_predicate,
    external_catalog_sql,
    external_metadata_sql,
)


def _catalog_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(
        """CREATE TABLE svv_external_columns(
             redshift_database_name VARCHAR, schemaname VARCHAR, tablename VARCHAR,
             columnname VARCHAR, external_type VARCHAR, columnnum INT,
             part_key INT, is_nullable VARCHAR)"""
    )
    rows = []
    for schema, table in [
        ("spectrum", "fact_orders"),
        ("spectrum", "factx_orders"),
        ("spectrum", "dim_date"),
        ("raw", "fact_events"),
        ("junk", "fact_ignoreme"),
        ("spectrum", "staging_tmp"),
    ]:
        for number, (column, dtype) in enumerate(
            [("id", "bigint"), ("dt", "date")], start=1
        ):
            rows.append(
                ("warehouse", schema, table, column, dtype, number,
                 1 if column == "dt" else 0, "true")
            )
    con.executemany(
        "INSERT INTO svv_external_columns VALUES (?,?,?,?,?,?,?,?)", rows
    )
    return con


def test_no_scope_returns_the_statement_unchanged() -> None:
    assert external_capture_predicate((), ()) == ""
    assert external_metadata_sql((), ()) == EXTERNAL_TABLE_METADATA_SQL
    assert external_catalog_sql((), ()) == EXTERNAL_TABLES_CATALOG_SQL


def test_glob_escapes_literal_underscores() -> None:
    """``_`` is a SQL wildcard and table names are full of them.

    A naive ``*`` -> ``%`` swap would let ``fact_*`` also match
    ``factX_orders``, silently widening the capture it was meant to narrow.
    """
    assert _glob_to_like("fact_*") == r"fact\_%"
    assert _glob_to_like("dim_?") == r"dim\__"
    assert _glob_to_like("pct%weird") == r"pct\%weird"


def test_predicate_quotes_are_escaped() -> None:
    assert "''" in external_capture_predicate(["o'brien"], ())


def test_filtered_catalog_excludes_near_miss_table_names() -> None:
    con = _catalog_db()
    try:
        unfiltered = con.execute(EXTERNAL_TABLES_CATALOG_SQL).fetchall()
        filtered = con.execute(
            external_catalog_sql(["spectrum", "raw"], ["fact_*", "dim_*"])
        ).fetchall()
    finally:
        con.close()

    assert len(unfiltered) == 6
    names = sorted(row[0] for row in filtered)
    assert names == [
        "warehouse.raw.fact_events",
        "warehouse.spectrum.dim_date",
        "warehouse.spectrum.fact_orders",
    ]
    # factx_orders must NOT match fact_* - that is the escaping working.
    assert not any("factx" in name for name in names)


def test_filtered_metadata_reduces_rows_fetched() -> None:
    con = _catalog_db()
    try:
        total = con.execute("SELECT COUNT(*) FROM svv_external_columns").fetchone()[0]
        filtered = con.execute(
            external_metadata_sql(["spectrum", "raw"], ["fact_*", "dim_*"])
        ).fetchall()
    finally:
        con.close()

    assert total == 12
    assert len(filtered) == 6, "the filter must cut what crosses the wire"


def test_catalog_predicate_lands_before_the_group_by() -> None:
    """Filtering the grouped result would still scan the whole catalog."""
    sql = external_catalog_sql(["spectrum"], ())

    assert sql.index("WHERE") < sql.index("GROUP BY")


def test_runner_keeps_an_equivalent_standalone_copy() -> None:
    """runner.py is standalone-importable and duplicates these helpers."""
    for schemas, patterns in (
        ((), ()),
        (["spectrum"], []),
        ([], ["fact_*"]),
        (["spectrum", "raw"], ["fact_*", "dim_?"]),
    ):
        assert runner.external_capture_predicate(schemas, patterns) == (
            external_capture_predicate(schemas, patterns)
        )


def test_scope_is_read_from_the_profile_environment(monkeypatch) -> None:
    from analyzer.ingest_redshift import external_capture_scope

    monkeypatch.setenv("REDSHIFT_PRODUCER_EXTERNAL_SCHEMAS", "spectrum, raw")
    monkeypatch.setenv("REDSHIFT_PRODUCER_EXTERNAL_TABLE_PATTERNS", "fact_*; dim_*")

    assert external_capture_scope() == (("spectrum", "raw"), ("fact_*", "dim_*"))
    assert runner.external_capture_scope() == (("spectrum", "raw"), ("fact_*", "dim_*"))


def test_profile_json_accepts_the_new_scope_keys() -> None:
    """runner.py allowlists profile fields; unlisted keys are silently dropped."""
    import inspect

    source = inspect.getsource(runner)
    marker = source.index("allowed = {")
    block = source[marker:marker + 500]
    assert "external_schemas" in block
    assert "external_table_patterns" in block
