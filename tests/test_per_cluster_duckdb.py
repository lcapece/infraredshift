"""Per-cluster DuckDB file isolation: endpoint keying, path derivation, and
first-run legacy-file adoption."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analyzer.settings import (
    per_cluster_duckdb_path,
    resolve_source_cluster_config,
    source_cluster_endpoint,
    source_cluster_endpoint_key,
)
from analyzer.duckdb_store import adopt_legacy_duckdb_file


class _Args:
    connection = "native"
    host = ""
    port = "5439"
    primary_database = ""
    table_databases = ""
    jdbc_url = ""


def _cfg(host="", primary="", tdbs="", port="5439", connection="native", jdbc_url=""):
    a = _Args()
    a.connection, a.host, a.port = connection, host, port
    a.primary_database, a.table_databases, a.jdbc_url = primary, tdbs, jdbc_url
    return resolve_source_cluster_config(a)


def test_endpoint_ignores_database_scope():
    # Same host:port, different primary/table databases -> SAME endpoint key.
    a = _cfg("prod-host", "dev", "db1,db2")
    b = _cfg("prod-host", "analytics", "db1,db2,db3")
    assert source_cluster_endpoint(a) == source_cluster_endpoint(b) == "prod-host:5439"
    assert source_cluster_endpoint_key(a) == source_cluster_endpoint_key(b)


def test_endpoint_differs_by_cluster():
    prod = _cfg("prod-host", "dev", "db1")
    dev = _cfg("dev-host", "dev", "db1")
    assert source_cluster_endpoint_key(prod) != source_cluster_endpoint_key(dev)


def test_endpoint_differs_by_port():
    assert source_cluster_endpoint_key(_cfg("h", port="5439")) != source_cluster_endpoint_key(_cfg("h", port="5440"))


def test_jdbc_endpoint_used_when_jdbc():
    a = _cfg(connection="jdbc", jdbc_url="jdbc:redshift://prod:5439/dev")
    assert source_cluster_endpoint(a) == "jdbc:redshift://prod:5439/dev"
    assert source_cluster_endpoint_key(a)


def test_per_cluster_path_same_dir_distinct_files(tmp_path):
    base = tmp_path / "redshift.duckdb"
    prod = per_cluster_duckdb_path(base, _cfg("prod-host"))
    dev = per_cluster_duckdb_path(base, _cfg("dev-host"))
    assert prod.parent == base.parent
    assert prod != dev
    assert prod.suffix == ".duckdb"
    # db-scope change on same cluster resolves to the SAME file
    prod_rescoped = per_cluster_duckdb_path(base, _cfg("prod-host", primary="other", tdbs="x,y"))
    assert prod_rescoped == prod


def test_per_cluster_path_unconfigured_returns_base(tmp_path):
    base = tmp_path / "redshift.duckdb"
    assert per_cluster_duckdb_path(base, _cfg()) == base  # no host -> base unchanged


def test_slug_is_human_recognizable(tmp_path):
    base = tmp_path / "redshift.duckdb"
    path = per_cluster_duckdb_path(base, _cfg("prod-cluster.abc123.us-east-1.redshift.amazonaws.com"))
    assert "prod-cluster" in path.name


def test_adopt_legacy_file_moves_nonempty_default(tmp_path):
    legacy = tmp_path / "redshift.duckdb"
    legacy.write_bytes(b"DUCKDBDATA" * 100)  # non-empty
    target = tmp_path / "redshift.prod.abc123.duckdb"
    assert adopt_legacy_duckdb_file(legacy, target) is True
    assert target.exists() and not legacy.exists()
    assert target.read_bytes().startswith(b"DUCKDB")


def test_adopt_is_noop_when_target_exists(tmp_path):
    legacy = tmp_path / "redshift.duckdb"
    legacy.write_bytes(b"legacy")
    target = tmp_path / "redshift.prod.duckdb"
    target.write_bytes(b"existing")
    assert adopt_legacy_duckdb_file(legacy, target) is False
    assert legacy.read_bytes() == b"legacy"       # untouched
    assert target.read_bytes() == b"existing"     # not overwritten


def test_adopt_is_noop_when_legacy_missing_or_empty(tmp_path):
    target = tmp_path / "t.duckdb"
    assert adopt_legacy_duckdb_file(tmp_path / "missing.duckdb", target) is False
    empty = tmp_path / "empty.duckdb"
    empty.write_bytes(b"")
    assert adopt_legacy_duckdb_file(empty, target) is False


def test_adopt_is_noop_when_same_path(tmp_path):
    p = tmp_path / "redshift.duckdb"
    p.write_bytes(b"data")
    assert adopt_legacy_duckdb_file(p, p) is False
    assert p.read_bytes() == b"data"
