from pathlib import Path

import duckdb

import repair_short_tables as repair


def _make_staged_roots(con, snapshot: str = "s1") -> None:
    con.execute(
        f"CREATE TABLE {repair.STAGE_PREFIX}query_history "
        "(snapshot_id VARCHAR, query_id VARCHAR, user_id VARCHAR, user_name VARCHAR, "
        "execution_time VARCHAR, status VARCHAR, result_cache_hit VARCHAR, query_type VARCHAR)"
    )
    con.execute(
        f"INSERT INTO {repair.STAGE_PREFIX}query_history VALUES "
        f"('{snapshot}', '101', '7', 'alice', '310000000', 'success', 'false', 'SELECT'), "
        f"('{snapshot}', '102', '7', 'alice', '420000000', 'success', 'false', 'SELECT'), "
        f"('{snapshot}', '201', '7', 'alice', '500000000', 'success', 'false', 'DELETE'), "
        f"('{snapshot}', '301', '8', 'bob',   '500000000', 'success', 'false', 'SELECT')"
    )
    con.execute(
        f"CREATE TABLE {repair.STAGE_PREFIX}query_text "
        "(snapshot_id VARCHAR, query_id VARCHAR, sequence VARCHAR, text VARCHAR)"
    )
    con.execute(
        f"INSERT INTO {repair.STAGE_PREFIX}query_text VALUES "
        f"('{snapshot}', '101', '0', 'select * from sales where id = 1'), "
        f"('{snapshot}', '102', '0', 'select * from sales where id = 2'), "
        f"('{snapshot}', '201', '0', 'delete from inventory where id = 9'), "
        f"('{snapshot}', '301', '0', 'select * from sales where id = 3')"
    )


def test_scope_reloads_history_text_and_all_query_id_auxiliary_tables():
    assert repair.REFRESH_TABLES == (
        "query_history", "query_text", "query_details", "query_health", "query_explain",
        "query_detail_flow", "table_scan_info"
    )
    assert "child_query_text" in repair.PROTECTED_TABLES
    assert "svv_table_info_all" in repair.PROTECTED_TABLES
    assert "query_history_all" in repair.PROTECTED_TABLES


def test_catalog_snapshot_is_reused_for_query_only_refresh(tmp_path: Path):
    con = duckdb.connect(str(tmp_path / "anchor.duckdb"))
    try:
        con.execute(
            "CREATE TABLE snapshot_runs(snapshot_id VARCHAR, captured_at TIMESTAMP, "
            "label VARCHAR, source VARCHAR)"
        )
        con.execute(
            "INSERT INTO snapshot_runs VALUES "
            "('catalog-snapshot', current_timestamp - INTERVAL 1 DAY, '', ''), "
            "('query-only-newer', current_timestamp, '', '')"
        )
        con.execute("CREATE TABLE svv_table_info_all(snapshot_id VARCHAR, table_name VARCHAR)")
        con.execute("INSERT INTO svv_table_info_all VALUES ('catalog-snapshot', 'sales')")

        assert repair._catalog_anchor_snapshot(con) == "catalog-snapshot"
    finally:
        con.close()


def test_history_gate_removes_singletons_before_full_query_text_fetch():
    import pandas as pd

    frame = pd.DataFrame([
        {"query_id": 1, "user_id": 7, "execution_time": 310_000_000,
         "status": "success", "result_cache_hit": False, "query_type": "SELECT",
         "query_text": "select * from sales where id = 1"},
        {"query_id": 2, "user_id": 7, "execution_time": 320_000_000,
         "status": "success", "result_cache_hit": False, "query_type": "SELECT",
         "query_text": "select * from sales where id = 2"},
        {"query_id": 3, "user_id": 7, "execution_time": 500_000_000,
         "status": "success", "result_cache_hit": False, "query_type": "DELETE",
         "query_text": "delete from inventory"},
        {"query_id": 4, "user_id": 7, "execution_time": 500_000_000,
         "status": "failed", "result_cache_hit": False, "query_type": "SELECT",
         "query_text": "select * from sales where id = 4"},
    ])

    assert repair.preselect_history_ids(frame, 300, 80) == ["1", "2"]


def test_sidecar_sqlglot_uses_full_query_text_and_returns_only_real_repeats(tmp_path: Path):
    import pandas as pd

    sidecar = tmp_path / "sysquery_history.db"
    history = pd.DataFrame([
        {"query_id": 1, "user_id": 7, "execution_time": 310_000_000, "query_type": "SELECT"},
        {"query_id": 2, "user_id": 7, "execution_time": 420_000_000, "query_type": "SELECT"},
        {"query_id": 3, "user_id": 7, "execution_time": 500_000_000, "query_type": "SELECT"},
    ])
    text = pd.DataFrame([
        {"query_id": 1, "sequence": 0, "text": "select * from sales where id = 10"},
        {"query_id": 2, "sequence": 0, "text": "select * from sales where id = 99"},
        {"query_id": 3, "sequence": 0, "text": "select count(*) from inventory"},
    ])
    repair.write_candidate_sidecar(sidecar, history)
    repair.write_candidate_text_sidecar(sidecar, text)

    repeated_ids, representatives = repair.sqlglot_representatives_from_sidecar(sidecar)

    assert repeated_ids == ["2", "1"]
    assert representatives == [2]
    con = duckdb.connect(str(sidecar), read_only=True)
    try:
        methods = con.execute("SELECT DISTINCT method FROM candidate_patterns").fetchall()
    finally:
        con.close()
    assert methods == [("ast",)]


def test_fast_cut_discards_owner_prefix_singletons_before_parent_selection(tmp_path: Path):
    con = duckdb.connect(str(tmp_path / "selector.duckdb"))
    try:
        _make_staged_roots(con)
        repeated_ids, representatives = repair.select_repeated_query_ids(
            con, "s1", minimum_seconds=300, prefix_chars=80
        )
    finally:
        con.close()

    # Alice's literal variants repeat and choose the heavier execution. Bob's
    # otherwise identical SQL is still a singleton for that owner and is cut.
    assert repeated_ids == ["101", "102"]
    assert representatives == [102]


def test_five_minute_floor_is_applied_before_repeat_count(tmp_path: Path):
    con = duckdb.connect(str(tmp_path / "floor.duckdb"))
    try:
        _make_staged_roots(con)
        con.execute(
            f"UPDATE {repair.STAGE_PREFIX}query_history SET execution_time = '299000000' "
            "WHERE query_id = '101'"
        )
        repeated_ids, representatives = repair.select_repeated_query_ids(
            con, "s1", minimum_seconds=300, prefix_chars=80
        )
    finally:
        con.close()

    assert repeated_ids == []
    assert representatives == []


def test_prune_staged_roots_keeps_all_executions_of_repeated_candidates(tmp_path: Path):
    con = duckdb.connect(str(tmp_path / "prune.duckdb"))
    try:
        _make_staged_roots(con)
        repair._prune_staged_roots(con, ["101", "102"])
        history_ids = con.execute(
            f"SELECT query_id FROM {repair.STAGE_PREFIX}query_history ORDER BY query_id"
        ).fetchall()
        text_ids = con.execute(
            f"SELECT query_id FROM {repair.STAGE_PREFIX}query_text ORDER BY query_id"
        ).fetchall()
    finally:
        con.close()

    assert history_ids == [("101",), ("102",)]
    assert text_ids == [("101",), ("102",)]


def test_atomic_promotion_preserves_table_info_and_other_protected_tables(tmp_path: Path):
    con = duckdb.connect(str(tmp_path / "promote.duckdb"))
    try:
        con.execute("CREATE TABLE svv_table_info_all(marker VARCHAR)")
        con.execute("INSERT INTO svv_table_info_all VALUES ('working-table-info')")
        con.execute("CREATE TABLE child_query_text(marker VARCHAR)")
        con.execute("INSERT INTO child_query_text VALUES ('untouched')")
        for table_name in repair.PROMOTE_TABLES:
            con.execute(f"CREATE TABLE {table_name}(marker VARCHAR)")
            con.execute(f"INSERT INTO {table_name} VALUES ('old')")
            con.execute(
                f"CREATE TABLE {repair.STAGE_PREFIX}{table_name} "
                "(snapshot_id VARCHAR, captured_at TIMESTAMP, marker VARCHAR)"
            )
            con.execute(
                f"INSERT INTO {repair.STAGE_PREFIX}{table_name} "
                "VALUES ('s2', current_timestamp, 'new')"
            )
        repair._promote(
            con, "s2", {name: f"SELECT '{name}'" for name in repair.REFRESH_TABLES}
        )

        assert con.execute("SELECT marker FROM svv_table_info_all").fetchone()[0] == "working-table-info"
        assert con.execute("SELECT marker FROM child_query_text").fetchone()[0] == "untouched"
        for table_name in repair.PROMOTE_TABLES:
            assert con.execute(f"SELECT marker FROM {table_name}").fetchone()[0] == "new"
    finally:
        con.close()


def test_restore_backup_keeps_a_pre_restore_safety_copy(tmp_path: Path):
    live = tmp_path / "live.duckdb"
    con = duckdb.connect(str(live))
    con.execute("CREATE TABLE marker(value VARCHAR)")
    con.execute("INSERT INTO marker VALUES ('known-good')")
    con.close()
    backup = repair._backup_file(live, 1.0, reason="known-good")

    con = duckdb.connect(str(live))
    con.execute("UPDATE marker SET value = 'broken'")
    con.close()
    assert repair.restore_backup(live, backup, 1.0) == 0

    con = duckdb.connect(str(live), read_only=True)
    try:
        assert con.execute("SELECT value FROM marker").fetchone()[0] == "known-good"
    finally:
        con.close()
    assert list((tmp_path / "backups").glob("*.before-manual-restore.*.duckdb"))
