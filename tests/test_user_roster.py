"""User roster: parse PG_USER names; persist across reloads."""
from __future__ import annotations

import duckdb
import pandas as pd

from analyzer.duckdb_store import PRESERVED_TABLES, DuckDBStore
from analyzer.user_roster import build_user_roster, parse_username


def test_parse_all_username_formats():
    p = parse_username("firstname.lastname@citizensbank.com")
    assert (p.first_name, p.middle_name, p.last_name, p.domain) == (
        "firstname", "", "lastname", "citizensbank.com",
    )
    p = parse_username("jane.doe@citizensprivatebank.com")
    assert (p.first_name, p.last_name, p.domain) == ("jane", "doe", "citizensprivatebank.com")
    # IAM: prefix stripped
    p = parse_username("IAM:john.smith@citizensbank.com")
    assert (p.first_name, p.last_name) == ("john", "smith") and p.parsed
    # middle initial
    p = parse_username("louis.n.capece@citizensbank.com")
    assert (p.first_name, p.middle_name, p.middle_initial, p.last_name) == (
        "louis", "n", "N", "capece",
    )
    # full middle name -> initial is first letter
    p = parse_username("mary.jane.watson@citizensbank.com")
    assert (p.first_name, p.middle_name, p.middle_initial, p.last_name) == (
        "mary", "jane", "J", "watson",
    )


def test_unparseable_username_is_kept_not_dropped():
    p = parse_username("serviceaccount")
    assert p.parsed is False
    assert p.user_name == "serviceaccount"


def test_build_roster_dedupes_and_shapes_columns():
    df = pd.DataFrame(
        [
            {"user_id": 1, "user_name": "louis.n.capece@citizensbank.com"},
            {"user_id": 2, "user_name": "IAM:jane.doe@citizensbank.com"},
            {"user_id": 1, "user_name": "louis.n.capece@citizensbank.com"},  # dup
        ]
    )
    roster = build_user_roster(df)
    assert len(roster) == 2
    assert set(roster.columns) >= {
        "user_id", "user_name", "first_name", "middle_initial", "last_name", "domain",
    }
    capece = roster[roster["last_name"] == "capece"].iloc[0]
    assert capece["middle_initial"] == "N"


def test_roster_survives_full_truncate_but_normal_tables_do_not():
    assert "user_roster" in PRESERVED_TABLES
    store = DuckDBStore(":memory:")
    with store.connect() as con:
        run = store.new_snapshot("t")
        store.record_snapshot(con, run)
        store.replace_table_from_frame(
            con, "user_roster",
            pd.DataFrame([{"user_id": 1, "user_name": "jane.doe@citizensbank.com",
                           "first_name": "jane", "last_name": "doe"}]),
            run,
        )
        store.replace_table_from_frame(con, "query_history", pd.DataFrame([{"query_id": "1"}]), run)

        store.truncate_all_tables(con)

        assert con.execute("SELECT COUNT(*) FROM user_roster").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM query_history").fetchone()[0] == 0
