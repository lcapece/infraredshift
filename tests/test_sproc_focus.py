import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analyzer.sproc_focus import executable_statement_spans


PROC = """CREATE OR REPLACE PROCEDURE etl.refresh_daily(p_day DATE)
AS $$
DECLARE
    v_rows BIGINT;
    rec RECORD;
BEGIN
    -- clear staging first
    TRUNCATE staging.daily_work;

    INSERT INTO staging.daily_work
    SELECT * FROM raw.events WHERE event_day = p_day;

    v_rows := (SELECT COUNT(*) FROM staging.daily_work);

    IF v_rows = 0 THEN
        RAISE INFO 'nothing to do';
        RETURN;
    END IF;

    FOR rec IN SELECT DISTINCT account_id FROM staging.daily_work LOOP
        UPDATE marts.accounts SET last_seen = p_day
        WHERE account_id = rec.account_id;
    END LOOP;

    ANALYZE marts.accounts;
END;
$$ LANGUAGE plpgsql;
"""


def _highlighted(text, spans):
    return [text[s:e] for s, e in spans]


def test_executable_statements_found():
    spans = executable_statement_spans(PROC)
    joined = "\n".join(_highlighted(PROC, spans))
    assert "TRUNCATE staging.daily_work" in joined
    assert "INSERT INTO staging.daily_work" in joined
    assert "SELECT COUNT(*) FROM staging.daily_work" in joined
    assert "SELECT DISTINCT account_id FROM staging.daily_work" in joined
    assert "UPDATE marts.accounts" in joined
    assert "ANALYZE marts.accounts" in joined


def test_scaffolding_not_highlighted():
    spans = executable_statement_spans(PROC)
    joined = "\n".join(_highlighted(PROC, spans))
    assert "CREATE OR REPLACE PROCEDURE" not in joined
    assert "DECLARE" not in joined
    assert "v_rows BIGINT" not in joined
    assert "RAISE INFO" not in joined
    assert "LANGUAGE plpgsql" not in joined
    assert "-- clear staging first" not in joined


def test_comments_and_strings_do_not_open_statements():
    sql = """BEGIN
    -- select this comment must stay dim
    RAISE INFO 'select inside a string';
    DELETE FROM audit.log WHERE note = '; select 1';
END;
"""
    spans = executable_statement_spans(sql)
    joined = "\n".join(_highlighted(sql, spans))
    assert "DELETE FROM audit.log" in joined
    assert "comment must stay dim" not in joined
    assert "RAISE INFO" not in joined


def test_plain_query_still_highlights_itself():
    sql = "SELECT a, b FROM t WHERE a > 1;"
    spans = executable_statement_spans(sql)
    assert _highlighted(sql, spans) == [sql.rstrip("\n")]


def test_empty_input():
    assert executable_statement_spans("") == []
    assert executable_statement_spans(None) == []


def test_body_only_definition_without_wrapper():
    sql = """BEGIN
    UPDATE t SET x = 1;
    COMMIT;
END;
"""
    spans = executable_statement_spans(sql)
    joined = "\n".join(_highlighted(sql, spans))
    assert "UPDATE t SET x = 1" in joined
