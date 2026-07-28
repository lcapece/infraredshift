from analyzer.widgets.cluster_dashboard import _extract_subquery_rows, _format_sql_text


def _normalized(text: str) -> str:
    return text.replace("\r\n", "\n").strip().lower()


def test_format_unload_returns_inner_query_only():
    formatted = _normalized(
        _format_sql_text(
            "UNLOAD ('select user_id, count(*) from public.events "
            "where event_dt >= ''2026-01-01'' group by 1') TO 's3://bucket/path/';"
        )
    )

    assert formatted.startswith("select")
    assert "from public.events" in formatted
    assert "unload" not in formatted
    assert "s3://bucket" not in formatted


def test_format_copy_returns_parenthesized_inner_query_only():
    formatted = _normalized(
        _format_sql_text(
            "COPY (select * from public.events where event_dt >= '2026-01-01') TO STDOUT;"
        )
    )

    assert formatted.startswith("select")
    assert "from public.events" in formatted
    assert "copy" not in formatted
    assert "to stdout" not in formatted


def test_identify_subqueries_finds_ctes_and_nested_selects():
    rows = _extract_subquery_rows(
        "WITH recent AS (SELECT id FROM public.events) "
        "SELECT * FROM recent WHERE id IN (SELECT event_id FROM public.audit)"
    )

    assert set(rows["kind"]) == {"cte", "subquery"}
