from redshift_decomposer import assess_decomposability


def test_clean_star_join_scores_high():
    report = assess_decomposability("""
        SELECT o.order_id, c.region, SUM(o.amount)
        FROM public.fact_orders o JOIN public.dim_customer c ON o.cust_id = c.cust_id
        WHERE o.order_date >= '2026-01-01'
        GROUP BY 1, 2
    """)
    assert report.parse_ok
    assert report.score >= 0.9
    assert report.likelihood == report.score
    assert "HIGH" in report.verdict
    assert not report.blocking
    brief = report.summary()
    assert "not perfect" in brief.lower()
    assert "skeleton" in brief.lower()
    assert "conversion-success likelihood" in brief


def test_json_super_manipulation_scores_low():
    report = assess_decomposability("""
        SELECT JSON_EXTRACT_PATH_TEXT(payload, 'user', 'id') AS uid,
               CAST(raw AS SUPER) AS doc
        FROM public.events
        WHERE IS_VALID_JSON(payload)
    """)
    assert report.score <= 0.55
    assert any("SUPER / JSON" in s.title for s in report.signals)


def test_recursive_cte_is_near_zero():
    report = assess_decomposability("""
        WITH RECURSIVE chain (id, parent) AS (
            SELECT id, parent FROM public.nodes WHERE parent IS NULL
            UNION ALL
            SELECT n.id, n.parent FROM public.nodes n JOIN chain c ON n.parent = c.id
        )
        SELECT * FROM chain
    """)
    assert report.score <= 0.15
    assert report.blocking or report.score <= 0.1
    assert any("Recursive" in s.title for s in report.signals)


def test_correlated_subquery_penalized():
    report = assess_decomposability("""
        SELECT o.order_id
        FROM public.fact_orders o
        WHERE o.amount > (SELECT AVG(i.amount) FROM public.fact_orders i
                          WHERE i.cust_id = o.cust_id)
    """)
    assert any("Correlated" in s.title for s in report.signals)
    assert report.score <= 0.8


def test_uncorrelated_in_subquery_not_flagged_as_correlated():
    """Regression: sqlglot marks many IN-subqueries as correlated; we must not."""
    report = assess_decomposability("""
        SELECT * FROM public.a
        WHERE id IN (SELECT id FROM public.b WHERE active = 1)
    """)
    assert not any("Correlated" in s.title for s in report.signals)
    # SELECT * is a mild ding only
    assert report.score >= 0.9


def test_union_all_not_false_correlated():
    report = assess_decomposability("""
        SELECT cust_id FROM public.a WHERE d > 1
        UNION ALL
        SELECT cust_id FROM public.b WHERE d > 1
    """)
    titles = {s.title for s in report.signals}
    assert any("Set operation" in t for t in titles)
    assert not any("Correlated" in t for t in titles)
    assert report.score >= 0.75


def test_garbage_sql_scores_zero():
    report = assess_decomposability("SELEKT foo FROM")
    assert report.score == 0.0
    assert not report.parse_ok
    assert report.blocking


def test_dml_is_not_a_candidate():
    report = assess_decomposability("DELETE FROM public.fact_orders WHERE amount < 0")
    assert report.score <= 0.05
    assert "UNLIKELY" in report.verdict
    assert report.blocking


def test_no_where_and_union_deducted():
    report = assess_decomposability("""
        SELECT cust_id FROM public.a
        UNION ALL
        SELECT cust_id FROM public.b
    """)
    titles = {s.title for s in report.signals}
    assert any("Set operation" in t for t in titles)
    assert any("No WHERE" in t for t in titles)


def test_self_join_multi_alias_review():
    report = assess_decomposability("""
        SELECT o1.id
        FROM public.orders o1
        JOIN public.orders o2 ON o1.parent = o2.id
        WHERE o1.status = 'OPEN' AND o2.status = 'RETURNED'
    """)
    assert any("self-join" in s.title.lower() or "Multi-alias" in s.title for s in report.signals)
    assert report.score <= 0.90
    assert report.score >= 0.70  # still a viable candidate, just review


def test_cte_filter_gap_detected():
    report = assess_decomposability("""
        WITH x AS (
            SELECT id, order_date, amount FROM public.fact_orders
        )
        SELECT x.id, b.region
        FROM x
        JOIN public.dim_customer b ON x.id = b.cust_id
        WHERE x.order_date >= DATE '2024-01-01'
    """)
    assert any("CTE filter gap" in s.title for s in report.signals)
    assert report.score < 0.90


def test_exists_correlated_penalized():
    report = assess_decomposability("""
        SELECT * FROM public.a a
        WHERE EXISTS (SELECT 1 FROM public.b b WHERE b.a_id = a.id)
    """)
    assert any("Correlated" in s.title for s in report.signals)


def test_having_only_wording():
    report = assess_decomposability("""
        SELECT cust_id, SUM(amt) s
        FROM public.f
        GROUP BY 1
        HAVING SUM(amt) > 10
    """)
    assert any("HAVING" in s.title for s in report.signals)


def test_external_schema_hint():
    report = assess_decomposability("""
        SELECT * FROM spectrum.s3_sales WHERE sale_date > DATE '2024-01-01'
    """)
    assert any("External" in s.title or "Spectrum" in s.title for s in report.signals)


def test_summary_is_ascii_only_and_names_likelihood():
    report = assess_decomposability("SELECT * FROM public.t WHERE x = 1")
    text = report.summary()
    text.encode("ascii")
    assert "likelihood" in text.lower() or "HIGH" in text


def test_signals_sorted_by_impact():
    report = assess_decomposability("""
        SELECT *
        FROM public.a
        UNION ALL
        SELECT *
        FROM public.b
    """)
    impacts = [s.impact for s in report.signals]
    assert impacts == sorted(impacts, reverse=True)
