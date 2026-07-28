import pandas as pd

from analyzer.query_decomposer import decompose_redshift_query


def _tables() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "source_db": "dev",
                "schema_name": "fraud",
                "table_name": "authorization_fact",
                "table_id": 10,
                "tbl_rows": 500_000_000,
                "size_mb": 50_000,
                "diststyle": "KEY(customer_id)",
                "sortkey1": "transaction_date",
                "stats_off": 2,
            },
            {
                "source_db": "dev",
                "schema_name": "fraud",
                "table_name": "customer",
                "table_id": 11,
                "tbl_rows": 100_000,
                "size_mb": 100,
                "diststyle": "KEY(customer_id)",
                "sortkey1": "customer_id",
                "stats_off": 1,
            },
        ]
    )


def test_decomposer_projects_required_columns_pushes_filter_and_designs_temp_table() -> None:
    sql = """
SELECT a.transaction_id, c.customer_name, SUM(a.amount) AS amount
FROM dev.fraud.authorization_fact a
JOIN dev.fraud.customer c ON a.customer_id = c.customer_id
WHERE a.transaction_date >= DATEADD(day, -25, CURRENT_DATE)
GROUP BY a.transaction_id, c.customer_name
"""

    result = decompose_redshift_query(sql, _tables(), pd.DataFrame())

    assert result.parse_ok
    assert len(result.stages) == 1
    stage = result.stages.iloc[0]
    assert stage["physical_table"] == "dev.fraud.authorization_fact"
    assert set(stage["required_columns"].split(", ")) == {
        "amount", "customer_id", "transaction_date", "transaction_id"
    }
    assert "transaction_date" in stage["pushed_predicates"]
    assert stage["distkey"] == "customer_id"
    assert stage["sortkey"] == "transaction_date"
    assert "CREATE TEMP TABLE tmp_decomp_01_authorization_fact" in result.generated_sql
    assert "FROM tmp_decomp_01_authorization_fact AS a" in result.generated_sql


def test_decomposer_resolves_schema_qualified_sql_to_unique_database_table() -> None:
    result = decompose_redshift_query(
        "SELECT a.transaction_id FROM fraud.authorization_fact a "
        "WHERE a.transaction_date >= CURRENT_DATE - 7",
        _tables(),
        pd.DataFrame(),
    )

    assert result.parse_ok
    assert len(result.stages) == 1
    assert result.stages.iloc[0]["physical_table"] == "dev.fraud.authorization_fact"
    assert "FROM tmp_decomp_01_authorization_fact AS a" in result.generated_sql


def test_decomposer_keeps_star_projection_conservative() -> None:
    result = decompose_redshift_query(
        "SELECT * FROM dev.fraud.authorization_fact a WHERE a.transaction_date >= CURRENT_DATE - 7",
        _tables(),
        pd.DataFrame(),
    )

    assert result.stages.iloc[0]["required_columns"] == "*"
    assert result.stages.iloc[0]["safety"] == "review"
    assert "Wildcard projection" in " ".join(result.findings["title"])


def test_count_star_does_not_force_wide_stage() -> None:
    result = decompose_redshift_query(
        "SELECT COUNT(*) FROM dev.fraud.authorization_fact a WHERE a.transaction_date >= CURRENT_DATE - 7",
        _tables(),
        pd.DataFrame(),
    )

    assert result.stages.iloc[0]["required_columns"] == "transaction_date"
    assert "Wildcard projection" not in " ".join(result.findings["title"])


def test_self_join_filters_are_not_unsafely_pushed_into_shared_stage() -> None:
    result = decompose_redshift_query(
        """
        SELECT a.transaction_id, prior.transaction_id
        FROM dev.fraud.authorization_fact a
        JOIN dev.fraud.authorization_fact prior ON a.customer_id = prior.customer_id
        WHERE a.transaction_date >= CURRENT_DATE - 7
        """,
        _tables(),
        pd.DataFrame(),
    )

    stage = result.stages.iloc[0]
    assert not stage["pushed_predicates"]
    assert "transaction_date" in stage["review_predicates"]
    assert "WHERE" not in stage["generated_sql"]


def test_decomposer_uses_explain_estimates_and_actual_scan_evidence() -> None:
    explain = pd.DataFrame([{
        "plan_node_id": 8,
        "plan_node": "XN Seq Scan on authorization_fact  (cost=0.00..81234.00 rows=9000000 width=72)",
        "plan_info": "",
    }])
    detail = pd.DataFrame([{
        "table_id": 10,
        "input_rows": 1234,
        "output_rows": 100,
        "input_bytes": 98765,
        "duration_s": 3.5,
    }])
    result = decompose_redshift_query(
        "SELECT a.transaction_id FROM dev.fraud.authorization_fact a",
        _tables(),
        pd.DataFrame(),
        explain,
        detail,
    )

    stage = result.stages.iloc[0]
    assert stage["plan_scan_nodes"] == "8"
    assert stage["plan_estimated_rows"] == 9_000_000
    assert stage["plan_estimated_width"] == 72
    assert stage["plan_max_cost"] == 81_234
    assert stage["actual_scan_rows"] == 1234


def test_mixed_dtype_catalog_merge_does_not_crash():
    """Regression: pandas 2.x refuses a cross-dtype .loc assignment.

    _candidate_table_rows backfills missing metadata columns from table_review.
    When the analyzer frame carried a float64 column and the catalog supplied a
    string-backed NA (or vice versa), the assignment raised TypeError and
    aborted decomposition. Measured against the sample warehouse, this hit
    roughly one query in six - and the GUI reported it as "failed safely", so
    it read as "no candidates found" rather than a crash.
    """
    import pandas as pd
    from analyzer.query_decomposer import decompose_redshift_query

    # size_mb/tbl_rows float64 in one frame, missing or string in the other.
    table_review = pd.DataFrame(
        [
            {
                "source_db": "dev",
                "schema_name": "marketing",
                "table_name": "fact_big",
                "object_type": "table",
                "tbl_rows": 1_443_606_520.0,
                "size_mb": 19_616.0,
                "diststyle": "EVEN",
                "sortkey1": None,
                "unsorted_pct": float("nan"),
                "stats_off": float("nan"),
            },
            {
                "source_db": "dev",
                "schema_name": "marketing",
                "table_name": "dim_small",
                "object_type": "table",
                "tbl_rows": 5_000.0,
                "size_mb": 3.0,
                "diststyle": "ALL",
                "sortkey1": "id",
                "unsorted_pct": None,
                "stats_off": None,
            },
        ]
    )
    sql = (
        "SELECT b.id, s.name FROM dev.marketing.fact_big b "
        "JOIN dev.marketing.dim_small s ON b.id = s.id "
        "WHERE b.dt >= '2024-01-01'"
    )
    result = decompose_redshift_query(sql, table_review)
    assert result.summary["parse_status"] == "parsed"
    assert result.findings is not None


def test_decomposer_survives_a_whole_workload():
    """No query in a mixed workload may abort the analyzer."""
    import pandas as pd
    from analyzer.query_decomposer import decompose_redshift_query

    table_review = pd.DataFrame(
        [
            {
                "source_db": "dev",
                "schema_name": "marketing",
                "table_name": f"fact_{i}",
                "object_type": "table",
                "tbl_rows": 2_000_000.0 * (i + 1),
                "size_mb": 2_048.0,
                "diststyle": "EVEN",
                "sortkey1": None,
            }
            for i in range(3)
        ]
    )
    workload = [
        "SELECT * FROM dev.marketing.fact_0 WHERE dt >= '2024-01-01'",
        "SELECT a.id FROM dev.marketing.fact_0 a JOIN dev.marketing.fact_1 b ON a.id = b.id",
        "SELECT COUNT(*) FROM dev.marketing.fact_2 GROUP BY region",
        "SELECT DISTINCT id FROM dev.marketing.fact_1 WHERE amount > 10",
        "WITH c AS (SELECT id FROM dev.marketing.fact_0) SELECT * FROM c",
        "SELECT !!! FROM ///",
    ]
    for sql in workload:
        decompose_redshift_query(sql, table_review)  # must not raise
