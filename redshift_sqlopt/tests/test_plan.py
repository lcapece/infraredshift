"""Plan evidence and the escalation ladder.

These tests cover the predictive half: findings must carry measured numbers, and
the tier ordering must prefer the cheapest fix that actually addresses the
problem.
"""

from __future__ import annotations

from redshift_sqlopt import Catalog, Tier, evidence_from_rows, optimize

BIG_UNKEYED = Catalog.from_rows(
    table_rows=[
        {
            "database": "analytics",
            "schema": "public",
            "table": "fact_orders",
            "distkey": "",
            "diststyle": "EVEN",
            "sortkeys": "",
            "row_count": 2_000_000_000,
            "size_mb": 430_000,
        }
    ]
)

KEYED = Catalog.from_rows(
    table_rows=[
        {
            "database": "analytics",
            "schema": "public",
            "table": "fact_orders",
            "distkey": "cust_id",
            "diststyle": "KEY",
            "sortkeys": "order_date",
            "row_count": 2_000_000_000,
        }
    ]
)

SQL = "SELECT order_id FROM analytics.public.fact_orders WHERE order_date >= '2024-01-01'"


def codes(findings) -> set[str]:
    return {finding.code for finding in findings}


def test_broadcast_of_large_row_set_is_reported() -> None:
    result = optimize(
        SQL,
        catalog=KEYED,
        explain_rows=[{"step": 7, "operation": "XN Hash Join DS_BCAST_INNER", "rows": 1000}],
        detail_rows=[{"step": 7, "output_rows": 2_100_000_000}],
    )
    assert "PLAN_BROADCAST" in codes(result.findings)


def test_small_broadcast_is_not_reported() -> None:
    """Broadcasting a small dimension table is correct behaviour, not a problem."""
    result = optimize(
        SQL,
        catalog=KEYED,
        explain_rows=[{"step": 3, "operation": "XN Hash Join DS_BCAST_INNER", "rows": 500}],
        detail_rows=[{"step": 3, "output_rows": 500}],
    )
    assert "PLAN_BROADCAST" not in codes(result.findings)


def test_spill_is_reported_with_size() -> None:
    result = optimize(
        SQL,
        catalog=KEYED,
        detail_rows=[{"step": 4, "spilled_bytes": 84_000_000_000}],
    )
    spill = [f for f in result.findings if f.code == "PLAN_SPILL"]
    assert spill
    assert "84.0 GB" in spill[0].title


def test_stale_stats_detected_from_estimate_error() -> None:
    result = optimize(
        SQL,
        catalog=KEYED,
        explain_rows=[{"step": 2, "operation": "XN Seq Scan", "rows": 1000}],
        detail_rows=[{"step": 2, "output_rows": 500_000_000}],
    )
    assert "PLAN_STATS_STALE" in codes(result.findings)


def test_accurate_estimate_produces_no_stats_finding() -> None:
    result = optimize(
        SQL,
        catalog=KEYED,
        explain_rows=[{"step": 2, "operation": "XN Seq Scan", "rows": 1_000_000}],
        detail_rows=[{"step": 2, "output_rows": 1_050_000}],
    )
    assert "PLAN_STATS_STALE" not in codes(result.findings)


def test_unkeyed_large_table_is_a_ddl_finding_not_decomposition() -> None:
    """The whole point of the ladder: fix the table, do not decompose around it."""
    result = optimize(SQL, catalog=BIG_UNKEYED)
    unkeyed = [f for f in result.findings if f.code == "TABLE_UNKEYED"]
    assert unkeyed
    assert unkeyed[0].tier is Tier.DDL
    assert not result.should_escalate_to_decomposition()
    assert "ALTER TABLE" in unkeyed[0].suggested_ddl


def test_keyed_table_produces_no_unkeyed_finding() -> None:
    result = optimize(SQL, catalog=KEYED)
    assert "TABLE_UNKEYED" not in codes(result.findings)


def test_small_unkeyed_table_is_ignored() -> None:
    """Distribution choices do not matter on a tiny table."""
    catalog = Catalog.from_rows(
        table_rows=[
            {
                "database": "analytics",
                "schema": "public",
                "table": "fact_orders",
                "distkey": "",
                "diststyle": "EVEN",
                "sortkeys": "",
                "row_count": 500,
            }
        ]
    )
    result = optimize(SQL, catalog=catalog)
    assert "TABLE_UNKEYED" not in codes(result.findings)


def test_findings_rank_cheapest_tier_first() -> None:
    result = optimize(
        SQL,
        catalog=BIG_UNKEYED,
        explain_rows=[{"step": 7, "operation": "XN Hash Join DS_BCAST_INNER", "rows": 100}],
        detail_rows=[
            {"step": 7, "output_rows": 2_100_000_000, "spilled_bytes": 84_000_000_000}
        ],
    )
    tiers = [finding.tier for finding in result.ranked_findings()]
    assert tiers == sorted(tiers), "findings must be ordered cheapest tier first"


def test_plan_findings_survive_a_parse_failure() -> None:
    """Plan evidence never needed the AST, so it must still be reported."""
    result = optimize(
        "SELECT !!! FROM ///",
        catalog=BIG_UNKEYED,
        detail_rows=[{"step": 4, "spilled_bytes": 20_000_000_000}],
    )
    assert not result.parsed
    assert "PLAN_SPILL" in codes(result.findings)


def test_evidence_records_carry_measured_numbers() -> None:
    evidence = evidence_from_rows(
        explain_rows=[{"step": 1, "operation": "XN Seq Scan", "rows": 100}],
        detail_rows=[{"step": 1, "output_rows": 50_000, "duration_s": 12.5}],
    )
    assert len(evidence) == 1
    item = evidence[0]
    assert item.actual_rows == 50_000
    assert item.estimated_rows == 100
    assert item.estimate_error_ratio == 500.0
    assert "50,000 rows" in item.describe()


def test_no_plan_rows_yields_no_plan_findings() -> None:
    result = optimize(SQL, catalog=KEYED)
    plan_codes = {c for c in codes(result.findings) if c.startswith("PLAN_")}
    assert plan_codes == set()


def test_skew_is_reported() -> None:
    catalog = Catalog.from_rows(
        table_rows=[
            {
                "database": "analytics",
                "schema": "public",
                "table": "fact_orders",
                "distkey": "",
                "diststyle": "EVEN",
                "sortkeys": "",
                "row_count": 2_000_000_000,
                "skew_ratio": 9.4,
            }
        ]
    )
    result = optimize(SQL, catalog=catalog)
    assert "TABLE_SKEW" in codes(result.findings)


# ---------------------------------------------------------------------------
# real Redshift column names and units
#
# Verified against the published pg_catalog schema for SYS_QUERY_EXPLAIN,
# SYS_QUERY_DETAIL and SVV_TABLE_INFO. An earlier version of this module guessed
# these and guessed wrong on nearly every one, which would have made the whole
# predictive half silently produce nothing on a real cluster.
# ---------------------------------------------------------------------------


def test_real_sys_query_detail_columns() -> None:
    """plan_node_id / output_rows / spilled_block_* / duration in microseconds."""
    evidence = evidence_from_rows(
        explain_rows=[
            {
                "query_id": 1,
                "plan_node_id": 7,
                "plan_node": "XN Hash Join DS_BCAST_INNER",
                "plan_info": "(cost=0.00..1234.56 rows=1000 width=42)",
            }
        ],
        detail_rows=[
            {
                "query_id": 1,
                "plan_node_id": 7,
                "output_rows": 2_100_000_000,
                "input_bytes": 60_000_000_000,
                "spilled_block_local_disk": 80_000,
                "spilled_block_remote_disk": 1_000,
                "duration": 412_500_000,
            }
        ],
    )
    assert len(evidence) == 1
    item = evidence[0]
    assert item.step == 7
    assert item.actual_rows == 2_100_000_000
    assert item.is_broadcast
    # 81,000 blocks x 1 MiB
    assert item.spill_bytes == 81_000 * 1_048_576
    # microseconds -> seconds
    assert item.duration_s == 412.5


def test_estimate_parsed_from_plan_info_text() -> None:
    """SYS_QUERY_EXPLAIN has no numeric estimate column; it is inside plan_info."""
    evidence = evidence_from_rows(
        explain_rows=[
            {"plan_node_id": 3, "plan_node": "XN Seq Scan", "plan_info": "rows=500 width=8"}
        ],
        detail_rows=[{"plan_node_id": 3, "output_rows": 5_000_000}],
    )
    assert evidence[0].estimated_rows == 500
    assert evidence[0].estimate_error_ratio == 10_000.0


def test_real_svv_table_info_columns() -> None:
    """tbl_rows / size / sortkey1 / skew_rows / unsorted / stats_off."""
    catalog = Catalog.from_rows(
        table_rows=[
            {
                "database": "dev",
                "schema": "marketing",
                "table": "fact_big",
                "diststyle": "EVEN",
                "distkey": "",
                "sortkey1": "policy_date",
                "tbl_rows": 1_443_606_520,
                "size": 19_616,
                "skew_rows": 8.2,
                "unsorted": 88.0,
                "stats_off": 64.0,
            }
        ]
    )
    stats = catalog.resolve_table("dev.marketing.fact_big")
    assert stats is not None
    assert stats.row_count == 1_443_606_520
    assert stats.size_mb == 19_616
    assert stats.sortkeys == ("policy_date",)
    assert stats.skew_ratio == 8.2
    assert stats.is_large


def test_interleaved_sortkey_prefix_is_stripped() -> None:
    """SVV_TABLE_INFO marks an interleaved key with a leading '-'."""
    catalog = Catalog.from_rows(
        table_rows=[
            {"schema": "s", "table": "t", "sortkey1": "-event_date", "tbl_rows": 5_000_000}
        ]
    )
    stats = catalog.resolve_table("s.t")
    assert stats.sortkeys == ("event_date",)
    assert catalog.is_sortkey("s.t", "event_date")


def test_friendly_aliases_still_work() -> None:
    """Hand-built catalogs and the analyzer's DuckDB copy use other spellings."""
    catalog = Catalog.from_rows(
        table_rows=[
            {
                "schema": "s",
                "table": "t",
                "sortkeys": "a,b",
                "row_count": 9_000_000,
                "size_mb": 4_096,
                "skew_ratio": 3.0,
            }
        ]
    )
    stats = catalog.resolve_table("s.t")
    assert stats.row_count == 9_000_000
    assert stats.size_mb == 4_096
    assert stats.sortkeys == ("a", "b")
