"""Generate deterministic mock Redshift diagnostic snapshots for demos."""
from __future__ import annotations

import argparse
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from .duckdb_store import DuckDBStore


DEFAULT_QUERY_COUNT = 420
DEFAULT_TABLE_COUNT = 360
DEFAULT_SEED = 20260624


DATABASES = ("dev", "enterprise_datawarehouse", "businesslayer")
SCHEMAS = ("edw", "mart", "finance", "claims", "marketing", "identity", "staging", "sandbox")
USERS = (
    "bi_looker",
    "etl_airflow",
    "finance_poweruser",
    "claims_dbt",
    "marketing_tableau",
    "risk_modeling",
    "svc_ingestion",
    "ad_hoc_analyst",
)
SERVICE_CLASSES = ("bi", "etl", "adhoc", "critical_reporting", "data_science")


ARCHETYPES = (
    "BAD_DISTRIBUTION",
    "SPILL_HEAVY",
    "EXTERNAL_S3",
    "SCAN_HEAVY",
    "NESTED_LOOP_RISK",
    "MISSING_STATS",
    "TIME_SERIES_MONSTER",
    "GENERAL",
)


@dataclass(frozen=True)
class MockSnapshotSummary:
    path: Path
    snapshot_id: str
    query_rows: int
    table_rows: int
    text_rows: int
    scan_rows: int
    view_rows: int
    procedure_rows: int
    explain_rows: int
    detail_flow_rows: int
    external_table_rows: int
    user_rows: int

    @property
    def total_rows(self) -> int:
        return (
            self.query_rows * 4
            + self.text_rows
            + self.table_rows
            + self.scan_rows
            + self.view_rows
            + self.procedure_rows
            + self.explain_rows
            + self.detail_flow_rows
            + self.external_table_rows
            + self.user_rows
        )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = generate_mock_snapshot(
        output=args.output,
        query_count=args.query_count,
        table_count=args.table_count,
        seed=args.seed,
        label=args.label,
    )
    print(f"Mock snapshot: {summary.snapshot_id}")
    print(f"DuckDB path: {summary.path}")
    print(f"Rows: {summary.total_rows:,}")
    print(f"  query_history: {summary.query_rows:,}")
    print(f"  query_history_all: {summary.query_rows:,}")
    print(f"  query_details: {summary.query_rows:,}")
    print(f"  query_health: {summary.query_rows:,}")
    print(f"  query_text: {summary.text_rows:,}")
    print(f"  svv_table_info_all: {summary.table_rows:,}")
    print(f"  table_scan_info: {summary.scan_rows:,}")
    print(f"  view_definitions: {summary.view_rows:,}")
    print(f"  procedure_definitions: {summary.procedure_rows:,}")
    print(f"  query_explain: {summary.explain_rows:,}")
    print(f"  query_detail_flow: {summary.detail_flow_rows:,}")
    print(f"  external_table_info_all: {summary.external_table_rows:,}")
    print(f"  user_info: {summary.user_rows:,}")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a mock Redshift DuckDB demo snapshot.")
    parser.add_argument(
        "--output",
        default=str(Path(__file__).parent / "samples" / "mock_redshift_3300.duckdb"),
        help="Output DuckDB file.",
    )
    parser.add_argument("--query-count", type=int, default=DEFAULT_QUERY_COUNT)
    parser.add_argument("--table-count", type=int, default=DEFAULT_TABLE_COUNT)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--label", default="mock 3300 row Redshift demo")
    return parser.parse_args(argv)


def generate_mock_snapshot(
    *,
    output: str | Path,
    query_count: int = DEFAULT_QUERY_COUNT,
    table_count: int = DEFAULT_TABLE_COUNT,
    seed: int = DEFAULT_SEED,
    label: str = "mock Redshift demo",
) -> MockSnapshotSummary:
    rng = random.Random(seed)
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    tables = build_table_info(rng, table_count)
    queries, details, health, text, explain, detail_flow = build_queries(rng, query_count, tables)
    users = (
        queries[["user_id", "user_name"]]
        .drop_duplicates()
        .rename(columns={"user_id": "usesysid", "user_name": "usename"})
        .assign(user_id=lambda frame: frame["usesysid"], user_name=lambda frame: frame["usename"])
    )
    scan_info = build_table_scan_info(rng, tables, query_count)
    view_defs = build_view_definitions(rng, tables)
    procedure_defs = build_procedure_definitions(rng, tables)
    external_tables = build_external_table_info(rng, queries)

    store = DuckDBStore(output_path)
    run = store.new_snapshot(label)
    with store.connect() as con:
        store.record_snapshot(con, run, source="mock-redshift")
        store.replace_table_from_frame(con, "query_history", queries, run)
        store.replace_table_from_frame(con, "query_history_all", queries.copy(), run)
        store.replace_table_from_frame(con, "query_details", details, run)
        store.replace_table_from_frame(con, "query_health", health, run)
        store.replace_table_from_frame(con, "query_text", text, run)
        store.replace_table_from_frame(con, "query_explain", explain, run)
        store.replace_table_from_frame(con, "query_detail_flow", detail_flow, run)
        store.replace_table_from_frame(con, "user_info", users, run)
        store.replace_table_from_frame(con, "table_scan_info", scan_info, run)
        store.replace_table_from_frame(con, "svv_table_info_all", tables, run)
        store.replace_table_from_frame(con, "external_table_info_all", external_tables, run)
        store.replace_table_from_frame(con, "view_definitions", view_defs, run)
        store.replace_table_from_frame(con, "procedure_definitions", procedure_defs, run)

    return MockSnapshotSummary(
        path=output_path,
        snapshot_id=run.snapshot_id,
        query_rows=len(queries),
        table_rows=len(tables),
        text_rows=len(text),
        scan_rows=len(scan_info),
        view_rows=len(view_defs),
        procedure_rows=len(procedure_defs),
        explain_rows=len(explain),
        detail_flow_rows=len(detail_flow),
        external_table_rows=len(external_tables),
        user_rows=len(users),
    )


def build_table_info(rng: random.Random, count: int) -> pd.DataFrame:
    rows: list[dict] = []
    per_db = max(1, math.ceil(count / len(DATABASES)))
    table_id = 100000
    for db in DATABASES:
        for i in range(per_db):
            if len(rows) >= count:
                break
            kind = weighted_choice(
                rng,
                (
                    ("fact", 0.34),
                    ("dim", 0.22),
                    ("stage", 0.18),
                    ("archive", 0.12),
                    ("bridge", 0.08),
                    ("audit", 0.06),
                ),
            )
            schema = rng.choice(SCHEMAS)
            table_name = table_name_for(rng, kind, i)
            size_mb, tbl_rows = table_size_for(rng, kind)
            high_risk = rng.random() < (0.30 if kind in {"fact", "archive", "stage"} else 0.13)
            diststyle = diststyle_for(rng, kind, size_mb, high_risk)
            sortkey1 = sortkey_for(rng, kind)
            unsorted = clamp(rng.gauss(18 if high_risk else 7, 18 if high_risk else 6), 0, 95)
            stats_off = clamp(rng.gauss(22 if high_risk else 5, 14 if high_risk else 5), 0, 90)
            skew_rows = clamp(rng.lognormvariate(0.1 if not high_risk else 1.1, 0.35), 1, 12)
            vacuum_benefit = clamp((unsorted * 0.6) + rng.gauss(0, 4), 0, 70)
            pct_used = clamp(rng.gauss(68, 16), 5, 98)
            risk_event = "none"
            if skew_rows >= 5:
                risk_event = "skew"
            elif unsorted >= 50:
                risk_event = "vacuum_sort"
            elif stats_off >= 25:
                risk_event = "stats"
            rows.append(
                {
                    "database": db,
                    "source_db": db,
                    "schema": schema,
                    "table_id": table_id,
                    "table": table_name,
                    "encoded": "Y" if rng.random() > 0.04 else "N",
                    "diststyle": diststyle,
                    "sortkey1": sortkey1,
                    "max_varchar": rng.choice((32, 64, 128, 256, 512, 1024)),
                    "sortkey1_enc": rng.choice(("zstd", "az64", "raw", "bytedict", "lzo")),
                    "sortkey_num": 1 if sortkey1 else 0,
                    "size": int(size_mb),
                    "pct_used": round(pct_used, 1),
                    "empty": int(max(0, rng.gauss(20 if kind == "stage" else 4, 8))),
                    "unsorted": round(unsorted, 1),
                    "stats_off": round(stats_off, 1),
                    "tbl_rows": int(tbl_rows),
                    "skew_sortkey1": round(clamp(rng.lognormvariate(0.0, 0.25), 1, 6), 2),
                    "skew_rows": round(skew_rows, 2),
                    "estimated_visible_rows": int(tbl_rows * rng.uniform(0.92, 1.0)),
                    "risk_event": risk_event,
                    "vacuum_sort_benefit": round(vacuum_benefit, 1),
                    "create_time": (datetime(2024, 1, 1) + timedelta(days=rng.randrange(850))).strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            table_id += 1
    return pd.DataFrame(rows)


def build_view_definitions(rng: random.Random, tables: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    if tables.empty:
        return pd.DataFrame(columns=["database", "schema", "view_name", "source_definition"])
    facts = tables[tables["table"].str.contains("fact|event|sales|claims|transaction|order", case=False, regex=True)]
    dims = tables[tables["table"].str.contains("dim|customer|provider|product|policy", case=False, regex=True)]
    if facts.empty:
        facts = tables
    if dims.empty:
        dims = tables
    for i, (_, fact) in enumerate(facts.head(36).iterrows(), start=1):
        same_db_dims = dims[dims["database"] == fact["database"]]
        dim_pool = same_db_dims if not same_db_dims.empty else dims
        dim = dim_pool.sample(n=1, random_state=rng.randrange(1_000_000)).iloc[0]
        join_col = rng.choice(("customer_id", "provider_id", "product_id", "account_id"))
        date_col = rng.choice(("event_date", "created_at", "updated_at", "service_date"))
        view_name = f"vw_{str(fact['table']).replace('fact_', '').replace('stg_', '')[:36]}_{i:02d}"
        definition = f"""CREATE OR REPLACE VIEW {fact['schema']}.{view_name} AS
SELECT
  f.{join_col},
  f.{date_col},
  d.status,
  SUM(f.amount) AS total_amount,
  COUNT(*) AS row_count
FROM {fact['schema']}.{fact['table']} f
JOIN {dim['schema']}.{dim['table']} d
  ON f.{join_col} = d.{join_col}
WHERE f.{date_col} >= DATEADD(day, -90, CURRENT_DATE)
GROUP BY 1, 2, 3"""
        rows.append(
            {
                "database": fact["database"],
                "schema": fact["schema"],
                "view_name": view_name,
                "source_definition": definition,
            }
        )
    return pd.DataFrame(rows)


def build_procedure_definitions(rng: random.Random, tables: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    if tables.empty:
        return pd.DataFrame(
            columns=[
                "database",
                "schema",
                "procedure_name",
                "procedure_oid",
                "procedure_key",
                "owner_name",
                "argument_names",
                "argument_modes",
                "argument_type_oids",
                "all_argument_type_oids",
                "source_length",
                "source_definition",
            ]
        )
    facts = tables[tables["table"].str.contains("fact|event|sales|claims|transaction|order", case=False, regex=True)]
    if facts.empty:
        facts = tables
    for i, (_, fact) in enumerate(facts.head(18).iterrows(), start=1):
        procedure_name = f"sp_refresh_{str(fact['table']).replace('fact_', '').replace('stg_', '')[:34]}_{i:02d}"
        procedure_key = f"{fact['database']}.{fact['schema']}.{procedure_name}".lower()
        date_col = rng.choice(("event_date", "created_at", "updated_at", "service_date"))
        body = f"""BEGIN
  DELETE FROM {fact['schema']}.{fact['table']}
  WHERE {date_col} >= p_start_date;

  INSERT INTO {fact['schema']}.{fact['table']}
  SELECT *
  FROM staging.{fact['table']}
  WHERE {date_col} >= p_start_date;

  RAISE INFO 'loaded rows into {fact['schema']}.{fact['table']}';
END"""
        rows.append(
            {
                "database": fact["database"],
                "schema": fact["schema"],
                "procedure_name": procedure_name,
                "procedure_oid": str(700000 + i),
                "procedure_key": procedure_key,
                "owner_name": rng.choice(USERS),
                "argument_names": "{p_start_date}",
                "argument_modes": "{i}",
                "argument_type_oids": "1082",
                "all_argument_type_oids": "",
                "source_length": len(body),
                "source_definition": body,
            }
        )
    return pd.DataFrame(rows)


def build_queries(
    rng: random.Random,
    count: int,
    tables: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    history_rows: list[dict] = []
    detail_rows: list[dict] = []
    health_rows: list[dict] = []
    text_rows: list[dict] = []
    explain_rows: list[dict] = []
    flow_rows: list[dict] = []
    base_time = datetime(2026, 6, 17, 6, 0, 0)
    query_id_start = 9100000

    facts = tables[tables["table"].str.contains("fact|event|sales|claims|transaction|order", case=False, regex=True)]
    if facts.empty:
        facts = tables
    dims = tables[tables["table"].str.contains("dim|customer|provider|product|policy", case=False, regex=True)]
    if dims.empty:
        dims = tables

    # A useful demo must contain genuine recurring workload families. Keep the
    # SQL shape, owner, and source tables stable for six executions while the
    # runtime/IO evidence varies per execution.
    pattern_size = 6
    archetype_cycle = tuple(name for name in ARCHETYPES if name != "GENERAL") + ("GENERAL",)
    patterns: list[dict] = []
    for pattern_index in range(max(1, math.ceil(count / pattern_size))):
        pattern_fact = facts.sample(n=1, random_state=rng.randrange(1_000_000)).iloc[0]
        same_database_dims = dims[dims["source_db"] == pattern_fact["source_db"]]
        dimension_pool = same_database_dims if not same_database_dims.empty else dims
        patterns.append(
            {
                "archetype": archetype_cycle[pattern_index % len(archetype_cycle)],
                "fact": pattern_fact,
                "dim": dimension_pool.sample(n=1, random_state=rng.randrange(1_000_000)).iloc[0],
                "user": USERS[pattern_index % len(USERS)],
                "sql_index": pattern_index,
            }
        )

    for i in range(count):
        query_id = query_id_start + i
        pattern = patterns[i // pattern_size]
        archetype = str(pattern["archetype"])
        fact = pattern["fact"]
        dim = pattern["dim"]
        elapsed_s = elapsed_for(rng, archetype)
        input_rows = input_rows_for(rng, archetype, fact)
        selectivity = selectivity_for(rng, archetype)
        output_rows = max(1, int(input_rows * selectivity))
        input_bytes = input_rows * rng.randint(52, 220)
        output_bytes = output_rows * rng.randint(40, 180)
        total_steps = rng.randint(12, 58)
        scan_steps = rng.randint(3, 18)
        join_steps = rng.randint(1, 9)
        sort_steps = rng.randint(0, 6)
        agg_steps = rng.randint(0, 6)
        dist_total = dist_total_for(rng, archetype)
        dist_both = 1 if archetype == "BAD_DISTRIBUTION" or rng.random() < 0.05 else 0
        bcast = bcast_for(rng, archetype)
        s3_scans = rng.randint(1, 4) if archetype == "EXTERNAL_S3" else (1 if rng.random() < 0.08 else 0)
        nested_loop = 1 if archetype == "NESTED_LOOP_RISK" else (1 if rng.random() < 0.04 else 0)
        missing_stats = 1 if archetype == "MISSING_STATS" or rng.random() < 0.12 else 0
        partition_loop = rng.randint(1, 3) if archetype == "EXTERNAL_S3" and rng.random() < 0.55 else 0
        spill = spill_for(rng, archetype)
        remote_io_ratio = remote_io_for(rng, archetype)
        max_data_skew = skew_for(rng, archetype)
        max_time_skew = skew_for(rng, archetype)
        external_pct = external_pct_for(rng, archetype)
        alert_count = (1 if missing_stats else 0) + (1 if dist_both else 0) + (1 if spill else 0)
        cost_score = (
            scan_steps
            + s3_scans * 3
            + dist_both * 5
            + bcast * 2
            + nested_loop * 10
            + missing_stats * 8
            + partition_loop * 2
            + rng.randint(0, 12)
        )
        start_time = base_time + timedelta(minutes=i * rng.randint(4, 11), seconds=rng.randint(0, 59))
        end_time = start_time + timedelta(seconds=elapsed_s)
        database = str(fact["source_db"])
        user = str(pattern["user"])
        service_class = rng.choice(SERVICE_CLASSES)
        sql = sql_for(rng, archetype, fact, dim, int(pattern["sql_index"]))

        user_id = 100 + rng.randrange(45)
        history_rows.append(
            {
                "query_id": query_id,
                "user_id": user_id,
                "user_name": user,
                "database": database,
                "database_name": database,
                "transaction_id": 880000 + i,
                "session_id": 400000 + rng.randrange(5000),
                "service_class": rng.randrange(5, 20),
                "service_class_name": service_class,
                "query_type": "SELECT",
                "status": "success" if rng.random() > 0.035 else "aborted",
                "aborted": "f" if rng.random() > 0.035 else "t",
                "result_cache_hit": "f",
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "elapsed_time": int(elapsed_s * 1_000_000),
                "execution_time": int(elapsed_s * rng.uniform(0.78, 0.99) * 1_000_000),
                "queue_time": int(rng.choice((0, 0, 0, 5, 12, 28, 55)) * 1_000_000),
                "planning_time": int(rng.uniform(0.2, 8.0) * 1_000_000),
                "compile_time": int(rng.uniform(0.1, 5.0) * 1_000_000),
                "lock_wait_time": int(rng.choice((0, 0, 0, 0, 2, 7, 18)) * 1_000_000),
                "rows": output_rows,
                "returned_rows": output_rows,
                "wlm_query_slot_count": rng.choice((1, 1, 2, 2, 3, 4, 6)),
                "query_label": f"{service_class}:{archetype.lower()}",
                "error_message": "",
            }
        )
        local_read = int(input_bytes / 8192 * rng.uniform(0.15, 0.9))
        remote_read = int(local_read * remote_io_ratio / max(0.05, 1 - remote_io_ratio))
        external_duration = int(elapsed_s * external_pct * 1_000_000) if external_pct else 0
        external_rows = int(input_rows * rng.uniform(0.15, 0.8)) if external_pct else 0
        external_selectivity = rng.uniform(0.002, 0.2) if external_pct else None
        detail_rows.append(
            {
                "query_id": query_id,
                "total_spill": spill,
                "input_bytes": int(input_bytes),
                "output_bytes": int(output_bytes),
                "blocks_read": int(input_bytes / 8192),
                "blocks_write": int(output_bytes / 8192),
                "local_read_io": local_read,
                "remote_read_io": remote_read,
                "input_rows": int(input_rows),
                "output_rows": int(output_rows),
                "selectivity_ratio": round(output_rows / max(1, input_rows), 8),
                "total_step_duration": int(elapsed_s * 1_000_000 * rng.uniform(0.86, 1.18)),
                "max_step_duration": int(elapsed_s * 1_000_000 * rng.uniform(0.18, 0.68)),
                "avg_step_duration": int(elapsed_s * 1_000_000 / total_steps),
                "total_steps": total_steps,
                "max_data_skewness": round(max_data_skew, 2),
                "max_time_skewness": round(max_time_skew, 2),
                "segments_used": rng.randint(2, 12),
                "streams_used": rng.randint(1, 5),
                "scan_steps": scan_steps,
                "join_steps": join_steps,
                "sort_steps": sort_steps,
                "agg_steps": agg_steps,
                "tables_touched": rng.randint(2, 14),
                "alert_count": alert_count,
                "external_steps": rng.randint(1, 5) if external_pct else 0,
                "s3_steps": s3_scans,
                "external_input_bytes": int(input_bytes * rng.uniform(0.1, 0.8)) if external_pct else None,
                "external_input_rows": external_rows if external_pct else None,
                "external_duration": external_duration if external_pct else None,
                "external_duration_pct": round(external_pct, 3) if external_pct else None,
                "remote_io_ratio": round(remote_io_ratio, 3),
                "external_selectivity": round(external_selectivity, 4) if external_selectivity else None,
                "external_spill_blocks": int(spill * rng.uniform(0.04, 0.35)) if external_pct and spill else 0,
                "external_tables_touched": rng.randint(1, 4) if external_pct else 0,
                "external_data_skew": round(skew_for(rng, archetype), 2) if external_pct else None,
            }
        )
        dominant_issue = dominant_issue_for(archetype, dist_both, nested_loop, s3_scans, missing_stats, scan_steps)
        health_rows.append(
            {
                "query_id": query_id,
                "seq_scan_cnt": scan_steps,
                "s3_scan_cnt": s3_scans,
                "partition_loop_cnt": partition_loop,
                "dist_both_cnt": dist_both,
                "bcast_cnt": bcast,
                "dist_total_cnt": dist_total,
                "has_nested_loop": nested_loop,
                "hash_join_cnt": rng.randint(1, 8),
                "subquery_cnt": rng.randint(0, 5),
                "network_cnt": rng.randint(0, 5) if dist_total or bcast else rng.randint(0, 1),
                "missing_stats_flag": missing_stats,
                "max_est_rows": int(input_rows * rng.uniform(0.5, 6.0)),
                "max_cost": round(rng.uniform(10000, 350000) * (1 + cost_score / 55), 2),
                "cost_score": cost_score,
                "dominant_issue": dominant_issue,
                "cost_tier": cost_tier_for(cost_score),
            }
        )
        for sequence, fragment in enumerate(split_sql(sql, 3)):
            text_rows.append(
                {
                    "query_id": query_id,
                    "sequence": sequence,
                    "sequence_num": sequence,
                    "query_text": fragment,
                }
            )
        join_column = _mock_join_column(archetype)
        movement = "DS_DIST_BOTH" if dist_both else ("DS_BCAST_INNER" if bcast else "DS_DIST_NONE")
        join_operator = "XN Nested Loop" if nested_loop else "XN Hash Join"
        fact_table = str(fact["table"])
        dim_table = str(dim["table"])
        explain_rows.extend(
            [
                {
                    "userid": user_id, "user_id": user_id, "query_id": query_id,
                    "child_query_sequence": 1, "plan_node_id": 1, "plan_parent_id": 0,
                    "plan_node": f"XN HashAggregate  (cost=0.00..{max(1000, cost_score * 950):.2f} rows={max(1, output_rows)} width=96)",
                    "plan_info": "Group Key: result dimensions",
                },
                {
                    "userid": user_id, "user_id": user_id, "query_id": query_id,
                    "child_query_sequence": 1, "plan_node_id": 2, "plan_parent_id": 1,
                    "plan_node": f"{join_operator} {movement}  (cost=100.00..{max(5000, cost_score * 2250):.2f} rows={max(1, output_rows)} width=128)",
                    "plan_info": f"Hash Cond: (f.{join_column} = d.{join_column})",
                },
                {
                    "userid": user_id, "user_id": user_id, "query_id": query_id,
                    "child_query_sequence": 1, "plan_node_id": 3, "plan_parent_id": 2,
                    "plan_node": f"XN Seq Scan on {fact_table}  (cost=0.00..{max(1000, cost_score * 1750):.2f} rows={input_rows} width=84)",
                    "plan_info": "Filter: workload predicate" + ("; missing statistics" if missing_stats else ""),
                },
                {
                    "userid": user_id, "user_id": user_id, "query_id": query_id,
                    "child_query_sequence": 1, "plan_node_id": 4, "plan_parent_id": 2,
                    "plan_node": (
                        f"XN S3 Seq Scan on spectrum.raw_clickstream_{i % 12:02d}"
                        if archetype == "EXTERNAL_S3"
                        else f"XN Seq Scan on {dim_table}"
                    ) + f"  (cost=0.00..{max(500, cost_score * 650):.2f} rows={max(1000, input_rows // 20)} width=48)",
                    "plan_info": "S3 partitions scanned" if archetype == "EXTERNAL_S3" else "Dimension scan",
                },
            ]
        )
        step_specs = (
            (3, "scan", int(fact["table_id"]), fact_table, input_rows, max(output_rows * 3, 1), 0.47),
            (4, "s3scan" if archetype == "EXTERNAL_S3" else "scan", int(dim["table_id"]), dim_table,
             max(1000, input_rows // 20), max(100, output_rows), 0.18),
            (2, "nestloop" if nested_loop else "hashjoin", None, "", max(input_rows, output_rows), output_rows, 0.35),
        )
        for step_id, step_name, table_id, table_name, step_input, step_output, duration_share in step_specs:
            duration_us = int(elapsed_s * duration_share * 1_000_000)
            local_spill = int(spill * duration_share) if spill else 0
            remote_spill = int(local_spill * (0.20 if archetype == "EXTERNAL_S3" else 0.04))
            flow_rows.append(
                {
                    "user_id": user_id, "query_id": query_id, "child_query_sequence": 1,
                    "metrics_level": "step", "step_name": step_name, "step_id": step_id,
                    "step_attribute": movement if step_id == 2 else "",
                    "stream_id": 0, "segment_id": 0, "plan_node_id": step_id,
                    "plan_parent_id": 1 if step_id == 2 else 2,
                    "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_time": (start_time + timedelta(microseconds=duration_us)).strftime("%Y-%m-%d %H:%M:%S.%f"),
                    "duration": duration_us, "table_id": table_id, "table_name": table_name,
                    "source": "s3" if step_name == "s3scan" else "local",
                    "is_rrscan": "t" if step_id == 3 and str(fact.get("sortkey1") or "") else "f",
                    "input_bytes": int(input_bytes * duration_share), "input_rows": int(step_input),
                    "output_bytes": int(output_bytes * duration_share), "output_rows": int(step_output),
                    "blocks_read": int(input_bytes * duration_share / 8192),
                    "blocks_write": int(output_bytes * duration_share / 8192),
                    "local_read_io": int(local_read * duration_share),
                    "remote_read_io": int(remote_read * duration_share),
                    "spilled_block_local_disk": local_spill,
                    "spilled_block_remote_disk": remote_spill,
                    "data_skewness": round(max_data_skew, 2), "time_skewness": round(max_time_skew, 2),
                    "alert": dominant_issue if alert_count else "", "detail_row_count": 1,
                    "max_duration": duration_us, "alert_count": 1 if alert_count else 0,
                    "pain_score": round(cost_score * duration_share, 2), "dominant_pain": dominant_issue,
                    "pain_points": movement if step_id == 2 else archetype,
                    "detail_row_rank": step_id, "pain_rank": step_id,
                }
            )
    return (
        pd.DataFrame(history_rows),
        pd.DataFrame(detail_rows),
        pd.DataFrame(health_rows),
        pd.DataFrame(text_rows),
        pd.DataFrame(explain_rows),
        pd.DataFrame(flow_rows),
    )


def _mock_join_column(archetype: str) -> str:
    return {
        "BAD_DISTRIBUTION": "customer_id",
        "SPILL_HEAVY": "account_id",
        "EXTERNAL_S3": "session_id",
        "NESTED_LOOP_RISK": "status",
        "MISSING_STATS": "product_id",
        "GENERAL": "product_id",
    }.get(archetype, "customer_id")


def build_external_table_info(rng: random.Random, queries: pd.DataFrame) -> pd.DataFrame:
    """Create table-grain Spectrum telemetry so the demo exercises the external-table tab."""
    start = datetime(2026, 6, 10, 0, 0, 0)
    rows: list[dict] = []
    for index in range(12):
        query_count = rng.randint(4, 42)
        scan_gb = round(rng.uniform(12, 850), 3)
        efficiency = round(rng.uniform(8, 99), 1)
        output_gb = round(scan_gb * (1.0 - efficiency / 100.0), 3)
        partitions = rng.randint(150, 15000)
        scanned = max(1, int(partitions * rng.uniform(0.01, 0.82)))
        warnings = rng.choice((0, 0, 0, 1, 2, 7, 15))
        table_name = f"raw_clickstream_{index:02d}"
        file_format = ("PARQUET", "PARQUET", "PARQUET", "ORC", "JSON", "TEXTFILE")[index % 6]
        input_format = {
            "PARQUET": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "ORC": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
            "JSON": "org.apache.hadoop.mapred.TextInputFormat",
            "TEXTFILE": "org.apache.hadoop.mapred.TextInputFormat",
        }[file_format]
        serialization_lib = {
            "PARQUET": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "ORC": "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
            "JSON": "org.openx.data.jsonserde.JsonSerDe",
            "TEXTFILE": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        }[file_format]
        partition_columns = "event_date, region_code" if index % 4 != 3 else ""
        table_parameters = f"{{'numRows':'{rng.randint(2_000_000, 900_000_000)}'}}" if index % 3 == 0 else "{}"
        rows.append(
            {
                "external_table_key": f"dev.spectrum.{table_name}",
                "redshift_database_name": "dev", "schema_name": "spectrum", "table_name": table_name,
                "s3_location": f"s3://demo-datalake/clickstream/{index:02d}/", "table_type": "EXTERNAL TABLE",
                "input_format": input_format, "output_format": "", "serialization_lib": serialization_lib,
                "serde_parameters": "{}", "compressed": 1, "table_parameters": table_parameters,
                "column_count": rng.randint(18, 85), "partition_key_count": 2 if partition_columns else 0,
                "partition_key_columns": partition_columns,
                "catalog_partition_count": partitions, "observation_start_time": start,
                "observation_end_time": start + timedelta(days=7), "query_count": query_count,
                "external_segment_count": query_count * rng.randint(2, 12), "user_count": rng.randint(1, 9),
                "gross_scan_bytes": int(scan_gb * 1024**3), "gross_scan_gb": scan_gb,
                "gross_output_bytes": int(output_gb * 1024**3), "gross_output_gb": output_gb,
                "gross_scan_rows": int(scan_gb * rng.uniform(3_000_000, 15_000_000)),
                "gross_output_rows": int(output_gb * rng.uniform(1_000_000, 8_000_000)),
                "output_metric_match_count": query_count, "output_to_scan_byte_ratio": output_gb / scan_gb,
                "byte_reduction_pct_estimate": efficiency, "output_to_scan_row_ratio": 1.0 - efficiency / 100.0,
                "row_filter_efficiency_pct": efficiency,
                "filtering_assessment": "Efficient" if efficiency >= 90 else "Moderate" if efficiency >= 50 else "Poor",
                "total_partitions_considered": partitions, "qualified_partitions_scanned": scanned,
                "partition_pruning_pct": round(100.0 * (1.0 - scanned / partitions), 1),
                "pruning_event_count": query_count if scanned < partitions else 0,
                "no_pruning_event_count": 0 if scanned < partitions else query_count,
                "scanned_files": scanned * rng.randint(1, 6), "avg_files_per_segment": rng.randint(4, 650),
                "max_files_per_segment": rng.randint(650, 2400), "external_duration_s": rng.uniform(300, 7200),
                "avg_external_duration_s": rng.uniform(12, 420), "max_external_duration_s": rng.uniform(300, 1800),
                "s3list_time_ms": rng.uniform(400, 25000), "avg_s3list_time_ms": rng.uniform(8, 600),
                "max_s3list_time_ms": rng.uniform(500, 6000), "get_partition_time_total_raw": rng.uniform(50, 8000),
                "avg_get_partition_time_raw": rng.uniform(2, 180), "max_get_partition_time_raw": rng.uniform(50, 1800),
                "get_partition_time_unit": "ms", "recursive_scan_count": rng.randint(0, 4),
                "nested_scan_count": rng.randint(0, 3), "warning_event_count": warnings,
                "security_warning_count": 0, "partition_warning_count": warnings,
                "schema_format_warning_count": 0, "file_location_warning_count": 0,
                "connectivity_warning_count": 0, "other_warning_count": 0,
                "warning_example": "Partition pruning opportunity" if warnings else "",
                "sampled_error_count": rng.choice((0, 0, 0, 1, 3)), "queries_with_sampled_errors": 0,
                "truncation_error_count": 0, "overflow_error_count": 0, "invalid_data_error_count": 0,
                "null_handling_error_count": 0, "other_error_count": 0, "distinct_error_code_count": 0,
                "affected_column_count": 0, "max_data_skewness": rng.uniform(1, 7),
                "max_time_skewness": rng.uniform(1, 7), "external_spill_blocks": rng.randint(0, 5000),
                "observed_file_format": file_format,
            }
        )
    return pd.DataFrame(rows)


def build_table_scan_info(rng: random.Random, tables: pd.DataFrame, query_count: int) -> pd.DataFrame:
    rows: list[dict] = []
    sample = tables.sample(n=min(len(tables), max(80, len(tables) // 2)), random_state=rng.randrange(1_000_000))
    for _, table in sample.iterrows():
        scan_queries = int(clamp(rng.lognormvariate(1.35, 0.9), 1, max(2, query_count // 5)))
        rrscan_ratio = rng.uniform(0.05, 0.9) if str(table.get("sortkey1") or "") else rng.uniform(0.0, 0.25)
        rrscan = int(round(scan_queries * rrscan_ratio))
        non_rrscan = max(0, scan_queries - rrscan)
        input_rows_m = int(clamp(rng.lognormvariate(4.4, 1.2), 1, 120_000))
        output_rows_m = int(clamp(input_rows_m * rng.uniform(0.0001, 0.12), 0, input_rows_m))
        duration_s = int(clamp(input_rows_m * rng.uniform(0.35, 2.2), 4, 180_000))
        rows.append(
            {
                "full_table_name": f"{table['source_db']}.{table['schema']}.{table['table']}",
                "table_database": table["source_db"],
                "schema_name": table["schema"],
                "table_name": table["table"],
                "queries": scan_queries,
                "duration_s": duration_s,
                "input_rows_m": input_rows_m,
                "output_rows_m": output_rows_m,
                "rrscan_queries": rrscan,
                "non_rrscan_queries": non_rrscan,
            }
        )
    return pd.DataFrame(rows)


def weighted_choice(rng: random.Random, weighted: tuple[tuple[str, float], ...]) -> str:
    total = sum(weight for _, weight in weighted)
    pick = rng.random() * total
    running = 0.0
    for value, weight in weighted:
        running += weight
        if pick <= running:
            return value
    return weighted[-1][0]


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def table_name_for(rng: random.Random, kind: str, index: int) -> str:
    noun = rng.choice(
        (
            "orders",
            "claims",
            "payments",
            "customers",
            "events",
            "sales",
            "policies",
            "providers",
            "inventory",
            "sessions",
            "transactions",
            "members",
            "campaigns",
            "ledger",
        )
    )
    if kind == "dim":
        return f"dim_{noun}"
    if kind == "fact":
        return f"fact_{noun}"
    if kind == "stage":
        return f"stg_{noun}_daily"
    if kind == "archive":
        return f"fact_{noun}_archive"
    if kind == "bridge":
        return f"bridge_{noun}_{rng.choice(('account', 'policy', 'provider'))}"
    return f"audit_{noun}_{index:03d}"


def table_size_for(rng: random.Random, kind: str) -> tuple[int, int]:
    if kind in {"fact", "archive"}:
        rows = int(rng.lognormvariate(19.0, 1.0))
        rows = int(clamp(rows, 2_000_000, 4_500_000_000))
        size_mb = int(clamp(rows * rng.uniform(0.000006, 0.00003), 800, 85_000))
        return size_mb, rows
    if kind == "stage":
        rows = int(clamp(rng.lognormvariate(17.2, 0.9), 400_000, 950_000_000))
        size_mb = int(clamp(rows * rng.uniform(0.000004, 0.00002), 250, 20_000))
        return size_mb, rows
    if kind == "dim":
        rows = int(clamp(rng.lognormvariate(13.4, 1.0), 500, 35_000_000))
        size_mb = int(clamp(rows * rng.uniform(0.000002, 0.000018), 4, 4_000))
        return size_mb, rows
    rows = int(clamp(rng.lognormvariate(14.6, 0.8), 5_000, 80_000_000))
    size_mb = int(clamp(rows * rng.uniform(0.000003, 0.00002), 20, 8_000))
    return size_mb, rows


def diststyle_for(rng: random.Random, kind: str, size_mb: int, high_risk: bool) -> str:
    if high_risk and size_mb >= 1024 and rng.random() < 0.12:
        return "ALL"
    if kind == "dim" and size_mb < 800 and rng.random() < 0.55:
        return "ALL"
    if kind in {"fact", "archive", "stage"}:
        if high_risk and rng.random() < 0.45:
            return "EVEN"
        return f"KEY({rng.choice(('customer_id', 'account_id', 'claim_id', 'order_id', 'event_date_key', 'provider_id'))})"
    if rng.random() < 0.5:
        return "EVEN"
    return f"KEY({rng.choice(('id', 'customer_id', 'provider_id', 'policy_id'))})"


def sortkey_for(rng: random.Random, kind: str) -> str:
    if kind in {"fact", "archive", "stage"}:
        return rng.choice(("event_date", "created_at", "service_date", "transaction_dt", "load_date", ""))
    if kind == "dim":
        return rng.choice(("effective_date", "updated_at", "id", ""))
    return rng.choice(("created_at", "event_date", ""))


def elapsed_for(rng: random.Random, archetype: str) -> int:
    base = {
        "BAD_DISTRIBUTION": (1100, 500),
        "SPILL_HEAVY": (1500, 750),
        "EXTERNAL_S3": (1250, 620),
        "SCAN_HEAVY": (900, 420),
        "NESTED_LOOP_RISK": (2100, 1100),
        "MISSING_STATS": (820, 320),
        "TIME_SERIES_MONSTER": (2600, 1200),
        "GENERAL": (720, 180),
    }[archetype]
    return int(clamp(rng.gauss(*base), 610, 14_400))


def input_rows_for(rng: random.Random, archetype: str, fact: pd.Series) -> int:
    table_rows = int(fact["tbl_rows"])
    multiplier = {
        "TIME_SERIES_MONSTER": rng.uniform(0.35, 1.35),
        "SCAN_HEAVY": rng.uniform(0.25, 1.0),
        "SPILL_HEAVY": rng.uniform(0.18, 0.75),
        "BAD_DISTRIBUTION": rng.uniform(0.08, 0.55),
        "EXTERNAL_S3": rng.uniform(0.12, 0.8),
        "NESTED_LOOP_RISK": rng.uniform(0.03, 0.28),
        "MISSING_STATS": rng.uniform(0.04, 0.35),
        "GENERAL": rng.uniform(0.02, 0.18),
    }[archetype]
    return int(clamp(table_rows * multiplier, 250_000, 3_500_000_000))


def selectivity_for(rng: random.Random, archetype: str) -> float:
    if archetype in {"SCAN_HEAVY", "TIME_SERIES_MONSTER"}:
        return rng.uniform(0.00001, 0.04)
    if archetype == "NESTED_LOOP_RISK":
        return rng.uniform(0.8, 18.0)
    if archetype == "SPILL_HEAVY":
        return rng.uniform(0.02, 0.9)
    return rng.uniform(0.002, 0.35)


def dist_total_for(rng: random.Random, archetype: str) -> int:
    if archetype == "BAD_DISTRIBUTION":
        return rng.randint(4, 11)
    if archetype in {"SPILL_HEAVY", "TIME_SERIES_MONSTER"}:
        return rng.randint(1, 6)
    return rng.randint(0, 3)


def bcast_for(rng: random.Random, archetype: str) -> int:
    if archetype == "BAD_DISTRIBUTION":
        return rng.randint(1, 5)
    if archetype == "NESTED_LOOP_RISK":
        return rng.randint(0, 3)
    return rng.randint(0, 2)


def spill_for(rng: random.Random, archetype: str) -> int:
    if archetype == "SPILL_HEAVY":
        return int(rng.lognormvariate(11.4, 0.9))
    if archetype in {"BAD_DISTRIBUTION", "TIME_SERIES_MONSTER", "NESTED_LOOP_RISK"} and rng.random() < 0.65:
        return int(rng.lognormvariate(9.3, 0.8))
    if rng.random() < 0.18:
        return int(rng.lognormvariate(7.7, 0.7))
    return 0


def remote_io_for(rng: random.Random, archetype: str) -> float:
    if archetype in {"EXTERNAL_S3", "SCAN_HEAVY", "TIME_SERIES_MONSTER"}:
        return round(clamp(rng.gauss(0.34, 0.18), 0.02, 0.92), 3)
    return round(clamp(rng.gauss(0.12, 0.11), 0.0, 0.65), 3)


def skew_for(rng: random.Random, archetype: str) -> float:
    if archetype in {"BAD_DISTRIBUTION", "SPILL_HEAVY", "TIME_SERIES_MONSTER"}:
        return clamp(rng.lognormvariate(1.2, 0.48), 1, 14)
    return clamp(rng.lognormvariate(0.45, 0.42), 1, 8)


def external_pct_for(rng: random.Random, archetype: str) -> float:
    if archetype == "EXTERNAL_S3":
        return clamp(rng.gauss(0.48, 0.2), 0.12, 0.92)
    if rng.random() < 0.08:
        return clamp(rng.gauss(0.22, 0.12), 0.05, 0.55)
    return 0.0


def dominant_issue_for(archetype: str, dist_both: int, nested_loop: int, s3_scans: int, missing_stats: int, scans: int) -> str:
    if dist_both:
        return "BAD_DISTRIBUTION"
    if nested_loop:
        return "NESTED_LOOP_RISK"
    if s3_scans:
        return "S3_HEAVY"
    if missing_stats:
        return "MISSING_STATS"
    if scans > 8:
        return "SCAN_HEAVY"
    return "GENERAL"


def cost_tier_for(score: int) -> str:
    if score > 80:
        return "VERY_EXPENSIVE"
    if score > 35:
        return "EXPENSIVE"
    if score > 15:
        return "MODERATE"
    return "LIGHT"


def sql_for(rng: random.Random, archetype: str, fact: pd.Series, dim: pd.Series, index: int) -> str:
    fact_ref = f"{fact['schema']}.{fact['table']}"
    dim_ref = f"{dim['schema']}.{dim['table']}"
    date_col = str(fact["sortkey1"] or "event_date")
    if archetype == "TIME_SERIES_MONSTER":
        return (
            f"SELECT customer_id, SUM(net_amount) AS net_amount FROM {fact_ref} "
            f"WHERE {date_col} BETWEEN DATE '2024-01-01' AND DATE '2026-06-01' "
            f"GROUP BY customer_id UNION SELECT customer_id, SUM(net_amount) FROM {fact_ref} "
            f"WHERE {date_col} BETWEEN DATE '2021-01-01' AND DATE '2023-12-31' "
            "GROUP BY customer_id ORDER BY 2 DESC"
        )
    if archetype == "BAD_DISTRIBUTION":
        return (
            f"SELECT d.region, SUM(f.net_amount) FROM {fact_ref} f "
            f"JOIN {dim_ref} d ON f.customer_id = d.customer_id "
            f"WHERE f.{date_col} >= DATE '2025-01-01' GROUP BY d.region ORDER BY 2 DESC LIMIT 500"
        )
    if archetype == "SPILL_HEAVY":
        return (
            f"SELECT d.segment, f.channel, COUNT(*), SUM(f.net_amount) FROM {fact_ref} f "
            f"JOIN {dim_ref} d ON f.account_id = d.account_id "
            "GROUP BY d.segment, f.channel, f.product_id, f.provider_id"
        )
    if archetype == "EXTERNAL_S3":
        return (
            f"SELECT * FROM spectrum.raw_clickstream_{index % 12:02d} s "
            f"JOIN {fact_ref} f ON s.session_id = f.session_id "
            "WHERE s.load_date >= DATE '2026-01-01'"
        )
    if archetype == "NESTED_LOOP_RISK":
        return (
            f"SELECT COUNT(*) FROM {fact_ref} f CROSS JOIN {dim_ref} d "
            "WHERE LOWER(d.description) LIKE '%urgent%' AND f.status = d.status"
        )
    if archetype == "MISSING_STATS":
        return (
            f"SELECT f.customer_id, d.category, SUM(f.net_amount) FROM {fact_ref} f "
            f"JOIN {dim_ref} d ON f.product_id = d.product_id "
            "WHERE d.updated_at >= CURRENT_DATE - INTERVAL '365 day' GROUP BY 1,2"
        )
    if archetype == "SCAN_HEAVY":
        return f"SELECT * FROM {fact_ref} WHERE LOWER(notes) LIKE '%exception%' ORDER BY created_at"
    return (
        f"SELECT d.category, SUM(f.net_amount) FROM {fact_ref} f "
        f"JOIN {dim_ref} d ON f.product_id = d.product_id "
        f"WHERE f.{date_col} >= DATE '2026-01-01' GROUP BY 1 LIMIT 1000"
    )


def split_sql(sql: str, parts: int) -> list[str]:
    chunk = math.ceil(len(sql) / parts)
    return [sql[i * chunk : (i + 1) * chunk] for i in range(parts)]


if __name__ == "__main__":
    raise SystemExit(main())
