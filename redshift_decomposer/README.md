<div align="center">

# Redshift Query Decomposer

**Compiles a monolithic Amazon Redshift query into a staged, DISTKEY/SORTKEY-tuned
pipeline of temp tables — with the reasoning shown.**

[![PyPI](https://img.shields.io/pypi/v/redshift-query-decomposer)](https://pypi.org/project/redshift-query-decomposer/)
[![Python](https://img.shields.io/pypi/pyversions/redshift-query-decomposer)](https://pypi.org/project/redshift-query-decomposer/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/lcapece/redshift-query-decomposer/blob/main/LICENSE)
[![Built on SQLGlot](https://img.shields.io/badge/built%20on-SQLGlot-blueviolet)](https://github.com/tobymao/sqlglot)

</div>

Somewhere right now, a 2-billion-row table is being scanned in full to answer a
question about last Tuesday. This library exists for that query: it parses
Redshift SQL, explodes the views, pushes the safe predicates down, prunes the
columns, picks distribution and sort keys for each stage, and hands back a
script of narrow temp tables plus a rewritten final query — **and explains
every decision it made**.

```bash
pip install redshift-query-decomposer
```

```python
from redshift_decomposer import decompose, assess_decomposability
```

No database connection required. The library never touches your cluster — it
has no idea what your password is, and would like to keep it that way.

> **For engineers:** This tool is **not perfect**. Use it **one query at a
> time**. Always show the **estimated conversion-success likelihood**
> (`assess_decomposability` / `plan.conversion_likelihood`) and treat the
> generated script as a **skeleton to move forward** — not a drop-in production
> rewrite. Validate row counts, nulls, and EXPLAIN vs the original before use.
> The script header and findings say the same thing on purpose.

---

## Before → After

**Before** — the query your BI tool wrote. It looks innocent, which is how
they always look:

```sql
SELECT o.order_id, c.region, o.amount
FROM analytics.reporting.v_orders o
JOIN analytics.public.dim_customer c
  ON o.cust_id = c.cust_id
WHERE o.order_date >= DATE '2024-01-01'
  AND c.segment = 'enterprise'
```

What's actually behind it: `v_orders` is a view hiding
**`fact_orders` — 2,000,000,000 rows × 14 columns (~420 GB)** — and the join
key, the filters, and the three columns anyone asked for are buried inside it.
Redshift will happily scan all of it.

**After** — actual, unedited library output, shown piece by piece with what
each piece *used to be*.

### Stage 1 — the dimension, cut down to the enterprise slice

> **This was:** `analytics.public.dim_customer`, 5M rows joined in full, its
> `segment` filter applied only at the end.
> **Now:** only enterprise rows, co-located on the join key before the join
> happens.

```sql
DROP TABLE IF EXISTS tmp_rsd_01_dim_customer;
CREATE TEMP TABLE tmp_rsd_01_dim_customer
DISTKEY("cust_id")
SORTKEY("segment")
AS
SELECT
    "cust_id", "region", "segment"
FROM "analytics"."public"."dim_customer" AS src
WHERE (src.segment = 'enterprise');
ANALYZE tmp_rsd_01_dim_customer;
```

### Stage 2 — the deeply embedded fact table, extracted to need-to-know

> **This was:** the 2-billion-row, 14-column `fact_orders` hiding inside the
> view — `channel`, `promo_code`, `warehouse_id`, `carrier`, `ship_date`,
> `return_flag`, `tax`, `discount`, `etl_batch_id` all along for a ride
> nobody asked them to take.
> **Now:** **5 of 14 columns** — the three the query returns, the join key,
> and `status` (kept because the *view's own* WHERE clause needs it — the
> planner reads view bodies, not just your query). The date filter was pushed
> **through the view alias** into the stage, `DISTKEY(cust_id)` is preserved
> from the source so the join stays local, and `SORTKEY(order_date)` keeps
> the pushed range filter fast.

```sql
DROP TABLE IF EXISTS tmp_rsd_02_fact_orders;
CREATE TEMP TABLE tmp_rsd_02_fact_orders
DISTKEY("cust_id")
SORTKEY("order_date")
AS
SELECT
    "order_id", "cust_id", "order_date", "amount", "status"
FROM "analytics"."public"."fact_orders" AS src
WHERE (src.order_date >= CAST('2024-01-01' AS DATE));
ANALYZE tmp_rsd_02_fact_orders;
```

### Final query — the view, unmasked

> **This was:** `FROM analytics.reporting.v_orders o` — a black box.
> **Now:** the view is inlined as a visible subquery *with its
> `status <> 'CANCELLED'` guard preserved*, reading from the slim staged temp
> instead of the 420 GB original. Semantics identical; I/O is not.

```sql
SELECT
  o.order_id,
  c.region,
  o.amount
FROM (
  SELECT
    order_id,
    cust_id,
    order_date,
    amount,
    status
  FROM tmp_rsd_02_fact_orders AS fact_orders
  WHERE
    status <> 'CANCELLED'
) AS o
JOIN tmp_rsd_01_dim_customer AS c
  ON o.cust_id = c.cust_id
WHERE
  o.order_date >= CAST('2024-01-01' AS DATE) AND c.segment = 'enterprise';
```

### The receipts

Every stage explains itself — this column is generated, not hand-written:

| Stage | Was | Became | Rationale (verbatim) |
|---|---|---|---|
| `tmp_rsd_02_fact_orders` | 2B rows × 14 cols behind a view | 5 need-to-know cols, date-filtered | large source (~2,000,000,000 rows, 420,000 MB); pushed 1 predicate(s); DISTKEY preserves source key cust_id; SORTKEY preserves source leading key order_date |
| `tmp_rsd_01_dim_customer` | 5M-row dim, filtered after the join | enterprise slice, filtered before it | large source (~5,000,000 rows, 800 MB); pushed 1 predicate(s); DISTKEY from join column cust_id; SORTKEY supports pushed filter on segment |

<details>
<summary><b>The full example, runnable</b> (click to expand)</summary>

```python
from redshift_decomposer import Catalog, TableStats, ViewDef, decompose

catalog = Catalog(
    tables={
        "analytics.public.fact_orders": TableStats(
            columns={c: "VARCHAR" for c in (
                "order_id", "cust_id", "order_date", "amount", "status",
                "channel", "promo_code", "warehouse_id", "carrier",
                "ship_date", "return_flag", "tax", "discount", "etl_batch_id",
            )},
            diststyle="KEY",
            distkey="cust_id",
            sortkeys=("order_date",),
            rows=2_000_000_000,
            size_mb=420_000,
        ),
        "analytics.public.dim_customer": TableStats(
            columns={"cust_id": "BIGINT", "region": "VARCHAR", "segment": "VARCHAR"},
            diststyle="ALL",
            rows=5_000_000,
            size_mb=800,
        ),
    },
    views={
        "analytics.reporting.v_orders": ViewDef(
            sql="""
            SELECT order_id, cust_id, order_date, amount, status
            FROM analytics.public.fact_orders
            WHERE status <> 'CANCELLED'
            """
        ),
    },
)

plan = decompose(
    """
    SELECT o.order_id, c.region, o.amount
    FROM analytics.reporting.v_orders o
    JOIN analytics.public.dim_customer c
      ON o.cust_id = c.cust_id
    WHERE o.order_date >= DATE '2024-01-01'
      AND c.segment = 'enterprise'
    """,
    catalog,
)

print(plan.script)
for stage in plan.stages:
    print(stage.name, stage.distkey, stage.sortkeys, stage.rationale)
```

</details>

---

## Triage first: estimated conversion-success likelihood

Hand an engineer one slow query — not a bulk queue. The scorer answers in
milliseconds (parse-only; no catalog, no connection). The **0.0–1.0 score is
the estimated likelihood of a successful conversion** into a usable staged
skeleton — not a runtime-savings estimate, and not a guarantee. Show them this
number. Every deduction is a named signal tied to real planner behavior.

```python
from redshift_decomposer import assess_decomposability, decompose

report = assess_decomposability(sql)
print(report.summary())          # likelihood + "not perfect / skeleton" framing
# after decompose(...):
# print(plan.conversion_likelihood, plan.engineer_brief())
```

```text
Redshift Query Decomposer is not perfect — treat output as a skeleton,
not a guaranteed production rewrite. One query at a time; engineer review required.
[######----] 0.65  estimated conversion-success likelihood
  MODERATE estimated conversion success — expect review findings; not drop-in
  likelihood reductions:
  -0.25  Correlated subquery: ...
```

| Band | Meaning |
|------|---------|
| **≥ 0.70 HIGH** | Higher chance of a usable skeleton — still validate |
| **0.40–0.70 MODERATE** | Skeleton expected; review findings before running |
| **0.05–0.40 LOW** | Show the reasons; cautious sketch only |
| **≤ 0.05 UNLIKELY** | Not a reliable candidate (unparseable, DML, recursive CTE, …) |

It knows about SUPER/JSON manipulation, recursive CTEs, **true** correlated
subqueries (uncorrelated `IN (SELECT …)` is not penalized), multi-alias
self-joins, the CTE filter-pushdown gap, Spectrum/external schemas, set
operations, missing filters, deep nesting, and `SELECT *` — which remains
perfectly legal; proving it was a good idea is another matter.
Also runs standalone with nothing but `sqlglot` installed:

```bash
python -m redshift_decomposer.triage        # paste a query interactively
```

---

## How it works

```text
 monolithic SQL ──▶ parse ──▶ explode views ──▶ analyze lineage & predicates
 (sqlglot, redshift dialect)                            │
                                                        ▼
   final query  ◀── rewrite ◀── plan stages (CTAS + DISTKEY/SORTKEY
  (over temps)                  + column pruning + rationale + safety label)
```

| You provide | Redshift Query Decomposer produces |
|-------------|------------------------------------|
| Query text | Multi-statement Redshift script |
| Column schemas for referenced tables | Qualified / pruned stage SELECTs |
| Optional physical stats (rows, size, dist/sort keys) | DISTKEY / SORTKEY on temps |
| View SQL definitions | Inlined (exploded) physical plan |

---

## Feeding it a catalog

<details>
<summary><b>Cluster-wide table repository cache</b> — recommended; build once, reuse forever</summary>

`SVV_TABLE_INFO` is **per-database** and can be monstrously slow — run the
build overnight so it queries every database once and you never have to again:

1. List **local** databases (`svv_redshift_databases` where `database_type = 'local'` — excludes datashares)
2. Open a **new connection per database** (Redshift cannot switch DB in-session)
3. Capture full `SVV_TABLE_INFO` + **`pg_table_def`** (compound sort key positions, distkey, types)
4. Store everything in **one SQLite file**

Lookup policy: **cache hit → use it; miss → live SVV/pg_table_def** (optional write-through).

```python
import redshift_connector
from redshift_decomposer import build_table_repository, decompose, TableRepository

def connect(database: str):
    return redshift_connector.connect(
        host="...", database=database, user="...", password="..."
    )

# Slow path — schedule overnight / after major DDL
report = build_table_repository(
    connect,
    path="C:/cache/redshift_table_repo.sqlite",
    bootstrap_database="analytics",
)
print(report.databases_ok, report.table_count)

# Fast path — metrics from cache; live only on misses / views
plan = decompose(
    sql,
    repository="C:/cache/redshift_table_repo.sqlite",
    connect=connect,
    database="analytics",
)

repo = TableRepository("C:/cache/redshift_table_repo.sqlite")
print(repo.get_table("analytics", "public", "fact_orders").sortkeys)  # full compound key
```

**Sort keys:** `SVV_TABLE_INFO.sortkey1` is only the leading column. The
repository merges **`pg_table_def.sortkey`** positions so compound
`SORTKEY(a, b, c)` is preserved.

</details>

<details>
<summary><b>Live fetch / offline frames</b> — the quick alternatives</summary>

```python
plan = decompose(sql, connection=conn)  # current database only
```

Offline frames (e.g. DataBasix DuckDB):

```python
from redshift_decomposer import catalog_from_databasix_frames, decompose

catalog = catalog_from_databasix_frames(table_info_df, view_definitions_df)
plan = decompose(sql, catalog)
```

</details>

<details>
<summary><b>Non-superuser / <code>metadata_schema</code></b> — switch + exact field names</summary>

### The switch

Pass a non-empty **`metadata_schema`** to live catalog paths. The package then
reads from **that schema** using the **same bare table names** as the system
catalogs (not renamed tables):

| System relation | When `metadata_schema="my_meta"` |
|-----------------|----------------------------------|
| `SVV_TABLE_INFO` | `my_meta.svv_table_info` |
| `pg_table_def` | `my_meta.pg_table_def` |
| `pg_views` | `my_meta.pg_views` |
| `information_schema.columns` | `my_meta.columns` |
| `SVV_EXTERNAL_COLUMNS` | `my_meta.svv_external_columns` |

```python
plan = decompose(
    sql,
    connection=conn,
    database="analytics",
    metadata_schema="my_meta",   # None / "" = system catalogs
)
```

Also accepted on `fetch_catalog_for_sql`, `fetch_catalog_for_refs`,
`fetch_catalog_with_repository`, `build_table_repository`, and
`TableRepository.build`.

Mirrors need only the columns this package SELECTs (extras are fine and
ignored). You can also still feed rows offline via `catalog_from_rows` /
`Catalog` without using the switch.

### External columns — summary is enough

For **`svv_external_columns`**, the package does **not** load a full column
list. It only harvests the **first partition key**:

| Exact field name | Meaning |
|------------------|---------|
| **`schemaname`** | Schema of the external table |
| **`tablename`** | Table name |
| **`columnname`** | Partition-key **column** name (only where `part_key = 1`) |
| **`part_key`** | Must be `1` for the first partition key (filter in SQL) |
| **`redshift_database_name`** | Optional database |

A thin summary table (one row per external table) is valid if it uses these
names. Result lands on `TableStats.partition_key` and marks the table
`is_external=True`.

### Table-metrics mirror (like `SVV_TABLE_INFO`)

Use these **exact field names** in the result set (case-insensitive; the loader
normalizes keys to lower case). Aliases listed on the same line are accepted
equivalents.

#### Identity (required)

| Field name(s) | Meaning |
|---------------|---------|
| **`schema`** (or `schema_name`, `schemaname`) | Schema of the physical table |
| **`table`** or **`table_name`** (or `tablename`) | Table name |
| **`database`** (or `source_db`, `redshift_database_name`) | Optional but recommended when multi-DB; else pass `default_database=` |

Without **`schema` + `table`/`table_name`**, the row is ignored.

#### Physical design & size (strongly recommended — drives staging / keys)

| Field name(s) | Meaning | Used for |
|---------------|---------|----------|
| **`tbl_rows`** (or `rows`) | Approximate row count | Stage if ≥ `minimum_rows` |
| **`size`** (or `size_mb`) | Size in **MB** (Redshift `SVV_TABLE_INFO.size` is already MB) | Stage if ≥ `minimum_size_mb` |
| **`diststyle`** | e.g. `EVEN`, `ALL`, `KEY(cust_id)` | Style + parse dist key |
| **`distkey`** | Dist-key column name | Preferred over parsing `diststyle` when set |
| **`sortkey1`** (or `sortkey`) | Leading sort-key column (SVV name is **`sortkey1`**) | Stage `SORTKEY` fallback |
| **`sortkeys`** | Optional list/tuple of full compound sort keys | Better than `sortkey1` alone |

#### Health / stats fields you asked about

| Field name | Meaning | Notes |
|------------|---------|--------|
| **`stats_off`** | Stats off percentage (SVV_TABLE_INFO.`stats_off`) | Stored in the table repository cache. **Not** consumed by the planner today for stage decisions, but include it so mirrors stay SVV-compatible and future versions can use it. |
| **`sortkey1`** | Leading sort key (SVV_TABLE_INFO.`sortkey1`) | **Used** (see above). |
| **`distkey`** | Distribution key column | **Used** when present; else parsed from `diststyle` via `KEY(...)`. |
| **`unsorted`** | Percent of the table that is **unsorted** (SVV_TABLE_INFO.`unsorted`) | This is Redshift’s field — **not** “percent sorted.” Percent sorted ≈ `100 - unsorted`. Stored in the repository as **`unsorted`**. **Not** used by the planner today; still include it for parity. There is **no** accepted alias `percent_sorted` / `pct_sorted` yet. |

#### Other fields the package knows / stores (include when available)

| Field name | Meaning |
|------------|---------|
| **`table_id`** | OID / table id |
| **`encoded`** | Compression encoding summary |
| **`sortkey_num`** | Number of sort-key columns |
| **`pct_used`** | Percent of disk used |
| **`empty`** | Empty-block percentage |
| **`skew_sortkey1`** | Skew on leading sort key |
| **`skew_rows`** | Row skew across slices |
| **`estimated_visible_rows`** | Estimated visible rows |
| **`create_time`** | Table create time |
| **`max_varchar`** | Max varchar length (stored in repository `extras`) |
| **`object_type`** / **`is_external`** | Mark Spectrum / external tables |

#### Minimal example mirror (what non-superusers should prepare)

At least:

```text
schema
table          -- or table_name
tbl_rows       -- or rows
size           -- or size_mb  (megabytes)
diststyle
distkey
sortkey1
stats_off
unsorted       -- percent unsorted; percent sorted ≈ 100 - unsorted
```

Better (full SVV-shaped capture used by the repository build):

```text
database
schema
table_name     -- package also accepts column name "table"
table_id
encoded
diststyle
sortkey1
sortkey_num
size
pct_used
empty
unsorted
stats_off
tbl_rows
skew_sortkey1
skew_rows
estimated_visible_rows
create_time
distkey        -- if your extract stores it as its own column
```

Example feed path:

```python
from redshift_decomposer import catalog_from_rows, decompose

# You run: SELECT ... FROM my_meta.svv_table_info_mirror
# with the field names above in the result set.
catalog = catalog_from_rows(
    table_rows,                 # list[dict] or frame rows
    column_rows=column_rows,    # optional — see below
    view_rows=view_rows,        # optional — for view explosion
    default_database="analytics",
)
plan = decompose(sql, catalog)
```

### Column-definition mirror (like `pg_table_def` / `information_schema.columns`)

Needed for **column pruning**, types, and **full compound sort keys** (SVV only
gives `sortkey1`). Exact names accepted:

| Field name(s) | Meaning |
|---------------|---------|
| **`schema`** (or `table_schema`, `schema_name`, `schemaname`) | Schema |
| **`table_name`** (or `table`) | Table |
| **`column_name`** (or `column`) | Column |
| **`data_type`** (or `type`) | Type string |
| **`database`** (or `table_catalog`) | Optional |
| **`ordinal_position`** | Optional order |
| **`distkey`** | Boolean/flag — column is the dist key (`pg_table_def`) |
| **`sortkey`** | Integer position in compound sort key (`pg_table_def`; 0 = not in key) |
| **`encoding`** | Optional |
| **`is_not_null`** / **`notnull`** | Optional |

### View-definition mirror (like `pg_views`)

| Field name(s) | Meaning |
|---------------|---------|
| **`schema`** (or `schema_name`, `schemaname`) | Schema |
| **`view_name`** (or `viewname`) | View name |
| **`source_definition`** (or `definition`, `view_definition`) | View SQL body |
| **`database`** (or `source_db`) | Optional |
| **`is_late_binding`** | Optional |

### What the planner actually needs today vs. nice-to-have

| Need | Fields |
|------|--------|
| **Required for useful staging** | `schema`, `table`/`table_name`, `tbl_rows`/`rows`, `size`/`size_mb` |
| **Required for good DISTKEY/SORTKEY on temps** | `diststyle` and/or `distkey`, `sortkey1` (or full `sortkeys` / `pg_table_def` `sortkey` positions) |
| **Required for column pruning / qualify** | Column mirror: `column_name`, `data_type` (+ schema/table) |
| **Required for view explosion** | View mirror: `source_definition` (+ schema/view name) |
| **Stored for repository parity; not planner inputs yet** | `stats_off`, `unsorted`, `skew_rows`, `skew_sortkey1`, `pct_used`, `empty`, `encoded`, … |

**Important:** Live auto-fetch still hardcodes `FROM SVV_TABLE_INFO` (and friends).
Alternate-schema mirrors work only through **bring-your-own rows**
(`catalog_from_rows` / frames / `Catalog`) until a `metadata_schema` /
relation override is added.

</details>

---

## Design principles

- **SQLGlot-first** — parse, transform, generate with `dialect="redshift"`
- **Catalog-required for advanced work** — no silent network catalog fetches
- **Conservative defaults** — refuse or warn on unsafe boundaries (outer
  joins, multi-alias shared stages, unresolved `*`); a wrong rewrite is worse
  than no rewrite
- **Honest output** — every stage explains itself; every plan says what to review
- **Publishable core** — no GUI, DuckDB, or connector dependency

## Known limitations (0.2.x)

Stated plainly, because you should know them before trusting a plan — and
because we would rather you hear it from us:

- **Predicate pushdown reads top-level WHERE clauses only.** A large table
  referenced solely inside a CTE body or a set-operation branch can be staged
  *without* its filter — an unfiltered copy that may cost more than it saves.
  Review stage SQL before running (every plan tells you to). Fix planned for 0.3.x.
- **Column qualification degrades** when catalog keys (`db.schema.table`) and
  query references (`schema.table`) disagree on depth; plans fall back to
  catalog-order projection instead of proven pruning.
- Decomposition targets read queries; DML/DDL and recursive CTEs are refused
  — the triage scorer will tell you so before you find out the hard way.
- This is *not* query decorrelation, federation, or a dbt replacement — it
  rewrites one query into explicit staged steps you can read and tune.

## Status

Alpha (`0.2.x`) — the API may still change; the honesty is permanent. Focus
is a correct, testable decomposition pipeline that can grow more advanced
staging strategies without breaking the public API.

## License

Licensed under the **Apache License, Version 2.0**. See [LICENSE](LICENSE)
and [NOTICE](NOTICE).

Copyright 2026 Louis N. Capece, Jr.
