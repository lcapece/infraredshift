# redshift-sqlopt

**Evidence-driven Amazon Redshift query optimization.** Optimizes queries that
have *already run*, using their own execution plan as proof.

```bash
pip install redshift-sqlopt
```

Built on [SQLGlot](https://github.com/tobymao/sqlglot). One dependency. Pure
Python. No database connection required, no build tools, no compilation.

---

## Two guarantees

**Deterministic.** Every rewrite is rule-based and refuses unless its
preconditions are proven against catalog metadata. A rewrite that cannot be
justified is reported as *blocked, with the reason* — never applied on a guess.

**Predictive.** Findings carry measured numbers from `SYS_QUERY_EXPLAIN` and
`SYS_QUERY_DETAIL` — rows broadcast, bytes spilled, planner estimate error — so
a recommendation tells you what it is worth, not merely that it exists.

---

## The escalation ladder

Fixes are ranked cheapest-first, and that ordering is the product:

| Tier | Fix | Cost to adopt |
|------|-----|---------------|
| **1 — REWRITE** | Reorganize the SQL in place | Deploy new SQL. Reversible. |
| **2 — DDL** | Add the missing DISTKEY / SORTKEY | Maintenance window. Fixes *every* query on that table. |
| **3 — DECOMPOSE** | Stage into temp tables | Invasive. Hand off to [`redshift-query-decomposer`](https://pypi.org/project/redshift-query-decomposer/). Last resort. |

A large table with no distribution key is a **DDL finding, not a decomposition
finding**. Decomposing queries around an unkeyed table treats the symptom while
every other query on that table keeps paying the same cost. One `ALTER TABLE`
fixes the whole workload.

---

## Use

```python
from redshift_sqlopt import Catalog, optimize

catalog = Catalog.from_rows(table_rows=svv_table_info_rows)

result = optimize(
    sql,
    catalog=catalog,
    explain_rows=sys_query_explain_rows,
    detail_rows=sys_query_detail_rows,
)

print(result.summary())

if result.has_rewrite:
    print(result.rewritten_sql)      # validate before deploying

for finding in result.ranked_findings():
    print(finding.tier.name, finding.title, finding.estimated_benefit)
```

### What it produces

```
[REWRITE/HIGH] Step 7: 84.0 GB spilled to disk
  evidence: step 7, XN Hash Join DS_BCAST_INNER, BROADCAST,
            2,100,000,000 rows, 2,100,000x over plan estimate, 412.5s

[DDL/CRITICAL] Step 7: broadcast of 2,100,000,000 rows
  DDL: ALTER TABLE analytics.public.fact_orders
         ALTER DISTSTYLE KEY DISTKEY (<join_column>);
  benefit: Eliminates network movement of 2.1B rows on every run.
```

And the rewrite itself:

```sql
-- before: DATE() defeats zone-map pruning, so every block is read
WHERE DATE(o.order_date) = '2024-01-01'

-- after: sargable half-open range, same rows, block pruning restored
WHERE o.order_date >= '2024-01-01'
  AND o.order_date <  DATEADD(day, 1, '2024-01-01')
```

---

## Order-anonymous fingerprinting

Queries that differ only in clause order, literal values, or alias names
collapse to one identity — so a single fix can be credited against every run of
that shape.

```python
from redshift_sqlopt import same_shape

same_shape("SELECT a, b FROM t WHERE x = 1 AND y = 2",
           "SELECT b, a FROM t WHERE y = 2 AND x = 1")   # True
same_shape("SELECT a FROM t ORDER BY a ASC",
           "SELECT a FROM t ORDER BY a DESC")            # False
```

`ORDER BY` is never normalized away — its order is the entire point.

> The fingerprint canonicalizer sorts the SELECT list, which is safe for hashing
> and **unsafe to execute** (it would break `GROUP BY 1, 2` positional
> references). Fingerprinting and SQL emission are deliberately separate code
> paths for this reason.

---

## Rules

| Rule | Precondition it must prove |
|------|----------------------------|
| `SARGABLE_SORTKEY` | Column is a sort key **and** the wrapper and constant are both date-valued |
| `NOT_IN_TO_NOT_EXISTS` | Subquery projects one plain column **and** that column is NOT NULL per the catalog |
| `REDUNDANT_DISTINCT` | Every projection is a grouping key or aggregate |
| `PROPAGATE_JOIN_FILTER` | Join is INNER (never OUTER) |

`NOT IN` and `NOT EXISTS` differ *exactly* when the subquery column contains
NULLs — `NOT IN` then returns zero rows. So the rewrite is applied only when the
catalog proves the column is NOT NULL. Without that proof it is reported as
blocked, with a note that the query is worth reviewing by hand: if NULLs are
present, it is already returning wrong results.

To supply nullability, include `not_null_columns` in your catalog rows:

```python
Catalog.from_rows(table_rows=[
    {"schema": "public", "table": "dim_customer",
     "distkey": "cust_id", "sortkeys": "cust_id",
     "not_null_columns": "cust_id,region"},
])
```

An empty `not_null_columns` means **unknown**, never "nullable" — rules treat
absence of evidence as unproven and refuse.

Generic rewrites — predicate pushdown, subquery unnesting, projection pruning,
boolean simplification — come from SQLGlot's own optimizer and are not
reimplemented here.

---

## Safety

Every candidate rewrite passes a validation gate before it is emitted:

- rewritten SQL re-parses as Redshift SQL;
- the referenced table set is unchanged;
- no column identifier was invented;
- the top-level projection contract is unchanged (count, order, and aliases).

Anything failing these checks is discarded and reported as blocked.

**These checks are necessary, not sufficient.** Nothing short of running both
queries proves equivalence. Always `EXPLAIN` and compare row counts before
deploying a rewrite. The tool says so in its own output, on purpose.

If the SQL does not parse, rewriting stops — you cannot safely reorganize a
statement you could not read. Plan- and catalog-derived findings still work,
because they never needed the AST.

---

## Offline / air-gapped install

The only hard dependency is `sqlglot`. To install with no network access:

```bash
# on a connected machine
pip download redshift-sqlopt -d ./bundle

# on the locked-down machine
pip install --no-index --find-links ./bundle redshift-sqlopt
```

Live cluster access is optional and lazily imported:

```bash
pip install redshift-sqlopt[redshift]   # adds redshift-connector
```

Omit it if the destination already has `redshift_connector`, or if you feed
plan rows in from elsewhere — `optimize()` never opens a connection itself.

---

## Known limitations

**View inlining changes the shape of `rewritten_sql`.** When the catalog
contains view definitions, views are expanded *before* rules run, so the
returned SQL references base tables rather than the view. That is intentional —
it is what lets catalog and plan reasoning see the real table — but it means the
output is not a minimal edit of your original text. Pass `expand_views=False` if
you want rewrites expressed against the view.

**No connection helper yet.** `optimize()` accepts already-fetched rows and
never opens a connection. The `[redshift]` extra exists for callers that want
`redshift-connector` available, but no `from_connection()` convenience wrapper
ships in 0.1.0 — fetch the SYS rows with your existing tooling and pass them in.

**SYS view column names are not cluster-verified.** `evidence_from_rows()`
accepts several spellings for each field (`step`/`step_id`/`stepid`,
`output_rows`/`rows`/`actual_rows`, and so on) because column naming varies
across Redshift versions. This has been exercised against synthetic rows only.
If a finding fails to appear, check that your column names are among the
accepted spellings.

**Structural validation is not proof of equivalence.** See *Safety* above.

## Requirements

- Python 3.10+
- `sqlglot >= 26.0, < 28` (tested against 26.33 and 27.29)

## License

Apache-2.0

---

*Note: this package currently duplicates a small amount of logic that also lives
in the `analyzer/` application in the same repository. The analyzer ships as a
concatenated single-file launcher, so wiring it to import this package is a
separate change; the duplication is intentional and temporary.*
