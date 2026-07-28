# Changelog

All notable changes to `redshift-sqlopt` are recorded here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning is [semantic](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-07-27

First release. Alpha: the API may change before 1.0.

### Added

- **Order-anonymous fingerprinting.** Queries differing only in clause order,
  literal values, table aliases, or `IN`-list length collapse to one identity,
  so a single fix can be credited against every run of that shape. `ORDER BY`
  is never normalised away - its order is the semantics.
- **Four Redshift-specific rewrite rules**, each of which refuses unless its
  precondition is proven against catalog metadata:
  - `SARGABLE_SORTKEY` - unwraps a date function from a sort-key column so zone
    maps can prune again. Requires a catalogued sort key, a date-valued wrapper
    and constant, and day granularity.
  - `NOT_IN_TO_NOT_EXISTS` - requires the subquery column to be provably
    `NOT NULL` and both sides to resolve to distinct aliases.
  - `REDUNDANT_DISTINCT` - requires every projection to be a grouping key or an
    aggregate.
  - `PROPAGATE_JOIN_FILTER` - `INNER` joins only; refuses on `OUTER`, where it
    would drop rows the join is meant to preserve.
- **Plan-evidence findings** from `SYS_QUERY_EXPLAIN` and `SYS_QUERY_DETAIL`:
  broadcast/redistribution volume, spill, planner estimate error, wide scans,
  unkeyed and skewed tables. Each carries measured numbers, not heuristics.
- **Escalation ladder** - findings rank cheapest-fix-first: rewrite the query,
  else change the table, else decompose. An unkeyed large table is a DDL
  finding, because one `ALTER TABLE` fixes every query touching it.
- **View explosion** from catalog definitions, so reasoning happens against real
  base tables rather than a name hiding a two-billion-row scan.
- **Validation gate.** Every candidate rewrite must re-parse, preserve the table
  set, invent no columns, keep the projection contract, and introduce no
  self-referential predicate. Failures are reported as blocked, with the reason.
- **CLI** - `python -m redshift_sqlopt`, with `--json` and `--sql-only`.

### Verified

- SYS and SVV column names and units checked against the published Redshift
  `pg_catalog` schema rather than assumed. Spill is reported in 1 MiB blocks
  (`spilled_block_local_disk`), duration in microseconds, and the planner's row
  estimate is embedded in `plan_info` text rather than exposed as a column - all
  three are handled.
- 351 tests, including a 90-scenario corpus spanning window functions over
  joins, CTE chains, correlated subqueries, DML, views over views, and malformed
  input, run against a catalog with deliberate physical-design flaws.
- Wheel install verified in a clean virtualenv against sqlglot 26.33 and 27.29.

### Known limitations

- Finding thresholds are not calibrated against a production workload.
- No `from_connection()` helper; `optimize()` accepts already-fetched rows and
  never opens a connection itself.
- View inlining means `rewritten_sql` references base tables rather than the
  view. Pass `expand_views=False` to keep the view.
- Structural validation is necessary but not sufficient: always `EXPLAIN` and
  compare row counts before deploying a rewrite.

[0.1.0]: https://github.com/lcapece/redshift/tree/main/redshift_sqlopt
