# Changelog

## 0.2.3 - 2026-07-21

- **License:** project relicensed from MIT to **Apache License 2.0** (see
  `LICENSE` and `NOTICE`). Earlier PyPI artifacts remain under the license
  they shipped with; this and later source trees are Apache-2.0.
- **`metadata_schema` switch:** when a non-empty schema name is supplied to
  `decompose` / `fetch_catalog_for_sql` / `build_table_repository` / etc.,
  catalog SQL is retargeted to that schema using the **same bare table names**
  as system catalogs: `svv_table_info`, `pg_table_def`, `pg_views`, `columns`
  (mirrors `information_schema.columns`), and `svv_external_columns`. Mirrors
  need only the columns the package SELECTs.
- **`svv_external_columns` harvest is summary-only:** database (optional),
  `schemaname`, `tablename`, and `columnname` where `part_key = 1` (first
  partition key). Full external column lists are not required. Stored on
  `TableStats.partition_key` and marks the table external.
- **Engineer communication:** the tool is explicitly framed as imperfect and
  **query-by-query**. Triage summary, `DecompositionPlan.engineer_brief()`,
  script SQL headers, and a required finding all surface
  **estimated conversion-success likelihood** and call the output a
  **skeleton to move forward**, not a drop-in production rewrite.
- `DecompositionPlan.conversion_likelihood` / `conversion_verdict` populated
  on every `decompose()` path (independent of the existing payoff `score`).

## 0.2.2 - 2026-07-21

Correctness hardening + triage overhaul:

- **Fix:** predicate pushdown no longer accepts non-simple filters via a
  column-lineage bypass of `_is_simple_pushable` (OR groups, volatiles, etc.
  stay on the final query).
- **Fix:** multi-reference physical tables (same alias twice, e.g. CTE body +
  outer scan) no longer share one over-filtered stage.
- **Fix:** `getdate` / `current_timestamp` / `current_date` / `now` and similar
  are non-pushable (stage time vs final-eval time skew).
- **Fix:** `decompose()` refuses DML/DDL roots and `WITH RECURSIVE`; only
  SELECT / UNION / set queries are staged.
- **Fix:** empty `sql_pg_table_def_for_tables([])` no longer emits invalid SQL
  (`ORDER BY ... AND 1=0`); uses a complete `WHERE 1=0` SELECT.
- **Triage scorer overhaul** (`assess_decomposability`): reframed as a true
  **likelihood that `decompose()` will produce a usable plan** (not a value
  or effort estimate). Verdicts now read HIGH / MODERATE / LOW / UNLIKELY.
- **Fix:** uncorrelated `IN (SELECT …)` and `UNION` branches were false-flagged
  as correlated (sqlglot's `external_columns` over-fires). Correlation is now
  detected only when a nested scope references an outer relation alias.
- **New signals aligned with real planner behavior / known limitations:**
  multi-alias self-joins (`safety=review`), CTE filter gap (outer WHERE on a
  CTE alias while the physical table lives only inside the CTE body), external
  / Spectrum schema hints, CROSS JOIN without equijoin keys, OR-in-WHERE,
  mixed name-qualification depth, HAVING-only filters.
- Report gains `likelihood` (alias of `score`), `blocking`, impact-sorted
  signals, and a clearer summary header.

## 0.2.1 - 2026-07-21

- **Fix:** a column used only in an exploded view's WHERE clause (e.g. the
  view filters on `status` but never projects it into your query) was pruned
  out of the staged temp, producing a script that referenced a column the
  temp did not have. Unqualified columns now attribute to their enclosing
  scope's sources, not the whole query.
- Docs: annotated Before/After walkthrough (deeply embedded fact table
  extracted to a need-to-know stage), modern README layout, badges.
- Generated script banner now names the published package.

## 0.2.0 - 2026-07-21

First public release on PyPI as **redshift-query-decomposer**.

- Query decomposition: view explosion, lineage-aware predicate pushdown,
  column pruning, DISTKEY/SORTKEY selection, staged temp-table script emission
  with per-stage rationale and safety labels.
- Cluster-wide table repository cache (SQLite, cache-first with live
  fallback and write-through), merging `SVV_TABLE_INFO` with `pg_table_def`
  so compound sort keys are preserved.
- **New: decomposability triage** (`assess_decomposability`) - a fast,
  parse-only 0.0-1.0 pre-flight score with named risk signals (SUPER/JSON
  manipulation, recursive CTEs, correlated subqueries, LATERAL, set
  operations, missing filters, deep nesting). Also runnable as a standalone
  interactive script.

Known limitations (tracked for 0.3.x):

- Predicate pushdown inspects top-level WHERE clauses only; a large table
  referenced solely inside a CTE or set-operation branch can be staged
  without its filter. Review stage SQL before running (every plan says so).
- SQLGlot column qualification is skipped when catalog keys and query
  references disagree on qualification depth; plans fall back to
  catalog-order projection.

## 0.1.0

Internal development versions (not published).
