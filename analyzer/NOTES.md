# Redshift Analyzer Notes

## Current Install State

Installed and verified with `python -m pip install -r analyzer\requirements.txt`:

- `PySide6 6.11.1`
- `pandas 3.0.3`
- `numpy 2.5.0`
- `duckdb 1.5.4`
- `redshift-connector 2.1.15`

`JayDeBeApi` / `JPype1` are intentionally not core requirements. On this
Windows ARM / Python 3.13 environment, `JPype1` attempted a source build and
failed without local C/C++ build tools. JDBC mode remains optional for machines
that already have a compatible JPype setup. Native Redshift protocol capture is
the default installable path.

## Mock Dataset

Generated file:

```text
analyzer\samples\mock_redshift_3300.duckdb
```

Generator:

```powershell
python -m analyzer.mock_data --output analyzer\samples\mock_redshift_3300.duckdb
```

Deterministic seed: `20260624`

Row distribution:

| Table | Rows |
|---|---:|
| `query_history` | 420 |
| `query_history_all` | 420 |
| `query_details` | 420 |
| `query_health` | 420 |
| `query_text` | 1,260 |
| `svv_table_info_all` | 360 |
| **Total** | **3,300** |

Validation results from `load_cluster_report()`:

- 420 slow queries
- 360 physical table records
- 3,273 triggered diagnostics
- 1,501 rewrite opportunities
- 2,148 query/family heat-map cells
- 39 high-risk tables
- 279 table blast-radius rows
- 2,091 prioritized DBA actions before UI limiting

The generated data includes realistic query and physical-design pathologies:

- `DS_DIST_BOTH` and broadcast-heavy joins
- local and remote spill
- severe execution data/time skew
- external/S3 scans
- nested-loop risk and row fan-out
- stale or missing statistics
- broad time-series scans
- large `EVEN` facts and oversized `ALL` tables
- unsorted/stale/skewed `SVV_TABLE_INFO` tables
- SQL rewrite triggers such as `SELECT *`, `UNION`, leading wildcard, full
  final sort, and cross join

## Locked-Down Delivery

Single-file artifact:

```text
redshift_analyzer_fat.py
```

Build command:

```powershell
python tools\build_fat_py.py
```

Usage:

```powershell
python redshift_analyzer_fat.py
python redshift_analyzer_fat.py --demo
python redshift_analyzer_fat.py --make-mock --output mock.duckdb
python redshift_analyzer_fat.py --ingest -- --connection native --host <host>
```

The fat file embeds the `analyzer` package as a base64 zip and extracts it to
the system temp directory at runtime. It still requires the pip dependencies
already listed in `requirements.txt`, but it does not require delivering the
multi-file source tree.

## Live Redshift Validation Rule

Before any live capture, ingestion validates actual Redshift metadata by
running:

```sql
SELECT * FROM <source_view> WHERE 1 = 0
```

It reads `cursor.description`, compares the returned column names to the exact
columns required by the extraction SQL, and aborts before capture if any column
is missing.

Validated sources:

- `sys_query_detail`
- `sys_query_history`
- `sys_query_explain`
- `sys_query_text`
- `SVV_TABLE_INFO`

This is intentionally metadata-first to avoid relying on remembered or guessed
Redshift column names.

## Product Bar Raised In This Pass

The app now has a DBA action layer in addition to diagnostic views:

- `v_query_table_refs`: correlates SQL text to table inventory.
- `v_table_query_impact`: computes table blast radius across slow queries.
- `v_action_queue`: ranks maintenance, physical design, rewrite, external data,
  WLM, and memory actions.

UI additions:

- Action Queue tab
- Table Impact tab
- Action KPI
- Aggregated hidden-thief cards instead of repeated per-query cards

## 32 High-Value Utilities To Build From This Data

These are utilities that become possible because the tool combines
`sys_query_history`, `sys_query_detail`, `sys_query_explain`, `sys_query_text`,
and multi-database `SVV_TABLE_INFO` into one local DuckDB model. Many would be
hard to do directly in Redshift console work because they require cross-view
correlation, repeated heuristics, and ranking.

1. **Cluster Slowdown Triage Board**  
   Rank every slow query by composite risk score, not just elapsed time.

2. **Hidden Performance Thief Ledger**  
   Aggregate repeated root causes across queries so DBAs see systemic issues,
   such as broadcast joins or stale stats, instead of isolated query noise.

3. **Query x Issue Heat Map**  
   Matrix of slow queries against issue families to show which queries share
   the same bottleneck signature.

4. **Issue Flow Diagram**  
   Visual flow from slow queries to issue families to severity. Useful for an
   executive or architecture-review view.

5. **Physical Table-Risk Scorecard**  
   Score every table by skew, unsorted percentage, stale stats, vacuum benefit,
   distribution style, missing sort key, and size.

6. **Redistribution Blast-Radius Finder**  
   Identify queries with `DS_DIST_BOTH`, high redistribution counts, network
   operators, and likely poor join co-location.

7. **Broadcast Sanity Checker**  
   Flag broadcasts that are likely fine small dimensions versus broadcasts that
   imply large table copies across slices.

8. **Spill Pressure Map**  
   Rank memory-starved queries by spill blocks, remote spill, query width, and
   hash/sort indicators.

9. **Remote I/O Heat Detector**  
   Find slow queries whose reads skew remote, suggesting cache misses, scan
   pressure, or external/storage-heavy patterns.

10. **Slice Skew Sentinel**  
    Combine execution `data_skewness` / `time_skewness` with table `skew_rows`
    to separate bad table design from one-off plan behavior.

11. **Missing Stats Impact Queue**  
    Prioritize `ANALYZE` work by slow-query impact rather than raw `stats_off`
    alone.

12. **Vacuum ROI Queue**  
    Prioritize `VACUUM SORT` by combining table `unsorted`, `vacuum_sort_benefit`,
    size, and slow-query involvement.

13. **External/S3 Drag Analyzer**  
    Quantify how much slow-query runtime is external work and whether external
    selectivity is poor.

14. **Spectrum Materialization Candidate Finder**  
    Recommend staging external recurring scans into Redshift when external
    duration, scan count, or spill is high.

15. **Time-Series Split Candidate Finder**  
    Detect large date/range scans that may benefit from hot/cold tables or
    explicit `UNION ALL` parts.

16. **Monster Query Stage Planner**  
    Identify queries that should be broken into analyzed temp-table stages
    because they combine spill, skew, high plan score, or nested-loop risk.

17. **Distkey Redesign Candidate List**  
    Rank large `EVEN` or badly skewed tables that likely need a better `KEY`
    distribution for dominant joins.

18. **Oversized DISTSTYLE ALL Detector**  
    Flag large replicated tables that create storage and maintenance drag.

19. **Missing Sort-Key Detector**  
    Find large tables without leading sort keys and rank them by size and row
    count.

20. **Sort-Key Ineffectiveness Detector**  
    Highlight queries that scan heavily despite sort keys, usually because
    predicates do not match the leading key.

21. **Low-Selectivity Scan Hunter**  
    Find queries reading huge row counts to produce tiny outputs. These are
    prime filter-pushdown and sort-key opportunities.

22. **Row Fan-Out Detector**  
    Flag queries where output rows exceed input rows, pointing to accidental
    many-to-many joins or missing predicates.

23. **Nested Loop Risk Workbench**  
    Isolate nested loop signatures and connect them to missing join predicates,
    stale stats, or low-cardinality joins.

24. **SQL Shape Linter**  
    Offline scan of slow SQL text for `SELECT *`, `UNION` instead of `UNION ALL`,
    leading wildcard `LIKE`, `CROSS JOIN`, and unbounded `ORDER BY`.

25. **Rewrite Opportunity Queue**  
    Rank rewrite candidates by impact and explain the rewrite shape with a
    candidate SQL skeleton.

26. **WLM Queue Pressure View**  
    Use queue time, execution time, slot count, service class, and user to
    separate query design problems from workload-management pressure.

27. **Worst User / Workload Attribution**  
    Attribute slow-query cost to users, service classes, and query labels so
    DBAs can find workload owners quickly.

28. **Recurring Anti-Pattern Fingerprint**  
    Group queries by dominant issue, SQL-shape signature, and physical table
    risk to show recurring families of bad queries.

29. **Table Ownership Hotspot Report**  
    Show which schemas or databases carry the highest physical risk and which
    ones appear most often in slow-query patterns.

30. **Before/After Maintenance Verifier**  
    Compare two DuckDB snapshots to prove whether `ANALYZE`, `VACUUM`, or
    dist/sort changes reduced risk scores and elapsed time.

31. **DBA Evidence Bundle Exporter**  
    Export a single query or table finding with metrics, SQL text, root cause,
    recommended action, and source-view evidence for ticketing or AWS support.

32. **Redshift Team Demo Mode**  
    Load a curated synthetic workload that visibly triggers all major views:
    heat maps, flow diagrams, rewrite cards, table-risk scoring, and executive
    KPIs.
