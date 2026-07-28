# Databa6ix

Native desktop analyzer for Redshift recurring-workload triage. The product
captures Redshift system-view snapshots into a local DuckDB file, then runs
offline on a corporate laptop with no AI service, browser runtime, or external
API calls.

The primary purpose is **repeat-query intelligence**: group captured queries
into parent patterns (the same SQL shape even when literals, dates, and IN-list
lengths differ), rank the patterns by total pain, expose the individual query
IDs behind each pattern, and hand the DBA team a verdict — fix the query, fix
the tables it hits, or both — with evidence and a recommended fix. One-off slow
queries are secondary; the recurring workload is where fixes pay off daily.

## How Repeat Grouping Works

1. Every captured statement gets a **canonical fingerprint**: the SQL is parsed
   with `sqlglot` (Redshift dialect), every literal becomes a placeholder,
   IN-lists and VALUES rows collapse to one placeholder, identifiers are
   normalized, and comments are dropped. Two queries that differ only in
   predicate values, dates, or IN-list length produce the same fingerprint.
   Statements sqlglot cannot parse fall back to a regex literal-stripper, and
   `UNLOAD ('...')` wrappers are unwrapped and fingerprinted on the inner SQL.
2. `CALL` statements group by canonical `db.schema.procedure` with parameters
   stripped.
3. A guarded **fuzzy merge** pass then unions near-identical shapes (for
   example, one extra projected column) — only within the same statement type
   and the same referenced table set, at >= 95% text similarity by default.
4. Each parent pattern is joined to the physical tables it references and
   classified: query-side defects (spill, `SELECT *`, unfiltered joins,
   broadcast/`DS_DIST_BOTH` movement, nested loops, S3 pressure) and
   table-side defects (missing/ineffective sort keys, unsorted rows, stale
   stats, distribution style and skew) become the **FIX QUERY / FIX TABLES /
   FIX BOTH / MONITOR** verdict with recommendation text.

Grouping knobs in `settings.json`: `repeat_min_group_size` (default 2),
`repeat_fuzzy_merge_threshold` (default 0.95), and `repeat_scope_by_user`
(default false — the same shape run by two users is one problem).

### Audit The Grouping

Grouping correctness is testable, not assumed. To verify against a real
capture, run:

```bat
python -m analyzer.grouping_audit --output grouping_audit.html
```

Open the HTML in any browser. It shows every parent group with the full SQL of
each member (check nothing was wrongly merged) and a "possible missed groups"
section of ungrouped queries that touch the same tables (check nothing was
wrongly split). Any error found there should be reported and added to
`tests/test_grouping.py`, which locks in the grouping guarantees:

```bat
python -m pytest tests\test_grouping.py -q
```

### Generate A Fix Script

The **Generate Fix Script** button on Workload Triage (or
`python -m analyzer.fix_script --output fix_script.sql`) turns the current
findings into a DBA-approvable SQL script. Safe maintenance (`ANALYZE`,
`VACUUM SORT ONLY`) is emitted as runnable statements; design changes
(`ALTER ... SORTKEY`, `ALTER ... DISTSTYLE KEY`) are always commented out with
their workload evidence and candidate columns, so a DBA must verify and
consciously uncomment each one. Every statement names the finding and the
repeat pattern(s) that produced it.

## What It Analyzes

The cluster command center is built from captured snapshot tables:

- `query_details`: query-level rollup from `sys_query_detail`
- `query_history`: slow queries from `sys_query_history`
- `query_health`: explain-plan signals from `sys_query_explain`
- `query_history_all`: raw slow-query history backup
- `query_text`: reconstructed SQL text from `sys_query_text`
- `child_query_text`: reconstructed optimizer-rewritten SQL for each child
  sequence from `sys_child_query_text`
- `table_scan_info`: table-level scan, RR-scan, full-scan, input-row, and
  duration summary from `sys_query_detail`
- `svv_table_info_all`: `SVV_TABLE_INFO` captured across databases
- `view_definitions`: view SQL captured across discovered databases
- `procedure_definitions`: stored procedure bodies captured across discovered
  databases for CALL diagnostics, trimmed to the `BEGIN ... END` body when
  possible

All high-value analysis happens in DuckDB SQL views. The UI reads those views,
so every finding is auditable and reproducible.

## Install

```bat
python -m pip install -r analyzer\requirements.txt
```

Core dependencies are PySide6, pandas, DuckDB, `sqlglot`, `rapidfuzz`, and
`redshift-connector`. `rapidfuzz` accelerates the fuzzy shape-merge pass; if it
is missing the analyzer silently falls back to the stdlib `difflib` matcher.

JDBC capture is supported as an optional mode via `JayDeBeApi`, but it is not a
hard dependency because `JPype1` may not have a prebuilt wheel on every Windows
ARM/Python combination.

## Capture A Snapshot

Preferred native DB connection:

Create `.env` in the repo folder:

```env
REDSHIFT_CONNECTION=native
REDSHIFT_HOST=cluster.example.us-east-1.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_USER=redshift_user
REDSHIFT_PASSWORD=
REDSHIFT_PRIMARY_DATABASE=dev
REDSHIFT_DATABASE_MIN_QUERY_COUNT=250
```

Leave `REDSHIFT_PASSWORD` blank to be prompted at runtime. Do not commit this
file.

```bat
python -m analyzer.ingest_redshift ^
  --minutes 10 ^
  --label "prod slow queries"
```

If `REDSHIFT_PASSWORD` is not set, the command prompts for the password in the
terminal. This avoids storing credentials in Windows environment variables or
in the repo.

Current-shell environment variables are still supported when allowed:

```bat
set "REDSHIFT_HOST=cluster.example.us-east-1.redshift.amazonaws.com"
set "REDSHIFT_USER=redshift_user"
set "REDSHIFT_PASSWORD=<set in Command Prompt only>"

python -m analyzer.ingest_redshift ^
  --connection native ^
  --primary-database dev ^
  --minutes 10 ^
  --label "prod slow queries"
```

By default, ingestion discovers databases from `sys_query_history` where
`database_name` has more than the configured threshold, saves that database
list in `%LOCALAPPDATA%\Databa6ix\settings.json (falls back to RedshiftQueryAnatomy if present)`, then reuses the
saved list for `SVV_TABLE_INFO` and `pg_views`. The SYS queries are
cluster-wide and run only against `REDSHIFT_PRIMARY_DATABASE`.

Force the discovery query to run again and update the saved database list:

```bat
python -m analyzer.ingest_redshift --reload-databases
```

You can also update the discovery SQL and threshold from **Settings** in the
desktop app, then click **Reload Databases**. The query must return a database
name column such as `database_name`, `datname`, `database`, `db_name`, or
`name`; otherwise the first result column is used.

Use `REDSHIFT_TABLE_DATABASES` only when you need to override the saved list:

```env
REDSHIFT_TABLE_DATABASES=dev,enterprise_datawarehouse,businesslayer
```

Optional JDBC mode when the laptop already has a working JPype/JDBC setup:

```bat
python -m pip install JayDeBeApi JPype1

set "REDSHIFT_JDBC_URL=jdbc:redshift://cluster.example:5439/{database}"
set "REDSHIFT_JDBC_JAR=C:\drivers\redshift-jdbc42.jar"
set "REDSHIFT_USER=redshift_user"
set "REDSHIFT_PASSWORD=<set in Command Prompt only>"

python -m analyzer.ingest_redshift --connection jdbc --primary-database dev
```

The default DuckDB path is:

```text
%USERPROFILE%\RQP\data\redshift.duckdb
```

Override it with `--duckdb-path` or `REDSHIFT_ANALYZER_HOME`.

### Reload View Definitions

View definitions come from Redshift `pg_views` and are stored locally in the
DuckDB table `view_definitions`. Reload them without recapturing slow-query
history:

```bat
python -m analyzer.ingest_redshift ^
  --reload-view-definitions
```

The reload uses the saved database list from Settings unless
`REDSHIFT_TABLE_DATABASES` or `--table-databases` is supplied. It prompts for
the password if `REDSHIFT_PASSWORD` is not set.

### Manual File Load

To benchmark individual exported files, open **Settings**, use **Manual File
Load**, choose the target DuckDB analyzer table, choose the CSV/TSV file, and
click **Load File**. The status line reports rows, columns, elapsed import
time, and the snapshot id. Leave **Append to target table** enabled when loading
one file per database into `svv_table_info_all`, `view_definitions`, or
`procedure_definitions`.

Command-line equivalent:

```bat
python -m analyzer.ingest_redshift ^
  --import-table query_history ^
  --import-file C:\path\query_history.tsv
```

For per-database files, pass the source database so table/view diagnostics keep
their database context:

```bat
python -m analyzer.ingest_redshift ^
  --import-table svv_table_info_all ^
  --import-source-database analytics ^
  --import-append ^
  --import-file C:\path\analytics_svv_table_info.tsv

python -m analyzer.ingest_redshift ^
  --import-table view_definitions ^
  --import-source-database analytics ^
  --import-append ^
  --import-file C:\path\analytics_views.tsv

python -m analyzer.ingest_redshift `
  --import-table procedure_definitions `
  --import-source-database analytics `
  --import-append `
  --import-file C:\path\analytics_procedures.tsv
```

### Capture Selection

Open **Settings -> Capture Selection** to choose which Redshift extraction
blocks are included in full or empty-table refreshes. Query roots are selected
by the configured minimum-seconds threshold, then normalized into parent SQL
patterns. Bounded evidence tables use one representative query ID per parent
pattern; the optional parent evidence cap defaults to `0`, which means every
threshold-selected parent pattern.

Command-line equivalent:

```bat
python -m analyzer.ingest_redshift ^
  --min-execution-seconds 30 ^
  --floor-basis execution_time ^
  --evidence-parent-limit 0 ^
  --include-tables query_history,query_text,child_query_text,query_details,procedure_definitions
```

Repeat-query diagnostics keep a bridge from each deduplicated parent row back to
the underlying `(snapshot_id, query_id)` records. The UI shows no more than ten
children under a parent group, but the retained member table keeps the full
query-ID set for follow-on table lookups. Non-procedure parent SQL starts with
the first matching SQL instance, and the representative viewer can rotate
through additional examples.

`child_query_text` is included in normal full and incremental captures. To load
or replace only that dataset in the latest snapshot, run:

```bat
python -m analyzer.ingest_redshift --refresh-table child_query_text
```

DuckDB view `v_child_query_text` reconstructs the 200-character source fragments
into one `child_sql_text` value per `(snapshot_id, query_id,
child_query_sequence)`.

## Run The Desktop App

```bat
python -m analyzer
```

The default screen is **Workload Triage**: recurring parent patterns ranked by
total pain with proportional impact bars, each expandable into its child query
IDs, with a verdict chip, query/table evidence, the recommended fix, and the
representative SQL. Double-click a child query to open its lineage diagram.

The remaining tabs provide drill-down detail:

- Table Review with sortable/hideable table inventory columns and
  selected-table metrics for sort-key use, distribution pressure, full scans,
  and workload impact
- Data freshness and coverage status for every local DuckDB analyzer table,
  with explicit warnings when scan metrics are incomplete
- KPI strip for slow-query count, criticals, runtime, rewrite count, table risk
- Repeat-query intelligence with normalized SQL-shape similarity scores
- Top repeat-query patterns in the command center, because recurring workload
  is more valuable to fix than a one-off slow query
- Prioritized DBA action queue
- Slider-tuned severe-query finder for runtime, spill, remote I/O, skew,
  join movement, external/S3 pressure, and repeat-pattern impact
- Ranked "hidden performance thieves"
- Issue-family bars
- Query-by-issue heat map
- Native issue-flow diagram
- Slow-query risk table
- Table blast-radius impact map
- Physical table-risk heat map
- Full insight ledger
- Repeat Queries tab that clusters recurring SQL shapes even when literal
  dates, numbers, and quoted values differ
- `sqlglot` SQL intelligence for AST-backed query fingerprints, table
  extraction, joins, predicates, CTEs, projections, wildcards, subqueries,
  aggregate/function counts, and column-role extraction for join keys, filter
  columns, projected columns, order columns, and group-by columns
- Rewrite Opportunities tab with 11 rewrite patterns
- Plain-language **Fix This Query** action in SQL Lens. Its default screen
  gives a non-technical user one recommended fix, a one-sentence explanation,
  and one large copy action. Supporting explanations, safe-use instructions,
  checks, and the SQL preview are collapsed into a one-section-at-a-time
  accordion with larger typography. Optimizer scores, physical-design
  terminology, alternate candidates, and the evidence report are kept on an
  optional Technical Details tab. The fixer uses the Redshift AST
  plus captured table telemetry and requires no LLM, API, network connection,
  or additional install. High-confidence rewrites currently include bare
  half-open date/year ranges for function-wrapped leading sort keys, safe
  predicate propagation across inner-join equality columns when the receiving
  column is a leading sort key, and removal of provably redundant `DISTINCT`
  above a complete projected grouping key. Risky changes stay as review-only
  advisories. Every candidate is reparsed, checked for the same table set,
  checked for invented columns, and checked for an unchanged top-level
  projection before it is offered; EXPLAIN and result comparison remain
  mandatory before execution.
- The same optimizer can generate **multi-step decomposition plans**. It
  identifies either a repeated/complex CTE or the largest safely prefiltered
  fact-side input as the query's "heart," projects only referenced columns,
  creates a session-scoped temp table with a telemetry-selected `DISTKEY` and
  compound `SORTKEY`, and rewrites the final query around that stage. The UI
  exposes these scripts beside the conservative single-statement rewrite for
  copy/review only; it never executes them. CTAS-created temp tables receive
  initial statistics from Redshift, and every generated script reminds the
  operator to run the complete plan in one session and validate row counts,
  results, and EXPLAIN output.

The **Slow Queries** tab includes severity sliders. Move the minimum-score
slider to filter the list, then adjust the signal weights to bias the ranking
toward repeat-pattern impact, runtime, spill, remote I/O, skew, join movement,
or external/S3 work.

The **Repeat Queries** tab is the primary optimization queue. It uses
`query_text` from DuckDB, preserves original SQL line breaks for display,
normalizes literals and whitespace for scoring, and uses `sqlglot` AST features
to group recurring query shapes.

The **Config** button opens local analyzer configuration: configured Redshift
connection environment, editable database-discovery SQL, the saved database
array used for `SVV_TABLE_INFO` and `pg_views`, manual file loading with import
timing, DuckDB table row counts, table truncation, and a Refresh Empty Tables
action. Truncated/empty analyzer tables can also be repopulated from the
command line:

```bat
python -m analyzer.ingest_redshift --refresh-empty-only
```

The **Single Query Lab** tab keeps the original paste-based deep dive for one
query's `sys_query_detail` and matching `SVV_TABLE_INFO` rows.

## Demo Dataset

A deterministic mock Redshift snapshot is available at:

```text
analyzer\samples\mock_redshift_3300.duckdb
```

It contains 3,534 populated mock rows. The `child_query_text` table and
`v_child_query_text` reconstruction view are present but start empty because
optimizer-generated child SQL is populated by a live Redshift capture.
Regenerate it with:

```bat
python -m analyzer.mock_data --output analyzer\samples\mock_redshift_3300.duckdb
```

The generator intentionally creates realistic performance pathologies so the
dashboard has meaningful density for demos: spills, skew, broadcasts,
`DS_DIST_BOTH`, stale stats, S3/external scans, broad time-series queries, and
rewrite triggers. It also includes table-scan rows for Table Review.

## Single-File Delivery

For locked-down environments where delivering a source tree is painful, build
one single-file launcher.

Plain-text launcher, preferred when security review may flag base64 payloads:

```bat
python tools\build_text_py.py
```

Generated artifact:

```text
redshift_analyzer_text.py
```

The text launcher embeds the analyzer package as inspectable quoted source
lines. It is larger, but there is no base64 or compressed payload.

Compressed base64 launcher, smaller but more opaque:

```bat
python tools\build_fat_py.py
```

Generated artifact:

```text
redshift_analyzer_fat.py
```

Usage:

```bat
python redshift_analyzer_text.py
python redshift_analyzer_text.py --ingest -- --help

python redshift_analyzer_fat.py
python redshift_analyzer_fat.py --demo
python redshift_analyzer_fat.py --make-mock --output mock.duckdb
python redshift_analyzer_fat.py --ingest -- --connection native --host <host>
```

The fat file embeds the analyzer package and extracts it to a temp zip at
runtime. It still requires the packages in `requirements.txt`.

## Live Capture Validation

Live capture validates Redshift source metadata before running extraction SQL.
For each source view, the ingester runs `SELECT * FROM <view> WHERE 1 = 0`,
reads the actual returned column names, and compares them to the columns
required by the extraction SQL. If a column is missing, ingestion stops before
any expensive query runs.

Validated sources:

- `sys_query_detail`
- `sys_query_history`
- `sys_query_explain`
- `sys_query_text`
- `sys_child_query_text`
- `SVV_TABLE_INFO`
- `pg_views`
- `pg_catalog.pg_proc_info`
- `pg_catalog.pg_namespace`
- `pg_user`

## Diagnostic Coverage

The DuckDB insight ledger currently runs 32 SQL-backed checks across:

- disk spill and remote I/O
- execution data/time skew
- broadcast and redistribution pathologies
- `DS_DIST_BOTH`
- nested loop risk
- missing statistics
- S3/external scan pressure
- partition loops and network nodes
- row fan-out and low selectivity
- SQL-shape risks such as `SELECT *`, `UNION`, leading wildcard, cross join,
  and full final sorts
- table skew, unsorted rows, stale stats, vacuum benefit, large `ALL` tables,
  large `EVEN` facts, and missing sort keys

## Rewrite Opportunities

The rewrite layer identifies 11 high-impact patterns:

1. Split broad time-series scans into explicit `UNION ALL` parts
2. Push filters before high-cardinality joins
3. Pre-aggregate facts before dimension joins
4. Stage large joins with explicit `DISTKEY` / `SORTKEY`
5. Materialize recurring external/S3 scans
6. Replace `SELECT *` with column projection
7. Use `UNION ALL` instead of `UNION` when dedupe is unnecessary
8. Replace leading-wildcard `LIKE`
9. Convert full final sorts to Top-N patterns
10. Eliminate accidental fan-out or cross joins
11. Break monster queries into analyzed temp-table stages

Each triggered opportunity includes the evidence trigger, rewrite shape, why it
matters, and a candidate SQL skeleton.

## Security Notes

- Do not store credentials in the repo.
- Use environment variables or the interactive password prompt; do not pass
  passwords as command-line arguments.
- The DuckDB file stores diagnostic metadata and SQL text. Treat it as
  production-sensitive output.
- The desktop app itself does not call AI services or cloud APIs.
