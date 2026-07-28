# Infraredshift

**Workload triage for large Amazon Redshift producer/consumer estates.**

[![CI](https://github.com/lcapece/infraredshift/actions/workflows/ci.yml/badge.svg)](https://github.com/lcapece/infraredshift/actions/workflows/ci.yml)
[![Security](https://github.com/lcapece/infraredshift/actions/workflows/security.yml/badge.svg)](https://github.com/lcapece/infraredshift/actions/workflows/security.yml)
[![PyPI](https://img.shields.io/pypi/v/infraredshift)](https://pypi.org/project/infraredshift/)
[![Python](https://img.shields.io/pypi/pyversions/infraredshift)](https://pypi.org/project/infraredshift/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Infraredshift captures SYS/SVV telemetry from every cluster into a single local
DuckDB file, collapses queries that are *the same query with different literals*
into one pattern, and ranks those patterns by the total time they actually cost.

It is a desktop application. There is no service, no API, no telemetry, and no
outbound network call except to your own Redshift clusters.

---

## 1. What the tool is for

You open a monitoring console and see fifteen thousand slow queries. Which one
do you fix?

That question has no good answer, because it is the wrong question. A cluster
running fifteen thousand slow queries does not have fifteen thousand problems.
It has a few dozen **query shapes**, each run hundreds or thousands of times a
day by a scheduled job, a dashboard refresh, or an ETL step.

Infraredshift finds the shapes. It answers:

- Which query *patterns* consume the most cluster time in total?
- Is each one slow because of the SQL, or because of the physical design
  (missing DISTKEY, unsorted data, stale statistics, unpartitioned Spectrum)?
- Which tables drag down the most patterns at once?
- Which fix buys back the most hours?

It is a triage tool, not a monitoring dashboard. It is meant to be opened when
someone asks *"why is the warehouse slow, and what should we do first?"*

---

## 2. Built for regulated environments

This tool was written for a bank. That constraint shaped the architecture, and
it is the reason it can be used where a SaaS observability product cannot.

**For all practical purposes it is air-gapped.** And critically: it is built for
firms that cannot — or are not yet ready to — expose their cluster to AI.

### No AI touches your cluster, your SQL, or your schema

Most query-optimization tooling now routes your SQL to a model. That is a hard
stop for many institutions, and a policy question nobody has finished answering
at many others. Production SQL is not neutral text: it carries table names,
column names, business logic, join keys, filter values, and often the shape of
regulated data itself. Sending it to a third-party model is a data-egress event,
whatever the vendor's retention policy says.

Infraredshift does none of that. **There is no model, no inference, no prompt,
no embedding, and no vendor endpoint — not optional, not opt-out, not
configurable. It does not exist in the codebase.**

The analysis is deterministic:

- **SQLGlot** parses your SQL into a syntax tree. It is a local Python library,
  the same class of tool as a compiler front-end.
- **DuckDB** aggregates the telemetry Redshift already recorded about your own
  queries.
- **The verdicts are rules** over measured facts — spill, scan volume, skew,
  sort-key coverage, statistics staleness — not predictions.

That means the same warehouse always produces the same answer, and every
recommendation traces back to a number you can check in Redshift yourself. There
is no confidence score to interpret and no hallucination to guard against.

If your organization later adopts AI tooling, nothing here blocks it. But
Infraredshift will not be the reason your query corpus left the building.

- **No API.** Nothing listens on a port. There is no server component, no
  webhook, no callback, no control plane.
- **No CLI dependency on anything remote.** The command-line entry points read
  and write local files only.
- **No AI or external service.** No model is called, no query text is sent
  anywhere for analysis, no vendor sees your schema. Parsing is a local library
  (SQLGlot); analysis is local DuckDB and pandas.
- **No telemetry, analytics, crash reporting, or update check.** The application
  never phones home. There is nothing to opt out of.
- **One network connection exists at all**, and it is yours: a **read-only**
  JDBC/native connection to your own Redshift clusters, on your own network,
  using credentials you supply. Nothing else leaves the machine. The application
  issues no `INSERT`, `UPDATE`, `DELETE`, `COPY`, `CREATE`, or `DROP` against
  Redshift — every statement it sends is a `SELECT`.
- **It reads system catalogs only — never your data.** The complete list of
  Redshift objects the application queries is:

  | | |
  |---|---|
  | `SYS_QUERY_HISTORY` · `SYS_QUERY_DETAIL` · `SYS_QUERY_EXPLAIN` | what queries ran and how they performed |
  | `SYS_QUERY_TEXT` · `SYS_CHILD_QUERY_TEXT` | the SQL text of those queries |
  | `SYS_EXTERNAL_QUERY_DETAIL` · `SYS_EXTERNAL_QUERY_ERROR` | Spectrum execution |
  | `SVV_TABLE_INFO` | physical design: distribution, sort, skew, statistics |
  | `SVV_EXTERNAL_TABLES` · `SVV_EXTERNAL_COLUMNS` | Spectrum catalog |
  | `SVV_REDSHIFT_DATABASES` · `SVV_USER_INFO` | database and user lookup |
  | `PG_VIEWS` · `pg_catalog.pg_proc_info` · `pg_catalog.pg_namespace` | view and procedure definitions |

  That is the entire surface. **There is no facility in the application to
  connect to a data schema or read a business table** — no code path constructs
  such a query, so there is nothing to misconfigure. What it reads is Redshift's
  own record of what your queries *did* — runtimes, row counts, spill, scan
  volume, distribution and sort metadata — never the rows themselves.

  The one nuance worth stating plainly: capturing `SYS_QUERY_TEXT` means the app
  stores **your SQL statements**, which can embed literal values in predicates.
  That is unavoidable for pattern grouping, and it stays in your local DuckDB
  file. No table contents are ever read.
- **Single-file delivery.** The whole application ships as one text file that
  can be reviewed, virus-scanned, and hash-verified before it is run. It can be
  emailed through filters that block `.py` and `.zip`.
- **Self-verifying.** The delivered file can prove it is the exact artifact that
  was built (`--self-check`), so what was reviewed is what runs.
- **Credentials never leave the OS keystore boundary.** Redshift credentials are
  encrypted with Windows DPAPI, scoped to one Windows user on one machine. They
  are never written to `.env`, never copied into environment variables, and
  never included in any export. A copied credential file will not decrypt for
  anyone else — by design.
- **Your data stays put.** Captured telemetry and SQL text live in a local
  DuckDB file under your own user profile.

What this means in practice: the analysis that would normally require shipping
query logs to a vendor happens entirely on the analyst's laptop, inside the
network boundary, with an auditable artifact.

### Why a desktop GUI (PySide6), not a web app

Almost every modern analytics tool is a web application. This one is not, and
that is the same decision as everything above rather than a separate one.

**The deciding constraint is the target machine.** This runs on a locked-down
corporate laptop where you do not get to install a web server, open a port,
stand up a container, or ask for a firewall exception. What you reliably *do*
get is Python and `pip install`. So the tool has to be something that a `pip
install` can fully deliver and that then just runs — no service to register, no
admin rights, no infrastructure request.

That rules out a browser-based GUI. Even served on `localhost`, a web UI means
a **server**: a process listening on a port, a request path taking untrusted
input, session handling, and a browser rendering it. On a managed laptop the
listening socket alone can trip endpoint protection or policy, and every one of
those pieces is something a bank's security review must assess. A desktop
application has none of them — no port to scan, no endpoint to fuzz, no CORS
policy, no cookie.

It also cannot depend on the browser itself. Corporate browsers are managed:
locked versions, forced extensions, blocked local content, policies that change
without notice. Making the analyst's tooling depend on that is a support burden
you never stop paying.

Qt via **PySide6** was chosen over the alternatives for reasons that follow
directly from the deployment constraints:

- **It ships as one file.** The entire application — code and assets — can be
  concatenated into a single text file, emailed through filters that block
  `.py` and `.zip`, hash-verified, and reviewed before it runs. An Electron app
  is a hundred-megabyte browser runtime; a web app is a server plus a browser.
- **No bundled browser engine.** Electron would mean shipping Chromium, and
  inheriting its CVE stream into an air-gapped environment where patching is
  slow and deliberate.
- **PySide6 is the official Qt binding, LGPL-licensed.** No commercial licence
  is required to use or distribute it, which matters when legal review is part
  of the deployment path. (PyQt's GPL terms would impose obligations this
  project does not want to place on its users.)
- **It renders tens of thousands of items fast.** The heat map draws twelve
  thousand tables in ~0.3s using `QPainter` directly. The same view in the DOM
  requires virtualisation, and the bubble chart is a custom-painted widget —
  both are natural in Qt and a project in a browser.
- **Same code on Windows, macOS and Linux**, with native file dialogs and
  clipboard, and no packaging story per platform.

The cost is honest: Qt is a large dependency (PySide6 is most of the install
size), desktop UI is less familiar to web developers, and there is no "just
send someone a link". For a tool that is deliberately single-user, offline, and
handling regulated query text, those are the right trade-offs.

---

## 3. Why this is critical in a large producer/consumer estate

### The problem scales with your architecture, not your query count

A datashare estate has one producer and many consumers. That design is good for
isolation and bad for observability:

- **Telemetry is fragmented.** `SYS_QUERY_HISTORY` is cluster-wide, but `SVV_*`
  catalog views are per-database and must be cycled. A producer with 35
  databases needs 35 passes to see its own tables.
- **Consumers hide the cost.** A consumer running a slow query against a shared
  table is billed to the consumer, but the *fix* — a sort key, a distribution
  key — belongs to the producer. Neither side sees the whole picture alone.
- **The same query runs everywhere.** One badly-shaped report SQL, copied into
  four teams' dashboards, looks like four unrelated problems.
- **Nobody owns the aggregate.** Each team sees their own slow queries. Nobody
  sees that one table with no sort key sits behind 40% of the estate's runtime.

Infraredshift loads every cluster into one DuckDB file, attributes each query to
its namespace, then groups **across** clusters. That is what makes an
estate-wide answer possible.

### The 80/20 becomes a 98/2

The familiar rule of thumb is that ~80% of slowness comes from ~20% of queries.
That is what you see when you count queries *as text* — every literal a distinct
query, every dashboard refresh a new row.

Once queries are parsed and grouped by shape, the distribution sharpens
dramatically. The same workload typically resolves to something closer to
**98% of total slowness concentrated in ~2% of query patterns**, because:

- thousands of "distinct" queries collapse into one recurring shape;
- that shape's cost is now *summed* rather than scattered across thousands of
  rows that each looked individually unremarkable;
- and genuinely one-off ad-hoc queries — noisy, but non-recurring — drop out.

This is the whole point. A query that takes 4 seconds looks harmless. Run 9,000
times a day, it is ten hours of cluster time. It will never appear at the top of
a list sorted by single-query duration. It is *always* at the top of a list
sorted by pattern total.

**Fixing 2% of patterns is a week of work. Fixing 20% of queries is a quarter.**

---

## 4. The role of SQLGlot

SQLGlot is the parser that makes the grouping possible. Without it there is no
"2%" — only thousands of unique strings.

### Text matching does not work

These are the same query:

```sql
SELECT c.name, SUM(o.amt) FROM orders o JOIN cust c ON o.cid=c.id
 WHERE o.dt='2026-07-01' AND o.region='EAST' GROUP BY 1

select c.name, sum(o.amt)  from orders o  join cust c on o.cid = c.id
 where o.region = 'WEST' and o.dt = '2026-07-02' group by 1
```

Different literals, different casing, different whitespace, **different clause
order**. Every string-based approach — `LIKE`, checksums, `query_hash` — sees
two unrelated queries. Run that shape a thousand times a day and you get a
thousand rows, none individually alarming.

### What Infraredshift does instead

Each query is parsed into an abstract syntax tree, and the tree — not the text —
is normalized into a fingerprint:

| Normalized away | Why |
|---|---|
| Literal values (`'EAST'` → `?`) | The value changes per run; the shape does not |
| Case and whitespace | Cosmetic |
| Table and column aliases | `o` vs `orders_alias` is not a different query |
| Commutative clause order | `a AND b` is `b AND a` |
| IN-list length | `IN (1,2,3)` and `IN (1..500)` are one shape |
| Run-scoped table suffixes | `stg_tmp_12345` → `stg_tmp_#` |

Both queries above produce **one** fingerprint. A genuinely different query —
different joins, different predicates — produces a different one. This is
verified in the test suite, not asserted.

### Why a parser rather than a hash

Redshift's own `query_hash` groups only textually-identical queries after
literal substitution. It misses reordered predicates, alias changes, and
formatting differences — precisely what varies between two teams running the
same report. Parsing closes that gap.

SQLGlot also yields the query's *structure*, which drives everything downstream:

- **Table extraction** — which physical tables a pattern touches, so a table fix
  can be credited to every pattern it would help.
- **View expansion** — the base tables actually read once views are resolved,
  usually more than the SQL suggests.
- **Decomposition** — splitting one slow query into the stages it really runs,
  to find the expensive join or scan.
- **Verdict routing** — whether a pattern is a SQL problem or a physical-design
  problem.

`sqlglot` is pinned to an exact version. Its AST shape *is* the fingerprint, so
a version drift would silently change which queries group together — changing
results rather than failing loudly.

### What SQLGlot is not doing

It does not rewrite your SQL, does not connect to Redshift, and does not make
recommendations. It is the parser. The cost model comes from Redshift's own
execution telemetry.

### Early release — grouping is actively improving

This is an early release, and the honest statement is that **grouping arbitrary
production SQL is a hard problem, made harder by refusing to use AI to do it.**

An LLM would paper over the difficult cases by "understanding" that two queries
are similar. Deterministic grouping cannot do that. Every equivalence has to be
earned explicitly in the AST: this alias is irrelevant, that clause order is
commutative, this generated suffix is run-scoped noise. Real warehouse SQL is
brutal on that approach — deeply nested CTEs, generated code, 200,000-character
statements, temp tables named per run, dialect quirks, and queries assembled by
BI tools that no human ever formatted.

The tradeoff is deliberate and it is the right one for a regulated environment:
the grouping is **reproducible, auditable, and explainable**. When two queries
group together you can see exactly which normalization made them equal. When
they do not, that is a rule that has not been written yet — not a model that
changed its mind.

What this means for you:

- The patterns it finds are real, and the totals are correct.
- Some queries that *are* the same shape will not yet group together, so a
  pattern's cost can be **understated** — never overstated.
- Coverage improves with each release as more equivalences are added. Grouping
  quality is the primary axis of ongoing work.

If you hit a pair of queries that obviously belong together and do not,
that is a bug worth reporting — it is exactly the input that drives the next
improvement.

---

## 5. How it works

```
Redshift producer + consumers          Analyst laptop (inside the boundary)
┌──────────────────────────┐          ┌────────────────────────────┐
│ SYS_QUERY_HISTORY        │          │  redshift.duckdb           │
│ SYS_QUERY_DETAIL         │  ──────► │   one file, all clusters   │
│ SYS_QUERY_EXPLAIN        │  capture │                            │
│ SVV_TABLE_INFO (per db)  │          │   SQLGlot fingerprinting   │
│ SVV_EXTERNAL_COLUMNS     │          │   repeat grouping          │
└──────────────────────────┘          │   cached analysis          │
        the only outbound             └────────────┬───────────────┘
        connection, to your                        │
        own clusters                        Desktop UI (Qt)
```

1. **Capture** — telemetry is pulled per cluster under a per-cluster
   `floor_seconds` threshold, so a busy producer can capture only 300s+ queries
   while a quiet consumer captures 30s+.
2. **Store** — everything lands in one DuckDB file. Loads are incremental and
   de-duplicated by query id.
3. **Group** — SQL is parsed, fingerprinted, and grouped. Results are cached in
   the same file, so a second open is warm.
4. **Rank** — patterns are scored by total cost and routed to a verdict.

---

## 6. The screens

**Workload Triage** — one bubble per pattern. Horizontal axis is run frequency,
vertical is your chosen metric (runtime, rows, spill). The big bubble in the
upper right is where your time goes. Each carries a verdict: fix the query, fix
the tables, or leave it alone.

**Table Heat Map** — every table as a square, coloured by distribution and sort
health: missing DISTKEY, unsorted, stale statistics. Spectrum tables get their
own view, blue where a partition key exists and orange where it does not.

**Query Decomposer** — one slow query split into the stages it actually runs,
and optionally rewritten into a staged pipeline (see below).

**Fix Queue** — the ranked, DBA-ready work list: what to change, in what order.

**Behind Views** — the base tables a query really touches once views are
expanded.

---

## 7. The Decomposer library

Infraredshift ships with **`redshift-query-decomposer`**, a separate library
that does something more aggressive than analysis: it **rewrites** a query.

### What it does

Given one slow query, it parses the SQL, explodes any views to reach the real
base tables, and compiles the whole thing into a **staged pipeline of narrow
temporary tables** — each with its own DISTKEY and SORTKEY chosen for the stage
that follows it — plus a rewritten final query.

The move it is making is always the same one, and it is the move a good DBA
makes by hand:

> A 2-billion-row, 14-column fact table is hiding inside a view. The query
> returns three columns and filters on a date. Redshift will scan all of it.
> The decomposer extracts **5 of 14 columns**, applies the filter *first*,
> co-locates the result on the join key, and joins that instead.

Predicates are pushed down before the join rather than after. Columns nobody
asked for are pruned. Each stage is distributed on the key the next stage joins
on, so the redistribution never happens.

**It explains every decision it makes.** Each stage carries a note saying what
it used to be and what changed. That is the point — the output is meant to be
read, not just run.

### It is a proposal, not a patch

This is evolving technology, and the documentation is deliberate about it:
**the output is a proposal for a human to review, not a drop-in production
rewrite.**

- **Use it one query at a time.** It is not a batch rewriter.
- **Every result carries a `conversion_likelihood` score** (0.0–1.0) and a
  verdict. Read it. A low score means the query has shapes the rewriter handles
  poorly, and the script is a sketch at best.
- **Validate before you use it.** Row counts, nulls, and `EXPLAIN` against the
  original. The generated script says this in its own header, on purpose.
- **Treat it as a skeleton to move forward from** — a starting point that
  encodes the tedious reasoning, leaving the judgement to you.

The rewriting is aggressive by design. That is what makes it useful and what
makes review non-optional. A tool that only suggested safe changes would rarely
suggest the change that matters.

### How it relates to the app

| | |
|---|---|
| **Package** | `redshift-query-decomposer` (published on PyPI) |
| **Import name** | `redshift_decomposer` |
| **Role** | Rewrites one query into a staged pipeline; ships as a required dependency |
| **Cluster access** | None. It parses SQL and never connects to a database |
| **In the app** | Feeds the Query Decomposer screen and its decomposability triage |

The app calls it through `analyzer/decomposer_bridge.py`, which imports it
lazily and degrades gracefully — if the library is missing or broken, the
built-in decomposition still works and the screen says so rather than failing.

Used directly:

```python
from redshift_decomposer import assess_decomposability, decompose

triage = assess_decomposability(sql)      # is this worth rewriting?
plan = decompose(sql)                      # stages, findings, rewritten SQL
print(plan.conversion_likelihood)          # read this before trusting the rest
```

Like the rest of Infraredshift, it is offline: no database connection, no
network call, no model.

---

## 8. Install

```bash
pip install infraredshift          # add [redshift] for live capture
infraredshift
```

Python 3.10+. Full onboarding — profiles, credentials, first load — is in
[README-PYPI.md](README-PYPI.md).

The first screen is a **local** sign-in. It is not a Redshift login: it creates
an access code and PIN that encrypt your Redshift credentials at rest.

A mock warehouse ships with the app, so every screen can be explored before
connecting to anything.

### Locked-down delivery (no pip)

Where PyPI is not reachable, build the single-file handoff:

```bat
python tools\build_text_py.py
```

Send only:

```text
SEND-THIS-ONE\Infraredshift-WORK-LAPTOP-ONLY.zip
```

One entry point: `python Infraredshift_APP.txt`. The recoverable loader and the
producer-only `SVV_EXTERNAL_COLUMNS` capture are embedded inside it. Developer
and legacy launchers are never included.

Before launch, the delivered file can verify it is the exact built artifact:

```bat
python Infraredshift_APP.txt --self-check
```

---

## 9. Operational notes

**One DuckDB file.** It holds captured telemetry, SQL text, and the analysis
cache. It can be copied to a teammate, who then gets a warm start because the
cache travels with it. It contains **no credentials**. Close the app first;
DuckDB allows a single writer.

**Credentials are separate and non-portable.** DPAPI-encrypted, scoped to one
Windows user on one machine. Copying that file elsewhere will not decrypt.

**Large Spectrum catalogs.** `SVV_EXTERNAL_COLUMNS` returns one row per
*column*, so a catalog with millions of external tables is tens of millions of
rows. Restrict it in the producer profile:

```json
"external_schemas": "curated, analytics",
"external_table_patterns": "fact_*, dim_*"
```

The filter runs on Redshift, so those rows never cross the wire.

**Health Check.** The DuckDB panel has a Health Check button that inspects the
active warehouse without loading it — core tables, captured SQL text, cluster
identity, cached analysis. Run it first when a screen looks wrong: it separates
"the data never arrived" from "the analyzer has a bug."

---

## 10. Repository layout

| Path | Purpose |
|---|---|
| `analyzer/` | Desktop package (PySide6 + DuckDB + sqlglot) |
| `runner.py` | Standalone capture/ingest runner |
| `Infraredshift.txt` / `.py` | Generated single-file launchers |
| `tools/` | Launcher builders and operator scripts |
| `SEND-THIS-ONE/` | Generated canonical work-laptop ZIP; send only this |
| `tests/` | Test suite |

---

## 11. Development

```bash
pip install -r requirements.txt
QT_QPA_PLATFORM=offscreen python -m pytest tests/ -q
python tools/build_text_py.py     # rebuild the single-file launcher
```

Deeper notes: [analyzer/README.md](analyzer/README.md).

---

## License

MIT — see [LICENSE](LICENSE).
