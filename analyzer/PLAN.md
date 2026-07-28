# Plan — "The Query Story"

A visual workflow that lets a non-technical user read what happened to their
query. Built entirely from the two paste inputs we already support
(`sys_query_details` + `svv_table_info`). No SQL text yet — that paste
becomes the *next* phase.

## Why this phase

The current app shows a plan DAG, a step timeline, and a diagnostics grid.
That's a tool for DBAs. A PM, a data analyst, or a founder reading it sees
noise: `hjoin`, `bcast`, `alert_event`, `stream_id`. The product has to
*narrate* the query, not expose its internals.

We don't need the SQL text yet to do this. `sys_query_details` alone tells
us which tables were scanned (via `table_id`), which steps joined what,
which moved data, which spilled, and where seconds actually went. Combined
with `svv_table_info`, every second and every row can be attributed to a
table and explained in plain English.

## The product — five "scenes"

Replace the tabbed panel layout with a vertical scroll. Each scene answers
one question a non-technical person would ask.

### Scene 1 — "What happened?"

A single headline card at the top. Big runtime. One-sentence verdict:

> **Your query ran for 42 seconds and touched 5 tables. Most of the time
> — 18 seconds — was spent moving data between machines because one table
> wasn't arranged to match the way you joined it.**

Below it, a **Time Budget Bar**: a horizontal bar 100% wide, segmented by
*category of work*, not by step:

- 🔎 Reading from disk
- 🤝 Joining tables
- 🚚 Moving data between machines
- 📊 Sorting / grouping
- 🧮 Computing
- 📤 Returning results

Each segment shows seconds + % on hover. Color-coded by severity (a big
"Moving data" slice is red; a big "Reading" slice is just blue).

### Scene 2 — "Which tables did it touch?"

The hero scene. One **Table Warehouse Card** per table the query touched,
sorted by time spent on that table.

Each card shows:

- **Shape** — a small icon/bar proportional to `size_mb` and `tbl_rows` so
  users *see* big-vs-small
- **How it's organized** — a pair of icons for distkey and sortkey,
  annotated in plain English: "Organized by `ss_sold_date_sk` across 4
  machines, sorted on `ss_sold_date_sk`"
- **What this query did with it** — "Read 24M of 288M rows" (and whether
  it used range-restricted scan)
- **Health** — colored dots for stats-freshness, skew, unsorted %,
  vacuum benefit
- **Verdict line** — one sentence:
  - ✅ "Good — your filter matched the way this table is sorted"
  - ⚠️ "Problem — stats are 47% out of date, so Redshift guessed wrong"
  - 🚫 "Big problem — this table was reshuffled because it isn't organized
        around the column you joined on"

### Scene 3 — "How did the data move?"

An animated **Data Flow Ribbon** (Sankey-style, Qt-native). Each ribbon's
thickness is proportional to bytes moved; its color indicates cost. You
see at a glance which hop was fat.

Clicking a ribbon highlights the corresponding row on the Time Budget Bar
(Scene 1) and the card(s) in Scene 2. Cross-filtering, but guided.

### Scene 4 — "What hurt the most?"

A **Bottleneck Spotlight** card. The single biggest cost, framed
before/after:

> **Now:** 28 million rows were shipped between machines during the join of
> `store_sales` and `item`. That took **18 of your 42 seconds**.
>
> **Why:** `store_sales` is organized by `ss_sold_date_sk`, but the join
> was on `ss_item_sk`. Redshift had no choice but to reshuffle.
>
> **Fix:** Either change `store_sales`' DISTKEY to `ss_item_sk`, or mark
> `item` as distributed to every machine (ALL) since it's small (12 MB).
> Predicted runtime after fix: ~24 seconds.

### Scene 5 — "What else could be better?"

A stacked list of secondary findings, each a compact card with the same
*now / why / fix* shape. Sorted by predicted time saved. Technical terms
are inline-glossed on hover.

## Non-technical visual language

These metaphors should appear consistently across scenes:

| Redshift concept | Visual metaphor |
|---|---|
| Table | Warehouse |
| Rows | Boxes |
| Slice / node | A forklift lane inside the warehouse |
| DISTKEY | How boxes are sorted onto lanes |
| SORTKEY | Which aisle each box is in |
| Broadcast | "Send a copy to every warehouse" |
| Redistribute | "Relabel every box and ship to new warehouses" |
| Hash join | "Two warehouses exchanging a list to match up orders" |
| Spill to disk | "Ran out of shelf space, had to use the overflow lot" |
| Stale stats | "The inventory sheet is out of date" |

Every hover tooltip uses the metaphor; every technical term in the finding
body is linked to its plain-English definition.

## Technical plan

### Data & analysis layer

One new module and two rewritten ones.

**`insights.py` (new)** — the analytical core. Consumes
`(steps_df, tables_df)` and produces a single `QueryContext`:

```python
@dataclass
class QueryContext:
    total_runtime_s: float
    time_budget: dict[Category, float]        # 6 buckets
    touched_tables: list[TableTouch]          # one per table scanned
    data_flows: list[Flow]                    # sankey edges
    bottleneck: Finding                       # #1 cost
    opportunities: list[Finding]              # sorted by predicted impact
    step_graph: networkx.DiGraph              # full plan graph
```

Key derivations, all doable from existing inputs:

- **Touched tables** — group scan steps by `table_id`, left-join to
  `svv_table_info`. Per-table: bytes scanned, rows in/out, rrscan %,
  time attributed.
- **Join participants** — walk the step graph: for each `hjoin` / `mjoin`,
  trace its two input streams back to their `scan` roots to identify the
  two tables (or CTE legs).
- **Redistribution causality** — for each `bcast`/`dist` step, find the
  consumer join downstream. Attribute the redistribute's cost to that
  join's table pair. Lets Scene 4 produce the "why" line.
- **Time attribution** — classify every step into one of 6 buckets using
  `step_name` → category map. Sum into `time_budget`.
- **Critical path** — use `networkx.dag_longest_path` on the step graph
  weighted by step runtime. The bottleneck spotlight is the heaviest
  node on it.
- **Skew actuals** — where `sys_query_details` exposes per-slice metrics,
  compute coefficient of variation; otherwise fall back to `svv_table_info.skew_rows`.
- **Predicted savings** — for DISTKEY-mismatch findings, estimate as
  `(redistribute_seconds × 0.8)` as a first-pass heuristic. Refined in
  a later phase.

**`narrate.py` (new)** — the English layer. Pure functions that turn a
`Finding` / `TableTouch` / `QueryContext` into sentences. All strings
live here so copy can be tuned without touching widgets.

**`diagnose.py` (rewritten)** — rules now consume `QueryContext`, not raw
DataFrames. Each rule returns a `Finding` with: severity, category,
seconds-cost, now/why/fix strings, table references, step references.

**`normalize.py` (unchanged)** — already solves the paste-to-DataFrame
problem.

### Visual layer

Drop the current tab layout. `app.py` becomes a `QScrollArea` hosting the
five scenes, in order, each a widget.

**New widgets under `widgets/`:**

| File | Responsibility |
|---|---|
| `headline_card.py` | Scene 1 top — runtime + verdict |
| `time_budget_bar.py` | Scene 1 bottom — segmented horizontal bar, hover tooltips |
| `warehouse_card.py` | Scene 2 — one per touched table |
| `flow_ribbon.py` | Scene 3 — animated Sankey in QGraphicsScene |
| `bottleneck_spotlight.py` | Scene 4 — big before/after card |
| `opportunity_list.py` | Scene 5 — stacked finding cards |
| `glossary_tooltip.py` | Reusable hover for plain-English term defs |

**Keep** (possibly repurposed): `paste_panel.py`, `title_bar.py`.
**Retire** from the main flow (kept behind a "Classic view" toggle during
development): `plan_graph.py`, `step_timeline.py`, `table_diagnostics.py`,
`recommendations.py`, `inspector.py`.

### Libraries to add

- `networkx` — graph algorithms on the step graph (critical path,
  ancestor/descendant traces for redistribute causality)
- `pyqtgraph` — richer Qt-native charts and the Sankey-style ribbon
  (pure Python, no WebEngine, fits the air-gapped constraint)

No sqlglot in this phase. No DuckDB. Air-gap contract preserved.

### Design tokens

Extend `theme.py` with severity colors (good / caution / problem),
category colors (one per time-budget bucket), and warehouse-card
geometry. Mirror into `style.qss`.

## Milestones & order of work

1. **Backbone** — `insights.py` + new `QueryContext`, plus rewritten
   `diagnose.py` on top of it. No UI change yet. Verify with the two
   demo TSVs in `analyzer/samples/`.
2. **Scene 1** — `HeadlineCard` + `TimeBudgetBar`. Throw this in the app
   alongside the old UI. First "wow" moment.
3. **Scene 2** — `WarehouseCard` + table grid layout.
4. **Scene 4** — `BottleneckSpotlight` (easier to land than Scene 3).
5. **Scene 5** — `OpportunityList`.
6. **Scene 3** — `FlowRibbon` (animation last; can ship without it
   and add as polish).
7. **Strip the old UI** into a hidden "Classic" menu entry.
8. **Narrative polish** — copy pass on `narrate.py` with the metaphor
   vocabulary above.

Each milestone is a separate commit on this branch.

## Out of scope for this phase (queued for next)

- Third paste input: the SQL query text
- `sqlglot` — tables/columns/joins/filters/CTEs/lineage extracted from SQL
- Distkey / sortkey alignment checked against the SQL (currently only
  inferred from the plan)
- Column-level lineage
- Query rewrite suggestions
- DuckDB as a local what-if oracle

## Success criteria

A non-technical user, given nothing but the app and two TSV dumps, can in
under 2 minutes answer:

1. How long did my query take?
2. Where did that time go?
3. Which table hurt the most, and why?
4. What should I change first?

If any of those four requires DBA knowledge to answer from the screen,
the phase isn't done.
