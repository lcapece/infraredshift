# Build journal — `redshift-sqlopt`

A record of what was built, what broke, and what the evidence said. Written
because several conclusions here reversed earlier ones, and the reversals matter
more than the conclusions.

---

## Session 1 — 2026-07-25/26

### Starting request

> "A small language model capable of order-anonymous query optimizations and
> rewrites, using SQLGlot, packaged as a pip-installable wheel for a locked-down
> destination."

### What went wrong first

The phrase "small language model" was read as *probably meaning* a
rule-based engine. It did not. The user meant a real ~2B-parameter model. Hours
went into steering toward the deterministic reading before that was corrected —
a costly misread of a clear request, and the single biggest error of the session.

The eventual scope came from the user directly: **"I JUST need sql
optimizations."** Deterministic engine first; the model becomes a later phase
that proposes candidates the deterministic layer validates.

---

## Findings that reversed an assumption

### 1. Order-anonymity did *not* already exist

The claim was made that `canonical_sql_fingerprint()` in
`analyzer/query_similarity.py` already produced order-anonymous shapes. Tested:

```python
f("SELECT a,b FROM t WHERE x=1 AND y=2")  # select a, b ... where x = ? and y = ?
f("SELECT b,a FROM t WHERE y=2 AND x=1")  # select b, a ... where y = ? and x = ?
# -> DIFFERENT
```

`normalize=True` in sqlglot normalizes **identifier casing**, not operand order.
Literal-stripping did work. So order-anonymity was net-new work, not inherited.

### 2. SQLGlot already ships 14 optimizer rules

Prompted by the user asking whether an existing Postgres project could be
adapted. The answer turned out to be better than "yes": the library already in
use provides `qualify`, `pushdown_projections`, `normalize`,
`unnest_subqueries`, `pushdown_predicates`, `optimize_joins`,
`eliminate_subqueries`, `merge_subqueries`, `eliminate_joins`, `eliminate_ctes`,
`quote_identifiers`, `annotate_types`, `canonicalize`, `simplify`.

Verified that `simplify` **does** canonicalize AND/OR operand order — so the
hardest half of order-anonymity was already solved upstream. Only the SELECT-list
ordering needed adding, and only in the hash path.

What no Postgres project can provide: DISTKEY, SORTKEY, `DS_BCAST` vs
`DS_DIST_BOTH`, zone maps, slice skew. Postgres has no concept of distributing a
table across compute nodes. That layer stays custom regardless of starting point.

---

## Bugs found and fixed

### A. `'Placeholder' is not iterable` — whole-workload grouping failure

**Reported symptom:** no circles rendering; error log showed
`Repeat Grouping: 'Placeholder' is not iterable`.

**Cause:** `transform()` visits children before parents. By the time
`_canonicalize_shape_node` reached an `In`/`Values` node, its literal children
were already `Placeholder` nodes. A `Placeholder` has no `len()`, cannot be
sliced, cannot be iterated — so `node.args.get("expressions") or []` followed by
`len()` raised. The exception escaped `_deterministic_repeat_candidates`, was
caught at `cluster_analyze.py:1181`, and **zeroed the entire workload's
grouping**. One unanalyzable query cost all 1,260.

**Fix** (`69e107b`): `_as_node_list()` coerces arg slots to real lists;
`_iter_column_nodes()` catches `TypeError`/`ValueError`/`AttributeError` from
`find_all()`; per-query isolation records and skips a failing query instead of
aborting. Skipped query IDs surface through the progress callback so a swallowed
failure names the offender.

**Verified:** 68 groups / 420 members before and after on sample data. With a
poisoned row: 68/419 rather than a crash. 482/482 project tests pass.

**Cache:** confirmed by reading `_repeat_cache_key` that it hashes
`_REPEAT_CACHE_VERSION` plus data identities, never source code. No re-analysis
forced.

### B. Non-date cast rewritten as a date range

The port of `_unwrap_sortkey_column` dropped the date-type guard the original
analyzer code had. `CAST(amount AS VARCHAR) = '100'` on a numeric sort key became:

```sql
amount >= '100' AND amount < DATEADD(day, 1, '100')
```

Structurally unchanged — same tables, same columns, same projection — so the
validation gate could not see it. Fixed with a type check on the `Cast` branch,
removal of overloaded `TRUNC` from the `Anonymous` branch, and a second guard
requiring an ISO date literal.

### C. CRITICAL — `NOT IN` rewrite produced a wrong query

Found by adversarially probing the package's own output during review.

Unqualified input produced:

```sql
WHERE NOT EXISTS(SELECT 1 FROM dim WHERE k = k)
```

`k = k` is a tautology: unqualified columns inside a subquery resolve to the
**inner** table. The anti-join became an unconditional existence test meaning
"is `dim` empty" — **silently returning wrong rows.** Invisible to structural
validation for the same reason as bug B.

**Fix** (`630c14a`): resolve distinct inner and outer aliases before building the
predicate; refuse when either is ambiguous. Qualified input correctly emits
`i.k = o.k`. Also closed the gate hole — `validate_rewrite` now rejects
self-referential equalities that a rewrite *introduces* (a tautology already in
the original is the author's business).

### D. `DATE_TRUNC` never matched at all

Found by the 90-scenario corpus: the rule fired on only 7/10 of its own target
shape. Redshift `DATE_TRUNC` parses to sqlglot's **`TimestampTrunc`**, not
`DateTrunc`, so the handler checked the wrong class.

Fixed, and added the granularity guard that was missing: only day-level
truncation is accepted, because the rewrite builds a one-day range and
`DATE_TRUNC('month', col)` would silently narrow a month to a day.

---

## Claims that testing disproved

Three suspected weaknesses turned out not to be real. Recorded because
unfounded worries cost as much attention as real bugs:

| Suspicion | Reality |
|---|---|
| `DATE_TRUNC` granularity unguarded | Already correctly excluded (before the fix it never matched at all) |
| Rule exceptions silently swallowed | An injected `RuntimeError` surfaces as a `blocked` entry with its message |
| 64-bit fingerprint truncation risky | Birthday bound: 2.7e-8 at 1M queries, 2.7e-6 at 10M. Negligible |

---

## Design decisions and why

**Two code paths for canonicalization.** The fingerprint canonicalizer sorts the
SELECT list; the SQL emitter never does. Sorting projections is safe in a hash
and *fatal* in emitted SQL — it changes the result contract and breaks
`GROUP BY 1, 2` positional references. Sharing a normalizer between them is how a
fingerprinting shortcut becomes a corrupted rewrite.

**Parse failure is terminal for rewriting, not for analysis.** You cannot safely
reorganize a statement you could not read. Plan- and catalog-derived findings
never needed the AST, so they still work.

**One hard dependency.** `redshift-connector` pulls boto3, botocore, requests,
scramp, lxml, beautifulsoup4, pytz. On an air-gapped box every extra wheel is a
liability, and the destination already has the connector installed. It lives in
an optional extra and is never imported at module scope.

**Empty means unknown, never "safe."** `not_null_columns` absent does not mean
nullable — it means unproven, and rules must refuse. Bugs B and C both came from
acting on unproven assumptions.

**Tier ladder is the product.** REWRITE → DDL → DECOMPOSE. A large table with no
distribution key is a **DDL** finding, not a decomposition finding: decomposing
around an unkeyed table leaves every other query paying the same cost, while one
`ALTER TABLE` fixes the workload.

---

## Cost decisions declined

**8×H100 rental.** Fine-tuning a 1.5B model on 8,000 examples is 20–40 minutes on
*one* H100. Eight finish in five. The GPU was never the constraint — the corpus,
the eval harness, and CPU inference at the destination are. Renting eight would
have burned budget on idle hardware.

**Redshift Serverless for validation.** Deferred in favour of the free path: the
SYS view column names — the biggest untested assumption — can be lifted from
`analyzer/redshift_queries.py`, which is already field-tested against the real
cluster. Serverless auto-loads `tickit`, but at a few million rows it cannot
produce the billion-row broadcasts the findings are tuned for. A scaled-down
simulation (~5s queries, ratios preserved) tests the same logic for nothing.

---

## Where it stands

| Item | State |
|---|---|
| Grouping crash | **Fixed**, 482/482 project tests |
| `redshift-sqlopt` package | 346 tests, wheel built, CLI shipped |
| Order-anonymous fingerprint | Working incl. alias anonymity |
| 4 rewrite rules | All fire; all refuse when unproven |
| Plan-evidence findings | Working on synthetic rows |
| 90-scenario corpus | 0 crashes, 0 invalid SQL, 0 misses |

### Known gaps

1. **SYS view column names never cluster-verified** — spellings are guessed; a
   wrong guess means findings silently never fire. Fix: lift them from
   `analyzer/redshift_queries.py`.
2. **Thresholds uncalibrated** — invented, not derived from a real workload.
3. **Analyzer not wired to the package** — it ships as a concatenated single-file
   launcher, so importing a real package means changing the builder too.
4. **Decomposer schema resolution is broken** — unqualified table names resolve to
   the wrong schema (`dev.mart` vs `dev.marketing`), so they match no metadata,
   get no row count, and can never cross the 1M-row threshold. This is the
   likeliest cause of hundreds of queries yielding zero decomposition
   candidates. Plus a pandas dtype crash in `_candidate_table_rows` and an
   `except Exception` at `widgets/query_decomposer.py:170` hiding both.
5. **No trained model yet** — deliberately deferred; the deterministic validator
   it needs now exists.

---

## Commits

| Commit | Change |
|---|---|
| `0ea3cd1` | New `redshift_sqlopt` package |
| `0e66939` | Non-date cast + nullability gates; alias anonymity |
| `69e107b` | Analyzer grouping hardening (`Placeholder`) |
| `630c14a` | **CRITICAL** `NOT IN` tautology fix; CLI |
| `11dc655` | 90-scenario corpus; `DATE_TRUNC` fix |

---

## Lesson

Every one of bugs B, C, and D was **invisible to structural validation** — same
tables, same columns, same projection, valid SQL. They were found by running
diverse inputs and checking the *output*, not by reasoning about the code.

Structural checks are necessary and nowhere near sufficient. That is why the
emitted SQL still carries "validate before deploying," and why it should stay.
