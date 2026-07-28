"""Generate a verified bad→good SQL rewrite corpus from seed patterns.

Direction matters. This generator does NOT invent bad SQL and then guess at a
fix — that produces pairs whose equivalence nobody has established, and a model
trained on silently-wrong labels learns to emit silently-wrong rewrites. Instead
every pair originates from a *known transformation* in ``seeds/patterns.py``:
the corruption is applied deliberately, so its inverse is correct by
construction.

On top of that guarantee, every emitted pair is machine-verified:

* both sides parse as Redshift SQL under sqlglot;
* the referenced table multiset is identical;
* the projected column count is identical (or the pattern declares otherwise);
* the two sides are not textually identical.

Pairs failing any check are dropped and counted, never emitted. The reject
count is reported — a silent drop would let a broken pattern quietly shrink the
corpus.

Usage::

    python generate.py --count 8000 --out corpus.jsonl
    python generate.py --count 200 --out sample.jsonl --seed 7
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import sqlglot
from sqlglot import exp

from seeds.patterns import ALL_PATTERNS, Pattern

DIALECT = "redshift"

# ---------------------------------------------------------------------------
# Vocabulary. Deliberately warehouse-flavored: the model should see realistic
# star-schema naming rather than foo/bar, because it must generalize to the
# user's actual Redshift workload.
# ---------------------------------------------------------------------------

SCHEMAS = (
    "analytics", "reporting", "staging", "warehouse", "finance",
    "sales", "marketing", "ops", "public", "dw",
)

FACT_TABLES = (
    "fact_orders", "fact_sales", "fact_events", "fact_shipments",
    "fact_transactions", "fact_clicks", "fact_impressions", "fact_inventory",
    "order_line", "web_events", "payment_history", "session_activity",
)

DIM_TABLES = (
    "dim_customer", "dim_product", "dim_store", "dim_date",
    "dim_supplier", "dim_channel", "dim_region", "dim_employee",
    "customer_master", "product_catalog", "store_lookup", "account_dim",
)

KEY_COLUMNS = (
    "customer_id", "order_id", "product_id", "store_id", "account_id",
    "user_id", "session_id", "supplier_id", "employee_id", "region_id",
)

DATE_COLUMNS = (
    "order_date", "event_date", "created_at", "updated_at", "ship_date",
    "transaction_date", "effective_date", "load_ts", "event_ts",
)

MEASURE_COLUMNS = (
    "amount", "quantity", "net_sales", "gross_amount", "unit_price",
    "discount", "tax_amount", "line_total", "revenue", "cost",
)

ATTR_COLUMNS = (
    "status", "region", "segment", "channel", "category",
    "country_code", "currency", "order_type", "source_system", "brand",
)

STR_LITERALS = (
    "'enterprise'", "'ACTIVE'", "'US'", "'retail'", "'completed'",
    "'PENDING'", "'web'", "'A'", "'EMEA'", "'shipped'",
)

DATE_LITERALS = (
    "'2024-01-01'", "'2024-06-30'", "'2023-12-31'", "'2025-01-01'",
    "'2024-03-15'", "'2024-09-01'", "'2023-07-01'", "'2025-04-01'",
)

ALIASES = ("a", "b", "c", "d", "t1", "t2", "o", "cst", "src", "tgt")


@dataclass
class Example:
    """One training pair plus the metadata a training script needs."""

    id: str
    pattern_code: str
    category: str
    severity: str
    bad_sql: str
    good_sql: str
    rationale: str
    plan_signature: str
    tags: list[str]

    def to_chat(self) -> dict:
        """Render as an instruction-tuning chat record.

        The assistant turn includes the reasoning before the SQL so the model
        learns to justify a rewrite rather than pattern-match one. Downstream
        consumers can strip the prose if they want SQL-only completions.
        """
        return {
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a Amazon Redshift query optimizer. Rewrite the "
                        "given SQL so it executes more efficiently while returning "
                        "exactly the same rows. Explain the reason, then give the "
                        "rewritten SQL."
                    ),
                },
                {"role": "user", "content": self.bad_sql},
                {
                    "role": "assistant",
                    "content": f"{self.rationale}\n\n```sql\n{self.good_sql}\n```",
                },
            ],
            "meta": {
                "id": self.id,
                "pattern_code": self.pattern_code,
                "category": self.category,
                "severity": self.severity,
                "tags": self.tags,
            },
        }


class Vocabulary:
    """Draws a mutually-consistent set of names for one generated example."""

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    def draw(self) -> dict[str, str]:
        rng = self.rng
        alias_a, alias_b = rng.sample(ALIASES, 2)
        # Alias for a derived/grouped subquery must not collide with either side.
        agg_alias = rng.choice([x for x in ALIASES if x not in (alias_a, alias_b)])
        sub_alias = rng.choice(
            [x for x in ALIASES if x not in (alias_a, alias_b, agg_alias)]
        )
        year = rng.randint(2021, 2025)
        return {
            "schema": rng.choice(SCHEMAS),
            "ta": rng.choice(FACT_TABLES),
            "tb": rng.choice(DIM_TABLES),
            "a": alias_a,
            "b": alias_b,
            "agg": agg_alias,
            "sub": sub_alias,
            "k": rng.choice(KEY_COLUMNS),
            "dt": rng.choice(DATE_COLUMNS),
            "m": rng.choice(MEASURE_COLUMNS),
            "c1": rng.choice(ATTR_COLUMNS),
            "c2": rng.choice(ATTR_COLUMNS),
            "lit_str": rng.choice(STR_LITERALS),
            "lit_int": str(rng.randint(1, 5000)),
            "lit_date": rng.choice(DATE_LITERALS),
            "lit_date2": rng.choice(DATE_LITERALS),
            "lit_year": str(year),
            "lit_year_start": f"'{year}-01-01'",
            "lit_year_end": f"'{year + 1}-01-01'",
        }


def _table_multiset(tree: exp.Expression) -> list[str]:
    names: list[str] = []
    for table in tree.find_all(exp.Table):
        parts = [p for p in (table.catalog, table.db, table.name) if p]
        names.append(".".join(parts).lower())
    return sorted(names)


def _projection_width(tree: exp.Expression) -> int:
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if select is None:
        return -1
    return len(select.expressions or [])


class VerificationError(Exception):
    """A generated pair failed a structural equivalence check."""


def verify_pair(bad_sql: str, good_sql: str, pattern: Pattern) -> None:
    """Structural sanity checks. Raises VerificationError on failure.

    These are necessary-but-not-sufficient conditions for equivalence. They
    cannot prove two queries return the same rows — nothing short of running
    both can — but they catch template bugs, which is what they are for.
    """
    if bad_sql.strip() == good_sql.strip():
        raise VerificationError("bad and good sides are identical")

    try:
        bad_tree = sqlglot.parse_one(bad_sql, read=DIALECT)
    except Exception as exc:
        raise VerificationError(f"bad side does not parse: {exc}") from exc
    try:
        good_tree = sqlglot.parse_one(good_sql, read=DIALECT)
    except Exception as exc:
        raise VerificationError(f"good side does not parse: {exc}") from exc

    # Patterns that legitimately restructure table references declare it; for
    # everything else, inventing or dropping a table is a template bug.
    if "union" not in pattern.tags and "semijoin" not in pattern.tags:
        bad_tables = _table_multiset(bad_tree)
        good_tables = _table_multiset(good_tree)
        if bad_tables != good_tables:
            raise VerificationError(
                f"table multiset changed: {bad_tables} -> {good_tables}"
            )

    # A rewrite must not change the shape of the result set. Aggregate-shaped
    # rewrites intentionally collapse rows, so their projection may differ.
    if not {"aggregate", "count_distinct", "union"} & set(pattern.tags):
        bad_width = _projection_width(bad_tree)
        good_width = _projection_width(good_tree)
        if bad_width != good_width and bad_width > 0 and good_width > 0:
            # SELECT * legitimately expands to named columns.
            if "projection" not in pattern.tags:
                raise VerificationError(
                    f"projection width changed: {bad_width} -> {good_width}"
                )


def render(pattern: Pattern, values: dict[str, str]) -> tuple[str, str]:
    """Fill a pattern's templates, then pretty-print through sqlglot.

    Round-tripping through sqlglot normalizes whitespace and keyword casing so
    the model sees consistent formatting and cannot learn to key off incidental
    layout differences between the bad and good sides.
    """
    bad_raw = pattern.bad.format(**values)
    good_raw = pattern.good.format(**values)
    bad_sql = sqlglot.parse_one(bad_raw, read=DIALECT).sql(dialect=DIALECT, pretty=True)
    good_sql = sqlglot.parse_one(good_raw, read=DIALECT).sql(dialect=DIALECT, pretty=True)
    return bad_sql, good_sql


def generate(
    count: int,
    *,
    seed: int = 1234,
    patterns: tuple[Pattern, ...] = ALL_PATTERNS,
) -> tuple[list[Example], dict[str, int]]:
    """Produce ``count`` verified examples, round-robin across patterns.

    Round-robin rather than random choice keeps categories balanced: a model
    trained on a corpus that is 60% one pattern learns that pattern and little
    else. Returns the examples plus a reject tally by reason.
    """
    rng = random.Random(seed)
    vocab = Vocabulary(rng)
    examples: list[Example] = []
    rejects: dict[str, int] = {}
    seen: set[tuple[str, str]] = set()

    attempts = 0
    max_attempts = count * 40
    index = 0
    while len(examples) < count and attempts < max_attempts:
        attempts += 1
        pattern = patterns[index % len(patterns)]
        index += 1
        values = vocab.draw()
        try:
            bad_sql, good_sql = render(pattern, values)
            verify_pair(bad_sql, good_sql, pattern)
        except VerificationError as exc:
            key = f"{pattern.code}: {exc}"
            rejects[key] = rejects.get(key, 0) + 1
            continue
        except Exception as exc:  # template/render failure
            key = f"{pattern.code}: render error: {str(exc)[:120]}"
            rejects[key] = rejects.get(key, 0) + 1
            continue

        dedup_key = (bad_sql, good_sql)
        if dedup_key in seen:
            rejects["duplicate"] = rejects.get("duplicate", 0) + 1
            continue
        seen.add(dedup_key)

        examples.append(
            Example(
                id=f"{pattern.code}-{len(examples):06d}",
                pattern_code=pattern.code,
                category=pattern.category,
                severity=pattern.severity,
                bad_sql=bad_sql,
                good_sql=good_sql,
                rationale=pattern.rationale,
                plan_signature=pattern.plan_signature,
                tags=list(pattern.tags),
            )
        )
    return examples, rejects


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=8000)
    parser.add_argument("--seed", type=int, default=1234)
    parser.add_argument("--out", type=Path, default=Path("corpus.jsonl"))
    parser.add_argument(
        "--format",
        choices=("chat", "raw"),
        default="chat",
        help="chat = instruction-tuning records; raw = flat pair records",
    )
    args = parser.parse_args(argv)

    examples, rejects = generate(args.count, seed=args.seed)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as handle:
        for example in examples:
            record = example.to_chat() if args.format == "chat" else asdict(example)
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    by_category: dict[str, int] = {}
    for example in examples:
        by_category[example.category] = by_category.get(example.category, 0) + 1

    print(f"wrote {len(examples):,} verified examples -> {args.out}")
    print("\nby category:")
    for category, n in sorted(by_category.items(), key=lambda kv: -kv[1]):
        print(f"  {category:<24} {n:>6,}")
    if rejects:
        total_rejects = sum(rejects.values())
        print(f"\nrejected {total_rejects:,} candidate(s):")
        for reason, n in sorted(rejects.items(), key=lambda kv: -kv[1])[:15]:
            print(f"  {n:>6,}  {reason}")
    if len(examples) < args.count:
        print(
            f"\nWARNING: produced {len(examples):,} of {args.count:,} requested. "
            "Pattern pool likely too small for this volume without duplication.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
