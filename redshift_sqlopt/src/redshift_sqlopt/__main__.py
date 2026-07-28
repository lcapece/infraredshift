"""Command-line entry point: ``python -m redshift_sqlopt``.

Reads SQL from a file, an argument, or stdin and prints the optimizer's verdict.
Catalog metadata (distribution keys, sort keys, row counts) and plan rows are
optional; without them the rules that require proof will refuse rather than
guess, so the output degrades to findings-only. That is the intended behaviour,
not a failure.

Examples::

    python -m redshift_sqlopt query.sql
    python -m redshift_sqlopt --sql "SELECT ..." --catalog tables.json
    cat query.sql | python -m redshift_sqlopt --plan plan.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import __version__
from .catalog import Catalog
from .models import Severity, Tier
from .optimizer import optimize

_TIER_LABEL = {
    Tier.REWRITE: "REWRITE  (deploy new SQL; reversible)",
    Tier.DDL: "DDL      (table change; fixes every query on it)",
    Tier.DECOMPOSE: "DECOMPOSE(stage into temp tables; last resort)",
}


def _read_sql(args: argparse.Namespace) -> str:
    if args.sql:
        return args.sql
    if args.file:
        return Path(args.file).read_text(encoding="utf-8")
    if not sys.stdin.isatty():
        return sys.stdin.read()
    return ""


def _load_json(path: str | None) -> object:
    if not path:
        return None
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _rows(value: object, *keys: str) -> list[dict]:
    """Pull a row list out of a JSON payload that may be a list or a dict."""
    if value is None:
        return []
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    if isinstance(value, dict):
        for key in keys:
            if key in value:
                return _rows(value[key])
    return []


def _bar(count: int, width: int = 28) -> str:
    return "#" * min(count, width)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="redshift-sqlopt",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("file", nargs="?", help="path to a .sql file (or use --sql / stdin)")
    parser.add_argument("--sql", help="SQL text given directly on the command line")
    parser.add_argument(
        "--catalog",
        help='JSON with table metadata: [{"schema":..,"table":..,"distkey":..,'
        '"sortkeys":..,"row_count":..}] or {"tables":[...],"views":[...]}',
    )
    parser.add_argument("--plan", help='JSON with {"explain":[...],"detail":[...]} plan rows')
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    parser.add_argument("--sql-only", action="store_true", help="print only the rewritten SQL")
    parser.add_argument("--no-views", action="store_true", help="do not inline view definitions")
    parser.add_argument("--version", action="version", version=f"redshift-sqlopt {__version__}")
    args = parser.parse_args(argv)

    sql = _read_sql(args).strip()
    if not sql:
        parser.error("no SQL supplied (give a file, --sql, or pipe it on stdin)")

    catalog_payload = _load_json(args.catalog)
    catalog = Catalog.from_rows(
        table_rows=_rows(catalog_payload, "tables", "table_rows"),
        view_rows=_rows(catalog_payload, "views", "view_rows"),
    )
    plan_payload = _load_json(args.plan)

    result = optimize(
        sql,
        catalog=catalog,
        explain_rows=_rows(plan_payload, "explain", "explain_rows"),
        detail_rows=_rows(plan_payload, "detail", "detail_rows"),
        expand_views=not args.no_views,
    )

    if args.sql_only:
        print(result.rewritten_sql or sql)
        return 0

    if args.json:
        print(
            json.dumps(
                {
                    "fingerprint": result.fingerprint,
                    "fingerprint_method": result.fingerprint_method,
                    "parsed": result.parsed,
                    "parse_error": result.parse_failure.reason if result.parse_failure else "",
                    "has_rewrite": result.has_rewrite,
                    "rewritten_sql": result.rewritten_sql,
                    "recommended_tier": (
                        result.recommended_tier.name if result.recommended_tier else None
                    ),
                    "exploded_views": list(result.exploded_views),
                    "applied": [
                        {"code": a.code, "title": a.title, "rationale": a.rationale}
                        for a in result.applied
                    ],
                    "blocked": [
                        {"code": b.code, "title": b.title, "reason": b.reason}
                        for b in result.blocked
                    ],
                    "findings": [
                        {
                            "tier": f.tier.name,
                            "severity": f.severity.name,
                            "code": f.code,
                            "title": f.title,
                            "explanation": f.explanation,
                            "suggested_ddl": f.suggested_ddl,
                            "estimated_benefit": f.estimated_benefit,
                            "evidence": [e.describe() for e in f.evidence],
                        }
                        for f in result.ranked_findings()
                    ],
                },
                indent=2,
            )
        )
        return 0

    width = 74
    print("=" * width)
    print(f" redshift-sqlopt {__version__}")
    print("=" * width)

    if not result.parsed:
        print(f"\n!! SQL DID NOT PARSE — no rewrite attempted")
        print(f"   {result.parse_failure.reason}")
        print("   Plan- and catalog-derived findings below remain valid.\n")
    else:
        print(f"\n fingerprint  {result.fingerprint}  ({result.fingerprint_method})")
    if result.exploded_views:
        print(f" views inlined {', '.join(result.exploded_views)}")
    if result.recommended_tier is not None:
        print(f" cheapest fix  {_TIER_LABEL[result.recommended_tier]}")

    if result.applied:
        print("\n" + "-" * width)
        print(f" REWRITES APPLIED ({len(result.applied)})")
        print("-" * width)
        for item in result.applied:
            print(f"\n  [{item.code}] {item.title}")
            print(f"      why: {item.rationale}")
            if item.precondition:
                print(f"      proven: {item.precondition}")

    if result.blocked:
        print("\n" + "-" * width)
        print(f" REWRITES WITHHELD ({len(result.blocked)}) — preconditions not proven")
        print("-" * width)
        for item in result.blocked:
            print(f"\n  [{item.code}] {item.title}")
            print(f"      reason: {item.reason}")

    findings = result.ranked_findings()
    if findings:
        print("\n" + "-" * width)
        print(f" FINDINGS ({len(findings)}) — cheapest fix first")
        print("-" * width)
        for finding in findings:
            marker = "!" * (int(finding.severity) - int(Severity.MEDIUM) + 1)
            print(f"\n  {finding.tier.name:<9} {finding.severity.name:<8} {marker}")
            print(f"  {finding.title}")
            print(f"      {finding.explanation}")
            for item in finding.evidence:
                print(f"      evidence: {item.describe()}")
            if finding.suggested_ddl:
                print(f"      DDL: {finding.suggested_ddl}")
            if finding.estimated_benefit:
                print(f"      benefit: {finding.estimated_benefit}")

    if result.has_rewrite:
        print("\n" + "=" * width)
        print(" REWRITTEN SQL")
        print("=" * width)
        print(result.rewritten_sql)
        print("\n" + "!" * width)
        print(" VALIDATE BEFORE DEPLOYING: run EXPLAIN on both, and compare row")
        print(" counts, NULLs, and representative results. Structural checks")
        print(" passed, but only execution proves equivalence.")
        print("!" * width)
    elif result.parsed and not findings:
        print("\n  No findings. Nothing to change.")
    elif result.parsed:
        print("\n  No safe rewrite was found; see findings above.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
