"""Public decompose() entry point."""

from __future__ import annotations

from typing import Any

from sqlglot import exp, parse_one
from sqlglot.optimizer.qualify import qualify

from .analyze import analyze_query
from .catalog import Catalog
from .models import DecompositionPlan, Finding
from .planner import (
    PlannerConfig,
    plan_stages,
    rewrite_final_query,
    stages_to_models,
)
from .triage import assess_decomposability
from .views import ViewExplosionError, explode_views


def decompose(
    sql: str,
    catalog: Catalog | None = None,
    *,
    connection: Any | None = None,
    execute: Any | None = None,
    connect: Any | None = None,
    repository: Any | None = None,
    database: str | None = None,
    metadata_schema: str | None = None,
    minimum_rows: float = 1_000_000,
    minimum_size_mb: float = 1024.0,
    explode_view_depth: int = 8,
    qualify_columns: bool = True,
    materialize_repeated_ctes: bool = True,
    push_predicates: bool = True,
    prune_columns: bool = True,
    analyze_after_ctas: bool = True,
    fetch_views: bool = True,
    fetch_columns: bool = True,
) -> DecompositionPlan:
    """Decompose a Redshift SQL query into optimized temporary table stages.

    Parameters
    ----------
    sql:
        Source SQL (typically a SELECT).
    catalog:
        Table attributes and view definitions. Optional when *connection* or
        *execute* is provided — attributes are then fetched from Redshift for
        every object referenced in *sql*.
    connection:
        DB-API connection (``redshift_connector``, ``psycopg2``, etc.).
        Used only when *catalog* is omitted.
    execute:
        ``callable(sql) -> sequence[mapping]`` alternative to *connection*.
    connect:
        ``connect(database_name) -> connection|execute`` for multi-database
        live fills when using a repository.
    repository:
        :class:`~redshift_decomposer.TableRepository` path or instance. When
        set, table metrics are read from cache first; misses fall back to live
        SVV_TABLE_INFO / pg_table_def (requires connection/execute/connect).
    database:
        Database name for catalog keys when fetching (default: current_database()).
    metadata_schema:
        When non-empty, live catalog queries use this schema instead of system
        catalogs. Mirrors must use the same bare table names
        (``svv_table_info``, ``pg_table_def``, ``pg_views``, ``columns``,
        ``svv_external_columns``) and the expected column names. A summary
        ``svv_external_columns`` is enough — only the first partition key
        (``part_key = 1``) is harvested. ``None`` / blank = system catalogs.
    minimum_rows / minimum_size_mb:
        Thresholds for staging physical tables.
    explode_view_depth:
        Max recursive view-inlining passes.
    qualify_columns:
        Run SQLGlot qualify with the catalog schema when possible.
    fetch_views / fetch_columns:
        When auto-fetching a catalog, also load view definitions and column types.
    """
    original = str(sql or "").strip()
    if not original:
        return DecompositionPlan(
            False,
            original,
            parse_error="SQL text is empty.",
            findings=[Finding("blocked", "No SQL", "Provide a non-empty SQL string.")],
        )

    findings: list[Finding] = []
    # Always surface conversion likelihood for the engineer — this tool is not
    # perfect; the plan is a skeleton to move forward on a single query.
    triage = assess_decomposability(original)
    conversion_likelihood = triage.likelihood
    conversion_verdict = triage.verdict
    findings.append(
        Finding(
            "required",
            "Not perfect - skeleton for engineer review",
            (
                f"Estimated conversion-success likelihood: "
                f"{conversion_likelihood:.2f} ({conversion_likelihood:.0%}). "
                f"{conversion_verdict}. "
                "Use the staged script as a starting skeleton for this one query, "
                "not a guaranteed production rewrite. Validate equivalence before use."
            ),
        )
    )
    if catalog is None:
        if repository is None and connection is None and execute is None and connect is None:
            return DecompositionPlan(
                False,
                original,
                parse_error="catalog is required unless repository/connection/execute is provided.",
                findings=findings
                + [
                    Finding(
                        "blocked",
                        "No catalog or connection",
                        "Pass a Catalog, a TableRepository, or a Redshift connection/execute callable.",
                    )
                ],
                conversion_likelihood=conversion_likelihood,
                conversion_verdict=conversion_verdict,
            )
        try:
            if repository is not None:
                from .repository import fetch_catalog_with_repository

                catalog = fetch_catalog_with_repository(
                    original,
                    repository=repository,
                    connect=connect,
                    execute=execute if execute is not None else connection,
                    database=database,
                    fetch_views=fetch_views,
                    metadata_schema=metadata_schema,
                )
                cov = catalog.coverage or {}
                findings.append(
                    Finding(
                        "info",
                        "Catalog loaded (repository cache-first)",
                        (
                            f"cache_hits={cov.get('cache_hits', 0)}, "
                            f"cache_misses={cov.get('cache_misses', 0)}, "
                            f"tables={len(catalog.tables)}, views={len(catalog.views)}"
                        ),
                    )
                )
            else:
                from .fetch import fetch_catalog_for_sql

                catalog = fetch_catalog_for_sql(
                    original,
                    execute if execute is not None else connection,
                    database=database,
                    fetch_views=fetch_views,
                    fetch_columns=fetch_columns,
                    metadata_schema=metadata_schema,
                )
                cov = catalog.coverage or {}
                findings.append(
                    Finding(
                        "info",
                        "Catalog fetched from Redshift",
                        (
                            f"{len(catalog.tables)} table(s), {len(catalog.views)} view(s) "
                            f"loaded; discovered {len(cov.get('discovered', []))} relation name(s)."
                        ),
                    )
                )
            missing = list((catalog.coverage or {}).get("missing") or [])
            if missing:
                findings.append(
                    Finding(
                        "warning",
                        "Unresolved relation references",
                        (
                            "No cache/SVV_TABLE_INFO / pg_views hit for: "
                            + ", ".join(missing)
                            + ". Decomposition may under-stage or fail to explode views. "
                            "Rebuild the table repository or check database targeting."
                        ),
                    )
                )
            elif (catalog.coverage or {}).get("complete"):
                findings.append(
                    Finding(
                        "info",
                        "Relation coverage complete",
                        "Every discovered table/view reference resolved to catalog metadata.",
                    )
                )
        except Exception as exc:
            return DecompositionPlan(
                False,
                original,
                parse_error=str(exc).splitlines()[0][:400],
                findings=findings
                + [
                    Finding(
                        "blocked",
                        "Catalog fetch failed",
                        str(exc).splitlines()[0][:400],
                    )
                ],
                conversion_likelihood=conversion_likelihood,
                conversion_verdict=conversion_verdict,
            )

    try:
        tree = parse_one(original, read="redshift")
    except Exception as exc:
        return DecompositionPlan(
            False,
            original,
            parse_error=str(exc).splitlines()[0][:400],
            findings=findings
            + [Finding("blocked", "Parse failed", str(exc).splitlines()[0][:400])],
            conversion_likelihood=conversion_likelihood,
            conversion_verdict=conversion_verdict,
        )

    # Read queries only — never rewrite DML/DDL targets onto temp stages.
    _READ_ROOTS = (exp.Select, exp.Union, exp.Except, exp.Intersect, exp.Subquery)
    if not isinstance(tree, _READ_ROOTS):
        kind = type(tree).__name__
        return DecompositionPlan(
            False,
            original,
            parse_error=f"Decomposition only supports read queries; got {kind}.",
            findings=findings
            + [
                Finding(
                    "blocked",
                    "Not a read query",
                    f"{kind} is not decomposed. Pass a SELECT / UNION / set query "
                    "(extract the SELECT body from INSERT/CTAS/CREATE wrappers first).",
                )
            ],
            conversion_likelihood=conversion_likelihood,
            conversion_verdict=conversion_verdict,
        )

    with_clause = tree.args.get("with") if hasattr(tree, "args") else None
    if with_clause is not None and with_clause.args.get("recursive"):
        return DecompositionPlan(
            False,
            original,
            parse_error="WITH RECURSIVE cannot be staged as independent temps.",
            findings=findings
            + [
                Finding(
                    "blocked",
                    "Recursive CTE",
                    "WITH RECURSIVE cannot be materialized as independent staged temps.",
                )
            ],
            conversion_likelihood=conversion_likelihood,
            conversion_verdict=conversion_verdict,
        )

    # 1) Explode views using provided definitions
    try:
        tree, view_findings = explode_views(tree, catalog, max_depth=explode_view_depth)
        findings.extend(view_findings)
    except ViewExplosionError as exc:
        return DecompositionPlan(
            False,
            original,
            parse_error=str(exc),
            findings=findings
            + [Finding("blocked", "View explosion failed", str(exc))],
            conversion_likelihood=conversion_likelihood,
            conversion_verdict=conversion_verdict,
        )

    exploded_sql = tree.sql(dialect="redshift", pretty=True)

    # 2) Optional qualify (expand stars / qualify columns)
    if qualify_columns:
        schema = catalog.sqlglot_schema_mapping()
        if schema:
            try:
                tree = qualify(
                    tree,
                    schema=schema,
                    dialect="redshift",
                    expand_stars=True,
                    qualify_columns=True,
                    validate_qualify_columns=False,
                )
            except Exception as exc:
                findings.append(
                    Finding(
                        "review",
                        "Column qualification skipped",
                        f"SQLGlot qualify failed: {str(exc).splitlines()[0][:240]}",
                    )
                )

    # 3) Analyze + plan stages
    analysis = analyze_query(tree, catalog)
    config = PlannerConfig(
        minimum_rows=minimum_rows,
        minimum_size_mb=minimum_size_mb,
        materialize_repeated_ctes=materialize_repeated_ctes,
        push_predicates=push_predicates,
        prune_columns=prune_columns,
        analyze_after_ctas=analyze_after_ctas,
    )
    build = plan_stages(analysis, catalog, config)
    findings.extend(build.findings)

    stages = stages_to_models(build.stages, config)
    if not stages:
        findings.append(
            Finding(
                "required",
                "Validate before use",
                "No stages emitted; original SQL returned as the final statement.",
            )
        )
        script = (
            _script_disclaimer_header(conversion_likelihood, conversion_verdict)
            + "-- redshift-decomposer: no stages selected (skeleton empty)\n"
            + tree.sql(dialect="redshift", pretty=True)
            + ";\n"
        )
        return DecompositionPlan(
            True,
            original,
            exploded_sql=exploded_sql,
            final_sql=tree.sql(dialect="redshift", pretty=True),
            script=script,
            stages=[],
            findings=findings,
            score=0.0,
            conversion_likelihood=conversion_likelihood,
            conversion_verdict=conversion_verdict,
        )

    final_tree = rewrite_final_query(tree, build.replacements, build.cte_replacements)
    final_sql = final_tree.sql(dialect="redshift", pretty=True)

    header = _script_disclaimer_header(
        conversion_likelihood, conversion_verdict
    ).splitlines()
    header.append("")
    body = "\n\n".join(stage.sql for stage in stages)
    script = (
        "\n".join(header)
        + body
        + "\n\n-- Final query over staged temps\n"
        + final_sql
        + ";\n"
    )

    findings.append(
        Finding(
            "required",
            "Validate equivalence before use",
            "Compare results and EXPLAIN plans between the original and the "
            "decomposed skeleton. Do not promote without engineer sign-off.",
        )
    )

    score = _score(stages)
    return DecompositionPlan(
        True,
        original,
        exploded_sql=exploded_sql,
        final_sql=final_sql,
        script=script,
        stages=stages,
        findings=findings,
        score=score,
        conversion_likelihood=conversion_likelihood,
        conversion_verdict=conversion_verdict,
    )


def _script_disclaimer_header(likelihood: float, verdict: str) -> str:
    """SQL comment banner every engineer sees when they open the script."""
    lines = [
        "-- Generated by Redshift Query Decomposer "
        "(pip install redshift-query-decomposer)",
        "--",
        "-- THIS IS NOT PERFECT. Candidate skeleton for engineer review - not a",
        "-- drop-in production rewrite. One query at a time.",
        f"-- Estimated conversion-success likelihood: {likelihood:.2f} "
        f"({likelihood:.0%}).",
        f"--   {verdict}",
        "-- Temporary tables are session-scoped; run the entire script in one session.",
        "-- Before use: validate row counts, nulls, duplicates, and EXPLAIN vs original.",
        "",
    ]
    return "\n".join(lines)


def _score(stages: list) -> float:
    if not stages:
        return 0.0
    base = 20.0 * len(stages)
    size = sum(min(s.estimated_source_mb / 1024.0, 25.0) for s in stages)
    return round(min(100.0, base + size), 1)
