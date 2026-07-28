"""Warehouse health check for the local DuckDB file.

Answers the question the app cannot otherwise answer without a load: is this
warehouse actually usable, and if the screens look wrong, is the data or the
analyzer at fault?

Kept free of Qt so it can be unit-tested and run from a script. The dialog in
cluster_dashboard.py only renders what this returns.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import time

import duckdb


# Tables that must hold rows for the main screens to work. Anything else being
# empty is a missing optional capture, not a broken warehouse.
_CORE_TABLES = ("query_history", "query_text", "svv_table_info_all")

# Cached analysis. Present means a warm start; absent only means the next open
# recomputes, so this is reported as information rather than a problem.
_CACHE_TABLES = (
    "analysis_cache_repeat_groups",
    "analysis_cache_repeat_members",
    "analysis_cache_repeat_group_tables",
    "analysis_cache_slow_features",
)

OK = "ok"
WARN = "warn"
FAIL = "fail"


@dataclass
class HealthCheck:
    """One named check with a verdict and a human-readable detail line."""

    name: str
    status: str
    detail: str
    advice: str = ""


@dataclass
class HealthReport:
    path: Path
    checks: list[HealthCheck] = field(default_factory=list)
    elapsed_s: float = 0.0

    @property
    def status(self) -> str:
        if any(check.status == FAIL for check in self.checks):
            return FAIL
        if any(check.status == WARN for check in self.checks):
            return WARN
        return OK

    @property
    def headline(self) -> str:
        counts = {
            level: sum(1 for check in self.checks if check.status == level)
            for level in (OK, WARN, FAIL)
        }
        if self.status == FAIL:
            return f"{counts[FAIL]} problem(s) found - this warehouse needs attention"
        if self.status == WARN:
            return f"Usable, with {counts[WARN]} thing(s) worth knowing"
        return f"Healthy - all {counts[OK]} checks passed"


def _fmt_bytes(size: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:,.1f} {unit}" if unit != "B" else f"{int(size):,} B"
        size /= 1024.0
    return f"{size:,.1f} GB"


def _table_names(con) -> set[str]:
    return {
        str(row[0])
        for row in con.execute("SELECT table_name FROM duckdb_tables()").fetchall()
    }


def _count(con, table: str) -> int:
    try:
        return int(con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] or 0)
    except Exception:
        return -1


def check_warehouse(path: str | Path) -> HealthReport:
    """Run every check against ``path`` read-only and return a report.

    Read-only throughout: a health check must never be the thing that modifies
    the warehouse, and it has to work while the app holds the file.
    """
    started = time.time()
    target = Path(path)
    report = HealthReport(path=target)

    if not target.exists():
        report.checks.append(
            HealthCheck(
                "File",
                FAIL,
                f"No file at {target}",
                "Run a load, or point at the right file with Browse.",
            )
        )
        report.elapsed_s = time.time() - started
        return report

    size = target.stat().st_size
    report.checks.append(
        HealthCheck("File", OK, f"{_fmt_bytes(size)} at {target}")
    )

    try:
        con = duckdb.connect(str(target), read_only=True)
    except Exception as exc:
        # The overwhelmingly common cause is the loader holding the write lock,
        # which is a normal state rather than corruption - so say which it is.
        text = str(exc).lower()
        locked = "lock" in text or "being used" in text
        report.checks.append(
            HealthCheck(
                "Open",
                FAIL,
                f"Could not open: {type(exc).__name__}: {str(exc)[:160]}",
                (
                    "Another process holds it. Close the loader (DuckDB allows a "
                    "single writer) and retry."
                    if locked
                    else "The file may be corrupt or from an incompatible DuckDB version."
                ),
            )
        )
        report.elapsed_s = time.time() - started
        return report

    try:
        tables = _table_names(con)
        report.checks.append(
            HealthCheck("Structure", OK, f"{len(tables):,} table(s) present")
        )

        # --- core data ---------------------------------------------------
        missing = [name for name in _CORE_TABLES if name not in tables]
        empty = [
            name for name in _CORE_TABLES if name in tables and _count(con, name) == 0
        ]
        # A brand-new install has the full schema and no rows. That is a
        # correct state, not a fault, and reporting it as a failure would
        # teach users to ignore this check on the one run where it is most
        # likely to be looked at.
        fresh_install = (
            not missing
            and len(empty) == len(_CORE_TABLES)
            and _count(con, "snapshot_runs") <= 0
        )
        if missing:
            report.checks.append(
                HealthCheck(
                    "Core tables",
                    FAIL,
                    f"Missing: {', '.join(missing)}",
                    "This file was not created by a completed load. Re-run the loader.",
                )
            )
        elif empty:
            # A fresh install is empty by definition and is NOT broken. Only
            # call it a failure when the schema shows a load was attempted -
            # otherwise the very first health check on a correct install
            # reports FAIL and teaches the user to ignore the check.
            report.checks.append(
                HealthCheck(
                    "Core tables",
                    OK if fresh_install else FAIL,
                    (
                        "New warehouse - schema in place, nothing captured yet"
                        if fresh_install
                        else f"Empty: {', '.join(empty)}"
                    ),
                    (
                        "Expected on a new install. Run a load to populate it."
                        if fresh_install
                        else "A load ran but captured nothing. Check the cluster "
                        "profile and credentials, then run a load."
                    ),
                )
            )
        else:
            counts = ", ".join(
                f"{name} {_count(con, name):,}" for name in _CORE_TABLES
            )
            report.checks.append(HealthCheck("Core tables", OK, counts))

        # --- captured SQL text -------------------------------------------
        # Repeat grouping is driven entirely by SQL text; without it the
        # bubbles cannot be built, and that is the single most common cause of
        # an empty triage screen.
        if "query_text" in tables:
            with_sql = con.execute(
                "SELECT COUNT(*) FROM query_text "
                "WHERE sql_text IS NOT NULL AND LENGTH(TRIM(sql_text)) > 0"
            ).fetchone()[0]
            total = _count(con, "query_text")
            if total > 0 and with_sql == 0:
                report.checks.append(
                    HealthCheck(
                        "SQL text",
                        FAIL,
                        f"0 of {total:,} rows have SQL text",
                        "Repeat grouping needs SQL text. Without it there are no "
                        "bubbles no matter how much else loaded.",
                    )
                )
            elif total > 0 and with_sql < total * 0.5:
                report.checks.append(
                    HealthCheck(
                        "SQL text",
                        WARN,
                        f"{with_sql:,} of {total:,} rows have SQL text",
                        "Patterns will be built from a partial workload.",
                    )
                )
            elif total > 0:
                report.checks.append(
                    HealthCheck("SQL text", OK, f"{with_sql:,} of {total:,} rows")
                )

        # --- snapshots ----------------------------------------------------
        if "snapshot_runs" in tables:
            row = con.execute(
                "SELECT COUNT(*), MAX(captured_at) FROM snapshot_runs"
            ).fetchone()
            runs = int(row[0] or 0)
            if runs == 0:
                report.checks.append(
                    HealthCheck(
                        "Snapshots",
                        OK if fresh_install else WARN,
                        (
                            "New warehouse - no loads run yet"
                            if fresh_install
                            else "No load runs recorded"
                        ),
                        (
                            "Run a load to populate it."
                            if fresh_install
                            else "The warehouse has no capture history."
                        ),
                    )
                )
            else:
                report.checks.append(
                    HealthCheck(
                        "Snapshots", OK, f"{runs:,} run(s), most recent {row[1]}"
                    )
                )

        # --- clusters -----------------------------------------------------
        # A placeholder namespace means the profile JSON was never filled in,
        # which silently mis-attributes every captured row.
        if "query_history" in tables:
            namespaces = con.execute(
                "SELECT DISTINCT namespace_id FROM query_history "
                "WHERE namespace_id IS NOT NULL"
            ).fetchall()
            values = [str(item[0]) for item in namespaces]
            placeholders = [v for v in values if v.upper().startswith("REPLACE-")]
            if placeholders:
                report.checks.append(
                    HealthCheck(
                        "Cluster identity",
                        FAIL,
                        f"{len(placeholders)} placeholder namespace id(s): "
                        f"{', '.join(placeholders[:3])}",
                        "redshift_cluster_profiles.json still has REPLACE-* values, so "
                        "rows are filed under a fake cluster.",
                    )
                )
            elif values:
                report.checks.append(
                    HealthCheck("Cluster identity", OK, f"{len(values)} cluster(s) present")
                )

        # --- analysis cache ------------------------------------------------
        present = [name for name in _CACHE_TABLES if name in tables]
        cached_groups = _count(con, "analysis_cache_repeat_groups") if present else 0
        if cached_groups > 0:
            report.checks.append(
                HealthCheck(
                    "Analysis cache",
                    OK,
                    f"{cached_groups:,} repeat group(s) cached - next open is warm",
                )
            )
        else:
            report.checks.append(
                HealthCheck(
                    "Analysis cache",
                    OK if fresh_install else WARN,
                    (
                        "Not built yet - nothing captured to group"
                        if fresh_install
                        else "No cached grouping"
                    ),
                    (
                        ""
                        if fresh_install
                        else "The next open recomputes patterns, which is the slow "
                        "path. This is normal right after a load."
                    ),
                )
            )

        # --- external (Spectrum) -------------------------------------------
        if "external_table_metadata" in tables:
            rows = _count(con, "external_table_metadata")
            if rows == 0:
                report.checks.append(
                    HealthCheck(
                        "External tables",
                        OK,
                        "Not captured (opt-in)",
                        "Run with --external-tables if you want the Spectrum view.",
                    )
                )
            else:
                report.checks.append(
                    HealthCheck("External tables", OK, f"{rows:,} column row(s)")
                )

        # --- readability ----------------------------------------------------
        # The checks above are metadata; this proves the pages actually read.
        try:
            con.execute("SELECT * FROM query_history LIMIT 5").fetchall()
            report.checks.append(HealthCheck("Read test", OK, "Sample read succeeded"))
        except Exception as exc:
            report.checks.append(
                HealthCheck(
                    "Read test",
                    FAIL,
                    f"{type(exc).__name__}: {str(exc)[:160]}",
                    "The file opens but cannot be read - suspect corruption.",
                )
            )
    finally:
        con.close()

    report.elapsed_s = time.time() - started
    return report


def format_report(report: HealthReport) -> str:
    """Plain-text rendering, used by the dialog's copy button and by scripts."""
    mark = {OK: "OK  ", WARN: "WARN", FAIL: "FAIL"}
    lines = [
        "DuckDB health check",
        f"  {report.path}",
        f"  {report.headline}  ({report.elapsed_s:.2f}s)",
        "",
    ]
    for check in report.checks:
        lines.append(f"  [{mark.get(check.status, '?')}] {check.name}: {check.detail}")
        if check.advice and check.status != OK:
            lines.append(f"           -> {check.advice}")
    return "\n".join(lines)
