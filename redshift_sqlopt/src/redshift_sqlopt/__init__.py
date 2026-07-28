"""redshift-sqlopt — evidence-driven Redshift query optimization.

Optimizes queries that have *already run*, using their own execution plan as
evidence. Two guarantees shape the whole package:

* **Deterministic.** Every rewrite is rule-based and refuses unless its
  preconditions are proven against catalog metadata. A rewrite that cannot be
  justified is reported as blocked, with the reason, rather than applied.
* **Predictive.** Findings carry measured numbers from ``SYS_QUERY_EXPLAIN`` and
  ``SYS_QUERY_DETAIL`` — rows broadcast, bytes spilled, estimate error — so a
  recommendation says how much it is worth, not merely that it exists.

Fixes escalate cheapest-first: rewrite the query, else change the table, else
decompose. A table with no distribution or sort key is a DDL finding, because
fixing the table helps every query that touches it.

Typical use::

    from redshift_sqlopt import Catalog, optimize

    catalog = Catalog.from_rows(table_rows=svv_table_info_rows)
    result = optimize(sql, catalog=catalog, explain_rows=..., detail_rows=...)

    print(result.summary())
    if result.has_rewrite:
        print(result.rewritten_sql)
"""

from .catalog import Catalog, TableStats, ViewDef, table_identities
from .fingerprint import canonical_shape, fingerprint, same_shape
from .models import (
    AppliedRewrite,
    BlockedRewrite,
    Finding,
    OptimizationResult,
    ParseFailure,
    PlanEvidence,
    Severity,
    Tier,
)
from .optimizer import optimize, validate_rewrite
from .plan import evidence_from_rows, findings_from_evidence
from .views import explode_views

__all__ = [
    "Catalog",
    "TableStats",
    "ViewDef",
    "table_identities",
    "fingerprint",
    "canonical_shape",
    "same_shape",
    "Tier",
    "Severity",
    "PlanEvidence",
    "Finding",
    "AppliedRewrite",
    "BlockedRewrite",
    "ParseFailure",
    "OptimizationResult",
    "optimize",
    "validate_rewrite",
    "evidence_from_rows",
    "findings_from_evidence",
    "explode_views",
]

__version__ = "0.1.0"
