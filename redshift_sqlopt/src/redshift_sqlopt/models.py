"""Core value types for the optimizer.

Three ideas drive every type in this module:

1. **The query already ran.** Findings are backed by observed plan evidence
   (``SYS_QUERY_EXPLAIN`` / ``SYS_QUERY_DETAIL``), not by guesses about what
   might be slow. A finding that cannot cite evidence says so.

2. **Cheapest fix first.** Every finding carries a :class:`Tier`. A rewrite the
   user can deploy today outranks a DDL change that needs a maintenance window,
   which outranks decomposing the query into staged temp tables.

3. **Refuse unless provable.** A rewrite that cannot verify its preconditions is
   reported as :class:`BlockedRewrite` with the reason. It is never emitted and
   never silently applied.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum


class Tier(IntEnum):
    """Escalation ladder — lower is cheaper and safer to adopt.

    The ordering is the product. A table with no DISTKEY and no SORTKEY is a
    ``DDL`` finding, not a ``DECOMPOSE`` one: decomposing queries against an
    unkeyed table treats the symptom while every other query on that table keeps
    paying the same cost. Only escalate when the tier below genuinely cannot fix
    the problem.
    """

    REWRITE = 1
    """Reorganize the SQL in place. Same statement, same result set. Reversible."""

    DDL = 2
    """Change the table (DISTKEY / SORTKEY / encoding). Needs a maintenance
    window, but fixes every query touching that table, not just this one."""

    DECOMPOSE = 3
    """Split into a staged pipeline of temp tables. Invasive; hand off to
    ``redshift-query-decomposer``. Last resort."""


class Severity(IntEnum):
    INFO = 0
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


@dataclass(frozen=True)
class PlanEvidence:
    """An observed fact from the execution plan that justifies a finding.

    This is what separates this tool from a static SQL linter: every number here
    was measured on a real run. ``step`` refers to the plan step in
    ``SYS_QUERY_EXPLAIN``; the row/byte figures come from ``SYS_QUERY_DETAIL``.
    """

    step: int | None = None
    node: str = ""
    detail: str = ""
    actual_rows: int | None = None
    estimated_rows: int | None = None
    bytes_scanned: int | None = None
    spill_bytes: int | None = None
    duration_s: float | None = None
    is_broadcast: bool = False
    is_redistribute: bool = False
    tables: tuple[str, ...] = ()

    @property
    def estimate_error_ratio(self) -> float | None:
        """How badly the planner mis-estimated this step.

        A large ratio is the classic signature of missing or stale statistics,
        and it is often the true cause of a bad join order. ``None`` when either
        side is unknown.
        """
        if self.actual_rows is None or self.estimated_rows is None:
            return None
        if self.estimated_rows <= 0:
            return None if self.actual_rows <= 0 else float("inf")
        return self.actual_rows / self.estimated_rows

    def describe(self) -> str:
        bits: list[str] = []
        if self.step is not None:
            bits.append(f"step {self.step}")
        if self.node:
            bits.append(self.node)
        if self.is_broadcast:
            bits.append("BROADCAST")
        if self.is_redistribute:
            bits.append("REDISTRIBUTE")
        if self.actual_rows is not None:
            bits.append(f"{self.actual_rows:,} rows")
        ratio = self.estimate_error_ratio
        if ratio is not None and ratio != float("inf") and ratio >= 10:
            bits.append(f"{ratio:,.0f}x over plan estimate")
        if self.spill_bytes:
            bits.append(f"{self.spill_bytes / 1e9:.1f} GB spilled")
        if self.duration_s is not None:
            bits.append(f"{self.duration_s:,.1f}s")
        text = ", ".join(bits)
        return f"{text} — {self.detail}" if self.detail else text


@dataclass(frozen=True)
class Finding:
    """A single actionable observation, ranked and evidence-backed."""

    tier: Tier
    severity: Severity
    code: str
    title: str
    explanation: str
    evidence: tuple[PlanEvidence, ...] = ()
    tables: tuple[str, ...] = ()
    suggested_ddl: str = ""
    estimated_benefit: str = ""

    @property
    def has_evidence(self) -> bool:
        return bool(self.evidence)

    def describe(self) -> str:
        head = f"[{self.tier.name}/{self.severity.name}] {self.title}"
        lines = [head, f"  {self.explanation}"]
        for item in self.evidence:
            lines.append(f"  evidence: {item.describe()}")
        if self.suggested_ddl:
            lines.append(f"  DDL: {self.suggested_ddl}")
        if self.estimated_benefit:
            lines.append(f"  benefit: {self.estimated_benefit}")
        return "\n".join(lines)


@dataclass(frozen=True)
class AppliedRewrite:
    """A rewrite that fired because its preconditions were proven."""

    code: str
    title: str
    rationale: str
    precondition: str = ""
    validation_notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class BlockedRewrite:
    """A rewrite that was recognized as applicable but refused.

    This type exists so that "we did not do this" is a first-class, inspectable
    result rather than silence. ``reason`` must say what could not be proven —
    e.g. a column that must be NOT NULL is nullable per the catalog, so an
    ``IN`` → ``EXISTS`` conversion would change the row count when NULLs appear.
    """

    code: str
    title: str
    reason: str
    precondition: str = ""
    would_have_done: str = ""


@dataclass(frozen=True)
class ParseFailure:
    """Terminal state: the SQL did not parse, so no rewrite may be attempted.

    Redshift accepts syntax that sqlglot does not always model, and these
    queries are known-valid because they already executed. For a *fingerprint*,
    degrading to a regex shape is acceptable. For a *rewriter* it is not — you
    cannot safely reorganize a statement you could not parse.

    Findings derived from plan rows and catalog metadata remain valid here,
    because they never depended on the AST.
    """

    reason: str
    sql: str = ""

    def __bool__(self) -> bool:  # pragma: no cover - trivial
        return True


@dataclass
class OptimizationResult:
    """Everything the optimizer concluded about one executed query."""

    original_sql: str
    fingerprint: str = ""
    fingerprint_method: str = ""
    rewritten_sql: str = ""
    findings: list[Finding] = field(default_factory=list)
    applied: list[AppliedRewrite] = field(default_factory=list)
    blocked: list[BlockedRewrite] = field(default_factory=list)
    parse_failure: ParseFailure | None = None
    exploded_views: tuple[str, ...] = ()

    @property
    def parsed(self) -> bool:
        return self.parse_failure is None

    @property
    def has_rewrite(self) -> bool:
        """True only when a rewrite was produced AND it differs from the input."""
        return bool(self.rewritten_sql) and self.rewritten_sql != self.original_sql

    @property
    def recommended_tier(self) -> Tier | None:
        """The cheapest tier that has something to say.

        Callers should act on this before considering anything more invasive.
        """
        if not self.findings:
            return None
        return min(finding.tier for finding in self.findings)

    def findings_by_tier(self, tier: Tier) -> list[Finding]:
        return [finding for finding in self.findings if finding.tier == tier]

    def ranked_findings(self) -> list[Finding]:
        """Cheapest tier first, then most severe, then evidence-backed first."""
        return sorted(
            self.findings,
            key=lambda f: (int(f.tier), -int(f.severity), not f.has_evidence),
        )

    def should_escalate_to_decomposition(self) -> bool:
        """Only recommend decomposition once cheaper tiers are exhausted.

        A DDL fix on an unkeyed table repairs the whole workload; decomposing
        around it would leave every other query paying the same cost.
        """
        if self.findings_by_tier(Tier.REWRITE) or self.findings_by_tier(Tier.DDL):
            return False
        return bool(self.findings_by_tier(Tier.DECOMPOSE))

    def summary(self) -> str:
        lines: list[str] = []
        if self.parse_failure is not None:
            lines.append(f"SQL DID NOT PARSE — no rewrite attempted: {self.parse_failure.reason}")
            lines.append("Plan- and catalog-derived findings below remain valid.")
        if self.fingerprint:
            lines.append(f"Fingerprint {self.fingerprint} ({self.fingerprint_method})")
        if self.exploded_views:
            lines.append("Views inlined: " + ", ".join(self.exploded_views))
        for finding in self.ranked_findings():
            lines.append(finding.describe())
        for item in self.applied:
            lines.append(f"[applied] {item.title} — {item.rationale}")
        for item in self.blocked:
            lines.append(f"[BLOCKED] {item.title} — {item.reason}")
        if not lines:
            lines.append("No findings.")
        return "\n".join(lines)
