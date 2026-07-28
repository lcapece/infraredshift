"""Public result types for a decomposition plan."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Finding:
    """Human-readable note about safety, assumptions, or review items."""

    level: str  # info | review | warning | blocked | required
    title: str
    detail: str


@dataclass(frozen=True)
class Stage:
    """One CREATE TEMP TABLE materialization step."""

    index: int
    name: str
    stage_type: str  # physical_input | cte | intermediate_join
    source: str
    sql: str
    columns: tuple[str, ...]
    diststyle: str
    distkey: str
    sortkeys: tuple[str, ...]
    estimated_source_rows: float
    estimated_source_mb: float
    pushed_predicates: tuple[str, ...]
    rationale: str
    safety: str  # safe | review


@dataclass
class DecompositionPlan:
    """Full output of decompose().

    Communicate to engineers: this is **not perfect**. Prefer showing
    ``conversion_likelihood`` (0–1 estimated chance of a usable conversion)
    and treat ``script`` as a **skeleton to move forward**, not a drop-in.
    """

    parse_ok: bool
    original_sql: str
    exploded_sql: str = ""
    final_sql: str = ""
    script: str = ""
    stages: list[Stage] = field(default_factory=list)
    findings: list[Finding] = field(default_factory=list)
    parse_error: str = ""
    score: float = 0.0
    # Estimated P(usable conversion) from assess_decomposability; independent
    # of ``score`` (which ranks staging payoff, not correctness confidence).
    conversion_likelihood: float = 0.0
    conversion_verdict: str = ""

    @property
    def changed(self) -> bool:
        return bool(self.stages)

    def engineer_brief(self) -> str:
        """Short note for the engineer who will own this rewrite."""
        pct = f"{self.conversion_likelihood:.0%}"
        verdict = self.conversion_verdict or "see assess_decomposability()"
        lines = [
            "This tool is not perfect.",
            f"Estimated conversion-success likelihood: {self.conversion_likelihood:.2f} ({pct}) - {verdict}",
            "Output is a staged skeleton to move forward, not a production-ready rewrite.",
            "Validate row counts, nulls, duplicates, and EXPLAIN against the original before use.",
        ]
        if self.stages:
            lines.append(
                f"Stages proposed: {len(self.stages)} "
                f"({sum(1 for s in self.stages if s.safety == 'review')} marked safety=review)."
            )
        else:
            lines.append("No stages emitted - original shape returned; little to adopt as-is.")
        return "\n".join(lines)
