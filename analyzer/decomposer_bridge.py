"""Optional bridge to the published ``redshift-query-decomposer`` package.

Infraredshift ships its own decomposer (``analyzer/query_decomposer.py``) and works
completely without this bridge. When the PyPI package happens to be installed,
the app can additionally offer its decomposability triage - a fast 0.0-1.0
estimate of whether ``decompose()`` would produce a usable staged plan - which
the built-in decomposer does not provide.

Design constraints, in order of importance:

**The package must never be a hard dependency.** Infraredshift is delivered as a
single concatenated file to a locked-down machine; a missing import at module
scope would break the whole application. Everything here imports lazily inside a
function and degrades to a clearly-labelled "unavailable" result.

**Import failure is reported, not swallowed.** ``availability()`` distinguishes
"not installed" from "installed but broken", because a silent absence looks
identical to a feature that simply does nothing - the failure mode that has cost
this project the most time.

**Version is checked, not assumed.** The triage API arrived in 0.2.0. An older
install is reported as too old rather than raising ``AttributeError`` somewhere
further downstream.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache

MINIMUM_VERSION = (0, 2, 0)
PACKAGE_NAME = "redshift-query-decomposer"
IMPORT_NAME = "redshift_decomposer"


@dataclass(frozen=True)
class Availability:
    """Whether the optional package can be used, and why not when it cannot."""

    installed: bool
    usable: bool
    version: str = ""
    reason: str = ""

    @property
    def status_text(self) -> str:
        if self.usable:
            return f"{PACKAGE_NAME} {self.version} available"
        if self.installed:
            return f"{PACKAGE_NAME} {self.version} present but unusable: {self.reason}"
        return f"{PACKAGE_NAME} not installed"

    @property
    def install_hint(self) -> str:
        return f"pip install '{PACKAGE_NAME}>=0.2.0'"


@dataclass(frozen=True)
class DecomposabilityAssessment:
    """Triage verdict for one query, or an explanation of why none was produced."""

    available: bool
    score: float = 0.0
    verdict: str = ""
    parse_ok: bool = False
    signals: tuple[str, ...] = field(default_factory=tuple)
    brief: str = ""
    error: str = ""

    @property
    def likely_worth_decomposing(self) -> bool:
        """Conservative gate for surfacing a decomposition suggestion.

        0.4 matches the package's own MODERATE threshold. Below it the generated
        script needs enough hand-editing that recommending it wastes the
        engineer's time.
        """
        return self.available and self.parse_ok and self.score >= 0.4


@lru_cache(maxsize=1)
def availability() -> Availability:
    """Report whether the optional package is importable and recent enough.

    Cached: the answer cannot change while the process runs, and the GUI asks on
    every render.
    """
    try:
        module = __import__(IMPORT_NAME)
    except ImportError:
        return Availability(installed=False, usable=False, reason="not installed")
    except Exception as exc:  # broken install, partial extraction, etc.
        return Availability(
            installed=True,
            usable=False,
            reason=f"import failed: {type(exc).__name__}: {exc}",
        )

    version = str(getattr(module, "__version__", "") or "unknown")
    try:
        parsed = tuple(int(part) for part in version.split(".")[:3])
    except ValueError:
        parsed = ()

    if parsed and parsed < MINIMUM_VERSION:
        wanted = ".".join(str(part) for part in MINIMUM_VERSION)
        return Availability(
            installed=True,
            usable=False,
            version=version,
            reason=f"version {version} is older than the required {wanted}",
        )

    if not hasattr(module, "assess_decomposability"):
        return Availability(
            installed=True,
            usable=False,
            version=version,
            reason="assess_decomposability is missing from this build",
        )

    return Availability(installed=True, usable=True, version=version)


def assess(sql: str) -> DecomposabilityAssessment:
    """Estimate whether *sql* is worth decomposing.

    Returns an ``available=False`` result rather than raising when the package is
    absent, so callers need no try/except and no feature flag.
    """
    state = availability()
    if not state.usable:
        return DecomposabilityAssessment(available=False, error=state.status_text)

    text = str(sql or "").strip()
    if not text:
        return DecomposabilityAssessment(available=True, error="no SQL supplied")

    try:
        from redshift_decomposer import assess_decomposability
    except Exception as exc:  # pragma: no cover - availability() already checked
        return DecomposabilityAssessment(
            available=False, error=f"{type(exc).__name__}: {exc}"
        )

    try:
        report = assess_decomposability(text)
    except Exception as exc:
        # A triage failure must never propagate into the GUI: the built-in
        # decomposer remains fully functional without it.
        return DecomposabilityAssessment(
            available=True, error=f"triage failed: {type(exc).__name__}: {exc}"
        )

    signals: list[str] = []
    for signal in getattr(report, "signals", ()) or ():
        label = str(getattr(signal, "label", "") or getattr(signal, "name", "") or signal)
        if label.strip():
            signals.append(label.strip())

    brief = ""
    for attribute in ("brief", "summary", "explanation"):
        value = getattr(report, attribute, None)
        if callable(value):
            try:
                value = value()
            except Exception:
                value = None
        if value:
            brief = str(value)
            break

    return DecomposabilityAssessment(
        available=True,
        score=float(getattr(report, "score", 0.0) or 0.0),
        verdict=str(getattr(report, "verdict", "") or ""),
        parse_ok=bool(getattr(report, "parse_ok", False)),
        signals=tuple(signals),
        brief=brief,
    )


def status_line() -> str:
    """One-line status for a settings screen or an About box."""
    state = availability()
    if state.usable:
        return f"Decomposability triage: enabled ({PACKAGE_NAME} {state.version})"
    return (
        f"Decomposability triage: unavailable - {state.reason}. "
        f"Optional; install with: {state.install_hint}"
    )
