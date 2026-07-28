"""Drive the full diverse corpus through the optimizer.

Unit tests prove one rule against one shape. This proves the pipeline against a
workload that mixes them — window functions over joins, CTE chains, DML, views
over views, and malformed input — against a catalog with realistic physical-design
flaws. Regressions that only appear in combination surface here.
"""

from __future__ import annotations

import sqlglot
import pytest

from corpus import ALL_SCENARIOS, CATALOG, default_plan
from redshift_sqlopt import optimize


def _run(index, scenario):
    if scenario.plan:
        explain, detail = scenario.plan.get("explain"), scenario.plan.get("detail")
    else:
        explain, detail = default_plan(index)
    return optimize(scenario.sql, catalog=CATALOG, explain_rows=explain, detail_rows=detail)


CASES = list(enumerate(ALL_SCENARIOS))
IDS = [s.name for _, s in CASES]


@pytest.mark.parametrize("index,scenario", CASES, ids=IDS)
def test_scenario_never_raises(index, scenario) -> None:
    _run(index, scenario)


@pytest.mark.parametrize("index,scenario", CASES, ids=IDS)
def test_emitted_sql_always_reparses(index, scenario) -> None:
    """The single most important invariant: never emit invalid SQL."""
    result = _run(index, scenario)
    if result.has_rewrite:
        assert sqlglot.parse_one(result.rewritten_sql, read="redshift") is not None


@pytest.mark.parametrize(
    "index,scenario",
    [(i, s) for i, s in CASES if s.expect_applied],
    ids=[s.name for _, s in CASES if s.expect_applied],
)
def test_expected_rules_fire(index, scenario) -> None:
    result = _run(index, scenario)
    codes = {item.code for item in result.applied}
    for expected in scenario.expect_applied:
        assert expected in codes, f"{scenario.notes or scenario.name}: got {sorted(codes)}"


@pytest.mark.parametrize(
    "index,scenario",
    [(i, s) for i, s in CASES if s.expect_blocked],
    ids=[s.name for _, s in CASES if s.expect_blocked],
)
def test_unsound_rewrites_are_refused(index, scenario) -> None:
    """These are the soundness cases — a rule firing here would be a bug."""
    result = _run(index, scenario)
    applied = {item.code for item in result.applied}
    blocked = {item.code for item in result.blocked}
    for expected in scenario.expect_blocked:
        assert expected not in applied, f"{scenario.name}: {expected} must NOT fire"
        assert expected in blocked, f"{scenario.name}: expected {expected} in blocked"


@pytest.mark.parametrize(
    "index,scenario",
    [(i, s) for i, s in CASES if s.expect_no_rewrite],
    ids=[s.name for _, s in CASES if s.expect_no_rewrite],
)
def test_queries_that_must_be_left_alone(index, scenario) -> None:
    result = _run(index, scenario)
    assert not result.has_rewrite, f"{scenario.name} was rewritten but should not be"


@pytest.mark.parametrize(
    "index,scenario",
    [(i, s) for i, s in CASES if s.expect_parse_failure],
    ids=[s.name for _, s in CASES if s.expect_parse_failure],
)
def test_malformed_input_degrades(index, scenario) -> None:
    result = _run(index, scenario)
    assert not result.parsed
    assert not result.has_rewrite


def test_blocked_rewrites_always_carry_a_reason() -> None:
    for index, scenario in CASES:
        for item in _run(index, scenario).blocked:
            assert item.reason.strip(), f"{scenario.name}/{item.code} blocked with no reason"


def test_findings_are_ranked_cheapest_first() -> None:
    for index, scenario in CASES:
        tiers = [int(f.tier) for f in _run(index, scenario).ranked_findings()]
        assert tiers == sorted(tiers)


def test_corpus_covers_every_category() -> None:
    assert len({s.category for s in ALL_SCENARIOS}) >= 10


def test_corpus_exercises_the_rule_set() -> None:
    """Every rule must fire somewhere, or the corpus has a blind spot."""
    fired = set()
    for index, scenario in CASES:
        fired.update(item.code for item in _run(index, scenario).applied)
    for code in (
        "SARGABLE_SORTKEY",
        "NOT_IN_TO_NOT_EXISTS",
        "REDUNDANT_DISTINCT",
        "PROPAGATE_JOIN_FILTER",
    ):
        assert code in fired, f"{code} never fired anywhere in the corpus"
