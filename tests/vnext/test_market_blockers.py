from __future__ import annotations

from dataclasses import replace

from app.vnext.markets.translators import translate_market_candidates
from app.vnext.posterior.builder import build_scenario_posterior_result
from app.vnext.selection.match_selector import build_match_market_selection_result
from tests.vnext.live_factories import build_reference_posterior_result, make_live_snapshot


def test_result_family_is_hard_blocked_and_not_selectable() -> None:
    posterior_result = build_reference_posterior_result()
    translation = translate_market_candidates(posterior_result)
    result_candidates = [candidate for candidate in translation.candidates if candidate.family == "RESULT"]

    assert result_candidates
    assert all(candidate.is_blocked for candidate in result_candidates)
    assert all(candidate.is_selectable is False for candidate in result_candidates)
    assert all(any(blocker.code == "family_on_probation" and blocker.tier == "HARD" for blocker in candidate.blockers) for candidate in result_candidates)


def test_low_quality_snapshot_creates_confidence_blockers() -> None:
    prior_result = build_reference_posterior_result().prior_result
    previous = make_live_snapshot(minute=18, status="1H", home_goals=0, away_goals=0)
    current = make_live_snapshot(
        minute=21,
        status="1H",
        home_goals=0,
        away_goals=0,
        home_shots_total=None,
        away_shots_total=None,
        home_shots_on=None,
        away_shots_on=None,
        home_dangerous_attacks=None,
        away_dangerous_attacks=None,
        home_xg=None,
        away_xg=None,
    )
    posterior = build_scenario_posterior_result(prior_result, current, previous)
    translation = translate_market_candidates(posterior)

    assert translation.candidates
    assert any(any(blocker.tier == "CONFIDENCE" for blocker in candidate.blockers) for candidate in translation.candidates)


def test_flat_posterior_creates_no_directional_selection() -> None:
    prior_result = build_reference_posterior_result().prior_result
    previous = make_live_snapshot(
        minute=26,
        status="1H",
        home_goals=0,
        away_goals=0,
        home_shots_total=4,
        away_shots_total=4,
        home_shots_on=1,
        away_shots_on=1,
        home_dangerous_attacks=12,
        away_dangerous_attacks=12,
        home_xg=0.18,
        away_xg=0.18,
    )
    current = make_live_snapshot(
        minute=32,
        status="1H",
        home_goals=0,
        away_goals=0,
        home_shots_total=5,
        away_shots_total=5,
        home_shots_on=2,
        away_shots_on=2,
        home_dangerous_attacks=14,
        away_dangerous_attacks=14,
        home_xg=0.28,
        away_xg=0.28,
    )
    posterior = build_scenario_posterior_result(prior_result, current, previous)
    flat_scenarios = tuple(replace(candidate, posterior_score=0.18, delta_score=0.0) for candidate in posterior.scenarios)
    flat_posterior = replace(
        posterior,
        scenarios=flat_scenarios,
        top_posterior_scenario=flat_scenarios[0],
    )

    translation = translate_market_candidates(flat_posterior)
    selection = build_match_market_selection_result(flat_posterior)

    assert translation.candidates
    assert selection.best_candidate is None
    assert selection.no_selection_reason == "no_family_with_directional_support"
