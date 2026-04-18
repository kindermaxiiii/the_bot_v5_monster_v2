from __future__ import annotations

from dataclasses import replace

from app.vnext.posterior.builder import build_scenario_posterior_result
from app.vnext.selection.match_selector import build_match_market_selection_result
from tests.vnext.live_factories import build_reference_posterior_result, make_live_snapshot


def test_match_selector_returns_best_approved_candidate() -> None:
    posterior_result = build_reference_posterior_result()

    selection = build_match_market_selection_result(posterior_result)

    assert selection.best_candidate is not None
    assert selection.no_selection_reason is None
    assert selection.best_candidate.candidate.family in {"OU_FT", "BTTS", "TEAM_TOTAL"}
    assert selection.best_candidate.candidate.family != "RESULT"
    assert selection.best_candidate.candidate.is_selectable is True


def test_match_selector_explains_all_candidates_blocked() -> None:
    prior_result = build_reference_posterior_result().prior_result
    previous = make_live_snapshot(minute=82, status="2H", home_goals=1, away_goals=0)
    current = make_live_snapshot(
        minute=88,
        status="2H",
        home_goals=1,
        away_goals=1,
        home_shots_total=7,
        away_shots_total=7,
        home_shots_on=2,
        away_shots_on=2,
        home_dangerous_attacks=18,
        away_dangerous_attacks=18,
        home_xg=0.55,
        away_xg=0.50,
    )
    posterior = build_scenario_posterior_result(prior_result, current, previous)

    selection = build_match_market_selection_result(posterior)

    assert selection.best_candidate is None
    assert selection.no_selection_reason == "all_candidates_blocked"


def test_match_selector_explains_posterior_too_weak() -> None:
    posterior = build_reference_posterior_result()
    weak_reliability = replace(
        posterior.posterior_reliability,
        posterior_reliability_score=0.44,
        live_snapshot_quality_score=0.52,
    )
    weak_posterior = replace(posterior, posterior_reliability=weak_reliability)

    selection = build_match_market_selection_result(weak_posterior)

    assert selection.best_candidate is None
    assert selection.no_selection_reason == "posterior_too_weak"
