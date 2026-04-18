from __future__ import annotations

from app.vnext.posterior.builder import build_scenario_posterior_result
from tests.vnext.live_factories import build_reference_prior_result, make_live_snapshot


def test_small_live_noise_does_not_flip_top_scenario() -> None:
    prior_result = build_reference_prior_result()
    previous = make_live_snapshot(minute=24, status="1H", home_goals=0, away_goals=0)
    current = make_live_snapshot(
        minute=29,
        status="1H",
        home_goals=0,
        away_goals=0,
        home_shots_total=8,
        away_shots_total=6,
        home_shots_on=3,
        away_shots_on=2,
        home_dangerous_attacks=24,
        away_dangerous_attacks=20,
        home_xg=0.54,
        away_xg=0.32,
    )

    result = build_scenario_posterior_result(prior_result, current, previous)

    assert result.top_changed is False
    assert result.top_posterior_scenario.key == prior_result.top_scenario.key


def test_break_event_strong_can_reclassify_top_scenario() -> None:
    prior_result = build_reference_prior_result()
    previous = make_live_snapshot(minute=61, status="2H", home_goals=1, away_goals=0)
    current = make_live_snapshot(
        minute=79,
        status="2H",
        home_goals=1,
        away_goals=2,
        home_shots_total=10,
        away_shots_total=16,
        home_shots_on=3,
        away_shots_on=8,
        home_dangerous_attacks=23,
        away_dangerous_attacks=44,
        home_xg=0.9,
        away_xg=2.0,
    )

    result = build_scenario_posterior_result(prior_result, current, previous)

    assert result.top_changed is True
    assert result.top_posterior_scenario.key in {"AWAY_CONTROL", "AWAY_ATTACKING_BIAS"}
