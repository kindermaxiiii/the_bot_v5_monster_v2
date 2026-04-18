from __future__ import annotations

from app.vnext.posterior.builder import build_scenario_posterior_result
from tests.vnext.live_factories import build_reference_prior_result, make_live_snapshot


def test_scenario_posterior_result_is_reliable_and_explainable() -> None:
    prior_result = build_reference_prior_result()
    previous = make_live_snapshot(minute=26, status="1H", home_goals=0, away_goals=0)
    current = make_live_snapshot(minute=33, status="1H", home_goals=0, away_goals=0)

    result = build_scenario_posterior_result(prior_result, current, previous)

    assert result.posterior_reliability.prior_reliability_score > 0.0
    assert result.posterior_reliability.live_snapshot_quality_score > 0.0
    assert result.posterior_reliability.event_clarity_score > 0.0
    assert result.posterior_reliability.posterior_reliability_score > 0.0
    assert result.top_posterior_scenario.explanation != ""
    assert len(result.top_posterior_scenario.modifier.rationale) > 0
    assert result.top_posterior_scenario.modifier.phase in {"EARLY", "MID", "LATE"}
    assert result.top_posterior_scenario.prior_score >= 0.0
    assert result.top_posterior_scenario.posterior_score >= 0.0
