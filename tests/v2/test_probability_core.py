from __future__ import annotations

from app.core.match_state import MatchState, TeamLiveStats
from app.v2.intelligence.match_intelligence_layer import MatchIntelligenceLayer
from app.v2.probability.unified_probability_core import UnifiedProbabilityCore


def _sample_state() -> MatchState:
    return MatchState(
        fixture_id=777,
        competition_id=39,
        competition_name="Premier League",
        country_name="England",
        minute=63,
        phase="2H",
        status="2H",
        home_goals=1,
        away_goals=1,
        feed_quality_score=0.78,
        market_quality_score=0.69,
        home=TeamLiveStats(
            team_id=1,
            name="Home",
            shots_total=11,
            shots_on_target=5,
            shots_inside_box=7,
            corners=4,
            possession=54.0,
            dangerous_attacks=31,
            attacks=73,
        ),
        away=TeamLiveStats(
            team_id=2,
            name="Away",
            shots_total=9,
            shots_on_target=3,
            shots_inside_box=4,
            corners=3,
            possession=46.0,
            dangerous_attacks=23,
            attacks=61,
        ),
    )


def _first_half_state() -> MatchState:
    return MatchState(
        fixture_id=778,
        competition_id=39,
        competition_name="Premier League",
        country_name="England",
        minute=23,
        phase="1H",
        status="1H",
        home_goals=0,
        away_goals=0,
        feed_quality_score=0.80,
        market_quality_score=0.71,
        home=TeamLiveStats(
            team_id=1,
            name="Home",
            shots_total=7,
            shots_on_target=3,
            shots_inside_box=5,
            corners=3,
            possession=53.0,
            dangerous_attacks=20,
            attacks=49,
        ),
        away=TeamLiveStats(
            team_id=2,
            name="Away",
            shots_total=5,
            shots_on_target=2,
            shots_inside_box=3,
            corners=2,
            possession=47.0,
            dangerous_attacks=16,
            attacks=41,
        ),
    )


def test_probability_grid_sums_to_one() -> None:
    state = _sample_state()
    intelligence = MatchIntelligenceLayer().build(state)
    probability = UnifiedProbabilityCore().build(intelligence)

    assert abs(sum(probability.ft_score_grid.values()) - 1.0) < 1e-9
    assert abs(sum(probability.remaining_added_goal_probs.values()) - 1.0) < 1e-9
    assert abs(sum(probability.final_total_goal_probs.values()) - 1.0) < 1e-9
    assert abs(sum(probability.home_goal_probs.values()) - 1.0) < 1e-9
    assert abs(sum(probability.away_goal_probs.values()) - 1.0) < 1e-9


def test_lambdas_are_non_negative() -> None:
    state = _sample_state()
    intelligence = MatchIntelligenceLayer().build(state)
    probability = UnifiedProbabilityCore().build(intelligence)

    assert probability.lambda_home_remaining >= 0.0
    assert probability.lambda_away_remaining >= 0.0
    assert probability.lambda_total_remaining >= 0.0


def test_total_probabilities_align_with_score_grid() -> None:
    state = _sample_state()
    intelligence = MatchIntelligenceLayer().build(state)
    probability = UnifiedProbabilityCore().build(intelligence)

    by_grid_added_total: dict[int, float] = {}
    by_grid_final_total: dict[int, float] = {}
    for score_key, mass in probability.ft_score_grid.items():
        home_final, away_final = [int(part) for part in score_key.split("-")]
        remaining_goals = (home_final - probability.home_goals) + (away_final - probability.away_goals)
        final_total = home_final + away_final
        by_grid_added_total[remaining_goals] = by_grid_added_total.get(remaining_goals, 0.0) + mass
        by_grid_final_total[final_total] = by_grid_final_total.get(final_total, 0.0) + mass

    assert set(by_grid_added_total) == set(probability.remaining_added_goal_probs)
    for total, mass in by_grid_added_total.items():
        assert abs(mass - probability.remaining_added_goal_probs[total]) < 1e-9

    assert set(by_grid_final_total) == set(probability.final_total_goal_probs)
    for total, mass in by_grid_final_total.items():
        assert abs(mass - probability.final_total_goal_probs[total]) < 1e-9


def test_final_total_goal_probs_sum_to_one() -> None:
    state = _sample_state()
    intelligence = MatchIntelligenceLayer().build(state)
    probability = UnifiedProbabilityCore().build(intelligence)

    assert abs(sum(probability.final_total_goal_probs.values()) - 1.0) < 1e-9


def test_first_half_remaining_distribution_sums_to_one() -> None:
    state = _first_half_state()
    intelligence = MatchIntelligenceLayer().build(state)
    probability = UnifiedProbabilityCore().build(intelligence)

    assert abs(sum(probability.ht_remaining_added_goal_probs.values()) - 1.0) < 1e-9
    assert abs(sum(probability.ht_final_total_goal_probs.values()) - 1.0) < 1e-9
    assert abs(sum(probability.ht_score_grid.values()) - 1.0) < 1e-9


def test_first_half_remaining_distribution_is_distinct_from_ft_distribution() -> None:
    state = _first_half_state()
    intelligence = MatchIntelligenceLayer().build(state)
    probability = UnifiedProbabilityCore().build(intelligence)

    assert probability.ht_remaining_added_goal_probs != probability.remaining_added_goal_probs
    assert probability.ht_final_total_goal_probs != probability.final_total_goal_probs
    assert probability.lambda_ht_total_remaining < probability.lambda_total_remaining
