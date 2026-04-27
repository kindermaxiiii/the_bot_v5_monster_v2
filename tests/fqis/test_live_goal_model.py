from __future__ import annotations

import math

import pytest

from app.fqis.probability.live_goal_model import (
    LiveGoalFeatures,
    LiveGoalModelConfig,
    build_live_score_distribution,
    estimate_remaining_expectancy,
)
from app.fqis.probability.market_probabilities import (
    probability_btts_no,
    probability_match_total_under,
    probability_team_total_under,
)
from app.fqis.contracts.enums import TeamRole


def test_estimate_remaining_expectancy_uses_xg_when_available() -> None:
    features = LiveGoalFeatures(
        event_id=2301,
        minute=58,
        home_score=1,
        away_score=0,
        home_xg_live=0.95,
        away_xg_live=0.18,
        home_shots_on_target=4,
        away_shots_on_target=1,
    )

    expectancy = estimate_remaining_expectancy(features)

    assert expectancy.lambda_home_remaining > expectancy.lambda_away_remaining
    assert expectancy.lambda_home_remaining > 0.0
    assert expectancy.lambda_away_remaining > 0.0


def test_estimate_remaining_expectancy_works_without_xg_using_proxy_stats() -> None:
    features = LiveGoalFeatures(
        event_id=2302,
        minute=54,
        home_score=0,
        away_score=1,
        home_shots_total=2,
        away_shots_total=9,
        home_shots_on_target=0,
        away_shots_on_target=4,
        home_corners=1,
        away_corners=5,
    )

    expectancy = estimate_remaining_expectancy(features)

    assert expectancy.lambda_away_remaining > expectancy.lambda_home_remaining
    assert expectancy.lambda_home_remaining >= 0.0
    assert expectancy.lambda_away_remaining > 0.0


def test_remaining_expectancy_declines_as_time_runs_out_with_same_signal() -> None:
    early = LiveGoalFeatures(
        event_id=2303,
        minute=35,
        home_score=0,
        away_score=0,
        home_xg_live=0.70,
        away_xg_live=0.40,
    )
    late = LiveGoalFeatures(
        event_id=2303,
        minute=80,
        home_score=0,
        away_score=0,
        home_xg_live=0.70,
        away_xg_live=0.40,
    )

    early_expectancy = estimate_remaining_expectancy(early)
    late_expectancy = estimate_remaining_expectancy(late)

    assert early_expectancy.lambda_home_remaining > late_expectancy.lambda_home_remaining
    assert early_expectancy.lambda_away_remaining > late_expectancy.lambda_away_remaining


def test_red_card_reduces_own_attacking_lambda_and_boosts_opponent() -> None:
    base = LiveGoalFeatures(
        event_id=2304,
        minute=50,
        home_score=0,
        away_score=0,
        home_xg_live=0.60,
        away_xg_live=0.60,
        home_red_cards=0,
        away_red_cards=0,
    )
    home_red = LiveGoalFeatures(
        event_id=2304,
        minute=50,
        home_score=0,
        away_score=0,
        home_xg_live=0.60,
        away_xg_live=0.60,
        home_red_cards=1,
        away_red_cards=0,
    )

    base_expectancy = estimate_remaining_expectancy(base)
    red_expectancy = estimate_remaining_expectancy(home_red)

    assert red_expectancy.lambda_home_remaining < base_expectancy.lambda_home_remaining
    assert red_expectancy.lambda_away_remaining > base_expectancy.lambda_away_remaining


def test_score_state_boosts_trailing_team_attack() -> None:
    draw_state = LiveGoalFeatures(
        event_id=2305,
        minute=60,
        home_score=0,
        away_score=0,
        home_xg_live=0.50,
        away_xg_live=0.50,
    )
    home_trailing = LiveGoalFeatures(
        event_id=2305,
        minute=60,
        home_score=0,
        away_score=1,
        home_xg_live=0.50,
        away_xg_live=0.50,
    )

    draw_expectancy = estimate_remaining_expectancy(draw_state)
    trailing_expectancy = estimate_remaining_expectancy(home_trailing)

    assert trailing_expectancy.lambda_home_remaining > draw_expectancy.lambda_home_remaining
    assert trailing_expectancy.lambda_away_remaining < draw_expectancy.lambda_away_remaining


def test_minute_90_has_zero_remaining_expectancy() -> None:
    features = LiveGoalFeatures(
        event_id=2306,
        minute=90,
        home_score=2,
        away_score=1,
        home_xg_live=2.0,
        away_xg_live=1.0,
    )

    expectancy = estimate_remaining_expectancy(features)

    assert expectancy.lambda_home_remaining == 0.0
    assert expectancy.lambda_away_remaining == 0.0


def test_live_score_distribution_can_price_markets() -> None:
    features = LiveGoalFeatures(
        event_id=2307,
        minute=58,
        home_score=1,
        away_score=0,
        home_xg_live=0.95,
        away_xg_live=0.18,
        home_shots_on_target=4,
        away_shots_on_target=1,
    )

    distribution = build_live_score_distribution(features, max_remaining_goals=10)

    assert math.isclose(distribution.total_probability, 1.0, rel_tol=1e-12)

    away_under_1_5 = probability_team_total_under(
        distribution,
        team_role=TeamRole.AWAY,
        line=1.5,
    ).probability
    btts_no = probability_btts_no(distribution).probability
    under_2_5 = probability_match_total_under(distribution, line=2.5).probability

    assert away_under_1_5 > 0.90
    assert btts_no > 0.70
    assert 0.0 <= under_2_5 <= 1.0


def test_live_goal_features_reject_negative_values() -> None:
    with pytest.raises(ValueError):
        LiveGoalFeatures(
            event_id=2308,
            minute=20,
            home_score=0,
            away_score=0,
            home_xg_live=-0.1,
        )


def test_model_config_rejects_invalid_weight() -> None:
    with pytest.raises(ValueError):
        LiveGoalModelConfig(xg_signal_weight=1.5)