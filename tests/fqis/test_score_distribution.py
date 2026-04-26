from __future__ import annotations

import math

import pytest

from app.fqis.probability.models import MatchScoreState, RemainingGoalExpectancy
from app.fqis.probability.score_distribution import build_score_distribution


def test_score_distribution_sums_to_one() -> None:
    distribution = build_score_distribution(
        MatchScoreState(event_id=1, minute=58, home_score=1, away_score=0),
        RemainingGoalExpectancy(lambda_home_remaining=0.52, lambda_away_remaining=0.10),
        max_remaining_goals=10,
    )

    assert math.isclose(distribution.total_probability, 1.0, rel_tol=1e-12)


def test_score_distribution_contains_final_scores() -> None:
    distribution = build_score_distribution(
        MatchScoreState(event_id=1, minute=58, home_score=1, away_score=0),
        RemainingGoalExpectancy(lambda_home_remaining=0.52, lambda_away_remaining=0.10),
        max_remaining_goals=6,
    )

    probability_1_0 = distribution.probability_of_final_score(1, 0)
    probability_2_0 = distribution.probability_of_final_score(2, 0)

    assert probability_1_0 > 0
    assert probability_2_0 > 0
    assert probability_1_0 > probability_2_0


def test_zero_lambdas_make_current_score_certain() -> None:
    distribution = build_score_distribution(
        MatchScoreState(event_id=1, minute=90, home_score=2, away_score=1),
        RemainingGoalExpectancy(lambda_home_remaining=0.0, lambda_away_remaining=0.0),
        max_remaining_goals=5,
    )

    assert distribution.probability_of_final_score(2, 1) == 1.0
    assert distribution.probability_of_final_score(3, 1) == 0.0
    assert math.isclose(distribution.total_probability, 1.0, rel_tol=1e-12)


def test_score_distribution_rejects_negative_max_goals() -> None:
    with pytest.raises(ValueError):
        build_score_distribution(
            MatchScoreState(event_id=1, minute=50, home_score=0, away_score=0),
            RemainingGoalExpectancy(lambda_home_remaining=0.5, lambda_away_remaining=0.5),
            max_remaining_goals=-1,
        )

        