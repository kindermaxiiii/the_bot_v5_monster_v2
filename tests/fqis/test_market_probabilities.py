from __future__ import annotations

import math

from app.fqis.contracts.enums import TeamRole
from app.fqis.probability.market_probabilities import (
    probability_1x2,
    probability_btts_no,
    probability_btts_yes,
    probability_match_total_over,
    probability_match_total_under,
    probability_no_more_goals,
    probability_team_total_under,
)
from app.fqis.probability.models import MatchScoreState, RemainingGoalExpectancy
from app.fqis.probability.score_distribution import build_score_distribution


def _sample_distribution():
    return build_score_distribution(
        MatchScoreState(event_id=1, minute=58, home_score=1, away_score=0),
        RemainingGoalExpectancy(lambda_home_remaining=0.52, lambda_away_remaining=0.10),
        max_remaining_goals=12,
    )


def test_no_more_goals_matches_poisson_zero_zero_case() -> None:
    distribution = _sample_distribution()

    probability = probability_no_more_goals(distribution).probability
    expected = math.exp(-(0.52 + 0.10))

    assert math.isclose(probability, expected, rel_tol=1e-9)


def test_btts_yes_and_no_are_complementary() -> None:
    distribution = _sample_distribution()

    yes = probability_btts_yes(distribution).probability
    no = probability_btts_no(distribution).probability

    assert math.isclose(yes + no, 1.0, rel_tol=1e-12)
    assert no > yes


def test_match_total_over_under_are_complementary_for_half_line() -> None:
    distribution = _sample_distribution()

    over = probability_match_total_over(distribution, line=2.5).probability
    under = probability_match_total_under(distribution, line=2.5).probability

    assert math.isclose(over + under, 1.0, rel_tol=1e-12)
    assert under > over


def test_team_total_under_for_away_is_high_when_away_lambda_is_low() -> None:
    distribution = _sample_distribution()

    probability = probability_team_total_under(
        distribution,
        team_role=TeamRole.AWAY,
        line=1.5,
    ).probability

    assert probability > 0.99


def test_1x2_probabilities_sum_to_one() -> None:
    distribution = _sample_distribution()

    result = probability_1x2(distribution)

    total = (
        result["HOME"].probability
        + result["DRAW"].probability
        + result["AWAY"].probability
    )

    assert math.isclose(total, 1.0, rel_tol=1e-12)
    assert result["HOME"].probability > result["AWAY"].probability


def test_current_score_already_btts_yes_makes_btts_yes_certain_if_both_scored() -> None:
    distribution = build_score_distribution(
        MatchScoreState(event_id=2, minute=70, home_score=1, away_score=1),
        RemainingGoalExpectancy(lambda_home_remaining=0.10, lambda_away_remaining=0.10),
        max_remaining_goals=8,
    )

    assert probability_btts_yes(distribution).probability == 1.0
    assert probability_btts_no(distribution).probability == 0.0


    