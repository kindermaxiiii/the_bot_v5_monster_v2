from __future__ import annotations

from app.fqis.probability.models import (
    MatchScoreState,
    RemainingGoalExpectancy,
    ScoreDistribution,
    ScoreProbability,
)
from app.fqis.probability.poisson import truncated_poisson_probabilities


def build_score_distribution(
    state: MatchScoreState,
    expectancy: RemainingGoalExpectancy,
    *,
    max_remaining_goals: int = 10,
) -> ScoreDistribution:
    if max_remaining_goals < 0:
        raise ValueError("max_remaining_goals must be >= 0")

    home_probs = truncated_poisson_probabilities(
        expectancy.lambda_home_remaining,
        max_remaining_goals,
    )
    away_probs = truncated_poisson_probabilities(
        expectancy.lambda_away_remaining,
        max_remaining_goals,
    )

    cells: list[ScoreProbability] = []

    for home_remaining, home_probability in enumerate(home_probs):
        for away_remaining, away_probability in enumerate(away_probs):
            probability = home_probability * away_probability

            cells.append(
                ScoreProbability(
                    remaining_home_goals=home_remaining,
                    remaining_away_goals=away_remaining,
                    final_home_goals=state.home_score + home_remaining,
                    final_away_goals=state.away_score + away_remaining,
                    probability=probability,
                )
            )

    return ScoreDistribution(
        state=state,
        expectancy=expectancy,
        max_remaining_goals=max_remaining_goals,
        cells=tuple(cells),
    )

    