from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class MatchScoreState:
    event_id: int
    minute: int
    home_score: int
    away_score: int

    def __post_init__(self) -> None:
        if self.minute < 0:
            raise ValueError("minute must be >= 0")
        if self.home_score < 0:
            raise ValueError("home_score must be >= 0")
        if self.away_score < 0:
            raise ValueError("away_score must be >= 0")


@dataclass(slots=True, frozen=True)
class RemainingGoalExpectancy:
    lambda_home_remaining: float
    lambda_away_remaining: float

    def __post_init__(self) -> None:
        if self.lambda_home_remaining < 0:
            raise ValueError("lambda_home_remaining must be >= 0")
        if self.lambda_away_remaining < 0:
            raise ValueError("lambda_away_remaining must be >= 0")


@dataclass(slots=True, frozen=True)
class ScoreProbability:
    remaining_home_goals: int
    remaining_away_goals: int
    final_home_goals: int
    final_away_goals: int
    probability: float


@dataclass(slots=True, frozen=True)
class ScoreDistribution:
    state: MatchScoreState
    expectancy: RemainingGoalExpectancy
    max_remaining_goals: int
    cells: tuple[ScoreProbability, ...]

    @property
    def total_probability(self) -> float:
        return sum(cell.probability for cell in self.cells)

    def probability_of_final_score(self, home_goals: int, away_goals: int) -> float:
        return sum(
            cell.probability
            for cell in self.cells
            if cell.final_home_goals == home_goals and cell.final_away_goals == away_goals
        )


@dataclass(slots=True, frozen=True)
class MarketProbability:
    market_key: str
    probability: float

    def __post_init__(self) -> None:
        if not 0.0 <= self.probability <= 1.0:
            raise ValueError("market probability must be between 0 and 1")

            