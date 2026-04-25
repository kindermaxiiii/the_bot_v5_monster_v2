from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class SimpleMatchFeatures:
    event_id: int
    home_xg_live: float
    away_xg_live: float
    home_shots_on_target: int
    away_shots_on_target: int
    minute: int
    home_score: int
    away_score: int

    @property
    def total_xg_live(self) -> float:
        return self.home_xg_live + self.away_xg_live

    @property
    def score_diff(self) -> int:
        return self.home_score - self.away_score