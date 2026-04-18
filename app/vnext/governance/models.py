from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


InternalMatchStatus = Literal["TRACKING", "ARMED", "READY"]
PublicMatchStatus = Literal["NO_BET", "WATCHLIST", "ELITE"]


@dataclass(slots=True, frozen=True)
class PromotionDecision:
    internal_status: InternalMatchStatus
    public_status: PublicMatchStatus
    match_refusals: tuple[str, ...] = ()
    board_refusals: tuple[str, ...] = ()
    source: str = "promotion_decision.v1"


@dataclass(slots=True, frozen=True)
class GovernanceThresholds:
    ready_min_support: float = 0.62
    ready_min_confidence: float = 0.62
    ready_min_directionality: float = 0.60
    ready_min_posterior_reliability: float = 0.62
    watchlist_min_score: float = 0.62
    watchlist_min_support: float = 0.62
    watchlist_min_confidence: float = 0.60
    elite_min_score: float = 0.70
    elite_min_support: float = 0.70
    elite_min_confidence: float = 0.66
    elite_min_dominance_gap: float = 0.07
    max_elite: int = 1
    max_watchlist: int = 3
