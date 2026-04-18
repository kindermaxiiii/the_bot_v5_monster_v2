from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class MatchIntelligenceSnapshot:
    fixture_id: int
    minute: int
    score: str
    home_goals: int
    away_goals: int
    fixture_priority_score: float
    regime_label: str
    regime_confidence: float
    pressure_home: float
    pressure_away: float
    threat_home: float
    threat_away: float
    openness: float
    slowdown: float
    chaos: float
    remaining_goal_expectancy: float
    score_state_fragility: float
    feed_quality: float
    market_quality: float
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ProbabilityState:
    fixture_id: int
    minute: int
    score: str
    home_goals: int
    away_goals: int
    lambda_home_remaining: float
    lambda_away_remaining: float
    lambda_total_remaining: float
    ft_score_grid: dict[str, float]
    remaining_added_goal_probs: dict[int, float]
    final_total_goal_probs: dict[int, float]
    home_goal_probs: dict[int, float]
    away_goal_probs: dict[int, float]
    uncertainty_score: float
    diagnostics: dict[str, Any] = field(default_factory=dict)
    lambda_ht_home_remaining: float = 0.0
    lambda_ht_away_remaining: float = 0.0
    lambda_ht_total_remaining: float = 0.0
    ht_score_grid: dict[str, float] = field(default_factory=dict)
    ht_remaining_added_goal_probs: dict[int, float] = field(default_factory=dict)
    ht_final_total_goal_probs: dict[int, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class MarketProjectionV2:
    market_key: str
    side: str
    line: float | None
    bookmaker: str
    odds_decimal: float | None
    raw_probability: float
    calibrated_probability: float
    market_no_vig_probability: float
    edge: float
    expected_value: float
    executable: bool
    price_state: str
    payload: dict[str, Any] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)
    vetoes: list[str] = field(default_factory=list)
    favorable_resolution_distance: float | None = None
    adverse_resolution_distance: float | None = None
    resolution_pressure: float = 0.0
    state_fragility_score: float = 0.0
    late_fragility_score: float = 0.0
    early_fragility_score: float = 0.0
    score_state_budget: int | None = None
    market_findability_score: float = 0.0
    publishability_score: float = 0.0
    reasons_of_refusal: list[str] = field(default_factory=list)
    market_gate_state: str = "UNKNOWN"
    thesis_gate_state: str = "UNKNOWN"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "market_key": self.market_key,
            "side": self.side,
            "line": self.line,
            "bookmaker": self.bookmaker,
            "odds_decimal": self.odds_decimal,
            "calibrated_probability": self.calibrated_probability,
            "market_no_vig_probability": self.market_no_vig_probability,
            "edge": self.edge,
            "expected_value": self.expected_value,
            "executable": self.executable,
            "price_state": self.price_state,
        }

    def to_debug_dict(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(slots=True)
class MatchPrioritySnapshot:
    fixture_id: int
    q_match: float
    q_stats: float
    q_odds: float
    q_live: float
    q_competition: float
    q_noise: float
    priority_tier: str
    match_gate_state: str = "UNKNOWN"
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "fixture_id": self.fixture_id,
            "q_match": self.q_match,
            "q_stats": self.q_stats,
            "q_odds": self.q_odds,
            "priority_tier": self.priority_tier,
        }

    def to_debug_dict(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(slots=True)
class MatchBestVehicle:
    fixture_id: int
    best_projection: MarketProjectionV2 | None
    dominance_score: float
    candidate_count: int
    second_best_projection: MarketProjectionV2 | None = None
    rejected_same_match_candidates: list[MarketProjectionV2] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "fixture_id": self.fixture_id,
            "best_projection": None if self.best_projection is None else self.best_projection.to_public_dict(),
            "dominance_score": self.dominance_score,
            "candidate_count": self.candidate_count,
            "second_best_projection": (
                None if self.second_best_projection is None else self.second_best_projection.to_public_dict()
            ),
        }

    def to_debug_dict(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(slots=True)
class BoardBestVehicle:
    best_projection: MarketProjectionV2 | None
    match_rankings: list[MatchBestVehicle] = field(default_factory=list)
    board_dominance_score: float = 0.0
    top_bet_eligible: bool = False
    board_gate_state: str = "UNKNOWN"
    shadow_alert_tier: str = "NONE"
    elite_shadow_eligible: bool = False
    watchlist_shadow_eligible: bool = False
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_public_dict(self, *, shadow_alert_tier: str | None = None) -> dict[str, Any]:
        return {
            "best_projection": None if self.best_projection is None else self.best_projection.to_public_dict(),
            "board_dominance_score": self.board_dominance_score,
            "top_bet_eligible": self.top_bet_eligible,
            "shadow_alert_tier": shadow_alert_tier or self.shadow_alert_tier,
            "diagnostics": {
                "best_fixture_id": self.diagnostics.get("best_fixture_id"),
            },
        }

    def to_debug_dict(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(slots=True)
class ShadowMatchComparison:
    fixture_id: int
    v1_best_market_key: str | None
    v1_best_side: str | None
    v1_best_line: float | None
    v2_best_market_key: str | None
    v2_best_side: str | None
    v2_best_line: float | None
    same_market_family: bool
    same_direction: bool
    v2_board_best_flag: bool
    v2_top_bet_eligible: bool
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ShadowBoardComparison:
    v1_best_fixture_id: int | None
    v1_best_market_key: str | None
    v1_best_side: str | None
    v1_best_line: float | None
    v2_best_fixture_id: int | None
    v2_best_market_key: str | None
    v2_best_side: str | None
    v2_best_line: float | None
    same_market_family: bool
    same_direction: bool
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ShadowComparisonSummary:
    compared_match_count: int
    same_market_family_count: int
    same_direction_count: int
    v2_divergence_count: int
    board_best_difference_count: int
    v2_top_bet_eligible_true_count: int
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
