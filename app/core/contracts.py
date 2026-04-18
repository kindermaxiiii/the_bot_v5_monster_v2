from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

# ----------------------------------------------------------------------
# Canonical value families
# ----------------------------------------------------------------------

MarketKey = Literal[
    "OU_FT",
    "OU_1H",
    "BTTS",
    "TEAM_TOTAL",
    "RESULT",
    "CORRECT_SCORE",
]

PriceState = Literal[
    "UNKNOWN",
    "VIVANT",
    "DEGRADE_MAIS_VIVANT",
    "MORT",
]

DocumentaryStatus = Literal[
    "DOC_ONLY",
    "DOC_STRONG",
]

RealStatus = Literal[
    "NO_BET",
    "REAL_VALID",
    "TOP_BET",
]


# ----------------------------------------------------------------------
# Regime / intensity / hazard / distribution contracts
# ----------------------------------------------------------------------

@dataclass(slots=True)
class RegimeDecision:
    """
    Regime classifier output.

    Notes:
    - regime_label must be one of the live regime labels used by regime_engine.py
    - diagnostics is the place for non-contractual debug/support values
    """
    regime_label: str
    regime_confidence: float
    pace_state: str
    control_state: str
    chaos_state: str
    pressure_state: str
    transition_state: str
    freeze_flag: bool = False
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class IntensityDecision:
    """
    Intensity engine output.

    lambda_* fields are expected goal intensities over:
    - next_5m
    - to_end
    """
    lambda_home_next_5m: float
    lambda_away_next_5m: float
    lambda_home_to_end: float
    lambda_away_to_end: float
    quality_penalty: float
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class HazardDecision:
    """
    Hazard layer output.

    Converts lambdas into short-horizon and to-end goal hazard metrics.
    """
    goal_hazard_next_5m: float
    goal_hazard_next_10m: float
    home_goal_hazard_to_end: float
    away_goal_hazard_to_end: float
    total_goal_expectancy_remaining: float
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ScorelineDistribution:
    """
    Final score distribution contract.

    final_score_probs uses the canonical key format: 'home-away'
    Example: '2-1'
    """
    home_goal_probs: dict[int, float]
    away_goal_probs: dict[int, float]
    final_score_probs: dict[str, float]
    home_win_prob: float
    draw_prob: float
    away_win_prob: float
    btts_yes_prob: float
    btts_no_prob: float
    remaining_home_goal_probs: dict[int, float] = field(default_factory=dict)
    remaining_away_goal_probs: dict[int, float] = field(default_factory=dict)
    remaining_total_goal_probs: dict[int, float] = field(default_factory=dict)
    lambda_home_remaining: float = 0.0
    lambda_away_remaining: float = 0.0
    lambda_total_remaining: float = 0.0


# ----------------------------------------------------------------------
# Market projection contract
# ----------------------------------------------------------------------

@dataclass(slots=True)
class MarketProjection:
    """
    Unified market candidate contract.

    Contractual semantics:
    - raw_probability:
        model probability BEFORE final calibration,
        but AFTER any market-engine structural adjustment/haircut/boost.
    - calibrated_probability:
        final model probability after calibration layer.
    - market_no_vig_probability:
        de-vigged market probability used as comparison baseline.
    - edge:
        calibrated_probability - market_no_vig_probability
    - expected_value:
        expected value computed from calibrated_probability and odds_decimal

    Status conventions:
    - documentary_status in {"DOC_ONLY", "DOC_STRONG"}
    - real_status in {"NO_BET", "REAL_VALID", "TOP_BET"}

    payload:
    - flexible debug/transport area
    - should at least contain for serious live candidates:
        regime_label
        regime_confidence
        minute
        current_total
        chaos
        calibration_confidence
        feed_quality
    - for O/U:
        goals_needed_for_over
        breathing_room_under
        structural_multiplier
    """
    market_key: str
    side: str
    line: float | None
    raw_probability: float

    calibrated_probability: float = 0.0
    market_no_vig_probability: float = 0.0
    edge: float = 0.0
    expected_value: float = 0.0

    bookmaker: str = ""
    odds_decimal: float | None = None
    executable: bool = False

    price_state: str = "UNKNOWN"
    documentary_status: str = "DOC_ONLY"
    real_status: str = "NO_BET"
    top_bet_flag: bool = False

    reasons: list[str] = field(default_factory=list)
    vetoes: list[str] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)
