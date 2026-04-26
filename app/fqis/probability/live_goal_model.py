from __future__ import annotations

from dataclasses import dataclass

from app.fqis.probability.models import (
    MatchScoreState,
    RemainingGoalExpectancy,
    ScoreDistribution,
)
from app.fqis.probability.score_distribution import build_score_distribution


@dataclass(slots=True, frozen=True)
class LiveGoalFeatures:
    event_id: int
    minute: int
    home_score: int
    away_score: int

    home_xg_live: float | None = None
    away_xg_live: float | None = None

    home_shots_total: int | None = None
    away_shots_total: int | None = None

    home_shots_on_target: int | None = None
    away_shots_on_target: int | None = None

    home_corners: int | None = None
    away_corners: int | None = None

    home_red_cards: int | None = None
    away_red_cards: int | None = None

    def __post_init__(self) -> None:
        if self.minute < 0:
            raise ValueError("minute must be >= 0")
        if self.home_score < 0:
            raise ValueError("home_score must be >= 0")
        if self.away_score < 0:
            raise ValueError("away_score must be >= 0")

        _validate_optional_non_negative_float(self.home_xg_live, "home_xg_live")
        _validate_optional_non_negative_float(self.away_xg_live, "away_xg_live")

        _validate_optional_non_negative_int(self.home_shots_total, "home_shots_total")
        _validate_optional_non_negative_int(self.away_shots_total, "away_shots_total")
        _validate_optional_non_negative_int(self.home_shots_on_target, "home_shots_on_target")
        _validate_optional_non_negative_int(self.away_shots_on_target, "away_shots_on_target")
        _validate_optional_non_negative_int(self.home_corners, "home_corners")
        _validate_optional_non_negative_int(self.away_corners, "away_corners")
        _validate_optional_non_negative_int(self.home_red_cards, "home_red_cards")
        _validate_optional_non_negative_int(self.away_red_cards, "away_red_cards")


@dataclass(slots=True, frozen=True)
class LiveGoalModelConfig:
    regulation_minutes: int = 90

    baseline_home_goals_per_90: float = 1.38
    baseline_away_goals_per_90: float = 1.12

    xg_signal_weight: float = 0.70
    proxy_signal_weight: float = 0.45

    shot_on_target_xg_value: float = 0.18
    shot_off_target_xg_value: float = 0.04
    corner_xg_value: float = 0.03

    leading_attack_multiplier: float = 0.92
    trailing_attack_multiplier: float = 1.10
    draw_attack_multiplier: float = 1.00

    own_red_card_attack_multiplier: float = 0.72
    opponent_red_card_attack_multiplier: float = 1.18

    min_team_lambda_remaining: float = 0.0
    max_team_lambda_remaining: float = 4.50

    def __post_init__(self) -> None:
        if self.regulation_minutes <= 0:
            raise ValueError("regulation_minutes must be > 0")

        _validate_non_negative_float(
            self.baseline_home_goals_per_90,
            "baseline_home_goals_per_90",
        )
        _validate_non_negative_float(
            self.baseline_away_goals_per_90,
            "baseline_away_goals_per_90",
        )

        _validate_weight(self.xg_signal_weight, "xg_signal_weight")
        _validate_weight(self.proxy_signal_weight, "proxy_signal_weight")

        _validate_non_negative_float(self.shot_on_target_xg_value, "shot_on_target_xg_value")
        _validate_non_negative_float(self.shot_off_target_xg_value, "shot_off_target_xg_value")
        _validate_non_negative_float(self.corner_xg_value, "corner_xg_value")

        _validate_non_negative_float(self.leading_attack_multiplier, "leading_attack_multiplier")
        _validate_non_negative_float(self.trailing_attack_multiplier, "trailing_attack_multiplier")
        _validate_non_negative_float(self.draw_attack_multiplier, "draw_attack_multiplier")
        _validate_non_negative_float(
            self.own_red_card_attack_multiplier,
            "own_red_card_attack_multiplier",
        )
        _validate_non_negative_float(
            self.opponent_red_card_attack_multiplier,
            "opponent_red_card_attack_multiplier",
        )

        _validate_non_negative_float(
            self.min_team_lambda_remaining,
            "min_team_lambda_remaining",
        )
        _validate_non_negative_float(
            self.max_team_lambda_remaining,
            "max_team_lambda_remaining",
        )

        if self.max_team_lambda_remaining < self.min_team_lambda_remaining:
            raise ValueError("max_team_lambda_remaining must be >= min_team_lambda_remaining")


def estimate_remaining_expectancy(
    features: LiveGoalFeatures,
    *,
    config: LiveGoalModelConfig | None = None,
) -> RemainingGoalExpectancy:
    cfg = config or LiveGoalModelConfig()

    remaining_minutes = _remaining_minutes(features.minute, cfg.regulation_minutes)

    if remaining_minutes <= 0:
        return RemainingGoalExpectancy(
            lambda_home_remaining=0.0,
            lambda_away_remaining=0.0,
        )

    home_lambda = _estimate_team_lambda_remaining(
        minute=features.minute,
        remaining_minutes=remaining_minutes,
        baseline_goals_per_90=cfg.baseline_home_goals_per_90,
        observed_xg=features.home_xg_live,
        shots_total=features.home_shots_total,
        shots_on_target=features.home_shots_on_target,
        corners=features.home_corners,
        is_home=True,
        features=features,
        config=cfg,
    )
    away_lambda = _estimate_team_lambda_remaining(
        minute=features.minute,
        remaining_minutes=remaining_minutes,
        baseline_goals_per_90=cfg.baseline_away_goals_per_90,
        observed_xg=features.away_xg_live,
        shots_total=features.away_shots_total,
        shots_on_target=features.away_shots_on_target,
        corners=features.away_corners,
        is_home=False,
        features=features,
        config=cfg,
    )

    return RemainingGoalExpectancy(
        lambda_home_remaining=_clamp(
            home_lambda,
            cfg.min_team_lambda_remaining,
            cfg.max_team_lambda_remaining,
        ),
        lambda_away_remaining=_clamp(
            away_lambda,
            cfg.min_team_lambda_remaining,
            cfg.max_team_lambda_remaining,
        ),
    )


def build_live_score_distribution(
    features: LiveGoalFeatures,
    *,
    config: LiveGoalModelConfig | None = None,
    max_remaining_goals: int = 10,
) -> ScoreDistribution:
    state = MatchScoreState(
        event_id=features.event_id,
        minute=features.minute,
        home_score=features.home_score,
        away_score=features.away_score,
    )
    expectancy = estimate_remaining_expectancy(features, config=config)

    return build_score_distribution(
        state,
        expectancy,
        max_remaining_goals=max_remaining_goals,
    )


def _estimate_team_lambda_remaining(
    *,
    minute: int,
    remaining_minutes: int,
    baseline_goals_per_90: float,
    observed_xg: float | None,
    shots_total: int | None,
    shots_on_target: int | None,
    corners: int | None,
    is_home: bool,
    features: LiveGoalFeatures,
    config: LiveGoalModelConfig,
) -> float:
    prior_remaining = baseline_goals_per_90 * (remaining_minutes / config.regulation_minutes)

    if observed_xg is not None:
        live_signal_remaining = _project_remaining_from_observed_signal(
            observed_signal=float(observed_xg),
            minute=minute,
            remaining_minutes=remaining_minutes,
        )
        signal_weight = config.xg_signal_weight
    else:
        proxy_xg = _proxy_xg_from_counting_stats(
            shots_total=shots_total,
            shots_on_target=shots_on_target,
            corners=corners,
            config=config,
        )
        live_signal_remaining = _project_remaining_from_observed_signal(
            observed_signal=proxy_xg,
            minute=minute,
            remaining_minutes=remaining_minutes,
        )
        signal_weight = config.proxy_signal_weight

    blended = (signal_weight * live_signal_remaining) + ((1.0 - signal_weight) * prior_remaining)

    score_multiplier = _score_state_multiplier(
        home_score=features.home_score,
        away_score=features.away_score,
        is_home=is_home,
        config=config,
    )
    card_multiplier = _red_card_multiplier(
        home_red_cards=features.home_red_cards or 0,
        away_red_cards=features.away_red_cards or 0,
        is_home=is_home,
        config=config,
    )

    return blended * score_multiplier * card_multiplier


def _project_remaining_from_observed_signal(
    *,
    observed_signal: float,
    minute: int,
    remaining_minutes: int,
) -> float:
    elapsed = max(1, minute)
    observed_rate_per_minute = observed_signal / elapsed

    return max(0.0, observed_rate_per_minute * remaining_minutes)


def _proxy_xg_from_counting_stats(
    *,
    shots_total: int | None,
    shots_on_target: int | None,
    corners: int | None,
    config: LiveGoalModelConfig,
) -> float:
    total_shots = max(0, shots_total or 0)
    shots_ot = max(0, shots_on_target or 0)
    shots_off_target = max(0, total_shots - shots_ot)
    corner_count = max(0, corners or 0)

    return (
        (shots_ot * config.shot_on_target_xg_value)
        + (shots_off_target * config.shot_off_target_xg_value)
        + (corner_count * config.corner_xg_value)
    )


def _score_state_multiplier(
    *,
    home_score: int,
    away_score: int,
    is_home: bool,
    config: LiveGoalModelConfig,
) -> float:
    score_diff = home_score - away_score

    if score_diff == 0:
        return config.draw_attack_multiplier

    team_is_leading = score_diff > 0 if is_home else score_diff < 0

    if team_is_leading:
        return config.leading_attack_multiplier

    return config.trailing_attack_multiplier


def _red_card_multiplier(
    *,
    home_red_cards: int,
    away_red_cards: int,
    is_home: bool,
    config: LiveGoalModelConfig,
) -> float:
    own_red_cards = home_red_cards if is_home else away_red_cards
    opponent_red_cards = away_red_cards if is_home else home_red_cards

    return (
        config.own_red_card_attack_multiplier ** own_red_cards
        * config.opponent_red_card_attack_multiplier ** opponent_red_cards
    )


def _remaining_minutes(minute: int, regulation_minutes: int) -> int:
    return max(0, regulation_minutes - max(0, minute))


def _validate_optional_non_negative_int(value: int | None, field_name: str) -> None:
    if value is None:
        return

    if value < 0:
        raise ValueError(f"{field_name} must be >= 0")


def _validate_optional_non_negative_float(value: float | None, field_name: str) -> None:
    if value is None:
        return

    _validate_non_negative_float(value, field_name)


def _validate_non_negative_float(value: float, field_name: str) -> None:
    if value < 0:
        raise ValueError(f"{field_name} must be >= 0")


def _validate_weight(value: float, field_name: str) -> None:
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{field_name} must be between 0 and 1")


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))

    