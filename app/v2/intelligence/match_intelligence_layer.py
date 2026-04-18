from __future__ import annotations

from app.core.match_state import MatchState, TeamLiveStats
from app.v2.contracts import MatchIntelligenceSnapshot


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


class MatchIntelligenceLayer:
    """
    Phase 1 intelligence layer.

    The job here is deliberately narrow:
    - reuse V1 MatchState as input
    - emit a market-agnostic snapshot
    - avoid any market-specific pricing logic
    """

    def _team_pressure(self, team: TeamLiveStats) -> float:
        shots_signal = _clamp(
            (
                0.22 * _safe_int(team.shots_total)
                + 0.48 * _safe_int(team.shots_on_target)
                + 0.34 * _safe_int(team.shots_inside_box)
                + 0.16 * _safe_int(team.corners)
            )
            / 6.0,
            0.0,
            1.0,
        )
        territory_signal = _clamp(
            (_safe_int(team.dangerous_attacks) + 0.30 * _safe_int(team.attacks)) / 55.0,
            0.0,
            1.0,
        )
        possession_signal = _clamp((_safe_float(team.possession, 50.0) - 35.0) / 35.0, 0.0, 1.0)
        return _clamp(0.46 * shots_signal + 0.39 * territory_signal + 0.15 * possession_signal, 0.0, 1.0)

    def _team_threat(self, team: TeamLiveStats) -> float:
        return _clamp(
            (
                0.64 * _safe_int(team.shots_on_target)
                + 0.38 * _safe_int(team.shots_inside_box)
                + 0.10 * _safe_int(team.dangerous_attacks)
            )
            / 5.6,
            0.0,
            1.0,
        )

    def _regime_label(
        self,
        *,
        home_pressure: float,
        away_pressure: float,
        openness: float,
        slowdown: float,
        chaos: float,
        total_reds: int,
    ) -> tuple[str, float]:
        pressure_gap = home_pressure - away_pressure

        if total_reds > 0:
            return "RED_CARD_DISTORTED", _clamp(0.64 + 0.10 * min(total_reds, 2) + 0.08 * chaos, 0.0, 0.96)
        if chaos >= 0.72 and openness >= 0.60:
            return "CHAOTIC_TRANSITIONS", _clamp(0.62 + 0.20 * chaos, 0.0, 0.94)
        if openness >= 0.65 and min(home_pressure, away_pressure) >= 0.34:
            return "OPEN_EXCHANGE", _clamp(0.58 + 0.22 * openness, 0.0, 0.92)
        if pressure_gap >= 0.16:
            return "ASYMMETRIC_SIEGE_HOME", _clamp(0.54 + 0.85 * pressure_gap, 0.0, 0.92)
        if pressure_gap <= -0.16:
            return "ASYMMETRIC_SIEGE_AWAY", _clamp(0.54 + 0.85 * abs(pressure_gap), 0.0, 0.92)
        if slowdown >= 0.60 and openness <= 0.42:
            return "LATE_LOCKDOWN", _clamp(0.56 + 0.28 * slowdown, 0.0, 0.90)
        return "CLOSED_LOW_EVENT", _clamp(0.52 + 0.28 * (1.0 - openness), 0.0, 0.88)

    def build(self, state: MatchState) -> MatchIntelligenceSnapshot:
        minute = _safe_int(getattr(state, "minute", 0), 0)

        pressure_home = self._team_pressure(state.home)
        pressure_away = self._team_pressure(state.away)
        threat_home = self._team_threat(state.home)
        threat_away = self._team_threat(state.away)

        pressure_total = _clamp((pressure_home + pressure_away) / 2.0, 0.0, 1.0)
        threat_total = _clamp((threat_home + threat_away) / 2.0, 0.0, 1.0)
        openness = _clamp(0.58 * pressure_total + 0.42 * threat_total, 0.0, 1.0)

        total_reds = _safe_int(getattr(state, "home_reds", 0), 0) + _safe_int(getattr(state, "away_reds", 0), 0)
        goal_diff = abs(_safe_int(getattr(state, "goal_diff", 0), 0))
        remaining_minutes = max(0, int(getattr(state, "time_remaining_estimate", 0) or 0))

        chaos = _clamp(
            0.45 * abs(pressure_home - pressure_away)
            + 0.28 * openness
            + 0.17 * min(total_reds, 2)
            + 0.10 * (1.0 if goal_diff <= 1 else 0.45),
            0.0,
            1.0,
        )
        slowdown = _clamp(
            0.46 * (minute / 95.0)
            + 0.34 * (1.0 - openness)
            + 0.20 * (1.0 if goal_diff >= 2 else 0.25),
            0.0,
            1.0,
        )

        feed_quality = _clamp(_safe_float(getattr(state, "feed_quality_score", 0.58), 0.58), 0.0, 1.0)
        market_quality = _clamp(_safe_float(getattr(state, "market_quality_score", 0.62), 0.62), 0.0, 1.0)

        time_scale = remaining_minutes / 90.0
        red_bonus = 0.14 * min(total_reds, 2)
        score_bonus = 0.08 * min(goal_diff, 2)
        live_multiplier = 0.54 + 0.68 * openness + 0.32 * chaos + 0.28 * pressure_total + red_bonus + score_bonus
        remaining_goal_expectancy = _clamp(2.40 * time_scale * live_multiplier, 0.05, 4.50)

        score_state_fragility = _clamp(
            0.48 * openness
            + 0.24 * chaos
            + 0.28 * (remaining_goal_expectancy / max(1.0, float(state.total_goals + 1))),
            0.0,
            1.0,
        )

        regime_label, regime_confidence = self._regime_label(
            home_pressure=pressure_home,
            away_pressure=pressure_away,
            openness=openness,
            slowdown=slowdown,
            chaos=chaos,
            total_reds=total_reds,
        )

        quote_count = len(getattr(state, "quotes", []) or [])
        quote_density = _clamp(quote_count / 24.0, 0.0, 1.0)
        if minute < 20:
            minute_focus = 0.25
        elif minute <= 75:
            minute_focus = 1.0
        elif minute <= 90:
            minute_focus = 0.78
        else:
            minute_focus = 0.55

        fixture_priority_score = _clamp(
            10.0
            * (
                0.24 * feed_quality
                + 0.20 * market_quality
                + 0.20 * minute_focus
                + 0.16 * quote_density
                + 0.12 * openness
                + 0.08 * threat_total
            ),
            0.0,
            10.0,
        )

        diagnostics = {
            "goal_diff": goal_diff,
            "quote_count": quote_count,
            "remaining_minutes": remaining_minutes,
            "pressure_total": pressure_total,
            "threat_total": threat_total,
            "competition_quality": _safe_float(getattr(state, "competition_quality_score", 0.60), 0.60),
            "total_reds": total_reds,
        }

        return MatchIntelligenceSnapshot(
            fixture_id=int(state.fixture_id),
            minute=minute,
            score=state.score_text,
            home_goals=int(state.home_goals),
            away_goals=int(state.away_goals),
            fixture_priority_score=fixture_priority_score,
            regime_label=regime_label,
            regime_confidence=regime_confidence,
            pressure_home=pressure_home,
            pressure_away=pressure_away,
            threat_home=threat_home,
            threat_away=threat_away,
            openness=openness,
            slowdown=slowdown,
            chaos=chaos,
            remaining_goal_expectancy=remaining_goal_expectancy,
            score_state_fragility=score_state_fragility,
            feed_quality=feed_quality,
            market_quality=market_quality,
            diagnostics=diagnostics,
        )
