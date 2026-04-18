from __future__ import annotations

from math import isfinite

from app.config import settings
from app.core.contracts import MarketProjection, ScorelineDistribution
from app.core.match_state import MatchState
from app.markets.common import calibration_layer, execution_layer, market_engine


class TeamTotalsEngine:
    lines = [0.5, 1.5, 2.5, 3.5]

    def _f(self, value, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            value = float(value)
            if not isfinite(value):
                return default
            return value
        except (TypeError, ValueError):
            return default

    def _clamp(self, x: float, lo: float, hi: float) -> float:
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _team_goal_probabilities(self, distribution: ScorelineDistribution, team_side: str) -> dict[int, float]:
        if team_side == "HOME":
            return distribution.home_goal_probs
        return distribution.away_goal_probs

    def _current_team_goals(self, state: MatchState, team_side: str) -> int:
        if team_side == "HOME":
            return int(getattr(state, "home_goals", 0) or 0)
        return int(getattr(state, "away_goals", 0) or 0)

    def _prob_under(self, probs: dict[int, float], line: float) -> float:
        return sum(p for goals, p in probs.items() if goals < line)

    def _prob_over(self, probs: dict[int, float], line: float) -> float:
        return 1.0 - self._prob_under(probs, line)

    def _reason_block(self, team_side: str, side: str, line: float, regime_label: str, fair_prob: float, price_state: str) -> list[str]:
        return [
            f"regime={regime_label}",
            f"team={team_side}",
            f"side={side}",
            f"line={line:.1f}",
            f"fair_prob={fair_prob:.3f}",
            f"price_state={price_state}",
            "team_total_distribution",
        ]

    def _structural_penalty(self, state: MatchState, team_side: str, side: str, line: float, regime_label: str) -> float:
        minute = int(getattr(state, "minute", 0) or 0)
        regime = str(regime_label or "").upper()
        current_goals = self._current_team_goals(state, team_side)

        penalty = 0.0

        if "OVER" in side:
            goals_needed = max(0.0, line - current_goals)
            if goals_needed >= 2.5:
                penalty += 0.08
            elif goals_needed >= 1.5:
                penalty += 0.04

            if minute >= 70 and goals_needed >= 1.5:
                penalty += 0.05

            if regime in {"CLOSED_LOW_EVENT", "LATE_LOCKDOWN"}:
                penalty += 0.04

        if "UNDER" in side:
            if current_goals >= line:
                penalty += 0.50
            elif current_goals + 0.5 >= line:
                penalty += 0.05

            if regime in {"OPEN_EXCHANGE", "CHAOTIC_TRANSITIONS"}:
                penalty += 0.04

        return self._clamp(penalty, 0.0, 0.22)

    def evaluate(self, state: MatchState, distribution: ScorelineDistribution, regime_label: str) -> list[MarketProjection]:
        if not settings.team_totals_enabled:
            return []

        quotes = market_engine.quotes_for(state, "team_total", "FT")
        if not quotes:
            return []

        out: list[MarketProjection] = []

        for line in self.lines:
            for team_side in ("HOME", "AWAY"):
                # compatible with raw normalized selections such as HOME_OVER / HOME_UNDER / OVER / UNDER
                positive_aliases = {"over", f"{team_side.lower()} over", f"{team_side}_OVER"}
                negative_aliases = {"under", f"{team_side.lower()} under", f"{team_side}_UNDER"}

                pair = market_engine.pair_two_way(quotes, positive_aliases, negative_aliases, line=line)
                if not pair:
                    continue

                probs = self._team_goal_probabilities(distribution, team_side)
                prob_under = self._prob_under(probs, line)
                prob_over = self._prob_over(probs, line)

                for side, raw_probability, market_prob, quote in [
                    (f"{team_side}_UNDER", prob_under, pair["negative_no_vig"], pair["negative_quote"]),
                    (f"{team_side}_OVER", prob_over, pair["positive_no_vig"], pair["positive_quote"]),
                ]:
                    adjusted_raw = self._clamp(
                        self._f(raw_probability) * (1.0 - self._structural_penalty(state, team_side, side, line, regime_label)),
                        0.0001,
                        0.9999,
                    )

                    cal = calibration_layer.calibrate(
                        "team_total",
                        adjusted_raw,
                        minute=state.minute,
                        regime=regime_label,
                        quality=state.feed_quality_score,
                        market_probability=market_prob,
                        side=side,
                    )

                    calibrated_probability = self._clamp(self._f(cal.calibrated_probability), 0.0001, 0.9999)

                    proj = MarketProjection(
                        market_key="TEAM_TOTAL",
                        side=side,
                        line=line,
                        raw_probability=adjusted_raw,
                        calibrated_probability=calibrated_probability,
                        market_no_vig_probability=market_prob,
                        edge=calibrated_probability - self._f(market_prob),
                        expected_value=market_engine.expected_value(calibrated_probability, quote.odds_decimal),
                        bookmaker=quote.bookmaker,
                        odds_decimal=quote.odds_decimal,
                        executable=pair["price_state"] != "MORT",
                        price_state=pair["price_state"],
                        reasons=self._reason_block(team_side, side, line, regime_label, calibrated_probability, pair["price_state"]),
                        payload={
                            "regime_label": regime_label,
                            "team_side": team_side,
                            "raw_probability_pre_haircut": raw_probability,
                            "raw_probability_post_haircut": adjusted_raw,
                            "market_probability": market_prob,
                            "price_state": pair["price_state"],
                        },
                    )
                    out.append(execution_layer.classify(proj))

        return out
