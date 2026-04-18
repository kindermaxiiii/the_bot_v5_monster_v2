from __future__ import annotations

from math import isfinite

from app.config import settings
from app.core.contracts import MarketProjection, ScorelineDistribution
from app.core.match_state import MatchState
from app.markets.common import calibration_layer, execution_layer, market_engine


class ResultEngine:
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

    def _structure_penalty(self, state: MatchState, side: str, regime_label: str, raw_probability: float) -> float:
        minute = int(getattr(state, "minute", 0) or 0)
        regime = str(regime_label or "").upper()
        side = str(side or "").upper()
        goal_diff = int(getattr(state, "goal_diff", 0) or 0)

        penalty = 0.0

        if side == "DRAW":
            if minute >= 75 and goal_diff != 0:
                penalty += 0.08
            if regime in {"OPEN_EXCHANGE", "CHAOTIC_TRANSITIONS"}:
                penalty += 0.04

        if side == "HOME":
            if goal_diff < 0 and minute >= 70:
                penalty += 0.05
            if regime == "CONTROLLED_AWAY_PRESSURE":
                penalty += 0.04

        if side == "AWAY":
            if goal_diff > 0 and minute >= 70:
                penalty += 0.05
            if regime == "CONTROLLED_HOME_PRESSURE":
                penalty += 0.04

        if raw_probability >= 0.80:
            penalty += 0.03

        return self._clamp(penalty, 0.0, 0.20)

    def _reason_block(self, regime_label: str, side: str, fair_prob: float, price_state: str) -> list[str]:
        return [
            f"regime={regime_label}",
            f"side={side}",
            f"fair_prob={fair_prob:.3f}",
            f"price_state={price_state}",
            "scoreline_result_projection",
        ]

    def evaluate(self, state: MatchState, distribution: ScorelineDistribution, regime_label: str) -> list[MarketProjection]:
        if not settings.result_engine_enabled:
            return []

        quotes = market_engine.quotes_for(state, "1x2", "FT")
        pair = market_engine.pair_three_way(quotes)
        if not pair:
            return []

        out: list[MarketProjection] = []

        for side, raw_probability, market_prob, quote in [
            ("HOME", distribution.home_win_prob, pair["home_no_vig"], pair["home_quote"]),
            ("DRAW", distribution.draw_prob, pair["draw_no_vig"], pair["draw_quote"]),
            ("AWAY", distribution.away_win_prob, pair["away_no_vig"], pair["away_quote"]),
        ]:
            adjusted_raw = self._clamp(
                self._f(raw_probability) * (1.0 - self._structure_penalty(state, side, regime_label, self._f(raw_probability))),
                0.0001,
                0.9999,
            )

            cal = calibration_layer.calibrate(
                "1x2",
                adjusted_raw,
                minute=state.minute,
                regime=regime_label,
                quality=state.feed_quality_score,
                market_probability=market_prob,
                side=side,
            )

            calibrated_probability = self._clamp(self._f(cal.calibrated_probability), 0.0001, 0.9999)

            proj = MarketProjection(
                market_key="1X2",
                side=side,
                line=None,
                raw_probability=adjusted_raw,
                calibrated_probability=calibrated_probability,
                market_no_vig_probability=market_prob,
                edge=calibrated_probability - self._f(market_prob),
                expected_value=market_engine.expected_value(calibrated_probability, quote.odds_decimal),
                bookmaker=quote.bookmaker,
                odds_decimal=quote.odds_decimal,
                executable=pair["price_state"] != "MORT",
                price_state=pair["price_state"],
                reasons=self._reason_block(regime_label, side, calibrated_probability, pair["price_state"]),
                payload={
                    "regime_label": regime_label,
                    "raw_probability_pre_haircut": raw_probability,
                    "raw_probability_post_haircut": adjusted_raw,
                    "market_probability": market_prob,
                    "price_state": pair["price_state"],
                },
            )
            out.append(execution_layer.classify(proj))

        return out
