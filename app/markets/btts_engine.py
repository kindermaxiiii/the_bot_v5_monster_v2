from __future__ import annotations

from math import isfinite

from app.config import settings
from app.core.contracts import MarketProjection, ScorelineDistribution
from app.core.match_state import MatchState
from app.markets.common import calibration_layer, execution_layer, market_engine


class BTTSEngine:
    """
    BTTS remains a secondary market.
    This engine is intentionally more selective than O/U.
    """

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

    def _reason_block(self, regime_label: str, side: str, fair_prob: float, price_state: str) -> list[str]:
        return [
            f"regime={regime_label}",
            f"side={side}",
            f"fair_prob={fair_prob:.3f}",
            f"price_state={price_state}",
            "btts_distribution",
        ]

    def _structure_penalty(self, state: MatchState, side: str, regime_label: str, raw_probability: float) -> float:
        minute = int(getattr(state, "minute", 0) or 0)
        current_home = int(getattr(state, "home_goals", 0) or 0)
        current_away = int(getattr(state, "away_goals", 0) or 0)
        regime = str(regime_label or "").upper()
        side = str(side or "").upper()

        penalty = 0.0

        # BTTS YES late with one side still blank is structurally fragile
        if side == "BTTS_YES":
            if minute >= 70 and (current_home == 0 or current_away == 0):
                penalty += 0.06
            if regime in {"CLOSED_LOW_EVENT", "LATE_LOCKDOWN"}:
                penalty += 0.05
            if raw_probability >= 0.82:
                penalty += 0.03

        if side == "BTTS_NO":
            if regime in {"OPEN_EXCHANGE", "CHAOTIC_TRANSITIONS"}:
                penalty += 0.05
            if minute <= 20 and current_home == 0 and current_away == 0:
                penalty += 0.02

        return self._clamp(penalty, 0.0, 0.18)

    def evaluate(self, state: MatchState, distribution: ScorelineDistribution, regime_label: str) -> list[MarketProjection]:
        if not settings.btts_enabled:
            return []

        quotes = market_engine.quotes_for(state, "btts", "FT")
        pair = market_engine.pair_two_way(quotes, {"yes", "btts yes", "BTTS_YES"}, {"no", "btts no", "BTTS_NO"})
        if not pair:
            return []

        out: list[MarketProjection] = []

        for side, raw_probability, market_prob, quote in [
            ("BTTS_YES", distribution.btts_yes_prob, pair["positive_no_vig"], pair["positive_quote"]),
            ("BTTS_NO", distribution.btts_no_prob, pair["negative_no_vig"], pair["negative_quote"]),
        ]:
            adjusted_raw = self._clamp(
                self._f(raw_probability) * (1.0 - self._structure_penalty(state, side, regime_label, self._f(raw_probability))),
                0.0001,
                0.9999,
            )

            cal = calibration_layer.calibrate(
                "btts",
                adjusted_raw,
                minute=state.minute,
                regime=regime_label,
                quality=state.feed_quality_score,
                market_probability=market_prob,
                side=side,
            )

            calibrated_probability = self._clamp(self._f(cal.calibrated_probability), 0.0001, 0.9999)

            proj = MarketProjection(
                market_key="BTTS",
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
