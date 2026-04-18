from __future__ import annotations

from math import isfinite

from app.config import settings
from app.core.contracts import MarketProjection, ScorelineDistribution
from app.core.match_state import MatchState
from app.markets.common import calibration_layer, execution_layer, market_engine


class CorrectScoreEngine:
    """
    Correct score stays documentary-first by doctrine.
    Even when enabled, the engine is heavily capped.
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

    def _score_volatility_penalty(self, state: MatchState, regime_label: str, raw_probability: float) -> float:
        minute = int(getattr(state, "minute", 0) or 0)
        regime = str(regime_label or "").upper()

        penalty = 0.08  # base prudence for CS
        if regime in {"OPEN_EXCHANGE", "CHAOTIC_TRANSITIONS", "POST_GOAL_REPRICING", "RED_CARD_DISTORTED"}:
            penalty += 0.08
        elif regime in {"CLOSED_LOW_EVENT", "LATE_LOCKDOWN"}:
            penalty += 0.03

        if minute <= 20:
            penalty += 0.04
        if raw_probability >= 0.50:
            penalty += 0.03

        return self._clamp(penalty, 0.0, 0.30)

    def evaluate(self, state: MatchState, distribution: ScorelineDistribution, regime_label: str) -> list[MarketProjection]:
        if not settings.correct_score_enabled:
            return []

        quotes = market_engine.quotes_for(state, "correct_score", "FT")
        if not quotes:
            return []

        quote_map = {}
        for q in quotes:
            side = str(getattr(q, "side", "") or "").strip()
            if side and getattr(q, "odds_decimal", None) and q.odds_decimal > 1.0:
                quote_map[side] = q

        ordered_scores = sorted(distribution.final_score_probs.items(), key=lambda kv: kv[1], reverse=True)

        out: list[MarketProjection] = []
        doc_kept = 0
        doc_cap = int(getattr(settings, "correct_score_max_doc_candidates", 1) or 1)

        for score, raw_probability in ordered_scores:
            quote = quote_map.get(score)
            if not quote:
                continue

            adjusted_raw = self._clamp(
                self._f(raw_probability) * (1.0 - self._score_volatility_penalty(state, regime_label, self._f(raw_probability))),
                0.0001,
                0.9999,
            )

            # For correct score we use plain implied probability here;
            # full no-vig pairing is not realistically available in most live books.
            market_prob = 1.0 / quote.odds_decimal

            cal = calibration_layer.calibrate(
                "correct_score",
                adjusted_raw,
                minute=state.minute,
                regime=regime_label,
                quality=state.feed_quality_score,
                market_probability=market_prob,
                side=score,
            )

            calibrated_probability = self._clamp(self._f(cal.calibrated_probability), 0.0001, 0.9999)

            proj = MarketProjection(
                market_key="CORRECT_SCORE",
                side=score,
                line=None,
                raw_probability=adjusted_raw,
                calibrated_probability=calibrated_probability,
                market_no_vig_probability=market_prob,
                edge=calibrated_probability - market_prob,
                expected_value=market_engine.expected_value(calibrated_probability, quote.odds_decimal),
                bookmaker=quote.bookmaker,
                odds_decimal=quote.odds_decimal,
                executable=quote.odds_decimal > 1.0,
                price_state="DEGRADE_MAIS_VIVANT",
                reasons=[
                    f"regime={regime_label}",
                    f"score={score}",
                    f"fair_prob={calibrated_probability:.3f}",
                    "distribution_top_score",
                ],
                payload={
                    "regime_label": regime_label,
                    "raw_probability_pre_haircut": raw_probability,
                    "raw_probability_post_haircut": adjusted_raw,
                    "market_probability": market_prob,
                },
            )

            proj = execution_layer.classify(proj)

            if not settings.correct_score_real_enabled:
                proj.real_status = "NO_BET"
                proj.top_bet_flag = False

            if proj.documentary_status != "DOC_STRONG":
                continue

            if doc_kept >= doc_cap:
                continue

            doc_kept += 1
            out.append(proj)

        return out
