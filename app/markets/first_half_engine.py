from __future__ import annotations

from app.config import settings
from app.core.contracts import MarketProjection, ScorelineDistribution
from app.core.match_state import MatchState
from app.markets.common import calibration_layer, execution_layer, market_engine


class FirstHalfEngine:
    lines = [0.5, 1.5, 2.5]

    def evaluate(self, state: MatchState, distribution: ScorelineDistribution, regime_label: str) -> list[MarketProjection]:
        if not settings.first_half_enabled:
            return []
        if state.phase not in {"1H", "HT", ""} and (state.minute or 0) > 45:
            return []
        time_left = max(0, 45 - int(state.minute or 0))
        if time_left <= 0:
            return []
        quotes = market_engine.quotes_for(state, "ou", "1H")
        out: list[MarketProjection] = []
        current_total = state.total_goals
        for line in self.lines:
            pair = market_engine.pair_two_way(quotes, {"over"}, {"under"}, line=line)
            if not pair:
                continue
            prob_under = sum(p for score, p in distribution.final_score_probs.items() if sum(int(x) for x in score.split("-")) < max(line, current_total + 0.01))
            prob_over = 1.0 - prob_under
            for side, raw_probability, market_prob, quote in [
                ("UNDER", prob_under, pair["negative_no_vig"], pair["negative_quote"]),
                ("OVER", prob_over, pair["positive_no_vig"], pair["positive_quote"]),
            ]:
                cal = calibration_layer.calibrate("ou_1h", raw_probability, minute=state.minute, regime=regime_label, quality=state.feed_quality_score)
                proj = MarketProjection(
                    market_key="ou_1h", side=side, line=line,
                    raw_probability=raw_probability,
                    calibrated_probability=cal.calibrated_probability,
                    market_no_vig_probability=market_prob,
                    edge=cal.calibrated_probability - market_prob,
                    expected_value=market_engine.expected_value(cal.calibrated_probability, quote.odds_decimal),
                    bookmaker=quote.bookmaker, odds_decimal=quote.odds_decimal,
                    executable=pair["price_state"] != "MORT", price_state=pair["price_state"],
                    reasons=[regime_label, "1h_projection", f"time_left={time_left}"], payload={"regime_label": regime_label},
                )
                out.append(execution_layer.classify(proj))
        return out
