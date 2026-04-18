from __future__ import annotations

from math import ceil
from typing import Iterable

from app.core.match_state import MarketQuote, MatchState
from app.v2.contracts import MatchIntelligenceSnapshot, MarketProjectionV2, ProbabilityState


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


class OU1HTranslator:
    _market_aliases = {"OU_1H", "OU_HT"}

    def _group_quotes(self, quotes: Iterable[MarketQuote]) -> dict[tuple[str, float], dict[str, MarketQuote]]:
        grouped: dict[tuple[str, float], dict[str, MarketQuote]] = {}
        for quote in quotes:
            market_key = str(getattr(quote, "market_key", "") or "").upper()
            if market_key not in self._market_aliases:
                continue
            line = getattr(quote, "line", None)
            if line is None:
                continue
            bookmaker = str(getattr(quote, "bookmaker", "") or "")
            grouped.setdefault((bookmaker, float(line)), {})[str(getattr(quote, "side", "") or "").upper()] = quote
        return grouped

    def _quote_is_live(self, quote: MarketQuote | None) -> bool:
        if quote is None:
            return False
        raw = getattr(quote, "raw", {}) or {}
        if raw.get("is_blocked") or raw.get("is_stopped") or raw.get("is_finished"):
            return False
        return _safe_float(getattr(quote, "odds_decimal", None), 0.0) > 1.0

    def _pair_is_live_same_book(self, quote: MarketQuote, opposite_quote: MarketQuote | None) -> bool:
        if opposite_quote is None:
            return False
        if not self._quote_is_live(quote) or not self._quote_is_live(opposite_quote):
            return False
        return (
            str(getattr(quote, "bookmaker", "") or "") == str(getattr(opposite_quote, "bookmaker", "") or "")
            and getattr(quote, "line", None) == getattr(opposite_quote, "line", None)
        )

    def _price_state(self, quote: MarketQuote, opposite_quote: MarketQuote | None) -> str:
        if not self._quote_is_live(quote):
            return "MORT"
        if opposite_quote is None or not self._quote_is_live(opposite_quote):
            return "DEGRADE_MAIS_VIVANT"
        return "VIVANT"

    def _market_no_vig_probability(self, quote: MarketQuote, opposite_quote: MarketQuote | None) -> float:
        odds = _safe_float(getattr(quote, "odds_decimal", None), 0.0)
        if odds <= 1.0:
            return 0.0

        implied = 1.0 / odds
        if opposite_quote is None:
            return _clamp(implied, 0.0, 1.0)

        opposite_odds = _safe_float(getattr(opposite_quote, "odds_decimal", None), 0.0)
        if opposite_odds <= 1.0:
            return _clamp(implied, 0.0, 1.0)

        opposite_implied = 1.0 / opposite_odds
        denom = implied + opposite_implied
        if denom <= 0.0:
            return _clamp(implied, 0.0, 1.0)
        return _clamp(implied / denom, 0.0, 1.0)

    def _calibrated_probability(self, raw_probability: float, market_probability: float, uncertainty_score: float) -> float:
        blended = raw_probability if market_probability <= 0.0 else 0.85 * raw_probability + 0.15 * market_probability
        prudent_shrink = 1.0 - 0.10 * uncertainty_score
        return _clamp(blended * prudent_shrink, 0.0, 1.0)

    def _allowed_additional_goals_before_under_loss(self, line: float, current_total: int) -> int:
        return int(ceil(float(line) - float(current_total)) - 1)

    def _minimum_additional_goals_for_over(self, line: float, current_total: int) -> int:
        return max(0, int(ceil(float(line) - float(current_total))))

    def _window_mode(self, state: MatchState) -> str:
        phase = str(getattr(state, "phase", "") or "").upper()
        status = str(getattr(state, "status", "") or "").upper()
        minute = int(getattr(state, "minute", 0) or 0)

        if phase == "1H" or status == "1H":
            return "LIVE_1H"
        if phase == "HT" or status == "HT":
            return "HT_DOC"
        if minute > 0 and minute < 45 and status not in {"2H", "FT", "AET", "PEN"}:
            return "LIVE_1H"
        return "CLOSED"

    def translate(
        self,
        state: MatchState,
        intelligence: MatchIntelligenceSnapshot,
        probability_state: ProbabilityState,
    ) -> list[MarketProjectionV2]:
        window_mode = self._window_mode(state)
        if window_mode == "CLOSED":
            return []

        grouped_quotes = self._group_quotes(getattr(state, "quotes", []) or [])
        if not grouped_quotes:
            return []

        current_total = int(getattr(state, "total_goals", 0))
        lambda_ht_total = float(probability_state.lambda_ht_total_remaining)
        projections: list[MarketProjectionV2] = []

        for (bookmaker, line), pair in sorted(grouped_quotes.items(), key=lambda item: (item[0][1], item[0][0])):
            for side, quote, opposite_quote in (
                ("OVER", pair.get("OVER"), pair.get("UNDER")),
                ("UNDER", pair.get("UNDER"), pair.get("OVER")),
            ):
                if quote is None:
                    continue

                goals_needed = self._minimum_additional_goals_for_over(line, current_total)
                goal_budget = self._allowed_additional_goals_before_under_loss(line, current_total)
                over_already_won = side == "OVER" and current_total > float(line)
                under_already_lost = side == "UNDER" and current_total > float(line)

                if side == "OVER":
                    raw_probability = _clamp(
                        sum(
                            prob
                            for final_total, prob in probability_state.ht_final_total_goal_probs.items()
                            if float(final_total) > float(line)
                        ),
                        0.0,
                        1.0,
                    )
                    favorable_distance = float(goals_needed)
                    adverse_distance = 0.0
                    score_state_budget = goals_needed
                    resolution_pressure = goals_needed / max(lambda_ht_total, 0.05)
                    state_fragility = _clamp(
                        0.55 * intelligence.score_state_fragility
                        + 0.45 * _clamp(favorable_distance / max(lambda_ht_total, 0.25), 0.0, 2.0),
                        0.0,
                        1.0,
                    )
                    late_fragility = _clamp(
                        (intelligence.minute / 45.0) * _clamp(favorable_distance / 1.75, 0.0, 1.0),
                        0.0,
                        1.0,
                    )
                    early_fragility = _clamp(
                        (1.0 - intelligence.minute / 45.0) * _clamp(favorable_distance / 2.25, 0.0, 1.0),
                        0.0,
                        1.0,
                    )
                else:
                    raw_probability = _clamp(
                        sum(
                            prob
                            for final_total, prob in probability_state.ht_final_total_goal_probs.items()
                            if float(final_total) < float(line)
                        ),
                        0.0,
                        1.0,
                    )
                    favorable_distance = 0.0
                    adverse_distance = float(max(0, goal_budget + 1))
                    score_state_budget = goal_budget
                    resolution_pressure = lambda_ht_total / max(goal_budget + 1, 1)
                    state_fragility = _clamp(
                        0.55 * intelligence.score_state_fragility
                        + 0.45 * _clamp(resolution_pressure / 1.75, 0.0, 1.0),
                        0.0,
                        1.0,
                    )
                    late_fragility = _clamp(
                        (intelligence.minute / 45.0) * _clamp(1.0 / max(goal_budget + 1, 1), 0.0, 1.0),
                        0.0,
                        1.0,
                    )
                    early_fragility = _clamp(
                        (1.0 - intelligence.minute / 45.0) * _clamp(2.0 / max(goal_budget + 1, 1), 0.0, 1.0),
                        0.0,
                        1.0,
                    )

                market_no_vig_probability = self._market_no_vig_probability(quote, opposite_quote)
                calibrated_probability = self._calibrated_probability(
                    raw_probability=raw_probability,
                    market_probability=market_no_vig_probability,
                    uncertainty_score=probability_state.uncertainty_score,
                )
                odds_decimal = _safe_float(getattr(quote, "odds_decimal", None), 0.0)
                expected_value = calibrated_probability * odds_decimal - 1.0 if odds_decimal > 1.0 else 0.0
                price_state = self._price_state(quote, opposite_quote)
                pair_is_live_same_book = self._pair_is_live_same_book(quote, opposite_quote)

                vetoes: list[str] = []
                if window_mode != "LIVE_1H":
                    vetoes.append("ou_1h_not_in_first_half_window")
                if not self._quote_is_live(quote):
                    vetoes.append("quote_not_live")
                if not pair_is_live_same_book:
                    vetoes.append("pair_not_fully_live_same_book")
                if side == "UNDER" and under_already_lost:
                    vetoes.append("under_already_lost_at_score")
                if side == "OVER" and over_already_won:
                    vetoes.append("over_already_won_at_score")

                executable = window_mode == "LIVE_1H" and pair_is_live_same_book and not over_already_won and not under_already_lost

                payload = {
                    "score": state.score_text,
                    "window_mode": window_mode,
                    "current_total": current_total,
                    "lambda_ht_total_remaining": lambda_ht_total,
                    "lambda_ht_home_remaining": probability_state.lambda_ht_home_remaining,
                    "lambda_ht_away_remaining": probability_state.lambda_ht_away_remaining,
                    "ht_remaining_added_goal_probs": probability_state.ht_remaining_added_goal_probs,
                    "ht_final_total_goal_probs": probability_state.ht_final_total_goal_probs,
                    "goals_needed_for_over": goals_needed,
                    "goal_budget_under": goal_budget,
                    "feed_quality": intelligence.feed_quality,
                    "market_quality": intelligence.market_quality,
                    "regime_label": intelligence.regime_label,
                    "regime_confidence": intelligence.regime_confidence,
                    "uncertainty_score": probability_state.uncertainty_score,
                }

                reasons = ["first_half_horizon_core", f"bookmaker={bookmaker}", f"window_mode={window_mode}"]
                if price_state == "VIVANT":
                    reasons.append("two_sided_live_pair")
                elif price_state == "DEGRADE_MAIS_VIVANT":
                    reasons.append("single_sided_or_degraded_pair")
                else:
                    reasons.append("quote_dead")

                projections.append(
                    MarketProjectionV2(
                        market_key="OU_1H",
                        side=side,
                        line=float(line),
                        bookmaker=bookmaker,
                        odds_decimal=odds_decimal if odds_decimal > 0.0 else None,
                        raw_probability=raw_probability,
                        calibrated_probability=calibrated_probability,
                        market_no_vig_probability=market_no_vig_probability,
                        edge=calibrated_probability - market_no_vig_probability,
                        expected_value=expected_value,
                        executable=executable,
                        price_state=price_state,
                        payload=payload,
                        reasons=reasons,
                        vetoes=vetoes,
                        favorable_resolution_distance=favorable_distance,
                        adverse_resolution_distance=adverse_distance,
                        resolution_pressure=resolution_pressure,
                        state_fragility_score=state_fragility,
                        late_fragility_score=late_fragility,
                        early_fragility_score=early_fragility,
                        score_state_budget=score_state_budget,
                    )
                )

        return projections
