from __future__ import annotations

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


class ResultTranslator:
    _market_aliases = {"RESULT", "RESULT_FT", "ML", "ML_FT", "MONEYLINE", "MONEYLINE_FT", "1X2", "1X2_FT"}
    _side_aliases = {
        "HOME": "HOME",
        "1": "HOME",
        "DRAW": "DRAW",
        "X": "DRAW",
        "AWAY": "AWAY",
        "2": "AWAY",
    }

    def _group_quotes(self, quotes: Iterable[MarketQuote]) -> dict[str, dict[str, MarketQuote]]:
        grouped: dict[str, dict[str, MarketQuote]] = {}
        for quote in quotes:
            market_key = str(getattr(quote, "market_key", "") or "").upper()
            if market_key not in self._market_aliases:
                continue

            raw_side = str(getattr(quote, "side", "") or "").upper()
            side = self._side_aliases.get(raw_side)
            if side is None:
                continue

            bookmaker = str(getattr(quote, "bookmaker", "") or "")
            grouped.setdefault(bookmaker, {})[side] = quote
        return grouped

    def _quote_is_live(self, quote: MarketQuote | None) -> bool:
        if quote is None:
            return False
        raw = getattr(quote, "raw", {}) or {}
        if raw.get("is_blocked") or raw.get("is_stopped") or raw.get("is_finished"):
            return False
        return _safe_float(getattr(quote, "odds_decimal", None), 0.0) > 1.0

    def _triplet_is_live_same_book(self, bookmaker: str, triplet: dict[str, MarketQuote]) -> bool:
        home_quote = triplet.get("HOME")
        draw_quote = triplet.get("DRAW")
        away_quote = triplet.get("AWAY")
        if not all((home_quote, draw_quote, away_quote)):
            return False
        if not all(self._quote_is_live(quote) for quote in (home_quote, draw_quote, away_quote)):
            return False
        return all(str(getattr(quote, "bookmaker", "") or "") == bookmaker for quote in (home_quote, draw_quote, away_quote))

    def _price_state(self, quote: MarketQuote, bookmaker: str, triplet: dict[str, MarketQuote]) -> str:
        if not self._quote_is_live(quote):
            return "MORT"
        if not self._triplet_is_live_same_book(bookmaker, triplet):
            return "DEGRADE_MAIS_VIVANT"
        return "VIVANT"

    def _market_no_vig_probability(self, side: str, triplet: dict[str, MarketQuote]) -> float:
        selected = triplet.get(side)
        if selected is None:
            return 0.0

        implieds: dict[str, float] = {}
        for key in ("HOME", "DRAW", "AWAY"):
            quote = triplet.get(key)
            odds = _safe_float(getattr(quote, "odds_decimal", None), 0.0) if quote is not None else 0.0
            if odds > 1.0:
                implieds[key] = 1.0 / odds

        if side not in implieds:
            return 0.0

        denom = sum(implieds.values())
        if denom <= 0.0:
            return _clamp(implieds[side], 0.0, 1.0)
        return _clamp(implieds[side] / denom, 0.0, 1.0)

    def _calibrated_probability(self, raw_probability: float, market_probability: float, uncertainty_score: float) -> float:
        blended = raw_probability if market_probability <= 0.0 else 0.85 * raw_probability + 0.15 * market_probability
        prudent_shrink = 1.0 - 0.10 * uncertainty_score
        return _clamp(blended * prudent_shrink, 0.0, 1.0)

    def _result_probabilities(self, probability_state: ProbabilityState) -> dict[str, float]:
        out = {"HOME": 0.0, "DRAW": 0.0, "AWAY": 0.0}
        for score_key, mass in probability_state.ft_score_grid.items():
            home_final_text, away_final_text = score_key.split("-", maxsplit=1)
            home_final = int(home_final_text)
            away_final = int(away_final_text)
            if home_final > away_final:
                out["HOME"] += float(mass)
            elif home_final < away_final:
                out["AWAY"] += float(mass)
            else:
                out["DRAW"] += float(mass)
        total = sum(out.values()) or 1.0
        return {key: value / total for key, value in out.items()}

    def _current_result_side(self, state: MatchState) -> str:
        goal_diff = int(getattr(state, "home_goals", 0)) - int(getattr(state, "away_goals", 0))
        if goal_diff > 0:
            return "HOME"
        if goal_diff < 0:
            return "AWAY"
        return "DRAW"

    def _result_metrics(
        self,
        *,
        state: MatchState,
        side: str,
        raw_probability: float,
        intelligence: MatchIntelligenceSnapshot,
        probability_state: ProbabilityState,
    ) -> tuple[float, float, float, float, float, float, int]:
        minute_share = intelligence.minute / 95.0
        goal_diff = int(getattr(state, "home_goals", 0)) - int(getattr(state, "away_goals", 0))
        home_lambda = float(probability_state.lambda_home_remaining)
        away_lambda = float(probability_state.lambda_away_remaining)
        total_lambda = float(probability_state.lambda_total_remaining)

        if side == "HOME":
            signed_margin = goal_diff
            team_lambda = home_lambda
            opponent_lambda = away_lambda
            favorable_distance = 0.0 if signed_margin > 0 else 1.0 if signed_margin == 0 else float(abs(signed_margin) + 1)
            adverse_distance = float(max(0, signed_margin))
            score_state_budget = max(0, signed_margin - 1) if signed_margin > 0 else int(favorable_distance)
            if signed_margin > 0:
                resolution_pressure = opponent_lambda / max(adverse_distance, 1.0)
            else:
                resolution_pressure = favorable_distance / max(team_lambda, 0.05)
            late_fragility = (
                minute_share * _clamp(opponent_lambda / max(adverse_distance + 0.5, 0.5), 0.0, 1.25)
                if signed_margin > 0
                else minute_share * _clamp(favorable_distance / max(team_lambda + 0.50, 0.50), 0.0, 1.25)
            )
            early_fragility = (
                (1.0 - minute_share) * _clamp(max(adverse_distance, 1.0) / 2.0, 0.0, 1.0)
                if signed_margin > 0
                else (1.0 - minute_share) * _clamp(favorable_distance / 2.5, 0.0, 1.0)
            )
        elif side == "AWAY":
            signed_margin = -goal_diff
            team_lambda = away_lambda
            opponent_lambda = home_lambda
            favorable_distance = 0.0 if signed_margin > 0 else 1.0 if signed_margin == 0 else float(abs(signed_margin) + 1)
            adverse_distance = float(max(0, signed_margin))
            score_state_budget = max(0, signed_margin - 1) if signed_margin > 0 else int(favorable_distance)
            if signed_margin > 0:
                resolution_pressure = opponent_lambda / max(adverse_distance, 1.0)
            else:
                resolution_pressure = favorable_distance / max(team_lambda, 0.05)
            late_fragility = (
                minute_share * _clamp(opponent_lambda / max(adverse_distance + 0.5, 0.5), 0.0, 1.25)
                if signed_margin > 0
                else minute_share * _clamp(favorable_distance / max(team_lambda + 0.50, 0.50), 0.0, 1.25)
            )
            early_fragility = (
                (1.0 - minute_share) * _clamp(max(adverse_distance, 1.0) / 2.0, 0.0, 1.0)
                if signed_margin > 0
                else (1.0 - minute_share) * _clamp(favorable_distance / 2.5, 0.0, 1.0)
            )
        else:
            goal_gap = abs(goal_diff)
            favorable_distance = float(goal_gap)
            adverse_distance = 1.0 if goal_gap == 0 else 0.0
            score_state_budget = 0 if goal_gap == 0 else goal_gap
            if goal_gap == 0:
                resolution_pressure = total_lambda
                late_fragility = minute_share * _clamp(total_lambda / 1.5, 0.0, 1.0)
                early_fragility = (1.0 - minute_share) * 0.35
            else:
                resolution_pressure = favorable_distance / max(total_lambda, 0.05)
                late_fragility = minute_share * _clamp(favorable_distance / max(total_lambda + 0.50, 0.50), 0.0, 1.25)
                early_fragility = (1.0 - minute_share) * _clamp(favorable_distance / 2.5, 0.0, 1.0)

        state_fragility = _clamp(
            0.50 * intelligence.score_state_fragility
            + 0.30 * _clamp(resolution_pressure / 2.2, 0.0, 1.0)
            + 0.20 * (1.0 - raw_probability),
            0.0,
            1.0,
        )
        return (
            favorable_distance,
            adverse_distance,
            resolution_pressure,
            state_fragility,
            _clamp(late_fragility, 0.0, 1.0),
            _clamp(early_fragility, 0.0, 1.0),
            int(score_state_budget),
        )

    def translate(
        self,
        state: MatchState,
        intelligence: MatchIntelligenceSnapshot,
        probability_state: ProbabilityState,
    ) -> list[MarketProjectionV2]:
        grouped_quotes = self._group_quotes(getattr(state, "quotes", []) or [])
        if not grouped_quotes:
            return []

        current_result_side = self._current_result_side(state)
        result_probabilities = self._result_probabilities(probability_state)
        projections: list[MarketProjectionV2] = []

        for bookmaker, triplet in sorted(grouped_quotes.items(), key=lambda item: item[0]):
            for side in ("HOME", "DRAW", "AWAY"):
                quote = triplet.get(side)
                if quote is None:
                    continue

                raw_probability = result_probabilities[side]
                price_state = self._price_state(quote, bookmaker, triplet)
                triplet_is_live = self._triplet_is_live_same_book(bookmaker, triplet)

                result_home_already_won = side == "HOME" and current_result_side == "HOME"
                result_draw_already_won = side == "DRAW" and current_result_side == "DRAW"
                result_away_already_won = side == "AWAY" and current_result_side == "AWAY"

                (
                    favorable_distance,
                    adverse_distance,
                    resolution_pressure,
                    state_fragility_score,
                    late_fragility_score,
                    early_fragility_score,
                    score_state_budget,
                ) = self._result_metrics(
                    state=state,
                    side=side,
                    raw_probability=raw_probability,
                    intelligence=intelligence,
                    probability_state=probability_state,
                )

                market_no_vig_probability = self._market_no_vig_probability(side, triplet)
                calibrated_probability = self._calibrated_probability(
                    raw_probability=raw_probability,
                    market_probability=market_no_vig_probability,
                    uncertainty_score=probability_state.uncertainty_score,
                )
                odds_decimal = _safe_float(getattr(quote, "odds_decimal", None), 0.0)
                expected_value = calibrated_probability * odds_decimal - 1.0 if odds_decimal > 1.0 else 0.0

                vetoes: list[str] = []
                if not self._quote_is_live(quote):
                    vetoes.append("quote_not_live")
                if not triplet_is_live:
                    vetoes.append("pair_or_triplet_not_fully_live_same_book")
                if result_home_already_won:
                    vetoes.append("result_home_already_won_at_score")
                if result_draw_already_won:
                    vetoes.append("result_draw_already_won_at_score")
                if result_away_already_won:
                    vetoes.append("result_away_already_won_at_score")

                executable = triplet_is_live and not (result_home_already_won or result_draw_already_won or result_away_already_won)

                payload = {
                    "score": state.score_text,
                    "current_result_side": current_result_side,
                    "current_goal_diff": int(getattr(state, "home_goals", 0)) - int(getattr(state, "away_goals", 0)),
                    "remaining_goal_expectancy": probability_state.lambda_total_remaining,
                    "lambda_home_remaining": probability_state.lambda_home_remaining,
                    "lambda_away_remaining": probability_state.lambda_away_remaining,
                    "feed_quality": intelligence.feed_quality,
                    "market_quality": intelligence.market_quality,
                    "regime_label": intelligence.regime_label,
                    "regime_confidence": intelligence.regime_confidence,
                    "uncertainty_score": probability_state.uncertainty_score,
                }

                reasons = ["joint_ft_score_grid", f"bookmaker={bookmaker}", f"current_result_side={current_result_side}"]
                if price_state == "VIVANT":
                    reasons.append("three_way_live_triplet")
                elif price_state == "DEGRADE_MAIS_VIVANT":
                    reasons.append("triplet_incomplete_or_degraded")
                else:
                    reasons.append("quote_dead")

                projections.append(
                    MarketProjectionV2(
                        market_key="RESULT",
                        side=side,
                        line=None,
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
                        state_fragility_score=state_fragility_score,
                        late_fragility_score=late_fragility_score,
                        early_fragility_score=early_fragility_score,
                        score_state_budget=score_state_budget,
                    )
                )

        return projections
