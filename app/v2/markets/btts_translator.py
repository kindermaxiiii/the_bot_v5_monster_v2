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


class BTTSTranslator:
    def _group_quotes(self, quotes: Iterable[MarketQuote]) -> dict[str, dict[str, MarketQuote]]:
        grouped: dict[str, dict[str, MarketQuote]] = {}
        for quote in quotes:
            market_key = str(getattr(quote, "market_key", "") or "").upper()
            if market_key not in {"BTTS", "BTTS_FT"}:
                continue

            side = str(getattr(quote, "side", "") or "").upper()
            if side.startswith("BTTS_"):
                side = side.removeprefix("BTTS_")
            if side not in {"YES", "NO"}:
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

    def _pair_is_live_same_book(self, quote: MarketQuote, opposite_quote: MarketQuote | None) -> bool:
        if opposite_quote is None:
            return False
        if not self._quote_is_live(quote) or not self._quote_is_live(opposite_quote):
            return False
        return str(getattr(quote, "bookmaker", "") or "") == str(getattr(opposite_quote, "bookmaker", "") or "")

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

    def _price_state(self, quote: MarketQuote, opposite_quote: MarketQuote | None) -> str:
        if not self._quote_is_live(quote):
            return "MORT"
        if opposite_quote is None or not self._quote_is_live(opposite_quote):
            return "DEGRADE_MAIS_VIVANT"
        return "VIVANT"

    def _calibrated_probability(self, raw_probability: float, market_probability: float, uncertainty_score: float) -> float:
        blended = raw_probability if market_probability <= 0.0 else 0.85 * raw_probability + 0.15 * market_probability
        prudent_shrink = 1.0 - 0.10 * uncertainty_score
        return _clamp(blended * prudent_shrink, 0.0, 1.0)

    def _score_context(self, state: MatchState) -> dict[str, object]:
        home_scored = int(getattr(state, "home_goals", 0) or 0) >= 1
        away_scored = int(getattr(state, "away_goals", 0) or 0) >= 1
        both_scored = home_scored and away_scored

        if both_scored:
            silent_team = "NONE"
            missing_scorers = 0
        elif home_scored and not away_scored:
            silent_team = "AWAY"
            missing_scorers = 1
        elif away_scored and not home_scored:
            silent_team = "HOME"
            missing_scorers = 1
        else:
            silent_team = "BOTH"
            missing_scorers = 2

        return {
            "home_scored": home_scored,
            "away_scored": away_scored,
            "silent_team": silent_team,
            "teams_already_both_scored": both_scored,
            "btts_state_resolved": both_scored,
            "missing_scorers": missing_scorers,
        }

    def _silent_scoring_lambda(self, context: dict[str, object], probability_state: ProbabilityState) -> float:
        silent_team = str(context["silent_team"])
        if silent_team == "HOME":
            return float(probability_state.lambda_home_remaining)
        if silent_team == "AWAY":
            return float(probability_state.lambda_away_remaining)
        if silent_team == "BOTH":
            return float(probability_state.lambda_home_remaining + probability_state.lambda_away_remaining)
        return 0.0

    def _btts_yes_probability(self, probability_state: ProbabilityState) -> float:
        total = 0.0
        for score_key, mass in probability_state.ft_score_grid.items():
            home_final_text, away_final_text = score_key.split("-", maxsplit=1)
            if int(home_final_text) >= 1 and int(away_final_text) >= 1:
                total += float(mass)
        return _clamp(total, 0.0, 1.0)

    def translate(
        self,
        state: MatchState,
        intelligence: MatchIntelligenceSnapshot,
        probability_state: ProbabilityState,
    ) -> list[MarketProjectionV2]:
        grouped_quotes = self._group_quotes(getattr(state, "quotes", []) or [])
        if not grouped_quotes:
            return []

        context = self._score_context(state)
        teams_already_both_scored = bool(context["teams_already_both_scored"])
        silent_team = str(context["silent_team"])
        missing_scorers = int(context["missing_scorers"])
        silent_scoring_lambda = self._silent_scoring_lambda(context, probability_state)
        minute_share = intelligence.minute / 95.0

        raw_yes_probability = self._btts_yes_probability(probability_state)
        raw_no_probability = _clamp(1.0 - raw_yes_probability, 0.0, 1.0)

        projections: list[MarketProjectionV2] = []

        for bookmaker, pair in sorted(grouped_quotes.items(), key=lambda item: item[0]):
            for side, quote, opposite_quote, raw_probability in (
                ("YES", pair.get("YES"), pair.get("NO"), raw_yes_probability),
                ("NO", pair.get("NO"), pair.get("YES"), raw_no_probability),
            ):
                if quote is None:
                    continue

                price_state = self._price_state(quote, opposite_quote)
                pair_is_live_same_book = self._pair_is_live_same_book(quote, opposite_quote)

                btts_yes_already_won = side == "YES" and teams_already_both_scored
                btts_no_already_lost = side == "NO" and teams_already_both_scored

                if side == "YES":
                    favorable_resolution_distance = float(missing_scorers)
                    adverse_resolution_distance = 0.0 if teams_already_both_scored else 1.0
                    score_state_budget = missing_scorers
                    resolution_pressure = 0.0 if missing_scorers == 0 else missing_scorers / max(silent_scoring_lambda, 0.05)
                    state_fragility_score = _clamp(
                        0.50 * intelligence.score_state_fragility
                        + 0.30 * _clamp(resolution_pressure / 2.5, 0.0, 1.0)
                        + 0.20 * (1.0 - raw_probability),
                        0.0,
                        1.0,
                    )
                    late_fragility_score = _clamp(
                        minute_share * _clamp(missing_scorers / max(silent_scoring_lambda + 0.50, 0.50), 0.0, 1.25),
                        0.0,
                        1.0,
                    )
                    early_fragility_score = _clamp(
                        (1.0 - minute_share) * _clamp(missing_scorers / 2.0, 0.0, 1.0),
                        0.0,
                        1.0,
                    )
                else:
                    break_distance = 0 if teams_already_both_scored else missing_scorers
                    favorable_resolution_distance = 0.0
                    adverse_resolution_distance = float(break_distance)
                    score_state_budget = -1 if teams_already_both_scored else break_distance
                    resolution_pressure = 0.0 if teams_already_both_scored else silent_scoring_lambda / max(float(break_distance), 1.0)
                    state_fragility_score = _clamp(
                        0.50 * intelligence.score_state_fragility
                        + 0.30 * _clamp(resolution_pressure / 1.8, 0.0, 1.0)
                        + 0.20 * (1.0 - raw_probability),
                        0.0,
                        1.0,
                    )
                    late_fragility_score = _clamp(
                        minute_share * _clamp(silent_scoring_lambda / 1.5, 0.0, 1.0),
                        0.0,
                        1.0,
                    )
                    early_fragility_score = _clamp(
                        (1.0 - minute_share) * _clamp(float(break_distance) / 2.0, 0.0, 1.0),
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

                vetoes: list[str] = []
                if not self._quote_is_live(quote):
                    vetoes.append("quote_not_live")
                if not pair_is_live_same_book:
                    vetoes.append("pair_not_fully_live_same_book")
                if btts_yes_already_won:
                    vetoes.append("btts_yes_already_won_at_score")
                if btts_no_already_lost:
                    vetoes.append("btts_no_already_lost_at_score")

                executable = pair_is_live_same_book and not btts_yes_already_won and not btts_no_already_lost

                payload = {
                    "score": state.score_text,
                    "silent_team": silent_team,
                    "teams_already_both_scored": teams_already_both_scored,
                    "btts_state_resolved": bool(context["btts_state_resolved"]),
                    "score_state_budget": score_state_budget,
                    "missing_scorers": missing_scorers,
                    "remaining_goal_expectancy": probability_state.lambda_total_remaining,
                    "lambda_home_remaining": probability_state.lambda_home_remaining,
                    "lambda_away_remaining": probability_state.lambda_away_remaining,
                    "silent_scoring_lambda": silent_scoring_lambda,
                    "feed_quality": intelligence.feed_quality,
                    "market_quality": intelligence.market_quality,
                    "regime_label": intelligence.regime_label,
                    "regime_confidence": intelligence.regime_confidence,
                    "uncertainty_score": probability_state.uncertainty_score,
                }

                reasons = ["joint_ft_score_grid", f"bookmaker={bookmaker}"]
                if teams_already_both_scored:
                    reasons.append("teams_already_both_scored")
                elif silent_team == "BOTH":
                    reasons.append("two_teams_still_need_first_goal")
                else:
                    reasons.append(f"silent_team={silent_team}")

                if price_state == "VIVANT":
                    reasons.append("two_sided_live_pair")
                elif price_state == "DEGRADE_MAIS_VIVANT":
                    reasons.append("single_sided_or_degraded_pair")
                else:
                    reasons.append("quote_dead")

                projections.append(
                    MarketProjectionV2(
                        market_key="BTTS",
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
                        favorable_resolution_distance=favorable_resolution_distance,
                        adverse_resolution_distance=adverse_resolution_distance,
                        resolution_pressure=resolution_pressure,
                        state_fragility_score=state_fragility_score,
                        late_fragility_score=late_fragility_score,
                        early_fragility_score=early_fragility_score,
                        score_state_budget=score_state_budget,
                    )
                )

        return projections
