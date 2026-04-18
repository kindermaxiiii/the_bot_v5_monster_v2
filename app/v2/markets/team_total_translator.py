from __future__ import annotations

from math import ceil
from typing import Callable, Iterable

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


class TeamTotalTranslator:
    _side_map = {
        "HOME_OVER": ("HOME", "OVER"),
        "HOME_UNDER": ("HOME", "UNDER"),
        "AWAY_OVER": ("AWAY", "OVER"),
        "AWAY_UNDER": ("AWAY", "UNDER"),
    }

    def _group_quotes(self, quotes: Iterable[MarketQuote]) -> dict[tuple[str, str, float], dict[str, MarketQuote]]:
        grouped: dict[tuple[str, str, float], dict[str, MarketQuote]] = {}
        for quote in quotes:
            market_key = str(getattr(quote, "market_key", "") or "").upper()
            if market_key not in {"TEAM_TOTAL", "TEAM_TOTAL_FT"}:
                continue

            line = getattr(quote, "line", None)
            if line is None:
                continue

            side = str(getattr(quote, "side", "") or "").upper()
            if side not in self._side_map:
                continue

            team_side, direction = self._side_map[side]
            bookmaker = str(getattr(quote, "bookmaker", "") or "")
            grouped.setdefault((bookmaker, team_side, float(line)), {})[direction] = quote
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

    def _team_goal_probability(self, probabilities: dict[int, float], predicate: Callable[[int], bool]) -> float:
        return _clamp(sum(prob for final_goals, prob in probabilities.items() if predicate(int(final_goals))), 0.0, 1.0)

    def _allowed_additional_goals_before_under_loss(self, line: float, current_goals: int) -> int:
        return int(ceil(float(line) - float(current_goals)) - 1)

    def _minimum_additional_goals_for_over(self, line: float, current_goals: int) -> int:
        return max(0, int(ceil(float(line) - float(current_goals))))

    def translate(
        self,
        state: MatchState,
        intelligence: MatchIntelligenceSnapshot,
        probability_state: ProbabilityState,
    ) -> list[MarketProjectionV2]:
        grouped_quotes = self._group_quotes(getattr(state, "quotes", []) or [])
        if not grouped_quotes:
            return []

        minute_share = intelligence.minute / 95.0
        projections: list[MarketProjectionV2] = []

        for (bookmaker, team_side, line), pair in sorted(grouped_quotes.items(), key=lambda item: (item[0][1], item[0][2], item[0][0])):
            if team_side == "HOME":
                current_team_goals = int(getattr(state, "home_goals", 0))
                team_goal_probs = probability_state.home_goal_probs
                lambda_team_remaining = probability_state.lambda_home_remaining
            else:
                current_team_goals = int(getattr(state, "away_goals", 0))
                team_goal_probs = probability_state.away_goal_probs
                lambda_team_remaining = probability_state.lambda_away_remaining

            for direction, quote, opposite_quote in (
                ("OVER", pair.get("OVER"), pair.get("UNDER")),
                ("UNDER", pair.get("UNDER"), pair.get("OVER")),
            ):
                if quote is None:
                    continue

                goals_needed = self._minimum_additional_goals_for_over(line, current_team_goals)
                goal_budget = self._allowed_additional_goals_before_under_loss(line, current_team_goals)
                over_already_won = direction == "OVER" and current_team_goals > float(line)
                under_already_lost = direction == "UNDER" and current_team_goals > float(line)

                if direction == "OVER":
                    raw_probability = self._team_goal_probability(
                        team_goal_probs,
                        lambda final_goals: float(final_goals) > float(line),
                    )
                    favorable_resolution_distance = float(goals_needed)
                    adverse_resolution_distance = 0.0
                    score_state_budget = goals_needed
                    resolution_pressure = goals_needed / max(lambda_team_remaining, 0.05)
                    state_fragility_score = _clamp(
                        0.55 * intelligence.score_state_fragility
                        + 0.45 * _clamp(favorable_resolution_distance / max(lambda_team_remaining, 0.25), 0.0, 2.0),
                        0.0,
                        1.0,
                    )
                    late_fragility_score = _clamp(
                        minute_share * _clamp(favorable_resolution_distance / 2.0, 0.0, 1.0),
                        0.0,
                        1.0,
                    )
                    early_fragility_score = _clamp(
                        (1.0 - minute_share) * _clamp(favorable_resolution_distance / 2.5, 0.0, 1.0),
                        0.0,
                        1.0,
                    )
                else:
                    raw_probability = self._team_goal_probability(
                        team_goal_probs,
                        lambda final_goals: float(final_goals) < float(line),
                    )
                    favorable_resolution_distance = 0.0
                    adverse_resolution_distance = float(max(0, goal_budget + 1))
                    score_state_budget = goal_budget
                    resolution_pressure = lambda_team_remaining / max(goal_budget + 1, 1)
                    state_fragility_score = _clamp(
                        0.55 * intelligence.score_state_fragility
                        + 0.45 * _clamp(resolution_pressure / 2.0, 0.0, 1.0),
                        0.0,
                        1.0,
                    )
                    late_fragility_score = _clamp(
                        minute_share * _clamp(1.0 / max(goal_budget + 1, 1), 0.0, 1.0),
                        0.0,
                        1.0,
                    )
                    early_fragility_score = _clamp(
                        (1.0 - minute_share) * _clamp(2.0 / max(goal_budget + 1, 1), 0.0, 1.0),
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
                if not self._quote_is_live(quote):
                    vetoes.append("quote_not_live")
                if not pair_is_live_same_book:
                    vetoes.append("pair_not_fully_live_same_book")
                if direction == "OVER" and over_already_won:
                    vetoes.append("team_total_over_already_won_at_score")
                if direction == "UNDER" and under_already_lost:
                    vetoes.append("team_total_under_already_lost_at_score")

                executable = pair_is_live_same_book and not over_already_won and not under_already_lost

                payload = {
                    "score": state.score_text,
                    "focus_team": team_side,
                    "current_team_goals": current_team_goals,
                    "lambda_team_remaining": lambda_team_remaining,
                    "lambda_home_remaining": probability_state.lambda_home_remaining,
                    "lambda_away_remaining": probability_state.lambda_away_remaining,
                    "team_goals_needed_for_over": goals_needed,
                    "team_goal_budget_under": goal_budget,
                    "feed_quality": intelligence.feed_quality,
                    "market_quality": intelligence.market_quality,
                    "regime_label": intelligence.regime_label,
                    "regime_confidence": intelligence.regime_confidence,
                    "uncertainty_score": probability_state.uncertainty_score,
                }

                reasons = ["team_marginal_probability", f"bookmaker={bookmaker}", f"focus_team={team_side}"]
                if price_state == "VIVANT":
                    reasons.append("two_sided_live_pair")
                elif price_state == "DEGRADE_MAIS_VIVANT":
                    reasons.append("single_sided_or_degraded_pair")
                else:
                    reasons.append("quote_dead")

                projections.append(
                    MarketProjectionV2(
                        market_key="TEAM_TOTAL",
                        side=f"{team_side}_{direction}",
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
