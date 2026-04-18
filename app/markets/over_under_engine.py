from __future__ import annotations

from math import ceil, isfinite
from typing import Iterable

from app.config import settings
from app.core.contracts import MarketProjection, ScorelineDistribution
from app.core.match_state import MatchState
from app.markets.common import calibration_layer, execution_layer, market_engine
from app.utils.math_tools import poisson_cdf, poisson_sf


class OverUnderEngine:
    """
    O/U ENGINE — version sélective avec exécution same-book stricte.

    Philosophie :
    - under : rester dur, surtout sur les lignes serrées tardives
    - over : rouvrir les profils propres, surtout quand un seul but suffit
    - analyse cross-book autorisée
    - exécution réelle = same-book seulement
    """

    default_lines = [0.5, 1.5, 2.5, 3.5, 4.5, 5.5]

    # ------------------------------------------------------------------
    # Basic helpers
    # ------------------------------------------------------------------
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

    def _i(self, value, default: int = 0) -> int:
        try:
            if value is None:
                return default
            return int(value)
        except (TypeError, ValueError):
            return default

    def _clamp(self, x: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, x))

    def _side_text(self, side: str) -> str:
        return (side or "").upper().strip()

    def _is_under_side(self, side: str) -> bool:
        return "UNDER" in self._side_text(side)

    def _is_over_side(self, side: str) -> bool:
        return "OVER" in self._side_text(side)

    def _current_total(self, state: MatchState) -> int:
        return self._i(getattr(state, "home_goals", 0), 0) + self._i(getattr(state, "away_goals", 0), 0)

    def _available_lines(self, quotes: Iterable) -> list[float]:
        found = set(self.default_lines)
        for q in quotes:
            try:
                if getattr(q, "line", None) is not None:
                    found.add(float(getattr(q, "line")))
            except (TypeError, ValueError):
                continue
        return sorted(x for x in found if x >= 0.5)

    def _distribution_totals(self, distribution: ScorelineDistribution) -> dict[int, float]:
        totals: dict[int, float] = {}
        for score, prob in (distribution.final_score_probs or {}).items():
            try:
                a, b = score.split("-")
                total = int(a) + int(b)
            except Exception:
                total = 0
            totals[total] = totals.get(total, 0.0) + self._f(prob)

        total_mass = sum(totals.values()) or 1.0
        return {k: v / total_mass for k, v in totals.items()}

    def _remaining_total_probs(self, distribution: ScorelineDistribution) -> dict[int, float]:
        probs = getattr(distribution, "remaining_total_goal_probs", None) or {}
        if not probs:
            return {}
        total_mass = sum(self._f(v) for v in probs.values()) or 1.0
        return {int(k): self._f(v) / total_mass for k, v in probs.items()}

    def _under_prob(self, totals: dict[int, float], line: float) -> float:
        return sum(p for total, p in totals.items() if total < line)

    def _fair_odds(self, p: float) -> float | None:
        return None if p <= 0.0 else 1.0 / p

    def _goals_needed_for_over(self, line: float, current_total: int) -> float:
        return max(0.0, line - current_total)

    def _breathing_room_under(self, line: float, current_total: int) -> float:
        return max(0.0, line - current_total)

    def _allowed_additional_goals_before_loss(self, line: float, current_total: int) -> int:
        return int(ceil(float(line) - float(current_total)) - 1)

    def _minimum_additional_goals_for_over(self, line: float, current_total: int) -> int:
        return max(0, int(ceil(float(line) - float(current_total))))

    def _under_survival_probability(self, remaining_total_probs: dict[int, float], goal_budget: int | None) -> float:
        if goal_budget is None:
            return 0.0
        return self._clamp(
            sum(self._f(prob) for total, prob in (remaining_total_probs or {}).items() if total <= goal_budget),
            0.0,
            1.0,
        )

    def _over_hit_probability(self, remaining_total_probs: dict[int, float], goals_required: int) -> float:
        if goals_required <= 0:
            return 1.0
        return self._clamp(
            sum(self._f(prob) for total, prob in (remaining_total_probs or {}).items() if total >= goals_required),
            0.0,
            1.0,
        )

    def _poisson_anchor_probability(
        self,
        side: str,
        lambda_total_remaining: float,
        goal_budget: int | None,
        goals_required: int,
    ) -> float:
        lam = max(0.0, self._f(lambda_total_remaining))
        if self._is_under_side(side):
            if goal_budget is None:
                return 0.0
            return self._clamp(poisson_cdf(int(goal_budget), lam), 0.0, 1.0)
        if goals_required <= 0:
            return 1.0
        return self._clamp(poisson_sf(int(goals_required) - 1, lam), 0.0, 1.0)

    def _remaining_goal_expectancy(self, totals: dict[int, float], current_total: int) -> float:
        expected_final_total = sum(float(total) * self._f(prob) for total, prob in (totals or {}).items())
        return max(0.0, expected_final_total - float(current_total))

    def _remaining_side_goal_expectancies(self, distribution: ScorelineDistribution, state: MatchState) -> tuple[float, float]:
        home_now = self._i(getattr(state, "home_goals", 0), 0)
        away_now = self._i(getattr(state, "away_goals", 0), 0)

        expected_home_final = sum(float(goals) * self._f(prob) for goals, prob in (distribution.home_goal_probs or {}).items())
        expected_away_final = sum(float(goals) * self._f(prob) for goals, prob in (distribution.away_goal_probs or {}).items())

        return (
            max(0.0, expected_home_final - float(home_now)),
            max(0.0, expected_away_final - float(away_now)),
        )

    # ------------------------------------------------------------------
    # Feature helpers
    # ------------------------------------------------------------------
    def _feature(self, feature_map: dict | None, key: str, default: float = 0.0) -> float:
        return self._f((feature_map or {}).get(key), default)

    def _quality(self, state: MatchState) -> float:
        return self._f(getattr(state, "feed_quality_score", 0.58), 0.58)

    # ------------------------------------------------------------------
    # Candidate gates
    # ------------------------------------------------------------------
    def _candidate_gate(self, side: str, line: float, current_total: int, minute: int, regime_label: str) -> bool:
        regime = (regime_label or "").upper()

        if self._is_under_side(side):
            if current_total >= line:
                return False

            if line <= 0.5 and minute < 68:
                return False
            if line <= 1.5 and minute < 48:
                return False

            if line <= 2.5:
                if current_total >= 2 and minute < 56:
                    return False
                if current_total <= 1 and minute < 34:
                    return False

            if regime in {"OPEN_EXCHANGE", "CHAOTIC_TRANSITIONS"}:
                return False

            return True

        if self._is_over_side(side):
            goals_needed = self._goals_needed_for_over(line, current_total)

            if minute >= 78:
                return goals_needed <= 0.5
            if minute >= 65:
                return goals_needed <= 1.0
            if minute >= 52:
                return goals_needed <= 1.5
            return goals_needed <= 2.0

        return False

    # ------------------------------------------------------------------
    # Structural penalty / boost
    # ------------------------------------------------------------------
    def _structural_adjustment(
        self,
        *,
        side: str,
        line: float,
        current_total: int,
        minute: int,
        regime_label: str,
        feature_map: dict | None,
        totals: dict[int, float],
        price_state: str,
        odds: float,
        state: MatchState,
        remaining_goal_expectancy: float,
        allowed_additional_goals_before_loss: int | None,
    ) -> float:
        regime = (regime_label or "").upper()
        price_state = (price_state or "").upper()

        trailer_chase = self._feature(feature_map, "trailer_chase_signal")
        single_goal_risk = self._feature(feature_map, "single_goal_risk")
        set_piece_risk = self._feature(feature_map, "set_piece_risk")
        recent_ratio = self._feature(feature_map, "recent_pressure_ratio")
        chaos = self._feature(feature_map, "chaos_qadj", self._feature(feature_map, "chaos"))
        pressure_total = self._feature(feature_map, "pressure_total")
        two_sided_liveness = self._feature(feature_map, "two_sided_liveness")
        quality = self._quality(state)

        boundary_low = totals.get(int(line - 1), 0.0)
        boundary_high = totals.get(int(line), 0.0)
        boundary_mass = self._clamp(boundary_low + boundary_high, 0.0, 1.0)

        multiplier = 1.0

        if self._is_under_side(side):
            breathing_room = self._breathing_room_under(line, current_total)
            goal_budget = -1 if allowed_additional_goals_before_loss is None else int(allowed_additional_goals_before_loss)

            if regime in {"OPEN_EXCHANGE", "CHAOTIC_TRANSITIONS"}:
                multiplier -= 0.16
            elif regime in {"ASYMMETRIC_SIEGE_HOME", "ASYMMETRIC_SIEGE_AWAY"}:
                multiplier -= 0.10
            elif regime == "RED_CARD_DISTORTED":
                multiplier -= 0.18
            elif regime in {"CONTROLLED_HOME_PRESSURE", "CONTROLLED_AWAY_PRESSURE"}:
                multiplier -= 0.06
            elif regime in {"CLOSED_LOW_EVENT", "LATE_LOCKDOWN"}:
                multiplier += 0.04

            if line <= 0.5:
                multiplier -= 0.12
            elif line <= 1.5:
                multiplier -= 0.08
            elif line <= 2.5 and current_total >= 2:
                multiplier -= 0.05

            if breathing_room <= 0.5:
                multiplier -= 0.10
            elif breathing_room >= 1.5:
                multiplier += 0.04

            if goal_budget <= 0:
                multiplier -= 0.18
            elif goal_budget == 1:
                if minute < settings.under_one_goal_budget_real_max_minute:
                    multiplier -= 0.12
                if remaining_goal_expectancy > settings.under_one_goal_budget_expectancy_real_max:
                    multiplier -= 0.08
            elif goal_budget == 2:
                if minute < settings.under_two_goal_budget_real_max_minute:
                    multiplier -= 0.07
                if remaining_goal_expectancy > settings.under_two_goal_budget_expectancy_real_max:
                    multiplier -= 0.04

            multiplier -= 0.10 * trailer_chase
            multiplier -= 0.08 * single_goal_risk
            multiplier -= 0.06 * set_piece_risk
            multiplier -= 0.06 * recent_ratio
            multiplier -= 0.06 * chaos

            multiplier -= 0.10 * boundary_mass
            multiplier -= 0.10 * max(0.0, 0.55 - quality)

            if price_state == "DEGRADE_MAIS_VIVANT":
                multiplier -= 0.03
            elif price_state == "MORT":
                multiplier -= 0.18

        else:
            goals_needed = self._goals_needed_for_over(line, current_total)

            if regime == "OPEN_EXCHANGE":
                multiplier += 0.16
            elif regime in {"ASYMMETRIC_SIEGE_HOME", "ASYMMETRIC_SIEGE_AWAY"}:
                multiplier += 0.12
            elif regime in {"CONTROLLED_HOME_PRESSURE", "CONTROLLED_AWAY_PRESSURE"}:
                multiplier += 0.07
            elif regime == "CHAOTIC_TRANSITIONS":
                multiplier += 0.10
            elif regime in {"CLOSED_LOW_EVENT", "LATE_LOCKDOWN"}:
                multiplier -= 0.14
            elif regime == "RED_CARD_DISTORTED":
                multiplier -= 0.08

            if goals_needed <= 0.5:
                multiplier += 0.12
            elif goals_needed <= 1.0:
                multiplier += 0.07
            elif minute >= 60 and goals_needed > 1.0:
                multiplier -= 0.14
            elif minute >= 72 and goals_needed > 0.5:
                multiplier -= 0.12

            multiplier += 0.08 * trailer_chase
            multiplier += 0.04 * recent_ratio
            multiplier += 0.04 * two_sided_liveness
            multiplier += 0.03 * min(1.0, pressure_total / 8.0)

            multiplier -= 0.05 * boundary_mass

            if odds >= 2.60:
                multiplier -= 0.04
            elif 1.45 <= odds <= 2.20:
                multiplier += 0.02

            multiplier -= 0.06 * max(0.0, 0.50 - quality)

            if price_state == "DEGRADE_MAIS_VIVANT":
                multiplier -= 0.01
            elif price_state == "MORT":
                multiplier -= 0.12

        return self._clamp(multiplier, 0.62, 1.28)

    # ------------------------------------------------------------------
    # Projection ranking
    # ------------------------------------------------------------------
    def _selection_score(self, proj: MarketProjection) -> float:
        payload = getattr(proj, "payload", {}) or {}
        edge = self._f(getattr(proj, "edge", None))
        ev = self._f(getattr(proj, "expected_value", None))
        conf = self._f(payload.get("regime_confidence"), 0.60)
        cal_conf = self._f(payload.get("calibration_confidence"), 0.55)
        data_quality = self._f(payload.get("data_quality_score", payload.get("feed_quality")), 0.58)
        display_conf = self._f(payload.get("display_confidence_score"), 0.0)
        structural_mult = self._f(payload.get("structural_multiplier"), 1.0)
        executable = 1.0 if bool(getattr(proj, "executable", False)) else 0.0

        real_status = str(getattr(proj, "real_status", "") or "").upper()
        status_bonus = 0.40 if real_status == "TOP_BET" else 0.20 if real_status == "REAL_VALID" else 0.0

        score = (
            2.2 * edge
            + 1.6 * ev
            + 0.55 * max(0.0, conf - 0.50)
            + 0.65 * max(0.0, cal_conf - 0.45)
            + 0.55 * max(0.0, data_quality - 0.45)
            + 0.10 * max(0.0, structural_mult - 1.0)
            + 0.10 * (display_conf / 10.0)
            + 0.08 * executable
            + status_bonus
        )
        return round(score, 4)

    def _rank_projection(self, proj: MarketProjection) -> tuple:
        payload = getattr(proj, "payload", {}) or {}
        side = self._side_text(getattr(proj, "side", ""))
        edge = self._f(getattr(proj, "edge", None))
        ev = self._f(getattr(proj, "expected_value", None))
        real_status = str(getattr(proj, "real_status", "") or "").upper()
        conf = self._f(payload.get("regime_confidence"), 0.60)
        cal_conf = self._f(payload.get("calibration_confidence"), 0.55)
        data_quality = self._f(payload.get("data_quality_score", payload.get("feed_quality")), 0.58)
        display_conf = self._f(payload.get("display_confidence_score"), 0.0)
        structural_mult = self._f(payload.get("structural_multiplier"), 1.0)
        line = self._f(getattr(proj, "line", None), 0.0)
        current_total = self._i(payload.get("current_total"), 0)
        goals_needed = self._f(payload.get("goals_needed_for_over"), 99.0)
        executable_bonus = 0.05 if bool(getattr(proj, "executable", False)) else 0.0
        selection_score = self._f(payload.get("selection_score"), self._selection_score(proj))

        over_bonus = 0.0
        if self._is_over_side(side):
            if goals_needed <= 0.5:
                over_bonus = 0.10
            elif goals_needed <= 1.0:
                over_bonus = 0.04

        under_breathing = 0.0
        if self._is_under_side(side):
            under_breathing = max(0.0, line - current_total)

        return (
            1 if real_status == "TOP_BET" else 0,
            1 if real_status in {"TOP_BET", "REAL_VALID"} else 0,
            selection_score,
            edge + over_bonus + executable_bonus,
            display_conf,
            ev,
            structural_mult,
            conf,
            cal_conf,
            data_quality,
            under_breathing,
        )

    # ------------------------------------------------------------------
    # Main evaluation
    # ------------------------------------------------------------------
    def evaluate(
        self,
        state: MatchState,
        distribution: ScorelineDistribution,
        regime_label: str,
        regime_confidence: float = 0.60,
        chaos: float = 0.0,
        feature_map: dict | None = None,
    ) -> list[MarketProjection]:
        if not settings.over_under_enabled:
            return []

        quotes = market_engine.quotes_for(state, "ou", "FT")
        if not quotes:
            return []

        minute = self._i(getattr(state, "minute", 0), 0)
        current_total = self._current_total(state)
        totals = self._distribution_totals(distribution)
        remaining_total_probs = self._remaining_total_probs(distribution)
        total_goal_expectancy_remaining = self._f(
            getattr(distribution, "lambda_total_remaining", None),
            self._remaining_goal_expectancy(totals, current_total),
        )
        home_goal_expectancy_remaining = self._f(getattr(distribution, "lambda_home_remaining", None), -1.0)
        away_goal_expectancy_remaining = self._f(getattr(distribution, "lambda_away_remaining", None), -1.0)
        if home_goal_expectancy_remaining < 0.0 or away_goal_expectancy_remaining < 0.0:
            home_goal_expectancy_remaining, away_goal_expectancy_remaining = self._remaining_side_goal_expectancies(
                distribution,
                state,
            )

        out: list[MarketProjection] = []

        for line in self._available_lines(quotes):
            pair = market_engine.pair_two_way(quotes, {"over"}, {"under"}, line=line)
            if not pair:
                continue

            allowed_additional_goals_before_loss = self._allowed_additional_goals_before_loss(line, current_total)
            goals_required_for_over = self._minimum_additional_goals_for_over(line, current_total)

            prob_under_distribution = (
                0.0
                if current_total >= line
                else self._under_survival_probability(remaining_total_probs, allowed_additional_goals_before_loss)
            )
            prob_over_distribution = self._over_hit_probability(remaining_total_probs, goals_required_for_over)
            prob_under_anchor = self._poisson_anchor_probability(
                "UNDER",
                total_goal_expectancy_remaining,
                allowed_additional_goals_before_loss,
                goals_required_for_over,
            )
            prob_over_anchor = self._poisson_anchor_probability(
                "OVER",
                total_goal_expectancy_remaining,
                allowed_additional_goals_before_loss,
                goals_required_for_over,
            )

            candidates = [
                (
                    "UNDER",
                    prob_under_distribution,
                    prob_under_anchor,
                    pair["negative_no_vig"],
                    pair["negative_quote"],
                    allowed_additional_goals_before_loss,
                    goals_required_for_over,
                ),
                (
                    "OVER",
                    prob_over_distribution,
                    prob_over_anchor,
                    pair["positive_no_vig"],
                    pair["positive_quote"],
                    allowed_additional_goals_before_loss,
                    goals_required_for_over,
                ),
            ]

            for side, distribution_prob, poisson_anchor_prob, market_prob, quote, goal_budget, goals_required in candidates:
                if not self._candidate_gate(side, line, current_total, minute, regime_label):
                    continue

                raw_prob = self._clamp(distribution_prob, 0.0001, 0.9999)
                event_probability_gap = abs(raw_prob - self._clamp(poisson_anchor_prob, 0.0001, 0.9999))

                odds = self._f(getattr(quote, "odds_decimal", None), 0.0)
                if odds <= 1.0:
                    continue

                structural_multiplier = self._structural_adjustment(
                    side=side,
                    line=line,
                    current_total=current_total,
                    minute=minute,
                    regime_label=regime_label,
                    feature_map=feature_map,
                    totals=totals,
                    price_state=pair["price_state"],
                    odds=odds,
                    state=state,
                    remaining_goal_expectancy=total_goal_expectancy_remaining,
                    allowed_additional_goals_before_loss=goal_budget if self._is_under_side(side) else None,
                )

                adjusted_raw = self._clamp(raw_prob * structural_multiplier, 0.0001, 0.9999)

                cal = calibration_layer.calibrate(
                    "ou_ft",
                    adjusted_raw,
                    minute=state.minute,
                    regime=regime_label,
                    quality=state.feed_quality_score,
                    market_probability=market_prob,
                    side=side,
                    current_total=current_total,
                    line=line,
                    total_goal_expectancy_remaining=total_goal_expectancy_remaining,
                    home_goal_expectancy_remaining=home_goal_expectancy_remaining,
                    away_goal_expectancy_remaining=away_goal_expectancy_remaining,
                    allowed_additional_goals_before_loss=goal_budget if self._is_under_side(side) else None,
                    score_state_budget=goal_budget if self._is_under_side(side) else None,
                    distribution_event_probability=raw_prob,
                    poisson_anchor_event_probability=poisson_anchor_prob,
                    event_probability_gap=event_probability_gap,
                )

                calibrated_probability = self._clamp(
                    self._f(getattr(cal, "calibrated_probability", adjusted_raw)),
                    0.0001,
                    0.9999,
                )

                fair_odds = self._fair_odds(calibrated_probability)
                edge = calibrated_probability - self._f(market_prob)
                expected_value = market_engine.expected_value(calibrated_probability, odds)

                payload = {
                    "regime_label": regime_label,
                    "regime_confidence": regime_confidence,
                    "minute": minute,
                    "current_total": current_total,
                    "line": line,
                    "chaos": chaos,
                    "distribution_event_probability": round(raw_prob, 6),
                    "poisson_anchor_event_probability": round(self._clamp(poisson_anchor_prob, 0.0001, 0.9999), 6),
                    "event_probability_gap": round(event_probability_gap, 6),
                    "under_survival_probability": round(prob_under_distribution, 6),
                    "over_hit_probability": round(prob_over_distribution, 6),
                    "additional_goals_required_to_win": goals_required,
                    "goals_needed_for_over": self._goals_needed_for_over(line, current_total),
                    "breathing_room_under": self._breathing_room_under(line, current_total),
                    "total_goal_expectancy_remaining": total_goal_expectancy_remaining,
                    "lambda_total_remaining": total_goal_expectancy_remaining,
                    "remaining_total_goal_probs": {str(k): round(v, 6) for k, v in remaining_total_probs.items()},
                    "home_goal_expectancy_remaining": home_goal_expectancy_remaining,
                    "away_goal_expectancy_remaining": away_goal_expectancy_remaining,
                    "lambda_home_remaining": home_goal_expectancy_remaining,
                    "lambda_away_remaining": away_goal_expectancy_remaining,
                    "allowed_additional_goals_before_loss": goal_budget if self._is_under_side(side) else None,
                    "score_state_budget": goal_budget if self._is_under_side(side) else None,
                    "raw_probability_pre_adjustment": raw_prob,
                    "raw_probability_post_adjustment": adjusted_raw,
                    "structural_multiplier": structural_multiplier,
                    "calibrated_probability": calibrated_probability,
                    "market_probability": market_prob,
                    "fair_odds": fair_odds,
                    "calibration_confidence": self._f(getattr(cal, "confidence", 0.55), 0.55),
                    "feed_quality": self._f(getattr(state, "feed_quality_score", 0.58), 0.58),
                    "data_quality_score": self._f(getattr(state, "feed_quality_score", 0.58), 0.58),
                    "distribution_boundary_low_mass": totals.get(int(line - 1), 0.0),
                    "distribution_boundary_high_mass": totals.get(int(line), 0.0),
                    "trailer_chase_signal": self._feature(feature_map, "trailer_chase_signal"),
                    "single_goal_risk": self._feature(feature_map, "single_goal_risk"),
                    "set_piece_risk": self._feature(feature_map, "set_piece_risk"),
                    "recent_pressure_ratio": self._feature(feature_map, "recent_pressure_ratio"),
                    "two_sided_liveness": self._feature(feature_map, "two_sided_liveness"),
                    "pressure_total": self._feature(feature_map, "pressure_total"),
                    "same_bookmaker": bool(pair.get("same_bookmaker", False)),
                    "synthetic_cross_book": bool(pair.get("synthetic_cross_book", False)),
                    "used_truth": "same_book" if bool(pair.get("is_executable", False)) else "cross_book_analysis_only",
                }

                reasons = [
                    f"regime={regime_label}",
                    f"minute={minute}",
                    f"score_total={current_total}",
                    f"line={line:.1f}",
                    f"side={side}",
                    f"struct_mult={structural_multiplier:.3f}",
                    f"fair_prob={calibrated_probability:.3f}",
                    f"same_bookmaker={bool(pair.get('same_bookmaker', False))}",
                ]
                if fair_odds is not None:
                    reasons.append(f"fair_odds={fair_odds:.2f}")

                proj = MarketProjection(
                    market_key="OU_FT",
                    side=side,
                    line=line,
                    raw_probability=adjusted_raw,
                    calibrated_probability=calibrated_probability,
                    market_no_vig_probability=market_prob,
                    edge=edge,
                    expected_value=expected_value,
                    bookmaker=getattr(quote, "bookmaker", None),
                    odds_decimal=odds,
                    executable=bool(pair.get("is_executable", False)),
                    price_state=pair["price_state"],
                    reasons=reasons,
                    payload=payload,
                )

                classified = execution_layer.classify(proj)
                classified.payload["selection_score"] = self._selection_score(classified)
                out.append(classified)

        if not out:
            return []

        out.sort(key=self._rank_projection, reverse=True)

        if getattr(settings, "ou_one_candidate_per_match", False):
            return [out[0]]

        return out
