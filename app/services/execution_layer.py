from __future__ import annotations

from math import ceil, isfinite

from app.config import settings
from app.core.contracts import MarketProjection


class ExecutionLayer:
    """
    EXECUTION LAYER — ouverture contrôlée V3

    Principes :
    - la config reste la source de vérité
    - under serrés toujours très durs
    - overs réouverts seulement sur structure propre
    - pas de seuils cachés qui tordent .env
    - confiance finale plus lisible et moins biaisée
    """

    def classify(self, projection: MarketProjection) -> MarketProjection:
        self._reset(projection)

        market_key = self._market_key(projection)
        side = self._side(projection)
        regime = self._regime_label(projection)
        chaos = self._chaos_value(projection)

        odds = self._float(getattr(projection, "odds_decimal", None))
        p_cal = self._float(getattr(projection, "calibrated_probability", None))
        p_mkt = self._float(getattr(projection, "market_no_vig_probability", None))
        edge = self._float(getattr(projection, "edge", None))
        ev = self._float(getattr(projection, "expected_value", None))
        executable = bool(getattr(projection, "executable", False))
        price_state = self._price_state(projection)

        minute = self._minute(projection)
        current_total = self._current_total(projection)
        line = self._line(projection)
        regime_confidence = self._regime_confidence(projection)
        calibration_confidence = self._calibration_confidence(projection)
        data_quality = self._data_quality(projection)
        remaining_goal_expectancy = self._remaining_goal_expectancy(projection)
        score_tuple = self._score_tuple(projection)
        score_gap = self._score_gap(score_tuple)

        goals_needed = self._goals_needed_for_over(line, current_total)
        breathing_room = self._breathing_room_under(line, current_total)
        score_state_budget = self._score_state_budget(
            market_key=market_key,
            side=side,
            line=line,
            current_total=current_total,
            score_tuple=score_tuple,
            projection=projection,
        )
        favorable_resolution_distance = self._favorable_resolution_distance(
            market_key=market_key,
            side=side,
            line=line,
            current_total=current_total,
            score_tuple=score_tuple,
        )
        adverse_resolution_distance = self._adverse_resolution_distance(
            market_key=market_key,
            side=side,
            line=line,
            current_total=current_total,
            score_tuple=score_tuple,
        )
        remaining_minutes = self._remaining_minutes(minute)
        resolution_pressure = self._resolution_pressure(
            market_key=market_key,
            side=side,
            favorable_resolution_distance=favorable_resolution_distance,
            adverse_resolution_distance=adverse_resolution_distance,
            remaining_minutes=remaining_minutes,
        )
        state_fragility_score = self._state_fragility_score(
            market_key=market_key,
            side=side,
            line=line,
            current_total=current_total,
            favorable_resolution_distance=favorable_resolution_distance,
            adverse_resolution_distance=adverse_resolution_distance,
            score_gap=score_gap,
        )
        early_fragility_score = self._early_fragility_score(
            market_key=market_key,
            side=side,
            minute=minute,
            remaining_minutes=remaining_minutes,
            favorable_resolution_distance=favorable_resolution_distance,
            adverse_resolution_distance=adverse_resolution_distance,
            score_state_budget=score_state_budget,
            remaining_goal_expectancy=remaining_goal_expectancy,
        )
        late_fragility_score = self._late_fragility_score(
            market_key=market_key,
            side=side,
            line=line,
            current_total=current_total,
            minute=minute,
            remaining_minutes=remaining_minutes,
            favorable_resolution_distance=favorable_resolution_distance,
            adverse_resolution_distance=adverse_resolution_distance,
            resolution_pressure=resolution_pressure,
            state_fragility_score=state_fragility_score,
            executable=executable,
            price_state=price_state,
        )

        payload = self._payload(projection)
        payload["remaining_goal_expectancy"] = round(remaining_goal_expectancy, 3)
        payload["score_state_budget"] = score_state_budget
        payload["favorable_resolution_distance"] = round(favorable_resolution_distance, 3)
        payload["adverse_resolution_distance"] = round(adverse_resolution_distance, 3)
        payload["resolution_distance"] = round(favorable_resolution_distance, 3)
        payload["remaining_minutes_estimate"] = round(remaining_minutes, 3)
        payload["resolution_pressure"] = round(resolution_pressure, 4)
        payload["early_fragility_score"] = round(early_fragility_score, 4)
        payload["state_fragility_score"] = round(state_fragility_score, 4)
        payload["late_fragility_score"] = round(late_fragility_score, 4)
        payload["late_fragility"] = round(late_fragility_score, 4)
        projection.payload = payload

        # --------------------------------------------------------------
        # 0) Hard guards
        # --------------------------------------------------------------
        if not self._market_family_enabled(market_key):
            self._veto(projection, "market_family_disabled")
            projection.executable = False
            return self._finalize_confidence(projection)

        # En l'état du projet, seul OU_FT est réellement exécutable.
        if market_key != "OU_FT":
            self._veto(projection, "market_not_yet_supported_in_execution")
            return self._finalize_confidence(projection)

        if odds <= 1.0:
            self._veto(projection, "missing_live_price")
            projection.executable = False
            return self._finalize_confidence(projection)

        if not self._valid_probability(p_cal):
            self._veto(projection, "invalid_calibrated_probability")
            return self._finalize_confidence(projection)

        if not self._valid_probability(p_mkt):
            self._veto(projection, "invalid_market_probability")
            return self._finalize_confidence(projection)

        if edge <= 0.0:
            self._veto(projection, "non_positive_edge")
            return self._finalize_confidence(projection)

        if price_state == "MORT":
            self._veto(projection, "dead_price")
            return self._finalize_confidence(projection)

        if calibration_confidence < 0.36:
            self._veto(projection, "low_calibration_confidence")
            return self._finalize_confidence(projection)

        if data_quality < 0.38:
            self._veto(projection, "data_quality_too_low")
            return self._finalize_confidence(projection)

        if self._is_model_price_absurd(p_cal=p_cal, p_mkt=p_mkt, odds=odds):
            self._veto(projection, "model_price_absurd")
            return self._finalize_confidence(projection)

        # --------------------------------------------------------------
        # 1) Structural geometry
        # --------------------------------------------------------------
        geometry_ok, geometry_reason = self._score_geometry_ok(
            side=side,
            line=line,
            current_total=current_total,
            minute=minute,
        )
        if not geometry_ok:
            self._veto(projection, geometry_reason)
            return self._finalize_confidence(projection)

        structure_ok, structure_reason = self._structure_ok(
            side=side,
            regime=regime,
            chaos=chaos,
            minute=minute,
        )
        if not structure_ok:
            self._veto(projection, structure_reason)
            return self._finalize_confidence(projection)

        # --------------------------------------------------------------
        # 2) Documentary
        # --------------------------------------------------------------
        doc_allowed = (
            edge >= settings.min_edge_doc
            and price_state in {"VIVANT", "DEGRADE_MAIS_VIVANT"}
            and calibration_confidence >= 0.42
            and data_quality >= 0.44
        )

        if settings.documentary_requires_executable and not executable:
            doc_allowed = False
            self._veto(projection, "doc_requires_executable")

        if regime == "RED_CARD_DISTORTED":
            doc_allowed = False
            self._veto(projection, "red_card_doc_ban")

        # Under documentaire — toujours dur
        if self._is_under_side(side):
            under_doc_ok, under_doc_reason = self._under_doc_ok(
                line=line,
                current_total=current_total,
                minute=minute,
            )
            if not under_doc_ok:
                doc_allowed = False
                self._veto(projection, under_doc_reason)

        # Over documentaire — ouverture contrôlée pilotée par la config
        if self._is_over_side(side):
            over_doc_ok, over_doc_reason = self._over_doc_ok(
                minute=minute,
                goals_needed=goals_needed,
                odds=odds,
                regime=regime,
            )
            if not over_doc_ok:
                doc_allowed = False
                self._veto(projection, over_doc_reason)

        if doc_allowed:
            projection.documentary_status = "DOC_STRONG"

        # --------------------------------------------------------------
        # 3) REAL
        # --------------------------------------------------------------
        real_allowed = True

        if settings.require_executable_for_real and not executable:
            real_allowed = False
            self._veto(projection, "not_executable")

        if edge < settings.min_edge_real:
            real_allowed = False
            self._veto(projection, "edge_below_real_floor")

        if ev < settings.min_ev_real:
            real_allowed = False
            self._veto(projection, "ev_below_real_floor")

        if chaos > settings.max_chaos_real:
            real_allowed = False
            self._veto(projection, "chaos_too_high")

        if regime_confidence < settings.min_regime_confidence_real:
            real_allowed = False
            self._veto(projection, "regime_confidence_too_low")

        if calibration_confidence < 0.46:
            real_allowed = False
            self._veto(projection, "calibration_confidence_too_low_for_real")

        if data_quality < 0.46:
            real_allowed = False
            self._veto(projection, "data_quality_too_low_for_real")

        if p_cal < p_mkt + settings.min_prob_gap_real:
            real_allowed = False
            self._veto(projection, "advantage_too_thin_after_calibration")

        if regime == "RED_CARD_DISTORTED":
            real_allowed = False
            self._veto(projection, "red_card_real_ban")

        # Under réel — très dur
        if self._is_under_side(side):
            under_real_ok, under_real_reason = self._under_real_ok(
                line=line,
                current_total=current_total,
                minute=minute,
                breathing_room=breathing_room,
                remaining_minutes=remaining_minutes,
                adverse_resolution_distance=adverse_resolution_distance,
                resolution_pressure=resolution_pressure,
                remaining_goal_expectancy=remaining_goal_expectancy,
                score_state_budget=score_state_budget,
                early_fragility_score=early_fragility_score,
                state_fragility_score=state_fragility_score,
            )
            if not under_real_ok:
                real_allowed = False
                self._veto(projection, under_real_reason)

        # Over réel — réouverture contrôlée
        if self._is_over_side(side):
            over_real_ok, over_real_reason = self._over_real_ok(
                line=line,
                current_total=current_total,
                minute=minute,
                goals_needed=goals_needed,
                odds=odds,
                regime=regime,
                regime_confidence=regime_confidence,
                calibration_confidence=calibration_confidence,
                data_quality=data_quality,
                executable=executable,
                price_state=price_state,
                favorable_resolution_distance=favorable_resolution_distance,
                adverse_resolution_distance=adverse_resolution_distance,
                remaining_minutes=remaining_minutes,
                resolution_pressure=resolution_pressure,
                state_fragility_score=state_fragility_score,
                late_fragility_score=late_fragility_score,
                score_gap=score_gap,
            )
            if not over_real_ok:
                real_allowed = False
                self._veto(projection, over_real_reason)

        if real_allowed:
            projection.real_status = "REAL_VALID"

        # --------------------------------------------------------------
        # 4) TOP BET
        # --------------------------------------------------------------
        top_bet_ok = False

        if projection.real_status == "REAL_VALID":
            if self._is_under_side(side):
                top_bet_ok = (
                    price_state == "VIVANT"
                    and edge >= settings.min_edge_top_bet
                    and ev >= settings.min_ev_top_bet
                    and chaos <= 0.45
                    and 0.63 <= p_cal <= 0.84
                    and 1.35 <= odds <= 2.45
                    and regime_confidence >= settings.min_regime_confidence_top_bet
                    and calibration_confidence >= 0.58
                    and data_quality >= 0.58
                    and regime in {"CLOSED_LOW_EVENT", "LATE_LOCKDOWN"}
                    and self._top_bet_score_geometry_ok(
                        side=side,
                        line=line,
                        current_total=current_total,
                        minute=minute,
                    )
                )

            elif self._is_over_side(side):
                top_bet_ok = (
                    price_state == "VIVANT"
                    and edge >= settings.min_edge_top_bet
                    and ev >= settings.min_ev_top_bet
                    and chaos <= 0.82
                    and 0.60 <= p_cal <= 0.84
                    and 1.45 <= odds <= 2.30
                    and regime_confidence >= settings.min_regime_confidence_top_bet
                    and calibration_confidence >= 0.58
                    and data_quality >= 0.58
                    and regime in {
                        "OPEN_EXCHANGE",
                        "ASYMMETRIC_SIEGE_HOME",
                        "ASYMMETRIC_SIEGE_AWAY",
                        "CONTROLLED_HOME_PRESSURE",
                        "CONTROLLED_AWAY_PRESSURE",
                    }
                    and self._top_bet_score_geometry_ok(
                        side=side,
                        line=line,
                        current_total=current_total,
                        minute=minute,
                    )
                )

        if top_bet_ok:
            projection.real_status = "TOP_BET"
            projection.top_bet_flag = True

        if projection.real_status == "TOP_BET":
            top_bet_confidence = self._confidence_score(projection, real_status_override="TOP_BET")
            if top_bet_confidence < settings.min_display_confidence_top_bet:
                projection.real_status = "REAL_VALID"
                projection.top_bet_flag = False
                self._veto(projection, "display_confidence_below_top_bet_floor")

        if projection.real_status == "REAL_VALID":
            real_confidence = self._confidence_score(projection, real_status_override="REAL_VALID")
            if real_confidence < settings.min_display_confidence_real:
                projection.real_status = "NO_BET"
                projection.top_bet_flag = False
                self._veto(projection, "display_confidence_below_real_floor")

        if settings.real_only_top_bets and projection.real_status != "TOP_BET":
            projection.real_status = "NO_BET"
            projection.top_bet_flag = False
            self._veto(projection, "real_only_top_bets_filter")

        return self._finalize_confidence(projection)

    # ------------------------------------------------------------------
    # Confidence
    # ------------------------------------------------------------------
    def _confidence_score(self, projection: MarketProjection, real_status_override: str | None = None) -> float:
        edge = self._float(getattr(projection, "edge", None))
        ev = self._float(getattr(projection, "expected_value", None))
        p_cal = self._float(getattr(projection, "calibrated_probability", None))
        p_mkt = self._float(getattr(projection, "market_no_vig_probability", None))
        regime_conf = self._regime_confidence(projection)
        cal_conf = self._calibration_confidence(projection)
        data_quality = self._data_quality(projection)
        price_state = self._price_state(projection)

        score = 1.6
        score += min(2.0, max(0.0, edge * 12.0))
        score += min(1.3, max(0.0, ev * 2.2))
        score += 1.1 * max(0.0, regime_conf - 0.50)
        score += 1.2 * max(0.0, cal_conf - 0.42)
        score += 1.0 * max(0.0, data_quality - 0.42)
        score += 0.7 * max(0.0, p_cal - p_mkt) * 6.0

        if price_state == "VIVANT":
            score += 0.25
        elif price_state == "DEGRADE_MAIS_VIVANT":
            score += 0.08

        real_status = str(real_status_override or getattr(projection, "real_status", "NO_BET") or "").upper()
        if real_status == "TOP_BET":
            score += 0.45
            score = min(score, settings.confidence_top_bet_cap)
        elif real_status == "REAL_VALID":
            score += 0.15
            score = min(score, settings.confidence_real_cap)
        else:
            score = min(score, settings.confidence_doc_cap)

        # pénalité modérée seulement
        if projection.vetoes and real_status == "NO_BET":
            score -= min(2.2, 0.35 * len(projection.vetoes))
        elif projection.vetoes:
            score -= min(0.8, 0.12 * len(projection.vetoes))

        return max(0.0, min(10.0, score))

    def _finalize_confidence(self, projection: MarketProjection) -> MarketProjection:
        score = self._confidence_score(projection)
        payload = self._payload(projection)
        payload["display_confidence_score"] = round(score, 1)
        payload["regime_confidence"] = round(self._regime_confidence(projection), 3)
        payload["calibration_confidence"] = round(self._calibration_confidence(projection), 3)
        payload["data_quality_score"] = round(self._data_quality(projection), 3)
        projection.payload = payload
        return projection

    # ------------------------------------------------------------------
    # Reset
    # ------------------------------------------------------------------
    def _reset(self, projection: MarketProjection) -> None:
        projection.documentary_status = "DOC_ONLY"
        projection.real_status = "NO_BET"
        projection.top_bet_flag = False

    # ------------------------------------------------------------------
    # Payload / accessors
    # ------------------------------------------------------------------
    def _market_key(self, projection: MarketProjection) -> str:
        return str(getattr(projection, "market_key", "") or "").strip().upper()

    def _side(self, projection: MarketProjection) -> str:
        return str(getattr(projection, "side", "") or "").strip().upper()

    def _payload(self, projection: MarketProjection) -> dict:
        payload = getattr(projection, "payload", None)
        return payload if isinstance(payload, dict) else {}

    def _regime_label(self, projection: MarketProjection) -> str:
        return str(self._payload(projection).get("regime_label") or "").strip().upper()

    def _regime_confidence(self, projection: MarketProjection) -> float:
        return self._float(self._payload(projection).get("regime_confidence"), 0.60)

    def _calibration_confidence(self, projection: MarketProjection) -> float:
        return self._float(self._payload(projection).get("calibration_confidence"), 0.55)

    def _data_quality(self, projection: MarketProjection) -> float:
        payload = self._payload(projection)
        explicit = payload.get("data_quality_score")
        if explicit is not None:
            return self._float(explicit, settings.feed_quality_default)
        return self._float(payload.get("feed_quality"), settings.feed_quality_default)

    def _chaos_value(self, projection: MarketProjection) -> float:
        return self._float(self._payload(projection).get("chaos"), 0.0)

    def _minute(self, projection: MarketProjection) -> int:
        return int(self._float(self._payload(projection).get("minute"), 0.0))

    def _current_total(self, projection: MarketProjection) -> int:
        return int(self._float(self._payload(projection).get("current_total"), 0.0))

    def _line(self, projection: MarketProjection) -> float | None:
        try:
            value = getattr(projection, "line", None)
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def _price_state(self, projection: MarketProjection) -> str:
        return str(getattr(projection, "price_state", "") or "").strip().upper()

    # ------------------------------------------------------------------
    # Generic helpers
    # ------------------------------------------------------------------
    def _float(self, value, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            value = float(value)
            if not isfinite(value):
                return default
            return value
        except (TypeError, ValueError):
            return default

    def _valid_probability(self, value: float) -> bool:
        return 0.0 < value < 1.0

    def _veto(self, projection: MarketProjection, reason: str) -> None:
        if reason and reason not in projection.vetoes:
            projection.vetoes.append(reason)

    # ------------------------------------------------------------------
    # Market families
    # ------------------------------------------------------------------
    def _market_family_enabled(self, market_key: str) -> bool:
        mapping = {
            "OU_FT": settings.over_under_enabled,
            "OU_1H": settings.first_half_enabled,
            "BTTS": settings.btts_enabled,
            "TEAM_TOTAL": settings.team_totals_enabled,
            "RESULT": settings.result_engine_enabled,
            "CORRECT_SCORE": settings.correct_score_enabled,
        }
        return bool(mapping.get(market_key, False))

    # ------------------------------------------------------------------
    # Side helpers
    # ------------------------------------------------------------------
    def _is_under_side(self, side: str) -> bool:
        return "UNDER" in side or side in {"BTTS_NO", "NO"}

    def _is_over_side(self, side: str) -> bool:
        return "OVER" in side or side in {"BTTS_YES", "YES"}

    def _goals_needed_for_over(self, line: float | None, current_total: int) -> float:
        if line is None:
            return 99.0
        return max(0.0, float(line) - float(current_total))

    def _breathing_room_under(self, line: float | None, current_total: int) -> float:
        if line is None:
            return 0.0
        return max(0.0, float(line) - float(current_total))

    def _allowed_additional_goals_before_loss(self, line: float | None, current_total: int) -> int | None:
        if line is None:
            return None
        return int(ceil(float(line) - float(current_total)) - 1)

    def _remaining_goal_expectancy(self, projection: MarketProjection) -> float:
        payload = self._payload(projection)
        return self._float(
            payload.get("total_goal_expectancy_remaining", payload.get("remaining_goal_expectancy")),
            0.0,
        )

    def _score_state_budget(
        self,
        market_key: str,
        side: str,
        line: float | None,
        current_total: int,
        score_tuple: tuple[int | None, int | None],
        projection: MarketProjection,
    ) -> int | None:
        payload = self._payload(projection)
        explicit = payload.get("score_state_budget", payload.get("allowed_additional_goals_before_loss"))
        if explicit is not None:
            try:
                return int(explicit)
            except (TypeError, ValueError):
                pass

        if market_key.startswith("OU") and self._is_under_side(side):
            return self._allowed_additional_goals_before_loss(line, current_total)

        if market_key == "BTTS" and self._is_under_side(side):
            home_goals, away_goals = score_tuple
            if (home_goals or 0) > 0 or (away_goals or 0) > 0:
                return 0
            return 1

        return None

    def _score_tuple(self, projection: MarketProjection) -> tuple[int | None, int | None]:
        raw = str(self._payload(projection).get("state_score") or "").strip()
        if not raw:
            return None, None

        left, sep, right = raw.partition("-")
        if not sep:
            return None, None

        try:
            return int(left.strip()), int(right.strip())
        except (TypeError, ValueError):
            return None, None

    def _score_gap(self, score_tuple: tuple[int | None, int | None]) -> int:
        home_goals, away_goals = score_tuple
        if home_goals is None or away_goals is None:
            return 0
        return abs(home_goals - away_goals)

    def _favorable_resolution_distance(
        self,
        market_key: str,
        side: str,
        line: float | None,
        current_total: int,
        score_tuple: tuple[int | None, int | None],
    ) -> float:
        if market_key.startswith("OU"):
            if self._is_over_side(side):
                return self._goals_needed_for_over(line, current_total)
            if self._is_under_side(side):
                return 0.0

        if market_key == "BTTS":
            home_goals, away_goals = score_tuple
            if self._is_over_side(side):
                missing = 0
                if home_goals is None or home_goals <= 0:
                    missing += 1
                if away_goals is None or away_goals <= 0:
                    missing += 1
                return float(missing)
            if self._is_under_side(side):
                return 0.0

        return self._goals_needed_for_over(line, current_total) if self._is_over_side(side) else 0.0

    def _adverse_resolution_distance(
        self,
        market_key: str,
        side: str,
        line: float | None,
        current_total: int,
        score_tuple: tuple[int | None, int | None],
    ) -> float:
        if market_key.startswith("OU"):
            if self._is_under_side(side):
                return self._breathing_room_under(line, current_total)
            if self._is_over_side(side):
                return 99.0

        if market_key == "BTTS":
            home_goals, away_goals = score_tuple
            if self._is_under_side(side):
                if (home_goals or 0) > 0 or (away_goals or 0) > 0:
                    return 0.0
                return 0.5
            if self._is_over_side(side):
                return 99.0

        return 99.0

    def _resolution_distance(self, side: str, line: float | None, current_total: int) -> float:
        if self._is_over_side(side):
            return self._goals_needed_for_over(line, current_total)
        if self._is_under_side(side):
            return self._breathing_room_under(line, current_total)
        return 99.0

    def _remaining_minutes(self, minute: int) -> float:
        return max(1.0, 95.0 - float(max(0, minute)))

    def _resolution_pressure(
        self,
        market_key: str,
        side: str,
        favorable_resolution_distance: float,
        adverse_resolution_distance: float,
        remaining_minutes: float,
    ) -> float:
        if remaining_minutes <= 0.0:
            return 99.0

        if market_key.startswith("OU"):
            if self._is_over_side(side):
                return max(0.0, favorable_resolution_distance) / remaining_minutes
            if self._is_under_side(side):
                return (1.0 / max(0.25, adverse_resolution_distance + 0.25)) / remaining_minutes

        if self._is_under_side(side):
            return (1.0 / max(0.25, adverse_resolution_distance + 0.25)) / remaining_minutes
        return max(0.0, favorable_resolution_distance) / remaining_minutes

    def _state_fragility_score(
        self,
        market_key: str,
        side: str,
        line: float | None,
        current_total: int,
        favorable_resolution_distance: float,
        adverse_resolution_distance: float,
        score_gap: int,
    ) -> float:
        if market_key.startswith("OU"):
            if self._is_under_side(side):
                return 1.0 / max(0.25, adverse_resolution_distance + 0.25)

            score = max(0.0, favorable_resolution_distance)
            if favorable_resolution_distance <= 0.5 and score_gap >= 2:
                score += 0.30 + 0.08 * min(2, score_gap - 2)
            if line is not None and line >= settings.late_extreme_total_line_threshold and current_total >= max(0.0, line - 1.0):
                score += 0.18
            return score

        if self._is_under_side(side):
            return 1.0 / max(0.25, adverse_resolution_distance + 0.25)
        return max(0.0, favorable_resolution_distance)

    def _early_fragility_score(
        self,
        market_key: str,
        side: str,
        minute: int,
        remaining_minutes: float,
        favorable_resolution_distance: float,
        adverse_resolution_distance: float,
        score_state_budget: int | None,
        remaining_goal_expectancy: float,
    ) -> float:
        if market_key.startswith("OU") and self._is_under_side(side):
            budget = -1 if score_state_budget is None else int(score_state_budget)
            time_weight = self._float(max(0.0, 60.0 - float(minute)) / 60.0, 0.0)
            expectancy_excess = max(0.0, remaining_goal_expectancy - max(0, budget) - 0.10)

            if budget <= 0:
                score = 1.05 + 0.28 * time_weight + 0.22 * min(1.0, expectancy_excess)
            elif budget == 1:
                score = 0.72 + 0.24 * time_weight + 0.24 * min(1.0, expectancy_excess)
            elif budget == 2:
                score = 0.34 + 0.18 * max(0.0, 40.0 - float(minute)) / 40.0
                score += 0.16 * max(0.0, remaining_goal_expectancy - 1.45)
            else:
                score = 0.08 * max(0.0, remaining_goal_expectancy - float(budget))

            return self._float(score, 0.0)

        if self._is_over_side(side):
            early_weight = max(0.0, 38.0 - float(minute)) / 38.0
            return max(0.0, favorable_resolution_distance - 1.0) * early_weight

        return max(0.0, adverse_resolution_distance - 1.0) * max(0.0, 38.0 - float(minute)) / 38.0

    def _late_fragility_score(
        self,
        market_key: str,
        side: str,
        line: float | None,
        current_total: int,
        minute: int,
        remaining_minutes: float,
        favorable_resolution_distance: float,
        adverse_resolution_distance: float,
        resolution_pressure: float,
        state_fragility_score: float,
        executable: bool,
        price_state: str,
    ) -> float:
        score = resolution_pressure + 0.018 * max(0.0, state_fragility_score)
        if minute >= 70:
            score *= 1.18
        if minute >= 80:
            score *= 1.22
        if (
            market_key.startswith("OU")
            and line is not None
            and self._is_over_side(side)
            and float(line) >= settings.late_extreme_total_line_threshold
        ):
            score *= 1.20
        if self._is_over_side(side) and current_total == 0 and line is not None and float(line) <= 0.5:
            score *= 1.30
        if self._is_under_side(side) and adverse_resolution_distance <= 0.5 and remaining_minutes >= 28.0:
            score *= 1.18
        if self._is_over_side(side) and favorable_resolution_distance <= 0.5 and minute >= 76:
            score *= 1.10
        if not executable:
            score *= 1.10
        if price_state != "VIVANT":
            score *= 1.06
        return score

    # ------------------------------------------------------------------
    # Structural logic
    # ------------------------------------------------------------------
    def _score_geometry_ok(
        self,
        side: str,
        line: float | None,
        current_total: int,
        minute: int,
    ) -> tuple[bool, str]:
        if line is None:
            return False, "missing_line"

        if self._is_under_side(side):
            if current_total >= line:
                return False, "under_already_dead_by_score"
            if minute <= 12 and current_total >= 3:
                return False, "under_early_open_score_conflict"
            if minute <= 20 and current_total >= 4:
                return False, "under_early_score_explosion"
            return True, ""

        if self._is_over_side(side):
            goals_needed = self._goals_needed_for_over(line, current_total)
            if minute >= 78 and goals_needed > 0.5:
                return False, "late_over_needs_more_than_one_goal"
            if minute >= 55 and goals_needed > 1.0:
                return False, "over_needs_too_many_goals_after_55"
            return True, ""

        return False, "unknown_ou_side"

    def _structure_ok(
        self,
        side: str,
        regime: str,
        chaos: float,
        minute: int,
    ) -> tuple[bool, str]:
        if chaos > 0.94:
            return False, "extreme_chaos"

        if self._is_under_side(side):
            if regime in {"OPEN_EXCHANGE", "CHAOTIC_TRANSITIONS"}:
                return False, "under_regime_conflict"
            if regime == "LATE_LOCKDOWN" and minute < 66:
                return False, "late_lockdown_time_conflict"
            return True, ""

        if self._is_over_side(side):
            if regime in {"CLOSED_LOW_EVENT", "LATE_LOCKDOWN"}:
                return False, "over_regime_conflict"
            return True, ""

        return False, "unknown_ou_side"

    def _under_doc_ok(
        self,
        line: float | None,
        current_total: int,
        minute: int,
    ) -> tuple[bool, str]:
        if line is None:
            return False, "missing_line"

        if line <= 0.5 and minute < settings.under_doc_minute_u05:
            return False, "under_0_5_doc_ban"
        if line <= 1.5 and minute < settings.under_doc_minute_u15:
            return False, "under_1_5_doc_ban"
        if line <= 2.5 and current_total >= 2 and minute < settings.under_doc_minute_u25_score2plus:
            return False, "under_2_5_tight_doc_ban"
        if line <= 2.5 and current_total <= 1 and minute < settings.under_doc_minute_u25_score0or1:
            return False, "under_2_5_not_mature_enough_doc"

        return True, ""

    def _under_real_ok(
        self,
        line: float | None,
        current_total: int,
        minute: int,
        breathing_room: float,
        remaining_minutes: float,
        adverse_resolution_distance: float,
        resolution_pressure: float,
        remaining_goal_expectancy: float,
        score_state_budget: int | None,
        early_fragility_score: float,
        state_fragility_score: float,
    ) -> tuple[bool, str]:
        if line is None:
            return False, "missing_line"

        if line <= 0.5 and minute < settings.under_real_minute_u05:
            return False, "under_0_5_real_ban"
        if line <= 1.5 and minute < settings.under_real_minute_u15:
            return False, "under_1_5_real_ban"
        if line <= 2.5 and current_total >= 2 and minute < settings.under_real_minute_u25_score2plus:
            return False, "under_2_5_tight_real_ban"
        if line <= 2.5 and current_total <= 1 and minute < settings.under_real_minute_u25_score0or1:
            return False, "under_2_5_not_mature_enough"
        if line <= 3.5 and current_total >= 3 and minute < 62:
            return False, "under_3_5_tight_real_ban"

        if breathing_room < 0.5:
            return False, "under_without_breathing_room"

        goal_budget = -1 if score_state_budget is None else int(score_state_budget)
        if goal_budget <= 0 and minute < settings.under_small_goal_budget_real_max_minute:
            return False, "under_small_goal_budget_too_early"
        if goal_budget == 1 and minute < settings.under_one_goal_budget_real_max_minute:
            return False, "under_one_goal_budget_too_early"
        if goal_budget == 2 and minute < settings.under_two_goal_budget_real_max_minute:
            return False, "under_two_goal_budget_too_early"

        if goal_budget <= 1 and remaining_minutes >= settings.under_goal_budget_time_conflict_min_remaining_minutes:
            return False, "under_goal_budget_vs_time_conflict"
        if goal_budget <= 1 and remaining_goal_expectancy > settings.under_one_goal_budget_expectancy_real_max:
            return False, "under_goal_budget_vs_expectancy_conflict"
        if goal_budget == 2 and minute <= 40 and remaining_goal_expectancy > settings.under_two_goal_budget_expectancy_real_max:
            return False, "under_goal_budget_vs_expectancy_conflict"

        if early_fragility_score > settings.under_early_fragility_real_max and minute <= 50:
            return False, "under_early_fragility_too_high_for_real"
        if resolution_pressure > settings.resolution_pressure_real_max:
            return False, "under_resolution_pressure_too_high_for_real"
        if state_fragility_score >= 1.30 and remaining_minutes >= 24.0:
            return False, "under_state_too_fragile_for_real"

        return True, ""

    def _over_doc_ok(
        self,
        minute: int,
        goals_needed: float,
        odds: float,
        regime: str,
    ) -> tuple[bool, str]:
        if minute >= 60 and goals_needed > settings.over_doc_max_goals_needed_after60:
            return False, "over_too_ambitious_for_doc"
        if minute >= 70 and goals_needed > settings.over_doc_max_goals_needed_after70:
            return False, "late_over_needs_too_many_goals"
        if odds >= 3.10:
            return False, "doc_over_price_too_long"
        if regime not in {
            "OPEN_EXCHANGE",
            "ASYMMETRIC_SIEGE_HOME",
            "ASYMMETRIC_SIEGE_AWAY",
            "CONTROLLED_HOME_PRESSURE",
            "CONTROLLED_AWAY_PRESSURE",
            "BALANCED_NEUTRAL",
        }:
            return False, "over_doc_regime_not_clean_enough"
        return True, ""

    def _over_real_ok(
        self,
        line: float | None,
        current_total: int,
        minute: int,
        goals_needed: float,
        odds: float,
        regime: str,
        regime_confidence: float,
        calibration_confidence: float,
        data_quality: float,
        executable: bool,
        price_state: str,
        favorable_resolution_distance: float,
        adverse_resolution_distance: float,
        remaining_minutes: float,
        resolution_pressure: float,
        state_fragility_score: float,
        late_fragility_score: float,
        score_gap: int,
    ) -> tuple[bool, str]:
        if minute >= 55 and goals_needed > settings.over_real_max_goals_needed_after55:
            return False, "over_needs_too_many_goals_for_real"
        if minute >= 65 and goals_needed > settings.over_real_max_goals_needed_after65:
            return False, "late_over_not_clean_enough"
        if minute >= settings.late_over_0_5_nil_nil_real_ban_minute and current_total == 0 and line is not None and line <= 0.5:
            return False, "late_over_0_5_nil_nil_real_ban"
        if minute >= 77 and line is not None and line <= 2.5 and current_total == 2 and score_gap >= 2:
            return False, "late_one_sided_single_goal_over_real_ban"
        if (
            line is not None
            and line >= settings.late_extreme_total_line_threshold
            and minute >= settings.late_extreme_total_ban_minute
            and goals_needed > settings.late_extreme_total_max_goals_needed
        ):
            return False, "late_extreme_total_real_ban"
        if (
            line is not None
            and line >= settings.late_extreme_total_line_threshold
            and minute >= max(74, settings.late_extreme_total_ban_minute - 2)
            and favorable_resolution_distance <= 0.5
        ):
            return False, "late_extreme_total_quasi_ban"
        if resolution_pressure > settings.resolution_pressure_real_max:
            return False, "resolution_pressure_too_high_for_real"
        if late_fragility_score > settings.late_fragility_real_max:
            return False, "late_fragility_too_high_for_real"
        if state_fragility_score >= 0.80 and minute >= 76:
            return False, "over_state_too_fragile_for_real"
        if odds >= 2.95:
            return False, "real_over_price_too_long"
        if regime not in {
            "OPEN_EXCHANGE",
            "ASYMMETRIC_SIEGE_HOME",
            "ASYMMETRIC_SIEGE_AWAY",
            "CONTROLLED_HOME_PRESSURE",
            "CONTROLLED_AWAY_PRESSURE",
        }:
            return False, "over_regime_not_strong_enough"
        if (
            line is not None
            and line <= 1.5
            and current_total == 1
            and score_gap == 1
            and minute >= settings.late_over_1_5_small_score_strict_minute
        ):
            if price_state != "VIVANT":
                return False, "late_over_1_5_small_score_requires_vivant_price"
            if not executable:
                return False, "late_over_1_5_small_score_requires_executable"
            if regime_confidence < settings.late_over_1_5_small_score_min_regime_confidence:
                return False, "late_over_1_5_small_score_regime_not_strong_enough"
            if calibration_confidence < settings.late_over_1_5_small_score_min_calibration_confidence:
                return False, "late_over_1_5_small_score_calibration_not_clean_enough"
            if data_quality < settings.late_over_1_5_small_score_min_data_quality:
                return False, "late_over_1_5_small_score_data_not_clean_enough"
            if favorable_resolution_distance > settings.late_over_1_5_small_score_max_resolution_distance:
                return False, "late_over_1_5_small_score_resolution_too_far"
            if remaining_minutes <= 0.0 or favorable_resolution_distance / remaining_minutes > settings.resolution_pressure_real_max:
                return False, "late_over_1_5_small_score_resolution_pressure_too_high"
        if minute >= 74 and calibration_confidence < 0.52:
            return False, "late_over_calibration_not_clean_enough"
        if minute >= 74 and data_quality < 0.50:
            return False, "late_over_data_quality_not_clean_enough"
        return True, ""

    def _top_bet_score_geometry_ok(
        self,
        side: str,
        line: float | None,
        current_total: int,
        minute: int,
    ) -> bool:
        if line is None:
            return False

        if self._is_under_side(side):
            breathing_room = line - current_total
            if minute < 35:
                return breathing_room >= 1.5
            if minute < 60:
                return breathing_room >= 1.0
            return breathing_room >= 0.5

        if self._is_over_side(side):
            goals_needed = self._goals_needed_for_over(line, current_total)
            if minute >= 68:
                return goals_needed <= 0.5
            return goals_needed <= 1.0

        return False

    # ------------------------------------------------------------------
    # Sanity check
    # ------------------------------------------------------------------
    def _is_model_price_absurd(self, p_cal: float, p_mkt: float, odds: float) -> bool:
        if odds >= 3.40 and p_cal >= 0.82:
            return True
        if p_cal >= 0.90 and p_mkt <= 0.18:
            return True
        return False
