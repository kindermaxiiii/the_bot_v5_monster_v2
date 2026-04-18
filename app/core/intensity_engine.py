from __future__ import annotations

from math import isfinite

from app.core.contracts import IntensityDecision, RegimeDecision


class IntensityEngine:
    """
    Intensity engine V8.

    Objectifs :
    - rester cohérent avec le feature_engine actuel
    - reconstruire les signaux manquants au lieu de dépendre d'inputs morts
    - éviter la sur-compression tardive
    - préserver les chase states propres
    - rester prudent sur RED_CARD_DISTORTED
    """

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _f(self, value, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            value = float(value)
            if not isfinite(value):
                return default
            return default if value != value else value
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

    def _safe_div(self, a: float, b: float, default: float = 0.0) -> float:
        return default if abs(b) < 1e-12 else a / b

    # ------------------------------------------------------------------
    # Time-left model with stoppage-time estimate
    # ------------------------------------------------------------------
    def _added_time_estimate(
        self,
        minute: int,
        total_goals: int,
        red_cards: int,
        set_piece_risk: float,
        chaos: float,
    ) -> float:
        if minute <= 45:
            base = 1.5
            base += 0.28 * total_goals
            base += 0.80 * red_cards
            base += 0.35 * set_piece_risk
            base += 0.25 * chaos
            return self._clamp(base, 1.0, 5.5)

        base = 3.5
        base += 0.38 * total_goals
        base += 1.10 * red_cards
        base += 0.50 * set_piece_risk
        base += 0.35 * chaos
        return self._clamp(base, 2.5, 8.5)

    def _time_left_estimate(
        self,
        minute: int,
        total_goals: int,
        red_cards: int,
        set_piece_risk: float,
        chaos: float,
    ) -> float:
        added = self._added_time_estimate(
            minute=minute,
            total_goals=total_goals,
            red_cards=red_cards,
            set_piece_risk=set_piece_risk,
            chaos=chaos,
        )
        horizon = 45.0 + added if minute <= 45 else 90.0 + added
        return max(1.0, horizon - float(minute))

    # ------------------------------------------------------------------
    # Regime multipliers
    # ------------------------------------------------------------------
    def _regime_multipliers(self, regime_label: str) -> tuple[float, float]:
        label = (regime_label or "").upper()

        if label == "CLOSED_LOW_EVENT":
            return 0.80, 0.80
        if label == "LATE_LOCKDOWN":
            return 0.72, 0.72
        if label == "OPEN_EXCHANGE":
            return 1.18, 1.18
        if label == "CHAOTIC_TRANSITIONS":
            return 1.14, 1.14
        if label == "ASYMMETRIC_SIEGE_HOME":
            return 1.26, 0.86
        if label == "ASYMMETRIC_SIEGE_AWAY":
            return 0.86, 1.26
        if label == "CONTROLLED_HOME_PRESSURE":
            return 1.12, 0.92
        if label == "CONTROLLED_AWAY_PRESSURE":
            return 0.92, 1.12
        if label == "RED_CARD_DISTORTED":
            return 0.98, 0.98

        return 1.00, 1.00

    # ------------------------------------------------------------------
    # Derived signals from current feature map
    # ------------------------------------------------------------------
    def _recent_pressure_split(
        self,
        pressure_total: float,
        pressure_share_home: float,
        pressure_share_away: float,
        recent_pressure_ratio: float,
        explicit_home: float,
        explicit_away: float,
    ) -> tuple[float, float, bool]:
        if explicit_home > 0.0 or explicit_away > 0.0:
            return explicit_home, explicit_away, False

        # feature_engine actuel définit grossièrement :
        # recent_pressure_ratio ~= (recent_total / pressure_total) * 2
        recent_total = pressure_total * self._clamp(recent_pressure_ratio / 2.0, 0.0, 1.2)
        derived_home = recent_total * self._clamp(pressure_share_home, 0.0, 1.0)
        derived_away = recent_total * self._clamp(pressure_share_away, 0.0, 1.0)
        return derived_home, derived_away, True

    def _derive_over_support_signal(
        self,
        openness: float,
        two_sided_liveness: float,
        danger_confirmation: float,
        recent_pressure_ratio: float,
        trailer_chase: float,
        set_piece_risk: float,
        slowdown: float,
        fake_pressure_penalty: float,
    ) -> float:
        recent_scaled = self._clamp(recent_pressure_ratio / 1.25, 0.0, 1.0)
        signal = (
            0.24 * openness
            + 0.18 * two_sided_liveness
            + 0.18 * danger_confirmation
            + 0.14 * recent_scaled
            + 0.12 * trailer_chase
            + 0.10 * set_piece_risk
            - 0.14 * slowdown
            - 0.12 * fake_pressure_penalty
        )
        return self._clamp(signal, 0.0, 1.0)

    def _derive_under_stability_signal(
        self,
        slowdown: float,
        openness: float,
        two_sided_liveness: float,
        chaos: float,
        recent_pressure_ratio: float,
        trailer_chase: float,
        set_piece_risk: float,
        danger_confirmation: float,
        fake_pressure_penalty: float,
    ) -> float:
        recent_scaled = self._clamp(recent_pressure_ratio / 1.25, 0.0, 1.0)
        signal = (
            0.28 * slowdown
            + 0.18 * (1.0 - openness)
            + 0.14 * (1.0 - two_sided_liveness)
            + 0.12 * (1.0 - chaos)
            + 0.10 * (1.0 - recent_scaled)
            + 0.08 * (1.0 - set_piece_risk)
            + 0.10 * fake_pressure_penalty
            - 0.12 * trailer_chase
            - 0.08 * danger_confirmation
        )
        return self._clamp(signal, 0.0, 1.0)

    # ------------------------------------------------------------------
    # Main estimate
    # ------------------------------------------------------------------
    def estimate(self, features: dict[str, float | int | str | None], regime: RegimeDecision) -> IntensityDecision:
        minute = self._i(features.get("minute"), 0)
        total_goals = self._i(features.get("total_goals"), 0)
        goal_diff = self._i(features.get("goal_diff"), 0)
        red_cards = self._i(features.get("red_cards"), 0)

        pressure_home = self._f(features.get("pressure_home"))
        pressure_away = self._f(features.get("pressure_away"))
        pressure_total = self._f(features.get("pressure_total_qadj") or features.get("pressure_total"))

        openness = self._f(features.get("openness_qadj") or features.get("openness"))
        slowdown = self._f(features.get("slowdown_qadj") or features.get("slowdown"))
        chaos = self._f(features.get("chaos_qadj") or features.get("chaos"))

        recent_pressure_ratio = self._f(features.get("recent_pressure_ratio"))
        pressure_share_home = self._f(features.get("pressure_share_home"))
        pressure_share_away = self._f(features.get("pressure_share_away"))

        recent_pressure_home_raw = self._f(features.get("recent_pressure_home"))
        recent_pressure_away_raw = self._f(features.get("recent_pressure_away"))
        recent_pressure_home, recent_pressure_away, recent_pressure_fallback_used = self._recent_pressure_split(
            pressure_total=pressure_total,
            pressure_share_home=pressure_share_home,
            pressure_share_away=pressure_share_away,
            recent_pressure_ratio=recent_pressure_ratio,
            explicit_home=recent_pressure_home_raw,
            explicit_away=recent_pressure_away_raw,
        )

        trailer_chase = self._f(features.get("trailer_chase_signal"))
        leader_protect = self._f(features.get("leader_protect_signal"))
        set_piece_risk = self._f(features.get("set_piece_risk"))
        single_goal_risk = self._f(features.get("single_goal_risk"))
        danger_confirmation = self._f(features.get("danger_confirmation"))
        two_sided_liveness = self._f(features.get("two_sided_liveness"))
        fake_pressure_penalty = self._f(features.get("fake_pressure_penalty"))

        explicit_over_support = self._f(features.get("over_support_signal"))
        explicit_under_stability = self._f(features.get("under_stability_signal"))

        over_support_signal = (
            explicit_over_support
            if explicit_over_support > 0.0
            else self._derive_over_support_signal(
                openness=openness,
                two_sided_liveness=two_sided_liveness,
                danger_confirmation=danger_confirmation,
                recent_pressure_ratio=recent_pressure_ratio,
                trailer_chase=trailer_chase,
                set_piece_risk=set_piece_risk,
                slowdown=slowdown,
                fake_pressure_penalty=fake_pressure_penalty,
            )
        )

        under_stability_signal = (
            explicit_under_stability
            if explicit_under_stability > 0.0
            else self._derive_under_stability_signal(
                slowdown=slowdown,
                openness=openness,
                two_sided_liveness=two_sided_liveness,
                chaos=chaos,
                recent_pressure_ratio=recent_pressure_ratio,
                trailer_chase=trailer_chase,
                set_piece_risk=set_piece_risk,
                danger_confirmation=danger_confirmation,
                fake_pressure_penalty=fake_pressure_penalty,
            )
        )

        feed = self._f(features.get("feed_quality_score"), 0.58)
        comp = self._f(features.get("competition_quality_score"), 0.60)
        market = self._f(features.get("market_quality_score"), 0.62)
        missing_penalty = self._f(features.get("missing_data_penalty"), 0.0)

        quality = self._clamp(
            0.44 * feed + 0.28 * comp + 0.18 * market + 0.10 * (1.0 - missing_penalty),
            0.20,
            1.00,
        )
        quality_penalty = 1.0 - quality

        time_left = self._time_left_estimate(
            minute=minute,
            total_goals=total_goals,
            red_cards=red_cards,
            set_piece_risk=set_piece_risk,
            chaos=chaos,
        )

        regime_mult_home, regime_mult_away = self._regime_multipliers(regime.regime_label)

        home_trailing = 1.0 if goal_diff < 0 else 0.0
        away_trailing = 1.0 if goal_diff > 0 else 0.0
        one_goal_game = 1.0 if abs(goal_diff) == 1 else 0.0
        late_game = 1.0 if minute >= 70 else 0.0
        very_late = 1.0 if minute >= 80 else 0.0

        recent_ratio_scaled = self._clamp(recent_pressure_ratio / 1.25, 0.0, 1.30)

        # ------------------------------------------------------------------
        # Instantaneous next-5m base flow
        # ------------------------------------------------------------------
        base_home = (
            0.014
            + 0.014 * pressure_home
            + 0.020 * recent_pressure_home
            + 0.010 * openness
            + 0.008 * danger_confirmation
            + 0.007 * set_piece_risk
            + 0.006 * over_support_signal
            + 0.003 * chaos
            - 0.008 * under_stability_signal
            - 0.005 * fake_pressure_penalty
        )

        base_away = (
            0.014
            + 0.014 * pressure_away
            + 0.020 * recent_pressure_away
            + 0.010 * openness
            + 0.008 * danger_confirmation
            + 0.007 * set_piece_risk
            + 0.006 * over_support_signal
            + 0.003 * chaos
            - 0.008 * under_stability_signal
            - 0.005 * fake_pressure_penalty
        )

        # Chase override : trailing side doit survivre au gel
        home_chase_boost = (
            0.16 * home_trailing * trailer_chase
            + 0.08 * home_trailing * one_goal_game
            + 0.05 * home_trailing * late_game
            + 0.04 * home_trailing * recent_ratio_scaled
        )

        away_chase_boost = (
            0.16 * away_trailing * trailer_chase
            + 0.08 * away_trailing * one_goal_game
            + 0.05 * away_trailing * late_game
            + 0.04 * away_trailing * recent_ratio_scaled
        )

        # Le leader peut encore contrer
        home_counter_boost = 0.03 * (1.0 if goal_diff > 0 else 0.0) * max(0.0, 1.0 - leader_protect)
        away_counter_boost = 0.03 * (1.0 if goal_diff < 0 else 0.0) * max(0.0, 1.0 - leader_protect)

        # Vrai gel tardif : drag partiel, pas écrasement total
        lockdown_drag = 0.0
        if (regime.regime_label or "").upper() == "LATE_LOCKDOWN":
            lockdown_drag = 0.12
            lockdown_drag -= 0.06 * trailer_chase
            lockdown_drag -= 0.05 * recent_ratio_scaled
            lockdown_drag -= 0.04 * over_support_signal
            lockdown_drag = self._clamp(lockdown_drag, 0.00, 0.12)

        # Rouge : prudence, on évite l'emballement artificiel
        red_card_uncertainty_drag = 0.0
        if (regime.regime_label or "").upper() == "RED_CARD_DISTORTED":
            red_card_uncertainty_drag = 0.06 + 0.04 * min(1.0, red_cards / 2.0)

        quality_mult = 1.0 - 0.14 * quality_penalty

        lambda_home_next_5m = (
            base_home
            * regime_mult_home
            * quality_mult
            * (1.0 + home_chase_boost + home_counter_boost - lockdown_drag - red_card_uncertainty_drag)
        )

        lambda_away_next_5m = (
            base_away
            * regime_mult_away
            * quality_mult
            * (1.0 + away_chase_boost + away_counter_boost - lockdown_drag - red_card_uncertainty_drag)
        )

        # dead states seulement : damping tardif léger
        if minute >= 82 and (regime.regime_label or "").upper() in {"CLOSED_LOW_EVENT", "LATE_LOCKDOWN"}:
            lambda_home_next_5m *= 0.93
            lambda_away_next_5m *= 0.93

        lambda_home_next_5m = self._clamp(lambda_home_next_5m, 0.004, 0.36)
        lambda_away_next_5m = self._clamp(lambda_away_next_5m, 0.004, 0.36)

        # ------------------------------------------------------------------
        # Persistence to match end
        # ------------------------------------------------------------------
        persistence = self._clamp(
            0.88
            + 0.10 * recent_ratio_scaled
            + 0.10 * trailer_chase
            + 0.08 * over_support_signal
            + 0.04 * set_piece_risk
            + 0.03 * single_goal_risk
            - 0.10 * under_stability_signal
            - 0.08 * slowdown,
            0.72,
            1.32,
        )

        if very_late and one_goal_game and trailer_chase >= 0.40:
            persistence = self._clamp(persistence + 0.06, 0.72, 1.36)

        lambda_home_to_end = lambda_home_next_5m * (time_left / 5.0) * persistence
        lambda_away_to_end = lambda_away_next_5m * (time_left / 5.0) * persistence

        if goal_diff < 0:
            lambda_home_to_end *= 1.0 + 0.08 * trailer_chase
        elif goal_diff > 0:
            lambda_away_to_end *= 1.0 + 0.08 * trailer_chase

        lambda_home_to_end = self._clamp(lambda_home_to_end, 0.01, 2.30)
        lambda_away_to_end = self._clamp(lambda_away_to_end, 0.01, 2.30)

        return IntensityDecision(
            lambda_home_next_5m=round(lambda_home_next_5m, 4),
            lambda_away_next_5m=round(lambda_away_next_5m, 4),
            lambda_home_to_end=round(lambda_home_to_end, 4),
            lambda_away_to_end=round(lambda_away_to_end, 4),
            quality_penalty=round(quality_penalty, 4),
            diagnostics={
                "quality": round(quality, 4),
                "minute": minute,
                "time_left_estimated": round(time_left, 2),
                "added_time_estimated": round(
                    self._added_time_estimate(
                        minute=minute,
                        total_goals=total_goals,
                        red_cards=red_cards,
                        set_piece_risk=set_piece_risk,
                        chaos=chaos,
                    ),
                    2,
                ),
                "persistence": round(persistence, 4),
                "trailer_chase_signal": round(trailer_chase, 4),
                "over_support_signal": round(over_support_signal, 4),
                "under_stability_signal": round(under_stability_signal, 4),
                "single_goal_risk": round(single_goal_risk, 4),
                "pressure_total": round(pressure_total, 4),
                "recent_pressure_ratio": round(recent_pressure_ratio, 4),
                "recent_pressure_home": round(recent_pressure_home, 4),
                "recent_pressure_away": round(recent_pressure_away, 4),
                "recent_pressure_fallback_used": 1 if recent_pressure_fallback_used else 0,
                "home_pressure_share": round(pressure_share_home, 4),
                "away_pressure_share": round(pressure_share_away, 4),
            },
        )