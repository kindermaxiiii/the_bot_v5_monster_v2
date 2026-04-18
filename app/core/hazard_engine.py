from __future__ import annotations

from math import exp, isfinite

from app.core.contracts import HazardDecision, IntensityDecision


class HazardEngine:
    """
    Hazard engine V6.

    Mission :
    transformer les intensités (lambdas) en probabilités exploitables :
    - but dans les 5 prochaines minutes
    - but dans les 10 prochaines minutes
    - but domicile d'ici la fin
    - but extérieur d'ici la fin
    - espérance totale de buts restante

    Principes :
    - cohérence mathématique
    - prudence live
    - utilisation du temps restant réel
    - légère prise en compte de la qualité
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
            return value
        except (TypeError, ValueError):
            return default

    def _clamp(self, x: float, lo: float = 0.0, hi: float = 1.0) -> float:
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _poisson_at_least_one(self, lam: float) -> float:
        lam = max(0.0, lam)
        if lam >= 20.0:
            return 1.0
        return 1.0 - exp(-lam)

    def _poisson_exactly_zero(self, lam: float) -> float:
        lam = max(0.0, lam)
        if lam >= 20.0:
            return 0.0
        return exp(-lam)

    def _poisson_exactly_one(self, lam: float) -> float:
        lam = max(0.0, lam)
        if lam >= 20.0:
            return 0.0
        return lam * exp(-lam)

    def _poisson_exactly_two(self, lam: float) -> float:
        lam = max(0.0, lam)
        if lam >= 20.0:
            return 0.0
        return (lam * lam / 2.0) * exp(-lam)

    def _time_left_estimated(self, intensity: IntensityDecision) -> float:
        diagnostics = getattr(intensity, "diagnostics", {}) or {}
        return max(1.0, self._f(diagnostics.get("time_left_estimated"), 10.0))

    def _quality_penalty(self, intensity: IntensityDecision) -> float:
        return self._clamp(self._f(getattr(intensity, "quality_penalty", 0.0), 0.0), 0.0, 1.0)

    def _effective_next10_multiplier(
        self,
        lambda_total_next_5m: float,
        lambda_total_to_end: float,
        time_left_estimated: float,
        quality_penalty: float,
    ) -> float:
        """
        Construit un next-10 prudent :
        - respecte le temps restant
        - respecte la masse totale restante
        - se compresse un peu si la qualité est faible
        """
        horizon_factor = self._clamp(time_left_estimated / 10.0, 0.20, 1.00)
        quality_drag = 1.0 - 0.10 * quality_penalty

        raw_mult = 1.85 * horizon_factor * quality_drag

        if lambda_total_next_5m < 0.03:
            raw_mult = min(raw_mult, 1.55)
        elif lambda_total_next_5m > 0.35:
            raw_mult = min(raw_mult, 1.72)

        if time_left_estimated <= 5.0:
            raw_mult = min(raw_mult, 1.22)
        elif time_left_estimated <= 7.5:
            raw_mult = min(raw_mult, 1.45)

        # Jamais en dessous du prochain 5m, jamais au-dessus du reliquat quasi total
        raw_lambda_total_next_10m = lambda_total_next_5m * max(1.0, raw_mult)
        raw_lambda_total_next_10m = min(raw_lambda_total_next_10m, lambda_total_to_end * 0.985)

        if lambda_total_to_end <= 0.0:
            return 1.0

        return max(1.0, raw_lambda_total_next_10m / max(lambda_total_next_5m, 1e-12))

    # ------------------------------------------------------------------
    # Main
    # ------------------------------------------------------------------
    def derive(self, intensity: IntensityDecision) -> HazardDecision:
        lambda_home_next_5m = max(0.0, self._f(intensity.lambda_home_next_5m))
        lambda_away_next_5m = max(0.0, self._f(intensity.lambda_away_next_5m))
        lambda_home_to_end = max(0.0, self._f(intensity.lambda_home_to_end))
        lambda_away_to_end = max(0.0, self._f(intensity.lambda_away_to_end))

        lambda_total_next_5m = lambda_home_next_5m + lambda_away_next_5m
        lambda_total_to_end = lambda_home_to_end + lambda_away_to_end

        time_left_estimated = self._time_left_estimated(intensity)
        quality_penalty = self._quality_penalty(intensity)

        # --------------------------------------------------------------
        # Construction prudente du next 10m
        # --------------------------------------------------------------
        next10_multiplier = self._effective_next10_multiplier(
            lambda_total_next_5m=lambda_total_next_5m,
            lambda_total_to_end=lambda_total_to_end,
            time_left_estimated=time_left_estimated,
            quality_penalty=quality_penalty,
        )

        lambda_total_next_10m = lambda_total_next_5m * next10_multiplier
        lambda_total_next_10m = min(lambda_total_next_10m, lambda_total_to_end * 0.985)
        lambda_total_next_10m = max(lambda_total_next_5m, lambda_total_next_10m)

        # Répartition home/away selon le share du next 5m
        share_home_next5 = lambda_home_next_5m / max(lambda_total_next_5m, 1e-12)
        share_away_next5 = lambda_away_next_5m / max(lambda_total_next_5m, 1e-12)

        lambda_home_next_10m = lambda_total_next_10m * share_home_next5
        lambda_away_next_10m = lambda_total_next_10m * share_away_next5

        # --------------------------------------------------------------
        # Hazards
        # --------------------------------------------------------------
        goal_hazard_next_5m = self._poisson_at_least_one(lambda_total_next_5m)
        goal_hazard_next_10m = self._poisson_at_least_one(lambda_total_next_10m)

        home_goal_hazard_to_end = self._poisson_at_least_one(lambda_home_to_end)
        away_goal_hazard_to_end = self._poisson_at_least_one(lambda_away_to_end)

        total_goal_hazard_to_end = self._poisson_at_least_one(lambda_total_to_end)
        no_goal_hazard_to_end = self._poisson_exactly_zero(lambda_total_to_end)

        # --------------------------------------------------------------
        # Diagnostics plus poussés
        # --------------------------------------------------------------
        exactly_one_goal_to_end = self._poisson_exactly_one(lambda_total_to_end)
        exactly_two_goals_to_end = self._poisson_exactly_two(lambda_total_to_end)
        three_plus_goals_to_end = self._clamp(
            1.0 - no_goal_hazard_to_end - exactly_one_goal_to_end - exactly_two_goals_to_end,
            0.0,
            1.0,
        )

        both_teams_score_to_end = self._clamp(
            home_goal_hazard_to_end * away_goal_hazard_to_end,
            0.0,
            1.0,
        )

        home_goal_share_to_end = lambda_home_to_end / max(lambda_total_to_end, 1e-12)
        away_goal_share_to_end = lambda_away_to_end / max(lambda_total_to_end, 1e-12)
        concentration_score = self._clamp(abs(home_goal_share_to_end - away_goal_share_to_end), 0.0, 1.0)

        diagnostics = {
            # Lambdas
            "lambda_home_next_5m": round(lambda_home_next_5m, 6),
            "lambda_away_next_5m": round(lambda_away_next_5m, 6),
            "lambda_total_next_5m": round(lambda_total_next_5m, 6),
            "lambda_home_next_10m": round(lambda_home_next_10m, 6),
            "lambda_away_next_10m": round(lambda_away_next_10m, 6),
            "lambda_total_next_10m": round(lambda_total_next_10m, 6),
            "lambda_home_to_end": round(lambda_home_to_end, 6),
            "lambda_away_to_end": round(lambda_away_to_end, 6),
            "lambda_total_to_end": round(lambda_total_to_end, 6),

            # Horizon / quality
            "time_left_estimated": round(time_left_estimated, 4),
            "effective_next10_minutes": round(min(10.0, time_left_estimated), 4),
            "quality_penalty": round(quality_penalty, 6),
            "next10_multiplier": round(next10_multiplier, 6),

            # Short-term hazards
            "home_goal_hazard_next_5m": round(self._poisson_at_least_one(lambda_home_next_5m), 6),
            "away_goal_hazard_next_5m": round(self._poisson_at_least_one(lambda_away_next_5m), 6),
            "home_goal_hazard_next_10m": round(self._poisson_at_least_one(lambda_home_next_10m), 6),
            "away_goal_hazard_next_10m": round(self._poisson_at_least_one(lambda_away_next_10m), 6),

            # Rest-of-match structure
            "total_goal_hazard_to_end": round(total_goal_hazard_to_end, 6),
            "no_goal_hazard_to_end": round(no_goal_hazard_to_end, 6),
            "exactly_one_goal_to_end": round(exactly_one_goal_to_end, 6),
            "exactly_two_goals_to_end": round(exactly_two_goals_to_end, 6),
            "three_plus_goals_to_end": round(three_plus_goals_to_end, 6),

            # By-team balance
            "home_goal_share_to_end": round(home_goal_share_to_end, 6),
            "away_goal_share_to_end": round(away_goal_share_to_end, 6),
            "concentration_score": round(concentration_score, 6),
            "both_teams_score_to_end_proxy": round(both_teams_score_to_end, 6),
        }

        return HazardDecision(
            goal_hazard_next_5m=round(goal_hazard_next_5m, 6),
            goal_hazard_next_10m=round(goal_hazard_next_10m, 6),
            home_goal_hazard_to_end=round(home_goal_hazard_to_end, 6),
            away_goal_hazard_to_end=round(away_goal_hazard_to_end, 6),
            total_goal_expectancy_remaining=round(lambda_total_to_end, 6),
            diagnostics=diagnostics,
        )