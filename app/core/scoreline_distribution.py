from __future__ import annotations

from math import isfinite, sqrt

from app.config import settings
from app.core.contracts import HazardDecision, IntensityDecision, ScorelineDistribution
from app.core.match_state import MatchState
from app.utils.math_tools import poisson_pmf


class ScorelineDistributionEngine:
    """
    Distribution V8.

    Objectifs :
    - utiliser les lambdas side issues d'intensity comme vérité de forme
    - ne réconcilier qu'avec la masse totale restante du hazard
    - éviter toute confusion probabilité <-> lambda
    - garder un late jump asymétrique et stable
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

    def _i(self, value, default: int = 0) -> int:
        try:
            if value is None:
                return default
            return int(value)
        except (TypeError, ValueError):
            return default

    def _clamp(self, x: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, x))

    def _normalize(self, probs: dict[int, float]) -> dict[int, float]:
        total = sum(float(v) for v in probs.values()) or 1.0
        return {k: float(v) / total for k, v in probs.items()}

    def _total_goal_probs_from_remaining(
        self,
        home_extra: dict[int, float],
        away_extra: dict[int, float],
    ) -> dict[int, float]:
        totals: dict[int, float] = {}
        for h_add, p_h in (home_extra or {}).items():
            for a_add, p_a in (away_extra or {}).items():
                total_add = int(h_add) + int(a_add)
                totals[total_add] = totals.get(total_add, 0.0) + self._f(p_h) * self._f(p_a)
        return self._normalize(totals)

    # ------------------------------------------------------------------
    # Cap
    # ------------------------------------------------------------------
    def _cap(self, lam_home: float, lam_away: float, hazard_total: float, minute: int) -> int:
        lam_total = max(0.0, lam_home + lam_away, hazard_total)
        raw = lam_total + 4.0 * sqrt(max(lam_total, 0.10))

        if minute >= 80:
            raw -= 1.0
        elif minute <= 25:
            raw += 1.0

        return max(6, min(12, int(round(raw))))

    # ------------------------------------------------------------------
    # Poisson helpers
    # ------------------------------------------------------------------
    def _truncated_poisson(self, lam: float, max_extra: int) -> dict[int, float]:
        lam = max(0.0, self._f(lam))
        probs: dict[int, float] = {}
        cumulative = 0.0

        for k in range(max_extra):
            p = max(0.0, self._f(poisson_pmf(k, lam)))
            probs[k] = p
            cumulative += p

        probs[max_extra] = max(0.0, 1.0 - cumulative)
        return self._normalize(probs)

    def _mix_distributions(self, base: dict[int, float], alt: dict[int, float], w_alt: float) -> dict[int, float]:
        w_alt = self._clamp(w_alt, 0.0, 0.40)
        w_base = 1.0 - w_alt
        keys = set(base) | set(alt)
        return self._normalize({k: w_base * base.get(k, 0.0) + w_alt * alt.get(k, 0.0) for k in keys})

    # ------------------------------------------------------------------
    # Reconciliation helpers
    # ------------------------------------------------------------------
    def _reconcile_total_only(
        self,
        lam_home: float,
        lam_away: float,
        hazard: HazardDecision,
    ) -> tuple[float, float]:
        """
        IMPORTANT :
        - lam_home / lam_away = lambdas
        - hazard.home_goal_hazard_to_end / away_goal_hazard_to_end = probabilités
        => on NE DOIT PAS les comparer directement.

        Ici on garde la forme side issue d'intensity,
        et on ne réconcilie que le total avec total_goal_expectancy_remaining.
        """
        lam_home = max(0.0, self._f(lam_home))
        lam_away = max(0.0, self._f(lam_away))
        hazard_total = max(0.0, self._f(getattr(hazard, "total_goal_expectancy_remaining", 0.0)))

        lam_total = lam_home + lam_away
        if lam_total > 0.0 and hazard_total > 0.0:
            total_scale = self._clamp(hazard_total / lam_total, 0.90, 1.12)
            lam_home *= total_scale
            lam_away *= total_scale

        return lam_home, lam_away

    def _late_jump_multipliers(
        self,
        state: MatchState,
        intensity: IntensityDecision,
        hazard: HazardDecision,
    ) -> tuple[float, float, float]:
        minute = self._i(getattr(state, "minute", 0), 0)
        goal_diff = self._i(getattr(state, "goal_diff", 0), 0)

        base_blend = self._f(getattr(settings, "late_state_jump_blend", 0.12), 0.12)
        base_blend = self._clamp(base_blend, 0.0, 0.35)

        if minute < self._i(getattr(settings, "late_state_jump_minute", 68), 68):
            return 1.0, 1.0, 0.0

        if abs(goal_diff) > self._i(getattr(settings, "late_state_jump_max_goal_diff", 2), 2):
            return 1.0, 1.0, 0.0

        hazard_total = max(0.0, self._f(getattr(hazard, "total_goal_expectancy_remaining", 0.0)))
        next10 = max(0.0, self._f(getattr(hazard, "goal_hazard_next_10m", 0.0)))

        diag = getattr(intensity, "diagnostics", {}) or {}
        trailer_chase = self._f(diag.get("trailer_chase_signal", 0.0), 0.0)

        urgency = self._clamp(
            0.42 * self._clamp(hazard_total / 1.60, 0.0, 1.0)
            + 0.33 * self._clamp(next10 / 0.85, 0.0, 1.0)
            + 0.15 * trailer_chase
            + 0.10 * (1.0 if minute >= 80 else 0.0),
            0.0,
            1.0,
        )

        blend = self._clamp(base_blend + 0.08 * urgency, 0.0, 0.38)

        if goal_diff == 0:
            mult = 1.05 + 0.06 * urgency
            return mult, mult, blend

        if goal_diff > 0:
            # home mène, away chase
            home_mult = 1.02 + 0.03 * urgency
            away_mult = 1.10 + 0.11 * urgency + 0.04 * trailer_chase
            return self._clamp(home_mult, 1.00, 1.10), self._clamp(away_mult, 1.06, 1.28), blend

        # away mène, home chase
        home_mult = 1.10 + 0.11 * urgency + 0.04 * trailer_chase
        away_mult = 1.02 + 0.03 * urgency
        return self._clamp(home_mult, 1.06, 1.28), self._clamp(away_mult, 1.00, 1.10), blend

    # ------------------------------------------------------------------
    # Main
    # ------------------------------------------------------------------
    def build(self, state: MatchState, intensity: IntensityDecision, hazard: HazardDecision) -> ScorelineDistribution:
        minute = self._i(getattr(state, "minute", 0), 0)
        home_goals_now = self._i(getattr(state, "home_goals", 0), 0)
        away_goals_now = self._i(getattr(state, "away_goals", 0), 0)

        lam_home = max(0.0, self._f(intensity.lambda_home_to_end))
        lam_away = max(0.0, self._f(intensity.lambda_away_to_end))

        hazard_total = max(0.0, self._f(getattr(hazard, "total_goal_expectancy_remaining", 0.0)))
        lam_home, lam_away = self._reconcile_total_only(lam_home, lam_away, hazard)

        max_extra = self._cap(lam_home, lam_away, hazard_total, minute)

        # Base distributions
        home_extra = self._truncated_poisson(lam_home, max_extra)
        away_extra = self._truncated_poisson(lam_away, max_extra)

        # Late-state jump asymétrique
        home_jump_mult, away_jump_mult, blend = self._late_jump_multipliers(state, intensity, hazard)
        if blend > 0.0:
            jump_home = self._truncated_poisson(lam_home * home_jump_mult + 0.02, max_extra)
            jump_away = self._truncated_poisson(lam_away * away_jump_mult + 0.02, max_extra)
            home_extra = self._mix_distributions(home_extra, jump_home, blend)
            away_extra = self._mix_distributions(away_extra, jump_away, blend)

        remaining_home_goal_probs = dict(sorted(home_extra.items(), key=lambda kv: kv[0]))
        remaining_away_goal_probs = dict(sorted(away_extra.items(), key=lambda kv: kv[0]))
        remaining_total_goal_probs = dict(
            sorted(self._total_goal_probs_from_remaining(home_extra, away_extra).items(), key=lambda kv: kv[0])
        )

        final_score_probs: dict[str, float] = {}
        final_home_goal_probs: dict[int, float] = {}
        final_away_goal_probs: dict[int, float] = {}

        home_win = 0.0
        draw = 0.0
        away_win = 0.0
        btts_yes = 0.0

        for h_add, p_h in home_extra.items():
            for a_add, p_a in away_extra.items():
                p = p_h * p_a
                if p <= 0:
                    continue

                h_final = home_goals_now + h_add
                a_final = away_goals_now + a_add

                final_home_goal_probs[h_final] = final_home_goal_probs.get(h_final, 0.0) + p
                final_away_goal_probs[a_final] = final_away_goal_probs.get(a_final, 0.0) + p

                key = f"{h_final}-{a_final}"
                final_score_probs[key] = final_score_probs.get(key, 0.0) + p

                if h_final > a_final:
                    home_win += p
                elif h_final == a_final:
                    draw += p
                else:
                    away_win += p

                if h_final > 0 and a_final > 0:
                    btts_yes += p

        total_mass = home_win + draw + away_win
        total_mass = total_mass if total_mass > 0 else 1.0

        final_score_probs = {k: v / total_mass for k, v in final_score_probs.items()}
        final_home_goal_probs = {k: v / total_mass for k, v in final_home_goal_probs.items()}
        final_away_goal_probs = {k: v / total_mass for k, v in final_away_goal_probs.items()}

        final_home_goal_probs = dict(sorted(final_home_goal_probs.items(), key=lambda kv: kv[0]))
        final_away_goal_probs = dict(sorted(final_away_goal_probs.items(), key=lambda kv: kv[0]))
        final_score_probs = dict(sorted(final_score_probs.items(), key=lambda kv: kv[1], reverse=True))

        home_win_prob = home_win / total_mass
        draw_prob = draw / total_mass
        away_win_prob = away_win / total_mass
        btts_yes_prob = btts_yes / total_mass

        return ScorelineDistribution(
            home_goal_probs=final_home_goal_probs,
            away_goal_probs=final_away_goal_probs,
            final_score_probs=final_score_probs,
            home_win_prob=round(home_win_prob, 6),
            draw_prob=round(draw_prob, 6),
            away_win_prob=round(away_win_prob, 6),
            btts_yes_prob=round(btts_yes_prob, 6),
            btts_no_prob=round(1.0 - btts_yes_prob, 6),
            remaining_home_goal_probs=remaining_home_goal_probs,
            remaining_away_goal_probs=remaining_away_goal_probs,
            remaining_total_goal_probs=remaining_total_goal_probs,
            lambda_home_remaining=round(lam_home, 6),
            lambda_away_remaining=round(lam_away, 6),
            lambda_total_remaining=round(lam_home + lam_away, 6),
        )
