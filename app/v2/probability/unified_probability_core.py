from __future__ import annotations

from app.utils.math_tools import adaptive_poisson_cap, safe_non_negative, truncated_poisson_distribution
from app.v2.contracts import MatchIntelligenceSnapshot, ProbabilityState


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class UnifiedProbabilityCore:
    """
    Phase 1 unified probability core.

    It translates the intelligence snapshot into one shared score distribution
    and derived marginal distributions, without embedding market-specific logic.
    """

    def _share(self, home_force: float, away_force: float) -> tuple[float, float]:
        total_force = max(1e-9, home_force + away_force)
        return home_force / total_force, away_force / total_force

    def _forces(self, snapshot: MatchIntelligenceSnapshot) -> tuple[float, float]:
        home_force = 0.30 + 0.45 * snapshot.threat_home + 0.25 * snapshot.pressure_home
        away_force = 0.30 + 0.45 * snapshot.threat_away + 0.25 * snapshot.pressure_away

        if snapshot.home_goals < snapshot.away_goals:
            home_force *= 1.07
        elif snapshot.home_goals > snapshot.away_goals:
            away_force *= 1.07

        if snapshot.regime_label == "ASYMMETRIC_SIEGE_HOME":
            home_force *= 1.10
        elif snapshot.regime_label == "ASYMMETRIC_SIEGE_AWAY":
            away_force *= 1.10
        elif snapshot.regime_label == "LATE_LOCKDOWN":
            home_force *= 0.94
            away_force *= 0.94

        return home_force, away_force

    def _horizon_lambdas(
        self,
        snapshot: MatchIntelligenceSnapshot,
    ) -> tuple[float, float, float, float, float, float]:
        home_force, away_force = self._forces(snapshot)
        home_share, away_share = self._share(home_force, away_force)

        if snapshot.minute < 45:
            lambda_ht_total = safe_non_negative(snapshot.remaining_goal_expectancy)
            second_half_multiplier = _clamp(
                0.55
                + 0.20 * snapshot.openness
                + 0.10 * snapshot.chaos
                - 0.10 * snapshot.slowdown,
                0.35,
                0.95,
            )
            lambda_total = lambda_ht_total * (1.0 + second_half_multiplier)
        else:
            lambda_total = safe_non_negative(snapshot.remaining_goal_expectancy)
            lambda_ht_total = 0.0

        lambda_home = lambda_total * home_share
        lambda_away = lambda_total * away_share
        lambda_ht_home = lambda_ht_total * home_share
        lambda_ht_away = lambda_ht_total * away_share
        return lambda_home, lambda_away, lambda_total, lambda_ht_home, lambda_ht_away, lambda_ht_total

    def _convolve_totals(self, home_dist: dict[int, float], away_dist: dict[int, float]) -> dict[int, float]:
        out: dict[int, float] = {}
        for home_add, p_home in home_dist.items():
            for away_add, p_away in away_dist.items():
                total_add = int(home_add) + int(away_add)
                out[total_add] = out.get(total_add, 0.0) + float(p_home) * float(p_away)
        mass = sum(out.values()) or 1.0
        return {k: v / mass for k, v in sorted(out.items(), key=lambda item: item[0])}

    def _build_horizon_distributions(
        self,
        *,
        home_goals: int,
        away_goals: int,
        lambda_home: float,
        lambda_away: float,
        minute: int,
        floor_cap: int,
        ceil_cap: int,
    ) -> tuple[dict[str, float], dict[int, float], dict[int, float], dict[int, float], int]:
        lambda_total = safe_non_negative(lambda_home + lambda_away)
        cap = adaptive_poisson_cap(lambda_total, minute=minute, floor_cap=floor_cap, ceil_cap=ceil_cap)

        home_extra = truncated_poisson_distribution(lambda_home, cap)
        away_extra = truncated_poisson_distribution(lambda_away, cap)
        remaining_added_goal_probs = self._convolve_totals(home_extra, away_extra)

        score_grid: dict[str, float] = {}
        home_goal_probs: dict[int, float] = {}
        away_goal_probs: dict[int, float] = {}

        for home_add, p_home in home_extra.items():
            for away_add, p_away in away_extra.items():
                mass = float(p_home) * float(p_away)
                if mass <= 0.0:
                    continue

                home_final = home_goals + int(home_add)
                away_final = away_goals + int(away_add)
                score_grid[f"{home_final}-{away_final}"] = score_grid.get(f"{home_final}-{away_final}", 0.0) + mass
                home_goal_probs[home_final] = home_goal_probs.get(home_final, 0.0) + mass
                away_goal_probs[away_final] = away_goal_probs.get(away_final, 0.0) + mass

        total_mass = sum(score_grid.values()) or 1.0
        score_grid = {
            key: value / total_mass
            for key, value in sorted(score_grid.items(), key=lambda item: item[1], reverse=True)
        }
        home_goal_probs = {key: value / total_mass for key, value in sorted(home_goal_probs.items(), key=lambda item: item[0])}
        away_goal_probs = {key: value / total_mass for key, value in sorted(away_goal_probs.items(), key=lambda item: item[0])}
        remaining_added_goal_probs = {
            key: value / total_mass for key, value in sorted(remaining_added_goal_probs.items(), key=lambda item: item[0])
        }
        return score_grid, remaining_added_goal_probs, home_goal_probs, away_goal_probs, cap

    def build(self, snapshot: MatchIntelligenceSnapshot) -> ProbabilityState:
        (
            lambda_home,
            lambda_away,
            lambda_total,
            lambda_ht_home,
            lambda_ht_away,
            lambda_ht_total,
        ) = self._horizon_lambdas(snapshot)
        (
            ft_score_grid,
            remaining_added_goal_probs,
            home_goal_probs,
            away_goal_probs,
            ft_cap,
        ) = self._build_horizon_distributions(
            home_goals=snapshot.home_goals,
            away_goals=snapshot.away_goals,
            lambda_home=lambda_home,
            lambda_away=lambda_away,
            minute=snapshot.minute,
            floor_cap=6,
            ceil_cap=12,
        )
        current_total = snapshot.home_goals + snapshot.away_goals
        final_total_goal_probs = {
            current_total + added_goals: prob
            for added_goals, prob in remaining_added_goal_probs.items()
        }
        (
            ht_score_grid,
            ht_remaining_added_goal_probs,
            _,
            _,
            ht_cap,
        ) = self._build_horizon_distributions(
            home_goals=snapshot.home_goals,
            away_goals=snapshot.away_goals,
            lambda_home=lambda_ht_home,
            lambda_away=lambda_ht_away,
            minute=min(snapshot.minute, 45),
            floor_cap=3,
            ceil_cap=8,
        )
        ht_final_total_goal_probs = {
            current_total + added_goals: prob
            for added_goals, prob in ht_remaining_added_goal_probs.items()
        }

        uncertainty_score = _clamp(
            1.0
            - (
                0.42 * snapshot.feed_quality
                + 0.24 * snapshot.market_quality
                + 0.20 * snapshot.regime_confidence
                + 0.14 * (1.0 - snapshot.chaos * 0.35)
            ),
            0.02,
            0.95,
        )

        diagnostics = {
            "probability_mass": sum(ft_score_grid.values()),
            "distribution_cap": ft_cap,
            "ht_distribution_cap": ht_cap,
            "home_share": 0.0 if lambda_total <= 0.0 else lambda_home / lambda_total,
            "away_share": 0.0 if lambda_total <= 0.0 else lambda_away / lambda_total,
            "remaining_goal_expectancy": lambda_total,
            "ht_remaining_goal_expectancy": lambda_ht_total,
        }

        return ProbabilityState(
            fixture_id=snapshot.fixture_id,
            minute=snapshot.minute,
            score=snapshot.score,
            home_goals=snapshot.home_goals,
            away_goals=snapshot.away_goals,
            lambda_home_remaining=lambda_home,
            lambda_away_remaining=lambda_away,
            lambda_total_remaining=lambda_total,
            ft_score_grid=ft_score_grid,
            remaining_added_goal_probs=remaining_added_goal_probs,
            final_total_goal_probs=final_total_goal_probs,
            home_goal_probs=home_goal_probs,
            away_goal_probs=away_goal_probs,
            uncertainty_score=uncertainty_score,
            diagnostics=diagnostics,
            lambda_ht_home_remaining=lambda_ht_home,
            lambda_ht_away_remaining=lambda_ht_away,
            lambda_ht_total_remaining=lambda_ht_total,
            ht_score_grid=ht_score_grid,
            ht_remaining_added_goal_probs=ht_remaining_added_goal_probs,
            ht_final_total_goal_probs=ht_final_total_goal_probs,
        )
