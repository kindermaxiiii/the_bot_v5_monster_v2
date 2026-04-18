from __future__ import annotations

from math import isfinite
from typing import Any

from app.core.match_state import MatchState


class FeatureEngine:
    """
    Feature engine V7.

    Mission :
    - transformer l'état brut du live en signaux exploitables
    - mieux distinguer :
        * vrai gel / vrai lockdown
        * faux match fermé
        * chase state asymétrique
        * profil over respirable à 1 but requis
    - réduire le biais "late = lockdown"
    """

    # ------------------------------------------------------------------
    # Basic helpers
    # ------------------------------------------------------------------
    def _f(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            value = float(value)
            if not isfinite(value):
                return default
            return value
        except (TypeError, ValueError):
            return default

    def _i(self, value: Any, default: int = 0) -> int:
        try:
            if value is None:
                return default
            return int(value)
        except (TypeError, ValueError):
            return default

    def _clamp(self, x: float, lo: float = 0.0, hi: float = 1.0) -> float:
        return max(lo, min(hi, x))

    def _safe_div(self, a: float, b: float, default: float = 0.0) -> float:
        return default if abs(b) < 1e-12 else a / b

    def _share(self, part: float, total: float) -> float:
        return self._clamp(self._safe_div(part, total, 0.0))

    def _norm_gap(self, a: float, b: float) -> float:
        total = abs(a) + abs(b)
        if total <= 0.0:
            return 0.0
        return (a - b) / total

    def _abs_norm_gap(self, a: float, b: float) -> float:
        return abs(self._norm_gap(a, b))

    def _per_minute(self, value: float, minute: int) -> float:
        return self._safe_div(value, max(1.0, float(minute)), 0.0)

    def _team_stat(self, team: Any, name: str, default: float = 0.0) -> float:
        return self._f(getattr(team, name, default), default)

    def _window_value(self, state: MatchState, bucket: str, key: str, fallback: float = 0.0) -> float:
        windows = getattr(state, "stats_windows", {}) or {}
        block = windows.get(bucket) or {}
        if key in block and block.get(key) is not None:
            return self._f(block.get(key), fallback)
        return fallback

    def _phase_bucket(self, minute: int, phase: str) -> str:
        if (phase or "").upper() == "HT":
            return "HT"
        if minute <= 15:
            return "EARLY"
        if minute <= 30:
            return "EARLY_MID"
        if minute <= 45:
            return "LATE_1H"
        if minute <= 60:
            return "EARLY_2H"
        if minute <= 75:
            return "MID_2H"
        return "LATE_2H"

    # ------------------------------------------------------------------
    # Pace / recent proxies
    # ------------------------------------------------------------------
    def _expected_recent_share(self, minute: int) -> float:
        """
        Part attendue du volume cumulé qui pourrait raisonnablement se retrouver
        dans une fenêtre récente de 5 minutes selon le moment du match.
        """
        base = 5.0 / max(5.0, float(minute))
        if minute >= 80:
            base *= 1.38
        elif minute >= 70:
            base *= 1.26
        elif minute >= 55:
            base *= 1.14
        return self._clamp(base, 0.06, 0.28)

    def _recent_pressure_proxy(
        self,
        *,
        pressure: float,
        pressure_share: float,
        trailing_flag: int,
        openness: float,
        set_piece_risk: float,
        minute: int,
    ) -> float:
        recent_share = self._expected_recent_share(minute)
        uplift = (
            1.0
            + 0.42 * trailing_flag
            + 0.18 * pressure_share
            + 0.16 * openness
            + 0.12 * set_piece_risk
        )
        return pressure * recent_share * uplift

    # ------------------------------------------------------------------
    # Main build
    # ------------------------------------------------------------------
    def build(self, state: MatchState) -> dict[str, float | int | str | None]:
        h = state.home
        a = state.away

        minute = self._i(getattr(state, "minute", 0), 0)
        phase = str(getattr(state, "phase", "") or "")
        status = str(getattr(state, "status", "") or "")
        total_goals = self._i(getattr(state, "total_goals", 0), 0)
        goal_diff = self._i(getattr(state, "goal_diff", 0), 0)
        red_cards = self._i(getattr(state, "home_reds", 0), 0) + self._i(getattr(state, "away_reds", 0), 0)

        h_shots = self._team_stat(h, "shots_total")
        a_shots = self._team_stat(a, "shots_total")
        h_sot = self._team_stat(h, "shots_on_target")
        a_sot = self._team_stat(a, "shots_on_target")
        h_inside = self._team_stat(h, "shots_inside_box")
        a_inside = self._team_stat(a, "shots_inside_box")
        h_corners = self._team_stat(h, "corners")
        a_corners = self._team_stat(a, "corners")
        h_saves = self._team_stat(h, "saves")
        a_saves = self._team_stat(a, "saves")
        h_attacks = self._team_stat(h, "attacks")
        a_attacks = self._team_stat(a, "attacks")
        h_danger = self._team_stat(h, "dangerous_attacks")
        a_danger = self._team_stat(a, "dangerous_attacks")
        h_poss = self._team_stat(h, "possession", -1.0)
        a_poss = self._team_stat(a, "possession", -1.0)

        total_shots = h_shots + a_shots
        total_sot = h_sot + a_sot
        total_inside = h_inside + a_inside
        total_corners = h_corners + a_corners
        total_saves = h_saves + a_saves
        total_attacks = h_attacks + a_attacks
        total_danger = h_danger + a_danger

        shots_pm = self._per_minute(total_shots, minute)
        sot_pm = self._per_minute(total_sot, minute)
        inside_pm = self._per_minute(total_inside, minute)
        corners_pm = self._per_minute(total_corners, minute)
        danger_pm = self._per_minute(total_danger, minute)
        attacks_pm = self._per_minute(total_attacks, minute)

        # ------------------------------------------------------------------
        # Core pressure / threat
        # ------------------------------------------------------------------
        pressure_home = (
            0.38 * h_sot
            + 0.22 * h_inside
            + 0.12 * h_corners
            + 0.07 * h_shots
            + 0.12 * self._safe_div(h_danger, 10.0)
            + 0.05 * self._safe_div(h_attacks, 12.0)
            + 0.08 * max(0.0, a_saves)
        )
        pressure_away = (
            0.38 * a_sot
            + 0.22 * a_inside
            + 0.12 * a_corners
            + 0.07 * a_shots
            + 0.12 * self._safe_div(a_danger, 10.0)
            + 0.05 * self._safe_div(a_attacks, 12.0)
            + 0.08 * max(0.0, h_saves)
        )
        pressure_total = pressure_home + pressure_away
        pressure_share_home = self._share(pressure_home, pressure_total)
        pressure_share_away = self._share(pressure_away, pressure_total)
        pressure_asymmetry = self._abs_norm_gap(pressure_home, pressure_away)

        threat_home = 0.46 * h_sot + 0.30 * h_inside + 0.14 * h_corners + 0.10 * self._safe_div(h_danger, 10.0)
        threat_away = 0.46 * a_sot + 0.30 * a_inside + 0.14 * a_corners + 0.10 * self._safe_div(a_danger, 10.0)
        threat_total = threat_home + threat_away
        threat_share_home = self._share(threat_home, threat_total)
        threat_share_away = self._share(threat_away, threat_total)
        threat_asymmetry = self._abs_norm_gap(threat_home, threat_away)

        possession_gap = (h_poss - a_poss) if h_poss >= 0 and a_poss >= 0 else None
        possession_balance = abs(possession_gap) / 100.0 if possession_gap is not None else 0.0

        # ------------------------------------------------------------------
        # Quality / missingness
        # ------------------------------------------------------------------
        missing_components = 0
        for x in [
            total_shots,
            total_sot,
            total_inside,
            total_corners,
            total_attacks,
            total_danger,
            h_poss if h_poss >= 0 else None,
            a_poss if a_poss >= 0 else None,
        ]:
            if x is None or x == 0:
                missing_components += 1

        missing_data_penalty = self._clamp(missing_components / 11.0, 0.0, 0.40)

        feed_quality = self._f(getattr(state, "feed_quality_score", 0.58), 0.58)
        competition_quality = self._f(getattr(state, "competition_quality_score", 0.60), 0.60)
        market_quality = self._f(getattr(state, "market_quality_score", 0.62), 0.62)

        quality_adjustment = self._clamp(
            0.46 * feed_quality
            + 0.30 * competition_quality
            + 0.24 * market_quality
            - 0.18 * missing_data_penalty,
            0.24,
            1.00,
        )

        # ------------------------------------------------------------------
        # Structural descriptors
        # ------------------------------------------------------------------
        two_sided_liveness = self._clamp(
            0.32 * self._share(min(h_sot, 1.0) + min(a_sot, 1.0), 2.0)
            + 0.28 * self._clamp(sot_pm / 0.14)
            + 0.20 * self._clamp(danger_pm / 1.00)
            + 0.20 * (1.0 - pressure_asymmetry),
            0.0,
            1.0,
        )

        danger_confirmation = self._clamp(
            0.42 * self._share(total_inside, max(2.0, total_inside + 1.0))
            + 0.34 * self._share(total_sot, max(2.0, total_sot + 1.0))
            + 0.24 * self._share(total_corners, max(2.0, total_corners + 1.0))
        )

        sterile_total = self._clamp(
            0.44 * self._share(max(0.0, total_shots - total_sot), max(1.0, total_shots))
            + 0.32 * self._share(max(0.0, total_attacks - total_danger), max(1.0, total_attacks))
            + 0.24 * self._share(max(0.0, total_corners - total_inside), max(1.0, total_corners + total_inside))
        )

        openness = self._clamp(
            0.28 * self._clamp(shots_pm / 0.34)
            + 0.24 * self._clamp(sot_pm / 0.13)
            + 0.18 * self._clamp(inside_pm / 0.15)
            + 0.14 * self._clamp(corners_pm / 0.17)
            + 0.16 * self._clamp(danger_pm / 1.05)
        )

        pace_score = self._clamp(
            0.34 * self._clamp(pressure_total / 7.5)
            + 0.24 * self._clamp(threat_total / 6.6)
            + 0.20 * self._clamp(sot_pm / 0.15)
            + 0.12 * self._clamp(danger_pm / 1.00)
            + 0.10 * self._clamp(corners_pm / 0.16)
        )

        slowdown = self._clamp(1.0 - pace_score)

        chaos = self._clamp(
            0.22 * self._clamp(total_sot / 8.0)
            + 0.14 * self._clamp(total_inside / 10.0)
            + 0.10 * self._clamp(total_corners / 10.0)
            + 0.16 * self._clamp(abs(goal_diff) / 3.0)
            + 0.18 * self._clamp(red_cards / 2.0)
            + 0.10 * pressure_asymmetry
            + 0.10 * self._clamp(openness)
        )

        # ------------------------------------------------------------------
        # Score state
        # ------------------------------------------------------------------
        home_trailing_flag = 1 if goal_diff < 0 else 0
        away_trailing_flag = 1 if goal_diff > 0 else 0
        draw_game_flag = 1 if goal_diff == 0 else 0
        is_late_game = 1 if minute >= 70 else 0
        is_very_late_game = 1 if minute >= 80 else 0
        one_goal_game = 1 if abs(goal_diff) == 1 else 0

        # ------------------------------------------------------------------
        # Set-piece risk early because recent proxies use it
        # ------------------------------------------------------------------
        set_piece_risk = self._clamp(
            0.50 * self._share(total_corners, max(2.0, total_corners + 1.0))
            + 0.25 * self._share(total_inside, max(2.0, total_inside + 1.0))
            + 0.25 * self._clamp(corners_pm / 0.16)
        )

        # ------------------------------------------------------------------
        # Recent pressure windows / proxies
        # ------------------------------------------------------------------
        recent_pressure_home_proxy = self._recent_pressure_proxy(
            pressure=pressure_home,
            pressure_share=pressure_share_home,
            trailing_flag=home_trailing_flag,
            openness=openness,
            set_piece_risk=set_piece_risk,
            minute=minute,
        )
        recent_pressure_away_proxy = self._recent_pressure_proxy(
            pressure=pressure_away,
            pressure_share=pressure_share_away,
            trailing_flag=away_trailing_flag,
            openness=openness,
            set_piece_risk=set_piece_risk,
            minute=minute,
        )

        recent_pressure_home = self._window_value(state, "5m", "home_pressure", recent_pressure_home_proxy)
        recent_pressure_away = self._window_value(state, "5m", "away_pressure", recent_pressure_away_proxy)
        recent_pressure_total = recent_pressure_home + recent_pressure_away

        recent_corners_total = self._window_value(
            state,
            "5m",
            "corners_total",
            max(0.0, total_corners * self._expected_recent_share(minute) * (1.0 + 0.18 * is_late_game)),
        )

        expected_recent_pressure = max(
            0.35,
            pressure_total * self._expected_recent_share(minute),
        )

        recent_pressure_ratio = self._clamp(
            self._safe_div(recent_pressure_total, expected_recent_pressure, 0.0),
            0.0,
            2.2,
        )

        # ------------------------------------------------------------------
        # Chase / protect logic
        # ------------------------------------------------------------------
        trailer_pressure_share = 0.0
        trailer_threat_share = 0.0
        leader_protect_signal = 0.0
        trailer_chase_signal = 0.0

        if home_trailing_flag:
            trailer_pressure_share = pressure_share_home
            trailer_threat_share = threat_share_home
            leader_protect_signal = self._clamp(
                0.38 * pressure_share_away
                + 0.24 * self._share(a_corners + a_saves, max(1.0, total_corners + total_saves))
                + 0.18 * (1.0 if minute >= 70 else 0.0)
                + 0.20 * self._clamp(max(0.0, a_poss - h_poss) / 35.0 if h_poss >= 0 and a_poss >= 0 else 0.0)
            )
            trailer_chase_signal = self._clamp(
                0.28 * trailer_pressure_share
                + 0.22 * trailer_threat_share
                + 0.24 * self._clamp(recent_pressure_home / 2.8)
                + 0.12 * one_goal_game
                + 0.08 * is_late_game
                + 0.06 * set_piece_risk
            )
        elif away_trailing_flag:
            trailer_pressure_share = pressure_share_away
            trailer_threat_share = threat_share_away
            leader_protect_signal = self._clamp(
                0.38 * pressure_share_home
                + 0.24 * self._share(h_corners + h_saves, max(1.0, total_corners + total_saves))
                + 0.18 * (1.0 if minute >= 70 else 0.0)
                + 0.20 * self._clamp(max(0.0, h_poss - a_poss) / 35.0 if h_poss >= 0 and a_poss >= 0 else 0.0)
            )
            trailer_chase_signal = self._clamp(
                0.28 * trailer_pressure_share
                + 0.22 * trailer_threat_share
                + 0.24 * self._clamp(recent_pressure_away / 2.8)
                + 0.12 * one_goal_game
                + 0.08 * is_late_game
                + 0.06 * set_piece_risk
            )

        # ------------------------------------------------------------------
        # Goal risk descriptors
        # ------------------------------------------------------------------
        single_goal_risk = self._clamp(
            0.22 * self._clamp(recent_pressure_ratio / 1.4)
            + 0.22 * trailer_chase_signal
            + 0.16 * set_piece_risk
            + 0.14 * self._clamp(total_sot / 7.0)
            + 0.10 * self._clamp(total_inside / 8.0)
            + 0.08 * one_goal_game
            + 0.08 * self._clamp(openness)
        )

        over_support_signal = self._clamp(
            0.24 * self._clamp(recent_pressure_ratio / 1.4)
            + 0.20 * trailer_chase_signal
            + 0.18 * set_piece_risk
            + 0.14 * two_sided_liveness
            + 0.12 * self._clamp(openness)
            + 0.12 * self._clamp(danger_confirmation)
        )

        under_stability_signal = self._clamp(
            0.32 * slowdown
            + 0.18 * (1.0 - recent_pressure_ratio / 1.4)
            + 0.16 * (1.0 - set_piece_risk)
            + 0.16 * (1.0 - trailer_chase_signal)
            + 0.10 * (1.0 - openness)
            + 0.08 * (1.0 - chaos)
        )

        # important: lockdown only if real freeze, not just late + lead
        late_lockdown_signal = self._clamp(
            0.18 * (1.0 if minute >= 78 else 0.0)
            + 0.14 * one_goal_game
            + 0.22 * slowdown
            + 0.16 * (1.0 - recent_pressure_ratio / 1.4)
            + 0.12 * leader_protect_signal
            + 0.10 * (1.0 - set_piece_risk)
            + 0.08 * (1.0 - trailer_chase_signal)
        )

        # reduce fake pressure but don't over-penalize late attacking matches
        fake_pressure_penalty = self._clamp(
            0.44 * sterile_total
            + 0.20 * self._share(max(0.0, total_shots - total_inside), max(1.0, total_shots))
            + 0.16 * self._share(max(0.0, total_attacks - total_danger), max(1.0, total_attacks))
            - 0.10 * self._clamp(recent_pressure_ratio / 1.4)
            - 0.10 * trailer_chase_signal,
            0.0,
            1.0,
        )

        # Quality-adjusted versions
        pressure_total_qadj = pressure_total * quality_adjustment
        threat_total_qadj = threat_total * quality_adjustment
        openness_qadj = openness * quality_adjustment
        slowdown_qadj = self._clamp(slowdown + 0.14 * (1.0 - quality_adjustment), 0.0, 1.0)
        chaos_qadj = self._clamp(chaos + 0.08 * (1.0 - quality_adjustment), 0.0, 1.0)

        return {
            "minute": minute,
            "phase": phase,
            "status": status,
            "phase_bucket": self._phase_bucket(minute, phase),

            "total_goals": total_goals,
            "goal_diff": goal_diff,
            "red_cards": red_cards,

            "total_shots": total_shots,
            "total_shots_on": total_sot,
            "total_inside_box": total_inside,
            "total_corners": total_corners,
            "total_attacks": total_attacks,
            "total_dangerous_attacks": total_danger,

            "shots_on_per_minute": sot_pm,
            "danger_per_minute": danger_pm,
            "attacks_per_minute": attacks_pm,
            "corners_per_minute": corners_pm,

            "pressure_home": pressure_home,
            "pressure_away": pressure_away,
            "pressure_total": pressure_total,
            "pressure_total_qadj": pressure_total_qadj,
            "pressure_share_home": pressure_share_home,
            "pressure_share_away": pressure_share_away,
            "pressure_asymmetry": pressure_asymmetry,

            "threat_home": threat_home,
            "threat_away": threat_away,
            "threat_total": threat_total,
            "threat_total_qadj": threat_total_qadj,
            "threat_share_home": threat_share_home,
            "threat_share_away": threat_share_away,
            "threat_asymmetry": threat_asymmetry,

            "openness": openness,
            "openness_qadj": openness_qadj,

            "slowdown": slowdown,
            "slowdown_qadj": slowdown_qadj,

            "chaos": chaos,
            "chaos_qadj": chaos_qadj,

            "two_sided_liveness": two_sided_liveness,
            "danger_confirmation": danger_confirmation,
            "sterile_total": sterile_total,
            "fake_pressure_penalty": fake_pressure_penalty,

            "possession_gap": possession_gap,
            "possession_balance": possession_balance,

            "home_trailing_flag": home_trailing_flag,
            "away_trailing_flag": away_trailing_flag,
            "draw_game_flag": draw_game_flag,
            "is_late_game": is_late_game,
            "is_very_late_game": is_very_late_game,
            "one_goal_game": one_goal_game,

            "trailer_pressure_share": trailer_pressure_share,
            "trailer_threat_share": trailer_threat_share,
            "leader_protect_signal": leader_protect_signal,
            "trailer_chase_signal": trailer_chase_signal,

            "set_piece_risk": set_piece_risk,
            "single_goal_risk": single_goal_risk,
            "over_support_signal": over_support_signal,
            "under_stability_signal": under_stability_signal,

            "recent_pressure_home": recent_pressure_home,
            "recent_pressure_away": recent_pressure_away,
            "recent_pressure_total": recent_pressure_total,
            "recent_pressure_ratio": recent_pressure_ratio,

            "late_lockdown_signal": late_lockdown_signal,

            "feed_quality_score": feed_quality,
            "competition_quality_score": competition_quality,
            "market_quality_score": market_quality,
            "missing_data_penalty": missing_data_penalty,
            "quality_adjustment": quality_adjustment,
        }

        