from __future__ import annotations

from math import isfinite

from app.core.contracts import RegimeDecision


class RegimeEngine:
    """
    Regime engine V7.

    Objectif :
    - réduire le faux classement "match fermé"
    - rendre LATE_LOCKDOWN rare et mérité
    - mieux distinguer :
        * OPEN_EXCHANGE
        * ASYMMETRIC_SIEGE
        * CONTROLLED_PRESSURE
        * vrai CLOSED_LOW_EVENT
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

    def _clamp(self, x: float, lo: float = 0.0, hi: float = 1.0) -> float:
        return max(lo, min(hi, x))

    def _lin(self, value: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return self._clamp((value - lo) / (hi - lo), 0.0, 1.0)

    # ------------------------------------------------------------------
    # Core classification
    # ------------------------------------------------------------------
    def classify(self, features: dict[str, float | int | str | None]) -> RegimeDecision:
        minute = self._i(features.get("minute"), 0)
        total_goals = self._i(features.get("total_goals"), 0)
        goal_diff = self._i(features.get("goal_diff"), 0)
        red_cards = self._i(features.get("red_cards"), 0)

        pressure_total = self._f(features.get("pressure_total_qadj") or features.get("pressure_total"))
        threat_total = self._f(features.get("threat_total_qadj") or features.get("threat_total"))
        openness = self._f(features.get("openness_qadj") or features.get("openness"))
        slowdown = self._f(features.get("slowdown_qadj") or features.get("slowdown"))
        chaos = self._f(features.get("chaos_qadj") or features.get("chaos"))
        two_sided = self._f(features.get("two_sided_liveness"))
        pressure_asym = self._f(features.get("pressure_asymmetry"))
        threat_asym = self._f(features.get("threat_asymmetry"))

        trailer_chase = self._f(features.get("trailer_chase_signal"))
        leader_protect = self._f(features.get("leader_protect_signal"))
        set_piece_risk = self._f(features.get("set_piece_risk"))
        single_goal_risk = self._f(features.get("single_goal_risk"))
        recent_pressure_ratio = self._f(features.get("recent_pressure_ratio"))
        over_support_signal = self._f(features.get("over_support_signal"))
        under_stability_signal = self._f(features.get("under_stability_signal"))

        pressure_share_home = self._f(features.get("pressure_share_home"))
        pressure_share_away = self._f(features.get("pressure_share_away"))
        threat_share_home = self._f(features.get("threat_share_home"))
        threat_share_away = self._f(features.get("threat_share_away"))

        feed_quality = self._f(features.get("feed_quality_score"), 0.58)
        competition_quality = self._f(features.get("competition_quality_score"), 0.60)
        market_quality = self._f(features.get("market_quality_score"), 0.62)
        missing_penalty = self._f(features.get("missing_data_penalty"), 0.0)

        reliability = self._clamp(
            0.40 * feed_quality
            + 0.30 * competition_quality
            + 0.20 * market_quality
            + 0.10 * (1.0 - missing_penalty),
            0.20,
            1.0,
        )

        # ------------------------------------------------------------------
        # Macro descriptors
        # ------------------------------------------------------------------
        live_tempo = self._clamp(
            0.28 * self._lin(pressure_total, 1.6, 7.2)
            + 0.24 * self._lin(threat_total, 1.2, 6.4)
            + 0.18 * openness
            + 0.18 * self._clamp(recent_pressure_ratio / 1.35)
            + 0.12 * over_support_signal
        )

        if live_tempo < 0.33:
            pace_state = "low"
        elif live_tempo < 0.66:
            pace_state = "medium"
        else:
            pace_state = "high"

        if chaos >= 0.75:
            chaos_state = "high"
        elif chaos >= 0.45:
            chaos_state = "medium"
        else:
            chaos_state = "low"

        # ------------------------------------------------------------------
        # Hard distorted state
        # ------------------------------------------------------------------
        red_card_score = self._clamp(
            0.56 * (1.0 if red_cards > 0 else 0.0)
            + 0.24 * chaos
            + 0.20 * pressure_asym
        )

        # ------------------------------------------------------------------
        # Candidate regime scores
        # ------------------------------------------------------------------
        chaotic_score = self._clamp(
            0.28 * chaos
            + 0.18 * openness
            + 0.16 * pressure_asym
            + 0.12 * threat_asym
            + 0.14 * single_goal_risk
            + 0.12 * self._lin(abs(goal_diff), 1.0, 3.0)
        )

        open_exchange_score = self._clamp(
            0.20 * openness
            + 0.16 * two_sided
            + 0.14 * self._lin(pressure_total, 2.5, 7.4)
            + 0.12 * self._lin(threat_total, 1.8, 6.2)
            + 0.14 * self._clamp(recent_pressure_ratio / 1.25)
            + 0.12 * set_piece_risk
            + 0.12 * over_support_signal
        )

        siege_home_score = self._clamp(
            0.18 * self._lin(pressure_share_home, 0.58, 0.84)
            + 0.16 * self._lin(threat_share_home, 0.58, 0.84)
            + 0.14 * pressure_asym
            + 0.10 * self._lin(pressure_total, 2.3, 7.0)
            + 0.08 * self._lin(threat_total, 1.8, 6.0)
            + 0.18 * (trailer_chase if goal_diff < 0 else 0.0)
            + 0.10 * self._clamp(recent_pressure_ratio / 1.20)
            + 0.06 * over_support_signal
        )

        siege_away_score = self._clamp(
            0.18 * self._lin(pressure_share_away, 0.58, 0.84)
            + 0.16 * self._lin(threat_share_away, 0.58, 0.84)
            + 0.14 * pressure_asym
            + 0.10 * self._lin(pressure_total, 2.3, 7.0)
            + 0.08 * self._lin(threat_total, 1.8, 6.0)
            + 0.18 * (trailer_chase if goal_diff > 0 else 0.0)
            + 0.10 * self._clamp(recent_pressure_ratio / 1.20)
            + 0.06 * over_support_signal
        )

        controlled_home_score = self._clamp(
            0.20 * self._lin(pressure_share_home, 0.54, 0.72)
            + 0.18 * self._lin(threat_share_home, 0.54, 0.72)
            + 0.12 * self._lin(pressure_total, 2.0, 5.8)
            + 0.08 * self._lin(threat_total, 1.7, 5.2)
            + 0.10 * (1.0 - chaos)
            + 0.08 * (1.0 - slowdown)
            + 0.10 * (1.0 - max(0.0, trailer_chase - 0.40))
            + 0.08 * self._clamp(recent_pressure_ratio / 1.20)
            + 0.06 * over_support_signal
        )

        controlled_away_score = self._clamp(
            0.20 * self._lin(pressure_share_away, 0.54, 0.72)
            + 0.18 * self._lin(threat_share_away, 0.54, 0.72)
            + 0.12 * self._lin(pressure_total, 2.0, 5.8)
            + 0.08 * self._lin(threat_total, 1.7, 5.2)
            + 0.10 * (1.0 - chaos)
            + 0.08 * (1.0 - slowdown)
            + 0.10 * (1.0 - max(0.0, trailer_chase - 0.40))
            + 0.08 * self._clamp(recent_pressure_ratio / 1.20)
            + 0.06 * over_support_signal
        )

        closed_low_event_score = self._clamp(
            0.24 * under_stability_signal
            + 0.16 * slowdown
            + 0.12 * (1.0 - openness)
            + 0.10 * (1.0 - two_sided)
            + 0.10 * (1.0 - self._lin(pressure_total, 2.2, 6.0))
            + 0.08 * (1.0 - self._lin(threat_total, 1.8, 5.0))
            + 0.10 * (1.0 - set_piece_risk)
            + 0.10 * (1.0 - trailer_chase)
        )

        late_lockdown_score = self._clamp(
            0.18 * (1.0 if minute >= 80 else 0.0)
            + 0.16 * (1.0 if abs(goal_diff) >= 1 else 0.0)
            + 0.18 * under_stability_signal
            + 0.14 * slowdown
            + 0.10 * (1.0 - two_sided)
            + 0.10 * leader_protect
            + 0.08 * (1.0 - set_piece_risk)
            + 0.06 * (1.0 - trailer_chase)
        )

        # ------------------------------------------------------------------
        # Anti-false-lockdown safeguards
        # ------------------------------------------------------------------
        if minute < 72:
            late_lockdown_score *= 0.28
        elif minute < 78:
            late_lockdown_score *= 0.55

        # chase / recent pressure kills fake lockdown
        if trailer_chase >= 0.44:
            late_lockdown_score *= 0.52
        if recent_pressure_ratio >= 0.92:
            late_lockdown_score *= 0.50
        if over_support_signal >= 0.56:
            late_lockdown_score *= 0.45
        if set_piece_risk >= 0.58:
            late_lockdown_score *= 0.70

        # if game is still clearly live, don't call it closed
        if recent_pressure_ratio >= 1.05 or over_support_signal >= 0.60:
            closed_low_event_score *= 0.72

        # high event / early open match should not become closed by inertia
        if total_goals >= 4 and minute <= 25:
            closed_low_event_score *= 0.15
            late_lockdown_score *= 0.10

        # one-goal late chase should prefer siege / controlled pressure over lockdown
        if abs(goal_diff) == 1 and minute >= 68 and trailer_chase >= 0.36:
            if goal_diff < 0:
                siege_home_score += 0.08
                controlled_home_score += 0.05
            elif goal_diff > 0:
                siege_away_score += 0.08
                controlled_away_score += 0.05

        siege_home_score = self._clamp(siege_home_score)
        siege_away_score = self._clamp(siege_away_score)
        controlled_home_score = self._clamp(controlled_home_score)
        controlled_away_score = self._clamp(controlled_away_score)
        closed_low_event_score = self._clamp(closed_low_event_score)
        late_lockdown_score = self._clamp(late_lockdown_score)

        # ------------------------------------------------------------------
        # Label selection
        # ------------------------------------------------------------------
        if red_card_score >= 0.62:
            label = "RED_CARD_DISTORTED"
            freeze = True
        else:
            candidates = {
                "CHAOTIC_TRANSITIONS": chaotic_score,
                "OPEN_EXCHANGE": open_exchange_score,
                "ASYMMETRIC_SIEGE_HOME": siege_home_score,
                "ASYMMETRIC_SIEGE_AWAY": siege_away_score,
                "CONTROLLED_HOME_PRESSURE": controlled_home_score,
                "CONTROLLED_AWAY_PRESSURE": controlled_away_score,
                "CLOSED_LOW_EVENT": closed_low_event_score,
                "LATE_LOCKDOWN": late_lockdown_score,
                "BALANCED_NEUTRAL": self._clamp(
                    0.42
                    + 0.12 * (1.0 - abs(open_exchange_score - closed_low_event_score))
                    + 0.12 * (1.0 - chaos)
                    + 0.14 * reliability
                ),
            }

            label = max(candidates, key=candidates.get)
            freeze = False

            # final arbitration rules
            if label == "LATE_LOCKDOWN":
                if trailer_chase > 0.40 or single_goal_risk > 0.48 or over_support_signal > 0.52:
                    if goal_diff < 0:
                        label = "ASYMMETRIC_SIEGE_HOME"
                    elif goal_diff > 0:
                        label = "ASYMMETRIC_SIEGE_AWAY"
                    else:
                        label = "BALANCED_NEUTRAL"

            if label == "CLOSED_LOW_EVENT":
                if trailer_chase > 0.46 or recent_pressure_ratio > 0.95 or over_support_signal > 0.54:
                    if goal_diff < 0:
                        label = "CONTROLLED_HOME_PRESSURE"
                    elif goal_diff > 0:
                        label = "CONTROLLED_AWAY_PRESSURE"
                    else:
                        label = "BALANCED_NEUTRAL"

            if label == "BALANCED_NEUTRAL":
                # avoid wasting genuinely live situations in neutral
                if open_exchange_score >= 0.56:
                    label = "OPEN_EXCHANGE"
                elif siege_home_score >= 0.58 and siege_home_score > siege_away_score:
                    label = "ASYMMETRIC_SIEGE_HOME"
                elif siege_away_score >= 0.58 and siege_away_score > siege_home_score:
                    label = "ASYMMETRIC_SIEGE_AWAY"

        # ------------------------------------------------------------------
        # Confidence
        # ------------------------------------------------------------------
        base_score = {
            "RED_CARD_DISTORTED": red_card_score,
            "CHAOTIC_TRANSITIONS": chaotic_score,
            "OPEN_EXCHANGE": open_exchange_score,
            "ASYMMETRIC_SIEGE_HOME": siege_home_score,
            "ASYMMETRIC_SIEGE_AWAY": siege_away_score,
            "CONTROLLED_HOME_PRESSURE": controlled_home_score,
            "CONTROLLED_AWAY_PRESSURE": controlled_away_score,
            "CLOSED_LOW_EVENT": closed_low_event_score,
            "LATE_LOCKDOWN": late_lockdown_score,
            "BALANCED_NEUTRAL": 0.52,
        }.get(label, 0.52)

        confidence = self._clamp(
            0.40
            + 0.20 * base_score
            + 0.20 * reliability
            - 0.06 * self._f(features.get("missing_data_penalty"), 0.0),
            0.38,
            0.88,
        )

        # ------------------------------------------------------------------
        # States
        # ------------------------------------------------------------------
        if label.endswith("HOME"):
            control_state = "home"
            pressure_state = "home"
            transition_state = "active" if "SIEGE" in label else "stable"
        elif label.endswith("AWAY"):
            control_state = "away"
            pressure_state = "away"
            transition_state = "active" if "SIEGE" in label else "stable"
        elif label == "OPEN_EXCHANGE":
            control_state = "balanced"
            pressure_state = "both"
            transition_state = "active"
        elif label in {"CLOSED_LOW_EVENT", "LATE_LOCKDOWN"}:
            control_state = "neutral"
            pressure_state = "low"
            transition_state = "locked"
        elif label == "CHAOTIC_TRANSITIONS":
            control_state = "unstable"
            pressure_state = "mixed"
            transition_state = "chaotic"
        elif label == "RED_CARD_DISTORTED":
            control_state = "distorted"
            pressure_state = "distorted"
            transition_state = "distorted"
        else:
            control_state = "balanced"
            pressure_state = "mixed"
            transition_state = "stable"

        diagnostics = {
            "pace": pace_state,
            "control": control_state,
            "chaos": chaos,
            "pressure_total": pressure_total,
            "threat_total": threat_total,
            "slowdown": slowdown,
            "two_sided_liveness": two_sided,
            "trailer_chase_signal": trailer_chase,
            "leader_protect_signal": leader_protect,
            "single_goal_risk": single_goal_risk,
            "set_piece_risk": set_piece_risk,
            "recent_pressure_ratio": recent_pressure_ratio,
            "over_support_signal": over_support_signal,
            "under_stability_signal": under_stability_signal,
            "late_lockdown_score": late_lockdown_score,
            "closed_low_event_score": closed_low_event_score,
            "open_exchange_score": open_exchange_score,
            "siege_home_score": siege_home_score,
            "siege_away_score": siege_away_score,
            "controlled_home_score": controlled_home_score,
            "controlled_away_score": controlled_away_score,
            "reliability": reliability,
        }

        return RegimeDecision(
            regime_label=label,
            regime_confidence=round(confidence, 3),
            pace_state=pace_state,
            control_state=control_state,
            chaos_state=chaos_state,
            pressure_state=pressure_state,
            transition_state=transition_state,
            freeze_flag=freeze,
            diagnostics=diagnostics,
        )