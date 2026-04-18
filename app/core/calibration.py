from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

from app.config import settings


@dataclass(slots=True)
class CalibrationResult:
    raw_probability: float
    calibrated_probability: float
    segment: str
    confidence: float


class CalibrationLayer:
    """
    Calibration V6.

    Mission :
    rendre la probabilité modèle plus prudente, plus réaliste,
    sans laisser le marché construire la vérité.

    Principes :
    - structure avant prix
    - ancre marché légère mais adaptative
    - utilisation du side (OVER / UNDER / autres)
    - prudence accrue si divergence modèle/marché trop grande et qualité moyenne
    - confiance plus sobre
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

    def _clamp(self, x: float, lo: float, hi: float) -> float:
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _market_key(self, market_key: str) -> str:
        return str(market_key or "").strip().lower()

    def _regime_key(self, regime: str) -> str:
        return str(regime or "").strip().upper()

    def _side_key(self, side: str) -> str:
        return str(side or "").strip().upper()

    def _minute_bucket(self, minute: int) -> str:
        if minute <= 10:
            return "m00_10"
        if minute <= 20:
            return "m11_20"
        if minute <= 30:
            return "m21_30"
        if minute <= 45:
            return "m31_45"
        if minute <= 60:
            return "m46_60"
        if minute <= 75:
            return "m61_75"
        return "m76_plus"

    def _quality_bucket(self, quality: float) -> str:
        if quality >= 0.80:
            return "qA"
        if quality >= 0.70:
            return "qB"
        if quality >= 0.60:
            return "qC"
        return "qD"

    # ------------------------------------------------------------------
    # Market profiles
    # ------------------------------------------------------------------
    def _market_profile(self, market_key: str) -> dict[str, float]:
        mk = self._market_key(market_key)

        profiles = {
            "ou_ft": {
                "base_shrink": 0.09,
                "floor": 0.015,
                "ceil": 0.985,
                "max_shrink": 0.24,
            },
            "ou_1h": {
                "base_shrink": 0.14,
                "floor": 0.020,
                "ceil": 0.980,
                "max_shrink": 0.32,
            },
            "btts": {
                "base_shrink": 0.16,
                "floor": 0.020,
                "ceil": 0.975,
                "max_shrink": 0.35,
            },
            "team_total": {
                "base_shrink": 0.17,
                "floor": 0.020,
                "ceil": 0.975,
                "max_shrink": 0.36,
            },
            "result": {
                "base_shrink": 0.18,
                "floor": 0.030,
                "ceil": 0.960,
                "max_shrink": 0.38,
            },
            "1x2": {
                "base_shrink": 0.18,
                "floor": 0.030,
                "ceil": 0.960,
                "max_shrink": 0.38,
            },
            "correct_score": {
                "base_shrink": 0.28,
                "floor": 0.005,
                "ceil": 0.700,
                "max_shrink": 0.52,
            },
        }

        return profiles.get(
            mk,
            {
                "base_shrink": 0.17,
                "floor": 0.020,
                "ceil": 0.975,
                "max_shrink": 0.36,
            },
        )

    # ------------------------------------------------------------------
    # Penalties
    # ------------------------------------------------------------------
    def _quality_penalty(self, quality: float, market_key: str) -> float:
        q = self._clamp(quality, 0.20, 1.00)
        base = max(0.0, 0.74 - q)

        mk = self._market_key(market_key)
        mult = 0.24 if mk == "ou_ft" else 0.30 if mk in {"ou_1h", "btts"} else 0.36

        return self._clamp(base * mult, 0.0, 0.14)

    def _minute_penalty(self, minute: int, market_key: str, side: str) -> float:
        mk = self._market_key(market_key)
        sk = self._side_key(side)

        penalty = 0.0

        # très tôt : peu d'information
        if minute <= 8:
            penalty += 0.04
        elif minute <= 18:
            penalty += 0.02

        # très tard : prudence, mais plus douce sur certains OVER OU_FT
        if minute >= 80:
            if mk == "ou_ft" and "OVER" in sk:
                penalty += 0.015
            else:
                penalty += 0.03
        elif minute >= 70:
            if mk == "ou_ft" and "OVER" in sk:
                penalty += 0.008
            else:
                penalty += 0.015

        if mk == "ou_1h":
            if minute <= 5:
                penalty += 0.03
            if minute >= 35:
                penalty += 0.05

        if mk == "correct_score":
            penalty += 0.03

        return self._clamp(penalty, 0.0, 0.14)

    def _regime_penalty(self, regime: str, market_key: str, side: str) -> float:
        rk = self._regime_key(regime)
        mk = self._market_key(market_key)
        sk = self._side_key(side)

        penalty = 0.0

        if rk == "RED_CARD_DISTORTED":
            penalty += 0.10
        elif rk == "CHAOTIC_TRANSITIONS":
            penalty += 0.05
        elif rk == "LATE_LOCKDOWN":
            if "OVER" in sk:
                penalty += 0.04
            else:
                penalty += 0.02
        elif rk == "CLOSED_LOW_EVENT":
            if "OVER" in sk:
                penalty += 0.02
            else:
                penalty += 0.01

        if mk in {"btts", "team_total", "result", "correct_score"}:
            if rk in {"CHAOTIC_TRANSITIONS", "RED_CARD_DISTORTED"}:
                penalty += 0.04

        if mk == "correct_score":
            penalty += 0.03

        return self._clamp(penalty, 0.0, 0.16)

    def _extreme_probability_penalty(self, raw_probability: float, market_key: str) -> float:
        raw = self._clamp(raw_probability, 0.0001, 0.9999)
        extremeness = abs(raw - 0.5) * 2.0

        mk = self._market_key(market_key)
        base = max(0.0, extremeness - 0.30)

        mult = 0.10 if mk == "ou_ft" else 0.14 if mk in {"ou_1h", "btts"} else 0.19
        penalty = base * mult

        if raw >= 0.88 or raw <= 0.12:
            penalty += 0.025
        if raw >= 0.93 or raw <= 0.07:
            penalty += 0.025

        return self._clamp(penalty, 0.0, 0.17)

    def _market_divergence_penalty(
        self,
        raw_probability: float,
        market_probability: float | None,
        quality: float,
        market_key: str,
    ) -> float:
        mp = self._f(market_probability, -1.0)
        if not (0.0 < mp < 1.0):
            return 0.0

        q = self._clamp(quality, 0.20, 1.00)
        divergence = abs(raw_probability - mp)

        # Si la qualité est excellente, on laisse respirer davantage.
        # Si la qualité est moyenne/faible, gros écart = prudence supplémentaire.
        fragility = max(0.0, 0.82 - q)
        mk = self._market_key(market_key)

        mult = 0.16 if mk == "ou_ft" else 0.20 if mk in {"ou_1h", "btts"} else 0.24
        penalty = max(0.0, divergence - 0.12) * mult * (0.6 + fragility)

        return self._clamp(penalty, 0.0, 0.12)

    def _context_penalty(
        self,
        market_key: str,
        minute: int,
        side: str,
        context: dict[str, object],
    ) -> float:
        mk = self._market_key(market_key)
        sk = self._side_key(side)

        if mk != "ou_ft" or "UNDER" not in sk:
            return 0.0

        try:
            goal_budget_raw = context.get("score_state_budget", context.get("allowed_additional_goals_before_loss"))
            goal_budget = int(goal_budget_raw) if goal_budget_raw is not None else None
        except (TypeError, ValueError):
            goal_budget = None

        if goal_budget is None:
            return 0.0

        remaining_goal_expectancy = self._f(context.get("total_goal_expectancy_remaining"), -1.0)
        current_total = self._f(context.get("current_total"), 0.0)
        line = self._f(context.get("line"), 0.0)

        penalty = 0.0

        if goal_budget <= 0:
            if minute <= settings.under_small_goal_budget_real_max_minute:
                penalty += 0.08
            else:
                penalty += 0.04
        elif goal_budget == 1:
            if minute <= 30:
                penalty += 0.07
            elif minute <= 45:
                penalty += 0.05
            elif minute <= settings.under_one_goal_budget_real_max_minute:
                penalty += 0.03

            if remaining_goal_expectancy > settings.under_one_goal_budget_expectancy_real_max:
                penalty += 0.03
        elif goal_budget == 2:
            if minute <= settings.under_two_goal_budget_real_max_minute:
                penalty += 0.035
            if remaining_goal_expectancy > settings.under_two_goal_budget_expectancy_real_max:
                penalty += 0.02

        if line <= 3.5 and current_total >= 2 and minute <= 35:
            penalty += 0.015

        return self._clamp(penalty, 0.0, settings.calibration_under_early_goal_budget_shrink_max)

    def _distribution_consistency_penalty(
        self,
        market_key: str,
        quality: float,
        context: dict[str, object],
    ) -> float:
        mk = self._market_key(market_key)
        if mk != "ou_ft":
            return 0.0

        event_gap = self._f(context.get("event_probability_gap"), -1.0)
        if event_gap <= 0.0:
            return 0.0

        q = self._clamp(quality, 0.20, 1.00)
        fragility = max(0.0, 0.82 - q)
        penalty = max(0.0, event_gap - 0.06) * (0.18 + 0.40 * fragility)
        return self._clamp(penalty, 0.0, 0.06)

    # ------------------------------------------------------------------
    # Caps
    # ------------------------------------------------------------------
    def _dynamic_caps(
        self,
        market_key: str,
        minute: int,
        regime: str,
        quality: float,
        side: str,
    ) -> tuple[float, float]:
        profile = self._market_profile(market_key)
        floor = profile["floor"]
        ceil = profile["ceil"]

        rk = self._regime_key(regime)
        sk = self._side_key(side)
        q = self._clamp(quality, 0.20, 1.00)
        mk = self._market_key(market_key)

        if q < 0.65:
            floor += 0.01
            ceil -= 0.02

        if rk == "RED_CARD_DISTORTED":
            ceil -= 0.04
        elif rk == "CHAOTIC_TRANSITIONS":
            ceil -= 0.02

        # Très tôt
        if minute <= 8:
            ceil -= 0.03

        # Très tard : plus dur sauf certains OVER OU_FT où on évite d'écraser trop fort
        if minute >= 80:
            if mk == "ou_ft" and "OVER" in sk:
                ceil -= 0.01
            else:
                ceil -= 0.03

        if mk == "correct_score":
            floor = max(floor, 0.005)
            ceil = min(ceil, 0.60)
        elif mk in {"result", "1x2"}:
            ceil = min(ceil, 0.92)
        elif mk in {"btts", "team_total", "ou_1h"}:
            ceil = min(ceil, 0.96)

        floor = self._clamp(floor, 0.001, 0.40)
        ceil = self._clamp(ceil, 0.55, 0.995)

        if floor >= ceil:
            floor = min(floor, 0.45)
            ceil = max(ceil, floor + 0.10)

        return floor, ceil

    # ------------------------------------------------------------------
    # Main
    # ------------------------------------------------------------------
    def calibrate(
        self,
        market_key: str,
        raw_probability: float,
        *,
        minute: int | None = None,
        regime: str = "",
        quality: float = 0.65,
        market_probability: float | None = None,
        side: str = "",
        **context,
    ) -> CalibrationResult:
        mk = self._market_key(market_key)
        sk = self._side_key(side)
        minute = int(minute or 0)
        q = self._clamp(self._f(quality, 0.65), 0.20, 1.00)
        raw = self._clamp(self._f(raw_probability, 0.50), 0.0001, 0.9999)

        profile = self._market_profile(mk)
        base_shrink = profile["base_shrink"]
        max_shrink = profile["max_shrink"]

        quality_penalty = self._quality_penalty(q, mk)
        minute_penalty = self._minute_penalty(minute, mk, sk)
        regime_penalty = self._regime_penalty(regime, mk, sk)
        extreme_penalty = self._extreme_probability_penalty(raw, mk)
        divergence_penalty = self._market_divergence_penalty(raw, market_probability, q, mk)
        context_penalty = self._context_penalty(mk, minute, sk, context)
        consistency_penalty = self._distribution_consistency_penalty(mk, q, context)

        total_shrink = (
            base_shrink
            + quality_penalty
            + minute_penalty
            + regime_penalty
            + extreme_penalty
            + divergence_penalty
            + context_penalty
            + consistency_penalty
        )
        total_shrink = self._clamp(total_shrink, base_shrink, max_shrink)

        # Centre de rétraction :
        # - par défaut 0.50
        # - si market_probability existe, on l'utilise comme ancre adaptative
        center = 0.50
        mp = self._f(market_probability, -1.0)
        divergence = abs(raw - mp) if 0.0 < mp < 1.0 else 0.0

        if 0.0 < mp < 1.0:
            market_anchor_weight = 0.10 + 0.18 * max(0.0, 0.78 - q) + 0.12 * max(0.0, divergence - 0.10)
            market_anchor_weight = self._clamp(market_anchor_weight, 0.08, 0.32)
            center = (1.0 - market_anchor_weight) * 0.50 + market_anchor_weight * mp

        calibrated = center + (raw - center) * (1.0 - total_shrink)

        floor, ceil = self._dynamic_caps(
            market_key=mk,
            minute=minute,
            regime=regime,
            quality=q,
            side=sk,
        )
        calibrated = self._clamp(calibrated, floor, ceil)

        shrink_pressure = self._clamp(
            (total_shrink - base_shrink) / max(0.0001, max_shrink - base_shrink),
            0.0,
            1.0,
        )

        extremeness = abs(calibrated - 0.5) * 2.0
        divergence_after = abs(calibrated - mp) if 0.0 < mp < 1.0 else 0.10

        confidence = self._clamp(
            0.22
            + 0.50 * q
            + 0.10 * (1.0 - shrink_pressure)
            + 0.08 * (1.0 - min(1.0, divergence_after / 0.25))
            + 0.10 * (1.0 - extremeness),
            0.22,
            0.93,
        )

        segment = "|".join(
            [
                mk or "unknown",
                self._minute_bucket(minute),
                self._regime_key(regime) or "NA",
                self._quality_bucket(q),
                sk or "NA",
            ]
        )

        return CalibrationResult(
            raw_probability=round(raw, 6),
            calibrated_probability=round(calibrated, 6),
            segment=segment,
            confidence=round(confidence, 4),
        )
