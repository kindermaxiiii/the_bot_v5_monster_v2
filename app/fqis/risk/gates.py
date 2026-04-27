from __future__ import annotations

from dataclasses import dataclass

from app.fqis.audit.rejection_codes import RejectionCode, RejectionStage
from app.fqis.contracts.core import ExecutableBet


@dataclass(slots=True, frozen=True)
class RiskDecision:
    accepted: bool
    rejections: tuple[tuple[RejectionStage, RejectionCode, str], ...]


def apply_risk_gates(
    bet: ExecutableBet | None,
    *,
    min_strength: float,
    min_confidence: float,
    min_edge: float,
    min_ev: float,
    min_odds: float,
    max_odds: float,
) -> RiskDecision:
    if bet is None:
        return RiskDecision(
            accepted=False,
            rejections=((RejectionStage.RISK, RejectionCode.RISK_BLOCKED, "no bet produced"),),
        )

    rejections: list[tuple[RejectionStage, RejectionCode, str]] = []

    strength = _extract_metric(bet.rationale, "strength")
    confidence = _extract_metric(bet.rationale, "confidence")

    if strength is not None and strength < min_strength:
        rejections.append((RejectionStage.RISK, RejectionCode.RISK_BLOCKED, f"strength={strength:.4f}"))

    if confidence is not None and confidence < min_confidence:
        rejections.append((RejectionStage.RISK, RejectionCode.RISK_BLOCKED, f"confidence={confidence:.4f}"))

    if bet.edge < min_edge:
        rejections.append((RejectionStage.RISK, RejectionCode.EDGE_TOO_LOW, f"edge={bet.edge:.4f}"))

    if bet.ev < min_ev:
        rejections.append((RejectionStage.RISK, RejectionCode.EV_TOO_LOW, f"ev={bet.ev:.4f}"))

    if bet.odds_decimal < min_odds:
        rejections.append((RejectionStage.RISK, RejectionCode.PRICE_TOO_LOW, f"odds={bet.odds_decimal:.4f}"))

    if bet.odds_decimal > max_odds:
        rejections.append((RejectionStage.RISK, RejectionCode.PRICE_TOO_HIGH, f"odds={bet.odds_decimal:.4f}"))

    return RiskDecision(
        accepted=len(rejections) == 0,
        rejections=tuple(rejections),
    )


def _extract_metric(rationale: tuple[str, ...], prefix: str) -> float | None:
    target = f"{prefix}="
    for item in rationale:
        if item.startswith(target):
            try:
                return float(item.split("=", 1)[1])
            except ValueError:
                return None
    return None

    