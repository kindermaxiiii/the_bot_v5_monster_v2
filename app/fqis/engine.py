from __future__ import annotations

from dataclasses import dataclass

from app.fqis.audit.rejection_codes import RejectionCode, RejectionStage
from app.fqis.contracts.core import BookOffer, ExecutableBet, StatisticalThesis
from app.fqis.pipeline import PipelineOutcome, PipelineRejection, run_thesis_pipeline
from app.fqis.risk.gates import RiskDecision, apply_risk_gates


@dataclass(slots=True, frozen=True)
class GovernedOutcome:
    technical_best_bet: ExecutableBet | None
    accepted_bet: ExecutableBet | None
    pipeline_rejections: tuple[PipelineRejection, ...]
    risk_rejections: tuple[tuple[RejectionStage, RejectionCode, str], ...]


def run_governed_thesis_pipeline(
    thesis: StatisticalThesis,
    offers: tuple[BookOffer, ...],
    *,
    p_real_by_intent_key: dict[str, float],
    min_strength: float,
    min_confidence: float,
    min_edge: float,
    min_ev: float,
    min_odds: float,
    max_odds: float,
    technical_min_edge: float = 0.0,
    technical_min_ev: float = -1.0,
) -> GovernedOutcome:
    pipeline_outcome: PipelineOutcome = run_thesis_pipeline(
        thesis,
        offers,
        p_real_by_intent_key=p_real_by_intent_key,
        min_edge=technical_min_edge,
        min_ev=technical_min_ev,
        min_odds=min_odds,
        max_odds=max_odds,
    )

    risk_outcome: RiskDecision = apply_risk_gates(
        pipeline_outcome.best_bet,
        min_strength=min_strength,
        min_confidence=min_confidence,
        min_edge=min_edge,
        min_ev=min_ev,
        min_odds=min_odds,
        max_odds=max_odds,
    )

    accepted_bet = pipeline_outcome.best_bet if risk_outcome.accepted else None

    return GovernedOutcome(
        technical_best_bet=pipeline_outcome.best_bet,
        accepted_bet=accepted_bet,
        pipeline_rejections=pipeline_outcome.rejections,
        risk_rejections=risk_outcome.rejections,
    )