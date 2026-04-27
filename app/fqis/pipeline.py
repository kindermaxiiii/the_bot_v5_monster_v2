from __future__ import annotations

from dataclasses import dataclass

from app.fqis.audit.rejection_codes import RejectionCode, RejectionStage
from app.fqis.binding.binder import bind_offers_to_intent
from app.fqis.contracts.core import BookOffer, ExecutableBet, StatisticalThesis
from app.fqis.pricing.intent_pricer import price_intent, to_executable_bet
from app.fqis.pricing.ranking import select_best_priced_intent
from app.fqis.thesis.intent_mapper import map_thesis_to_market_intents


@dataclass(slots=True, frozen=True)
class PipelineRejection:
    stage: RejectionStage
    code: RejectionCode
    detail: str


@dataclass(slots=True, frozen=True)
class PipelineOutcome:
    best_bet: ExecutableBet | None
    rejections: tuple[PipelineRejection, ...]


def run_thesis_pipeline(
    thesis: StatisticalThesis,
    offers: tuple[BookOffer, ...],
    *,
    p_real_by_intent_key: dict[str, float],
    min_edge: float = 0.01,
    min_ev: float = 0.0,
    min_odds: float = 1.50,
    max_odds: float = 2.80,
) -> PipelineOutcome:
    intents = map_thesis_to_market_intents(thesis)
    if not intents:
        return PipelineOutcome(
            best_bet=None,
            rejections=(
                PipelineRejection(
                    stage=RejectionStage.INTENT,
                    code=RejectionCode.NO_MARKET_INTENT,
                    detail="no intents generated from thesis",
                ),
            ),
        )

    priced_candidates = []
    rejections: list[PipelineRejection] = []

    for intent in intents:
        intent_key = _intent_key(intent)
        p_real = p_real_by_intent_key.get(intent_key)
        if p_real is None:
            rejections.append(
                PipelineRejection(
                    stage=RejectionStage.PRICING,
                    code=RejectionCode.EDGE_TOO_LOW,
                    detail=f"missing p_real for {intent_key}",
                )
            )
            continue

        binding = bind_offers_to_intent(intent, offers)
        if not binding.matched_offers:
            rejections.append(
                PipelineRejection(
                    stage=RejectionStage.BINDING,
                    code=RejectionCode.OFFER_NOT_FOUND,
                    detail=f"no compatible offer for {intent_key}",
                )
            )
            continue

        for offer in binding.matched_offers:
            if offer.odds_decimal < min_odds:
                rejections.append(
                    PipelineRejection(
                        stage=RejectionStage.PRICING,
                        code=RejectionCode.PRICE_TOO_LOW,
                        detail=f"{intent_key} odds={offer.odds_decimal}",
                    )
                )
                continue

            if offer.odds_decimal > max_odds:
                rejections.append(
                    PipelineRejection(
                        stage=RejectionStage.PRICING,
                        code=RejectionCode.PRICE_TOO_HIGH,
                        detail=f"{intent_key} odds={offer.odds_decimal}",
                    )
                )
                continue

            priced = price_intent(thesis, intent, offer, p_real=p_real)

            if priced.edge < min_edge:
                rejections.append(
                    PipelineRejection(
                        stage=RejectionStage.PRICING,
                        code=RejectionCode.EDGE_TOO_LOW,
                        detail=f"{intent_key} edge={priced.edge:.4f}",
                    )
                )
                continue

            if priced.ev < min_ev:
                rejections.append(
                    PipelineRejection(
                        stage=RejectionStage.PRICING,
                        code=RejectionCode.EV_TOO_LOW,
                        detail=f"{intent_key} ev={priced.ev:.4f}",
                    )
                )
                continue

            priced_candidates.append(priced)

    best = select_best_priced_intent(tuple(priced_candidates))
    if best is None:
        return PipelineOutcome(best_bet=None, rejections=tuple(rejections))

    return PipelineOutcome(
        best_bet=to_executable_bet(best),
        rejections=tuple(rejections),
    )


def _intent_key(intent) -> str:
    line = "NA" if intent.line is None else str(intent.line)
    return f"{intent.family.value}|{intent.side.value}|{intent.team_role.value}|{line}"

    