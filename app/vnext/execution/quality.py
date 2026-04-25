from __future__ import annotations

from app.vnext.execution.models import ExecutionBlocker, ExecutionQualityBreakdown, MarketOfferGroup


def _clip(value: float, *, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _price_integrity(odds: float) -> float:
    if odds <= 1.01:
        return 0.0
    if odds <= 1.10:
        return 0.5
    if odds <= 8.0:
        return 1.0
    if odds <= 15.0:
        return 0.6
    return 0.3


def _freshness_score(seconds: int | None) -> float:
    if seconds is None:
        return 0.5
    if seconds <= 30:
        return 1.0
    if seconds <= 120:
        return 0.8
    if seconds <= 300:
        return 0.6
    if seconds <= 600:
        return 0.4
    return 0.2


def _bookmaker_diversity_score(bookmaker_count: int) -> float:
    if bookmaker_count <= 0:
        return 0.0
    if bookmaker_count == 1:
        return 0.38
    if bookmaker_count == 2:
        return 0.72
    return 1.0


def build_execution_quality(offer_group: MarketOfferGroup) -> ExecutionQualityBreakdown:
    offer_exists_score = 1.0 if offer_group.offer_exists else 0.0
    template_binding_score = (
        1.0
        if offer_group.template_binding_status == "EXACT"
        else 0.72
        if offer_group.template_binding_status == "RELAXED"
        else 0.0
    )
    market_clarity_score = (
        1.0
        if offer_group.template_binding_status == "EXACT"
        else 0.78
        if offer_group.template_binding_status == "RELAXED"
        else 0.0
    )

    unique_bookmakers = {offer.bookmaker_id for offer in offer_group.offers}
    bookmaker_diversity_score = _bookmaker_diversity_score(len(unique_bookmakers))

    best_offer = max(offer_group.offers, key=lambda offer: offer.odds_decimal, default=None)
    price_integrity_score = _price_integrity(best_offer.odds_decimal) if best_offer else 0.0
    freshness_score = _freshness_score(best_offer.freshness_seconds if best_offer else None)

    retrievability_score = _clip(
        (offer_exists_score * 0.33)
        + (template_binding_score * 0.24)
        + (bookmaker_diversity_score * 0.23)
        + (freshness_score * 0.20)
    )

    publishability_score = _clip(
        (offer_exists_score * 0.18)
        + (market_clarity_score * 0.16)
        + (template_binding_score * 0.14)
        + (bookmaker_diversity_score * 0.16)
        + (price_integrity_score * 0.18)
        + (freshness_score * 0.08)
        + (retrievability_score * 0.10)
    )

    return ExecutionQualityBreakdown(
        offer_exists_score=round(offer_exists_score, 4),
        template_binding_score=round(template_binding_score, 4),
        market_clarity_score=round(market_clarity_score, 4),
        bookmaker_diversity_score=round(bookmaker_diversity_score, 4),
        price_integrity_score=round(price_integrity_score, 4),
        freshness_score=round(freshness_score, 4),
        retrievability_score=round(retrievability_score, 4),
        publishability_score=round(publishability_score, 4),
    )


def execution_blockers(
    quality: ExecutionQualityBreakdown,
    offer_group: MarketOfferGroup,
) -> tuple[ExecutionBlocker, ...]:
    blockers: list[ExecutionBlocker] = []

    if not offer_group.offer_exists:
        blockers.append(ExecutionBlocker(tier="BINDING", code="no_offer_found"))
        blockers.append(ExecutionBlocker(tier="BINDING", code="market_unavailable"))

    if offer_group.template_binding_status == "NO_BIND":
        blockers.append(ExecutionBlocker(tier="BINDING", code="template_bind_failed"))

    if quality.price_integrity_score < 0.5:
        blockers.append(ExecutionBlocker(tier="QUALITY", code="price_integrity_low"))

    if quality.bookmaker_diversity_score < 0.34:
        blockers.append(ExecutionBlocker(tier="QUALITY", code="insufficient_bookmaker_diversity"))

    if quality.retrievability_score < 0.52:
        blockers.append(ExecutionBlocker(tier="PRODUCT", code="retrievability_low"))

    if quality.publishability_score < 0.58:
        blockers.append(ExecutionBlocker(tier="PRODUCT", code="publishability_low"))

    return tuple(blockers)
    