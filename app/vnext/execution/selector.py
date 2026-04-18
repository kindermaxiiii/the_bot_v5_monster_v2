from __future__ import annotations

from dataclasses import replace

from app.vnext.execution.models import (
    ExecutionCandidate,
    ExecutableMarketSelectionResult,
    MarketOffer,
    MarketOfferGroup,
)
from app.vnext.execution.offer_binding import bind_offers_to_template
from app.vnext.execution.quality import build_execution_quality, execution_blockers
from app.vnext.markets.lines import line_template
from app.vnext.markets.models import LineTemplate
from app.vnext.selection.models import MatchMarketSelectionResult


def _clip(value: float, *, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _selection_score(quality) -> float:
    return _clip(
        (quality.publishability_score * 0.50)
        + (quality.retrievability_score * 0.25)
        + (quality.bookmaker_diversity_score * 0.15)
        + (quality.price_integrity_score * 0.10)
    )


def _offer_score(offer: MarketOffer) -> float:
    freshness = 1.0
    if offer.freshness_seconds is not None:
        if offer.freshness_seconds <= 30:
            freshness = 1.0
        elif offer.freshness_seconds <= 120:
            freshness = 0.8
        elif offer.freshness_seconds <= 300:
            freshness = 0.6
        elif offer.freshness_seconds <= 600:
            freshness = 0.4
        else:
            freshness = 0.2
    if offer.odds_decimal <= 1.01:
        integrity = 0.0
    elif offer.odds_decimal <= 1.10:
        integrity = 0.5
    elif offer.odds_decimal <= 8.0:
        integrity = 1.0
    elif offer.odds_decimal <= 15.0:
        integrity = 0.6
    else:
        integrity = 0.3
    return _clip((freshness * 0.55) + (integrity * 0.45))


def _bound_offer_group(offer_group: MarketOfferGroup) -> MarketOfferGroup:
    if offer_group.bound_line is None:
        return offer_group
    bound_offers = tuple(offer for offer in offer_group.offers if offer.line == offer_group.bound_line)
    return replace(
        offer_group,
        offers=bound_offers,
        offer_exists=bool(bound_offers),
    )


def _build_candidate(template: LineTemplate, offers: tuple[MarketOffer, ...]) -> ExecutionCandidate:
    offer_group = bind_offers_to_template(template, offers)
    bound_group = _bound_offer_group(offer_group)
    quality = build_execution_quality(bound_group)
    blockers = execution_blockers(quality, bound_group)
    selected_offer = max(bound_group.offers, key=_offer_score, default=None)
    alternatives = tuple(sorted(bound_group.offers, key=_offer_score, reverse=True))
    is_blocked = bool(blockers)
    is_selectable = bound_group.offer_exists and not is_blocked
    selection_score = _selection_score(quality) if is_selectable else 0.0
    return ExecutionCandidate(
        template_key=template.key,
        market_family=template.family,
        template_binding_status=offer_group.template_binding_status,
        offer_group=bound_group,
        selected_offer=selected_offer,
        alternatives=alternatives,
        offer_exists=bound_group.offer_exists,
        is_blocked=is_blocked,
        is_selectable=is_selectable,
        selection_score=round(selection_score, 4),
        quality=quality,
        blockers=blockers,
        explanation=",".join(
            [
                template.key,
                offer_group.template_binding_status,
                f"diversity={quality.bookmaker_diversity_score:.2f}",
            ]
        ),
    )


def build_executable_market_selection(
    selection_result: MatchMarketSelectionResult,
    offers: tuple[MarketOffer, ...],
) -> ExecutableMarketSelectionResult:
    template_keys = []
    if selection_result.best_candidate is not None:
        template_keys.append(selection_result.best_candidate.candidate.line_template.key)
    for candidate in selection_result.translation_result.candidates:
        if candidate.line_template.key not in template_keys:
            template_keys.append(candidate.line_template.key)

    candidates: list[ExecutionCandidate] = []
    for template_key in template_keys:
        template = line_template(template_key)
        candidates.append(_build_candidate(template, offers))

    selectable = [candidate for candidate in candidates if candidate.is_selectable]
    if not selectable:
        reason = "no_offer_found"
        if any(candidate.offer_exists for candidate in candidates):
            reason = "publishability_low"
        return ExecutableMarketSelectionResult(
            fixture_id=selection_result.translation_result.posterior_result.prior_result.fixture_id,
            template_key=template_keys[0] if template_keys else "UNKNOWN",
            execution_candidate=None,
            alternatives=tuple(candidates),
            offer_chosen=None,
            no_executable_vehicle_reason=reason,
        )

    ranked = sorted(selectable, key=lambda candidate: (candidate.selection_score, candidate.quality.publishability_score), reverse=True)
    best = ranked[0]
    return ExecutableMarketSelectionResult(
        fixture_id=selection_result.translation_result.posterior_result.prior_result.fixture_id,
        template_key=best.template_key,
        execution_candidate=best,
        alternatives=tuple(candidates),
        offer_chosen=best.selected_offer,
        no_executable_vehicle_reason=None,
    )
