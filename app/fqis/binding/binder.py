from __future__ import annotations

from dataclasses import dataclass

from app.fqis.contracts.core import BookOffer, MarketIntent


@dataclass(slots=True, frozen=True)
class BindingResult:
    matched_offers: tuple[BookOffer, ...]
    rejected_offers: tuple[tuple[BookOffer, str], ...]


def bind_offers_to_intent(
    intent: MarketIntent,
    offers: tuple[BookOffer, ...],
) -> BindingResult:
    matched: list[BookOffer] = []
    rejected: list[tuple[BookOffer, str]] = []

    for offer in offers:
        reason = _reject_reason(intent, offer)
        if reason is None:
            matched.append(offer)
        else:
            rejected.append((offer, reason))

    matched_sorted = sorted(
        matched,
        key=lambda item: (
            item.odds_decimal,
            -(item.freshness_seconds or 999999),
        ),
        reverse=True,
    )

    return BindingResult(
        matched_offers=tuple(matched_sorted),
        rejected_offers=tuple(rejected),
    )


def select_best_bound_offer(
    intent: MarketIntent,
    offers: tuple[BookOffer, ...],
) -> BookOffer | None:
    result = bind_offers_to_intent(intent, offers)
    if not result.matched_offers:
        return None
    return result.matched_offers[0]


def _reject_reason(intent: MarketIntent, offer: BookOffer) -> str | None:
    if intent.event_id != offer.event_id:
        return "event_id_mismatch"

    if intent.family != offer.family:
        return "family_mismatch"

    if intent.side != offer.side:
        return "side_mismatch"

    if intent.period != offer.period:
        return "period_mismatch"

    if intent.team_role != offer.team_role:
        return "team_role_mismatch"

    if intent.line != offer.line:
        return "line_mismatch"

    return None

    