from __future__ import annotations

from app.vnext.execution.models import MarketOffer, MarketOfferGroup, TemplateBindingStatus
from app.vnext.markets.models import LineTemplate


def _preferred_lines(line_family: str) -> tuple[float, ...]:
    mapping = {
        "over_1_5_or_2_5": (1.5, 2.5),
        "under_2_5_or_3_5": (2.5, 3.5),
        "home_over_0_5_or_1_5": (0.5, 1.5),
        "away_over_0_5_or_1_5": (0.5, 1.5),
        "home_under_1_5_or_2_5": (1.5, 2.5),
        "away_under_1_5_or_2_5": (1.5, 2.5),
        "btts_yes_core": (),
        "btts_no_core": (),
        "home_result_lab_only": (),
        "away_result_lab_only": (),
    }
    return mapping.get(line_family, ())


def _binding_status(template: LineTemplate, offers: tuple[MarketOffer, ...]) -> tuple[TemplateBindingStatus, float | None]:
    if not offers:
        return "NO_BIND", None
    preferred = _preferred_lines(template.suggested_line_family)
    if not preferred:
        return "EXACT", offers[0].line
    exact = [offer for offer in offers if offer.line in preferred]
    if exact:
        return "EXACT", exact[0].line
    return "RELAXED", offers[0].line


def bind_offers_to_template(
    template: LineTemplate,
    offers: tuple[MarketOffer, ...],
) -> MarketOfferGroup:
    matching = tuple(
        offer
        for offer in offers
        if offer.market_family == template.family
        and offer.side == template.direction
        and offer.team_scope == ("HOME" if template.direction.startswith("HOME") else "AWAY" if template.direction.startswith("AWAY") else "NONE")
    )
    status, bound_line = _binding_status(template, matching)
    return MarketOfferGroup(
        template_key=template.key,
        market_family=template.family,
        side=template.direction,  # type: ignore[arg-type]
        team_scope="HOME" if template.direction.startswith("HOME") else "AWAY" if template.direction.startswith("AWAY") else "NONE",
        requested_line_family=template.suggested_line_family,
        bound_line=bound_line,
        template_binding_status=status,
        offers=matching,
        offer_exists=bool(matching),
    )
