from __future__ import annotations

from app.fqis.pricing.intent_pricer import PricedIntent


def rank_priced_intents(priced_intents: tuple[PricedIntent, ...]) -> tuple[PricedIntent, ...]:
    return tuple(
        sorted(
            priced_intents,
            key=lambda item: (
                item.score_final,
                item.ev,
                item.edge,
                item.offer.odds_decimal,
            ),
            reverse=True,
        )
    )


def select_best_priced_intent(priced_intents: tuple[PricedIntent, ...]) -> PricedIntent | None:
    ranked = rank_priced_intents(priced_intents)
    if not ranked:
        return None
    return ranked[0]