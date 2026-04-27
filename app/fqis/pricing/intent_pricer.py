from __future__ import annotations

from dataclasses import dataclass

from app.fqis.contracts.core import BookOffer, ExecutableBet, MarketIntent, StatisticalThesis
from app.fqis.pricing.math import compute_edge, compute_ev, implied_probability


@dataclass(slots=True, frozen=True)
class PricedIntent:
    thesis: StatisticalThesis
    intent: MarketIntent
    offer: BookOffer
    p_real: float
    p_implied: float
    edge: float
    ev: float
    score_stat: float
    score_exec: float
    score_final: float


def price_intent(
    thesis: StatisticalThesis,
    intent: MarketIntent,
    offer: BookOffer,
    *,
    p_real: float,
) -> PricedIntent:
    p_implied = implied_probability(offer.odds_decimal)
    edge = compute_edge(p_real, p_implied)
    ev = compute_ev(p_real, offer.odds_decimal)

    score_stat = _score_stat(thesis.strength, thesis.confidence, edge, ev)
    score_exec = _score_exec(offer.freshness_seconds)
    score_final = (0.7 * score_stat) + (0.3 * score_exec)

    return PricedIntent(
        thesis=thesis,
        intent=intent,
        offer=offer,
        p_real=p_real,
        p_implied=p_implied,
        edge=edge,
        ev=ev,
        score_stat=score_stat,
        score_exec=score_exec,
        score_final=score_final,
    )


def to_executable_bet(priced: PricedIntent) -> ExecutableBet:
    return ExecutableBet(
        event_id=priced.intent.event_id,
        thesis_key=priced.thesis.thesis_key,
        family=priced.intent.family,
        side=priced.intent.side,
        period=priced.intent.period,
        team_role=priced.intent.team_role,
        line=priced.intent.line,
        bookmaker_id=priced.offer.bookmaker_id,
        bookmaker_name=priced.offer.bookmaker_name,
        odds_decimal=priced.offer.odds_decimal,
        p_real=priced.p_real,
        p_implied=priced.p_implied,
        edge=priced.edge,
        ev=priced.ev,
        score_stat=priced.score_stat,
        score_exec=priced.score_exec,
        score_final=priced.score_final,
        rationale=priced.intent.rationale + (
            f"strength={priced.thesis.strength:.4f}",
            f"confidence={priced.thesis.confidence:.4f}",
        ),
    )


def _score_stat(strength: float, confidence: float, edge: float, ev: float) -> float:
    base = (
        (0.35 * strength)
        + (0.25 * confidence)
        + (0.20 * max(edge, 0.0))
        + (0.20 * max(ev, 0.0))
    )
    return max(0.0, min(1.0, base))


def _score_exec(freshness_seconds: int | None) -> float:
    if freshness_seconds is None:
        return 0.55
    if freshness_seconds <= 10:
        return 0.95
    if freshness_seconds <= 30:
        return 0.85
    if freshness_seconds <= 60:
        return 0.72
    return 0.55