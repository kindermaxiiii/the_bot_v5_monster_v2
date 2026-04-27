from __future__ import annotations

from dataclasses import dataclass
from statistics import mean

from app.fqis.contracts.core import BookOffer


@dataclass(slots=True, frozen=True)
class MarketPriorProbability:
    intent_key: str
    group_key: str
    bookmaker_id: int | None
    bookmaker_name: str
    odds_decimal: float
    raw_implied_probability: float
    no_vig_probability: float
    overround: float


@dataclass(slots=True, frozen=True)
class MarketPriorGroup:
    group_key: str
    event_id: int
    bookmaker_id: int | None
    bookmaker_name: str
    outcome_count: int
    overround: float
    probabilities: tuple[MarketPriorProbability, ...]
    rejection_reason: str | None = None

    @property
    def is_complete(self) -> bool:
        return self.rejection_reason is None and bool(self.probabilities)


@dataclass(slots=True, frozen=True)
class MarketModelComparison:
    intent_key: str
    p_model: float
    p_market_no_vig: float | None
    delta_model_market: float | None

    @property
    def has_market_prior(self) -> bool:
        return self.p_market_no_vig is not None


def implied_probability_from_decimal_odds(odds_decimal: float) -> float:
    if odds_decimal <= 1.0:
        raise ValueError("decimal odds must be > 1.0")

    return 1.0 / odds_decimal


def normalize_no_vig_probabilities(raw_probabilities: tuple[float, ...]) -> tuple[float, ...]:
    if not raw_probabilities:
        raise ValueError("raw_probabilities must not be empty")

    if any(probability < 0.0 for probability in raw_probabilities):
        raise ValueError("raw probabilities must be >= 0")

    total = sum(raw_probabilities)

    if total <= 0.0:
        raise ValueError("raw probability sum must be > 0")

    return tuple(probability / total for probability in raw_probabilities)


def build_market_prior_groups(
    offers: tuple[BookOffer, ...],
    *,
    min_outcomes: int = 2,
) -> tuple[MarketPriorGroup, ...]:
    if min_outcomes <= 0:
        raise ValueError("min_outcomes must be > 0")

    grouped: dict[str, list[BookOffer]] = {}

    for offer in offers:
        grouped.setdefault(_market_group_key(offer), []).append(offer)

    groups: list[MarketPriorGroup] = []

    for group_key in sorted(grouped):
        group_offers = tuple(grouped[group_key])
        first = group_offers[0]

        if len(group_offers) < min_outcomes:
            groups.append(
                MarketPriorGroup(
                    group_key=group_key,
                    event_id=first.event_id,
                    bookmaker_id=first.bookmaker_id,
                    bookmaker_name=first.bookmaker_name,
                    outcome_count=len(group_offers),
                    overround=0.0,
                    probabilities=(),
                    rejection_reason=f"market group has fewer than {min_outcomes} outcomes",
                )
            )
            continue

        raw_probabilities = tuple(
            implied_probability_from_decimal_odds(offer.odds_decimal)
            for offer in group_offers
        )
        overround = sum(raw_probabilities)
        no_vig_probabilities = normalize_no_vig_probabilities(raw_probabilities)

        probabilities = tuple(
            MarketPriorProbability(
                intent_key=offer_probability_key(offer),
                group_key=group_key,
                bookmaker_id=offer.bookmaker_id,
                bookmaker_name=offer.bookmaker_name,
                odds_decimal=offer.odds_decimal,
                raw_implied_probability=raw_probability,
                no_vig_probability=no_vig_probability,
                overround=overround,
            )
            for offer, raw_probability, no_vig_probability in zip(
                group_offers,
                raw_probabilities,
                no_vig_probabilities,
            )
        )

        groups.append(
            MarketPriorGroup(
                group_key=group_key,
                event_id=first.event_id,
                bookmaker_id=first.bookmaker_id,
                bookmaker_name=first.bookmaker_name,
                outcome_count=len(group_offers),
                overround=overround,
                probabilities=probabilities,
                rejection_reason=None,
            )
        )

    return tuple(groups)


def build_market_prior_by_intent_key(
    offers: tuple[BookOffer, ...],
    *,
    min_outcomes: int = 2,
) -> dict[str, float]:
    groups = build_market_prior_groups(offers, min_outcomes=min_outcomes)
    values_by_intent: dict[str, list[float]] = {}

    for group in groups:
        if not group.is_complete:
            continue

        for probability in group.probabilities:
            values_by_intent.setdefault(probability.intent_key, []).append(
                probability.no_vig_probability
            )

    return {
        intent_key: mean(values)
        for intent_key, values in values_by_intent.items()
    }


def compare_model_to_market_prior(
    p_model_by_intent_key: dict[str, float],
    p_market_by_intent_key: dict[str, float],
) -> tuple[MarketModelComparison, ...]:
    comparisons: list[MarketModelComparison] = []

    for intent_key in sorted(p_model_by_intent_key):
        p_model = float(p_model_by_intent_key[intent_key])
        p_market = p_market_by_intent_key.get(intent_key)

        if p_market is None:
            comparisons.append(
                MarketModelComparison(
                    intent_key=intent_key,
                    p_model=p_model,
                    p_market_no_vig=None,
                    delta_model_market=None,
                )
            )
            continue

        comparisons.append(
            MarketModelComparison(
                intent_key=intent_key,
                p_model=p_model,
                p_market_no_vig=float(p_market),
                delta_model_market=p_model - float(p_market),
            )
        )

    return tuple(comparisons)


def offer_probability_key(offer: BookOffer) -> str:
    line = "NA" if offer.line is None else str(offer.line)
    return f"{offer.family.value}|{offer.side.value}|{offer.team_role.value}|{line}"


def _market_group_key(offer: BookOffer) -> str:
    line = "NA" if offer.line is None else str(offer.line)

    return (
        f"{offer.event_id}|"
        f"{offer.bookmaker_id}|"
        f"{offer.family.value}|"
        f"{offer.period.value}|"
        f"{offer.team_role.value}|"
        f"{line}"
    )

    