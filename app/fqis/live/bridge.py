from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from app.fqis.contracts.enums import ThesisKey
from app.fqis.live.adapters import adapt_live_match_to_features, adapt_live_offers_to_book_offers
from app.fqis.runtime.demo_runner import DemoCycleResult, run_demo_cycle


def run_live_bridge_cycle(
    live_match_row: Mapping[str, Any],
    live_offer_rows: Iterable[Mapping[str, Any]],
    *,
    p_real_by_thesis: dict[ThesisKey, dict[str, float]],
    min_strength: float,
    min_confidence: float,
    min_edge: float,
    min_ev: float,
    min_odds: float,
    max_odds: float,
    technical_min_edge: float = 0.0,
    technical_min_ev: float = -1.0,
) -> DemoCycleResult:
    features = adapt_live_match_to_features(live_match_row)
    offers = adapt_live_offers_to_book_offers(live_offer_rows)

    return run_demo_cycle(
        features,
        offers,
        p_real_by_thesis=p_real_by_thesis,
        min_strength=min_strength,
        min_confidence=min_confidence,
        min_edge=min_edge,
        min_ev=min_ev,
        min_odds=min_odds,
        max_odds=max_odds,
        technical_min_edge=technical_min_edge,
        technical_min_ev=technical_min_ev,
    )

    