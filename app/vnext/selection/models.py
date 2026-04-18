from __future__ import annotations

from dataclasses import dataclass, field

from app.vnext.markets.models import MarketCandidate, MarketTranslationResult


@dataclass(slots=True, frozen=True)
class MatchBestCandidate:
    candidate: MarketCandidate
    selection_score: float
    rationale: tuple[str, ...] = ()
    source: str = "match_selector.v1"


@dataclass(slots=True, frozen=True)
class MatchMarketSelectionResult:
    translation_result: MarketTranslationResult
    best_candidate: MatchBestCandidate | None
    no_selection_reason: str | None
    source_version: str = "match_market_selection.v1"
    notes: tuple[str, ...] = field(default_factory=tuple)
