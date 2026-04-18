from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from app.vnext.markets.models import LineTemplate, MarketFamily

OfferSide = Literal["OVER", "UNDER", "YES", "NO", "HOME_OVER", "AWAY_OVER", "HOME_UNDER", "AWAY_UNDER", "HOME", "AWAY"]
TemplateBindingStatus = Literal["EXACT", "RELAXED", "NO_BIND"]
BlockerTier = Literal["BINDING", "QUALITY", "PRODUCT"]


@dataclass(slots=True, frozen=True)
class MarketOffer:
    bookmaker_id: int
    bookmaker_name: str
    market_family: MarketFamily
    side: OfferSide
    line: float | None
    team_scope: Literal["HOME", "AWAY", "NONE"]
    odds_decimal: float
    normalized_market_label: str
    offer_timestamp_utc: datetime | None
    freshness_seconds: int | None
    raw_source_ref: str
    source: str = "live_offer.v1"


@dataclass(slots=True, frozen=True)
class MarketOfferGroup:
    template_key: str
    market_family: MarketFamily
    side: OfferSide
    team_scope: Literal["HOME", "AWAY", "NONE"]
    requested_line_family: str
    bound_line: float | None
    template_binding_status: TemplateBindingStatus
    offers: tuple[MarketOffer, ...]
    offer_exists: bool
    source: str = "offer_group.v1"


@dataclass(slots=True, frozen=True)
class ExecutionBlocker:
    tier: BlockerTier
    code: str
    detail: str = ""
    source: str = "execution_blocker.v1"


@dataclass(slots=True, frozen=True)
class ExecutionQualityBreakdown:
    offer_exists_score: float
    template_binding_score: float
    market_clarity_score: float
    bookmaker_diversity_score: float
    price_integrity_score: float
    freshness_score: float
    retrievability_score: float
    publishability_score: float
    source: str = "execution_quality.v1"


@dataclass(slots=True, frozen=True)
class ExecutionCandidate:
    template_key: str
    market_family: MarketFamily
    template_binding_status: TemplateBindingStatus
    offer_group: MarketOfferGroup
    selected_offer: MarketOffer | None
    alternatives: tuple[MarketOffer, ...]
    offer_exists: bool
    is_blocked: bool
    is_selectable: bool
    selection_score: float
    quality: ExecutionQualityBreakdown
    blockers: tuple[ExecutionBlocker, ...] = ()
    explanation: str = ""
    source: str = "execution_candidate.v1"


@dataclass(slots=True, frozen=True)
class ExecutableMarketSelectionResult:
    fixture_id: int
    template_key: str
    execution_candidate: ExecutionCandidate | None
    alternatives: tuple[ExecutionCandidate, ...]
    offer_chosen: MarketOffer | None
    no_executable_vehicle_reason: str | None
    source_version: str = "execution_selection.v1"
    notes: tuple[str, ...] = field(default_factory=tuple)
