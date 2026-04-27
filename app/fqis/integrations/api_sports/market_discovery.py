from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Mapping

from app.fqis.integrations.api_sports.client import ApiSportsClient
from app.fqis.integrations.api_sports.schemas import ApiSportsResponse


class ApiSportsMarketSource(str, Enum):
    PRE_MATCH = "pre_match"
    LIVE = "live"


class FqisMarketFamily(str, Enum):
    TOTALS_FULL_TIME = "totals_full_time"
    TOTALS_HALF_TIME = "totals_half_time"
    BTTS = "both_teams_to_score"
    TEAM_TOTALS = "team_totals"
    MATCH_WINNER = "match_winner"


class FqisMarketPeriod(str, Enum):
    FULL_TIME = "full_time"
    FIRST_HALF = "first_half"
    TEAM = "team"


class MarketMappingStatus(str, Enum):
    MAPPED = "mapped"
    REVIEW = "review"
    IGNORED = "ignored"


@dataclass(frozen=True)
class ApiSportsMarketCandidate:
    source: ApiSportsMarketSource
    provider_market_id: int
    provider_name: str
    normalized_name: str
    status: MarketMappingStatus
    fqis_family: FqisMarketFamily | None
    period: FqisMarketPeriod | None
    requires_line: bool
    confidence: float
    reasons: tuple[str, ...]

    @property
    def provider_key(self) -> str:
        return f"api_sports:{self.source.value}:{self.provider_market_id}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": "api_sports",
            "source": self.source.value,
            "provider_market_id": self.provider_market_id,
            "provider_key": self.provider_key,
            "provider_name": self.provider_name,
            "normalized_name": self.normalized_name,
            "status": self.status.value,
            "fqis_family": self.fqis_family.value if self.fqis_family else None,
            "period": self.period.value if self.period else None,
            "requires_line": self.requires_line,
            "confidence": self.confidence,
            "reasons": list(self.reasons),
        }


def classify_market_bet(
    *,
    source: ApiSportsMarketSource,
    provider_market_id: int,
    provider_name: str,
) -> ApiSportsMarketCandidate:
    normalized = normalize_market_name(provider_name)
    if provider_market_id < 0:
        return _candidate(
            source=source,
            provider_market_id=provider_market_id,
            provider_name=provider_name,
            normalized_name=normalized,
            status=MarketMappingStatus.REVIEW,
            fqis_family=None,
            period=None,
            requires_line=False,
            confidence=0.0,
            reasons=("provider_market_id_negative",),
        )

    if not normalized:
        return _candidate(
            source=source,
            provider_market_id=provider_market_id,
            provider_name=provider_name,
            normalized_name=normalized,
            status=MarketMappingStatus.REVIEW,
            fqis_family=None,
            period=None,
            requires_line=False,
            confidence=0.0,
            reasons=("empty_market_name",),
        )

    excluded_reason = _excluded_market_reason(normalized)
    if excluded_reason:
        return _candidate(
            source=source,
            provider_market_id=provider_market_id,
            provider_name=provider_name,
            normalized_name=normalized,
            status=MarketMappingStatus.IGNORED,
            fqis_family=None,
            period=None,
            requires_line=False,
            confidence=0.95,
            reasons=(excluded_reason,),
        )

    if _is_btts(normalized):
        return _candidate(
            source=source,
            provider_market_id=provider_market_id,
            provider_name=provider_name,
            normalized_name=normalized,
            status=MarketMappingStatus.MAPPED,
            fqis_family=FqisMarketFamily.BTTS,
            period=FqisMarketPeriod.FULL_TIME,
            requires_line=False,
            confidence=0.95,
            reasons=("matched_btts",),
        )

    if _is_match_winner(normalized):
        return _candidate(
            source=source,
            provider_market_id=provider_market_id,
            provider_name=provider_name,
            normalized_name=normalized,
            status=MarketMappingStatus.MAPPED,
            fqis_family=FqisMarketFamily.MATCH_WINNER,
            period=FqisMarketPeriod.FULL_TIME,
            requires_line=False,
            confidence=0.95,
            reasons=("matched_match_winner",),
        )

    if _is_team_total(normalized):
        return _candidate(
            source=source,
            provider_market_id=provider_market_id,
            provider_name=provider_name,
            normalized_name=normalized,
            status=MarketMappingStatus.MAPPED,
            fqis_family=FqisMarketFamily.TEAM_TOTALS,
            period=FqisMarketPeriod.TEAM,
            requires_line=True,
            confidence=0.85,
            reasons=("matched_team_total",),
        )

    if _is_half_total(normalized):
        return _candidate(
            source=source,
            provider_market_id=provider_market_id,
            provider_name=provider_name,
            normalized_name=normalized,
            status=MarketMappingStatus.MAPPED,
            fqis_family=FqisMarketFamily.TOTALS_HALF_TIME,
            period=FqisMarketPeriod.FIRST_HALF,
            requires_line=True,
            confidence=0.85,
            reasons=("matched_half_time_total",),
        )

    if _is_full_time_total(normalized):
        return _candidate(
            source=source,
            provider_market_id=provider_market_id,
            provider_name=provider_name,
            normalized_name=normalized,
            status=MarketMappingStatus.MAPPED,
            fqis_family=FqisMarketFamily.TOTALS_FULL_TIME,
            period=FqisMarketPeriod.FULL_TIME,
            requires_line=True,
            confidence=0.90,
            reasons=("matched_full_time_total",),
        )

    return _candidate(
        source=source,
        provider_market_id=provider_market_id,
        provider_name=provider_name,
        normalized_name=normalized,
        status=MarketMappingStatus.REVIEW,
        fqis_family=None,
        period=None,
        requires_line=False,
        confidence=0.0,
        reasons=("not_mapped_to_priority_market",),
    )


def discover_markets(
    client: ApiSportsClient,
    *,
    source: ApiSportsMarketSource,
    search: str | None = None,
    include_unmapped: bool = False,
) -> list[ApiSportsMarketCandidate]:
    response = _fetch_bets(client, source=source, search=search)
    candidates = [
        classify_market_bet(
            source=source,
            provider_market_id=item["id"],
            provider_name=item["name"],
        )
        for item in extract_market_items(response)
    ]
    if include_unmapped:
        return candidates
    return [candidate for candidate in candidates if candidate.status is MarketMappingStatus.MAPPED]


def discover_all_markets(
    client: ApiSportsClient,
    *,
    search: str | None = None,
    include_unmapped: bool = False,
) -> list[ApiSportsMarketCandidate]:
    return [
        *discover_markets(
            client,
            source=ApiSportsMarketSource.PRE_MATCH,
            search=search,
            include_unmapped=include_unmapped,
        ),
        *discover_markets(
            client,
            source=ApiSportsMarketSource.LIVE,
            search=search,
            include_unmapped=include_unmapped,
        ),
    ]


def build_market_discovery_report(
    candidates: Iterable[ApiSportsMarketCandidate],
) -> dict[str, Any]:
    rows = [candidate.to_dict() for candidate in candidates]
    mapped = [row for row in rows if row["status"] == MarketMappingStatus.MAPPED.value]
    review = [row for row in rows if row["status"] == MarketMappingStatus.REVIEW.value]
    ignored = [row for row in rows if row["status"] == MarketMappingStatus.IGNORED.value]
    return {
        "status": "COMPLETED",
        "mode": "shadow_only_market_discovery",
        "provider": "api_sports_api_football",
        "summary": {
            "total": len(rows),
            "mapped": len(mapped),
            "review": len(review),
            "ignored": len(ignored),
        },
        "markets": rows,
    }


def extract_market_items(response: ApiSportsResponse) -> list[dict[str, Any]]:
    raw_items = response.response if isinstance(response.response, list) else []
    items: list[dict[str, Any]] = []
    for raw in raw_items:
        if not isinstance(raw, Mapping):
            continue
        market_id = _safe_int(raw.get("id"))
        market_name = raw.get("name")
        if market_id is None or not isinstance(market_name, str):
            continue
        items.append({"id": market_id, "name": market_name})
    return items


def normalize_market_name(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip().lower())


def _fetch_bets(
    client: ApiSportsClient,
    *,
    source: ApiSportsMarketSource,
    search: str | None,
) -> ApiSportsResponse:
    if source is ApiSportsMarketSource.PRE_MATCH:
        return client.odds_bets(search=search)
    if source is ApiSportsMarketSource.LIVE:
        return client.live_odds_bets(search=search)
    raise ValueError(f"Unsupported API-Sports market source: {source}")


def _candidate(
    *,
    source: ApiSportsMarketSource,
    provider_market_id: int,
    provider_name: str,
    normalized_name: str,
    status: MarketMappingStatus,
    fqis_family: FqisMarketFamily | None,
    period: FqisMarketPeriod | None,
    requires_line: bool,
    confidence: float,
    reasons: tuple[str, ...],
) -> ApiSportsMarketCandidate:
    return ApiSportsMarketCandidate(
        source=source,
        provider_market_id=provider_market_id,
        provider_name=provider_name,
        normalized_name=normalized_name,
        status=status,
        fqis_family=fqis_family,
        period=period,
        requires_line=requires_line,
        confidence=confidence,
        reasons=reasons,
    )


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _excluded_market_reason(normalized: str) -> str | None:
    if _contains_any(normalized, ("corner", "corners")):
        return "excluded_corners_market"
    if _contains_any(normalized, ("card", "cards", "booking", "bookings")):
        return "excluded_cards_market"
    if _contains_any(normalized, ("penalty", "penalties")):
        return "excluded_penalty_market"
    if _contains_any(normalized, ("player", "scorer", "assist")):
        return "excluded_player_prop_market"
    return None


def _is_btts(normalized: str) -> bool:
    return "both teams" in normalized and "score" in normalized


def _is_match_winner(normalized: str) -> bool:
    return normalized in {"match winner", "1x2"} or "match winner" in normalized


def _is_team_total(normalized: str) -> bool:
    return "team" in normalized and _is_over_under_name(normalized) and "goal" in normalized


def _is_half_total(normalized: str) -> bool:
    return _is_over_under_name(normalized) and "goal" in normalized and _contains_any(
        normalized,
        ("1st half", "first half", "half time", "halftime", "1st period"),
    )


def _is_full_time_total(normalized: str) -> bool:
    if not (_is_over_under_name(normalized) and "goal" in normalized):
        return False
    if _is_half_total(normalized) or _is_team_total(normalized):
        return False
    return True


def _is_over_under_name(normalized: str) -> bool:
    return "over/under" in normalized or ("over" in normalized and "under" in normalized)


def _contains_any(value: str, needles: tuple[str, ...]) -> bool:
    return any(needle in value for needle in needles)