from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from app.fqis.integrations.api_sports.market_discovery import (
    ApiSportsMarketSource,
    FqisMarketFamily,
    MarketMappingStatus,
    classify_market_bet,
)


class ApiSportsNormalizationError(RuntimeError):
    """Raised when raw API-Sports data cannot be normalized safely."""


class FqisOddsSelection(str, Enum):
    HOME = "home"
    DRAW = "draw"
    AWAY = "away"
    OVER = "over"
    UNDER = "under"
    YES = "yes"
    NO = "no"
    TEAM_HOME = "team_home"
    TEAM_AWAY = "team_away"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class FqisNormalizedFixture:
    provider: str
    provider_fixture_id: str
    fixture_key: str
    league_id: str | None
    league_name: str | None
    season: int | None
    country: str | None
    kickoff_utc: str | None
    status_short: str | None
    status_long: str | None
    elapsed: int | None
    home_team_id: str | None
    home_team_name: str | None
    away_team_id: str | None
    away_team_name: str | None
    raw: Mapping[str, Any] = field(repr=False)


@dataclass(frozen=True)
class FqisNormalizedOddsOffer:
    provider: str
    source: str
    provider_fixture_id: str | None
    fixture_key: str | None
    provider_bookmaker_id: str | None
    bookmaker_name: str | None
    provider_market_id: str
    provider_market_name: str
    provider_market_key: str
    fqis_market_family: str | None
    mapping_status: str
    period: str | None
    line: float | None
    selection: str
    label: str
    decimal_odds: float | None
    offered_at_utc: str | None
    normalization_status: str
    warnings: tuple[str, ...] = ()
    raw: Mapping[str, Any] = field(repr=False, default_factory=dict)


@dataclass(frozen=True)
class FqisNormalizedBatch:
    provider: str
    source: str
    run_id: str
    snapshot_id: str | None
    normalized_at_utc: str
    fixtures: tuple[FqisNormalizedFixture, ...]
    odds_offers: tuple[FqisNormalizedOddsOffer, ...]
    summary: Mapping[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "source": self.source,
            "run_id": self.run_id,
            "snapshot_id": self.snapshot_id,
            "normalized_at_utc": self.normalized_at_utc,
            "summary": dict(self.summary),
            "fixtures": [asdict(item) for item in self.fixtures],
            "odds_offers": [asdict(item) for item in self.odds_offers],
        }


class ApiSportsNormalizer:
    provider = "api_sports_api_football"

    def normalize_payload(
        self,
        payload: Mapping[str, Any],
        *,
        source: str,
        run_id: str,
        snapshot_id: str | None = None,
    ) -> FqisNormalizedBatch:
        source_value = _normalize_source(source)
        response = payload.get("response", [])
        if not isinstance(response, Sequence) or isinstance(response, (str, bytes)):
            response = []

        fixtures: list[FqisNormalizedFixture] = []
        odds_offers: list[FqisNormalizedOddsOffer] = []

        if _looks_like_fixtures_payload(payload, source_value):
            fixtures = [fixture for item in response if (fixture := normalize_fixture(item)) is not None]
        elif _looks_like_odds_payload(payload, source_value):
            odds_offers = normalize_odds_response(response, source=source_value)
        else:
            # Try both paths defensively. Unknown payloads should never crash the audit pipeline.
            fixtures = [fixture for item in response if (fixture := normalize_fixture(item)) is not None]
            odds_offers = normalize_odds_response(response, source=source_value)

        summary = {
            "fixtures": len(fixtures),
            "odds_offers": len(odds_offers),
            "odds_normalized": sum(1 for item in odds_offers if item.normalization_status == "NORMALIZED"),
            "odds_review": sum(1 for item in odds_offers if item.normalization_status == "REVIEW"),
            "odds_rejected": sum(1 for item in odds_offers if item.normalization_status == "REJECTED"),
        }

        return FqisNormalizedBatch(
            provider=self.provider,
            source=source_value,
            run_id=run_id,
            snapshot_id=snapshot_id,
            normalized_at_utc=datetime.now(UTC).isoformat(),
            fixtures=tuple(fixtures),
            odds_offers=tuple(odds_offers),
            summary=summary,
        )

    def normalize_snapshot_file(self, path: str | Path) -> FqisNormalizedBatch:
        snapshot_path = Path(path)
        data = json.loads(snapshot_path.read_text(encoding="utf-8"))
        if not isinstance(data, Mapping):
            raise ApiSportsNormalizationError(f"Snapshot must be a JSON object: {snapshot_path}")

        payload = _extract_payload(data)
        metadata = _extract_metadata(data)
        source = _infer_source(data, snapshot_path)
        run_id = str(metadata.get("run_id") or data.get("run_id") or "manual_normalization")
        snapshot_id = _optional_str(metadata.get("snapshot_id") or data.get("snapshot_id"))
        return self.normalize_payload(payload, source=source, run_id=run_id, snapshot_id=snapshot_id)


def normalize_fixture(raw_item: Any) -> FqisNormalizedFixture | None:
    item = _as_mapping(raw_item)
    fixture = _as_mapping(item.get("fixture"))
    league = _as_mapping(item.get("league"))
    teams = _as_mapping(item.get("teams"))
    home = _as_mapping(teams.get("home"))
    away = _as_mapping(teams.get("away"))
    status = _as_mapping(fixture.get("status"))

    fixture_id = _optional_str(fixture.get("id") or item.get("fixture_id") or item.get("id"))
    if not fixture_id:
        return None

    return FqisNormalizedFixture(
        provider="api_sports_api_football",
        provider_fixture_id=fixture_id,
        fixture_key=f"api_sports:fixture:{fixture_id}",
        league_id=_optional_str(league.get("id")),
        league_name=_optional_str(league.get("name")),
        season=_safe_int(league.get("season")),
        country=_optional_str(league.get("country")),
        kickoff_utc=_optional_str(fixture.get("date")),
        status_short=_optional_str(status.get("short")),
        status_long=_optional_str(status.get("long")),
        elapsed=_safe_int(status.get("elapsed")),
        home_team_id=_optional_str(home.get("id")),
        home_team_name=_optional_str(home.get("name")),
        away_team_id=_optional_str(away.get("id")),
        away_team_name=_optional_str(away.get("name")),
        raw=item,
    )


def normalize_odds_response(items: Iterable[Any], *, source: str) -> list[FqisNormalizedOddsOffer]:
    source_value = _normalize_source(source)
    offers: list[FqisNormalizedOddsOffer] = []

    for raw_item in items:
        item = _as_mapping(raw_item)
        fixture = _as_mapping(item.get("fixture"))
        fixture_id = _optional_str(fixture.get("id") or item.get("fixture_id") or item.get("fixture"))
        offered_at = _optional_str(item.get("update") or item.get("last_update") or fixture.get("date"))
        bookmakers = item.get("bookmakers", [])
        if not isinstance(bookmakers, Sequence) or isinstance(bookmakers, (str, bytes)):
            bookmakers = []

        for bookmaker_raw in bookmakers:
            bookmaker = _as_mapping(bookmaker_raw)
            bookmaker_id = _optional_str(bookmaker.get("id"))
            bookmaker_name = _optional_str(bookmaker.get("name"))
            bets = bookmaker.get("bets", [])
            if not isinstance(bets, Sequence) or isinstance(bets, (str, bytes)):
                bets = []

            for bet_raw in bets:
                bet = _as_mapping(bet_raw)
                bet_id = _optional_str(bet.get("id"))
                bet_name = _optional_str(bet.get("name")) or "unknown"
                if not bet_id:
                    continue

                market_candidate = classify_market_bet(
                    {"id": bet_id, "name": bet_name},
                    source=_market_source(source_value),
                )

                values = bet.get("values", [])
                if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
                    values = []

                for value_raw in values:
                    value = _as_mapping(value_raw)
                    label = _optional_str(value.get("value") or value.get("label") or value.get("name")) or "unknown"
                    decimal_odds = _safe_float(value.get("odd") or value.get("odds"))
                    selection = infer_selection(label, market_name=bet_name)
                    line = infer_line(label)
                    period = infer_period(bet_name, label)
                    warnings = _offer_warnings(
                        market_status=market_candidate.status.value,
                        decimal_odds=decimal_odds,
                        line=line,
                        selection=selection,
                    )
                    normalization_status = _normalization_status(
                        market_status=market_candidate.status.value,
                        decimal_odds=decimal_odds,
                        warnings=warnings,
                    )

                    offers.append(
                        FqisNormalizedOddsOffer(
                            provider="api_sports_api_football",
                            source=source_value,
                            provider_fixture_id=fixture_id,
                            fixture_key=f"api_sports:fixture:{fixture_id}" if fixture_id else None,
                            provider_bookmaker_id=bookmaker_id,
                            bookmaker_name=bookmaker_name,
                            provider_market_id=bet_id,
                            provider_market_name=bet_name,
                            provider_market_key=market_candidate.provider_market_key,
                            fqis_market_family=(
                                market_candidate.fqis_market_family.value
                                if market_candidate.fqis_market_family is not None
                                else None
                            ),
                            mapping_status=market_candidate.status.value,
                            period=period,
                            line=line,
                            selection=selection.value,
                            label=label,
                            decimal_odds=decimal_odds,
                            offered_at_utc=offered_at,
                            normalization_status=normalization_status,
                            warnings=warnings,
                            raw=value,
                        )
                    )

    return offers


def infer_selection(label: str, *, market_name: str) -> FqisOddsSelection:
    normalized = _normalize_text(label)
    market_normalized = _normalize_text(market_name)

    if normalized in {"home", "1", "home win"}:
        return FqisOddsSelection.HOME
    if normalized in {"draw", "x"}:
        return FqisOddsSelection.DRAW
    if normalized in {"away", "2", "away win"}:
        return FqisOddsSelection.AWAY
    if normalized.startswith("over"):
        return FqisOddsSelection.OVER
    if normalized.startswith("under"):
        return FqisOddsSelection.UNDER
    if normalized in {"yes", "y"}:
        return FqisOddsSelection.YES
    if normalized in {"no", "n"}:
        return FqisOddsSelection.NO
    if "home" in normalized and "team" in market_normalized:
        return FqisOddsSelection.TEAM_HOME
    if "away" in normalized and "team" in market_normalized:
        return FqisOddsSelection.TEAM_AWAY
    return FqisOddsSelection.UNKNOWN


def infer_line(label: str) -> float | None:
    normalized = label.replace(",", ".")
    tokens = normalized.replace("+", " ").replace("-", " ").split()
    for token in reversed(tokens):
        try:
            return float(token)
        except ValueError:
            continue
    return None


def infer_period(market_name: str, label: str = "") -> str | None:
    text = _normalize_text(f"{market_name} {label}")
    if "1st half" in text or "first half" in text or "half time" in text or "halftime" in text:
        return "first_half"
    if "2nd half" in text or "second half" in text:
        return "second_half"
    if "full time" in text or "match" in text or "goals" in text:
        return "full_time"
    return None


class FqisNormalizedWriter:
    def __init__(self, root: str | Path = "data/normalized/api_sports") -> None:
        self.root = Path(root)

    def write(self, batch: FqisNormalizedBatch) -> Path:
        safe_run_id = _safe_path_part(batch.run_id)
        safe_source = _safe_path_part(batch.source)
        safe_snapshot = _safe_path_part(batch.snapshot_id or "no_snapshot")
        path = self.root / safe_run_id / f"normalized_{safe_source}_{safe_snapshot}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(batch.to_dict(), indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        return path


def _extract_payload(data: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = data.get("payload") or data.get("raw_payload") or data.get("api_response")
    if isinstance(payload, Mapping):
        return payload
    # Some Day 48 snapshots may already be the provider payload object.
    if "response" in data or "get" in data:
        return data
    raise ApiSportsNormalizationError("Snapshot payload is missing or invalid.")


def _extract_metadata(data: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = data.get("metadata") or data.get("manifest") or {}
    return metadata if isinstance(metadata, Mapping) else {}


def _infer_source(data: Mapping[str, Any], path: Path) -> str:
    metadata = _extract_metadata(data)
    raw = (
        metadata.get("source")
        or metadata.get("kind")
        or data.get("source")
        or data.get("kind")
        or path.stem
    )
    text = _normalize_text(str(raw))
    if "live" in text and "odd" in text:
        return "live"
    if "odd" in text:
        return "pre_match"
    if "fixture" in text:
        return "fixtures"
    return str(raw)


def _looks_like_fixtures_payload(payload: Mapping[str, Any], source: str) -> bool:
    endpoint = _normalize_text(str(payload.get("get", "")))
    return source == "fixtures" or endpoint == "fixtures"


def _looks_like_odds_payload(payload: Mapping[str, Any], source: str) -> bool:
    endpoint = _normalize_text(str(payload.get("get", "")))
    return source in {"pre_match", "live"} or endpoint.startswith("odds")


def _normalize_source(source: str) -> str:
    text = _normalize_text(source)
    if text in {"pre_match", "prematch", "odds", "odds_bets", "pre match"}:
        return "pre_match"
    if text in {"live", "live_odds", "odds_live"} or "live" in text:
        return "live"
    if "fixture" in text:
        return "fixtures"
    return text.replace(" ", "_")


def _market_source(source: str) -> ApiSportsMarketSource:
    return ApiSportsMarketSource.LIVE if source == "live" else ApiSportsMarketSource.PRE_MATCH


def _offer_warnings(
    *,
    market_status: str,
    decimal_odds: float | None,
    line: float | None,
    selection: FqisOddsSelection,
) -> tuple[str, ...]:
    warnings: list[str] = []
    if market_status == MarketMappingStatus.REVIEW.value:
        warnings.append("market_mapping_review")
    if market_status == MarketMappingStatus.IGNORED.value:
        warnings.append("market_mapping_ignored")
    if decimal_odds is None:
        warnings.append("missing_decimal_odds")
    elif decimal_odds <= 1.0:
        warnings.append("invalid_decimal_odds")
    elif decimal_odds > 100.0:
        warnings.append("extreme_decimal_odds")
    if selection == FqisOddsSelection.UNKNOWN:
        warnings.append("unknown_selection")
    if line is None and selection in {FqisOddsSelection.OVER, FqisOddsSelection.UNDER}:
        warnings.append("missing_total_line")
    return tuple(warnings)


def _normalization_status(*, market_status: str, decimal_odds: float | None, warnings: tuple[str, ...]) -> str:
    if market_status == MarketMappingStatus.IGNORED.value:
        return "REJECTED"
    if decimal_odds is None or decimal_odds <= 1.0:
        return "REJECTED"
    if warnings or market_status == MarketMappingStatus.REVIEW.value:
        return "REVIEW"
    return "NORMALIZED"


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _normalize_text(value: str) -> str:
    return " ".join(value.replace("-", "_").strip().lower().split())


def _safe_path_part(value: str) -> str:
    allowed = []
    for char in value:
        if char.isalnum() or char in {"_", "-", "."}:
            allowed.append(char)
        else:
            allowed.append("_")
    return "".join(allowed).strip("_") or "unknown"
