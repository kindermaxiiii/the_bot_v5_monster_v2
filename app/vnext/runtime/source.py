from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.clients.api_football import APIFootballRequestError, api_client
from app.normalizers.fixtures import normalize_live_fixtures
from app.normalizers.odds import normalize_live_odds
from app.normalizers.statistics import normalize_fixture_statistics
from app.vnext.execution.models import MarketOffer
from app.vnext.live.models import LiveSnapshot
from app.vnext.live.normalizers import normalize_live_snapshot
from app.vnext.runtime.models import LiveSource


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _team_scope_from_side(side: str) -> str:
    if side.startswith("HOME"):
        return "HOME"
    if side.startswith("AWAY"):
        return "AWAY"
    return "NONE"


def _map_odds_row_to_offer(row: dict[str, Any], bookmaker_id: int) -> MarketOffer | None:
    market_key = row.get("market_key")
    if market_key == "ou":
        family = "OU_FT"
        selection = row.get("selection_name")
        if selection not in {"OVER", "UNDER"}:
            return None
        side = selection
        line = row.get("line_value")
    elif market_key == "btts":
        family = "BTTS"
        selection = row.get("selection_name")
        if selection == "BTTS_YES":
            side = "YES"
        elif selection == "BTTS_NO":
            side = "NO"
        else:
            return None
        line = None
    elif market_key == "team_total":
        family = "TEAM_TOTAL"
        selection = row.get("selection_name")
        if selection not in {"HOME_OVER", "AWAY_OVER", "HOME_UNDER", "AWAY_UNDER"}:
            return None
        side = selection
        line = row.get("line_value")
    elif market_key == "1x2":
        family = "RESULT"
        selection = row.get("selection_name")
        if selection not in {"HOME", "AWAY"}:
            return None
        side = selection
        line = None
    else:
        return None

    odds = row.get("odds_decimal")
    if odds is None:
        return None

    bookmaker_name = str(row.get("bookmaker") or "API-FOOTBALL")
    return MarketOffer(
        bookmaker_id=bookmaker_id,
        bookmaker_name=bookmaker_name,
        market_family=family,  # type: ignore[arg-type]
        side=side,  # type: ignore[arg-type]
        line=line,
        team_scope=_team_scope_from_side(side),
        odds_decimal=float(odds),
        normalized_market_label=str(row.get("market_name") or row.get("market_key") or family),
        offer_timestamp_utc=_now_utc(),
        freshness_seconds=0,
        raw_source_ref=f"live_odds:{bookmaker_name}",
    )


@dataclass(slots=True, frozen=True)
class SnapshotSource:
    snapshots: tuple[LiveSnapshot, ...]
    offers_by_fixture: dict[int, tuple[MarketOffer, ...]]

    def fetch_live_snapshots(self, max_matches: int) -> tuple[LiveSnapshot, ...]:
        return self.snapshots[:max_matches]

    def fetch_market_offers(self, fixture_id: int) -> tuple[MarketOffer, ...]:
        return self.offers_by_fixture.get(fixture_id, ())


@dataclass(slots=True)
class LiveApiSource:
    max_fixtures: int = 30
    _stats_cache: dict[int, dict[str, Any]] = field(default_factory=dict)
    _bookmaker_ids: dict[str, int] = field(default_factory=dict)

    def fetch_live_snapshots(self, max_matches: int) -> tuple[LiveSnapshot, ...]:
        raw_live = api_client.get_live_fixtures()
        fixtures = normalize_live_fixtures(raw_live)
        fixtures = fixtures[: min(max_matches, self.max_fixtures)]
        snapshots: list[LiveSnapshot] = []
        for fixture_row in fixtures:
            fixture_id = int(fixture_row.get("fixture_id") or 0)
            stats_row = self._fetch_stats_row(fixture_row, fixture_id)
            snapshots.append(normalize_live_snapshot(fixture_row, stats_row))
        return tuple(snapshots)

    def fetch_market_offers(self, fixture_id: int) -> tuple[MarketOffer, ...]:
        try:
            odds_rows = normalize_live_odds(api_client.get_live_odds(fixture_id))
        except APIFootballRequestError:
            odds_rows = []
        offers: list[MarketOffer] = []
        for row in odds_rows:
            bookmaker_name = str(row.get("bookmaker") or "API-FOOTBALL")
            bookmaker_id = self._bookmaker_ids.get(bookmaker_name)
            if bookmaker_id is None:
                bookmaker_id = len(self._bookmaker_ids) + 1
                self._bookmaker_ids[bookmaker_name] = bookmaker_id
            offer = _map_odds_row_to_offer(row, bookmaker_id)
            if offer is not None:
                offers.append(offer)
        return tuple(offers)

    def _fetch_stats_row(self, fixture_row: dict[str, Any], fixture_id: int) -> dict[str, Any]:
        if fixture_id in self._stats_cache:
            return self._stats_cache[fixture_id]
        try:
            stats = normalize_fixture_statistics(
                api_client.get_fixture_statistics(fixture_id),
                expected_home_team_id=fixture_row.get("home_team_id"),
                expected_away_team_id=fixture_row.get("away_team_id"),
            )
        except APIFootballRequestError:
            stats = {}
        self._stats_cache[fixture_id] = stats
        return stats
