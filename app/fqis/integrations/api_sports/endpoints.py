from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ApiSportsEndpoint(str, Enum):
    STATUS = "status"
    COUNTRIES = "countries"
    LEAGUES = "leagues"
    LEAGUE_SEASONS = "leagues/seasons"

    FIXTURES = "fixtures"
    FIXTURES_EVENTS = "fixtures/events"
    FIXTURES_HEAD_TO_HEAD = "fixtures/headtohead"
    FIXTURES_LINEUPS = "fixtures/lineups"
    FIXTURES_PLAYERS = "fixtures/players"
    FIXTURES_STATISTICS = "fixtures/statistics"

    STANDINGS = "standings"
    TEAMS = "teams"
    TEAMS_STATISTICS = "teams/statistics"

    INJURIES = "injuries"
    PREDICTIONS = "predictions"

    ODDS = "odds"
    ODDS_BOOKMAKERS = "odds/bookmakers"
    ODDS_BETS = "odds/bets"

    ODDS_LIVE = "odds/live"
    ODDS_LIVE_BETS = "odds/live/bets"


@dataclass(frozen=True)
class MarketPriority:
    name: str
    family: str
    api_hint: str


MARKET_PRIORITIES: tuple[MarketPriority, ...] = (
    MarketPriority("Over / Under match", "totals_full_time", "odds/bets search=Over"),
    MarketPriority("Over / Under mi-temps", "totals_half_time", "odds/bets search=Over"),
    MarketPriority("BTTS", "both_teams_to_score", "odds/bets search=Both Teams Score"),
    MarketPriority("Team totals", "team_totals", "odds/bets search=Team"),
    MarketPriority("1X2", "match_winner", "odds/bets search=Match Winner"),
)


def normalize_endpoint(endpoint: ApiSportsEndpoint | str) -> str:
    value = endpoint.value if isinstance(endpoint, ApiSportsEndpoint) else str(endpoint)
    normalized = value.strip().lstrip("/").rstrip("/")
    if not normalized:
        raise ValueError("API-Sports endpoint cannot be empty.")
    return normalized
