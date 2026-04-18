from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any


@dataclass(slots=True)
class RawCompetitionRecord:
    competition_id: Any
    season: Any
    name: Any
    country_name: Any
    as_of_date: date
    market_depth_score: Any = None
    payload: dict[str, Any] = field(default_factory=dict)
    source: str = "historical"


@dataclass(slots=True)
class RawTeamRecord:
    team_id: Any
    name: Any
    as_of_date: date
    short_name: Any = None
    country_name: Any = None
    payload: dict[str, Any] = field(default_factory=dict)
    source: str = "historical"


@dataclass(slots=True)
class RawFixtureRecord:
    fixture_id: Any
    competition_id: Any
    season: Any
    kickoff_utc: Any
    as_of_date: date
    home_team_id: Any
    away_team_id: Any
    home_team_name: Any
    away_team_name: Any
    competition_name: Any
    home_score: Any
    away_score: Any
    status: Any = "FT"
    is_finished: Any = True
    market_depth_score: Any = None
    payload: dict[str, Any] = field(default_factory=dict)
    source: str = "historical"


@dataclass(slots=True)
class RawFixtureTeamStatsRecord:
    fixture_id: Any
    team_id: Any
    team_name: Any
    venue: Any
    xg: Any = None
    shots: Any = None
    shots_on: Any = None
    corners: Any = None
    dangerous_attacks: Any = None
    possession: Any = None
    saves: Any = None
    red_cards: Any = None
    payload: dict[str, Any] = field(default_factory=dict)
    source: str = "historical"


@dataclass(slots=True)
class RawGoalEventRecord:
    fixture_id: Any
    team_id: Any
    minute: Any
    as_of_date: date
    event_id: Any = None
    extra_minute: Any = None
    detail: Any = None
    player_name: Any = None
    payload: dict[str, Any] = field(default_factory=dict)
    source: str = "historical"


@dataclass(slots=True)
class RawCardEventRecord:
    fixture_id: Any
    team_id: Any
    minute: Any
    as_of_date: date
    card_type: Any
    event_id: Any = None
    extra_minute: Any = None
    player_name: Any = None
    payload: dict[str, Any] = field(default_factory=dict)
    source: str = "historical"
