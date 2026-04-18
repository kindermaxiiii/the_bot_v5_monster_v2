from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

DataQualityFlag = Literal["HIGH", "MEDIUM", "LOW", "INCONSISTENT"]
Venue = Literal["HOME", "AWAY"]
GoalEventKind = Literal["GOAL", "PENALTY", "OWN_GOAL", "OTHER"]
CardEventType = Literal["YELLOW", "RED", "SECOND_YELLOW_RED"]


@dataclass(slots=True, frozen=True)
class CompetitionRecord:
    competition_id: int
    season: int
    name: str
    country_name: str
    as_of_date: date
    market_depth_score: float = 0.5
    data_quality_flag: DataQualityFlag = "HIGH"
    data_completeness_score: float = 1.0
    source: str = "historical"


@dataclass(slots=True, frozen=True)
class TeamRecord:
    team_id: int
    name: str
    as_of_date: date
    short_name: str | None = None
    country_name: str | None = None
    data_quality_flag: DataQualityFlag = "HIGH"
    data_completeness_score: float = 1.0
    source: str = "historical"


@dataclass(slots=True, frozen=True)
class FixtureRecord:
    fixture_id: int
    competition_id: int
    season: int
    kickoff_utc: datetime
    as_of_date: date
    home_team_id: int
    away_team_id: int
    home_team_name: str
    away_team_name: str
    competition_name: str
    home_score: int
    away_score: int
    status: str
    is_finished: bool
    goal_events_coherent: bool = True
    data_quality_flag: DataQualityFlag = "HIGH"
    data_completeness_score: float = 1.0
    market_depth_score: float = 0.5
    notes: tuple[str, ...] = ()
    source: str = "historical"


@dataclass(slots=True, frozen=True)
class FixtureTeamStatsRecord:
    fixture_id: int
    competition_id: int
    season: int
    kickoff_utc: datetime
    as_of_date: date
    team_id: int
    opponent_team_id: int
    team_name: str
    opponent_team_name: str
    venue: Venue
    goals_for: int
    goals_against: int
    xg_for: float | None
    xg_against: float | None
    shots_for: int | None
    shots_against: int | None
    shots_on_for: int | None
    shots_on_against: int | None
    corners_for: int | None
    corners_against: int | None
    dangerous_attacks_for: int | None
    dangerous_attacks_against: int | None
    possession: float | None
    saves: int | None
    red_cards: int
    clean_sheet: bool
    failed_to_score: bool
    points: int
    data_quality_flag: DataQualityFlag
    data_completeness_score: float
    source: str = "historical"


@dataclass(slots=True, frozen=True)
class GoalEventRecord:
    event_id: str
    fixture_id: int
    competition_id: int
    season: int
    kickoff_utc: datetime
    as_of_date: date
    team_id: int
    team_name: str
    minute: int
    extra_minute: int
    event_kind: GoalEventKind
    is_home_team: bool
    source: str = "historical"


@dataclass(slots=True, frozen=True)
class CardEventRecord:
    event_id: str
    fixture_id: int
    competition_id: int
    season: int
    kickoff_utc: datetime
    as_of_date: date
    team_id: int
    team_name: str
    minute: int
    extra_minute: int
    card_type: CardEventType
    is_home_team: bool
    source: str = "historical"


@dataclass(slots=True, frozen=True)
class NormalizedFixtureBundle:
    fixture: FixtureRecord
    team_stats: tuple[FixtureTeamStatsRecord, FixtureTeamStatsRecord]
    goal_events: tuple[GoalEventRecord, ...] = ()
    card_events: tuple[CardEventRecord, ...] = ()


@dataclass(slots=True, frozen=True)
class HistoricalDataset:
    competitions: tuple[CompetitionRecord, ...] = ()
    teams: tuple[TeamRecord, ...] = ()
    fixtures: tuple[FixtureRecord, ...] = ()
    fixture_team_stats: tuple[FixtureTeamStatsRecord, ...] = ()
    goal_events: tuple[GoalEventRecord, ...] = ()
    card_events: tuple[CardEventRecord, ...] = ()
    bundles: tuple[NormalizedFixtureBundle, ...] = ()

    @classmethod
    def from_bundles(
        cls,
        *,
        competitions: tuple[CompetitionRecord, ...] = (),
        teams: tuple[TeamRecord, ...] = (),
        bundles: tuple[NormalizedFixtureBundle, ...] = (),
    ) -> "HistoricalDataset":
        fixtures = tuple(bundle.fixture for bundle in bundles)
        team_stats = tuple(stat for bundle in bundles for stat in bundle.team_stats)
        goal_events = tuple(event for bundle in bundles for event in bundle.goal_events)
        card_events = tuple(event for bundle in bundles for event in bundle.card_events)
        return cls(
            competitions=competitions,
            teams=teams,
            fixtures=fixtures,
            fixture_team_stats=team_stats,
            goal_events=goal_events,
            card_events=card_events,
            bundles=bundles,
        )

    def fixture_by_id(self, fixture_id: int) -> FixtureRecord:
        for fixture in self.fixtures:
            if fixture.fixture_id == fixture_id:
                return fixture
        raise KeyError(f"unknown fixture_id={fixture_id}")

    def team_by_id(self, team_id: int) -> TeamRecord:
        for team in self.teams:
            if team.team_id == team_id:
                return team
        raise KeyError(f"unknown team_id={team_id}")

    def competition_by_id(self, competition_id: int, season: int | None = None) -> CompetitionRecord:
        for competition in self.competitions:
            if competition.competition_id != competition_id:
                continue
            if season is not None and competition.season != season:
                continue
            return competition
        raise KeyError(f"unknown competition_id={competition_id} season={season}")


def quality_flag_from_score(score: float, *, inconsistent: bool = False) -> DataQualityFlag:
    if inconsistent:
        return "INCONSISTENT"
    if score >= 0.85:
        return "HIGH"
    if score >= 0.65:
        return "MEDIUM"
    return "LOW"


def worst_quality_flag(*flags: DataQualityFlag) -> DataQualityFlag:
    rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2, "INCONSISTENT": 3}
    return max(flags or ("HIGH",), key=rank.get)
