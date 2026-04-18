from __future__ import annotations

from datetime import date, datetime

from app.vnext.data.normalized_models import HistoricalDataset
from app.vnext.data.normalizers import (
    normalize_competition,
    normalize_fixture_bundle,
    normalize_team,
)
from app.vnext.data.raw_models import (
    RawCardEventRecord,
    RawCompetitionRecord,
    RawFixtureRecord,
    RawFixtureTeamStatsRecord,
    RawGoalEventRecord,
    RawTeamRecord,
)


TEAM_NAMES = {
    1: "Lions",
    2: "Falcons",
    3: "Bears",
    4: "Wolves",
}


def make_fixture_bundle(
    *,
    fixture_id: int,
    season: int,
    kickoff_utc: datetime,
    home_team_id: int,
    away_team_id: int,
    home_score: int,
    away_score: int,
    home_xg: float | None,
    away_xg: float | None,
    home_shots: int | None,
    away_shots: int | None,
    home_shots_on: int | None,
    away_shots_on: int | None,
    home_corners: int | None = 5,
    away_corners: int | None = 4,
    home_dangerous_attacks: int | None = 42,
    away_dangerous_attacks: int | None = 35,
    home_possession: float | None = 52.0,
    away_possession: float | None = 48.0,
    home_saves: int | None = 2,
    away_saves: int | None = 3,
    home_red_cards: int | None = 0,
    away_red_cards: int | None = 0,
    status: str = "FT",
    is_finished: bool = True,
    market_depth_score: float = 0.8,
    goal_minutes_home: list[int] | None = None,
    goal_minutes_away: list[int] | None = None,
    duplicate_home_goal: bool = False,
    omit_last_home_goal: bool = False,
    add_red_card: bool = False,
) -> object:
    as_of_date = kickoff_utc.date()
    raw_fixture = RawFixtureRecord(
        fixture_id=fixture_id,
        competition_id=100,
        season=season,
        kickoff_utc=kickoff_utc,
        as_of_date=as_of_date,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        home_team_name=TEAM_NAMES[home_team_id],
        away_team_name=TEAM_NAMES[away_team_id],
        competition_name="Premier Test",
        home_score=home_score,
        away_score=away_score,
        status=status,
        is_finished=is_finished,
        market_depth_score=market_depth_score,
    )
    raw_home_stats = RawFixtureTeamStatsRecord(
        fixture_id=fixture_id,
        team_id=home_team_id,
        team_name=TEAM_NAMES[home_team_id],
        venue="HOME",
        xg=home_xg,
        shots=home_shots,
        shots_on=home_shots_on,
        corners=home_corners,
        dangerous_attacks=home_dangerous_attacks,
        possession=home_possession,
        saves=home_saves,
        red_cards=home_red_cards,
    )
    raw_away_stats = RawFixtureTeamStatsRecord(
        fixture_id=fixture_id,
        team_id=away_team_id,
        team_name=TEAM_NAMES[away_team_id],
        venue="AWAY",
        xg=away_xg,
        shots=away_shots,
        shots_on=away_shots_on,
        corners=away_corners,
        dangerous_attacks=away_dangerous_attacks,
        possession=away_possession,
        saves=away_saves,
        red_cards=away_red_cards,
    )

    goal_minutes_home = goal_minutes_home or [11 + (index * 19) for index in range(home_score)]
    goal_minutes_away = goal_minutes_away or [15 + (index * 23) for index in range(away_score)]
    raw_goal_events: list[RawGoalEventRecord] = []
    for index, minute in enumerate(goal_minutes_home[: home_score]):
        if omit_last_home_goal and index == home_score - 1:
            continue
        raw_goal_events.append(
            RawGoalEventRecord(
                fixture_id=fixture_id,
                team_id=home_team_id,
                minute=minute,
                as_of_date=as_of_date,
                event_id=f"{fixture_id}-h-{index}",
                detail="Goal",
            )
        )
    for index, minute in enumerate(goal_minutes_away[: away_score]):
        raw_goal_events.append(
            RawGoalEventRecord(
                fixture_id=fixture_id,
                team_id=away_team_id,
                minute=minute,
                as_of_date=as_of_date,
                event_id=f"{fixture_id}-a-{index}",
                detail="Goal",
            )
        )
    if duplicate_home_goal and raw_goal_events:
        raw_goal_events.append(
            RawGoalEventRecord(
                fixture_id=fixture_id,
                team_id=home_team_id,
                minute=goal_minutes_home[0],
                as_of_date=as_of_date,
                event_id=f"{fixture_id}-h-0",
                detail="Goal",
            )
        )

    raw_card_events: list[RawCardEventRecord] = []
    if add_red_card:
        raw_card_events.append(
            RawCardEventRecord(
                fixture_id=fixture_id,
                team_id=away_team_id,
                minute=78,
                as_of_date=as_of_date,
                card_type="RED",
                event_id=f"{fixture_id}-red-away",
            )
        )

    return normalize_fixture_bundle(
        raw_fixture,
        raw_home_stats=raw_home_stats,
        raw_away_stats=raw_away_stats,
        raw_goal_events=raw_goal_events,
        raw_card_events=raw_card_events,
    )


def build_reference_dataset(*, sparse_team_ids: set[int] | None = None) -> HistoricalDataset:
    sparse_team_ids = sparse_team_ids or set()
    competitions = (
        normalize_competition(
            RawCompetitionRecord(
                competition_id=100,
                season=2024,
                name="Premier Test",
                country_name="England",
                as_of_date=date(2024, 11, 20),
                market_depth_score=0.82,
            )
        ),
        normalize_competition(
            RawCompetitionRecord(
                competition_id=100,
                season=2025,
                name="Premier Test",
                country_name="England",
                as_of_date=date(2025, 12, 15),
                market_depth_score=0.84,
            )
        ),
    )
    teams = tuple(
        normalize_team(
            RawTeamRecord(
                team_id=team_id,
                name=team_name,
                as_of_date=date(2025, 12, 15),
            )
        )
        for team_id, team_name in TEAM_NAMES.items()
    )

    specs = [
        (401, 2024, datetime(2024, 11, 20, 19, 0), 1, 2, 0, 2, 0.7, 1.8, 8, 12, 2, 6),
        (501, 2025, datetime(2025, 8, 1, 19, 0), 1, 3, 2, 0, 1.9, 0.5, 14, 6, 6, 2),
        (502, 2025, datetime(2025, 8, 8, 19, 0), 4, 2, 1, 1, 1.1, 1.0, 9, 10, 3, 4),
        (503, 2025, datetime(2025, 8, 15, 19, 0), 1, 2, 2, 1, 1.8, 1.2, 13, 11, 6, 4),
        (504, 2025, datetime(2025, 9, 1, 19, 0), 2, 3, 3, 0, 2.2, 0.4, 15, 5, 7, 1),
        (505, 2025, datetime(2025, 9, 10, 19, 0), 4, 1, 0, 1, 0.6, 1.4, 7, 10, 2, 5),
        (506, 2025, datetime(2025, 9, 20, 19, 0), 2, 1, 0, 0, 0.8, 0.7, 10, 8, 3, 3),
        (507, 2025, datetime(2025, 10, 5, 19, 0), 1, 4, 3, 1, 2.4, 1.0, 16, 9, 8, 3),
        (508, 2025, datetime(2025, 10, 15, 19, 0), 3, 2, 1, 2, 0.9, 1.5, 8, 11, 3, 5),
        (509, 2025, datetime(2025, 11, 1, 19, 0), 1, 2, 1, 1, 1.2, 1.1, 11, 10, 4, 4),
        (510, 2025, datetime(2025, 12, 1, 19, 0), 2, 1, 2, 3, 1.7, 2.1, 12, 14, 5, 7),
    ]

    bundles = []
    for spec in specs:
        fixture_id, season, kickoff, home_id, away_id, home_score, away_score, home_xg, away_xg, home_shots, away_shots, home_on, away_on = spec
        if home_id in sparse_team_ids:
            home_xg = None
            home_shots = None
            home_on = None
        if away_id in sparse_team_ids:
            away_xg = None
            away_shots = None
            away_on = None
        bundles.append(
            make_fixture_bundle(
                fixture_id=fixture_id,
                season=season,
                kickoff_utc=kickoff,
                home_team_id=home_id,
                away_team_id=away_id,
                home_score=home_score,
                away_score=away_score,
                home_xg=home_xg,
                away_xg=away_xg,
                home_shots=home_shots,
                away_shots=away_shots,
                home_shots_on=home_on,
                away_shots_on=away_on,
            )
        )

    bundles.append(
        make_fixture_bundle(
            fixture_id=999,
            season=2025,
            kickoff_utc=datetime(2025, 12, 15, 19, 0),
            home_team_id=1,
            away_team_id=2,
            home_score=0,
            away_score=0,
            home_xg=None,
            away_xg=None,
            home_shots=None,
            away_shots=None,
            home_shots_on=None,
            away_shots_on=None,
            status="NS",
            is_finished=False,
        )
    )

    return HistoricalDataset.from_bundles(
        competitions=competitions,
        teams=teams,
        bundles=tuple(bundles),
    )
