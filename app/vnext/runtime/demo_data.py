from __future__ import annotations

from datetime import date, datetime

from app.vnext.data.normalized_models import HistoricalDataset
from app.vnext.data.normalizers import normalize_competition, normalize_fixture_bundle, normalize_team
from app.vnext.data.raw_models import (
    RawCompetitionRecord,
    RawFixtureRecord,
    RawFixtureTeamStatsRecord,
    RawGoalEventRecord,
    RawTeamRecord,
)
from app.vnext.execution.models import MarketOffer
from app.vnext.live.models import LiveSnapshot
from app.vnext.markets.lines import LINE_TEMPLATES
from app.vnext.prior.builder import build_historical_prior_pack
from app.vnext.runtime.source import SnapshotSource
from app.vnext.scenario.builder import build_scenario_prior_result


TEAM_NAMES = {
    1: "Lions",
    2: "Falcons",
    3: "Bears",
    4: "Wolves",
}


def _demo_goal_events(
    fixture_id: int,
    team_id: int,
    as_of_date: date,
    goal_count: int,
    prefix: str,
) -> list[RawGoalEventRecord]:
    return [
        RawGoalEventRecord(
            fixture_id=fixture_id,
            team_id=team_id,
            minute=12 + (index * 17),
            as_of_date=as_of_date,
            event_id=f"{fixture_id}-{prefix}-{index}",
            detail="Goal",
            source="demo_goal_events.v1",
        )
        for index in range(goal_count)
    ]


def _demo_bundle(
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
    status: str = "FT",
    is_finished: bool = True,
):
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
        market_depth_score=0.84,
        source="demo_fixture.v1",
    )
    raw_home_stats = RawFixtureTeamStatsRecord(
        fixture_id=fixture_id,
        team_id=home_team_id,
        team_name=TEAM_NAMES[home_team_id],
        venue="HOME",
        xg=home_xg,
        shots=home_shots,
        shots_on=home_shots_on,
        corners=5,
        dangerous_attacks=40,
        possession=53.0,
        saves=2,
        red_cards=0,
        source="demo_stats.v1",
    )
    raw_away_stats = RawFixtureTeamStatsRecord(
        fixture_id=fixture_id,
        team_id=away_team_id,
        team_name=TEAM_NAMES[away_team_id],
        venue="AWAY",
        xg=away_xg,
        shots=away_shots,
        shots_on=away_shots_on,
        corners=4,
        dangerous_attacks=32,
        possession=47.0,
        saves=3,
        red_cards=0,
        source="demo_stats.v1",
    )
    goals = _demo_goal_events(fixture_id, home_team_id, as_of_date, home_score, "home")
    goals.extend(_demo_goal_events(fixture_id, away_team_id, as_of_date, away_score, "away"))
    return normalize_fixture_bundle(
        raw_fixture,
        raw_home_stats=raw_home_stats,
        raw_away_stats=raw_away_stats,
        raw_goal_events=goals,
        raw_card_events=[],
    )


def build_demo_dataset() -> HistoricalDataset:
    competitions = (
        normalize_competition(
            RawCompetitionRecord(
                competition_id=100,
                season=2025,
                name="Premier Test",
                country_name="England",
                as_of_date=date(2025, 12, 15),
                market_depth_score=0.84,
                source="demo_competition.v1",
            )
        ),
    )
    teams = tuple(
        normalize_team(
            RawTeamRecord(
                team_id=team_id,
                name=team_name,
                as_of_date=date(2025, 12, 15),
                source="demo_team.v1",
            )
        )
        for team_id, team_name in TEAM_NAMES.items()
    )
    specs = [
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
    bundles = [
        _demo_bundle(
            fixture_id=fixture_id,
            season=season,
            kickoff_utc=kickoff_utc,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            home_score=home_score,
            away_score=away_score,
            home_xg=home_xg,
            away_xg=away_xg,
            home_shots=home_shots,
            away_shots=away_shots,
            home_shots_on=home_shots_on,
            away_shots_on=away_shots_on,
        )
        for (
            fixture_id,
            season,
            kickoff_utc,
            home_team_id,
            away_team_id,
            home_score,
            away_score,
            home_xg,
            away_xg,
            home_shots,
            away_shots,
            home_shots_on,
            away_shots_on,
        ) in specs
    ]
    bundles.append(
        _demo_bundle(
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


def build_demo_snapshot() -> LiveSnapshot:
    return LiveSnapshot(
        fixture_id=999,
        competition_id=100,
        season=2025,
        kickoff_utc=datetime(2025, 12, 15, 19, 0),
        minute=28,
        status="1H",
        home_team_id=1,
        away_team_id=2,
        home_team_name="Lions",
        away_team_name="Falcons",
        home_goals=0,
        away_goals=0,
        home_red_cards=0,
        away_red_cards=0,
        home_shots_total=8,
        away_shots_total=5,
        home_shots_on=3,
        away_shots_on=1,
        home_corners=4,
        away_corners=2,
        home_dangerous_attacks=28,
        away_dangerous_attacks=16,
        home_attacks=45,
        away_attacks=33,
        home_possession=54.0,
        away_possession=46.0,
        home_xg=0.75,
        away_xg=0.22,
        live_snapshot_quality_score=1.0,
        data_quality_flag="HIGH",
        payload={
            "fixture_row": {
                "fixture_id": 999,
                "league_id": 100,
                "league_name": "Premier Test",
                "country_name": "England",
                "season": 2025,
                "status": "1H",
                "minute": 28,
                "start_time_utc": datetime(2025, 12, 15, 19, 0),
                "home_team_id": 1,
                "home_team_name": "Lions",
                "away_team_id": 2,
                "away_team_name": "Falcons",
                "home_goals": 0,
                "away_goals": 0,
                "home_red": 0,
                "away_red": 0,
            },
            "stats_row": {
                "home_shots_total": 8,
                "away_shots_total": 5,
                "home_shots_on": 3,
                "away_shots_on": 1,
                "home_corners": 4,
                "away_corners": 2,
                "home_dangerous_attacks": 28,
                "away_dangerous_attacks": 16,
                "home_attacks": 45,
                "away_attacks": 33,
                "home_possession": 54.0,
                "away_possession": 46.0,
                "home_xg": 0.75,
                "away_xg": 0.22,
                "home_red_cards": 0,
                "away_red_cards": 0,
            },
        },
    )


def build_demo_offers() -> tuple[MarketOffer, ...]:
    offers: list[MarketOffer] = []
    for template in LINE_TEMPLATES.values():
        if template.family == "RESULT":
            continue
        if template.suggested_line_family == "over_1_5_or_2_5":
            line = 2.5
        elif template.suggested_line_family == "under_2_5_or_3_5":
            line = 2.5
        elif template.suggested_line_family == "home_over_0_5_or_1_5":
            line = 0.5
        elif template.suggested_line_family == "away_over_0_5_or_1_5":
            line = 0.5
        elif template.suggested_line_family == "home_under_1_5_or_2_5":
            line = 1.5
        elif template.suggested_line_family == "away_under_1_5_or_2_5":
            line = 1.5
        else:
            line = None
        team_scope = "HOME" if template.direction.startswith("HOME") else "AWAY" if template.direction.startswith("AWAY") else "NONE"
        for bookmaker_id in (1, 2, 3):
            offers.append(
                MarketOffer(
                    bookmaker_id=bookmaker_id,
                    bookmaker_name=f"Book {bookmaker_id}",
                    market_family=template.family,
                    side=template.direction,
                    line=line,
                    team_scope=team_scope,
                    odds_decimal=1.85 + (0.02 * bookmaker_id),
                    normalized_market_label=template.family,
                    offer_timestamp_utc=datetime.now(),
                    freshness_seconds=30,
                    raw_source_ref=f"demo_offer:{bookmaker_id}:{template.key}",
                )
            )
    return tuple(offers)


def build_demo_snapshot_source() -> SnapshotSource:
    snapshot = build_demo_snapshot()
    return SnapshotSource(
        snapshots=(snapshot,),
        offers_by_fixture={snapshot.fixture_id: build_demo_offers()},
    )


def build_demo_prior_provider():
    dataset = build_demo_dataset()

    def _provider(snapshot: LiveSnapshot):
        prior_pack = build_historical_prior_pack(dataset, fixture_id=snapshot.fixture_id)
        return build_scenario_prior_result(prior_pack)

    return _provider
