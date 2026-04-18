from __future__ import annotations

import pytest

from app.vnext.data.normalizers import normalize_fixture_team_stats_pair
from app.vnext.data.raw_models import RawFixtureTeamStatsRecord
from tests.vnext.factories import make_fixture_bundle


def test_home_away_and_goal_events_are_normalized_coherently() -> None:
    bundle = make_fixture_bundle(
        fixture_id=1,
        season=2025,
        kickoff_utc=__import__("datetime").datetime(2025, 8, 1, 19, 0),
        home_team_id=1,
        away_team_id=2,
        home_score=2,
        away_score=1,
        home_xg=1.6,
        away_xg=0.9,
        home_shots=13,
        away_shots=8,
        home_shots_on=5,
        away_shots_on=3,
        add_red_card=True,
    )

    assert bundle.fixture.goal_events_coherent is True
    assert bundle.fixture.data_quality_flag == "HIGH"
    assert bundle.team_stats[0].venue == "HOME"
    assert bundle.team_stats[1].venue == "AWAY"
    assert bundle.team_stats[0].team_id == bundle.fixture.home_team_id
    assert bundle.team_stats[1].team_id == bundle.fixture.away_team_id
    assert len(bundle.goal_events) == 3
    assert len(bundle.card_events) == 1


def test_duplicate_goal_events_are_deduped_and_mismatch_is_flagged() -> None:
    bundle = make_fixture_bundle(
        fixture_id=2,
        season=2025,
        kickoff_utc=__import__("datetime").datetime(2025, 8, 2, 19, 0),
        home_team_id=1,
        away_team_id=2,
        home_score=2,
        away_score=1,
        home_xg=1.4,
        away_xg=1.0,
        home_shots=12,
        away_shots=9,
        home_shots_on=4,
        away_shots_on=4,
        duplicate_home_goal=True,
        omit_last_home_goal=True,
    )

    assert len(bundle.goal_events) == 2
    assert bundle.fixture.goal_events_coherent is False
    assert bundle.fixture.data_quality_flag == "INCONSISTENT"
    assert "goal_event_score_mismatch" in bundle.fixture.notes


def test_fixture_team_stats_reject_wrong_home_away_mapping() -> None:
    bundle = make_fixture_bundle(
        fixture_id=3,
        season=2025,
        kickoff_utc=__import__("datetime").datetime(2025, 8, 3, 19, 0),
        home_team_id=1,
        away_team_id=2,
        home_score=1,
        away_score=0,
        home_xg=1.1,
        away_xg=0.4,
        home_shots=9,
        away_shots=6,
        home_shots_on=3,
        away_shots_on=2,
    )
    fixture = bundle.fixture
    wrong_home = RawFixtureTeamStatsRecord(
        fixture_id=fixture.fixture_id,
        team_id=fixture.home_team_id,
        team_name=fixture.home_team_name,
        venue="AWAY",
    )
    away = RawFixtureTeamStatsRecord(
        fixture_id=fixture.fixture_id,
        team_id=fixture.away_team_id,
        team_name=fixture.away_team_name,
        venue="AWAY",
    )

    with pytest.raises(ValueError):
        normalize_fixture_team_stats_pair(fixture, wrong_home, away)
