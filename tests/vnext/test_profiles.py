from __future__ import annotations

from app.vnext.profiles.builders import (
    build_competition_profile,
    build_matchup_profile,
    build_team_recent_profile,
    build_team_strength_profile,
    build_team_venue_profile,
)
from tests.vnext.factories import build_reference_dataset


def test_team_recent_profile_respects_primary_and_control_windows() -> None:
    dataset = build_reference_dataset()
    as_of = dataset.fixture_by_id(999).kickoff_utc
    profile = build_team_recent_profile(dataset, team_id=1, as_of=as_of, competition_id=100)

    assert profile.sample_size == 8
    assert profile.control_sample_size == 5
    assert profile.primary_window == 8
    assert profile.control_window == 5
    assert profile.goals_for_per_match > profile.goals_against_per_match
    assert 0.0 <= profile.form_score <= 1.0


def test_team_venue_profile_distinguishes_home_and_away() -> None:
    dataset = build_reference_dataset()
    as_of = dataset.fixture_by_id(999).kickoff_utc
    home_profile = build_team_venue_profile(dataset, team_id=1, venue="HOME", as_of=as_of, competition_id=100)
    away_profile = build_team_venue_profile(dataset, team_id=1, venue="AWAY", as_of=as_of, competition_id=100)

    assert home_profile.sample_size > 0
    assert away_profile.sample_size > 0
    assert home_profile.goals_for_per_match > away_profile.goals_for_per_match
    assert home_profile.venue == "HOME"
    assert away_profile.venue == "AWAY"


def test_team_strength_profile_and_competition_profile_are_coherent() -> None:
    dataset = build_reference_dataset()
    as_of = dataset.fixture_by_id(999).kickoff_utc
    lions = build_team_strength_profile(dataset, team_id=1, as_of=as_of, competition_id=100)
    wolves = build_team_strength_profile(dataset, team_id=4, as_of=as_of, competition_id=100)
    competition = build_competition_profile(dataset, competition_id=100, season=2025, as_of=as_of)

    assert lions.global_rating > wolves.global_rating
    assert lions.offensive_rating > wolves.offensive_rating
    assert competition.sample_size == 10
    assert competition.avg_goals_per_match == 2.5
    assert competition.btts_rate == 0.6
    assert competition.over_2_5_rate == 0.5


def test_matchup_profile_respects_recent_h2h_limits() -> None:
    dataset = build_reference_dataset()
    as_of = dataset.fixture_by_id(999).kickoff_utc
    profile = build_matchup_profile(dataset, home_team_id=1, away_team_id=2, as_of=as_of)

    assert profile.sample_size == 5
    assert profile.seasons_covered <= 3
    assert profile.confidence_weight <= 0.35
