from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from app.vnext.data.normalized_models import DataQualityFlag, Venue


@dataclass(slots=True)
class TeamRecentProfile:
    team_id: int
    team_name: str
    as_of_date: date
    sample_size: int
    primary_window: int
    control_window: int
    control_sample_size: int
    confidence_weight: float
    data_quality_flag: DataQualityFlag
    source: str
    goals_for_per_match: float
    goals_against_per_match: float
    xg_for_per_match: float
    xg_against_per_match: float
    shots_for_per_match: float
    shots_on_for_per_match: float
    shots_on_against_per_match: float
    clean_sheet_rate: float
    failed_to_score_rate: float
    points_per_match: float
    form_score: float


@dataclass(slots=True)
class TeamVenueProfile:
    team_id: int
    team_name: str
    venue: Venue
    as_of_date: date
    sample_size: int
    season_sample_size: int
    shrinkage_weight: float
    confidence_weight: float
    data_quality_flag: DataQualityFlag
    source: str
    goals_for_per_match: float
    goals_against_per_match: float
    xg_for_per_match: float
    xg_against_per_match: float
    shots_for_per_match: float
    shots_on_for_per_match: float
    shots_on_against_per_match: float
    clean_sheet_rate: float
    failed_to_score_rate: float


@dataclass(slots=True)
class TeamStrengthProfile:
    team_id: int
    team_name: str
    as_of_date: date
    sample_size: int
    confidence_weight: float
    data_quality_flag: DataQualityFlag
    source: str
    global_rating: float
    offensive_rating: float
    defensive_rating: float
    stability_score: float


@dataclass(slots=True)
class TeamStyleProfile:
    team_id: int
    team_name: str
    as_of_date: date
    sample_size: int
    confidence_weight: float
    data_quality_flag: DataQualityFlag
    source: str
    btts_rate: float
    under_2_5_rate: float
    over_2_5_rate: float
    team_total_over_0_5_rate: float
    team_total_over_1_5_rate: float
    team_total_over_2_5_rate: float
    clean_sheet_rate: float
    failed_to_score_rate: float


@dataclass(slots=True)
class MatchupProfile:
    home_team_id: int
    away_team_id: int
    home_team_name: str
    away_team_name: str
    as_of_date: date
    sample_size: int
    seasons_covered: int
    confidence_weight: float
    data_quality_flag: DataQualityFlag
    source: str
    home_team_goals_per_match: float
    away_team_goals_per_match: float
    btts_rate: float
    over_2_5_rate: float
    draw_rate: float


@dataclass(slots=True)
class CompetitionProfile:
    competition_id: int
    competition_name: str
    season: int
    as_of_date: date
    sample_size: int
    confidence_weight: float
    data_quality_flag: DataQualityFlag
    source: str
    avg_goals_per_match: float
    btts_rate: float
    over_2_5_rate: float
    data_quality_score: float
    market_depth_score: float
    competition_confidence_score: float
    variance_score: float
