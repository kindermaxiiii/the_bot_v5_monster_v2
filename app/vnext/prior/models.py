from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from app.vnext.data.normalized_models import DataQualityFlag


@dataclass(slots=True)
class PriorBlockMeta:
    source: str
    sample_size: int
    confidence_weight: float
    data_quality_flag: DataQualityFlag


@dataclass(slots=True)
class TeamAttackSnapshot:
    team_id: int
    team_name: str
    goals_for_per_match: float
    xg_for_per_match: float
    shots_for_per_match: float
    shots_on_for_per_match: float


@dataclass(slots=True)
class TeamDefenseSnapshot:
    team_id: int
    team_name: str
    goals_against_per_match: float
    xg_against_per_match: float
    shots_on_against_per_match: float
    clean_sheet_rate: float


@dataclass(slots=True)
class TeamVenueSnapshot:
    team_id: int
    team_name: str
    venue: str
    goals_for_per_match: float
    goals_against_per_match: float
    xg_for_per_match: float
    xg_against_per_match: float
    shrinkage_weight: float


@dataclass(slots=True)
class TeamFormSnapshot:
    team_id: int
    team_name: str
    points_per_match: float
    form_score: float
    clean_sheet_rate: float
    failed_to_score_rate: float


@dataclass(slots=True)
class TeamStrengthSnapshot:
    team_id: int
    team_name: str
    global_rating: float
    offensive_rating: float
    defensive_rating: float
    stability_score: float


@dataclass(slots=True)
class TeamStyleSnapshot:
    team_id: int
    team_name: str
    btts_rate: float
    under_2_5_rate: float
    over_2_5_rate: float
    team_total_over_0_5_rate: float
    team_total_over_1_5_rate: float
    team_total_over_2_5_rate: float
    clean_sheet_rate: float
    failed_to_score_rate: float


@dataclass(slots=True)
class MatchupSnapshot:
    home_team_goals_per_match: float
    away_team_goals_per_match: float
    btts_rate: float
    over_2_5_rate: float
    draw_rate: float
    seasons_covered: int


@dataclass(slots=True)
class CompetitionSnapshot:
    competition_id: int
    competition_name: str
    season: int
    avg_goals_per_match: float
    btts_rate: float
    over_2_5_rate: float
    data_quality_score: float
    market_depth_score: float
    competition_confidence_score: float
    variance_score: float


@dataclass(slots=True)
class AttackContext(PriorBlockMeta):
    home: TeamAttackSnapshot
    away: TeamAttackSnapshot


@dataclass(slots=True)
class DefenseContext(PriorBlockMeta):
    home: TeamDefenseSnapshot
    away: TeamDefenseSnapshot


@dataclass(slots=True)
class VenueContext(PriorBlockMeta):
    home: TeamVenueSnapshot
    away: TeamVenueSnapshot


@dataclass(slots=True)
class FormContext(PriorBlockMeta):
    home: TeamFormSnapshot
    away: TeamFormSnapshot


@dataclass(slots=True)
class StrengthContext(PriorBlockMeta):
    home: TeamStrengthSnapshot
    away: TeamStrengthSnapshot


@dataclass(slots=True)
class StyleContext(PriorBlockMeta):
    home: TeamStyleSnapshot
    away: TeamStyleSnapshot


@dataclass(slots=True)
class MatchupContext(PriorBlockMeta):
    matchup: MatchupSnapshot


@dataclass(slots=True)
class CompetitionContext(PriorBlockMeta):
    competition: CompetitionSnapshot


@dataclass(slots=True)
class HistoricalPriorPack:
    fixture_id: int
    competition_id: int
    season: int
    as_of_date: date
    kickoff_utc: datetime
    home_team_id: int
    away_team_id: int
    home_team_name: str
    away_team_name: str
    source_version: str
    attack_context: AttackContext
    defense_context: DefenseContext
    venue_context: VenueContext
    form_context: FormContext
    strength_context: StrengthContext
    style_context: StyleContext
    matchup_context: MatchupContext
    competition_context: CompetitionContext
