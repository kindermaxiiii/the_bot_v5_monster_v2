from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from app.vnext.data.normalized_models import DataQualityFlag


@dataclass(slots=True)
class PriorReliabilityBreakdown:
    sample_size_score: float
    data_quality_score: float
    competition_confidence_score: float
    pack_confidence_score: float
    prior_reliability_score: float
    source: str = "prior_reliability.v1"


@dataclass(slots=True)
class HistoricalSubScores:
    source: str
    home_attack_edge: float
    away_attack_edge: float
    home_defense_edge: float
    away_defense_edge: float
    form_edge: float
    venue_edge: float
    strength_edge: float
    balance_score: float
    btts_affinity: float
    under_2_5_affinity: float
    over_2_5_affinity: float
    clean_sheet_home_affinity: float
    clean_sheet_away_affinity: float
    competition_goal_bias: float
    matchup_nudge: float


@dataclass(slots=True)
class ScenarioDefinition:
    key: str
    label: str
    structural_weights: dict[str, float]
    style_weights: dict[str, float]
    matchup_weight: float = 0.0
    minimum_structural_supports: int = 2
    description: str = ""


@dataclass(slots=True)
class ScenarioScoreBreakdown:
    structural_score: float
    style_score: float
    matchup_score: float
    convergence_bonus: float
    structural_support_count: int
    support_shortfall: int
    style_capped: bool
    top_supporting_subscores: tuple[str, ...] = ()


@dataclass(slots=True)
class ScenarioCandidate:
    key: str
    label: str
    score: float
    breakdown: ScenarioScoreBreakdown
    supporting_subscores: tuple[str, ...] = ()
    explanation: str = ""


@dataclass(slots=True)
class ScenarioPriorResult:
    fixture_id: int
    competition_id: int
    competition_name: str
    season: int
    as_of_date: date
    kickoff_utc: datetime
    home_team_id: int
    away_team_id: int
    home_team_name: str
    away_team_name: str
    source_version: str
    catalog_version: str
    prior_source_version: str
    prior_reliability: PriorReliabilityBreakdown
    subscores: HistoricalSubScores
    scenarios: tuple[ScenarioCandidate, ...]
    top_scenario: ScenarioCandidate
    data_quality_flag: DataQualityFlag
    notes: tuple[str, ...] = field(default_factory=tuple)
