from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from app.vnext.posterior.models import ScenarioPosteriorResult

MarketFamily = Literal["OU_FT", "BTTS", "TEAM_TOTAL", "RESULT"]
FamilyMaturity = Literal["APPROVED", "PROBATION", "LAB_ONLY"]
BlockerTier = Literal["HARD", "STRUCTURAL", "CONFIDENCE"]
CandidateDirection = Literal[
    "OVER",
    "UNDER",
    "YES",
    "NO",
    "HOME_OVER",
    "AWAY_OVER",
    "HOME_UNDER",
    "AWAY_UNDER",
    "HOME",
    "AWAY",
]


@dataclass(slots=True, frozen=True)
class LineTemplate:
    key: str
    family: MarketFamily
    direction: CandidateDirection
    suggested_line_family: str
    label: str
    source: str = "line_templates.v1"


@dataclass(slots=True, frozen=True)
class MarketBlocker:
    tier: BlockerTier
    code: str
    detail: str = ""
    source: str = "market_blockers.v1"


@dataclass(slots=True, frozen=True)
class MarketSupportBreakdown:
    scenario_support_score: float
    attack_support_score: float
    defensive_support_score: float
    directionality_score: float
    live_support_score: float
    reliability_score: float
    conflict_score: float
    supporting_scenarios: tuple[str, ...] = ()
    supporting_signals: tuple[str, ...] = ()
    source: str = "market_support.v1"


@dataclass(slots=True, frozen=True)
class MarketCandidate:
    fixture_id: int
    family: MarketFamily
    maturity: FamilyMaturity
    line_template: LineTemplate
    exists: bool
    is_blocked: bool
    is_selectable: bool
    support_score: float
    confidence_score: float
    support_breakdown: MarketSupportBreakdown
    blockers: tuple[MarketBlocker, ...] = ()
    explanation: str = ""
    notes: tuple[str, ...] = ()
    source: str = "market_candidate.v1"


@dataclass(slots=True, frozen=True)
class MarketTranslationResult:
    posterior_result: ScenarioPosteriorResult
    candidates: tuple[MarketCandidate, ...]
    source_version: str = "market_translation.v1"
    notes: tuple[str, ...] = field(default_factory=tuple)
