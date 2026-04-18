from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from app.vnext.data.normalized_models import DataQualityFlag
from app.vnext.live.models import LiveContextPack
from app.vnext.scenario.models import ScenarioPriorResult

ModifierStatus = Literal["ATTENDU", "CONFIRME", "CONTRARIE", "RUPTURE"]
ModifierIntensity = Literal["FAIBLE", "MOYENNE", "FORTE"]


@dataclass(slots=True)
class PosteriorReliabilityBreakdown:
    prior_reliability_score: float
    live_snapshot_quality_score: float
    event_clarity_score: float
    state_coherence_score: float
    posterior_reliability_score: float
    source: str = "posterior_reliability.v1"


@dataclass(slots=True)
class ScenarioModifier:
    status: ModifierStatus
    intensity: ModifierIntensity
    phase: str
    live_alignment_score: float
    break_event_impact: float
    active_event_flags: tuple[str, ...] = ()
    rationale: tuple[str, ...] = ()


@dataclass(slots=True)
class ScenarioPosteriorCandidate:
    key: str
    label: str
    prior_score: float
    posterior_score: float
    delta_score: float
    modifier: ScenarioModifier
    explanation: str = ""


@dataclass(slots=True)
class ScenarioPosteriorResult:
    prior_result: ScenarioPriorResult
    live_context: LiveContextPack
    posterior_reliability: PosteriorReliabilityBreakdown
    scenarios: tuple[ScenarioPosteriorCandidate, ...]
    top_prior_scenario_key: str
    top_posterior_scenario: ScenarioPosteriorCandidate
    top_changed: bool
    data_quality_flag: DataQualityFlag
    source_version: str = "scenario_posterior.v1"
    notes: tuple[str, ...] = field(default_factory=tuple)
