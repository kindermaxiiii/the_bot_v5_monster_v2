from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from app.fqis.contracts.core import BookOffer, StatisticalThesis
from app.fqis.engine import GovernedOutcome, run_governed_thesis_pipeline
from app.fqis.pipeline import PipelineOutcome, run_thesis_pipeline
from app.fqis.probability.intent_bridge import (
    ProbabilityIntentBridgeResult,
    build_probability_intent_bridge_result,
)
from app.fqis.probability.live_goal_model import LiveGoalFeatures, LiveGoalModelConfig


ProbabilityPipelineSource = Literal["external", "model"]


@dataclass(slots=True, frozen=True)
class ProbabilityPipelineOutcome:
    p_real_source: ProbabilityPipelineSource
    p_real_by_intent_key: dict[str, float]
    bridge_result: ProbabilityIntentBridgeResult | None
    pipeline_outcome: PipelineOutcome


@dataclass(slots=True, frozen=True)
class ProbabilityGovernedOutcome:
    p_real_source: ProbabilityPipelineSource
    p_real_by_intent_key: dict[str, float]
    bridge_result: ProbabilityIntentBridgeResult | None
    governed_outcome: GovernedOutcome


def run_probability_thesis_pipeline(
    thesis: StatisticalThesis,
    offers: tuple[BookOffer, ...],
    *,
    p_real_source: ProbabilityPipelineSource = "model",
    features: LiveGoalFeatures | None = None,
    p_real_by_intent_key: dict[str, float] | None = None,
    config: LiveGoalModelConfig | None = None,
    max_remaining_goals: int = 10,
    min_edge: float = 0.01,
    min_ev: float = 0.0,
    min_odds: float = 1.50,
    max_odds: float = 2.80,
) -> ProbabilityPipelineOutcome:
    resolved = _resolve_p_real_by_intent_key(
        thesis,
        p_real_source=p_real_source,
        features=features,
        p_real_by_intent_key=p_real_by_intent_key,
        config=config,
        max_remaining_goals=max_remaining_goals,
    )

    pipeline_outcome = run_thesis_pipeline(
        thesis,
        offers,
        p_real_by_intent_key=resolved.p_real_by_intent_key,
        min_edge=min_edge,
        min_ev=min_ev,
        min_odds=min_odds,
        max_odds=max_odds,
    )

    return ProbabilityPipelineOutcome(
        p_real_source=p_real_source,
        p_real_by_intent_key=resolved.p_real_by_intent_key,
        bridge_result=resolved.bridge_result,
        pipeline_outcome=pipeline_outcome,
    )


def run_model_generated_thesis_pipeline(
    thesis: StatisticalThesis,
    offers: tuple[BookOffer, ...],
    *,
    features: LiveGoalFeatures,
    config: LiveGoalModelConfig | None = None,
    max_remaining_goals: int = 10,
    min_edge: float = 0.01,
    min_ev: float = 0.0,
    min_odds: float = 1.50,
    max_odds: float = 2.80,
) -> ProbabilityPipelineOutcome:
    return run_probability_thesis_pipeline(
        thesis,
        offers,
        p_real_source="model",
        features=features,
        config=config,
        max_remaining_goals=max_remaining_goals,
        min_edge=min_edge,
        min_ev=min_ev,
        min_odds=min_odds,
        max_odds=max_odds,
    )


def run_external_probability_thesis_pipeline(
    thesis: StatisticalThesis,
    offers: tuple[BookOffer, ...],
    *,
    p_real_by_intent_key: dict[str, float],
    min_edge: float = 0.01,
    min_ev: float = 0.0,
    min_odds: float = 1.50,
    max_odds: float = 2.80,
) -> ProbabilityPipelineOutcome:
    return run_probability_thesis_pipeline(
        thesis,
        offers,
        p_real_source="external",
        p_real_by_intent_key=p_real_by_intent_key,
        min_edge=min_edge,
        min_ev=min_ev,
        min_odds=min_odds,
        max_odds=max_odds,
    )


def run_probability_governed_thesis_pipeline(
    thesis: StatisticalThesis,
    offers: tuple[BookOffer, ...],
    *,
    p_real_source: ProbabilityPipelineSource = "model",
    features: LiveGoalFeatures | None = None,
    p_real_by_intent_key: dict[str, float] | None = None,
    config: LiveGoalModelConfig | None = None,
    max_remaining_goals: int = 10,
    min_strength: float = 0.70,
    min_confidence: float = 0.70,
    min_edge: float = 0.01,
    min_ev: float = 0.0,
    min_odds: float = 1.50,
    max_odds: float = 2.80,
) -> ProbabilityGovernedOutcome:
    resolved = _resolve_p_real_by_intent_key(
        thesis,
        p_real_source=p_real_source,
        features=features,
        p_real_by_intent_key=p_real_by_intent_key,
        config=config,
        max_remaining_goals=max_remaining_goals,
    )

    governed_outcome = run_governed_thesis_pipeline(
        thesis,
        offers,
        p_real_by_intent_key=resolved.p_real_by_intent_key,
        min_strength=min_strength,
        min_confidence=min_confidence,
        min_edge=min_edge,
        min_ev=min_ev,
        min_odds=min_odds,
        max_odds=max_odds,
    )

    return ProbabilityGovernedOutcome(
        p_real_source=p_real_source,
        p_real_by_intent_key=resolved.p_real_by_intent_key,
        bridge_result=resolved.bridge_result,
        governed_outcome=governed_outcome,
    )


def run_model_generated_governed_thesis_pipeline(
    thesis: StatisticalThesis,
    offers: tuple[BookOffer, ...],
    *,
    features: LiveGoalFeatures,
    config: LiveGoalModelConfig | None = None,
    max_remaining_goals: int = 10,
    min_strength: float = 0.70,
    min_confidence: float = 0.70,
    min_edge: float = 0.01,
    min_ev: float = 0.0,
    min_odds: float = 1.50,
    max_odds: float = 2.80,
) -> ProbabilityGovernedOutcome:
    return run_probability_governed_thesis_pipeline(
        thesis,
        offers,
        p_real_source="model",
        features=features,
        config=config,
        max_remaining_goals=max_remaining_goals,
        min_strength=min_strength,
        min_confidence=min_confidence,
        min_edge=min_edge,
        min_ev=min_ev,
        min_odds=min_odds,
        max_odds=max_odds,
    )


@dataclass(slots=True, frozen=True)
class _ResolvedProbabilityInput:
    p_real_by_intent_key: dict[str, float]
    bridge_result: ProbabilityIntentBridgeResult | None


def _resolve_p_real_by_intent_key(
    thesis: StatisticalThesis,
    *,
    p_real_source: ProbabilityPipelineSource,
    features: LiveGoalFeatures | None,
    p_real_by_intent_key: dict[str, float] | None,
    config: LiveGoalModelConfig | None,
    max_remaining_goals: int,
) -> _ResolvedProbabilityInput:
    if p_real_source == "external":
        if p_real_by_intent_key is None:
            raise ValueError("external probability source requires p_real_by_intent_key")

        return _ResolvedProbabilityInput(
            p_real_by_intent_key=dict(p_real_by_intent_key),
            bridge_result=None,
        )

    if p_real_source == "model":
        if features is None:
            raise ValueError("model probability source requires LiveGoalFeatures")

        bridge_result = build_probability_intent_bridge_result(
            (thesis,),
            features,
            config=config,
            max_remaining_goals=max_remaining_goals,
        )

        return _ResolvedProbabilityInput(
            p_real_by_intent_key=dict(bridge_result.p_real_by_intent_key),
            bridge_result=bridge_result,
        )

    raise ValueError(f"unsupported probability source: {p_real_source}")

