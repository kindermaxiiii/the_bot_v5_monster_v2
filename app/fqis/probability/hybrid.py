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
from app.fqis.probability.market_prior import (
    MarketModelComparison,
    build_market_prior_by_intent_key,
    compare_model_to_market_prior,
)


HybridProbabilitySource = Literal["hybrid", "model_only"]


@dataclass(slots=True, frozen=True)
class HybridProbabilityConfig:
    model_weight: float = 0.70
    market_weight: float = 0.30

    def __post_init__(self) -> None:
        _validate_non_negative_weight(self.model_weight, "model_weight")
        _validate_non_negative_weight(self.market_weight, "market_weight")

        if self.model_weight + self.market_weight <= 0.0:
            raise ValueError("model_weight + market_weight must be > 0")

    @property
    def normalized_model_weight(self) -> float:
        return self.model_weight / (self.model_weight + self.market_weight)

    @property
    def normalized_market_weight(self) -> float:
        return self.market_weight / (self.model_weight + self.market_weight)


@dataclass(slots=True, frozen=True)
class HybridProbability:
    intent_key: str
    p_model: float
    p_market_no_vig: float | None
    p_hybrid: float
    model_weight: float
    market_weight: float
    source: HybridProbabilitySource
    delta_model_market: float | None

    @property
    def has_market_prior(self) -> bool:
        return self.p_market_no_vig is not None


@dataclass(slots=True, frozen=True)
class HybridProbabilityResult:
    p_real_by_intent_key: dict[str, float]
    probabilities: tuple[HybridProbability, ...]
    model_p_real_by_intent_key: dict[str, float]
    market_p_real_by_intent_key: dict[str, float]
    comparisons: tuple[MarketModelComparison, ...]

    @property
    def hybrid_count(self) -> int:
        return sum(1 for probability in self.probabilities if probability.source == "hybrid")

    @property
    def model_only_count(self) -> int:
        return sum(1 for probability in self.probabilities if probability.source == "model_only")


@dataclass(slots=True, frozen=True)
class HybridPipelineOutcome:
    bridge_result: ProbabilityIntentBridgeResult
    market_prior_by_intent_key: dict[str, float]
    hybrid_result: HybridProbabilityResult
    pipeline_outcome: PipelineOutcome


@dataclass(slots=True, frozen=True)
class HybridGovernedOutcome:
    bridge_result: ProbabilityIntentBridgeResult
    market_prior_by_intent_key: dict[str, float]
    hybrid_result: HybridProbabilityResult
    governed_outcome: GovernedOutcome


def blend_model_and_market_probability(
    *,
    p_model: float,
    p_market_no_vig: float,
    config: HybridProbabilityConfig | None = None,
) -> float:
    cfg = config or HybridProbabilityConfig()

    _validate_probability(p_model, "p_model")
    _validate_probability(p_market_no_vig, "p_market_no_vig")

    blended = (
        cfg.normalized_model_weight * p_model
        + cfg.normalized_market_weight * p_market_no_vig
    )

    return _clamp_probability(blended)


def build_hybrid_probability_result(
    model_p_real_by_intent_key: dict[str, float],
    market_p_real_by_intent_key: dict[str, float],
    *,
    config: HybridProbabilityConfig | None = None,
) -> HybridProbabilityResult:
    cfg = config or HybridProbabilityConfig()

    probabilities: list[HybridProbability] = []
    p_real_by_intent_key: dict[str, float] = {}

    comparisons = compare_model_to_market_prior(
        model_p_real_by_intent_key,
        market_p_real_by_intent_key,
    )

    for comparison in comparisons:
        _validate_probability(comparison.p_model, "p_model")

        if comparison.p_market_no_vig is None:
            p_hybrid = comparison.p_model
            model_weight = 1.0
            market_weight = 0.0
            source: HybridProbabilitySource = "model_only"
        else:
            _validate_probability(comparison.p_market_no_vig, "p_market_no_vig")
            p_hybrid = blend_model_and_market_probability(
                p_model=comparison.p_model,
                p_market_no_vig=comparison.p_market_no_vig,
                config=cfg,
            )
            model_weight = cfg.normalized_model_weight
            market_weight = cfg.normalized_market_weight
            source = "hybrid"

        p_real_by_intent_key[comparison.intent_key] = p_hybrid

        probabilities.append(
            HybridProbability(
                intent_key=comparison.intent_key,
                p_model=comparison.p_model,
                p_market_no_vig=comparison.p_market_no_vig,
                p_hybrid=p_hybrid,
                model_weight=model_weight,
                market_weight=market_weight,
                source=source,
                delta_model_market=comparison.delta_model_market,
            )
        )

    return HybridProbabilityResult(
        p_real_by_intent_key=p_real_by_intent_key,
        probabilities=tuple(probabilities),
        model_p_real_by_intent_key=dict(model_p_real_by_intent_key),
        market_p_real_by_intent_key=dict(market_p_real_by_intent_key),
        comparisons=comparisons,
    )


def build_hybrid_probability_result_for_thesis(
    thesis: StatisticalThesis,
    offers: tuple[BookOffer, ...],
    *,
    features: LiveGoalFeatures,
    config: LiveGoalModelConfig | None = None,
    hybrid_config: HybridProbabilityConfig | None = None,
    max_remaining_goals: int = 10,
    market_min_outcomes: int = 2,
) -> tuple[ProbabilityIntentBridgeResult, dict[str, float], HybridProbabilityResult]:
    bridge_result = build_probability_intent_bridge_result(
        (thesis,),
        features,
        config=config,
        max_remaining_goals=max_remaining_goals,
    )

    market_prior_by_intent_key = build_market_prior_by_intent_key(
        offers,
        min_outcomes=market_min_outcomes,
    )

    hybrid_result = build_hybrid_probability_result(
        bridge_result.p_real_by_intent_key,
        market_prior_by_intent_key,
        config=hybrid_config,
    )

    return bridge_result, market_prior_by_intent_key, hybrid_result


def run_hybrid_model_thesis_pipeline(
    thesis: StatisticalThesis,
    offers: tuple[BookOffer, ...],
    *,
    features: LiveGoalFeatures,
    config: LiveGoalModelConfig | None = None,
    hybrid_config: HybridProbabilityConfig | None = None,
    max_remaining_goals: int = 10,
    market_min_outcomes: int = 2,
    min_edge: float = 0.01,
    min_ev: float = 0.0,
    min_odds: float = 1.50,
    max_odds: float = 2.80,
) -> HybridPipelineOutcome:
    bridge_result, market_prior_by_intent_key, hybrid_result = (
        build_hybrid_probability_result_for_thesis(
            thesis,
            offers,
            features=features,
            config=config,
            hybrid_config=hybrid_config,
            max_remaining_goals=max_remaining_goals,
            market_min_outcomes=market_min_outcomes,
        )
    )

    pipeline_outcome = run_thesis_pipeline(
        thesis,
        offers,
        p_real_by_intent_key=hybrid_result.p_real_by_intent_key,
        min_edge=min_edge,
        min_ev=min_ev,
        min_odds=min_odds,
        max_odds=max_odds,
    )

    return HybridPipelineOutcome(
        bridge_result=bridge_result,
        market_prior_by_intent_key=market_prior_by_intent_key,
        hybrid_result=hybrid_result,
        pipeline_outcome=pipeline_outcome,
    )


def run_hybrid_model_governed_thesis_pipeline(
    thesis: StatisticalThesis,
    offers: tuple[BookOffer, ...],
    *,
    features: LiveGoalFeatures,
    config: LiveGoalModelConfig | None = None,
    hybrid_config: HybridProbabilityConfig | None = None,
    max_remaining_goals: int = 10,
    market_min_outcomes: int = 2,
    min_strength: float = 0.70,
    min_confidence: float = 0.70,
    min_edge: float = 0.01,
    min_ev: float = 0.0,
    min_odds: float = 1.50,
    max_odds: float = 2.80,
) -> HybridGovernedOutcome:
    bridge_result, market_prior_by_intent_key, hybrid_result = (
        build_hybrid_probability_result_for_thesis(
            thesis,
            offers,
            features=features,
            config=config,
            hybrid_config=hybrid_config,
            max_remaining_goals=max_remaining_goals,
            market_min_outcomes=market_min_outcomes,
        )
    )

    governed_outcome = run_governed_thesis_pipeline(
        thesis,
        offers,
        p_real_by_intent_key=hybrid_result.p_real_by_intent_key,
        min_strength=min_strength,
        min_confidence=min_confidence,
        min_edge=min_edge,
        min_ev=min_ev,
        min_odds=min_odds,
        max_odds=max_odds,
    )

    return HybridGovernedOutcome(
        bridge_result=bridge_result,
        market_prior_by_intent_key=market_prior_by_intent_key,
        hybrid_result=hybrid_result,
        governed_outcome=governed_outcome,
    )


def _validate_probability(value: float, field_name: str) -> None:
    if not 0.0 <= float(value) <= 1.0:
        raise ValueError(f"{field_name} must be between 0 and 1")


def _validate_non_negative_weight(value: float, field_name: str) -> None:
    if float(value) < 0.0:
        raise ValueError(f"{field_name} must be >= 0")


def _clamp_probability(value: float) -> float:
    value = float(value)

    if abs(value) <= 1e-15:
        return 0.0

    if abs(value - 1.0) <= 1e-15:
        return 1.0

    return max(0.0, min(1.0, value))