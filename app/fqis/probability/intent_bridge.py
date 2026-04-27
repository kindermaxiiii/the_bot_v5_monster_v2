from __future__ import annotations

from dataclasses import dataclass

from app.fqis.contracts.core import MarketIntent, StatisticalThesis
from app.fqis.probability.live_goal_model import (
    LiveGoalFeatures,
    LiveGoalModelConfig,
    build_live_score_distribution,
)
from app.fqis.probability.market_probabilities import probability_for_intent
from app.fqis.probability.models import MarketProbability, ScoreDistribution
from app.fqis.thesis.intent_mapper import map_thesis_to_market_intents


@dataclass(slots=True, frozen=True)
class UnsupportedProbabilityIntent:
    thesis_key: str
    intent_key: str
    reason: str


@dataclass(slots=True, frozen=True)
class ProbabilityIntentBridgeResult:
    p_real_by_intent_key: dict[str, float]
    market_probabilities: tuple[MarketProbability, ...]
    unsupported_intents: tuple[UnsupportedProbabilityIntent, ...]
    distribution: ScoreDistribution

    @property
    def has_probabilities(self) -> bool:
        return bool(self.p_real_by_intent_key)

    @property
    def unsupported_count(self) -> int:
        return len(self.unsupported_intents)


def build_probability_intent_bridge_result(
    theses: tuple[StatisticalThesis, ...],
    features: LiveGoalFeatures,
    *,
    config: LiveGoalModelConfig | None = None,
    max_remaining_goals: int = 10,
    fail_on_unsupported: bool = False,
) -> ProbabilityIntentBridgeResult:
    distribution = build_live_score_distribution(
        features,
        config=config,
        max_remaining_goals=max_remaining_goals,
    )

    return build_probability_intent_bridge_result_from_distribution(
        theses,
        distribution,
        fail_on_unsupported=fail_on_unsupported,
    )


def build_probability_intent_bridge_result_from_distribution(
    theses: tuple[StatisticalThesis, ...],
    distribution: ScoreDistribution,
    *,
    fail_on_unsupported: bool = False,
) -> ProbabilityIntentBridgeResult:
    p_real_by_intent_key: dict[str, float] = {}
    market_probabilities: list[MarketProbability] = []
    unsupported_intents: list[UnsupportedProbabilityIntent] = []

    for thesis in theses:
        intents = map_thesis_to_market_intents(thesis)

        for intent in intents:
            intent_key = intent_probability_key(intent)

            try:
                market_probability = probability_for_intent(distribution, intent)
            except ValueError as exc:
                unsupported = UnsupportedProbabilityIntent(
                    thesis_key=thesis.thesis_key.value,
                    intent_key=intent_key,
                    reason=str(exc),
                )

                if fail_on_unsupported:
                    raise ValueError(
                        f"unsupported probability intent {intent_key}: {exc}"
                    ) from exc

                unsupported_intents.append(unsupported)
                continue

            p_real_by_intent_key[intent_key] = market_probability.probability
            market_probabilities.append(market_probability)

    return ProbabilityIntentBridgeResult(
        p_real_by_intent_key=p_real_by_intent_key,
        market_probabilities=tuple(market_probabilities),
        unsupported_intents=tuple(unsupported_intents),
        distribution=distribution,
    )


def build_p_real_by_intent_key(
    theses: tuple[StatisticalThesis, ...],
    features: LiveGoalFeatures,
    *,
    config: LiveGoalModelConfig | None = None,
    max_remaining_goals: int = 10,
    fail_on_unsupported: bool = False,
) -> dict[str, float]:
    result = build_probability_intent_bridge_result(
        theses,
        features,
        config=config,
        max_remaining_goals=max_remaining_goals,
        fail_on_unsupported=fail_on_unsupported,
    )

    return result.p_real_by_intent_key


def intent_probability_key(intent: MarketIntent) -> str:
    line = "NA" if intent.line is None else str(intent.line)
    return f"{intent.family.value}|{intent.side.value}|{intent.team_role.value}|{line}"