from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from app.fqis.contracts.core import BookOffer, ExecutableBet, StatisticalThesis
from app.fqis.contracts.enums import (
    MarketFamily,
    MarketSide,
    Period,
    TeamRole,
    ThesisKey,
)
from app.fqis.probability.hybrid import (
    HybridProbability,
    HybridProbabilityConfig,
    HybridProbabilityResult,
    run_hybrid_model_governed_thesis_pipeline,
)
from app.fqis.probability.live_goal_model import LiveGoalFeatures, LiveGoalModelConfig
from app.fqis.runtime.model_shadow import ModelShadowInput


@dataclass(slots=True, frozen=True)
class HybridShadowThesisResult:
    thesis_key: str
    p_real_source: str
    p_real_by_intent_key: dict[str, float]
    model_p_real_by_intent_key: dict[str, float]
    market_p_real_by_intent_key: dict[str, float]
    hybrid_probabilities: tuple[HybridProbability, ...]
    accepted_bet: ExecutableBet | None
    technical_best_bet: ExecutableBet | None
    pipeline_rejections: tuple[Any, ...]
    risk_rejections: tuple[Any, ...]

    @property
    def accepted(self) -> bool:
        return self.accepted_bet is not None

    @property
    def hybrid_count(self) -> int:
        return sum(1 for probability in self.hybrid_probabilities if probability.source == "hybrid")

    @property
    def model_only_count(self) -> int:
        return sum(1 for probability in self.hybrid_probabilities if probability.source == "model_only")


@dataclass(slots=True, frozen=True)
class HybridShadowCycleOutcome:
    status: str
    event_id: int
    generated_at_utc: str
    thesis_results: tuple[HybridShadowThesisResult, ...]

    @property
    def thesis_count(self) -> int:
        return len(self.thesis_results)

    @property
    def accepted_bets(self) -> tuple[ExecutableBet, ...]:
        return tuple(
            result.accepted_bet
            for result in self.thesis_results
            if result.accepted_bet is not None
        )

    @property
    def accepted_bet_count(self) -> int:
        return len(self.accepted_bets)

    @property
    def rejected_thesis_count(self) -> int:
        return self.thesis_count - self.accepted_bet_count

    @property
    def hybrid_probability_count(self) -> int:
        return sum(len(result.hybrid_probabilities) for result in self.thesis_results)

    @property
    def hybrid_count(self) -> int:
        return sum(result.hybrid_count for result in self.thesis_results)

    @property
    def model_only_count(self) -> int:
        return sum(result.model_only_count for result in self.thesis_results)


def build_demo_hybrid_shadow_input() -> ModelShadowInput:
    event_id = 3001

    features = LiveGoalFeatures(
        event_id=event_id,
        minute=58,
        home_score=1,
        away_score=0,
        home_xg_live=0.95,
        away_xg_live=0.18,
        home_shots_total=8,
        away_shots_total=3,
        home_shots_on_target=4,
        away_shots_on_target=1,
        home_corners=4,
        away_corners=1,
        home_red_cards=0,
        away_red_cards=0,
    )

    theses = (
        StatisticalThesis(
            event_id=event_id,
            thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
            strength=0.84,
            confidence=0.80,
        ),
        StatisticalThesis(
            event_id=event_id,
            thesis_key=ThesisKey.CAGEY_GAME,
            strength=0.74,
            confidence=0.72,
        ),
    )

    offers = (
        BookOffer(
            event_id=event_id,
            bookmaker_id=1,
            bookmaker_name="BookA",
            family=MarketFamily.TEAM_TOTAL_AWAY,
            side=MarketSide.UNDER,
            period=Period.FT,
            team_role=TeamRole.AWAY,
            line=1.5,
            odds_decimal=1.92,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=8,
        ),
        BookOffer(
            event_id=event_id,
            bookmaker_id=1,
            bookmaker_name="BookA",
            family=MarketFamily.TEAM_TOTAL_AWAY,
            side=MarketSide.OVER,
            period=Period.FT,
            team_role=TeamRole.AWAY,
            line=1.5,
            odds_decimal=1.92,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=8,
        ),
        BookOffer(
            event_id=event_id,
            bookmaker_id=2,
            bookmaker_name="BookB",
            family=MarketFamily.BTTS,
            side=MarketSide.NO,
            period=Period.FT,
            team_role=TeamRole.NONE,
            line=None,
            odds_decimal=1.75,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=9,
        ),
        BookOffer(
            event_id=event_id,
            bookmaker_id=2,
            bookmaker_name="BookB",
            family=MarketFamily.BTTS,
            side=MarketSide.YES,
            period=Period.FT,
            team_role=TeamRole.NONE,
            line=None,
            odds_decimal=2.05,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=9,
        ),
        BookOffer(
            event_id=event_id,
            bookmaker_id=3,
            bookmaker_name="BookC",
            family=MarketFamily.MATCH_TOTAL,
            side=MarketSide.UNDER,
            period=Period.FT,
            team_role=TeamRole.NONE,
            line=2.5,
            odds_decimal=1.82,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=10,
        ),
        BookOffer(
            event_id=event_id,
            bookmaker_id=3,
            bookmaker_name="BookC",
            family=MarketFamily.MATCH_TOTAL,
            side=MarketSide.OVER,
            period=Period.FT,
            team_role=TeamRole.NONE,
            line=2.5,
            odds_decimal=2.00,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=10,
        ),
    )

    return ModelShadowInput(
        event_id=event_id,
        features=features,
        theses=theses,
        offers=offers,
    )


def run_hybrid_shadow_cycle(
    shadow_input: ModelShadowInput,
    *,
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
) -> HybridShadowCycleOutcome:
    thesis_results: list[HybridShadowThesisResult] = []

    for thesis in shadow_input.theses:
        outcome = run_hybrid_model_governed_thesis_pipeline(
            thesis,
            shadow_input.offers,
            features=shadow_input.features,
            config=config,
            hybrid_config=hybrid_config,
            max_remaining_goals=max_remaining_goals,
            market_min_outcomes=market_min_outcomes,
            min_strength=min_strength,
            min_confidence=min_confidence,
            min_edge=min_edge,
            min_ev=min_ev,
            min_odds=min_odds,
            max_odds=max_odds,
        )

        thesis_results.append(
            _to_thesis_result(
                thesis_key=thesis.thesis_key.value,
                hybrid_result=outcome.hybrid_result,
                accepted_bet=outcome.governed_outcome.accepted_bet,
                technical_best_bet=outcome.governed_outcome.technical_best_bet,
                pipeline_rejections=tuple(outcome.governed_outcome.pipeline_rejections),
                risk_rejections=tuple(outcome.governed_outcome.risk_rejections),
            )
        )

    return HybridShadowCycleOutcome(
        status="ok",
        event_id=shadow_input.event_id,
        generated_at_utc=datetime.now(UTC).isoformat(),
        thesis_results=tuple(thesis_results),
    )


def write_hybrid_shadow_jsonl(
    outcome: HybridShadowCycleOutcome,
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(hybrid_shadow_cycle_to_record(outcome), ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return path


def hybrid_shadow_cycle_to_record(outcome: HybridShadowCycleOutcome) -> dict[str, Any]:
    return {
        "status": outcome.status,
        "source": "fqis_hybrid_shadow",
        "event_id": outcome.event_id,
        "generated_at_utc": outcome.generated_at_utc,
        "thesis_count": outcome.thesis_count,
        "accepted_bet_count": outcome.accepted_bet_count,
        "rejected_thesis_count": outcome.rejected_thesis_count,
        "hybrid_probability_count": outcome.hybrid_probability_count,
        "hybrid_count": outcome.hybrid_count,
        "model_only_count": outcome.model_only_count,
        "accepted_bets": [_bet_to_record(bet) for bet in outcome.accepted_bets],
        "thesis_results": [
            _thesis_result_to_record(result)
            for result in outcome.thesis_results
        ],
    }


def _to_thesis_result(
    *,
    thesis_key: str,
    hybrid_result: HybridProbabilityResult,
    accepted_bet: ExecutableBet | None,
    technical_best_bet: ExecutableBet | None,
    pipeline_rejections: tuple[Any, ...],
    risk_rejections: tuple[Any, ...],
) -> HybridShadowThesisResult:
    return HybridShadowThesisResult(
        thesis_key=thesis_key,
        p_real_source="hybrid",
        p_real_by_intent_key=dict(hybrid_result.p_real_by_intent_key),
        model_p_real_by_intent_key=dict(hybrid_result.model_p_real_by_intent_key),
        market_p_real_by_intent_key=dict(hybrid_result.market_p_real_by_intent_key),
        hybrid_probabilities=tuple(hybrid_result.probabilities),
        accepted_bet=accepted_bet,
        technical_best_bet=technical_best_bet,
        pipeline_rejections=pipeline_rejections,
        risk_rejections=risk_rejections,
    )


def _thesis_result_to_record(result: HybridShadowThesisResult) -> dict[str, Any]:
    return {
        "thesis_key": result.thesis_key,
        "p_real_source": result.p_real_source,
        "accepted": result.accepted,
        "hybrid_count": result.hybrid_count,
        "model_only_count": result.model_only_count,
        "p_real_by_intent_key": dict(result.p_real_by_intent_key),
        "model_p_real_by_intent_key": dict(result.model_p_real_by_intent_key),
        "market_p_real_by_intent_key": dict(result.market_p_real_by_intent_key),
        "hybrid_probability_diagnostics": [
            _hybrid_probability_to_record(probability)
            for probability in result.hybrid_probabilities
        ],
        "accepted_bet": _bet_to_record(result.accepted_bet),
        "technical_best_bet": _bet_to_record(result.technical_best_bet),
        "pipeline_rejections": [
            _rejection_to_record(rejection)
            for rejection in result.pipeline_rejections
        ],
        "risk_rejections": [
            _rejection_to_record(rejection)
            for rejection in result.risk_rejections
        ],
    }


def _hybrid_probability_to_record(probability: HybridProbability) -> dict[str, Any]:
    return {
        "intent_key": probability.intent_key,
        "p_model": probability.p_model,
        "p_market_no_vig": probability.p_market_no_vig,
        "p_hybrid": probability.p_hybrid,
        "source": probability.source,
        "has_market_prior": probability.has_market_prior,
        "delta_model_market": probability.delta_model_market,
        "model_weight": probability.model_weight,
        "market_weight": probability.market_weight,
    }


def _bet_to_record(bet: ExecutableBet | None) -> dict[str, Any] | None:
    if bet is None:
        return None

    return {
        "event_id": bet.event_id,
        "thesis_key": _serialize_value(bet.thesis_key),
        "family": _serialize_value(bet.family),
        "side": _serialize_value(bet.side),
        "period": _serialize_value(bet.period),
        "team_role": _serialize_value(bet.team_role),
        "line": bet.line,
        "bookmaker_id": bet.bookmaker_id,
        "bookmaker_name": bet.bookmaker_name,
        "odds_decimal": bet.odds_decimal,
        "p_real": bet.p_real,
        "p_implied": bet.p_implied,
        "edge": bet.edge,
        "ev": bet.ev,
        "score_stat": bet.score_stat,
        "score_exec": bet.score_exec,
        "score_final": bet.score_final,
        "rationale": list(bet.rationale),
    }


def _rejection_to_record(rejection: Any) -> dict[str, Any]:
    if hasattr(rejection, "stage") and hasattr(rejection, "code") and hasattr(rejection, "detail"):
        return {
            "stage": _serialize_value(rejection.stage),
            "code": _serialize_value(rejection.code),
            "detail": str(rejection.detail),
        }

    if isinstance(rejection, tuple) and len(rejection) >= 3:
        return {
            "stage": _serialize_value(rejection[0]),
            "code": _serialize_value(rejection[1]),
            "detail": str(rejection[2]),
        }

    return {
        "stage": "UNKNOWN",
        "code": "UNKNOWN",
        "detail": str(rejection),
    }


def _serialize_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value

    return value
    