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
from app.fqis.probability.live_goal_model import LiveGoalFeatures, LiveGoalModelConfig
from app.fqis.probability.model_pipeline import (
    ProbabilityGovernedOutcome,
    run_model_generated_governed_thesis_pipeline,
)


@dataclass(slots=True, frozen=True)
class ModelShadowInput:
    event_id: int
    features: LiveGoalFeatures
    theses: tuple[StatisticalThesis, ...]
    offers: tuple[BookOffer, ...]


@dataclass(slots=True, frozen=True)
class ModelShadowThesisResult:
    thesis_key: str
    p_real_source: str
    p_real_by_intent_key: dict[str, float]
    accepted_bet: ExecutableBet | None
    technical_best_bet: ExecutableBet | None
    pipeline_rejections: tuple[Any, ...]
    risk_rejections: tuple[Any, ...]

    @property
    def accepted(self) -> bool:
        return self.accepted_bet is not None


@dataclass(slots=True, frozen=True)
class ModelShadowCycleOutcome:
    status: str
    event_id: int
    generated_at_utc: str
    thesis_results: tuple[ModelShadowThesisResult, ...]

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
    def model_probability_count(self) -> int:
        return sum(len(result.p_real_by_intent_key) for result in self.thesis_results)


def build_demo_model_shadow_input() -> ModelShadowInput:
    event_id = 2601

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
    )

    return ModelShadowInput(
        event_id=event_id,
        features=features,
        theses=theses,
        offers=offers,
    )


def run_model_shadow_cycle(
    shadow_input: ModelShadowInput,
    *,
    config: LiveGoalModelConfig | None = None,
    max_remaining_goals: int = 10,
    min_strength: float = 0.70,
    min_confidence: float = 0.70,
    min_edge: float = 0.01,
    min_ev: float = 0.0,
    min_odds: float = 1.50,
    max_odds: float = 2.80,
) -> ModelShadowCycleOutcome:
    thesis_results: list[ModelShadowThesisResult] = []

    for thesis in shadow_input.theses:
        outcome = run_model_generated_governed_thesis_pipeline(
            thesis,
            shadow_input.offers,
            features=shadow_input.features,
            config=config,
            max_remaining_goals=max_remaining_goals,
            min_strength=min_strength,
            min_confidence=min_confidence,
            min_edge=min_edge,
            min_ev=min_ev,
            min_odds=min_odds,
            max_odds=max_odds,
        )

        thesis_results.append(_to_thesis_result(outcome, thesis))

    return ModelShadowCycleOutcome(
        status="ok",
        event_id=shadow_input.event_id,
        generated_at_utc=datetime.now(UTC).isoformat(),
        thesis_results=tuple(thesis_results),
    )


def write_model_shadow_jsonl(
    outcome: ModelShadowCycleOutcome,
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    record = model_shadow_cycle_to_record(outcome)

    path.write_text(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return path


def model_shadow_cycle_to_record(outcome: ModelShadowCycleOutcome) -> dict[str, Any]:
    return {
        "status": outcome.status,
        "source": "fqis_model_shadow",
        "event_id": outcome.event_id,
        "generated_at_utc": outcome.generated_at_utc,
        "thesis_count": outcome.thesis_count,
        "accepted_bet_count": outcome.accepted_bet_count,
        "rejected_thesis_count": outcome.rejected_thesis_count,
        "model_probability_count": outcome.model_probability_count,
        "accepted_bets": [_bet_to_record(bet) for bet in outcome.accepted_bets],
        "thesis_results": [
            _thesis_result_to_record(result)
            for result in outcome.thesis_results
        ],
    }


def _to_thesis_result(
    outcome: ProbabilityGovernedOutcome,
    thesis: StatisticalThesis,
) -> ModelShadowThesisResult:
    governed = outcome.governed_outcome

    return ModelShadowThesisResult(
        thesis_key=thesis.thesis_key.value,
        p_real_source=outcome.p_real_source,
        p_real_by_intent_key=dict(outcome.p_real_by_intent_key),
        accepted_bet=governed.accepted_bet,
        technical_best_bet=governed.technical_best_bet,
        pipeline_rejections=tuple(governed.pipeline_rejections),
        risk_rejections=tuple(governed.risk_rejections),
    )


def _thesis_result_to_record(result: ModelShadowThesisResult) -> dict[str, Any]:
    return {
        "thesis_key": result.thesis_key,
        "p_real_source": result.p_real_source,
        "accepted": result.accepted,
        "p_real_by_intent_key": dict(result.p_real_by_intent_key),
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

    