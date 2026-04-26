from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.fqis.audit.rejection_codes import RejectionCode, RejectionStage
from app.fqis.contracts.core import ExecutableBet, StatisticalThesis
from app.fqis.contracts.enums import ThesisKey
from app.fqis.engine import GovernedOutcome
from app.fqis.live.bridge import run_live_bridge_cycle
from app.fqis.pipeline import PipelineRejection


@dataclass(slots=True, frozen=True)
class FqisShadowInput:
    live_match_row: dict[str, Any]
    live_offer_rows: tuple[dict[str, Any], ...]
    p_real_by_thesis: dict[ThesisKey, dict[str, float]]


def build_demo_shadow_input() -> FqisShadowInput:
    return FqisShadowInput(
        live_match_row={
            "event_id": 1201,
            "home_xg_live": 0.95,
            "away_xg_live": 0.18,
            "home_shots_on_target": 4,
            "away_shots_on_target": 1,
            "minute": 58,
            "home_score": 1,
            "away_score": 0,
        },
        live_offer_rows=(
            {
                "event_id": 1201,
                "bookmaker_id": 1,
                "bookmaker_name": "BookA",
                "family": "TEAM_TOTAL_AWAY",
                "side": "UNDER",
                "period": "FT",
                "team_role": "AWAY",
                "line": 1.5,
                "odds_decimal": 1.92,
                "source_timestamp_utc": "2026-04-26T00:00:00+00:00",
                "freshness_seconds": 8,
            },
            {
                "event_id": 1201,
                "bookmaker_id": 2,
                "bookmaker_name": "BookB",
                "family": "BTTS",
                "side": "NO",
                "period": "FT",
                "team_role": "NONE",
                "line": None,
                "odds_decimal": 1.75,
                "source_timestamp_utc": "2026-04-26T00:00:00+00:00",
                "freshness_seconds": 9,
            },
        ),
        p_real_by_thesis={
            ThesisKey.LOW_AWAY_SCORING_HAZARD: {
                "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5": 0.62,
                "BTTS|NO|NONE|NA": 0.59,
            },
            ThesisKey.CAGEY_GAME: {
                "MATCH_TOTAL|UNDER|NONE|2.5": 0.57,
            },
        },
    )


def run_shadow_cycle(
    shadow_input: FqisShadowInput,
    *,
    min_strength: float = 0.70,
    min_confidence: float = 0.70,
    min_edge: float = 0.02,
    min_ev: float = 0.01,
    min_odds: float = 1.50,
    max_odds: float = 2.80,
) -> dict[str, Any]:
    result = run_live_bridge_cycle(
        shadow_input.live_match_row,
        shadow_input.live_offer_rows,
        p_real_by_thesis=shadow_input.p_real_by_thesis,
        min_strength=min_strength,
        min_confidence=min_confidence,
        min_edge=min_edge,
        min_ev=min_ev,
        min_odds=min_odds,
        max_odds=max_odds,
    )

    accepted_bets = [
        thesis_result.outcome.accepted_bet
        for thesis_result in result.thesis_results
        if thesis_result.outcome.accepted_bet is not None
    ]

    return {
        "schema_version": 1,
        "engine": "fqis",
        "mode": "shadow",
        "source": "demo",
        "status": "ok",
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "event_id": shadow_input.live_match_row["event_id"],
        "thesis_count": len(result.theses),
        "thesis_result_count": len(result.thesis_results),
        "accepted_bet_count": len(accepted_bets),
        "best_accepted_bet": _serialize_bet(result.best_accepted_bet),
        "thesis_results": [
            {
                "thesis": _serialize_thesis(thesis_result.thesis),
                "outcome": _serialize_outcome(thesis_result.outcome),
            }
            for thesis_result in result.thesis_results
        ],
    }


def write_shadow_jsonl(record: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
        handle.write("\n")


def _serialize_thesis(thesis: StatisticalThesis) -> dict[str, Any]:
    return {
        "event_id": thesis.event_id,
        "thesis_key": thesis.thesis_key.value,
        "strength": thesis.strength,
        "confidence": thesis.confidence,
        "rationale": list(thesis.rationale),
        "features": thesis.features,
    }


def _serialize_outcome(outcome: GovernedOutcome) -> dict[str, Any]:
    return {
        "technical_best_bet": _serialize_bet(outcome.technical_best_bet),
        "accepted_bet": _serialize_bet(outcome.accepted_bet),
        "pipeline_rejections": [
            _serialize_pipeline_rejection(rejection)
            for rejection in outcome.pipeline_rejections
        ],
        "risk_rejections": [
            _serialize_risk_rejection(stage, code, detail)
            for stage, code, detail in outcome.risk_rejections
        ],
    }


def _serialize_bet(bet: ExecutableBet | None) -> dict[str, Any] | None:
    if bet is None:
        return None

    return {
        "event_id": bet.event_id,
        "thesis_key": bet.thesis_key.value,
        "family": bet.family.value,
        "side": bet.side.value,
        "period": bet.period.value,
        "team_role": bet.team_role.value,
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


def _serialize_pipeline_rejection(rejection: PipelineRejection) -> dict[str, Any]:
    return {
        "stage": rejection.stage.value,
        "code": rejection.code.value,
        "detail": rejection.detail,
    }


def _serialize_risk_rejection(
    stage: RejectionStage,
    code: RejectionCode,
    detail: str,
) -> dict[str, Any]:
    return {
        "stage": stage.value,
        "code": code.value,
        "detail": detail,
    }

    