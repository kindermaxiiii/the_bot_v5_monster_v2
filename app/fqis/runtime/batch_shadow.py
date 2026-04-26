from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.fqis.contracts.enums import ThesisKey
from app.fqis.runtime.shadow import FqisShadowInput, build_demo_shadow_input, run_shadow_cycle


@dataclass(slots=True, frozen=True)
class FqisShadowBatchResult:
    records: tuple[dict[str, Any], ...]
    summary: dict[str, Any]


def build_demo_shadow_inputs() -> tuple[FqisShadowInput, ...]:
    accepted_low_away = build_demo_shadow_input()

    rejected_open_game = FqisShadowInput(
        live_match_row={
            "event_id": 1202,
            "home_xg_live": 1.10,
            "away_xg_live": 0.95,
            "home_shots_on_target": 5,
            "away_shots_on_target": 4,
            "minute": 64,
            "home_score": 1,
            "away_score": 1,
        },
        live_offer_rows=(
            {
                "event_id": 1202,
                "bookmaker_id": 1,
                "bookmaker_name": "BookA",
                "family": "MATCH_TOTAL",
                "side": "UNDER",
                "period": "FT",
                "team_role": "NONE",
                "line": 2.5,
                "odds_decimal": 1.90,
                "source_timestamp_utc": "2026-04-26T00:00:00+00:00",
                "freshness_seconds": 8,
            },
        ),
        p_real_by_thesis={
            ThesisKey.OPEN_GAME: {
                "MATCH_TOTAL|OVER|NONE|2.5": 0.58,
                "BTTS|YES|NONE|NA": 0.56,
            },
        },
    )

    accepted_low_home = FqisShadowInput(
        live_match_row={
            "event_id": 1203,
            "home_xg_live": 0.20,
            "away_xg_live": 0.80,
            "home_shots_on_target": 0,
            "away_shots_on_target": 4,
            "minute": 54,
            "home_score": 0,
            "away_score": 1,
        },
        live_offer_rows=(
            {
                "event_id": 1203,
                "bookmaker_id": 3,
                "bookmaker_name": "BookC",
                "family": "TEAM_TOTAL_HOME",
                "side": "UNDER",
                "period": "FT",
                "team_role": "HOME",
                "line": 1.5,
                "odds_decimal": 1.88,
                "source_timestamp_utc": "2026-04-26T00:00:00+00:00",
                "freshness_seconds": 6,
            },
            {
                "event_id": 1203,
                "bookmaker_id": 4,
                "bookmaker_name": "BookD",
                "family": "BTTS",
                "side": "NO",
                "period": "FT",
                "team_role": "NONE",
                "line": None,
                "odds_decimal": 1.80,
                "source_timestamp_utc": "2026-04-26T00:00:00+00:00",
                "freshness_seconds": 10,
            },
        ),
        p_real_by_thesis={
            ThesisKey.LOW_HOME_SCORING_HAZARD: {
                "TEAM_TOTAL_HOME|UNDER|HOME|1.5": 0.61,
                "BTTS|NO|NONE|NA": 0.58,
            },
            ThesisKey.OPEN_GAME: {
                "MATCH_TOTAL|OVER|NONE|2.5": 0.55,
                "BTTS|YES|NONE|NA": 0.53,
            },
        },
    )

    return (
        accepted_low_away,
        rejected_open_game,
        accepted_low_home,
    )


def run_shadow_batch(
    shadow_inputs: tuple[FqisShadowInput, ...],
    *,
    min_strength: float = 0.70,
    min_confidence: float = 0.70,
    min_edge: float = 0.02,
    min_ev: float = 0.01,
    min_odds: float = 1.50,
    max_odds: float = 2.80,
) -> FqisShadowBatchResult:
    records: list[dict[str, Any]] = []

    for index, shadow_input in enumerate(shadow_inputs, start=1):
        record = run_shadow_cycle(
            shadow_input,
            min_strength=min_strength,
            min_confidence=min_confidence,
            min_edge=min_edge,
            min_ev=min_ev,
            min_odds=min_odds,
            max_odds=max_odds,
        )
        record["batch_index"] = index
        records.append(record)

    summary = _build_batch_summary(tuple(records))

    return FqisShadowBatchResult(
        records=tuple(records),
        summary=summary,
    )


def write_shadow_batch_jsonl(records: tuple[dict[str, Any], ...], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            handle.write("\n")


def _build_batch_summary(records: tuple[dict[str, Any], ...]) -> dict[str, Any]:
    match_count = len(records)
    accepted_match_count = sum(1 for record in records if record.get("best_accepted_bet") is not None)
    total_accepted_bet_count = sum(int(record.get("accepted_bet_count", 0)) for record in records)
    total_thesis_count = sum(int(record.get("thesis_count", 0)) for record in records)
    total_thesis_result_count = sum(int(record.get("thesis_result_count", 0)) for record in records)

    return {
        "schema_version": 1,
        "engine": "fqis",
        "mode": "shadow_batch",
        "source": "demo",
        "status": "ok",
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "match_count": match_count,
        "accepted_match_count": accepted_match_count,
        "rejected_match_count": match_count - accepted_match_count,
        "total_accepted_bet_count": total_accepted_bet_count,
        "total_thesis_count": total_thesis_count,
        "total_thesis_result_count": total_thesis_result_count,
        "acceptance_rate": 0.0 if match_count == 0 else accepted_match_count / match_count,
    }

    