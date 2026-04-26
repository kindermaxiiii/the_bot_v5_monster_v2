from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.fqis.probability.hybrid import HybridProbabilityConfig
from app.fqis.probability.live_goal_model import LiveGoalModelConfig
from app.fqis.runtime.hybrid_shadow import (
    HybridShadowCycleOutcome,
    hybrid_shadow_cycle_to_record,
    run_hybrid_shadow_cycle,
)
from app.fqis.runtime.model_batch_shadow import load_model_shadow_inputs_from_jsonl
from app.fqis.runtime.model_shadow import ModelShadowInput


@dataclass(slots=True, frozen=True)
class HybridShadowBatchOutcome:
    status: str
    source_path: Path | None
    generated_at_utc: str
    cycle_outcomes: tuple[HybridShadowCycleOutcome, ...]

    @property
    def match_count(self) -> int:
        return len(self.cycle_outcomes)

    @property
    def accepted_match_count(self) -> int:
        return sum(1 for outcome in self.cycle_outcomes if outcome.accepted_bet_count > 0)

    @property
    def rejected_match_count(self) -> int:
        return self.match_count - self.accepted_match_count

    @property
    def accepted_bet_count(self) -> int:
        return sum(outcome.accepted_bet_count for outcome in self.cycle_outcomes)

    @property
    def thesis_count(self) -> int:
        return sum(outcome.thesis_count for outcome in self.cycle_outcomes)

    @property
    def hybrid_probability_count(self) -> int:
        return sum(outcome.hybrid_probability_count for outcome in self.cycle_outcomes)

    @property
    def hybrid_count(self) -> int:
        return sum(outcome.hybrid_count for outcome in self.cycle_outcomes)

    @property
    def model_only_count(self) -> int:
        return sum(outcome.model_only_count for outcome in self.cycle_outcomes)

    @property
    def acceptance_rate(self) -> float:
        if self.match_count == 0:
            return 0.0

        return self.accepted_match_count / self.match_count


def run_hybrid_shadow_batch(
    shadow_inputs: tuple[ModelShadowInput, ...],
    *,
    source_path: Path | None = None,
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
) -> HybridShadowBatchOutcome:
    cycle_outcomes = tuple(
        run_hybrid_shadow_cycle(
            shadow_input,
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
        for shadow_input in shadow_inputs
    )

    return HybridShadowBatchOutcome(
        status="ok",
        source_path=source_path,
        generated_at_utc=datetime.now(UTC).isoformat(),
        cycle_outcomes=cycle_outcomes,
    )


def run_hybrid_shadow_batch_from_jsonl(
    path: Path,
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
) -> HybridShadowBatchOutcome:
    shadow_inputs = load_model_shadow_inputs_from_jsonl(path)

    return run_hybrid_shadow_batch(
        shadow_inputs,
        source_path=path,
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


def write_hybrid_shadow_batch_jsonl(
    outcome: HybridShadowBatchOutcome,
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    record = hybrid_shadow_batch_to_record(outcome)

    path.write_text(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return path


def hybrid_shadow_batch_to_record(outcome: HybridShadowBatchOutcome) -> dict[str, Any]:
    return {
        "status": outcome.status,
        "source": "fqis_hybrid_shadow_batch",
        "source_path": str(outcome.source_path) if outcome.source_path is not None else None,
        "generated_at_utc": outcome.generated_at_utc,
        "match_count": outcome.match_count,
        "accepted_match_count": outcome.accepted_match_count,
        "rejected_match_count": outcome.rejected_match_count,
        "accepted_bet_count": outcome.accepted_bet_count,
        "thesis_count": outcome.thesis_count,
        "hybrid_probability_count": outcome.hybrid_probability_count,
        "hybrid_count": outcome.hybrid_count,
        "model_only_count": outcome.model_only_count,
        "acceptance_rate": outcome.acceptance_rate,
        "cycles": [
            hybrid_shadow_cycle_to_record(cycle_outcome)
            for cycle_outcome in outcome.cycle_outcomes
        ],
    }