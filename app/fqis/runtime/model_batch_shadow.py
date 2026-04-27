from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.fqis.contracts.core import BookOffer, StatisticalThesis
from app.fqis.contracts.enums import (
    MarketFamily,
    MarketSide,
    Period,
    TeamRole,
    ThesisKey,
)
from app.fqis.probability.live_goal_model import LiveGoalFeatures, LiveGoalModelConfig
from app.fqis.runtime.model_shadow import (
    ModelShadowCycleOutcome,
    ModelShadowInput,
    model_shadow_cycle_to_record,
    run_model_shadow_cycle,
)


@dataclass(slots=True, frozen=True)
class ModelShadowBatchOutcome:
    status: str
    source_path: Path | None
    generated_at_utc: str
    cycle_outcomes: tuple[ModelShadowCycleOutcome, ...]

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
    def model_probability_count(self) -> int:
        return sum(outcome.model_probability_count for outcome in self.cycle_outcomes)

    @property
    def acceptance_rate(self) -> float:
        if self.match_count == 0:
            return 0.0

        return self.accepted_match_count / self.match_count


def load_model_shadow_inputs_from_jsonl(path: Path) -> tuple[ModelShadowInput, ...]:
    if not path.exists():
        raise FileNotFoundError(f"model shadow input file not found: {path}")

    inputs: list[ModelShadowInput] = []

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue

        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on line {line_number}: {exc}") from exc

        if not isinstance(row, dict):
            raise ValueError(f"line {line_number}: row must be a JSON object")

        inputs.append(_row_to_model_shadow_input(row, line_number=line_number))

    if not inputs:
        raise ValueError(f"model shadow input file is empty: {path}")

    return tuple(inputs)


def run_model_shadow_batch(
    shadow_inputs: tuple[ModelShadowInput, ...],
    *,
    source_path: Path | None = None,
    config: LiveGoalModelConfig | None = None,
    max_remaining_goals: int = 10,
    min_strength: float = 0.70,
    min_confidence: float = 0.70,
    min_edge: float = 0.01,
    min_ev: float = 0.0,
    min_odds: float = 1.50,
    max_odds: float = 2.80,
) -> ModelShadowBatchOutcome:
    cycle_outcomes = tuple(
        run_model_shadow_cycle(
            shadow_input,
            config=config,
            max_remaining_goals=max_remaining_goals,
            min_strength=min_strength,
            min_confidence=min_confidence,
            min_edge=min_edge,
            min_ev=min_ev,
            min_odds=min_odds,
            max_odds=max_odds,
        )
        for shadow_input in shadow_inputs
    )

    return ModelShadowBatchOutcome(
        status="ok",
        source_path=source_path,
        generated_at_utc=datetime.now(UTC).isoformat(),
        cycle_outcomes=cycle_outcomes,
    )


def run_model_shadow_batch_from_jsonl(
    path: Path,
    *,
    config: LiveGoalModelConfig | None = None,
    max_remaining_goals: int = 10,
    min_strength: float = 0.70,
    min_confidence: float = 0.70,
    min_edge: float = 0.01,
    min_ev: float = 0.0,
    min_odds: float = 1.50,
    max_odds: float = 2.80,
) -> ModelShadowBatchOutcome:
    shadow_inputs = load_model_shadow_inputs_from_jsonl(path)

    return run_model_shadow_batch(
        shadow_inputs,
        source_path=path,
        config=config,
        max_remaining_goals=max_remaining_goals,
        min_strength=min_strength,
        min_confidence=min_confidence,
        min_edge=min_edge,
        min_ev=min_ev,
        min_odds=min_odds,
        max_odds=max_odds,
    )


def write_model_shadow_batch_jsonl(
    outcome: ModelShadowBatchOutcome,
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    record = model_shadow_batch_to_record(outcome)

    path.write_text(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return path


def model_shadow_batch_to_record(outcome: ModelShadowBatchOutcome) -> dict[str, Any]:
    return {
        "status": outcome.status,
        "source": "fqis_model_shadow_batch",
        "source_path": str(outcome.source_path) if outcome.source_path is not None else None,
        "generated_at_utc": outcome.generated_at_utc,
        "match_count": outcome.match_count,
        "accepted_match_count": outcome.accepted_match_count,
        "rejected_match_count": outcome.rejected_match_count,
        "accepted_bet_count": outcome.accepted_bet_count,
        "thesis_count": outcome.thesis_count,
        "model_probability_count": outcome.model_probability_count,
        "acceptance_rate": outcome.acceptance_rate,
        "cycles": [
            model_shadow_cycle_to_record(cycle_outcome)
            for cycle_outcome in outcome.cycle_outcomes
        ],
    }


def _row_to_model_shadow_input(row: dict[str, Any], *, line_number: int) -> ModelShadowInput:
    live_match_row = _resolve_live_match_row(row, line_number=line_number)
    event_id = _resolve_event_id(row, live_match_row, line_number=line_number)

    features = _live_match_row_to_features(live_match_row, event_id=event_id, line_number=line_number)
    offers = _resolve_offers(row, event_id=event_id, line_number=line_number)
    theses = _resolve_theses(row, event_id=event_id, line_number=line_number)

    return ModelShadowInput(
        event_id=event_id,
        features=features,
        theses=theses,
        offers=offers,
    )


def _resolve_live_match_row(row: dict[str, Any], *, line_number: int) -> dict[str, Any]:
    raw = (
        row.get("live_match_row")
        or row.get("live_match")
        or row.get("features")
        or row.get("match")
    )

    if raw is None:
        raise ValueError(f"line {line_number}: missing live_match_row")

    if not isinstance(raw, dict):
        raise ValueError(f"line {line_number}: live_match_row must be a JSON object")

    return raw


def _resolve_event_id(
    row: dict[str, Any],
    live_match_row: dict[str, Any],
    *,
    line_number: int,
) -> int:
    for source in (row, live_match_row):
        for key in ("event_id", "fixture_id", "match_id"):
            if source.get(key) not in (None, ""):
                return int(source[key])

    raise ValueError(f"line {line_number}: missing event_id")


def _live_match_row_to_features(
    live_match_row: dict[str, Any],
    *,
    event_id: int,
    line_number: int,
) -> LiveGoalFeatures:
    return LiveGoalFeatures(
        event_id=event_id,
        minute=_required_int(live_match_row, "minute", line_number=line_number),
        home_score=_required_int(live_match_row, "home_score", line_number=line_number),
        away_score=_required_int(live_match_row, "away_score", line_number=line_number),
        home_xg_live=_optional_float(live_match_row, "home_xg_live"),
        away_xg_live=_optional_float(live_match_row, "away_xg_live"),
        home_shots_total=_optional_int(live_match_row, "home_shots_total"),
        away_shots_total=_optional_int(live_match_row, "away_shots_total"),
        home_shots_on_target=_optional_int(live_match_row, "home_shots_on_target"),
        away_shots_on_target=_optional_int(live_match_row, "away_shots_on_target"),
        home_corners=_optional_int(live_match_row, "home_corners"),
        away_corners=_optional_int(live_match_row, "away_corners"),
        home_red_cards=_optional_int(live_match_row, "home_red_cards"),
        away_red_cards=_optional_int(live_match_row, "away_red_cards"),
    )


def _resolve_offers(
    row: dict[str, Any],
    *,
    event_id: int,
    line_number: int,
) -> tuple[BookOffer, ...]:
    raw_offers = row.get("live_offer_rows") or row.get("offers")

    if raw_offers is None:
        raise ValueError(f"line {line_number}: missing live_offer_rows")

    if not isinstance(raw_offers, list):
        raise ValueError(f"line {line_number}: live_offer_rows must be a JSON array")

    if not raw_offers:
        raise ValueError(f"line {line_number}: live_offer_rows must not be empty")

    return tuple(
        _raw_offer_to_book_offer(raw_offer, event_id=event_id, line_number=line_number)
        for raw_offer in raw_offers
    )


def _raw_offer_to_book_offer(
    raw_offer: dict[str, Any],
    *,
    event_id: int,
    line_number: int,
) -> BookOffer:
    if not isinstance(raw_offer, dict):
        raise ValueError(f"line {line_number}: offer row must be a JSON object")

    return BookOffer(
        event_id=int(raw_offer.get("event_id", event_id)),
        bookmaker_id=raw_offer.get("bookmaker_id"),
        bookmaker_name=str(raw_offer.get("bookmaker_name", "UNKNOWN")),
        family=MarketFamily(str(raw_offer["family"])),
        side=MarketSide(str(raw_offer["side"])),
        period=Period(str(raw_offer.get("period", Period.FT.value))),
        team_role=TeamRole(str(raw_offer.get("team_role", TeamRole.NONE.value))),
        line=_optional_float(raw_offer, "line"),
        odds_decimal=float(raw_offer["odds_decimal"]),
        source_timestamp_utc=raw_offer.get("source_timestamp_utc"),
        freshness_seconds=_optional_int(raw_offer, "freshness_seconds"),
    )


def _resolve_theses(
    row: dict[str, Any],
    *,
    event_id: int,
    line_number: int,
) -> tuple[StatisticalThesis, ...]:
    raw_theses = row.get("theses")

    if raw_theses is not None:
        return _resolve_explicit_theses(raw_theses, event_id=event_id, line_number=line_number)

    raw_p_real_by_thesis = row.get("p_real_by_thesis") or row.get("probabilities_by_thesis")

    if isinstance(raw_p_real_by_thesis, dict) and raw_p_real_by_thesis:
        return tuple(
            _build_default_thesis(event_id, str(thesis_key))
            for thesis_key in raw_p_real_by_thesis.keys()
        )

    raise ValueError(f"line {line_number}: missing theses or p_real_by_thesis keys")


def _resolve_explicit_theses(
    raw_theses: Any,
    *,
    event_id: int,
    line_number: int,
) -> tuple[StatisticalThesis, ...]:
    if not isinstance(raw_theses, list):
        raise ValueError(f"line {line_number}: theses must be a JSON array")

    if not raw_theses:
        raise ValueError(f"line {line_number}: theses must not be empty")

    theses: list[StatisticalThesis] = []

    for raw_thesis in raw_theses:
        if not isinstance(raw_thesis, dict):
            raise ValueError(f"line {line_number}: thesis row must be a JSON object")

        thesis_key = str(raw_thesis["thesis_key"])
        theses.append(
            StatisticalThesis(
                event_id=int(raw_thesis.get("event_id", event_id)),
                thesis_key=ThesisKey(thesis_key),
                strength=float(raw_thesis.get("strength", 0.75)),
                confidence=float(raw_thesis.get("confidence", 0.75)),
            )
        )

    return tuple(theses)


def _build_default_thesis(event_id: int, thesis_key: str) -> StatisticalThesis:
    return StatisticalThesis(
        event_id=event_id,
        thesis_key=ThesisKey(thesis_key),
        strength=0.75,
        confidence=0.75,
    )


def _required_int(row: dict[str, Any], key: str, *, line_number: int) -> int:
    if row.get(key) in (None, ""):
        raise ValueError(f"line {line_number}: missing required field {key}")

    return int(row[key])


def _optional_int(row: dict[str, Any], key: str) -> int | None:
    if row.get(key) in (None, ""):
        return None

    return int(row[key])


def _optional_float(row: dict[str, Any], key: str) -> float | None:
    if row.get(key) in (None, ""):
        return None

    return float(row[key])

    