from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.fqis.contracts.enums import ThesisKey
from app.fqis.runtime.match_snapshot import (
    FqisMatchSnapshotBuildResult,
    build_match_snapshot_record,
    write_match_snapshot_jsonl,
)
from app.fqis.runtime.shadow import FqisShadowInput


@dataclass(slots=True, frozen=True)
class SemiliveSnapshotBuildResult:
    output_path: Path
    record_count: int
    total_offer_count: int
    event_ids: tuple[int, ...]


def load_semilive_source_rows(path: Path) -> tuple[dict[str, Any], ...]:
    if not path.exists():
        raise FileNotFoundError(f"semilive source file not found: {path}")

    rows: list[dict[str, Any]] = []

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

        rows.append(row)

    if not rows:
        raise ValueError(f"semilive source file is empty: {path}")

    return tuple(rows)


def build_shadow_inputs_from_semilive_rows(
    rows: tuple[dict[str, Any], ...],
) -> tuple[FqisShadowInput, ...]:
    return tuple(_row_to_shadow_input(row, line_number=index) for index, row in enumerate(rows, start=1))


def build_semilive_match_snapshot_records(
    rows: tuple[dict[str, Any], ...],
) -> tuple[dict[str, Any], ...]:
    shadow_inputs = build_shadow_inputs_from_semilive_rows(rows)
    records: list[dict[str, Any]] = []

    for index, shadow_input in enumerate(shadow_inputs, start=1):
        source_row = rows[index - 1]
        records.append(
            build_match_snapshot_record(
                shadow_input,
                source_audit={
                    "source": source_row.get("source", "semilive"),
                    "source_kind": "semilive_jsonl",
                    "source_index": index,
                    "source_event_id": source_row.get("event_id") or source_row.get("fixture_id"),
                    "source_match_label": source_row.get("match_label"),
                },
            )
        )

    return tuple(records)


def build_semilive_snapshot_from_jsonl(
    source_path: Path,
    output_path: Path,
) -> SemiliveSnapshotBuildResult:
    rows = load_semilive_source_rows(source_path)
    records = build_semilive_match_snapshot_records(rows)
    result = write_match_snapshot_jsonl(records, output_path)

    return _to_semilive_result(result)


def _row_to_shadow_input(row: dict[str, Any], *, line_number: int) -> FqisShadowInput:
    event_id = _resolve_event_id(row, line_number=line_number)
    live_match_row = _resolve_live_match_row(row, event_id=event_id, line_number=line_number)
    live_offer_rows = _resolve_live_offer_rows(row, event_id=event_id, line_number=line_number)
    p_real_by_thesis = _resolve_p_real_by_thesis(row, line_number=line_number)

    return FqisShadowInput(
        live_match_row=live_match_row,
        live_offer_rows=live_offer_rows,
        p_real_by_thesis=p_real_by_thesis,
    )


def _resolve_event_id(row: dict[str, Any], *, line_number: int) -> int:
    for key in ("event_id", "fixture_id", "match_id"):
        if row.get(key) not in (None, ""):
            return int(row[key])

    live_match = row.get("live_match") or row.get("live_match_row")
    if isinstance(live_match, dict):
        for key in ("event_id", "fixture_id", "match_id"):
            if live_match.get(key) not in (None, ""):
                return int(live_match[key])

    raise ValueError(f"line {line_number}: missing event_id")


def _resolve_live_match_row(
    row: dict[str, Any],
    *,
    event_id: int,
    line_number: int,
) -> dict[str, Any]:
    raw = row.get("live_match") or row.get("live_match_row") or row.get("features")

    if raw is None:
        raw = row

    if not isinstance(raw, dict):
        raise ValueError(f"line {line_number}: live_match must be a JSON object")

    required = (
        "home_xg_live",
        "away_xg_live",
        "home_shots_on_target",
        "away_shots_on_target",
        "minute",
        "home_score",
        "away_score",
    )

    for field in required:
        if field not in raw:
            raise ValueError(f"line {line_number}: missing live match field: {field}")

    return {
        "event_id": event_id,
        "home_xg_live": float(raw["home_xg_live"]),
        "away_xg_live": float(raw["away_xg_live"]),
        "home_shots_on_target": int(raw["home_shots_on_target"]),
        "away_shots_on_target": int(raw["away_shots_on_target"]),
        "minute": int(raw["minute"]),
        "home_score": int(raw["home_score"]),
        "away_score": int(raw["away_score"]),
    }


def _resolve_live_offer_rows(
    row: dict[str, Any],
    *,
    event_id: int,
    line_number: int,
) -> tuple[dict[str, Any], ...]:
    raw_offers = row.get("offers") or row.get("live_offer_rows")

    if raw_offers is None:
        raise ValueError(f"line {line_number}: missing offers")

    if not isinstance(raw_offers, list):
        raise ValueError(f"line {line_number}: offers must be a JSON array")

    offers: list[dict[str, Any]] = []

    for offer_index, raw_offer in enumerate(raw_offers, start=1):
        if not isinstance(raw_offer, dict):
            raise ValueError(f"line {line_number}: offer #{offer_index} must be a JSON object")

        offers.append(_normalize_offer(raw_offer, event_id=event_id, line_number=line_number, offer_index=offer_index))

    return tuple(offers)


def _normalize_offer(
    offer: dict[str, Any],
    *,
    event_id: int,
    line_number: int,
    offer_index: int,
) -> dict[str, Any]:
    required = (
        "bookmaker_name",
        "family",
        "side",
        "period",
        "team_role",
        "line",
        "odds_decimal",
    )

    for field in required:
        if field not in offer:
            raise ValueError(f"line {line_number}: offer #{offer_index} missing field: {field}")

    return {
        "event_id": int(offer.get("event_id", event_id)),
        "bookmaker_id": offer.get("bookmaker_id"),
        "bookmaker_name": str(offer["bookmaker_name"]),
        "family": str(offer["family"]),
        "side": str(offer["side"]),
        "period": str(offer["period"]),
        "team_role": str(offer["team_role"]),
        "line": offer.get("line"),
        "odds_decimal": float(offer["odds_decimal"]),
        "source_timestamp_utc": offer.get("source_timestamp_utc"),
        "freshness_seconds": offer.get("freshness_seconds"),
    }


def _resolve_p_real_by_thesis(
    row: dict[str, Any],
    *,
    line_number: int,
) -> dict[ThesisKey, dict[str, float]]:
    raw = row.get("p_real_by_thesis") or row.get("probabilities_by_thesis")

    if not isinstance(raw, dict) or not raw:
        raise ValueError(f"line {line_number}: missing p_real_by_thesis")

    parsed: dict[ThesisKey, dict[str, float]] = {}

    for thesis_key_raw, intent_probs_raw in raw.items():
        try:
            thesis_key = ThesisKey(str(thesis_key_raw))
        except ValueError as exc:
            raise ValueError(f"line {line_number}: unknown thesis key: {thesis_key_raw}") from exc

        if not isinstance(intent_probs_raw, dict):
            raise ValueError(f"line {line_number}: probabilities for {thesis_key.value} must be a JSON object")

        parsed[thesis_key] = {
            str(intent_key): float(probability)
            for intent_key, probability in intent_probs_raw.items()
        }

    return parsed


def _to_semilive_result(result: FqisMatchSnapshotBuildResult) -> SemiliveSnapshotBuildResult:
    return SemiliveSnapshotBuildResult(
        output_path=result.output_path,
        record_count=result.record_count,
        total_offer_count=result.total_offer_count,
        event_ids=result.event_ids,
    )

    